//! Atomic canonical-event transitions for admitted wallet projections.
//!
//! This module deliberately uses only exact `RocksDB` point reads while it
//! plans a transition. All six row families, the full projection accumulator,
//! the source cursor/digest, and READY control are then published in one
//! synchronous write batch. A malformed event or replay therefore leaves the
//! admitted wallet unchanged without falling back to a historical scan.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    time::{Duration, Instant},
};

use rust_rocksdb::{WriteBatch, WriteOptions};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
    CanonicalBlockFactsSequenceDigest, CanonicalBlockFactsSequenceDigestBuilder,
    CanonicalBlockReplayFormatVersion, Network, TransparentAddressScriptHash, TransparentOutPoint,
    ValidatedCanonicalBlockReplay, wire::UtxoSetCommitmentElement,
};
use zinder_store::{
    BoundedRocksDbOpen, CanonicalEventFence, CanonicalEventKind, CanonicalRetainedEvent,
    CanonicalStoreError, MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS,
};
use zinder_wallet_projection::{
    WalletAddressBalance, WalletAddressTransaction, WalletAddressTransactionKey,
    WalletAddressUnspentOutputKey, WalletCanonicalSourceIdentity, WalletOutpointKey,
    WalletProjectionBuildState, WalletProjectionDigestBuilder, WalletProjectionEventCursor,
    WalletProjectionReadyEvidence, WalletProjectionRowFamily, WalletProjectionSourcePosition,
    WalletReorgUndo, WalletSpentOutput, WalletStoreControlRecord, WalletTransactionPosition,
    WalletUnspentOutput, WalletUtxoSetSummary,
};

use crate::store::{
    REORG_UNDO_COLUMN_FAMILY, TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY, TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
    TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY, TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
    column_family, decode_only_control,
};
use crate::{RocksDbWalletError, RocksDbWalletStore};

#[cfg(test)]
use zinder_testkit::process_crash::crash_point;

#[cfg(not(test))]
const fn crash_point(_name: &str) {}

/// One unit of planner work on a retained reorg-undo row.
#[derive(Clone, Copy)]
enum UndoRowWork {
    DurableRead,
    ValueCopy,
    FullDecode,
}

#[cfg(not(test))]
const fn count_undo_row_work(_column_family_name: &str, _work: UndoRowWork) {}

/// Undo-row work the planner performed on this thread since the last take.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct UndoRowWorkCounts {
    durable_reads: u64,
    value_copies: u64,
    full_decodes: u64,
}

#[cfg(test)]
thread_local! {
    static UNDO_ROW_WORK: std::cell::Cell<UndoRowWorkCounts> = const {
        std::cell::Cell::new(UndoRowWorkCounts {
            durable_reads: 0,
            value_copies: 0,
            full_decodes: 0,
        })
    };
}

#[cfg(test)]
fn count_undo_row_work(column_family_name: &str, work: UndoRowWork) {
    if column_family_name != REORG_UNDO_COLUMN_FAMILY {
        return;
    }
    UNDO_ROW_WORK.with(|counts| {
        let mut next = counts.get();
        match work {
            UndoRowWork::DurableRead => next.durable_reads += 1,
            UndoRowWork::ValueCopy => next.value_copies += 1,
            UndoRowWork::FullDecode => next.full_decodes += 1,
        }
        counts.set(next);
    });
}

#[cfg(test)]
fn take_undo_row_work() -> UndoRowWorkCounts {
    UNDO_ROW_WORK.with(std::cell::Cell::take)
}

#[cfg(not(test))]
const fn durable_read_memo_room(unused_bytes: u64) -> u64 {
    unused_bytes
}

#[cfg(test)]
thread_local! {
    /// Whether plans on this thread leave their durable-read memo no room.
    static DURABLE_READ_MEMO_DISABLED: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

#[cfg(test)]
fn durable_read_memo_room(unused_bytes: u64) -> u64 {
    if DURABLE_READ_MEMO_DISABLED.get() {
        0
    } else {
        unused_bytes
    }
}

/// How a planned transition establishes that its undo suffix is publishable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UndoSuffixCheck {
    /// Reads and fully decodes every retained undo row.
    Full,
    /// Rechecks no row the plan keeps, which belong to the suffix this handle
    /// verified for the READY publication the plan starts from. The plan still
    /// reads each row it prunes and the successor that binds the new settled
    /// tip, and checks that the pruned rows' source digests connect to that
    /// successor. Every row it writes chains from the verified tip by
    /// construction.
    Incremental,
}

/// Largest permitted in-process logical-byte plan for one wallet transition.
///
/// Each caller selects a nonzero ceiling no larger than this. A transition
/// keeps the final key and value of each distinct row it touches and
/// publishes exactly those rows, followed by the control record, in one
/// write batch. The planner accounts each such row twice, once in its plan
/// and once in that batch, plus the control record. The rows it remembers
/// from durable reads use only what that leaves of the ceiling, and are
/// dropped before the batch is built.
pub const MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES: u64 = 1024 * 1024 * 1024;

/// What one published wallet transition planned, read, and wrote.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct WalletTransitionFootprint {
    /// Logical bytes the transition held against its ceiling, under the rule
    /// [`MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES`] describes.
    pub accounted_logical_bytes: u64,
    /// Distinct rows the transition wrote or deleted, not counting the
    /// control record.
    pub planned_rows: u64,
    /// Records in the published write batch, including the control record.
    pub batch_records: u64,
    /// Encoded size of the published write batch.
    pub batch_bytes: u64,
    /// Point reads the plan made against the store.
    pub durable_point_reads: u64,
    /// Time the synchronous batch write took.
    pub write_duration: Duration,
}

impl RocksDbWalletStore {
    /// Applies one exact retained canonical event and its bounded replay rows atomically.
    ///
    /// `expected_source` must name the source currently committed by this READY
    /// wallet. `event` must be the next retained event, `resulting_fence` its
    /// resulting canonical source fence, and `resulting_settled_tip` the
    /// canonical READY settlement boundary paired with that fence. Rows at or
    /// below that boundary are pruned in the same write batch. `replay_rows`
    /// must come directly from `RocksDbCanonicalSecondary::scan_canonical_replay_range`
    /// for the event's committed range. No historical canonical scan is
    /// accepted or performed by this API.
    #[allow(
        clippy::too_many_arguments,
        reason = "the public transition boundary keeps its authenticated source, fence, settlement, budget, and replay input explicit"
    )]
    pub fn apply_canonical_event_range<I>(
        &mut self,
        expected_source: WalletCanonicalSourceIdentity,
        event: CanonicalRetainedEvent,
        resulting_fence: CanonicalEventFence,
        resulting_settled_tip: BlockId,
        max_logical_bytes: NonZeroU64,
        replay_rows: I,
    ) -> Result<(), RocksDbWalletError>
    where
        I: IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
    {
        self.apply_canonical_event_range_cancellable(
            expected_source,
            event,
            resulting_fence,
            resulting_settled_tip,
            max_logical_bytes,
            replay_rows,
            || false,
        )
    }

    /// Plans one canonical event and abandons it if cancellation arrives before its write.
    ///
    /// The cancellation callback runs only after all source, replay, row, and
    /// accumulator checks have completed, immediately before the synchronous
    /// batch write. Returning `true` guarantees that no wallet bytes changed.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one atomic planner keeps every durable transition invariant in one boundary"
    )]
    pub fn apply_canonical_event_range_cancellable<I, Cancel>(
        &mut self,
        expected_source: WalletCanonicalSourceIdentity,
        event: CanonicalRetainedEvent,
        resulting_fence: CanonicalEventFence,
        resulting_settled_tip: BlockId,
        max_logical_bytes: NonZeroU64,
        replay_rows: I,
        cancelled_before_write: Cancel,
    ) -> Result<(), RocksDbWalletError>
    where
        I: IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
        Cancel: FnOnce() -> bool,
    {
        self.require_current_ready_control()?;
        let max_logical_bytes = validate_transition_logical_byte_limit(max_logical_bytes)?;
        let observed_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(&self.ready_evidence);
        if observed_source != expected_source {
            return Err(RocksDbWalletError::CanonicalSourceMismatch {
                expected: Box::new(expected_source),
                observed: Box::new(observed_source),
            });
        }
        validate_event_fences(expected_source, event, resulting_fence)?;
        validate_resulting_settled_tip(
            expected_source.settled_tip(),
            resulting_settled_tip,
            resulting_fence.visible_tip(),
        )?;
        let undo_suffix_check = match event.kind() {
            CanonicalEventKind::Committed
                if self.verified_undo_suffix_generation == Some(self.control.writer_generation) =>
            {
                UndoSuffixCheck::Incremental
            }
            CanonicalEventKind::Committed | CanonicalEventKind::Reorged => UndoSuffixCheck::Full,
        };

        let mut planner = WalletTransitionPlanner::new(self, max_logical_bytes);
        match event.kind() {
            CanonicalEventKind::Committed => {
                validate_committed_event_shape(&planner, event, resulting_fence)?;
            }
            CanonicalEventKind::Reorged => {
                let reverted = event.reverted_range().ok_or(
                    RocksDbWalletError::ProjectionTransitionRejected {
                        reason: "reorg event lacks a reverted canonical range",
                    },
                )?;
                validate_reorg_event_shape(&planner, event, resulting_fence, reverted)?;
                planner.rollback_range(reverted)?;
            }
        }

        planner.apply_replay_range(event.committed_range(), replay_rows)?;
        if planner.current_tip != resulting_fence.visible_tip() {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "applied replay tip differs from the resulting canonical fence",
            });
        }
        if planner.source_sequence_digest != resulting_fence.sequence_digest() {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "applied replay digest differs from the resulting canonical fence",
            });
        }
        planner.advance_settled_tip(resulting_settled_tip)?;
        planner.require_publishable_undo_suffix(undo_suffix_check)?;

        let event_cursor = WalletProjectionEventCursor::from_bytes(event.cursor().as_bytes())?;
        let source_position = WalletProjectionSourcePosition::with_event_cursor(
            resulting_fence.chain_epoch_id(),
            resulting_fence.visible_tip(),
            resulting_fence.chain_event_sequence(),
            event_cursor,
        )?;
        self.publish_planned_transition(
            planner.into_publication(source_position),
            cancelled_before_write,
        )
    }

    /// Reconciles a lagged following wallet directly to one current canonical fence.
    ///
    /// The supplied retained events must be contiguous after `expected_source`
    /// and end at `target_fence`. `rollback_range`, when present, must be the
    /// exact suffix of the persisted wallet state through a verified common
    /// ancestor. `replay_range` must then be exactly the current canonical
    /// suffix from that ancestor through `target_fence`. `target_settled_tip`
    /// must be the canonical READY settlement boundary paired with the target
    /// fence. This permits a page containing an append later overwritten by a
    /// reorg to converge without reading that overwritten append from current
    /// canonical rows.
    ///
    /// All rollback rows, replay rows, source cursor/digest, full projection
    /// accumulator, and READY control are published in one synchronous batch.
    /// The replay range is independently capped at the canonical 4,096-block
    /// incremental limit and this method never scans wallet or canonical
    /// history beyond its explicit point reads and supplied replay iterator.
    #[allow(
        clippy::too_many_arguments,
        reason = "the public reconciliation boundary keeps its retained history, authenticated target, settlement, rollback, and replay inputs explicit"
    )]
    pub fn reconcile_canonical_event_sequence<I>(
        &mut self,
        expected_source: WalletCanonicalSourceIdentity,
        retained_events: &[CanonicalRetainedEvent],
        target_fence: CanonicalEventFence,
        target_settled_tip: BlockId,
        rollback_range: Option<BlockHeightRange>,
        replay_range: BlockHeightRange,
        max_logical_bytes: NonZeroU64,
        replay_rows: I,
    ) -> Result<(), RocksDbWalletError>
    where
        I: IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
    {
        self.reconcile_canonical_event_sequence_cancellable(
            expected_source,
            retained_events,
            target_fence,
            target_settled_tip,
            rollback_range,
            replay_range,
            max_logical_bytes,
            replay_rows,
            || false,
        )
    }

    /// Plans a direct reconciliation and abandons it before the atomic write on cancellation.
    ///
    /// The callback runs after every retained-event, undo, replay, digest, and
    /// control-encoding check, immediately before the synchronous write.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "the one-batch reconciliation boundary keeps rollback and replay invariants coupled"
    )]
    pub fn reconcile_canonical_event_sequence_cancellable<I, Cancel>(
        &mut self,
        expected_source: WalletCanonicalSourceIdentity,
        retained_events: &[CanonicalRetainedEvent],
        target_fence: CanonicalEventFence,
        target_settled_tip: BlockId,
        rollback_range: Option<BlockHeightRange>,
        replay_range: BlockHeightRange,
        max_logical_bytes: NonZeroU64,
        replay_rows: I,
        cancelled_before_write: Cancel,
    ) -> Result<(), RocksDbWalletError>
    where
        I: IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
        Cancel: FnOnce() -> bool,
    {
        self.require_current_ready_control()?;
        let max_logical_bytes = validate_transition_logical_byte_limit(max_logical_bytes)?;
        let observed_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(&self.ready_evidence);
        if observed_source != expected_source {
            return Err(RocksDbWalletError::CanonicalSourceMismatch {
                expected: Box::new(expected_source),
                observed: Box::new(observed_source),
            });
        }
        validate_reconciliation_event_sequence(expected_source, retained_events, target_fence)?;
        validate_resulting_settled_tip(
            expected_source.settled_tip(),
            target_settled_tip,
            target_fence.visible_tip(),
        )?;

        let mut planner = WalletTransitionPlanner::new(self, max_logical_bytes);
        if let Some(rollback_range) = rollback_range {
            validate_reconciliation_rollback_range(&planner, rollback_range)?;
            planner.rollback_range(rollback_range)?;
        }

        let expected_replay_range = replay_range_from_ancestor(planner.current_tip, target_fence)?;
        if replay_range != expected_replay_range {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "current canonical replay range is not the exact suffix from the verified ancestor",
            });
        }
        validate_incremental_replay_range(replay_range)?;
        planner.apply_replay_range(replay_range, replay_rows)?;
        if planner.current_tip != target_fence.visible_tip() {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "reconciled replay tip differs from the target canonical fence",
            });
        }
        if planner.source_sequence_digest != target_fence.sequence_digest() {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "reconciled replay digest differs from the target canonical fence",
            });
        }
        planner.advance_settled_tip(target_settled_tip)?;
        planner.require_publishable_undo_suffix(UndoSuffixCheck::Full)?;

        let final_event =
            retained_events
                .last()
                .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "reconciliation requires at least one retained event",
                })?;
        let event_cursor =
            WalletProjectionEventCursor::from_bytes(final_event.cursor().as_bytes())?;
        let source_position = WalletProjectionSourcePosition::with_event_cursor(
            target_fence.chain_epoch_id(),
            target_fence.visible_tip(),
            target_fence.chain_event_sequence(),
            event_cursor,
        )?;
        self.publish_planned_transition(
            planner.into_publication(source_position),
            cancelled_before_write,
        )
    }

    fn publish_planned_transition<Cancel>(
        &mut self,
        publication: PlannedPublication,
        cancelled_before_write: Cancel,
    ) -> Result<(), RocksDbWalletError>
    where
        Cancel: FnOnce() -> bool,
    {
        let PlannedPublication {
            rows: planned_rows,
            ready_evidence,
            mut logical_bytes,
            durable_point_reads,
        } = publication;
        let writer_generation = self
            .control
            .writer_generation
            .checked_add(1)
            .ok_or(RocksDbWalletError::ProjectionTransitionGenerationOverflow)?;
        let next_control = WalletStoreControlRecord {
            network: self.control.network,
            supported_reorg_depth: self.control.supported_reorg_depth,
            writer_generation,
            build_lease: None,
            build_state: WalletProjectionBuildState::Ready(ready_evidence.clone()),
        };
        let encoded_control = next_control.encode()?;
        logical_bytes.reserve_control(
            zinder_wallet_projection::WALLET_STORE_CONTROL_KEY,
            &encoded_control,
        )?;
        let planned_row_count = u64::try_from(planned_rows.len()).unwrap_or(u64::MAX);
        let batch = coalesced_write_batch(
            &self.bounded_open,
            planned_rows,
            zinder_wallet_projection::WALLET_STORE_CONTROL_KEY,
            &encoded_control,
        )?;

        // Re-admit immediately before publication so a sequential second
        // handle cannot overwrite a newer READY control it did not observe.
        self.require_current_ready_control()?;
        if cancelled_before_write() {
            return Err(RocksDbWalletError::ProjectionTransitionCancelled);
        }
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        // A failed or unconfirmed write leaves no verified suffix, so the next
        // transition reads the whole suffix again.
        self.verified_undo_suffix_generation = None;
        crash_point("wallet_projection.transition.before_atomic_write");
        let write_started = Instant::now();
        self.bounded_open
            .db
            .write_opt(&batch, &write_options)
            .map_err(|source| {
                RocksDbWalletError::rocksdb("atomic wallet projection transition", source)
            })?;
        let write_duration = write_started.elapsed();
        crash_point("wallet_projection.transition.after_atomic_write");

        self.control = next_control;
        self.ready_evidence = ready_evidence;
        let persisted = decode_only_control(&self.bounded_open)?;
        if persisted != self.control {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "READY control differs after atomic wallet transition",
            });
        }
        self.verified_undo_suffix_generation = Some(self.control.writer_generation);
        self.last_transition_footprint = Some(WalletTransitionFootprint {
            accounted_logical_bytes: logical_bytes.accounted_bytes(),
            planned_rows: planned_row_count,
            batch_records: u64::try_from(batch.len()).unwrap_or(u64::MAX),
            batch_bytes: u64::try_from(batch.size_in_bytes()).unwrap_or(u64::MAX),
            durable_point_reads,
            write_duration,
        });
        Ok(())
    }

    fn require_current_ready_control(&self) -> Result<(), RocksDbWalletError> {
        let persisted = decode_only_control(&self.bounded_open)?;
        if persisted != self.control {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "wallet control changed since READY admission",
            });
        }
        let WalletProjectionBuildState::Ready(evidence) = &persisted.build_state else {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "wallet transition requires an admitted READY control",
            });
        };
        if *evidence != self.ready_evidence {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "wallet READY evidence changed since admission",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
fn source_identity_from_fence(fence: CanonicalEventFence) -> WalletCanonicalSourceIdentity {
    WalletCanonicalSourceIdentity::new(
        WalletProjectionSourcePosition::new(
            fence.chain_epoch_id(),
            fence.visible_tip(),
            fence.chain_event_sequence(),
        ),
        fence.sequence_digest(),
        fence.visible_tip(),
    )
}

fn validate_event_fences(
    expected_source: WalletCanonicalSourceIdentity,
    event: CanonicalRetainedEvent,
    resulting: CanonicalEventFence,
) -> Result<(), RocksDbWalletError> {
    let expected = expected_source.source_position();
    if event.resulting_fence() != resulting {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "retained event fence differs from the requested resulting canonical fence",
        });
    }
    if event.previous_epoch_id() != Some(expected.chain_epoch_id) {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "retained event does not name the wallet source epoch as its predecessor",
        });
    }
    if event.resulting_epoch_id() != resulting.chain_epoch_id()
        || event.cursor().event_sequence() != resulting.chain_event_sequence()
    {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "retained event cursor or epoch differs from the resulting canonical fence",
        });
    }
    let expected_sequence = expected
        .event_sequence
        .checked_add(1)
        .ok_or(RocksDbWalletError::ProjectionTransitionGenerationOverflow)?;
    if resulting.chain_event_sequence() != expected_sequence {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "retained event does not immediately follow the wallet source cursor",
        });
    }
    Ok(())
}

fn validate_resulting_settled_tip(
    previous_settled_tip: BlockId,
    resulting_settled_tip: BlockId,
    resulting_visible_tip: BlockId,
) -> Result<(), RocksDbWalletError> {
    if resulting_settled_tip.height < previous_settled_tip.height
        || resulting_settled_tip.height > resulting_visible_tip.height
        || (resulting_settled_tip.height == previous_settled_tip.height
            && resulting_settled_tip.hash != previous_settled_tip.hash)
        || (resulting_settled_tip.height == resulting_visible_tip.height
            && resulting_settled_tip.hash != resulting_visible_tip.hash)
    {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "resulting canonical settled tip is not a monotonic ancestor of the resulting visible tip",
        });
    }
    Ok(())
}

fn validate_reconciliation_event_sequence(
    expected_source: WalletCanonicalSourceIdentity,
    retained_events: &[CanonicalRetainedEvent],
    target_fence: CanonicalEventFence,
) -> Result<(), RocksDbWalletError> {
    let first =
        retained_events
            .first()
            .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "reconciliation requires at least one retained event",
            })?;
    let source_position = expected_source.source_position();
    let mut previous_epoch = source_position.chain_epoch_id;
    let mut previous_sequence = source_position.event_sequence;
    let mut previous_tip = source_position.tip;
    let mut previous_digest = expected_source.source_sequence_digest();

    if first.previous_epoch_id() != Some(previous_epoch) {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "first retained event does not name the persisted wallet epoch as its predecessor",
        });
    }

    for event in retained_events {
        let expected_sequence = previous_sequence
            .checked_add(1)
            .ok_or(RocksDbWalletError::ProjectionTransitionGenerationOverflow)?;
        let resulting = event.resulting_fence();
        if event.cursor().event_sequence() != expected_sequence
            || resulting.chain_event_sequence() != expected_sequence
            || event.resulting_epoch_id() != resulting.chain_epoch_id()
        {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "retained reconciliation event cursor or epoch is not contiguous",
            });
        }
        if event.previous_epoch_id() != Some(previous_epoch) {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "retained reconciliation event does not name its immediate predecessor epoch",
            });
        }
        validate_reconciliation_event_shape(event, previous_tip, previous_digest)?;
        previous_epoch = resulting.chain_epoch_id();
        previous_sequence = resulting.chain_event_sequence();
        previous_tip = resulting.visible_tip();
        previous_digest = resulting.sequence_digest();
    }

    let final_event =
        retained_events
            .last()
            .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "reconciliation requires at least one retained event",
            })?;
    if final_event.resulting_fence() != target_fence {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "retained reconciliation sequence does not end at the requested target fence",
        });
    }
    Ok(())
}

#[allow(
    clippy::suspicious_operation_groupings,
    reason = "the reorg event compares its reverted end to the predecessor tip and its committed end to the resulting tip"
)]
fn validate_reconciliation_event_shape(
    event: &CanonicalRetainedEvent,
    previous_tip: BlockId,
    previous_digest: CanonicalBlockFactsSequenceDigest,
) -> Result<(), RocksDbWalletError> {
    let resulting = event.resulting_fence();
    let committed = event.committed_range();
    validate_event_range(committed, true)?;
    match event.kind() {
        CanonicalEventKind::Committed => {
            if event.reverted_range().is_some() {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "committed reconciliation event unexpectedly carries a reverted range",
                });
            }
            if range_is_anchored_empty(committed) {
                if resulting.visible_tip() != previous_tip
                    || resulting.sequence_digest() != previous_digest
                {
                    return Err(RocksDbWalletError::ProjectionTransitionRejected {
                        reason: "empty committed reconciliation event changes canonical block state",
                    });
                }
                return Ok(());
            }
            let expected_start = previous_tip.height.next().ok_or(
                RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "retained committed event extends beyond the height domain",
                },
            )?;
            if committed.start != expected_start || committed.end != resulting.visible_tip().height
            {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "committed reconciliation event is not an exact append from its predecessor tip",
                });
            }
        }
        CanonicalEventKind::Reorged => {
            let reverted =
                event
                    .reverted_range()
                    .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
                        reason: "reorg reconciliation event lacks a reverted canonical range",
                    })?;
            validate_event_range(reverted, false)?;
            if reverted.end != previous_tip.height
                || committed.start != reverted.start
                || committed.end != resulting.visible_tip().height
            {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "reorg reconciliation event does not exactly replace its predecessor suffix",
                });
            }
        }
    }
    Ok(())
}

fn validate_reconciliation_rollback_range(
    planner: &WalletTransitionPlanner<'_>,
    rollback_range: BlockHeightRange,
) -> Result<(), RocksDbWalletError> {
    validate_event_range(rollback_range, false)?;
    if rollback_range.end != planner.current_tip.height {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "reconciliation rollback is not the exact persisted wallet tip suffix",
        });
    }
    let rollback_count = inclusive_range_len(rollback_range)?;
    if rollback_count > u64::from(planner.supported_reorg_depth) {
        return Err(RocksDbWalletError::WalletProjectionRebuildRequired {
            reason: "requested reconciliation rollback exceeds the wallet's retained undo capacity",
        });
    }
    Ok(())
}

fn replay_range_from_ancestor(
    ancestor: BlockId,
    target_fence: CanonicalEventFence,
) -> Result<BlockHeightRange, RocksDbWalletError> {
    let target_tip = target_fence.visible_tip();
    if target_tip.height < ancestor.height {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "reconciliation target precedes the verified common ancestor",
        });
    }
    if target_tip.height == ancestor.height {
        return Ok(BlockHeightRange::empty_at(ancestor.height));
    }
    let start = ancestor
        .height
        .next()
        .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "reconciliation replay starts beyond the height domain",
        })?;
    Ok(BlockHeightRange::inclusive(start, target_tip.height))
}

fn validate_incremental_replay_range(
    replay_range: BlockHeightRange,
) -> Result<(), RocksDbWalletError> {
    let replay_count = inclusive_range_len(replay_range)?;
    if replay_count > u64::from(MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS) {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "reconciliation replay exceeds the canonical incremental replay limit",
        });
    }
    Ok(())
}

fn validate_event_range(
    range: BlockHeightRange,
    allow_anchored_empty: bool,
) -> Result<(), RocksDbWalletError> {
    if range.start <= range.end || (allow_anchored_empty && range_is_anchored_empty(range)) {
        return Ok(());
    }
    Err(RocksDbWalletError::ProjectionTransitionRejected {
        reason: "canonical event range is neither inclusive nor anchored empty",
    })
}

fn range_is_anchored_empty(range: BlockHeightRange) -> bool {
    range.start > range.end && range.end.next() == Some(range.start)
}

fn inclusive_range_len(range: BlockHeightRange) -> Result<u64, RocksDbWalletError> {
    if range_is_anchored_empty(range) {
        return Ok(0);
    }
    validate_event_range(range, false)?;
    u64::from(range.end.value())
        .checked_sub(u64::from(range.start.value()))
        .and_then(|distance| distance.checked_add(1))
        .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "canonical range length overflows the replay domain",
        })
}

fn extend_source_sequence_digest(
    source_sequence_digest_before: CanonicalBlockFactsSequenceDigest,
    replay_digest: zinder_core::CanonicalBlockFactsDigest,
) -> Result<CanonicalBlockFactsSequenceDigest, RocksDbWalletError> {
    let expected_source_count = source_sequence_digest_before
        .block_count()
        .checked_add(1)
        .ok_or(zinder_core::CanonicalBlockFactsSequenceLengthOverflow)?;
    let mut source_builder =
        CanonicalBlockFactsSequenceDigestBuilder::resume_from_prefix(source_sequence_digest_before);
    source_builder.try_append(replay_digest)?;
    let source_sequence_digest_after = source_builder.finish();
    if source_sequence_digest_after.block_count() != expected_source_count {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "bounded canonical replay does not advance the source digest by one block",
        });
    }
    Ok(source_sequence_digest_after)
}

fn validate_committed_event_shape(
    planner: &WalletTransitionPlanner<'_>,
    event: CanonicalRetainedEvent,
    resulting: CanonicalEventFence,
) -> Result<(), RocksDbWalletError> {
    if event.reverted_range().is_some() {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "committed event unexpectedly carries a reverted range",
        });
    }
    let range = event.committed_range();
    if range.start > range.end {
        if resulting.visible_tip() != planner.current_tip
            || resulting.sequence_digest() != planner.source_sequence_digest
        {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "empty committed event changes wallet block state",
            });
        }
        return Ok(());
    }
    let expected_start = planner.current_tip.height.next().ok_or(
        RocksDbWalletError::ProjectionTransitionRejected {
            reason: "wallet tip cannot be extended beyond the height domain",
        },
    )?;
    if range.start != expected_start || range.end != resulting.visible_tip().height {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "committed range is not the exact append from the wallet tip",
        });
    }
    Ok(())
}

fn validate_reorg_event_shape(
    planner: &WalletTransitionPlanner<'_>,
    event: CanonicalRetainedEvent,
    resulting: CanonicalEventFence,
    reverted: BlockHeightRange,
) -> Result<(), RocksDbWalletError> {
    if reverted.start > reverted.end
        || reverted.end != planner.current_tip.height
        || event.committed_range().start != reverted.start
        || event.committed_range().start > event.committed_range().end
        || event.committed_range().end != resulting.visible_tip().height
    {
        return Err(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "reorg ranges do not exactly replace the wallet tip suffix",
        });
    }
    let depth = reverted
        .end
        .value()
        .checked_sub(reverted.start.value())
        .and_then(|distance| distance.checked_add(1))
        .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
            reason: "reorg range has no positive bounded depth",
        })?;
    if depth > planner.supported_reorg_depth {
        return Err(RocksDbWalletError::WalletProjectionRebuildRequired {
            reason: "requested canonical reorg exceeds the wallet's retained undo capacity",
        });
    }
    Ok(())
}

fn validate_transition_logical_byte_limit(
    max_logical_bytes: NonZeroU64,
) -> Result<u64, RocksDbWalletError> {
    let requested_bytes = max_logical_bytes.get();
    if requested_bytes > MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES {
        return Err(RocksDbWalletError::InvalidTransitionLogicalByteLimit {
            requested_bytes,
            maximum_bytes: MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES,
        });
    }
    Ok(requested_bytes)
}

/// Logical bytes that one transition holds against its limit before its write.
///
/// The plan keeps one final entry per distinct row it touches, and the write
/// batch that publishes it copies exactly those entries, so each entry counts
/// twice. The control record joins the batch last and counts once.
#[derive(Debug)]
struct TransitionLogicalByteAccounting {
    limit: u64,
    /// Key and final value bytes of every distinct row the plan touches.
    overlay: u64,
    control: u64,
}

impl TransitionLogicalByteAccounting {
    const fn new(limit: u64) -> Self {
        Self {
            limit,
            overlay: 0,
            control: 0,
        }
    }

    fn reserve_put(
        &mut self,
        key: &[u8],
        encoded_value: &[u8],
        previous_overlay: Option<&Option<Vec<u8>>>,
    ) -> Result<(), RocksDbWalletError> {
        self.replace_overlay_entry(key, previous_overlay, Some(encoded_value))
    }

    fn reserve_delete(
        &mut self,
        key: &[u8],
        previous_overlay: Option<&Option<Vec<u8>>>,
    ) -> Result<(), RocksDbWalletError> {
        self.replace_overlay_entry(key, previous_overlay, None)
    }

    fn reserve_control(
        &mut self,
        key: &[u8],
        encoded_control: &[u8],
    ) -> Result<(), RocksDbWalletError> {
        let control_bytes = logical_entry_bytes(key, Some(encoded_control))?;
        self.ensure_within_limit(self.overlay, control_bytes)?;
        self.control = control_bytes;
        Ok(())
    }

    fn replace_overlay_entry(
        &mut self,
        key: &[u8],
        previous_overlay: Option<&Option<Vec<u8>>>,
        next_overlay_value: Option<&[u8]>,
    ) -> Result<(), RocksDbWalletError> {
        let previous_overlay_bytes = match previous_overlay {
            Some(previous_value) => logical_entry_bytes(key, previous_value.as_deref())?,
            None => 0,
        };
        let next_overlay_entry_bytes = logical_entry_bytes(key, next_overlay_value)?;
        let next_overlay_bytes = self
            .overlay
            .checked_sub(previous_overlay_bytes)
            .ok_or(RocksDbWalletError::TransitionLogicalByteAccountingOverflow)?;
        let next_overlay_bytes =
            checked_transition_bytes(next_overlay_bytes, next_overlay_entry_bytes)?;
        self.ensure_within_limit(next_overlay_bytes, self.control)?;
        self.overlay = next_overlay_bytes;
        Ok(())
    }

    fn ensure_within_limit(
        &self,
        overlay_bytes: u64,
        control_bytes: u64,
    ) -> Result<(), RocksDbWalletError> {
        let planned_and_batched_bytes = overlay_bytes
            .checked_mul(2)
            .ok_or(RocksDbWalletError::TransitionLogicalByteAccountingOverflow)?;
        let required_bytes = checked_transition_bytes(planned_and_batched_bytes, control_bytes)?;
        if required_bytes > self.limit {
            return Err(RocksDbWalletError::TransitionLogicalByteLimit {
                limit_bytes: self.limit,
                required_bytes,
            });
        }
        Ok(())
    }

    /// Bytes the planned rows and their write batch hold against the limit.
    const fn accounted_bytes(&self) -> u64 {
        self.overlay.saturating_mul(2).saturating_add(self.control)
    }

    /// Bytes of the limit that the planned rows and their write batch leave
    /// unused.
    const fn unused_bytes(&self) -> u64 {
        self.limit.saturating_sub(self.accounted_bytes())
    }
}

fn logical_entry_bytes(
    key: &[u8],
    encoded_value: Option<&[u8]>,
) -> Result<u64, RocksDbWalletError> {
    let key_bytes = u64::try_from(key.len())
        .map_err(|_| RocksDbWalletError::TransitionLogicalByteAccountingOverflow)?;
    let value_bytes = encoded_value.map_or(Ok(0), |encoded_value| {
        u64::try_from(encoded_value.len())
            .map_err(|_| RocksDbWalletError::TransitionLogicalByteAccountingOverflow)
    })?;
    checked_transition_bytes(key_bytes, value_bytes)
}

fn checked_transition_bytes(left: u64, right: u64) -> Result<u64, RocksDbWalletError> {
    left.checked_add(right)
        .ok_or(RocksDbWalletError::TransitionLogicalByteAccountingOverflow)
}

fn decode_undo(key: &[u8], encoded: &[u8]) -> Result<WalletReorgUndo, RocksDbWalletError> {
    count_undo_row_work(REORG_UNDO_COLUMN_FAMILY, UndoRowWork::FullDecode);
    Ok(WalletReorgUndo::decode(key, encoded)?)
}

type OverlayKey = (&'static str, Vec<u8>);

/// The final value of every row one plan touches, or `None` for a row it
/// deletes.
type PlannedRows = BTreeMap<OverlayKey, Option<Vec<u8>>>;

/// A finished plan, ready to publish.
struct PlannedPublication {
    rows: PlannedRows,
    ready_evidence: WalletProjectionReadyEvidence,
    logical_bytes: TransitionLogicalByteAccounting,
    durable_point_reads: u64,
}

/// Write-batch header bytes: an 8-byte sequence number and a 4-byte count.
const WRITE_BATCH_HEADER_BYTES: usize = 12;

/// Largest framing of one write-batch record: a type tag, a column-family id,
/// and the key and value lengths, each id or length a varint32 of at most five
/// bytes.
const WRITE_BATCH_RECORD_FRAMING_BYTES: usize = 16;

/// Builds the one write batch that publishes a plan: the final value of each
/// planned row in key order, followed by the control record.
///
/// The batch reserves its whole size up front, so building it never
/// reallocates.
fn coalesced_write_batch(
    bounded_open: &BoundedRocksDbOpen,
    planned_rows: PlannedRows,
    control_key: &[u8],
    encoded_control: &[u8],
) -> Result<WriteBatch, RocksDbWalletError> {
    let mut batch = WriteBatch::with_capacity_bytes(coalesced_write_batch_capacity(
        &planned_rows,
        control_key,
        encoded_control,
    ));
    let mut family = None;
    for ((family_name, key), planned) in planned_rows {
        let handle = match family.take() {
            Some((name, handle)) if name == family_name => handle,
            Some(_) | None => column_family(bounded_open, family_name)?,
        };
        match planned {
            Some(encoded) => batch.put_cf(&handle, key, encoded),
            None => batch.delete_cf(&handle, key),
        }
        family = Some((family_name, handle));
    }
    batch.put(control_key, encoded_control);
    Ok(batch)
}

/// Upper bound on the size of the write batch that publishes `planned_rows`
/// and the control record.
fn coalesced_write_batch_capacity(
    planned_rows: &PlannedRows,
    control_key: &[u8],
    encoded_control: &[u8],
) -> usize {
    planned_rows.iter().fold(
        WRITE_BATCH_HEADER_BYTES
            .saturating_add(WRITE_BATCH_RECORD_FRAMING_BYTES)
            .saturating_add(control_key.len())
            .saturating_add(encoded_control.len()),
        |capacity, ((_, key), planned)| {
            capacity
                .saturating_add(WRITE_BATCH_RECORD_FRAMING_BYTES)
                .saturating_add(key.len())
                .saturating_add(planned.as_ref().map_or(0, Vec::len))
        },
    )
}

/// Durable rows, or their absence, that one plan has read and not yet written.
///
/// A plan runs under an exclusive borrow of the store's only writer handle,
/// so its durable rows cannot change between two of its reads, and a later
/// read of the same key reuses the first observation. The memo holds only the
/// bytes that the plan's rows and their write batch leave of its logical-byte
/// limit, and forgets every read when they need that room, so it never
/// changes which plans the limit refuses. The plan drops the memo before it
/// builds that batch, so the memo, the planned rows, and the batch never hold
/// more than the limit together.
#[derive(Default)]
struct DurableReadMemo {
    rows: BTreeMap<OverlayKey, Option<Vec<u8>>>,
    logical_bytes: u64,
}

impl DurableReadMemo {
    fn get(&self, key: &OverlayKey) -> Option<&Option<Vec<u8>>> {
        self.rows.get(key)
    }

    /// Remembers one durable read if the memo still fits in `available_bytes`.
    fn remember(
        &mut self,
        key: OverlayKey,
        encoded: Option<&[u8]>,
        available_bytes: u64,
    ) -> Result<(), RocksDbWalletError> {
        let entry_bytes = logical_entry_bytes(&key.1, encoded)?;
        let next_bytes = checked_transition_bytes(self.logical_bytes, entry_bytes)?;
        if next_bytes <= available_bytes {
            self.rows.insert(key, encoded.map(<[u8]>::to_vec));
            self.logical_bytes = next_bytes;
        }
        Ok(())
    }

    /// Forgets the read of `key`, which the plan now writes, and forgets every
    /// read if the memo still does not fit in `available_bytes`.
    fn release_for_write(
        &mut self,
        key: &OverlayKey,
        available_bytes: u64,
    ) -> Result<(), RocksDbWalletError> {
        if let Some(encoded) = self.rows.remove(key) {
            let entry_bytes = logical_entry_bytes(&key.1, encoded.as_deref())?;
            self.logical_bytes = self
                .logical_bytes
                .checked_sub(entry_bytes)
                .ok_or(RocksDbWalletError::TransitionLogicalByteAccountingOverflow)?;
        }
        if self.logical_bytes > available_bytes {
            *self = Self::default();
        }
        Ok(())
    }
}

struct WalletTransitionPlanner<'store> {
    store: &'store RocksDbWalletStore,
    overlay: PlannedRows,
    durable_reads: DurableReadMemo,
    durable_point_reads: u64,
    logical_bytes: TransitionLogicalByteAccounting,
    accumulator: WalletProjectionDigestBuilder,
    accumulator_row_counts: zinder_wallet_projection::WalletProjectionFamilyRowCounts,
    utxo_summary: WalletUtxoSetSummary,
    current_tip: BlockId,
    settled_tip: BlockId,
    source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    supported_reorg_depth: u32,
    network: Network,
}

impl<'store> WalletTransitionPlanner<'store> {
    fn new(store: &'store RocksDbWalletStore, max_logical_bytes: u64) -> Self {
        Self {
            store,
            overlay: BTreeMap::new(),
            durable_reads: DurableReadMemo::default(),
            durable_point_reads: 0,
            logical_bytes: TransitionLogicalByteAccounting::new(max_logical_bytes),
            accumulator: WalletProjectionDigestBuilder::from_parts(
                store.ready_evidence.projection_accumulator.clone(),
                store.ready_evidence.row_counts,
            ),
            accumulator_row_counts: store.ready_evidence.row_counts,
            utxo_summary: store.ready_evidence.utxo_summary.clone(),
            current_tip: store.ready_evidence.source_position.tip,
            settled_tip: store.ready_evidence.settled_tip,
            source_sequence_digest: store.ready_evidence.source_sequence_digest,
            supported_reorg_depth: store.control.supported_reorg_depth,
            network: store.control.network,
        }
    }

    fn apply_replay_range<I>(
        &mut self,
        range: BlockHeightRange,
        replay_rows: I,
    ) -> Result<(), RocksDbWalletError>
    where
        I: IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
    {
        validate_incremental_replay_range(range)?;
        let mut expected_heights = range.into_iter();
        for replay in replay_rows {
            let replay = replay.map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
            let expected_height = expected_heights.next().ok_or(
                RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "bounded canonical replay contains more rows than its event range",
                },
            )?;
            if replay.facts().block_header.height != expected_height {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "bounded canonical replay height differs from its event range",
                });
            }
            self.apply_replay(&replay)?;
        }
        if expected_heights.next().is_some() {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "bounded canonical replay ends before its event range",
            });
        }
        Ok(())
    }

    /// Ends the plan with its planned rows, the READY evidence they publish,
    /// and their accounting, dropping the remembered durable reads.
    fn into_publication(
        self,
        source_position: WalletProjectionSourcePosition,
    ) -> PlannedPublication {
        let (projection_accumulator, projection_digest) =
            self.accumulator.finish_with_accumulator();
        PlannedPublication {
            rows: self.overlay,
            ready_evidence: WalletProjectionReadyEvidence {
                source_position,
                source_sequence_digest: self.source_sequence_digest,
                settled_tip: self.settled_tip,
                projection_digest,
                projection_accumulator,
                row_counts: self.accumulator_row_counts,
                utxo_summary: self.utxo_summary,
            },
            logical_bytes: self.logical_bytes,
            durable_point_reads: self.durable_point_reads,
        }
    }

    fn apply_replay(
        &mut self,
        replay: &ValidatedCanonicalBlockReplay,
    ) -> Result<(), RocksDbWalletError> {
        if replay.format_version() != CanonicalBlockReplayFormatVersion::V1
            || replay.reference_digest()
                != replay.facts().digest(CanonicalBlockFactsDigestVersion::V1)
        {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "bounded canonical replay does not satisfy the admitted facts contract",
            });
        }
        let facts = replay.facts();
        let block = BlockId::new(facts.block_header.height, facts.block_header.block_hash);
        let expected_height = self.current_tip.height.next().ok_or(
            RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet tip cannot be extended beyond the height domain",
            },
        )?;
        if block.height != expected_height
            || facts.block_header.parent_hash != self.current_tip.hash
        {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "bounded canonical replay is detached from the wallet tip",
            });
        }
        let source_sequence_digest_before = self.source_sequence_digest;
        let source_sequence_digest_after = extend_source_sequence_digest(
            source_sequence_digest_before,
            replay.reference_digest(),
        )?;
        self.apply_block_rows(
            facts,
            block,
            source_sequence_digest_before,
            source_sequence_digest_after,
        )?;
        self.current_tip = block;
        self.source_sequence_digest = source_sequence_digest_after;
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "block-local output, spend, and inverse-delta planning must remain visibly coupled"
    )]
    fn apply_block_rows(
        &mut self,
        facts: &CanonicalBlockFacts,
        block: BlockId,
        source_sequence_digest_before: CanonicalBlockFactsSequenceDigest,
        source_sequence_digest_after: CanonicalBlockFactsSequenceDigest,
    ) -> Result<(), RocksDbWalletError> {
        let mut created_outpoints = Vec::new();
        let mut created_outpoint_keys = BTreeSet::new();
        let mut spent_outpoints = Vec::new();
        let mut address_transaction_keys = Vec::new();

        for (transaction_index, transaction) in facts.transactions.iter().enumerate() {
            let tx_index_in_block = u32::try_from(transaction_index).map_err(|_| {
                zinder_wallet_projection::WalletProjectionContractError::FactIndexOverflow
            })?;
            let transaction_id = transaction.public_facts.transaction_id;
            let transaction_position =
                WalletTransactionPosition::new(transaction_id, tx_index_in_block, block);
            let mut touched_addresses = BTreeSet::new();

            for (input_position, input) in transaction.transparent_inputs.iter().enumerate() {
                let expected_input_index = u32::try_from(input_position).map_err(|_| {
                    zinder_wallet_projection::WalletProjectionContractError::FactIndexOverflow
                })?;
                if input.input_index != expected_input_index {
                    return Err(
                        zinder_wallet_projection::WalletProjectionContractError::FactIndexMismatch
                            .into(),
                    );
                }
                if input.spent_outpoint.is_coinbase_sentinel() {
                    continue;
                }
                let key = WalletOutpointKey::new(input.spent_outpoint);
                let output = match self.read_unspent(key)? {
                    Some(output) => output,
                    None if self.read_spent(key)?.is_some() => {
                        return Err(
                            zinder_wallet_projection::WalletProjectionContractError::DuplicateSpend
                                .into(),
                        );
                    }
                    None => {
                        return Err(zinder_wallet_projection::WalletProjectionContractError::MissingTransparentPredecessor.into());
                    }
                };
                self.remove_unspent(&output)?;
                touched_addresses.insert(output.address_script_hash.as_bytes());
                self.insert_spent(&WalletSpentOutput::new(
                    output,
                    transaction_position,
                    input.input_index,
                ))?;
                if !created_outpoint_keys.contains(&key) {
                    spent_outpoints.push(key);
                }
            }

            for (output_position, output) in transaction.transparent_outputs.iter().enumerate() {
                let expected_output_index = u32::try_from(output_position).map_err(|_| {
                    zinder_wallet_projection::WalletProjectionContractError::FactIndexOverflow
                })?;
                if output.output_index != expected_output_index {
                    return Err(
                        zinder_wallet_projection::WalletProjectionContractError::FactIndexMismatch
                            .into(),
                    );
                }
                let outpoint = TransparentOutPoint::new(transaction_id, output.output_index);
                let key = WalletOutpointKey::new(outpoint);
                if self.read_unspent(key)?.is_some() || self.read_spent(key)?.is_some() {
                    return Err(
                        zinder_wallet_projection::WalletProjectionContractError::DuplicateOutput
                            .into(),
                    );
                }
                let unspent = WalletUnspentOutput::new(
                    outpoint,
                    output.address_script_hash,
                    output.value_zat,
                    output.script_pub_key.clone(),
                    transaction_position,
                )?;
                self.insert_unspent(&unspent)?;
                touched_addresses.insert(output.address_script_hash.as_bytes());
                created_outpoints.push(key);
                created_outpoint_keys.insert(key);
            }

            for address_bytes in touched_addresses {
                let address = TransparentAddressScriptHash::from_bytes(address_bytes);
                let key =
                    WalletAddressTransactionKey::new(address, block.height, tx_index_in_block);
                self.insert_address_transaction(WalletAddressTransaction::new(
                    key,
                    transaction_id,
                    block.hash,
                ))?;
                address_transaction_keys.push(key);
            }
        }

        created_outpoints.sort_unstable();
        spent_outpoints.sort_unstable();
        address_transaction_keys.sort_unstable();
        let undo = WalletReorgUndo {
            block,
            parent_hash: facts.block_header.parent_hash,
            source_sequence_digest_before,
            source_sequence_digest_after,
            created_outpoints,
            spent_outpoints,
            address_transaction_keys,
        };
        self.retain_undo(&undo)
    }

    fn retain_undo(&mut self, undo: &WalletReorgUndo) -> Result<(), RocksDbWalletError> {
        if undo.block.height <= self.settled_tip.height {
            return Ok(());
        }
        let key = undo.encode_key();
        let encoded = undo.encode_value()?;
        self.insert_row(
            WalletProjectionRowFamily::ReorgUndo,
            REORG_UNDO_COLUMN_FAMILY,
            &key,
            encoded,
        )?;
        Ok(())
    }

    fn rollback_range(&mut self, reverted: BlockHeightRange) -> Result<(), RocksDbWalletError> {
        let mut height = reverted.end.value();
        loop {
            let block_height = BlockHeight::new(height);
            let (undo, encoded) = self.load_undo(block_height)?.ok_or(
                RocksDbWalletError::WalletProjectionRebuildRequired {
                    reason: "durable undo suffix cannot roll back the requested canonical range",
                },
            )?;
            if undo.block != self.current_tip
                || undo.source_sequence_digest_after != self.source_sequence_digest
            {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "durable wallet undo does not match the current source fence",
                });
            }
            self.rollback_undo(&undo, &encoded)?;
            let parent_height = block_height.value().checked_sub(1).ok_or(
                RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "wallet undo attempts to roll back below height zero",
                },
            )?;
            self.current_tip = BlockId::new(BlockHeight::new(parent_height), undo.parent_hash);
            self.source_sequence_digest = undo.source_sequence_digest_before;
            if height == reverted.start.value() {
                break;
            }
            height =
                height
                    .checked_sub(1)
                    .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
                        reason: "wallet reorg range underflows while walking undo rows",
                    })?;
        }
        Ok(())
    }

    fn advance_settled_tip(
        &mut self,
        resulting_settled_tip: BlockId,
    ) -> Result<(), RocksDbWalletError> {
        if resulting_settled_tip == self.settled_tip {
            return Ok(());
        }
        if resulting_settled_tip.height <= self.settled_tip.height
            || resulting_settled_tip.height > self.current_tip.height
        {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "planned canonical settled tip is not a forward wallet undo floor",
            });
        }

        // Pruned rows leave the suffix a later full check reads, so their
        // digest chain is checked here, up to where the kept suffix starts.
        let pruned_digest_after = if resulting_settled_tip == self.current_tip {
            // The caller's resulting fence already authenticated the exact
            // visible tip, so no successor undo row exists to bind the floor.
            self.source_sequence_digest
        } else {
            let successor_height = resulting_settled_tip.height.next().ok_or(
                RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "canonical settled tip cannot have a successor beyond the height domain",
                },
            )?;
            let successor = self.inspect_undo(successor_height)?.ok_or(
                RocksDbWalletError::WalletProjectionRebuildRequired {
                    reason: "durable undo suffix cannot authenticate the advanced canonical settled tip",
                },
            )?;
            if successor.block.height != successor_height
                || successor.parent_hash != resulting_settled_tip.hash
            {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "canonical settled tip does not match the retained successor undo record",
                });
            }
            successor.source_sequence_digest_before
        };

        let mut height = self.settled_tip.height.next().ok_or(
            RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet undo floor cannot advance beyond the height domain",
            },
        )?;
        let mut previous_digest_after = None;
        loop {
            let (undo, encoded) = self.load_undo(height)?.ok_or(
                RocksDbWalletError::WalletProjectionRebuildRequired {
                    reason: "durable undo suffix cannot prune the advanced canonical settled range",
                },
            )?;
            if undo.block.height != height {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "durable wallet undo floor has a height/key mismatch",
                });
            }
            let is_last_pruned = height == resulting_settled_tip.height;
            if previous_digest_after
                .is_some_and(|digest| digest != undo.source_sequence_digest_before)
                || (is_last_pruned && undo.source_sequence_digest_after != pruned_digest_after)
            {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "durable wallet undo suffix has disconnected source digests",
                });
            }
            previous_digest_after = Some(undo.source_sequence_digest_after);
            self.remove_row_after_value_observed(
                WalletProjectionRowFamily::ReorgUndo,
                REORG_UNDO_COLUMN_FAMILY,
                &height.value().to_be_bytes(),
                &encoded,
            )?;
            if is_last_pruned {
                break;
            }
            height = height
                .next()
                .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "wallet undo floor overflows while pruning settled rows",
                })?;
        }
        self.settled_tip = resulting_settled_tip;
        Ok(())
    }

    fn require_publishable_undo_suffix(
        &mut self,
        check: UndoSuffixCheck,
    ) -> Result<(), RocksDbWalletError> {
        let undo_count = self.accumulator_row_counts.reorg_undo_count;
        let expected_count = u64::from(self.current_tip.height.value())
            .checked_sub(u64::from(self.settled_tip.height.value()))
            .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet settled tip lies beyond the planned source tip",
            })?;
        if expected_count > u64::from(self.supported_reorg_depth) {
            return Err(RocksDbWalletError::WalletProjectionRebuildRequired {
                reason: "canonical unsettled suffix exceeds the wallet's configured undo capacity",
            });
        }
        if undo_count != expected_count {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "planned wallet undo rows do not exactly cover the canonical unsettled suffix",
            });
        }
        if expected_count == 0 || check == UndoSuffixCheck::Incremental {
            return Ok(());
        }
        let first_height = self.settled_tip.height.next().ok_or(
            RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet undo suffix starts beyond the height domain",
            },
        )?;
        let mut previous_digest_after = None;
        let mut block_height = first_height;
        for offset in 0..expected_count {
            let Some(undo) = self.inspect_undo(block_height)? else {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "planned wallet undo suffix has a missing durable record",
                });
            };
            if undo.block.height != block_height {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "durable wallet undo suffix has a height/key mismatch",
                });
            }
            if previous_digest_after
                .is_some_and(|digest| digest != undo.source_sequence_digest_before)
            {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "durable wallet undo suffix has disconnected source digests",
                });
            }
            previous_digest_after = Some(undo.source_sequence_digest_after);
            if offset + 1 == expected_count
                && (undo.block != self.current_tip
                    || undo.source_sequence_digest_after != self.source_sequence_digest)
            {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "durable wallet tip undo does not match the planned source fence",
                });
            }
            if offset + 1 != expected_count {
                block_height = block_height.next().ok_or(
                    RocksDbWalletError::ProjectionTransitionRejected {
                        reason: "wallet undo suffix height overflows while checking publication",
                    },
                )?;
            }
        }
        Ok(())
    }

    fn rollback_undo(
        &mut self,
        undo: &WalletReorgUndo,
        encoded_undo: &[u8],
    ) -> Result<(), RocksDbWalletError> {
        for key in &undo.address_transaction_keys {
            self.remove_address_transaction(*key, undo.block)?;
        }
        for key in &undo.created_outpoints {
            self.remove_created_outpoint(*key, undo.block)?;
        }
        for key in &undo.spent_outpoints {
            let spent = self.remove_spent(*key)?;
            if spent.spent_at.block != undo.block {
                return Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "wallet undo restores a spend from a different block",
                });
            }
            self.insert_unspent(&spent.output)?;
        }
        self.remove_row_after_value_observed(
            WalletProjectionRowFamily::ReorgUndo,
            REORG_UNDO_COLUMN_FAMILY,
            &undo.encode_key(),
            encoded_undo,
        )
    }

    fn remove_created_outpoint(
        &mut self,
        key: WalletOutpointKey,
        block: BlockId,
    ) -> Result<(), RocksDbWalletError> {
        let unspent = self.read_unspent(key)?;
        let spent = self.read_spent(key)?;
        match (unspent, spent) {
            (Some(output), None) if output.created_at.block == block => {
                self.remove_unspent(&output)
            }
            (None, Some(output))
                if output.output.created_at.block == block && output.spent_at.block == block =>
            {
                self.remove_spent(key).map(|_| ())
            }
            (Some(_), Some(_)) => Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet undo finds one created outpoint in both output families",
            }),
            (Some(_), None) | (None, Some(_)) => {
                Err(RocksDbWalletError::ProjectionTransitionRejected {
                    reason: "wallet undo created outpoint does not belong to its block",
                })
            }
            (None, None) => Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet undo created outpoint is absent from durable rows",
            }),
        }
    }

    fn read_unspent(
        &mut self,
        key: WalletOutpointKey,
    ) -> Result<Option<WalletUnspentOutput>, RocksDbWalletError> {
        self.raw(TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY, key.as_bytes())?
            .map(|encoded| WalletUnspentOutput::decode_value(key, &encoded).map_err(Into::into))
            .transpose()
    }

    fn read_spent(
        &mut self,
        key: WalletOutpointKey,
    ) -> Result<Option<WalletSpentOutput>, RocksDbWalletError> {
        self.raw(TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY, key.as_bytes())?
            .map(|encoded| WalletSpentOutput::decode_value(key, &encoded).map_err(Into::into))
            .transpose()
    }

    fn insert_unspent(&mut self, output: &WalletUnspentOutput) -> Result<(), RocksDbWalletError> {
        let key = WalletOutpointKey::new(output.outpoint);
        if self.read_unspent(key)?.is_some() || self.read_spent(key)?.is_some() {
            return Err(
                zinder_wallet_projection::WalletProjectionContractError::DuplicateOutput.into(),
            );
        }
        self.validate_add_unspent(output)?;
        let encoded = output.encode_value()?;
        self.insert_row_after_absence_observed(
            WalletProjectionRowFamily::TransparentUnspentOutput,
            TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
            key.as_bytes(),
            encoded,
        )?;
        let address_key = WalletAddressUnspentOutputKey::new(output);
        self.insert_row(
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
            TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
            address_key.as_bytes(),
            Vec::new(),
        )?;
        self.add_balance(output.address_script_hash, output.value_zat)?;
        self.commit_add_unspent(output);
        Ok(())
    }

    fn remove_unspent(&mut self, output: &WalletUnspentOutput) -> Result<(), RocksDbWalletError> {
        let key = WalletOutpointKey::new(output.outpoint);
        let encoded = output.encode_value()?;
        let observed = self
            .raw(TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY, key.as_bytes())?
            .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet unspent output is absent while applying a spend",
            })?;
        if observed != encoded {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet unspent output bytes differ from its decoded row",
            });
        }
        let address_key = WalletAddressUnspentOutputKey::new(output);
        let address_index = self
            .raw(
                TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
                address_key.as_bytes(),
            )?
            .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet unspent output has no address index row",
            })?;
        if !address_index.is_empty() {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet address index row must have an empty value",
            });
        }
        self.validate_remove_unspent(output)?;
        self.remove_row_after_value_observed(
            WalletProjectionRowFamily::TransparentUnspentOutput,
            TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
            key.as_bytes(),
            &observed,
        )?;
        self.remove_row_after_value_observed(
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
            TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
            address_key.as_bytes(),
            &address_index,
        )?;
        self.subtract_balance(output.address_script_hash, output.value_zat)?;
        self.commit_remove_unspent(output);
        Ok(())
    }

    fn insert_spent(&mut self, spent: &WalletSpentOutput) -> Result<(), RocksDbWalletError> {
        let key = WalletOutpointKey::new(spent.output.outpoint);
        if self.read_spent(key)?.is_some() {
            return Err(
                zinder_wallet_projection::WalletProjectionContractError::DuplicateSpend.into(),
            );
        }
        let encoded = spent.encode_value()?;
        self.insert_row_after_absence_observed(
            WalletProjectionRowFamily::TransparentSpentOutput,
            TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
            key.as_bytes(),
            encoded,
        )
    }

    fn remove_spent(
        &mut self,
        key: WalletOutpointKey,
    ) -> Result<WalletSpentOutput, RocksDbWalletError> {
        let encoded = self
            .raw(TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY, key.as_bytes())?
            .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet spent output is absent while reversing a reorg",
            })?;
        let spent = WalletSpentOutput::decode_value(key, &encoded)?;
        self.remove_row_after_value_observed(
            WalletProjectionRowFamily::TransparentSpentOutput,
            TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
            key.as_bytes(),
            &encoded,
        )?;
        Ok(spent)
    }

    fn insert_address_transaction(
        &mut self,
        transaction: WalletAddressTransaction,
    ) -> Result<(), RocksDbWalletError> {
        let encoded = transaction.encode_value();
        self.insert_row(
            WalletProjectionRowFamily::TransparentAddressTransaction,
            TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
            transaction.key.as_bytes(),
            encoded.to_vec(),
        )
    }

    fn remove_address_transaction(
        &mut self,
        key: WalletAddressTransactionKey,
        block: BlockId,
    ) -> Result<(), RocksDbWalletError> {
        let encoded = self
            .raw(
                TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
                key.as_bytes(),
            )?
            .ok_or(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet undo address transaction is absent",
            })?;
        let transaction = WalletAddressTransaction::decode_value(key, &encoded)?;
        if key.block_height() != block.height || transaction.block_hash != block.hash {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet undo address transaction belongs to a different block",
            });
        }
        self.remove_row_after_value_observed(
            WalletProjectionRowFamily::TransparentAddressTransaction,
            TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
            key.as_bytes(),
            &encoded,
        )
    }

    /// Reads one retained undo row with the bytes a plan needs to remove it.
    fn load_undo(
        &mut self,
        height: BlockHeight,
    ) -> Result<Option<(WalletReorgUndo, Vec<u8>)>, RocksDbWalletError> {
        let key = height.value().to_be_bytes();
        self.raw(REORG_UNDO_COLUMN_FAMILY, &key)?
            .map(|encoded| decode_undo(&key, &encoded).map(|undo| (undo, encoded)))
            .transpose()
    }

    /// Decodes one retained undo row in place, from the plan's overlay or a
    /// pinned durable value, for a plan that keeps the row.
    fn inspect_undo(
        &mut self,
        height: BlockHeight,
    ) -> Result<Option<WalletReorgUndo>, RocksDbWalletError> {
        let key = height.value().to_be_bytes();
        if let Some(planned) = self.overlay.get(&(REORG_UNDO_COLUMN_FAMILY, key.to_vec())) {
            return planned
                .as_deref()
                .map(|encoded| decode_undo(&key, encoded))
                .transpose();
        }
        let column_family = column_family(&self.store.bounded_open, REORG_UNDO_COLUMN_FAMILY)?;
        count_undo_row_work(REORG_UNDO_COLUMN_FAMILY, UndoRowWork::DurableRead);
        self.durable_point_reads = self.durable_point_reads.saturating_add(1);
        self.store
            .bounded_open
            .db
            .get_pinned_cf(&column_family, key)
            .map_err(|source| RocksDbWalletError::rocksdb("wallet transition point read", source))?
            .map(|encoded| decode_undo(&key, &encoded))
            .transpose()
    }

    fn validate_add_unspent(
        &mut self,
        output: &WalletUnspentOutput,
    ) -> Result<(), RocksDbWalletError> {
        self.utxo_summary
            .utxo_count
            .checked_add(1)
            .ok_or(zinder_wallet_projection::WalletProjectionContractError::UtxoCountOverflow)?;
        self.utxo_summary
            .total_value_zat
            .checked_add(output.value_zat)
            .ok_or(zinder_wallet_projection::WalletProjectionContractError::UtxoValueOverflow)?;
        if output.value_zat > 0 {
            self.current_balance(output.address_script_hash)?
                .checked_add(output.value_zat)
                .ok_or(
                    zinder_wallet_projection::WalletProjectionContractError::AddressBalanceOverflow,
                )?;
        }
        Ok(())
    }

    fn validate_remove_unspent(
        &mut self,
        output: &WalletUnspentOutput,
    ) -> Result<(), RocksDbWalletError> {
        self.utxo_summary
            .utxo_count
            .checked_sub(1)
            .ok_or(zinder_wallet_projection::WalletProjectionContractError::UtxoCountUnderflow)?;
        self.utxo_summary
            .total_value_zat
            .checked_sub(output.value_zat)
            .ok_or(zinder_wallet_projection::WalletProjectionContractError::UtxoValueUnderflow)?;
        if output.value_zat > 0 {
            self.current_balance(output.address_script_hash)?
                .checked_sub(output.value_zat)
                .ok_or(zinder_wallet_projection::WalletProjectionContractError::AddressBalanceUnderflow)?;
        }
        Ok(())
    }

    fn commit_add_unspent(&mut self, output: &WalletUnspentOutput) {
        self.utxo_summary.utxo_count += 1;
        self.utxo_summary.total_value_zat += output.value_zat;
        self.utxo_summary
            .commitment
            .insert(&UtxoSetCommitmentElement {
                network_id: self.network.id(),
                outpoint: output.outpoint,
                value_zat: output.value_zat,
                script_pub_key: &output.script_pub_key,
                block_height: output.created_at.block.height,
            });
    }

    fn commit_remove_unspent(&mut self, output: &WalletUnspentOutput) {
        self.utxo_summary.utxo_count -= 1;
        self.utxo_summary.total_value_zat -= output.value_zat;
        self.utxo_summary
            .commitment
            .subtract(&UtxoSetCommitmentElement {
                network_id: self.network.id(),
                outpoint: output.outpoint,
                value_zat: output.value_zat,
                script_pub_key: &output.script_pub_key,
                block_height: output.created_at.block.height,
            });
    }

    fn add_balance(
        &mut self,
        address: TransparentAddressScriptHash,
        value_zat: u64,
    ) -> Result<(), RocksDbWalletError> {
        if value_zat == 0 {
            return Ok(());
        }
        let current = self.current_balance(address)?;
        let next = current.checked_add(value_zat).ok_or(
            zinder_wallet_projection::WalletProjectionContractError::AddressBalanceOverflow,
        )?;
        self.replace_balance(address, current, next)
    }

    fn subtract_balance(
        &mut self,
        address: TransparentAddressScriptHash,
        value_zat: u64,
    ) -> Result<(), RocksDbWalletError> {
        if value_zat == 0 {
            return Ok(());
        }
        let current = self.current_balance(address)?;
        let next = current.checked_sub(value_zat).ok_or(
            zinder_wallet_projection::WalletProjectionContractError::AddressBalanceUnderflow,
        )?;
        self.replace_balance(address, current, next)
    }

    fn current_balance(
        &mut self,
        address: TransparentAddressScriptHash,
    ) -> Result<u64, RocksDbWalletError> {
        let key = address.as_bytes();
        let Some(encoded) = self.raw(TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY, &key)? else {
            return Ok(0);
        };
        let balance = WalletAddressBalance::decode(&key, &encoded)?;
        if balance.address_script_hash != address || balance.balance_zat == 0 {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet address balance row is malformed or non-canonical",
            });
        }
        Ok(balance.balance_zat)
    }

    fn replace_balance(
        &mut self,
        address: TransparentAddressScriptHash,
        current: u64,
        next: u64,
    ) -> Result<(), RocksDbWalletError> {
        let key = address.as_bytes();
        if current > 0 {
            let previous = WalletAddressBalance {
                address_script_hash: address,
                balance_zat: current,
            };
            self.remove_row(
                WalletProjectionRowFamily::TransparentAddressBalance,
                TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
                &key,
                &previous.encode_value(),
            )?;
        }
        if next > 0 {
            let replacement = WalletAddressBalance {
                address_script_hash: address,
                balance_zat: next,
            };
            self.insert_row(
                WalletProjectionRowFamily::TransparentAddressBalance,
                TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
                &key,
                replacement.encode_value().to_vec(),
            )?;
        }
        Ok(())
    }

    fn insert_row(
        &mut self,
        family: WalletProjectionRowFamily,
        column_family_name: &'static str,
        key: &[u8],
        encoded_value: Vec<u8>,
    ) -> Result<(), RocksDbWalletError> {
        if self.raw(column_family_name, key)?.is_some() {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet transition attempts to overwrite an existing logical row",
            });
        }
        self.insert_row_after_absence_observed(family, column_family_name, key, encoded_value)
    }

    fn insert_row_after_absence_observed(
        &mut self,
        family: WalletProjectionRowFamily,
        column_family_name: &'static str,
        key: &[u8],
        encoded_value: Vec<u8>,
    ) -> Result<(), RocksDbWalletError> {
        self.accumulator.append_row(family, key, &encoded_value)?;
        self.accumulator_row_counts = self.accumulator.row_counts();
        self.put_raw(column_family_name, key, encoded_value)
    }

    fn remove_row(
        &mut self,
        family: WalletProjectionRowFamily,
        column_family_name: &'static str,
        key: &[u8],
        expected_value: &[u8],
    ) -> Result<(), RocksDbWalletError> {
        let observed = self.raw(column_family_name, key)?.ok_or(
            RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet transition attempts to remove an absent logical row",
            },
        )?;
        if observed != expected_value {
            return Err(RocksDbWalletError::ProjectionTransitionRejected {
                reason: "wallet transition logical row differs from its expected durable bytes",
            });
        }
        self.remove_row_after_value_observed(family, column_family_name, key, expected_value)
    }

    fn remove_row_after_value_observed(
        &mut self,
        family: WalletProjectionRowFamily,
        column_family_name: &'static str,
        key: &[u8],
        observed_value: &[u8],
    ) -> Result<(), RocksDbWalletError> {
        self.accumulator.remove_row(family, key, observed_value)?;
        self.accumulator_row_counts = self.accumulator.row_counts();
        self.delete_raw(column_family_name, key)
    }

    fn raw(
        &mut self,
        column_family_name: &'static str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, RocksDbWalletError> {
        let overlay_key = (column_family_name, key.to_vec());
        let encoded = if let Some(overlay_value) = self.overlay.get(&overlay_key) {
            overlay_value.clone()
        } else if let Some(observed) = self.durable_reads.get(&overlay_key) {
            observed.clone()
        } else {
            let column_family = column_family(&self.store.bounded_open, column_family_name)?;
            count_undo_row_work(column_family_name, UndoRowWork::DurableRead);
            self.durable_point_reads = self.durable_point_reads.saturating_add(1);
            let encoded = self
                .store
                .bounded_open
                .db
                .get_cf(&column_family, key)
                .map_err(|source| {
                    RocksDbWalletError::rocksdb("wallet transition point read", source)
                })?;
            // The plan writes or deletes every undo row it reads, so
            // remembering one would only copy it.
            if column_family_name != REORG_UNDO_COLUMN_FAMILY {
                self.durable_reads.remember(
                    overlay_key,
                    encoded.as_deref(),
                    durable_read_memo_room(self.logical_bytes.unused_bytes()),
                )?;
            }
            encoded
        };
        if encoded.is_some() {
            count_undo_row_work(column_family_name, UndoRowWork::ValueCopy);
        }
        Ok(encoded)
    }

    fn put_raw(
        &mut self,
        column_family_name: &'static str,
        key: &[u8],
        encoded_value: Vec<u8>,
    ) -> Result<(), RocksDbWalletError> {
        let overlay_key = (column_family_name, key.to_vec());
        self.logical_bytes
            .reserve_put(key, &encoded_value, self.overlay.get(&overlay_key))?;
        self.durable_reads
            .release_for_write(&overlay_key, self.logical_bytes.unused_bytes())?;
        self.overlay.insert(overlay_key, Some(encoded_value));
        Ok(())
    }

    fn delete_raw(
        &mut self,
        column_family_name: &'static str,
        key: &[u8],
    ) -> Result<(), RocksDbWalletError> {
        let overlay_key = (column_family_name, key.to_vec());
        self.logical_bytes
            .reserve_delete(key, self.overlay.get(&overlay_key))?;
        self.durable_reads
            .release_for_write(&overlay_key, self.logical_bytes.unused_bytes())?;
        self.overlay.insert(overlay_key, None);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        num::{NonZeroU32, NonZeroU64},
        path::Path,
    };

    #[cfg(unix)]
    use std::num::NonZeroU16;

    use rust_rocksdb::{
        Cache, ColumnFamilyDescriptor, DBCompressionType, FlushOptions, IteratorMode, Options,
        statistics::Ticker,
    };
    use tempfile::TempDir;
    use zinder_core::{
        BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId,
        CanonicalBlockFacts, CanonicalBlockFactsDigest, CanonicalBlockFactsDigestVersion,
        CanonicalBlockFactsSequenceDigest, CanonicalBlockFactsSequenceDigestVersion,
        CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts, ChainTipMetadata,
        CommitmentTreeAccumulator, CommitmentTreeCheckpoint, CommitmentTreeFrontiers,
        CompactBlockArtifact, CompactChainMetadata, ConsensusBranchId, LockTime, Network,
        NetworkUpgradeActivation, NetworkUpgradeActivations, PrivacyShape, SerializedBytesDigest,
        TransactionBlobArtifact, TransactionComponentCounts, TransactionId,
        TransactionIntrinsicValueBalances, TransactionLocation, TransactionPublicFacts,
        TransactionVersion, TransparentAddressScriptHash, TransparentInputFact,
        TransparentOutPoint, TransparentOutputFact, TransparentUtxoSetCommitment,
        UnixTimestampMillis, UnsupportedSection, encode_canonical_block_replay,
    };
    use zinder_store::{
        CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalEventFence,
        CanonicalEventHistoryRequest, CanonicalLiveAppend, CanonicalLiveReplacement,
        CanonicalReorgPolicy, CanonicalReplacementBlock, CanonicalRetainedEvent,
        CanonicalStoreBuildPlan, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder,
        RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbOpenRole, RocksDbResourceBudget,
        build_block_based_table_factory, open_bounded_rocksdb,
    };
    use zinder_testkit::process_crash;
    use zinder_wallet_projection::{
        WALLET_STORE_CONTROL_KEY, WalletAddressUnspentOutputKey, WalletCanonicalSourceIdentity,
        WalletOutpointKey, WalletProjectionBuildLeaseRequest, WalletProjectionBuildOwner,
        WalletProjectionBuildState, WalletProjectionContractError, WalletProjectionDigestBuilder,
        WalletProjectionReadyEvidence, WalletProjectionRetainedEventAnchor,
        WalletProjectionSourcePosition, WalletReorgUndo, WalletSpentOutput,
        WalletStoreControlRecord, WalletUtxoSetSummary,
    };

    use super::{
        DurableReadMemo, MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES, OverlayKey,
        TransitionLogicalByteAccounting, UndoRowWorkCounts, WalletTransitionPlanner,
        coalesced_write_batch, coalesced_write_batch_capacity, extend_source_sequence_digest,
        source_identity_from_fence, take_undo_row_work,
    };
    use crate::{
        RocksDbWalletBuildOptions, RocksDbWalletBuildStore, RocksDbWalletError,
        RocksDbWalletFollowingStore, RocksDbWalletStore, build_wallet_from_canonical,
        store::{
            REORG_UNDO_COLUMN_FAMILY, RocksDbWalletBuilder,
            TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
            TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY, TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
            TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
            TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY, WALLET_DATA_COLUMN_FAMILIES,
            WalletColdValidationConfig, column_family, decode_only_control,
        },
    };

    #[test]
    fn checkpointed_source_digest_advances_without_a_height_derived_count()
    -> Result<(), crate::RocksDbWalletError> {
        // A canonical checkpoint at height 100 can retain a sequence prefix
        // containing only its post-checkpoint rows. The sequence count is
        // therefore deliberately unrelated to the block height.
        let checkpointed_height = BlockHeight::new(100);
        let source = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            1,
            [0x7a; 32],
        );
        assert_ne!(source.block_count(), u64::from(checkpointed_height.value()));

        let after = extend_source_sequence_digest(
            source,
            CanonicalBlockFactsDigest::from_reference_encoding(
                CanonicalBlockFactsDigestVersion::V1,
                b"checkpointed height-101 replay",
            ),
        )?;

        assert_eq!(after.block_count(), 2);
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the checkpointed follower fixture keeps canonical construction, following, and retained-history assertions together"
    )]
    fn checkpointed_history_follower_reconciles_then_rejects_a_pre_floor_spend()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let PublishedCheckpointedWallet {
            activations,
            canonical_store,
            initial_fence,
        } = publish_checkpointed_wallet(temporary.path())?;
        let canonical_path = temporary.path().join("canonical");
        let wallet_path = temporary.path().join("wallet");
        let initial_source = source_identity_from_fence(initial_fence);
        let mut append_accumulator = checkpoint_accumulator(&activations, initial_fence)?;
        let append_block = checkpointed_append_block(
            &mut append_accumulator,
            101,
            [0xa0; 32],
            [0xa1; 32],
            Vec::new(),
        )?;

        let (canonical_store, append_fence) = canonical_store.commit_live_append(
            CanonicalLiveAppend::new(
                initial_fence,
                append_block,
                Vec::new(),
                initial_fence.visible_tip(),
                UnixTimestampMillis::new(1_750_000_100_001),
            ),
            &activations,
        )?;
        let mut secondary = RocksDbCanonicalSecondary::open_ready(
            &canonical_path,
            temporary.path().join("canonical-secondary"),
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let source_cursor = initial_source.source_position().event_cursor.as_bytes();
        let retained_events = secondary.canonical_event_history(
            CanonicalEventHistoryRequest::new(Some(&source_cursor), NonZeroU32::MIN),
        )?;
        assert_eq!(retained_events.len(), 1);
        assert_eq!(retained_events[0].resulting_fence(), append_fence);
        let replay_range =
            BlockHeightRange::inclusive(BlockHeight::new(101), BlockHeight::new(101));
        let mut following = RocksDbWalletStore::open_ready_for_following(
            &wallet_path,
            Network::ZcashRegtest,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        following.reconcile_canonical_event_sequence(
            initial_source,
            &retained_events,
            append_fence,
            initial_fence.visible_tip(),
            None,
            replay_range,
            test_transition_logical_byte_limit(),
            secondary.scan_canonical_replay_range(replay_range)?,
        )?;

        assert_eq!(
            following.ready_evidence().source_position.tip,
            append_fence.visible_tip()
        );
        assert_eq!(
            following
                .ready_evidence()
                .source_sequence_digest
                .block_count(),
            2
        );

        let precheckpoint_outpoint =
            TransparentOutPoint::new(TransactionId::from_bytes([0x55; 32]), 0);
        let spending_transaction_id = TransactionId::from_bytes([0x66; 32]);
        let public_facts = transparent_spend_public_facts(spending_transaction_id);
        let spending_transaction = CanonicalTransactionFacts {
            public_facts,
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[0x66]),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs: vec![TransparentInputFact::new(0, precheckpoint_outpoint)],
            transparent_outputs: Vec::new(),
        };
        let missing_predecessor_block = checkpointed_append_block(
            &mut append_accumulator,
            102,
            [0xa1; 32],
            [0xa2; 32],
            vec![spending_transaction],
        )?;
        let (_canonical_store, missing_predecessor_fence) = canonical_store.commit_live_append(
            CanonicalLiveAppend::new(
                append_fence,
                missing_predecessor_block,
                Vec::new(),
                append_fence.visible_tip(),
                UnixTimestampMillis::new(1_750_000_100_002),
            ),
            &activations,
        )?;
        secondary.try_catch_up()?;
        let following_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(following.ready_evidence());
        let following_cursor = following_source.source_position().event_cursor.as_bytes();
        let retained_events = secondary.canonical_event_history(
            CanonicalEventHistoryRequest::new(Some(&following_cursor), NonZeroU32::MIN),
        )?;
        let replay_range =
            BlockHeightRange::inclusive(BlockHeight::new(102), BlockHeight::new(102));
        let outcome = following.reconcile_canonical_event_sequence(
            following_source,
            &retained_events,
            missing_predecessor_fence,
            append_fence.visible_tip(),
            None,
            replay_range,
            test_transition_logical_byte_limit(),
            secondary.scan_canonical_replay_range(replay_range)?,
        );

        assert!(
            matches!(
                &outcome,
                Err(crate::RocksDbWalletError::Contract(
                    WalletProjectionContractError::MissingTransparentPredecessor
                ))
            ),
            "{outcome:?}"
        );
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one proof keeps the held settlement, the refused transition, the raise, and the restart together"
    )]
    fn raised_depth_retains_an_unsettled_suffix_deeper_than_the_original_depth()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let PublishedCheckpointedWallet {
            activations,
            canonical_store,
            initial_fence,
        } = publish_checkpointed_wallet(temporary.path())?;
        let canonical_path = temporary.path().join("canonical");
        let wallet_path = temporary.path().join("wallet");
        let initial_source = source_identity_from_fence(initial_fence);
        let checkpoint_block = initial_fence.visible_tip();
        let mut accumulator = checkpoint_accumulator(&activations, initial_fence)?;
        // The canonical settled tip holds at the checkpoint while two blocks
        // arrive, as it does after a canonical window raise.
        let mut canonical_store = canonical_store;
        let mut fence = initial_fence;
        for (height, parent_hash, block_hash) in
            [(101, [0xa0; 32], [0xa1; 32]), (102, [0xa1; 32], [0xa2; 32])]
        {
            let block = checkpointed_append_block(
                &mut accumulator,
                height,
                parent_hash,
                block_hash,
                Vec::new(),
            )?;
            (canonical_store, fence) = canonical_store.commit_live_append(
                CanonicalLiveAppend::new(
                    fence,
                    block,
                    Vec::new(),
                    checkpoint_block,
                    UnixTimestampMillis::new(1_750_000_100_000 + u64::from(height)),
                ),
                &activations,
            )?;
        }
        drop(canonical_store);
        let secondary = RocksDbCanonicalSecondary::open_ready(
            &canonical_path,
            temporary.path().join("canonical-secondary"),
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let source_cursor = initial_source.source_position().event_cursor.as_bytes();
        let events = secondary.canonical_event_history(CanonicalEventHistoryRequest::new(
            Some(&source_cursor),
            NonZeroU32::new(2).ok_or("event page limit must be nonzero")?,
        ))?;
        let [first, second] = events.as_slice() else {
            return Err("expected two retained append events".into());
        };
        let mut following = RocksDbWalletStore::open_ready_for_following(
            &wallet_path,
            Network::ZcashRegtest,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(following.supported_reorg_depth(), 1);
        let replay = |height| {
            let range =
                BlockHeightRange::inclusive(BlockHeight::new(height), BlockHeight::new(height));
            secondary.scan_canonical_replay_range(range)
        };
        following.apply_canonical_event_range(
            initial_source,
            *first,
            first.resulting_fence(),
            checkpoint_block,
            test_transition_logical_byte_limit(),
            replay(101)?,
        )?;
        let after_first =
            WalletCanonicalSourceIdentity::from_ready_evidence(following.ready_evidence());
        let refused = following.apply_canonical_event_range(
            after_first,
            *second,
            second.resulting_fence(),
            checkpoint_block,
            test_transition_logical_byte_limit(),
            replay(102)?,
        );
        assert!(
            matches!(
                refused,
                Err(crate::RocksDbWalletError::WalletProjectionRebuildRequired { .. })
            ),
            "{refused:?}"
        );

        following.raise_supported_reorg_depth(2)?;
        following.apply_canonical_event_range(
            after_first,
            *second,
            second.resulting_fence(),
            checkpoint_block,
            test_transition_logical_byte_limit(),
            replay(102)?,
        )?;
        assert_eq!(following.ready_evidence().row_counts.reorg_undo_count, 2);
        assert_eq!(following.ready_evidence().settled_tip, checkpoint_block);
        drop(following);

        let mut reopened = RocksDbWalletStore::open_ready_for_following(
            &wallet_path,
            Network::ZcashRegtest,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(reopened.supported_reorg_depth(), 2);
        let lowered = reopened.raise_supported_reorg_depth(1);
        assert!(
            matches!(
                lowered,
                Err(crate::RocksDbWalletError::WalletProjectionRebuildRequired { .. })
            ),
            "{lowered:?}"
        );
        assert_eq!(reopened.supported_reorg_depth(), 2);
        Ok(())
    }

    #[cfg(unix)]
    const TRANSITION_CRASH_CHILD: &str = "transition::tests::wallet_transition_crash_child";

    #[test]
    fn wallet_transition_crash_child() -> Result<(), Box<dyn std::error::Error>> {
        let Some(child) = process_crash::crash_child() else {
            return Ok(());
        };
        let secondary = open_checkpointed_secondary(child.store_path(), "canonical-secondary")?;
        let mut following = RocksDbWalletStore::open_ready_for_following(
            child.store_path().join("wallet"),
            Network::ZcashRegtest,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        apply_next_retained_event(&secondary, &mut following)?;
        Err("wallet transition returned without reaching its crash point".into())
    }

    #[cfg(unix)]
    const CRASH_OUTPUT_VALUE_ZAT: u64 = 7;

    #[cfg(unix)]
    #[test]
    fn wallet_transition_reopens_at_one_complete_publication_across_process_abort()
    -> Result<(), Box<dyn std::error::Error>> {
        let address = TransparentAddressScriptHash::from_bytes([0xc7; 32]);
        let crash_transaction_id = TransactionId::from_bytes([0xc8; 32]);
        let created_outpoint = TransparentOutPoint::new(crash_transaction_id, 0);
        for crash_point in ["before_atomic_write", "after_atomic_write"] {
            let temporary = TempDir::new()?;
            let published = publish_checkpointed_wallet(temporary.path())?;
            let initial_fence = published.initial_fence;
            let mut accumulator = checkpoint_accumulator(&published.activations, initial_fence)?;
            let append_block = checkpointed_append_block(
                &mut accumulator,
                101,
                [0xa0; 32],
                [0xa1; 32],
                vec![coinbase_output_transaction(
                    crash_transaction_id,
                    vec![TransparentOutputFact::new(
                        0,
                        CRASH_OUTPUT_VALUE_ZAT,
                        [0xc9],
                        address,
                    )],
                )],
            )?;
            let (canonical_store, append_fence) = published.canonical_store.commit_live_append(
                CanonicalLiveAppend::new(
                    initial_fence,
                    append_block,
                    Vec::new(),
                    initial_fence.visible_tip(),
                    UnixTimestampMillis::new(1_750_000_100_001),
                ),
                &published.activations,
            )?;
            drop(canonical_store);

            process_crash::run_crash_child(
                TRANSITION_CRASH_CHILD,
                temporary.path(),
                &format!("wallet_projection.transition.{crash_point}"),
            )?;

            let committed = crash_point == "after_atomic_write";
            let expected_fence = if committed {
                append_fence
            } else {
                initial_fence
            };
            assert_crash_transition_rows(
                temporary.path(),
                expected_fence,
                address,
                created_outpoint,
                committed,
            )?;
            if !committed {
                let mut reopened = RocksDbWalletStore::open_ready_for_following(
                    temporary.path().join("wallet"),
                    Network::ZcashRegtest,
                    RocksDbResourceBudget::for_local_tests(),
                )?;
                let secondary = open_checkpointed_secondary(temporary.path(), "resume-secondary")?;
                apply_next_retained_event(&secondary, &mut reopened)?;
                drop(reopened);
                assert_crash_transition_rows(
                    temporary.path(),
                    append_fence,
                    address,
                    created_outpoint,
                    true,
                )?;
            }
        }
        Ok(())
    }

    /// Reopens the wallet at `expected_fence` and asserts that every row family
    /// the crash transition writes holds its rows exactly when `committed`.
    #[cfg(unix)]
    fn assert_crash_transition_rows(
        root: &Path,
        expected_fence: CanonicalEventFence,
        address: TransparentAddressScriptHash,
        created_outpoint: TransparentOutPoint,
        committed: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let following = RocksDbWalletStore::open_ready_for_following(
            root.join("wallet"),
            Network::ZcashRegtest,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let evidence = following.ready_evidence();
        assert_eq!(evidence.source_position.tip, expected_fence.visible_tip());
        assert_eq!(
            evidence.source_sequence_digest,
            expected_fence.sequence_digest()
        );
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(evidence);
        let wallet = following.into_ready_store(source)?;

        let unspent = wallet.find_unspent_output(created_outpoint)?;
        assert_eq!(unspent.is_some(), committed);
        if let Some(output) = &unspent {
            assert_eq!(output.address_script_hash, address);
            assert_eq!(output.value_zat, CRASH_OUTPUT_VALUE_ZAT);
            assert_eq!(output.created_at.block.height, BlockHeight::new(101));
        }
        let address_unspent =
            wallet.address_unspent_outputs_page(address, None, NonZeroU16::MIN)?;
        assert_eq!(
            address_unspent.outputs,
            unspent.into_iter().collect::<Vec<_>>()
        );
        let history = wallet.address_transaction_history_page(address, None, NonZeroU16::MIN)?;
        assert_eq!(history.transactions.len(), usize::from(committed));
        assert_eq!(
            wallet.address_balance(address)?,
            if committed { CRASH_OUTPUT_VALUE_ZAT } else { 0 }
        );
        assert_eq!(wallet.utxo_summary().utxo_count, u64::from(committed));

        let undo = wallet.find_reorg_undo(BlockHeight::new(101))?;
        assert_eq!(undo.is_some(), committed);
        if let Some(undo) = undo {
            assert_eq!(
                undo.created_outpoints,
                vec![WalletOutpointKey::new(created_outpoint)]
            );
            assert_eq!(
                undo.spent_outpoints,
                [] as [zinder_wallet_projection::WalletOutpointKey; 0]
            );
            assert_eq!(undo.address_transaction_keys.len(), 1);
            assert_eq!(
                undo.source_sequence_digest_after,
                expected_fence.sequence_digest()
            );
        }
        Ok(())
    }

    fn apply_next_retained_event(
        secondary: &RocksDbCanonicalSecondary,
        following: &mut RocksDbWalletFollowingStore,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(following.ready_evidence());
        let cursor = source.source_position().event_cursor.as_bytes();
        let retained_events = secondary.canonical_event_history(
            CanonicalEventHistoryRequest::new(Some(&cursor), NonZeroU32::MIN),
        )?;
        let event = *retained_events
            .first()
            .ok_or("no retained canonical event follows the wallet cursor")?;
        following.apply_canonical_event_range(
            source,
            event,
            event.resulting_fence(),
            source.settled_tip(),
            test_transition_logical_byte_limit(),
            secondary.scan_canonical_replay_range(event.committed_range())?,
        )?;
        Ok(())
    }

    fn open_checkpointed_secondary(
        root: &Path,
        secondary_directory: &str,
    ) -> Result<RocksDbCanonicalSecondary, Box<dyn std::error::Error>> {
        Ok(RocksDbCanonicalSecondary::open_ready(
            root.join("canonical"),
            root.join(secondary_directory),
            &checkpointed_activations()?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?)
    }

    struct PublishedCheckpointedWallet {
        activations: NetworkUpgradeActivations,
        canonical_store: RocksDbCanonicalStore,
        initial_fence: CanonicalEventFence,
    }

    /// Publishes a canonical store checkpointed at height 100 under
    /// `root/canonical` and an empty READY wallet under `root/wallet`.
    fn publish_checkpointed_wallet(
        root: &Path,
    ) -> Result<PublishedCheckpointedWallet, Box<dyn std::error::Error>> {
        let activations = checkpointed_activations()?;
        let predecessor = BlockId::new(BlockHeight::new(99), BlockHash::from_bytes([0x99; 32]));
        let checkpoint_block =
            BlockId::new(BlockHeight::new(100), BlockHash::from_bytes([0xa0; 32]));
        let plan = CanonicalStoreBuildPlan::checkpointed(
            &activations,
            CommitmentTreeCheckpoint::new(predecessor, 99, CommitmentTreeFrontiers::default()),
            checkpoint_block,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )?;
        let mut canonical_builder = RocksDbCanonicalBuilder::create_fresh(
            root.join("canonical"),
            CanonicalStoreWorkload::Wallet,
            plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        canonical_builder.bulk_load_blocks(
            &activations,
            [Ok::<_, std::io::Error>(checkpointed_build_block(
                100, [0x99; 32], [0xa0; 32], true,
            ))],
        )?;
        canonical_builder.load_subtree_roots(std::iter::empty())?;
        canonical_builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            checkpoint_block,
            100,
            CommitmentTreeFrontiers::default(),
        ))?;
        let validated = canonical_builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            checkpoint_block,
            UnixTimestampMillis::new(1_750_000_100_000),
        ))?;
        let canonical_store = validated.publish_baseline(publication)?;
        let initial_fence = canonical_store.event_fence();
        assert_eq!(initial_fence.sequence_digest().block_count(), 1);
        publish_empty_wallet(root, initial_fence)?;
        Ok(PublishedCheckpointedWallet {
            activations,
            canonical_store,
            initial_fence,
        })
    }

    fn publish_empty_wallet(
        root: &Path,
        initial_fence: CanonicalEventFence,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let initial_source = source_identity_from_fence(initial_fence);
        let wallet_build_store = RocksDbWalletBuildStore::create_fresh(
            root.join("wallet"),
            Network::ZcashRegtest,
            initial_source,
            1,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let lease_request = WalletProjectionBuildLeaseRequest::new(
            WalletProjectionBuildOwner::from_bytes([0x42; 16]),
            initial_source,
            WalletProjectionRetainedEventAnchor::new(initial_fence.chain_event_sequence()),
            UnixTimestampMillis::new(u64::MAX),
        );
        let wallet_builder = RocksDbWalletBuilder::create_fresh(
            wallet_build_store,
            lease_request,
            UnixTimestampMillis::new(0),
        )?;
        let projection_digest = WalletProjectionDigestBuilder::new();
        let row_counts = projection_digest.row_counts();
        let (projection_accumulator, projection_display_digest) =
            projection_digest.finish_with_accumulator();
        let ready_evidence = WalletProjectionReadyEvidence {
            source_position: WalletProjectionSourcePosition::new(
                initial_fence.chain_epoch_id(),
                initial_fence.visible_tip(),
                initial_fence.chain_event_sequence(),
            ),
            source_sequence_digest: initial_fence.sequence_digest(),
            settled_tip: initial_fence.visible_tip(),
            projection_digest: projection_display_digest,
            projection_accumulator,
            row_counts,
            utxo_summary: WalletUtxoSetSummary {
                utxo_count: 0,
                total_value_zat: 0,
                commitment: TransparentUtxoSetCommitment::empty(),
            },
        };
        wallet_builder
            .reopen_for_validation()?
            .validate_rows(
                ready_evidence,
                WalletColdValidationConfig {
                    max_accounted_reorg_undo_bytes: 16 * 1024 * 1024,
                },
            )?
            .publish_ready_at(UnixTimestampMillis::new(0))?;
        Ok(())
    }

    fn checkpoint_accumulator(
        activations: &NetworkUpgradeActivations,
        checkpoint_fence: CanonicalEventFence,
    ) -> Result<CommitmentTreeAccumulator, Box<dyn std::error::Error>> {
        Ok(CommitmentTreeAccumulator::from_validated_frontiers(
            checkpoint_fence.visible_tip().height,
            &CommitmentTreeFrontiers::default(),
            activations,
        )?)
    }

    /// Builds the next live block above the checkpoint, advancing `accumulator`.
    fn checkpointed_append_block(
        accumulator: &mut CommitmentTreeAccumulator,
        height: u32,
        parent_hash: [u8; 32],
        block_hash: [u8; 32],
        transactions: Vec<CanonicalTransactionFacts>,
    ) -> Result<CanonicalBuildBlock, Box<dyn std::error::Error>> {
        let mut block = checkpointed_build_block_with_transactions(
            height,
            parent_hash,
            block_hash,
            false,
            transactions,
        );
        let block_id = BlockId::new(
            block.facts.block_header.height,
            block.facts.block_header.block_hash,
        );
        accumulator.append_block_commitments(block_id.height, &[], &[], &[])?;
        block.tip_metadata = accumulator.tip_metadata();
        block.tree_state_checkpoint = Some(CommitmentTreeCheckpoint::new(
            block_id,
            height,
            accumulator.validated_frontiers()?,
        ));
        Ok(block)
    }

    fn checkpointed_activations()
    -> Result<NetworkUpgradeActivations, zinder_core::NetworkUpgradeActivationsError> {
        let activations = [
            ("Overwinter", 1, 100),
            ("Sapling", 2, 101),
            ("Blossom", 3, 102),
            ("Heartwood", 4, 103),
            ("Canopy", 5, 104),
            ("NU5", 6, 105),
            ("NU6", 7, 106),
            ("NU6.1", 8, 107),
            ("NU6.2", 9, 108),
            ("NU6.3", 10, 109),
        ]
        .into_iter()
        .map(
            |(name, branch_id, activation_height)| NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(branch_id),
                activation_height: BlockHeight::new(activation_height),
                name: name.to_owned(),
            },
        )
        .collect();
        NetworkUpgradeActivations::new(Network::ZcashRegtest, activations)
    }

    fn test_transition_logical_byte_limit() -> NonZeroU64 {
        NonZeroU64::new(512 * 1024 * 1024).unwrap_or(NonZeroU64::MIN)
    }

    fn transparent_spend_public_facts(transaction_id: TransactionId) -> TransactionPublicFacts {
        TransactionPublicFacts {
            transaction_id,
            auth_digest: None,
            wtxid: None,
            version: TransactionVersion::Unsupported {
                effective_version: 0,
                version_group_id: None,
            },
            consensus_branch_id: None,
            lock_time: LockTime::Unlocked,
            expiry_height: None,
            size_bytes: 1,
            counts: TransactionComponentCounts {
                transparent_input_count: 1,
                ..TransactionComponentCounts::EMPTY
            },
            orchard_value_balance_zat: None,
            orchard_anchor: None,
            ironwood_value_balance_zat: None,
            privacy_shape: PrivacyShape::Unclassified,
            is_coinbase: false,
            unsupported_sections: vec![UnsupportedSection::FutureVersionHeader],
        }
    }

    fn coinbase_output_transaction(
        transaction_id: TransactionId,
        outputs: Vec<TransparentOutputFact>,
    ) -> CanonicalTransactionFacts {
        let mut coinbase = transparent_spend_transaction(
            transaction_id,
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            outputs,
        );
        coinbase.public_facts.version = TransactionVersion::V4;
        coinbase.public_facts.is_coinbase = true;
        coinbase.public_facts.unsupported_sections = Vec::new();
        coinbase
    }

    fn transparent_spend_transaction(
        transaction_id: TransactionId,
        transparent_inputs: Vec<TransparentInputFact>,
        transparent_outputs: Vec<TransparentOutputFact>,
    ) -> CanonicalTransactionFacts {
        let mut public_facts = transparent_spend_public_facts(transaction_id);
        public_facts.counts.transparent_input_count =
            u32::try_from(transparent_inputs.len()).unwrap_or(u32::MAX);
        public_facts.counts.transparent_output_count =
            u32::try_from(transparent_outputs.len()).unwrap_or(u32::MAX);
        CanonicalTransactionFacts {
            public_facts,
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[0x66]),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs,
            transparent_outputs,
        }
    }

    fn checkpointed_build_block(
        height: u32,
        parent_hash: [u8; 32],
        block_hash: [u8; 32],
        is_tip: bool,
    ) -> CanonicalBuildBlock {
        checkpointed_build_block_with_transactions(
            height,
            parent_hash,
            block_hash,
            is_tip,
            Vec::new(),
        )
    }

    fn checkpointed_build_block_with_transactions(
        height: u32,
        parent_hash: [u8; 32],
        block_hash: [u8; 32],
        is_tip: bool,
        transactions: Vec<CanonicalTransactionFacts>,
    ) -> CanonicalBuildBlock {
        let block_height = BlockHeight::new(height);
        let block_hash = BlockHash::from_bytes(block_hash);
        let transaction_blobs = transactions
            .iter()
            .enumerate()
            .map(|(transaction_index, transaction)| {
                TransactionBlobArtifact::new(
                    TransactionLocation::new(
                        transaction.public_facts.transaction_id,
                        block_height,
                        block_hash,
                        u32::try_from(transaction_index).unwrap_or(u32::MAX),
                    ),
                    vec![0x66],
                )
            })
            .collect();
        let facts = CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                block_height,
                block_hash,
                BlockHash::from_bytes(parent_hash),
                [0; 32],
                [0; 32],
                i64::from(height),
                0,
                [0; 32],
                0,
                0,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                &block_hash.as_bytes(),
            ),
            transactions,
        };
        let compact_block = CompactBlockArtifact::empty(
            BlockId::new(block_height, block_hash),
            BlockHash::from_bytes(parent_hash),
            height,
            CompactChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        );
        let replay_envelope = encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );
        CanonicalBuildBlock {
            facts,
            replay_envelope,
            compact_block,
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            tree_state_checkpoint: is_tip.then(|| {
                CommitmentTreeCheckpoint::new(
                    BlockId::new(block_height, block_hash),
                    height,
                    CommitmentTreeFrontiers::default(),
                )
            }),
            block_final_note_commitment_roots: None,
            transaction_blobs,
            block_blob: None,
        }
    }

    /// Durable outputs the read-gate transition spends, each at its own address.
    const READ_GATE_SPENDS: u8 = 3;

    /// Outputs the read-gate transition creates, each at a new address.
    const READ_GATE_CREATED_OUTPUTS: u8 = 2;

    /// A checkpointed canonical chain for measuring planner reads, with an
    /// empty READY wallet at its height-100 checkpoint.
    ///
    /// Block 101 funds [`READ_GATE_SPENDS`] outputs, each at its own address.
    /// Block 102 spends them all and creates [`READ_GATE_CREATED_OUTPUTS`]
    /// outputs at new addresses in one transaction, and a second transaction
    /// spends the first of those outputs.
    struct ReadGateChain {
        temporary: TempDir,
        secondary: RocksDbCanonicalSecondary,
        funded_fence: CanonicalEventFence,
        activations: NetworkUpgradeActivations,
        canonical_store: RocksDbCanonicalStore,
        fence: CanonicalEventFence,
        accumulator: CommitmentTreeAccumulator,
    }

    impl ReadGateChain {
        fn publish() -> Result<Self, Box<dyn std::error::Error>> {
            let temporary = TempDir::new()?;
            let PublishedCheckpointedWallet {
                activations,
                canonical_store,
                initial_fence,
            } = publish_checkpointed_wallet(temporary.path())?;
            let mut accumulator = checkpoint_accumulator(&activations, initial_fence)?;
            let funding = coinbase_output_transaction(
                READ_GATE_FUNDING_ID,
                (0..READ_GATE_SPENDS)
                    .map(|lane| read_gate_output(lane, lane))
                    .collect(),
            );
            let funded_block = checkpointed_append_block(
                &mut accumulator,
                101,
                [0xa0; 32],
                [0xa1; 32],
                vec![funding],
            )?;
            let (canonical_store, funded_fence) = canonical_store.commit_live_append(
                CanonicalLiveAppend::new(
                    initial_fence,
                    funded_block,
                    Vec::new(),
                    initial_fence.visible_tip(),
                    UnixTimestampMillis::new(1_750_000_100_101),
                ),
                &activations,
            )?;
            let spending_block = checkpointed_append_block(
                &mut accumulator,
                102,
                [0xa1; 32],
                [0xa2; 32],
                read_gate_spending_transactions(),
            )?;
            let (canonical_store, fence) = canonical_store.commit_live_append(
                CanonicalLiveAppend::new(
                    funded_fence,
                    spending_block,
                    Vec::new(),
                    funded_fence.visible_tip(),
                    UnixTimestampMillis::new(1_750_000_100_102),
                ),
                &activations,
            )?;
            let secondary = open_checkpointed_secondary(temporary.path(), "canonical-secondary")?;
            Ok(Self {
                temporary,
                secondary,
                funded_fence,
                activations,
                canonical_store,
                fence,
                accumulator,
            })
        }

        /// Appends block 103, which spends block 102's unspent created
        /// output and pays a new address, and catches the secondary up.
        fn append_follow_up_block(mut self) -> Result<Self, Box<dyn std::error::Error>> {
            let spend = transparent_spend_transaction(
                TransactionId::from_bytes([0x74; 32]),
                vec![TransparentInputFact::new(
                    0,
                    TransparentOutPoint::new(READ_GATE_SPENDING_ID, 1),
                )],
                vec![read_gate_output(0, 0x40)],
            );
            let block = checkpointed_append_block(
                &mut self.accumulator,
                103,
                [0xa2; 32],
                [0xa3; 32],
                vec![spend],
            )?;
            let settled_tip = self.fence.visible_tip();
            (self.canonical_store, self.fence) = self.canonical_store.commit_live_append(
                CanonicalLiveAppend::new(
                    self.fence,
                    block,
                    Vec::new(),
                    settled_tip,
                    UnixTimestampMillis::new(1_750_000_100_103),
                ),
                &self.activations,
            )?;
            self.secondary.try_catch_up()?;
            Ok(self)
        }

        /// Opens the wallet and applies block 101, leaving block 102 next.
        fn funded_wallet(&self) -> Result<RocksDbWalletStore, Box<dyn std::error::Error>> {
            let following = RocksDbWalletStore::open_ready_for_following(
                self.temporary.path().join("wallet"),
                Network::ZcashRegtest,
                RocksDbResourceBudget::for_local_tests(),
            )?;
            let source =
                WalletCanonicalSourceIdentity::from_ready_evidence(following.ready_evidence());
            let mut wallet = following.into_ready_store(source)?;
            self.apply_next_block(&mut wallet)?;
            Ok(wallet)
        }

        /// Applies the next block, settling the wallet one block below it.
        fn apply_next_block(
            &self,
            wallet: &mut RocksDbWalletStore,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let settled_tip = wallet.ready_evidence().source_position.tip;
            apply_next_event_settling_at(wallet, &self.secondary, settled_tip)
        }

        /// Plans block 102 on `wallet` within `max_logical_bytes` and cancels
        /// it before its write.
        fn plan_spending_block(
            &self,
            wallet: &mut RocksDbWalletStore,
            max_logical_bytes: NonZeroU64,
        ) -> Result<Result<u64, RocksDbWalletError>, Box<dyn std::error::Error>> {
            plan_next_event_and_cancel(
                wallet,
                &self.secondary,
                self.funded_fence.visible_tip(),
                max_logical_bytes,
            )
        }

        /// Asserts that `wallet` equals a cold build of the whole chain.
        fn assert_matches_cold_build(
            &self,
            wallet: &RocksDbWalletStore,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let cold = build_wallet_from_canonical(
                &self.secondary,
                self.temporary.path().join("wallet-cold"),
                RocksDbWalletBuildOptions {
                    supported_reorg_depth: 1,
                    ..RocksDbWalletBuildOptions::for_local_tests()
                },
            )?;
            assert_eq!(wallet.ready_evidence(), cold.store.ready_evidence());
            Ok(())
        }
    }

    const READ_GATE_FUNDING_ID: TransactionId = TransactionId::from_bytes([0x71; 32]);
    const READ_GATE_SPENDING_ID: TransactionId = TransactionId::from_bytes([0x72; 32]);

    /// Block 102's transactions: one spends every funded output and creates
    /// the new outputs, and the next spends the first new output.
    fn read_gate_spending_transactions() -> Vec<CanonicalTransactionFacts> {
        let spending = transparent_spend_transaction(
            READ_GATE_SPENDING_ID,
            (0..READ_GATE_SPENDS)
                .map(|lane| {
                    TransparentInputFact::new(
                        u32::from(lane),
                        TransparentOutPoint::new(READ_GATE_FUNDING_ID, u32::from(lane)),
                    )
                })
                .collect(),
            (0..READ_GATE_CREATED_OUTPUTS)
                .map(|lane| read_gate_output(lane, READ_GATE_SPENDS + lane))
                .collect(),
        );
        let same_block_spend = transparent_spend_transaction(
            TransactionId::from_bytes([0x73; 32]),
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::new(READ_GATE_SPENDING_ID, 0),
            )],
            Vec::new(),
        );
        vec![spending, same_block_spend]
    }

    /// Point reads of the read-gate transition for a plan that remembers
    /// each durable read until it writes the row.
    ///
    /// Each spend of a durable output reads its unspent, address-index, and
    /// balance rows and proves its spent row absent. Each created output
    /// proves its unspent, spent, and address-index rows and its new
    /// address's balance absent. The same-block spend finds its rows among
    /// the plan's writes, and its spent row among the absences the plan
    /// already observed. Each touched address proves one address-transaction
    /// row absent, and the block proves its new undo row absent and reads the
    /// one undo row it prunes.
    const fn remembered_read_gate_keys_read() -> u64 {
        let spends = READ_GATE_SPENDS as u64;
        let created_outputs = READ_GATE_CREATED_OUTPUTS as u64;
        let touched_addresses = spends + created_outputs + 1;
        4 * spends + 4 * created_outputs + touched_addresses + 2
    }

    /// Point reads of the read-gate transition for a plan that remembers no
    /// durable read.
    ///
    /// A spend of a durable output reads its unspent row, rereads it to
    /// remove it, reads its address-index row, reads its balance once to
    /// check the new total, once to compute it, and once to remove the old
    /// row, and proves its spent row absent: 7 reads. A created output proves
    /// its unspent and spent rows absent, proves them absent again before it
    /// inserts the output, proves its address-index row absent, and proves its
    /// balance absent to check, compute, and insert the new total: 8 reads.
    /// The same-block spend proves its spent row absent again. Address
    /// transactions and undo rows cost what they cost with the memo.
    const fn unremembered_read_gate_keys_read() -> u64 {
        let spends = READ_GATE_SPENDS as u64;
        let created_outputs = READ_GATE_CREATED_OUTPUTS as u64;
        let touched_addresses = spends + created_outputs + 1;
        7 * spends + 8 * created_outputs + 1 + touched_addresses + 2
    }

    #[test]
    fn spends_and_created_outputs_read_each_durable_row_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain = ReadGateChain::publish()?;
        let mut wallet = chain.funded_wallet()?;
        let keys_read_before = wallet_keys_read(&wallet);
        chain.apply_next_block(&mut wallet)?;
        let keys_read = wallet_keys_read(&wallet) - keys_read_before;

        assert_eq!(keys_read, remembered_read_gate_keys_read());
        chain.assert_matches_cold_build(&wallet)
    }

    #[test]
    fn plans_without_room_to_remember_reads_reread_rows_within_the_same_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain = ReadGateChain::publish()?;
        let mut wallet = chain.funded_wallet()?;
        let generous = test_transition_logical_byte_limit();
        assert_eq!(
            chain.plan_spending_block(&mut wallet, generous)??,
            remembered_read_gate_keys_read()
        );
        assert_eq!(
            without_read_memo(|| chain.plan_spending_block(&mut wallet, generous))??,
            unremembered_read_gate_keys_read()
        );

        let tightest =
            tightest_admitted_limit(|limit| chain.plan_spending_block(&mut wallet, limit))?;
        assert_eq!(
            tightest_admitted_limit(|limit| without_read_memo(
                || chain.plan_spending_block(&mut wallet, limit)
            ))?,
            tightest
        );
        // The control record joins the batch only after planning, so its
        // share of the limit stays free while the plan runs, and these reads
        // fit in it.
        assert!(remembered_read_gate_bytes() < control_entry_bytes(&wallet)?);
        assert_eq!(
            chain.plan_spending_block(&mut wallet, tightest)??,
            remembered_read_gate_keys_read()
        );
        apply_next_event_within(
            &mut wallet,
            &chain.secondary,
            chain.funded_fence.visible_tip(),
            tightest,
        )?;
        chain.assert_matches_cold_build(&wallet)
    }

    /// Outputs the rolled-back block created, each at a new address.
    const ROLLED_BACK_OUTPUTS: u8 = 200;

    #[test]
    fn a_rollback_near_its_limit_rereads_rows_it_could_not_remember()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let chain = FanOutChain::publish(temporary.path())?;
        let mut secondary = open_checkpointed_secondary(temporary.path(), "canonical-secondary")?;
        let mut wallet = open_wallet_at_depth(temporary.path(), 1)?;
        let settled_tip = wallet.ready_evidence().settled_tip;
        apply_next_event_settling_at(&mut wallet, &secondary, settled_tip)?;
        chain.replace_tip()?;
        secondary.try_catch_up()?;
        let plan = |wallet: &mut RocksDbWalletStore, limit| {
            plan_next_event_and_cancel(wallet, &secondary, settled_tip, limit)
        };
        // The rollback reads the block's undo row, and the address-transaction
        // row of each address its coinbase paid. Each created output then
        // costs its unspent row, the absence of its spent row, its
        // address-index row, and its balance, which the rollback removes. The
        // empty replacement reads nothing durable. Without the memo, each
        // output rereads its unspent row once and its balance twice more.
        let outputs = u64::from(ROLLED_BACK_OUTPUTS);
        let remembered = 5 * outputs + 1;
        let unremembered = 8 * outputs + 1;
        let generous = test_transition_logical_byte_limit();
        assert_eq!(plan(&mut wallet, generous)??, remembered);
        assert_eq!(
            without_read_memo(|| plan(&mut wallet, generous))??,
            unremembered
        );

        // Near its limit the rollback has deleted most rows and the small
        // replacement writes few, so the remembered spent-row absences
        // outgrow their room and the memo drops them.
        let tightest = tightest_admitted_limit(|limit| plan(&mut wallet, limit))?;
        let just_refused = NonZeroU64::new(tightest.get() - 1).ok_or("tightest limit is 1")?;
        assert!(without_read_memo(|| plan(&mut wallet, tightest))?.is_ok());
        assert!(matches!(
            without_read_memo(|| plan(&mut wallet, just_refused))?,
            Err(RocksDbWalletError::TransitionLogicalByteLimit { .. })
        ));
        let tight_keys_read = plan(&mut wallet, tightest)??;
        assert!(
            remembered < tight_keys_read && tight_keys_read < unremembered,
            "{tight_keys_read}"
        );

        apply_next_event_within(&mut wallet, &secondary, settled_tip, tightest)?;
        let cold = build_wallet_from_canonical(
            &secondary,
            temporary.path().join("wallet-cold"),
            RocksDbWalletBuildOptions {
                supported_reorg_depth: 1,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        assert_eq!(wallet.ready_evidence(), cold.store.ready_evidence());
        Ok(())
    }

    /// A checkpointed canonical store whose block 101 pays
    /// [`ROLLED_BACK_OUTPUTS`] new addresses from its coinbase, with an empty
    /// READY wallet at the checkpoint.
    struct FanOutChain {
        activations: NetworkUpgradeActivations,
        canonical_store: RocksDbCanonicalStore,
        fence: CanonicalEventFence,
        checkpoint: CommitmentTreeAccumulator,
    }

    impl FanOutChain {
        fn publish(root: &Path) -> Result<Self, Box<dyn std::error::Error>> {
            let PublishedCheckpointedWallet {
                activations,
                canonical_store,
                initial_fence,
            } = publish_checkpointed_wallet(root)?;
            let checkpoint = checkpoint_accumulator(&activations, initial_fence)?;
            let coinbase = coinbase_output_transaction(
                TransactionId::from_bytes([0x81; 32]),
                (0..ROLLED_BACK_OUTPUTS)
                    .map(|lane| read_gate_output(lane, lane))
                    .collect(),
            );
            let block = checkpointed_append_block(
                &mut checkpoint.clone(),
                101,
                [0xa0; 32],
                [0xa1; 32],
                vec![coinbase],
            )?;
            let (canonical_store, fence) = canonical_store.commit_live_append(
                CanonicalLiveAppend::new(
                    initial_fence,
                    block,
                    Vec::new(),
                    initial_fence.visible_tip(),
                    UnixTimestampMillis::new(1_750_000_100_101),
                ),
                &activations,
            )?;
            Ok(Self {
                activations,
                canonical_store,
                fence,
                checkpoint,
            })
        }

        /// Replaces block 101 with a block whose coinbase pays no one.
        fn replace_tip(self) -> Result<(), Box<dyn std::error::Error>> {
            let replacement_hash = [0xc1; 32];
            let replacement = checkpointed_append_block(
                &mut self.checkpoint.clone(),
                101,
                [0xa0; 32],
                replacement_hash,
                vec![coinbase_without_transparent_outputs(replacement_hash)],
            )?;
            self.canonical_store.commit_live_replacement(
                CanonicalLiveReplacement::new(
                    self.fence,
                    vec![CanonicalReplacementBlock::new(replacement, Vec::new())],
                    UnixTimestampMillis::new(1_750_000_200_000),
                ),
                &self.activations,
            )?;
            Ok(())
        }
    }

    /// Runs `plan` with no room for the durable-read memo.
    fn without_read_memo<T>(plan: impl FnOnce() -> T) -> T {
        super::DURABLE_READ_MEMO_DISABLED.set(true);
        let outcome = plan();
        super::DURABLE_READ_MEMO_DISABLED.set(false);
        outcome
    }

    /// Logical bytes of the read-gate reads that the memo still holds when
    /// the same-block spend reads its spent row: the spent-row absence of
    /// each created output.
    fn remembered_read_gate_bytes() -> u64 {
        36 * u64::from(READ_GATE_CREATED_OUTPUTS)
    }

    /// Logical bytes the wallet's current control record charges a
    /// transition, which the next control record matches within a few bytes.
    fn control_entry_bytes(wallet: &RocksDbWalletStore) -> Result<u64, Box<dyn std::error::Error>> {
        Ok(u64::try_from(
            WALLET_STORE_CONTROL_KEY.len() + wallet.control.encode()?.len(),
        )?)
    }

    /// Plans the next retained event on `wallet` within `max_logical_bytes`
    /// and cancels it before its write, returning the plan's point reads, or
    /// the error that refused it.
    fn plan_next_event_and_cancel(
        wallet: &mut RocksDbWalletStore,
        secondary: &RocksDbCanonicalSecondary,
        settled_tip: BlockId,
        max_logical_bytes: NonZeroU64,
    ) -> Result<Result<u64, RocksDbWalletError>, Box<dyn std::error::Error>> {
        let event = next_retained_event(secondary, wallet)?;
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        let keys_read_before = wallet_keys_read(wallet);
        let outcome = wallet.apply_canonical_event_range_cancellable(
            source,
            event,
            event.resulting_fence(),
            settled_tip,
            max_logical_bytes,
            secondary.scan_canonical_replay_range(event.committed_range())?,
            || true,
        );
        Ok(match outcome {
            Err(RocksDbWalletError::ProjectionTransitionCancelled) => {
                Ok(wallet_keys_read(wallet) - keys_read_before)
            }
            Err(error) => Err(error),
            Ok(()) => return Err("a cancelled plan was published".into()),
        })
    }

    fn apply_next_event_within(
        wallet: &mut RocksDbWalletStore,
        secondary: &RocksDbCanonicalSecondary,
        settled_tip: BlockId,
        max_logical_bytes: NonZeroU64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let event = next_retained_event(secondary, wallet)?;
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        wallet.apply_canonical_event_range(
            source,
            event,
            event.resulting_fence(),
            settled_tip,
            max_logical_bytes,
            secondary.scan_canonical_replay_range(event.committed_range())?,
        )?;
        Ok(())
    }

    #[test]
    fn a_release_without_wallet_filters_reads_and_follows_a_filtered_store()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain = ReadGateChain::publish()?;
        let mut wallet = chain.funded_wallet()?;
        chain.apply_next_block(&mut wallet)?;
        flush_wallet(&wallet)?;
        assert_hash_keyed_files_carry_partitioned_filters(&wallet)?;
        let rows = wallet_rows(&wallet)?;
        // This key sorts between the stored spent keys, so a read of it
        // cannot skip the file on its key range.
        let absent_spent_key = WalletOutpointKey::new(TransparentOutPoint::new(
            TransactionId::from_bytes([0x71; 32]),
            u32::from(READ_GATE_SPENDS),
        ));
        assert!(read_absent_spent_row(&wallet, absent_spent_key)?);
        drop(wallet);
        let chain = chain.append_follow_up_block()?;

        let mut earlier = open_with_pre_filter_options(&chain.temporary.path().join("wallet"))?;
        assert_hash_keyed_files_carry_partitioned_filters(&earlier)?;
        assert_eq!(wallet_rows(&earlier)?, rows);
        for ((family_name, key), encoded) in &rows {
            let family = column_family(&earlier.bounded_open, family_name)?;
            assert_eq!(
                earlier.bounded_open.db.get_cf(&family, key)?.as_ref(),
                Some(encoded),
                "{family_name}"
            );
        }
        // The earlier options name no filter policy, so their reads ignore
        // the filters in these files.
        assert!(!read_absent_spent_row(&earlier, absent_spent_key)?);

        chain.apply_next_block(&mut earlier)?;
        chain.assert_matches_cold_build(&earlier)?;
        let followed_rows = wallet_rows(&earlier)?;
        let followed_evidence = earlier.ready_evidence().clone();
        drop(earlier);

        let following = RocksDbWalletStore::open_ready_for_following(
            chain.temporary.path().join("wallet"),
            Network::ZcashRegtest,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(following.ready_evidence());
        let current = following.into_ready_store(source)?;
        assert_eq!(wallet_rows(&current)?, followed_rows);
        assert_eq!(current.ready_evidence(), &followed_evidence);
        Ok(())
    }

    /// Opens the READY wallet at `path` as releases before the wallet filters
    /// did.
    ///
    /// Every family takes the shared table factory alone, with no filter
    /// policy and a single-level index.
    fn open_with_pre_filter_options(
        path: &Path,
    ) -> Result<RocksDbWalletStore, Box<dyn std::error::Error>> {
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary { path },
            RocksDbResourceBudget::for_local_tests(),
            pre_filter_column_family_descriptors,
        )?;
        let control = decode_only_control(&bounded_open)?;
        let WalletProjectionBuildState::Ready(ready_evidence) = &control.build_state else {
            return Err("wallet is not READY".into());
        };
        Ok(RocksDbWalletStore {
            ready_evidence: ready_evidence.clone(),
            bounded_open,
            control,
            verified_undo_suffix_generation: None,
            last_transition_footprint: None,
        })
    }

    fn pre_filter_column_family_descriptors(
        block_cache: &Cache,
        resource_budget: RocksDbResourceBudget,
    ) -> Vec<ColumnFamilyDescriptor> {
        WALLET_DATA_COLUMN_FAMILIES
            .into_iter()
            .map(|(name, _)| {
                let mut options = Options::default();
                options.set_compression_type(DBCompressionType::Snappy);
                options
                    .set_block_based_table_factory(&build_block_based_table_factory(block_cache));
                options.set_write_buffer_size(
                    usize::try_from(resource_budget.write_buffer_bytes).unwrap_or(usize::MAX),
                );
                options.set_max_write_buffer_number(resource_budget.max_write_buffer_count);
                ColumnFamilyDescriptor::new(name, options)
            })
            .collect()
    }

    fn flush_wallet(wallet: &RocksDbWalletStore) -> Result<(), Box<dyn std::error::Error>> {
        let mut families = Vec::new();
        for (family_name, _) in WALLET_DATA_COLUMN_FAMILIES {
            families.push(column_family(&wallet.bounded_open, family_name)?);
        }
        let mut options = FlushOptions::default();
        options.set_wait(true);
        wallet
            .bounded_open
            .db
            .flush_cfs_opt(&families.iter().collect::<Vec<_>>(), &options)?;
        Ok(())
    }

    /// Asserts that the live files of each hash-keyed family hold rows under
    /// a filter and a partitioned index.
    fn assert_hash_keyed_files_carry_partitioned_filters(
        wallet: &RocksDbWalletStore,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for family_name in [
            TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
            TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
            TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
            TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
        ] {
            let family = column_family(&wallet.bounded_open, family_name)?;
            let properties = wallet
                .bounded_open
                .db
                .property_value_cf(&family, "rocksdb.aggregated-table-properties")?
                .ok_or("aggregated table properties are unavailable")?;
            let property = |field: &str| -> Result<u64, Box<dyn std::error::Error>> {
                for entry in properties.split("; ") {
                    if let Some((name, number)) = entry.split_once('=')
                        && name.trim() == field
                    {
                        return Ok(number.trim().parse()?);
                    }
                }
                Ok(0)
            };
            assert!(property("# entries for filter")? > 0, "{family_name}");
            assert!(property("filter block size")? > 0, "{family_name}");
            assert!(property("# index partitions")? > 0, "{family_name}");
        }
        Ok(())
    }

    /// Reads the absent spent row `key` and returns whether a filter
    /// rejected the read.
    fn read_absent_spent_row(
        wallet: &RocksDbWalletStore,
        key: WalletOutpointKey,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let statistics = &wallet.bounded_open.statistics;
        let rejections_before = statistics.get_ticker_count(Ticker::BloomFilterUseful);
        let family = column_family(&wallet.bounded_open, TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY)?;
        assert_eq!(
            wallet.bounded_open.db.get_cf(&family, key.as_bytes())?,
            None
        );
        Ok(statistics.get_ticker_count(Ticker::BloomFilterUseful) > rejections_before)
    }

    /// Logical-byte limit that admits every plan the limit searches below run.
    const TIGHTEST_LIMIT_SEARCH_CEILING: u64 = 1024 * 1024;

    /// Returns the smallest logical-byte limit at which `plan` is admitted,
    /// given that it is admitted at [`TIGHTEST_LIMIT_SEARCH_CEILING`].
    fn tightest_admitted_limit(
        mut plan: impl FnMut(
            NonZeroU64,
        )
            -> Result<Result<u64, RocksDbWalletError>, Box<dyn std::error::Error>>,
    ) -> Result<NonZeroU64, Box<dyn std::error::Error>> {
        let mut refused = 0;
        let mut admitted = TIGHTEST_LIMIT_SEARCH_CEILING;
        while admitted - refused > 1 {
            let limit = refused + (admitted - refused) / 2;
            match plan(NonZeroU64::new(limit).ok_or("limit is zero")?)? {
                Ok(_) => admitted = limit,
                Err(RocksDbWalletError::TransitionLogicalByteLimit { .. }) => refused = limit,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(NonZeroU64::new(admitted).ok_or("limit is zero")?)
    }

    #[test]
    fn durable_read_memo_keeps_only_the_bytes_the_plan_leaves_it() -> Result<(), RocksDbWalletError>
    {
        let key = |byte| (TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY, vec![byte; 4]);
        let mut memo = DurableReadMemo::default();
        memo.remember(key(1), Some([0; 4].as_slice()), 8)?;
        memo.remember(key(2), None, 11)?;
        assert!(memo.get(&key(1)).is_some());
        assert!(memo.get(&key(2)).is_none());

        memo.remember(key(3), None, 12)?;
        memo.release_for_write(&key(3), 8)?;
        assert!(memo.get(&key(3)).is_none());
        assert!(memo.get(&key(1)).is_some());

        memo.release_for_write(&key(4), 7)?;
        assert!(memo.get(&key(1)).is_none());
        assert_eq!(memo.logical_bytes, 0);
        Ok(())
    }

    /// One output of the read-gate fixture, paying the address of `lane`.
    fn read_gate_output(output_index: u8, lane: u8) -> TransparentOutputFact {
        let mut address = [0x7a; 32];
        address[0] = lane;
        TransparentOutputFact::new(
            u32::from(output_index),
            5,
            [0x76, lane],
            TransparentAddressScriptHash::from_bytes(address),
        )
    }

    fn wallet_keys_read(wallet: &RocksDbWalletStore) -> u64 {
        wallet
            .bounded_open
            .statistics
            .get_ticker_count(Ticker::NumberKeysRead)
    }

    fn apply_next_event_settling_at(
        wallet: &mut RocksDbWalletStore,
        secondary: &RocksDbCanonicalSecondary,
        settled_tip: BlockId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let event = next_retained_event(secondary, wallet)?;
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        wallet.apply_canonical_event_range(
            source,
            event,
            event.resulting_fence(),
            settled_tip,
            test_transition_logical_byte_limit(),
            secondary.scan_canonical_replay_range(event.committed_range())?,
        )?;
        Ok(())
    }

    /// Settlement lag of the coinbase-only chains below, and the reorg depth
    /// their wallets support.
    const COINBASE_ONLY_DEPTH: u32 = 4;

    /// Visible tip a [`FollowedCoinbaseOnlyChain`] wallet reaches on one handle.
    const FOLLOWED_TIP: u32 = 106;

    /// A retained undo row that the next append neither prunes nor binds to
    /// its settled tip, and that replacing the tip does not roll back.
    const DAMAGED_UNDO_HEIGHT: u32 = 105;

    /// A reorg window sixteen times [`COINBASE_ONLY_DEPTH`], within the
    /// canonical store's reorg policy.
    const LARGE_COINBASE_ONLY_DEPTH: u32 = 16 * COINBASE_ONLY_DEPTH;

    #[test]
    fn appends_on_a_verified_suffix_read_only_the_undo_rows_they_change()
    -> Result<(), Box<dyn std::error::Error>> {
        let small = steady_state_append_work_at_depth(COINBASE_ONLY_DEPTH)?;
        let large = steady_state_append_work_at_depth(LARGE_COINBASE_ONLY_DEPTH)?;
        assert_eq!(small, large);
        assert!(large.keys_read < u64::from(LARGE_COINBASE_ONLY_DEPTH));
        Ok(())
    }

    /// Work that one append of a block without transparent activity does on
    /// a handle that verified its undo suffix.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct AppendWork {
        undo_rows: UndoRowWorkCounts,
        /// Delta of the wallet's `number.keys.read` ticker, which counts
        /// every point read of every family.
        keys_read: u64,
    }

    /// Asserts the work of every append on a coinbase-only chain that
    /// settles `depth` blocks below its tip, and returns the last one's.
    ///
    /// The wallet follows three reorg windows and reopens once.
    fn steady_state_append_work_at_depth(
        depth: u32,
    ) -> Result<AppendWork, Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let window_full = 100 + depth;
        let reopened_at = 100 + 2 * depth + 1;
        let through = 100 + 3 * depth;
        let _chain = CoinbaseOnlyChain::publish_at_depth(temporary.path(), depth, through)?;
        let secondary = open_checkpointed_secondary(temporary.path(), "canonical-secondary")?;
        let mut wallet = open_wallet_at_depth(temporary.path(), depth)?;
        // A block without transparent activity reads no output, balance, or
        // address-transaction row, so each of its point reads is a durable
        // undo-row read.
        let work = |durable_reads, value_copies, full_decodes| AppendWork {
            undo_rows: UndoRowWorkCounts {
                durable_reads,
                value_copies,
                full_decodes,
            },
            keys_read: durable_reads,
        };
        let window = u64::from(depth);
        let mut observed = Vec::new();
        let mut expected = Vec::new();
        for height in 101..=through {
            if height == reopened_at {
                drop(wallet);
                wallet = open_wallet_at_depth(temporary.path(), depth)?;
            }
            take_undo_row_work();
            let keys_read_before = wallet_keys_read(&wallet);
            apply_next_coinbase_only_event_at_depth(&mut wallet, &secondary, depth)?;
            let keys_read = wallet_keys_read(&wallet) - keys_read_before;
            // The control record is read by a scan, so the store's point-read
            // ticker counts exactly the plan's point reads.
            let footprint = wallet
                .last_transition_footprint()
                .ok_or("a published append records its footprint")?;
            assert_eq!(footprint.durable_point_reads, keys_read, "height {height}");
            observed.push((
                height,
                AppendWork {
                    undo_rows: take_undo_row_work(),
                    keys_read,
                },
            ));
            // Every append reads its new undo key once to prove it absent.
            // The first transition on a handle then decodes the whole suffix
            // in place; later appends read and decode only the successor row
            // that binds the new settled tip and the one row they prune.
            let expected_work = if height == 101 {
                work(1, 0, 1)
            } else if height <= window_full {
                work(1, 0, 0)
            } else if height == reopened_at {
                work(window + 2, 1, window + 2)
            } else {
                work(3, 1, 2)
            };
            expected.push((height, expected_work));
        }
        assert_eq!(observed, expected, "depth {depth}");

        let cold = build_wallet_from_canonical(
            &secondary,
            temporary.path().join("wallet-cold"),
            RocksDbWalletBuildOptions {
                supported_reorg_depth: depth,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        assert_eq!(wallet.ready_evidence(), cold.store.ready_evidence());
        let (_, steady_state) = observed.last().ok_or("no append was observed")?;
        Ok(*steady_state)
    }

    #[test]
    fn first_transition_after_open_rejects_a_damaged_retained_undo_row()
    -> Result<(), Box<dyn std::error::Error>> {
        for damage in UndoRowDamage::ALL {
            let fixture = FollowedCoinbaseOnlyChain::follow()?;
            damage.apply(&fixture.wallet, BlockHeight::new(DAMAGED_UNDO_HEIGHT))?;
            let before = fixture.wallet.ready_evidence().clone();
            drop(fixture.wallet);
            let _chain = fixture.chain.append_through(FOLLOWED_TIP + 1)?;
            let mut secondary = fixture.secondary;
            secondary.try_catch_up()?;
            let mut reopened = open_coinbase_only_wallet(fixture.temporary.path())?;
            assert_eq!(reopened.verified_undo_suffix_generation, None);

            let outcome = apply_next_coinbase_only_event(&mut reopened, &secondary);
            assert_damage_reported(damage, outcome)?;
            assert_eq!(reopened.ready_evidence(), &before);
        }
        Ok(())
    }

    #[test]
    fn rollback_on_a_verified_handle_rejects_a_damaged_retained_undo_row()
    -> Result<(), Box<dyn std::error::Error>> {
        for damage in UndoRowDamage::ALL {
            let mut fixture = FollowedCoinbaseOnlyChain::follow()?;
            damage.apply(&fixture.wallet, BlockHeight::new(DAMAGED_UNDO_HEIGHT))?;
            let before = fixture.wallet.ready_evidence().clone();
            let _chain = fixture.chain.replace_tip()?;
            fixture.secondary.try_catch_up()?;
            let event = next_retained_event(&fixture.secondary, &fixture.wallet)?;
            assert!(event.reverted_range().is_some(), "{event:?}");

            let outcome = apply_next_coinbase_only_event(&mut fixture.wallet, &fixture.secondary);
            assert_damage_reported(damage, outcome)?;
            assert_eq!(fixture.wallet.ready_evidence(), &before);
        }
        Ok(())
    }

    #[test]
    fn reconciliation_on_a_verified_handle_rejects_a_damaged_retained_undo_row()
    -> Result<(), Box<dyn std::error::Error>> {
        for damage in UndoRowDamage::ALL {
            let mut fixture = FollowedCoinbaseOnlyChain::follow()?;
            damage.apply(&fixture.wallet, BlockHeight::new(DAMAGED_UNDO_HEIGHT))?;
            let before = fixture.wallet.ready_evidence().clone();
            let _chain = fixture.chain.append_through(FOLLOWED_TIP + 1)?;
            fixture.secondary.try_catch_up()?;

            let outcome =
                reconcile_next_coinbase_only_event(&mut fixture.wallet, &fixture.secondary);
            assert_damage_reported(damage, outcome)?;
            assert_eq!(fixture.wallet.ready_evidence(), &before);
        }
        Ok(())
    }

    #[test]
    fn appends_on_a_verified_handle_reject_a_damaged_undo_row_before_pruning_it()
    -> Result<(), Box<dyn std::error::Error>> {
        for damage in UndoRowDamage::ALL {
            let mut fixture = FollowedCoinbaseOnlyChain::follow()?;
            damage.apply(&fixture.wallet, BlockHeight::new(DAMAGED_UNDO_HEIGHT))?;
            let _chain = fixture
                .chain
                .append_through(DAMAGED_UNDO_HEIGHT + COINBASE_ONLY_DEPTH)?;
            fixture.secondary.try_catch_up()?;

            // This append prunes a row two below the damaged one.
            apply_next_coinbase_only_event(&mut fixture.wallet, &fixture.secondary)?;
            assert_eq!(
                fixture.wallet.verified_undo_suffix_generation,
                Some(fixture.wallet.control.writer_generation)
            );
            let before = fixture.wallet.ready_evidence().clone();
            // This append settles the block below the damaged row, so it
            // reads that row as the successor that binds the settled tip.
            let outcome = apply_next_coinbase_only_event(&mut fixture.wallet, &fixture.secondary);
            let error = outcome
                .err()
                .ok_or_else(|| format!("append accepted a {damage:?} undo row"))?;
            let wallet_error = error
                .downcast_ref::<RocksDbWalletError>()
                .ok_or_else(|| format!("{damage:?} undo row failed outside the wallet: {error}"))?;
            assert!(
                damage.is_reported_on_append_by(wallet_error),
                "{damage:?}: {wallet_error:?}"
            );
            assert_eq!(fixture.wallet.ready_evidence(), &before);
            assert_eq!(
                before.settled_tip.height.next(),
                Some(BlockHeight::new(DAMAGED_UNDO_HEIGHT - 1))
            );
        }
        Ok(())
    }

    /// Damage done to one retained undo row behind a wallet handle's back.
    #[derive(Clone, Copy, Debug)]
    enum UndoRowDamage {
        Missing,
        Corrupt,
        Disconnected,
    }

    impl UndoRowDamage {
        const ALL: [Self; 3] = [Self::Missing, Self::Corrupt, Self::Disconnected];

        fn apply(
            self,
            wallet: &RocksDbWalletStore,
            height: BlockHeight,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let db = &wallet.bounded_open.db;
            let family = column_family(&wallet.bounded_open, REORG_UNDO_COLUMN_FAMILY)?;
            let key = height.value().to_be_bytes();
            let encoded = db
                .get_cf(&family, key)?
                .ok_or("damaged undo row must be retained")?;
            match self {
                Self::Missing => db.delete_cf(&family, key)?,
                Self::Corrupt => {
                    let mut corrupt = encoded;
                    corrupt.push(0);
                    db.put_cf(&family, key, corrupt)?;
                }
                Self::Disconnected => {
                    let mut undo = WalletReorgUndo::decode(&key, &encoded)?;
                    undo.source_sequence_digest_before =
                        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                            CanonicalBlockFactsSequenceDigestVersion::V1,
                            undo.source_sequence_digest_before.block_count(),
                            [0x5a; 32],
                        );
                    db.put_cf(&family, key, undo.encode_value()?)?;
                }
            }
            Ok(())
        }

        /// Returns whether `error` is the rejection a whole-suffix check
        /// reports for this damage.
        fn is_reported_by(self, error: &RocksDbWalletError) -> bool {
            match self {
                Self::Missing => matches!(
                    error,
                    RocksDbWalletError::ProjectionTransitionRejected { reason }
                        if *reason == "planned wallet undo suffix has a missing durable record"
                ),
                Self::Corrupt => matches!(
                    error,
                    RocksDbWalletError::Contract(
                        WalletProjectionContractError::DurableLengthPrefixMismatch {
                            field: "reorg_undo value"
                        }
                    )
                ),
                Self::Disconnected => matches!(
                    error,
                    RocksDbWalletError::ProjectionTransitionRejected { reason }
                        if *reason == "durable wallet undo suffix has disconnected source digests"
                ),
            }
        }

        /// Returns whether `error` is the rejection an incremental append
        /// reports when it reads this damaged row as the successor of its new
        /// settled tip.
        fn is_reported_on_append_by(self, error: &RocksDbWalletError) -> bool {
            match self {
                Self::Missing => matches!(
                    error,
                    RocksDbWalletError::WalletProjectionRebuildRequired { reason }
                        if *reason == "durable undo suffix cannot authenticate the advanced canonical settled tip"
                ),
                Self::Corrupt | Self::Disconnected => self.is_reported_by(error),
            }
        }
    }

    fn assert_damage_reported(
        damage: UndoRowDamage,
        outcome: Result<(), Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = outcome
            .err()
            .ok_or_else(|| format!("transition accepted a {damage:?} undo row"))?;
        let wallet_error = error
            .downcast_ref::<RocksDbWalletError>()
            .ok_or_else(|| format!("{damage:?} undo row failed outside the wallet: {error}"))?;
        assert!(
            damage.is_reported_by(wallet_error),
            "{damage:?}: {wallet_error:?}"
        );
        Ok(())
    }

    /// A wallet that followed a coinbase-only chain to [`FOLLOWED_TIP`] on one
    /// handle, so the handle has verified its full undo window.
    struct FollowedCoinbaseOnlyChain {
        chain: CoinbaseOnlyChain,
        secondary: RocksDbCanonicalSecondary,
        wallet: RocksDbWalletStore,
        temporary: TempDir,
    }

    impl FollowedCoinbaseOnlyChain {
        fn follow() -> Result<Self, Box<dyn std::error::Error>> {
            let temporary = TempDir::new()?;
            let chain = CoinbaseOnlyChain::publish(temporary.path(), FOLLOWED_TIP)?;
            let secondary = open_checkpointed_secondary(temporary.path(), "canonical-secondary")?;
            let mut wallet = open_coinbase_only_wallet(temporary.path())?;
            while wallet.ready_evidence().source_position.tip.height
                < BlockHeight::new(FOLLOWED_TIP)
            {
                apply_next_coinbase_only_event(&mut wallet, &secondary)?;
            }
            assert_eq!(
                wallet.ready_evidence().row_counts.reorg_undo_count,
                u64::from(COINBASE_ONLY_DEPTH)
            );
            assert_eq!(
                wallet.verified_undo_suffix_generation,
                Some(wallet.control.writer_generation)
            );
            Ok(Self {
                chain,
                secondary,
                wallet,
                temporary,
            })
        }
    }

    /// A checkpointed canonical store of blocks without transparent activity.
    ///
    /// Each block's only transaction is a coinbase without transparent
    /// outputs, and the store settles `depth` blocks below its tip,
    /// [`COINBASE_ONLY_DEPTH`] unless published at another depth.
    struct CoinbaseOnlyChain {
        activations: NetworkUpgradeActivations,
        canonical_store: RocksDbCanonicalStore,
        fence: CanonicalEventFence,
        accumulator: CommitmentTreeAccumulator,
        accumulator_below_tip: CommitmentTreeAccumulator,
        depth: u32,
    }

    impl CoinbaseOnlyChain {
        /// Publishes the height-100 checkpoint and its READY wallet under
        /// `root`, then appends coinbase-only blocks through height `through`.
        fn publish(root: &Path, through: u32) -> Result<Self, Box<dyn std::error::Error>> {
            Self::publish_at_depth(root, COINBASE_ONLY_DEPTH, through)
        }

        /// Publishes like [`Self::publish`] a chain that settles `depth`
        /// blocks below its tip.
        fn publish_at_depth(
            root: &Path,
            depth: u32,
            through: u32,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let published = publish_checkpointed_wallet(root)?;
            let accumulator =
                checkpoint_accumulator(&published.activations, published.initial_fence)?;
            Self {
                activations: published.activations,
                canonical_store: published.canonical_store,
                fence: published.initial_fence,
                accumulator_below_tip: accumulator.clone(),
                accumulator,
                depth,
            }
            .append_through(through)
        }

        fn append_through(mut self, through: u32) -> Result<Self, Box<dyn std::error::Error>> {
            for height in self.fence.visible_tip().height.value() + 1..=through {
                self.accumulator_below_tip = self.accumulator.clone();
                let block_hash = coinbase_only_hash(height);
                let block = checkpointed_append_block(
                    &mut self.accumulator,
                    height,
                    coinbase_only_hash(height - 1),
                    block_hash,
                    vec![coinbase_without_transparent_outputs(block_hash)],
                )?;
                (self.canonical_store, self.fence) = self.canonical_store.commit_live_append(
                    CanonicalLiveAppend::new(
                        self.fence,
                        block,
                        Vec::new(),
                        coinbase_only_settled_tip_at_depth(BlockHeight::new(height), self.depth),
                        UnixTimestampMillis::new(1_750_000_100_000 + u64::from(height)),
                    ),
                    &self.activations,
                )?;
            }
            Ok(self)
        }

        /// Replaces the visible tip with a coinbase-only sibling block.
        fn replace_tip(mut self) -> Result<Self, Box<dyn std::error::Error>> {
            let height = self.fence.visible_tip().height.value();
            let mut accumulator = self.accumulator_below_tip.clone();
            let block_hash = [0xc0; 32];
            let block = checkpointed_append_block(
                &mut accumulator,
                height,
                coinbase_only_hash(height - 1),
                block_hash,
                vec![coinbase_without_transparent_outputs(block_hash)],
            )?;
            (self.canonical_store, self.fence) = self.canonical_store.commit_live_replacement(
                CanonicalLiveReplacement::new(
                    self.fence,
                    vec![CanonicalReplacementBlock::new(block, Vec::new())],
                    UnixTimestampMillis::new(1_750_000_200_000),
                ),
                &self.activations,
            )?;
            self.accumulator = accumulator;
            Ok(self)
        }
    }

    /// A block's unique coinbase, identified by the block's hash bytes.
    fn coinbase_without_transparent_outputs(block_hash: [u8; 32]) -> CanonicalTransactionFacts {
        let mut public_facts =
            transparent_spend_public_facts(TransactionId::from_bytes(block_hash));
        public_facts.version = TransactionVersion::V4;
        public_facts.is_coinbase = true;
        public_facts.unsupported_sections = Vec::new();
        CanonicalTransactionFacts {
            public_facts,
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[0x66]),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs: vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            transparent_outputs: Vec::new(),
        }
    }

    fn coinbase_only_hash(height: u32) -> [u8; 32] {
        if height == 100 {
            return [0xa0; 32];
        }
        let mut hash = [0xb0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }

    fn coinbase_only_settled_tip(visible_height: BlockHeight) -> BlockId {
        coinbase_only_settled_tip_at_depth(visible_height, COINBASE_ONLY_DEPTH)
    }

    fn coinbase_only_settled_tip_at_depth(visible_height: BlockHeight, depth: u32) -> BlockId {
        let height = visible_height.value().saturating_sub(depth).max(100);
        BlockId::new(
            BlockHeight::new(height),
            BlockHash::from_bytes(coinbase_only_hash(height)),
        )
    }

    /// Opens the wallet under `root` at the coinbase-only chain's reorg depth.
    fn open_coinbase_only_wallet(
        root: &Path,
    ) -> Result<RocksDbWalletStore, Box<dyn std::error::Error>> {
        open_wallet_at_depth(root, COINBASE_ONLY_DEPTH)
    }

    /// Opens the wallet under `root` for following, raised to `depth`.
    fn open_wallet_at_depth(
        root: &Path,
        depth: u32,
    ) -> Result<RocksDbWalletStore, Box<dyn std::error::Error>> {
        let mut following = RocksDbWalletStore::open_ready_for_following(
            root.join("wallet"),
            Network::ZcashRegtest,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        following.raise_supported_reorg_depth(depth)?;
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(following.ready_evidence());
        Ok(following.into_ready_store(source)?)
    }

    fn next_retained_event(
        secondary: &RocksDbCanonicalSecondary,
        wallet: &RocksDbWalletStore,
    ) -> Result<CanonicalRetainedEvent, Box<dyn std::error::Error>> {
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        let cursor = source.source_position().event_cursor.as_bytes();
        let events = secondary.canonical_event_history(CanonicalEventHistoryRequest::new(
            Some(&cursor),
            NonZeroU32::MIN,
        ))?;
        Ok(*events
            .first()
            .ok_or("no retained canonical event follows the wallet cursor")?)
    }

    fn apply_next_coinbase_only_event(
        wallet: &mut RocksDbWalletStore,
        secondary: &RocksDbCanonicalSecondary,
    ) -> Result<(), Box<dyn std::error::Error>> {
        apply_next_coinbase_only_event_at_depth(wallet, secondary, COINBASE_ONLY_DEPTH)
    }

    fn apply_next_coinbase_only_event_at_depth(
        wallet: &mut RocksDbWalletStore,
        secondary: &RocksDbCanonicalSecondary,
        depth: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let event = next_retained_event(secondary, wallet)?;
        let fence = event.resulting_fence();
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        wallet.apply_canonical_event_range(
            source,
            event,
            fence,
            coinbase_only_settled_tip_at_depth(fence.visible_tip().height, depth),
            test_transition_logical_byte_limit(),
            secondary.scan_canonical_replay_range(event.committed_range())?,
        )?;
        Ok(())
    }

    fn reconcile_next_coinbase_only_event(
        wallet: &mut RocksDbWalletStore,
        secondary: &RocksDbCanonicalSecondary,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let event = next_retained_event(secondary, wallet)?;
        let fence = event.resulting_fence();
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        let replay_range = event.committed_range();
        wallet.reconcile_canonical_event_sequence(
            source,
            &[event],
            fence,
            coinbase_only_settled_tip(fence.visible_tip().height),
            None,
            replay_range,
            test_transition_logical_byte_limit(),
            secondary.scan_canonical_replay_range(replay_range)?,
        )?;
        Ok(())
    }

    #[test]
    fn transition_accounting_charges_each_final_row_twice_and_control_once()
    -> Result<(), RocksDbWalletError> {
        const KEY: [u8; 4] = [0x11; 4];
        const VALUE: [u8; 6] = [0x22; 6];
        const CONTROL_KEY: &[u8] = b"ctl";
        let refused_at = |outcome: Result<(), RocksDbWalletError>, limit: u64, required: u64| {
            assert!(
                matches!(
                    outcome,
                    Err(RocksDbWalletError::TransitionLogicalByteLimit {
                        limit_bytes,
                        required_bytes,
                    }) if limit_bytes == limit && required_bytes == required
                ),
                "{outcome:?}"
            );
        };

        // A delete that a put then replaces leaves the key and value, twice,
        // and the control record once.
        let delete_then_put = |limit| {
            let mut accounting = TransitionLogicalByteAccounting::new(limit);
            accounting.reserve_delete(&KEY, None)?;
            accounting.reserve_put(&KEY, &VALUE, Some(&None))?;
            accounting.reserve_control(CONTROL_KEY, &[0x33; 5])
        };
        let required = 2 * (4 + 6) + (3 + 5);
        delete_then_put(required)?;
        refused_at(delete_then_put(required - 1), required - 1, required);

        // A put that a delete then replaces leaves only the key, twice, so the
        // room the put needed is free again for the control record.
        let put_then_delete = |control: &[u8]| {
            let mut accounting = TransitionLogicalByteAccounting::new(2 * (4 + 6));
            accounting.reserve_put(&KEY, &VALUE, None)?;
            accounting.reserve_delete(&KEY, Some(&Some(VALUE.to_vec())))?;
            accounting.reserve_control(CONTROL_KEY, control)
        };
        put_then_delete(&[0x33; 2 * 6 - 3])?;
        refused_at(
            put_then_delete(&[0x33; 2 * 6 - 2]),
            2 * (4 + 6),
            2 * 4 + 2 * 6 + 1,
        );
        Ok(())
    }

    #[test]
    fn coalesced_batch_holds_one_record_per_planned_row() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        publish_checkpointed_wallet(temporary.path())?;
        let wallet = open_wallet_at_depth(temporary.path(), 1)?;
        let outpoint_key = [0x31; 36];
        let address_key = [0x32; 32];
        let mut planner =
            WalletTransitionPlanner::new(&wallet, MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES);
        planner.put_raw(
            TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
            &outpoint_key,
            vec![1; 8],
        )?;
        planner.put_raw(
            TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
            &outpoint_key,
            vec![2; 9],
        )?;
        planner.delete_raw(TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY, &outpoint_key)?;
        planner.put_raw(
            TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
            &address_key,
            vec![3; 8],
        )?;
        planner.delete_raw(TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY, &address_key)?;
        let planned_rows = planner
            .into_publication(wallet.ready_evidence().source_position)
            .rows;
        assert_eq!(planned_rows.len(), 3);

        let control = wallet.control.encode()?;
        let capacity =
            coalesced_write_batch_capacity(&planned_rows, WALLET_STORE_CONTROL_KEY, &control);
        let batch = coalesced_write_batch(
            &wallet.bounded_open,
            planned_rows,
            WALLET_STORE_CONTROL_KEY,
            &control,
        )?;
        assert_eq!(batch.len(), 3 + 1);
        assert!(
            batch.size_in_bytes() <= capacity,
            "batch of {} bytes exceeds its reserved {capacity}",
            batch.size_in_bytes()
        );
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one proof keeps the derived row set, the refused and admitted limits, and the cold comparison together"
    )]
    fn hot_address_reorg_publishes_each_touched_row_once_within_twice_its_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        for replacement in [
            HotAddressBranch::SameTransactions,
            HotAddressBranch::DistinctCoinbases,
        ] {
            let mut fixture = HotAddressReorg::publish(replacement)?;
            let cold = build_wallet_from_canonical(
                &fixture.secondary,
                fixture.temporary.path().join("wallet-cold"),
                RocksDbWalletBuildOptions {
                    supported_reorg_depth: HOT_ADDRESS_BLOCKS,
                    ..RocksDbWalletBuildOptions::for_local_tests()
                },
            )?;
            let before = wallet_rows(&fixture.wallet)?;
            let after = wallet_rows(&cold.store)?;
            assert_ne!(before, after, "{replacement:?}");

            // The reorg rolls back every row of the followed wallet and
            // replays every row of the cold build, so its plan holds the final
            // entry of each of their keys and of each unspent and address-index
            // row it restores or creates for an output that it then spends.
            let touched = rewritten_row_keys(&before, &after)?;
            let final_entry_bytes: usize = touched
                .iter()
                .map(|key| key.1.len() + after.get(key).map_or(0, Vec::len))
                .sum();
            let next_control = WalletStoreControlRecord {
                writer_generation: fixture.wallet.control.writer_generation + 1,
                build_state: WalletProjectionBuildState::Ready(cold.store.ready_evidence().clone()),
                ..fixture.wallet.control.clone()
            }
            .encode()?;
            let required = u64::try_from(
                2 * final_entry_bytes + WALLET_STORE_CONTROL_KEY.len() + next_control.len(),
            )?;

            let control_before = decode_only_control(&fixture.wallet.bounded_open)?;
            let evidence_before = fixture.wallet.ready_evidence().clone();
            let footprint_before = fixture.wallet.last_transition_footprint();
            let refused = fixture.apply_reorg(required - 1)?;
            assert!(
                matches!(
                    refused,
                    Err(RocksDbWalletError::TransitionLogicalByteLimit {
                        limit_bytes,
                        required_bytes,
                    }) if limit_bytes == required - 1 && required_bytes == required
                ),
                "{replacement:?}: {refused:?}"
            );
            assert_eq!(fixture.wallet.ready_evidence(), &evidence_before);
            assert_eq!(
                decode_only_control(&fixture.wallet.bounded_open)?,
                control_before
            );
            assert_eq!(wallet_rows(&fixture.wallet)?, before);
            assert_eq!(fixture.wallet.last_transition_footprint(), footprint_before);

            let keys_written_before = wallet_keys_written(&fixture.wallet);
            let keys_read_before = wallet_keys_read(&fixture.wallet);
            fixture.apply_reorg(required)??;
            assert_eq!(
                wallet_keys_written(&fixture.wallet) - keys_written_before,
                u64::try_from(touched.len() + 1)?,
                "{replacement:?}"
            );
            let footprint = fixture
                .wallet
                .last_transition_footprint()
                .ok_or("a published reorg records its footprint")?;
            assert_eq!(footprint.accounted_logical_bytes, required);
            assert_eq!(footprint.planned_rows, u64::try_from(touched.len())?);
            assert_eq!(footprint.batch_records, footprint.planned_rows + 1);
            assert!(footprint.batch_bytes > u64::try_from(final_entry_bytes)?);
            assert_eq!(
                footprint.durable_point_reads,
                wallet_keys_read(&fixture.wallet) - keys_read_before
            );
            assert_eq!(wallet_rows(&fixture.wallet)?, after, "{replacement:?}");
            assert_eq!(
                fixture.wallet.ready_evidence().projection_digest,
                cold.store.ready_evidence().projection_digest
            );
            assert_eq!(
                fixture.wallet.ready_evidence().row_counts,
                cold.store.ready_evidence().row_counts
            );
            assert_eq!(fixture.wallet.ready_evidence(), cold.store.ready_evidence());
            assert_eq!(
                decode_only_control(&fixture.wallet.bounded_open)?.encode()?,
                next_control
            );
        }
        Ok(())
    }

    /// Blocks above the checkpoint that the hot-address chain holds and its
    /// reorg replaces.
    const HOT_ADDRESS_BLOCKS: u32 = 3;

    /// Outputs each hot-address coinbase pays to the hot address.
    const HOT_ADDRESS_OUTPUTS_PER_BLOCK: u32 = 4;

    /// Hot-address outputs each hot-address block spends.
    const HOT_ADDRESS_SPENDS_PER_BLOCK: u32 = 2;

    /// Which chain a hot-address block belongs to.
    #[derive(Clone, Copy, Debug)]
    enum HotAddressBranch {
        /// The blocks the wallet follows before the reorg.
        Original,
        /// Replacements that keep every transaction under new block hashes.
        SameTransactions,
        /// Replacements whose coinbases, and so every outpoint, differ.
        DistinctCoinbases,
    }

    impl HotAddressBranch {
        const fn block_tag(self) -> u8 {
            match self {
                Self::Original => 0xd0,
                Self::SameTransactions | Self::DistinctCoinbases => 0xd1,
            }
        }

        const fn transaction_tag(self) -> u8 {
            match self {
                Self::Original | Self::SameTransactions => 0xe0,
                Self::DistinctCoinbases => 0xe1,
            }
        }
    }

    /// A wallet that followed [`HOT_ADDRESS_BLOCKS`] blocks paying and
    /// spending one address, whose canonical store then replaced all of them.
    struct HotAddressReorg {
        secondary: RocksDbCanonicalSecondary,
        wallet: RocksDbWalletStore,
        settled_tip: BlockId,
        temporary: TempDir,
    }

    impl HotAddressReorg {
        fn publish(replacement: HotAddressBranch) -> Result<Self, Box<dyn std::error::Error>> {
            let temporary = TempDir::new()?;
            let published = publish_checkpointed_wallet(temporary.path())?;
            let settled_tip = published.initial_fence.visible_tip();
            let base_accumulator =
                checkpoint_accumulator(&published.activations, published.initial_fence)?;
            let mut accumulator = base_accumulator.clone();
            let mut canonical_store = published.canonical_store;
            let mut fence = published.initial_fence;
            for height in hot_address_heights() {
                let block =
                    hot_address_block(&mut accumulator, height, HotAddressBranch::Original)?;
                (canonical_store, fence) = canonical_store.commit_live_append(
                    CanonicalLiveAppend::new(
                        fence,
                        block,
                        Vec::new(),
                        settled_tip,
                        UnixTimestampMillis::new(1_750_000_300_000 + u64::from(height)),
                    ),
                    &published.activations,
                )?;
            }
            let mut secondary =
                open_checkpointed_secondary(temporary.path(), "canonical-secondary")?;
            let mut wallet = open_wallet_at_depth(temporary.path(), HOT_ADDRESS_BLOCKS)?;
            for _ in hot_address_heights() {
                apply_next_event_settling_at(&mut wallet, &secondary, settled_tip)?;
            }

            let mut accumulator = base_accumulator;
            let mut replacement_blocks = Vec::new();
            for height in hot_address_heights() {
                replacement_blocks.push(CanonicalReplacementBlock::new(
                    hot_address_block(&mut accumulator, height, replacement)?,
                    Vec::new(),
                ));
            }
            canonical_store.commit_live_replacement(
                CanonicalLiveReplacement::new(
                    fence,
                    replacement_blocks,
                    UnixTimestampMillis::new(1_750_000_400_000),
                ),
                &published.activations,
            )?;
            secondary.try_catch_up()?;
            Ok(Self {
                secondary,
                wallet,
                settled_tip,
                temporary,
            })
        }

        /// Applies the retained replacement event under `max_logical_bytes`,
        /// returning the transition's own outcome.
        fn apply_reorg(
            &mut self,
            max_logical_bytes: u64,
        ) -> Result<Result<(), RocksDbWalletError>, Box<dyn std::error::Error>> {
            let event = next_retained_event(&self.secondary, &self.wallet)?;
            if event.reverted_range().is_none() {
                return Err(format!("expected the replacement event, found {event:?}").into());
            }
            let source =
                WalletCanonicalSourceIdentity::from_ready_evidence(self.wallet.ready_evidence());
            let limit =
                NonZeroU64::new(max_logical_bytes).ok_or("transition limit must be nonzero")?;
            let replay_rows = self
                .secondary
                .scan_canonical_replay_range(event.committed_range())?;
            Ok(self.wallet.apply_canonical_event_range(
                source,
                event,
                event.resulting_fence(),
                self.settled_tip,
                limit,
                replay_rows,
            ))
        }
    }

    fn hot_address_heights() -> std::ops::RangeInclusive<u32> {
        101..=100 + HOT_ADDRESS_BLOCKS
    }

    /// Builds the hot-address block at `height` on `branch`, advancing
    /// `accumulator`.
    ///
    /// Its coinbase pays [`HOT_ADDRESS_OUTPUTS_PER_BLOCK`] outputs to the hot
    /// address, and a second transaction spends
    /// [`HOT_ADDRESS_SPENDS_PER_BLOCK`] of them and returns change to it. The
    /// first block spends outputs of its own coinbase, and each later block
    /// spends the outputs that the previous coinbase left unspent.
    fn hot_address_block(
        accumulator: &mut CommitmentTreeAccumulator,
        height: u32,
        branch: HotAddressBranch,
    ) -> Result<CanonicalBuildBlock, Box<dyn std::error::Error>> {
        let address = TransparentAddressScriptHash::from_bytes([0xd7; 32]);
        let coinbase_id = hot_address_transaction_id(height, branch, false);
        let coinbase = coinbase_output_transaction(
            coinbase_id,
            (0..HOT_ADDRESS_OUTPUTS_PER_BLOCK)
                .map(|index| {
                    TransparentOutputFact::new(index, 100 + u64::from(index), [0x76, 0xd7], address)
                })
                .collect(),
        );
        let (funding_id, first_spent_index) = if height == 101 {
            (coinbase_id, 0)
        } else {
            (
                hot_address_transaction_id(height - 1, branch, false),
                HOT_ADDRESS_OUTPUTS_PER_BLOCK - HOT_ADDRESS_SPENDS_PER_BLOCK,
            )
        };
        let spending = transparent_spend_transaction(
            hot_address_transaction_id(height, branch, true),
            (0..HOT_ADDRESS_SPENDS_PER_BLOCK)
                .map(|input| {
                    TransparentInputFact::new(
                        input,
                        TransparentOutPoint::new(funding_id, first_spent_index + input),
                    )
                })
                .collect(),
            vec![TransparentOutputFact::new(0, 1, [0x76, 0xd7], address)],
        );
        checkpointed_append_block(
            accumulator,
            height,
            hot_address_block_hash(height - 1, branch),
            hot_address_block_hash(height, branch),
            vec![coinbase, spending],
        )
    }

    fn hot_address_block_hash(height: u32, branch: HotAddressBranch) -> [u8; 32] {
        if height == 100 {
            return [0xa0; 32];
        }
        let mut hash = [branch.block_tag(); 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }

    fn hot_address_transaction_id(
        height: u32,
        branch: HotAddressBranch,
        spending: bool,
    ) -> TransactionId {
        let mut id = [branch.transaction_tag(); 32];
        id[..4].copy_from_slice(&height.to_be_bytes());
        id[4] = u8::from(spending);
        TransactionId::from_bytes(id)
    }

    /// Every row of a wallet's data column families, by family and key.
    type WalletRows = BTreeMap<OverlayKey, Vec<u8>>;

    fn wallet_rows(wallet: &RocksDbWalletStore) -> Result<WalletRows, Box<dyn std::error::Error>> {
        let mut rows = BTreeMap::new();
        for (family_name, _) in WALLET_DATA_COLUMN_FAMILIES {
            let family = column_family(&wallet.bounded_open, family_name)?;
            for row in wallet
                .bounded_open
                .db
                .iterator_cf(&family, IteratorMode::Start)
            {
                let (key, value) = row?;
                rows.insert((family_name, key.to_vec()), value.to_vec());
            }
        }
        Ok(rows)
    }

    /// Keys of every row that a transition from `before` to `after` touches
    /// when it rolls back every block of `before` and replays every block of
    /// `after`.
    ///
    /// Besides the rows of either state, the rollback restores and then
    /// removes the unspent-output and address-index rows of each output that
    /// `before` holds as spent by a later block, and the replay creates and
    /// then spends those rows of each output that `after` holds as spent.
    fn rewritten_row_keys(
        before: &WalletRows,
        after: &WalletRows,
    ) -> Result<BTreeSet<OverlayKey>, Box<dyn std::error::Error>> {
        let mut keys: BTreeSet<_> = before.keys().chain(after.keys()).cloned().collect();
        let spent_rows = before
            .iter()
            .map(|row| (row, false))
            .chain(after.iter().map(|row| (row, true)));
        for (((family_name, key), encoded), replayed) in spent_rows {
            if *family_name != TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY {
                continue;
            }
            let spent = WalletSpentOutput::decode_value(WalletOutpointKey::decode(key)?, encoded)?;
            if !replayed && spent.spent_at.block == spent.output.created_at.block {
                continue;
            }
            keys.insert((TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY, key.clone()));
            keys.insert((
                TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
                WalletAddressUnspentOutputKey::new(&spent.output)
                    .as_bytes()
                    .to_vec(),
            ));
        }
        Ok(keys)
    }

    fn wallet_keys_written(wallet: &RocksDbWalletStore) -> u64 {
        wallet
            .bounded_open
            .statistics
            .get_ticker_count(Ticker::NumberKeysWritten)
    }
}
