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
};

use rust_rocksdb::{WriteBatch, WriteOptions};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
    CanonicalBlockFactsSequenceDigest, CanonicalBlockFactsSequenceDigestBuilder,
    CanonicalBlockReplayFormatVersion, Network, TransparentAddressScriptHash, TransparentOutPoint,
    ValidatedCanonicalBlockReplay, wire::UtxoSetCommitmentElement,
};
use zinder_store::{
    CanonicalEventFence, CanonicalEventKind, CanonicalRetainedEvent, CanonicalStoreError,
    MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS,
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
/// Each caller must select a smaller nonzero ceiling. The planner accounts
/// duplicated key/value payloads held by its `WriteBatch` and overlay before
/// the synchronous publication point.
pub const MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES: u64 = 1024 * 1024 * 1024;

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
        let (batch, ready_evidence, logical_bytes) = planner.into_publication(source_position);
        self.publish_planned_transition(
            batch,
            ready_evidence,
            logical_bytes,
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
        let (batch, ready_evidence, logical_bytes) = planner.into_publication(source_position);
        self.publish_planned_transition(
            batch,
            ready_evidence,
            logical_bytes,
            cancelled_before_write,
        )
    }

    fn publish_planned_transition<Cancel>(
        &mut self,
        mut batch: WriteBatch,
        ready_evidence: WalletProjectionReadyEvidence,
        mut logical_bytes: TransitionLogicalByteAccounting,
        cancelled_before_write: Cancel,
    ) -> Result<(), RocksDbWalletError>
    where
        Cancel: FnOnce() -> bool,
    {
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

        // Re-admit immediately before publication so a sequential second
        // handle cannot overwrite a newer READY control it did not observe.
        self.require_current_ready_control()?;
        if cancelled_before_write() {
            return Err(RocksDbWalletError::ProjectionTransitionCancelled);
        }
        batch.put(
            zinder_wallet_projection::WALLET_STORE_CONTROL_KEY,
            encoded_control,
        );
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        // A failed or unconfirmed write leaves no verified suffix, so the next
        // transition reads the whole suffix again.
        self.verified_undo_suffix_generation = None;
        crash_point("wallet_projection.transition.before_atomic_write");
        self.bounded_open
            .db
            .write_opt(&batch, &write_options)
            .map_err(|source| {
                RocksDbWalletError::rocksdb("atomic wallet projection transition", source)
            })?;
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

#[derive(Debug)]
struct TransitionLogicalByteAccounting {
    limit: u64,
    batch: u64,
    overlay: u64,
}

impl TransitionLogicalByteAccounting {
    const fn new(limit: u64) -> Self {
        Self {
            limit,
            batch: 0,
            overlay: 0,
        }
    }

    fn reserve_put(
        &mut self,
        key: &[u8],
        encoded_value: &[u8],
        previous_overlay: Option<&Option<Vec<u8>>>,
    ) -> Result<(), RocksDbWalletError> {
        let entry_bytes = logical_entry_bytes(key, Some(encoded_value))?;
        self.reserve_batch_and_replace_overlay(
            key,
            entry_bytes,
            previous_overlay,
            Some(encoded_value),
        )
    }

    fn reserve_delete(
        &mut self,
        key: &[u8],
        previous_overlay: Option<&Option<Vec<u8>>>,
    ) -> Result<(), RocksDbWalletError> {
        let bytes = logical_entry_bytes(key, None)?;
        self.reserve_batch_and_replace_overlay(key, bytes, previous_overlay, None)
    }

    fn reserve_control(
        &mut self,
        key: &[u8],
        encoded_control: &[u8],
    ) -> Result<(), RocksDbWalletError> {
        let batch_entry_bytes = logical_entry_bytes(key, Some(encoded_control))?;
        let next_batch_bytes = checked_transition_bytes(self.batch, batch_entry_bytes)?;
        self.ensure_within_limit(next_batch_bytes, self.overlay)?;
        self.batch = next_batch_bytes;
        Ok(())
    }

    fn reserve_batch_and_replace_overlay(
        &mut self,
        key: &[u8],
        batch_entry_bytes: u64,
        previous_overlay: Option<&Option<Vec<u8>>>,
        next_overlay_value: Option<&[u8]>,
    ) -> Result<(), RocksDbWalletError> {
        let previous_overlay_bytes = match previous_overlay {
            Some(previous_value) => logical_entry_bytes(key, previous_value.as_deref())?,
            None => 0,
        };
        let next_overlay_entry_bytes = logical_entry_bytes(key, next_overlay_value)?;
        let next_batch_bytes = checked_transition_bytes(self.batch, batch_entry_bytes)?;
        let next_overlay_bytes = self
            .overlay
            .checked_sub(previous_overlay_bytes)
            .ok_or(RocksDbWalletError::TransitionLogicalByteAccountingOverflow)?;
        let next_overlay_bytes =
            checked_transition_bytes(next_overlay_bytes, next_overlay_entry_bytes)?;
        self.ensure_within_limit(next_batch_bytes, next_overlay_bytes)?;
        self.batch = next_batch_bytes;
        self.overlay = next_overlay_bytes;
        Ok(())
    }

    fn ensure_within_limit(
        &self,
        batch_bytes: u64,
        overlay_bytes: u64,
    ) -> Result<(), RocksDbWalletError> {
        let required_bytes = checked_transition_bytes(batch_bytes, overlay_bytes)?;
        if required_bytes > self.limit {
            return Err(RocksDbWalletError::TransitionLogicalByteLimit {
                limit_bytes: self.limit,
                required_bytes,
            });
        }
        Ok(())
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

struct WalletTransitionPlanner<'store> {
    store: &'store RocksDbWalletStore,
    batch: WriteBatch,
    overlay: BTreeMap<OverlayKey, Option<Vec<u8>>>,
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
            batch: WriteBatch::default(),
            overlay: BTreeMap::new(),
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

    fn into_publication(
        self,
        source_position: WalletProjectionSourcePosition,
    ) -> (
        WriteBatch,
        WalletProjectionReadyEvidence,
        TransitionLogicalByteAccounting,
    ) {
        let (projection_accumulator, projection_digest) =
            self.accumulator.finish_with_accumulator();
        (
            self.batch,
            WalletProjectionReadyEvidence {
                source_position,
                source_sequence_digest: self.source_sequence_digest,
                settled_tip: self.settled_tip,
                projection_digest,
                projection_accumulator,
                row_counts: self.accumulator_row_counts,
                utxo_summary: self.utxo_summary,
            },
            self.logical_bytes,
        )
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
        &self,
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
        &self,
        key: WalletOutpointKey,
    ) -> Result<Option<WalletUnspentOutput>, RocksDbWalletError> {
        self.raw(TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY, key.as_bytes())?
            .map(|encoded| WalletUnspentOutput::decode_value(key, &encoded).map_err(Into::into))
            .transpose()
    }

    fn read_spent(
        &self,
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
        &self,
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
        &self,
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
        self.store
            .bounded_open
            .db
            .get_pinned_cf(&column_family, key)
            .map_err(|source| RocksDbWalletError::rocksdb("wallet transition point read", source))?
            .map(|encoded| decode_undo(&key, &encoded))
            .transpose()
    }

    fn validate_add_unspent(&self, output: &WalletUnspentOutput) -> Result<(), RocksDbWalletError> {
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
        &self,
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
        &self,
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
        &self,
        column_family_name: &'static str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, RocksDbWalletError> {
        let overlay_key = (column_family_name, key.to_vec());
        let encoded = if let Some(overlay_value) = self.overlay.get(&overlay_key) {
            overlay_value.clone()
        } else {
            let column_family = column_family(&self.store.bounded_open, column_family_name)?;
            count_undo_row_work(column_family_name, UndoRowWork::DurableRead);
            self.store
                .bounded_open
                .db
                .get_cf(&column_family, key)
                .map_err(|source| {
                    RocksDbWalletError::rocksdb("wallet transition point read", source)
                })?
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
        let column_family = column_family(&self.store.bounded_open, column_family_name)?;
        let overlay_key = (column_family_name, key.to_vec());
        self.logical_bytes
            .reserve_put(key, &encoded_value, self.overlay.get(&overlay_key))?;
        self.batch.put_cf(&column_family, key, &encoded_value);
        self.overlay.insert(overlay_key, Some(encoded_value));
        Ok(())
    }

    fn delete_raw(
        &mut self,
        column_family_name: &'static str,
        key: &[u8],
    ) -> Result<(), RocksDbWalletError> {
        let column_family = column_family(&self.store.bounded_open, column_family_name)?;
        let overlay_key = (column_family_name, key.to_vec());
        self.logical_bytes
            .reserve_delete(key, self.overlay.get(&overlay_key))?;
        self.batch.delete_cf(&column_family, key);
        self.overlay.insert(overlay_key, None);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU32, NonZeroU64},
        path::Path,
    };

    #[cfg(unix)]
    use std::num::NonZeroU16;

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
        TransactionVersion, TransparentInputFact, TransparentOutPoint,
        TransparentUtxoSetCommitment, UnixTimestampMillis, UnsupportedSection,
        encode_canonical_block_replay,
    };
    #[cfg(unix)]
    use zinder_core::{TransparentAddressScriptHash, TransparentOutputFact};
    use zinder_store::{
        CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalEventFence,
        CanonicalEventHistoryRequest, CanonicalLiveAppend, CanonicalLiveReplacement,
        CanonicalReorgPolicy, CanonicalReplacementBlock, CanonicalRetainedEvent,
        CanonicalStoreBuildPlan, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder,
        RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget,
    };
    use zinder_testkit::process_crash;
    #[cfg(unix)]
    use zinder_wallet_projection::WalletOutpointKey;
    use zinder_wallet_projection::{
        WalletCanonicalSourceIdentity, WalletProjectionBuildLeaseRequest,
        WalletProjectionBuildOwner, WalletProjectionContractError, WalletProjectionDigestBuilder,
        WalletProjectionReadyEvidence, WalletProjectionRetainedEventAnchor,
        WalletProjectionSourcePosition, WalletReorgUndo, WalletUtxoSetSummary,
    };

    use super::{
        UndoRowWorkCounts, extend_source_sequence_digest, source_identity_from_fence,
        take_undo_row_work,
    };
    use crate::{
        RocksDbWalletBuildOptions, RocksDbWalletBuildStore, RocksDbWalletError,
        RocksDbWalletFollowingStore, RocksDbWalletStore, build_wallet_from_canonical,
        store::{
            REORG_UNDO_COLUMN_FAMILY, RocksDbWalletBuilder, WalletColdValidationConfig,
            column_family,
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
                    TransparentOutputFact::new(0, CRASH_OUTPUT_VALUE_ZAT, [0xc9], address),
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
            assert!(undo.spent_outpoints.is_empty());
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
        canonical_builder.bulk_load_blocks([Ok::<_, std::io::Error>(checkpointed_build_block(
            100, [0x99; 32], [0xa0; 32], true,
        ))])?;
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

    #[cfg(unix)]
    fn coinbase_output_transaction(
        transaction_id: TransactionId,
        output: TransparentOutputFact,
    ) -> CanonicalTransactionFacts {
        let mut public_facts = transparent_spend_public_facts(transaction_id);
        public_facts.version = TransactionVersion::V4;
        public_facts.is_coinbase = true;
        public_facts.unsupported_sections = Vec::new();
        public_facts.counts.transparent_output_count = 1;
        CanonicalTransactionFacts {
            public_facts,
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[0x66]),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs: vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            transparent_outputs: vec![output],
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

    /// Settlement lag of the coinbase-only chains below, and the reorg depth
    /// their wallets support.
    const COINBASE_ONLY_DEPTH: u32 = 4;

    /// Visible tip a [`FollowedCoinbaseOnlyChain`] wallet reaches on one handle.
    const FOLLOWED_TIP: u32 = 106;

    /// A retained undo row that the next append neither prunes nor binds to
    /// its settled tip, and that replacing the tip does not roll back.
    const DAMAGED_UNDO_HEIGHT: u32 = 105;

    #[test]
    fn appends_on_a_verified_suffix_read_only_the_undo_rows_they_change()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let depth = u64::from(COINBASE_ONLY_DEPTH);
        let window_full = 100 + COINBASE_ONLY_DEPTH;
        let reopened_at = 100 + 2 * COINBASE_ONLY_DEPTH + 1;
        let through = 100 + 3 * COINBASE_ONLY_DEPTH;
        let _chain = CoinbaseOnlyChain::publish(temporary.path(), through)?;
        let secondary = open_checkpointed_secondary(temporary.path(), "canonical-secondary")?;
        let mut wallet = open_coinbase_only_wallet(temporary.path())?;
        let work = |durable_reads, value_copies, full_decodes| UndoRowWorkCounts {
            durable_reads,
            value_copies,
            full_decodes,
        };
        let mut observed = Vec::new();
        let mut expected = Vec::new();
        for height in 101..=through {
            if height == reopened_at {
                drop(wallet);
                wallet = open_coinbase_only_wallet(temporary.path())?;
            }
            take_undo_row_work();
            apply_next_coinbase_only_event(&mut wallet, &secondary)?;
            observed.push((height, take_undo_row_work()));
            // Every append reads its new undo key once to prove it absent.
            // The first transition on a handle then decodes the whole suffix
            // in place; later appends read and decode only the successor row
            // that binds the new settled tip and the one row they prune.
            let expected_work = if height == 101 {
                work(1, 0, 1)
            } else if height <= window_full {
                work(1, 0, 0)
            } else if height == reopened_at {
                work(depth + 2, 1, depth + 2)
            } else {
                work(3, 1, 2)
            };
            expected.push((height, expected_work));
        }
        assert_eq!(observed, expected);

        let cold = build_wallet_from_canonical(
            &secondary,
            temporary.path().join("wallet-cold"),
            RocksDbWalletBuildOptions {
                supported_reorg_depth: COINBASE_ONLY_DEPTH,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        assert_eq!(wallet.ready_evidence(), cold.store.ready_evidence());
        Ok(())
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
    fn appends_on_a_verified_handle_reject_a_disconnected_undo_row_before_pruning_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let damage = UndoRowDamage::Disconnected;
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
        let outcome = apply_next_coinbase_only_event(&mut fixture.wallet, &fixture.secondary);
        assert_damage_reported(damage, outcome)?;
        assert_eq!(fixture.wallet.ready_evidence(), &before);
        assert!(
            before.source_position.tip.height
                < BlockHeight::new(DAMAGED_UNDO_HEIGHT + COINBASE_ONLY_DEPTH)
        );
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
    /// outputs, and the store settles [`COINBASE_ONLY_DEPTH`] blocks below its
    /// tip.
    struct CoinbaseOnlyChain {
        activations: NetworkUpgradeActivations,
        canonical_store: RocksDbCanonicalStore,
        fence: CanonicalEventFence,
        accumulator: CommitmentTreeAccumulator,
        accumulator_below_tip: CommitmentTreeAccumulator,
    }

    impl CoinbaseOnlyChain {
        /// Publishes the height-100 checkpoint and its READY wallet under
        /// `root`, then appends coinbase-only blocks through height `through`.
        fn publish(root: &Path, through: u32) -> Result<Self, Box<dyn std::error::Error>> {
            let published = publish_checkpointed_wallet(root)?;
            let accumulator =
                checkpoint_accumulator(&published.activations, published.initial_fence)?;
            Self {
                activations: published.activations,
                canonical_store: published.canonical_store,
                fence: published.initial_fence,
                accumulator_below_tip: accumulator.clone(),
                accumulator,
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
                        coinbase_only_settled_tip(BlockHeight::new(height)),
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
        let height = visible_height
            .value()
            .saturating_sub(COINBASE_ONLY_DEPTH)
            .max(100);
        BlockId::new(
            BlockHeight::new(height),
            BlockHash::from_bytes(coinbase_only_hash(height)),
        )
    }

    /// Opens the wallet under `root` at the coinbase-only chain's reorg depth.
    fn open_coinbase_only_wallet(
        root: &Path,
    ) -> Result<RocksDbWalletStore, Box<dyn std::error::Error>> {
        let mut following = RocksDbWalletStore::open_ready_for_following(
            root.join("wallet"),
            Network::ZcashRegtest,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        following.raise_supported_reorg_depth(COINBASE_ONLY_DEPTH)?;
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
        let event = next_retained_event(secondary, wallet)?;
        let fence = event.resulting_fence();
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        wallet.apply_canonical_event_range(
            source,
            event,
            fence,
            coinbase_only_settled_tip(fence.visible_tip().height),
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
}
