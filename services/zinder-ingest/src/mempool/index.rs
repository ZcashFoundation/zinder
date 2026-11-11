//! Live in-memory mempool index owned by `zinder-ingest`.
//!
//! Stores hydrated [`MempoolEntry`] records and the secondary indexes
//! needed by the wallet data plane: transparent outputs by address and
//! transparent spends by outpoint. Mempool state is non-canonical and is
//! never written by a canonical block commit; the ingest layer holds it
//! exclusively and exposes read methods to the colocated `IngestControl`
//! gRPC handlers.
//!
//! The index is concurrency-safe; readers (snapshot, address-lookup,
//! presence checks) take a read lock, writers ([`MempoolIndex::apply_added`],
//! [`MempoolIndex::apply_invalidated`], [`MempoolIndex::apply_mined`]) take
//! a write lock.

use std::collections::{BTreeMap, HashMap, HashSet, btree_map::Entry};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;
use zinder_core::{
    MempoolEntry, TransactionId, TransparentAddressScriptHash, TransparentMempoolOutput,
    TransparentMempoolSpend, TransparentOutPoint, TransparentOutput, TransparentOutputEntry,
};
use zinder_store::{MempoolEvent, MempoolEventPosition};

/// Outcome of applying a single source-observed event to the index.
///
/// Surfaced to the caller so that ingest can decide whether to write a
/// canonical [`zinder_store::MempoolEvent`] envelope (only on a real state
/// change) or treat the source observation as a duplicate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MempoolApplyOutcome {
    /// State changed: an entry was inserted, removed, or replaced.
    Applied,
    /// Source observation was a no-op (e.g. duplicate `Added` for an
    /// already-known txid, or `Invalidated`/`Mined` for an unknown txid).
    NoChange,
}

/// Result of validating an index transition before durable event append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MempoolIndexPreflight {
    /// The matching in-memory mutation is guaranteed to change the index.
    Apply,
    /// The observation is already represented or names no live entry.
    NoChange,
}

/// A source event cannot safely mutate the live transparent overlays.
#[derive(Debug, Error)]
pub(crate) enum MempoolIndexInvariantError {
    /// A new entry would overwrite an output currently owned by another mempool transaction.
    #[error("mempool output {outpoint:?} conflicts with an existing live entry")]
    OutputCollision {
        /// Output that would be overwritten.
        outpoint: TransparentOutPoint,
    },
    /// A new entry would overwrite a spender currently indexed for one outpoint.
    #[error("mempool spend {outpoint:?} conflicts with an existing live entry")]
    SpendCollision {
        /// Outpoint with multiple live spenders.
        outpoint: TransparentOutPoint,
    },
    /// One source entry repeated an output outpoint internally.
    #[error("mempool entry repeats output {outpoint:?}")]
    DuplicateOutput {
        /// Repeated output outpoint.
        outpoint: TransparentOutPoint,
    },
    /// One source entry repeated a spent outpoint internally.
    #[error("mempool entry repeats spend {outpoint:?}")]
    DuplicateSpend {
        /// Repeated spent outpoint.
        outpoint: TransparentOutPoint,
    },
    /// An unrecognized source-event variant has no verified index transition.
    #[error("mempool event variant is unsupported")]
    UnsupportedEvent,
}

/// Deterministic page of live mempool entries.
#[derive(Clone, Debug)]
pub struct MempoolSnapshotPage {
    /// Entries in transaction-id order.
    pub entries: Vec<Arc<MempoolEntry>>,
    /// Last transaction id included in this page when more entries remain.
    pub next_after_transaction_id: Option<TransactionId>,
    /// Log position of the last event applied to the index, read atomically
    /// with the page entries. `None` before any event has been applied.
    pub last_applied_event: Option<MempoolEventPosition>,
}

/// Concurrent live mempool index.
#[derive(Clone, Debug, Default)]
pub struct MempoolIndex {
    state: Arc<RwLock<MempoolIndexState>>,
}

#[derive(Debug, Default)]
struct MempoolIndexState {
    entries: BTreeMap<TransactionId, Arc<MempoolEntry>>,
    outputs_by_address: HashMap<
        TransparentAddressScriptHash,
        HashMap<TransparentOutPoint, TransparentMempoolOutput>,
    >,
    output_by_outpoint: HashMap<TransparentOutPoint, TransparentMempoolOutput>,
    spend_by_outpoint: HashMap<TransparentOutPoint, TransactionId>,
    last_applied_event: Option<MempoolEventPosition>,
}

impl MempoolIndex {
    /// Creates an empty mempool index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates a source transition before it is appended to the durable log.
    ///
    /// A caller that receives [`MempoolIndexPreflight::Apply`] can append the
    /// matching event, then invoke the corresponding `apply_*` method without
    /// risking a duplicate, missing entry, or transparent-overlay overwrite.
    pub(crate) fn preflight_event(
        &self,
        event: &MempoolEvent,
    ) -> Result<MempoolIndexPreflight, MempoolIndexInvariantError> {
        let state = self.state.read();
        match event {
            MempoolEvent::Added { entry } => {
                if state.entries.contains_key(&entry.transaction_id()) {
                    return Ok(MempoolIndexPreflight::NoChange);
                }
                let mut output_outpoints = HashSet::new();
                for output in entry.transparent_outputs() {
                    if !output_outpoints.insert(output.outpoint) {
                        return Err(MempoolIndexInvariantError::DuplicateOutput {
                            outpoint: output.outpoint,
                        });
                    }
                    if state.output_by_outpoint.contains_key(&output.outpoint) {
                        return Err(MempoolIndexInvariantError::OutputCollision {
                            outpoint: output.outpoint,
                        });
                    }
                }
                let mut spent_outpoints = HashSet::new();
                for spend in entry.transparent_spends() {
                    if !spent_outpoints.insert(spend.spent_outpoint) {
                        return Err(MempoolIndexInvariantError::DuplicateSpend {
                            outpoint: spend.spent_outpoint,
                        });
                    }
                    if state.spend_by_outpoint.contains_key(&spend.spent_outpoint) {
                        return Err(MempoolIndexInvariantError::SpendCollision {
                            outpoint: spend.spent_outpoint,
                        });
                    }
                }
                Ok(MempoolIndexPreflight::Apply)
            }
            MempoolEvent::Invalidated { transaction_id, .. }
            | MempoolEvent::Mined { transaction_id, .. } => {
                Ok(if state.entries.contains_key(transaction_id) {
                    MempoolIndexPreflight::Apply
                } else {
                    MempoolIndexPreflight::NoChange
                })
            }
            _ => Err(MempoolIndexInvariantError::UnsupportedEvent),
        }
    }

    /// Clears the process-local state before a source reseed after an
    /// unexpected post-append divergence.
    pub(crate) fn reset(&self) {
        *self.state.write() = MempoolIndexState::default();
    }

    /// Inserts a hydrated entry, recording `applied_event` as the index's
    /// last-applied log position under the same write lock. Returns
    /// [`MempoolApplyOutcome::NoChange`] when the entry's txid is already
    /// present; existing entries are immutable until the source emits an
    /// `Invalidated` or `Mined` event for them.
    ///
    /// The entry is wrapped in an [`Arc`] so subsequent reads return shared
    /// references without re-cloning the entry payload.
    #[must_use]
    pub fn apply_added(
        &self,
        entry: MempoolEntry,
        applied_event: MempoolEventPosition,
    ) -> MempoolApplyOutcome {
        let mut state = self.state.write();
        state.last_applied_event = Some(applied_event);
        let inserted_entry = match state.entries.entry(entry.transaction_id()) {
            Entry::Occupied(_) => {
                drop(state);
                return MempoolApplyOutcome::NoChange;
            }
            Entry::Vacant(slot) => Arc::clone(slot.insert(Arc::new(entry))),
        };
        index_secondary_overlays(&mut state, inserted_entry.as_ref());
        drop(state);
        MempoolApplyOutcome::Applied
    }

    /// Removes the entry for `transaction_id` as if the upstream source
    /// invalidated it, recording `applied_event` under the same write lock.
    #[must_use]
    pub fn apply_invalidated(
        &self,
        transaction_id: TransactionId,
        applied_event: MempoolEventPosition,
    ) -> MempoolApplyOutcome {
        self.remove_entry(transaction_id, applied_event)
    }

    /// Removes the entry for `transaction_id` as if the upstream source
    /// mined it into a block, recording `applied_event` under the same
    /// write lock.
    #[must_use]
    pub fn apply_mined(
        &self,
        transaction_id: TransactionId,
        applied_event: MempoolEventPosition,
    ) -> MempoolApplyOutcome {
        self.remove_entry(transaction_id, applied_event)
    }

    fn remove_entry(
        &self,
        transaction_id: TransactionId,
        applied_event: MempoolEventPosition,
    ) -> MempoolApplyOutcome {
        let mut state = self.state.write();
        state.last_applied_event = Some(applied_event);
        let Some(removed_entry) = state.entries.remove(&transaction_id) else {
            drop(state);
            return MempoolApplyOutcome::NoChange;
        };
        unindex_secondary_overlays(&mut state, removed_entry.as_ref());
        drop(state);
        MempoolApplyOutcome::Applied
    }

    /// Returns the log position of the last event applied to the index.
    #[must_use]
    pub fn last_applied_event(&self) -> Option<MempoolEventPosition> {
        self.state.read().last_applied_event
    }

    /// Returns whether `transaction_id` is currently in the live index.
    #[must_use]
    pub fn is_in_mempool(&self, transaction_id: TransactionId) -> bool {
        self.state.read().entries.contains_key(&transaction_id)
    }

    /// Returns a shared handle to the entry for `transaction_id`, when
    /// present. The returned [`Arc`] aliases the in-index entry so callers
    /// avoid cloning the underlying payload.
    #[must_use]
    pub fn entry_for(&self, transaction_id: TransactionId) -> Option<Arc<MempoolEntry>> {
        self.state.read().entries.get(&transaction_id).cloned()
    }

    /// Returns the number of entries currently in the index.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.state.read().entries.len()
    }

    /// Returns transparent mempool outputs for `address_script_hash`,
    /// bounded by `max_entries`.
    #[must_use]
    pub fn transparent_outputs_by_address(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        max_entries: u32,
    ) -> Vec<TransparentMempoolOutput> {
        let state = self.state.read();
        let Some(outputs) = state.outputs_by_address.get(&address_script_hash) else {
            return Vec::new();
        };
        let bound = u32_to_usize(max_entries);
        let transparent_outputs = outputs.values().take(bound).cloned().collect::<Vec<_>>();
        drop(state);
        transparent_outputs
    }

    /// Resolves a batch of outpoints against the live mempool index.
    ///
    /// Returns one entry per requested outpoint, in input order. Each entry
    /// carries the resolved transparent output when the outpoint references
    /// a transaction currently in the mempool and the output index is in
    /// bounds; otherwise the entry's `prevout` is `None`.
    #[must_use]
    pub fn transparent_outputs_by_outpoints(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Vec<TransparentOutputEntry> {
        let state = self.state.read();
        outpoints
            .iter()
            .map(|outpoint| {
                let prevout = state
                    .output_by_outpoint
                    .get(outpoint)
                    .map(|mempool_output| TransparentOutput {
                        value_zat: mempool_output.value_zat,
                        script_pub_key: mempool_output.script_pub_key.clone(),
                    });
                TransparentOutputEntry {
                    outpoint: *outpoint,
                    output: prevout,
                }
            })
            .collect()
    }

    /// Returns the mempool spend that consumes `spent_outpoint`, when
    /// present.
    #[must_use]
    pub fn transparent_spend_by_outpoint(
        &self,
        spent_outpoint: TransparentOutPoint,
    ) -> Option<TransparentMempoolSpend> {
        let state = self.state.read();
        let outcome = state
            .spend_by_outpoint
            .get(&spent_outpoint)
            .and_then(|spending_transaction_id| state.entries.get(spending_transaction_id))
            .and_then(|entry| {
                entry
                    .transparent_spends()
                    .iter()
                    .find(|spend| spend.spent_outpoint == spent_outpoint)
                    .copied()
            });
        drop(state);
        outcome
    }

    /// Returns a snapshot of up to `max_entries` mempool entries.
    ///
    /// Each returned [`Arc`] aliases the in-index entry so callers serialize
    /// the snapshot without re-cloning the underlying payload. The order is
    /// ascending transaction-id order.
    #[must_use]
    pub fn snapshot(&self, max_entries: u32) -> Vec<Arc<MempoolEntry>> {
        self.snapshot_page(max_entries, None).entries
    }

    /// Returns a deterministic page of mempool entries after `after_transaction_id`.
    ///
    /// Pagination is ordered by transaction id. Only the requested page and
    /// one lookahead entry are cloned from the index.
    #[must_use]
    pub fn snapshot_page(
        &self,
        max_entries: u32,
        after_transaction_id: Option<TransactionId>,
    ) -> MempoolSnapshotPage {
        let state = self.state.read();
        let last_applied_event = state.last_applied_event;
        let bound = u32_to_usize(max_entries);
        let mut entries = after_transaction_id.map_or_else(
            || {
                state
                    .entries
                    .values()
                    .take(bound.saturating_add(1))
                    .cloned()
                    .collect::<Vec<_>>()
            },
            |after_transaction_id| {
                state
                    .entries
                    .range((Excluded(after_transaction_id), Unbounded))
                    .take(bound.saturating_add(1))
                    .map(|(_transaction_id, entry)| Arc::clone(entry))
                    .collect::<Vec<_>>()
            },
        );
        drop(state);
        let has_more = entries.len() > bound;
        entries.truncate(bound);
        let next_after_transaction_id = if has_more {
            entries.last().map(|entry| entry.transaction_id())
        } else {
            None
        };

        MempoolSnapshotPage {
            entries,
            next_after_transaction_id,
            last_applied_event,
        }
    }
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "zinder-core rejects targets with pointer widths below 32 bits"
)]
const fn u32_to_usize(count: u32) -> usize {
    count as usize
}

fn index_secondary_overlays(state: &mut MempoolIndexState, entry: &MempoolEntry) {
    for transparent_output in entry.transparent_outputs() {
        state
            .outputs_by_address
            .entry(transparent_output.address_script_hash)
            .or_default()
            .insert(transparent_output.outpoint, transparent_output.clone());
        state
            .output_by_outpoint
            .insert(transparent_output.outpoint, transparent_output.clone());
    }
    for transparent_spend in entry.transparent_spends() {
        state
            .spend_by_outpoint
            .insert(transparent_spend.spent_outpoint, entry.transaction_id());
    }
}

fn unindex_secondary_overlays(state: &mut MempoolIndexState, entry: &MempoolEntry) {
    for transparent_output in entry.transparent_outputs() {
        if let Some(outputs) = state
            .outputs_by_address
            .get_mut(&transparent_output.address_script_hash)
        {
            outputs.remove(&transparent_output.outpoint);
            if outputs.is_empty() {
                state
                    .outputs_by_address
                    .remove(&transparent_output.address_script_hash);
            }
        }
        state
            .output_by_outpoint
            .remove(&transparent_output.outpoint);
    }
    for transparent_spend in entry.transparent_spends() {
        state
            .spend_by_outpoint
            .remove(&transparent_spend.spent_outpoint);
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use super::{MempoolApplyOutcome, MempoolIndex};
    use zinder_core::{
        BlockHash, BlockHeight, ChainEpoch, ChainEpochId, ChainTipMetadata, CompactTransactionData,
        CompactTransparentInput, CompactTransparentOutput, MempoolEntry, MempoolEntryBuildError,
        MempoolObservation, Network, RawTransactionBytes, TransactionId,
        TransparentAddressScriptHash, TransparentOutPoint, UnixTimestampMillis,
    };
    use zinder_store::{CURRENT_ARTIFACT_SCHEMA_VERSION, MempoolEventPosition};

    fn applied_at(event_sequence: u64, transaction_id: TransactionId) -> MempoolEventPosition {
        MempoolEventPosition {
            event_sequence,
            transaction_id,
        }
    }

    fn synthetic_chain_epoch() -> ChainEpoch {
        ChainEpoch {
            id: ChainEpochId::new(7),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(100),
            visible_tip_hash: BlockHash::from_bytes([0x42; 32]),
            settled_tip_height: BlockHeight::new(100),
            settled_tip_hash: BlockHash::from_bytes([0x42; 32]),
            artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(1_700_000_000_000),
        }
    }

    fn entry_with_outputs_and_spend(
        transaction_id_byte: u8,
        address_byte: u8,
        spent_outpoint_txid_byte: u8,
    ) -> Result<MempoolEntry, MempoolEntryBuildError> {
        let transaction_id = TransactionId::from_bytes([transaction_id_byte; 32]);
        MempoolEntry::new(
            transaction_id,
            None,
            RawTransactionBytes::new(vec![transaction_id_byte; 8]),
            CompactTransactionData {
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 1_000,
                    script_pub_key: vec![address_byte; 25],
                }],
                transparent_inputs: vec![CompactTransparentInput {
                    previous_transaction_id: TransactionId::from_bytes(
                        [spent_outpoint_txid_byte; 32],
                    ),
                    previous_output_index: 0,
                }],
                ..CompactTransactionData::default()
            },
            MempoolObservation {
                first_seen_unix_millis: UnixTimestampMillis::new(1_700_000_000_000),
                first_seen_chain_epoch: synthetic_chain_epoch(),
            },
        )
    }

    #[test]
    fn apply_added_inserts_entry_and_secondary_indexes() -> Result<(), MempoolEntryBuildError> {
        let index = MempoolIndex::new();
        let entry = entry_with_outputs_and_spend(0x10, 0xAA, 0x20)?;

        let outcome = index.apply_added(entry.clone(), applied_at(1, entry.transaction_id()));

        assert_eq!(outcome, MempoolApplyOutcome::Applied);
        assert!(index.is_in_mempool(entry.transaction_id()));
        assert_eq!(index.entry_count(), 1);
        assert_eq!(
            index.last_applied_event(),
            Some(applied_at(1, entry.transaction_id()))
        );

        let outputs = index.transparent_outputs_by_address(
            TransparentAddressScriptHash::of_script_pub_key(&[0xAA; 25]),
            10,
        );
        assert_eq!(outputs.len(), 1);

        let spend = index.transparent_spend_by_outpoint(TransparentOutPoint::new(
            TransactionId::from_bytes([0x20; 32]),
            0,
        ));
        assert!(spend.is_some());
        Ok(())
    }

    #[test]
    fn apply_added_is_idempotent_for_duplicate_txid() -> Result<(), MempoolEntryBuildError> {
        let index = MempoolIndex::new();
        let entry = entry_with_outputs_and_spend(0x10, 0xAA, 0x20)?;
        assert_eq!(
            index.apply_added(entry.clone(), applied_at(1, entry.transaction_id())),
            MempoolApplyOutcome::Applied
        );

        let transaction_id = entry.transaction_id();
        let outcome = index.apply_added(entry, applied_at(2, transaction_id));

        assert_eq!(outcome, MempoolApplyOutcome::NoChange);
        assert_eq!(index.entry_count(), 1);
        // A logged no-op still advances the last-applied position.
        assert_eq!(
            index.last_applied_event(),
            Some(applied_at(2, transaction_id))
        );
        Ok(())
    }

    #[test]
    fn apply_invalidated_removes_entry_and_secondary_indexes() -> Result<(), MempoolEntryBuildError>
    {
        let index = MempoolIndex::new();
        let entry = entry_with_outputs_and_spend(0x10, 0xAA, 0x20)?;
        let _ = index.apply_added(entry.clone(), applied_at(1, entry.transaction_id()));

        let outcome = index.apply_invalidated(
            entry.transaction_id(),
            applied_at(2, entry.transaction_id()),
        );

        assert_eq!(outcome, MempoolApplyOutcome::Applied);
        assert!(!index.is_in_mempool(entry.transaction_id()));
        assert!(
            index
                .transparent_outputs_by_address(
                    TransparentAddressScriptHash::of_script_pub_key(&[0xAA; 25]),
                    10,
                )
                .is_empty()
        );
        assert!(
            index
                .transparent_spend_by_outpoint(TransparentOutPoint::new(
                    TransactionId::from_bytes([0x20; 32]),
                    0,
                ))
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn apply_mined_returns_no_change_for_unknown_txid() {
        let index = MempoolIndex::new();
        let unknown = TransactionId::from_bytes([0xFF; 32]);
        let outcome = index.apply_mined(unknown, applied_at(1, unknown));
        assert_eq!(outcome, MempoolApplyOutcome::NoChange);
    }

    #[test]
    fn snapshot_respects_max_entries_bound() -> Result<(), MempoolEntryBuildError> {
        let index = MempoolIndex::new();
        for index_byte in 0u8..5 {
            let entry = entry_with_outputs_and_spend(index_byte, 0xAA, 0x20)?;
            let transaction_id = entry.transaction_id();
            let _ = index.apply_added(entry, applied_at(u64::from(index_byte) + 1, transaction_id));
        }
        assert_eq!(index.snapshot(2).len(), 2);
        assert_eq!(index.snapshot(10).len(), 5);
        Ok(())
    }

    #[test]
    fn snapshot_pages_follow_transaction_id_order_without_repetition()
    -> Result<(), MempoolEntryBuildError> {
        let index = MempoolIndex::new();
        let mut expected_transaction_ids = Vec::new();
        for index_byte in [0x50, 0x10, 0x40, 0x20, 0x30] {
            let entry = entry_with_outputs_and_spend(index_byte, 0xAA, 0x20)?;
            let transaction_id = entry.transaction_id();
            expected_transaction_ids.push(transaction_id);
            let _ = index.apply_added(entry, applied_at(u64::from(index_byte), transaction_id));
        }
        expected_transaction_ids.sort_unstable();

        let mut observed_transaction_ids = Vec::new();
        let mut after_transaction_id = None;
        loop {
            let page = index.snapshot_page(2, after_transaction_id);
            observed_transaction_ids
                .extend(page.entries.iter().map(|entry| entry.transaction_id()));
            let Some(next_after_transaction_id) = page.next_after_transaction_id else {
                break;
            };
            after_transaction_id = Some(next_after_transaction_id);
        }

        assert_eq!(observed_transaction_ids, expected_transaction_ids);
        Ok(())
    }

    #[test]
    fn snapshot_page_carries_last_applied_event() -> Result<(), MempoolEntryBuildError> {
        let index = MempoolIndex::new();
        assert!(index.snapshot_page(10, None).last_applied_event.is_none());

        let entry = entry_with_outputs_and_spend(0x10, 0xAA, 0x20)?;
        let position = applied_at(7, entry.transaction_id());
        let _ = index.apply_added(entry, position);

        assert_eq!(
            index.snapshot_page(10, None).last_applied_event,
            Some(position)
        );
        Ok(())
    }

    #[test]
    fn transparent_outputs_lookup_returns_empty_for_unknown_address() {
        let index = MempoolIndex::new();
        let outputs = index.transparent_outputs_by_address(
            TransparentAddressScriptHash::from_bytes([0x99; 32]),
            10,
        );
        assert!(outputs.is_empty());
    }
}
