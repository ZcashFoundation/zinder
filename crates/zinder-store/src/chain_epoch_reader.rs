//! Epoch-bound chain artifact reader.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use zinder_core::{
    BlockBlobArtifact, BlockFinalNoteCommitmentRoots, BlockHash, BlockHeaderArtifact, BlockHeight,
    BlockHeightRange, BlockValuePoolBalances, CanonicalHistoryBounds, ChainEpoch, ChainEpochId,
    CompactBlockArtifact, DisplacedRootArchiveCoverage, DisplacedRootCandidate,
    FinalNoteCommitmentRoot, ShieldedProtocol, SubtreeRootArtifact, SubtreeRootRange,
    TransactionBlobArtifact, TransactionFactsArtifact, TransactionId,
    TransactionIntrinsicValueBalancesArtifact, TransactionLocation, TransparentAddressScriptHash,
    TransparentOutPoint, TransparentOutputArtifact, TransparentOutputEntry, TransparentSpendFact,
    TransparentUnspentOutput, TransparentUtxoSetSummary, TreeStateArtifact,
    ValidatedCanonicalBlockReplay,
};

use crate::{
    StoreError,
    address_output_index::{
        AddressOutputIndexStore, TransparentAddressBalanceSnapshot, read_address_output_index,
        read_transparent_address_balance_snapshot, read_transparent_utxo_set_aggregate,
    },
    block_artifact::{
        BlockBlobStore, BlockHeaderStore, BlockTransactionIndexStore, CompactBlockStore,
        read_block_blob_artifact, read_block_blob_artifacts, read_block_header_artifact,
        read_block_header_artifacts, read_block_transaction_index_artifact,
        read_block_transaction_index_artifacts_at_height, read_compact_block_artifact,
        read_compact_block_artifacts,
    },
    block_hash_index::{BlockHashLookup, read_block_hash_lookup},
    block_replay::{
        BlockReplayBatchRequest, BlockReplayStore, read_block_replay, read_block_replay_batch,
    },
    block_value_pool_balances::{
        BlockValuePoolBalancesStore, read_block_value_pool_balances,
        read_block_value_pool_balances_in_range,
    },
    displaced_block::{read_displaced_root_archive_coverage, read_displaced_root_candidates},
    final_note_commitment_roots::{
        FinalNoteCommitmentRootsStore, read_final_note_commitment_roots,
        read_final_note_commitment_roots_in_range,
    },
    kv::RocksChainStoreReadView,
    subtree_root::{SubtreeRootStore, read_subtree_root_artifacts},
    transaction_artifact::{
        TransactionBlobStore, TransactionFactsStore, TransactionIntrinsicValueBalancesStore,
        TransactionLocationStore, read_transaction_blob_artifact, read_transaction_facts_artifact,
        read_transaction_facts_artifacts_batch,
        read_transaction_facts_artifacts_batch_with_known_headers,
        read_transaction_intrinsic_value_balances, read_transaction_intrinsic_value_balances_batch,
        read_transaction_location,
    },
    transparent_output::{
        read_current_transparent_outputs_by_outpoints,
        read_visible_transparent_outputs_by_outpoints,
    },
    transparent_spend_fact::{
        read_current_transparent_spend_fact_block_facts,
        read_current_transparent_spend_facts_by_outpoints,
        read_current_transparent_spend_replay_block,
        read_visible_transparent_spend_facts_by_outpoints,
    },
    tree_state::{TreeStateStore, read_tree_state_checkpoint_at_or_before},
};

/// In-process read view pinned to one [`ChainEpoch`].
pub struct ChainEpochReader<'store> {
    chain_epoch: ChainEpoch,
    canonical_history_bounds: CanonicalHistoryBounds,
    read_view: RocksChainStoreReadView<'store>,
    is_current: bool,
    secondary_visible_epoch: Option<Arc<AtomicU64>>,
}

impl<'store> ChainEpochReader<'store> {
    pub(crate) fn current(
        chain_epoch: ChainEpoch,
        canonical_history_bounds: CanonicalHistoryBounds,
        read_view: RocksChainStoreReadView<'store>,
        secondary_visible_epoch: Option<Arc<AtomicU64>>,
    ) -> Self {
        Self {
            chain_epoch,
            canonical_history_bounds,
            read_view,
            is_current: true,
            secondary_visible_epoch,
        }
    }

    pub(crate) fn at_epoch(
        chain_epoch: ChainEpoch,
        canonical_history_bounds: CanonicalHistoryBounds,
        read_view: RocksChainStoreReadView<'store>,
        secondary_visible_epoch: Option<Arc<AtomicU64>>,
    ) -> Self {
        Self {
            chain_epoch,
            canonical_history_bounds,
            read_view,
            is_current: false,
            secondary_visible_epoch,
        }
    }

    fn read_at_pinned_secondary_epoch<T>(
        &self,
        read: impl FnOnce() -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.ensure_secondary_epoch_is_pinned()?;
        let read_outcome = read();
        self.ensure_secondary_epoch_is_pinned()?;
        read_outcome
    }

    fn ensure_secondary_epoch_is_pinned(&self) -> Result<(), StoreError> {
        let Some(visible_epoch) = &self.secondary_visible_epoch else {
            return Ok(());
        };
        let current = ChainEpochId::new(visible_epoch.load(Ordering::Acquire));
        if current == self.chain_epoch.id {
            return Ok(());
        }
        Err(StoreError::ChainEpochConflict {
            current,
            attempted: self.chain_epoch.id,
        })
    }

    /// Returns the chain epoch this reader is pinned to.
    #[must_use]
    pub const fn chain_epoch(&self) -> ChainEpoch {
        self.chain_epoch
    }

    /// Verifies that a secondary reader still exposes this reader's pinned
    /// epoch.
    ///
    /// Primary readers always pass. Secondary readers return
    /// [`StoreError::ChainEpochConflict`] after catchup advances them to a
    /// different epoch.
    pub fn validate_epoch_pin(&self) -> Result<(), StoreError> {
        self.ensure_secondary_epoch_is_pinned()
    }

    /// Returns the durable canonical-history bounds for this read session.
    #[must_use]
    pub const fn canonical_history_bounds(&self) -> CanonicalHistoryBounds {
        self.canonical_history_bounds
    }

    fn ensure_history_height_available(&self, height: BlockHeight) -> Result<(), StoreError> {
        if !self.canonical_history_bounds.intentionally_excludes(height) {
            return Ok(());
        }
        let checkpoint = self
            .canonical_history_bounds
            .preceding_checkpoint()
            .ok_or(StoreError::CanonicalHistoryBoundsMissing)?;
        Err(StoreError::CanonicalHistoryUnavailable {
            requested_height: height,
            first_available_height: self.canonical_history_bounds.first_available_height(),
            checkpoint,
        })
    }

    /// Reads block-header facts by height.
    pub fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockHeaderArtifact>, StoreError> {
        self.ensure_history_height_available(height)?;
        self.read_at_pinned_secondary_epoch(|| {
            read_block_header_artifact(&self.read_view, self.chain_epoch, height)
        })
    }

    /// Reads block-header facts in one batched store read.
    pub fn block_headers_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockHeaderArtifact>>, StoreError> {
        self.ensure_history_height_available(block_range.start)?;
        read_block_header_artifacts(&self.read_view, self.chain_epoch, block_range)
    }

    /// Reads one complete semantic replay envelope at a canonical height.
    pub fn block_replay_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<ValidatedCanonicalBlockReplay>, StoreError> {
        self.ensure_history_height_available(height)?;
        self.read_at_pinned_secondary_epoch(|| {
            read_block_replay(&self.read_view, self.chain_epoch, height)
        })
    }

    /// Reads a bounded, ascending batch of complete semantic replay envelopes.
    ///
    /// The final batch is clipped to this reader's pinned visible tip. The
    /// store resolves visibility with one ordered scan and fetches payloads
    /// with one `multi_get`.
    pub fn block_replay_batch(
        &self,
        request: BlockReplayBatchRequest,
    ) -> Result<Vec<ValidatedCanonicalBlockReplay>, StoreError> {
        request.ensure_within_limit()?;
        if request.start_height <= self.chain_epoch.visible_tip_height {
            self.ensure_history_height_available(request.start_height)?;
        }
        self.read_at_pinned_secondary_epoch(|| {
            read_block_replay_batch(&self.read_view, self.chain_epoch, request)
        })
    }

    /// Reads an optional raw block blob by height.
    pub fn block_blob_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockBlobArtifact>, StoreError> {
        self.ensure_history_height_available(height)?;
        read_block_blob_artifact(&self.read_view, self.chain_epoch, height)
    }

    /// Reads optional raw block blobs in one batched store read.
    pub fn block_blobs_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockBlobArtifact>>, StoreError> {
        self.ensure_history_height_available(block_range.start)?;
        read_block_blob_artifacts(&self.read_view, self.chain_epoch, block_range)
    }

    /// Reads the transaction id at a block-local index.
    pub fn transaction_id_at_block_index(
        &self,
        height: BlockHeight,
        tx_index_in_block: u32,
    ) -> Result<Option<TransactionId>, StoreError> {
        self.ensure_history_height_available(height)?;
        Ok(read_block_transaction_index_artifact(
            &self.read_view,
            self.chain_epoch,
            height,
            tx_index_in_block,
        )?
        .map(|artifact| artifact.transaction_id))
    }

    /// Reads the ordered transaction ids for every transaction in a block.
    pub fn transaction_ids_at_height(
        &self,
        height: BlockHeight,
    ) -> Result<Vec<TransactionId>, StoreError> {
        self.ensure_history_height_available(height)?;
        read_block_transaction_index_artifacts_at_height(&self.read_view, self.chain_epoch, height)
            .map(|artifacts| {
                artifacts
                    .into_iter()
                    .map(|artifact| artifact.transaction_id)
                    .collect()
            })
    }

    /// Reads a compact block artifact by height.
    pub fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CompactBlockArtifact>, StoreError> {
        self.ensure_history_height_available(height)?;
        read_compact_block_artifact(&self.read_view, self.chain_epoch, height)
    }

    /// Reads compact block artifacts in one batched store read.
    pub fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<Vec<Option<CompactBlockArtifact>>, StoreError> {
        self.ensure_history_height_available(block_range.start)?;
        read_compact_block_artifacts(&self.read_view, self.chain_epoch, block_range)
    }

    /// Reads transaction facts by transaction id.
    pub fn transaction_facts_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionFactsArtifact>, StoreError> {
        read_transaction_facts_artifact(&self.read_view, self.chain_epoch, transaction_id)
    }

    /// Reads transaction facts by id, answering `None` only for a proven
    /// absence.
    ///
    /// `Ok(None)` means the transaction is absent from this epoch's canonical
    /// chain: the store retains complete canonical history, so every height
    /// the transaction could be mined at was searched. When the store's
    /// history starts after a checkpoint, a miss returns
    /// [`StoreError::CanonicalHistoryUnavailable`] instead, because the
    /// transaction may be mined at a height the store does not retain. A
    /// secondary reader that moves off this epoch during the read returns
    /// [`StoreError::ChainEpochConflict`], and storage faults keep their own
    /// errors.
    pub fn transaction_facts_or_proven_absence(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionFactsArtifact>, StoreError> {
        let transaction_facts = self.read_at_pinned_secondary_epoch(|| {
            read_transaction_facts_artifact(&self.read_view, self.chain_epoch, transaction_id)
        })?;
        if transaction_facts.is_none() {
            // Complete history starts at height 1; genesis carries no
            // spendable transaction a caller can look up.
            self.ensure_history_height_available(BlockHeight::new(1))?;
        }
        Ok(transaction_facts)
    }

    /// Reads transaction facts for many ids in one batched store read.
    pub fn transaction_facts_by_ids(
        &self,
        transaction_ids: &[TransactionId],
    ) -> Result<HashMap<TransactionId, Option<TransactionFactsArtifact>>, StoreError> {
        read_transaction_facts_artifacts_batch(&self.read_view, self.chain_epoch, transaction_ids)
    }

    /// Reads transaction facts for many ids, reusing block headers the
    /// caller already holds for the reorg-safety cross-check instead of
    /// re-reading them from the store.
    pub fn transaction_facts_by_ids_with_known_headers(
        &self,
        transaction_ids: &[TransactionId],
        known_block_headers: &HashMap<BlockHeight, BlockHeaderArtifact>,
    ) -> Result<HashMap<TransactionId, Option<TransactionFactsArtifact>>, StoreError> {
        read_transaction_facts_artifacts_batch_with_known_headers(
            &self.read_view,
            self.chain_epoch,
            transaction_ids,
            known_block_headers,
        )
    }

    /// Reads a transaction location by transaction id.
    pub fn transaction_location_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionLocation>, StoreError> {
        read_transaction_location(&self.read_view, self.chain_epoch, transaction_id)
    }

    /// Reads an optional raw transaction blob by transaction id.
    pub fn transaction_blob_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionBlobArtifact>, StoreError> {
        read_transaction_blob_artifact(&self.read_view, self.chain_epoch, transaction_id)
    }

    /// Reads optional transaction-intrinsic shielded value balances by transaction id.
    pub fn transaction_intrinsic_value_balances_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionIntrinsicValueBalancesArtifact>, StoreError> {
        read_transaction_intrinsic_value_balances(&self.read_view, self.chain_epoch, transaction_id)
    }

    /// Reads optional transaction-intrinsic shielded value balances in one batched store read.
    pub fn transaction_intrinsic_value_balances_by_ids(
        &self,
        transaction_ids: &[TransactionId],
    ) -> Result<HashMap<TransactionId, Option<TransactionIntrinsicValueBalancesArtifact>>, StoreError>
    {
        read_transaction_intrinsic_value_balances_batch(
            &self.read_view,
            self.chain_epoch,
            transaction_ids,
        )
    }

    /// Reads a checkpoint tree-state artifact at or before `max_height`.
    pub fn tree_state_checkpoint_at_or_before(
        &self,
        max_height: BlockHeight,
    ) -> Result<Option<TreeStateArtifact>, StoreError> {
        self.ensure_history_height_available(max_height)?;
        read_tree_state_checkpoint_at_or_before(&self.read_view, self.chain_epoch, max_height)
    }

    /// Reads the latest checkpoint tree-state artifact visible to this reader.
    pub fn latest_tree_state_checkpoint(&self) -> Result<Option<TreeStateArtifact>, StoreError> {
        self.tree_state_checkpoint_at_or_before(self.chain_epoch.visible_tip_height)
    }

    /// Reads final note-commitment roots associated with one canonical block.
    pub fn final_note_commitment_roots_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockFinalNoteCommitmentRoots>, StoreError> {
        self.ensure_history_height_available(height)?;
        self.read_at_pinned_secondary_epoch(|| {
            read_final_note_commitment_roots(&self.read_view, self.chain_epoch, height)
        })
    }

    /// Reads final note-commitment roots in ascending height order.
    pub fn final_note_commitment_roots_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockFinalNoteCommitmentRoots>>, StoreError> {
        self.ensure_history_height_available(block_range.start)?;
        read_final_note_commitment_roots_in_range(&self.read_view, self.chain_epoch, block_range)
    }

    /// Reads newest-first displaced occurrences matching one final root and protocol.
    ///
    /// The candidates share this reader's exact `RocksDB` snapshot with canonical
    /// header validation and [`Self::displaced_root_archive_coverage`]. The
    /// append-only reverse index is not epoch-versioned, so historical readers
    /// reject this operation.
    pub fn displaced_root_candidates(
        &self,
        protocol: ShieldedProtocol,
        root: FinalNoteCommitmentRoot,
        limit: NonZeroU32,
    ) -> Result<Vec<DisplacedRootCandidate>, StoreError> {
        if !self.is_current {
            return Err(StoreError::Unsupported {
                feature: "displaced root candidates on historical chain epochs",
            });
        }
        self.read_at_pinned_secondary_epoch(|| {
            read_displaced_root_candidates(
                &self.read_view,
                self.chain_epoch.network,
                protocol,
                root,
                limit,
            )
        })
    }

    /// Reads displaced-root coverage from this reader's exact `RocksDB` snapshot.
    ///
    /// Historical readers reject this operation because coverage describes an
    /// append-only writer sequence rather than a versioned canonical artifact.
    pub fn displaced_root_archive_coverage(
        &self,
    ) -> Result<Option<DisplacedRootArchiveCoverage>, StoreError> {
        if !self.is_current {
            return Err(StoreError::Unsupported {
                feature: "displaced root coverage on historical chain epochs",
            });
        }
        self.read_at_pinned_secondary_epoch(|| {
            read_displaced_root_archive_coverage(&self.read_view)
        })
    }

    /// Reads cumulative value-pool balances after one canonical block.
    pub fn block_value_pool_balances_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockValuePoolBalances>, StoreError> {
        self.ensure_history_height_available(height)?;
        read_block_value_pool_balances(&self.read_view, self.chain_epoch, height)
    }

    /// Reads cumulative value-pool balances in ascending height order.
    pub fn block_value_pool_balances_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockValuePoolBalances>>, StoreError> {
        self.ensure_history_height_available(block_range.start)?;
        read_block_value_pool_balances_in_range(&self.read_view, self.chain_epoch, block_range)
    }

    /// Reads subtree-root artifacts in ascending subtree-index order.
    pub fn subtree_roots(
        &self,
        subtree_root_range: SubtreeRootRange,
    ) -> Result<Vec<Option<SubtreeRootArtifact>>, StoreError> {
        read_subtree_root_artifacts(&self.read_view, self.chain_epoch, subtree_root_range)
    }

    /// Reads unspent transparent outputs for an address script hash.
    pub fn address_output_index(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
        max_entries: NonZeroU32,
    ) -> Result<Vec<TransparentUnspentOutput>, StoreError> {
        self.ensure_history_height_available(start_height)?;
        read_address_output_index(
            &self.read_view,
            self.chain_epoch,
            address_script_hash,
            start_height,
            max_entries,
        )
    }

    /// Reads exact current transparent balances at this reader's visible tip.
    ///
    /// The current projection cannot reconstruct an older epoch after later
    /// spends have changed it, so this operation is available only on a reader
    /// returned by `current_chain_epoch_reader`.
    pub fn transparent_address_balance_snapshot(
        &self,
    ) -> Result<TransparentAddressBalanceSnapshot, StoreError> {
        if !self.is_current {
            return Err(StoreError::Unsupported {
                feature: "transparent address balance snapshots on historical chain epochs",
            });
        }
        self.ensure_history_height_available(self.chain_epoch.visible_tip_height)?;
        read_transparent_address_balance_snapshot(
            &self.read_view,
            self.chain_epoch,
            self.chain_epoch.visible_tip_height,
        )
    }

    /// Reads exact transparent balances at this reader's settled tip.
    pub fn settled_transparent_address_balance_snapshot(
        &self,
    ) -> Result<TransparentAddressBalanceSnapshot, StoreError> {
        if !self.is_current {
            return Err(StoreError::Unsupported {
                feature: "settled transparent address balance snapshots on historical chain epochs",
            });
        }
        self.ensure_history_height_available(self.chain_epoch.settled_tip_height)?;
        read_transparent_address_balance_snapshot(
            &self.read_view,
            self.chain_epoch,
            self.chain_epoch.settled_tip_height,
        )
    }

    /// Aggregates the chain-wide transparent UTXO set at this epoch's settled tip.
    ///
    /// Streams the whole current-UTXO projection and folds it into an unspent
    /// count and total value over the outputs created at or below
    /// `settled_tip_height`, where the projection is the settled-tip unspent set
    /// under the configured reorg policy; a deeper reorg fails closed.
    /// Request-time, full-set scan with constant memory.
    ///
    /// When `commitment_enabled` is set, the same scan also folds the `LtHash16`
    /// homomorphic commitment over the full set; otherwise the commitment field
    /// is absent.
    pub fn transparent_utxo_set_summary(
        &self,
        commitment_enabled: bool,
    ) -> Result<TransparentUtxoSetSummary, StoreError> {
        self.ensure_history_height_available(self.chain_epoch.settled_tip_height)?;
        let aggregate = read_transparent_utxo_set_aggregate(
            &self.read_view,
            self.chain_epoch,
            commitment_enabled,
        )?;
        Ok(TransparentUtxoSetSummary {
            utxo_count: aggregate.utxo_count,
            total_value_zat: aggregate.total_value_zat,
            commitment: aggregate.commitment,
            summarized_height: self.chain_epoch.settled_tip_height,
            chain_epoch: self.chain_epoch,
        })
    }

    /// Resolves a block hash through the canonical best-chain index.
    pub fn block_hash_lookup(&self, block_hash: BlockHash) -> Result<BlockHashLookup, StoreError> {
        read_block_hash_lookup(&self.read_view, self.chain_epoch, block_hash)
    }

    /// Resolves transparent output artifacts by outpoint.
    ///
    /// This does not filter out spent rows: prevout resolution needs the
    /// original value and script after the output has been spent. Current
    /// readers use the exact current projection; pinned historical readers
    /// use the same canonical rows and verify producing-block visibility.
    pub fn transparent_outputs_by_outpoints(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<HashMap<TransparentOutPoint, TransparentOutputArtifact>, StoreError> {
        if self.is_current {
            return read_current_transparent_outputs_by_outpoints(
                &self.read_view,
                self.chain_epoch,
                outpoints,
            );
        }
        read_visible_transparent_outputs_by_outpoints(&self.read_view, self.chain_epoch, outpoints)
    }

    /// Resolves transparent output artifacts by outpoint on the primary writer's
    /// commit path.
    ///
    /// This intentionally skips external-reader visibility and spend-state
    /// filtering. Use it only while the writer is deriving a node-validated
    /// batch against its own current epoch.
    pub fn transparent_outputs_by_outpoints_for_writer_commit(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<HashMap<TransparentOutPoint, TransparentOutputArtifact>, StoreError> {
        read_current_transparent_outputs_by_outpoints(&self.read_view, self.chain_epoch, outpoints)
    }

    /// Resolves transparent spend facts by spent outpoint.
    pub fn transparent_spend_facts_by_outpoints(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<HashMap<TransparentOutPoint, TransparentSpendFact>, StoreError> {
        if self.is_current {
            return read_current_transparent_spend_facts_by_outpoints(
                &self.read_view,
                self.chain_epoch,
                outpoints,
            );
        }
        read_visible_transparent_spend_facts_by_outpoints(
            &self.read_view,
            self.chain_epoch,
            outpoints,
        )
    }

    /// Resolves the unspent transparent outputs among `outpoints` at this
    /// epoch.
    ///
    /// An outpoint is kept only when its output is present and carries no
    /// canonical spend fact at the pinned epoch. Duplicate input outpoints
    /// yield at most one entry, preserving first-seen order. Each kept entry's
    /// `output` is always populated.
    pub fn transparent_unspent_outputs_by_outpoints(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<Vec<TransparentOutputEntry>, StoreError> {
        let outputs_by_outpoint = self.transparent_outputs_by_outpoints(outpoints)?;
        let spends_by_outpoint = self.transparent_spend_facts_by_outpoints(outpoints)?;

        let mut entries = Vec::with_capacity(outputs_by_outpoint.len());
        let mut seen = HashSet::with_capacity(outputs_by_outpoint.len());
        for outpoint in outpoints {
            if spends_by_outpoint.contains_key(outpoint) {
                continue;
            }
            if let Some(output) = outputs_by_outpoint.get(outpoint)
                && seen.insert(*outpoint)
            {
                entries.push(TransparentOutputEntry {
                    outpoint: *outpoint,
                    output: Some(output.clone().into_output()),
                });
            }
        }
        Ok(entries)
    }

    /// Resolves transparent spend facts from the current projection, skipping
    /// the per-outpoint reorg-visibility header reads.
    ///
    /// Correct only when every referenced block is settled (at or below
    /// `settled_tip_height`): such blocks are outside the configured reorg window, so the visibility filter
    /// that [`Self::transparent_spend_facts_by_outpoints`] applies on a
    /// non-current reader can never drop a fact. Skipping it turns two reads per
    /// outpoint into a single `multi_get`, which is the dominant cost of
    /// from-genesis materialized-view replay.
    pub fn current_transparent_spend_facts_by_outpoints(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<HashMap<TransparentOutPoint, TransparentSpendFact>, StoreError> {
        read_current_transparent_spend_facts_by_outpoints(
            &self.read_view,
            self.chain_epoch,
            outpoints,
        )
    }

    /// Reads the complete transparent spend facts produced by one settled
    /// block from its durable block-local replay record.
    ///
    /// This path remains available after point-row retention and performs one
    /// ordered index seek instead of one random lookup per spent outpoint.
    pub fn current_transparent_spend_facts_at_height(
        &self,
        height: BlockHeight,
    ) -> Result<Vec<TransparentSpendFact>, StoreError> {
        read_current_transparent_spend_fact_block_facts(&self.read_view, self.chain_epoch, height)
    }

    /// Reads the complete transparent input set and resolved spend facts for
    /// one settled block.
    pub fn current_transparent_spend_replay_at_height(
        &self,
        height: BlockHeight,
    ) -> Result<Option<crate::TransparentSpendReplayBlock>, StoreError> {
        read_current_transparent_spend_replay_block(&self.read_view, self.chain_epoch, height)
    }

    /// Reads the height through which transparent-retention maintenance has actually
    /// deleted transparent spend facts, or `None` before any real deletion.
    ///
    /// A canonical spend-fact miss for an outpoint spent at or below this
    /// height means the fact was swept, not that the outpoint is unspent: a
    /// durable projection is the only remaining source of the spender identity.
    /// Below it a canonical miss must consult the projection; a checkpoint
    /// bootstrap that advanced only the swept cursor leaves this marker unset.
    pub fn transparent_retention_deleted_through_height(
        &self,
    ) -> Result<Option<BlockHeight>, StoreError> {
        crate::chain_store::read_transparent_retention_deleted_through_height(&self.read_view)
    }
}

impl BlockHeaderStore for ChainEpochReader<'_> {
    fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockHeaderArtifact>, StoreError> {
        self.block_header_at(height)
    }

    fn block_headers_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockHeaderArtifact>>, StoreError> {
        self.block_headers_in_range(block_range)
    }
}

impl BlockReplayStore for ChainEpochReader<'_> {
    fn block_replay_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<ValidatedCanonicalBlockReplay>, StoreError> {
        self.block_replay_at(height)
    }

    fn block_replay_batch(
        &self,
        request: BlockReplayBatchRequest,
    ) -> Result<Vec<ValidatedCanonicalBlockReplay>, StoreError> {
        self.block_replay_batch(request)
    }
}

impl BlockBlobStore for ChainEpochReader<'_> {
    fn block_blob_at(&self, height: BlockHeight) -> Result<Option<BlockBlobArtifact>, StoreError> {
        self.block_blob_at(height)
    }
}

impl BlockTransactionIndexStore for ChainEpochReader<'_> {
    fn transaction_id_at_block_index(
        &self,
        height: BlockHeight,
        tx_index_in_block: u32,
    ) -> Result<Option<TransactionId>, StoreError> {
        self.transaction_id_at_block_index(height, tx_index_in_block)
    }

    fn transaction_ids_at_height(
        &self,
        height: BlockHeight,
    ) -> Result<Vec<TransactionId>, StoreError> {
        self.transaction_ids_at_height(height)
    }
}

impl CompactBlockStore for ChainEpochReader<'_> {
    fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CompactBlockArtifact>, StoreError> {
        self.compact_block_at(height)
    }
}

impl TransactionFactsStore for ChainEpochReader<'_> {
    fn transaction_facts_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionFactsArtifact>, StoreError> {
        self.transaction_facts_by_id(transaction_id)
    }

    fn transaction_facts_by_ids(
        &self,
        transaction_ids: &[TransactionId],
    ) -> Result<HashMap<TransactionId, Option<TransactionFactsArtifact>>, StoreError> {
        self.transaction_facts_by_ids(transaction_ids)
    }
}

impl TransactionLocationStore for ChainEpochReader<'_> {
    fn transaction_location_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionLocation>, StoreError> {
        self.transaction_location_by_id(transaction_id)
    }
}

impl TransactionBlobStore for ChainEpochReader<'_> {
    fn transaction_blob_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionBlobArtifact>, StoreError> {
        self.transaction_blob_by_id(transaction_id)
    }
}

impl TransactionIntrinsicValueBalancesStore for ChainEpochReader<'_> {
    fn transaction_intrinsic_value_balances_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionIntrinsicValueBalancesArtifact>, StoreError> {
        self.transaction_intrinsic_value_balances_by_id(transaction_id)
    }

    fn transaction_intrinsic_value_balances_by_ids(
        &self,
        transaction_ids: &[TransactionId],
    ) -> Result<HashMap<TransactionId, Option<TransactionIntrinsicValueBalancesArtifact>>, StoreError>
    {
        self.transaction_intrinsic_value_balances_by_ids(transaction_ids)
    }
}

impl TreeStateStore for ChainEpochReader<'_> {
    fn tree_state_checkpoint_at_or_before(
        &self,
        max_height: BlockHeight,
    ) -> Result<Option<TreeStateArtifact>, StoreError> {
        self.tree_state_checkpoint_at_or_before(max_height)
    }
}

impl FinalNoteCommitmentRootsStore for ChainEpochReader<'_> {
    fn final_note_commitment_roots_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockFinalNoteCommitmentRoots>, StoreError> {
        self.final_note_commitment_roots_at(height)
    }

    fn final_note_commitment_roots_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockFinalNoteCommitmentRoots>>, StoreError> {
        self.final_note_commitment_roots_in_range(block_range)
    }
}

impl BlockValuePoolBalancesStore for ChainEpochReader<'_> {
    fn block_value_pool_balances_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockValuePoolBalances>, StoreError> {
        self.block_value_pool_balances_at(height)
    }

    fn block_value_pool_balances_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockValuePoolBalances>>, StoreError> {
        self.block_value_pool_balances_in_range(block_range)
    }
}

impl SubtreeRootStore for ChainEpochReader<'_> {
    fn subtree_roots(
        &self,
        subtree_root_range: SubtreeRootRange,
    ) -> Result<Vec<Option<SubtreeRootArtifact>>, StoreError> {
        self.subtree_roots(subtree_root_range)
    }
}

impl AddressOutputIndexStore for ChainEpochReader<'_> {
    fn address_output_index(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
        max_entries: NonZeroU32,
    ) -> Result<Vec<TransparentUnspentOutput>, StoreError> {
        self.address_output_index(address_script_hash, start_height, max_entries)
    }

    fn transparent_address_balance_snapshot(
        &self,
    ) -> Result<TransparentAddressBalanceSnapshot, StoreError> {
        self.transparent_address_balance_snapshot()
    }

    fn settled_transparent_address_balance_snapshot(
        &self,
    ) -> Result<TransparentAddressBalanceSnapshot, StoreError> {
        self.settled_transparent_address_balance_snapshot()
    }
}
