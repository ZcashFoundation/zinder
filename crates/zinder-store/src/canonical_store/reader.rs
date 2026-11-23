//! Typed serving reads for an admitted canonical store.

use std::mem::size_of;

use crate::{BlockHashLookup, BoundedRocksDbOpen};
use rust_rocksdb::{DB, Direction, IteratorMode};
use zinder_core::{
    BlockBlobArtifact, BlockFinalNoteCommitmentRoots, BlockHash, BlockHeaderArtifact, BlockHeight,
    BlockHeightRange, BlockId, ChainEpoch, ChainTipMetadata, CommitmentTreeCheckpoint,
    CompactBlockArtifact, Network, NetworkUpgradeActivations, ShieldedProtocol,
    SubtreeRootArtifact, SubtreeRootIndex, SubtreeRootRange, TransactionBlobArtifact,
    TransactionId, TransactionLocation, UnixTimestampMillis,
    wire::{encode_internal_block_hash, encode_internal_transaction_id},
};

use super::{
    CURRENT_ARTIFACT_SCHEMA_VERSION, CanonicalStoreError, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore,
    block_load::{
        BLOCK_HEADER_VALUE_LEN, StoredTreeStateRoots, decode_served_tree_state_checkpoint,
        decode_stored_tree_state_roots, decode_tree_state_checkpoint, encode_block_position,
        encode_transaction_position, encode_tree_state_checkpoint,
    },
    block_replay::validate_incremental_replay_range_bound,
    pool_presence_mismatch,
    publication::column_family,
    rocksdb::{
        BLOCK_BLOB_COLUMN_FAMILY, BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY,
        CHAIN_EPOCH_COLUMN_FAMILY, COMPACT_BLOCK_COLUMN_FAMILY, SUBTREE_ROOT_COLUMN_FAMILY,
        TRANSACTION_BLOB_COLUMN_FAMILY, TRANSACTION_LOCATION_COLUMN_FAMILY,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    },
    served_tree_state::ServedTreeState,
    subtree_load::{
        CanonicalSubtreeRootLoadCoverage, decode_subtree_root, encode_subtree_root_position,
    },
    tree_state_replay::{
        TreeStateLookup, TreeStateReplay, TreeStateReplayOutcome, refuse_unadmitted_activations,
    },
};

const CHAIN_EPOCH_VALUE_BYTES: usize = 93;
const TRANSACTION_LOCATION_VALUE_BYTES: usize = 40;
const COMPACT_BLOCK_MULTI_GET_BATCH_SIZE: usize = 1_024;
const BLOCK_BLOB_MULTI_GET_BATCH_SIZE: usize = 1_024;
const TREE_STATE_ROOTS_MULTI_GET_BATCH_SIZE: usize = 1_024;
const SUBTREE_ROOT_PAGE_SIZE: usize = zinder_core::MAX_SUBTREE_ROOTS_PER_REQUEST as usize;

pub(super) trait CanonicalServingRead {
    fn serving_open(&self) -> &BoundedRocksDbOpen;
    fn serving_network(&self) -> Network;
    fn serving_build_plan(&self) -> &super::CanonicalStoreBuildPlan;
    fn serving_ready_evidence(&self) -> super::CanonicalStoreReadyEvidence;
    fn serving_subtree_root_coverage(&self) -> CanonicalSubtreeRootLoadCoverage;
    fn serving_admits_activations_through(
        &self,
        activations: &NetworkUpgradeActivations,
        height: BlockHeight,
    ) -> bool;
}

impl CanonicalServingRead for RocksDbCanonicalStore {
    fn serving_open(&self) -> &BoundedRocksDbOpen {
        &self.bounded_open
    }

    fn serving_network(&self) -> Network {
        self.network()
    }

    fn serving_build_plan(&self) -> &super::CanonicalStoreBuildPlan {
        self.build_plan()
    }

    fn serving_ready_evidence(&self) -> super::CanonicalStoreReadyEvidence {
        self.ready_evidence()
    }

    fn serving_subtree_root_coverage(&self) -> CanonicalSubtreeRootLoadCoverage {
        self.subtree_root_coverage
    }

    fn serving_admits_activations_through(
        &self,
        activations: &NetworkUpgradeActivations,
        height: BlockHeight,
    ) -> bool {
        super::activation_schedule::admits_activation_table_through(
            &self.build_plan,
            self.activation_schedule.as_ref(),
            activations,
            height,
        )
    }
}

impl CanonicalServingRead for RocksDbCanonicalSecondary {
    fn serving_open(&self) -> &BoundedRocksDbOpen {
        &self.bounded_open
    }

    fn serving_network(&self) -> Network {
        self.network()
    }

    fn serving_build_plan(&self) -> &super::CanonicalStoreBuildPlan {
        self.build_plan()
    }

    fn serving_ready_evidence(&self) -> super::CanonicalStoreReadyEvidence {
        self.ready_evidence()
    }

    fn serving_subtree_root_coverage(&self) -> CanonicalSubtreeRootLoadCoverage {
        self.subtree_root_coverage
    }

    fn serving_admits_activations_through(
        &self,
        activations: &NetworkUpgradeActivations,
        height: BlockHeight,
    ) -> bool {
        super::activation_schedule::admits_activation_table_through(
            &self.build_plan,
            self.activation_schedule.as_ref(),
            activations,
            height,
        )
    }
}

macro_rules! impl_canonical_typed_reads {
    ($store:ty) => {
        impl $store {
            /// Reads the exact visible epoch admitted by this READY store.
            pub fn chain_epoch(&self) -> Result<ChainEpoch, CanonicalStoreError> {
                read_chain_epoch(self)
            }

            /// Reads one immutable canonical epoch retained by this admitted store.
            ///
            /// Retained transition replay uses this to reconstruct the exact
            /// `ChainView` that resulted from an event, rather than projecting a
            /// later visible epoch onto historical events. The method does not
            /// open another store or mutate the primary.
            pub fn chain_epoch_at(
                &self,
                epoch_id: zinder_core::ChainEpochId,
            ) -> Result<ChainEpoch, CanonicalStoreError> {
                read_chain_epoch_at(self, epoch_id)
            }

            /// Reads one canonical block header by height.
            pub fn block_header_at(
                &self,
                height: BlockHeight,
            ) -> Result<Option<BlockHeaderArtifact>, CanonicalStoreError> {
                read_block_header_at(self, height)
            }

            /// Resolves a block hash through the canonical best-chain index.
            pub fn block_hash_lookup(
                &self,
                block_hash: BlockHash,
            ) -> Result<BlockHashLookup, CanonicalStoreError> {
                read_block_hash_lookup(self, block_hash)
            }

            /// Reads one retained raw block by height.
            pub fn block_blob_at(
                &self,
                height: BlockHeight,
            ) -> Result<Option<BlockBlobArtifact>, CanonicalStoreError> {
                read_block_blob_at(self, height)
            }

            /// Batch-reads retained raw blocks for an inclusive height range.
            ///
            /// The output preserves range order and contains one entry per
            /// requested height. `None` means this store did not retain that
            /// height's raw block bytes.
            pub fn block_blobs_in_range(
                &self,
                range: BlockHeightRange,
            ) -> Result<Vec<Option<BlockBlobArtifact>>, CanonicalStoreError> {
                read_block_blobs_in_range(self, range)
            }

            /// Reads one compact block by height.
            pub fn compact_block_at(
                &self,
                height: BlockHeight,
            ) -> Result<Option<CompactBlockArtifact>, CanonicalStoreError> {
                read_compact_block_at(&self.bounded_open.db, height)
            }

            /// Reads an inclusive compact-block range in ascending height order.
            ///
            /// Every height of `range` is checked against retained history
            /// before any row is read. A height below the first retained block
            /// or above the visible tip fails with
            /// [`CanonicalStoreError::ArtifactUnavailable`] naming the lowest
            /// such height, even where a row is stored for it. A missing row
            /// inside that span is store damage and fails as
            /// [`CanonicalStoreError::PublicationRefused`].
            pub fn compact_blocks_in_range(
                &self,
                range: zinder_core::BlockHeightRange,
            ) -> Result<Vec<CompactBlockArtifact>, CanonicalStoreError> {
                read_compact_blocks_in_range(self, range)
            }

            /// Reads compact blocks from one end of an inclusive range until
            /// the next block would take their stored bytes past `max_bytes`.
            ///
            /// The read starts at the lowest height for
            /// [`HeightOrder::Ascending`] and at the highest for
            /// [`HeightOrder::Descending`], and walks toward the other end. It
            /// always keeps the first block, so a block larger than
            /// `max_bytes` is read alone. The blocks come back in ascending
            /// height order and cover consecutive heights at the starting end.
            ///
            /// Every height of `range` is checked against retained history
            /// first, so a range that reaches outside it fails with
            /// [`CanonicalStoreError::ArtifactUnavailable`] exactly as
            /// [`Self::compact_blocks_in_range`] does, even where the byte
            /// budget would stop the read before that height. A row missing
            /// inside retained history fails the read as store damage once
            /// the read reaches it. Unless the kept blocks take at least
            /// `max_bytes`, as they do when they fill it or when the first
            /// block alone is larger, the read reaches the row after the last
            /// block it keeps, which it looks at to learn whether that row
            /// fits.
            pub fn compact_blocks_in_range_up_to(
                &self,
                range: zinder_core::BlockHeightRange,
                order: HeightOrder,
                max_bytes: usize,
            ) -> Result<Vec<CompactBlockArtifact>, CanonicalStoreError> {
                read_compact_blocks_up_to(self, range, order, max_bytes, |block, _| block)
            }

            /// Reads the canonical location of one transaction.
            pub fn transaction_location(
                &self,
                transaction_id: TransactionId,
            ) -> Result<Option<TransactionLocation>, CanonicalStoreError> {
                read_transaction_location(self, transaction_id)
            }

            /// Reads the canonical locations of several transactions in one
            /// batched lookup.
            ///
            /// Returns one entry per requested id, in request order, each
            /// `None` exactly when [`Self::transaction_location`] would return
            /// `None`. The lookups run in key order, so ids whose rows share a
            /// data block cost one block read.
            pub fn transaction_locations(
                &self,
                transaction_ids: &[TransactionId],
            ) -> Result<Vec<Option<TransactionLocation>>, CanonicalStoreError> {
                read_transaction_locations(self, transaction_ids)
            }

            /// Reads the canonical location of one transaction, answering
            /// `None` only for a proven absence.
            ///
            /// `Ok(None)` means the transaction is absent from this store's
            /// canonical chain from height 1 through the visible tip. A store
            /// whose retained history starts after a checkpoint answers a miss
            /// with [`CanonicalStoreError::CanonicalHistoryUnavailable`]
            /// instead, because the transaction may be mined at a height the
            /// store never retained.
            pub fn transaction_location_or_proven_absence(
                &self,
                transaction_id: TransactionId,
            ) -> Result<Option<TransactionLocation>, CanonicalStoreError> {
                read_transaction_location_or_proven_absence(self, transaction_id)
            }

            /// Reads one retained raw transaction at its authenticated location.
            pub fn transaction_blob(
                &self,
                location: TransactionLocation,
            ) -> Result<Option<TransactionBlobArtifact>, CanonicalStoreError> {
                read_transaction_blob(self, location)
            }

            /// Reads the newest commitment-tree checkpoint stored at or below
            /// `height`, as a wallet read serves it.
            ///
            /// Only the row's framing is checked: its frontier bytes are
            /// returned without decoding or hashing tree nodes, and its roots
            /// are not read. Every row is validated when it is written, and
            /// `RocksDB` block checksums detect corruption at rest. Tree state
            /// that seeds further tree state comes from
            /// [`Self::tree_state_checkpoint_at`] instead.
            pub fn tree_state_checkpoint_at_or_before(
                &self,
                height: BlockHeight,
            ) -> Result<Option<ServedTreeState>, CanonicalStoreError> {
                read_tree_state_checkpoint_row_at_or_before(self, height)?
                    .map(|row| decode_served_tree_state(self, row.height, &row.encoded))
                    .transpose()
            }

            /// Reads the wallet checkpoint stored at exactly `height` without loading a replay seed.
            pub fn stored_tree_state_at(
                &self,
                height: BlockHeight,
            ) -> Result<Option<ServedTreeState>, CanonicalStoreError> {
                let predecessor = self
                    .serving_build_plan()
                    .history_predecessor()
                    .block_id
                    .height;
                if height < predecessor || height > self.serving_ready_evidence().visible_tip.height
                {
                    return Ok(None);
                }
                read_tree_state_row_at(self, height)?
                    .map(|encoded| decode_served_tree_state(self, height, &encoded))
                    .transpose()
            }

            /// Reads the commitment-tree checkpoint after the canonical block
            /// at `height`, reconstructing it when no checkpoint is stored
            /// there.
            ///
            /// A reconstruction seeds the trees from the newest stored
            /// checkpoint below `height` and appends each stored compact
            /// block's note commitments up to `height`, one block at a time.
            /// That checkpoint must be no lower than the newest
            /// [`TREE_STATE_CHECKPOINT_STRIDE`](crate::TREE_STATE_CHECKPOINT_STRIDE)
            /// multiple at or below `height`, or the history predecessor when
            /// that is newer, so a reconstruction replays fewer than
            /// `TREE_STATE_CHECKPOINT_STRIDE` blocks. Each block must extend
            /// the one below it and reach the tree sizes its compact block
            /// records, the last must be the canonical block at `height`, and
            /// the resulting frontiers are re-encoded and checked against the
            /// appended trees. A stored checkpoint, whether returned or used
            /// as a seed, is parsed with the official Zcash codec and its
            /// roots re-derived, so the result can seed further tree state. A
            /// missing checkpoint or a failed check is store damage and fails
            /// with [`CanonicalStoreError::PublicationRefused`].
            ///
            /// `network_upgrade_activations` must agree with the schedule this
            /// store admits at every height through `height`, so a node table
            /// that only changes activations above `height` is accepted before
            /// the writer admits it. Without a writer-admitted schedule it must
            /// equal the construction table. Any other table fails with
            /// [`CanonicalStoreError::AdmissionRefused`]. Returns `Ok(None)`
            /// for a height below the history predecessor or above the
            /// visible tip.
            pub fn tree_state_checkpoint_at(
                &self,
                height: BlockHeight,
                network_upgrade_activations: &zinder_core::NetworkUpgradeActivations,
            ) -> Result<Option<CommitmentTreeCheckpoint>, CanonicalStoreError> {
                super::tree_state_replay::read_tree_state_checkpoint_at(
                    self,
                    height,
                    network_upgrade_activations,
                )
            }

            /// Finds how the tree state at `height` is answered: by the
            /// checkpoint stored there, or by a reconstruction from the
            /// newest stored checkpoint below it.
            ///
            /// Applies every check of [`Self::tree_state_checkpoint_at`]
            /// that precedes the replay, with the same errors, and returns
            /// `Ok(None)` for the same heights, except that a checkpoint
            /// stored at `height` is read as
            /// [`Self::tree_state_checkpoint_at_or_before`] reads it. A
            /// replay's seed is checked in full. Pass a
            /// [`TreeStateLookup::Replay`] to [`Self::replay_tree_state`] to
            /// reconstruct it without reading the seed again.
            pub fn tree_state_lookup(
                &self,
                height: BlockHeight,
                network_upgrade_activations: &zinder_core::NetworkUpgradeActivations,
            ) -> Result<Option<TreeStateLookup>, CanonicalStoreError> {
                super::tree_state_replay::lookup_tree_state(
                    self,
                    height,
                    network_upgrade_activations,
                )
            }

            /// Looks up a wallet checkpoint while the caller still needs its validated seed.
            /// Checks cancellation before reading the row and again before decoding it.
            pub fn tree_state_lookup_until_cancelled(
                &self,
                height: BlockHeight,
                network_upgrade_activations: &zinder_core::NetworkUpgradeActivations,
                cancelled: impl Fn() -> bool,
            ) -> Result<Option<TreeStateLookup>, CanonicalStoreError> {
                super::tree_state_replay::lookup_until_cancelled(
                    self,
                    height,
                    network_upgrade_activations,
                    cancelled,
                )
            }

            /// Reconstructs the tree state a [`Self::tree_state_lookup`]
            /// found no checkpoint for, applying every check of
            /// [`Self::tree_state_checkpoint_at`], and reports how it was
            /// answered.
            ///
            /// Replays from the seed the lookup read. When this store shows
            /// another visible chain than the store that issued `replay`, or
            /// `replay` was built with [`TreeStateReplay::new`] rather than
            /// issued by a lookup, it looks [`TreeStateReplay::height`] up
            /// again as [`Self::tree_state_lookup`] does, including
            /// `Ok(None)`, and answers from the checkpoint it stores there or
            /// by a replay from the seed it reads itself, which the outcome
            /// names.
            pub fn replay_tree_state(
                &self,
                replay: &TreeStateReplay,
                network_upgrade_activations: &zinder_core::NetworkUpgradeActivations,
            ) -> Result<Option<TreeStateReplayOutcome>, CanonicalStoreError> {
                super::tree_state_replay::replay_tree_state(
                    self,
                    replay,
                    network_upgrade_activations,
                )
            }

            /// Reconstructs a frontier while a caller still needs it. Checks for
            /// cancellation before starting and after each replayed block.
            pub fn replay_tree_state_until_cancelled(
                &self,
                replay: &TreeStateReplay,
                activations: &zinder_core::NetworkUpgradeActivations,
                cancelled: impl Fn() -> bool,
            ) -> Result<Option<TreeStateReplayOutcome>, CanonicalStoreError> {
                super::tree_state_replay::replay_until_cancelled(
                    self,
                    replay,
                    activations,
                    cancelled,
                )
            }

            /// Batch-reads stored final note-commitment roots from
            /// `tree_state_checkpoint` rows across an inclusive height range.
            ///
            /// The output preserves range order and contains one entry per
            /// requested height, up to [`crate::MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS`].
            /// A height outside retained history (below the
            /// first retained block or above the visible tip) or without a
            /// stored checkpoint row returns `None`. For each stored row inside
            /// retained history, this checks the exact framing of every present
            /// frontier without decoding or hashing tree nodes, verifies that
            /// the present pools match `network_upgrade_activations` at that
            /// height, and verifies that the row's block time matches the
            /// canonical block header at that height; any mismatch fails closed
            /// as store damage ([`CanonicalStoreError::PublicationRefused`]).
            ///
            /// `network_upgrade_activations` must agree with the schedule this
            /// store admits through `range.end`; otherwise the read fails with
            /// [`CanonicalStoreError::AdmissionRefused`]. The returned roots carry
            /// no frontier bytes and cannot seed a commitment-tree accumulator.
            pub fn stored_final_note_commitment_roots_in_range(
                &self,
                range: BlockHeightRange,
                network_upgrade_activations: &NetworkUpgradeActivations,
            ) -> Result<Vec<Option<BlockFinalNoteCommitmentRoots>>, CanonicalStoreError> {
                read_stored_final_note_commitment_roots_in_range(
                    self,
                    range,
                    network_upgrade_activations,
                )
            }

            /// Reads the contiguous subtree-root rows requested by `range`.
            ///
            /// In a store loaded with retained-range coverage, an absent root
            /// that the history predecessor's trees already completed fails
            /// with [`CanonicalStoreError::SubtreeRootUnavailable`]. Any other
            /// absent root, including every root of a complete-prefix store,
            /// is a publication refusal.
            pub fn subtree_roots(
                &self,
                range: SubtreeRootRange,
            ) -> Result<Vec<SubtreeRootArtifact>, CanonicalStoreError> {
                read_subtree_roots(self, range)
            }
        }
    };
}

impl_canonical_typed_reads!(RocksDbCanonicalStore);
impl_canonical_typed_reads!(RocksDbCanonicalSecondary);

impl RocksDbCanonicalSecondary {
    /// Reads ascending blocks and their original stored lengths for a derived history segment.
    /// The byte budget includes the first row even when that row alone exceeds it.
    ///
    /// # Errors
    ///
    /// Returns the same retention, missing-row, and decoding errors as
    /// [`Self::compact_blocks_in_range_up_to`].
    pub fn compact_block_rows_with_lengths(
        &self,
        range: BlockHeightRange,
        max_bytes: usize,
    ) -> Result<Vec<(CompactBlockArtifact, usize)>, CanonicalStoreError> {
        read_compact_blocks_up_to(
            self,
            range,
            HeightOrder::Ascending,
            max_bytes,
            |block, len| (block, len),
        )
    }
}

fn read_chain_epoch(store: &impl CanonicalServingRead) -> Result<ChainEpoch, CanonicalStoreError> {
    read_chain_epoch_at(store, store.serving_ready_evidence().visible_epoch)
}

fn read_chain_epoch_at(
    store: &impl CanonicalServingRead,
    epoch_id: zinder_core::ChainEpochId,
) -> Result<ChainEpoch, CanonicalStoreError> {
    let encoded = read_optional(
        store,
        CHAIN_EPOCH_COLUMN_FAMILY,
        &epoch_id.value().to_be_bytes(),
        "chain epoch read",
    )?
    .ok_or_else(|| CanonicalStoreError::CanonicalEpochNotRetained {
        epoch_id: epoch_id.value(),
    })?;
    decode_chain_epoch(store.serving_network(), epoch_id, &encoded)
}

pub(super) fn read_block_header_at(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
) -> Result<Option<BlockHeaderArtifact>, CanonicalStoreError> {
    read_optional(
        store,
        BLOCK_HEADER_COLUMN_FAMILY,
        &encode_block_position(height),
        "block header read",
    )?
    .map(|encoded| decode_block_header(height, &encoded))
    .transpose()
}

fn read_block_hash_lookup(
    store: &impl CanonicalServingRead,
    block_hash: BlockHash,
) -> Result<BlockHashLookup, CanonicalStoreError> {
    let Some(encoded_height) = read_optional(
        store,
        BLOCK_HASH_INDEX_COLUMN_FAMILY,
        &encode_internal_block_hash(block_hash),
        "block hash index read",
    )?
    else {
        return Ok(BlockHashLookup::NotIndexed);
    };
    if encoded_height.len() != size_of::<u32>() {
        return Err(CanonicalStoreError::publication(
            "block hash index height is not the exact version-1 value",
        ));
    }
    let height = BlockHeight::new(read_u32_be(&encoded_height, 0)?);
    if height > read_chain_epoch(store)?.visible_tip_height {
        return Ok(BlockHashLookup::NotInBestChain);
    }
    match read_block_header_at(store, height)? {
        Some(header) if header.block_hash == block_hash => {
            Ok(BlockHashLookup::Resolved(BlockId::new(height, block_hash)))
        }
        Some(_) | None => Ok(BlockHashLookup::NotInBestChain),
    }
}

pub(super) fn read_compact_block_at(
    db: &DB,
    height: BlockHeight,
) -> Result<Option<CompactBlockArtifact>, CanonicalStoreError> {
    let family = column_family(db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    db.get_cf(&family, encode_block_position(height))
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "compact block read",
            source,
        })?
        .map(|encoded_compact_block| decode_compact_block_row(height, &encoded_compact_block))
        .transpose()
}

fn read_compact_blocks_in_range(
    store: &impl CanonicalServingRead,
    range: zinder_core::BlockHeightRange,
) -> Result<Vec<CompactBlockArtifact>, CanonicalStoreError> {
    let mut blocks = Vec::new();
    visit_compact_block_batches(
        store,
        range,
        COMPACT_BLOCK_MULTI_GET_BATCH_SIZE,
        |compact_block| {
            blocks.push(compact_block);
            Ok(())
        },
    )?;
    Ok(blocks)
}

/// Height order of a byte-bounded compact-block read.
///
/// The read starts at the end of its range this order visits first, so the
/// blocks it keeps under its byte budget are the ones a stream in this order
/// sends next.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeightOrder {
    /// From the lowest height up.
    Ascending,
    /// From the highest height down.
    Descending,
}

/// Most rows one batch of a byte-bounded compact-block read fetches.
const BOUNDED_COMPACT_BLOCK_BATCH_SIZE: usize = 16;

fn read_compact_blocks_up_to<Row>(
    store: &impl CanonicalServingRead,
    range: BlockHeightRange,
    order: HeightOrder,
    max_bytes: usize,
    project: impl Fn(CompactBlockArtifact, usize) -> Row,
) -> Result<Vec<Row>, CanonicalStoreError> {
    require_retained_compact_blocks(store, range)?;
    let mut unread = range.start.value()..=range.end.value();
    let mut next_height = || {
        match order {
            HeightOrder::Ascending => unread.next(),
            HeightOrder::Descending => unread.next_back(),
        }
        .map(BlockHeight::new)
    };
    let mut blocks = Vec::new();
    let mut kept_bytes = 0_usize;
    let mut largest_row_bytes = 0_usize;
    let is_budget_spent =
        |blocks: &[Row], kept_bytes: usize| !blocks.is_empty() && kept_bytes >= max_bytes;
    'batches: while !is_budget_spent(&blocks, kept_bytes) {
        let batch_len = bounded_batch_len(max_bytes.saturating_sub(kept_bytes), largest_row_bytes);
        let batch = std::iter::from_fn(&mut next_height)
            .take(batch_len)
            .collect::<Vec<_>>();
        if batch.is_empty() {
            break;
        }
        let rows = fetch_compact_block_rows(store, &batch)?;
        for (height, row) in batch.iter().copied().zip(rows) {
            if is_budget_spent(&blocks, kept_bytes) {
                break 'batches;
            }
            let encoded_compact_block = present_compact_row(height, row)?;
            let row_bytes = encoded_compact_block.len();
            if !blocks.is_empty() && kept_bytes.saturating_add(row_bytes) > max_bytes {
                break 'batches;
            }
            kept_bytes = kept_bytes.saturating_add(row_bytes);
            largest_row_bytes = largest_row_bytes.max(row_bytes);
            blocks.push(project(
                decode_compact_block_row(height, &encoded_compact_block)?,
                row_bytes,
            ));
        }
    }
    if order == HeightOrder::Descending {
        blocks.reverse();
    }
    Ok(blocks)
}

/// Passes each compact block of `range` to `visit` in ascending height order,
/// reading rows in multi-get batches of at most
/// [`BOUNDED_COMPACT_BLOCK_BATCH_SIZE`], so only one batch of rows is held at
/// a time.
///
/// A height outside retained history fails with
/// [`CanonicalStoreError::ArtifactUnavailable`] before any row is read, and a
/// missing row inside it fails as store damage.
pub(super) fn visit_compact_blocks_in_range(
    store: &impl CanonicalServingRead,
    range: BlockHeightRange,
    visit: impl FnMut(CompactBlockArtifact) -> Result<(), CanonicalStoreError>,
) -> Result<(), CanonicalStoreError> {
    visit_compact_block_batches(store, range, BOUNDED_COMPACT_BLOCK_BATCH_SIZE, visit)
}

/// Passes each compact block of `range` to `visit` in ascending height order,
/// reading rows in multi-get batches of at most `batch_len`.
///
/// A height outside retained history fails with
/// [`CanonicalStoreError::ArtifactUnavailable`] before any row is read, and a
/// missing row inside it fails as store damage.
fn visit_compact_block_batches(
    store: &impl CanonicalServingRead,
    range: BlockHeightRange,
    batch_len: usize,
    mut visit: impl FnMut(CompactBlockArtifact) -> Result<(), CanonicalStoreError>,
) -> Result<(), CanonicalStoreError> {
    require_retained_compact_blocks(store, range)?;
    let mut heights = range.into_iter();
    loop {
        let batch = heights.by_ref().take(batch_len).collect::<Vec<_>>();
        if batch.is_empty() {
            return Ok(());
        }
        let rows = fetch_compact_block_rows(store, &batch)?;
        for (height, row) in batch.iter().copied().zip(rows) {
            let encoded_compact_block = present_compact_row(height, row)?;
            visit(decode_compact_block_row(height, &encoded_compact_block)?)?;
        }
    }
}

/// Rows the next batch of a byte-bounded read fetches.
///
/// That is as many as `remaining_bytes` holds at the size of the largest row
/// read so far, from one to [`BOUNDED_COMPACT_BLOCK_BATCH_SIZE`], and one
/// before any row is read.
///
/// A batch pins every row it fetches until the read has walked it, including
/// the rows past the point where the budget stops the read. Sizing each batch
/// to the budget left keeps those rows within that budget as long as no row is
/// larger than the largest already read.
fn bounded_batch_len(remaining_bytes: usize, largest_row_bytes: usize) -> usize {
    remaining_bytes
        .checked_div(largest_row_bytes)
        .unwrap_or(1)
        .clamp(1, BOUNDED_COMPACT_BLOCK_BATCH_SIZE)
}

/// Fails with [`CanonicalStoreError::ArtifactUnavailable`] naming the lowest
/// height of `range` below the first retained block or above the visible tip.
///
/// A range read checks this before it reads any row, so a row stored outside
/// that span, such as one above the visible tip of the admitted epoch, is
/// never served.
fn require_retained_compact_blocks(
    store: &impl CanonicalServingRead,
    range: BlockHeightRange,
) -> Result<(), CanonicalStoreError> {
    if let Some(height) = first_unretained_height(range, retained_compact_block_span(store)) {
        return Err(CanonicalStoreError::ArtifactUnavailable {
            family: crate::ArtifactFamily::CompactBlock,
            height,
        });
    }
    Ok(())
}

/// Heights from the first retained block through the visible tip.
fn retained_compact_block_span(store: &impl CanonicalServingRead) -> BlockHeightRange {
    let ready_evidence = store.serving_ready_evidence();
    BlockHeightRange::inclusive(
        ready_evidence.first_retained_block.height,
        ready_evidence.visible_tip.height,
    )
}

/// Returns the lowest height of `range` outside `retained_span`, the height
/// an ascending read of the whole range finds absent first.
fn first_unretained_height(
    range: BlockHeightRange,
    retained_span: BlockHeightRange,
) -> Option<BlockHeight> {
    if range.start > range.end {
        None
    } else if !(retained_span.start..=retained_span.end).contains(&range.start) {
        Some(range.start)
    } else if range.end > retained_span.end {
        retained_span.end.next()
    } else {
        None
    }
}

/// One fetched compact-block row: absent, present, or the lookup's failure.
type CompactBlockRow<'db> = Result<Option<rust_rocksdb::DBPinnableSlice<'db>>, rust_rocksdb::Error>;

/// Fetches the `compact_block` rows of one batch of heights with one multi-get,
/// in batch order, reading no other family.
fn fetch_compact_block_rows<'db>(
    store: &'db impl CanonicalServingRead,
    heights: &[BlockHeight],
) -> Result<Vec<CompactBlockRow<'db>>, CanonicalStoreError> {
    let db = &store.serving_open().db;
    let family = column_family(db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    let keys = heights
        .iter()
        .copied()
        .map(encode_block_position)
        .collect::<Vec<_>>();
    Ok(db.batched_multi_get_cf_slice(
        &family,
        keys.iter().map(<[u8; 4]>::as_slice),
        heights.is_sorted(),
    ))
}

/// Returns a fetched row, failing on its lookup error or, since the range was
/// checked against retained history first, as store damage when it is absent.
fn present_compact_row(
    height: BlockHeight,
    row: CompactBlockRow<'_>,
) -> Result<rust_rocksdb::DBPinnableSlice<'_>, CanonicalStoreError> {
    row.map_err(|source| CanonicalStoreError::RocksDbOperation {
        operation: "compact block read",
        source,
    })?
    .ok_or_else(|| missing_compact_block_row(height))
}

/// Decodes one compact-block row and checks it holds the block at its key's
/// height.
///
/// The row is not checked against the `block_header` row: construction and
/// every live transition write the two only after checking that they name the
/// same block, so a serving read takes the compact block on its own.
fn decode_compact_block_row(
    height: BlockHeight,
    encoded_compact_block: &[u8],
) -> Result<CompactBlockArtifact, CanonicalStoreError> {
    let compact_block =
        crate::decode_compact_block_artifact(encoded_compact_block).map_err(|_| {
            CanonicalStoreError::publication(format!(
                "compact block at height {} is invalid",
                height.value()
            ))
        })?;
    if compact_block.height() != height {
        return Err(CanonicalStoreError::publication(
            "compact block row has the wrong height",
        ));
    }
    Ok(compact_block)
}

/// Reports a missing compact-block row at a height inside retained history.
///
/// Publication and live commits write a row for every retained height, so
/// absence there is store damage.
fn missing_compact_block_row(height: BlockHeight) -> CanonicalStoreError {
    CanonicalStoreError::publication(format!(
        "compact block at height {} is absent",
        height.value()
    ))
}

fn read_transaction_location(
    store: &impl CanonicalServingRead,
    transaction_id: TransactionId,
) -> Result<Option<TransactionLocation>, CanonicalStoreError> {
    read_optional(
        store,
        TRANSACTION_LOCATION_COLUMN_FAMILY,
        &encode_internal_transaction_id(transaction_id),
        "transaction location read",
    )?
    .map(|encoded| decode_transaction_location(transaction_id, &encoded))
    .transpose()
}

fn read_transaction_locations(
    store: &impl CanonicalServingRead,
    transaction_ids: &[TransactionId],
) -> Result<Vec<Option<TransactionLocation>>, CanonicalStoreError> {
    let mut ordered = transaction_ids
        .iter()
        .enumerate()
        .map(|(index, transaction_id)| (encode_internal_transaction_id(*transaction_id), index))
        .collect::<Vec<_>>();
    ordered.sort_unstable();
    let family = column_family(&store.serving_open().db, TRANSACTION_LOCATION_COLUMN_FAMILY)?;
    let rows = store.serving_open().db.batched_multi_get_cf_slice(
        &family,
        ordered.iter().map(|(key, _)| key.as_slice()),
        true,
    );
    let mut locations = vec![None; transaction_ids.len()];
    for ((_, index), row) in ordered.iter().zip(rows) {
        let encoded = row.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "transaction location batch read",
            source,
        })?;
        if let (Some(encoded), Some(transaction_id), Some(slot)) = (
            encoded,
            transaction_ids.get(*index),
            locations.get_mut(*index),
        ) {
            *slot = Some(decode_transaction_location(*transaction_id, &encoded)?);
        }
    }
    Ok(locations)
}

fn read_transaction_location_or_proven_absence(
    store: &impl CanonicalServingRead,
    transaction_id: TransactionId,
) -> Result<Option<TransactionLocation>, CanonicalStoreError> {
    let location = read_transaction_location(store, transaction_id)?;
    // Canonical history starts at height 1 because the genesis block is never
    // indexed, so only a store that retains height 1 can turn a miss into an
    // absence. The genesis coinbase is therefore reported absent.
    let history_bounds = store.serving_build_plan().history_bounds();
    if location.is_none() && history_bounds.intentionally_excludes(BlockHeight::new(1)) {
        return Err(CanonicalStoreError::CanonicalHistoryUnavailable {
            first_available_height: history_bounds.first_available_height(),
        });
    }
    Ok(location)
}

fn read_transaction_blob(
    store: &impl CanonicalServingRead,
    location: TransactionLocation,
) -> Result<Option<TransactionBlobArtifact>, CanonicalStoreError> {
    if read_transaction_location(store, location.transaction_id)? != Some(location) {
        return Ok(None);
    }
    Ok(read_optional(
        store,
        TRANSACTION_BLOB_COLUMN_FAMILY,
        &encode_transaction_position(location.block_height, location.tx_index_in_block),
        "transaction blob read",
    )?
    .map(|raw_transaction_bytes| TransactionBlobArtifact::new(location, raw_transaction_bytes)))
}

fn read_block_blob_at(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
) -> Result<Option<BlockBlobArtifact>, CanonicalStoreError> {
    read_block_blobs_in_range(store, BlockHeightRange::inclusive(height, height))?
        .into_iter()
        .next()
        .ok_or_else(|| CanonicalStoreError::publication("single-height block blob read is empty"))
}

fn read_block_blobs_in_range(
    store: &impl CanonicalServingRead,
    range: BlockHeightRange,
) -> Result<Vec<Option<BlockBlobArtifact>>, CanonicalStoreError> {
    let mut blobs = Vec::new();
    let mut heights = Vec::with_capacity(BLOCK_BLOB_MULTI_GET_BATCH_SIZE);
    for height in range {
        heights.push(height);
        if heights.len() == BLOCK_BLOB_MULTI_GET_BATCH_SIZE {
            append_block_blob_batch(store, &heights, &mut blobs)?;
            heights.clear();
        }
    }
    append_block_blob_batch(store, &heights, &mut blobs)?;
    Ok(blobs)
}

fn append_block_blob_batch(
    store: &impl CanonicalServingRead,
    heights: &[BlockHeight],
    blobs: &mut Vec<Option<BlockBlobArtifact>>,
) -> Result<(), CanonicalStoreError> {
    if heights.is_empty() {
        return Ok(());
    }
    let block_blob_family = column_family(&store.serving_open().db, BLOCK_BLOB_COLUMN_FAMILY)?;
    let header_family = column_family(&store.serving_open().db, BLOCK_HEADER_COLUMN_FAMILY)?;
    let keys = heights
        .iter()
        .copied()
        .map(encode_block_position)
        .collect::<Vec<_>>();
    let mut inputs = Vec::with_capacity(keys.len().saturating_mul(2));
    for key in &keys {
        inputs.push((&block_blob_family, key.as_slice()));
        inputs.push((&header_family, key.as_slice()));
    }
    let mut rows = store.serving_open().db.multi_get_cf(inputs).into_iter();

    for height in heights {
        let raw_block_bytes = rows
            .next()
            .ok_or_else(|| CanonicalStoreError::publication("block blob multi-get is truncated"))?
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "block blob range read",
                source,
            })?;
        let encoded_header = rows
            .next()
            .ok_or_else(|| CanonicalStoreError::publication("block header multi-get is truncated"))?
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "block blob header range read",
                source,
            })?;
        let Some(raw_block_bytes) = raw_block_bytes else {
            blobs.push(None);
            continue;
        };
        let encoded_header = encoded_header.ok_or_else(|| {
            CanonicalStoreError::publication(format!(
                "retained block blob at height {} has no canonical header",
                height.value()
            ))
        })?;
        let header = decode_block_header(*height, &encoded_header)?;
        blobs.push(Some(BlockBlobArtifact::new(
            *height,
            header.block_hash,
            header.parent_hash,
            raw_block_bytes,
        )));
    }
    Ok(())
}

/// Reads and revalidates the checkpoint stored at exactly `height` with one
/// point read.
pub(super) fn read_stored_tree_state_checkpoint_at(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
) -> Result<Option<CommitmentTreeCheckpoint>, CanonicalStoreError> {
    read_tree_state_row_at(store, height)?
        .map(|encoded| decode_stored_tree_state_checkpoint(store, height, &encoded))
        .transpose()
}

fn read_tree_state_row_at(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
) -> Result<Option<Vec<u8>>, CanonicalStoreError> {
    read_optional(
        store,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
        &encode_block_position(height),
        "tree-state checkpoint read",
    )
}

/// One stored tree-state checkpoint row, not yet decoded.
pub(super) struct TreeStateCheckpointRow {
    /// Height the row is stored at.
    pub(super) height: BlockHeight,
    /// The stored checkpoint value.
    pub(super) encoded: Box<[u8]>,
}

/// Reads the newest checkpoint row stored at or below `height`.
pub(super) fn read_tree_state_checkpoint_row_at_or_before(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
) -> Result<Option<TreeStateCheckpointRow>, CanonicalStoreError> {
    let family = column_family(
        &store.serving_open().db,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    )?;
    let start = encode_block_position(height);
    let mut rows = store
        .serving_open()
        .db
        .iterator_cf(&family, IteratorMode::From(&start, Direction::Reverse));
    let Some(row) = rows.next() else {
        return Ok(None);
    };
    let (key, encoded) = row.map_err(|source| CanonicalStoreError::RocksDbOperation {
        operation: "tree-state checkpoint read",
        source,
    })?;
    Ok(Some(TreeStateCheckpointRow {
        height: decode_height_key(&key)?,
        encoded,
    }))
}

/// Decodes and revalidates the checkpoint row stored at `checkpoint_height`.
pub(super) fn decode_stored_tree_state_checkpoint(
    store: &impl CanonicalServingRead,
    checkpoint_height: BlockHeight,
    encoded: &[u8],
) -> Result<CommitmentTreeCheckpoint, CanonicalStoreError> {
    let (block_time_seconds, frontiers) = decode_tree_state_checkpoint(encoded)
        .map_err(|source| CanonicalStoreError::publication(source.to_string()))?;
    let block_id = stored_tree_state_block_id(store, checkpoint_height, encoded)?;
    Ok(CommitmentTreeCheckpoint::new(
        block_id,
        block_time_seconds,
        frontiers,
    ))
}

/// Decodes the checkpoint row stored at `checkpoint_height` for serving,
/// checking only its framing.
pub(super) fn decode_served_tree_state(
    store: &impl CanonicalServingRead,
    checkpoint_height: BlockHeight,
    encoded: &[u8],
) -> Result<ServedTreeState, CanonicalStoreError> {
    let (block_time_seconds, frontiers) = decode_served_tree_state_checkpoint(encoded)
        .map_err(|source| CanonicalStoreError::publication(source.to_string()))?;
    let block_id = stored_tree_state_block_id(store, checkpoint_height, encoded)?;
    Ok(ServedTreeState::new(
        block_id,
        block_time_seconds,
        frontiers,
    ))
}

/// Returns the block the checkpoint row stored at `checkpoint_height` follows.
///
/// The history predecessor has no retained header, so its row must hold
/// exactly the checkpoint the admitted build plan names. Any other row takes
/// its block hash from the canonical header at its height.
fn stored_tree_state_block_id(
    store: &impl CanonicalServingRead,
    checkpoint_height: BlockHeight,
    encoded: &[u8],
) -> Result<BlockId, CanonicalStoreError> {
    let history_predecessor = store.serving_build_plan().history_predecessor();
    if checkpoint_height == history_predecessor.block_id.height {
        if encoded
            != encode_tree_state_checkpoint(
                history_predecessor.block_time_seconds,
                &history_predecessor.frontiers,
            )
        {
            return Err(CanonicalStoreError::publication(
                "tree-state history predecessor does not match the admitted build plan",
            ));
        }
        return Ok(history_predecessor.block_id);
    }
    let header = read_block_header_at(store, checkpoint_height)?.ok_or_else(|| {
        CanonicalStoreError::publication("tree-state checkpoint header is absent")
    })?;
    Ok(BlockId::new(checkpoint_height, header.block_hash))
}

fn read_stored_final_note_commitment_roots_in_range(
    store: &impl CanonicalServingRead,
    range: BlockHeightRange,
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<Vec<Option<BlockFinalNoteCommitmentRoots>>, CanonicalStoreError> {
    validate_incremental_replay_range_bound(range)?;
    if range.start > range.end {
        return Ok(Vec::new());
    }
    refuse_unadmitted_activations(store, range.end, network_upgrade_activations)?;
    let retained = retained_compact_block_span(store);
    let mut roots = vec![None; range.into_iter().len()];
    let overlap_start = range.start.max(retained.start);
    let overlap_end = range.end.min(retained.end);
    if overlap_start > overlap_end {
        return Ok(roots);
    }
    let mut heights = BlockHeightRange::inclusive(overlap_start, overlap_end).into_iter();
    loop {
        let batch = heights
            .by_ref()
            .take(TREE_STATE_ROOTS_MULTI_GET_BATCH_SIZE)
            .collect::<Vec<_>>();
        if batch.is_empty() {
            break;
        }
        for block_roots in read_stored_roots_batch(store, &batch, network_upgrade_activations)? {
            let offset = usize::try_from(block_roots.height.value() - range.start.value())
                .map_err(|_| {
                    CanonicalStoreError::publication("block height offset exceeds usize")
                })?;
            let slot = roots.get_mut(offset).ok_or_else(|| {
                CanonicalStoreError::publication("stored roots range offset is out of bounds")
            })?;
            *slot = Some(block_roots);
        }
    }
    Ok(roots)
}

fn read_stored_roots_batch(
    store: &impl CanonicalServingRead,
    heights: &[BlockHeight],
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<Vec<BlockFinalNoteCommitmentRoots>, CanonicalStoreError> {
    let db = &store.serving_open().db;
    let checkpoint_family = column_family(db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    let keys = heights
        .iter()
        .copied()
        .map(encode_block_position)
        .collect::<Vec<_>>();
    let checkpoint_rows = db.batched_multi_get_cf_slice(
        &checkpoint_family,
        keys.iter().map(<[u8; 4]>::as_slice),
        true,
    );
    let mut decoded = Vec::new();
    for (height, row) in heights.iter().copied().zip(checkpoint_rows) {
        let encoded = row.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "tree-state checkpoint roots range read",
            source,
        })?;
        let Some(encoded) = encoded else {
            continue;
        };
        let stored = decode_checked_stored_roots(height, &encoded, network_upgrade_activations)?;
        decoded.push((height, stored));
    }
    if decoded.is_empty() {
        return Ok(Vec::new());
    }
    resolve_stored_roots_headers(store, &decoded)
}

fn decode_checked_stored_roots(
    height: BlockHeight,
    encoded: &[u8],
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<StoredTreeStateRoots, CanonicalStoreError> {
    let stored = decode_stored_tree_state_roots(encoded).map_err(|source| {
        CanonicalStoreError::publication(format!(
            "tree-state checkpoint at height {} is invalid: {source}",
            height.value()
        ))
    })?;
    if let Some(mismatch) =
        pool_presence_mismatch(network_upgrade_activations, height, |protocol| {
            stored.has_pool(protocol)
        })
    {
        return Err(CanonicalStoreError::publication(format!(
            "tree-state checkpoint at height {} {}",
            height.value(),
            mismatch.describe()
        )));
    }
    Ok(stored)
}

fn resolve_stored_roots_headers(
    store: &impl CanonicalServingRead,
    decoded: &[(BlockHeight, StoredTreeStateRoots)],
) -> Result<Vec<BlockFinalNoteCommitmentRoots>, CanonicalStoreError> {
    let db = &store.serving_open().db;
    let header_family = column_family(db, BLOCK_HEADER_COLUMN_FAMILY)?;
    let header_keys = decoded
        .iter()
        .map(|(height, _)| encode_block_position(*height))
        .collect::<Vec<_>>();
    let header_rows = db.batched_multi_get_cf_slice(
        &header_family,
        header_keys.iter().map(<[u8; 4]>::as_slice),
        true,
    );
    let mut resolved = Vec::with_capacity(decoded.len());
    for ((height, stored), header_row) in decoded.iter().copied().zip(header_rows) {
        let encoded_header = header_row
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "tree-state checkpoint header range read",
                source,
            })?
            .ok_or_else(|| {
                CanonicalStoreError::publication(format!(
                    "tree-state checkpoint header at height {} is absent",
                    height.value()
                ))
            })?;
        let header = decode_block_header(height, &encoded_header)?;
        if header.block_time != i64::from(stored.block_time_seconds) {
            return Err(CanonicalStoreError::publication(format!(
                "tree-state checkpoint at height {} has block time {} differing from canonical block time {}",
                height.value(),
                stored.block_time_seconds,
                header.block_time,
            )));
        }
        resolved.push(stored.into_block_roots(height, header.block_hash));
    }
    Ok(resolved)
}

fn read_subtree_roots(
    store: &impl CanonicalServingRead,
    range: SubtreeRootRange,
) -> Result<Vec<SubtreeRootArtifact>, CanonicalStoreError> {
    let db = &store.serving_open().db;
    let family = column_family(db, SUBTREE_ROOT_COLUMN_FAMILY)?;
    let coverage_start = super::stored_set::first_subtree_root_index(
        store.serving_subtree_root_coverage(),
        store
            .serving_build_plan()
            .history_predecessor()
            .tip_metadata(),
        range.protocol,
    );
    let mut indices = range.into_iter();
    let mut roots = Vec::new();
    let read_point = |index| {
        let key = encode_subtree_root_position(range.protocol, index);
        let row = db.get_cf(&family, key);
        decode_subtree_root_row(range.protocol, index, coverage_start, &key, row)
    };
    // Stop at the first unavailable history row before fetching a complete page.
    for (_, index) in (range.start_index.value()..coverage_start).zip(indices.by_ref()) {
        roots.push(read_point(index)?);
    }
    while indices.len() >= SUBTREE_ROOT_PAGE_SIZE {
        let mut keys = Vec::with_capacity(SUBTREE_ROOT_PAGE_SIZE);
        keys.extend(
            indices
                .by_ref()
                .take(SUBTREE_ROOT_PAGE_SIZE)
                .map(|index| (index, encode_subtree_root_position(range.protocol, index))),
        );
        // Within a pool, big-endian indices are in the column family's bytewise order.
        let rows = db.batched_multi_get_cf_slice(&family, keys.iter().map(|(_, key)| key), true);
        for ((index, key), row) in keys.into_iter().zip(rows) {
            roots.push(decode_subtree_root_row(
                range.protocol,
                index,
                coverage_start,
                &key,
                row,
            )?);
        }
    }
    for index in indices {
        roots.push(read_point(index)?);
    }
    Ok(roots)
}

fn decode_subtree_root_row(
    protocol: ShieldedProtocol,
    subtree_index: SubtreeRootIndex,
    coverage_start: u32,
    key: &[u8],
    row: Result<Option<impl AsRef<[u8]>>, rust_rocksdb::Error>,
) -> Result<SubtreeRootArtifact, CanonicalStoreError> {
    let encoded = row
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "subtree-root read",
            source,
        })?
        .ok_or_else(|| {
            if subtree_index.value() < coverage_start {
                CanonicalStoreError::SubtreeRootUnavailable {
                    protocol,
                    subtree_index,
                }
            } else {
                CanonicalStoreError::publication(format!(
                    "advertised {protocol:?} subtree root {} is absent",
                    subtree_index.value()
                ))
            }
        })?;
    decode_subtree_root(key, encoded.as_ref())
}

fn read_optional(
    store: &impl CanonicalServingRead,
    family_name: &'static str,
    key: &[u8],
    operation: &'static str,
) -> Result<Option<Vec<u8>>, CanonicalStoreError> {
    let family = column_family(&store.serving_open().db, family_name)?;
    store
        .serving_open()
        .db
        .get_cf(&family, key)
        .map_err(|source| CanonicalStoreError::RocksDbOperation { operation, source })
}

fn decode_chain_epoch(
    network: Network,
    epoch_id: zinder_core::ChainEpochId,
    encoded: &[u8],
) -> Result<ChainEpoch, CanonicalStoreError> {
    if encoded.len() != CHAIN_EPOCH_VALUE_BYTES || encoded.first() != Some(&1) {
        return Err(CanonicalStoreError::publication(
            "chain epoch is not the exact version-1 value",
        ));
    }
    Ok(ChainEpoch {
        id: epoch_id,
        network,
        visible_tip_height: BlockHeight::new(read_u32_le(encoded, 1)?),
        visible_tip_hash: BlockHash::from_bytes(read_array(encoded, 5)?),
        settled_tip_height: BlockHeight::new(read_u32_le(encoded, 37)?),
        settled_tip_hash: BlockHash::from_bytes(read_array(encoded, 41)?),
        // The epoch row stores no version, so an epoch reports the artifact
        // schema version of this binary, the value `ServerInfo` reports, not
        // the store layout version.
        artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
        tip_metadata: ChainTipMetadata::new(
            read_u32_le(encoded, 73)?,
            read_u32_le(encoded, 77)?,
            read_u32_le(encoded, 81)?,
        ),
        created_at: UnixTimestampMillis::new(read_u64_le(encoded, 85)?),
    })
}

pub(super) fn decode_block_header(
    height: BlockHeight,
    encoded: &[u8],
) -> Result<BlockHeaderArtifact, CanonicalStoreError> {
    if encoded.len() != BLOCK_HEADER_VALUE_LEN {
        return Err(CanonicalStoreError::publication(
            "block header is not the exact version-1 value",
        ));
    }
    Ok(BlockHeaderArtifact::new(
        height,
        BlockHash::from_bytes(read_array(encoded, 0)?),
        BlockHash::from_bytes(read_array(encoded, 32)?),
        read_array(encoded, 64)?,
        read_array(encoded, 96)?,
        read_i64_le(encoded, 128)?,
        read_u32_le(encoded, 136)?,
        read_array(encoded, 140)?,
        read_u32_le(encoded, 172)?,
        read_u64_le(encoded, 176)?,
    ))
}

fn decode_transaction_location(
    transaction_id: TransactionId,
    encoded: &[u8],
) -> Result<TransactionLocation, CanonicalStoreError> {
    if encoded.len() != TRANSACTION_LOCATION_VALUE_BYTES {
        return Err(CanonicalStoreError::publication(
            "transaction location is not the exact version-1 value",
        ));
    }
    Ok(TransactionLocation::new(
        transaction_id,
        BlockHeight::new(read_u32_be(encoded, 0)?),
        BlockHash::from_bytes(read_array(encoded, 4)?),
        read_u32_be(encoded, 36)?,
    ))
}

fn decode_height_key(encoded: &[u8]) -> Result<BlockHeight, CanonicalStoreError> {
    if encoded.len() != 4 {
        return Err(CanonicalStoreError::publication(
            "block-height key is not the exact version-1 value",
        ));
    }
    Ok(BlockHeight::new(u32::from_be_bytes(read_array(
        encoded, 0,
    )?)))
}

fn read_i64_le(encoded: &[u8], offset: usize) -> Result<i64, CanonicalStoreError> {
    Ok(i64::from_le_bytes(read_array(encoded, offset)?))
}

fn read_u32_le(encoded: &[u8], offset: usize) -> Result<u32, CanonicalStoreError> {
    Ok(u32::from_le_bytes(read_array(encoded, offset)?))
}

fn read_u32_be(encoded: &[u8], offset: usize) -> Result<u32, CanonicalStoreError> {
    Ok(u32::from_be_bytes(read_array(encoded, offset)?))
}

fn read_u64_le(encoded: &[u8], offset: usize) -> Result<u64, CanonicalStoreError> {
    Ok(u64::from_le_bytes(read_array(encoded, offset)?))
}

fn read_array<const LENGTH: usize>(
    encoded: &[u8],
    offset: usize,
) -> Result<[u8; LENGTH], CanonicalStoreError> {
    encoded
        .get(offset..offset.saturating_add(LENGTH))
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| CanonicalStoreError::publication("version-1 serving value is truncated"))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use zinder_core::BlockHeight;

    use super::{BOUNDED_COMPACT_BLOCK_BATCH_SIZE, bounded_batch_len};
    use crate::{
        CURRENT_ARTIFACT_SCHEMA_VERSION, canonical_store::live_replacement_tests::published_store,
    };

    /// A canonical epoch names the artifact schema its artifacts use, the
    /// version `ServerInfo` reports, not the store's own layout version.
    #[test]
    fn a_chain_epoch_carries_the_artifact_schema_version() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let store = published_store(
            &temporary.path().join("canonical"),
            2,
            BlockHeight::new(5),
            BlockHeight::new(5),
        )?;
        let visible = store.chain_epoch()?;

        assert_eq!(
            visible.artifact_schema_version,
            CURRENT_ARTIFACT_SCHEMA_VERSION
        );
        assert_eq!(
            store.chain_epoch_at(visible.id)?.artifact_schema_version,
            CURRENT_ARTIFACT_SCHEMA_VERSION
        );
        Ok(())
    }

    #[test]
    fn a_bounded_batch_fits_the_budget_left_at_the_largest_row_read() {
        // Before any row is read, a batch fetches one row to learn its size.
        assert_eq!(bounded_batch_len(2 * 1024 * 1024, 0), 1);
        assert_eq!(bounded_batch_len(0, 0), 1);
        // Small rows fill the largest batch.
        assert_eq!(
            bounded_batch_len(2 * 1024 * 1024, 1_000),
            BOUNDED_COMPACT_BLOCK_BATCH_SIZE
        );
        // Rows of about 250 KB fit 8 times in 2 MiB, and 3 times in what is
        // left after 5 of them.
        assert_eq!(bounded_batch_len(2 * 1024 * 1024, 250_000), 8);
        assert_eq!(bounded_batch_len(2 * 1024 * 1024 - 5 * 250_000, 250_000), 3);
        // A row larger than the budget left is still fetched alone, to find
        // where the read stops.
        assert_eq!(bounded_batch_len(100_000, 250_000), 1);
        assert_eq!(bounded_batch_len(0, 250_000), 1);
    }
}
