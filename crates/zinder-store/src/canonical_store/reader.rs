//! Typed serving reads for an admitted canonical store.

use std::mem::size_of;

use crate::{BlockHashLookup, BoundedRocksDbOpen};
use rust_rocksdb::{Direction, IteratorMode};
use zinder_core::{
    ArtifactSchemaVersion, BlockBlobArtifact, BlockHash, BlockHeaderArtifact, BlockHeight,
    BlockHeightRange, BlockId, ChainEpoch, ChainTipMetadata, CommitmentTreeCheckpoint,
    CompactBlockArtifact, Network, SubtreeRootArtifact, SubtreeRootRange, TransactionBlobArtifact,
    TransactionId, TransactionLocation, UnixTimestampMillis,
    wire::{encode_internal_block_hash, encode_internal_transaction_id},
};

use super::{
    CANONICAL_STORE_SCHEMA_VERSION, CanonicalStoreError, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore,
    block_load::{
        BLOCK_HEADER_VALUE_LEN, decode_tree_state_checkpoint, encode_block_position,
        encode_transaction_position,
    },
    publication::column_family,
    rocksdb::{
        BLOCK_BLOB_COLUMN_FAMILY, BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY,
        CHAIN_EPOCH_COLUMN_FAMILY, COMPACT_BLOCK_COLUMN_FAMILY, SUBTREE_ROOT_COLUMN_FAMILY,
        TRANSACTION_BLOB_COLUMN_FAMILY, TRANSACTION_LOCATION_COLUMN_FAMILY,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    },
    subtree_load::{
        CanonicalSubtreeRootLoadCoverage, decode_subtree_root, encode_subtree_root_key,
    },
};

const CHAIN_EPOCH_VALUE_BYTES: usize = 93;
const TRANSACTION_LOCATION_VALUE_BYTES: usize = 40;
const COMPACT_BLOCK_MULTI_GET_BATCH_SIZE: usize = 1_024;
const BLOCK_BLOB_MULTI_GET_BATCH_SIZE: usize = 1_024;

trait CanonicalServingRead {
    fn serving_open(&self) -> &BoundedRocksDbOpen;
    fn serving_network(&self) -> Network;
    fn serving_build_plan(&self) -> &super::CanonicalStoreBuildPlan;
    fn serving_ready_evidence(&self) -> super::CanonicalStoreReadyEvidence;
    fn serving_subtree_root_coverage(&self) -> CanonicalSubtreeRootLoadCoverage;
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
                read_compact_block_at(self, height)
            }

            /// Reads an inclusive compact-block range in ascending height order.
            ///
            /// A requested height below the first retained block or above the
            /// visible tip fails with [`CanonicalStoreError::ArtifactUnavailable`]
            /// naming that height. A missing row inside that span is store
            /// damage and fails as [`CanonicalStoreError::PublicationRefused`].
            pub fn compact_blocks_in_range(
                &self,
                range: zinder_core::BlockHeightRange,
            ) -> Result<Vec<CompactBlockArtifact>, CanonicalStoreError> {
                read_compact_blocks_in_range(self, range)
            }

            /// Reads the canonical location of one transaction.
            pub fn transaction_location(
                &self,
                transaction_id: TransactionId,
            ) -> Result<Option<TransactionLocation>, CanonicalStoreError> {
                read_transaction_location(self, transaction_id)
            }

            /// Reads one retained raw transaction at its authenticated location.
            pub fn transaction_blob(
                &self,
                location: TransactionLocation,
            ) -> Result<Option<TransactionBlobArtifact>, CanonicalStoreError> {
                read_transaction_blob(self, location)
            }

            /// Reads the newest commitment-tree checkpoint at or below `height`.
            pub fn tree_state_checkpoint_at_or_before(
                &self,
                height: BlockHeight,
            ) -> Result<Option<CommitmentTreeCheckpoint>, CanonicalStoreError> {
                read_tree_state_checkpoint_at_or_before(self, height)
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

fn read_block_header_at(
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

fn read_compact_block_at(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
) -> Result<Option<CompactBlockArtifact>, CanonicalStoreError> {
    let Some(encoded_compact_block) = read_optional(
        store,
        COMPACT_BLOCK_COLUMN_FAMILY,
        &encode_block_position(height),
        "compact block read",
    )?
    else {
        return Ok(None);
    };
    let compact_block =
        crate::decode_compact_block_artifact(&encoded_compact_block).map_err(|_| {
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
    Ok(Some(compact_block))
}

fn read_compact_blocks_in_range(
    store: &impl CanonicalServingRead,
    range: zinder_core::BlockHeightRange,
) -> Result<Vec<CompactBlockArtifact>, CanonicalStoreError> {
    let ready_evidence = store.serving_ready_evidence();
    let retained_span = BlockHeightRange::inclusive(
        ready_evidence.first_retained_block.height,
        ready_evidence.visible_tip.height,
    );
    let mut blocks = Vec::new();
    let mut heights = Vec::with_capacity(COMPACT_BLOCK_MULTI_GET_BATCH_SIZE);
    for height in range {
        heights.push(height);
        if heights.len() == COMPACT_BLOCK_MULTI_GET_BATCH_SIZE {
            append_compact_block_batch(store, retained_span, &heights, &mut blocks)?;
            heights.clear();
        }
    }
    append_compact_block_batch(store, retained_span, &heights, &mut blocks)?;
    Ok(blocks)
}

fn append_compact_block_batch(
    store: &impl CanonicalServingRead,
    retained_span: BlockHeightRange,
    heights: &[BlockHeight],
    blocks: &mut Vec<CompactBlockArtifact>,
) -> Result<(), CanonicalStoreError> {
    if heights.is_empty() {
        return Ok(());
    }
    let compact_family = column_family(&store.serving_open().db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    let header_family = column_family(&store.serving_open().db, BLOCK_HEADER_COLUMN_FAMILY)?;
    let keys = heights
        .iter()
        .copied()
        .map(encode_block_position)
        .collect::<Vec<_>>();
    let compact_rows = store.serving_open().db.batched_multi_get_cf_slice(
        &compact_family,
        keys.iter().map(<[u8; 4]>::as_slice),
        true,
    );
    let header_rows = store.serving_open().db.batched_multi_get_cf_slice(
        &header_family,
        keys.iter().map(<[u8; 4]>::as_slice),
        true,
    );

    for ((height, compact_row), header_row) in
        heights.iter().copied().zip(compact_rows).zip(header_rows)
    {
        let encoded_compact_block = compact_row
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "compact block read",
                source,
            })?
            .ok_or_else(|| absent_compact_block(retained_span, height))?;
        let encoded_header = header_row
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "block header read",
                source,
            })?
            .ok_or_else(|| CanonicalStoreError::publication("compact block header is absent"))?;
        let header = decode_block_header(height, &encoded_header)?;
        let compact_block =
            crate::decode_compact_block_artifact(&encoded_compact_block).map_err(|_| {
                CanonicalStoreError::publication(format!(
                    "compact block at height {} is invalid",
                    height.value()
                ))
            })?;
        if compact_block.height() != height || compact_block.block_hash() != header.block_hash {
            return Err(CanonicalStoreError::publication(
                "compact block row does not match its canonical header",
            ));
        }
        blocks.push(compact_block);
    }
    Ok(())
}

/// Classifies a missing compact-block row by the retained span it falls in.
///
/// Outside the span the store never held the height. Inside it, publication
/// and live commits write a row for every height, so absence is store damage.
fn absent_compact_block(
    retained_span: BlockHeightRange,
    height: BlockHeight,
) -> CanonicalStoreError {
    if height < retained_span.start || height > retained_span.end {
        CanonicalStoreError::ArtifactUnavailable {
            family: crate::ArtifactFamily::CompactBlock,
            height,
        }
    } else {
        CanonicalStoreError::publication(format!(
            "compact block at height {} is absent",
            height.value()
        ))
    }
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

fn read_tree_state_checkpoint_at_or_before(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
) -> Result<Option<CommitmentTreeCheckpoint>, CanonicalStoreError> {
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
    let checkpoint_height = decode_height_key(&key)?;
    let (block_time_seconds, frontiers) = decode_tree_state_checkpoint(&encoded)
        .map_err(|source| CanonicalStoreError::publication(source.to_string()))?;
    let history_predecessor = store.serving_build_plan().history_predecessor();
    let block_id = if checkpoint_height == history_predecessor.block_id.height {
        if block_time_seconds != history_predecessor.block_time_seconds
            || frontiers != history_predecessor.frontiers
        {
            return Err(CanonicalStoreError::publication(
                "tree-state history predecessor does not match the admitted build plan",
            ));
        }
        history_predecessor.block_id
    } else {
        let header = read_block_header_at(store, checkpoint_height)?.ok_or_else(|| {
            CanonicalStoreError::publication("tree-state checkpoint header is absent")
        })?;
        BlockId::new(checkpoint_height, header.block_hash)
    };
    Ok(Some(CommitmentTreeCheckpoint::new(
        block_id,
        block_time_seconds,
        frontiers,
    )))
}

fn read_subtree_roots(
    store: &impl CanonicalServingRead,
    range: SubtreeRootRange,
) -> Result<Vec<SubtreeRootArtifact>, CanonicalStoreError> {
    let family = column_family(&store.serving_open().db, SUBTREE_ROOT_COLUMN_FAMILY)?;
    // A complete-prefix load holds every advertised root. A retained-range
    // load holds the roots its retained blocks complete, so only an index the
    // predecessor's trees already completed may be absent.
    let coverage_start = match store.serving_subtree_root_coverage() {
        CanonicalSubtreeRootLoadCoverage::CompletePrefix => 0,
        CanonicalSubtreeRootLoadCoverage::RetainedRange => store
            .serving_build_plan()
            .history_predecessor()
            .tip_metadata()
            .completed_subtree_count(range.protocol),
    };
    range
        .into_iter()
        .map(|subtree_index| {
            let probe = SubtreeRootArtifact::new(
                range.protocol,
                subtree_index,
                zinder_core::SubtreeRootHash::from_bytes([0; 32]),
                BlockHeight::new(0),
                BlockHash::from_bytes([0; 32]),
            );
            let key = encode_subtree_root_key(&probe);
            let encoded = store
                .serving_open()
                .db
                .get_cf(&family, key)
                .map_err(|source| CanonicalStoreError::RocksDbOperation {
                    operation: "subtree-root read",
                    source,
                })?
                .ok_or_else(|| {
                    if subtree_index.value() < coverage_start {
                        CanonicalStoreError::SubtreeRootUnavailable {
                            protocol: range.protocol,
                            subtree_index,
                        }
                    } else {
                        CanonicalStoreError::publication(format!(
                            "advertised {:?} subtree root {} is absent",
                            range.protocol,
                            subtree_index.value()
                        ))
                    }
                })?;
            decode_subtree_root(&key, &encoded)
        })
        .collect::<Result<Vec<_>, _>>()
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
        artifact_schema_version: ArtifactSchemaVersion::new(CANONICAL_STORE_SCHEMA_VERSION),
        tip_metadata: ChainTipMetadata::new(
            read_u32_le(encoded, 73)?,
            read_u32_le(encoded, 77)?,
            read_u32_le(encoded, 81)?,
        ),
        created_at: UnixTimestampMillis::new(read_u64_le(encoded, 85)?),
    })
}

fn decode_block_header(
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
