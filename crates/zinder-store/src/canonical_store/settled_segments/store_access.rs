//! RocksDB-to-settled-segments bridge.
//!
//! Provides `O(log N)` settled routing, split reads, lazy per-module opening,
//! grouped single-decompression raw-tx reads, live-tail conversion, consensus
//! tail-slicing, and segment manifest sync.

use std::{collections::BTreeMap, path::Path, sync::Arc};

use rust_rocksdb::{BoundColumnFamily, DB, DBPinnableSlice, Direction, IteratorMode, ReadOptions};
use zinder_core::{
    BlockBlobArtifact, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId,
    CanonicalBlockFacts, CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion,
    ChainTipMetadata, CompactBlockArtifact, Network, SerializedBytesDigest,
    TransactionBlobArtifact, ValidatedCanonicalBlockReplay, encode_canonical_block_replay,
    wire::{decode_height_key_ascending, decode_internal_block_hash, encode_height_key_ascending},
};

use crate::{
    ArtifactFamily, HeightOrder, RawBlobRetention,
    settled_raw_block_segment::{
        RawBlockTransactionSlice, SettledRawBlockSegmentError, SettledRawBlockSegmentReader,
    },
    settled_shielded_segment::{SettledShieldedSegmentError, SettledShieldedSegmentReader},
    settled_supplement_segment::{
        BlockSupplementRecord, SettledSupplementSegmentError, SettledSupplementSegmentReader,
        reconstruct_canonical_block_facts, reconstruct_compact_block_artifact,
    },
};

use super::super::live_tail::{LiveTailBlockRecord, LiveTailEncodeOptions, LiveTailRawBlock};
use super::super::{
    CanonicalStoreError, CanonicalStoredData, CanonicalStoredSet,
    block_load::{BLOCK_HEADER_VALUE_LEN, CanonicalBuildBlock},
    block_replay::PersistedReplayStep,
    reader::LocatedTransactionBlob,
    rocksdb::{
        BLOCK_HEADER_COLUMN_FAMILY, LIVE_TAIL_BLOCK_COLUMN_FAMILY,
        LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY, SETTLED_SEGMENT_INDEX_COLUMN_FAMILY,
    },
};
use super::{
    ByteReader, SettledSegmentsError,
    cache::SettledSegmentReaderCache,
    layout::{
        CanonicalSegmentsManifest, SettledSegmentIndexEntry, SettledSegmentModules,
        read_segments_manifest, validate_contiguous_index_entries, write_segments_manifest_atomic,
    },
    snapshot::verify_pinned_segments_into_cache,
};

/// Shared shielded and optional supplement readers for compact-block reconstruction.
#[derive(Clone)]
pub(in crate::canonical_store) struct SegmentReplayReaders {
    /// Authenticated `ZSHRNG02` shielded segment reader.
    pub(in crate::canonical_store) shielded: Arc<SettledShieldedSegmentReader>,
    /// Authenticated `ZSTSUP01` supplement reader when enabled for this segment.
    pub(in crate::canonical_store) supplement: Option<Arc<SettledSupplementSegmentReader>>,
}

/// Borrowed store handles for settled/live-tail routing without exceeding clippy argument limits.
#[derive(Clone, Copy)]
pub(in crate::canonical_store) struct SegmentStoreContext<'a> {
    pub(in crate::canonical_store) db: &'a DB,
    pub(in crate::canonical_store) segment_cache: Option<&'a SettledSegmentReaderCache>,
    pub(in crate::canonical_store) sealed_segments: Option<&'a [SettledSegmentIndexEntry]>,
}

/// Binary-searches `entries` in `O(log N)` for the contiguous settled segment covering `height`.
#[must_use]
pub(in crate::canonical_store) fn find_sealed_segment_in_slice(
    entries: &[SettledSegmentIndexEntry],
    height: BlockHeight,
) -> Option<&SettledSegmentIndexEntry> {
    let idx = entries.partition_point(|entry| entry.end_block.height < height);
    let candidate = entries.get(idx)?;
    (candidate.start_height <= height && height <= candidate.end_block.height).then_some(candidate)
}

/// Resolves the settled segment covering `height`, preferring the in-memory `O(log N)`
/// index when supplied by an open store handle.
pub(in crate::canonical_store) fn resolve_settled_segment_for_height(
    db: &DB,
    sealed_segments: Option<&[SettledSegmentIndexEntry]>,
    height: BlockHeight,
) -> Result<Option<SettledSegmentIndexEntry>, CanonicalStoreError> {
    if let Some(entries) = sealed_segments {
        return Ok(find_sealed_segment_in_slice(entries, height).cloned());
    }
    find_settled_segment_for_height(db, height)
}

/// Reads and validates every [`SettledSegmentIndexEntry`] from `settled_segment_index`.
pub(in crate::canonical_store) fn read_all_settled_segment_entries(
    db: &DB,
) -> Result<Vec<SettledSegmentIndexEntry>, CanonicalStoreError> {
    let family = db
        .cf_handle(SETTLED_SEGMENT_INDEX_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::SettledSegment {
            reason: "settled_segment_index column family is absent".to_owned(),
        })?;
    let mut entries = Vec::new();
    for row in db.iterator_cf(&family, IteratorMode::Start) {
        let (key, encoded) = row.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "settled segment index scan",
            source,
        })?;
        entries.push(decode_and_check_index_row(&key, &encoded)?);
    }
    let _network = validate_contiguous_index_entries(&entries)?;
    Ok(entries)
}

/// Resolves the [`SettledSegmentIndexEntry`] covering `height` from `settled_segment_index`.
pub(in crate::canonical_store) fn find_settled_segment_for_height(
    db: &DB,
    height: BlockHeight,
) -> Result<Option<SettledSegmentIndexEntry>, CanonicalStoreError> {
    let family = db
        .cf_handle(SETTLED_SEGMENT_INDEX_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::SettledSegment {
            reason: "settled_segment_index column family is absent".to_owned(),
        })?;
    let start_key = encode_height_key_ascending(height);
    let mut rows = db.iterator_cf(&family, IteratorMode::From(&start_key, Direction::Reverse));
    let Some(row) = rows.next() else {
        return Ok(None);
    };
    let (key, encoded) = row.map_err(|source| CanonicalStoreError::RocksDbOperation {
        operation: "settled segment index lookup",
        source,
    })?;
    let entry = decode_and_check_index_row(&key, &encoded)?;
    Ok((height >= entry.start_height && height <= entry.end_block.height).then_some(entry))
}

/// Returns the inclusive `sealed_tip` of the latest sealed segment, or `None`.
pub(in crate::canonical_store) fn read_sealed_tip(
    db: &DB,
) -> Result<Option<BlockId>, CanonicalStoreError> {
    let family = db
        .cf_handle(SETTLED_SEGMENT_INDEX_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::SettledSegment {
            reason: "settled_segment_index column family is absent".to_owned(),
        })?;
    let mut rows = db.iterator_cf(&family, IteratorMode::End);
    let Some(row) = rows.next() else {
        return Ok(None);
    };
    let (key, encoded) = row.map_err(|source| CanonicalStoreError::RocksDbOperation {
        operation: "settled segment sealed_tip lookup",
        source,
    })?;
    let entry = decode_and_check_index_row(&key, &encoded)?;
    Ok(Some(entry.end_block))
}

fn decode_and_check_index_row(
    key: &[u8],
    encoded: &[u8],
) -> Result<SettledSegmentIndexEntry, CanonicalStoreError> {
    let key_height =
        decode_height_key_ascending(key).map_err(|source| CanonicalStoreError::SettledSegment {
            reason: format!("invalid settled_segment_index key: {source}"),
        })?;
    let entry = SettledSegmentIndexEntry::decode_v1(encoded)?;
    if entry.start_height != key_height {
        return Err(CanonicalStoreError::SettledSegment {
            reason: format!(
                "settled_segment_index key {} disagrees with entry start_height {}",
                key_height.value(),
                entry.start_height.value()
            ),
        });
    }
    Ok(entry)
}

pub(in crate::canonical_store) fn verified_read_options() -> ReadOptions {
    let mut read_options = ReadOptions::default();
    read_options.set_verify_checksums(true);
    read_options
}

pub(in crate::canonical_store) fn uncached_read_options() -> ReadOptions {
    let mut read_options = verified_read_options();
    read_options.fill_cache(false);
    read_options
}

fn live_tail_family_and_key<'db>(
    db: &'db DB,
    family_name: &'static str,
    height: BlockHeight,
) -> Result<(Arc<BoundColumnFamily<'db>>, [u8; 4]), CanonicalStoreError> {
    let family = db
        .cf_handle(family_name)
        .ok_or_else(|| CanonicalStoreError::SettledSegment {
            reason: format!("{family_name} column family is absent"),
        })?;
    Ok((family, encode_height_key_ascending(height)))
}

/// Reads the raw `V1` bytes of one unsealed `live_tail_block` row using `read_options`.
pub(in crate::canonical_store) fn read_live_tail_raw_bytes_with_options(
    db: &DB,
    height: BlockHeight,
    read_options: &ReadOptions,
) -> Result<Option<Vec<u8>>, CanonicalStoreError> {
    let (family, key) = live_tail_family_and_key(db, LIVE_TAIL_BLOCK_COLUMN_FAMILY, height)?;
    db.get_cf_opt(&family, key, read_options).map_err(|source| {
        CanonicalStoreError::RocksDbOperation {
            operation: "live tail block read",
            source,
        }
    })
}

/// Batch-reads pinned slices from `live_tail_block` preserving caller order.
pub(in crate::canonical_store) fn fetch_live_tail_batch_pinned<'a>(
    db: &'a DB,
    heights: &[BlockHeight],
    read_options: &ReadOptions,
) -> Result<Vec<Result<Option<DBPinnableSlice<'a>>, CanonicalStoreError>>, CanonicalStoreError> {
    fetch_height_batch_pinned(
        db,
        LIVE_TAIL_BLOCK_COLUMN_FAMILY,
        heights,
        read_options,
        "compact block batch read",
    )
}

fn fetch_height_batch_pinned<'a>(
    db: &'a DB,
    family_name: &'static str,
    heights: &[BlockHeight],
    read_options: &ReadOptions,
    operation: &'static str,
) -> Result<Vec<Result<Option<DBPinnableSlice<'a>>, CanonicalStoreError>>, CanonicalStoreError> {
    let family = db
        .cf_handle(family_name)
        .ok_or_else(|| CanonicalStoreError::SettledSegment {
            reason: format!("{family_name} column family is absent"),
        })?;
    let sorted = heights.windows(2).all(|pair| pair[0] < pair[1]);
    let keys = heights
        .iter()
        .copied()
        .map(encode_height_key_ascending)
        .collect::<Vec<_>>();
    Ok(db
        .batched_multi_get_cf_opt(
            &family,
            keys.iter().map(<[u8; 4]>::as_slice),
            sorted,
            read_options,
        )
        .into_iter()
        .map(|row| {
            row.map_err(|source| CanonicalStoreError::RocksDbOperation { operation, source })
        })
        .collect())
}

/// Reads and authenticates one unsealed [`LiveTailBlockRecord`] at `height`.
pub(in crate::canonical_store) fn read_live_tail_record(
    db: &DB,
    height: BlockHeight,
) -> Result<Option<LiveTailBlockRecord>, CanonicalStoreError> {
    let Some(encoded) =
        read_live_tail_raw_bytes_with_options(db, height, &verified_read_options())?
    else {
        return Ok(None);
    };
    let record = LiveTailBlockRecord::decode_v1(&encoded)?;
    if record.block_id.height != height {
        return Err(CanonicalStoreError::SettledSegment {
            reason: format!(
                "live_tail_block key {} disagrees with record height {}",
                height.value(),
                record.block_id.height.value()
            ),
        });
    }
    Ok(Some(record))
}

/// Reads and authenticates one unsealed [`LiveTailRawBlock`] from `live_tail_raw_block`.
pub(in crate::canonical_store) fn read_live_tail_raw_block(
    db: &DB,
    height: BlockHeight,
) -> Result<Option<LiveTailRawBlock>, CanonicalStoreError> {
    let (family, key) = live_tail_family_and_key(db, LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY, height)?;
    let Some(encoded) = db
        .get_cf_opt(&family, key, &verified_read_options())
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "live tail raw block read",
            source,
        })?
    else {
        return Ok(None);
    };
    Ok(Some(LiveTailRawBlock::decode_v1(&encoded)?))
}

/// Builds a validated [`LiveTailRawBlock`] from binary [`RawBlobRetention`] inputs.
pub(in crate::canonical_store) fn live_tail_raw_block_from_blobs(
    facts: &CanonicalBlockFacts,
    block_blob: Option<&BlockBlobArtifact>,
    transaction_blobs: &[TransactionBlobArtifact],
    retention: RawBlobRetention,
) -> Result<Option<LiveTailRawBlock>, CanonicalStoreError> {
    match retention {
        RawBlobRetention::None => {
            if block_blob.is_some() || !transaction_blobs.is_empty() {
                return Err(CanonicalStoreError::block_load_sequence(format!(
                    "block {} unexpectedly carries raw blobs under none retention",
                    facts.block_header.height.value()
                )));
            }
            Ok(None)
        }
        RawBlobRetention::All => {
            let blob = block_blob.ok_or_else(|| {
                CanonicalStoreError::block_load_sequence(format!(
                    "block {} lacks raw block blob under all retention",
                    facts.block_header.height.value()
                ))
            })?;
            verify_block_blob_identity(facts, blob)?;
            verify_tx_blobs_against_facts(facts, transaction_blobs)?;
            let slices = verify_consensus_tail_slices(&blob.raw_block_bytes, transaction_blobs)?;
            let raw_block =
                LiveTailRawBlock::from_bytes_and_slices(blob.raw_block_bytes.clone(), &slices)?;
            Ok(Some(raw_block))
        }
    }
}

/// Encoded split live-tail payloads for `live_tail_block` and optional `live_tail_raw_block`.
pub(in crate::canonical_store) struct EncodedSplitLiveTail {
    /// Encoded `ZSLVTL01` bytes for `live_tail_block`.
    pub(in crate::canonical_store) block_bytes: Vec<u8>,
    /// Optional encoded `ZSLVRB01` bytes for `live_tail_raw_block` when `full_blocks` is active.
    pub(in crate::canonical_store) raw_block_bytes: Option<Vec<u8>>,
}

/// Validates and encodes one [`CanonicalBuildBlock`] into split `live_tail_block` (`ZSLVTL01`)
/// and optional `live_tail_raw_block` (`ZSLVRB01`) records.
pub(in crate::canonical_store) fn encode_split_live_tail_from_build_block(
    block: &CanonicalBuildBlock,
    stored_set: CanonicalStoredSet,
) -> Result<EncodedSplitLiveTail, CanonicalStoreError> {
    let raw_block = live_tail_raw_block_from_blobs(
        &block.facts,
        block.block_blob.as_ref(),
        &block.transaction_blobs,
        stored_set.raw_blob_retention(),
    )?;
    let raw_block_bytes = raw_block
        .as_ref()
        .map(LiveTailRawBlock::encode_v1)
        .transpose()?;
    let include_supplement = segment_modules_for_stored_set(stored_set).supplement;
    let (block_bytes, _) = LiveTailBlockRecord::encode_from_compact_block_v1(
        &block.compact_block,
        Some(block.facts.clone()),
        LiveTailEncodeOptions {
            include_supplement,
            retain_canonical_facts: true,
        },
    )?;
    Ok(EncodedSplitLiveTail {
        block_bytes,
        raw_block_bytes,
    })
}

/// Maps [`CanonicalStoredSet`] retention flags to settled segment module configuration.
pub(in crate::canonical_store) const fn segment_modules_for_stored_set(
    stored_set: CanonicalStoredSet,
) -> SettledSegmentModules {
    SettledSegmentModules {
        supplement: true,
        raw_bodies: stored_set.raw_blob_retention().retains_block_blobs(),
    }
}

/// Reads and decodes one canonical [`BlockHeaderArtifact`] without polluting the block cache.
fn read_block_header_from_db(
    db: &DB,
    height: BlockHeight,
) -> Result<Option<BlockHeaderArtifact>, CanonicalStoreError> {
    let family = db
        .cf_handle(BLOCK_HEADER_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::publication("block_header column family is absent"))?;
    let key = encode_height_key_ascending(height);
    let Some(encoded) = db
        .get_cf_opt(&family, key, &uncached_read_options())
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "block header read",
            source,
        })?
    else {
        return Ok(None);
    };
    decode_block_header_record(height, &encoded).map(Some)
}

/// Opens ONLY `ZSHRNG02`, never opening `ZSTSUP01` or `ZSBLKB01`.
pub(in crate::canonical_store) fn open_shielded_segment_reader(
    ctx: SegmentStoreContext<'_>,
    entry: &SettledSegmentIndexEntry,
) -> Result<Arc<SettledShieldedSegmentReader>, CanonicalStoreError> {
    let opened = if let Some(cache) = ctx.segment_cache {
        cache.get_or_open_shielded(entry)
    } else {
        let temp_cache = SettledSegmentReaderCache::new(ctx.db.path(), 1, false)
            .map_err(map_settled_segments_read_error)?;
        temp_cache.get_or_open_shielded(entry)
    };
    opened.map_err(map_settled_segments_read_error)
}

/// Opens only `ZSTSUP01` when `entry.modules.supplement` is enabled.
pub(in crate::canonical_store) fn open_supplement_segment_reader(
    ctx: SegmentStoreContext<'_>,
    entry: &SettledSegmentIndexEntry,
) -> Result<Option<Arc<SettledSupplementSegmentReader>>, CanonicalStoreError> {
    if !entry.modules.supplement {
        return Ok(None);
    }
    let opened = if let Some(cache) = ctx.segment_cache {
        cache.get_or_open_supplement(entry)
    } else {
        let temp_cache = SettledSegmentReaderCache::new(ctx.db.path(), 1, false)
            .map_err(map_settled_segments_read_error)?;
        temp_cache.get_or_open_supplement(entry)
    };
    opened.map_err(map_settled_segments_read_error)
}

pub(in crate::canonical_store) fn map_supplement_read_error(
    error: SettledSupplementSegmentError,
) -> CanonicalStoreError {
    match error {
        SettledSupplementSegmentError::Io(ref io_err)
            if rustix::io::Errno::from_io_error(io_err) == Some(rustix::io::Errno::IO) =>
        {
            CanonicalStoreError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                reason: error.to_string(),
            }
        }
        SettledSupplementSegmentError::Io(io_err) => CanonicalStoreError::SettledSegment {
            reason: io_err.to_string(),
        },
        SettledSupplementSegmentError::InvalidFormat(_)
        | SettledSupplementSegmentError::InvalidCodecId(_)
        | SettledSupplementSegmentError::ChainLinkage(_)
        | SettledSupplementSegmentError::PayloadIntegrity(_)
        | SettledSupplementSegmentError::PayloadLimitExceeded { .. }
        | SettledSupplementSegmentError::FsVerityRequired
        | SettledSupplementSegmentError::HeightOutOfRange { .. } => {
            CanonicalStoreError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                reason: error.to_string(),
            }
        }
    }
}

pub(in crate::canonical_store) fn map_shielded_read_error(
    error: SettledShieldedSegmentError,
) -> CanonicalStoreError {
    match error {
        SettledShieldedSegmentError::Cancelled => CanonicalStoreError::ReplayCancelled,
        SettledShieldedSegmentError::Io(ref io_err)
            if rustix::io::Errno::from_io_error(io_err) == Some(rustix::io::Errno::IO) =>
        {
            CanonicalStoreError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                reason: error.to_string(),
            }
        }
        SettledShieldedSegmentError::Io(io_err) => CanonicalStoreError::SettledSegment {
            reason: io_err.to_string(),
        },
        SettledShieldedSegmentError::InvalidFormat(_)
        | SettledShieldedSegmentError::ChainLinkage(_)
        | SettledShieldedSegmentError::PayloadIntegrity(_)
        | SettledShieldedSegmentError::PayloadLimitExceeded { .. }
        | SettledShieldedSegmentError::FsVerityRequired
        | SettledShieldedSegmentError::HeightOutOfRange { .. } => {
            CanonicalStoreError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                reason: error.to_string(),
            }
        }
    }
}

pub(in crate::canonical_store) fn map_settled_segments_read_error(
    error: SettledSegmentsError,
) -> CanonicalStoreError {
    match error {
        SettledSegmentsError::Shielded(inner) => map_shielded_read_error(inner),
        SettledSegmentsError::Io(ref io_err)
            if rustix::io::Errno::from_io_error(io_err) == Some(rustix::io::Errno::IO) =>
        {
            CanonicalStoreError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                reason: error.to_string(),
            }
        }
        SettledSegmentsError::Io(io_err) => CanonicalStoreError::SettledSegment {
            reason: io_err.to_string(),
        },
        SettledSegmentsError::InvalidFormat(_)
        | SettledSegmentsError::Integrity(_)
        | SettledSegmentsError::ChainLinkage(_)
        | SettledSegmentsError::InvalidLayout(_)
        | SettledSegmentsError::Json(_) => CanonicalStoreError::ArtifactCorrupt {
            family: ArtifactFamily::CompactBlock,
            reason: error.to_string(),
        },
        SettledSegmentsError::Supplement(_) | SettledSegmentsError::RawBlock(_) => {
            CanonicalStoreError::from(error)
        }
    }
}

/// Opens ONLY `ZSHRNG02` and (when enabled by `entry.modules.supplement`) `ZSTSUP01`,
/// never opening `ZSBLKB01`.
pub(in crate::canonical_store) fn open_replay_segment_readers(
    db: &DB,
    segment_cache: Option<&SettledSegmentReaderCache>,
    entry: &SettledSegmentIndexEntry,
) -> Result<SegmentReplayReaders, CanonicalStoreError> {
    if let Some(cache) = segment_cache {
        return open_replay_from_cache(cache, entry);
    }
    let temp_cache = SettledSegmentReaderCache::new(db.path(), 1, false)?;
    open_replay_from_cache(&temp_cache, entry)
}

fn open_replay_from_cache(
    cache: &SettledSegmentReaderCache,
    entry: &SettledSegmentIndexEntry,
) -> Result<SegmentReplayReaders, CanonicalStoreError> {
    let shielded = cache.get_or_open_shielded(entry)?;
    let supplement = if entry.modules.supplement {
        cache.get_or_open_supplement(entry)?
    } else {
        None
    };
    Ok(SegmentReplayReaders {
        shielded,
        supplement,
    })
}

fn open_raw_block_reader(
    db: &DB,
    segment_cache: Option<&SettledSegmentReaderCache>,
    entry: &SettledSegmentIndexEntry,
) -> Result<Option<Arc<SettledRawBlockSegmentReader>>, CanonicalStoreError> {
    if !entry.modules.raw_bodies {
        return Ok(None);
    }
    if let Some(cache) = segment_cache {
        return Ok(cache.get_or_open_raw_block(entry)?);
    }
    let temp_cache = SettledSegmentReaderCache::new(db.path(), 1, false)?;
    Ok(temp_cache.get_or_open_raw_block(entry)?)
}

/// Opens the authenticated supplement required for canonical replay.
pub(in crate::canonical_store) fn open_replay_supplement_reader(
    ctx: SegmentStoreContext<'_>,
    entry: &SettledSegmentIndexEntry,
) -> Result<Arc<SettledSupplementSegmentReader>, CanonicalStoreError> {
    open_supplement_segment_reader(ctx, entry)?.ok_or_else(|| CanonicalStoreError::SettledSegment {
        reason: format!(
            "settled supplement reader missing for segment at height {}",
            entry.start_height.value()
        ),
    })
}

fn reconstruct_replay_from_parts(
    header: BlockHeaderArtifact,
    sup_record: &BlockSupplementRecord,
) -> Result<ValidatedCanonicalBlockReplay, CanonicalStoreError> {
    let height = header.height;
    let facts = reconstruct_canonical_block_facts(header, sup_record).map_err(|err| {
        CanonicalStoreError::SettledSegment {
            reason: format!(
                "failed to reconstruct canonical facts at height {}: {err}",
                height.value()
            ),
        }
    })?;
    Ok(ValidatedCanonicalBlockReplay::from_authenticated_facts(
        facts,
        CanonicalBlockReplayFormatVersion::CURRENT,
        CanonicalBlockFactsDigestVersion::CURRENT,
    ))
}

/// Reconstructs and authenticates replay at `height` from `block_header` and `ZSTSUP01`.
pub(in crate::canonical_store) fn reconstruct_replay_from_segment(
    db: &DB,
    supplement: &SettledSupplementSegmentReader,
    height: BlockHeight,
) -> Result<ValidatedCanonicalBlockReplay, CanonicalStoreError> {
    let header = read_block_header_from_db(db, height)?.ok_or_else(|| {
        CanonicalStoreError::block_replay_sequence(format!(
            "block header at settled height {} is absent",
            height.value()
        ))
    })?;
    let mut records = supplement
        .read_range_for_headers(std::slice::from_ref(&header))
        .map_err(SettledSegmentsError::Supplement)?;
    let record = records
        .pop()
        .ok_or_else(|| CanonicalStoreError::SettledSegment {
            reason: format!(
                "settled supplement record missing at height {}",
                height.value()
            ),
        })?;
    reconstruct_replay_from_parts(header, &record)
}

/// Batch-reconstructs replay within one settled segment using a single header
/// iterator and a single supplement chunk pass.
pub(in crate::canonical_store) fn reconstruct_replay_range_from_segment(
    db: &DB,
    supplement: &SettledSupplementSegmentReader,
    range: BlockHeightRange,
) -> Result<Vec<(BlockHeight, ValidatedCanonicalBlockReplay)>, CanonicalStoreError> {
    if range.start > range.end {
        return Ok(Vec::new());
    }
    let headers = read_replay_headers(db, range)?;
    let records = supplement
        .read_range_for_headers(&headers)
        .map_err(SettledSegmentsError::Supplement)?;
    if records.len() != headers.len() {
        return Err(CanonicalStoreError::SettledSegment {
            reason: format!(
                "settled supplement returned unexpected batch count for {}..={}",
                range.start.value(),
                range.end.value()
            ),
        });
    }
    headers
        .into_iter()
        .zip(records)
        .map(|(header, record)| {
            let height = header.height;
            Ok((height, reconstruct_replay_from_parts(header, &record)?))
        })
        .collect()
}

fn read_replay_headers(
    db: &DB,
    range: BlockHeightRange,
) -> Result<Vec<BlockHeaderArtifact>, CanonicalStoreError> {
    let family = db
        .cf_handle(BLOCK_HEADER_COLUMN_FAMILY)
        .ok_or_else(|| CanonicalStoreError::publication("block_header column family is absent"))?;
    let mut rows = db.raw_iterator_cf_opt(&family, uncached_read_options());
    rows.seek(encode_height_key_ascending(range.start));
    let scan_error = |source| CanonicalStoreError::RocksDbOperation {
        operation: "block header replay scan",
        source,
    };
    let mut headers = Vec::with_capacity(range.into_iter().len());
    for height in range {
        let Some((key, encoded)) = rows.item() else {
            rows.status().map_err(scan_error)?;
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "block header at settled height {} is absent",
                height.value()
            )));
        };
        if key != encode_height_key_ascending(height) {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "block header scan expected settled height {}, found key {key:02x?}",
                height.value()
            )));
        }
        headers.push(decode_block_header_record(height, encoded)?);
        rows.next();
    }
    // The next batch seeks to its start, so check for keys that seek would skip.
    match rows.item() {
        None => rows.status().map_err(scan_error)?,
        Some((key, _))
            if range
                .end
                .next()
                .is_none_or(|height| key < encode_height_key_ascending(height).as_slice()) =>
        {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "block header scan found unexpected key {key:02x?} after settled height {}",
                range.end.value()
            )));
        }
        Some(_) => {}
    }
    Ok(headers)
}

/// Converts already-fetched `live_tail_block` bytes at `height` into a [`PersistedReplayStep`]
/// without rereading `RocksDB`.
pub(in crate::canonical_store) fn replay_step_from_live_tail_bytes(
    height: BlockHeight,
    encoded: &[u8],
) -> Result<PersistedReplayStep, CanonicalStoreError> {
    let facts =
        LiveTailBlockRecord::decode_canonical_facts_v1(encoded, height)?.ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence(format!(
                "live-tail block {} is missing canonical_facts",
                height.value()
            ))
        })?;
    let replay = ValidatedCanonicalBlockReplay::from_authenticated_facts(
        facts,
        CanonicalBlockReplayFormatVersion::CURRENT,
        CanonicalBlockFactsDigestVersion::CURRENT,
    );
    replay_step_from_validated(replay)
}

pub(in crate::canonical_store) fn replay_step_from_validated(
    replay: ValidatedCanonicalBlockReplay,
) -> Result<PersistedReplayStep, CanonicalStoreError> {
    let encoded_len = encode_canonical_block_replay(
        replay.facts(),
        CanonicalBlockReplayFormatVersion::CURRENT,
        CanonicalBlockFactsDigestVersion::CURRENT,
    )
    .as_bytes()
    .len();
    let logical_replay_bytes = u64::try_from(encoded_len).map_err(|_| {
        CanonicalStoreError::block_replay_sequence("canonical replay bytes exceed u64::MAX")
    })?;
    Ok(PersistedReplayStep {
        replay,
        logical_replay_bytes,
    })
}

/// Reads and reconstructs one replay step across settled segments and `live_tail_block`.
///
/// Uses `O(log N)` settled routing first so settled heights skip `live_tail_block` probes.
pub(in crate::canonical_store) fn read_replay_step_split(
    ctx: SegmentStoreContext<'_>,
    height: BlockHeight,
) -> Result<PersistedReplayStep, CanonicalStoreError> {
    if let Some(entry) = resolve_settled_segment_for_height(ctx.db, ctx.sealed_segments, height)? {
        let supplement = open_replay_supplement_reader(ctx, &entry)?;
        let replay = reconstruct_replay_from_segment(ctx.db, &supplement, height)?;
        return replay_step_from_validated(replay);
    }
    if let Some(encoded) =
        read_live_tail_raw_bytes_with_options(ctx.db, height, &ReadOptions::default())?
    {
        return replay_step_from_live_tail_bytes(height, &encoded);
    }
    Err(CanonicalStoreError::block_replay_sequence(format!(
        "canonical block replay at height {} is absent",
        height.value()
    )))
}

fn read_compact_block_from_segment(
    readers: &SegmentReplayReaders,
    height: BlockHeight,
) -> Result<CompactBlockArtifact, CanonicalStoreError> {
    let range = BlockHeightRange::inclusive(height, height);
    let encoded_blocks = if readers.supplement.is_some() {
        readers
            .shielded
            .read_default_range_for_reconstruction(range, HeightOrder::Ascending)
    } else {
        readers
            .shielded
            .read_default_range(range, HeightOrder::Ascending)
    }
    .map_err(map_shielded_read_error)?;
    let [encoded_block] =
        <[_; 1]>::try_from(encoded_blocks).map_err(|_| CanonicalStoreError::SettledSegment {
            reason: format!(
                "shielded segment returned unexpected block count at height {}",
                height.value()
            ),
        })?;
    reconstruct_compact_from_shielded(readers, height, &encoded_block.payload())
}

fn segment_needs_supplement(
    sup_reader: &SettledSupplementSegmentReader,
    height: BlockHeight,
) -> Result<bool, CanonicalStoreError> {
    let has_transparent = sup_reader
        .has_transparent_at(height)
        .map_err(SettledSegmentsError::Supplement)?;
    let has_wide_fee = sup_reader
        .has_wide_fee_at(height)
        .map_err(SettledSegmentsError::Supplement)?;
    Ok(has_transparent || has_wide_fee)
}

fn empty_supplement_record() -> BlockSupplementRecord {
    BlockSupplementRecord {
        transparent_transactions: Vec::new(),
        canonical_metadata: None,
    }
}

fn reconstruct_compact_from_shielded(
    readers: &SegmentReplayReaders,
    height: BlockHeight,
    shielded_payload: &[u8],
) -> Result<CompactBlockArtifact, CanonicalStoreError> {
    if let Some(sup_reader) = &readers.supplement
        && segment_needs_supplement(sup_reader, height)?
    {
        return Ok(sup_reader
            .reconstruct_block(height, shielded_payload)
            .map_err(SettledSegmentsError::Supplement)?);
    }
    let empty_sup = empty_supplement_record();
    reconstruct_compact_block_artifact(shielded_payload, &empty_sup).map_err(|err| {
        CanonicalStoreError::SettledSegment {
            reason: format!(
                "failed to reconstruct shielded compact block at height {}: {err}",
                height.value()
            ),
        }
    })
}

/// Reads and reconstructs one [`CompactBlockArtifact`] across settled segments and `live_tail_block`.
pub(in crate::canonical_store) fn read_compact_block_split(
    ctx: SegmentStoreContext<'_>,
    height: BlockHeight,
) -> Result<Option<CompactBlockArtifact>, CanonicalStoreError> {
    read_compact_block_split_with_options(ctx, height, &verified_read_options())
}

fn read_compact_block_split_with_options(
    ctx: SegmentStoreContext<'_>,
    height: BlockHeight,
    read_options: &ReadOptions,
) -> Result<Option<CompactBlockArtifact>, CanonicalStoreError> {
    if let Some(entry) = resolve_settled_segment_for_height(ctx.db, ctx.sealed_segments, height)? {
        let readers = open_replay_segment_readers(ctx.db, ctx.segment_cache, &entry)?;
        return Ok(Some(read_compact_block_from_segment(&readers, height)?));
    }
    if let Some(encoded) = read_live_tail_raw_bytes_with_options(ctx.db, height, read_options)? {
        let (compact, _) = LiveTailBlockRecord::decode_compact_block_v1(&encoded, height)
            .map_err(map_settled_segments_read_error)?;
        return Ok(Some(compact));
    }
    Ok(None)
}

/// Batch-reconstructs compact blocks across an aligned slice within a single settled segment.
pub(in crate::canonical_store) fn reconstruct_segment_compact_range(
    readers: &SegmentReplayReaders,
    range: BlockHeightRange,
) -> Result<Vec<CompactBlockArtifact>, CanonicalStoreError> {
    if range.start > range.end {
        return Ok(Vec::new());
    }
    let encoded_blocks = if readers.supplement.is_some() {
        readers
            .shielded
            .read_default_range_for_reconstruction(range, HeightOrder::Ascending)
    } else {
        readers
            .shielded
            .read_default_range(range, HeightOrder::Ascending)
    }
    .map_err(map_shielded_read_error)?;
    let expected =
        usize::try_from(u64::from(range.end.value()) - u64::from(range.start.value()) + 1)
            .map_err(|_| CanonicalStoreError::SettledSegment {
                reason: "compact block range exceeds usize".to_owned(),
            })?;
    if encoded_blocks.len() != expected {
        return Err(CanonicalStoreError::SettledSegment {
            reason: format!(
                "shielded segment returned unexpected count for {}..={}",
                range.start.value(),
                range.end.value()
            ),
        });
    }
    let empty_sup = empty_supplement_record();
    if let Some(sup_reader) = &readers.supplement {
        return reconstruct_range_with_optional_supplement(
            sup_reader,
            range,
            &encoded_blocks,
            &empty_sup,
        );
    }
    let mut artifacts = Vec::with_capacity(encoded_blocks.len());
    for encoded_block in encoded_blocks {
        artifacts.push(
            reconstruct_compact_block_artifact(&encoded_block.payload(), &empty_sup)
                .map_err(SettledSegmentsError::Supplement)?,
        );
    }
    Ok(artifacts)
}

fn reconstruct_range_with_optional_supplement(
    sup_reader: &SettledSupplementSegmentReader,
    range: BlockHeightRange,
    encoded_blocks: &[zinder_proto::compat::lightwalletd::EncodedBlock],
    empty_sup: &BlockSupplementRecord,
) -> Result<Vec<CompactBlockArtifact>, CanonicalStoreError> {
    let mut needs = Vec::with_capacity(encoded_blocks.len());
    for height in range {
        needs.push(segment_needs_supplement(sup_reader, height)?);
    }
    let mut artifacts = Vec::with_capacity(encoded_blocks.len());
    let mut idx = 0_usize;
    while idx < encoded_blocks.len() {
        if !needs[idx] {
            artifacts.push(
                reconstruct_compact_block_artifact(&encoded_blocks[idx].payload(), empty_sup)
                    .map_err(SettledSegmentsError::Supplement)?,
            );
            idx += 1;
            continue;
        }
        let run_start = idx;
        while idx < encoded_blocks.len() && needs[idx] {
            idx += 1;
        }
        let run_offset =
            u32::try_from(run_start).map_err(|_| CanonicalStoreError::SettledSegment {
                reason: "segment run index exceeds u32".to_owned(),
            })?;
        let run_height = range
            .start
            .value()
            .checked_add(run_offset)
            .map(BlockHeight::new)
            .ok_or_else(|| CanonicalStoreError::SettledSegment {
                reason: "segment run height overflow".to_owned(),
            })?;
        let payloads = encoded_blocks[run_start..idx]
            .iter()
            .map(zinder_proto::compat::lightwalletd::EncodedBlock::payload)
            .collect::<Vec<_>>();
        artifacts.extend(
            sup_reader
                .reconstruct_range(run_height, &payloads)
                .map_err(SettledSegmentsError::Supplement)?,
        );
    }
    Ok(artifacts)
}

/// Reads raw consensus block bytes across `ZSBLKB01` segments and `live_tail_raw_block`.
pub(in crate::canonical_store) fn read_raw_block_split(
    ctx: SegmentStoreContext<'_>,
    stored_set: CanonicalStoredSet,
    height: BlockHeight,
) -> Result<Option<Vec<u8>>, CanonicalStoreError> {
    if !stored_set.contains(CanonicalStoredData::BlockBodies) {
        return Ok(None);
    }
    if let Some(entry) = resolve_settled_segment_for_height(ctx.db, ctx.sealed_segments, height)? {
        let Some(raw_reader) = open_raw_block_reader(ctx.db, ctx.segment_cache, &entry)? else {
            return Ok(None);
        };
        let raw_bytes = raw_reader
            .read_raw_block(height)
            .map_err(|err| CanonicalStoreError::from(SettledSegmentsError::RawBlock(err)))?;
        return Ok(Some(raw_bytes));
    }
    Ok(read_live_tail_raw_block(ctx.db, height)?.map(|raw_block| raw_block.raw_block_bytes))
}

/// Batch-reads raw consensus blocks across settled segments and `live_tail_raw_block` in `range` order.
pub(in crate::canonical_store) fn read_raw_blocks_in_range_split(
    ctx: SegmentStoreContext<'_>,
    stored_set: CanonicalStoredSet,
    range: BlockHeightRange,
) -> Result<Vec<Option<Vec<u8>>>, CanonicalStoreError> {
    if range.start > range.end {
        return Ok(Vec::new());
    }
    let count = usize::try_from(u64::from(range.end.value()) - u64::from(range.start.value()) + 1)
        .map_err(|_| CanonicalStoreError::SettledSegment {
            reason: "raw block range exceeds usize".to_owned(),
        })?;
    if !stored_set.contains(CanonicalStoredData::BlockBodies) {
        return Ok(vec![None; count]);
    }
    let mut out = vec![None; count];
    let mut live_tail_heights = Vec::new();
    let mut active_segment: Option<(SettledSegmentIndexEntry, Arc<SettledRawBlockSegmentReader>)> =
        None;
    for (idx, height) in range.into_iter().enumerate() {
        let in_active = active_segment.as_ref().is_some_and(|(entry, _)| {
            height >= entry.start_height && height <= entry.end_block.height
        });
        if in_active {
            if let Some((_, raw_reader)) = active_segment.as_ref() {
                out[idx] = Some(raw_reader.read_raw_block(height).map_err(|err| {
                    CanonicalStoreError::from(SettledSegmentsError::RawBlock(err))
                })?);
            }
            continue;
        }
        if let Some(entry) =
            resolve_settled_segment_for_height(ctx.db, ctx.sealed_segments, height)?
        {
            if let Some(raw_reader) = open_raw_block_reader(ctx.db, ctx.segment_cache, &entry)? {
                out[idx] = Some(raw_reader.read_raw_block(height).map_err(|err| {
                    CanonicalStoreError::from(SettledSegmentsError::RawBlock(err))
                })?);
                active_segment = Some((entry, raw_reader));
            } else {
                active_segment = None;
            }
        } else {
            live_tail_heights.push((idx, height));
        }
    }
    populate_live_tail_raw_blocks(ctx.db, &live_tail_heights, &mut out)?;
    Ok(out)
}

fn populate_live_tail_raw_blocks(
    db: &DB,
    heights: &[(usize, BlockHeight)],
    out: &mut [Option<Vec<u8>>],
) -> Result<(), CanonicalStoreError> {
    if heights.is_empty() {
        return Ok(());
    }
    let keys_heights = heights.iter().map(|(_, h)| *h).collect::<Vec<_>>();
    let rows = fetch_height_batch_pinned(
        db,
        LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY,
        &keys_heights,
        &verified_read_options(),
        "block blob batch read",
    )?;
    for ((out_idx, _), row) in heights.iter().zip(rows) {
        if let Some(encoded) = row? {
            let raw_block = LiveTailRawBlock::decode_v1(encoded.as_ref())?;
            out[*out_idx] = Some(raw_block.raw_block_bytes);
        }
    }
    Ok(())
}

/// Reads raw consensus transaction bytes across `ZSBLKB01` segments and `live_tail_raw_block`,
/// returning `Ok(None)` when retention excludes transaction bodies or `tx_index` is absent.
pub(in crate::canonical_store) fn read_raw_transaction_split(
    ctx: SegmentStoreContext<'_>,
    stored_set: CanonicalStoredSet,
    height: BlockHeight,
    tx_index: u32,
) -> Result<Option<Vec<u8>>, CanonicalStoreError> {
    if !stored_set.contains(CanonicalStoredData::TransactionBodies) {
        return Ok(None);
    }
    if let Some(entry) = resolve_settled_segment_for_height(ctx.db, ctx.sealed_segments, height)? {
        let Some(raw_reader) = open_raw_block_reader(ctx.db, ctx.segment_cache, &entry)? else {
            return Ok(None);
        };
        return match raw_reader.read_raw_transaction(height, tx_index) {
            Ok(raw_tx) => Ok(Some(raw_tx)),
            Err(SettledRawBlockSegmentError::TransactionIndexOutOfRange { .. }) => Ok(None),
            Err(err) => Err(CanonicalStoreError::from(SettledSegmentsError::RawBlock(
                err,
            ))),
        };
    }
    let Some(raw_block) = read_live_tail_raw_block(ctx.db, height)? else {
        return Ok(None);
    };
    let Ok(idx) = usize::try_from(tx_index) else {
        return Ok(None);
    };
    if idx >= raw_block.tx_slices.len() {
        return Ok(None);
    }
    Ok(Some(raw_block.read_verified_raw_transaction(idx)?))
}

/// Populates raw transaction blobs for every `Some((location, None))` slot.
///
/// Groups lookups by height so each settled block Zstd frame and each live-tail
/// raw block is decoded at most once across the entire batch.
pub(in crate::canonical_store) fn populate_raw_transaction_blobs_batch(
    ctx: SegmentStoreContext<'_>,
    stored_set: CanonicalStoredSet,
    slots: &mut [Option<LocatedTransactionBlob>],
) -> Result<(), CanonicalStoreError> {
    if !stored_set.contains(CanonicalStoredData::TransactionBodies) {
        return Ok(());
    }
    let mut requests = Vec::new();
    for (slot_idx, slot) in slots.iter().enumerate() {
        if let Some((location, None)) = slot {
            requests.push((location.block_height, location.tx_index_in_block, slot_idx));
        }
    }
    if requests.is_empty() {
        return Ok(());
    }
    requests.sort_unstable();

    let mut settled_groups: BTreeMap<BlockHeight, Vec<(u32, usize)>> = BTreeMap::new();
    let mut live_groups: BTreeMap<BlockHeight, Vec<(u32, usize)>> = BTreeMap::new();
    let mut settled_entries = Vec::<SettledSegmentIndexEntry>::new();

    for &(height, tx_index, slot_idx) in &requests {
        if find_sealed_segment_in_slice(&settled_entries, height).is_some() {
            settled_groups
                .entry(height)
                .or_default()
                .push((tx_index, slot_idx));
            continue;
        }
        if let Some(entry) =
            resolve_settled_segment_for_height(ctx.db, ctx.sealed_segments, height)?
        {
            settled_entries.push(entry);
            settled_groups
                .entry(height)
                .or_default()
                .push((tx_index, slot_idx));
        } else {
            live_groups
                .entry(height)
                .or_default()
                .push((tx_index, slot_idx));
        }
    }

    populate_settled_raw_tx_groups(ctx, &settled_entries, settled_groups, slots)?;
    populate_live_raw_tx_groups(ctx.db, live_groups, slots)
}

fn populate_settled_raw_tx_groups(
    ctx: SegmentStoreContext<'_>,
    settled_entries: &[SettledSegmentIndexEntry],
    settled_groups: BTreeMap<BlockHeight, Vec<(u32, usize)>>,
    slots: &mut [Option<LocatedTransactionBlob>],
) -> Result<(), CanonicalStoreError> {
    let mut active_reader: Option<(BlockHeight, BlockHeight, Arc<SettledRawBlockSegmentReader>)> =
        None;
    for (height, items) in settled_groups {
        let raw_reader = if let Some((start, end, cached)) = &active_reader
            && height >= *start
            && height <= *end
        {
            Arc::clone(cached)
        } else {
            let Some(entry) = find_sealed_segment_in_slice(settled_entries, height) else {
                continue;
            };
            let Some(reader) = open_raw_block_reader(ctx.db, ctx.segment_cache, entry)? else {
                continue;
            };
            active_reader = Some((
                entry.start_height,
                entry.end_block.height,
                Arc::clone(&reader),
            ));
            reader
        };
        let tx_indices = items.iter().map(|(tx_idx, _)| *tx_idx).collect::<Vec<_>>();
        let txs = match raw_reader.read_raw_transactions_in_block(height, &tx_indices) {
            Ok(txs) => txs,
            Err(SettledRawBlockSegmentError::TransactionIndexOutOfRange { .. }) => {
                for (tx_idx, slot_idx) in items {
                    match raw_reader.read_raw_transaction(height, tx_idx) {
                        Ok(bytes) => assign_tx_blob(slots, slot_idx, bytes),
                        Err(SettledRawBlockSegmentError::TransactionIndexOutOfRange { .. }) => {}
                        Err(err) => {
                            return Err(CanonicalStoreError::from(SettledSegmentsError::RawBlock(
                                err,
                            )));
                        }
                    }
                }
                continue;
            }
            Err(err) => {
                return Err(CanonicalStoreError::from(SettledSegmentsError::RawBlock(
                    err,
                )));
            }
        };
        for ((_, slot_idx), bytes) in items.into_iter().zip(txs) {
            assign_tx_blob(slots, slot_idx, bytes);
        }
    }
    Ok(())
}

fn populate_live_raw_tx_groups(
    db: &DB,
    live_groups: BTreeMap<BlockHeight, Vec<(u32, usize)>>,
    slots: &mut [Option<LocatedTransactionBlob>],
) -> Result<(), CanonicalStoreError> {
    if live_groups.is_empty() {
        return Ok(());
    }
    let heights = live_groups.keys().copied().collect::<Vec<_>>();
    let rows = fetch_height_batch_pinned(
        db,
        LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY,
        &heights,
        &verified_read_options(),
        "transaction blob batch read",
    )?;
    for ((_, items), row) in live_groups.into_iter().zip(rows) {
        let Some(encoded) = row? else {
            continue;
        };
        let raw_block = LiveTailRawBlock::decode_v1(encoded.as_ref())?;
        for (tx_idx, slot_idx) in items {
            let Ok(idx) = usize::try_from(tx_idx) else {
                continue;
            };
            if idx < raw_block.tx_slices.len() {
                let bytes = raw_block.read_verified_raw_transaction(idx)?;
                assign_tx_blob(slots, slot_idx, bytes);
            }
        }
    }
    Ok(())
}

fn assign_tx_blob(
    slots: &mut [Option<LocatedTransactionBlob>],
    slot_idx: usize,
    raw_bytes: Vec<u8>,
) {
    if let Some(Some((location, blob_slot))) = slots.get_mut(slot_idx) {
        *blob_slot = Some(TransactionBlobArtifact::new(*location, raw_bytes));
    }
}

/// Reads commitment-tree tip metadata for `tip` across settled segments and `live_tail_block`.
pub(in crate::canonical_store) fn read_persisted_tip_metadata(
    ctx: SegmentStoreContext<'_>,
    tip: BlockId,
) -> Result<ChainTipMetadata, CanonicalStoreError> {
    let compact = read_compact_block_split_with_options(ctx, tip.height, &uncached_read_options())?
        .ok_or_else(|| {
            CanonicalStoreError::block_load_sequence("published tip compact block is absent")
        })?;
    if compact.height() != tip.height || compact.block_hash() != tip.hash {
        return Err(CanonicalStoreError::block_load_sequence(
            "published tip compact block has the wrong canonical identity",
        ));
    }
    let metadata = compact.chain_metadata();
    Ok(ChainTipMetadata::new(
        metadata.sapling_commitment_tree_size,
        metadata.orchard_commitment_tree_size,
        metadata.ironwood_commitment_tree_size,
    ))
}

/// Rewrites `segments-manifest.json` atomically from `settled_segment_index`.
pub(in crate::canonical_store) fn sync_segments_manifest_from_db(
    db: &DB,
    network: Network,
) -> Result<CanonicalSegmentsManifest, CanonicalStoreError> {
    let entries = read_all_settled_segment_entries(db)?;
    if let Some(entries_net) = validate_contiguous_index_entries(&entries)?
        && entries_net != network
    {
        return Err(CanonicalStoreError::SettledSegment {
            reason: format!(
                "settled segment network {entries_net:?} disagrees with store network {network:?}"
            ),
        });
    }
    let manifest = CanonicalSegmentsManifest::from_entries(network, entries)?;
    write_segments_manifest_atomic(db.path(), &manifest)?;
    Ok(manifest)
}

/// Verifies `settled_segment_index`, `segments-manifest.json`, and every pinned segment file
/// into one shared [`SettledSegmentReaderCache`], validating module parity against `stored_set`.
pub(in crate::canonical_store) fn verify_cold_segments_and_manifest(
    store_root: &Path,
    db: &DB,
    expected_network: Network,
    stored_set: CanonicalStoredSet,
) -> Result<
    (
        Vec<SettledSegmentIndexEntry>,
        Arc<SettledSegmentReaderCache>,
    ),
    CanonicalStoreError,
> {
    let entries = read_all_settled_segment_entries(db)?;
    let expected_modules = segment_modules_for_stored_set(stored_set);
    for entry in &entries {
        if entry.modules != expected_modules {
            return Err(CanonicalStoreError::SettledSegment {
                reason: format!(
                    "settled segment {}..={} modules {:?} disagree with stored set {:?}",
                    entry.start_height.value(),
                    entry.end_block.height.value(),
                    entry.modules,
                    expected_modules
                ),
            });
        }
    }
    let manifest = read_segments_manifest(store_root)?;
    if manifest.network != expected_network {
        return Err(CanonicalStoreError::SettledSegment {
            reason: format!(
                "segments manifest network {:?} disagrees with expected store network {:?}",
                manifest.network, expected_network
            ),
        });
    }
    if manifest.entries != entries {
        return Err(CanonicalStoreError::SettledSegment {
            reason: format!(
                "segments manifest entries ({}) disagree with settled_segment_index ({})",
                manifest.entries.len(),
                entries.len()
            ),
        });
    }
    let segment_cache = SettledSegmentReaderCache::with_default_capacity(store_root, false)?;
    verify_pinned_segments_into_cache(store_root, &entries, false, &segment_cache)?;
    Ok((entries, segment_cache))
}

fn decode_block_header_record(
    height: BlockHeight,
    encoded: &[u8],
) -> Result<BlockHeaderArtifact, CanonicalStoreError> {
    if encoded.len() != BLOCK_HEADER_VALUE_LEN {
        return Err(CanonicalStoreError::publication(
            "block header is not the exact version-1 value",
        ));
    }
    let mut cursor = ByteReader::new(encoded);
    let block_hash_bytes = cursor.read_array::<32>("block_header.block_hash")?;
    let block_hash = decode_internal_block_hash(&block_hash_bytes).map_err(|source| {
        CanonicalStoreError::publication(format!("invalid block_header block_hash: {source}"))
    })?;
    let parent_hash_bytes = cursor.read_array::<32>("block_header.parent_hash")?;
    let parent_hash = decode_internal_block_hash(&parent_hash_bytes).map_err(|source| {
        CanonicalStoreError::publication(format!("invalid block_header parent_hash: {source}"))
    })?;
    let merkle_root_hash = cursor.read_array::<32>("block_header.merkle_root_hash")?;
    let commitment_bytes = cursor.read_array::<32>("block_header.commitment_bytes")?;
    let block_time = i64::from_le_bytes(cursor.read_array::<8>("block_header.block_time")?);
    let bits = u32::from_le_bytes(cursor.read_array::<4>("block_header.bits")?);
    let nonce = cursor.read_array::<32>("block_header.nonce")?;
    let version = u32::from_le_bytes(cursor.read_array::<4>("block_header.version")?);
    let block_size_bytes =
        u64::from_le_bytes(cursor.read_array::<8>("block_header.block_size_bytes")?);
    cursor.finish("block_header")?;
    Ok(BlockHeaderArtifact::new(
        height,
        block_hash,
        parent_hash,
        merkle_root_hash,
        commitment_bytes,
        block_time,
        bits,
        nonce,
        version,
        block_size_bytes,
    ))
}

fn verify_block_blob_identity(
    facts: &CanonicalBlockFacts,
    blob: &BlockBlobArtifact,
) -> Result<(), CanonicalStoreError> {
    let header = &facts.block_header;
    if blob.height != header.height
        || blob.block_hash != header.block_hash
        || blob.parent_hash != header.parent_hash
        || SerializedBytesDigest::from_serialized_bytes(&blob.raw_block_bytes)
            != facts.serialized_bytes_digest
    {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "block {} raw block blob does not match canonical facts",
            header.height.value()
        )));
    }
    Ok(())
}

fn verify_tx_blobs_against_facts(
    facts: &CanonicalBlockFacts,
    transaction_blobs: &[TransactionBlobArtifact],
) -> Result<(), CanonicalStoreError> {
    let header = &facts.block_header;
    if transaction_blobs.len() != facts.transactions.len() {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "block {} transaction blob count {} differs from facts count {}",
            header.height.value(),
            transaction_blobs.len(),
            facts.transactions.len()
        )));
    }
    for (tx_idx, (tx_facts, blob)) in facts.transactions.iter().zip(transaction_blobs).enumerate() {
        let expected_idx = u32::try_from(tx_idx).map_err(|_| {
            CanonicalStoreError::block_load_sequence("transaction index exceeds u32::MAX")
        })?;
        let loc = blob.location;
        if loc.transaction_id != tx_facts.public_facts.transaction_id
            || loc.block_height != header.height
            || loc.block_hash != header.block_hash
            || loc.tx_index_in_block != expected_idx
            || SerializedBytesDigest::from_serialized_bytes(&blob.raw_transaction_bytes)
                != tx_facts.serialized_bytes_digest
        {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} transaction {expected_idx} blob disagrees with canonical facts",
                header.height.value()
            )));
        }
    }
    Ok(())
}

fn verify_consensus_tail_slices(
    raw_block_bytes: &[u8],
    transaction_blobs: &[TransactionBlobArtifact],
) -> Result<Vec<RawBlockTransactionSlice>, CanonicalStoreError> {
    let total_tx_bytes = transaction_blobs.iter().try_fold(0_usize, |sum, blob| {
        sum.checked_add(blob.raw_transaction_bytes.len())
            .ok_or_else(|| {
                CanonicalStoreError::block_load_sequence("raw transaction bytes exceed usize::MAX")
            })
    })?;
    let tail_start = raw_block_bytes
        .len()
        .checked_sub(total_tx_bytes)
        .ok_or_else(|| {
            CanonicalStoreError::block_load_sequence(
                "raw block bytes are shorter than concatenated transaction blobs",
            )
        })?;
    let mut slices = Vec::with_capacity(transaction_blobs.len());
    let mut cursor = tail_start;
    for blob in transaction_blobs {
        let len = blob.raw_transaction_bytes.len();
        let end = cursor.checked_add(len).ok_or_else(|| {
            CanonicalStoreError::block_load_sequence("raw tx tail slice range overflow")
        })?;
        let observed_slice = raw_block_bytes.get(cursor..end).ok_or_else(|| {
            CanonicalStoreError::block_load_sequence("raw tx tail slice out of block bounds")
        })?;
        if observed_slice != blob.raw_transaction_bytes.as_slice() {
            return Err(CanonicalStoreError::block_load_sequence(
                "raw transaction bytes do not match consensus tail slice of raw block",
            ));
        }
        let offset_u32 = u32::try_from(cursor).map_err(|_| {
            CanonicalStoreError::block_load_sequence("raw tx slice offset exceeds u32::MAX")
        })?;
        let len_u32 = u32::try_from(len).map_err(|_| {
            CanonicalStoreError::block_load_sequence("raw tx slice length exceeds u32::MAX")
        })?;
        slices.push(RawBlockTransactionSlice::new(offset_u32, len_u32));
        cursor = end;
    }
    Ok(slices)
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
    use rust_rocksdb::{ColumnFamilyDescriptor, Options};
    use tempfile::TempDir;
    use zinder_core::{
        BlockHash, CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion,
        CanonicalHistoryBounds, CanonicalTransactionFacts, ChainEpochId, CompactTransparentOutput,
        ConsensusBranchId, LockTime, PrivacyShape, TransactionComponentCounts, TransactionId,
        TransactionIntrinsicValueBalances, TransactionLocation, TransactionPublicFacts,
        TransactionVersion, TransparentAddressScriptHash, TransparentOutputFact,
        decode_canonical_block_replay, encode_canonical_block_replay,
    };

    use super::super::super::{
        CANONICAL_STORE_SCHEMA_VERSION, CanonicalStoreReadyEvidence,
        block_load::encode_block_header,
        block_replay::{
            CanonicalReplayRangeScan, CanonicalReplayScan, validate_persisted_block_replays,
        },
        settled_segments::{
            layout::SettledSegmentFilePaths,
            sealer::{SettledSegmentBlockInput, SettledSegmentSealer, SettledSegmentSealerConfig},
        },
        stored_set::CANONICAL_DATA_COLUMN_FAMILIES,
    };
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn open_bridge_test_db(path: &Path) -> Result<DB, Box<dyn std::error::Error>> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let descriptors = CANONICAL_DATA_COLUMN_FAMILIES
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
        Ok(DB::open_cf_descriptors(&opts, path, descriptors)?)
    }

    fn bridge_tx_fixture(
        txid: TransactionId,
        height: BlockHeight,
        block_hash: BlockHash,
        tx_index: u32,
        tx_bytes: &[u8],
    ) -> Result<
        (
            zinder_core::CompactTransaction,
            CanonicalTransactionFacts,
            TransactionBlobArtifact,
        ),
        Box<dyn std::error::Error>,
    > {
        let loc = TransactionLocation::new(txid, height, block_hash, tx_index);
        let script = vec![0x51];
        let compact_tx = zinder_core::CompactTransaction {
            index: u64::from(tx_index),
            transaction_id: txid,
            data: zinder_core::CompactTransactionData {
                fee_zat: Some(100),
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 5_000,
                    script_pub_key: script.clone(),
                }],
                ..Default::default()
            },
        };
        let tx_fact = CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id: txid,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::V5,
                consensus_branch_id: Some(ConsensusBranchId::new(0xc2d6_d0b4)),
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: u32::try_from(tx_bytes.len())?,
                counts: TransactionComponentCounts {
                    transparent_input_count: 0,
                    transparent_output_count: 1,
                    sapling_spend_count: 0,
                    sapling_output_count: 0,
                    orchard_action_count: 0,
                    ironwood_action_count: 0,
                    sprout_joinsplit_count: 0,
                },
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: PrivacyShape::TransparentOnly,
                is_coinbase: false,
                unsupported_sections: Vec::new(),
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(tx_bytes),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::new(0, 0, 0, 0),
            transparent_inputs: Vec::new(),
            transparent_outputs: vec![TransparentOutputFact::new(
                0,
                5_000,
                script,
                TransparentAddressScriptHash::of_script_pub_key(&[0x51]),
            )],
        };
        let blob = TransactionBlobArtifact::new(loc, tx_bytes.to_vec());
        Ok((compact_tx, tx_fact, blob))
    }

    fn bridge_block_hashes(height_num: u32) -> (BlockHash, BlockHash) {
        let mut hash_bytes = [0_u8; 32];
        hash_bytes[0] = 0xa5;
        hash_bytes[1..5].copy_from_slice(&height_num.to_be_bytes());
        let block_hash = BlockHash::from_bytes(hash_bytes);
        let mut prev_bytes = [0_u8; 32];
        prev_bytes[0] = 0xa5;
        prev_bytes[1..5].copy_from_slice(&height_num.saturating_sub(1).to_be_bytes());
        let parent_hash = if height_num == 0 {
            BlockHash::from_bytes([0_u8; 32])
        } else {
            BlockHash::from_bytes(prev_bytes)
        };
        (block_hash, parent_hash)
    }

    fn bridge_compact_block(
        height_num: u32,
        block_hash: BlockHash,
        parent_hash: BlockHash,
        compact_txs: Vec<zinder_core::CompactTransaction>,
    ) -> Result<(CompactBlockArtifact, ChainTipMetadata), Box<dyn std::error::Error>> {
        let height = BlockHeight::new(height_num);
        let tip_metadata = ChainTipMetadata::new(height_num, height_num * 2, height_num * 3);
        let compact_meta = zinder_core::CompactChainMetadata {
            sapling_commitment_tree_size: tip_metadata.sapling_commitment_tree_size,
            orchard_commitment_tree_size: tip_metadata.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: tip_metadata.ironwood_commitment_tree_size,
        };
        let compact_block = CompactBlockArtifact::new(
            BlockId::new(height, block_hash),
            parent_hash,
            1_700_000_000 + height_num,
            compact_txs,
            compact_meta,
        )?;
        Ok((compact_block, tip_metadata))
    }

    type BridgeBlockFixture = (CanonicalBuildBlock, Vec<Vec<u8>>);

    fn bridge_block_fixture(
        height_num: u32,
        tx_count: u32,
    ) -> Result<BridgeBlockFixture, Box<dyn std::error::Error>> {
        let height = BlockHeight::new(height_num);
        let (block_hash, parent_hash) = bridge_block_hashes(height_num);
        let mut compact_txs = Vec::new();
        let mut tx_facts = Vec::new();
        let mut tx_blobs = Vec::new();
        let mut raw_tx_list = Vec::new();
        let mut raw_block_bytes = b"hdr:".to_vec();
        for tx_idx in 0..tx_count {
            let tx_bytes = format!("tx-payload-{height_num}-{tx_idx}").into_bytes();
            let mut txid_bytes = [0_u8; 32];
            txid_bytes[0] = u8::try_from(height_num % 200).unwrap_or(1);
            txid_bytes[1] = u8::try_from(tx_idx + 1).unwrap_or(1);
            let txid = TransactionId::from_bytes(txid_bytes);
            let (compact_tx, tx_fact, blob) =
                bridge_tx_fixture(txid, height, block_hash, tx_idx, &tx_bytes)?;
            compact_txs.push(compact_tx);
            tx_facts.push(tx_fact);
            tx_blobs.push(blob);
            raw_block_bytes.extend_from_slice(&tx_bytes);
            raw_tx_list.push(tx_bytes);
        }
        let header = BlockHeaderArtifact::new(
            height,
            block_hash,
            parent_hash,
            [0x11; 32],
            [0x22; 32],
            i64::from(1_700_000_000 + height_num),
            0x1f07_ffff,
            [0x33; 32],
            4,
            u64::try_from(raw_block_bytes.len())?,
        );
        let facts = CanonicalBlockFacts {
            block_header: header,
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&raw_block_bytes),
            transactions: tx_facts,
        };
        let (compact_block, tip_metadata) =
            bridge_compact_block(height_num, block_hash, parent_hash, compact_txs)?;
        let replay_envelope = encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );
        let build_block = CanonicalBuildBlock {
            facts,
            replay_envelope,
            compact_block,
            tip_metadata,
            tree_state_checkpoint: None,
            transaction_blobs: tx_blobs,
            block_blob: Some(BlockBlobArtifact::new(
                height,
                block_hash,
                parent_hash,
                raw_block_bytes,
            )),
        };
        Ok((build_block, raw_tx_list))
    }

    type SealedSeedFixture = (
        Vec<SettledSegmentIndexEntry>,
        Vec<Vec<u8>>,
        Vec<Vec<Vec<u8>>>,
    );

    fn seed_settled_replay(
        db: &DB,
        stride: u32,
        segment_count: u32,
    ) -> Result<Vec<ValidatedCanonicalBlockReplay>, Box<dyn std::error::Error>> {
        let header_cf = db
            .cf_handle(BLOCK_HEADER_COLUMN_FAMILY)
            .ok_or("header cf missing")?;
        let index_cf = db
            .cf_handle(SETTLED_SEGMENT_INDEX_COLUMN_FAMILY)
            .ok_or("index cf missing")?;
        let modules = SettledSegmentModules {
            supplement: true,
            raw_bodies: false,
        };
        let config = SettledSegmentSealerConfig::new(Network::ZcashRegtest, 1, modules)
            .with_segment_blocks(stride);
        let mut sealer = SettledSegmentSealer::new(db.path(), config, None)?;
        let mut expected = Vec::new();
        for height in 1..=stride * segment_count {
            let (block, _) = bridge_block_fixture(height, 2)?;
            let header = &block.facts.block_header;
            db.put_cf(
                &header_cf,
                encode_height_key_ascending(header.height),
                encode_block_header(header),
            )?;
            expected.push(decode_canonical_block_replay(
                block.replay_envelope.as_bytes(),
            )?);
            let entries = sealer.push_block(SettledSegmentBlockInput {
                compact_block: block.compact_block,
                native_bytes: 512,
                canonical_facts: block.facts,
                raw_block: None,
            })?;
            for entry in entries {
                db.put_cf(
                    &index_cf,
                    encode_height_key_ascending(entry.start_height),
                    entry.encode_v1()?,
                )?;
            }
        }
        Ok(expected)
    }

    #[test]
    fn settled_replay_matches_full_facts_across_partial_segments_without_shielded_files()
    -> TestResult {
        let dir = TempDir::new()?;
        let db = open_bridge_test_db(dir.path())?;
        let expected = seed_settled_replay(&db, 272, 2)?;
        let entries = read_all_settled_segment_entries(&db)?;
        assert_eq!(entries.len(), 2);
        for entry in &entries {
            std::fs::remove_file(SettledSegmentFilePaths::for_entry(dir.path(), entry)?.shielded)?;
        }
        let cache = SettledSegmentReaderCache::new(dir.path(), 4, false)?;
        let ctx = SegmentStoreContext {
            db: &db,
            segment_cache: Some(&cache),
            sealed_segments: Some(&entries),
        };
        let checkpoint = entries[1].end_sequence_checkpoint;
        let first = &expected[0].facts().block_header;
        let ready = CanonicalStoreReadyEvidence {
            schema_version: crate::CANONICAL_STORE_SCHEMA_VERSION,
            first_retained_block: BlockId::new(first.height, first.block_hash),
            visible_tip: checkpoint.through(),
            visible_epoch: ChainEpochId::new(1),
            visible_event_sequence: 1,
            visible_block_count: checkpoint.retained_block_count(),
            block_digest_version: CanonicalBlockFactsDigestVersion::CURRENT,
            replay_format_version: CanonicalBlockReplayFormatVersion::CURRENT,
            sequence_digest_version: checkpoint.sequence_digest().version(),
            visible_sequence_digest: checkpoint.sequence_digest().as_bytes(),
            visible_logical_replay_bytes: checkpoint.logical_replay_bytes(),
            sequence_checkpoint: checkpoint,
            construction_manifest_version: crate::CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION,
            construction_manifest_sha256: [0; 32],
        };
        let full = CanonicalReplayScan::new(ctx, &ready)?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(full, expected);
        let range = BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(274));
        let partial =
            CanonicalReplayRangeScan::new(ctx, &ready, range)?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(partial, expected[1..274]);
        assert_eq!(
            read_replay_step_split(ctx, range.end)?.replay,
            expected[273]
        );
        let persisted = validate_persisted_block_replays(ctx)?;
        assert_eq!(persisted.sequence_digest, checkpoint.sequence_digest());

        let family = db
            .cf_handle(BLOCK_HEADER_COLUMN_FAMILY)
            .ok_or("header cf missing")?;
        let mut malformed_key = encode_height_key_ascending(entries[0].end_block.height).to_vec();
        malformed_key.push(0);
        db.put_cf(&family, malformed_key, [0xff])?;
        assert!(matches!(
            CanonicalReplayRangeScan::new(ctx, &ready, range)?.collect::<Result<Vec<_>, _>>(),
            Err(CanonicalStoreError::BlockReplaySequenceInvalid { .. })
        ));
        Ok(())
    }

    #[test]
    fn settled_replay_rejects_a_header_key_after_maximum_height() -> TestResult {
        let dir = TempDir::new()?;
        let db = open_bridge_test_db(dir.path())?;
        let family = db
            .cf_handle(BLOCK_HEADER_COLUMN_FAMILY)
            .ok_or("header cf missing")?;
        let (mut block, _) = bridge_block_fixture(1, 1)?;
        let height = BlockHeight::new(u32::MAX);
        block.facts.block_header.height = height;
        let key = encode_height_key_ascending(height);
        db.put_cf(&family, key, encode_block_header(&block.facts.block_header))?;
        let range = BlockHeightRange::inclusive(height, height);
        assert_eq!(read_replay_headers(&db, range)?, [block.facts.block_header]);

        let mut malformed_key = key.to_vec();
        malformed_key.push(0);
        db.put_cf(&family, malformed_key, [0xff])?;
        assert!(matches!(
            read_replay_headers(&db, range),
            Err(CanonicalStoreError::BlockReplaySequenceInvalid { .. })
        ));
        Ok(())
    }

    #[test]
    fn settled_replay_rejects_missing_and_corrupt_header_rows() -> TestResult {
        let dir = TempDir::new()?;
        let db = open_bridge_test_db(dir.path())?;
        let expected = seed_settled_replay(&db, 3, 1)?;
        let entries = read_all_settled_segment_entries(&db)?;
        let ctx = SegmentStoreContext {
            db: &db,
            segment_cache: None,
            sealed_segments: Some(&entries),
        };
        let supplement = open_replay_supplement_reader(ctx, &entries[0])?;
        let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(3));
        let family = db
            .cf_handle(BLOCK_HEADER_COLUMN_FAMILY)
            .ok_or("header cf missing")?;
        for replay in &expected {
            let header = &replay.facts().block_header;
            let key = encode_height_key_ascending(header.height);
            db.delete_cf(&family, key)?;
            assert!(matches!(
                reconstruct_replay_range_from_segment(&db, &supplement, range),
                Err(CanonicalStoreError::BlockReplaySequenceInvalid { .. })
            ));
            db.put_cf(&family, key, [0xff])?;
            let error = reconstruct_replay_range_from_segment(&db, &supplement, range)
                .err()
                .ok_or("malformed header accepted")?;
            assert!(
                error
                    .to_string()
                    .contains("block header is not the exact version-1 value")
            );
            db.put_cf(&family, key, encode_block_header(header))?;
        }

        let header = &expected[1].facts().block_header;
        let key = encode_height_key_ascending(header.height);
        let mut malformed_key = key.to_vec();
        malformed_key.push(0);
        db.put_cf(&family, &malformed_key, encode_block_header(header))?;
        assert!(matches!(
            reconstruct_replay_range_from_segment(&db, &supplement, range),
            Err(CanonicalStoreError::BlockReplaySequenceInvalid { .. })
        ));
        db.delete_cf(&family, malformed_key)?;

        let mut wrong_hash = header.clone();
        wrong_hash.block_hash = BlockHash::from_bytes([0xff; 32]);
        let mut wrong_parent = header.clone();
        wrong_parent.parent_hash = BlockHash::from_bytes([0xff; 32]);
        let mut wrong_time = header.clone();
        wrong_time.block_time += 1;
        for wrong_header in [wrong_hash, wrong_parent, wrong_time] {
            db.put_cf(&family, key, encode_block_header(&wrong_header))?;
            let error = reconstruct_replay_range_from_segment(&db, &supplement, range)
                .err()
                .ok_or("mismatched header accepted")?;
            assert!(
                error
                    .to_string()
                    .contains("disagrees with ZSTSUP01 envelope")
            );
        }
        Ok(())
    }

    #[test]
    fn settled_replay_requires_authentic_supplement_chunks() -> TestResult {
        let dir = TempDir::new()?;
        let db = open_bridge_test_db(dir.path())?;
        seed_settled_replay(&db, 3, 1)?;
        let entries = read_all_settled_segment_entries(&db)?;
        let ctx = SegmentStoreContext {
            db: &db,
            segment_cache: None,
            sealed_segments: Some(&entries),
        };
        let entry = &entries[0];
        let height = entry.start_height;
        let reader = open_replay_supplement_reader(ctx, entry)?;
        let plan = reader.index().plan_block_range(height, height)?;
        let chunk = plan.chunks().first().ok_or("missing supplement chunk")?;
        let chunk_offset = usize::try_from(chunk.compressed_range().offset)?;
        drop(reader);

        let path = SettledSegmentFilePaths::for_entry(dir.path(), entry)?
            .supplement
            .ok_or("missing supplement path")?;
        let mut bytes = std::fs::read(&path)?;
        std::fs::remove_file(&path)?;
        assert!(matches!(
            read_replay_step_split(ctx, height),
            Err(CanonicalStoreError::SettledSegment { .. })
        ));
        bytes[chunk_offset] ^= 0xff;
        std::fs::write(path, bytes)?;
        let error = read_replay_step_split(ctx, height)
            .err()
            .ok_or("corrupt supplement accepted")?;
        assert!(
            error
                .to_string()
                .contains("supplement chunk 0 SHA-256 digest mismatch")
        );
        Ok(())
    }

    fn seed_segment_and_live_tail_db(
        dir: &Path,
        db: &DB,
        stored_all: CanonicalStoredSet,
        settled_blocks: u32,
    ) -> Result<SealedSeedFixture, Box<dyn std::error::Error>> {
        let header_cf = db
            .cf_handle(BLOCK_HEADER_COLUMN_FAMILY)
            .ok_or("header cf missing")?;
        let live_cf = db
            .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
            .ok_or("live cf missing")?;
        let live_raw_cf = db
            .cf_handle(LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY)
            .ok_or("live raw cf missing")?;
        let index_cf = db
            .cf_handle(SETTLED_SEGMENT_INDEX_COLUMN_FAMILY)
            .ok_or("index cf missing")?;
        let modules = segment_modules_for_stored_set(stored_all);
        let config = SettledSegmentSealerConfig::new(
            Network::ZcashRegtest,
            CANONICAL_STORE_SCHEMA_VERSION,
            modules,
        )
        .with_segment_blocks(2);
        let mut sealer = SettledSegmentSealer::new(dir, config, None)?;
        let mut expected_txs = Vec::new();
        let mut expected_blocks = Vec::new();
        let mut sealed_entries = Vec::new();
        for h in 1..=settled_blocks + 1 {
            let (blk, tx_raws) = bridge_block_fixture(h, 2)?;
            let key = encode_height_key_ascending(BlockHeight::new(h));
            db.put_cf(
                &header_cf,
                key,
                encode_block_header(&blk.facts.block_header),
            )?;
            if h <= settled_blocks {
                let (input, raw_bytes) = bridge_settled_input(blk)?;
                expected_blocks.push(raw_bytes);
                expected_txs.push(tx_raws);
                let entries = sealer.push_block(input)?;
                for entry in entries {
                    db.put_cf(
                        &index_cf,
                        encode_height_key_ascending(entry.start_height),
                        entry.encode_v1()?,
                    )?;
                    sealed_entries.push(entry);
                }
            } else {
                let live_bytes = blk
                    .block_blob
                    .as_ref()
                    .ok_or("missing live block blob")?
                    .raw_block_bytes
                    .clone();
                expected_blocks.push(live_bytes);
                expected_txs.push(tx_raws);
                let encoded = encode_split_live_tail_from_build_block(&blk, stored_all)?;
                db.put_cf(&live_cf, key, encoded.block_bytes)?;
                if let Some(raw_bytes) = encoded.raw_block_bytes {
                    db.put_cf(&live_raw_cf, key, raw_bytes)?;
                }
            }
        }
        Ok((sealed_entries, expected_blocks, expected_txs))
    }

    fn bridge_settled_input(
        mut block: CanonicalBuildBlock,
    ) -> Result<(SettledSegmentBlockInput, Vec<u8>), Box<dyn std::error::Error>> {
        let mut blob = block.block_blob.take().ok_or("missing block blob")?;
        let raw_block = live_tail_raw_block_from_blobs(
            &block.facts,
            Some(&blob),
            &block.transaction_blobs,
            RawBlobRetention::All,
        )?;
        let raw_bytes = blob.raw_block_bytes.clone();
        blob.raw_block_bytes.push(0xff);
        assert!(
            live_tail_raw_block_from_blobs(
                &block.facts,
                Some(&blob),
                &block.transaction_blobs,
                RawBlobRetention::All,
            )
            .is_err()
        );
        Ok((
            SettledSegmentBlockInput {
                compact_block: block.compact_block,
                native_bytes: 512,
                canonical_facts: block.facts,
                raw_block,
            },
            raw_bytes,
        ))
    }

    #[test]
    fn split_reads_lazy_opening_and_grouped_batch_tx_reads_work() -> TestResult {
        let dir = TempDir::new()?;
        let db = open_bridge_test_db(dir.path())?;
        let stored_all = CanonicalStoredSet::for_history(
            RawBlobRetention::All,
            CanonicalHistoryBounds::complete(),
        );
        let (entries, expected_blocks, expected_txs) =
            seed_segment_and_live_tail_db(dir.path(), &db, stored_all, 2)?;
        let [entry] = entries.as_slice() else {
            return Err("expected one settled segment".into());
        };
        let manifest = sync_segments_manifest_from_db(&db, Network::ZcashRegtest)?;
        assert_eq!(manifest.sealed_tip, Some(entry.end_block));
        assert_eq!(read_sealed_tip(&db)?, Some(entry.end_block));
        assert_eq!(
            find_sealed_segment_in_slice(&entries, BlockHeight::new(2)),
            Some(entry)
        );
        assert_eq!(
            find_sealed_segment_in_slice(&entries, BlockHeight::new(3)),
            None
        );

        let cache = SettledSegmentReaderCache::new(dir.path(), 4, false)?;
        let ctx = SegmentStoreContext {
            db: &db,
            segment_cache: Some(&cache),
            sealed_segments: Some(&entries),
        };
        assert_split_reads_and_lazy_raw_open(
            ctx,
            &cache,
            stored_all,
            &expected_blocks,
            &expected_txs,
        )?;
        assert_grouped_batch_and_tip_reads(&db, ctx, stored_all, &expected_txs)?;

        let stored_none = CanonicalStoredSet::for_history(
            RawBlobRetention::None,
            CanonicalHistoryBounds::complete(),
        );
        assert_eq!(
            read_raw_block_split(ctx, stored_none, BlockHeight::new(1))?,
            None
        );
        assert_eq!(
            read_raw_transaction_split(ctx, stored_none, BlockHeight::new(1), 0)?,
            None
        );
        Ok(())
    }

    #[test]
    fn settled_segment_metrics_follow_schema_v2_reads_and_eviction() -> TestResult {
        let dir = TempDir::new()?;
        let db = open_bridge_test_db(dir.path())?;
        let stored_all = CanonicalStoredSet::for_history(
            RawBlobRetention::All,
            CanonicalHistoryBounds::complete(),
        );
        seed_segment_and_live_tail_db(dir.path(), &db, stored_all, 4)?;
        let entries = read_all_settled_segment_entries(&db)?;
        let [first, second] = entries.as_slice() else {
            return Err("expected two settled segments".into());
        };
        let recorder = PrometheusBuilder::new()
            .set_buckets_for_metric(Matcher::Suffix("_duration_seconds".to_owned()), &[1.0])?
            .build_recorder();
        let metrics = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            let cache = SettledSegmentReaderCache::new(dir.path(), 1, false)?;
            let ctx = SegmentStoreContext {
                db: &db,
                segment_cache: Some(&cache),
                sealed_segments: None,
            };
            let initial = metrics.render();
            assert_segment_cache_metrics(&initial, [0; 3], 0, 0)?;

            read_metrics_fixture_block(ctx, stored_all, first.start_height)?;
            let cold = metrics.render();
            read_metrics_fixture_block(ctx, stored_all, first.start_height)?;
            let warm = metrics.render();

            let readers = cache.get_or_open(first)?;
            let heap = [
                readers.shielded.index_heap_bytes(),
                readers
                    .supplement
                    .as_ref()
                    .ok_or("supplement reader")?
                    .index_heap_bytes(),
                readers
                    .raw_block
                    .as_ref()
                    .ok_or("raw block reader")?
                    .index_heap_bytes(),
            ];
            assert!(heap.iter().all(|bytes| *bytes > 0));
            assert_segment_cache_metrics(&cold, heap, 0, 1)?;

            assert_segment_cache_metrics(&warm, heap, 1, 1)?;
            assert_segment_io_increased(&cold, &warm)?;

            let second_reader = open_shielded_segment_reader(ctx, second)?;
            let evicted = metrics.render();
            assert_segment_residency(&evicted, [second_reader.index_heap_bytes(), 0, 0], [1; 3])?;
            assert!(!cache.contains_start_height(first.start_height));
            assert!(cache.contains_start_height(second.start_height));

            drop(cache);
            assert_segment_residency(&metrics.render(), [0; 3], [1; 3])?;
            Ok(())
        })
    }

    #[test]
    fn settled_segment_residency_gauges_preserve_other_caches_on_drop() -> TestResult {
        let dir = TempDir::new()?;
        let db = open_bridge_test_db(dir.path())?;
        let stored_all = CanonicalStoredSet::for_history(
            RawBlobRetention::All,
            CanonicalHistoryBounds::complete(),
        );
        seed_segment_and_live_tail_db(dir.path(), &db, stored_all, 4)?;
        let entries = read_all_settled_segment_entries(&db)?;
        let first = entries.first().ok_or("expected first settled segment")?;
        let recorder = PrometheusBuilder::new()
            .set_buckets_for_metric(Matcher::Suffix("_duration_seconds".to_owned()), &[1.0])?
            .build_recorder();
        let metrics = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            let cache_a = SettledSegmentReaderCache::new(dir.path(), 2, false)?;
            let cache_b = SettledSegmentReaderCache::new(dir.path(), 2, false)?;

            let readers = cache_a.get_or_open(first)?;
            let heap = [
                readers.shielded.index_heap_bytes(),
                readers
                    .supplement
                    .as_ref()
                    .ok_or("supplement reader")?
                    .index_heap_bytes(),
                readers
                    .raw_block
                    .as_ref()
                    .ok_or("raw block reader")?
                    .index_heap_bytes(),
            ];
            let _ = cache_b.get_or_open(first)?;

            let both = metrics.render();
            for (kind, bytes) in METRIC_KINDS.into_iter().zip(heap) {
                assert_eq!(segment_metric(&both, "cache_open_readers", kind)?, 2);
                assert_eq!(
                    segment_metric(&both, "cache_index_heap_bytes", kind)?,
                    u64::try_from(bytes)? * 2
                );
            }

            drop(cache_a);
            assert_segment_residency(&metrics.render(), heap, [0; 3])?;

            drop(cache_b);
            assert_segment_residency(&metrics.render(), [0; 3], [0; 3])?;
            Ok(())
        })
    }

    const METRIC_KINDS: [&str; 3] = ["shielded", "supplement", "block_blob"];

    fn read_metrics_fixture_block(
        ctx: SegmentStoreContext<'_>,
        stored: CanonicalStoredSet,
        height: BlockHeight,
    ) -> TestResult {
        assert!(read_compact_block_split(ctx, height)?.is_some());
        assert!(read_raw_block_split(ctx, stored, height)?.is_some());
        Ok(())
    }

    fn segment_metric(
        exposition: &str,
        suffix: &str,
        kind: &str,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let prefix = format!("zinder_store_settled_segment_{suffix}{{kind=\"{kind}\"}} ");
        let sample = exposition
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .ok_or_else(|| format!("missing metric {prefix}"))?;
        Ok(sample.parse()?)
    }

    fn assert_segment_residency(
        exposition: &str,
        heap: [usize; 3],
        evictions: [u64; 3],
    ) -> TestResult {
        for ((kind, bytes), evicted) in METRIC_KINDS.into_iter().zip(heap).zip(evictions) {
            assert_eq!(
                segment_metric(exposition, "cache_open_readers", kind)?,
                u64::from(bytes != 0)
            );
            assert_eq!(
                segment_metric(exposition, "cache_index_heap_bytes", kind)?,
                u64::try_from(bytes)?
            );
            assert_eq!(
                segment_metric(exposition, "cache_evictions_total", kind)?,
                evicted
            );
        }
        Ok(())
    }

    fn assert_segment_cache_metrics(
        exposition: &str,
        heap: [usize; 3],
        hits: u64,
        misses: u64,
    ) -> TestResult {
        assert_segment_residency(exposition, heap, [0; 3])?;
        for kind in METRIC_KINDS {
            assert_eq!(segment_metric(exposition, "cache_hits_total", kind)?, hits);
            assert_eq!(
                segment_metric(exposition, "cache_misses_total", kind)?,
                misses
            );
        }
        Ok(())
    }

    fn assert_segment_io_increased(cold: &str, warm: &str) -> TestResult {
        for kind in METRIC_KINDS {
            for metric in ["pread_bytes_total", "pread_duration_seconds_count"] {
                let before = segment_metric(cold, metric, kind)?;
                assert!(before > 0, "cold {kind} {metric}");
                assert!(
                    segment_metric(warm, metric, kind)? > before,
                    "warm {kind} {metric}"
                );
            }
        }
        Ok(())
    }

    fn assert_split_reads_and_lazy_raw_open(
        ctx: SegmentStoreContext<'_>,
        cache: &SettledSegmentReaderCache,
        stored_all: CanonicalStoredSet,
        expected_blocks: &[Vec<u8>],
        expected_txs: &[Vec<Vec<u8>>],
    ) -> TestResult {
        use super::super::layout::SettledSegmentKind;

        for idx in 0..3_usize {
            let h = BlockHeight::new(u32::try_from(idx + 1)?);
            assert_ne!(read_replay_step_split(ctx, h)?.logical_replay_bytes, 0);
            assert!(read_compact_block_split(ctx, h)?.is_some());
        }
        assert_eq!(cache.open_count(SettledSegmentKind::RawBlock), 0);

        for idx in 0..3_usize {
            let h = BlockHeight::new(u32::try_from(idx + 1)?);
            assert_eq!(
                read_raw_block_split(ctx, stored_all, h)?,
                Some(expected_blocks[idx].clone())
            );
            assert_eq!(
                read_raw_transaction_split(ctx, stored_all, h, 0)?,
                Some(expected_txs[idx][0].clone())
            );
            assert_eq!(read_raw_transaction_split(ctx, stored_all, h, 9)?, None);
        }
        assert_eq!(cache.open_count(SettledSegmentKind::RawBlock), 1);
        Ok(())
    }

    fn assert_grouped_batch_and_tip_reads(
        db: &DB,
        ctx: SegmentStoreContext<'_>,
        stored_all: CanonicalStoredSet,
        expected_txs: &[Vec<Vec<u8>>],
    ) -> TestResult {
        let mut slots = vec![
            Some((
                TransactionLocation::new(
                    TransactionId::from_bytes([1; 32]),
                    BlockHeight::new(1),
                    bridge_block_hashes(1).0,
                    1,
                ),
                None,
            )),
            Some((
                TransactionLocation::new(
                    TransactionId::from_bytes([2; 32]),
                    BlockHeight::new(1),
                    bridge_block_hashes(1).0,
                    0,
                ),
                None,
            )),
            Some((
                TransactionLocation::new(
                    TransactionId::from_bytes([3; 32]),
                    BlockHeight::new(3),
                    bridge_block_hashes(3).0,
                    1,
                ),
                None,
            )),
        ];
        populate_raw_transaction_blobs_batch(ctx, stored_all, &mut slots)?;
        assert_eq!(
            slots[0]
                .as_ref()
                .and_then(|(_, b)| b.as_ref())
                .map(|b| &b.raw_transaction_bytes),
            Some(&expected_txs[0][1])
        );
        assert_eq!(
            slots[1]
                .as_ref()
                .and_then(|(_, b)| b.as_ref())
                .map(|b| &b.raw_transaction_bytes),
            Some(&expected_txs[0][0])
        );
        assert_eq!(
            slots[2]
                .as_ref()
                .and_then(|(_, b)| b.as_ref())
                .map(|b| &b.raw_transaction_bytes),
            Some(&expected_txs[2][1])
        );

        let tip_3 = BlockId::new(
            BlockHeight::new(3),
            bridge_block_fixture(3, 2)?.0.facts.block_header.block_hash,
        );
        assert_eq!(
            read_persisted_tip_metadata(
                SegmentStoreContext {
                    db,
                    segment_cache: None,
                    sealed_segments: None,
                },
                tip_3
            )?,
            ChainTipMetadata::new(3, 6, 9)
        );
        Ok(())
    }

    #[test]
    fn empty_settled_index_still_writes_network_manifest() -> TestResult {
        let dir = TempDir::new()?;
        let db = open_bridge_test_db(dir.path())?;
        let manifest = sync_segments_manifest_from_db(&db, Network::ZcashTestnet)?;
        assert_eq!(manifest.network, Network::ZcashTestnet);
        assert_eq!(manifest.sealed_tip, None);
        assert_eq!(manifest.entries.len(), 0);
        assert_eq!(read_sealed_tip(&db)?, None);
        Ok(())
    }
}
