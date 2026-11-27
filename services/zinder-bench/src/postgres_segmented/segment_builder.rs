//! Blocking segment sealing for `ZSHRNG02`, `ZSTSUP01`, and `ZSBLKB01` segments
//! with recursive `max_segment_bytes` pre-split enforcement.

use std::fs;
use std::path::Path;

use tempfile::{NamedTempFile, TempDir};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CanonicalBlockFacts, CompactBlockArtifact, Network,
    TransactionBlobArtifact,
};
use zinder_store::{
    CURRENT_ARTIFACT_SCHEMA_VERSION, RawBlockCodec, RawBlockSegmentIdentity,
    RawBlockTransactionSlice, SettledRawBlockSegmentWriter, SettledShieldedSegmentWriter,
    SettledSupplementSegmentWriteOptions, SettledSupplementSegmentWriter, ShieldedSegmentIdentity,
    SupplementCompressionCodec, SupplementSegmentIdentity, encode_compact_block_artifact,
};

use crate::error::BenchError;

use super::database::candidate_error;
use super::schema::MAX_SETTLED_SEGMENT_BYTES;

/// Fully prepared block inputs ready for segment sealing or `canonical_live_tail` insertion.
#[derive(Clone, Debug)]
pub struct PreparedSegmentedBlock {
    /// Block height on the linear benchmark chain.
    pub height: BlockHeight,
    /// Block hash.
    pub block_hash: BlockHash,
    /// Parent block hash.
    pub previous_block_hash: BlockHash,
    /// Positioned native compact block artifact.
    pub compact_block: CompactBlockArtifact,
    /// Native protobuf byte length used for `ZSHRNG02` size accounting.
    pub compact_block_proto_len: usize,
    /// Encoded `CompactBlockArtifact` protobuf bytes for live-tail storage.
    pub compact_block_bytes: Vec<u8>,
    /// Block-local canonical facts.
    pub facts: CanonicalBlockFacts,
    /// Encoded `CanonicalBlockReplayEnvelope` bytes for live-tail storage.
    pub canonical_facts_bytes: Vec<u8>,
    /// Optional serialized consensus raw block bytes when `full_blocks = true`.
    pub raw_block_bytes: Option<Vec<u8>>,
    /// Per-transaction slice bounds within `raw_block_bytes` when `full_blocks = true`.
    pub raw_tx_slices: Vec<RawBlockTransactionSlice>,
}

/// Complete sealed artifact trio produced for one contiguous settled span.
#[derive(Clone, Debug)]
pub struct SealedSegmentBundle {
    /// Shielded history segment (`ZSHRNG02`).
    pub shielded: SealedSegmentBytes,
    /// Compact/canonical supplement segment (`ZSTSUP01`, 16-block Zstd chunks).
    pub supplement: SealedSegmentBytes,
    /// Optional raw block body segment (`ZSBLKB01`, per-block Zstd framing).
    pub raw_block: Option<SealedSegmentBytes>,
}

/// Framed immutable bytes and cryptographic identities for one settled segment.
#[derive(Clone, Debug)]
pub struct SealedSegmentBytes {
    /// First height covered (inclusive).
    pub start_height: BlockHeight,
    /// Last height covered (inclusive).
    pub end_height: BlockHeight,
    /// Whole-segment trailing SHA-256 digest.
    pub segment_sha256: [u8; 32],
    /// Cold-admission `index_sha256` digest.
    pub index_sha256: [u8; 32],
    /// Exact framed segment bytes ready for `BYTEA STORAGE EXTERNAL`.
    pub bytes: Vec<u8>,
}

/// Sealed batches paired with the largest segment byte length across all spans.
#[derive(Clone, Debug)]
pub struct SealedSegmentsOutput {
    /// Ordered contiguous settled segment bundles covering `[first_height..=last_height]`.
    pub bundles: Vec<SealedSegmentBundle>,
    /// Largest single sealed segment byte length across all segment kinds.
    pub max_segment_bytes_observed: u64,
}

struct SealContext<'a> {
    scratch_root: &'a Path,
    network: Network,
    full_blocks: bool,
    max_segment_bytes: u64,
}

/// Derives verified transaction byte slices from one consensus block payload and
/// ordered [`TransactionBlobArtifact`] payloads.
pub fn extract_consensus_tx_slices(
    raw_block_bytes: &[u8],
    tx_blobs: &[TransactionBlobArtifact],
) -> Result<Vec<RawBlockTransactionSlice>, BenchError> {
    let total_tx_bytes = tx_blobs
        .iter()
        .try_fold(0usize, |acc, blob| {
            acc.checked_add(blob.raw_transaction_bytes.len())
        })
        .ok_or_else(|| candidate_error("total raw transaction length overflow"))?;
    let mut cursor = raw_block_bytes
        .len()
        .checked_sub(total_tx_bytes)
        .ok_or_else(|| {
            candidate_error("serialized raw block is shorter than its transaction payloads")
        })?;
    let mut slices = Vec::with_capacity(tx_blobs.len());
    for blob in tx_blobs {
        let len = blob.raw_transaction_bytes.len();
        let end = cursor
            .checked_add(len)
            .ok_or_else(|| candidate_error("raw block transaction slice overflow"))?;
        let candidate = raw_block_bytes
            .get(cursor..end)
            .ok_or_else(|| candidate_error("raw block transaction slice out of block bounds"))?;
        if candidate != blob.raw_transaction_bytes.as_slice() {
            return Err(candidate_error(format!(
                "consensus transaction blob mismatch at offset {cursor}"
            )));
        }
        let offset_u32 = u32::try_from(cursor)
            .map_err(|_| candidate_error("raw transaction offset exceeds u32"))?;
        let len_u32 = u32::try_from(len)
            .map_err(|_| candidate_error("raw transaction length exceeds u32"))?;
        slices.push(RawBlockTransactionSlice::new(offset_u32, len_u32));
        cursor = end;
    }
    Ok(slices)
}

/// Encodes `compact_block` once and returns its protobuf payload bytes.
#[must_use]
pub fn encode_compact_block_for_tail(compact_block: &CompactBlockArtifact) -> Vec<u8> {
    encode_compact_block_artifact(compact_block)
}

/// Seals settled spans into immutable `ZSHRNG02`, `ZSTSUP01`, and optional `ZSBLKB01`
/// segments inside a blocking worker thread, recursively subdividing any batch
/// whose sealed bytes exceed `max_segment_bytes`.
pub async fn seal_settled_segments(
    network: Network,
    blocks: Vec<PreparedSegmentedBlock>,
    settled_segment_blocks: u32,
    full_blocks: bool,
    max_segment_bytes: u64,
) -> Result<SealedSegmentsOutput, BenchError> {
    tokio::task::spawn_blocking(move || {
        seal_settled_segments_sync(
            network,
            &blocks,
            settled_segment_blocks,
            full_blocks,
            max_segment_bytes,
        )
    })
    .await
    .map_err(|err| candidate_error(format!("segment sealing worker panicked or aborted: {err}")))?
}

/// Synchronously seals one or more settled spans, enforcing `max_segment_bytes`.
pub fn seal_settled_segments_sync(
    network: Network,
    blocks: &[PreparedSegmentedBlock],
    settled_segment_blocks: u32,
    full_blocks: bool,
    max_segment_bytes: u64,
) -> Result<SealedSegmentsOutput, BenchError> {
    if max_segment_bytes == 0 || max_segment_bytes > MAX_SETTLED_SEGMENT_BYTES {
        return Err(BenchError::invalid_argument(format!(
            "max_segment_bytes must be within 1..={MAX_SETTLED_SEGMENT_BYTES}"
        )));
    }
    if blocks.is_empty() {
        return Ok(SealedSegmentsOutput {
            bundles: Vec::new(),
            max_segment_bytes_observed: 0,
        });
    }
    let batch_len = usize::try_from(settled_segment_blocks)
        .map_err(|_| BenchError::invalid_argument("settled_segment_blocks exceeds usize"))?;
    if batch_len == 0 {
        return Err(BenchError::invalid_argument(
            "settled_segment_blocks must be >= 1",
        ));
    }

    let scratch_dir = TempDir::new().map_err(|err| BenchError::io("tempdir", err))?;
    let ctx = SealContext {
        scratch_root: scratch_dir.path(),
        network,
        full_blocks,
        max_segment_bytes,
    };
    let mut bundles = Vec::new();
    let mut max_observed = 0_u64;
    for chunk in blocks.chunks(batch_len) {
        seal_recursive_with_cap(&ctx, chunk, &mut bundles, &mut max_observed)?;
    }

    Ok(SealedSegmentsOutput {
        bundles,
        max_segment_bytes_observed: max_observed,
    })
}

fn seal_recursive_with_cap(
    ctx: &SealContext<'_>,
    slice: &[PreparedSegmentedBlock],
    bundles: &mut Vec<SealedSegmentBundle>,
    max_observed: &mut u64,
) -> Result<(), BenchError> {
    let candidate_bundle =
        seal_single_batch(ctx.scratch_root, ctx.network, slice, ctx.full_blocks)?;
    let bundle_max = bundle_max_byte_len(&candidate_bundle);
    if bundle_max <= ctx.max_segment_bytes {
        *max_observed = (*max_observed).max(bundle_max);
        bundles.push(candidate_bundle);
        return Ok(());
    }
    if slice.len() <= 1 {
        let block_height = slice.first().map_or(0, |b| b.height.value());
        return Err(candidate_error(format!(
            "single block at height {block_height} exceeds configured max_segment_bytes ({bundle_max} > {})",
            ctx.max_segment_bytes
        )));
    }
    let mid = slice.len() / 2;
    let (left, right) = slice.split_at(mid);
    seal_recursive_with_cap(ctx, left, bundles, max_observed)?;
    seal_recursive_with_cap(ctx, right, bundles, max_observed)
}

fn bundle_max_byte_len(bundle: &SealedSegmentBundle) -> u64 {
    let shielded_len = u64::try_from(bundle.shielded.bytes.len()).unwrap_or(u64::MAX);
    let supplement_len = u64::try_from(bundle.supplement.bytes.len()).unwrap_or(u64::MAX);
    let raw_len = bundle
        .raw_block
        .as_ref()
        .map_or(0, |seg| u64::try_from(seg.bytes.len()).unwrap_or(u64::MAX));
    shielded_len.max(supplement_len).max(raw_len)
}

fn seal_single_batch(
    scratch_root: &Path,
    network: Network,
    slice: &[PreparedSegmentedBlock],
    full_blocks: bool,
) -> Result<SealedSegmentBundle, BenchError> {
    let first = slice
        .first()
        .ok_or_else(|| candidate_error("cannot seal empty settled block span"))?;
    let last = slice
        .last()
        .ok_or_else(|| candidate_error("cannot seal empty settled block span"))?;
    let start_height = first.height;
    let end_block = BlockId::new(last.height, last.block_hash);

    let shielded = seal_shielded(scratch_root, network, start_height, end_block, slice)?;
    let supplement = seal_supplement(scratch_root, network, start_height, end_block, slice)?;
    let raw_block = if full_blocks {
        Some(seal_raw_block(
            scratch_root,
            network,
            start_height,
            end_block,
            slice,
        )?)
    } else {
        None
    };

    Ok(SealedSegmentBundle {
        shielded,
        supplement,
        raw_block,
    })
}

fn seal_shielded(
    scratch_root: &Path,
    network: Network,
    start_height: BlockHeight,
    end_block: BlockId,
    slice: &[PreparedSegmentedBlock],
) -> Result<SealedSegmentBytes, BenchError> {
    let target_path = scratch_root.join(format!(
        "shielded-{}-{}.bin",
        start_height.value(),
        end_block.height.value()
    ));
    let staged =
        NamedTempFile::new_in(scratch_root).map_err(|err| BenchError::io("staged", err))?;
    let expected = ShieldedSegmentIdentity::span(
        network,
        CURRENT_ARTIFACT_SCHEMA_VERSION.value(),
        start_height,
        end_block,
    );
    let mut writer = SettledShieldedSegmentWriter::create(&target_path, staged, expected)
        .map_err(|err| candidate_error(format!("shielded segment writer create: {err}")))?;
    for block in slice {
        writer
            .push_with_supplement(&block.compact_block, block.compact_block_proto_len)
            .map_err(|err| candidate_error(format!("shielded segment push: {err}")))?;
    }
    let identity = writer
        .finish()
        .map_err(|err| candidate_error(format!("shielded segment finish: {err}")))?;
    let bytes = fs::read(&target_path).map_err(|err| BenchError::io(&target_path, err))?;
    let _ = fs::remove_file(&target_path);
    Ok(SealedSegmentBytes {
        start_height,
        end_height: end_block.height,
        segment_sha256: identity.sha256,
        index_sha256: identity.index_sha256,
        bytes,
    })
}

fn seal_supplement(
    scratch_root: &Path,
    network: Network,
    start_height: BlockHeight,
    end_block: BlockId,
    slice: &[PreparedSegmentedBlock],
) -> Result<SealedSegmentBytes, BenchError> {
    let target_path = scratch_root.join(format!(
        "supplement-{}-{}.bin",
        start_height.value(),
        end_block.height.value()
    ));
    let expected = SupplementSegmentIdentity::span(
        network,
        CURRENT_ARTIFACT_SCHEMA_VERSION.value(),
        start_height,
        end_block,
    );
    let write_options = SettledSupplementSegmentWriteOptions {
        codec: SupplementCompressionCodec::Zstd,
        chunk_blocks: zinder_store::SETTLED_SUPPLEMENT_DEFAULT_CHUNK_BLOCKS,
    };
    let mut writer = SettledSupplementSegmentWriter::create(&target_path, expected, write_options)
        .map_err(|err| candidate_error(format!("supplement segment writer create: {err}")))?;
    for block in slice {
        writer
            .push(&block.compact_block, Some(&block.facts))
            .map_err(|err| candidate_error(format!("supplement segment push: {err}")))?;
    }
    let identity = writer
        .finish()
        .map_err(|err| candidate_error(format!("supplement segment finish: {err}")))?;
    let bytes = fs::read(&target_path).map_err(|err| BenchError::io(&target_path, err))?;
    let _ = fs::remove_file(&target_path);
    Ok(SealedSegmentBytes {
        start_height,
        end_height: end_block.height,
        segment_sha256: identity.sha256,
        index_sha256: identity.index_sha256,
        bytes,
    })
}

fn seal_raw_block(
    scratch_root: &Path,
    network: Network,
    start_height: BlockHeight,
    end_block: BlockId,
    slice: &[PreparedSegmentedBlock],
) -> Result<SealedSegmentBytes, BenchError> {
    let target_path = scratch_root.join(format!(
        "raw-block-{}-{}.bin",
        start_height.value(),
        end_block.height.value()
    ));
    let staged =
        NamedTempFile::new_in(scratch_root).map_err(|err| BenchError::io("staged", err))?;
    let expected =
        RawBlockSegmentIdentity::span(network, RawBlockCodec::Zstd, start_height, end_block);
    let mut writer = SettledRawBlockSegmentWriter::create(&target_path, staged, expected)
        .map_err(|err| candidate_error(format!("raw block segment writer create: {err}")))?;
    for block in slice {
        let raw_bytes = block.raw_block_bytes.as_deref().ok_or_else(|| {
            candidate_error(format!(
                "missing raw block bytes at height {} when full_blocks=true",
                block.height.value()
            ))
        })?;
        writer
            .push(
                BlockId::new(block.height, block.block_hash),
                block.previous_block_hash,
                raw_bytes,
                &block.raw_tx_slices,
            )
            .map_err(|err| candidate_error(format!("raw block segment push: {err}")))?;
    }
    let identity = writer
        .finish()
        .map_err(|err| candidate_error(format!("raw block segment finish: {err}")))?;
    let bytes = fs::read(&target_path).map_err(|err| BenchError::io(&target_path, err))?;
    let _ = fs::remove_file(&target_path);
    Ok(SealedSegmentBytes {
        start_height,
        end_height: end_block.height,
        segment_sha256: identity.sha256,
        index_sha256: identity.index_sha256,
        bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use zinder_core::{
        BlockHeaderArtifact, CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion,
        CompactChainMetadata, SerializedBytesDigest, encode_canonical_block_replay,
    };

    fn make_synthetic_prepared_block(height: u32, prev_hash: BlockHash) -> PreparedSegmentedBlock {
        let byte_seed = u8::try_from((height % 250) + 1).unwrap_or(1);
        let hash = BlockHash::from_bytes([byte_seed; 32]);
        let block_id = BlockId::new(BlockHeight::new(height), hash);
        let chain_metadata = CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        };
        let compact_block = CompactBlockArtifact::empty(
            block_id,
            prev_hash,
            1_700_000_000 + height,
            chain_metadata,
        );
        let compact_block_bytes = encode_compact_block_artifact(&compact_block);
        let compact_block_proto_len = compact_block_bytes.len();
        let header = BlockHeaderArtifact::new(
            BlockHeight::new(height),
            hash,
            prev_hash,
            [byte_seed; 32],
            [0_u8; 32],
            1_700_000_000 + i64::from(height),
            0x2007_ffff,
            [0_u8; 32],
            4,
            140,
        );
        let facts = CanonicalBlockFacts {
            block_header: header,
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                &compact_block_bytes,
            ),
            transactions: Vec::new(),
        };
        let replay = encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::CURRENT,
            CanonicalBlockFactsDigestVersion::CURRENT,
        );
        PreparedSegmentedBlock {
            height: BlockHeight::new(height),
            block_hash: hash,
            previous_block_hash: prev_hash,
            compact_block,
            compact_block_proto_len,
            compact_block_bytes,
            facts,
            canonical_facts_bytes: replay.into_bytes(),
            raw_block_bytes: Some(vec![0xaa; 140]),
            raw_tx_slices: Vec::new(),
        }
    }

    #[test]
    fn seal_recursive_presplit_respects_max_segment_bytes_and_preserves_order()
    -> Result<(), BenchError> {
        let mut blocks = Vec::new();
        let mut prev = BlockHash::from_bytes([0_u8; 32]);
        for height in 1_u32..=6 {
            let block = make_synthetic_prepared_block(height, prev);
            prev = block.block_hash;
            blocks.push(block);
        }

        let one_block_sealed = seal_settled_segments_sync(
            Network::ZcashRegtest,
            &blocks[..1],
            1,
            true,
            MAX_SETTLED_SEGMENT_BYTES,
        )?;
        let single_max = one_block_sealed.max_segment_bytes_observed;
        let all_sealed = seal_settled_segments_sync(
            Network::ZcashRegtest,
            &blocks,
            6,
            true,
            MAX_SETTLED_SEGMENT_BYTES,
        )?;
        let cap_requiring_split =
            single_max.max(all_sealed.max_segment_bytes_observed.saturating_sub(1));

        let split_sealed = seal_settled_segments_sync(
            Network::ZcashRegtest,
            &blocks,
            6,
            true,
            cap_requiring_split,
        )?;
        assert!(split_sealed.bundles.len() >= 2);
        assert!(split_sealed.max_segment_bytes_observed <= cap_requiring_split);
        let first_h = split_sealed
            .bundles
            .first()
            .map(|b| b.shielded.start_height.value());
        let last_h = split_sealed
            .bundles
            .last()
            .map(|b| b.shielded.end_height.value());
        assert_eq!(first_h, Some(1));
        assert_eq!(last_h, Some(6));
        Ok(())
    }

    #[test]
    fn extract_consensus_tx_slices_rejects_mismatched_bytes() {
        let raw = vec![1_u8, 2, 3, 4];
        let location = zinder_core::TransactionLocation::new(
            zinder_core::TransactionId::from_bytes([1_u8; 32]),
            BlockHeight::new(1),
            BlockHash::from_bytes([2_u8; 32]),
            0,
        );
        let blob = TransactionBlobArtifact::new(location, vec![9_u8, 9]);
        assert!(extract_consensus_tx_slices(&raw, &[blob]).is_err());
    }
}
