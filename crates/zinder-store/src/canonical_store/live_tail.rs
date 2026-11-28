//! Versioned `V1` fail-closed binary codecs and validated accessors for
//! unsealed canonical live-tail records (`ZSLVTL01` and split `ZSLVRB01`) at
//! heights `> sealed_tip`.

use prost::Message as _;
use sha2::{Digest as _, Sha256};
use zinder_core::{
    BlockHash, BlockHeaderArtifact, BlockHeight, BlockId, CanonicalBlockFacts,
    CanonicalBlockFactsDigestVersion, CanonicalBlockReplayEnvelope,
    CanonicalBlockReplayFormatVersion, CompactBlockArtifact, CompactTransparentInput,
    CompactTransparentOutput, ConsensusBranchId, LockTime, PrivacyShape, SerializedBytesDigest,
    TransactionComponentCounts, TransactionIntrinsicValueBalances, TransactionPublicFacts,
    TransactionVersion, UnsupportedSection, decode_canonical_block_replay,
    encode_canonical_block_replay,
    wire::{
        decode_internal_auth_digest, decode_internal_block_hash, decode_internal_transaction_id,
        decode_internal_wtxid, encode_internal_auth_digest, encode_internal_block_hash,
        encode_internal_transaction_id, encode_internal_wtxid,
    },
};
use zinder_proto::compat::lightwalletd;

use crate::{
    settled_raw_block_segment::{
        RawBlockTransactionSlice, SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES,
    },
    settled_shielded_segment::{
        PoolPresenceMask, SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES, SettledShieldedSegmentError,
    },
    settled_supplement_segment::{
        BlockSupplementRecord, CanonicalBlockSupplement, CanonicalTransactionSupplement,
        CompactTransparentSupplementTx, SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES,
        SupplementShieldedAnchors, SupplementTxFeeAndBurn,
        exact_zip317_logical_actions_from_outputs, reconstruct_compact_block_artifact,
        transparent_supplement_from_compact, validate_and_inspect_shielded_block_wire,
    },
};

use super::settled_segments::{ByteReader, SettledSegmentsError, push_len_prefixed_bytes};

/// Binary format version for encoded [`LiveTailBlockRecord`] (`ZSLVTL01`) blobs.
pub(crate) const LIVE_TAIL_RECORD_VERSION_V1: u16 = 1;
/// Binary format version for encoded [`LiveTailRawBlock`] (`ZSLVRB01`) blobs.
pub(crate) const LIVE_TAIL_RAW_BLOCK_VERSION_V1: u16 = 1;

const LIVE_TAIL_MAGIC: &[u8; 8] = b"ZSLVTL01";
const LIVE_TAIL_FLAG_SUPPLEMENT: u8 = 1 << 0;
const LIVE_TAIL_FLAG_CANONICAL_FACTS: u8 = 1 << 1;
// Bit 2 is reserved for the retired inline raw-block layout.
const LIVE_TAIL_FLAG_WIDE_FEE_KNOWN: u8 = 1 << 3;
const LIVE_TAIL_FLAG_HAS_WIDE_FEE: u8 = 1 << 4;
const LIVE_TAIL_KNOWN_FLAGS: u8 = LIVE_TAIL_FLAG_SUPPLEMENT
    | LIVE_TAIL_FLAG_CANONICAL_FACTS
    | LIVE_TAIL_FLAG_WIDE_FEE_KNOWN
    | LIVE_TAIL_FLAG_HAS_WIDE_FEE;
const LIVE_TAIL_HEADER_MIN_LEN: usize = 8 + 2 + 1 + 1 + 4 + 32 + 32 + 4 + 1 + 4 + 32;
const LIVE_TAIL_TRAILER_LEN: usize = 32;
const MAX_LIVE_TAIL_RECORD_BYTES: usize = 64 * 1024 * 1024;

const LIVE_TAIL_RAW_BLOCK_MAGIC: &[u8; 8] = b"ZSLVRB01";
const LIVE_TAIL_RAW_BLOCK_MIN_LEN: usize = 8 + 2 + 2 + 4 + 1 + 32 + 4 + 32;
const MAX_LIVE_TAIL_RAW_BLOCK_RECORD_BYTES: usize = 32 * 1024 * 1024;

const MAX_LIVE_TAIL_TX_COUNT: usize = 250_000;
const MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT: usize = 250_000;
const MAX_LIVE_TAIL_PREALLOC_COUNT: usize = 1_024;
const MAX_LIVE_TAIL_SCRIPT_PUB_KEY_BYTES: usize = 1024 * 1024;
const MAX_LIVE_TAIL_UNSUPPORTED_SECTIONS: usize = 64;

/// Authenticated transaction slice descriptor within [`LiveTailRawBlock`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LiveTailRawTxSlice {
    /// Zero-based byte offset of the serialized transaction inside `raw_block_bytes`.
    pub(crate) offset: u32,
    /// Serialized transaction length in bytes.
    pub(crate) len: u32,
    /// SHA-256 commitment to the exact raw transaction byte slice.
    pub(crate) sha256: [u8; 32],
}

impl LiveTailRawTxSlice {
    /// Creates an authenticated raw transaction slice descriptor.
    #[must_use]
    pub(crate) const fn new(offset: u32, len: u32, sha256: [u8; 32]) -> Self {
        Self {
            offset,
            len,
            sha256,
        }
    }

    /// Returns the offset/length pair accepted by `ZSBLKB01` writers.
    #[must_use]
    pub(crate) const fn as_raw_slice(self) -> RawBlockTransactionSlice {
        RawBlockTransactionSlice::new(self.offset, self.len)
    }

    fn verify_slice(self, raw_block_bytes: &[u8]) -> Result<&[u8], SettledSegmentsError> {
        let (start, end) =
            validate_raw_tx_slice_bounds(self.offset, self.len, raw_block_bytes.len(), 0)?;
        verify_raw_tx_slice_range(raw_block_bytes, start, end, self.sha256)?;
        Ok(&raw_block_bytes[start..end])
    }
}

fn validate_raw_tx_slice_bounds(
    offset: u32,
    len: u32,
    block_len: usize,
    prev_end: usize,
) -> Result<(usize, usize), SettledSegmentsError> {
    let start = usize::try_from(offset).map_err(|_| {
        SettledSegmentsError::InvalidFormat("raw tx offset exceeds host address space".to_owned())
    })?;
    let slice_len = usize::try_from(len).map_err(|_| {
        SettledSegmentsError::InvalidFormat("raw tx length exceeds host address space".to_owned())
    })?;
    if slice_len == 0 {
        return Err(SettledSegmentsError::InvalidFormat(
            "raw tx slice length must be positive".to_owned(),
        ));
    }
    if start < prev_end {
        return Err(SettledSegmentsError::InvalidFormat(
            "raw tx slices are out of order or overlapping".to_owned(),
        ));
    }
    let end = start.checked_add(slice_len).ok_or_else(|| {
        SettledSegmentsError::InvalidFormat("raw tx slice range overflow".to_owned())
    })?;
    if end > block_len {
        return Err(SettledSegmentsError::InvalidFormat(
            "raw tx slice exceeds raw block byte bounds".to_owned(),
        ));
    }
    Ok((start, end))
}

fn verify_raw_tx_slice_range(
    raw_block_bytes: &[u8],
    start: usize,
    end: usize,
    expected_sha256: [u8; 32],
) -> Result<(), SettledSegmentsError> {
    let observed: [u8; 32] = Sha256::digest(&raw_block_bytes[start..end]).into();
    if observed != expected_sha256 {
        return Err(SettledSegmentsError::Integrity(
            "raw tx slice SHA-256 mismatch in live-tail record".to_owned(),
        ));
    }
    Ok(())
}

/// Split unsealed consensus raw block record (`ZSLVRB01`) retained independently
/// in the `live_tail_raw_block` column family when `full_blocks` is enabled.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LiveTailRawBlock {
    /// Exact serialized consensus block bytes.
    pub(crate) raw_block_bytes: Vec<u8>,
    /// Ordered authenticated per-transaction byte slices within `raw_block_bytes`.
    pub(crate) tx_slices: Vec<LiveTailRawTxSlice>,
}

impl LiveTailRawBlock {
    /// Builds a validated [`LiveTailRawBlock`] by hashing each slice inside `raw_block_bytes`.
    pub(crate) fn from_bytes_and_slices(
        raw_block_bytes: Vec<u8>,
        slices: &[RawBlockTransactionSlice],
    ) -> Result<Self, SettledSegmentsError> {
        validate_raw_block_size(raw_block_bytes.len())?;
        ensure_within_limit(
            slices.len(),
            MAX_LIVE_TAIL_TX_COUNT,
            "live_tail.raw_tx_count",
        )?;
        let mut verified_slices = Vec::with_capacity(slices.len());
        let mut prev_end = 0_usize;
        for raw_slice in slices {
            let (start, end) = validate_raw_tx_slice_bounds(
                raw_slice.offset,
                raw_slice.len,
                raw_block_bytes.len(),
                prev_end,
            )?;
            let digest: [u8; 32] = Sha256::digest(&raw_block_bytes[start..end]).into();
            verified_slices.push(LiveTailRawTxSlice::new(
                raw_slice.offset,
                raw_slice.len,
                digest,
            ));
            prev_end = end;
        }
        Ok(Self {
            raw_block_bytes,
            tx_slices: verified_slices,
        })
    }

    /// Returns offset/length descriptors accepted by `SettledRawBlockSegmentWriter`.
    #[must_use]
    pub(crate) fn raw_slices(&self) -> Vec<RawBlockTransactionSlice> {
        self.tx_slices
            .iter()
            .copied()
            .map(LiveTailRawTxSlice::as_raw_slice)
            .collect()
    }

    /// Computes the SHA-256 [`SerializedBytesDigest`] of `raw_block_bytes`.
    #[must_use]
    pub(crate) fn serialized_bytes_digest(&self) -> SerializedBytesDigest {
        SerializedBytesDigest::from_serialized_bytes(&self.raw_block_bytes)
    }

    /// Validates byte limits, non-overlapping slice ordering, and every SHA-256 slice digest.
    pub(crate) fn validate(&self) -> Result<(), SettledSegmentsError> {
        validate_raw_block_size(self.raw_block_bytes.len())?;
        ensure_within_limit(
            self.tx_slices.len(),
            MAX_LIVE_TAIL_TX_COUNT,
            "live_tail.raw_tx_count",
        )?;
        let mut prev_end = 0_usize;
        for tx_slice in &self.tx_slices {
            let (start, end) = validate_raw_tx_slice_bounds(
                tx_slice.offset,
                tx_slice.len,
                self.raw_block_bytes.len(),
                prev_end,
            )?;
            verify_raw_tx_slice_range(&self.raw_block_bytes, start, end, tx_slice.sha256)?;
            prev_end = end;
        }
        Ok(())
    }

    /// Verifies that `self` matches `facts`'s serialized block and per-transaction digests.
    pub(crate) fn validate_against_facts(
        &self,
        facts: &CanonicalBlockFacts,
    ) -> Result<(), SettledSegmentsError> {
        if self.serialized_bytes_digest() != facts.serialized_bytes_digest {
            return Err(SettledSegmentsError::Integrity(
                "live-tail raw block digest disagrees with canonical_facts".to_owned(),
            ));
        }
        if self.tx_slices.len() != facts.transactions.len() {
            return Err(SettledSegmentsError::ChainLinkage(
                "live-tail raw tx slice count disagrees with canonical_facts".to_owned(),
            ));
        }
        for (slice_desc, tx_facts) in self.tx_slices.iter().zip(&facts.transactions) {
            if slice_desc.sha256 != tx_facts.serialized_bytes_digest.as_bytes() {
                return Err(SettledSegmentsError::Integrity(
                    "live-tail raw tx slice digest disagrees with canonical_facts".to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// Slices and SHA-256-verifies raw consensus transaction bytes at `tx_index`.
    pub(crate) fn read_verified_raw_transaction(
        &self,
        tx_index: usize,
    ) -> Result<Vec<u8>, SettledSegmentsError> {
        let slice_desc = self.tx_slices.get(tx_index).ok_or_else(|| {
            SettledSegmentsError::ChainLinkage(format!(
                "live-tail raw transaction index {tx_index} out of bounds"
            ))
        })?;
        Ok(slice_desc.verify_slice(&self.raw_block_bytes)?.to_vec())
    }

    /// Encodes this raw block into versioned `ZSLVRB01` big-endian format with trailing SHA-256.
    pub(crate) fn encode_v1(&self) -> Result<Vec<u8>, SettledSegmentsError> {
        self.validate()?;
        let capacity = encoded_raw_block_v1_len(self)?;
        if !(LIVE_TAIL_RAW_BLOCK_MIN_LEN..=MAX_LIVE_TAIL_RAW_BLOCK_RECORD_BYTES).contains(&capacity)
        {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "live-tail raw block encoded length {capacity} outside allowed range"
            )));
        }
        let mut out = Vec::with_capacity(capacity);
        out.extend_from_slice(LIVE_TAIL_RAW_BLOCK_MAGIC);
        out.extend_from_slice(&LIVE_TAIL_RAW_BLOCK_VERSION_V1.to_be_bytes());
        out.extend_from_slice(&0_u16.to_be_bytes());
        encode_raw_block_body(&mut out, self)?;
        let digest: [u8; 32] = Sha256::digest(&out).into();
        out.extend_from_slice(&digest);
        Ok(out)
    }

    /// Decodes and authenticates a standalone `ZSLVRB01` live-tail raw block record.
    pub(crate) fn decode_v1(bytes: &[u8]) -> Result<Self, SettledSegmentsError> {
        if !(LIVE_TAIL_RAW_BLOCK_MIN_LEN..=MAX_LIVE_TAIL_RAW_BLOCK_RECORD_BYTES)
            .contains(&bytes.len())
        {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "live-tail raw block length {} outside allowed range",
                bytes.len()
            )));
        }
        let split_idx = bytes.len() - LIVE_TAIL_TRAILER_LEN;
        let body = &bytes[..split_idx];
        let expected_trailer = &bytes[split_idx..];
        let observed_digest: [u8; 32] = Sha256::digest(body).into();
        if observed_digest.as_slice() != expected_trailer {
            return Err(SettledSegmentsError::Integrity(
                "live-tail raw block SHA-256 trailer verification failed".to_owned(),
            ));
        }
        let mut cursor = ByteReader::new(body);
        let magic = cursor.read_array::<8>("live_tail_raw.magic")?;
        if &magic != LIVE_TAIL_RAW_BLOCK_MAGIC {
            return Err(SettledSegmentsError::InvalidFormat(
                "live-tail raw block magic mismatch".to_owned(),
            ));
        }
        let version = cursor.read_u16_be("live_tail_raw.version")?;
        if version != LIVE_TAIL_RAW_BLOCK_VERSION_V1 {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "unsupported live-tail raw block version {version}"
            )));
        }
        let reserved = cursor.read_u16_be("live_tail_raw.reserved")?;
        if reserved != 0 {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "non-zero reserved header bytes {reserved:#06x} in live-tail raw block"
            )));
        }
        let raw_block = decode_raw_block_body(&mut cursor)?;
        cursor.finish("LiveTailRawBlock")?;
        Ok(raw_block)
    }
}

/// Complete unsealed per-height compact/supplement/facts record (`ZSLVTL01`)
/// retained for heights `> sealed_tip`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LiveTailBlockRecord {
    /// Canonical identity of this unsealed block.
    pub(crate) block_id: BlockId,
    /// Parent block hash in canonical chain linkage.
    pub(crate) previous_block_hash: BlockHash,
    /// Unix timestamp recorded in the block header.
    pub(crate) block_time: u32,
    /// Pre-encoded `DEFAULT_RANGE` `CompactBlock` protobuf payload.
    pub(crate) shielded_payload: Vec<u8>,
    /// Shielded pool presence bitmap derived from `shielded_payload`.
    pub(crate) pool_presence: PoolPresenceMask,
    /// Optional Tag-1/Tag-2 supplement record for full reconstruction & splicing.
    pub(crate) supplement: Option<BlockSupplementRecord>,
    /// Optional canonical block facts matching `block_id`.
    pub(crate) canonical_facts: Option<CanonicalBlockFacts>,
}

/// Authenticated zero-copy shielded wire payload and metadata decoded from a
/// `live_tail_block` `V1` row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LiveTailShieldedWire<'a> {
    pub(crate) block_id: BlockId,
    pub(crate) previous_block_hash: BlockHash,
    pub(crate) time: u32,
    pub(crate) shielded_payload: &'a [u8],
    pub(crate) pool_presence: PoolPresenceMask,
    pub(crate) has_unrepresentable_wire_fee: bool,
}

/// Authenticated shielded wire payload paired with decoded Tag-1 transparent
/// supplement transactions from a `live_tail_block` `V1` row, skipping
/// `canonical_facts` decoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LiveTailSpliceWire<'a> {
    pub(crate) block_id: BlockId,
    pub(crate) previous_block_hash: BlockHash,
    pub(crate) time: u32,
    pub(crate) shielded_payload: &'a [u8],
    pub(crate) pool_presence: PoolPresenceMask,
    pub(crate) transparent_transactions: Vec<CompactTransparentSupplementTx>,
    pub(crate) has_unrepresentable_wire_fee: bool,
}

/// Encoding toggles for [`LiveTailBlockRecord::encode_from_compact_block_v1`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LiveTailEncodeOptions {
    /// Whether Tag-1/Tag-2 supplement metadata is included.
    pub(crate) include_supplement: bool,
    /// Whether canonical block facts remain embedded in the serialized record.
    pub(crate) retain_canonical_facts: bool,
}

impl LiveTailBlockRecord {
    /// Constructs and validates one live-tail record from a [`CompactBlockArtifact`].
    pub(crate) fn from_compact_block(
        block: &CompactBlockArtifact,
        include_supplement: bool,
        canonical_facts: Option<CanonicalBlockFacts>,
    ) -> Result<Self, SettledSegmentsError> {
        let (record, _) = Self::construct_validated_from_compact_block(
            block,
            include_supplement,
            canonical_facts,
        )?;
        Ok(record)
    }

    /// Constructs, validates, and `V1`-encodes one live-tail record from a
    /// [`CompactBlockArtifact`] without re-decoding its freshly encoded shielded payload.
    pub(crate) fn encode_from_compact_block_v1(
        block: &CompactBlockArtifact,
        canonical_facts: Option<CanonicalBlockFacts>,
        options: LiveTailEncodeOptions,
    ) -> Result<(Vec<u8>, usize), SettledSegmentsError> {
        let (mut record, replay_envelope) = Self::construct_validated_from_compact_block(
            block,
            options.include_supplement,
            canonical_facts,
        )?;
        let shielded_payload_len = record.shielded_payload.len();
        let envelope = if options.retain_canonical_facts {
            replay_envelope
        } else {
            record.canonical_facts = None;
            None
        };
        let encoded = record.encode_v1_with_validated_envelope(envelope)?;
        Ok((encoded, shielded_payload_len))
    }

    fn construct_validated_from_compact_block(
        block: &CompactBlockArtifact,
        include_supplement: bool,
        canonical_facts: Option<CanonicalBlockFacts>,
    ) -> Result<(Self, Option<CanonicalBlockReplayEnvelope>), SettledSegmentsError> {
        let (shielded_payload, pool_presence) =
            encode_default_shielded_payload_with_options(block, include_supplement)?;
        let supplement = if include_supplement {
            let transparent_transactions = transparent_supplement_from_compact(block);
            let canonical_metadata = canonical_facts
                .as_ref()
                .map(|facts| CanonicalBlockSupplement::from_canonical_facts(facts, Some(block)))
                .transpose()?;
            Some(BlockSupplementRecord {
                transparent_transactions,
                canonical_metadata,
            })
        } else {
            None
        };
        let record = Self {
            block_id: BlockId::new(block.height(), block.block_hash()),
            previous_block_hash: block.previous_block_hash(),
            block_time: block.time(),
            shielded_payload,
            pool_presence,
            supplement,
            canonical_facts,
        };
        let replay_envelope = record.validate_from_compact_block(block)?;
        Ok((record, replay_envelope))
    }

    /// Authenticates a `V1` record and reconstructs only its [`CompactBlockArtifact`] and
    /// raw shielded payload length.
    pub(crate) fn decode_compact_block_v1(
        bytes: &[u8],
        expected_height: BlockHeight,
    ) -> Result<(CompactBlockArtifact, usize), SettledSegmentsError> {
        let (header, mut cursor) = parse_authenticated_header_v1(bytes, expected_height)?;
        let shielded_payload = cursor.read_len_prefixed_slice(
            SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES,
            "live_tail.shielded",
        )?;
        let supplement = if (header.flags & LIVE_TAIL_FLAG_SUPPLEMENT) != 0 {
            Some(decode_supplement_record(&mut cursor)?)
        } else {
            None
        };
        if (header.flags & LIVE_TAIL_FLAG_CANONICAL_FACTS) != 0 {
            let _ = cursor.read_len_prefixed_slice(
                SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES,
                "live_tail.facts",
            )?;
        }
        cursor.finish("LiveTailBlockRecord")?;
        let proto_block = decode_and_validate_shielded_payload(
            shielded_payload,
            header.block_id,
            header.previous_block_hash,
            header.block_time,
            header.pool_presence,
        )?;
        let empty_sup = BlockSupplementRecord {
            transparent_transactions: Vec::new(),
            canonical_metadata: None,
        };
        let compact_block = match &supplement {
            Some(sup) => {
                validate_supplement_limits(sup)?;
                validate_supplement_against_payload(&proto_block, sup)?
            }
            None => crate::settled_supplement_segment::reconstruct_compact_block_from_proto(
                &proto_block,
                &empty_sup,
            )?,
        };
        Ok((compact_block, shielded_payload.len()))
    }

    /// Authenticates a `V1` record and borrows its raw shielded `CompactBlock` protobuf
    /// payload zero-copy alongside header links and wide-fee detection.
    pub(crate) fn decode_shielded_wire_v1(
        bytes: &[u8],
        expected_height: BlockHeight,
    ) -> Result<LiveTailShieldedWire<'_>, SettledSegmentsError> {
        let (header, mut cursor) = parse_authenticated_header_v1(bytes, expected_height)?;
        let shielded_payload = cursor.read_len_prefixed_slice(
            SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES,
            "live_tail.shielded",
        )?;
        let has_unrepresentable_wire_fee = if header.flags & LIVE_TAIL_FLAG_SUPPLEMENT != 0
            && header.flags & LIVE_TAIL_FLAG_WIDE_FEE_KNOWN != 0
        {
            // The authenticated suffix contains only supplement and facts, which this
            // read does not consume. Their own readers still validate their framing.
            header.flags & LIVE_TAIL_FLAG_HAS_WIDE_FEE != 0
        } else {
            let wide = if header.flags & LIVE_TAIL_FLAG_SUPPLEMENT != 0 {
                scan_supplement_for_wide_fee(&mut cursor)?
            } else {
                false
            };
            if header.flags & LIVE_TAIL_FLAG_CANONICAL_FACTS != 0 {
                let _ = cursor.read_len_prefixed_slice(
                    SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES,
                    "live_tail.facts",
                )?;
            }
            cursor.finish("LiveTailBlockRecord")?;
            wide
        };
        validate_shielded_payload_wire_zero_alloc(&header, shielded_payload)?;
        Ok(LiveTailShieldedWire {
            block_id: header.block_id,
            previous_block_hash: header.previous_block_hash,
            time: header.block_time,
            shielded_payload,
            pool_presence: header.pool_presence,
            has_unrepresentable_wire_fee,
        })
    }

    /// Authenticates a `V1` record, borrows its raw shielded `CompactBlock` protobuf
    /// payload zero-copy alongside header links and wide-fee detection, and decodes
    /// Tag-1 transparent supplement entries without decoding `canonical_facts`.
    pub(crate) fn decode_splice_wire_v1(
        bytes: &[u8],
        expected_height: BlockHeight,
    ) -> Result<LiveTailSpliceWire<'_>, SettledSegmentsError> {
        let (header, mut cursor) = parse_authenticated_header_v1(bytes, expected_height)?;
        let shielded_payload = cursor.read_len_prefixed_slice(
            SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES,
            "live_tail.shielded",
        )?;
        let (transparent_transactions, has_unrepresentable_wire_fee) =
            if (header.flags & LIVE_TAIL_FLAG_SUPPLEMENT) != 0 {
                decode_tag1_and_scan_wide_fee(&mut cursor)?
            } else {
                (Vec::new(), false)
            };
        if (header.flags & LIVE_TAIL_FLAG_CANONICAL_FACTS) != 0 {
            let _ = cursor.read_len_prefixed_slice(
                SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES,
                "live_tail.facts",
            )?;
        }
        cursor.finish("LiveTailBlockRecord")?;
        validate_shielded_payload_wire_zero_alloc(&header, shielded_payload)?;
        Ok(LiveTailSpliceWire {
            block_id: header.block_id,
            previous_block_hash: header.previous_block_hash,
            time: header.block_time,
            shielded_payload,
            pool_presence: header.pool_presence,
            transparent_transactions,
            has_unrepresentable_wire_fee,
        })
    }

    /// Authenticates a `V1` record and extracts only [`CanonicalBlockFacts`] when present,
    /// skipping compact-block protobuf decoding.
    pub(crate) fn decode_canonical_facts_v1(
        bytes: &[u8],
        expected_height: BlockHeight,
    ) -> Result<Option<CanonicalBlockFacts>, SettledSegmentsError> {
        let (header, mut cursor) = parse_authenticated_header_v1(bytes, expected_height)?;
        let _ = cursor.read_len_prefixed_slice(
            SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES,
            "live_tail.shielded",
        )?;
        if (header.flags & LIVE_TAIL_FLAG_SUPPLEMENT) != 0 {
            skip_supplement_record(&mut cursor)?;
        }
        let canonical_facts = if (header.flags & LIVE_TAIL_FLAG_CANONICAL_FACTS) != 0 {
            let encoded_replay = cursor.read_len_prefixed_slice(
                SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES,
                "live_tail.facts",
            )?;
            let decoded = decode_canonical_block_replay(encoded_replay).map_err(|err| {
                SettledSegmentsError::Integrity(format!(
                    "live-tail canonical block replay failed validation: {err}"
                ))
            })?;
            let facts = decoded.into_facts();
            verify_facts_header_matches_record(&header, &facts)?;
            Some(facts)
        } else {
            None
        };
        cursor.finish("LiveTailBlockRecord")?;
        Ok(canonical_facts)
    }

    /// Authenticates internal wire payload, supplement, canonical facts, and limits.
    #[cfg(test)]
    pub(crate) fn validate(&self) -> Result<(), SettledSegmentsError> {
        let _ = self.validate_for_encode()?;
        Ok(())
    }

    fn validate_from_compact_block(
        &self,
        block: &CompactBlockArtifact,
    ) -> Result<Option<CanonicalBlockReplayEnvelope>, SettledSegmentsError> {
        if let Some(sup) = &self.supplement {
            validate_supplement_limits(sup)?;
            validate_transparent_supplement_entries(sup, block)?;
            if let Some(meta) = &sup.canonical_metadata {
                validate_canonical_meta_order(meta, block)?;
            }
        }
        if let Some(facts) = &self.canonical_facts {
            self.validate_canonical_facts_match(facts, block)?;
        }
        let replay_envelope = self
            .canonical_facts
            .as_ref()
            .map(validate_canonical_facts_replay)
            .transpose()?;
        let replay_len = replay_envelope.as_ref().map(|env| env.as_bytes().len());
        let predicted_len = self.predict_encoded_len(replay_len)?;
        if !(LIVE_TAIL_HEADER_MIN_LEN..=MAX_LIVE_TAIL_RECORD_BYTES).contains(&predicted_len) {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "live-tail encoded record length {predicted_len} outside allowed range"
            )));
        }
        Ok(replay_envelope)
    }

    fn encode_v1_with_validated_envelope(
        &self,
        replay_envelope: Option<CanonicalBlockReplayEnvelope>,
    ) -> Result<Vec<u8>, SettledSegmentsError> {
        let mut flags = LIVE_TAIL_FLAG_WIDE_FEE_KNOWN;
        if let Some(supplement) = &self.supplement {
            flags |= LIVE_TAIL_FLAG_SUPPLEMENT;
            if supplement_has_wide_fee(supplement) {
                flags |= LIVE_TAIL_FLAG_HAS_WIDE_FEE;
            }
        }
        if replay_envelope.is_some() {
            flags |= LIVE_TAIL_FLAG_CANONICAL_FACTS;
        }
        let mut out = Vec::with_capacity(self.shielded_payload.len() + 256);
        out.extend_from_slice(LIVE_TAIL_MAGIC);
        out.extend_from_slice(&LIVE_TAIL_RECORD_VERSION_V1.to_be_bytes());
        out.push(flags);
        out.push(0);
        out.extend_from_slice(&self.block_id.height.value().to_be_bytes());
        out.extend_from_slice(&encode_internal_block_hash(self.block_id.hash));
        out.extend_from_slice(&encode_internal_block_hash(self.previous_block_hash));
        out.extend_from_slice(&self.block_time.to_be_bytes());
        out.push(self.pool_presence.bits());
        push_len_prefixed_bytes(&mut out, &self.shielded_payload, "live_tail.shielded")?;
        if let Some(sup) = &self.supplement {
            encode_supplement_record(&mut out, sup)?;
        }
        if let Some(envelope) = replay_envelope {
            push_len_prefixed_bytes(&mut out, envelope.as_bytes(), "live_tail.facts")?;
        }
        let total_len = out
            .len()
            .checked_add(LIVE_TAIL_TRAILER_LEN)
            .ok_or_else(live_tail_size_overflow)?;
        if !(LIVE_TAIL_HEADER_MIN_LEN..=MAX_LIVE_TAIL_RECORD_BYTES).contains(&total_len) {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "live-tail encoded record length {total_len} outside allowed range"
            )));
        }
        let digest: [u8; 32] = Sha256::digest(&out).into();
        out.extend_from_slice(&digest);
        Ok(out)
    }

    fn validate_for_encode(
        &self,
    ) -> Result<Option<CanonicalBlockReplayEnvelope>, SettledSegmentsError> {
        self.validate_core()?;
        let replay_envelope = self
            .canonical_facts
            .as_ref()
            .map(validate_canonical_facts_replay)
            .transpose()?;
        let replay_len = replay_envelope.as_ref().map(|env| env.as_bytes().len());
        let predicted_len = self.predict_encoded_len(replay_len)?;
        if !(LIVE_TAIL_HEADER_MIN_LEN..=MAX_LIVE_TAIL_RECORD_BYTES).contains(&predicted_len) {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "live-tail encoded record length {predicted_len} outside allowed range"
            )));
        }
        Ok(replay_envelope)
    }

    fn validate_core(&self) -> Result<(), SettledSegmentsError> {
        let proto_block = decode_and_validate_shielded_payload(
            &self.shielded_payload,
            self.block_id,
            self.previous_block_hash,
            self.block_time,
            self.pool_presence,
        )?;
        let empty_sup = BlockSupplementRecord {
            transparent_transactions: Vec::new(),
            canonical_metadata: None,
        };
        let reconstructed = match &self.supplement {
            Some(sup) => {
                validate_supplement_limits(sup)?;
                validate_supplement_against_payload(&proto_block, sup)?
            }
            None => crate::settled_supplement_segment::reconstruct_compact_block_from_proto(
                &proto_block,
                &empty_sup,
            )?,
        };
        if let Some(facts) = &self.canonical_facts {
            self.validate_canonical_facts_match(facts, &reconstructed)?;
        }
        Ok(())
    }

    fn predict_encoded_len(
        &self,
        replay_len: Option<usize>,
    ) -> Result<usize, SettledSegmentsError> {
        let mut total = LIVE_TAIL_HEADER_MIN_LEN
            .checked_add(self.shielded_payload.len())
            .ok_or_else(live_tail_size_overflow)?;
        if let Some(sup) = &self.supplement {
            total = total
                .checked_add(encoded_supplement_len(sup)?)
                .ok_or_else(live_tail_size_overflow)?;
        }
        if let Some(bytes_len) = replay_len {
            let facts_len = 4_usize
                .checked_add(bytes_len)
                .ok_or_else(live_tail_size_overflow)?;
            total = total
                .checked_add(facts_len)
                .ok_or_else(live_tail_size_overflow)?;
        }
        Ok(total)
    }

    #[cfg(test)]
    pub(crate) fn predict_encoded_v1_len(&self) -> Result<usize, SettledSegmentsError> {
        let replay_len = self.canonical_facts.as_ref().map(|facts| {
            encode_canonical_block_replay(
                facts,
                CanonicalBlockReplayFormatVersion::V1,
                CanonicalBlockFactsDigestVersion::V1,
            )
            .as_bytes()
            .len()
        });
        self.predict_encoded_len(replay_len)
    }

    fn validate_canonical_facts_match(
        &self,
        facts: &CanonicalBlockFacts,
        compact: &CompactBlockArtifact,
    ) -> Result<(), SettledSegmentsError> {
        let header: &BlockHeaderArtifact = &facts.block_header;
        if header.height != self.block_id.height
            || header.block_hash != self.block_id.hash
            || header.parent_hash != self.previous_block_hash
            || header.block_time != i64::from(self.block_time)
        {
            return Err(SettledSegmentsError::ChainLinkage(
                "canonical block facts header disagrees with live-tail block identity".to_owned(),
            ));
        }
        let expected_meta = CanonicalBlockSupplement::from_canonical_facts(facts, Some(compact))?;
        if let Some(sup) = &self.supplement
            && let Some(observed_meta) = &sup.canonical_metadata
            && *observed_meta != expected_meta
        {
            return Err(SettledSegmentsError::Integrity(
                "live-tail supplement canonical_metadata disagrees with canonical_facts".to_owned(),
            ));
        }
        Ok(())
    }

    /// Reconstructs a lossless [`CompactBlockArtifact`] from this live-tail record.
    pub(crate) fn reconstruct_compact_block(
        &self,
    ) -> Result<CompactBlockArtifact, SettledSegmentsError> {
        let empty_sup = BlockSupplementRecord {
            transparent_transactions: Vec::new(),
            canonical_metadata: None,
        };
        let sup = self.supplement.as_ref().unwrap_or(&empty_sup);
        Ok(reconstruct_compact_block_artifact(
            &self.shielded_payload,
            sup,
        )?)
    }

    /// Splices a gRPC-framed [`EncodedBlock`] according to `selection`, failing closed
    /// when transparent data or all-transaction retention is requested without a supplement.
    #[cfg(test)]
    pub(crate) fn splice_compact_block(
        &self,
        selection: crate::settled_supplement_segment::CompactBlockSpliceSelection,
    ) -> Result<zinder_proto::compat::lightwalletd::EncodedBlock, SettledSegmentsError> {
        if (selection.include_transparent || selection.retain_all_transactions)
            && self.supplement.is_none()
        {
            return Err(SettledSegmentsError::ChainLinkage(
                "transparent compact block requested for live-tail record without supplement"
                    .to_owned(),
            ));
        }
        if let Some(sup) = &self.supplement
            && supplement_has_wide_fee(sup)
        {
            return Err(SettledSegmentsError::Integrity(
                "compact transaction fee exceeds u32".to_owned(),
            ));
        }
        let transparent = self
            .supplement
            .as_ref()
            .map_or(&[][..], |sup| &sup.transparent_transactions);
        Ok(
            crate::settled_supplement_segment::splice_compact_block_wire(
                &self.shielded_payload,
                transparent,
                selection,
            )?,
        )
    }

    /// Encodes this record in strict versioned `V1` big-endian format with trailing SHA-256.
    pub(crate) fn encode_v1(&self) -> Result<Vec<u8>, SettledSegmentsError> {
        let replay_envelope = self.validate_for_encode()?;
        self.encode_v1_with_validated_envelope(replay_envelope)
    }

    /// Decodes and authenticates a `V1` live-tail record, failing closed on any tamper.
    pub(crate) fn decode_v1(bytes: &[u8]) -> Result<Self, SettledSegmentsError> {
        let body = verify_outer_digest_and_len(bytes)?;
        let mut cursor = ByteReader::new(body);
        let magic = cursor.read_array::<8>("live_tail.magic")?;
        if &magic != LIVE_TAIL_MAGIC {
            return Err(SettledSegmentsError::InvalidFormat(
                "live-tail record magic mismatch".to_owned(),
            ));
        }
        let version = cursor.read_u16_be("live_tail.version")?;
        if version != LIVE_TAIL_RECORD_VERSION_V1 {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "unsupported live-tail record version {version}"
            )));
        }
        let flags = cursor.read_u8("live_tail.flags")?;
        validate_live_tail_flags(flags)?;
        let reserved = cursor.read_u8("live_tail.reserved")?;
        if reserved != 0 {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "non-zero reserved byte {reserved} in live-tail record"
            )));
        }
        let record = decode_record_fields(&mut cursor, flags)?;
        cursor.finish("LiveTailBlockRecord")?;
        record.validate_core()?;
        Ok(record)
    }
}

/// Encodes one [`CompactBlockArtifact`] into the `DEFAULT_RANGE` `CompactBlock` protobuf
/// payload paired with its [`PoolPresenceMask`].
#[cfg(test)]
pub(crate) fn encode_default_shielded_payload(
    block: &CompactBlockArtifact,
) -> Result<(Vec<u8>, PoolPresenceMask), SettledSegmentsError> {
    encode_default_shielded_payload_with_options(block, false)
}

pub(crate) fn encode_default_shielded_payload_with_options(
    block: &CompactBlockArtifact,
    allow_wide_fee_in_supplement: bool,
) -> Result<(Vec<u8>, PoolPresenceMask), SettledSegmentsError> {
    let mut presence = PoolPresenceMask::EMPTY;
    let mut vtx = Vec::new();
    for compact_tx in block.transactions() {
        let scan = &compact_tx.data;
        let wire_fee = match scan.fee_zat {
            Some(fee_zat) => match u32::try_from(fee_zat) {
                Ok(fee) => Some(fee),
                Err(_) if allow_wide_fee_in_supplement => None,
                Err(_) => {
                    return Err(SettledSegmentsError::InvalidFormat(
                        "compact transaction fee exceeds u32".to_owned(),
                    ));
                }
            },
            None => Some(0),
        };
        let tx_mask = transaction_pool_mask(scan);
        presence = presence.union(tx_mask);
        if tx_mask.is_empty() {
            continue;
        }
        let fee = wire_fee.unwrap_or(0);
        vtx.push(lightwalletd::CompactTx {
            index: compact_tx.index,
            txid: encode_internal_transaction_id(compact_tx.transaction_id).to_vec(),
            fee,
            spends: encode_sapling_spends(&scan.sapling_spends),
            outputs: encode_sapling_outputs(&scan.sapling_outputs),
            actions: encode_shielded_actions(&scan.orchard_actions),
            ironwood_actions: encode_shielded_actions(&scan.ironwood_actions),
            vin: Vec::new(),
            vout: Vec::new(),
        });
    }
    let metadata = block.chain_metadata();
    let proto_block = lightwalletd::CompactBlock {
        height: u64::from(block.height().value()),
        hash: encode_internal_block_hash(block.block_hash()).to_vec(),
        prev_hash: encode_internal_block_hash(block.previous_block_hash()).to_vec(),
        time: block.time(),
        header: Vec::new(),
        vtx,
        chain_metadata: Some(lightwalletd::ChainMetadata {
            sapling_commitment_tree_size: metadata.sapling_commitment_tree_size,
            orchard_commitment_tree_size: metadata.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: metadata.ironwood_commitment_tree_size,
        }),
    };
    let payload = proto_block.encode_to_vec();
    if payload.len() > SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "default shielded payload ({} bytes) exceeds {} byte limit",
            payload.len(),
            SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES
        )));
    }
    Ok((payload, presence))
}

fn verify_outer_digest_and_len(bytes: &[u8]) -> Result<&[u8], SettledSegmentsError> {
    if bytes.len() < LIVE_TAIL_HEADER_MIN_LEN || bytes.len() > MAX_LIVE_TAIL_RECORD_BYTES {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "live-tail record length {} outside allowed range",
            bytes.len()
        )));
    }
    let split_idx = bytes.len() - LIVE_TAIL_TRAILER_LEN;
    let body = &bytes[..split_idx];
    let expected_trailer = &bytes[split_idx..];
    let observed_digest: [u8; 32] = Sha256::digest(body).into();
    if observed_digest.as_slice() != expected_trailer {
        return Err(SettledSegmentsError::Integrity(
            "live-tail record SHA-256 trailer verification failed".to_owned(),
        ));
    }
    Ok(body)
}

fn decode_record_fields(
    cursor: &mut ByteReader<'_>,
    flags: u8,
) -> Result<LiveTailBlockRecord, SettledSegmentsError> {
    let height = BlockHeight::new(cursor.read_u32_be("live_tail.height")?);
    let block_hash_bytes = cursor.read_array::<32>("live_tail.block_hash")?;
    let block_hash = decode_internal_block_hash(&block_hash_bytes).map_err(|err| {
        SettledSegmentsError::InvalidFormat(format!("invalid live-tail block_hash: {err}"))
    })?;
    let prev_hash_bytes = cursor.read_array::<32>("live_tail.previous_block_hash")?;
    let previous_block_hash = decode_internal_block_hash(&prev_hash_bytes).map_err(|err| {
        SettledSegmentsError::InvalidFormat(format!("invalid live-tail previous_block_hash: {err}"))
    })?;
    let block_time = cursor.read_u32_be("live_tail.block_time")?;
    let presence_bits = cursor.read_u8("live_tail.pool_presence")?;
    let pool_presence = PoolPresenceMask::from_bits(presence_bits).ok_or_else(|| {
        SettledSegmentsError::InvalidFormat(format!(
            "live-tail record has unknown pool_presence bits {presence_bits:#04x}"
        ))
    })?;
    let shielded_payload = cursor.read_len_prefixed_vec(
        SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES,
        "live_tail.shielded",
    )?;
    let supplement = if (flags & LIVE_TAIL_FLAG_SUPPLEMENT) != 0 {
        Some(decode_supplement_record(cursor)?)
    } else {
        None
    };
    let canonical_facts = if (flags & LIVE_TAIL_FLAG_CANONICAL_FACTS) != 0 {
        let encoded_replay = cursor.read_len_prefixed_vec(
            SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES,
            "live_tail.facts",
        )?;
        let decoded = decode_canonical_block_replay(&encoded_replay).map_err(|err| {
            SettledSegmentsError::Integrity(format!(
                "live-tail canonical block replay failed validation: {err}"
            ))
        })?;
        Some(decoded.into_facts())
    } else {
        None
    };
    Ok(LiveTailBlockRecord {
        block_id: BlockId::new(height, block_hash),
        previous_block_hash,
        block_time,
        shielded_payload,
        pool_presence,
        supplement,
        canonical_facts,
    })
}

fn validate_raw_block_size(byte_len: usize) -> Result<(), SettledSegmentsError> {
    if byte_len == 0 {
        return Err(SettledSegmentsError::InvalidFormat(
            "live-tail raw block bytes must not be empty".to_owned(),
        ));
    }
    if byte_len > SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "live-tail raw block ({byte_len} bytes) exceeds {SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES} byte limit"
        )));
    }
    Ok(())
}

fn validate_shielded_payload_wire_zero_alloc(
    header: &LiveTailHeader,
    shielded_payload: &[u8],
) -> Result<(), SettledSegmentsError> {
    if shielded_payload.len() > SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES {
        return Err(SettledSegmentsError::InvalidFormat(
            "live-tail shielded payload exceeds size bound".to_owned(),
        ));
    }
    let observed = validate_and_inspect_shielded_block_wire(shielded_payload)?;
    let reason = if observed.height != u64::from(header.block_id.height.value())
        || observed.hash != encode_internal_block_hash(header.block_id.hash)
        || observed.prev_hash != encode_internal_block_hash(header.previous_block_hash)
        || observed.time != u64::from(header.block_time)
        || !observed.header.is_empty()
    {
        "live-tail shielded payload identity disagrees with record"
    } else if observed.pool_presence != header.pool_presence {
        "live-tail shielded payload pool_presence disagrees with record"
    } else if !observed.has_chain_metadata {
        "live-tail shielded payload is missing chain_metadata"
    } else if observed.has_empty_tx {
        "live-tail DEFAULT_RANGE transaction carries neither Sapling, Orchard, nor Ironwood fields"
    } else {
        return Ok(());
    };
    Err(SettledShieldedSegmentError::PayloadIntegrity(reason.to_owned()).into())
}

fn decode_and_validate_shielded_payload(
    shielded_payload: &[u8],
    expected_block_id: BlockId,
    expected_prev_hash: BlockHash,
    expected_block_time: u32,
    expected_pool_presence: PoolPresenceMask,
) -> Result<lightwalletd::CompactBlock, SettledSegmentsError> {
    if shielded_payload.len() > SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES {
        return Err(SettledSegmentsError::InvalidFormat(
            "live-tail shielded payload exceeds size bound".to_owned(),
        ));
    }
    crate::settled_supplement_segment::validate_shielded_block_wire(shielded_payload)?;
    let proto_block = lightwalletd::CompactBlock::decode(shielded_payload).map_err(|err| {
        SettledSegmentsError::Integrity(format!("invalid CompactBlock protobuf: {err}"))
    })?;
    if proto_block.height != u64::from(expected_block_id.height.value())
        || proto_block.hash != encode_internal_block_hash(expected_block_id.hash)
        || proto_block.prev_hash != encode_internal_block_hash(expected_prev_hash)
        || proto_block.time != expected_block_time
        || !proto_block.header.is_empty()
        || proto_block.chain_metadata.is_none()
    {
        return Err(SettledSegmentsError::ChainLinkage(
            "live-tail shielded payload header disagrees with record identity".to_owned(),
        ));
    }
    let observed_mask = inspect_proto_vtx_pool_mask(&proto_block.vtx)?;
    if observed_mask != expected_pool_presence {
        return Err(SettledSegmentsError::Integrity(
            "live-tail pool_presence does not match shielded payload transactions".to_owned(),
        ));
    }
    Ok(proto_block)
}

struct LiveTailHeader {
    flags: u8,
    block_id: BlockId,
    previous_block_hash: BlockHash,
    block_time: u32,
    pool_presence: PoolPresenceMask,
}

fn validate_live_tail_flags(flags: u8) -> Result<(), SettledSegmentsError> {
    let wide_fee_prerequisites = LIVE_TAIL_FLAG_WIDE_FEE_KNOWN | LIVE_TAIL_FLAG_SUPPLEMENT;
    if flags & !LIVE_TAIL_KNOWN_FLAGS != 0
        || (flags & LIVE_TAIL_FLAG_HAS_WIDE_FEE != 0
            && flags & wide_fee_prerequisites != wide_fee_prerequisites)
    {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "unsupported live-tail flag bits {flags:#04x}"
        )));
    }
    Ok(())
}

fn parse_authenticated_header_v1(
    bytes: &[u8],
    expected_height: BlockHeight,
) -> Result<(LiveTailHeader, ByteReader<'_>), SettledSegmentsError> {
    let body = verify_outer_digest_and_len(bytes)?;
    let mut cursor = ByteReader::new(body);
    let magic = cursor.read_array::<8>("live_tail.magic")?;
    if &magic != LIVE_TAIL_MAGIC {
        return Err(SettledSegmentsError::InvalidFormat(
            "live-tail record magic mismatch".to_owned(),
        ));
    }
    let version = cursor.read_u16_be("live_tail.version")?;
    if version != LIVE_TAIL_RECORD_VERSION_V1 {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "unsupported live-tail record version {version}"
        )));
    }
    let flags = cursor.read_u8("live_tail.flags")?;
    validate_live_tail_flags(flags)?;
    let reserved = cursor.read_u8("live_tail.reserved")?;
    if reserved != 0 {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "non-zero reserved byte {reserved} in live-tail record"
        )));
    }
    let height = BlockHeight::new(cursor.read_u32_be("live_tail.height")?);
    if height != expected_height {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "live_tail_block key {} disagrees with record height {}",
            expected_height.value(),
            height.value()
        )));
    }
    let block_hash_bytes = cursor.read_array::<32>("live_tail.block_hash")?;
    let block_hash = decode_internal_block_hash(&block_hash_bytes).map_err(|err| {
        SettledSegmentsError::InvalidFormat(format!("invalid live-tail block_hash: {err}"))
    })?;
    let prev_hash_bytes = cursor.read_array::<32>("live_tail.previous_block_hash")?;
    let previous_block_hash = decode_internal_block_hash(&prev_hash_bytes).map_err(|err| {
        SettledSegmentsError::InvalidFormat(format!("invalid live-tail previous_block_hash: {err}"))
    })?;
    let block_time = cursor.read_u32_be("live_tail.block_time")?;
    let presence_bits = cursor.read_u8("live_tail.pool_presence")?;
    let pool_presence = PoolPresenceMask::from_bits(presence_bits).ok_or_else(|| {
        SettledSegmentsError::InvalidFormat(format!(
            "live-tail record has unknown pool_presence bits {presence_bits:#04x}"
        ))
    })?;
    Ok((
        LiveTailHeader {
            flags,
            block_id: BlockId::new(height, block_hash),
            previous_block_hash,
            block_time,
            pool_presence,
        },
        cursor,
    ))
}

fn verify_facts_header_matches_record(
    header: &LiveTailHeader,
    facts: &CanonicalBlockFacts,
) -> Result<(), SettledSegmentsError> {
    let block_header = &facts.block_header;
    if block_header.height != header.block_id.height
        || block_header.block_hash != header.block_id.hash
        || block_header.parent_hash != header.previous_block_hash
        || block_header.block_time != i64::from(header.block_time)
    {
        return Err(SettledSegmentsError::ChainLinkage(
            "canonical block facts header disagrees with live-tail block identity".to_owned(),
        ));
    }
    Ok(())
}

fn skip_opt(
    cursor: &mut ByteReader<'_>,
    len: usize,
    field_label: &str,
) -> Result<(), SettledSegmentsError> {
    if cursor.read_bool(field_label)? {
        let _ = cursor.read_slice(len, field_label)?;
    }
    Ok(())
}

fn opt_u64_exceeds_u32(
    cursor: &mut ByteReader<'_>,
    field_label: &str,
) -> Result<bool, SettledSegmentsError> {
    if cursor.read_bool(field_label)? {
        let fee_zats = cursor.read_u64_be(field_label)?;
        Ok(u32::try_from(fee_zats).is_err())
    } else {
        Ok(false)
    }
}

fn scan_tag2_for_wide_fee(cursor: &mut ByteReader<'_>) -> Result<bool, SettledSegmentsError> {
    if !cursor.read_bool("supplement.has_canonical_metadata")? {
        return Ok(false);
    }
    let mut has_wide_fee = false;
    let _ = cursor.read_slice(32, "canonical_sup.serialized_bytes_digest")?;
    let meta_tx_count =
        read_bounded_count(cursor, MAX_LIVE_TAIL_TX_COUNT, "canonical_sup.tx_count")?;
    for _ in 0..meta_tx_count {
        if scan_canonical_tx_supplement_for_wide_fee(cursor)? {
            has_wide_fee = true;
        }
    }
    Ok(has_wide_fee)
}

fn decode_tag1_and_scan_wide_fee(
    cursor: &mut ByteReader<'_>,
) -> Result<(Vec<CompactTransparentSupplementTx>, bool), SettledSegmentsError> {
    let tx_count = read_bounded_count(
        cursor,
        MAX_LIVE_TAIL_TX_COUNT,
        "supplement.transparent_count",
    )?;
    let mut transparent = Vec::with_capacity(tx_count.min(MAX_LIVE_TAIL_PREALLOC_COUNT));
    let mut has_wide_fee = false;
    for _ in 0..tx_count {
        let sup_tx = decode_transparent_supplement_tx(cursor)?;
        if sup_tx
            .fee_zat
            .is_some_and(|fee_zat| u32::try_from(fee_zat).is_err())
        {
            has_wide_fee = true;
        }
        transparent.push(sup_tx);
    }
    if scan_tag2_for_wide_fee(cursor)? {
        has_wide_fee = true;
    }
    Ok((transparent, has_wide_fee))
}

fn scan_supplement_for_wide_fee(cursor: &mut ByteReader<'_>) -> Result<bool, SettledSegmentsError> {
    let mut has_wide_fee = false;
    let tx_count = read_bounded_count(
        cursor,
        MAX_LIVE_TAIL_TX_COUNT,
        "supplement.transparent_count",
    )?;
    for _ in 0..tx_count {
        let _ = cursor.read_slice(8 + 32, "supplement_tx.head")?;
        if opt_u64_exceeds_u32(cursor, "supplement_tx.fee_zat")? {
            has_wide_fee = true;
        }
        let vin_count = read_bounded_count(
            cursor,
            MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
            "supplement_tx.vin_count",
        )?;
        let vin_bytes = vin_count
            .checked_mul(36)
            .ok_or_else(live_tail_size_overflow)?;
        let _ = cursor.read_slice(vin_bytes, "supplement_tx.vins")?;
        let vout_count = read_bounded_count(
            cursor,
            MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
            "supplement_tx.vout_count",
        )?;
        for _ in 0..vout_count {
            let _ = cursor.read_slice(8, "supplement_vout.value_zat")?;
            let _ = cursor.read_len_prefixed_slice(
                MAX_LIVE_TAIL_SCRIPT_PUB_KEY_BYTES,
                "supplement_vout.script",
            )?;
        }
    }
    if scan_tag2_for_wide_fee(cursor)? {
        has_wide_fee = true;
    }
    Ok(has_wide_fee)
}

fn skip_supplement_record(cursor: &mut ByteReader<'_>) -> Result<(), SettledSegmentsError> {
    let _ = scan_supplement_for_wide_fee(cursor)?;
    Ok(())
}

fn scan_canonical_tx_supplement_for_wide_fee(
    cursor: &mut ByteReader<'_>,
) -> Result<bool, SettledSegmentsError> {
    let _ = cursor.read_slice(8, "canonical_tx_sup.tx_index")?;
    skip_public_facts(cursor)?;
    let _ = cursor.read_slice(32 + 32, "canonical_tx_sup.digest_and_balances")?;
    skip_opt(cursor, 32, "canonical_tx_sup.orchard_anchor")?;
    skip_opt(cursor, 32, "canonical_tx_sup.ironwood_anchor")?;
    skip_opt(cursor, 32, "canonical_tx_sup.sapling_anchor")?;
    skip_opt(cursor, 4, "canonical_tx_sup.zip317")?;
    let wide_fee = opt_u64_exceeds_u32(cursor, "canonical_tx_sup.fee_zat")?;
    skip_opt(cursor, 8, "canonical_tx_sup.burn_zat")?;
    skip_explicit_transparent_facts(cursor)?;
    Ok(wide_fee)
}

fn skip_public_facts(cursor: &mut ByteReader<'_>) -> Result<(), SettledSegmentsError> {
    let _ = cursor.read_slice(32, "public_facts.txid")?;
    skip_opt(cursor, 32, "public_facts.auth_digest")?;
    skip_opt(cursor, 64, "public_facts.wtxid")?;
    match cursor.read_u8("public_facts.tx_version")? {
        1..=6 => {}
        7 => {
            let _ = cursor.read_slice(4, "tx_version.effective_version")?;
            skip_opt(cursor, 4, "tx_version.version_group_id")?;
        }
        kind => {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "unknown transaction version discriminant {kind}"
            )));
        }
    }
    skip_opt(cursor, 4, "public_facts.branch_id")?;
    match cursor.read_u8("public_facts.lock_time")? {
        1 => {}
        2 => {
            let _ = cursor.read_slice(4, "lock_time.height")?;
        }
        3 => {
            let _ = cursor.read_slice(8, "lock_time.unix_seconds")?;
        }
        kind => {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "unknown lock_time discriminant {kind}"
            )));
        }
    }
    skip_opt(cursor, 4, "public_facts.expiry")?;
    let _ = cursor.read_slice(4 + 28, "public_facts.size_and_counts")?;
    skip_opt(cursor, 8, "public_facts.orchard_vb")?;
    skip_opt(cursor, 32, "public_facts.orchard_anchor")?;
    skip_opt(cursor, 8, "public_facts.ironwood_vb")?;
    let _ = decode_privacy_shape(cursor.read_u8("public_facts.privacy_shape")?)?;
    let _ = cursor.read_bool("public_facts.is_coinbase")?;
    let unsup_count = read_bounded_count(
        cursor,
        MAX_LIVE_TAIL_UNSUPPORTED_SECTIONS,
        "public_facts.unsupported_count",
    )?;
    for _ in 0..unsup_count {
        match cursor.read_u8("public_facts.unsupported_section")? {
            1 | 2 => {}
            code => {
                return Err(SettledSegmentsError::InvalidFormat(format!(
                    "unknown unsupported section discriminant {code}"
                )));
            }
        }
    }
    Ok(())
}

const EXPLICIT_TRANSPARENT_FACTS_VERSION_V1: u8 = 1;

fn skip_explicit_transparent_facts(
    cursor: &mut ByteReader<'_>,
) -> Result<(), SettledSegmentsError> {
    if !cursor.read_bool("canonical_tx_sup.has_explicit_transparent")? {
        return Ok(());
    }
    let version = cursor.read_u8("explicit_transparent.version")?;
    if version != EXPLICIT_TRANSPARENT_FACTS_VERSION_V1 {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "unsupported explicit transparent facts version {version}"
        )));
    }
    let vin_count = read_bounded_count(
        cursor,
        MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
        "explicit_transparent.vin_count",
    )?;
    let vin_bytes = vin_count
        .checked_mul(40)
        .ok_or_else(live_tail_size_overflow)?;
    let _ = cursor.read_slice(vin_bytes, "explicit_transparent.vins")?;
    let vout_count = read_bounded_count(
        cursor,
        MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
        "explicit_transparent.vout_count",
    )?;
    for _ in 0..vout_count {
        let _ = cursor.read_slice(4 + 8, "explicit_transparent.vout_head")?;
        let _ = cursor.read_len_prefixed_slice(
            MAX_LIVE_TAIL_SCRIPT_PUB_KEY_BYTES,
            "explicit_transparent.script_pub_key",
        )?;
        let _ = cursor.read_slice(32, "explicit_transparent.address_script_hash")?;
    }
    Ok(())
}

fn inspect_proto_vtx_pool_mask(
    vtx: &[lightwalletd::CompactTx],
) -> Result<PoolPresenceMask, SettledSegmentsError> {
    let mut mask = PoolPresenceMask::EMPTY;
    for proto_tx in vtx {
        if !proto_tx.vin.is_empty() || !proto_tx.vout.is_empty() {
            return Err(SettledSegmentsError::InvalidFormat(
                "live-tail DEFAULT_RANGE shielded payload must not carry transparent fields"
                    .to_owned(),
            ));
        }
        let mut tx_mask = PoolPresenceMask::EMPTY;
        if !proto_tx.spends.is_empty() {
            tx_mask = tx_mask.union(PoolPresenceMask::SAPLING_SPEND);
        }
        if !proto_tx.outputs.is_empty() {
            tx_mask = tx_mask.union(PoolPresenceMask::SAPLING_OUTPUT);
        }
        if !proto_tx.actions.is_empty() {
            tx_mask = tx_mask.union(PoolPresenceMask::ORCHARD);
        }
        if !proto_tx.ironwood_actions.is_empty() {
            tx_mask = tx_mask.union(PoolPresenceMask::IRONWOOD);
        }
        if tx_mask.is_empty() {
            return Err(SettledSegmentsError::InvalidFormat(
                "live-tail DEFAULT_RANGE shielded payload contains empty shielded tx".to_owned(),
            ));
        }
        mask = mask.union(tx_mask);
    }
    Ok(mask)
}

fn validate_transparent_supplement_entries(
    sup: &BlockSupplementRecord,
    block: &CompactBlockArtifact,
) -> Result<(), SettledSegmentsError> {
    for sup_tx in &sup.transparent_transactions {
        if sup_tx.transparent_inputs.is_empty()
            && sup_tx.transparent_outputs.is_empty()
            && sup_tx.fee_zat.is_none()
            && let Ok(pos) = block
                .transactions()
                .binary_search_by_key(&sup_tx.index, |tx| tx.index)
            && !transaction_pool_mask(&block.transactions()[pos].data).is_empty()
        {
            return Err(SettledSegmentsError::InvalidFormat(
                "empty transparent supplement entry duplicates shielded transaction without fee"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_supplement_against_payload(
    proto_block: &lightwalletd::CompactBlock,
    sup: &BlockSupplementRecord,
) -> Result<CompactBlockArtifact, SettledSegmentsError> {
    let reconstructed =
        crate::settled_supplement_segment::reconstruct_compact_block_from_proto(proto_block, sup)?;
    validate_transparent_supplement_entries(sup, &reconstructed)?;
    if let Some(meta) = &sup.canonical_metadata {
        validate_canonical_meta_order(meta, &reconstructed)?;
    }
    Ok(reconstructed)
}

fn validate_canonical_meta_order(
    meta: &CanonicalBlockSupplement,
    reconstructed: &CompactBlockArtifact,
) -> Result<(), SettledSegmentsError> {
    for (idx, tx_sup) in meta.transactions.iter().enumerate() {
        let expected_idx = u64::try_from(idx).map_err(|_| {
            SettledSegmentsError::InvalidFormat("canonical meta tx index overflow".to_owned())
        })?;
        if tx_sup.tx_index != expected_idx {
            return Err(SettledSegmentsError::InvalidFormat(
                "canonical supplement transaction indexes are not contiguous from zero".to_owned(),
            ));
        }
        let matched_compact_tx = reconstructed
            .transactions()
            .binary_search_by_key(&expected_idx, |tx| tx.index)
            .ok()
            .map(|pos| &reconstructed.transactions()[pos]);
        validate_canonical_tx_zip317_actions(tx_sup, matched_compact_tx)?;
    }
    for compact_tx in reconstructed.transactions() {
        let idx = usize::try_from(compact_tx.index).map_err(|_| {
            SettledSegmentsError::ChainLinkage(
                "compact transaction index exceeds host address space".to_owned(),
            )
        })?;
        let Some(tx_sup) = meta.transactions.get(idx) else {
            return Err(SettledSegmentsError::ChainLinkage(
                "canonical supplement missing compact transaction".to_owned(),
            ));
        };
        if tx_sup.public_facts.transaction_id != compact_tx.transaction_id
            || tx_sup.fee_and_burn.fee_zat != compact_tx.data.fee_zat
        {
            return Err(SettledSegmentsError::ChainLinkage(
                "canonical supplement disagrees with reconstructed compact transaction".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_canonical_tx_zip317_actions(
    tx_sup: &CanonicalTransactionSupplement,
    matched_compact_tx: Option<&zinder_core::CompactTransaction>,
) -> Result<(), SettledSegmentsError> {
    let Some(observed_actions) = tx_sup.zip317_logical_actions else {
        return Ok(());
    };
    let counts = tx_sup.public_facts.counts;
    let exact = if counts.transparent_input_count == 0 {
        if counts.transparent_output_count == 0 {
            exact_zip317_logical_actions_from_outputs(counts, std::iter::empty())
        } else if let Some(explicit) = &tx_sup.explicit_transparent_facts {
            exact_zip317_logical_actions_from_outputs(
                counts,
                explicit
                    .transparent_outputs
                    .iter()
                    .map(|out| out.script_pub_key.len()),
            )
        } else if let Some(compact_tx) = matched_compact_tx {
            exact_zip317_logical_actions_from_outputs(
                counts,
                compact_tx
                    .data
                    .transparent_outputs
                    .iter()
                    .map(|out| out.script_pub_key.len()),
            )
        } else {
            None
        }
    } else {
        None
    };
    if let Some(expected) = exact {
        if observed_actions != expected {
            return Err(SettledSegmentsError::Integrity(
                "canonical supplement ZIP-317 logical actions mismatch".to_owned(),
            ));
        }
        return Ok(());
    }
    let min_vin = (u64::from(counts.transparent_input_count).saturating_mul(41)).div_ceil(150);
    let min_vout = (u64::from(counts.transparent_output_count).saturating_mul(9)).div_ceil(34);
    let min_sprout = u64::from(counts.sprout_joinsplit_count).saturating_mul(2);
    let min_sapling = u64::from(counts.sapling_spend_count.max(counts.sapling_output_count));
    let min_total = min_vin
        .max(min_vout)
        .saturating_add(min_sprout)
        .saturating_add(min_sapling)
        .saturating_add(u64::from(counts.orchard_action_count))
        .saturating_add(u64::from(counts.ironwood_action_count));
    if u64::from(observed_actions) < min_total {
        return Err(SettledSegmentsError::Integrity(
            "canonical supplement ZIP-317 logical actions below structural minimum".to_owned(),
        ));
    }
    Ok(())
}

fn supplement_has_wide_fee(sup: &BlockSupplementRecord) -> bool {
    sup.transparent_transactions
        .iter()
        .any(|tx| tx.fee_zat.is_some_and(|fee| u32::try_from(fee).is_err()))
        || sup.canonical_metadata.as_ref().is_some_and(|meta| {
            meta.transactions.iter().any(|tx| {
                tx.fee_and_burn
                    .fee_zat
                    .is_some_and(|fee| u32::try_from(fee).is_err())
            })
        })
}

fn transaction_pool_mask(scan: &zinder_core::CompactTransactionData) -> PoolPresenceMask {
    let mut mask = PoolPresenceMask::EMPTY;
    if !scan.sapling_spends.is_empty() {
        mask = mask.union(PoolPresenceMask::SAPLING_SPEND);
    }
    if !scan.sapling_outputs.is_empty() {
        mask = mask.union(PoolPresenceMask::SAPLING_OUTPUT);
    }
    if !scan.orchard_actions.is_empty() {
        mask = mask.union(PoolPresenceMask::ORCHARD);
    }
    if !scan.ironwood_actions.is_empty() {
        mask = mask.union(PoolPresenceMask::IRONWOOD);
    }
    mask
}

fn encode_sapling_spends(
    spends: &[zinder_core::CompactSaplingSpend],
) -> Vec<lightwalletd::CompactSaplingSpend> {
    spends
        .iter()
        .map(|spend| lightwalletd::CompactSaplingSpend {
            nf: spend.nullifier.to_vec(),
        })
        .collect()
}

fn encode_sapling_outputs(
    outputs: &[zinder_core::CompactSaplingOutput],
) -> Vec<lightwalletd::CompactSaplingOutput> {
    outputs
        .iter()
        .map(|output| lightwalletd::CompactSaplingOutput {
            cmu: output.commitment.to_vec(),
            ephemeral_key: output.ephemeral_key.to_vec(),
            ciphertext: output.ciphertext.to_vec(),
        })
        .collect()
}

fn encode_shielded_actions(
    actions: &[zinder_core::CompactShieldedAction],
) -> Vec<lightwalletd::CompactOrchardAction> {
    actions
        .iter()
        .map(|action| lightwalletd::CompactOrchardAction {
            nullifier: action.nullifier.to_vec(),
            cmx: action.commitment.to_vec(),
            ephemeral_key: action.ephemeral_key.to_vec(),
            ciphertext: action.ciphertext.to_vec(),
        })
        .collect()
}

fn encode_supplement_record(
    out: &mut Vec<u8>,
    sup: &BlockSupplementRecord,
) -> Result<(), SettledSegmentsError> {
    validate_supplement_limits(sup)?;
    push_count_u32(
        out,
        sup.transparent_transactions.len(),
        "supplement.transparent_count",
    )?;
    for sup_tx in &sup.transparent_transactions {
        encode_transparent_supplement_tx(out, sup_tx)?;
    }
    match &sup.canonical_metadata {
        Some(meta) => {
            out.push(1);
            encode_canonical_block_supplement(out, meta)?;
        }
        None => out.push(0),
    }
    Ok(())
}

fn validate_canonical_facts_replay(
    facts: &CanonicalBlockFacts,
) -> Result<CanonicalBlockReplayEnvelope, SettledSegmentsError> {
    let envelope = encode_canonical_block_replay(
        facts,
        CanonicalBlockReplayFormatVersion::V1,
        CanonicalBlockFactsDigestVersion::V1,
    );
    if envelope.as_bytes().len() > SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "live-tail canonical block replay ({} bytes) exceeds {} byte limit",
            envelope.as_bytes().len(),
            SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES
        )));
    }
    decode_canonical_block_replay(envelope.as_bytes()).map_err(|err| {
        SettledSegmentsError::Integrity(format!(
            "live-tail canonical block replay failed validation: {err}"
        ))
    })?;
    Ok(envelope)
}

fn validate_supplement_limits(sup: &BlockSupplementRecord) -> Result<(), SettledSegmentsError> {
    ensure_within_limit(
        sup.transparent_transactions.len(),
        MAX_LIVE_TAIL_TX_COUNT,
        "supplement.transparent_count",
    )?;
    for sup_tx in &sup.transparent_transactions {
        ensure_within_limit(
            sup_tx.transparent_inputs.len(),
            MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
            "supplement_tx.vin_count",
        )?;
        ensure_within_limit(
            sup_tx.transparent_outputs.len(),
            MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
            "supplement_tx.vout_count",
        )?;
        for vout in &sup_tx.transparent_outputs {
            ensure_within_limit(
                vout.script_pub_key.len(),
                MAX_LIVE_TAIL_SCRIPT_PUB_KEY_BYTES,
                "supplement_vout.script",
            )?;
        }
    }
    if let Some(meta) = &sup.canonical_metadata {
        validate_canonical_block_supplement_limits(meta)?;
    }
    Ok(())
}

fn validate_canonical_block_supplement_limits(
    meta: &CanonicalBlockSupplement,
) -> Result<(), SettledSegmentsError> {
    ensure_within_limit(
        meta.transactions.len(),
        MAX_LIVE_TAIL_TX_COUNT,
        "canonical_sup.tx_count",
    )?;
    for tx_sup in &meta.transactions {
        if tx_sup.public_facts.orchard_anchor != tx_sup.shielded_anchors.orchard {
            return Err(SettledSegmentsError::Integrity(
                "canonical supplement orchard anchor disagrees with public_facts".to_owned(),
            ));
        }
        ensure_within_limit(
            tx_sup.public_facts.unsupported_sections.len(),
            MAX_LIVE_TAIL_UNSUPPORTED_SECTIONS,
            "public_facts.unsupported_count",
        )?;
        for &section in &tx_sup.public_facts.unsupported_sections {
            unsupported_section_code(section)?;
        }
        if let Some(explicit) = &tx_sup.explicit_transparent_facts {
            ensure_within_limit(
                explicit.transparent_inputs.len(),
                MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
                "explicit_transparent.vin_count",
            )?;
            ensure_within_limit(
                explicit.transparent_outputs.len(),
                MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
                "explicit_transparent.vout_count",
            )?;
            for tx_out in &explicit.transparent_outputs {
                ensure_within_limit(
                    tx_out.script_pub_key.len(),
                    MAX_LIVE_TAIL_SCRIPT_PUB_KEY_BYTES,
                    "explicit_transparent.script_pub_key",
                )?;
            }
        }
    }
    Ok(())
}

fn decode_supplement_record(
    cursor: &mut ByteReader<'_>,
) -> Result<BlockSupplementRecord, SettledSegmentsError> {
    let tx_count = read_bounded_count(
        cursor,
        MAX_LIVE_TAIL_TX_COUNT,
        "supplement.transparent_count",
    )?;
    let mut transparent_transactions = Vec::with_capacity(tx_count);
    for _ in 0..tx_count {
        transparent_transactions.push(decode_transparent_supplement_tx(cursor)?);
    }
    let canonical_metadata = if cursor.read_bool("supplement.has_canonical_metadata")? {
        Some(decode_canonical_block_supplement(cursor)?)
    } else {
        None
    };
    Ok(BlockSupplementRecord {
        transparent_transactions,
        canonical_metadata,
    })
}

fn encode_transparent_supplement_tx(
    out: &mut Vec<u8>,
    sup_tx: &CompactTransparentSupplementTx,
) -> Result<(), SettledSegmentsError> {
    out.extend_from_slice(&sup_tx.index.to_be_bytes());
    out.extend_from_slice(&encode_internal_transaction_id(sup_tx.transaction_id));
    encode_opt_u64(out, sup_tx.fee_zat);
    push_count_u32(out, sup_tx.transparent_inputs.len(), "supplement.vin_count")?;
    for vin in &sup_tx.transparent_inputs {
        out.extend_from_slice(&encode_internal_transaction_id(vin.previous_transaction_id));
        out.extend_from_slice(&vin.previous_output_index.to_be_bytes());
    }
    let vout_len = sup_tx.transparent_outputs.len();
    push_count_u32(out, vout_len, "supplement.vout_count")?;
    for vout in &sup_tx.transparent_outputs {
        out.extend_from_slice(&vout.value_zat.to_be_bytes());
        push_len_prefixed_bytes(out, &vout.script_pub_key, "supplement.script_pub_key")?;
    }
    Ok(())
}

fn decode_transparent_supplement_tx(
    cursor: &mut ByteReader<'_>,
) -> Result<CompactTransparentSupplementTx, SettledSegmentsError> {
    let index = cursor.read_u64_be("supplement_tx.index")?;
    let txid_bytes = cursor.read_array::<32>("supplement_tx.transaction_id")?;
    let transaction_id = decode_internal_transaction_id(&txid_bytes).map_err(|err| {
        SettledSegmentsError::InvalidFormat(format!("invalid supplement txid: {err}"))
    })?;
    let fee_zat = decode_opt_u64(cursor, "supplement_tx.fee_zat")?;
    let vin_count = read_bounded_count(
        cursor,
        MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
        "supplement_tx.vin_count",
    )?;
    let mut transparent_inputs = Vec::with_capacity(vin_count);
    for _ in 0..vin_count {
        let prev_bytes = cursor.read_array::<32>("supplement_vin.txid")?;
        let previous_transaction_id =
            decode_internal_transaction_id(&prev_bytes).map_err(|err| {
                SettledSegmentsError::InvalidFormat(format!("invalid vin txid: {err}"))
            })?;
        let previous_output_index = cursor.read_u32_be("supplement_vin.vout")?;
        transparent_inputs.push(CompactTransparentInput {
            previous_transaction_id,
            previous_output_index,
        });
    }
    let vout_count = read_bounded_count(
        cursor,
        MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
        "supplement_tx.vout_count",
    )?;
    let mut transparent_outputs = Vec::with_capacity(vout_count);
    for _ in 0..vout_count {
        let value_zat = cursor.read_u64_be("supplement_vout.value_zat")?;
        let script_pub_key = cursor
            .read_len_prefixed_vec(MAX_LIVE_TAIL_SCRIPT_PUB_KEY_BYTES, "supplement_vout.script")?;
        transparent_outputs.push(CompactTransparentOutput {
            value_zat,
            script_pub_key,
        });
    }
    Ok(CompactTransparentSupplementTx {
        index,
        transaction_id,
        fee_zat,
        transparent_inputs,
        transparent_outputs,
    })
}

fn encode_canonical_block_supplement(
    out: &mut Vec<u8>,
    meta: &CanonicalBlockSupplement,
) -> Result<(), SettledSegmentsError> {
    out.extend_from_slice(&meta.serialized_bytes_digest.as_bytes());
    push_count_u32(out, meta.transactions.len(), "canonical_sup.tx_count")?;
    for tx_sup in &meta.transactions {
        encode_canonical_tx_supplement(out, tx_sup)?;
    }
    Ok(())
}

fn decode_canonical_block_supplement(
    cursor: &mut ByteReader<'_>,
) -> Result<CanonicalBlockSupplement, SettledSegmentsError> {
    let block_digest = SerializedBytesDigest::from_bytes(
        cursor.read_array::<32>("canonical_sup.serialized_bytes_digest")?,
    );
    let tx_count = read_bounded_count(cursor, MAX_LIVE_TAIL_TX_COUNT, "canonical_sup.tx_count")?;
    let mut transactions = Vec::with_capacity(tx_count);
    for _ in 0..tx_count {
        transactions.push(decode_canonical_tx_supplement(cursor)?);
    }
    Ok(CanonicalBlockSupplement {
        serialized_bytes_digest: block_digest,
        transactions,
    })
}

fn encode_canonical_tx_supplement(
    out: &mut Vec<u8>,
    tx_sup: &CanonicalTransactionSupplement,
) -> Result<(), SettledSegmentsError> {
    out.extend_from_slice(&tx_sup.tx_index.to_be_bytes());
    encode_public_facts(out, &tx_sup.public_facts)?;
    out.extend_from_slice(&tx_sup.serialized_bytes_digest.as_bytes());
    out.extend_from_slice(&tx_sup.intrinsic_value_balances.sprout_zat.to_be_bytes());
    out.extend_from_slice(&tx_sup.intrinsic_value_balances.sapling_zat.to_be_bytes());
    out.extend_from_slice(&tx_sup.intrinsic_value_balances.orchard_zat.to_be_bytes());
    out.extend_from_slice(&tx_sup.intrinsic_value_balances.ironwood_zat.to_be_bytes());
    encode_opt_fixed(out, tx_sup.shielded_anchors.orchard);
    encode_opt_fixed(out, tx_sup.shielded_anchors.ironwood);
    encode_opt_fixed(out, tx_sup.shielded_anchors.sapling);
    encode_opt_u32(out, tx_sup.zip317_logical_actions);
    encode_opt_u64(out, tx_sup.fee_and_burn.fee_zat);
    encode_opt_u64(out, tx_sup.fee_and_burn.burn_zat);
    encode_explicit_transparent_facts(out, tx_sup.explicit_transparent_facts.as_ref())
}

fn decode_canonical_tx_supplement(
    cursor: &mut ByteReader<'_>,
) -> Result<CanonicalTransactionSupplement, SettledSegmentsError> {
    let tx_index = cursor.read_u64_be("canonical_tx_sup.tx_index")?;
    let public_facts = decode_public_facts(cursor)?;
    let serialized_bytes_digest = SerializedBytesDigest::from_bytes(
        cursor.read_array::<32>("canonical_tx_sup.serialized_bytes_digest")?,
    );
    let intrinsic_value_balances = TransactionIntrinsicValueBalances::new(
        cursor.read_i64_be("canonical_tx_sup.sprout_zat")?,
        cursor.read_i64_be("canonical_tx_sup.sapling_zat")?,
        cursor.read_i64_be("canonical_tx_sup.orchard_zat")?,
        cursor.read_i64_be("canonical_tx_sup.ironwood_zat")?,
    );
    let shielded_anchors = SupplementShieldedAnchors {
        orchard: decode_opt_fixed::<32>(cursor, "canonical_tx_sup.orchard_anchor")?,
        ironwood: decode_opt_fixed::<32>(cursor, "canonical_tx_sup.ironwood_anchor")?,
        sapling: decode_opt_fixed::<32>(cursor, "canonical_tx_sup.sapling_anchor")?,
    };
    if public_facts.orchard_anchor != shielded_anchors.orchard {
        return Err(SettledSegmentsError::Integrity(
            "canonical supplement orchard anchor disagrees with public_facts".to_owned(),
        ));
    }
    let zip317_logical_actions = decode_opt_u32(cursor, "canonical_tx_sup.zip317")?;
    let fee_and_burn = SupplementTxFeeAndBurn {
        fee_zat: decode_opt_u64(cursor, "canonical_tx_sup.fee_zat")?,
        burn_zat: decode_opt_u64(cursor, "canonical_tx_sup.burn_zat")?,
    };
    let explicit_transparent_facts = decode_explicit_transparent_facts(cursor)?;
    Ok(CanonicalTransactionSupplement {
        tx_index,
        public_facts,
        serialized_bytes_digest,
        intrinsic_value_balances,
        shielded_anchors,
        zip317_logical_actions,
        fee_and_burn,
        explicit_transparent_facts,
    })
}

fn encode_explicit_transparent_facts(
    out: &mut Vec<u8>,
    explicit_transparent: Option<&crate::settled_supplement_segment::ExplicitTransparentFacts>,
) -> Result<(), SettledSegmentsError> {
    let Some(explicit) = explicit_transparent else {
        out.push(0);
        return Ok(());
    };
    if explicit.transparent_inputs.len() > MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT
        || explicit.transparent_outputs.len() > MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT
    {
        return Err(SettledSegmentsError::InvalidFormat(
            "explicit transparent facts IO count exceeds limit".to_owned(),
        ));
    }
    out.push(1);
    out.push(EXPLICIT_TRANSPARENT_FACTS_VERSION_V1);
    push_count_u32(
        out,
        explicit.transparent_inputs.len(),
        "explicit_transparent.vin_count",
    )?;
    for tx_in in &explicit.transparent_inputs {
        out.extend_from_slice(&tx_in.input_index.to_be_bytes());
        out.extend_from_slice(&encode_internal_transaction_id(
            tx_in.spent_outpoint.transaction_id,
        ));
        out.extend_from_slice(&tx_in.spent_outpoint.output_index.to_be_bytes());
    }
    push_count_u32(
        out,
        explicit.transparent_outputs.len(),
        "explicit_transparent.vout_count",
    )?;
    for tx_out in &explicit.transparent_outputs {
        if tx_out.script_pub_key.len() > MAX_LIVE_TAIL_SCRIPT_PUB_KEY_BYTES {
            return Err(SettledSegmentsError::InvalidFormat(
                "explicit transparent script_pub_key exceeds size limit".to_owned(),
            ));
        }
        out.extend_from_slice(&tx_out.output_index.to_be_bytes());
        out.extend_from_slice(&tx_out.value_zat.to_be_bytes());
        push_len_prefixed_bytes(
            out,
            &tx_out.script_pub_key,
            "explicit_transparent.script_pub_key",
        )?;
        out.extend_from_slice(&zinder_core::wire::encode_address_script_hash(
            tx_out.address_script_hash,
        ));
    }
    Ok(())
}

fn decode_explicit_transparent_facts(
    cursor: &mut ByteReader<'_>,
) -> Result<Option<crate::settled_supplement_segment::ExplicitTransparentFacts>, SettledSegmentsError>
{
    if !cursor.read_bool("canonical_tx_sup.has_explicit_transparent")? {
        return Ok(None);
    }
    let version = cursor.read_u8("explicit_transparent.version")?;
    if version != EXPLICIT_TRANSPARENT_FACTS_VERSION_V1 {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "unsupported explicit transparent facts version {version}"
        )));
    }
    let vin_count = read_bounded_count(
        cursor,
        MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
        "explicit_transparent.vin_count",
    )?;
    let mut transparent_inputs = Vec::with_capacity(vin_count.min(MAX_LIVE_TAIL_PREALLOC_COUNT));
    for _ in 0..vin_count {
        let input_index = cursor.read_u32_be("explicit_transparent.input_index")?;
        let txid_bytes = cursor.read_array::<32>("explicit_transparent.prev_txid")?;
        let transaction_id = decode_internal_transaction_id(&txid_bytes).map_err(|err| {
            SettledSegmentsError::InvalidFormat(format!("invalid explicit vin txid: {err}"))
        })?;
        let output_index = cursor.read_u32_be("explicit_transparent.prev_vout")?;
        transparent_inputs.push(zinder_core::TransparentInputFact::new(
            input_index,
            zinder_core::TransparentOutPoint::new(transaction_id, output_index),
        ));
    }
    let vout_count = read_bounded_count(
        cursor,
        MAX_LIVE_TAIL_TRANSPARENT_IO_COUNT,
        "explicit_transparent.vout_count",
    )?;
    let mut transparent_outputs = Vec::with_capacity(vout_count.min(MAX_LIVE_TAIL_PREALLOC_COUNT));
    for _ in 0..vout_count {
        let output_index = cursor.read_u32_be("explicit_transparent.output_index")?;
        let amount_zat = cursor.read_u64_be("explicit_transparent.value_zat")?;
        let script_pub_key = cursor.read_len_prefixed_vec(
            MAX_LIVE_TAIL_SCRIPT_PUB_KEY_BYTES,
            "explicit_transparent.script_pub_key",
        )?;
        let hash_bytes = cursor.read_array::<32>("explicit_transparent.address_script_hash")?;
        let address_script_hash = zinder_core::wire::decode_address_script_hash(&hash_bytes)
            .map_err(|err| {
                SettledSegmentsError::InvalidFormat(format!(
                    "invalid explicit address_script_hash: {err}"
                ))
            })?;
        transparent_outputs.push(zinder_core::TransparentOutputFact::new(
            output_index,
            amount_zat,
            script_pub_key,
            address_script_hash,
        ));
    }
    Ok(Some(
        crate::settled_supplement_segment::ExplicitTransparentFacts {
            transparent_inputs,
            transparent_outputs,
        },
    ))
}

fn encode_public_facts(
    out: &mut Vec<u8>,
    facts: &TransactionPublicFacts,
) -> Result<(), SettledSegmentsError> {
    out.extend_from_slice(&encode_internal_transaction_id(facts.transaction_id));
    encode_opt_fixed(out, facts.auth_digest.map(encode_internal_auth_digest));
    encode_opt_fixed(out, facts.wtxid.map(encode_internal_wtxid));
    encode_tx_version(out, facts.version);
    encode_opt_u32(out, facts.consensus_branch_id.map(ConsensusBranchId::value));
    encode_lock_time(out, facts.lock_time);
    encode_opt_u32(out, facts.expiry_height.map(BlockHeight::value));
    out.extend_from_slice(&facts.size_bytes.to_be_bytes());
    encode_component_counts(out, facts.counts);
    encode_opt_i64(out, facts.orchard_value_balance_zat);
    encode_opt_fixed(out, facts.orchard_anchor);
    encode_opt_i64(out, facts.ironwood_value_balance_zat);
    out.push(privacy_shape_code(facts.privacy_shape));
    out.push(u8::from(facts.is_coinbase));
    encode_unsupported_sections(out, &facts.unsupported_sections)
}

fn decode_public_facts(
    cursor: &mut ByteReader<'_>,
) -> Result<TransactionPublicFacts, SettledSegmentsError> {
    let txid_bytes = cursor.read_array::<32>("public_facts.txid")?;
    let transaction_id = decode_internal_transaction_id(&txid_bytes).map_err(|err| {
        SettledSegmentsError::InvalidFormat(format!("invalid public_facts txid: {err}"))
    })?;
    let auth_digest = decode_opt_fixed::<32>(cursor, "public_facts.auth_digest")?
        .map(|bytes| {
            decode_internal_auth_digest(&bytes).map_err(|err| {
                SettledSegmentsError::InvalidFormat(format!("invalid auth_digest: {err}"))
            })
        })
        .transpose()?;
    let wtxid = decode_opt_fixed::<64>(cursor, "public_facts.wtxid")?
        .map(|bytes| {
            decode_internal_wtxid(&bytes)
                .map_err(|err| SettledSegmentsError::InvalidFormat(format!("invalid wtxid: {err}")))
        })
        .transpose()?;
    let version = decode_tx_version(cursor)?;
    let consensus_branch_id =
        decode_opt_u32(cursor, "public_facts.branch_id")?.map(ConsensusBranchId::new);
    let lock_time = decode_lock_time(cursor)?;
    let expiry_height = decode_opt_u32(cursor, "public_facts.expiry")?.map(BlockHeight::new);
    let size_bytes = cursor.read_u32_be("public_facts.size_bytes")?;
    let counts = decode_component_counts(cursor)?;
    let orchard_value_balance_zat = decode_opt_i64(cursor, "public_facts.orchard_vb")?;
    let orchard_anchor = decode_opt_fixed::<32>(cursor, "public_facts.orchard_anchor")?;
    let ironwood_value_balance_zat = decode_opt_i64(cursor, "public_facts.ironwood_vb")?;
    let privacy_shape = decode_privacy_shape(cursor.read_u8("public_facts.privacy_shape")?)?;
    let is_coinbase = cursor.read_bool("public_facts.is_coinbase")?;
    let unsupported_sections = decode_unsupported_sections(cursor)?;
    Ok(TransactionPublicFacts {
        transaction_id,
        auth_digest,
        wtxid,
        version,
        consensus_branch_id,
        lock_time,
        expiry_height,
        size_bytes,
        counts,
        orchard_value_balance_zat,
        orchard_anchor,
        ironwood_value_balance_zat,
        privacy_shape,
        is_coinbase,
        unsupported_sections,
    })
}

fn encode_tx_version(out: &mut Vec<u8>, version: TransactionVersion) {
    match version {
        TransactionVersion::V1 => out.push(1),
        TransactionVersion::V2 => out.push(2),
        TransactionVersion::V3 => out.push(3),
        TransactionVersion::V4 => out.push(4),
        TransactionVersion::V5 => out.push(5),
        TransactionVersion::V6 => out.push(6),
        TransactionVersion::Unsupported {
            effective_version,
            version_group_id,
        } => {
            out.push(7);
            out.extend_from_slice(&effective_version.to_be_bytes());
            encode_opt_u32(out, version_group_id);
        }
    }
}

fn decode_tx_version(
    cursor: &mut ByteReader<'_>,
) -> Result<TransactionVersion, SettledSegmentsError> {
    match cursor.read_u8("public_facts.tx_version")? {
        1 => Ok(TransactionVersion::V1),
        2 => Ok(TransactionVersion::V2),
        3 => Ok(TransactionVersion::V3),
        4 => Ok(TransactionVersion::V4),
        5 => Ok(TransactionVersion::V5),
        6 => Ok(TransactionVersion::V6),
        7 => Ok(TransactionVersion::Unsupported {
            effective_version: cursor.read_u32_be("tx_version.effective_version")?,
            version_group_id: decode_opt_u32(cursor, "tx_version.version_group_id")?,
        }),
        kind => Err(SettledSegmentsError::InvalidFormat(format!(
            "unknown transaction version discriminant {kind}"
        ))),
    }
}

fn encode_lock_time(out: &mut Vec<u8>, lock_time: LockTime) {
    match lock_time {
        LockTime::Unlocked => out.push(1),
        LockTime::Height(height) => {
            out.push(2);
            out.extend_from_slice(&height.value().to_be_bytes());
        }
        LockTime::UnixSeconds(seconds) => {
            out.push(3);
            out.extend_from_slice(&seconds.to_be_bytes());
        }
    }
}

fn decode_lock_time(cursor: &mut ByteReader<'_>) -> Result<LockTime, SettledSegmentsError> {
    match cursor.read_u8("public_facts.lock_time")? {
        1 => Ok(LockTime::Unlocked),
        2 => Ok(LockTime::Height(BlockHeight::new(
            cursor.read_u32_be("lock_time.height")?,
        ))),
        3 => Ok(LockTime::UnixSeconds(
            cursor.read_u64_be("lock_time.unix_seconds")?,
        )),
        kind => Err(SettledSegmentsError::InvalidFormat(format!(
            "unknown lock_time discriminant {kind}"
        ))),
    }
}

fn encode_component_counts(out: &mut Vec<u8>, counts: TransactionComponentCounts) {
    out.extend_from_slice(&counts.transparent_input_count.to_be_bytes());
    out.extend_from_slice(&counts.transparent_output_count.to_be_bytes());
    out.extend_from_slice(&counts.sapling_spend_count.to_be_bytes());
    out.extend_from_slice(&counts.sapling_output_count.to_be_bytes());
    out.extend_from_slice(&counts.orchard_action_count.to_be_bytes());
    out.extend_from_slice(&counts.ironwood_action_count.to_be_bytes());
    out.extend_from_slice(&counts.sprout_joinsplit_count.to_be_bytes());
}

fn decode_component_counts(
    cursor: &mut ByteReader<'_>,
) -> Result<TransactionComponentCounts, SettledSegmentsError> {
    Ok(TransactionComponentCounts {
        transparent_input_count: cursor.read_u32_be("counts.vin")?,
        transparent_output_count: cursor.read_u32_be("counts.vout")?,
        sapling_spend_count: cursor.read_u32_be("counts.sapling_spends")?,
        sapling_output_count: cursor.read_u32_be("counts.sapling_outputs")?,
        orchard_action_count: cursor.read_u32_be("counts.orchard")?,
        ironwood_action_count: cursor.read_u32_be("counts.ironwood")?,
        sprout_joinsplit_count: cursor.read_u32_be("counts.sprout")?,
    })
}

const fn privacy_shape_code(shape: PrivacyShape) -> u8 {
    match shape {
        PrivacyShape::TransparentOnly => 1,
        PrivacyShape::Shielding => 2,
        PrivacyShape::Deshielding => 3,
        PrivacyShape::ShieldedOnly => 4,
        PrivacyShape::Mixed => 5,
        PrivacyShape::Coinbase => 6,
        PrivacyShape::ShieldedCoinbase => 7,
        PrivacyShape::Unclassified => 8,
    }
}

fn decode_privacy_shape(code: u8) -> Result<PrivacyShape, SettledSegmentsError> {
    match code {
        1 => Ok(PrivacyShape::TransparentOnly),
        2 => Ok(PrivacyShape::Shielding),
        3 => Ok(PrivacyShape::Deshielding),
        4 => Ok(PrivacyShape::ShieldedOnly),
        5 => Ok(PrivacyShape::Mixed),
        6 => Ok(PrivacyShape::Coinbase),
        7 => Ok(PrivacyShape::ShieldedCoinbase),
        8 => Ok(PrivacyShape::Unclassified),
        _ => Err(SettledSegmentsError::InvalidFormat(format!(
            "unknown privacy shape discriminant {code}"
        ))),
    }
}

fn unsupported_section_code(section: UnsupportedSection) -> Result<u8, SettledSegmentsError> {
    #[allow(
        clippy::wildcard_enum_match_arm,
        reason = "UnsupportedSection is #[non_exhaustive] in zinder-core."
    )]
    match section {
        UnsupportedSection::FutureVersionHeader => Ok(1_u8),
        UnsupportedSection::FutureShieldedProtocol => Ok(2_u8),
        other => Err(SettledSegmentsError::InvalidFormat(format!(
            "unsupported UnsupportedSection variant: {other:?}"
        ))),
    }
}

fn encode_unsupported_sections(
    out: &mut Vec<u8>,
    sections: &[UnsupportedSection],
) -> Result<(), SettledSegmentsError> {
    push_count_u32(out, sections.len(), "public_facts.unsupported_count")?;
    for &section in sections {
        out.push(unsupported_section_code(section)?);
    }
    Ok(())
}

fn decode_unsupported_sections(
    cursor: &mut ByteReader<'_>,
) -> Result<Vec<UnsupportedSection>, SettledSegmentsError> {
    let count = read_bounded_count(
        cursor,
        MAX_LIVE_TAIL_UNSUPPORTED_SECTIONS,
        "public_facts.unsupported_count",
    )?;
    let mut sections = Vec::with_capacity(count);
    for _ in 0..count {
        let code = cursor.read_u8("public_facts.unsupported_section")?;
        let section = match code {
            1 => UnsupportedSection::FutureVersionHeader,
            2 => UnsupportedSection::FutureShieldedProtocol,
            _ => {
                return Err(SettledSegmentsError::InvalidFormat(format!(
                    "unknown unsupported section discriminant {code}"
                )));
            }
        };
        sections.push(section);
    }
    Ok(sections)
}

fn encode_raw_block_body(
    out: &mut Vec<u8>,
    raw_block: &LiveTailRawBlock,
) -> Result<(), SettledSegmentsError> {
    push_len_prefixed_bytes(out, &raw_block.raw_block_bytes, "live_tail_raw.raw_bytes")?;
    let block_digest: [u8; 32] = Sha256::digest(&raw_block.raw_block_bytes).into();
    out.extend_from_slice(&block_digest);
    push_count_u32(out, raw_block.tx_slices.len(), "live_tail_raw.tx_count")?;
    for tx_slice in &raw_block.tx_slices {
        out.extend_from_slice(&tx_slice.offset.to_be_bytes());
        out.extend_from_slice(&tx_slice.len.to_be_bytes());
        out.extend_from_slice(&tx_slice.sha256);
    }
    Ok(())
}

fn decode_raw_block_body(
    cursor: &mut ByteReader<'_>,
) -> Result<LiveTailRawBlock, SettledSegmentsError> {
    let raw_block_bytes = cursor.read_len_prefixed_vec(
        SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES,
        "live_tail_raw.raw_bytes",
    )?;
    validate_raw_block_size(raw_block_bytes.len())?;
    let expected_digest = cursor.read_array::<32>("live_tail_raw.block_sha256")?;
    let observed_digest: [u8; 32] = Sha256::digest(&raw_block_bytes).into();
    if observed_digest != expected_digest {
        return Err(SettledSegmentsError::Integrity(
            "live-tail raw block SHA-256 verification failed".to_owned(),
        ));
    }
    let slice_count = read_bounded_count(cursor, MAX_LIVE_TAIL_TX_COUNT, "live_tail_raw.tx_count")?;
    let mut tx_slices = Vec::with_capacity(slice_count);
    let mut prev_end = 0_usize;
    for _ in 0..slice_count {
        let offset = cursor.read_u32_be("raw_slice.offset")?;
        let len = cursor.read_u32_be("raw_slice.len")?;
        let sha256 = cursor.read_array::<32>("raw_slice.sha256")?;
        let (start, end) =
            validate_raw_tx_slice_bounds(offset, len, raw_block_bytes.len(), prev_end)?;
        verify_raw_tx_slice_range(&raw_block_bytes, start, end, sha256)?;
        tx_slices.push(LiveTailRawTxSlice::new(offset, len, sha256));
        prev_end = end;
    }
    Ok(LiveTailRawBlock {
        raw_block_bytes,
        tx_slices,
    })
}

fn live_tail_size_overflow() -> SettledSegmentsError {
    SettledSegmentsError::InvalidFormat(
        "live-tail encoded record length overflows usize".to_owned(),
    )
}

fn encoded_supplement_len(sup: &BlockSupplementRecord) -> Result<usize, SettledSegmentsError> {
    let mut len = 4_usize;
    for sup_tx in &sup.transparent_transactions {
        let tx_fixed = 8_usize + 32 + encoded_opt_u64_len(sup_tx.fee_zat) + 4 + 4;
        let vin_bytes = sup_tx
            .transparent_inputs
            .len()
            .checked_mul(36)
            .ok_or_else(live_tail_size_overflow)?;
        len = len
            .checked_add(tx_fixed)
            .and_then(|acc| acc.checked_add(vin_bytes))
            .ok_or_else(live_tail_size_overflow)?;
        for vout in &sup_tx.transparent_outputs {
            let vout_len = 12_usize
                .checked_add(vout.script_pub_key.len())
                .ok_or_else(live_tail_size_overflow)?;
            len = len
                .checked_add(vout_len)
                .ok_or_else(live_tail_size_overflow)?;
        }
    }
    len = len.checked_add(1).ok_or_else(live_tail_size_overflow)?;
    if let Some(meta) = &sup.canonical_metadata {
        len = len.checked_add(36).ok_or_else(live_tail_size_overflow)?;
        for tx_sup in &meta.transactions {
            len = len
                .checked_add(encoded_canonical_tx_sup_len(tx_sup)?)
                .ok_or_else(live_tail_size_overflow)?;
        }
    }
    Ok(len)
}

fn encoded_canonical_tx_sup_len(
    tx_sup: &CanonicalTransactionSupplement,
) -> Result<usize, SettledSegmentsError> {
    let facts = &tx_sup.public_facts;
    let tx_ver_len = match facts.version {
        TransactionVersion::Unsupported {
            version_group_id, ..
        } => 5_usize + encoded_opt_u32_len(version_group_id),
        TransactionVersion::V1
        | TransactionVersion::V2
        | TransactionVersion::V3
        | TransactionVersion::V4
        | TransactionVersion::V5
        | TransactionVersion::V6 => 1_usize,
    };
    let lock_len = match facts.lock_time {
        LockTime::Unlocked => 1_usize,
        LockTime::Height(_) => 5_usize,
        LockTime::UnixSeconds(_) => 9_usize,
    };
    let fixed_len = 8_usize
        + 32
        + encoded_opt_fixed_len::<32>(facts.auth_digest.is_some())
        + encoded_opt_fixed_len::<64>(facts.wtxid.is_some())
        + tx_ver_len
        + encoded_opt_u32_len(facts.consensus_branch_id.map(ConsensusBranchId::value))
        + lock_len
        + encoded_opt_u32_len(facts.expiry_height.map(BlockHeight::value))
        + 4
        + 28
        + encoded_opt_i64_len(facts.orchard_value_balance_zat)
        + encoded_opt_fixed_len::<32>(facts.orchard_anchor.is_some())
        + encoded_opt_i64_len(facts.ironwood_value_balance_zat)
        + 2
        + 4
        + 32
        + 32
        + encoded_opt_fixed_len::<32>(tx_sup.shielded_anchors.orchard.is_some())
        + encoded_opt_fixed_len::<32>(tx_sup.shielded_anchors.ironwood.is_some())
        + encoded_opt_fixed_len::<32>(tx_sup.shielded_anchors.sapling.is_some())
        + encoded_opt_u32_len(tx_sup.zip317_logical_actions)
        + encoded_opt_u64_len(tx_sup.fee_and_burn.fee_zat)
        + encoded_opt_u64_len(tx_sup.fee_and_burn.burn_zat);
    let explicit_len =
        encoded_explicit_transparent_facts_len(tx_sup.explicit_transparent_facts.as_ref())?;
    fixed_len
        .checked_add(facts.unsupported_sections.len())
        .and_then(|acc| acc.checked_add(explicit_len))
        .ok_or_else(live_tail_size_overflow)
}

fn encoded_explicit_transparent_facts_len(
    explicit_transparent: Option<&crate::settled_supplement_segment::ExplicitTransparentFacts>,
) -> Result<usize, SettledSegmentsError> {
    let Some(explicit) = explicit_transparent else {
        return Ok(1);
    };
    let vin_bytes = explicit
        .transparent_inputs
        .len()
        .checked_mul(40)
        .ok_or_else(live_tail_size_overflow)?;
    let mut total = 10_usize
        .checked_add(vin_bytes)
        .ok_or_else(live_tail_size_overflow)?;
    for tx_out in &explicit.transparent_outputs {
        let out_bytes = 48_usize
            .checked_add(tx_out.script_pub_key.len())
            .ok_or_else(live_tail_size_overflow)?;
        total = total
            .checked_add(out_bytes)
            .ok_or_else(live_tail_size_overflow)?;
    }
    Ok(total)
}

fn encoded_raw_block_v1_len(raw: &LiveTailRawBlock) -> Result<usize, SettledSegmentsError> {
    let slice_bytes = raw
        .tx_slices
        .len()
        .checked_mul(40)
        .ok_or_else(live_tail_size_overflow)?;
    (12_usize + 4 + 32 + 4 + LIVE_TAIL_TRAILER_LEN)
        .checked_add(raw.raw_block_bytes.len())
        .and_then(|acc| acc.checked_add(slice_bytes))
        .ok_or_else(live_tail_size_overflow)
}

const fn encoded_opt_u32_len(opt: Option<u32>) -> usize {
    if opt.is_some() { 5 } else { 1 }
}

const fn encoded_opt_u64_len(opt: Option<u64>) -> usize {
    if opt.is_some() { 9 } else { 1 }
}

const fn encoded_opt_i64_len(opt: Option<i64>) -> usize {
    if opt.is_some() { 9 } else { 1 }
}

const fn encoded_opt_fixed_len<const N: usize>(is_some: bool) -> usize {
    if is_some { 1 + N } else { 1 }
}

fn push_count_u32(
    out: &mut Vec<u8>,
    count: usize,
    field_label: &str,
) -> Result<(), SettledSegmentsError> {
    let count_u32 = u32::try_from(count).map_err(|_| {
        SettledSegmentsError::InvalidFormat(format!("count for {field_label} exceeds u32"))
    })?;
    out.extend_from_slice(&count_u32.to_be_bytes());
    Ok(())
}

fn ensure_within_limit(
    observed: usize,
    max_allowed: usize,
    field_label: &str,
) -> Result<(), SettledSegmentsError> {
    if observed > max_allowed {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "{field_label} ({observed}) exceeds limit {max_allowed}"
        )));
    }
    Ok(())
}

fn read_bounded_count(
    cursor: &mut ByteReader<'_>,
    max_count: usize,
    field_label: &str,
) -> Result<usize, SettledSegmentsError> {
    let raw = cursor.read_u32_be(field_label)?;
    let count = usize::try_from(raw).map_err(|_| {
        SettledSegmentsError::InvalidFormat(format!(
            "count for {field_label} exceeds host address space"
        ))
    })?;
    ensure_within_limit(count, max_count, field_label)?;
    Ok(count)
}

fn encode_opt_u32(out: &mut Vec<u8>, optional: Option<u32>) {
    match optional {
        Some(num) => {
            out.push(1);
            out.extend_from_slice(&num.to_be_bytes());
        }
        None => out.push(0),
    }
}

fn decode_opt_u32(
    cursor: &mut ByteReader<'_>,
    field_label: &str,
) -> Result<Option<u32>, SettledSegmentsError> {
    if cursor.read_bool(field_label)? {
        Ok(Some(cursor.read_u32_be(field_label)?))
    } else {
        Ok(None)
    }
}

fn encode_opt_u64(out: &mut Vec<u8>, optional: Option<u64>) {
    match optional {
        Some(num) => {
            out.push(1);
            out.extend_from_slice(&num.to_be_bytes());
        }
        None => out.push(0),
    }
}

fn decode_opt_u64(
    cursor: &mut ByteReader<'_>,
    field_label: &str,
) -> Result<Option<u64>, SettledSegmentsError> {
    if cursor.read_bool(field_label)? {
        Ok(Some(cursor.read_u64_be(field_label)?))
    } else {
        Ok(None)
    }
}

fn encode_opt_i64(out: &mut Vec<u8>, optional: Option<i64>) {
    match optional {
        Some(num) => {
            out.push(1);
            out.extend_from_slice(&num.to_be_bytes());
        }
        None => out.push(0),
    }
}

fn decode_opt_i64(
    cursor: &mut ByteReader<'_>,
    field_label: &str,
) -> Result<Option<i64>, SettledSegmentsError> {
    if cursor.read_bool(field_label)? {
        Ok(Some(cursor.read_i64_be(field_label)?))
    } else {
        Ok(None)
    }
}

fn encode_opt_fixed<const N: usize>(out: &mut Vec<u8>, optional: Option<[u8; N]>) {
    match optional {
        Some(bytes) => {
            out.push(1);
            out.extend_from_slice(&bytes);
        }
        None => out.push(0),
    }
}

fn decode_opt_fixed<const N: usize>(
    cursor: &mut ByteReader<'_>,
    field_label: &str,
) -> Result<Option<[u8; N]>, SettledSegmentsError> {
    if cursor.read_bool(field_label)? {
        Ok(Some(cursor.read_array::<N>(field_label)?))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
pub(super) fn build_raw_v1_record_with_outer_checksum(
    record: &LiveTailBlockRecord,
    pool_presence: PoolPresenceMask,
    shielded_payload: &[u8],
) -> Result<Vec<u8>, SettledSegmentsError> {
    let mut out = Vec::with_capacity(128 + shielded_payload.len());
    out.extend_from_slice(LIVE_TAIL_MAGIC);
    out.extend_from_slice(&LIVE_TAIL_RECORD_VERSION_V1.to_be_bytes());
    out.push(0); // flags: no supplement or embedded canonical_facts
    out.push(0); // reserved
    out.extend_from_slice(&record.block_id.height.value().to_be_bytes());
    out.extend_from_slice(&encode_internal_block_hash(record.block_id.hash));
    out.extend_from_slice(&encode_internal_block_hash(record.previous_block_hash));
    out.extend_from_slice(&record.block_time.to_be_bytes());
    out.push(pool_presence.bits());
    push_len_prefixed_bytes(&mut out, shielded_payload, "live_tail.shielded")?;
    let digest: [u8; 32] = Sha256::digest(&out).into();
    out.extend_from_slice(&digest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use zinder_core::{
        BlockHash, BlockHeaderArtifact, BlockHeight, BlockId, CanonicalBlockFacts,
        CanonicalTransactionFacts, CompactBlockArtifact, CompactChainMetadata, CompactTransaction,
        CompactTransactionData, ConsensusBranchId, LockTime, PrivacyShape, SerializedBytesDigest,
        TransactionComponentCounts, TransactionId, TransactionIntrinsicValueBalances,
        TransactionPublicFacts, TransactionVersion, TransparentAddressScriptHash,
        TransparentInputFact, TransparentOutPoint, TransparentOutputFact,
    };

    use super::{
        LiveTailBlockRecord, LiveTailEncodeOptions, decode_explicit_transparent_facts,
        encode_explicit_transparent_facts,
    };
    use crate::canonical_store::settled_segments::ByteReader;
    use crate::settled_supplement_segment::ExplicitTransparentFacts;
    use zinder_proto::compat::lightwalletd;

    fn roundtrip_test_transaction(
        txid: TransactionId,
        script: Vec<u8>,
        custom_hash: TransparentAddressScriptHash,
    ) -> CanonicalTransactionFacts {
        CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id: txid,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::V5,
                consensus_branch_id: Some(ConsensusBranchId::new(0xC2D6_D0B4)),
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 128,
                counts: TransactionComponentCounts {
                    transparent_input_count: 1,
                    transparent_output_count: 1,
                    sprout_joinsplit_count: 0,
                    sapling_spend_count: 0,
                    sapling_output_count: 0,
                    orchard_action_count: 0,
                    ironwood_action_count: 0,
                },
                orchard_value_balance_zat: Some(0),
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: PrivacyShape::Coinbase,
                is_coinbase: true,
                unsupported_sections: Vec::new(),
            },
            serialized_bytes_digest: SerializedBytesDigest::from_bytes([0x77; 32]),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs: vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            transparent_outputs: vec![TransparentOutputFact::new(0, 50_000, script, custom_hash)],
        }
    }

    fn roundtrip_test_fixture()
    -> Result<(CompactBlockArtifact, CanonicalBlockFacts), Box<dyn std::error::Error>> {
        let block_hash = BlockHash::from_bytes([0x11; 32]);
        let prev_hash = BlockHash::from_bytes([0x00; 32]);
        let height = BlockHeight::new(42);
        let block_id = BlockId::new(height, block_hash);
        let txid = TransactionId::from_bytes([0x22; 32]);
        let script = vec![0x76, 0xa9, 0x14, 0x01];
        let custom_hash = TransparentAddressScriptHash::from_bytes([0xcd; 32]);
        let compact = CompactBlockArtifact::new(
            block_id,
            prev_hash,
            1_700_000_042,
            vec![CompactTransaction {
                index: 0,
                transaction_id: txid,
                data: CompactTransactionData {
                    fee_zat: Some(1_000),
                    transparent_outputs: vec![zinder_core::CompactTransparentOutput {
                        value_zat: 50_000,
                        script_pub_key: script.clone(),
                    }],
                    ..Default::default()
                },
            }],
            CompactChainMetadata {
                sapling_commitment_tree_size: 1,
                orchard_commitment_tree_size: 2,
                ironwood_commitment_tree_size: 0,
            },
        )?;
        let facts = CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                height,
                block_hash,
                prev_hash,
                [0x33; 32],
                [0x44; 32],
                1_700_000_042,
                0x1f07_ffff,
                [0x55; 32],
                1,
                128,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_bytes([0x66; 32]),
            transactions: vec![roundtrip_test_transaction(txid, script, custom_hash)],
        };
        Ok((compact, facts))
    }

    #[test]
    fn encode_from_compact_and_targeted_decoders_roundtrip_with_and_without_supplement()
    -> Result<(), Box<dyn std::error::Error>> {
        let (compact, facts) = roundtrip_test_fixture()?;
        let height = compact.height();

        let (encoded_full, shielded_len) = LiveTailBlockRecord::encode_from_compact_block_v1(
            &compact,
            Some(facts.clone()),
            LiveTailEncodeOptions {
                include_supplement: true,
                retain_canonical_facts: true,
            },
        )?;
        assert!(shielded_len > 0);
        let (decoded_compact, decoded_shielded_len) =
            LiveTailBlockRecord::decode_compact_block_v1(&encoded_full, height)?;
        assert_eq!(decoded_compact, compact);
        assert_eq!(decoded_shielded_len, shielded_len);
        let decoded_facts = LiveTailBlockRecord::decode_canonical_facts_v1(&encoded_full, height)?;
        assert_eq!(decoded_facts, Some(facts));

        let record = LiveTailBlockRecord::decode_v1(&encoded_full)?;
        let sup_meta = record
            .supplement
            .as_ref()
            .and_then(|sup| sup.canonical_metadata.as_ref())
            .ok_or("canonical meta missing")?;
        assert!(
            sup_meta.transactions[0]
                .explicit_transparent_facts
                .is_some()
        );
        let mut raw_explicit = Vec::new();
        encode_explicit_transparent_facts(
            &mut raw_explicit,
            sup_meta.transactions[0].explicit_transparent_facts.as_ref(),
        )?;
        assert_eq!(raw_explicit[0], 1);
        let mut cursor = ByteReader::new(&raw_explicit);
        let roundtrip: Option<ExplicitTransparentFacts> =
            decode_explicit_transparent_facts(&mut cursor)?;
        assert_eq!(
            roundtrip,
            sup_meta.transactions[0].explicit_transparent_facts
        );
        raw_explicit[1] = 2;
        let mut bad_cursor = ByteReader::new(&raw_explicit);
        assert!(decode_explicit_transparent_facts(&mut bad_cursor).is_err());

        let (encoded_min, _) = LiveTailBlockRecord::encode_from_compact_block_v1(
            &compact,
            None,
            LiveTailEncodeOptions {
                include_supplement: false,
                retain_canonical_facts: false,
            },
        )?;
        let (min_compact, _) = LiveTailBlockRecord::decode_compact_block_v1(&encoded_min, height)?;
        assert_eq!(min_compact.transactions(), []);
        assert_eq!(
            LiveTailBlockRecord::decode_canonical_facts_v1(&encoded_min, height)?,
            None
        );
        Ok(())
    }

    #[test]
    fn wide_fee_flags_match_the_legacy_supplement_scan() -> Result<(), Box<dyn std::error::Error>> {
        use sha2::{Digest as _, Sha256};

        use super::{
            LIVE_TAIL_FLAG_HAS_WIDE_FEE, LIVE_TAIL_FLAG_WIDE_FEE_KNOWN, LIVE_TAIL_TRAILER_LEN,
        };

        let (fixture, facts) = roundtrip_test_fixture()?;
        for is_wide in [false, true] {
            let mut transactions = fixture.transactions().to_vec();
            if is_wide {
                transactions[0].data.fee_zat = Some(u64::from(u32::MAX) + 1);
            }
            let compact = CompactBlockArtifact::new(
                BlockId::new(fixture.height(), fixture.block_hash()),
                fixture.previous_block_hash(),
                fixture.time(),
                transactions,
                fixture.chain_metadata(),
            )?;
            let height = compact.height();
            let record =
                LiveTailBlockRecord::from_compact_block(&compact, true, Some(facts.clone()))?;
            let encoded = record.encode_v1()?;
            let flags_offset = super::LIVE_TAIL_MAGIC.len() + size_of::<u16>();
            assert_ne!(encoded[flags_offset] & LIVE_TAIL_FLAG_WIDE_FEE_KNOWN, 0);
            assert_eq!(
                encoded[flags_offset] & LIVE_TAIL_FLAG_HAS_WIDE_FEE != 0,
                is_wide
            );

            // v0.28.0 uses the same layout with both of these bits clear.
            let mut legacy = encoded[..encoded.len() - LIVE_TAIL_TRAILER_LEN].to_vec();
            legacy[flags_offset] &= !(LIVE_TAIL_FLAG_WIDE_FEE_KNOWN | LIVE_TAIL_FLAG_HAS_WIDE_FEE);
            let digest = Sha256::digest(&legacy);
            legacy.extend_from_slice(&digest);
            let current_wire = LiveTailBlockRecord::decode_shielded_wire_v1(&encoded, height)?;
            let legacy_wire = LiveTailBlockRecord::decode_shielded_wire_v1(&legacy, height)?;
            assert_eq!(current_wire, legacy_wire);
            assert_eq!(legacy_wire.has_unrepresentable_wire_fee, is_wide);
            assert_eq!(LiveTailBlockRecord::decode_v1(&legacy)?, record);
            assert_eq!(
                LiveTailBlockRecord::decode_compact_block_v1(&legacy, height)?.0,
                compact,
            );
            assert_eq!(
                LiveTailBlockRecord::decode_canonical_facts_v1(&legacy, height)?,
                Some(facts.clone()),
            );
        }
        Ok(())
    }

    fn sample_shielded_live_tail_record() -> Result<LiveTailBlockRecord, Box<dyn std::error::Error>>
    {
        use zinder_core::{CompactSaplingOutput, CompactSaplingSpend};

        let height = BlockHeight::new(42);
        let block_hash = BlockHash::from_bytes([0x11; 32]);
        let prev_hash = BlockHash::from_bytes([0x00; 32]);
        let compact = CompactBlockArtifact::new(
            BlockId::new(height, block_hash),
            prev_hash,
            1_700_000_042,
            vec![CompactTransaction {
                index: 0,
                transaction_id: TransactionId::from_bytes([0x22; 32]),
                data: CompactTransactionData {
                    fee_zat: Some(1_000),
                    sapling_spends: vec![CompactSaplingSpend {
                        nullifier: [0x33; 32],
                    }],
                    sapling_outputs: vec![CompactSaplingOutput {
                        commitment: [0x44; 32],
                        ephemeral_key: [0x55; 32],
                        ciphertext: [0x66; 52],
                    }],
                    ..Default::default()
                },
            }],
            CompactChainMetadata {
                sapling_commitment_tree_size: 1,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )?;
        Ok(LiveTailBlockRecord::from_compact_block(
            &compact, false, None,
        )?)
    }

    fn assert_wire_decoders_reject(row: &[u8], height: BlockHeight, label: &str) {
        assert!(
            LiveTailBlockRecord::decode_shielded_wire_v1(row, height).is_err(),
            "decode_shielded_wire_v1 must reject {label}",
        );
        assert!(
            LiveTailBlockRecord::decode_splice_wire_v1(row, height).is_err(),
            "decode_splice_wire_v1 must reject {label}",
        );
    }

    #[test]
    fn decode_shielded_wire_v1_rejects_malformed_or_mismatched_payload_despite_valid_checksum()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::build_raw_v1_record_with_outer_checksum;
        use prost::Message as _;

        use crate::settled_shielded_segment::PoolPresenceMask;

        let record = sample_shielded_live_tail_record()?;
        let height = record.block_id.height;
        let valid_encoded = record.encode_v1()?;
        let valid_wire = LiveTailBlockRecord::decode_shielded_wire_v1(&valid_encoded, height)?;
        assert_eq!(valid_wire.block_id.height, height);
        let splice_wire = LiveTailBlockRecord::decode_splice_wire_v1(&valid_encoded, height)?;
        assert_eq!(splice_wire.block_id.height, height);

        let base_proto = lightwalletd::CompactBlock::decode(valid_wire.shielded_payload)?;

        let mut wrong_height = base_proto.clone();
        wrong_height.height += 1;
        let mut wrong_hash = base_proto.clone();
        wrong_hash.hash = vec![0x99; 32];
        let mut wrong_prev_hash = base_proto.clone();
        wrong_prev_hash.prev_hash = vec![0x88; 32];
        let mut wrong_time = base_proto.clone();
        wrong_time.time = record.block_time + 1;
        let mut non_empty_header = base_proto.clone();
        non_empty_header.header = vec![0xaa];
        let mut missing_meta = base_proto.clone();
        missing_meta.chain_metadata = None;
        let mut empty_shielded_tx = base_proto.clone();
        empty_shielded_tx.vtx[0].spends.clear();
        empty_shielded_tx.vtx[0].outputs.clear();
        let mut transparent_in_shielded = base_proto;
        transparent_in_shielded.vtx[0]
            .vin
            .push(lightwalletd::CompactTxIn {
                prevout_txid: vec![0x12; 32],
                prevout_index: 0,
            });

        for (label, mutated) in [
            ("wrong height", wrong_height),
            ("wrong hash", wrong_hash),
            ("wrong prev_hash", wrong_prev_hash),
            ("wrong time", wrong_time),
            ("non-empty header", non_empty_header),
            ("missing chain_metadata", missing_meta),
            ("empty vtx", empty_shielded_tx),
            ("transparent field in shielded vtx", transparent_in_shielded),
        ] {
            let row = build_raw_v1_record_with_outer_checksum(
                &record,
                valid_wire.pool_presence,
                &mutated.encode_to_vec(),
            )?;
            assert_wire_decoders_reject(&row, height, label);
        }

        let wrong_presence_row = build_raw_v1_record_with_outer_checksum(
            &record,
            PoolPresenceMask::ORCHARD,
            valid_wire.shielded_payload,
        )?;
        assert_wire_decoders_reject(&wrong_presence_row, height, "mismatched pool_presence");

        let malformed_proto_row = build_raw_v1_record_with_outer_checksum(
            &record,
            valid_wire.pool_presence,
            &[0xff, 0xff, 0xff],
        )?;
        assert_wire_decoders_reject(&malformed_proto_row, height, "malformed protobuf");
        Ok(())
    }
    #[test]
    fn shielded_wire_still_rejects_invalid_nested_fields_and_tag_order()
    -> Result<(), Box<dyn std::error::Error>> {
        use prost::Message as _;

        let record = sample_shielded_live_tail_record()?;
        let proto = lightwalletd::CompactBlock::decode(record.shielded_payload.as_slice())?;
        let mut missing_txid = proto.clone();
        missing_txid.vtx[0].txid.clear();
        let mut short_ciphertext = proto.clone();
        short_ciphertext.vtx[0].outputs[0].ciphertext.pop();
        let mut missing_nullifier = proto.clone();
        missing_nullifier.vtx[0].spends[0].nf.clear();
        let mut out_of_order = proto.encode_to_vec();
        out_of_order.extend_from_slice(&[0x08, 0x01]);
        let mut duplicate_metadata = proto.encode_to_vec();
        duplicate_metadata.extend_from_slice(&[0x42, 0x00]);

        for (label, payload) in [
            ("missing transaction ID", missing_txid.encode_to_vec()),
            ("short ciphertext", short_ciphertext.encode_to_vec()),
            ("missing nullifier", missing_nullifier.encode_to_vec()),
            ("out-of-order unknown tag", out_of_order),
            ("duplicate chain metadata", duplicate_metadata),
        ] {
            let row = super::build_raw_v1_record_with_outer_checksum(
                &record,
                record.pool_presence,
                &payload,
            )?;
            assert_wire_decoders_reject(&row, record.block_id.height, label);
        }
        Ok(())
    }
}
