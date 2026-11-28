//! Store-owned settled raw block body segment (`ZSBLKB01`).
//!
//! Each immutable segment (default slab cap `256 MiB` for single-object SQL
//! `BYTEA STORAGE EXTERNAL` and object-storage compatibility) covers one
//! contiguous settled height interval with:
//!
//! - `[0..128)`: Fixed big-endian [`RawBlockSegmentHeader`] (`ZSBLKB01`,
//!   network tag, flags, codec, `start_height`, `block_count`,
//!   `first_prev_hash_wire`, `last_block_hash_wire`, `slab_offset`,
//!   `tx_table_offset`, `row_index_offset`).
//! - `[slab_offset..tx_table_offset)`: `Slab 0` storing each consensus raw
//!   block once (uncompressed or `Zstd`-3 compressed).
//! - `[tx_table_offset..row_index_offset)`: Flat `40`-byte per-transaction
//!   slice table `(tx_offset, tx_len, tx_sha256)`, read on demand via
//!   positioned reads without retaining a full `Vec<TxSliceEntry>` in memory.
//! - `[row_index_offset..trailer_offset)`: `128`-byte per-block row index
//!   entries carrying block hash, payload/uncompressed SHA-256 integrity
//!   digests, slab offset, uncompressed length, and transaction slice bounds.
//! - `[trailer_offset..trailer_offset + 32)`: Whole-body SHA-256 trailer
//!   compatible with Linux `fs-verity`.
//!
//! Cold admission validates `index_sha256 = SHA-256(header || raw_tx_table ||
//! raw_row_index || trailer)` in a single positioned read (`O(index)`),
//! retaining compact `20 B/block` in-memory rows (plus optional `64`-row and
//! `256`-tx portable page digests when not backed by kernel `fs-verity`).
//! [`SettledRawBlockSegmentIndex`] exposes pure sans-I/O [`ByteRange`] planning
//! decoupled from [`PositionalByteSource`] I/O.

mod format;
mod reader;
#[cfg(test)]
mod tests;
mod writer;

use std::{io, path::Path};

use thiserror::Error;
use zinder_core::{BlockHeight, BlockId, Network};

pub use format::{
    DEFAULT_MAX_SLAB_BYTES as DEFAULT_SETTLED_RAW_BLOCK_SEGMENT_MAX_SLAB_BYTES,
    RawBlockSegmentHeader,
};
pub(crate) use format::{
    FRAME_PREFIX_LEN as RAW_FRAME_PREFIX_LEN, HEADER_LEN as RAW_HEADER_LEN,
    MAX_COMPRESSED_PAYLOAD_BYTES as MAX_RAW_COMPRESSED_PAYLOAD_BYTES,
    MAX_SEGMENT_TX_ENTRIES as MAX_RAW_SEGMENT_TX_ENTRIES, MAX_SLAB_BYTES as MAX_RAW_SLAB_BYTES,
    ROW_INDEX_ENTRY_LEN as RAW_ROW_INDEX_ENTRY_LEN, TRAILER_LEN as RAW_TRAILER_LEN,
    TX_SLICE_ENTRY_LEN as RAW_TX_SLICE_ENTRY_LEN,
};
pub use reader::{
    RawBlockReadPlan, RawBlockRowIndexSlice, RawBlockTxDigestsPlan, RawBlockTxTableSlice,
    RawTransactionReadPlan, SettledRawBlockSegmentIndex, SettledRawBlockSegmentOpenOptions,
    SettledRawBlockSegmentReader, VerifiedRawBlockRowIndexSlice, VerifiedRawBlockTxTableSlice,
};
pub use writer::SettledRawBlockSegmentWriter;

/// Magic header identifying the `ZSBLKB01` settled raw block segment format.
pub const SETTLED_RAW_BLOCK_SEGMENT_MAGIC: &[u8; 8] = b"ZSBLKB01";

/// Maximum uncompressed consensus raw block length accepted for one block (`16 MiB`).
pub const SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES: usize = 16 * 1024 * 1024;

/// Errors returned while writing, admitting, or reading a `ZSBLKB01` segment.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SettledRawBlockSegmentError {
    /// Underlying filesystem or positional I/O error.
    #[error("settled raw block segment I/O failure: {0}")]
    Io(#[from] io::Error),
    /// Segment framing, header, transaction slice table, or row index violation.
    #[error("invalid settled raw block segment format: {0}")]
    InvalidFormat(String),
    /// Block heights or parent-hash chain linkage failed verification.
    #[error("settled raw block segment chain linkage error: {0}")]
    ChainLinkage(String),
    /// Trailer, index digest, payload digest, uncompressed digest, transaction
    /// digest, codec decode, or `fs-verity` measurement failed.
    #[error("settled raw block segment payload integrity error: {0}")]
    PayloadIntegrity(String),
    /// Single block raw payload exceeded [`SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES`].
    #[error(
        "block {} raw payload ({payload_bytes} bytes) exceeds limit ({max_bytes} bytes)",
        block_height.value()
    )]
    PayloadLimitExceeded {
        /// Height of the oversized block.
        block_height: BlockHeight,
        /// Uncompressed consensus block length in bytes.
        payload_bytes: usize,
        /// Configured single-block maximum in bytes.
        max_bytes: usize,
    },
    /// Opening required kernel `fs-verity` protection on an unprotected segment.
    #[error("settled raw block segment requires Linux fs-verity protection")]
    FsVerityRequired,
    /// Requested height falls outside the segment's admitted coverage.
    #[error(
        "requested height {} is outside segment coverage [{}, {}]",
        height.value(),
        start.value(),
        end.value()
    )]
    HeightOutOfRange {
        /// Out-of-bounds height in the request.
        height: BlockHeight,
        /// Segment first covered height.
        start: BlockHeight,
        /// Segment last covered height.
        end: BlockHeight,
    },
    /// Requested block-local transaction index is out of range for the block.
    #[error(
        "requested transaction index {tx_index} at height {} exceeds transaction count {tx_count}",
        height.value()
    )]
    TransactionIndexOutOfRange {
        /// Block height in the request.
        height: BlockHeight,
        /// Out-of-bounds transaction index.
        tx_index: u32,
        /// Total transactions recorded for the block.
        tx_count: u32,
    },
}

impl From<crate::segment_verity::SegmentVerityError> for SettledRawBlockSegmentError {
    fn from(error: crate::segment_verity::SegmentVerityError) -> Self {
        match error {
            crate::segment_verity::SegmentVerityError::Io(source) => Self::Io(source),
            crate::segment_verity::SegmentVerityError::DigestMismatch(reason) => {
                Self::PayloadIntegrity(reason)
            }
        }
    }
}

/// Payload compression codec applied to each block stored in `Slab 0`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum RawBlockCodec {
    /// Store consensus raw block bytes uncompressed (`codec_id = 0`).
    None,
    /// Compress each consensus raw block independently with `Zstd` level 3 (`codec_id = 1`).
    #[default]
    Zstd,
}

impl RawBlockCodec {
    /// Returns the numeric codec identifier written to the segment header.
    #[must_use]
    pub const fn id(self) -> u16 {
        match self {
            Self::None => 0,
            Self::Zstd => 1,
        }
    }

    /// Decodes a header codec identifier, returning `None` if unsupported.
    #[must_use]
    pub const fn from_id(codec_id: u16) -> Option<Self> {
        match codec_id {
            0 => Some(Self::None),
            1 => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// Authenticated interval and integrity pins for one settled `ZSBLKB01` segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawBlockSegmentIdentity {
    /// Network the segment belongs to.
    pub network: Network,
    /// Payload compression codec recorded in the segment header.
    pub codec: RawBlockCodec,
    /// First block height covered by the segment (inclusive).
    pub start_height: BlockHeight,
    /// Last block covered by the segment (inclusive).
    pub end_block: BlockId,
    /// Trailing SHA-256 digest over all preceding segment bytes.
    pub sha256: [u8; 32],
    /// SHA-256 digest over header, raw tx slice table, raw row index, and
    /// trailer (`O(index)` cold admission pin).
    pub index_sha256: [u8; 32],
    /// Expected Linux `fs-verity` SHA-256 measurement, when known.
    pub fs_verity_sha256: Option<[u8; 32]>,
}

impl RawBlockSegmentIdentity {
    /// Creates an uncommitted interval span before writing a new segment.
    #[must_use]
    pub const fn span(
        network: Network,
        codec: RawBlockCodec,
        start_height: BlockHeight,
        end_block: BlockId,
    ) -> Self {
        Self {
            network,
            codec,
            start_height,
            end_block,
            sha256: [0; 32],
            index_sha256: [0; 32],
            fs_verity_sha256: None,
        }
    }

    /// Computes the number of contiguous blocks covered by this segment interval.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError::ChainLinkage`] on reversed intervals
    /// or [`SettledRawBlockSegmentError::InvalidFormat`] when exceeding the maximum
    /// segment block bound.
    pub fn block_count(&self) -> Result<u32, SettledRawBlockSegmentError> {
        let span = self
            .end_block
            .height
            .value()
            .checked_sub(self.start_height.value())
            .ok_or_else(|| {
                SettledRawBlockSegmentError::ChainLinkage(
                    "reversed raw block segment interval".to_owned(),
                )
            })?;
        let count = span.checked_add(1).ok_or_else(|| {
            SettledRawBlockSegmentError::ChainLinkage(
                "raw block segment interval exceeds u32 height span".to_owned(),
            )
        })?;
        if count > format::MAX_SEGMENT_BLOCKS {
            return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
                "raw block segment block count {count} exceeds maximum {}",
                format::MAX_SEGMENT_BLOCKS
            )));
        }
        Ok(count)
    }
}

/// Byte slice bounds of one consensus transaction within its uncompressed block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawBlockTransactionSlice {
    /// Zero-based byte offset of the serialized transaction inside the block.
    pub offset: u32,
    /// Serialized transaction byte length.
    pub len: u32,
}

impl RawBlockTransactionSlice {
    /// Creates a transaction slice descriptor from block-relative offset and length.
    #[must_use]
    pub const fn new(offset: u32, len: u32) -> Self {
        Self { offset, len }
    }
}

pub(super) fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}
