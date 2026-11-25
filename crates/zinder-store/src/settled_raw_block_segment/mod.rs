//! Store-owned settled raw block body segment (`ZSBLKB01`).
//!
//! Each immutable segment covers one contiguous settled height interval with a
//! single `Slab 0` storing each consensus raw block once (either uncompressed or
//! `Zstd`-3 compressed), a flat per-transaction slice table
//! `(tx_offset, tx_len, tx_sha256)`, a `128`-byte per-block row index carrying
//! uncompressed length and SHA-256 integrity digests, and a `32`-byte
//! whole-body SHA-256 trailer compatible with Linux `fs-verity`.
//!
//! Single-transaction reads decompress and verify the containing block once and
//! slice the exact consensus transaction bytes without storing duplicate raw
//! transaction blobs.

mod format;
mod reader;
#[cfg(test)]
mod tests;
mod verity;
mod writer;

use std::{io, path::Path};

use thiserror::Error;
use zinder_core::{BlockHeight, BlockId, Network};

pub use reader::{SettledRawBlockSegmentOpenOptions, SettledRawBlockSegmentReader};
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
    /// Trailer, payload digest, uncompressed digest, transaction digest, codec
    /// decode, or `fs-verity` measurement failed.
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
            fs_verity_sha256: None,
        }
    }

    /// Computes the number of contiguous blocks covered by this segment interval.
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
