//! Store-owned settled supplement history segment (`ZSTSUP01`).
//!
//! Each immutable segment covers one contiguous settled height interval with
//! chunked Zstd-3 (or uncompressed) per-block tagged sections:
//! - Tag `1`: compact transparent supplement per block (`tx.index`, `txid`,
//!   `fee_zat`, `vin`, `vout` covering transparent-only transactions and
//!   transparent components of mixed transactions).
//! - Tag `2`: extensible canonical block and transaction supplement metadata.
//!
//! Together with `ZSHRNG02` `Slab 0`, [`splice_compact_block_wire`] produces
//! byte-identical `CompactBlock` gRPC frames and
//! [`reconstruct_compact_block_artifact`] recovers lossless
//! [`zinder_core::CompactBlockArtifact`] records.

mod format;
mod reader;
mod section;
#[cfg(test)]
mod tests;
mod verity;
mod wire_splice;
mod writer;

use std::{io, path::Path};

use thiserror::Error;
use zinder_core::{BlockHeight, BlockId, Network};

pub use reader::{SettledSupplementSegmentOpenOptions, SettledSupplementSegmentReader};
pub use section::{
    BlockSupplementRecord, CanonicalBlockSupplement, CanonicalTransactionSupplement,
    CompactTransparentSupplementTx, SupplementShieldedAnchors, SupplementTxFeeAndBurn,
};
pub use wire_splice::{
    CompactBlockSpliceSelection, reconstruct_compact_block_artifact, splice_compact_block_wire,
};
pub use writer::{SettledSupplementSegmentWriteOptions, SettledSupplementSegmentWriter};

/// Magic header identifying the `ZSTSUP01` settled supplement segment format.
pub const SETTLED_SUPPLEMENT_SEGMENT_MAGIC: &[u8; 8] = b"ZSTSUP01";

/// Maximum uncompressed payload length accepted for a single block (`16 MiB`).
pub const SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES: usize = 16 * 1024 * 1024;

/// Default number of contiguous blocks grouped into one compressed chunk.
pub const SETTLED_SUPPLEMENT_DEFAULT_CHUNK_BLOCKS: u16 = 16;

/// Maximum supported number of blocks per chunk (`255 * 16 MiB <= u32::MAX`).
pub const SETTLED_SUPPLEMENT_MAX_CHUNK_BLOCKS: u16 = 255;

/// Compression codec identifier stored in the `ZSTSUP01` header.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum SupplementCompressionCodec {
    /// Uncompressed chunk payloads (`codec_id = 0`).
    None,
    /// Zstd level-3 compressed chunk payloads (`codec_id = 1`).
    #[default]
    Zstd,
}

impl SupplementCompressionCodec {
    /// Decodes a `ZSTSUP01` header `codec_id`, failing closed on unknown IDs.
    pub const fn from_id(codec_id: u16) -> Result<Self, SettledSupplementSegmentError> {
        match codec_id {
            0 => Ok(Self::None),
            1 => Ok(Self::Zstd),
            _ => Err(SettledSupplementSegmentError::InvalidCodecId(codec_id)),
        }
    }

    /// Returns the numeric `codec_id` written to the `ZSTSUP01` header.
    #[must_use]
    pub const fn id(self) -> u16 {
        match self {
            Self::None => 0,
            Self::Zstd => 1,
        }
    }
}

/// Errors returned while writing, admitting, splicing, or reading a `ZSTSUP01` segment.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SettledSupplementSegmentError {
    /// Underlying filesystem or positional I/O error.
    #[error("settled supplement segment I/O failure: {0}")]
    Io(#[from] io::Error),
    /// Segment framing, header, chunk index, or row index format violation.
    #[error("invalid settled supplement segment format: {0}")]
    InvalidFormat(String),
    /// Unsupported compression codec identifier in the segment header.
    #[error("unsupported settled supplement segment codec id {0}")]
    InvalidCodecId(u16),
    /// Block heights or parent-hash chain linkage failed verification.
    #[error("settled supplement segment chain linkage error: {0}")]
    ChainLinkage(String),
    /// Trailer, chunk digest, payload digest, `fs-verity` measurement, or section check failed.
    #[error("settled supplement segment payload integrity error: {0}")]
    PayloadIntegrity(String),
    /// Single block or chunk payload exceeded the configured byte bound.
    #[error(
        "block {} supplement payload ({payload_bytes} bytes) exceeds limit ({max_bytes} bytes)",
        block_height.value()
    )]
    PayloadLimitExceeded {
        /// Height of the oversized block.
        block_height: BlockHeight,
        /// Payload length in bytes.
        payload_bytes: usize,
        /// Configured maximum in bytes.
        max_bytes: usize,
    },
    /// Opening required kernel `fs-verity` protection on an unprotected segment.
    #[error("settled supplement segment requires Linux fs-verity protection")]
    FsVerityRequired,
    /// Requested height range falls outside the segment's admitted coverage or order.
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
}

/// Authenticated interval and integrity pins for one settled `ZSTSUP01` segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SupplementSegmentIdentity {
    /// Network the segment belongs to.
    pub network: Network,
    /// Schema revision of the `CompactBlock` artifact paired with this segment.
    pub artifact_schema_version: u16,
    /// First block height covered by the segment (inclusive).
    pub start_height: BlockHeight,
    /// Last block covered by the segment (inclusive).
    pub end_block: BlockId,
    /// Trailing SHA-256 digest over all preceding segment bytes.
    pub sha256: [u8; 32],
    /// Expected Linux `fs-verity` SHA-256 measurement, when known.
    pub fs_verity_sha256: Option<[u8; 32]>,
}

impl SupplementSegmentIdentity {
    /// Creates an uncommitted interval span before writing a new segment.
    #[must_use]
    pub const fn span(
        network: Network,
        artifact_schema_version: u16,
        start_height: BlockHeight,
        end_block: BlockId,
    ) -> Self {
        Self {
            network,
            artifact_schema_version,
            start_height,
            end_block,
            sha256: [0; 32],
            fs_verity_sha256: None,
        }
    }

    /// Computes the number of contiguous blocks covered by this segment interval.
    pub fn block_count(&self) -> Result<u32, SettledSupplementSegmentError> {
        let span = self
            .end_block
            .height
            .value()
            .checked_sub(self.start_height.value())
            .ok_or_else(|| {
                SettledSupplementSegmentError::ChainLinkage(
                    "reversed supplement segment interval".to_owned(),
                )
            })?;
        span.checked_add(1).ok_or_else(|| {
            SettledSupplementSegmentError::ChainLinkage(
                "supplement segment interval exceeds u32 height span".to_owned(),
            )
        })
    }
}

pub(super) fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}
