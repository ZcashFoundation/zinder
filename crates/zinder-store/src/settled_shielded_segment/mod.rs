//! Store-owned settled shielded history segment (`ZSHRNG02`).
//!
//! Each immutable segment (default slab cap `256 MiB` for single-object SQL
//! `BYTEA STORAGE EXTERNAL` and object-storage compatibility) covers one
//! contiguous settled height interval with:
//!
//! - `[0..128)`: Fixed big-endian `SegmentHeader` (`ZSHRNG02`, network tag,
//!   `artifact_schema_version`, flags, `start_height`, `block_count`,
//!   `first_prev_hash_wire`, `last_block_hash_wire`, `slab_offset`,
//!   `index_offset`).
//! - `[slab_offset..index_offset)`: `Slab 0` of pre-encoded `DEFAULT_RANGE`
//!   lightwalletd `CompactBlock` frames prefixed by 5-byte gRPC headers.
//! - `[index_offset..trailer_offset)`: `80`-byte per-block row index entries
//!   (`block_hash_wire`, `payload_sha256`, `block_time`, `slab_rel_offset`,
//!   `native_bytes`, `pool_presence_bitmap`, `row_flags`, `2` reserved zero
//!   bytes).
//! - `[trailer_offset..trailer_offset + 32)`: Whole-body SHA-256 trailer
//!   compatible with Linux `fs-verity`.
//!
//! Cold admission validates `index_sha256 = SHA-256(header || raw_index || trailer)`
//! in a single positioned read (`O(index)`), retaining only compact `12 B/block`
//! in-memory rows (`+ 0.5 B/block` 64-row page digests when not backed by
//! kernel `fs-verity`). Range planning separates sans-I/O `ByteRange`
//! computation (`SettledShieldedSegmentIndex::plan_block_range`) from I/O,
//! reads short rows (`< 32 KiB`) in `512 KiB` slabs, isolates rows `>= 32 KiB`,
//! skips `Slab 0` for unmatched/empty `nullifiers_only` rows even when cold, and
//! coalesces frames into `EncodedBlockBatcher` (`8 KiB` / `32 KiB`).

mod format;
mod reader;
#[cfg(test)]
mod tests;
mod wire_projection;
mod writer;

use std::{io, path::Path};

use thiserror::Error;
use zinder_core::{BlockHeight, BlockId, Network};

pub use format::{
    DEFAULT_MAX_SLAB_BYTES as DEFAULT_SETTLED_SHIELDED_SEGMENT_MAX_SLAB_BYTES,
    ShieldedSegmentHeader,
};
pub(crate) use format::{
    FRAME_PREFIX_LEN as SHIELDED_FRAME_PREFIX_LEN, HEADER_LEN as SHIELDED_HEADER_LEN,
    MAX_SLAB_BYTES as MAX_SHIELDED_SLAB_BYTES, ROW_INDEX_ENTRY_LEN as SHIELDED_ROW_INDEX_ENTRY_LEN,
    TRAILER_LEN as SHIELDED_TRAILER_LEN,
};
pub use reader::{
    BoundedShieldedRangePlan, SettledShieldedSegmentIndex, SettledShieldedSegmentOpenOptions,
    SettledShieldedSegmentReader, ShieldedBatchReadRequest, ShieldedBoundedBatchReadRequest,
    ShieldedFetchedSlab, ShieldedIndexSlice, ShieldedPlanExecution, ShieldedRangeByteBudget,
    ShieldedRangePlan, ShieldedReadScratch, ShieldedReadStep, ShieldedSlabDecodeRequest,
    VerifiedShieldedIndexSlice,
};
pub(crate) use wire_projection::{
    ExpectedShieldedBlock, synthesize_empty_nullifier_payload,
    validate_live_tail_shielded_block_wire,
};
pub use wire_projection::{
    is_passthrough_shielded_block_wire, project_shielded_block_wire,
    project_shielded_block_wire_with_scratch,
};
pub use writer::SettledShieldedSegmentWriter;

/// Magic header identifying the `ZSHRNG02` settled shielded segment format.
pub const SETTLED_SHIELDED_SEGMENT_MAGIC: &[u8; 8] = b"ZSHRNG02";

/// Maximum encoded protobuf payload length accepted for a single block (`16 MiB`).
pub const SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Header segment flag bit reserved for a future `shifted_epk` sidecar.
pub const SEGMENT_FLAG_RESERVED_SHIFTED_EPK: u16 = 0x0002;

/// Errors returned while writing, admitting, or reading a `ZSHRNG02` segment.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SettledShieldedSegmentError {
    /// Underlying filesystem or positional I/O error.
    #[error("settled shielded segment I/O failure: {0}")]
    Io(#[from] io::Error),
    /// Segment framing, header, index, or offset-table format violation.
    #[error("invalid settled shielded segment format: {0}")]
    InvalidFormat(String),
    /// Block heights or parent-hash chain linkage failed verification.
    #[error("settled shielded segment chain linkage error: {0}")]
    ChainLinkage(String),
    /// Trailer, index digest, payload digest, `fs-verity`, or wire payload check failed.
    #[error("settled shielded segment payload integrity error: {0}")]
    PayloadIntegrity(String),
    /// Single block payload exceeded [`SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES`].
    #[error(
        "block {} shielded payload ({payload_bytes} bytes) exceeds limit ({max_bytes} bytes)",
        block_height.value()
    )]
    PayloadLimitExceeded {
        /// Height of the oversized block.
        block_height: BlockHeight,
        /// Encoded protobuf payload length in bytes.
        payload_bytes: usize,
        /// Configured single-block maximum in bytes.
        max_bytes: usize,
    },
    /// Opening required kernel `fs-verity` protection on an unprotected segment.
    #[error("settled shielded segment requires Linux fs-verity protection")]
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
    /// Cooperative cancellation aborted the read before completion.
    #[error("settled shielded segment read cancelled")]
    Cancelled,
}

impl From<crate::segment_verity::SegmentVerityError> for SettledShieldedSegmentError {
    fn from(error: crate::segment_verity::SegmentVerityError) -> Self {
        match error {
            crate::segment_verity::SegmentVerityError::Io(source) => Self::Io(source),
            crate::segment_verity::SegmentVerityError::DigestMismatch(reason) => {
                Self::PayloadIntegrity(reason)
            }
        }
    }
}

/// Per-block presence bits for shielded components recorded in `ZSHRNG02`.
///
/// Distinguishes Sapling spends from Sapling outputs so `GetBlockRangeNullifiers`
/// preserves Sapling output-only deposit shells (`{index, txid, fee}`) per
/// `lightwalletd` parity while allowing empty blocks for all other pool subsets
/// to bypass `Slab 0` reads entirely.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoolPresenceMask(u8);

impl PoolPresenceMask {
    /// No shielded spends, outputs, or actions are present in the block.
    pub const EMPTY: Self = Self(0);
    /// At least one Sapling spend is present.
    pub const SAPLING_SPEND: Self = Self(1 << 0);
    /// At least one Sapling output is present.
    pub const SAPLING_OUTPUT: Self = Self(1 << 1);
    /// At least one Orchard action is present.
    pub const ORCHARD: Self = Self(1 << 2);
    /// At least one Ironwood action is present.
    pub const IRONWOOD: Self = Self(1 << 3);

    const ALL_KNOWN_BITS: u8 =
        Self::SAPLING_SPEND.0 | Self::SAPLING_OUTPUT.0 | Self::ORCHARD.0 | Self::IRONWOOD.0;

    /// Decodes a mask byte, returning `None` if any unknown reserved bit is set.
    #[must_use]
    pub const fn from_bits(bits: u8) -> Option<Self> {
        if (bits & !Self::ALL_KNOWN_BITS) == 0 {
            Some(Self(bits))
        } else {
            None
        }
    }

    /// Returns the raw presence bitmap byte.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Returns whether `self` is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Returns whether `self` contains every bit in `other`.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Returns whether `self` and `other` share any bit.
    #[must_use]
    pub const fn intersects(self, other: Self) -> bool {
        (self.0 & other.0) != 0
    }

    /// Combines two masks by bitwise OR.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Returns all known shielded pool bits not present in `self`.
    #[must_use]
    pub const fn complement_known(self) -> Self {
        Self(Self::ALL_KNOWN_BITS & !self.0)
    }
}

/// Authenticated interval and integrity pins for one settled `ZSHRNG02` segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShieldedSegmentIdentity {
    /// Network the segment belongs to.
    pub network: Network,
    /// Schema revision of the `CompactBlock` artifact encoded in `Slab 0`.
    pub artifact_schema_version: u16,
    /// First block height covered by the segment (inclusive).
    pub start_height: BlockHeight,
    /// Last block covered by the segment (inclusive).
    pub end_block: BlockId,
    /// Trailing SHA-256 digest over all preceding segment bytes.
    pub sha256: [u8; 32],
    /// SHA-256 digest over header bytes, raw row index bytes, and trailer digest (`O(index)` cold admission pin).
    pub index_sha256: [u8; 32],
    /// Expected Linux `fs-verity` SHA-256 measurement, when known.
    pub fs_verity_sha256: Option<[u8; 32]>,
}

impl ShieldedSegmentIdentity {
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
            index_sha256: [0; 32],
            fs_verity_sha256: None,
        }
    }

    /// Computes the number of contiguous blocks covered by this segment interval.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError::ChainLinkage`] if the interval is
    /// reversed or spans more than `u32::MAX` blocks.
    pub fn block_count(&self) -> Result<u32, SettledShieldedSegmentError> {
        let span = self
            .end_block
            .height
            .value()
            .checked_sub(self.start_height.value())
            .ok_or_else(|| {
                SettledShieldedSegmentError::ChainLinkage(
                    "reversed shielded segment interval".to_owned(),
                )
            })?;
        span.checked_add(1).ok_or_else(|| {
            SettledShieldedSegmentError::ChainLinkage(
                "shielded segment interval exceeds u32 height span".to_owned(),
            )
        })
    }
}

/// Shielded pool and redaction projection applied when reading `ZSHRNG02` blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "Each shielded pool and nullifier redaction flag is independently selected."
)]
pub struct ShieldedProjectionSelection {
    /// Retain Sapling spends (and outputs unless `nullifiers_only` is set).
    pub sapling: bool,
    /// Retain Orchard actions (redacted to nullifiers if `nullifiers_only` is set).
    pub orchard: bool,
    /// Retain Ironwood actions (redacted to nullifiers if `nullifiers_only` is set).
    pub ironwood: bool,
    /// Redact outputs, ciphertexts, ephemeral keys, and chain metadata.
    pub nullifiers_only: bool,
}

impl ShieldedProjectionSelection {
    /// Default shielded `GetBlockRange` projection (all shielded pools, full payloads).
    pub const DEFAULT_SHIELDED: Self = Self {
        sapling: true,
        orchard: true,
        ironwood: true,
        nullifiers_only: false,
    };

    /// Default `GetBlockRangeNullifiers` projection (all shielded pools, nullifiers only).
    pub const DEFAULT_NULLIFIERS: Self = Self {
        sapling: true,
        orchard: true,
        ironwood: true,
        nullifiers_only: true,
    };

    /// Constructs a custom pool projection selection.
    #[allow(
        clippy::fn_params_excessive_bools,
        reason = "Selects the three independent shielded pools and nullifier redaction mode."
    )]
    #[must_use]
    pub const fn new(sapling: bool, orchard: bool, ironwood: bool, nullifiers_only: bool) -> Self {
        Self {
            sapling,
            orchard,
            ironwood,
            nullifiers_only,
        }
    }

    /// Returns the [`PoolPresenceMask`] of transactions included by this selection.
    ///
    /// Includes both Sapling spends and Sapling outputs even when
    /// `nullifiers_only` is `true`, preserving deposit transaction shells per
    /// `lightwalletd` parity.
    #[must_use]
    pub const fn requested_pool_mask(self) -> PoolPresenceMask {
        let mut mask = PoolPresenceMask::EMPTY;
        if self.sapling {
            mask = mask
                .union(PoolPresenceMask::SAPLING_SPEND)
                .union(PoolPresenceMask::SAPLING_OUTPUT);
        }
        if self.orchard {
            mask = mask.union(PoolPresenceMask::ORCHARD);
        }
        if self.ironwood {
            mask = mask.union(PoolPresenceMask::IRONWOOD);
        }
        mask
    }

    /// Returns `true` when `presence` contains at least one pool requested by `self`.
    #[must_use]
    pub const fn matches_pool_presence(self, presence: PoolPresenceMask) -> bool {
        self.requested_pool_mask().intersects(presence)
    }
}

pub(super) fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}
