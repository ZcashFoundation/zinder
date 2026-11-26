//! Store-owned settled shielded history segment (`ZSHRNG02`).
//!
//! Each immutable segment covers one contiguous settled height interval with a
//! single `Slab 0` of pre-encoded `DEFAULT_RANGE` lightwalletd `CompactBlock`
//! frames (with 5-byte gRPC length prefixes), an `80`-byte per-block index
//! carrying `pool_presence_bitmap` and payload SHA-256 digests, and a 32-byte
//! whole-body SHA-256 trailer compatible with Linux `fs-verity`.
//!
//! Range queries filter and redact `Slab 0` frames via zero-allocation wire
//! projection rather than storing a duplicate nullifier slab, and skip `Slab 0`
//! reads entirely for empty or unmatched nullifier-range blocks.

mod format;
mod reader;
#[cfg(test)]
mod tests;
mod verity;
mod wire_projection;
mod writer;

use std::{io, path::Path};

use thiserror::Error;
use zinder_core::{BlockHeight, BlockId, Network};

pub use reader::{SettledShieldedSegmentOpenOptions, SettledShieldedSegmentReader};
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
    /// Trailer, payload digest, `fs-verity` measurement, or wire payload check failed.
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
            fs_verity_sha256: None,
        }
    }

    /// Computes the number of contiguous blocks covered by this segment interval.
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
}

pub(super) fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}
