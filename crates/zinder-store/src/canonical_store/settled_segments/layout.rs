//! External settled segment filesystem layout, filenames, aligned strides,
//! `SettledSegmentIndexEntry` V1 binary codec, and JSON manifest helpers.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestVersion, Network,
    wire::{
        decode_internal_block_hash, decode_rpc_block_hash_hex, decode_zinder_native_chain_name,
        encode_internal_block_hash, encode_rpc_block_hash_hex, encode_zinder_native_chain_name,
    },
};

use crate::{
    settled_raw_block_segment::{RawBlockCodec, RawBlockSegmentIdentity},
    settled_shielded_segment::ShieldedSegmentIdentity,
    settled_supplement_segment::SupplementSegmentIdentity,
};

use super::super::CanonicalSequenceCheckpoint;
use super::{ByteReader, SettledSegmentsError, VerifiedDir, reject_unsafe_component};

/// Top-level directory name under `<store_root>` storing external settled segments.
pub(crate) const SEGMENTS_DIR_NAME: &str = "segments";
/// Staging directory name under `<store_root>/segments` holding in-flight files.
pub(crate) const SEGMENTS_STAGING_DIR_NAME: &str = ".staging";
/// Directory name storing `ZSHRNG02` shielded segment files.
pub(crate) const SHIELDED_SEGMENT_DIR: &str = "shielded";
/// Directory name storing `ZSTSUP01` supplement segment files.
pub(crate) const SUPPLEMENT_SEGMENT_DIR: &str = "supplement";
/// Directory name storing `ZSBLKB01` raw-block segment files.
pub(crate) const RAW_BLOCK_SEGMENT_DIR: &str = "raw_block";

/// Canonical filename extension for `ZSHRNG02` segments.
pub(crate) const SHIELDED_SEGMENT_EXT: &str = "zshrng02";
/// Canonical filename extension for `ZSTSUP01` segments.
pub(crate) const SUPPLEMENT_SEGMENT_EXT: &str = "zstsup01";
/// Canonical filename extension for `ZSBLKB01` segments.
pub(crate) const RAW_BLOCK_SEGMENT_EXT: &str = "zsblkb01";

/// Filename of the atomic JSON manifest enumerating pinned settled segments.
pub(crate) const SEGMENTS_MANIFEST_FILE_NAME: &str = "segments-manifest.json";
/// Schema version recorded in [`CanonicalSegmentsManifest`].
pub(crate) const CANONICAL_SEGMENTS_MANIFEST_SCHEMA_VERSION: u32 = 2;
/// Binary format version of encoded [`SettledSegmentIndexEntry`] records.
pub(crate) const SETTLED_SEGMENT_INDEX_FORMAT_V1: u16 = 1;

const INDEX_FLAG_SUPPLEMENT: u8 = 1 << 0;
const INDEX_FLAG_RAW_BODIES: u8 = 1 << 1;
const INDEX_KNOWN_FLAGS: u8 = INDEX_FLAG_SUPPLEMENT | INDEX_FLAG_RAW_BODIES;
const MAX_SEGMENTS_MANIFEST_BYTES: usize = 64 * 1024 * 1024;
const SEGMENT_HEIGHT_DIGITS: usize = 10;

/// Kind of external settled segment file within `<store_root>/segments/`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum SettledSegmentKind {
    /// Always-present `ZSHRNG02` settled shielded segment.
    Shielded,
    /// Optional `ZSTSUP01` transparent and canonical-metadata supplement segment.
    Supplement,
    /// Optional `ZSBLKB01` consensus raw-block segment.
    RawBlock,
}

impl SettledSegmentKind {
    /// Returns the subdirectory name under `segments/`.
    #[must_use]
    pub(crate) const fn subdir_name(self) -> &'static str {
        match self {
            Self::Shielded => SHIELDED_SEGMENT_DIR,
            Self::Supplement => SUPPLEMENT_SEGMENT_DIR,
            Self::RawBlock => RAW_BLOCK_SEGMENT_DIR,
        }
    }

    /// Returns the lowercase extension for this segment kind.
    #[must_use]
    pub(crate) const fn extension(self) -> &'static str {
        match self {
            Self::Shielded => SHIELDED_SEGMENT_EXT,
            Self::Supplement => SUPPLEMENT_SEGMENT_EXT,
            Self::RawBlock => RAW_BLOCK_SEGMENT_EXT,
        }
    }
}

/// Optional external settled segment modules active alongside `ZSHRNG02`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct SettledSegmentModules {
    /// Whether `ZSTSUP01` supplement segments are retained.
    pub(crate) supplement: bool,
    /// Whether `ZSBLKB01` raw-block segments are retained.
    pub(crate) raw_bodies: bool,
}

impl SettledSegmentModules {
    const fn to_flag_byte(self) -> u8 {
        let mut flags = 0_u8;
        if self.supplement {
            flags |= INDEX_FLAG_SUPPLEMENT;
        }
        if self.raw_bodies {
            flags |= INDEX_FLAG_RAW_BODIES;
        }
        flags
    }

    fn from_flag_byte(flags: u8) -> Result<Self, SettledSegmentsError> {
        if (flags & !INDEX_KNOWN_FLAGS) != 0 {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "settled segment index entry has unknown module flags {flags:#04x}"
            )));
        }
        Ok(Self {
            supplement: (flags & INDEX_FLAG_SUPPLEMENT) != 0,
            raw_bodies: (flags & INDEX_FLAG_RAW_BODIES) != 0,
        })
    }
}

/// Returns `<store_root>/segments`.
#[must_use]
pub(crate) fn segments_root_dir(store_root: &Path) -> PathBuf {
    store_root.join(SEGMENTS_DIR_NAME)
}

/// Returns `<store_root>/segments/.staging`.
#[must_use]
pub(crate) fn segment_staging_dir_path(store_root: &Path) -> PathBuf {
    segments_root_dir(store_root).join(SEGMENTS_STAGING_DIR_NAME)
}

/// Returns `<store_root>/segments/{shielded,supplement,raw_block}`.
#[must_use]
pub(crate) fn segment_dir_path(store_root: &Path, kind: SettledSegmentKind) -> PathBuf {
    segments_root_dir(store_root).join(kind.subdir_name())
}

/// Returns the aligned inclusive end height for a stride beginning at `start_height`.
///
/// Positive heights align to positive multiples of `segment_blocks` (for
/// example `[1..=4096]`, `[4097..=8192]`, or mid-stride `[3..=4]` when
/// `segment_blocks == 4`). Height `0` aligns to `[0..=0]`.
pub(crate) fn aligned_segment_end_height(
    start_height: BlockHeight,
    segment_blocks: u32,
) -> Result<BlockHeight, SettledSegmentsError> {
    if segment_blocks == 0 {
        return Err(SettledSegmentsError::InvalidLayout(
            "segment_blocks stride must be positive".to_owned(),
        ));
    }
    let start_val = start_height.value();
    if start_val == 0 {
        return Ok(BlockHeight::new(0));
    }
    let offset_in_stride = (start_val - 1) % segment_blocks;
    let remaining = segment_blocks - 1 - offset_in_stride;
    let end_val = start_val.checked_add(remaining).ok_or_else(|| {
        SettledSegmentsError::InvalidLayout("aligned segment end height exceeds u32".to_owned())
    })?;
    Ok(BlockHeight::new(end_val))
}

/// Returns `true` when `height` is an aligned settled segment boundary for `segment_blocks`.
#[must_use]
pub(crate) const fn is_aligned_segment_end_height(
    height: BlockHeight,
    segment_blocks: u32,
) -> bool {
    segment_blocks > 0 && height.value().is_multiple_of(segment_blocks)
}

/// Formats `<start:010>-<end:010>.<ext>` after validating height ordering.
pub(crate) fn format_segment_filename(
    kind: SettledSegmentKind,
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> Result<String, SettledSegmentsError> {
    if start_height > end_height {
        return Err(SettledSegmentsError::InvalidLayout(format!(
            "reversed segment range {}..={}",
            start_height.value(),
            end_height.value()
        )));
    }
    Ok(format!(
        "{:0width$}-{:0width$}.{}",
        start_height.value(),
        end_height.value(),
        kind.extension(),
        width = SEGMENT_HEIGHT_DIGITS,
    ))
}

/// Strictly parses `<start:010>-<end:010>.<ext>` for `kind`.
pub(crate) fn parse_segment_filename(
    kind: SettledSegmentKind,
    file_name: &str,
) -> Result<(BlockHeight, BlockHeight), SettledSegmentsError> {
    reject_unsafe_component(file_name)?;
    if !file_name.is_ascii() {
        return Err(SettledSegmentsError::InvalidLayout(format!(
            "non-ASCII segment filename {file_name:?}"
        )));
    }
    let ext = kind.extension();
    let expected_len = SEGMENT_HEIGHT_DIGITS + 1 + SEGMENT_HEIGHT_DIGITS + 1 + ext.len();
    if file_name.len() != expected_len {
        return Err(SettledSegmentsError::InvalidLayout(format!(
            "invalid segment filename {file_name:?}"
        )));
    }
    let bytes = file_name.as_bytes();
    let start_str = &file_name[..SEGMENT_HEIGHT_DIGITS];
    let sep = bytes[SEGMENT_HEIGHT_DIGITS];
    let end_offset = SEGMENT_HEIGHT_DIGITS + 1;
    let end_str = &file_name[end_offset..end_offset + SEGMENT_HEIGHT_DIGITS];
    let dot = bytes[end_offset + SEGMENT_HEIGHT_DIGITS];
    let found_ext = &file_name[end_offset + SEGMENT_HEIGHT_DIGITS + 1..];
    if sep != b'-'
        || dot != b'.'
        || found_ext != ext
        || !start_str.bytes().all(|byte| byte.is_ascii_digit())
        || !end_str.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(SettledSegmentsError::InvalidLayout(format!(
            "invalid segment filename syntax {file_name:?}"
        )));
    }
    let start_val = start_str.parse::<u32>().map_err(|_| {
        SettledSegmentsError::InvalidLayout(format!(
            "invalid start height in filename {file_name:?}"
        ))
    })?;
    let end_val = end_str.parse::<u32>().map_err(|_| {
        SettledSegmentsError::InvalidLayout(format!("invalid end height in filename {file_name:?}"))
    })?;
    if start_val > end_val {
        return Err(SettledSegmentsError::InvalidLayout(format!(
            "reversed segment heights in filename {file_name:?}"
        )));
    }
    Ok((BlockHeight::new(start_val), BlockHeight::new(end_val)))
}

/// Builds the full path `<store_root>/segments/<dir>/<start:010>-<end:010>.<ext>`.
pub(crate) fn segment_file_path(
    store_root: &Path,
    kind: SettledSegmentKind,
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> Result<PathBuf, SettledSegmentsError> {
    let filename = format_segment_filename(kind, start_height, end_height)?;
    Ok(segment_dir_path(store_root, kind).join(filename))
}

/// Concrete external segment file paths for one [`SettledSegmentIndexEntry`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SettledSegmentFilePaths {
    /// Always-present `ZSHRNG02` file path.
    pub(crate) shielded: PathBuf,
    /// Present when `modules.supplement` is `true`.
    pub(crate) supplement: Option<PathBuf>,
    /// Present when `modules.raw_bodies` is `true`.
    pub(crate) raw_block: Option<PathBuf>,
}

impl SettledSegmentFilePaths {
    /// Resolves and validates external file paths for `entry` under `store_root`.
    pub(crate) fn for_entry(
        store_root: &Path,
        entry: &SettledSegmentIndexEntry,
    ) -> Result<Self, SettledSegmentsError> {
        entry.validate()?;
        let start = entry.start_height;
        let end = entry.end_block.height;
        let shielded = segment_file_path(store_root, SettledSegmentKind::Shielded, start, end)?;
        let supplement = entry
            .modules
            .supplement
            .then(|| segment_file_path(store_root, SettledSegmentKind::Supplement, start, end))
            .transpose()?;
        let raw_block = entry
            .modules
            .raw_bodies
            .then(|| segment_file_path(store_root, SettledSegmentKind::RawBlock, start, end))
            .transpose()?;
        Ok(Self {
            shielded,
            supplement,
            raw_block,
        })
    }
}

/// Authenticated metadata and checkpoint pin for one aligned settled segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SettledSegmentIndexEntry {
    /// First block height covered by the segment (inclusive).
    pub(crate) start_height: BlockHeight,
    /// Last block covered by the segment (inclusive).
    pub(crate) end_block: BlockId,
    /// Enabled optional segment modules (`supplement`, `raw_bodies`).
    pub(crate) modules: SettledSegmentModules,
    /// Authenticated identity of the mandatory `ZSHRNG02` shielded file.
    pub(crate) shielded_identity: ShieldedSegmentIdentity,
    /// Authenticated identity of the optional `ZSTSUP01` supplement file.
    pub(crate) supplement_identity: Option<SupplementSegmentIdentity>,
    /// Authenticated identity of the optional `ZSBLKB01` raw-block file.
    pub(crate) raw_block_identity: Option<RawBlockSegmentIdentity>,
    /// Cumulative canonical-fact sequence checkpoint at `end_block`.
    pub(crate) end_sequence_checkpoint: CanonicalSequenceCheckpoint,
}

impl SettledSegmentIndexEntry {
    /// Validates internal span, module/identity parity, and sequence checkpoint bounds.
    pub(crate) fn validate(&self) -> Result<(), SettledSegmentsError> {
        if self.start_height > self.end_block.height {
            return Err(SettledSegmentsError::ChainLinkage(format!(
                "settled segment reversed span {}..={}",
                self.start_height.value(),
                self.end_block.height.value()
            )));
        }
        let span_blocks = self.shielded_identity.block_count()?;
        self.validate_shielded_identity()?;
        self.validate_supplement_identity()?;
        self.validate_raw_block_identity()?;
        self.validate_checkpoint(u64::from(span_blocks))?;
        Ok(())
    }

    fn validate_shielded_identity(&self) -> Result<(), SettledSegmentsError> {
        if self.shielded_identity.start_height != self.start_height
            || self.shielded_identity.end_block != self.end_block
        {
            return Err(SettledSegmentsError::ChainLinkage(
                "shielded segment identity span does not match index entry".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_supplement_identity(&self) -> Result<(), SettledSegmentsError> {
        match (self.modules.supplement, &self.supplement_identity) {
            (false, None) => Ok(()),
            (true, Some(sup_id)) => {
                sup_id.block_count()?;
                if sup_id.network != self.shielded_identity.network
                    || sup_id.artifact_schema_version
                        != self.shielded_identity.artifact_schema_version
                    || sup_id.start_height != self.start_height
                    || sup_id.end_block != self.end_block
                {
                    return Err(SettledSegmentsError::ChainLinkage(
                        "supplement segment identity does not match shielded span".to_owned(),
                    ));
                }
                Ok(())
            }
            (true, None) | (false, Some(_)) => Err(SettledSegmentsError::InvalidFormat(
                "supplement module flag does not match supplement_identity presence".to_owned(),
            )),
        }
    }

    fn validate_raw_block_identity(&self) -> Result<(), SettledSegmentsError> {
        match (self.modules.raw_bodies, &self.raw_block_identity) {
            (false, None) => Ok(()),
            (true, Some(raw_id)) => {
                raw_id.block_count()?;
                if raw_id.network != self.shielded_identity.network
                    || raw_id.start_height != self.start_height
                    || raw_id.end_block != self.end_block
                {
                    return Err(SettledSegmentsError::ChainLinkage(
                        "raw block segment identity does not match shielded span".to_owned(),
                    ));
                }
                Ok(())
            }
            (true, None) | (false, Some(_)) => Err(SettledSegmentsError::InvalidFormat(
                "raw_bodies module flag does not match raw_block_identity presence".to_owned(),
            )),
        }
    }

    fn validate_checkpoint(&self, span_blocks: u64) -> Result<(), SettledSegmentsError> {
        let checkpoint = self.end_sequence_checkpoint;
        if checkpoint.through() != self.end_block {
            return Err(SettledSegmentsError::ChainLinkage(
                "end_sequence_checkpoint block id does not match segment end_block".to_owned(),
            ));
        }
        if checkpoint.retained_block_count() < span_blocks {
            return Err(SettledSegmentsError::ChainLinkage(
                "end_sequence_checkpoint retained_block_count is smaller than segment span"
                    .to_owned(),
            ));
        }
        let digest = checkpoint.sequence_digest();
        if digest.version() != CanonicalBlockFactsSequenceDigestVersion::V1 {
            return Err(SettledSegmentsError::InvalidFormat(
                "unsupported end_sequence_checkpoint sequence digest version".to_owned(),
            ));
        }
        if digest.block_count() != checkpoint.retained_block_count() {
            return Err(SettledSegmentsError::ChainLinkage(
                "end_sequence_checkpoint sequence digest count disagrees with retained count"
                    .to_owned(),
            ));
        }
        if checkpoint.logical_replay_bytes() == 0 {
            return Err(SettledSegmentsError::ChainLinkage(
                "end_sequence_checkpoint logical_replay_bytes must be positive".to_owned(),
            ));
        }
        Ok(())
    }

    /// Encodes `self` into the strict `V1` big-endian binary representation.
    pub(crate) fn encode_v1(&self) -> Result<Vec<u8>, SettledSegmentsError> {
        self.validate()?;
        let mut out = Vec::with_capacity(512);
        out.extend_from_slice(&SETTLED_SEGMENT_INDEX_FORMAT_V1.to_be_bytes());
        out.push(self.modules.to_flag_byte());
        out.push(0);
        encode_span(&mut out, self.start_height, self.end_block);
        let shielded = VersionedIdentityParts::from_shielded(&self.shielded_identity);
        encode_versioned_identity(&mut out, &shielded);
        match &self.supplement_identity {
            Some(sup_id) => {
                out.push(1);
                let parts = VersionedIdentityParts::from_supplement(sup_id);
                encode_versioned_identity(&mut out, &parts);
            }
            None => out.push(0),
        }
        match &self.raw_block_identity {
            Some(raw_id) => {
                out.push(1);
                encode_raw_block_identity(&mut out, raw_id);
            }
            None => out.push(0),
        }
        encode_sequence_checkpoint(&mut out, self.end_sequence_checkpoint);
        Ok(out)
    }

    /// Decodes one `V1` big-endian binary record, failing closed on any anomaly.
    pub(crate) fn decode_v1(bytes: &[u8]) -> Result<Self, SettledSegmentsError> {
        let mut cursor = ByteReader::new(bytes);
        let version = cursor.read_u16_be("index.format_version")?;
        if version != SETTLED_SEGMENT_INDEX_FORMAT_V1 {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "unsupported settled segment index format version {version}"
            )));
        }
        let flag_byte = cursor.read_u8("index.module_flags")?;
        let modules = SettledSegmentModules::from_flag_byte(flag_byte)?;
        let reserved = cursor.read_u8("index.reserved")?;
        if reserved != 0 {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "non-zero reserved byte {reserved} in settled segment index entry"
            )));
        }
        let (start_height, end_block) = decode_span(&mut cursor, "index")?;
        let shielded_identity = decode_versioned_identity(&mut cursor, "shielded")?.into_shielded();
        let supplement_identity = if cursor.read_bool("index.has_supplement")? {
            Some(decode_versioned_identity(&mut cursor, "supplement")?.into_supplement())
        } else {
            None
        };
        let raw_block_identity = if cursor.read_bool("index.has_raw_block")? {
            Some(decode_raw_block_identity(&mut cursor)?)
        } else {
            None
        };
        let end_sequence_checkpoint = decode_sequence_checkpoint(&mut cursor)?;
        cursor.finish("SettledSegmentIndexEntry")?;
        let entry = Self {
            start_height,
            end_block,
            modules,
            shielded_identity,
            supplement_identity,
            raw_block_identity,
            end_sequence_checkpoint,
        };
        entry.validate()?;
        Ok(entry)
    }
}

/// Validates contiguity, span integrity, and shared network across `entries`.
pub(crate) fn validate_contiguous_index_entries(
    entries: &[SettledSegmentIndexEntry],
) -> Result<Option<Network>, SettledSegmentsError> {
    let mut prior: Option<&SettledSegmentIndexEntry> = None;
    let mut expected_network: Option<Network> = None;
    for entry in entries {
        entry.validate()?;
        let entry_network = entry.shielded_identity.network;
        match expected_network {
            Some(net) if net != entry_network => {
                return Err(SettledSegmentsError::ChainLinkage(
                    "settled segment entries mix distinct networks".to_owned(),
                ));
            }
            Some(_) => {}
            None => expected_network = Some(entry_network),
        }
        if let Some(prev_entry) = prior {
            check_entry_contiguity(prev_entry, entry)?;
        }
        prior = Some(entry);
    }
    Ok(expected_network)
}

fn check_entry_contiguity(
    prev_entry: &SettledSegmentIndexEntry,
    next_entry: &SettledSegmentIndexEntry,
) -> Result<(), SettledSegmentsError> {
    let expected_start = prev_entry.end_block.height.value().checked_add(1);
    if expected_start != Some(next_entry.start_height.value()) {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "non-contiguous segment entries: prev end {} next start {}",
            prev_entry.end_block.height.value(),
            next_entry.start_height.value()
        )));
    }
    let span_blocks = u64::from(next_entry.shielded_identity.block_count()?);
    let expected_retained = prev_entry
        .end_sequence_checkpoint
        .retained_block_count()
        .checked_add(span_blocks);
    if expected_retained != Some(next_entry.end_sequence_checkpoint.retained_block_count()) {
        return Err(SettledSegmentsError::ChainLinkage(
            "non-contiguous retained_block_count across settled segments".to_owned(),
        ));
    }
    if next_entry.end_sequence_checkpoint.logical_replay_bytes()
        <= prev_entry.end_sequence_checkpoint.logical_replay_bytes()
    {
        return Err(SettledSegmentsError::ChainLinkage(
            "logical_replay_bytes did not strictly increase across settled segments".to_owned(),
        ));
    }
    Ok(())
}

/// JSON-persisted manifest enumerating pinned external settled segments.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalSegmentsManifest {
    /// Manifest format revision (`CANONICAL_SEGMENTS_MANIFEST_SCHEMA_VERSION`).
    pub(crate) schema_version: u32,
    /// Network shared by every entry in this store.
    pub(crate) network: Network,
    /// Last block pinned in `entries`, or `None` when zero segments are sealed.
    pub(crate) sealed_tip: Option<BlockId>,
    /// Ordered, contiguous settled segment entries up through `sealed_tip`.
    pub(crate) entries: Vec<SettledSegmentIndexEntry>,
}

impl CanonicalSegmentsManifest {
    /// Builds a validated manifest from contiguous `entries` and explicit `network`.
    pub(crate) fn from_entries(
        network: Network,
        entries: Vec<SettledSegmentIndexEntry>,
    ) -> Result<Self, SettledSegmentsError> {
        let sealed_tip = entries.last().map(|last| last.end_block);
        let manifest = Self {
            schema_version: CANONICAL_SEGMENTS_MANIFEST_SCHEMA_VERSION,
            network,
            sealed_tip,
            entries,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Validates schema version, network uniformity, contiguity, and `sealed_tip`.
    pub(crate) fn validate(&self) -> Result<(), SettledSegmentsError> {
        if self.schema_version != CANONICAL_SEGMENTS_MANIFEST_SCHEMA_VERSION {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "unsupported canonical segments manifest version {}",
                self.schema_version
            )));
        }
        let entries_net = validate_contiguous_index_entries(&self.entries)?;
        if let Some(net) = entries_net
            && net != self.network
        {
            return Err(SettledSegmentsError::ChainLinkage(
                "manifest network disagrees with segment entries".to_owned(),
            ));
        }
        let expected_tip = self.entries.last().map(|last| last.end_block);
        if self.sealed_tip != expected_tip {
            return Err(SettledSegmentsError::ChainLinkage(
                "manifest sealed_tip disagrees with trailing entry end_block".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Reads and validates `<store_root>/segments-manifest.json`.
pub(crate) fn read_segments_manifest(
    store_root: &Path,
) -> Result<CanonicalSegmentsManifest, SettledSegmentsError> {
    let root_dir = VerifiedDir::open_existing(store_root, &[])?;
    let raw = root_dir.read_bounded_regular_file_nofollow(
        SEGMENTS_MANIFEST_FILE_NAME,
        MAX_SEGMENTS_MANIFEST_BYTES,
    )?;
    let parsed: ManifestJson = serde_json::from_slice(&raw)?;
    let manifest = parsed.into_domain()?;
    manifest.validate()?;
    Ok(manifest)
}

/// Atomically writes and fsyncs `<store_root>/segments-manifest.json`.
#[cfg(test)]
pub(crate) fn write_segments_manifest_atomic(
    store_root: &Path,
    manifest: &CanonicalSegmentsManifest,
) -> Result<(), SettledSegmentsError> {
    let root_dir = VerifiedDir::create_or_open(store_root, &[])?;
    write_segments_manifest_atomic_in_dir(&root_dir, manifest)
}

/// Atomically writes and fsyncs `segments-manifest.json` via a pinned [`VerifiedDir`].
pub(super) fn write_segments_manifest_atomic_in_dir(
    root_dir: &VerifiedDir,
    manifest: &CanonicalSegmentsManifest,
) -> Result<(), SettledSegmentsError> {
    write_segments_manifest_atomic_in_dir_with_limit(
        root_dir,
        manifest,
        MAX_SEGMENTS_MANIFEST_BYTES,
    )
}

pub(super) fn write_segments_manifest_atomic_in_dir_with_limit(
    root_dir: &VerifiedDir,
    manifest: &CanonicalSegmentsManifest,
    max_bytes: usize,
) -> Result<(), SettledSegmentsError> {
    manifest.validate()?;
    let dto = ManifestJson::from_domain(manifest);
    let mut bytes = serde_json::to_vec_pretty(&dto)?;
    bytes.push(b'\n');
    if bytes.len() > max_bytes {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "canonical segments manifest length {} exceeds {max_bytes}-byte limit",
            bytes.len()
        )));
    }
    root_dir.write_file_atomic(SEGMENTS_MANIFEST_FILE_NAME, &bytes)
}

#[derive(Clone, Copy)]
struct VersionedIdentityParts {
    network: Network,
    artifact_schema_version: u16,
    start_height: BlockHeight,
    end_block: BlockId,
    sha256: [u8; 32],
    index_sha256: [u8; 32],
    fs_verity_sha256: Option<[u8; 32]>,
}

impl VersionedIdentityParts {
    const fn from_shielded(id: &ShieldedSegmentIdentity) -> Self {
        Self {
            network: id.network,
            artifact_schema_version: id.artifact_schema_version,
            start_height: id.start_height,
            end_block: id.end_block,
            sha256: id.sha256,
            index_sha256: id.index_sha256,
            fs_verity_sha256: id.fs_verity_sha256,
        }
    }

    const fn from_supplement(id: &SupplementSegmentIdentity) -> Self {
        Self {
            network: id.network,
            artifact_schema_version: id.artifact_schema_version,
            start_height: id.start_height,
            end_block: id.end_block,
            sha256: id.sha256,
            index_sha256: id.index_sha256,
            fs_verity_sha256: id.fs_verity_sha256,
        }
    }

    const fn into_shielded(self) -> ShieldedSegmentIdentity {
        ShieldedSegmentIdentity {
            network: self.network,
            artifact_schema_version: self.artifact_schema_version,
            start_height: self.start_height,
            end_block: self.end_block,
            sha256: self.sha256,
            index_sha256: self.index_sha256,
            fs_verity_sha256: self.fs_verity_sha256,
        }
    }

    const fn into_supplement(self) -> SupplementSegmentIdentity {
        SupplementSegmentIdentity {
            network: self.network,
            artifact_schema_version: self.artifact_schema_version,
            start_height: self.start_height,
            end_block: self.end_block,
            sha256: self.sha256,
            index_sha256: self.index_sha256,
            fs_verity_sha256: self.fs_verity_sha256,
        }
    }
}

fn encode_span(out: &mut Vec<u8>, start_height: BlockHeight, end_block: BlockId) {
    out.extend_from_slice(&start_height.value().to_be_bytes());
    out.extend_from_slice(&end_block.height.value().to_be_bytes());
    out.extend_from_slice(&encode_internal_block_hash(end_block.hash));
}

fn decode_span(
    cursor: &mut ByteReader<'_>,
    label: &str,
) -> Result<(BlockHeight, BlockId), SettledSegmentsError> {
    let start_val = cursor.read_u32_be(label)?;
    let end_val = cursor.read_u32_be(label)?;
    let hash_bytes = cursor.read_array::<32>(label)?;
    let end_hash = decode_internal_block_hash(&hash_bytes).map_err(|err| {
        SettledSegmentsError::InvalidFormat(format!("invalid {label} block hash: {err}"))
    })?;
    Ok((
        BlockHeight::new(start_val),
        BlockId::new(BlockHeight::new(end_val), end_hash),
    ))
}

fn encode_optional_verity(out: &mut Vec<u8>, verity: Option<[u8; 32]>) {
    match verity {
        Some(digest) => {
            out.push(1);
            out.extend_from_slice(&digest);
        }
        None => out.push(0),
    }
}

fn decode_optional_verity(
    cursor: &mut ByteReader<'_>,
    label: &str,
) -> Result<Option<[u8; 32]>, SettledSegmentsError> {
    if cursor.read_bool(label)? {
        Ok(Some(cursor.read_array::<32>(label)?))
    } else {
        Ok(None)
    }
}

fn encode_versioned_identity(out: &mut Vec<u8>, id: &VersionedIdentityParts) {
    out.extend_from_slice(&id.network.id().to_be_bytes());
    out.extend_from_slice(&id.artifact_schema_version.to_be_bytes());
    encode_span(out, id.start_height, id.end_block);
    out.extend_from_slice(&id.sha256);
    out.extend_from_slice(&id.index_sha256);
    encode_optional_verity(out, id.fs_verity_sha256);
}

fn decode_versioned_identity(
    cursor: &mut ByteReader<'_>,
    label: &str,
) -> Result<VersionedIdentityParts, SettledSegmentsError> {
    let net_label = format!("{label}.network");
    let schema_label = format!("{label}.artifact_schema_version");
    let sha_label = format!("{label}.sha256");
    let idx_sha_label = format!("{label}.index_sha256");
    let verity_label = format!("{label}.fs_verity");
    let network = decode_network_id(cursor.read_u32_be(&net_label)?)?;
    let artifact_schema_version = cursor.read_u16_be(&schema_label)?;
    let (start_height, end_block) = decode_span(cursor, label)?;
    let sha256 = cursor.read_array::<32>(&sha_label)?;
    let index_sha256 = cursor.read_array::<32>(&idx_sha_label)?;
    let fs_verity_sha256 = decode_optional_verity(cursor, &verity_label)?;
    Ok(VersionedIdentityParts {
        network,
        artifact_schema_version,
        start_height,
        end_block,
        sha256,
        index_sha256,
        fs_verity_sha256,
    })
}

fn encode_raw_block_identity(out: &mut Vec<u8>, id: &RawBlockSegmentIdentity) {
    out.extend_from_slice(&id.network.id().to_be_bytes());
    out.extend_from_slice(&id.codec.id().to_be_bytes());
    encode_span(out, id.start_height, id.end_block);
    out.extend_from_slice(&id.sha256);
    out.extend_from_slice(&id.index_sha256);
    encode_optional_verity(out, id.fs_verity_sha256);
}

fn decode_raw_block_identity(
    cursor: &mut ByteReader<'_>,
) -> Result<RawBlockSegmentIdentity, SettledSegmentsError> {
    let network = decode_network_id(cursor.read_u32_be("raw_block.network")?)?;
    let codec_id = cursor.read_u16_be("raw_block.codec")?;
    let codec = RawBlockCodec::from_id(codec_id).ok_or_else(|| {
        SettledSegmentsError::InvalidFormat(format!("unsupported raw block codec id {codec_id}"))
    })?;
    let (start_height, end_block) = decode_span(cursor, "raw_block")?;
    let sha256 = cursor.read_array::<32>("raw_block.sha256")?;
    let index_sha256 = cursor.read_array::<32>("raw_block.index_sha256")?;
    let fs_verity_sha256 = decode_optional_verity(cursor, "raw_block.fs_verity")?;
    Ok(RawBlockSegmentIdentity {
        network,
        codec,
        start_height,
        end_block,
        sha256,
        index_sha256,
        fs_verity_sha256,
    })
}

fn encode_sequence_checkpoint(out: &mut Vec<u8>, checkpoint: CanonicalSequenceCheckpoint) {
    let through = checkpoint.through();
    let digest = checkpoint.sequence_digest();
    out.extend_from_slice(&through.height.value().to_be_bytes());
    out.extend_from_slice(&encode_internal_block_hash(through.hash));
    out.extend_from_slice(&checkpoint.retained_block_count().to_be_bytes());
    out.extend_from_slice(&digest.version().value().to_be_bytes());
    out.extend_from_slice(&digest.block_count().to_be_bytes());
    out.extend_from_slice(&digest.as_bytes());
    out.extend_from_slice(&checkpoint.logical_replay_bytes().to_be_bytes());
}

fn decode_sequence_checkpoint(
    cursor: &mut ByteReader<'_>,
) -> Result<CanonicalSequenceCheckpoint, SettledSegmentsError> {
    let height = cursor.read_u32_be("checkpoint.height")?;
    let hash_bytes = cursor.read_array::<32>("checkpoint.hash")?;
    let hash = decode_internal_block_hash(&hash_bytes).map_err(|err| {
        SettledSegmentsError::InvalidFormat(format!("invalid checkpoint hash: {err}"))
    })?;
    let retained_block_count = cursor.read_u64_be("checkpoint.retained_block_count")?;
    let version_u16 = cursor.read_u16_be("checkpoint.sequence_digest_version")?;
    let digest_version = CanonicalBlockFactsSequenceDigestVersion::try_from(version_u16)
        .map_err(|err| SettledSegmentsError::InvalidFormat(err.to_string()))?;
    let digest_count = cursor.read_u64_be("checkpoint.sequence_digest_block_count")?;
    let digest_bytes = cursor.read_array::<32>("checkpoint.sequence_digest")?;
    let logical_replay_bytes = cursor.read_u64_be("checkpoint.logical_replay_bytes")?;
    let digest = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
        digest_version,
        digest_count,
        digest_bytes,
    );
    Ok(CanonicalSequenceCheckpoint::from_admitted_parts(
        BlockId::new(BlockHeight::new(height), hash),
        retained_block_count,
        digest,
        logical_replay_bytes,
    ))
}

fn decode_network_id(network_id: u32) -> Result<Network, SettledSegmentsError> {
    Network::from_id(network_id).ok_or_else(|| {
        SettledSegmentsError::InvalidFormat(format!("unknown network id {network_id}"))
    })
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestJson {
    schema_version: u32,
    network: String,
    sealed_tip: Option<ManifestBlockIdJson>,
    entries: Vec<ManifestSegmentEntryJson>,
}

impl ManifestJson {
    fn from_domain(manifest: &CanonicalSegmentsManifest) -> Self {
        Self {
            schema_version: manifest.schema_version,
            network: encode_zinder_native_chain_name(manifest.network).to_owned(),
            sealed_tip: manifest.sealed_tip.map(ManifestBlockIdJson::from_domain),
            entries: manifest
                .entries
                .iter()
                .map(ManifestSegmentEntryJson::from_domain)
                .collect(),
        }
    }

    fn into_domain(self) -> Result<CanonicalSegmentsManifest, SettledSegmentsError> {
        let network = decode_native_network(&self.network)?;
        let sealed_tip = self
            .sealed_tip
            .map(ManifestBlockIdJson::into_domain)
            .transpose()?;
        let entries = self
            .entries
            .into_iter()
            .map(ManifestSegmentEntryJson::into_domain)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CanonicalSegmentsManifest {
            schema_version: self.schema_version,
            network,
            sealed_tip,
            entries,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestBlockIdJson {
    height: u32,
    hash: String,
}

impl ManifestBlockIdJson {
    fn from_domain(block_id: BlockId) -> Self {
        Self {
            height: block_id.height.value(),
            hash: encode_rpc_block_hash_hex(block_id.hash),
        }
    }

    fn into_domain(self) -> Result<BlockId, SettledSegmentsError> {
        let hash = decode_rpc_hash(&self.hash, "block_id.hash")?;
        Ok(BlockId::new(BlockHeight::new(self.height), hash))
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestModulesJson {
    supplement: bool,
    raw_bodies: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestVersionedIdentityJson {
    network: String,
    artifact_schema_version: u16,
    start_height: u32,
    end_block: ManifestBlockIdJson,
    sha256: String,
    index_sha256: String,
    fs_verity_sha256: Option<String>,
}

impl ManifestVersionedIdentityJson {
    fn from_parts(parts: &VersionedIdentityParts) -> Self {
        Self {
            network: encode_zinder_native_chain_name(parts.network).to_owned(),
            artifact_schema_version: parts.artifact_schema_version,
            start_height: parts.start_height.value(),
            end_block: ManifestBlockIdJson::from_domain(parts.end_block),
            sha256: encode_lower_hex_32(parts.sha256),
            index_sha256: encode_lower_hex_32(parts.index_sha256),
            fs_verity_sha256: parts.fs_verity_sha256.map(encode_lower_hex_32),
        }
    }

    fn into_parts(self, label: &str) -> Result<VersionedIdentityParts, SettledSegmentsError> {
        let sha_label = format!("{label}.sha256");
        let idx_sha_label = format!("{label}.index_sha256");
        let verity_label = format!("{label}.fs_verity_sha256");
        Ok(VersionedIdentityParts {
            network: decode_native_network(&self.network)?,
            artifact_schema_version: self.artifact_schema_version,
            start_height: BlockHeight::new(self.start_height),
            end_block: self.end_block.into_domain()?,
            sha256: decode_lower_hex_32(&self.sha256, &sha_label)?,
            index_sha256: decode_lower_hex_32(&self.index_sha256, &idx_sha_label)?,
            fs_verity_sha256: self
                .fs_verity_sha256
                .as_deref()
                .map(|hex| decode_lower_hex_32(hex, &verity_label))
                .transpose()?,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestRawBlockIdentityJson {
    network: String,
    codec: String,
    start_height: u32,
    end_block: ManifestBlockIdJson,
    sha256: String,
    index_sha256: String,
    fs_verity_sha256: Option<String>,
}

impl ManifestRawBlockIdentityJson {
    fn from_domain(id: &RawBlockSegmentIdentity) -> Self {
        Self {
            network: encode_zinder_native_chain_name(id.network).to_owned(),
            codec: encode_raw_codec_name(id.codec).to_owned(),
            start_height: id.start_height.value(),
            end_block: ManifestBlockIdJson::from_domain(id.end_block),
            sha256: encode_lower_hex_32(id.sha256),
            index_sha256: encode_lower_hex_32(id.index_sha256),
            fs_verity_sha256: id.fs_verity_sha256.map(encode_lower_hex_32),
        }
    }

    fn into_domain(self) -> Result<RawBlockSegmentIdentity, SettledSegmentsError> {
        let sha256 = decode_lower_hex_32(&self.sha256, "raw_block_identity.sha256")?;
        let index_sha256 =
            decode_lower_hex_32(&self.index_sha256, "raw_block_identity.index_sha256")?;
        let fs_verity_sha256 = self
            .fs_verity_sha256
            .as_deref()
            .map(|hex| decode_lower_hex_32(hex, "raw_block_identity.fs_verity_sha256"))
            .transpose()?;
        Ok(RawBlockSegmentIdentity {
            network: decode_native_network(&self.network)?,
            codec: decode_raw_codec_name(&self.codec)?,
            start_height: BlockHeight::new(self.start_height),
            end_block: self.end_block.into_domain()?,
            sha256,
            index_sha256,
            fs_verity_sha256,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestSequenceCheckpointJson {
    through: ManifestBlockIdJson,
    retained_block_count: u64,
    sequence_digest_version: u16,
    sequence_digest_block_count: u64,
    sequence_digest_sha256: String,
    logical_replay_bytes: u64,
}

impl ManifestSequenceCheckpointJson {
    fn from_domain(cp: CanonicalSequenceCheckpoint) -> Self {
        let digest = cp.sequence_digest();
        Self {
            through: ManifestBlockIdJson::from_domain(cp.through()),
            retained_block_count: cp.retained_block_count(),
            sequence_digest_version: digest.version().value(),
            sequence_digest_block_count: digest.block_count(),
            sequence_digest_sha256: encode_lower_hex_32(digest.as_bytes()),
            logical_replay_bytes: cp.logical_replay_bytes(),
        }
    }

    fn into_domain(self) -> Result<CanonicalSequenceCheckpoint, SettledSegmentsError> {
        let through = self.through.into_domain()?;
        let version =
            CanonicalBlockFactsSequenceDigestVersion::try_from(self.sequence_digest_version)
                .map_err(|err| SettledSegmentsError::InvalidFormat(err.to_string()))?;
        let digest_bytes =
            decode_lower_hex_32(&self.sequence_digest_sha256, "sequence_digest_sha256")?;
        let digest = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            version,
            self.sequence_digest_block_count,
            digest_bytes,
        );
        Ok(CanonicalSequenceCheckpoint::from_admitted_parts(
            through,
            self.retained_block_count,
            digest,
            self.logical_replay_bytes,
        ))
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestSegmentEntryJson {
    start_height: u32,
    end_block: ManifestBlockIdJson,
    modules: ManifestModulesJson,
    shielded_identity: ManifestVersionedIdentityJson,
    supplement_identity: Option<ManifestVersionedIdentityJson>,
    raw_block_identity: Option<ManifestRawBlockIdentityJson>,
    end_sequence_checkpoint: ManifestSequenceCheckpointJson,
}

impl ManifestSegmentEntryJson {
    fn from_domain(entry: &SettledSegmentIndexEntry) -> Self {
        let shielded = VersionedIdentityParts::from_shielded(&entry.shielded_identity);
        Self {
            start_height: entry.start_height.value(),
            end_block: ManifestBlockIdJson::from_domain(entry.end_block),
            modules: ManifestModulesJson {
                supplement: entry.modules.supplement,
                raw_bodies: entry.modules.raw_bodies,
            },
            shielded_identity: ManifestVersionedIdentityJson::from_parts(&shielded),
            supplement_identity: entry.supplement_identity.as_ref().map(|sup| {
                let parts = VersionedIdentityParts::from_supplement(sup);
                ManifestVersionedIdentityJson::from_parts(&parts)
            }),
            raw_block_identity: entry
                .raw_block_identity
                .as_ref()
                .map(ManifestRawBlockIdentityJson::from_domain),
            end_sequence_checkpoint: ManifestSequenceCheckpointJson::from_domain(
                entry.end_sequence_checkpoint,
            ),
        }
    }

    fn into_domain(self) -> Result<SettledSegmentIndexEntry, SettledSegmentsError> {
        let shielded_identity = self
            .shielded_identity
            .into_parts("shielded_identity")?
            .into_shielded();
        let supplement_identity = self
            .supplement_identity
            .map(|sup| sup.into_parts("supplement_identity"))
            .transpose()?
            .map(VersionedIdentityParts::into_supplement);
        Ok(SettledSegmentIndexEntry {
            start_height: BlockHeight::new(self.start_height),
            end_block: self.end_block.into_domain()?,
            modules: SettledSegmentModules {
                supplement: self.modules.supplement,
                raw_bodies: self.modules.raw_bodies,
            },
            shielded_identity,
            supplement_identity,
            raw_block_identity: self
                .raw_block_identity
                .map(ManifestRawBlockIdentityJson::into_domain)
                .transpose()?,
            end_sequence_checkpoint: self.end_sequence_checkpoint.into_domain()?,
        })
    }
}

fn decode_native_network(raw: &str) -> Result<Network, SettledSegmentsError> {
    decode_zinder_native_chain_name(raw).map_err(|err| {
        SettledSegmentsError::InvalidFormat(format!("invalid network name {raw:?}: {err}"))
    })
}

fn decode_rpc_hash(hex_str: &str, field: &str) -> Result<BlockHash, SettledSegmentsError> {
    if hex_str.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "manifest {field} must be lowercase hex"
        )));
    }
    decode_rpc_block_hash_hex(hex_str).map_err(|err| {
        SettledSegmentsError::InvalidFormat(format!("invalid manifest {field}: {err}"))
    })
}

const fn encode_raw_codec_name(codec: RawBlockCodec) -> &'static str {
    match codec {
        RawBlockCodec::None => "none",
        RawBlockCodec::Zstd => "zstd",
    }
}

fn decode_raw_codec_name(raw: &str) -> Result<RawBlockCodec, SettledSegmentsError> {
    match raw {
        "none" => Ok(RawBlockCodec::None),
        "zstd" => Ok(RawBlockCodec::Zstd),
        other => Err(SettledSegmentsError::InvalidFormat(format!(
            "unsupported raw block codec {other:?}"
        ))),
    }
}

fn encode_lower_hex_32(bytes: [u8; 32]) -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

fn decode_lower_hex_32(hex_str: &str, field: &str) -> Result<[u8; 32], SettledSegmentsError> {
    if hex_str.len() != 64 {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "expected 64 lowercase hex characters for {field}"
        )));
    }
    let mut out = [0_u8; 32];
    let bytes = hex_str.as_bytes();
    for (idx, slot) in out.iter_mut().enumerate() {
        let hi = decode_lower_nibble(bytes[idx * 2], field)?;
        let lo = decode_lower_nibble(bytes[idx * 2 + 1], field)?;
        *slot = (hi << 4) | lo;
    }
    Ok(out)
}

fn decode_lower_nibble(byte: u8, field: &str) -> Result<u8, SettledSegmentsError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(SettledSegmentsError::InvalidFormat(format!(
            "non-lowercase-hex character in {field}"
        ))),
    }
}
