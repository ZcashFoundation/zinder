//! Private backups of a complete canonical and wallet checkpoint pair.
//!
//! [`export_private_backup`](crate::recovery_archive::export_private_backup) seals the captured files and publishes the backup
//! directory after both databases pass cold admission. Retain the returned
//! manifest digest independently. [`verify_private_backup`](crate::recovery_archive::verify_private_backup) checks a backup
//! against that digest. [`restore_private_backup`](crate::recovery_archive::restore_private_backup) installs the verified pair
//! in a new directory. Keep the archive under exclusive control during each
//! operation.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs::{self, File},
    io::{self, Read, Write},
    num::NonZeroU64,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use zinder_core::Network;
use zinder_store::{
    CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION, CanonicalConstructionManifestBinding,
    RAW_BLOCK_SEGMENT_DIR, RocksDbCanonicalStore, RocksDbResourceBudget, SEGMENTS_DIR_NAME,
    SEGMENTS_STAGING_DIR_NAME, SHIELDED_SEGMENT_DIR, SUPPLEMENT_SEGMENT_DIR,
    validate_settled_segment_filename,
};
use zinder_wallet_rocksdb::WalletRecoveryAdmissionConfig;

use crate::state_bundle::{
    CANONICAL_CHECKPOINT_DIRECTORY_NAME, STATE_BUNDLE_MANIFEST_FILE_NAME, StateBundleError,
    StateBundleManifest, WALLET_CHECKPOINT_DIRECTORY_NAME,
};

mod export;
mod restore;
mod workspace;

pub use export::{export_private_backup, validate_private_backup_destination};
use workspace::{
    create_private_directory, create_private_file, publish_directory, with_private_workspace,
};

pub use restore::{restore_private_backup, verify_private_backup};

/// Exact outer recovery-artifact identity admitted by this release.
pub const RECOVERY_ARCHIVE_IDENTITY: &str = "zinder-recovery-archive";
/// Exact recovery-artifact manifest format admitted by this release.
pub const RECOVERY_ARCHIVE_FORMAT_VERSION: u16 = 1;
/// Fixed outer manifest whose presence makes an artifact eligible for admission.
pub const RECOVERY_ARCHIVE_MANIFEST_FILE_NAME: &str = "recovery-archive.json";
/// Maximum regular payload files in one recovery artifact.
pub const MAX_RECOVERY_ARCHIVE_FILE_COUNT: u64 = 50_000;
/// Maximum bytes in any one regular payload file.
pub const MAX_RECOVERY_ARCHIVE_FILE_BYTES: u64 = 64 * 1024 * 1024 * 1024;
/// Maximum aggregate payload bytes in one recovery artifact.
pub const MAX_RECOVERY_ARCHIVE_TOTAL_BYTES: u64 = 1_024 * 1024 * 1024 * 1024;
/// Maximum outer-manifest bytes decoded before allocation.
pub const MAX_RECOVERY_ARCHIVE_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;

const RECOVERY_ARCHIVE_TEMPORARY_FILE_NAME: &str = ".recovery-archive.json.incomplete";
const RECOVERY_ARCHIVE_FILE_BUFFER_BYTES: usize = 64 * 1024;

/// Resource ceilings for the local semantic scan of a wallet checkpoint.
///
/// The operator supplies these limits independently of the archive. The default
/// allows 128 MiB of memory and 192 GiB of temporary files per sorter, and
/// 512 MiB of accounted reorg undo data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryValidationBudget {
    /// Accounted memory available to each external sorter. Defaults to 128 MiB.
    pub max_sort_memory_bytes_per_sorter: NonZeroU64,
    /// Temporary file space available to each external sorter. Defaults to 192 GiB.
    pub max_temporary_file_bytes_per_sorter: NonZeroU64,
    /// Accounted memory for rebuilding the retained reorg undo history. Defaults to 512 MiB.
    pub max_accounted_reorg_undo_bytes: NonZeroU64,
}

impl Default for RecoveryValidationBudget {
    fn default() -> Self {
        Self {
            max_sort_memory_bytes_per_sorter: NonZeroU64::new(128 * 1024 * 1024)
                .unwrap_or(NonZeroU64::MIN),
            max_temporary_file_bytes_per_sorter: NonZeroU64::new(192 * 1024 * 1024 * 1024)
                .unwrap_or(NonZeroU64::MIN),
            max_accounted_reorg_undo_bytes: NonZeroU64::new(512 * 1024 * 1024)
                .unwrap_or(NonZeroU64::MIN),
        }
    }
}

impl RecoveryValidationBudget {
    fn wallet_admission(self, staging_path: &Path) -> WalletRecoveryAdmissionConfig<'_> {
        WalletRecoveryAdmissionConfig {
            staging_path,
            resource_budget: RocksDbResourceBudget::read_only_probe_defaults(),
            max_sort_memory_bytes_per_sorter: self.max_sort_memory_bytes_per_sorter.get(),
            max_temporary_file_bytes_per_sorter: self.max_temporary_file_bytes_per_sorter.get(),
            max_accounted_reorg_undo_bytes: self.max_accounted_reorg_undo_bytes.get(),
        }
    }
}

/// Immutable-at-admission descriptor for one byte-verified recovery artifact.
///
/// This descriptor proves the bytes observed by one admission call. Any later
/// restore or serving transition must retain its own exclusive filesystem
/// ownership and repeat admission before use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedRecoveryArchive {
    root: PathBuf,
    manifest: RecoveryArchiveManifest,
}

impl AdmittedRecoveryArchive {
    /// Canonicalized, configured-root-derived artifact directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Opaque candidate identifier, never a caller-supplied filesystem path.
    #[must_use]
    pub fn candidate_id(&self) -> &str {
        &self.manifest.candidate_id
    }

    /// Fully validated outer manifest that sealed this artifact.
    #[must_use]
    pub const fn manifest(&self) -> &RecoveryArchiveManifest {
        &self.manifest
    }
}

/// Serializable outer recovery-artifact manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryArchiveManifest {
    identity: String,
    format_version: u16,
    candidate_id: String,
    network: String,
    source_state_bundle_manifest_sha256: String,
    canonical_construction_manifest_version: u16,
    canonical_construction_manifest_sha256: String,
    canonical_checkpoint_database_identity_sha256: String,
    wallet_checkpoint_database_identity_sha256: String,
    payload_file_count: u64,
    payload_byte_length: u64,
    payload_sha256: String,
    payload_files: Vec<RecoveryArchiveFile>,
}

impl RecoveryArchiveManifest {
    fn from_state_bundle(
        state_bundle: &StateBundleManifest,
        state_bundle_manifest_sha256: String,
        payload_files: Vec<RecoveryArchiveFile>,
    ) -> Result<Self, RecoveryArchiveError> {
        let payload_file_count = u64::try_from(payload_files.len()).map_err(|_| {
            RecoveryArchiveError::contract("recovery payload file count does not fit in u64")
        })?;
        let payload_byte_length = payload_files.iter().try_fold(0_u64, |total, file| {
            total.checked_add(file.byte_length).ok_or_else(|| {
                RecoveryArchiveError::contract("recovery payload byte length overflows u64")
            })
        })?;
        let manifest = Self {
            identity: RECOVERY_ARCHIVE_IDENTITY.to_owned(),
            format_version: RECOVERY_ARCHIVE_FORMAT_VERSION,
            candidate_id: state_bundle.candidate_id().to_owned(),
            network: zinder_core::wire::encode_zinder_native_chain_name(state_bundle.network()?)
                .to_owned(),
            source_state_bundle_manifest_sha256: state_bundle_manifest_sha256,
            canonical_construction_manifest_version: state_bundle
                .canonical_construction_manifest_version(),
            canonical_construction_manifest_sha256: state_bundle
                .canonical_construction_manifest_sha256()
                .to_owned(),
            canonical_checkpoint_database_identity_sha256: state_bundle
                .canonical_checkpoint_database_identity_sha256()
                .to_owned(),
            wallet_checkpoint_database_identity_sha256: state_bundle
                .wallet_checkpoint_database_identity_sha256()
                .to_owned(),
            payload_file_count,
            payload_byte_length,
            payload_sha256: payload_sha256(&payload_files),
            payload_files,
        };
        manifest.validate_shape()?;
        Ok(manifest)
    }

    fn validate_shape(&self) -> Result<(), RecoveryArchiveError> {
        require_contract(
            self.identity == RECOVERY_ARCHIVE_IDENTITY,
            "recovery archive identity must be exactly zinder-recovery-archive",
        )?;
        require_contract(
            self.format_version == RECOVERY_ARCHIVE_FORMAT_VERSION,
            "recovery archive format version must be exactly 1",
        )?;
        validate_candidate_id(&self.candidate_id)?;
        parse_network(&self.network).ok_or_else(|| {
            RecoveryArchiveError::contract(
                "recovery archive network is not an exact supported spelling",
            )
        })?;
        validate_lower_hex_32(
            &self.source_state_bundle_manifest_sha256,
            "source state-bundle manifest SHA-256",
        )?;
        require_contract(
            self.canonical_construction_manifest_version
                == CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION,
            "canonical construction-manifest version is unsupported",
        )?;
        validate_lower_hex_32(
            &self.canonical_construction_manifest_sha256,
            "canonical construction-manifest SHA-256",
        )?;
        validate_lower_hex_32(
            &self.canonical_checkpoint_database_identity_sha256,
            "canonical checkpoint database identity SHA-256",
        )?;
        validate_lower_hex_32(
            &self.wallet_checkpoint_database_identity_sha256,
            "wallet checkpoint database identity SHA-256",
        )?;
        require_contract(
            self.payload_file_count
                == u64::try_from(self.payload_files.len()).map_err(|_| {
                    RecoveryArchiveError::contract(
                        "recovery payload file count does not fit in u64",
                    )
                })?,
            "recovery payload file count does not match its file inventory",
        )?;
        require_contract(
            self.payload_file_count <= MAX_RECOVERY_ARCHIVE_FILE_COUNT,
            "recovery payload file count exceeds the fixed limit",
        )?;
        let mut paths = BTreeSet::new();
        let total = self.payload_files.iter().try_fold(0_u64, |total, file| {
            file.validate()?;
            require_contract(
                paths.insert(file.path.as_str()),
                "recovery payload contains a duplicate path",
            )?;
            total.checked_add(file.byte_length).ok_or_else(|| {
                RecoveryArchiveError::contract("recovery payload byte length overflows u64")
            })
        })?;
        require_contract(
            total == self.payload_byte_length,
            "recovery payload byte length does not match its file inventory",
        )?;
        require_contract(
            total <= MAX_RECOVERY_ARCHIVE_TOTAL_BYTES,
            "recovery payload byte length exceeds the fixed limit",
        )?;
        validate_lower_hex_32(&self.payload_sha256, "recovery payload SHA-256")?;
        require_contract(
            self.payload_sha256 == payload_sha256(&self.payload_files),
            "recovery payload SHA-256 does not match its file inventory",
        )
    }
}

/// One fixed-layout regular payload file committed by the outer manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryArchiveFile {
    path: String,
    byte_length: u64,
    sha256: String,
}

impl RecoveryArchiveFile {
    fn validate(&self) -> Result<(), RecoveryArchiveError> {
        validate_payload_path(&self.path)?;
        require_contract(
            self.byte_length <= MAX_RECOVERY_ARCHIVE_FILE_BYTES,
            "recovery payload file exceeds the fixed per-file byte limit",
        )?;
        validate_lower_hex_32(&self.sha256, "recovery payload file SHA-256")
    }
}

/// Reconstructs the configured-root-derived artifact path and verifies every
/// payload byte before a later restore or serving state may use it.
pub fn admit_recovery_archive(
    configured_archive_root: impl AsRef<Path>,
    candidate_id: &str,
    expected_network: Network,
) -> Result<AdmittedRecoveryArchive, RecoveryArchiveError> {
    validate_candidate_id(candidate_id)?;
    let archive_root = resolve_existing_directory(
        configured_archive_root.as_ref(),
        "configured recovery archive root",
    )?;
    let root = archive_root.join(candidate_id);
    let (manifest, _) = admit_archive_contents(&root, expected_network, Some(candidate_id), None)?;
    let root = fs::canonicalize(&root).map_err(|source| RecoveryArchiveError::io(&root, source))?;
    Ok(AdmittedRecoveryArchive { root, manifest })
}

fn admit_archive_contents(
    root: &Path,
    expected_network: Network,
    candidate_id: Option<&str>,
    expected_sha256: Option<&[u8; 32]>,
) -> Result<(RecoveryArchiveManifest, StateBundleManifest), RecoveryArchiveError> {
    let (manifest, _) =
        read_archive_manifest(root, candidate_id, expected_network, expected_sha256)?;
    let state_bundle = admit_archive_bindings(root, &manifest, expected_network)?;
    let observed_files = collect_payload_files(root, PayloadLayout::SealedArchive)?;
    require_contract(
        observed_files == manifest.payload_files,
        "recovery archive payload bytes do not match the outer manifest",
    )?;
    Ok((manifest, state_bundle))
}

/// Admits the inner state-bundle manifest and its bindings to the outer
/// manifest by reading only bounded manifest files.
///
/// It runs before any payload walk or copy, so an incompatible backup, such as
/// one from an earlier wallet schema, is refused before it costs payload I/O or
/// temporary space. The payload check that follows covers the same files.
fn admit_archive_bindings(
    root: &Path,
    manifest: &RecoveryArchiveManifest,
    expected_network: Network,
) -> Result<StateBundleManifest, RecoveryArchiveError> {
    let state_bundle = admit_recovery_archive_state_bundle(root, manifest, expected_network)?;
    let construction_binding = RocksDbCanonicalStore::read_construction_manifest_binding(
        root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME),
    )
    .map_err(RecoveryArchiveError::ConstructionManifest)?;
    require_construction_manifest_binding(&state_bundle, construction_binding)?;
    require_contract(
        state_bundle.canonical_checkpoint_database_identity_sha256()
            == manifest.canonical_checkpoint_database_identity_sha256,
        "recovery archive canonical checkpoint identity does not match the inner manifest",
    )?;
    require_contract(
        state_bundle.wallet_checkpoint_database_identity_sha256()
            == manifest.wallet_checkpoint_database_identity_sha256,
        "recovery archive wallet checkpoint identity does not match the inner manifest",
    )?;
    Ok(state_bundle)
}

fn read_archive_manifest(
    root: &Path,
    candidate_id: Option<&str>,
    expected_network: Network,
    expected_sha256: Option<&[u8; 32]>,
) -> Result<(RecoveryArchiveManifest, Vec<u8>), RecoveryArchiveError> {
    require_directory(root, "recovery archive candidate")?;
    let manifest_path = root.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME);
    require_regular_file(&manifest_path, "recovery archive manifest")?;
    require_not_hard_link(&manifest_path, "recovery archive manifest")?;
    let encoded = read_bounded_regular_file(
        &manifest_path,
        "recovery archive manifest",
        MAX_RECOVERY_ARCHIVE_MANIFEST_BYTES,
    )?;
    if let Some(expected) = expected_sha256 {
        require_contract(
            <[u8; 32]>::from(Sha256::digest(&encoded)) == *expected,
            "recovery archive outer manifest SHA-256 differs from the trusted digest",
        )?;
    }
    let manifest: RecoveryArchiveManifest = serde_json::from_slice(&encoded).map_err(|source| {
        RecoveryArchiveError::ManifestDecode {
            path: manifest_path.clone(),
            source,
        }
    })?;
    manifest.validate_shape()?;
    require_contract(
        candidate_id.is_none_or(|expected| manifest.candidate_id == expected),
        "recovery archive candidate does not match its outer manifest",
    )?;
    require_contract(
        parse_network(&manifest.network) == Some(expected_network),
        "recovery archive network does not match",
    )?;
    Ok((manifest, encoded))
}

fn admit_recovery_archive_state_bundle(
    root: &Path,
    manifest: &RecoveryArchiveManifest,
    expected_network: Network,
) -> Result<StateBundleManifest, RecoveryArchiveError> {
    let (state_bundle, digest) = StateBundleManifest::admit_manifest(
        root,
        expected_network,
        &[RECOVERY_ARCHIVE_MANIFEST_FILE_NAME],
    )?;
    require_contract(
        hex::encode(digest) == manifest.source_state_bundle_manifest_sha256,
        "recovery archive inner state-bundle manifest SHA-256 does not match",
    )?;
    require_contract(
        state_bundle.candidate_id() == manifest.candidate_id,
        "recovery archive inner state-bundle candidate does not match",
    )?;
    require_contract(
        state_bundle.canonical_construction_manifest_version()
            == manifest.canonical_construction_manifest_version,
        "recovery archive construction-manifest version does not match the inner manifest",
    )?;
    require_contract(
        state_bundle.canonical_construction_manifest_sha256()
            == manifest.canonical_construction_manifest_sha256,
        "recovery archive construction-manifest SHA-256 does not match the inner manifest",
    )?;
    Ok(state_bundle)
}

/// Recovery packaging or byte-admission failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RecoveryArchiveError {
    /// A configured or archive-contained path was unsafe.
    #[error("unsafe {purpose} path {path}: {reason}")]
    UnsafePath {
        /// Rejected path.
        path: PathBuf,
        /// Operational role of the path.
        purpose: &'static str,
        /// Stable rejection reason.
        reason: &'static str,
    },
    /// The derived candidate target already exists and was preserved.
    #[error("recovery archive target must be absent: {path}")]
    TargetExists {
        /// Existing target path.
        path: PathBuf,
    },
    /// An artifact tree has an entry outside the fixed layout.
    #[error("recovery archive directory {path} has unexpected entry {entry:?}")]
    UnexpectedEntry {
        /// Directory containing the unexpected entry.
        path: PathBuf,
        /// Rejected entry name.
        entry: OsString,
    },
    /// A manifest or exact byte invariant was rejected.
    #[error("recovery archive contract rejected: {reason}")]
    Contract {
        /// Exact fail-closed rejection reason.
        reason: String,
    },
    /// An outer manifest is not exact format-1 JSON.
    #[error("recovery archive manifest {path} is not valid format-1 JSON: {source}")]
    ManifestDecode {
        /// Manifest path.
        path: PathBuf,
        /// JSON decoding failure.
        #[source]
        source: serde_json::Error,
    },
    /// A source state-bundle was not admitted.
    #[error("state-bundle admission failed: {source}")]
    StateBundle {
        /// Underlying state-bundle rejection.
        #[source]
        source: StateBundleError,
    },
    /// The canonical construction-manifest sidecar was absent or invalid.
    #[error("canonical construction-manifest admission failed: {0}")]
    ConstructionManifest(#[source] zinder_store::CanonicalStoreError),
    /// A filesystem operation failed.
    #[error("recovery archive filesystem operation failed at {path}: {source}")]
    Io {
        /// Path involved in the operation.
        path: PathBuf,
        /// Concrete filesystem failure.
        #[source]
        source: io::Error,
    },
}

impl From<StateBundleError> for RecoveryArchiveError {
    fn from(source: StateBundleError) -> Self {
        Self::StateBundle { source }
    }
}

impl RecoveryArchiveError {
    fn io(path: &Path, source: io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }

    fn contract(reason: impl Into<String>) -> Self {
        Self::Contract {
            reason: reason.into(),
        }
    }
}

#[derive(Default)]
struct PayloadBudget<'inventory> {
    expected: Option<BTreeMap<&'inventory str, &'inventory RecoveryArchiveFile>>,
    files: Vec<RecoveryArchiveFile>,
    byte_length: u64,
}

impl PayloadBudget<'_> {
    fn reserve_file(&mut self, byte_length: u64) -> Result<(), RecoveryArchiveError> {
        let file_count = u64::try_from(self.files.len()).map_err(|_| {
            RecoveryArchiveError::contract("recovery payload file count does not fit in u64")
        })?;
        require_contract(
            file_count < MAX_RECOVERY_ARCHIVE_FILE_COUNT,
            "recovery payload file count exceeds the fixed limit",
        )?;
        require_contract(
            byte_length <= MAX_RECOVERY_ARCHIVE_FILE_BYTES,
            "recovery payload file exceeds the fixed per-file byte limit",
        )?;
        let total = self.byte_length.checked_add(byte_length).ok_or_else(|| {
            RecoveryArchiveError::contract("recovery payload byte length overflows u64")
        })?;
        require_contract(
            total <= MAX_RECOVERY_ARCHIVE_TOTAL_BYTES,
            "recovery payload byte length exceeds the fixed limit",
        )?;
        self.byte_length = total;
        Ok(())
    }
}

fn copy_archive_payload(
    source: &Path,
    target: &Path,
    expected: Option<&[RecoveryArchiveFile]>,
) -> Result<Vec<RecoveryArchiveFile>, RecoveryArchiveError> {
    validate_archive_root_entries(source, expected.is_some())?;
    let mut budget = PayloadBudget {
        expected: expected.map(|files| {
            files
                .iter()
                .map(|file| (file.path.as_str(), file))
                .collect()
        }),
        ..PayloadBudget::default()
    };
    copy_regular_file(
        &source.join(STATE_BUNDLE_MANIFEST_FILE_NAME),
        &target.join(STATE_BUNDLE_MANIFEST_FILE_NAME),
        Path::new(STATE_BUNDLE_MANIFEST_FILE_NAME),
        &mut budget,
    )?;
    for directory in [
        CANONICAL_CHECKPOINT_DIRECTORY_NAME,
        WALLET_CHECKPOINT_DIRECTORY_NAME,
    ] {
        copy_directory(
            &source.join(directory),
            &target.join(directory),
            Path::new(directory),
            &mut budget,
        )?;
    }
    sync_directory(target)?;
    budget
        .files
        .sort_by(|left, right| left.path.cmp(&right.path));
    Ok(budget.files)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum DirectoryRole<'a> {
    WalletRoot,
    CanonicalRoot,
    SegmentsRoot,
    SegmentSubdir(&'a str),
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum DirectoryEntryAction {
    RegularFile,
    Subdirectory,
}

fn relative_components(relative: &Path) -> Result<Vec<&str>, RecoveryArchiveError> {
    let mut parts = Vec::new();
    for component in relative.components() {
        let Component::Normal(segment) = component else {
            return Err(RecoveryArchiveError::contract(
                "recovery payload path is not a normalized relative path",
            ));
        };
        let text = segment.to_str().ok_or_else(|| {
            RecoveryArchiveError::contract("recovery archive paths must be valid UTF-8")
        })?;
        require_safe_component_str(text)?;
        parts.push(text);
    }
    Ok(parts)
}

fn classify_directory_role<'a>(
    parts: &'a [&'a str],
) -> Result<DirectoryRole<'a>, RecoveryArchiveError> {
    match parts {
        [WALLET_CHECKPOINT_DIRECTORY_NAME] => Ok(DirectoryRole::WalletRoot),
        [CANONICAL_CHECKPOINT_DIRECTORY_NAME] => Ok(DirectoryRole::CanonicalRoot),
        [CANONICAL_CHECKPOINT_DIRECTORY_NAME, SEGMENTS_DIR_NAME] => Ok(DirectoryRole::SegmentsRoot),
        [
            CANONICAL_CHECKPOINT_DIRECTORY_NAME,
            SEGMENTS_DIR_NAME,
            subdir,
        ] if is_known_segment_subdir(subdir) => Ok(DirectoryRole::SegmentSubdir(subdir)),
        _ => Err(RecoveryArchiveError::contract(
            "checkpoint directory is outside the fixed recovery archive layout",
        )),
    }
}

fn admit_directory_entry(
    role: DirectoryRole<'_>,
    entry_path: &Path,
    name: &str,
    metadata: &fs::Metadata,
) -> Result<DirectoryEntryAction, RecoveryArchiveError> {
    if metadata.file_type().is_symlink() {
        return Err(RecoveryArchiveError::UnsafePath {
            path: entry_path.to_path_buf(),
            purpose: "recovery payload entry",
            reason: "symlinks are forbidden in recovery payloads",
        });
    }
    match role {
        DirectoryRole::WalletRoot => admit_wallet_entry(entry_path, metadata),
        DirectoryRole::CanonicalRoot => admit_canonical_root_entry(entry_path, name, metadata),
        DirectoryRole::SegmentsRoot => admit_segments_root_entry(entry_path, name, metadata),
        DirectoryRole::SegmentSubdir(subdir) => {
            admit_segment_subdir_entry(subdir, entry_path, name, metadata)
        }
    }
}

fn admit_wallet_entry(
    entry_path: &Path,
    metadata: &fs::Metadata,
) -> Result<DirectoryEntryAction, RecoveryArchiveError> {
    if metadata.is_file() {
        return Ok(DirectoryEntryAction::RegularFile);
    }
    Err(RecoveryArchiveError::UnsafePath {
        path: entry_path.to_path_buf(),
        purpose: "recovery payload entry",
        reason: "wallet checkpoint directories must contain only flat regular files",
    })
}

fn admit_canonical_root_entry(
    entry_path: &Path,
    name: &str,
    metadata: &fs::Metadata,
) -> Result<DirectoryEntryAction, RecoveryArchiveError> {
    if metadata.is_file() {
        if name == SEGMENTS_DIR_NAME {
            return Err(RecoveryArchiveError::UnsafePath {
                path: entry_path.to_path_buf(),
                purpose: "recovery payload entry",
                reason: "canonical.rocksdb/segments must be a directory, not a regular file",
            });
        }
        return Ok(DirectoryEntryAction::RegularFile);
    }
    if metadata.is_dir() && name == SEGMENTS_DIR_NAME {
        return Ok(DirectoryEntryAction::Subdirectory);
    }
    Err(RecoveryArchiveError::UnsafePath {
        path: entry_path.to_path_buf(),
        purpose: "recovery payload entry",
        reason: "canonical checkpoint permits only top-level regular files and segments/",
    })
}

fn admit_segments_root_entry(
    entry_path: &Path,
    name: &str,
    metadata: &fs::Metadata,
) -> Result<DirectoryEntryAction, RecoveryArchiveError> {
    if !metadata.is_dir() {
        return Err(RecoveryArchiveError::UnsafePath {
            path: entry_path.to_path_buf(),
            purpose: "recovery payload entry",
            reason: "segments/ must contain only settled segment subdirectories",
        });
    }
    if name == SEGMENTS_STAGING_DIR_NAME {
        return Err(RecoveryArchiveError::UnsafePath {
            path: entry_path.to_path_buf(),
            purpose: "recovery payload entry",
            reason: "segments/.staging is forbidden in recovery payloads",
        });
    }
    if is_known_segment_subdir(name) {
        return Ok(DirectoryEntryAction::Subdirectory);
    }
    Err(RecoveryArchiveError::UnsafePath {
        path: entry_path.to_path_buf(),
        purpose: "recovery payload entry",
        reason: "segments/ permits only shielded, supplement, and raw_block subdirectories",
    })
}

fn admit_segment_subdir_entry(
    subdir: &str,
    entry_path: &Path,
    name: &str,
    metadata: &fs::Metadata,
) -> Result<DirectoryEntryAction, RecoveryArchiveError> {
    if !metadata.is_file() {
        return Err(RecoveryArchiveError::UnsafePath {
            path: entry_path.to_path_buf(),
            purpose: "recovery payload entry",
            reason: "settled segment subdirectories must contain only regular segment files",
        });
    }
    validate_segment_filename(subdir, name)?;
    Ok(DirectoryEntryAction::RegularFile)
}

fn visit_payload_directory(
    directory: &Path,
    relative: &Path,
    purpose: &'static str,
    mut on_entry: impl FnMut(
        &Path,
        &OsString,
        &Path,
        DirectoryEntryAction,
    ) -> Result<(), RecoveryArchiveError>,
) -> Result<(), RecoveryArchiveError> {
    let parts = relative_components(relative)?;
    let role = classify_directory_role(&parts)?;
    verify_directory_nofollow(directory, purpose)?;
    let entries =
        fs::read_dir(directory).map_err(|source| RecoveryArchiveError::io(directory, source))?;
    for entry in entries {
        let entry = entry.map_err(|source| RecoveryArchiveError::io(directory, source))?;
        let name = entry.file_name();
        let name_str = validate_component_name(&name)?;
        let entry_path = entry.path();
        let nested_relative = relative.join(&name);
        let metadata = fs::symlink_metadata(&entry_path)
            .map_err(|source| RecoveryArchiveError::io(&entry_path, source))?;
        let action = admit_directory_entry(role, &entry_path, name_str, &metadata)?;
        on_entry(&entry_path, &name, &nested_relative, action)?;
    }
    Ok(())
}

fn copy_directory(
    source: &Path,
    target: &Path,
    relative: &Path,
    budget: &mut PayloadBudget<'_>,
) -> Result<(), RecoveryArchiveError> {
    verify_directory_nofollow(source, "state-bundle payload directory")?;
    let parts = relative_components(relative)?;
    let _ = classify_directory_role(&parts)?;
    create_private_directory(target)?;
    visit_payload_directory(
        source,
        relative,
        "state-bundle payload directory",
        |source_path, name, relative_path, action| {
            let target_path = target.join(name);
            match action {
                DirectoryEntryAction::RegularFile => {
                    copy_regular_file(source_path, &target_path, relative_path, budget)
                }
                DirectoryEntryAction::Subdirectory => {
                    copy_directory(source_path, &target_path, relative_path, budget)
                }
            }
        },
    )?;
    sync_directory(target)
}

fn copy_regular_file(
    source: &Path,
    target: &Path,
    relative: &Path,
    budget: &mut PayloadBudget<'_>,
) -> Result<(), RecoveryArchiveError> {
    let relative = normalized_relative_path(relative)?;
    require_regular_file(source, "state-bundle payload file")?;
    let metadata = fs::symlink_metadata(source)
        .map_err(|source_error| RecoveryArchiveError::io(source, source_error))?;
    budget.reserve_file(metadata.len())?;
    let mut input = open_regular_file_nofollow(source, "state-bundle payload file")?;
    let input_metadata = input
        .metadata()
        .map_err(|source_error| RecoveryArchiveError::io(source, source_error))?;
    require_contract(
        input_metadata.is_file() && input_metadata.len() == metadata.len(),
        "state-bundle payload file changed while recovery packaging opened it",
    )?;
    let expected = budget
        .expected
        .as_ref()
        .map(|inventory| {
            inventory.get(relative.as_str()).copied().ok_or_else(|| {
                RecoveryArchiveError::contract(format!(
                    "file absent from pinned inventory: {relative}"
                ))
            })
        })
        .transpose()?;
    if let Some(expected) = expected {
        require_not_hard_link_metadata(source, &input_metadata, "sealed recovery payload file")?;
        require_contract(
            expected.byte_length == metadata.len(),
            "backup file size differs from pinned inventory",
        )?;
    }
    let (byte_length, sha256) = stream_file_copy(source, &mut input, target, metadata.len())?;
    let file = RecoveryArchiveFile {
        path: relative,
        byte_length,
        sha256,
    };
    file.validate()?;
    require_contract(
        expected.is_none_or(|pinned| pinned == &file),
        "backup file digest differs from pinned inventory",
    )?;
    budget.files.push(file);
    Ok(())
}

fn stream_file_copy(
    source: &Path,
    input: &mut File,
    target: &Path,
    expected_len: u64,
) -> Result<(u64, String), RecoveryArchiveError> {
    let mut output = create_private_file(target)?;
    let mut digest = Sha256::new();
    let mut byte_length = 0_u64;
    let mut buffer = vec![0_u8; RECOVERY_ARCHIVE_FILE_BUFFER_BYTES];
    loop {
        let read = input
            .read(&mut buffer)
            .map_err(|source_error| RecoveryArchiveError::io(source, source_error))?;
        if read == 0 {
            break;
        }
        let next_length =
            byte_length
                .checked_add(u64::try_from(read).map_err(|_| {
                    RecoveryArchiveError::contract("read length does not fit in u64")
                })?)
                .ok_or_else(|| {
                    RecoveryArchiveError::contract("recovery payload byte length overflows u64")
                })?;
        require_contract(
            next_length <= expected_len && next_length <= MAX_RECOVERY_ARCHIVE_FILE_BYTES,
            "state-bundle payload file changed or exceeds the fixed byte limit while copying",
        )?;
        output
            .write_all(&buffer[..read])
            .map_err(|source_error| RecoveryArchiveError::io(target, source_error))?;
        digest.update(&buffer[..read]);
        byte_length = next_length;
    }
    require_contract(
        byte_length == expected_len,
        "state-bundle payload file changed while recovery packaging copied it",
    )?;
    output
        .sync_all()
        .map_err(|source_error| RecoveryArchiveError::io(target, source_error))?;
    Ok((byte_length, hex::encode(digest.finalize())))
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum PayloadLayout {
    OwnerCapture,
    SealedArchive,
}

fn collect_payload_files(
    root: &Path,
    layout: PayloadLayout,
) -> Result<Vec<RecoveryArchiveFile>, RecoveryArchiveError> {
    validate_archive_root_entries(root, layout == PayloadLayout::SealedArchive)?;
    let mut budget = PayloadBudget::default();
    collect_regular_file(
        &root.join(STATE_BUNDLE_MANIFEST_FILE_NAME),
        Path::new(STATE_BUNDLE_MANIFEST_FILE_NAME),
        &mut budget,
        layout,
    )?;
    collect_directory(
        &root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME),
        Path::new(CANONICAL_CHECKPOINT_DIRECTORY_NAME),
        &mut budget,
        layout,
    )?;
    collect_directory(
        &root.join(WALLET_CHECKPOINT_DIRECTORY_NAME),
        Path::new(WALLET_CHECKPOINT_DIRECTORY_NAME),
        &mut budget,
        layout,
    )?;
    budget
        .files
        .sort_by(|left, right| left.path.cmp(&right.path));
    Ok(budget.files)
}

fn collect_directory(
    directory: &Path,
    relative: &Path,
    budget: &mut PayloadBudget<'_>,
    layout: PayloadLayout,
) -> Result<(), RecoveryArchiveError> {
    visit_payload_directory(
        directory,
        relative,
        "recovery payload directory",
        |path, _name, nested_relative, action| match action {
            DirectoryEntryAction::RegularFile => {
                collect_regular_file(path, nested_relative, budget, layout)
            }
            DirectoryEntryAction::Subdirectory => {
                collect_directory(path, nested_relative, budget, layout)
            }
        },
    )
}

fn collect_regular_file(
    path: &Path,
    relative: &Path,
    budget: &mut PayloadBudget<'_>,
    layout: PayloadLayout,
) -> Result<(), RecoveryArchiveError> {
    let relative = normalized_relative_path(relative)?;
    require_regular_file(path, "recovery payload file")?;
    let metadata =
        fs::symlink_metadata(path).map_err(|source| RecoveryArchiveError::io(path, source))?;
    budget.reserve_file(metadata.len())?;
    let mut file = open_regular_file_nofollow(path, "recovery payload file")?;
    let opened = file
        .metadata()
        .map_err(|source| RecoveryArchiveError::io(path, source))?;
    require_contract(
        opened.is_file() && opened.len() == metadata.len(),
        "recovery payload file changed while admission opened it",
    )?;
    if layout != PayloadLayout::OwnerCapture {
        require_not_hard_link_metadata(path, &opened, "recovery payload file")?;
    }
    let mut digest = Sha256::new();
    let mut byte_length = 0_u64;
    let mut buffer = vec![0_u8; RECOVERY_ARCHIVE_FILE_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| RecoveryArchiveError::io(path, source))?;
        if read == 0 {
            break;
        }
        byte_length =
            byte_length
                .checked_add(u64::try_from(read).map_err(|_| {
                    RecoveryArchiveError::contract("read length does not fit in u64")
                })?)
                .ok_or_else(|| {
                    RecoveryArchiveError::contract("recovery payload byte length overflows u64")
                })?;
        require_contract(
            byte_length <= metadata.len() && byte_length <= MAX_RECOVERY_ARCHIVE_FILE_BYTES,
            "recovery payload file changed or exceeds the fixed byte limit while admission read it",
        )?;
        digest.update(&buffer[..read]);
    }
    require_contract(
        byte_length == metadata.len(),
        "recovery payload file changed while admission read it",
    )?;
    budget.files.push(RecoveryArchiveFile {
        path: relative,
        byte_length,
        sha256: hex::encode(digest.finalize()),
    });
    Ok(())
}

fn sync_canonical_directory(directory: &Path, relative: &Path) -> Result<(), RecoveryArchiveError> {
    visit_payload_directory(
        directory,
        relative,
        "restored canonical store directory",
        |path, _name, nested_relative, action| match action {
            DirectoryEntryAction::RegularFile => {
                open_regular_file_nofollow(path, "restored canonical store file")?
                    .sync_all()
                    .map_err(|source| RecoveryArchiveError::io(path, source))
            }
            DirectoryEntryAction::Subdirectory => sync_canonical_directory(path, nested_relative),
        },
    )?;
    sync_directory(directory)
}

fn validate_archive_root_entries(root: &Path, published: bool) -> Result<(), RecoveryArchiveError> {
    require_directory(root, "recovery archive candidate")?;
    let mut expected = vec![
        CANONICAL_CHECKPOINT_DIRECTORY_NAME,
        WALLET_CHECKPOINT_DIRECTORY_NAME,
        STATE_BUNDLE_MANIFEST_FILE_NAME,
    ];
    if published {
        expected.push(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME);
    }
    let entries = fs::read_dir(root).map_err(|source| RecoveryArchiveError::io(root, source))?;
    let mut observed = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|source| RecoveryArchiveError::io(root, source))?;
        let name = entry.file_name();
        validate_component_name(&name)?;
        if !expected.iter().any(|expected_name| name == *expected_name) {
            return Err(RecoveryArchiveError::UnexpectedEntry {
                path: root.to_path_buf(),
                entry: name,
            });
        }
        require_contract(
            observed.insert(name),
            "recovery archive contains duplicate root entries",
        )?;
    }
    require_contract(
        observed.len() == expected.len(),
        "recovery archive is missing a fixed root entry",
    )
}

fn publish_manifest_last(
    root: &Path,
    manifest: &RecoveryArchiveManifest,
) -> Result<[u8; 32], RecoveryArchiveError> {
    validate_unpublished_root_entries(root)?;
    let manifest_path = root.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME);
    let temporary_path = root.join(RECOVERY_ARCHIVE_TEMPORARY_FILE_NAME);
    let mut encoded = serde_json::to_vec_pretty(manifest).map_err(|source| {
        RecoveryArchiveError::ManifestDecode {
            path: manifest_path.clone(),
            source,
        }
    })?;
    encoded.push(b'\n');
    require_contract(
        u64::try_from(encoded.len()).is_ok_and(|len| len <= MAX_RECOVERY_ARCHIVE_MANIFEST_BYTES),
        "recovery archive manifest exceeds its byte limit",
    )?;
    let mut temporary = create_private_file(&temporary_path)?;
    temporary
        .write_all(&encoded)
        .and_then(|()| temporary.sync_all())
        .map_err(|source| RecoveryArchiveError::io(&temporary_path, source))?;
    drop(temporary);
    fs::hard_link(&temporary_path, &manifest_path)
        .map_err(|source| RecoveryArchiveError::io(&manifest_path, source))?;
    fs::remove_file(&temporary_path)
        .map_err(|source| RecoveryArchiveError::io(&temporary_path, source))?;
    sync_directory(root)?;
    Ok(Sha256::digest(&encoded).into())
}

fn validate_unpublished_root_entries(root: &Path) -> Result<(), RecoveryArchiveError> {
    require_directory(root, "unpublished recovery archive candidate")?;
    let expected = [
        CANONICAL_CHECKPOINT_DIRECTORY_NAME,
        WALLET_CHECKPOINT_DIRECTORY_NAME,
        STATE_BUNDLE_MANIFEST_FILE_NAME,
    ];
    let entries = fs::read_dir(root).map_err(|source| RecoveryArchiveError::io(root, source))?;
    let mut observed = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|source| RecoveryArchiveError::io(root, source))?;
        let name = entry.file_name();
        validate_component_name(&name)?;
        if !expected.iter().any(|expected_name| name == *expected_name) {
            return Err(RecoveryArchiveError::UnexpectedEntry {
                path: root.to_path_buf(),
                entry: name,
            });
        }
        require_contract(
            observed.insert(name),
            "unpublished recovery archive contains duplicate root entries",
        )?;
    }
    require_contract(
        observed.len() == expected.len(),
        "unpublished recovery archive is missing a fixed root entry",
    )
}

fn resolve_existing_directory(
    path: &Path,
    purpose: &'static str,
) -> Result<PathBuf, RecoveryArchiveError> {
    validate_absolute_lexical_path(path, purpose)?;
    require_directory(path, purpose)?;
    let resolved =
        fs::canonicalize(path).map_err(|source| RecoveryArchiveError::io(path, source))?;
    require_directory(&resolved, purpose)?;
    Ok(resolved)
}

fn validate_absolute_lexical_path(
    path: &Path,
    purpose: &'static str,
) -> Result<(), RecoveryArchiveError> {
    if !path.is_absolute() {
        return Err(RecoveryArchiveError::UnsafePath {
            path: path.to_path_buf(),
            purpose,
            reason: "path must be absolute",
        });
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::CurDir | Component::ParentDir | Component::Prefix(_)
        )
    }) {
        return Err(RecoveryArchiveError::UnsafePath {
            path: path.to_path_buf(),
            purpose,
            reason: "path must not contain traversal or platform prefixes",
        });
    }
    Ok(())
}

fn require_directory(path: &Path, purpose: &'static str) -> Result<(), RecoveryArchiveError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| RecoveryArchiveError::io(path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RecoveryArchiveError::UnsafePath {
            path: path.to_path_buf(),
            purpose,
            reason: "path must be a real directory, not a symlink or another file type",
        });
    }
    Ok(())
}

fn require_regular_file(path: &Path, purpose: &'static str) -> Result<(), RecoveryArchiveError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| RecoveryArchiveError::io(path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(RecoveryArchiveError::UnsafePath {
            path: path.to_path_buf(),
            purpose,
            reason: "path must be a regular file, not a symlink or another file type",
        });
    }
    Ok(())
}

fn require_absent(path: &Path, _purpose: &'static str) -> Result<(), RecoveryArchiveError> {
    match fs::symlink_metadata(path) {
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(RecoveryArchiveError::TargetExists {
            path: path.to_path_buf(),
        }),
        Err(source) => Err(RecoveryArchiveError::io(path, source)),
    }
}

#[cfg(unix)]
fn require_not_hard_link(path: &Path, purpose: &'static str) -> Result<(), RecoveryArchiveError> {
    let metadata = fs::metadata(path).map_err(|source| RecoveryArchiveError::io(path, source))?;
    require_not_hard_link_metadata(path, &metadata, purpose)
}

#[cfg(not(unix))]
fn require_not_hard_link(path: &Path, purpose: &'static str) -> Result<(), RecoveryArchiveError> {
    Err(RecoveryArchiveError::UnsafePath {
        path: path.to_path_buf(),
        purpose,
        reason: "hard-link verification requires a Unix filesystem",
    })
}

#[cfg(unix)]
fn require_not_hard_link_metadata(
    path: &Path,
    metadata: &fs::Metadata,
    purpose: &'static str,
) -> Result<(), RecoveryArchiveError> {
    use std::os::unix::fs::MetadataExt;

    if metadata.nlink() != 1 {
        return Err(RecoveryArchiveError::UnsafePath {
            path: path.to_path_buf(),
            purpose,
            reason: "hard-linked files are forbidden in recovery payloads",
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_not_hard_link_metadata(
    path: &Path,
    _metadata: &fs::Metadata,
    purpose: &'static str,
) -> Result<(), RecoveryArchiveError> {
    Err(RecoveryArchiveError::UnsafePath {
        path: path.to_path_buf(),
        purpose,
        reason: "hard-link verification requires a Unix filesystem",
    })
}

fn verify_directory_nofollow(
    path: &Path,
    purpose: &'static str,
) -> Result<(), RecoveryArchiveError> {
    require_directory(path, purpose)?;
    #[cfg(unix)]
    {
        use rustix::fs::{Mode, OFlags, open};

        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let _ = open(path, flags, Mode::empty()).map_err(|errno| {
            if errno == rustix::io::Errno::LOOP || errno == rustix::io::Errno::NOTDIR {
                RecoveryArchiveError::UnsafePath {
                    path: path.to_path_buf(),
                    purpose,
                    reason: "path must be a real directory, not a symlink or another file type",
                }
            } else {
                RecoveryArchiveError::io(path, errno.into())
            }
        })?;
    }
    Ok(())
}

fn open_regular_file_nofollow(
    path: &Path,
    purpose: &'static str,
) -> Result<File, RecoveryArchiveError> {
    require_regular_file(path, purpose)?;
    #[cfg(unix)]
    let file = {
        use rustix::fs::{Mode, OFlags, open};

        let flags = OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let fd = open(path, flags, Mode::empty()).map_err(|errno| {
            if errno == rustix::io::Errno::LOOP {
                RecoveryArchiveError::UnsafePath {
                    path: path.to_path_buf(),
                    purpose,
                    reason: "path must be a regular file, not a symlink or another file type",
                }
            } else {
                RecoveryArchiveError::io(path, errno.into())
            }
        })?;
        File::from(fd)
    };
    #[cfg(not(unix))]
    let file = File::open(path).map_err(|source| RecoveryArchiveError::io(path, source))?;
    Ok(file)
}

fn validate_component_name(name: &OsString) -> Result<&str, RecoveryArchiveError> {
    let component = name.to_str().ok_or_else(|| {
        RecoveryArchiveError::contract("recovery archive paths must be valid UTF-8")
    })?;
    require_safe_component_str(component)?;
    Ok(component)
}

fn require_safe_component_str(component: &str) -> Result<(), RecoveryArchiveError> {
    require_contract(
        !component.is_empty()
            && component != "."
            && component != ".."
            && !component.contains('/')
            && !component.contains('\\')
            && !component.contains('\0'),
        "recovery archive path component is not a safe single name",
    )
}

fn is_known_segment_subdir(subdir: &str) -> bool {
    matches!(
        subdir,
        SHIELDED_SEGMENT_DIR | SUPPLEMENT_SEGMENT_DIR | RAW_BLOCK_SEGMENT_DIR
    )
}

fn validate_segment_filename(subdir: &str, file_name: &str) -> Result<(), RecoveryArchiveError> {
    if !is_known_segment_subdir(subdir) {
        return Err(RecoveryArchiveError::contract(format!(
            "unexpected segments subdirectory {subdir:?} in recovery payload"
        )));
    }
    if !validate_settled_segment_filename(subdir, file_name) {
        return Err(RecoveryArchiveError::contract(format!(
            "invalid settled segment filename or extension {file_name:?}"
        )));
    }
    Ok(())
}

fn validate_candidate_id(candidate_id: &str) -> Result<(), RecoveryArchiveError> {
    let bytes = candidate_id.as_bytes();
    let valid_length = (1..=64).contains(&bytes.len());
    let valid_edges = bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric);
    let valid_body = bytes
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-');
    require_contract(
        valid_length && valid_edges && valid_body,
        "candidate id must be 1-64 lowercase ASCII letters, digits, or hyphens and begin and end with an alphanumeric character",
    )
}

fn validate_payload_path(path: &str) -> Result<(), RecoveryArchiveError> {
    let parsed = Path::new(path);
    require_contract(
        !path.is_empty()
            && !path.starts_with('/')
            && !path.ends_with('/')
            && !path.contains('\\')
            && !path.contains('\0')
            && !parsed.is_absolute()
            && parsed
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
        "recovery payload path must be a normalized relative path",
    )?;
    let parts: Vec<&str> = path.split('/').collect();
    require_contract(
        parts
            .iter()
            .all(|part| !part.is_empty() && *part != "." && *part != ".."),
        "recovery payload path must be a normalized relative path",
    )?;
    match parts.as_slice() {
        [STATE_BUNDLE_MANIFEST_FILE_NAME] | [WALLET_CHECKPOINT_DIRECTORY_NAME, _] => Ok(()),
        [CANONICAL_CHECKPOINT_DIRECTORY_NAME, file_name] => require_contract(
            *file_name != SEGMENTS_DIR_NAME,
            "canonical.rocksdb/segments must be a directory, not a regular file",
        ),
        [
            CANONICAL_CHECKPOINT_DIRECTORY_NAME,
            SEGMENTS_DIR_NAME,
            subdir,
            file_name,
        ] => validate_segment_filename(subdir, file_name),
        _ => Err(RecoveryArchiveError::contract(
            "recovery payload path is outside the fixed archive layout",
        )),
    }
}

fn normalized_relative_path(path: &Path) -> Result<String, RecoveryArchiveError> {
    let mut names = Vec::new();
    for component in path.components() {
        let Component::Normal(name) = component else {
            return Err(RecoveryArchiveError::contract(
                "recovery payload path is not a normalized relative path",
            ));
        };
        names.push(name.to_str().ok_or_else(|| {
            RecoveryArchiveError::contract("recovery archive paths must be valid UTF-8")
        })?);
    }
    let normalized = names.join("/");
    validate_payload_path(&normalized)?;
    Ok(normalized)
}

fn payload_sha256(files: &[RecoveryArchiveFile]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"zinder-recovery-archive-payload-v1\0");
    for file in files {
        digest.update(file.path.as_bytes());
        digest.update([0]);
        digest.update(file.byte_length.to_be_bytes());
        digest.update(file.sha256.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn validate_lower_hex_32(encoded: &str, field: &str) -> Result<(), RecoveryArchiveError> {
    let mut bytes = [0_u8; 32];
    hex::decode_to_slice(encoded, &mut bytes).map_err(|_| {
        RecoveryArchiveError::contract(format!("{field} must be exactly 32-byte hexadecimal"))
    })?;
    require_contract(
        hex::encode(bytes) == encoded,
        format!("{field} must use canonical lowercase hexadecimal"),
    )
}

fn parse_network(name: &str) -> Option<Network> {
    zinder_core::wire::decode_zinder_native_chain_name(name).ok()
}

fn require_construction_manifest_binding(
    state_bundle: &StateBundleManifest,
    observed: CanonicalConstructionManifestBinding,
) -> Result<(), RecoveryArchiveError> {
    require_contract(
        observed.version == state_bundle.canonical_construction_manifest_version(),
        "canonical construction-manifest version does not match the inner state-bundle manifest",
    )?;
    require_contract(
        hex::encode(observed.sha256) == state_bundle.canonical_construction_manifest_sha256(),
        "canonical construction-manifest SHA-256 does not match the inner state-bundle manifest",
    )
}

fn require_contract(
    condition: bool,
    reason: impl Into<String>,
) -> Result<(), RecoveryArchiveError> {
    if condition {
        Ok(())
    } else {
        Err(RecoveryArchiveError::contract(reason))
    }
}

fn sync_directory(path: &Path) -> Result<(), RecoveryArchiveError> {
    require_directory(path, "recovery directory")?;
    #[cfg(unix)]
    let directory = {
        use rustix::fs::{Mode, OFlags, open};

        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let fd = open(path, flags, Mode::empty())
            .map_err(|errno| RecoveryArchiveError::io(path, errno.into()))?;
        File::from(fd)
    };
    #[cfg(not(unix))]
    let directory = File::open(path).map_err(|source| RecoveryArchiveError::io(path, source))?;
    directory
        .sync_all()
        .map_err(|source| RecoveryArchiveError::io(path, source))
}

fn read_bounded_regular_file(
    path: &Path,
    purpose: &'static str,
    maximum_bytes: u64,
) -> Result<Vec<u8>, RecoveryArchiveError> {
    require_regular_file(path, purpose)?;
    let metadata =
        fs::symlink_metadata(path).map_err(|source| RecoveryArchiveError::io(path, source))?;
    require_contract(
        metadata.len() <= maximum_bytes,
        "recovery manifest exceeds its fixed byte limit",
    )?;
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| RecoveryArchiveError::contract("recovery manifest length exceeds usize"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| RecoveryArchiveError::contract("recovery manifest allocation failed"))?;
    let file = open_regular_file_nofollow(path, purpose)?;
    let opened = file
        .metadata()
        .map_err(|source| RecoveryArchiveError::io(path, source))?;
    require_contract(
        opened.is_file() && opened.len() <= maximum_bytes,
        "recovery manifest changed while admission opened it",
    )?;
    let mut bounded = file.take(maximum_bytes.saturating_add(1));
    bounded
        .read_to_end(&mut bytes)
        .map_err(|source| RecoveryArchiveError::io(path, source))?;
    require_contract(
        u64::try_from(bytes.len()).map_err(|_| {
            RecoveryArchiveError::contract("recovery manifest length does not fit in u64")
        })? <= maximum_bytes,
        "recovery manifest exceeds its fixed byte limit",
    )?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;
    use zinder_core::Network;

    use super::*;

    #[test]
    fn restore_checks_pinned_file_sizes_before_allocating_the_destination_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let source = fixed_archive_root(&temporary)?;
        let relative = Path::new(CANONICAL_CHECKPOINT_DIRECTORY_NAME).join("000001.sst");
        fs::write(source.join(&relative), b"expected")?;
        let inventory = collect_payload_files(&source, PayloadLayout::SealedArchive)?;
        fs::write(source.join(&relative), b"larger than the pinned size")?;
        let target = temporary.path().join("inactive");
        fs::create_dir(&target)?;

        assert!(copy_archive_payload(&source, &target, Some(&inventory)).is_err());
        assert!(!target.join(relative).exists());
        Ok(())
    }

    #[test]
    fn payload_collection_rejects_nested_checkpoint_directories()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let root = fixed_archive_root(&temporary)?;
        let nested = root
            .join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
            .join("nested");
        fs::create_dir(&nested)?;
        fs::write(nested.join("payload"), b"unexpected")?;

        assert!(collect_payload_files(&root, PayloadLayout::SealedArchive).is_err());
        Ok(())
    }

    #[test]
    fn packaging_rejects_nested_source_checkpoint_directories()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let source = temporary.path().join("source");
        let target = temporary.path().join("target");
        fs::create_dir(&source)?;
        fs::create_dir(source.join("nested"))?;
        let mut budget = PayloadBudget::default();

        assert!(
            copy_directory(
                &source,
                &target,
                Path::new("canonical.rocksdb"),
                &mut budget
            )
            .is_err()
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn packaging_copies_hard_linked_source_files_into_unlinked_payload_files()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::MetadataExt;

        let temporary = TempDir::new()?;
        let source = temporary.path().join("source");
        let target = temporary.path().join("target");
        fs::create_dir(&source)?;
        let first = source.join("000001.sst");
        fs::write(&first, b"canonical")?;
        fs::hard_link(&first, source.join("000002.sst"))?;
        let mut budget = PayloadBudget::default();

        copy_directory(
            &source,
            &target,
            Path::new(CANONICAL_CHECKPOINT_DIRECTORY_NAME),
            &mut budget,
        )?;
        assert_eq!(fs::metadata(target.join("000001.sst"))?.nlink(), 1);
        assert_eq!(fs::metadata(target.join("000002.sst"))?.nlink(), 1);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn payload_collection_rejects_hard_linked_archive_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let root = fixed_archive_root(&temporary)?;
        let canonical = root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME);
        let source = canonical.join("000001.sst");
        fs::write(&source, b"canonical")?;
        fs::hard_link(&source, canonical.join("000002.sst"))?;

        assert!(collect_payload_files(&root, PayloadLayout::SealedArchive).is_err());
        Ok(())
    }

    #[test]
    fn admission_bounds_outer_manifest_bytes_before_json_decode()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let root = temporary.path().join("archives");
        let candidate = root.join("bundle-a");
        fs::create_dir(&root)?;
        fs::create_dir(&candidate)?;
        let oversized = usize::try_from(MAX_RECOVERY_ARCHIVE_MANIFEST_BYTES)?
            .checked_add(1)
            .ok_or("test manifest length overflow")?;
        fs::write(
            candidate.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME),
            vec![b'x'; oversized],
        )?;

        assert!(admit_recovery_archive(&root, "bundle-a", Network::ZcashRegtest).is_err());
        Ok(())
    }

    /// Writes an authentic backup of an unsupported wallet schema whose payload is unreadable.
    ///
    /// The inner state-bundle manifest records `unsupported_schema`, and the payload
    /// lists a file the backup does not hold. Returns the trusted outer SHA-256.
    fn unsupported_schema_backup_with_unreadable_payload(
        archive: &Path,
        unsupported_schema: u16,
    ) -> Result<[u8; 32], Box<dyn std::error::Error>> {
        fs::create_dir(archive)?;
        fs::create_dir(archive.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME))?;
        fs::create_dir(archive.join(WALLET_CHECKPOINT_DIRECTORY_NAME))?;
        let state_bundle = crate::state_bundle::tests::sample_manifest()?;
        let mut inner = serde_json::to_value(&state_bundle)?;
        *inner
            .pointer_mut("/wallet_checkpoint/schema_version")
            .ok_or("inner manifest has a wallet schema")? =
            serde_json::Value::from(unsupported_schema);
        if unsupported_schema == 1 {
            inner
                .pointer_mut("/wallet_checkpoint/row_counts")
                .and_then(serde_json::Value::as_object_mut)
                .ok_or("inner manifest has wallet row_counts")?
                .remove("block_hash_by_height");
        }
        let inner = serde_json::to_vec(&inner)?;
        fs::write(archive.join(STATE_BUNDLE_MANIFEST_FILE_NAME), &inner)?;
        let missing = RecoveryArchiveFile {
            path: format!("{WALLET_CHECKPOINT_DIRECTORY_NAME}/000001.sst"),
            byte_length: MAX_RECOVERY_ARCHIVE_FILE_BYTES,
            sha256: "00".repeat(32),
        };
        let outer = RecoveryArchiveManifest::from_state_bundle(
            &state_bundle,
            hex::encode(Sha256::digest(&inner)),
            vec![missing],
        )?;
        let outer = serde_json::to_vec(&outer)?;
        fs::write(archive.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME), &outer)?;
        Ok(Sha256::digest(&outer).into())
    }

    fn require_unsupported_schema_refusal(error: &RecoveryArchiveError, unsupported_schema: u16) {
        let message = error.to_string();
        assert!(
            matches!(
                error,
                RecoveryArchiveError::StateBundle {
                    source: StateBundleError::ManifestContract { .. }
                }
            ),
            "{message}"
        );
        assert!(
            message.contains(&format!("wallet schema version {unsupported_schema}")),
            "{message}"
        );
        assert!(message.contains("rebuild the wallet store"), "{message}");
    }

    #[test]
    fn restore_refuses_an_unsupported_schema_backup_before_copying_its_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        for unsupported_schema in [1_u16, 3_u16] {
            let temporary = TempDir::new()?;
            let archive = temporary.path().join("backup");
            let expected =
                unsupported_schema_backup_with_unreadable_payload(&archive, unsupported_schema)?;
            let destination = temporary.path().join("restored");

            let error = restore_private_backup(
                &archive,
                &destination,
                Network::ZcashRegtest,
                &expected,
                RecoveryValidationBudget::default(),
            )
            .err()
            .ok_or("an unsupported-schema backup was restored")?;

            require_unsupported_schema_refusal(&error, unsupported_schema);
            assert!(!destination.exists());
            let mut entries = fs::read_dir(temporary.path())?
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<Result<Vec<_>, _>>()?;
            entries.sort();
            assert_eq!(entries, ["backup"], "restore left a workspace behind");
        }
        Ok(())
    }

    #[test]
    fn verification_refuses_an_unsupported_schema_backup_before_walking_its_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        for unsupported_schema in [1_u16, 3_u16] {
            let temporary = TempDir::new()?;
            let archive = temporary.path().join("backup");
            let expected =
                unsupported_schema_backup_with_unreadable_payload(&archive, unsupported_schema)?;

            let error = verify_private_backup(
                &archive,
                Network::ZcashRegtest,
                &expected,
                RecoveryValidationBudget::default(),
            )
            .err()
            .ok_or("an unsupported-schema backup was verified")?;

            require_unsupported_schema_refusal(&error, unsupported_schema);
        }
        Ok(())
    }

    fn fixed_archive_root(temporary: &TempDir) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let root = temporary.path().join("archive");
        fs::create_dir(&root)?;
        fs::create_dir(root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME))?;
        fs::create_dir(root.join(WALLET_CHECKPOINT_DIRECTORY_NAME))?;
        fs::write(root.join(STATE_BUNDLE_MANIFEST_FILE_NAME), b"inner")?;
        fs::write(root.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME), b"outer")?;
        Ok(root)
    }

    #[test]
    fn recovery_archive_constants_cover_multi_year_mainnet_segments() {
        // At 4,096 blocks/segment and 3 segment files per stride, 50,000 files covers
        // >16,000 strides (>65M blocks) with RocksDB files, well beyond multi-year mainnet.
        const {
            assert!(MAX_RECOVERY_ARCHIVE_FILE_COUNT >= 50_000);
            assert!(MAX_RECOVERY_ARCHIVE_FILE_BYTES >= 64 * 1024 * 1024 * 1024);
            assert!(MAX_RECOVERY_ARCHIVE_TOTAL_BYTES >= 1_024 * 1024 * 1024 * 1024);
            assert!(MAX_RECOVERY_ARCHIVE_MANIFEST_BYTES >= 8 * 1024 * 1024);
        }
    }

    #[test]
    fn validate_payload_path_enforces_nested_segments_and_rejects_malformed_paths() {
        let version = CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION;
        let construction_manifest_path = format!(
            "{CANONICAL_CHECKPOINT_DIRECTORY_NAME}/canonical-construction-manifest.v{version}.json"
        );
        let segments_manifest_path = format!(
            "{CANONICAL_CHECKPOINT_DIRECTORY_NAME}/{}",
            zinder_store::SEGMENTS_MANIFEST_FILE_NAME
        );
        let valid_paths = [
            "state-bundle.json",
            "wallet.rocksdb/CURRENT",
            "wallet.rocksdb/000001.sst",
            "canonical.rocksdb/CURRENT",
            construction_manifest_path.as_str(),
            segments_manifest_path.as_str(),
            "canonical.rocksdb/segments/shielded/0000000000-0000000000.zshrng02",
            "canonical.rocksdb/segments/shielded/0000000001-0000004096.zshrng02",
            "canonical.rocksdb/segments/supplement/0000000001-0000004096.zstsup01",
            "canonical.rocksdb/segments/raw_block/0000000001-0000004096.zsblkb01",
            "canonical.rocksdb/segments/raw_block/4294967295-4294967295.zsblkb01",
        ];
        for path in valid_paths {
            assert!(validate_payload_path(path).is_ok(), "rejected valid {path}");
        }

        let invalid_paths = [
            "",
            "recovery-archive.json",
            "canonical.rocksdb",
            "canonical.rocksdb/segments",
            "canonical.rocksdb/other/000001.sst",
            "canonical.rocksdb/segments/0000000001-0000004096.zshrng02",
            "canonical.rocksdb/segments/.staging/0000000001-0000004096.zshrng02",
            "canonical.rocksdb/segments/other/0000000001-0000004096.zshrng02",
            "canonical.rocksdb/segments/shielded/nested/0000000001-0000004096.zshrng02",
            "canonical.rocksdb/segments/shielded/0000000001-0000004096.zstsup01",
            "canonical.rocksdb/segments/shielded/0000000001-0000004096.zsblkb01",
            "canonical.rocksdb/segments/supplement/0000000001-0000004096.zshrng02",
            "canonical.rocksdb/segments/supplement/0000000001-0000004096.zsblkb01",
            "canonical.rocksdb/segments/raw_block/0000000001-0000004096.zshrng02",
            "canonical.rocksdb/segments/raw_block/0000000001-0000004096.zstsup01",
            "canonical.rocksdb/segments/shielded/1-4096.zshrng02",
            "canonical.rocksdb/segments/shielded/00000000001-0000004096.zshrng02",
            "canonical.rocksdb/segments/shielded/0000004096-0000000001.zshrng02",
            "canonical.rocksdb/segments/shielded/4294967296-4294967296.zshrng02",
            "canonical.rocksdb/segments/shielded/000000000a-0000004096.zshrng02",
            "canonical.rocksdb/segments/shielded/0000000001_0000004096.zshrng02",
            "canonical.rocksdb/segments/shielded/0000000001-0000004096.ZSHRNG02",
            "canonical.rocksdb/segments/shielded/0000000001-0000004096",
            "wallet.rocksdb/nested/000001.sst",
            "wallet.rocksdb/segments/shielded/0000000001-0000004096.zshrng02",
            "canonical.rocksdb//CURRENT",
            "canonical.rocksdb/./CURRENT",
            "canonical.rocksdb/CURRENT/",
            "canonical.rocksdb\\CURRENT",
        ];
        for path in invalid_paths {
            assert!(
                validate_payload_path(path).is_err(),
                "admitted invalid {path}"
            );
        }
    }

    fn write_nested_segments_bundle(root: &Path) -> Result<[u8; 32], Box<dyn std::error::Error>> {
        let canonical = root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME);
        let wallet = root.join(WALLET_CHECKPOINT_DIRECTORY_NAME);
        let segments = canonical.join(SEGMENTS_DIR_NAME);
        let shielded = segments.join(SHIELDED_SEGMENT_DIR);
        let supplement = segments.join(SUPPLEMENT_SEGMENT_DIR);
        let raw_block = segments.join(RAW_BLOCK_SEGMENT_DIR);
        fs::create_dir_all(&shielded)?;
        fs::create_dir_all(&supplement)?;
        fs::create_dir_all(&raw_block)?;
        fs::create_dir(&wallet)?;
        fs::write(canonical.join("CURRENT"), b"MANIFEST-000001\n")?;
        let version = CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION;
        let construction_manifest_name = format!("canonical-construction-manifest.v{version}.json");
        fs::write(canonical.join(construction_manifest_name), b"{}")?;
        fs::write(
            canonical.join(zinder_store::SEGMENTS_MANIFEST_FILE_NAME),
            b"{}",
        )?;
        fs::write(shielded.join("0000000001-0000004096.zshrng02"), b"shielded")?;
        fs::write(
            supplement.join("0000000001-0000004096.zstsup01"),
            b"supplement",
        )?;
        fs::write(
            raw_block.join("0000000001-0000004096.zsblkb01"),
            b"raw_block",
        )?;
        fs::write(wallet.join("CURRENT"), b"MANIFEST-000002\n")?;
        let manifest = crate::state_bundle::tests::sample_manifest()?;
        let encoded = serde_json::to_vec_pretty(&manifest)?;
        let digest: [u8; 32] = Sha256::digest(&encoded).into();
        fs::write(root.join(STATE_BUNDLE_MANIFEST_FILE_NAME), encoded)?;
        Ok(digest)
    }

    #[test]
    fn nested_settled_segments_round_trip_export_verify_and_restore()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let source = temporary.path().join("source");
        fs::create_dir(&source)?;
        let bundle_digest = write_nested_segments_bundle(&source)?;

        #[cfg(unix)]
        {
            let shielded_file = source
                .join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                .join(SEGMENTS_DIR_NAME)
                .join(SHIELDED_SEGMENT_DIR)
                .join("0000000001-0000004096.zshrng02");
            fs::hard_link(&shielded_file, temporary.path().join("live-segment-link"))?;
        }

        let archives = temporary.path().join("archives");
        fs::create_dir(&archives)?;
        let archive = archives.join("bundle-a");
        create_private_directory(&archive)?;
        let copied = copy_archive_payload(&source, &archive, None)?;
        let (state_bundle, inner_digest) =
            StateBundleManifest::read_with_sha256(&archive, Network::ZcashRegtest)?;
        assert_eq!(inner_digest, bundle_digest);
        let outer = RecoveryArchiveManifest::from_state_bundle(
            &state_bundle,
            hex::encode(inner_digest),
            copied,
        )?;
        let outer_digest = publish_manifest_last(&archive, &outer)?;
        let (verified_outer, _) = read_archive_manifest(
            &archive,
            Some("bundle-a"),
            Network::ZcashRegtest,
            Some(&outer_digest),
        )?;
        assert_eq!(verified_outer, outer);
        admit_recovery_archive_state_bundle(&archive, &verified_outer, Network::ZcashRegtest)?;
        let observed_sealed = collect_payload_files(&archive, PayloadLayout::SealedArchive)?;
        assert_eq!(observed_sealed, verified_outer.payload_files);
        assert_sealed_and_restore_nested_segments(
            temporary.path(),
            &archive,
            &verified_outer,
            &outer_digest,
        )
    }

    fn assert_sealed_and_restore_nested_segments(
        scratch: &Path,
        archive: &Path,
        manifest: &RecoveryArchiveManifest,
        outer_digest: &[u8; 32],
    ) -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let sealed_seg = archive
                .join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                .join(SEGMENTS_DIR_NAME)
                .join(SHIELDED_SEGMENT_DIR)
                .join("0000000001-0000004096.zshrng02");
            assert_eq!(fs::metadata(sealed_seg)?.nlink(), 1);
        }
        let pair = scratch.join("pair");
        create_private_directory(&pair)?;
        let restored_files = copy_archive_payload(archive, &pair, Some(&manifest.payload_files))?;
        assert_eq!(restored_files, manifest.payload_files);
        fs::copy(
            archive.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME),
            pair.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME),
        )?;
        let (restored_outer, _) = read_archive_manifest(
            &pair,
            Some("bundle-a"),
            Network::ZcashRegtest,
            Some(outer_digest),
        )?;
        assert_eq!(&restored_outer, manifest);
        let (_, restored_inner_digest) = StateBundleManifest::admit_manifest(
            &pair,
            Network::ZcashRegtest,
            &[RECOVERY_ARCHIVE_MANIFEST_FILE_NAME],
        )?;
        assert_eq!(
            hex::encode(restored_inner_digest),
            manifest.source_state_bundle_manifest_sha256
        );
        admit_recovery_archive_state_bundle(&pair, manifest, Network::ZcashRegtest)?;
        let observed_restored = collect_payload_files(&pair, PayloadLayout::SealedArchive)?;
        assert_eq!(observed_restored, manifest.payload_files);
        sync_canonical_directory(
            &pair.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME),
            Path::new(CANONICAL_CHECKPOINT_DIRECTORY_NAME),
        )?;
        restore::prepare_deployment_directory(&pair)?;
        let deployed = scratch.join("deployed");
        publish_directory(&pair, &deployed)?;

        let deployed_segments = deployed.join("canonical").join("segments");
        assert_eq!(
            fs::read(deployed_segments.join("shielded/0000000001-0000004096.zshrng02"))?,
            b"shielded"
        );
        assert_eq!(
            fs::read(deployed_segments.join("supplement/0000000001-0000004096.zstsup01"))?,
            b"supplement"
        );
        assert_eq!(
            fs::read(deployed_segments.join("raw_block/0000000001-0000004096.zsblkb01"))?,
            b"raw_block"
        );
        let segments_manifest = deployed
            .join("canonical")
            .join(zinder_store::SEGMENTS_MANIFEST_FILE_NAME);
        assert_eq!(fs::read(segments_manifest)?, b"{}");
        assert_eq!(
            fs::read(deployed.join("wallet/CURRENT"))?,
            b"MANIFEST-000002\n"
        );
        Ok(())
    }

    #[test]
    fn nested_segments_reject_staging_wrong_ext_symlinks_hardlinks_and_wallet_subdirs()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_disallowed_tree_rejected(|root| {
            fs::create_dir_all(
                root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                    .join(SEGMENTS_DIR_NAME)
                    .join(SEGMENTS_STAGING_DIR_NAME),
            )?;
            Ok(())
        })?;
        assert_disallowed_tree_rejected(|root| {
            let staging = root
                .join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                .join(SEGMENTS_DIR_NAME)
                .join(SEGMENTS_STAGING_DIR_NAME);
            fs::create_dir_all(&staging)?;
            fs::write(staging.join("staged.tmp"), b"staged")?;
            Ok(())
        })?;
        assert_disallowed_tree_rejected(|root| {
            fs::create_dir_all(
                root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                    .join(SEGMENTS_DIR_NAME)
                    .join("unexpected"),
            )?;
            Ok(())
        })?;
        assert_disallowed_tree_rejected(|root| {
            let segments = root
                .join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                .join(SEGMENTS_DIR_NAME);
            fs::create_dir_all(&segments)?;
            fs::write(segments.join("0000000001-0000004096.zshrng02"), b"bad")?;
            Ok(())
        })?;
        assert_disallowed_tree_rejected(|root| {
            let shielded = root
                .join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                .join(SEGMENTS_DIR_NAME)
                .join(SHIELDED_SEGMENT_DIR);
            fs::create_dir_all(&shielded)?;
            fs::write(shielded.join("0000000001-0000004096.zstsup01"), b"bad")?;
            Ok(())
        })?;
        assert_disallowed_tree_rejected(|root| {
            fs::create_dir_all(
                root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                    .join(SEGMENTS_DIR_NAME)
                    .join(SHIELDED_SEGMENT_DIR)
                    .join("nested"),
            )?;
            Ok(())
        })?;
        assert_disallowed_tree_rejected(|root| {
            fs::write(
                root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                    .join(SEGMENTS_DIR_NAME),
                b"not-a-dir",
            )?;
            Ok(())
        })?;
        assert_disallowed_tree_rejected(|root| {
            fs::create_dir_all(
                root.join(WALLET_CHECKPOINT_DIRECTORY_NAME)
                    .join(SEGMENTS_DIR_NAME),
            )?;
            Ok(())
        })?;
        #[cfg(unix)]
        assert_unix_segment_links_rejected()?;
        Ok(())
    }

    fn assert_disallowed_tree_rejected(
        mutate: impl FnOnce(&Path) -> Result<(), Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let source = fixed_archive_root(&temporary)?;
        mutate(&source)?;
        assert!(collect_payload_files(&source, PayloadLayout::SealedArchive).is_err());
        fs::remove_file(source.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME))?;
        let target = temporary.path().join("target");
        fs::create_dir(&target)?;
        assert!(copy_archive_payload(&source, &target, None).is_err());
        Ok(())
    }

    #[cfg(unix)]
    fn assert_unix_segment_links_rejected() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::{MetadataExt, symlink};

        let temporary = TempDir::new()?;
        let outside_dir = temporary.path().join("outside-dir");
        fs::create_dir(&outside_dir)?;
        let outside_file = temporary.path().join("outside-file");
        fs::write(&outside_file, b"outside")?;

        assert_disallowed_tree_rejected(|root| {
            symlink(
                &outside_dir,
                root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                    .join(SEGMENTS_DIR_NAME),
            )?;
            Ok(())
        })?;
        assert_disallowed_tree_rejected(|root| {
            let segments = root
                .join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                .join(SEGMENTS_DIR_NAME);
            fs::create_dir_all(&segments)?;
            symlink(&outside_dir, segments.join(SHIELDED_SEGMENT_DIR))?;
            Ok(())
        })?;
        assert_disallowed_tree_rejected(|root| {
            let shielded = root
                .join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                .join(SEGMENTS_DIR_NAME)
                .join(SHIELDED_SEGMENT_DIR);
            fs::create_dir_all(&shielded)?;
            let seg_name = "0000000001-0000004096.zshrng02";
            symlink(&outside_file, shielded.join(seg_name))?;
            Ok(())
        })?;
        assert_disallowed_tree_rejected(|root| {
            symlink(
                &outside_dir,
                root.join(WALLET_CHECKPOINT_DIRECTORY_NAME).join("nested"),
            )?;
            Ok(())
        })?;

        let hardlinked_temp = TempDir::new()?;
        let source = fixed_archive_root(&hardlinked_temp)?;
        let manifest_path = source.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME);
        fs::remove_file(&manifest_path)?;
        let shielded = source
            .join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
            .join(SEGMENTS_DIR_NAME)
            .join(SHIELDED_SEGMENT_DIR);
        fs::create_dir_all(&shielded)?;
        let first_name = "0000000001-0000004096.zshrng02";
        let second_name = "0000004097-0000008192.zshrng02";
        let first = shielded.join(first_name);
        let second = shielded.join(second_name);
        fs::write(&first, b"seg")?;
        fs::hard_link(&first, &second)?;
        let inventory = collect_payload_files(&source, PayloadLayout::OwnerCapture)?;
        let export_target = hardlinked_temp.path().join("export-target");
        fs::create_dir(&export_target)?;
        let copied = copy_archive_payload(&source, &export_target, None)?;
        assert_eq!(copied, inventory);
        let copied_shielded = export_target
            .join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
            .join(SEGMENTS_DIR_NAME)
            .join(SHIELDED_SEGMENT_DIR);
        assert_eq!(fs::metadata(copied_shielded.join(first_name))?.nlink(), 1);
        assert_eq!(fs::metadata(copied_shielded.join(second_name))?.nlink(), 1);

        fs::write(&manifest_path, b"outer")?;
        assert!(collect_payload_files(&source, PayloadLayout::SealedArchive).is_err());
        let restore_target = hardlinked_temp.path().join("restore-target");
        fs::create_dir(&restore_target)?;
        assert!(copy_archive_payload(&source, &restore_target, Some(&inventory)).is_err());
        Ok(())
    }
}
