//! Coherent single-host canonical and wallet checkpoint bundles.
//!
//! The module deliberately stops at capture and admission primitives. The
//! canonical owner must create its checkpoint at the returned fixed path and
//! pass the cold-opened evidence back to the projector. The projector then
//! verifies that its wallet owner represents the exact same canonical fence,
//! creates the wallet checkpoint, and publishes the manifest last. Restore,
//! archive extraction, cross-process transport, and cursor rekeying are
//! separate production boundaries and are not implied by a valid manifest.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestVersion, MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES,
    Network,
    wire::{decode_zinder_native_chain_name, encode_zinder_native_chain_name},
};
use zinder_proto::v1::ingest::{CanonicalWriterFence, CreateCanonicalOwnerCheckpointResponse};
use zinder_store::{
    CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION, CANONICAL_STORE_IDENTITY,
    CANONICAL_STORE_SCHEMA_VERSION, CanonicalStoreWorkload, RawBlobRetention,
    RocksDbResourceBudget,
};
use zinder_wallet_projection::{
    WALLET_PROJECTION_STORE_IDENTITY, WalletCanonicalSourceIdentity,
    WalletProjectionFamilyRowCounts, WalletProjectionReadyEvidence,
};
use zinder_wallet_rocksdb::{
    RocksDbWalletFollowingStore, WALLET_ROCKSDB_SCHEMA_VERSION, WalletOwnerCheckpointEvidence,
};

/// Exact bundle identity admitted by this release.
pub const STATE_BUNDLE_IDENTITY: &str = "state-bundle";
/// Exact state-bundle manifest format admitted by this release.
pub const STATE_BUNDLE_FORMAT_VERSION: u16 = 3;
/// Only certified physical topology represented by a state bundle.
pub const STATE_BUNDLE_TOPOLOGY: &str = "rocksdb-single-host";
/// Fixed canonical checkpoint directory within a bundle.
pub const CANONICAL_CHECKPOINT_DIRECTORY_NAME: &str = "canonical.rocksdb";
/// Fixed wallet checkpoint directory within a bundle.
pub const WALLET_CHECKPOINT_DIRECTORY_NAME: &str = "wallet.rocksdb";
/// Manifest whose presence marks a completely captured bundle.
pub const STATE_BUNDLE_MANIFEST_FILE_NAME: &str = "state-bundle.json";
/// Maximum inner state-bundle manifest bytes decoded before allocation.
pub const STATE_BUNDLE_MANIFEST_MAX_BYTES: u64 = 1024 * 1024;

const MANIFEST_TEMPORARY_FILE_NAME: &str = ".state-bundle.json.incomplete";

/// Fixed paths in one unpublished state-bundle capture directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateBundleCapturePaths {
    candidate_id: String,
    root: PathBuf,
    staging_root_binding: Vec<u8>,
    canonical_checkpoint: PathBuf,
    wallet_checkpoint: PathBuf,
    manifest: PathBuf,
}

impl StateBundleCapturePaths {
    fn for_candidate(staging_root: &Path, candidate_id: String) -> Self {
        let root = staging_root.join(&candidate_id);
        Self {
            candidate_id,
            staging_root_binding: Sha256::digest(staging_root.as_os_str().as_encoded_bytes())
                .to_vec(),
            canonical_checkpoint: root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME),
            wallet_checkpoint: root.join(WALLET_CHECKPOINT_DIRECTORY_NAME),
            manifest: root.join(STATE_BUNDLE_MANIFEST_FILE_NAME),
            root,
        }
    }

    /// Opaque operator-selected identifier resolved below the staging root.
    #[must_use]
    pub fn candidate_id(&self) -> &str {
        &self.candidate_id
    }

    /// Unpublished bundle root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Absent target at which the canonical owner must create its checkpoint.
    #[must_use]
    pub fn canonical_checkpoint(&self) -> &Path {
        &self.canonical_checkpoint
    }

    /// Absent target reserved for the projector-owned wallet checkpoint.
    #[must_use]
    pub fn wallet_checkpoint(&self) -> &Path {
        &self.wallet_checkpoint
    }

    /// Manifest path published only after both checkpoints are cold-admitted.
    #[must_use]
    pub fn manifest(&self) -> &Path {
        &self.manifest
    }

    /// Opaque SHA-256 binding of the admitted shared staging root.
    ///
    /// This is sent to the canonical owner instead of a filesystem path so it
    /// can reject a projector configured against a different root.
    #[must_use]
    pub fn staging_root_binding(&self) -> &[u8] {
        &self.staging_root_binding
    }
}

/// Owned, fully validated canonical checkpoint evidence received over
/// `CanonicalControl.CreateOwnerCheckpoint`.
///
/// This DTO is the cross-process boundary. It intentionally exposes no
/// canonical primary or store-native checkpoint type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalCheckpointAdmissionEvidence {
    candidate_id: String,
    database_identity: Vec<u8>,
    admission: CanonicalAdmission,
}

impl CanonicalCheckpointAdmissionEvidence {
    /// Opaque staging candidate returned by the canonical owner.
    #[must_use]
    pub fn candidate_id(&self) -> &str {
        &self.candidate_id
    }

    /// Exact physical `RocksDB` database identity returned by the canonical
    /// owner. It is only sent back to that owner for immediate cold
    /// re-admission of the fixed checkpoint path; it is not filesystem
    /// authority.
    #[must_use]
    pub fn database_identity(&self) -> &[u8] {
        &self.database_identity
    }

    /// Exact visible canonical fence returned by the cold-admitted checkpoint.
    #[must_use]
    pub fn visible_fence(&self) -> CanonicalWriterFence {
        CanonicalWriterFence {
            chain_epoch_id: self.admission.visible_epoch,
            event_sequence: self.admission.visible_event_sequence,
            visible_tip_height: self.admission.visible_tip.height.value(),
            visible_tip_hash: self.admission.visible_tip.hash.as_bytes().to_vec(),
            canonical_sequence_digest: self.admission.visible_sequence_digest.as_bytes().to_vec(),
            visible_block_count: self.admission.visible_block_count,
        }
    }

    /// Rejects a cold re-admission that differs from the original owner
    /// checkpoint evidence before the wallet owner may create its checkpoint.
    pub fn verify_exact_readmission(&self, re_admitted: &Self) -> Result<(), StateBundleError> {
        require_manifest_value(
            self.candidate_id == re_admitted.candidate_id
                && self.database_identity == re_admitted.database_identity
                && self.admission == re_admitted.admission,
            "canonical checkpoint cold re-admission evidence differs from the original owner checkpoint",
        )
    }
}

/// Exact shared source fence committed by a complete state bundle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateBundleFence {
    chain_epoch_id: u64,
    chain_event_sequence: u64,
    visible_tip: BlockManifest,
    settled_tip: BlockManifest,
    sequence_digest: SequenceDigestManifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    network_upgrade_schedule: Option<NetworkUpgradeScheduleManifest>,
}

impl StateBundleFence {
    /// Canonical epoch represented by both checkpoints.
    #[must_use]
    pub const fn chain_epoch_id(&self) -> u64 {
        self.chain_epoch_id
    }

    /// Canonical event sequence represented by both checkpoints.
    #[must_use]
    pub const fn chain_event_sequence(&self) -> u64 {
        self.chain_event_sequence
    }

    /// Visible canonical tip height represented by both checkpoints.
    #[must_use]
    pub const fn visible_tip_height(&self) -> u32 {
        self.visible_tip.height
    }

    /// Writer-admitted network-upgrade schedule identity at this fence.
    ///
    /// Legacy format-2 bundles predate this identity and return `None`.
    #[must_use]
    pub const fn network_upgrade_schedule(&self) -> Option<&NetworkUpgradeScheduleManifest> {
        self.network_upgrade_schedule.as_ref()
    }
}

/// Writer-admitted schedule revision and table fingerprint at capture time.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkUpgradeScheduleManifest {
    revision: u64,
    fingerprint_version: u16,
    fingerprint: String,
}

impl NetworkUpgradeScheduleManifest {
    /// Monotonic writer-owned schedule revision at capture time.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// V1 table fingerprint as lower-case hexadecimal.
    #[must_use]
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    fn from_admitted(schedule: &AdmittedScheduleIdentity) -> Self {
        Self {
            revision: schedule.revision,
            fingerprint_version: 1,
            fingerprint: hex::encode(schedule.fingerprint),
        }
    }

    fn validate(&self) -> Result<(), StateBundleError> {
        require_manifest_value(
            self.revision > 0,
            "activation schedule revision must be nonzero",
        )?;
        require_manifest_value(
            self.fingerprint_version == 1,
            "activation schedule fingerprint version must be 1",
        )?;
        validate_lower_hex_32(&self.fingerprint, "activation schedule fingerprint")
    }
}

/// Validated state-bundle manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateBundleManifest {
    identity: String,
    format_version: u16,
    topology: String,
    candidate_id: String,
    network: String,
    fence: StateBundleFence,
    canonical_checkpoint: CanonicalCheckpointManifest,
    wallet_checkpoint: WalletCheckpointManifest,
}

impl StateBundleManifest {
    /// Reads and fail-closed validates a complete bundle.
    ///
    /// This validates the manifest and fixed directory layout. Restore code
    /// must still cold-open both stores and compare their observed evidence to
    /// this manifest before activating an inactive serving lane.
    pub fn read(
        root: impl AsRef<Path>,
        expected_network: Network,
    ) -> Result<Self, StateBundleError> {
        Self::read_with_additional_root_entries(root.as_ref(), expected_network, &[])
    }

    /// Reads a state-bundle nested inside a crate-owned fixed outer layout.
    ///
    /// The additional entries are compile-time names supplied by another
    /// projector boundary, never caller-selected paths.
    pub(crate) fn read_with_additional_root_entries(
        root: &Path,
        expected_network: Network,
        additional_root_entries: &[&str],
    ) -> Result<Self, StateBundleError> {
        let root = resolve_existing_directory(root, "state-bundle root")?;
        let manifest_path = root.join(STATE_BUNDLE_MANIFEST_FILE_NAME);
        require_regular_file(&manifest_path, "state-bundle manifest")?;
        let metadata = fs::metadata(&manifest_path)
            .map_err(|source| StateBundleError::io(&manifest_path, source))?;
        require_manifest_value(
            metadata.len() <= STATE_BUNDLE_MANIFEST_MAX_BYTES,
            "state-bundle manifest exceeds the fixed byte limit",
        )?;
        let capacity = usize::try_from(metadata.len()).map_err(|_| {
            StateBundleError::manifest_contract("state-bundle manifest length exceeds usize")
        })?;
        let mut encoded = Vec::new();
        encoded.try_reserve_exact(capacity).map_err(|_| {
            StateBundleError::manifest_contract("state-bundle manifest allocation failed")
        })?;
        let file = File::open(&manifest_path)
            .map_err(|source| StateBundleError::io(&manifest_path, source))?;
        let opened = file
            .metadata()
            .map_err(|source| StateBundleError::io(&manifest_path, source))?;
        require_manifest_value(
            opened.is_file() && opened.len() <= STATE_BUNDLE_MANIFEST_MAX_BYTES,
            "state-bundle manifest changed while admission opened it",
        )?;
        let mut bounded = file.take(STATE_BUNDLE_MANIFEST_MAX_BYTES.saturating_add(1));
        bounded
            .read_to_end(&mut encoded)
            .map_err(|source| StateBundleError::io(&manifest_path, source))?;
        require_manifest_value(
            u64::try_from(encoded.len())
                .is_ok_and(|length| length <= STATE_BUNDLE_MANIFEST_MAX_BYTES),
            "state-bundle manifest exceeds the fixed byte limit",
        )?;
        let manifest: Self = serde_json::from_slice(&encoded).map_err(|source| {
            StateBundleError::ManifestDecode {
                path: manifest_path.clone(),
                source,
            }
        })?;
        manifest.validate(expected_network)?;
        validate_complete_layout(&root, additional_root_entries)?;
        Ok(manifest)
    }

    /// Exact shared canonical-wallet fence admitted from this manifest.
    #[must_use]
    pub const fn fence(&self) -> &StateBundleFence {
        &self.fence
    }

    /// Network committed by this manifest.
    pub fn network(&self) -> Result<Network, StateBundleError> {
        parse_network(&self.network).ok_or_else(|| {
            StateBundleError::manifest_contract(format!(
                "network {:?} is not a supported exact spelling",
                self.network
            ))
        })
    }

    /// Opaque candidate identifier of this completed capture.
    #[must_use]
    pub fn candidate_id(&self) -> &str {
        &self.candidate_id
    }

    /// Exact manifest format identity.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Exact manifest format version.
    #[must_use]
    pub const fn format_version(&self) -> u16 {
        self.format_version
    }

    /// Certified physical topology.
    #[must_use]
    pub fn topology(&self) -> &str {
        &self.topology
    }

    /// SHA-256 identity committed by the cold-admitted canonical checkpoint.
    #[must_use]
    pub fn canonical_checkpoint_database_identity_sha256(&self) -> &str {
        &self.canonical_checkpoint.database_identity_sha256
    }

    /// Exact construction-manifest version bound by the canonical READY proof.
    #[must_use]
    pub const fn canonical_construction_manifest_version(&self) -> u16 {
        self.canonical_checkpoint.construction_manifest_version
    }

    /// SHA-256 of the immutable canonical construction-manifest sidecar.
    #[must_use]
    pub fn canonical_construction_manifest_sha256(&self) -> &str {
        &self.canonical_checkpoint.construction_manifest_sha256
    }

    /// SHA-256 identity committed by the cold-admitted wallet checkpoint.
    #[must_use]
    pub fn wallet_checkpoint_database_identity_sha256(&self) -> &str {
        &self.wallet_checkpoint.database_identity_sha256
    }

    fn from_admitted(
        candidate_id: &str,
        canonical: &CanonicalAdmission,
        wallet: &WalletAdmission,
    ) -> Result<Self, StateBundleError> {
        validate_candidate_id(candidate_id)?;
        validate_exact_fence(canonical, wallet)?;
        let fence = StateBundleFence::from_canonical(canonical);
        Ok(Self {
            identity: STATE_BUNDLE_IDENTITY.to_owned(),
            format_version: STATE_BUNDLE_FORMAT_VERSION,
            topology: STATE_BUNDLE_TOPOLOGY.to_owned(),
            candidate_id: candidate_id.to_owned(),
            network: network_name(canonical.network).to_owned(),
            fence,
            canonical_checkpoint: CanonicalCheckpointManifest::from_admitted(canonical),
            wallet_checkpoint: WalletCheckpointManifest::from_admitted(wallet)?,
        })
    }

    fn validate(&self, expected_network: Network) -> Result<(), StateBundleError> {
        require_manifest_value(
            self.identity == STATE_BUNDLE_IDENTITY,
            "identity must be exactly state-bundle",
        )?;
        let supported_format = matches!(
            (
                self.format_version,
                self.fence.network_upgrade_schedule.is_some()
            ),
            (2, false) | (STATE_BUNDLE_FORMAT_VERSION, true)
        );
        require_manifest_value(
            supported_format,
            "format version and activation-schedule evidence are incompatible; format 2 omits schedule identity and format 3 requires it",
        )?;
        require_manifest_value(
            self.topology == STATE_BUNDLE_TOPOLOGY,
            "topology must be exactly rocksdb-single-host",
        )?;
        validate_candidate_id(&self.candidate_id)?;
        let network = parse_network(&self.network).ok_or_else(|| {
            StateBundleError::manifest_contract(format!(
                "network {:?} is not a supported exact spelling",
                self.network
            ))
        })?;
        require_manifest_value(
            network == expected_network,
            "manifest network does not match",
        )?;
        self.fence.validate()?;
        self.canonical_checkpoint.validate(network, &self.fence)?;
        self.wallet_checkpoint.validate(network, &self.fence)?;
        Ok(())
    }
}

/// Creates one fresh, unpublished candidate below a configured staging root.
///
/// The configured root must already exist. `candidate_id` is an opaque safe
/// identifier, never a path. Existing candidate roots are preserved verbatim,
/// including empty directories. The canonical owner must next create only the
/// fixed [`StateBundleCapturePaths::canonical_checkpoint`] child.
pub fn prepare_state_bundle_capture(
    configured_staging_root: impl AsRef<Path>,
    candidate_id: &str,
) -> Result<StateBundleCapturePaths, StateBundleError> {
    validate_candidate_id(candidate_id)?;
    let staging_root = resolve_existing_directory(
        configured_staging_root.as_ref(),
        "configured state-bundle staging root",
    )?;
    let paths = StateBundleCapturePaths::for_candidate(&staging_root, candidate_id.to_owned());
    match fs::create_dir(&paths.root) {
        Ok(()) => Ok(paths),
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
            Err(StateBundleError::TargetExists { path: paths.root })
        }
        Err(source) => Err(StateBundleError::io(&paths.root, source)),
    }
}

/// Creates the wallet checkpoint and publishes a coherent manifest last.
///
/// The canonical checkpoint must already occupy the candidate's fixed
/// canonical path and must be the only entry in the capture root. Wallet
/// checkpoint creation is refused before any wallet files are written unless
/// the live wallet owner represents the exact canonical cold-opened fence.
pub fn complete_state_bundle_capture(
    paths: &StateBundleCapturePaths,
    canonical_checkpoint: &CanonicalCheckpointAdmissionEvidence,
    wallet: &mut RocksDbWalletFollowingStore,
    admission_resource_budget: RocksDbResourceBudget,
) -> Result<StateBundleManifest, StateBundleError> {
    validate_capture_candidate(paths, canonical_checkpoint.candidate_id())?;
    let canonical = &canonical_checkpoint.admission;
    let live_wallet = WalletAdmission::from_ready(
        wallet.network(),
        wallet.ready_evidence(),
        WALLET_PROJECTION_STORE_IDENTITY,
        WALLET_ROCKSDB_SCHEMA_VERSION,
        None,
    )?;
    validate_exact_fence(canonical, &live_wallet)?;

    let checkpoint = wallet
        .create_owner_checkpoint(&paths.wallet_checkpoint, admission_resource_budget)
        .map_err(|source| StateBundleError::WalletCheckpoint { source })?;
    let cold_wallet = WalletAdmission::try_from(&checkpoint)?;
    validate_exact_fence(canonical, &cold_wallet)?;
    require_manifest_value(
        live_wallet.has_same_source_evidence(&cold_wallet),
        "wallet checkpoint cold evidence changed from its admitted owner source",
    )?;

    let manifest = StateBundleManifest::from_admitted(
        canonical_checkpoint.candidate_id(),
        canonical,
        &cold_wallet,
    )?;
    publish_manifest_last(paths, &manifest)?;
    Ok(manifest)
}

/// State-bundle capture or admission failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StateBundleError {
    /// A path is relative, contains traversal, or resolves to the wrong kind.
    #[error("unsafe {purpose} path {path}: {reason}")]
    UnsafePath {
        /// Path rejected before capture or admission.
        path: PathBuf,
        /// Operational role of the path.
        purpose: &'static str,
        /// Stable rejection reason.
        reason: &'static str,
    },
    /// A fresh capture target already exists.
    #[error("state-bundle capture requires an absent target: {path}")]
    TargetExists {
        /// Existing path preserved by the failed operation.
        path: PathBuf,
    },
    /// Capture-root contents are not the exact unpublished or published layout.
    #[error("state-bundle directory {path} has unexpected entry {entry:?}")]
    UnexpectedEntry {
        /// Bundle root being validated.
        path: PathBuf,
        /// Unexpected top-level entry name.
        entry: std::ffi::OsString,
    },
    /// Canonical evidence names a different opaque staging candidate.
    #[error(
        "canonical checkpoint candidate {observed:?} does not equal prepared candidate {expected:?}"
    )]
    CanonicalCandidateMismatch {
        /// Prepared candidate identifier.
        expected: String,
        /// Identifier returned by the canonical owner.
        observed: String,
    },
    /// A manifest field or cross-store invariant is invalid.
    #[error("state-bundle manifest contract rejected: {reason}")]
    ManifestContract {
        /// Exact fail-closed rejection reason.
        reason: String,
    },
    /// A manifest was not valid exact JSON.
    #[error("state-bundle manifest {path} is not valid format-2 or format-3 JSON: {source}")]
    ManifestDecode {
        /// Manifest path.
        path: PathBuf,
        /// JSON decode failure.
        #[source]
        source: serde_json::Error,
    },
    /// The owner wallet checkpoint operation failed.
    #[error("wallet state-bundle checkpoint failed: {source}")]
    WalletCheckpoint {
        /// Concrete wallet checkpoint failure.
        #[source]
        source: zinder_wallet_rocksdb::RocksDbWalletError,
    },
    /// Filesystem operation failed.
    #[error("state-bundle filesystem operation failed at {path}: {source}")]
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Concrete I/O failure.
        #[source]
        source: io::Error,
    },
}

impl StateBundleError {
    fn io(path: &Path, source: io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }

    fn manifest_contract(reason: impl Into<String>) -> Self {
        Self::ManifestContract {
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalAdmission {
    database_identity_sha256: String,
    construction_manifest_version: u16,
    construction_manifest_sha256: [u8; 32],
    store_identity: String,
    schema_version: u16,
    network: Network,
    workload: CanonicalStoreWorkload,
    build_plan: CanonicalBuildPlanAdmission,
    effective_reorg_window_blocks: Option<u32>,
    admitted_schedule: AdmittedScheduleIdentity,
    first_retained_block: BlockId,
    visible_tip: BlockId,
    visible_epoch: u64,
    visible_event_sequence: u64,
    visible_block_count: u64,
    block_digest_version: u16,
    replay_format_version: u32,
    visible_sequence_digest: CanonicalBlockFactsSequenceDigest,
    visible_logical_replay_bytes: u64,
    settled_tip: BlockId,
    settled_retained_block_count: u64,
    settled_sequence_digest: CanonicalBlockFactsSequenceDigest,
    settled_logical_replay_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AdmittedScheduleIdentity {
    revision: u64,
    fingerprint: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalBuildPlanAdmission {
    activation_fingerprint_version: u16,
    activation_fingerprint: [u8; 32],
    raw_blob_retention: RawBlobRetention,
    reorg_window_blocks: u32,
    history_preceding_checkpoint: Option<BlockId>,
    history_predecessor: CanonicalHistoryPredecessorAdmission,
    build_tip: BlockId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalHistoryPredecessorAdmission {
    block_id: BlockId,
    block_time_seconds: u32,
    sapling_frontier: Option<CanonicalFrontierAdmission>,
    orchard_frontier: Option<CanonicalFrontierAdmission>,
    ironwood_frontier: Option<CanonicalFrontierAdmission>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalFrontierAdmission {
    final_root: [u8; 32],
    final_state: Vec<u8>,
}

struct DecodedCanonicalCheckpointResponse {
    candidate_id: String,
    database_identity: Vec<u8>,
    store_identity: String,
    schema_version: u16,
    network: Network,
    build_plan: zinder_proto::v1::ingest::CanonicalOwnerCheckpointBuildPlanEvidence,
    effective_reorg_window_blocks: Option<u32>,
    network_upgrade_schedule: zinder_proto::v1::ingest::CanonicalNetworkUpgradeSchedule,
    ready: zinder_proto::v1::ingest::CanonicalOwnerCheckpointReadyEvidence,
}

#[derive(Clone, Copy)]
struct DecodedCanonicalReadyEvidence {
    first_retained_block: BlockId,
    visible_tip: BlockId,
    visible_epoch: u64,
    visible_event_sequence: u64,
    visible_block_count: u64,
    block_digest_version: u16,
    replay_format_version: u32,
    sequence_digest_version: CanonicalBlockFactsSequenceDigestVersion,
    visible_sequence_digest: [u8; 32],
    visible_logical_replay_bytes: u64,
    settled_tip: BlockId,
    settled_retained_block_count: u64,
    settled_sequence_digest: [u8; 32],
    settled_logical_replay_bytes: u64,
    construction_manifest_version: u16,
    construction_manifest_sha256: [u8; 32],
}

struct DecodedVisibleFence {
    tip: BlockId,
    epoch: u64,
    event_sequence: u64,
    block_count: u64,
    sequence_digest: [u8; 32],
}

struct DecodedSettledCheckpoint {
    tip: BlockId,
    retained_block_count: u64,
    sequence_digest: [u8; 32],
    logical_replay_bytes: u64,
}

struct DecodedHistoryBoundary {
    preceding_checkpoint: Option<BlockId>,
    predecessor: CanonicalHistoryPredecessorAdmission,
    first_retained_height: u32,
}

impl TryFrom<CreateCanonicalOwnerCheckpointResponse> for CanonicalCheckpointAdmissionEvidence {
    type Error = StateBundleError;

    fn try_from(response: CreateCanonicalOwnerCheckpointResponse) -> Result<Self, Self::Error> {
        let DecodedCanonicalCheckpointResponse {
            candidate_id,
            database_identity,
            store_identity,
            schema_version,
            network,
            build_plan,
            effective_reorg_window_blocks,
            network_upgrade_schedule,
            ready,
        } = decode_checkpoint_response(response)?;
        let (activations, revision) =
            zinder_proto::wire::decode_canonical_network_upgrade_schedule(
                network_upgrade_schedule,
                network,
            )
            .map_err(|error| StateBundleError::manifest_contract(error.to_string()))?;
        let schedule_fingerprint =
            activations.fingerprint(zinder_core::NetworkUpgradeActivationsFingerprintVersion::V1);
        let ready = decode_ready_evidence(ready)?;
        let build_plan = decode_build_plan(
            build_plan,
            network,
            ready.first_retained_block,
            ready.visible_tip,
        )?;
        let admission = CanonicalAdmission {
            effective_reorg_window_blocks,
            ..assemble_canonical_admission(
                &database_identity,
                store_identity,
                schema_version,
                network,
                &ready,
                build_plan,
                AdmittedScheduleIdentity {
                    revision,
                    fingerprint: schedule_fingerprint.as_bytes(),
                },
            )
        };
        Ok(Self {
            candidate_id,
            database_identity,
            admission,
        })
    }
}

fn decode_checkpoint_response(
    response: CreateCanonicalOwnerCheckpointResponse,
) -> Result<DecodedCanonicalCheckpointResponse, StateBundleError> {
    validate_candidate_id(&response.candidate_id)?;
    require_manifest_value(
        response.store_identity == CANONICAL_STORE_IDENTITY,
        "canonical checkpoint identity must be exactly canonical",
    )?;
    let schema_version = u16::try_from(response.schema_version).map_err(|_| {
        StateBundleError::manifest_contract(
            "canonical checkpoint schema does not fit the physical u16 contract",
        )
    })?;
    require_manifest_value(
        schema_version == CANONICAL_STORE_SCHEMA_VERSION,
        "canonical checkpoint schema is unsupported",
    )?;
    require_manifest_value(
        response.workload == CanonicalStoreWorkload::Wallet.as_str(),
        "canonical checkpoint workload must be wallet",
    )?;
    require_manifest_value(
        !response.database_identity.is_empty() && response.database_identity.len() <= 256,
        "canonical checkpoint database identity must contain 1-256 bytes",
    )?;
    let network = decode_zinder_native_chain_name(&response.network_name).map_err(|_| {
        StateBundleError::manifest_contract(format!(
            "canonical checkpoint network {:?} is not an exact Zinder-native name",
            response.network_name
        ))
    })?;
    let build_plan = response.build_plan.ok_or_else(|| {
        StateBundleError::manifest_contract(
            "canonical checkpoint response omitted immutable build-plan evidence",
        )
    })?;
    let ready = response.ready_evidence.ok_or_else(|| {
        StateBundleError::manifest_contract(
            "canonical checkpoint response omitted cold READY evidence",
        )
    })?;
    let network_upgrade_schedule = response.network_upgrade_schedule.ok_or_else(|| {
        StateBundleError::manifest_contract(
            "canonical checkpoint response omitted admitted activation schedule evidence",
        )
    })?;
    require_manifest_value(
        response
            .effective_reorg_window_blocks
            .is_none_or(|effective| effective > build_plan.reorg_window_blocks),
        "canonical checkpoint effective reorg window must raise its construction window",
    )?;
    Ok(DecodedCanonicalCheckpointResponse {
        candidate_id: response.candidate_id,
        database_identity: response.database_identity,
        store_identity: response.store_identity,
        schema_version,
        network,
        build_plan,
        effective_reorg_window_blocks: response.effective_reorg_window_blocks,
        network_upgrade_schedule,
        ready,
    })
}

fn decode_ready_evidence(
    ready: zinder_proto::v1::ingest::CanonicalOwnerCheckpointReadyEvidence,
) -> Result<DecodedCanonicalReadyEvidence, StateBundleError> {
    require_manifest_value(
        ready.block_digest_version == 1,
        "canonical block digest version must be exactly 1",
    )?;
    require_manifest_value(
        ready.replay_format_version == 1,
        "canonical replay format version must be exactly 1",
    )?;
    require_manifest_value(
        ready.construction_manifest_version
            == u32::from(CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION),
        "canonical construction-manifest version is unsupported",
    )?;
    let block_digest_version = u16::try_from(ready.block_digest_version).map_err(|_| {
        StateBundleError::manifest_contract("canonical block digest version does not fit u16")
    })?;
    let sequence_digest_version = u16::try_from(ready.sequence_digest_version)
        .ok()
        .and_then(|version| CanonicalBlockFactsSequenceDigestVersion::try_from(version).ok())
        .ok_or_else(|| {
            StateBundleError::manifest_contract(
                "canonical checkpoint sequence digest version must be exactly 1",
            )
        })?;
    let construction_manifest_version = u16::try_from(ready.construction_manifest_version)
        .map_err(|_| {
            StateBundleError::manifest_contract(
                "canonical construction-manifest version does not fit u16",
            )
        })?;
    let first_retained_block =
        decode_checkpoint_block(ready.first_retained_block, "canonical first retained block")?;
    let visible = decode_visible_fence(ready.visible_fence)?;
    require_manifest_value(
        first_retained_block.height <= visible.tip.height,
        "canonical first retained block exceeds its visible tip",
    )?;
    let settled =
        decode_settled_checkpoint(ready.sequence_checkpoint, first_retained_block, visible.tip)?;
    Ok(DecodedCanonicalReadyEvidence {
        first_retained_block,
        visible_tip: visible.tip,
        visible_epoch: visible.epoch,
        visible_event_sequence: visible.event_sequence,
        visible_block_count: visible.block_count,
        block_digest_version,
        replay_format_version: ready.replay_format_version,
        sequence_digest_version,
        visible_sequence_digest: visible.sequence_digest,
        visible_logical_replay_bytes: ready.visible_logical_replay_bytes,
        settled_tip: settled.tip,
        settled_retained_block_count: settled.retained_block_count,
        settled_sequence_digest: settled.sequence_digest,
        settled_logical_replay_bytes: settled.logical_replay_bytes,
        construction_manifest_version,
        construction_manifest_sha256: decode_exact_32(
            &ready.construction_manifest_sha256,
            "canonical construction-manifest SHA-256",
        )?,
    })
}

fn decode_visible_fence(
    fence: Option<zinder_proto::v1::ingest::CanonicalWriterFence>,
) -> Result<DecodedVisibleFence, StateBundleError> {
    let fence = fence.ok_or_else(|| {
        StateBundleError::manifest_contract(
            "canonical checkpoint READY evidence omitted the visible fence",
        )
    })?;
    require_manifest_value(
        fence.chain_epoch_id > 0,
        "canonical checkpoint visible epoch must be nonzero",
    )?;
    require_manifest_value(
        fence.event_sequence > 0,
        "canonical checkpoint visible event sequence must be nonzero",
    )?;
    Ok(DecodedVisibleFence {
        tip: decode_block(
            fence.visible_tip_height,
            &fence.visible_tip_hash,
            "canonical visible tip",
        )?,
        epoch: fence.chain_epoch_id,
        event_sequence: fence.event_sequence,
        block_count: fence.visible_block_count,
        sequence_digest: decode_exact_32(
            &fence.canonical_sequence_digest,
            "canonical visible sequence digest",
        )?,
    })
}

fn decode_settled_checkpoint(
    checkpoint: Option<zinder_proto::v1::ingest::CanonicalCheckpointSequenceEvidence>,
    first_retained_block: BlockId,
    visible_tip: BlockId,
) -> Result<DecodedSettledCheckpoint, StateBundleError> {
    let checkpoint = checkpoint.ok_or_else(|| {
        StateBundleError::manifest_contract(
            "canonical checkpoint READY evidence omitted settled sequence evidence",
        )
    })?;
    let tip = decode_checkpoint_block(checkpoint.through, "canonical settled sequence checkpoint")?;
    require_manifest_value(
        tip.height <= visible_tip.height,
        "canonical settled checkpoint exceeds its visible tip",
    )?;
    require_manifest_value(
        first_retained_block.height <= tip.height,
        "canonical settled checkpoint precedes its first retained block",
    )?;
    Ok(DecodedSettledCheckpoint {
        tip,
        retained_block_count: checkpoint.retained_block_count,
        sequence_digest: decode_exact_32(
            &checkpoint.sequence_digest,
            "canonical settled sequence digest",
        )?,
        logical_replay_bytes: checkpoint.logical_replay_bytes,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the cross-process admission assembler keeps every independently decoded identity explicit"
)]
fn assemble_canonical_admission(
    database_identity: &[u8],
    store_identity: String,
    schema_version: u16,
    network: Network,
    ready: &DecodedCanonicalReadyEvidence,
    build_plan: CanonicalBuildPlanAdmission,
    admitted_schedule: AdmittedScheduleIdentity,
) -> CanonicalAdmission {
    CanonicalAdmission {
        database_identity_sha256: sha256_hex(database_identity),
        construction_manifest_version: ready.construction_manifest_version,
        construction_manifest_sha256: ready.construction_manifest_sha256,
        store_identity,
        schema_version,
        network,
        workload: CanonicalStoreWorkload::Wallet,
        build_plan,
        effective_reorg_window_blocks: None,
        admitted_schedule,
        first_retained_block: ready.first_retained_block,
        visible_tip: ready.visible_tip,
        visible_epoch: ready.visible_epoch,
        visible_event_sequence: ready.visible_event_sequence,
        visible_block_count: ready.visible_block_count,
        block_digest_version: ready.block_digest_version,
        replay_format_version: ready.replay_format_version,
        visible_sequence_digest: CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            ready.sequence_digest_version,
            ready.visible_block_count,
            ready.visible_sequence_digest,
        ),
        visible_logical_replay_bytes: ready.visible_logical_replay_bytes,
        settled_tip: ready.settled_tip,
        settled_retained_block_count: ready.settled_retained_block_count,
        settled_sequence_digest: CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            ready.sequence_digest_version,
            ready.settled_retained_block_count,
            ready.settled_sequence_digest,
        ),
        settled_logical_replay_bytes: ready.settled_logical_replay_bytes,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WalletAdmission {
    store_identity: String,
    schema_version: u16,
    network: Network,
    source: WalletCanonicalSourceIdentity,
    ready: WalletProjectionReadyEvidence,
    checkpoint_database_identity_sha256: Option<String>,
}

impl WalletAdmission {
    fn from_ready(
        network: Network,
        ready: &WalletProjectionReadyEvidence,
        store_identity: &[u8],
        schema_version: u16,
        checkpoint_database_identity: Option<&[u8]>,
    ) -> Result<Self, StateBundleError> {
        require_manifest_value(
            store_identity == WALLET_PROJECTION_STORE_IDENTITY,
            "wallet checkpoint identity must be exactly wallet",
        )?;
        require_manifest_value(
            schema_version == WALLET_ROCKSDB_SCHEMA_VERSION,
            "wallet checkpoint schema must be exactly 1",
        )?;
        if let Some(database_identity) = checkpoint_database_identity {
            require_manifest_value(
                !database_identity.is_empty(),
                "wallet checkpoint database identity must not be empty",
            )?;
        }
        Ok(Self {
            store_identity: String::from_utf8(store_identity.to_vec()).map_err(|_| {
                StateBundleError::manifest_contract("wallet checkpoint identity is not UTF-8")
            })?,
            schema_version,
            network,
            source: WalletCanonicalSourceIdentity::from_ready_evidence(ready),
            ready: ready.clone(),
            checkpoint_database_identity_sha256: checkpoint_database_identity.map(sha256_hex),
        })
    }

    fn has_same_source_evidence(&self, other: &Self) -> bool {
        self.store_identity == other.store_identity
            && self.schema_version == other.schema_version
            && self.network == other.network
            && self.source == other.source
            && self.ready == other.ready
    }
}

impl TryFrom<&WalletOwnerCheckpointEvidence> for WalletAdmission {
    type Error = StateBundleError;

    fn try_from(evidence: &WalletOwnerCheckpointEvidence) -> Result<Self, Self::Error> {
        Self::from_ready(
            evidence.network,
            &evidence.ready_evidence,
            evidence.store_identity,
            evidence.schema_version,
            Some(&evidence.database_identity),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockManifest {
    height: u32,
    hash: String,
}

impl BlockManifest {
    fn from_block(block: BlockId) -> Self {
        Self {
            height: block.height.value(),
            hash: hex::encode(block.hash.as_bytes()),
        }
    }

    fn validate(&self, field: &str) -> Result<(), StateBundleError> {
        validate_lower_hex_32(&self.hash, field)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SequenceDigestManifest {
    version: u16,
    block_count: u64,
    sha256: String,
}

impl SequenceDigestManifest {
    fn from_digest(digest: CanonicalBlockFactsSequenceDigest) -> Self {
        Self {
            version: digest.version().value(),
            block_count: digest.block_count(),
            sha256: hex::encode(digest.as_bytes()),
        }
    }

    fn validate(&self, field: &str) -> Result<(), StateBundleError> {
        require_manifest_value(self.version == 1, format!("{field} version must be 1"))?;
        validate_lower_hex_32(&self.sha256, field)
    }
}

impl StateBundleFence {
    fn from_canonical(canonical: &CanonicalAdmission) -> Self {
        Self {
            chain_epoch_id: canonical.visible_epoch,
            chain_event_sequence: canonical.visible_event_sequence,
            visible_tip: BlockManifest::from_block(canonical.visible_tip),
            settled_tip: BlockManifest::from_block(canonical.settled_tip),
            sequence_digest: SequenceDigestManifest::from_digest(canonical.visible_sequence_digest),
            network_upgrade_schedule: Some(NetworkUpgradeScheduleManifest::from_admitted(
                &canonical.admitted_schedule,
            )),
        }
    }

    fn validate(&self) -> Result<(), StateBundleError> {
        require_manifest_value(self.chain_epoch_id > 0, "fence epoch must be nonzero")?;
        require_manifest_value(
            self.chain_event_sequence > 0,
            "fence event sequence must be nonzero",
        )?;
        self.visible_tip.validate("fence visible-tip hash")?;
        self.settled_tip.validate("fence settled-tip hash")?;
        require_manifest_value(
            self.settled_tip.height <= self.visible_tip.height,
            "fence settled tip must not exceed visible tip",
        )?;
        self.sequence_digest.validate("fence sequence digest")?;
        if let Some(schedule) = &self.network_upgrade_schedule {
            schedule.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalCheckpointManifest {
    directory: String,
    store_identity: String,
    schema_version: u16,
    database_identity_sha256: String,
    construction_manifest_version: u16,
    construction_manifest_sha256: String,
    workload: String,
    cold_admitted: bool,
    build_plan: CanonicalBuildPlanManifest,
    /// Reorg window the checkpoint enforces when a canonical writer raised it
    /// above `build_plan.reorg_window_blocks`; absent at the construction
    /// window, so bundles of unraised stores keep their earlier shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    effective_reorg_window_blocks: Option<u32>,
    first_retained_block: BlockManifest,
    visible_block_count: u64,
    block_digest_version: u16,
    replay_format_version: u32,
    visible_logical_replay_bytes: u64,
    settled_retained_block_count: u64,
    settled_sequence_digest: SequenceDigestManifest,
    settled_logical_replay_bytes: u64,
}

impl CanonicalCheckpointManifest {
    fn from_admitted(canonical: &CanonicalAdmission) -> Self {
        Self {
            directory: CANONICAL_CHECKPOINT_DIRECTORY_NAME.to_owned(),
            store_identity: canonical.store_identity.clone(),
            schema_version: canonical.schema_version,
            database_identity_sha256: canonical.database_identity_sha256.clone(),
            construction_manifest_version: canonical.construction_manifest_version,
            construction_manifest_sha256: hex::encode(canonical.construction_manifest_sha256),
            workload: canonical.workload.as_str().to_owned(),
            cold_admitted: true,
            build_plan: CanonicalBuildPlanManifest::from_admitted(&canonical.build_plan),
            effective_reorg_window_blocks: canonical.effective_reorg_window_blocks,
            first_retained_block: BlockManifest::from_block(canonical.first_retained_block),
            visible_block_count: canonical.visible_block_count,
            block_digest_version: canonical.block_digest_version,
            replay_format_version: canonical.replay_format_version,
            visible_logical_replay_bytes: canonical.visible_logical_replay_bytes,
            settled_retained_block_count: canonical.settled_retained_block_count,
            settled_sequence_digest: SequenceDigestManifest::from_digest(
                canonical.settled_sequence_digest,
            ),
            settled_logical_replay_bytes: canonical.settled_logical_replay_bytes,
        }
    }

    fn validate(&self, network: Network, fence: &StateBundleFence) -> Result<(), StateBundleError> {
        require_manifest_value(
            self.directory == CANONICAL_CHECKPOINT_DIRECTORY_NAME,
            "canonical checkpoint directory must be exactly canonical.rocksdb",
        )?;
        require_manifest_value(
            safe_relative_name(&self.directory),
            "canonical checkpoint directory is not a safe fixed name",
        )?;
        require_manifest_value(
            self.store_identity == CANONICAL_STORE_IDENTITY,
            "canonical checkpoint identity must be exactly canonical",
        )?;
        require_manifest_value(
            self.schema_version == CANONICAL_STORE_SCHEMA_VERSION,
            "canonical checkpoint schema is unsupported",
        )?;
        validate_lower_hex_32(
            &self.database_identity_sha256,
            "canonical checkpoint database identity SHA-256",
        )?;
        require_manifest_value(
            self.construction_manifest_version == CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION,
            "canonical construction-manifest version is unsupported",
        )?;
        validate_lower_hex_32(
            &self.construction_manifest_sha256,
            "canonical construction-manifest SHA-256",
        )?;
        require_manifest_value(
            self.workload == CanonicalStoreWorkload::Wallet.as_str(),
            "canonical checkpoint workload must be wallet",
        )?;
        require_manifest_value(
            self.cold_admitted,
            "canonical checkpoint was not cold-admitted",
        )?;
        self.first_retained_block
            .validate("canonical first-retained-block hash")?;
        self.build_plan
            .validate(network, &self.first_retained_block, &fence.visible_tip)?;
        require_manifest_value(
            self.effective_reorg_window_blocks
                .is_none_or(|effective| effective > self.build_plan.reorg_window_blocks),
            "canonical effective reorg window must raise its construction window",
        )?;
        require_manifest_value(
            self.visible_block_count == fence.sequence_digest.block_count,
            "canonical visible block count does not match the shared sequence digest",
        )?;
        require_manifest_value(
            self.block_digest_version == 1,
            "canonical block digest version must be 1",
        )?;
        require_manifest_value(
            self.replay_format_version == 1,
            "canonical replay format version must be 1",
        )?;
        self.settled_sequence_digest
            .validate("canonical settled sequence digest")?;
        require_manifest_value(
            self.settled_retained_block_count == self.settled_sequence_digest.block_count,
            "canonical settled count does not match settled sequence digest",
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalBuildPlanManifest {
    activation_fingerprint_version: u16,
    activation_fingerprint: String,
    raw_blob_retention: RawBlobRetention,
    reorg_window_blocks: u32,
    history_preceding_checkpoint: Option<BlockManifest>,
    history_predecessor: CanonicalHistoryPredecessorManifest,
    build_tip: BlockManifest,
}

impl CanonicalBuildPlanManifest {
    fn from_admitted(build_plan: &CanonicalBuildPlanAdmission) -> Self {
        Self {
            activation_fingerprint_version: build_plan.activation_fingerprint_version,
            activation_fingerprint: hex::encode(build_plan.activation_fingerprint),
            raw_blob_retention: build_plan.raw_blob_retention,
            reorg_window_blocks: build_plan.reorg_window_blocks,
            history_preceding_checkpoint: build_plan
                .history_preceding_checkpoint
                .map(BlockManifest::from_block),
            history_predecessor: CanonicalHistoryPredecessorManifest::from_admitted(
                &build_plan.history_predecessor,
            ),
            build_tip: BlockManifest::from_block(build_plan.build_tip),
        }
    }

    fn validate(
        &self,
        network: Network,
        first_retained_block: &BlockManifest,
        visible_tip: &BlockManifest,
    ) -> Result<(), StateBundleError> {
        require_manifest_value(
            self.activation_fingerprint_version == 1,
            "canonical activation fingerprint version must be 1",
        )?;
        validate_lower_hex_32(
            &self.activation_fingerprint,
            "canonical activation fingerprint",
        )?;
        require_manifest_value(
            self.reorg_window_blocks > 0,
            "canonical build-plan reorg window must be nonzero",
        )?;
        self.history_predecessor.validate()?;
        self.build_tip.validate("canonical fixed build-tip hash")?;
        let expected_first_height = match &self.history_preceding_checkpoint {
            None => {
                require_manifest_value(
                    self.history_predecessor.block_id
                        == BlockManifest::from_block(BlockId::new(
                            BlockHeight::new(0),
                            network.genesis_hash(),
                        )),
                    "complete canonical history predecessor must be the configured network genesis",
                )?;
                require_manifest_value(
                    self.history_predecessor.sapling_frontier.is_none()
                        && self.history_predecessor.orchard_frontier.is_none()
                        && self.history_predecessor.ironwood_frontier.is_none(),
                    "complete canonical history predecessor must not contain commitment frontiers",
                )?;
                1
            }
            Some(checkpoint) => {
                checkpoint.validate("canonical history checkpoint hash")?;
                require_manifest_value(
                    checkpoint.height > 0,
                    "checkpointed canonical history must not use genesis as its checkpoint",
                )?;
                require_manifest_value(
                    &self.history_predecessor.block_id == checkpoint,
                    "canonical history predecessor does not equal its preceding checkpoint",
                )?;
                checkpoint.height.checked_add(1).ok_or_else(|| {
                    StateBundleError::manifest_contract(
                        "canonical history checkpoint has no representable successor",
                    )
                })?
            }
        };
        require_manifest_value(
            first_retained_block.height == expected_first_height,
            "canonical first retained block does not match the build-plan history boundary",
        )?;
        require_manifest_value(
            self.build_tip.height >= expected_first_height,
            "canonical fixed build tip precedes the first retained block",
        )?;
        require_manifest_value(
            self.build_tip.height <= visible_tip.height,
            "canonical fixed build tip exceeds the checkpoint visible tip",
        )?;
        if self.build_tip.height == visible_tip.height {
            require_manifest_value(
                self.build_tip.hash == visible_tip.hash,
                "canonical fixed build tip hash differs from the equal-height visible tip",
            )?;
        }
        if first_retained_block.height == self.build_tip.height {
            require_manifest_value(
                first_retained_block.hash == self.build_tip.hash,
                "canonical first retained block hash differs from the equal-height fixed build tip",
            )?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalHistoryPredecessorManifest {
    block_id: BlockManifest,
    block_time_seconds: u32,
    sapling_frontier: Option<CanonicalFrontierManifest>,
    orchard_frontier: Option<CanonicalFrontierManifest>,
    ironwood_frontier: Option<CanonicalFrontierManifest>,
}

impl CanonicalHistoryPredecessorManifest {
    fn from_admitted(predecessor: &CanonicalHistoryPredecessorAdmission) -> Self {
        Self {
            block_id: BlockManifest::from_block(predecessor.block_id),
            block_time_seconds: predecessor.block_time_seconds,
            sapling_frontier: predecessor
                .sapling_frontier
                .as_ref()
                .map(CanonicalFrontierManifest::from_admitted),
            orchard_frontier: predecessor
                .orchard_frontier
                .as_ref()
                .map(CanonicalFrontierManifest::from_admitted),
            ironwood_frontier: predecessor
                .ironwood_frontier
                .as_ref()
                .map(CanonicalFrontierManifest::from_admitted),
        }
    }

    fn validate(&self) -> Result<(), StateBundleError> {
        self.block_id
            .validate("canonical history-predecessor block hash")?;
        for (protocol, frontier) in [
            ("sapling", self.sapling_frontier.as_ref()),
            ("orchard", self.orchard_frontier.as_ref()),
            ("ironwood", self.ironwood_frontier.as_ref()),
        ] {
            if let Some(frontier) = frontier {
                frontier.validate(protocol)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalFrontierManifest {
    #[serde(rename = "final_root")]
    root: String,
    #[serde(rename = "final_state_bytes")]
    state_bytes: u64,
    #[serde(rename = "final_state_sha256")]
    state_sha256: String,
}

impl CanonicalFrontierManifest {
    fn from_admitted(frontier: &CanonicalFrontierAdmission) -> Self {
        Self {
            root: hex::encode(frontier.final_root),
            state_bytes: u64::try_from(frontier.final_state.len()).unwrap_or(u64::MAX),
            state_sha256: sha256_hex(&frontier.final_state),
        }
    }

    fn validate(&self, protocol: &str) -> Result<(), StateBundleError> {
        validate_lower_hex_32(
            &self.root,
            &format!("canonical {protocol} predecessor frontier root"),
        )?;
        require_manifest_value(
            self.state_bytes
                <= u64::try_from(MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES)
                    .unwrap_or(u64::MAX),
            format!("canonical {protocol} predecessor frontier state exceeds the hard bound"),
        )?;
        validate_lower_hex_32(
            &self.state_sha256,
            &format!("canonical {protocol} predecessor frontier state SHA-256"),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalletCheckpointManifest {
    directory: String,
    store_identity: String,
    schema_version: u16,
    cold_admitted: bool,
    database_identity_sha256: String,
    chain_epoch_id: u64,
    chain_event_sequence: u64,
    event_cursor: String,
    visible_tip: BlockManifest,
    settled_tip: BlockManifest,
    source_sequence_digest: SequenceDigestManifest,
    projection_digest: String,
    projection_accumulator_sha256: String,
    row_counts: WalletRowCountsManifest,
    utxo_count: u64,
    total_value_zat: u64,
    utxo_commitment_scheme: u32,
    utxo_commitment_sha256: String,
}

impl WalletCheckpointManifest {
    fn from_admitted(wallet: &WalletAdmission) -> Result<Self, StateBundleError> {
        let ready = &wallet.ready;
        let database_identity_sha256 = wallet
            .checkpoint_database_identity_sha256
            .clone()
            .ok_or_else(|| {
                StateBundleError::manifest_contract(
                    "wallet checkpoint is missing its cold-admitted database identity",
                )
            })?;
        Ok(Self {
            directory: WALLET_CHECKPOINT_DIRECTORY_NAME.to_owned(),
            store_identity: wallet.store_identity.clone(),
            schema_version: wallet.schema_version,
            cold_admitted: true,
            database_identity_sha256,
            chain_epoch_id: ready.source_position.chain_epoch_id.value(),
            chain_event_sequence: ready.source_position.event_sequence,
            event_cursor: hex::encode(ready.source_position.event_cursor.as_bytes()),
            visible_tip: BlockManifest::from_block(ready.source_position.tip),
            settled_tip: BlockManifest::from_block(ready.settled_tip),
            source_sequence_digest: SequenceDigestManifest::from_digest(
                ready.source_sequence_digest,
            ),
            projection_digest: hex::encode(ready.projection_digest.as_bytes()),
            projection_accumulator_sha256: sha256_hex(ready.projection_accumulator.as_bytes()),
            row_counts: WalletRowCountsManifest::from_counts(ready.row_counts),
            utxo_count: ready.utxo_summary.utxo_count,
            total_value_zat: ready.utxo_summary.total_value_zat,
            utxo_commitment_scheme: ready.utxo_summary.commitment.scheme().id(),
            utxo_commitment_sha256: sha256_hex(ready.utxo_summary.commitment.accumulator()),
        })
    }

    fn validate(
        &self,
        _network: Network,
        fence: &StateBundleFence,
    ) -> Result<(), StateBundleError> {
        require_manifest_value(
            self.directory == WALLET_CHECKPOINT_DIRECTORY_NAME,
            "wallet checkpoint directory must be exactly wallet.rocksdb",
        )?;
        require_manifest_value(
            safe_relative_name(&self.directory),
            "wallet checkpoint directory is not a safe fixed name",
        )?;
        require_manifest_value(
            self.store_identity == "wallet",
            "wallet checkpoint identity must be exactly wallet",
        )?;
        require_manifest_value(
            self.schema_version == WALLET_ROCKSDB_SCHEMA_VERSION,
            "wallet checkpoint schema must be exactly 1",
        )?;
        require_manifest_value(
            self.cold_admitted,
            "wallet checkpoint was not cold-admitted",
        )?;
        validate_lower_hex_32(
            &self.database_identity_sha256,
            "wallet checkpoint database identity SHA-256",
        )?;
        require_manifest_value(
            self.chain_epoch_id == fence.chain_epoch_id,
            "wallet epoch does not match the shared fence",
        )?;
        require_manifest_value(
            self.chain_event_sequence == fence.chain_event_sequence,
            "wallet event sequence does not match the shared fence",
        )?;
        validate_event_cursor(&self.event_cursor, self.chain_event_sequence)?;
        self.visible_tip.validate("wallet visible-tip hash")?;
        self.settled_tip.validate("wallet settled-tip hash")?;
        require_manifest_value(
            self.visible_tip == fence.visible_tip,
            "wallet visible tip does not match the shared fence",
        )?;
        require_manifest_value(
            self.settled_tip == fence.settled_tip,
            "wallet settled tip does not match the shared fence",
        )?;
        self.source_sequence_digest
            .validate("wallet source sequence digest")?;
        require_manifest_value(
            self.source_sequence_digest == fence.sequence_digest,
            "wallet sequence digest does not match the shared fence",
        )?;
        validate_lower_hex_32(&self.projection_digest, "wallet projection digest")?;
        validate_lower_hex_32(
            &self.projection_accumulator_sha256,
            "wallet projection accumulator SHA-256",
        )?;
        validate_lower_hex_32(
            &self.utxo_commitment_sha256,
            "wallet UTXO commitment SHA-256",
        )?;
        require_manifest_value(
            self.utxo_commitment_scheme == 1,
            "wallet UTXO commitment scheme must be 1",
        )?;
        self.row_counts.validate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalletRowCountsManifest {
    transparent_unspent_output: u64,
    transparent_unspent_output_by_address: u64,
    transparent_spent_output: u64,
    transparent_address_transaction: u64,
    transparent_address_balance: u64,
    reorg_undo: u64,
}

impl WalletRowCountsManifest {
    const fn from_counts(counts: WalletProjectionFamilyRowCounts) -> Self {
        Self {
            transparent_unspent_output: counts.transparent_unspent_output_count,
            transparent_unspent_output_by_address: counts
                .transparent_unspent_output_by_address_count,
            transparent_spent_output: counts.transparent_spent_output_count,
            transparent_address_transaction: counts.transparent_address_transaction_count,
            transparent_address_balance: counts.transparent_address_balance_count,
            reorg_undo: counts.reorg_undo_count,
        }
    }

    fn validate(&self) -> Result<(), StateBundleError> {
        require_manifest_value(
            self.transparent_unspent_output == self.transparent_unspent_output_by_address,
            "wallet unspent primary and address-index row counts differ",
        )
    }
}

fn validate_exact_fence(
    canonical: &CanonicalAdmission,
    wallet: &WalletAdmission,
) -> Result<(), StateBundleError> {
    require_manifest_value(
        canonical.network == wallet.network,
        "canonical and wallet checkpoint networks differ",
    )?;
    let source_position = wallet.source.source_position();
    require_manifest_value(
        canonical.visible_epoch == source_position.chain_epoch_id.value(),
        "canonical and wallet checkpoint epochs differ",
    )?;
    require_manifest_value(
        canonical.visible_event_sequence == source_position.event_sequence,
        "canonical and wallet checkpoint event sequences differ",
    )?;
    require_manifest_value(
        canonical.visible_tip == source_position.tip,
        "canonical and wallet checkpoint visible tips differ",
    )?;
    require_manifest_value(
        canonical.visible_sequence_digest == wallet.source.source_sequence_digest(),
        "canonical and wallet checkpoint sequence digests differ",
    )?;
    require_manifest_value(
        canonical.settled_tip == wallet.source.settled_tip(),
        "canonical and wallet checkpoint settled tips differ",
    )
}

fn validate_capture_candidate(
    paths: &StateBundleCapturePaths,
    canonical_candidate_id: &str,
) -> Result<(), StateBundleError> {
    let root = resolve_existing_directory(&paths.root, "capture root")?;
    validate_candidate_id(&paths.candidate_id)?;
    let staging_root = root.parent().ok_or_else(|| {
        StateBundleError::manifest_contract("capture root has no configured staging parent")
    })?;
    let expected = StateBundleCapturePaths::for_candidate(staging_root, paths.candidate_id.clone());
    require_manifest_value(
        &expected == paths,
        "capture paths are not the fixed paths derived from the capture root",
    )?;
    if canonical_candidate_id != paths.candidate_id {
        return Err(StateBundleError::CanonicalCandidateMismatch {
            expected: paths.candidate_id.clone(),
            observed: canonical_candidate_id.to_owned(),
        });
    }
    require_directory(&paths.canonical_checkpoint, "canonical checkpoint")?;
    require_absent(&paths.wallet_checkpoint, "wallet checkpoint")?;
    require_absent(&paths.manifest, "state-bundle manifest")?;
    require_absent(
        &paths.root.join(MANIFEST_TEMPORARY_FILE_NAME),
        "temporary state-bundle manifest",
    )?;
    validate_root_entries(&root, &[CANONICAL_CHECKPOINT_DIRECTORY_NAME])
}

fn validate_complete_layout(
    root: &Path,
    additional_root_entries: &[&str],
) -> Result<(), StateBundleError> {
    require_directory(
        &root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME),
        "canonical checkpoint",
    )?;
    require_directory(
        &root.join(WALLET_CHECKPOINT_DIRECTORY_NAME),
        "wallet checkpoint",
    )?;
    let mut allowed = vec![
        CANONICAL_CHECKPOINT_DIRECTORY_NAME,
        WALLET_CHECKPOINT_DIRECTORY_NAME,
        STATE_BUNDLE_MANIFEST_FILE_NAME,
    ];
    allowed.extend_from_slice(additional_root_entries);
    validate_root_entries(root, &allowed)
}

fn validate_root_entries(root: &Path, allowed: &[&str]) -> Result<(), StateBundleError> {
    let entries = fs::read_dir(root).map_err(|source| StateBundleError::io(root, source))?;
    for entry in entries {
        let entry = entry.map_err(|source| StateBundleError::io(root, source))?;
        if !allowed.iter().any(|allowed| entry.file_name() == *allowed) {
            return Err(StateBundleError::UnexpectedEntry {
                path: root.to_path_buf(),
                entry: entry.file_name(),
            });
        }
    }
    Ok(())
}

fn publish_manifest_last(
    paths: &StateBundleCapturePaths,
    manifest: &StateBundleManifest,
) -> Result<(), StateBundleError> {
    validate_complete_checkpoint_pair(paths)?;
    let encoded =
        serde_json::to_vec_pretty(manifest).map_err(|source| StateBundleError::ManifestDecode {
            path: paths.manifest.clone(),
            source,
        })?;
    let temporary = paths.root.join(MANIFEST_TEMPORARY_FILE_NAME);
    let mut file = open_new_file(&temporary)?;
    file.write_all(&encoded)
        .map_err(|source| StateBundleError::io(&temporary, source))?;
    file.write_all(b"\n")
        .map_err(|source| StateBundleError::io(&temporary, source))?;
    file.sync_all()
        .map_err(|source| StateBundleError::io(&temporary, source))?;
    drop(file);
    fs::hard_link(&temporary, &paths.manifest)
        .map_err(|source| StateBundleError::io(&paths.manifest, source))?;
    fs::remove_file(&temporary).map_err(|source| StateBundleError::io(&temporary, source))?;
    File::open(&paths.root)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| StateBundleError::io(&paths.root, source))
}

fn validate_complete_checkpoint_pair(
    paths: &StateBundleCapturePaths,
) -> Result<(), StateBundleError> {
    require_directory(&paths.canonical_checkpoint, "canonical checkpoint")?;
    require_directory(&paths.wallet_checkpoint, "wallet checkpoint")?;
    require_absent(&paths.manifest, "state-bundle manifest")?;
    validate_root_entries(
        &paths.root,
        &[
            CANONICAL_CHECKPOINT_DIRECTORY_NAME,
            WALLET_CHECKPOINT_DIRECTORY_NAME,
        ],
    )
}

fn open_new_file(path: &Path) -> Result<File, StateBundleError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| StateBundleError::io(path, source))
}

fn resolve_existing_directory(
    path: &Path,
    purpose: &'static str,
) -> Result<PathBuf, StateBundleError> {
    validate_absolute_lexical_path(path, purpose)?;
    require_directory(path, purpose)?;
    let resolved = fs::canonicalize(path).map_err(|source| StateBundleError::io(path, source))?;
    require_directory(&resolved, purpose)?;
    Ok(resolved)
}

fn validate_absolute_lexical_path(
    path: &Path,
    purpose: &'static str,
) -> Result<(), StateBundleError> {
    if !path.is_absolute() {
        return Err(StateBundleError::UnsafePath {
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
        return Err(StateBundleError::UnsafePath {
            path: path.to_path_buf(),
            purpose,
            reason: "path must not contain traversal or platform prefixes",
        });
    }
    Ok(())
}

fn require_directory(path: &Path, purpose: &'static str) -> Result<(), StateBundleError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| StateBundleError::io(path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(StateBundleError::UnsafePath {
            path: path.to_path_buf(),
            purpose,
            reason: "path must be a real directory, not a symlink or another file type",
        });
    }
    Ok(())
}

fn require_regular_file(path: &Path, purpose: &'static str) -> Result<(), StateBundleError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| StateBundleError::io(path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(StateBundleError::UnsafePath {
            path: path.to_path_buf(),
            purpose,
            reason: "path must be a regular file, not a symlink or another file type",
        });
    }
    Ok(())
}

fn require_absent(path: &Path, purpose: &'static str) -> Result<(), StateBundleError> {
    match fs::symlink_metadata(path) {
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(StateBundleError::UnsafePath {
            path: path.to_path_buf(),
            purpose,
            reason: "path must be absent",
        }),
        Err(source) => Err(StateBundleError::io(path, source)),
    }
}

fn safe_relative_name(name: &str) -> bool {
    let path = Path::new(name);
    !path.is_absolute()
        && path.components().count() == 1
        && matches!(path.components().next(), Some(Component::Normal(_)))
}

fn validate_candidate_id(candidate_id: &str) -> Result<(), StateBundleError> {
    let bytes = candidate_id.as_bytes();
    let has_valid_length = (1..=64).contains(&bytes.len());
    let has_valid_edges = bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric);
    let has_valid_body = bytes
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-');
    require_manifest_value(
        has_valid_length && has_valid_edges && has_valid_body,
        "candidate id must be 1-64 lowercase ASCII letters, digits, or hyphens and begin and end with an alphanumeric character",
    )
}

fn decode_checkpoint_block(
    block: Option<zinder_proto::v1::ingest::CanonicalCheckpointBlockId>,
    field: &str,
) -> Result<BlockId, StateBundleError> {
    let block =
        block.ok_or_else(|| StateBundleError::manifest_contract(format!("{field} is absent")))?;
    decode_block(block.height, &block.hash, field)
}

fn decode_build_plan(
    evidence: zinder_proto::v1::ingest::CanonicalOwnerCheckpointBuildPlanEvidence,
    network: Network,
    first_retained_block: BlockId,
    visible_tip: BlockId,
) -> Result<CanonicalBuildPlanAdmission, StateBundleError> {
    let activation_fingerprint_version = u16::try_from(evidence.activation_fingerprint_version)
        .map_err(|_| {
            StateBundleError::manifest_contract(
                "canonical activation fingerprint version does not fit u16",
            )
        })?;
    require_manifest_value(
        activation_fingerprint_version == 1,
        "canonical activation fingerprint version must be exactly 1",
    )?;
    let activation_fingerprint = decode_exact_32(
        &evidence.activation_fingerprint,
        "canonical activation fingerprint",
    )?;
    let raw_blob_retention = RawBlobRetention::from_kebab_case(&evidence.raw_blob_retention)
        .ok_or_else(|| {
            StateBundleError::manifest_contract(
                "canonical raw-blob retention must be none, transactions, or all",
            )
        })?;
    require_manifest_value(
        evidence.reorg_window_blocks > 0,
        "canonical build-plan reorg window must be nonzero",
    )?;
    let history = decode_history_boundary(
        evidence.history_preceding_checkpoint,
        evidence.history_predecessor,
        network,
    )?;
    let build_tip = decode_checkpoint_block(evidence.build_tip, "canonical fixed build tip")?;
    validate_build_plan_range(
        history.first_retained_height,
        first_retained_block,
        build_tip,
        visible_tip,
    )?;
    Ok(CanonicalBuildPlanAdmission {
        activation_fingerprint_version,
        activation_fingerprint,
        raw_blob_retention,
        reorg_window_blocks: evidence.reorg_window_blocks,
        history_preceding_checkpoint: history.preceding_checkpoint,
        history_predecessor: history.predecessor,
        build_tip,
    })
}

fn decode_history_boundary(
    preceding_checkpoint: Option<zinder_proto::v1::ingest::CanonicalCheckpointBlockId>,
    predecessor: Option<zinder_proto::v1::ingest::CanonicalCheckpointHistoryPredecessor>,
    network: Network,
) -> Result<DecodedHistoryBoundary, StateBundleError> {
    let preceding_checkpoint = preceding_checkpoint
        .map(|checkpoint| {
            decode_block(
                checkpoint.height,
                &checkpoint.hash,
                "canonical history preceding checkpoint",
            )
        })
        .transpose()?;
    let predecessor = predecessor.ok_or_else(|| {
        StateBundleError::manifest_contract("canonical build plan omitted its history predecessor")
    })?;
    let history_predecessor = CanonicalHistoryPredecessorAdmission {
        block_id: decode_checkpoint_block(
            predecessor.block_id,
            "canonical history predecessor block",
        )?,
        block_time_seconds: predecessor.block_time_seconds,
        sapling_frontier: decode_frontier(predecessor.sapling_frontier, "sapling")?,
        orchard_frontier: decode_frontier(predecessor.orchard_frontier, "orchard")?,
        ironwood_frontier: decode_frontier(predecessor.ironwood_frontier, "ironwood")?,
    };
    let first_retained_height = match preceding_checkpoint {
        None => {
            require_manifest_value(
                history_predecessor.block_id
                    == BlockId::new(BlockHeight::new(0), network.genesis_hash()),
                "complete canonical history predecessor must be the configured network genesis",
            )?;
            require_manifest_value(
                history_predecessor.sapling_frontier.is_none()
                    && history_predecessor.orchard_frontier.is_none()
                    && history_predecessor.ironwood_frontier.is_none(),
                "complete canonical history predecessor must not contain commitment frontiers",
            )?;
            1
        }
        Some(checkpoint) => {
            require_manifest_value(
                checkpoint.height.value() > 0,
                "checkpointed canonical history must not use genesis as its checkpoint",
            )?;
            require_manifest_value(
                history_predecessor.block_id == checkpoint,
                "canonical history predecessor does not equal its preceding checkpoint",
            )?;
            checkpoint.height.value().checked_add(1).ok_or_else(|| {
                StateBundleError::manifest_contract(
                    "canonical history checkpoint has no representable successor",
                )
            })?
        }
    };
    Ok(DecodedHistoryBoundary {
        preceding_checkpoint,
        predecessor: history_predecessor,
        first_retained_height,
    })
}

fn validate_build_plan_range(
    expected_first_height: u32,
    first_retained_block: BlockId,
    build_tip: BlockId,
    visible_tip: BlockId,
) -> Result<(), StateBundleError> {
    require_manifest_value(
        first_retained_block.height.value() == expected_first_height,
        "canonical first retained block does not match the build-plan history boundary",
    )?;
    require_manifest_value(
        build_tip.height.value() >= expected_first_height,
        "canonical fixed build tip precedes the first retained block",
    )?;
    require_manifest_value(
        build_tip.height <= visible_tip.height,
        "canonical fixed build tip exceeds the checkpoint visible tip",
    )?;
    if build_tip.height == visible_tip.height {
        require_manifest_value(
            build_tip.hash == visible_tip.hash,
            "canonical fixed build tip hash differs from the equal-height visible tip",
        )?;
    }
    if first_retained_block.height == build_tip.height {
        require_manifest_value(
            first_retained_block.hash == build_tip.hash,
            "canonical first retained block hash differs from the equal-height fixed build tip",
        )?;
    }
    Ok(())
}

fn decode_frontier(
    frontier: Option<zinder_proto::v1::ingest::CanonicalCheckpointFrontier>,
    protocol: &str,
) -> Result<Option<CanonicalFrontierAdmission>, StateBundleError> {
    frontier
        .map(|frontier| {
            let final_root = decode_exact_32(
                &frontier.final_root,
                &format!("canonical {protocol} predecessor frontier root"),
            )?;
            require_manifest_value(
                frontier.final_state.len() <= MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES,
                format!("canonical {protocol} predecessor frontier state exceeds the hard bound"),
            )?;
            Ok(CanonicalFrontierAdmission {
                final_root,
                final_state: frontier.final_state,
            })
        })
        .transpose()
}

fn decode_block(height: u32, hash: &[u8], field: &str) -> Result<BlockId, StateBundleError> {
    let hash = decode_exact_32(hash, &format!("{field} hash"))?;
    Ok(BlockId::new(
        BlockHeight::new(height),
        BlockHash::from_bytes(hash),
    ))
}

fn decode_exact_32(bytes: &[u8], field: &str) -> Result<[u8; 32], StateBundleError> {
    bytes.try_into().map_err(|_| {
        StateBundleError::manifest_contract(format!("{field} must contain exactly 32 bytes"))
    })
}

fn require_manifest_value(
    condition: bool,
    reason: impl Into<String>,
) -> Result<(), StateBundleError> {
    if condition {
        Ok(())
    } else {
        Err(StateBundleError::manifest_contract(reason))
    }
}

fn validate_event_cursor(cursor: &str, event_sequence: u64) -> Result<(), StateBundleError> {
    let bytes = hex::decode(cursor).map_err(|_| {
        StateBundleError::manifest_contract("wallet event cursor must be lowercase hexadecimal")
    })?;
    require_manifest_value(
        hex::encode(&bytes) == cursor,
        "wallet event cursor must use canonical lowercase hexadecimal",
    )?;
    require_manifest_value(
        bytes.len() == 9 && bytes[0] == 1,
        "wallet event cursor must be exact format 1",
    )?;
    let sequence_bytes: [u8; 8] = bytes[1..].try_into().map_err(|_| {
        StateBundleError::manifest_contract("wallet event cursor sequence has the wrong length")
    })?;
    require_manifest_value(
        u64::from_be_bytes(sequence_bytes) == event_sequence,
        "wallet event cursor does not encode the manifest event sequence",
    )
}

fn validate_lower_hex_32(encoded_hex: &str, field: &str) -> Result<(), StateBundleError> {
    let mut bytes = [0_u8; 32];
    hex::decode_to_slice(encoded_hex, &mut bytes).map_err(|_| {
        StateBundleError::manifest_contract(format!("{field} must be exactly 32-byte hexadecimal"))
    })?;
    require_manifest_value(
        hex::encode(bytes) == encoded_hex,
        format!("{field} must use canonical lowercase hexadecimal"),
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

const fn network_name(network: Network) -> &'static str {
    encode_zinder_native_chain_name(network)
}

fn parse_network(network: &str) -> Option<Network> {
    decode_zinder_native_chain_name(network).ok()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::Value;
    use tempfile::TempDir;
    use zinder_core::{
        BlockHash, BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
        CanonicalBlockFactsSequenceDigestVersion, ChainEpochId, Network, NetworkUpgradeActivations,
    };
    use zinder_proto::v1::ingest::{
        CanonicalCheckpointBlockId, CanonicalCheckpointFrontier,
        CanonicalCheckpointHistoryPredecessor, CanonicalCheckpointSequenceEvidence,
        CanonicalOwnerCheckpointBuildPlanEvidence, CanonicalOwnerCheckpointReadyEvidence,
        CanonicalWriterFence, CreateCanonicalOwnerCheckpointResponse,
    };
    use zinder_store::CanonicalStoreWorkload;
    use zinder_wallet_projection::{
        WalletCanonicalSourceIdentity, WalletProjectionAccumulator, WalletProjectionDigest,
        WalletProjectionEventCursor, WalletProjectionFamilyRowCounts,
        WalletProjectionReadyEvidence, WalletProjectionSourcePosition, WalletUtxoSetSummary,
    };

    use super::*;

    #[test]
    fn capture_target_rejects_traversal_and_preserves_existing_contents()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        assert!(matches!(
            prepare_state_bundle_capture(temporary.path(), "../bundle"),
            Err(StateBundleError::ManifestContract { .. })
        ));

        let existing = temporary.path().join("existing");
        fs::create_dir(&existing)?;
        let sentinel = existing.join("sentinel");
        fs::write(&sentinel, b"preserve")?;
        assert!(matches!(
            prepare_state_bundle_capture(temporary.path(), "existing"),
            Err(StateBundleError::TargetExists { .. })
        ));
        assert_eq!(fs::read(&sentinel)?, b"preserve");
        Ok(())
    }

    #[test]
    fn manifest_is_published_last_and_round_trips_the_exact_shared_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let paths = prepare_state_bundle_capture(temporary.path(), "bundle-a")?;
        fs::create_dir(paths.canonical_checkpoint())?;
        fs::create_dir(paths.wallet_checkpoint())?;
        assert!(!paths.manifest().exists());

        let canonical = sample_canonical_admission(7);
        let wallet = sample_wallet_admission(7)?;
        let manifest = StateBundleManifest::from_admitted("bundle-a", &canonical, &wallet)?;
        publish_manifest_last(&paths, &manifest)?;

        let admitted = StateBundleManifest::read(paths.root(), Network::ZcashRegtest)?;
        assert_eq!(admitted, manifest);
        assert_eq!(admitted.identity, STATE_BUNDLE_IDENTITY);
        assert_eq!(admitted.format_version, STATE_BUNDLE_FORMAT_VERSION);
        assert_eq!(admitted.topology, STATE_BUNDLE_TOPOLOGY);
        assert_eq!(admitted.fence.chain_epoch_id(), 7);
        assert_eq!(admitted.fence.chain_event_sequence(), 7);
        Ok(())
    }

    #[test]
    fn legacy_format_two_manifest_without_schedule_identity_remains_readable()
    -> Result<(), Box<dyn std::error::Error>> {
        let manifest = StateBundleManifest::from_admitted(
            "bundle-a",
            &sample_canonical_admission(7),
            &sample_wallet_admission(7)?,
        )?;
        let mut legacy = serde_json::to_value(manifest)?;
        legacy["format_version"] = Value::from(2);
        legacy
            .as_object_mut()
            .and_then(|object| object.get_mut("fence"))
            .and_then(Value::as_object_mut)
            .ok_or("manifest fence must be an object")?
            .remove("network_upgrade_schedule");

        let decoded: StateBundleManifest = serde_json::from_value(legacy)?;
        decoded.validate(Network::ZcashRegtest)?;
        assert_eq!(decoded.format_version(), 2);
        assert!(decoded.fence().network_upgrade_schedule().is_none());
        Ok(())
    }

    #[test]
    fn raised_reorg_window_is_recorded_only_when_it_differs_from_construction()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut raised = sample_canonical_control_response(7);
        raised.effective_reorg_window_blocks = Some(1_000);
        let admitted = CanonicalCheckpointAdmissionEvidence::try_from(raised.clone())?;
        assert_eq!(
            admitted.admission.effective_reorg_window_blocks,
            Some(1_000)
        );
        let manifest = StateBundleManifest::from_admitted(
            "bundle-a",
            &admitted.admission,
            &sample_wallet_admission(7)?,
        )?;
        let encoded = serde_json::to_value(&manifest)?;
        assert_eq!(
            encoded["canonical_checkpoint"]["effective_reorg_window_blocks"],
            Value::from(1_000)
        );
        let decoded: StateBundleManifest = serde_json::from_value(encoded)?;
        decoded.validate(Network::ZcashRegtest)?;
        assert_eq!(decoded, manifest);

        // An unraised checkpoint keeps the earlier manifest shape.
        let unraised = StateBundleManifest::from_admitted(
            "bundle-a",
            &sample_canonical_admission(7),
            &sample_wallet_admission(7)?,
        )?;
        assert!(
            serde_json::to_value(&unraised)?["canonical_checkpoint"]
                .get("effective_reorg_window_blocks")
                .is_none()
        );

        for not_raised in [100, 99] {
            let mut lowered = raised.clone();
            lowered.effective_reorg_window_blocks = Some(not_raised);
            assert!(CanonicalCheckpointAdmissionEvidence::try_from(lowered).is_err());
        }
        Ok(())
    }

    #[test]
    fn manifest_admission_rejects_identity_version_topology_schema_network_and_fence_corruption()
    -> Result<(), Box<dyn std::error::Error>> {
        let manifest = StateBundleManifest::from_admitted(
            "bundle-a",
            &sample_canonical_admission(7),
            &sample_wallet_admission(7)?,
        )?;
        let valid = serde_json::to_value(manifest)?;
        let unsupported_construction_manifest_version =
            CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION
                .checked_add(1)
                .ok_or("construction manifest test version overflow")?;
        let mutations: [(&str, Value); 20] = [
            ("/identity", Value::String("state_bundle".to_owned())),
            ("/format_version", Value::from(1)),
            ("/format_version", Value::from(2)),
            ("/topology", Value::String("mixed-runtime".to_owned())),
            ("/candidate_id", Value::String("../outside".to_owned())),
            ("/network", Value::String("mainnet".to_owned())),
            ("/canonical_checkpoint/schema_version", Value::from(3)),
            (
                "/canonical_checkpoint/database_identity_sha256",
                Value::String("not-a-sha256".to_owned()),
            ),
            (
                "/canonical_checkpoint/construction_manifest_version",
                Value::from(unsupported_construction_manifest_version),
            ),
            (
                "/canonical_checkpoint/construction_manifest_sha256",
                Value::String("not-a-sha256".to_owned()),
            ),
            (
                "/canonical_checkpoint/build_plan/activation_fingerprint_version",
                Value::from(2),
            ),
            (
                "/canonical_checkpoint/build_plan/reorg_window_blocks",
                Value::from(0),
            ),
            (
                "/canonical_checkpoint/build_plan/raw_blob_retention",
                Value::String("headers".to_owned()),
            ),
            (
                "/canonical_checkpoint/build_plan/history_predecessor/block_id/hash",
                Value::String(hex::encode([0x99; 32])),
            ),
            ("/wallet_checkpoint/schema_version", Value::from(2)),
            (
                "/wallet_checkpoint/database_identity_sha256",
                Value::String("not-a-sha256".to_owned()),
            ),
            ("/wallet_checkpoint/chain_epoch_id", Value::from(8)),
            ("/fence/network_upgrade_schedule/revision", Value::from(0)),
            (
                "/fence/network_upgrade_schedule/fingerprint",
                Value::String("not-a-sha256".to_owned()),
            ),
            (
                "/wallet_checkpoint/source_sequence_digest/sha256",
                Value::String(hex::encode([0x99; 32])),
            ),
        ];

        for (pointer, replacement) in mutations {
            let mut corrupted = valid.clone();
            let field = corrupted
                .pointer_mut(pointer)
                .ok_or("test mutation pointer must resolve")?;
            *field = replacement;
            let rejected = serde_json::from_value::<StateBundleManifest>(corrupted)
                .map_or(true, |decoded| {
                    decoded.validate(Network::ZcashRegtest).is_err()
                });
            assert!(rejected, "corruption at {pointer} was admitted");
        }
        Ok(())
    }

    #[test]
    fn manifest_admission_bounds_inner_manifest_bytes_before_json_decode()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        fs::create_dir(temporary.path().join(CANONICAL_CHECKPOINT_DIRECTORY_NAME))?;
        fs::create_dir(temporary.path().join(WALLET_CHECKPOINT_DIRECTORY_NAME))?;
        let oversized = usize::try_from(STATE_BUNDLE_MANIFEST_MAX_BYTES)?
            .checked_add(1)
            .ok_or("test manifest length overflow")?;
        fs::write(
            temporary.path().join(STATE_BUNDLE_MANIFEST_FILE_NAME),
            vec![b'x'; oversized],
        )?;

        assert!(StateBundleManifest::read(temporary.path(), Network::ZcashRegtest).is_err());
        Ok(())
    }

    #[test]
    fn wallet_checkpoint_identity_is_bound_in_the_serialized_manifest()
    -> Result<(), Box<dyn std::error::Error>> {
        let sample = sample_wallet_admission(7)?;
        let evidence = WalletOwnerCheckpointEvidence {
            database_identity: vec![0x42; 16],
            store_identity: WALLET_PROJECTION_STORE_IDENTITY,
            schema_version: WALLET_ROCKSDB_SCHEMA_VERSION,
            network: sample.network,
            ready_evidence: sample.ready,
        };
        let wallet = WalletAdmission::try_from(&evidence)?;
        let manifest = StateBundleManifest::from_admitted(
            "bundle-a",
            &sample_canonical_admission(7),
            &wallet,
        )?;
        let encoded = serde_json::to_value(&manifest)?;
        let expected_identity_hash = sha256_hex(&evidence.database_identity);
        assert_eq!(
            encoded
                .pointer("/wallet_checkpoint/database_identity_sha256")
                .and_then(Value::as_str),
            Some(expected_identity_hash.as_str())
        );

        let mut corrupted = encoded;
        let identity = corrupted
            .pointer_mut("/wallet_checkpoint/database_identity_sha256")
            .ok_or("identity hash must be serialized")?;
        *identity = Value::String("not-a-sha256".to_owned());
        let decoded: StateBundleManifest = serde_json::from_value(corrupted)?;
        assert!(decoded.validate(Network::ZcashRegtest).is_err());
        Ok(())
    }

    #[test]
    fn canonical_control_response_is_the_owned_fail_closed_admission_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = sample_canonical_control_response(7);
        let admitted = CanonicalCheckpointAdmissionEvidence::try_from(response.clone())?;
        assert_eq!(admitted.candidate_id(), "bundle-a");
        assert_eq!(admitted.admission, sample_canonical_admission(7));

        assert_checkpoint_response_rejects_invalid_identity(&response);
        assert_checkpoint_response_rejects_invalid_build_plan(&response)?;
        assert_checkpoint_response_rejects_invalid_ready_evidence(response)?;
        Ok(())
    }

    fn assert_checkpoint_response_rejects_invalid_identity(
        response: &CreateCanonicalOwnerCheckpointResponse,
    ) {
        let mut wrong_schema = response.clone();
        wrong_schema.schema_version = 3;
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(wrong_schema).is_err());

        let mut missing_ready = response.clone();
        missing_ready.ready_evidence = None;
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(missing_ready).is_err());

        let mut missing_build_plan = response.clone();
        missing_build_plan.build_plan = None;
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(missing_build_plan).is_err());

        let mut missing_database_identity = response.clone();
        missing_database_identity.database_identity.clear();
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(missing_database_identity).is_err());

        let mut missing_schedule = response.clone();
        missing_schedule.network_upgrade_schedule = None;
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(missing_schedule).is_err());

        let mut zero_schedule_revision = response.clone();
        if let Some(schedule) = zero_schedule_revision.network_upgrade_schedule.as_mut() {
            schedule.revision = 0;
        }
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(zero_schedule_revision).is_err());

        let mut mismatched_schedule_fingerprint = response.clone();
        if let Some(schedule) = mismatched_schedule_fingerprint
            .network_upgrade_schedule
            .as_mut()
            && let Some(first) = schedule.fingerprint.first_mut()
        {
            *first ^= 1;
        }
        assert!(
            CanonicalCheckpointAdmissionEvidence::try_from(mismatched_schedule_fingerprint)
                .is_err()
        );
    }

    fn assert_checkpoint_response_rejects_invalid_build_plan(
        response: &CreateCanonicalOwnerCheckpointResponse,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut wrong_fingerprint_version = response.clone();
        wrong_fingerprint_version
            .build_plan
            .as_mut()
            .ok_or("sample response must contain build-plan evidence")?
            .activation_fingerprint_version = 2;
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(wrong_fingerprint_version).is_err());

        let mut malformed_fingerprint = response.clone();
        malformed_fingerprint
            .build_plan
            .as_mut()
            .ok_or("sample response must contain build-plan evidence")?
            .activation_fingerprint = vec![0xaa; 31];
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(malformed_fingerprint).is_err());

        let mut zero_reorg_window = response.clone();
        zero_reorg_window
            .build_plan
            .as_mut()
            .ok_or("sample response must contain build-plan evidence")?
            .reorg_window_blocks = 0;
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(zero_reorg_window).is_err());

        let mut unknown_raw_blob_retention = response.clone();
        unknown_raw_blob_retention
            .build_plan
            .as_mut()
            .ok_or("sample response must contain build-plan evidence")?
            .raw_blob_retention = "headers".to_owned();
        assert!(
            CanonicalCheckpointAdmissionEvidence::try_from(unknown_raw_blob_retention).is_err()
        );

        let mut wrong_predecessor = response.clone();
        wrong_predecessor
            .build_plan
            .as_mut()
            .and_then(|plan| plan.history_predecessor.as_mut())
            .and_then(|predecessor| predecessor.block_id.as_mut())
            .ok_or("sample response must contain a history predecessor")?
            .hash = vec![0x99; 32];
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(wrong_predecessor).is_err());

        let mut malformed_frontier = response.clone();
        let predecessor = malformed_frontier
            .build_plan
            .as_mut()
            .and_then(|plan| plan.history_predecessor.as_mut())
            .ok_or("sample response must contain a history predecessor")?;
        predecessor.sapling_frontier = Some(CanonicalCheckpointFrontier {
            final_root: vec![0x77; 31],
            final_state: vec![0x88; 16],
        });
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(malformed_frontier).is_err());

        let mut missing_build_tip = response.clone();
        missing_build_tip
            .build_plan
            .as_mut()
            .ok_or("sample response must contain build-plan evidence")?
            .build_tip = None;
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(missing_build_tip).is_err());
        Ok(())
    }

    fn assert_checkpoint_response_rejects_invalid_ready_evidence(
        response: CreateCanonicalOwnerCheckpointResponse,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut malformed_digest = response;
        malformed_digest
            .ready_evidence
            .as_mut()
            .ok_or("sample response must contain READY evidence")?
            .visible_fence
            .as_mut()
            .ok_or("sample READY evidence must contain a visible fence")?
            .canonical_sequence_digest = vec![0x44; 31];
        assert!(CanonicalCheckpointAdmissionEvidence::try_from(malformed_digest).is_err());

        let mut wrong_construction_version = sample_canonical_control_response(7);
        let unsupported_construction_manifest_version =
            CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION
                .checked_add(1)
                .ok_or("construction manifest test version overflow")?;
        wrong_construction_version
            .ready_evidence
            .as_mut()
            .ok_or("sample response must contain READY evidence")?
            .construction_manifest_version = u32::from(unsupported_construction_manifest_version);
        assert!(
            CanonicalCheckpointAdmissionEvidence::try_from(wrong_construction_version).is_err()
        );

        let mut malformed_construction_digest = sample_canonical_control_response(7);
        malformed_construction_digest
            .ready_evidence
            .as_mut()
            .ok_or("sample response must contain READY evidence")?
            .construction_manifest_sha256
            .truncate(31);
        assert!(
            CanonicalCheckpointAdmissionEvidence::try_from(malformed_construction_digest).is_err()
        );
        Ok(())
    }

    #[test]
    fn captured_manifest_remains_immutable_when_both_live_sources_advance()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let paths = prepare_state_bundle_capture(temporary.path(), "bundle-a")?;
        fs::create_dir(paths.canonical_checkpoint())?;
        fs::create_dir(paths.wallet_checkpoint())?;
        let captured = StateBundleManifest::from_admitted(
            "bundle-a",
            &sample_canonical_admission(7),
            &sample_wallet_admission(7)?,
        )?;
        publish_manifest_last(&paths, &captured)?;
        let before = fs::read(paths.manifest())?;

        let advanced = StateBundleManifest::from_admitted(
            "bundle-a",
            &sample_canonical_admission(8),
            &sample_wallet_admission(8)?,
        )?;
        assert_ne!(advanced.fence, captured.fence);
        assert_eq!(fs::read(paths.manifest())?, before);
        assert_eq!(
            StateBundleManifest::read(paths.root(), Network::ZcashRegtest)?,
            captured
        );
        Ok(())
    }

    #[test]
    fn recovery_archive_refuses_a_source_without_a_valid_construction_manifest_sidecar()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let staging_root = temporary.path().join("staging");
        let archive_root = temporary.path().join("archives");
        fs::create_dir(&staging_root)?;
        fs::create_dir(&archive_root)?;
        let paths = prepare_state_bundle_capture(&staging_root, "bundle-a")?;
        fs::create_dir(paths.canonical_checkpoint())?;
        fs::create_dir(paths.wallet_checkpoint())?;
        fs::write(
            paths.canonical_checkpoint().join("000001.sst"),
            b"canonical",
        )?;
        fs::write(paths.wallet_checkpoint().join("000001.sst"), b"wallet")?;
        let manifest = StateBundleManifest::from_admitted(
            "bundle-a",
            &sample_canonical_admission(7),
            &sample_wallet_admission(7)?,
        )?;
        publish_manifest_last(&paths, &manifest)?;

        assert!(
            crate::recovery_archive::package_recovery_archive(
                &archive_root,
                &paths,
                Network::ZcashRegtest,
            )
            .is_err()
        );
        assert!(!archive_root.join("bundle-a").exists());
        Ok(())
    }

    #[test]
    fn wallet_checkpoint_is_refused_when_its_exact_fence_differs()
    -> Result<(), Box<dyn std::error::Error>> {
        let canonical = sample_canonical_admission(7);
        let wallet = sample_wallet_admission(8)?;
        assert!(matches!(
            validate_exact_fence(&canonical, &wallet),
            Err(StateBundleError::ManifestContract { .. })
        ));
        Ok(())
    }

    fn sample_canonical_admission(sequence: u64) -> CanonicalAdmission {
        let visible_tip = block(sequence);
        let settled_tip = block(sequence.saturating_sub(1));
        let digest = sequence_digest(sequence, 0x44);
        let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);
        let activation_fingerprint =
            activations.fingerprint(zinder_core::NetworkUpgradeActivationsFingerprintVersion::V1);
        CanonicalAdmission {
            database_identity_sha256: sha256_hex(b"sample-canonical-database-identity"),
            construction_manifest_version: CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION,
            construction_manifest_sha256: [0xbb; 32],
            store_identity: CANONICAL_STORE_IDENTITY.to_owned(),
            schema_version: CANONICAL_STORE_SCHEMA_VERSION,
            network: Network::ZcashRegtest,
            workload: CanonicalStoreWorkload::Wallet,
            effective_reorg_window_blocks: None,
            build_plan: CanonicalBuildPlanAdmission {
                activation_fingerprint_version: 1,
                activation_fingerprint: activation_fingerprint.as_bytes(),
                raw_blob_retention: RawBlobRetention::Transactions,
                reorg_window_blocks: 100,
                history_preceding_checkpoint: None,
                history_predecessor: CanonicalHistoryPredecessorAdmission {
                    block_id: BlockId::new(
                        BlockHeight::new(0),
                        Network::ZcashRegtest.genesis_hash(),
                    ),
                    block_time_seconds: 1_234,
                    sapling_frontier: None,
                    orchard_frontier: None,
                    ironwood_frontier: None,
                },
                build_tip: visible_tip,
            },
            admitted_schedule: AdmittedScheduleIdentity {
                revision: 1,
                fingerprint: activation_fingerprint.as_bytes(),
            },
            first_retained_block: block(1),
            visible_tip,
            visible_epoch: sequence,
            visible_event_sequence: sequence,
            visible_block_count: sequence,
            block_digest_version: 1,
            replay_format_version: 1,
            visible_sequence_digest: digest,
            visible_logical_replay_bytes: sequence * 100,
            settled_tip,
            settled_retained_block_count: sequence.saturating_sub(1),
            settled_sequence_digest: sequence_digest(sequence.saturating_sub(1), 0x33),
            settled_logical_replay_bytes: sequence.saturating_sub(1) * 100,
        }
    }

    fn sample_canonical_control_response(sequence: u64) -> CreateCanonicalOwnerCheckpointResponse {
        let canonical = sample_canonical_admission(sequence);
        CreateCanonicalOwnerCheckpointResponse {
            candidate_id: "bundle-a".to_owned(),
            database_identity: b"sample-canonical-database-identity".to_vec(),
            store_identity: canonical.store_identity.clone(),
            schema_version: u32::from(canonical.schema_version),
            workload: canonical.workload.as_str().to_owned(),
            network_name: network_name(canonical.network).to_owned(),
            effective_reorg_window_blocks: None,
            network_upgrade_schedule: Some(
                zinder_proto::wire::encode_canonical_network_upgrade_schedule(
                    &NetworkUpgradeActivations::empty(canonical.network),
                    canonical.admitted_schedule.revision,
                ),
            ),
            ready_evidence: Some(CanonicalOwnerCheckpointReadyEvidence {
                first_retained_block: Some(checkpoint_block(canonical.first_retained_block)),
                visible_fence: Some(CanonicalWriterFence {
                    chain_epoch_id: canonical.visible_epoch,
                    event_sequence: canonical.visible_event_sequence,
                    visible_tip_height: canonical.visible_tip.height.value(),
                    visible_tip_hash: canonical.visible_tip.hash.as_bytes().to_vec(),
                    canonical_sequence_digest: canonical
                        .visible_sequence_digest
                        .as_bytes()
                        .to_vec(),
                    visible_block_count: canonical.visible_block_count,
                }),
                block_digest_version: u32::from(canonical.block_digest_version),
                replay_format_version: canonical.replay_format_version,
                sequence_digest_version: u32::from(
                    canonical.visible_sequence_digest.version().value(),
                ),
                visible_logical_replay_bytes: canonical.visible_logical_replay_bytes,
                sequence_checkpoint: Some(CanonicalCheckpointSequenceEvidence {
                    through: Some(checkpoint_block(canonical.settled_tip)),
                    retained_block_count: canonical.settled_retained_block_count,
                    sequence_digest: canonical.settled_sequence_digest.as_bytes().to_vec(),
                    logical_replay_bytes: canonical.settled_logical_replay_bytes,
                }),
                construction_manifest_version: u32::from(canonical.construction_manifest_version),
                construction_manifest_sha256: canonical.construction_manifest_sha256.to_vec(),
            }),
            build_plan: Some(CanonicalOwnerCheckpointBuildPlanEvidence {
                activation_fingerprint_version: u32::from(
                    canonical.build_plan.activation_fingerprint_version,
                ),
                activation_fingerprint: canonical.build_plan.activation_fingerprint.to_vec(),
                raw_blob_retention: canonical
                    .build_plan
                    .raw_blob_retention
                    .as_kebab_case()
                    .to_owned(),
                reorg_window_blocks: canonical.build_plan.reorg_window_blocks,
                history_preceding_checkpoint: canonical
                    .build_plan
                    .history_preceding_checkpoint
                    .map(checkpoint_block),
                history_predecessor: Some(CanonicalCheckpointHistoryPredecessor {
                    block_id: Some(checkpoint_block(
                        canonical.build_plan.history_predecessor.block_id,
                    )),
                    block_time_seconds: canonical.build_plan.history_predecessor.block_time_seconds,
                    sapling_frontier: None,
                    orchard_frontier: None,
                    ironwood_frontier: None,
                }),
                build_tip: Some(checkpoint_block(canonical.build_plan.build_tip)),
            }),
        }
    }

    fn checkpoint_block(block: BlockId) -> CanonicalCheckpointBlockId {
        CanonicalCheckpointBlockId {
            height: block.height.value(),
            hash: block.hash.as_bytes().to_vec(),
        }
    }

    fn sample_wallet_admission(
        sequence: u64,
    ) -> Result<WalletAdmission, Box<dyn std::error::Error>> {
        let source_position = WalletProjectionSourcePosition::with_event_cursor(
            ChainEpochId::new(sequence),
            block(sequence),
            sequence,
            WalletProjectionEventCursor::from_bytes(event_cursor(sequence))?,
        )?;
        let source = WalletCanonicalSourceIdentity::new(
            source_position,
            sequence_digest(sequence, 0x44),
            block(sequence.saturating_sub(1)),
        );
        let ready = WalletProjectionReadyEvidence {
            source_position,
            source_sequence_digest: source.source_sequence_digest(),
            settled_tip: source.settled_tip(),
            projection_digest: WalletProjectionDigest::from_bytes([0x55; 32]),
            projection_accumulator: WalletProjectionAccumulator::empty(),
            row_counts: WalletProjectionFamilyRowCounts::default(),
            utxo_summary: WalletUtxoSetSummary {
                utxo_count: 0,
                total_value_zat: 0,
                commitment: zinder_core::TransparentUtxoSetCommitment::empty(),
            },
        };
        Ok(WalletAdmission {
            store_identity: "wallet".to_owned(),
            schema_version: WALLET_ROCKSDB_SCHEMA_VERSION,
            network: Network::ZcashRegtest,
            source,
            ready,
            checkpoint_database_identity_sha256: Some(sha256_hex(&[0x99; 16])),
        })
    }

    fn block(sequence: u64) -> BlockId {
        let height = u32::try_from(sequence).unwrap_or(u32::MAX);
        let byte = u8::try_from(sequence).unwrap_or(u8::MAX);
        BlockId::new(BlockHeight::new(height), BlockHash::from_bytes([byte; 32]))
    }

    fn sequence_digest(block_count: u64, byte: u8) -> CanonicalBlockFactsSequenceDigest {
        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            block_count,
            [byte; 32],
        )
    }

    fn event_cursor(sequence: u64) -> [u8; 9] {
        let mut cursor = [0_u8; 9];
        cursor[0] = 1;
        cursor[1..].copy_from_slice(&sequence.to_be_bytes());
        cursor
    }
}
