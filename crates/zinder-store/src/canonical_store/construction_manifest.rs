//! Immutable evidence for one fresh canonical construction.
//!
//! The manifest is deliberately a narrow sidecar, not a generic snapshot
//! format. It is written and synced before the first READY transition, and
//! READY stores its exact version and digest forever. Following may advance
//! the store, but it never rewrites the construction proof.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zinder_core::{BlockId, CommitmentTreeCheckpoint, CommitmentTreeFrontier, ShieldedProtocol};
use zinder_rocksdb_bulk_load::{
    OrderedKeyValueEvidence, OrderedKeyValueEvidenceAccumulator, SstFileEvidence,
};

use super::{
    CanonicalBlockLoadEvidence, CanonicalStoreBuildPlan, CanonicalStoreError,
    CanonicalStoreReadyEvidence, CanonicalStoreWorkload,
    block_load::CanonicalStagedSstEvidence,
    subtree_load::{CanonicalSubtreeRootLoadCoverage, CanonicalSubtreeRootLoadEvidence},
};

/// Fixed sidecar name copied with every owner-created canonical checkpoint.
pub(super) const CANONICAL_CONSTRUCTION_MANIFEST_FILE_NAME: &str =
    "canonical-construction-manifest.v4.json";
/// Exact immutable construction-manifest format accepted by this release.
pub const CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION: u16 = 4;
const MAX_CONSTRUCTION_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;
const CONSTRUCTION_MANIFEST_DIGEST_DOMAIN: &[u8] =
    b"zinder.canonical.construction-manifest.file.v4\0";
const CONSTRUCTION_BUILD_PLAN_DIGEST_DOMAIN: &[u8] =
    b"zinder.canonical.construction-manifest.build-plan.v4\0";
const CONSTRUCTION_CHECKPOINT_DIGEST_DOMAIN: &[u8] =
    b"zinder.canonical.construction-manifest.checkpoint.v4\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CanonicalConstructionProofProvenance {
    TrustedFreshWriter,
    ColdCertification,
}

impl CanonicalConstructionProofProvenance {
    const fn as_str(self) -> &'static str {
        match self {
            Self::TrustedFreshWriter => "trusted-fresh-writer",
            Self::ColdCertification => "cold-certification",
        }
    }
}

/// Exact immutable sidecar identity carried by every READY control record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalConstructionManifestBinding {
    /// Fixed sidecar format version.
    pub version: u16,
    /// Domain-separated SHA-256 over the exact sidecar bytes.
    pub sha256: [u8; 32],
}

/// One complete ordered family observation used by construction certification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CanonicalConstructionFamilyEvidence {
    pub(super) family: &'static str,
    pub(super) row_count: u64,
    pub(super) logical_bytes: u64,
    pub(super) first_key: Option<Vec<u8>>,
    pub(super) last_key: Option<Vec<u8>>,
    pub(super) ordered_key_value_digest: [u8; 32],
}

impl CanonicalConstructionFamilyEvidence {
    pub(super) fn accumulator(
        family: &'static str,
    ) -> CanonicalConstructionFamilyEvidenceAccumulator {
        CanonicalConstructionFamilyEvidenceAccumulator::new(family)
    }

    pub(super) fn from_ordered_writer(
        family: &'static str,
        evidence: OrderedKeyValueEvidence,
    ) -> Self {
        Self {
            family,
            row_count: evidence.row_count,
            logical_bytes: evidence.logical_bytes,
            first_key: evidence.first_key,
            last_key: evidence.last_key,
            ordered_key_value_digest: evidence.ordered_key_value_digest,
        }
    }
}

pub(super) struct CanonicalConstructionFamilyEvidenceAccumulator {
    family: &'static str,
    inner: OrderedKeyValueEvidenceAccumulator,
}

impl CanonicalConstructionFamilyEvidenceAccumulator {
    fn new(family: &'static str) -> Self {
        Self {
            family,
            inner: OrderedKeyValueEvidenceAccumulator::new(),
        }
    }

    pub(super) fn observe(
        &mut self,
        key: &[u8],
        encoded_value: &[u8],
    ) -> Result<(), CanonicalStoreError> {
        self.inner.record(key, encoded_value).map_err(|source| {
            CanonicalStoreError::publication(format!(
                "{} ordered family evidence is invalid: {source}",
                self.family
            ))
        })
    }

    pub(super) fn finish(self) -> CanonicalConstructionFamilyEvidence {
        CanonicalConstructionFamilyEvidence::from_ordered_writer(self.family, self.inner.finish())
    }
}

pub(super) struct CanonicalConstructionManifestDraft {
    evidence_provenance: CanonicalConstructionProofProvenance,
    build_plan: CanonicalStoreBuildPlan,
    workload: CanonicalStoreWorkload,
    source_checkpoint: CommitmentTreeCheckpoint,
    block_evidence: CanonicalBlockLoadEvidence,
    subtree_evidence: CanonicalSubtreeRootLoadEvidence,
    family_evidence: Vec<CanonicalConstructionFamilyEvidence>,
    staged_ssts: Vec<CanonicalStagedSstEvidence>,
}

pub(super) struct CanonicalConstructionManifestInputs {
    pub(super) build_plan: CanonicalStoreBuildPlan,
    pub(super) workload: CanonicalStoreWorkload,
    pub(super) source_checkpoint: CommitmentTreeCheckpoint,
    pub(super) block_evidence: CanonicalBlockLoadEvidence,
    pub(super) subtree_evidence: CanonicalSubtreeRootLoadEvidence,
    pub(super) family_evidence: Vec<CanonicalConstructionFamilyEvidence>,
    pub(super) staged_ssts: Vec<CanonicalStagedSstEvidence>,
}

impl CanonicalConstructionManifestDraft {
    pub(super) fn new_trusted_fresh(
        inputs: CanonicalConstructionManifestInputs,
    ) -> Result<Self, CanonicalStoreError> {
        Self::new(
            CanonicalConstructionProofProvenance::TrustedFreshWriter,
            inputs,
        )
    }

    pub(super) fn new_cold_certified(
        inputs: CanonicalConstructionManifestInputs,
    ) -> Result<Self, CanonicalStoreError> {
        Self::new(
            CanonicalConstructionProofProvenance::ColdCertification,
            inputs,
        )
    }

    fn new(
        evidence_provenance: CanonicalConstructionProofProvenance,
        inputs: CanonicalConstructionManifestInputs,
    ) -> Result<Self, CanonicalStoreError> {
        validate_complete_family_coverage(&inputs.family_evidence)?;
        validate_staged_sst_coverage(&inputs.family_evidence, &inputs.staged_ssts)?;
        Ok(Self {
            evidence_provenance,
            build_plan: inputs.build_plan,
            workload: inputs.workload,
            source_checkpoint: inputs.source_checkpoint,
            block_evidence: inputs.block_evidence,
            subtree_evidence: inputs.subtree_evidence,
            family_evidence: inputs.family_evidence,
            staged_ssts: inputs.staged_ssts,
        })
    }

    pub(super) const fn subtree_root_coverage(&self) -> CanonicalSubtreeRootLoadCoverage {
        self.subtree_evidence.coverage
    }

    pub(super) fn persist(
        self,
        store_path: &Path,
        initial_ready: &CanonicalStoreReadyEvidence,
    ) -> Result<CanonicalConstructionManifestBinding, CanonicalStoreError> {
        let manifest = PersistedConstructionManifest::from_draft(self, initial_ready)?;
        persist_manifest(store_path, &manifest)
    }
}

pub(super) fn read_construction_manifest_binding(
    store_path: &Path,
) -> Result<CanonicalConstructionManifestBinding, CanonicalStoreError> {
    let (manifest, bytes) = read_construction_manifest(store_path)?;
    Ok(CanonicalConstructionManifestBinding {
        version: manifest.format_version,
        sha256: manifest_digest(&bytes),
    })
}

pub(super) fn validate_ready_construction_manifest(
    store_path: &Path,
    ready: &CanonicalStoreReadyEvidence,
    workload: CanonicalStoreWorkload,
    build_plan: &CanonicalStoreBuildPlan,
) -> Result<CanonicalSubtreeRootLoadCoverage, CanonicalStoreError> {
    let (manifest, bytes) = read_construction_manifest(store_path)?;
    let binding = CanonicalConstructionManifestBinding {
        version: manifest.format_version,
        sha256: manifest_digest(&bytes),
    };
    if binding.version != ready.construction_manifest_version
        || binding.sha256 != ready.construction_manifest_sha256
    {
        return Err(CanonicalStoreError::admission(
            store_path,
            "construction manifest version or digest differs from READY",
        ));
    }
    validate_manifest_control_identity(store_path, &manifest, workload, build_plan)?;
    Ok(manifest.subtree_evidence.coverage)
}

fn read_construction_manifest(
    store_path: &Path,
) -> Result<(PersistedConstructionManifest, Vec<u8>), CanonicalStoreError> {
    let path = manifest_path(store_path);
    let bytes = read_manifest_bytes(&path)?;
    let manifest: PersistedConstructionManifest =
        serde_json::from_slice(&bytes).map_err(|source| {
            CanonicalStoreError::publication(format!(
                "construction manifest {} is invalid: {source}",
                path.display()
            ))
        })?;
    manifest.validate()?;
    Ok((manifest, bytes))
}

fn validate_manifest_control_identity(
    store_path: &Path,
    manifest: &PersistedConstructionManifest,
    workload: CanonicalStoreWorkload,
    build_plan: &CanonicalStoreBuildPlan,
) -> Result<(), CanonicalStoreError> {
    validate_manifest_control_parts(
        store_path,
        manifest.workload,
        &manifest.build_plan,
        workload,
        build_plan,
    )
}

fn validate_manifest_control_parts(
    store_path: &Path,
    manifest_workload: CanonicalStoreWorkload,
    manifest_build_plan: &PersistedBuildPlan,
    control_workload: CanonicalStoreWorkload,
    control_build_plan: &CanonicalStoreBuildPlan,
) -> Result<(), CanonicalStoreError> {
    if manifest_workload != control_workload
        || *manifest_build_plan != PersistedBuildPlan::from_plan(control_build_plan)
    {
        return Err(CanonicalStoreError::admission(
            store_path,
            "construction manifest workload or build plan differs from store control",
        ));
    }
    Ok(())
}

pub(super) fn copy_construction_manifest(
    source_store_path: &Path,
    target_store_path: &Path,
) -> Result<(), CanonicalStoreError> {
    let source = manifest_path(source_store_path);
    let target = manifest_path(target_store_path);
    let binding = read_construction_manifest_binding(source_store_path)?;
    let bytes = read_manifest_bytes(&source)?;
    let copied_binding = write_manifest_bytes(&target, &bytes)?;
    if copied_binding != binding {
        return Err(CanonicalStoreError::publication(
            "copied construction manifest has a different immutable identity",
        ));
    }
    Ok(())
}

fn persist_manifest(
    store_path: &Path,
    manifest: &PersistedConstructionManifest,
) -> Result<CanonicalConstructionManifestBinding, CanonicalStoreError> {
    let bytes = serde_json::to_vec(manifest).map_err(|source| {
        CanonicalStoreError::publication(format!(
            "construction manifest serialization failed: {source}"
        ))
    })?;
    write_manifest_bytes(&manifest_path(store_path), &bytes)
}

fn write_manifest_bytes(
    target: &Path,
    bytes: &[u8],
) -> Result<CanonicalConstructionManifestBinding, CanonicalStoreError> {
    let byte_length = u64::try_from(bytes.len()).map_err(|_| {
        CanonicalStoreError::publication("construction manifest byte length exceeds u64::MAX")
    })?;
    if byte_length > MAX_CONSTRUCTION_MANIFEST_BYTES {
        return Err(CanonicalStoreError::publication(
            "construction manifest bytes exceed the fixed size limit",
        ));
    }
    match fs::symlink_metadata(target) {
        Ok(_) => {
            return Err(CanonicalStoreError::publication(format!(
                "construction manifest target already exists: {}",
                target.display()
            )));
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(CanonicalStoreError::PathUnavailable {
                path: target.to_path_buf(),
                source,
            });
        }
    }
    let parent = target.parent().ok_or_else(|| {
        CanonicalStoreError::publication("construction manifest has no store-root parent")
    })?;
    let temporary = target.with_extension("json.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|source| CanonicalStoreError::PathUnavailable {
            path: temporary.clone(),
            source,
        })?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| CanonicalStoreError::PathUnavailable {
            path: temporary.clone(),
            source,
        })?;
    // `rename` can replace an attacker- or stale-created target. Link the
    // fully synced temporary inode instead: this succeeds only when the final
    // name is still absent, which keeps the construction proof immutable.
    fs::hard_link(&temporary, target).map_err(|source| CanonicalStoreError::PathUnavailable {
        path: target.to_path_buf(),
        source,
    })?;
    sync_directory(parent)?;
    fs::remove_file(&temporary).map_err(|source| CanonicalStoreError::PathUnavailable {
        path: temporary.clone(),
        source,
    })?;
    sync_directory(parent)?;
    Ok(CanonicalConstructionManifestBinding {
        version: CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION,
        sha256: manifest_digest(bytes),
    })
}

fn read_manifest_bytes(path: &Path) -> Result<Vec<u8>, CanonicalStoreError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| CanonicalStoreError::PathUnavailable {
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_CONSTRUCTION_MANIFEST_BYTES {
        return Err(CanonicalStoreError::publication(format!(
            "construction manifest {} is not a bounded regular file",
            path.display()
        )));
    }
    let maximum_bytes = usize::try_from(MAX_CONSTRUCTION_MANIFEST_BYTES).map_err(|_| {
        CanonicalStoreError::publication("construction manifest byte limit is invalid")
    })?;
    let capacity = usize::try_from(metadata.len()).map_err(|_| {
        CanonicalStoreError::publication("construction manifest length exceeds usize::MAX")
    })?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(capacity).map_err(|source| {
        CanonicalStoreError::publication(format!(
            "construction manifest allocation failed: {source}"
        ))
    })?;
    File::open(path)
        .and_then(|mut file| {
            Read::by_ref(&mut file)
                .take(MAX_CONSTRUCTION_MANIFEST_BYTES + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(|source| CanonicalStoreError::PathUnavailable {
            path: path.to_path_buf(),
            source,
        })?;
    if bytes.len() > maximum_bytes {
        return Err(CanonicalStoreError::publication(format!(
            "construction manifest {} exceeds its byte limit",
            path.display()
        )));
    }
    Ok(bytes)
}

fn sync_directory(path: &Path) -> Result<(), CanonicalStoreError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| CanonicalStoreError::PathUnavailable {
            path: path.to_path_buf(),
            source,
        })
}

fn manifest_path(store_path: &Path) -> PathBuf {
    store_path.join(CANONICAL_CONSTRUCTION_MANIFEST_FILE_NAME)
}

fn manifest_digest(bytes: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(CONSTRUCTION_MANIFEST_DIGEST_DOMAIN);
    digest.update(bytes);
    digest.finalize().into()
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedConstructionManifest {
    format_version: u16,
    evidence_provenance: String,
    workload: CanonicalStoreWorkload,
    build_plan: PersistedBuildPlan,
    source_tip_checkpoint: PersistedCheckpoint,
    block_evidence: PersistedBlockEvidence,
    subtree_evidence: PersistedSubtreeEvidence,
    family_evidence: Vec<PersistedFamilyEvidence>,
    staged_ssts: Vec<PersistedSstEvidence>,
    initial_ready: PersistedReadyEvidence,
}

impl PersistedConstructionManifest {
    fn from_draft(
        draft: CanonicalConstructionManifestDraft,
        ready: &CanonicalStoreReadyEvidence,
    ) -> Result<Self, CanonicalStoreError> {
        let mut block_evidence = PersistedBlockEvidence::from_evidence(&draft.block_evidence);
        block_evidence.source_checkpoint_sha256 =
            PersistedCheckpoint::from_checkpoint(&draft.source_checkpoint).digest_sha256;
        let manifest = Self {
            format_version: CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION,
            evidence_provenance: draft.evidence_provenance.as_str().to_owned(),
            workload: draft.workload,
            build_plan: PersistedBuildPlan::from_plan(&draft.build_plan),
            source_tip_checkpoint: PersistedCheckpoint::from_checkpoint(&draft.source_checkpoint),
            block_evidence,
            subtree_evidence: PersistedSubtreeEvidence::from_evidence(draft.subtree_evidence),
            family_evidence: draft
                .family_evidence
                .into_iter()
                .map(PersistedFamilyEvidence::from_evidence)
                .collect(),
            staged_ssts: draft
                .staged_ssts
                .into_iter()
                .map(PersistedSstEvidence::from_evidence)
                .collect(),
            initial_ready: PersistedReadyEvidence::from_ready(ready),
        };
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<(), CanonicalStoreError> {
        if self.format_version != CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION {
            return Err(CanonicalStoreError::publication(format!(
                "construction manifest format {} is not supported",
                self.format_version
            )));
        }
        if self.evidence_provenance
            != CanonicalConstructionProofProvenance::TrustedFreshWriter.as_str()
            && self.evidence_provenance
                != CanonicalConstructionProofProvenance::ColdCertification.as_str()
        {
            return Err(CanonicalStoreError::publication(
                "construction manifest has an unknown proof provenance",
            ));
        }
        self.build_plan.validate()?;
        self.source_tip_checkpoint.validate()?;
        if self.source_tip_checkpoint.block_id != self.build_plan.build_tip
            || self.source_tip_checkpoint.digest_sha256
                != self.block_evidence.source_checkpoint_sha256
        {
            return Err(CanonicalStoreError::publication(
                "construction manifest source checkpoint does not bind the build tip",
            ));
        }
        if self.block_evidence.block_count == 0
            || self.block_evidence.sequence_digest_version != 1
            || self.block_evidence.block_digest_version != 1
            || self.block_evidence.replay_format_version != 1
            || self.subtree_evidence.sequence_digest_version != 2
        {
            return Err(CanonicalStoreError::publication(
                "construction manifest has unsupported source evidence",
            ));
        }
        validate_persisted_family_coverage(&self.family_evidence)?;
        validate_persisted_sst_coverage(&self.family_evidence, &self.staged_ssts)?;
        validate_persisted_source_evidence(
            &self.family_evidence,
            &self.build_plan,
            &self.block_evidence,
            &self.subtree_evidence,
        )?;
        self.initial_ready
            .validate(&self.build_plan, &self.block_evidence)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedBuildPlan {
    network_id: u32,
    network_upgrade_activations_fingerprint_version: u16,
    network_upgrade_activations_fingerprint: [u8; 32],
    raw_blob_retention: crate::RawBlobRetention,
    reorg_window_blocks: u32,
    first_available_height: u32,
    history_predecessor: PersistedCheckpoint,
    build_tip: PersistedBlockId,
    digest_sha256: [u8; 32],
}

impl PersistedBuildPlan {
    fn from_plan(plan: &CanonicalStoreBuildPlan) -> Self {
        let mut persisted_plan = Self {
            network_id: plan.network().id(),
            network_upgrade_activations_fingerprint_version: plan
                .network_upgrade_activations_fingerprint()
                .version()
                .value(),
            network_upgrade_activations_fingerprint: plan
                .network_upgrade_activations_fingerprint()
                .as_bytes(),
            raw_blob_retention: plan.raw_blob_retention(),
            reorg_window_blocks: plan.reorg_policy().reorg_window_blocks(),
            first_available_height: plan.history_bounds().first_available_height().value(),
            history_predecessor: PersistedCheckpoint::from_checkpoint(plan.history_predecessor()),
            build_tip: PersistedBlockId::from_block_id(plan.build_tip()),
            digest_sha256: [0; 32],
        };
        persisted_plan.digest_sha256 = persisted_plan.digest();
        persisted_plan
    }

    fn validate(&self) -> Result<(), CanonicalStoreError> {
        if self.reorg_window_blocks == 0
            || self.history_predecessor.block_id.height.saturating_add(1)
                != self.first_available_height
            || self.first_available_height > self.build_tip.height
            || self.digest_sha256 != self.digest()
        {
            return Err(CanonicalStoreError::publication(
                "construction manifest build-plan evidence is invalid",
            ));
        }
        self.history_predecessor.validate()
    }

    fn digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(CONSTRUCTION_BUILD_PLAN_DIGEST_DOMAIN);
        digest.update(self.network_id.to_le_bytes());
        digest.update(
            self.network_upgrade_activations_fingerprint_version
                .to_le_bytes(),
        );
        digest.update(self.network_upgrade_activations_fingerprint);
        digest.update(self.raw_blob_retention.as_kebab_case().as_bytes());
        digest.update([0]);
        digest.update(self.reorg_window_blocks.to_le_bytes());
        digest.update(self.first_available_height.to_le_bytes());
        digest.update(self.history_predecessor.digest_sha256);
        digest.update(self.build_tip.height.to_le_bytes());
        digest.update(self.build_tip.hash);
        digest.finalize().into()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedCheckpoint {
    block_id: PersistedBlockId,
    block_time_seconds: u32,
    frontiers: Vec<PersistedFrontier>,
    digest_sha256: [u8; 32],
}

impl PersistedCheckpoint {
    fn from_checkpoint(checkpoint: &CommitmentTreeCheckpoint) -> Self {
        let mut persisted_checkpoint = Self {
            block_id: PersistedBlockId::from_block_id(checkpoint.block_id),
            block_time_seconds: checkpoint.block_time_seconds,
            frontiers: shielded_protocols()
                .into_iter()
                .map(|protocol| {
                    PersistedFrontier::from_frontier(protocol, checkpoint.frontiers.get(protocol))
                })
                .collect(),
            digest_sha256: [0; 32],
        };
        persisted_checkpoint.digest_sha256 = persisted_checkpoint.digest();
        persisted_checkpoint
    }

    fn validate(&self) -> Result<(), CanonicalStoreError> {
        let expected = BTreeSet::from([1_u8, 2, 3]);
        let observed = self
            .frontiers
            .iter()
            .map(|frontier| frontier.protocol)
            .collect::<BTreeSet<_>>();
        if observed != expected || self.digest_sha256 != self.digest() {
            return Err(CanonicalStoreError::publication(
                "construction manifest checkpoint evidence is invalid",
            ));
        }
        if self.frontiers.iter().any(|frontier| !frontier.is_valid()) {
            return Err(CanonicalStoreError::publication(
                "construction manifest checkpoint frontier evidence is invalid",
            ));
        }
        Ok(())
    }

    fn digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(CONSTRUCTION_CHECKPOINT_DIGEST_DOMAIN);
        digest.update(self.block_id.height.to_le_bytes());
        digest.update(self.block_id.hash);
        digest.update(self.block_time_seconds.to_le_bytes());
        for frontier in &self.frontiers {
            digest.update([frontier.protocol]);
            digest.update([u8::from(frontier.present)]);
            digest.update(frontier.tree_size.to_le_bytes());
            digest.update(frontier.final_root);
            update_length_prefixed(&mut digest, &frontier.final_state);
        }
        digest.finalize().into()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedFrontier {
    protocol: u8,
    present: bool,
    tree_size: u32,
    final_root: [u8; 32],
    final_state: Vec<u8>,
}

impl PersistedFrontier {
    fn from_frontier(
        protocol: ShieldedProtocol,
        frontier: Option<&CommitmentTreeFrontier>,
    ) -> Self {
        let protocol = protocol_tag(protocol);
        frontier.map_or_else(
            || Self {
                protocol,
                present: false,
                tree_size: 0,
                final_root: [0; 32],
                final_state: Vec::new(),
            },
            |frontier| Self {
                protocol,
                present: true,
                tree_size: frontier.tree_size(),
                final_root: frontier.final_root().as_bytes(),
                final_state: frontier.final_state_bytes().to_vec(),
            },
        )
    }

    fn is_valid(&self) -> bool {
        (self.present && !self.final_state.is_empty())
            || (!self.present
                && self.tree_size == 0
                && self.final_root == [0; 32]
                && self.final_state.is_empty())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedBlockId {
    height: u32,
    hash: [u8; 32],
}

impl PersistedBlockId {
    const fn from_block_id(block_id: BlockId) -> Self {
        Self {
            height: block_id.height.value(),
            hash: block_id.hash.as_bytes(),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedBlockEvidence {
    block_count: u64,
    transaction_count: u64,
    logical_bytes: u64,
    replay_format_version: u32,
    block_digest_version: u16,
    sequence_digest_version: u16,
    sequence_digest: [u8; 32],
    source_checkpoint_sha256: [u8; 32],
}

impl PersistedBlockEvidence {
    fn from_evidence(evidence: &CanonicalBlockLoadEvidence) -> Self {
        Self {
            block_count: evidence.block_count,
            transaction_count: evidence.transaction_count,
            logical_bytes: evidence.logical_bytes,
            replay_format_version: evidence.replay_format_version.value(),
            block_digest_version: evidence.block_digest_version.value(),
            sequence_digest_version: evidence.sequence_digest_version.value(),
            sequence_digest: evidence.sequence_digest.as_bytes(),
            source_checkpoint_sha256: [0; 32],
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedSubtreeEvidence {
    coverage: CanonicalSubtreeRootLoadCoverage,
    subtree_root_count: u64,
    subtree_root_logical_bytes: u64,
    subtree_root_sequence_digest: [u8; 32],
    sequence_digest_version: u16,
}

impl PersistedSubtreeEvidence {
    const fn from_evidence(evidence: CanonicalSubtreeRootLoadEvidence) -> Self {
        Self {
            coverage: evidence.coverage,
            subtree_root_count: evidence.subtree_root_count,
            subtree_root_logical_bytes: evidence.subtree_root_logical_bytes,
            subtree_root_sequence_digest: evidence.subtree_root_sequence_digest,
            sequence_digest_version: evidence.sequence_digest_version,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedFamilyEvidence {
    family: String,
    row_count: u64,
    logical_bytes: u64,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    ordered_key_value_digest: [u8; 32],
}

impl PersistedFamilyEvidence {
    fn from_evidence(evidence: CanonicalConstructionFamilyEvidence) -> Self {
        Self {
            family: evidence.family.to_owned(),
            row_count: evidence.row_count,
            logical_bytes: evidence.logical_bytes,
            first_key: evidence.first_key,
            last_key: evidence.last_key,
            ordered_key_value_digest: evidence.ordered_key_value_digest,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedSstEvidence {
    family: String,
    ordinal: u64,
    file_bytes: u64,
    entry_count: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    ordered_key_value_digest: [u8; 32],
}

impl PersistedSstEvidence {
    fn from_evidence(evidence: CanonicalStagedSstEvidence) -> Self {
        let SstFileEvidence {
            ordinal,
            file_bytes,
            entry_count,
            first_key,
            last_key,
            ordered_key_value_digest,
        } = evidence.file;
        Self {
            family: evidence.family.to_owned(),
            ordinal,
            file_bytes,
            entry_count,
            first_key,
            last_key,
            ordered_key_value_digest,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedReadyEvidence {
    first_retained_block: PersistedBlockId,
    visible_tip: PersistedBlockId,
    visible_epoch: u64,
    visible_event_sequence: u64,
    visible_block_count: u64,
    visible_sequence_digest: [u8; 32],
    visible_logical_replay_bytes: u64,
    settled_checkpoint: PersistedBlockId,
    settled_checkpoint_count: u64,
    settled_checkpoint_digest: [u8; 32],
    settled_checkpoint_logical_replay_bytes: u64,
}

impl PersistedReadyEvidence {
    fn from_ready(ready: &CanonicalStoreReadyEvidence) -> Self {
        let checkpoint = ready.sequence_checkpoint;
        Self {
            first_retained_block: PersistedBlockId::from_block_id(ready.first_retained_block),
            visible_tip: PersistedBlockId::from_block_id(ready.visible_tip),
            visible_epoch: ready.visible_epoch.value(),
            visible_event_sequence: ready.visible_event_sequence,
            visible_block_count: ready.visible_block_count,
            visible_sequence_digest: ready.visible_sequence_digest,
            visible_logical_replay_bytes: ready.visible_logical_replay_bytes,
            settled_checkpoint: PersistedBlockId::from_block_id(checkpoint.through()),
            settled_checkpoint_count: checkpoint.retained_block_count(),
            settled_checkpoint_digest: checkpoint.sequence_digest().as_bytes(),
            settled_checkpoint_logical_replay_bytes: checkpoint.logical_replay_bytes(),
        }
    }

    fn validate(
        &self,
        build_plan: &PersistedBuildPlan,
        block_evidence: &PersistedBlockEvidence,
    ) -> Result<(), CanonicalStoreError> {
        if self.first_retained_block.height != build_plan.first_available_height
            || self.visible_tip != build_plan.build_tip
            || self.visible_epoch != 1
            || self.visible_event_sequence != 1
            || self.visible_block_count != block_evidence.block_count
            || self.visible_sequence_digest != block_evidence.sequence_digest
            || self.visible_logical_replay_bytes == 0
            || self.settled_checkpoint.height > self.visible_tip.height
            || self.settled_checkpoint_count == 0
            || self.settled_checkpoint_logical_replay_bytes == 0
        {
            return Err(CanonicalStoreError::publication(
                "construction manifest initial READY evidence is invalid",
            ));
        }
        Ok(())
    }
}

fn validate_complete_family_coverage(
    families: &[CanonicalConstructionFamilyEvidence],
) -> Result<(), CanonicalStoreError> {
    let observed = families
        .iter()
        .map(|family| family.family)
        .collect::<BTreeSet<_>>();
    let expected = canonical_construction_families();
    if families.len() != expected.len() || observed != expected {
        return Err(CanonicalStoreError::publication(
            "construction manifest family evidence is incomplete or duplicated",
        ));
    }
    Ok(())
}

fn validate_staged_sst_coverage(
    families: &[CanonicalConstructionFamilyEvidence],
    staged_ssts: &[CanonicalStagedSstEvidence],
) -> Result<(), CanonicalStoreError> {
    let family_views = families
        .iter()
        .map(ConstructionFamilyEvidenceView::from_construction)
        .collect::<Vec<_>>();
    let sst_views = staged_ssts
        .iter()
        .map(ConstructionSstEvidenceView::from_staged)
        .collect::<Vec<_>>();
    validate_staged_sst_views(&family_views, &sst_views)
}

fn validate_persisted_family_coverage(
    families: &[PersistedFamilyEvidence],
) -> Result<(), CanonicalStoreError> {
    let observed = families
        .iter()
        .map(|family| family.family.as_str())
        .collect::<BTreeSet<_>>();
    let expected = canonical_construction_families();
    if families.len() != expected.len()
        || observed != expected
        || families.iter().any(|family| match family.row_count {
            0 => family.first_key.is_some() || family.last_key.is_some(),
            _ => {
                family.first_key.is_none()
                    || family.last_key.is_none()
                    || family
                        .first_key
                        .as_ref()
                        .zip(family.last_key.as_ref())
                        .is_some_and(|(first, last)| first > last)
            }
        })
    {
        return Err(CanonicalStoreError::publication(
            "construction manifest family evidence is invalid",
        ));
    }
    Ok(())
}

fn validate_persisted_sst_coverage(
    families: &[PersistedFamilyEvidence],
    staged_ssts: &[PersistedSstEvidence],
) -> Result<(), CanonicalStoreError> {
    let family_views = families
        .iter()
        .map(ConstructionFamilyEvidenceView::from_persisted)
        .collect::<Vec<_>>();
    let sst_views = staged_ssts
        .iter()
        .map(ConstructionSstEvidenceView::from_persisted)
        .collect::<Vec<_>>();
    validate_staged_sst_views(&family_views, &sst_views)
}

fn validate_persisted_source_evidence(
    families: &[PersistedFamilyEvidence],
    build_plan: &PersistedBuildPlan,
    block_evidence: &PersistedBlockEvidence,
    subtree_evidence: &PersistedSubtreeEvidence,
) -> Result<(), CanonicalStoreError> {
    use super::rocksdb::{
        BLOCK_BLOB_COLUMN_FAMILY, BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY,
        COMPACT_BLOCK_COLUMN_FAMILY, SUBTREE_ROOT_COLUMN_FAMILY, TRANSACTION_BLOB_COLUMN_FAMILY,
        TRANSACTION_LOCATION_COLUMN_FAMILY,
    };

    let by_name = families
        .iter()
        .map(|family| (family.family.as_str(), family))
        .collect::<BTreeMap<_, _>>();
    let require = |name| {
        by_name.get(name).copied().ok_or_else(|| {
            CanonicalStoreError::publication("construction manifest family evidence is absent")
        })
    };
    let replay = require(super::block_replay::BLOCK_REPLAY_COLUMN_FAMILY)?;
    let header = require(BLOCK_HEADER_COLUMN_FAMILY)?;
    let block_hash = require(BLOCK_HASH_INDEX_COLUMN_FAMILY)?;
    let compact = require(COMPACT_BLOCK_COLUMN_FAMILY)?;
    let transaction_location = require(TRANSACTION_LOCATION_COLUMN_FAMILY)?;
    let transaction_blob = require(TRANSACTION_BLOB_COLUMN_FAMILY)?;
    let block_blob = require(BLOCK_BLOB_COLUMN_FAMILY)?;
    let subtree = require(SUBTREE_ROOT_COLUMN_FAMILY)?;
    let staged_logical_bytes =
        canonical_staged_sst_families()
            .into_iter()
            .try_fold(0_u64, |total, name| {
                total
                    .checked_add(require(name)?.logical_bytes)
                    .ok_or_else(|| {
                        CanonicalStoreError::publication(
                            "construction manifest staged logical bytes exceed u64::MAX",
                        )
                    })
            })?;
    let expected_transaction_blob_count =
        if build_plan.raw_blob_retention.retains_transaction_blobs() {
            block_evidence.transaction_count
        } else {
            0
        };
    let expected_block_blob_count = if build_plan.raw_blob_retention.retains_block_blobs() {
        block_evidence.block_count
    } else {
        0
    };
    if replay.row_count != block_evidence.block_count
        || header.row_count != block_evidence.block_count
        || block_hash.row_count != block_evidence.block_count
        || compact.row_count != block_evidence.block_count
        || transaction_location.row_count != block_evidence.transaction_count
        || transaction_blob.row_count != expected_transaction_blob_count
        || block_blob.row_count != expected_block_blob_count
        || staged_logical_bytes != block_evidence.logical_bytes
        || subtree.row_count != subtree_evidence.subtree_root_count
        || subtree.logical_bytes != subtree_evidence.subtree_root_logical_bytes
    {
        return Err(CanonicalStoreError::publication(
            "construction manifest source evidence does not match family evidence",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct ConstructionFamilyEvidenceView<'a> {
    family: &'a str,
    row_count: u64,
    first_key: Option<&'a [u8]>,
    last_key: Option<&'a [u8]>,
}

impl<'a> ConstructionFamilyEvidenceView<'a> {
    fn from_construction(evidence: &'a CanonicalConstructionFamilyEvidence) -> Self {
        Self {
            family: evidence.family,
            row_count: evidence.row_count,
            first_key: evidence.first_key.as_deref(),
            last_key: evidence.last_key.as_deref(),
        }
    }

    fn from_persisted(evidence: &'a PersistedFamilyEvidence) -> Self {
        Self {
            family: &evidence.family,
            row_count: evidence.row_count,
            first_key: evidence.first_key.as_deref(),
            last_key: evidence.last_key.as_deref(),
        }
    }
}

#[derive(Clone, Copy)]
struct ConstructionSstEvidenceView<'a> {
    family: &'a str,
    ordinal: u64,
    file_bytes: u64,
    entry_count: u64,
    first_key: &'a [u8],
    last_key: &'a [u8],
}

impl<'a> ConstructionSstEvidenceView<'a> {
    fn from_staged(evidence: &'a CanonicalStagedSstEvidence) -> Self {
        Self {
            family: evidence.family,
            ordinal: evidence.file.ordinal,
            file_bytes: evidence.file.file_bytes,
            entry_count: evidence.file.entry_count,
            first_key: &evidence.file.first_key,
            last_key: &evidence.file.last_key,
        }
    }

    fn from_persisted(evidence: &'a PersistedSstEvidence) -> Self {
        Self {
            family: &evidence.family,
            ordinal: evidence.ordinal,
            file_bytes: evidence.file_bytes,
            entry_count: evidence.entry_count,
            first_key: &evidence.first_key,
            last_key: &evidence.last_key,
        }
    }
}

fn validate_staged_sst_views(
    families: &[ConstructionFamilyEvidenceView<'_>],
    staged_ssts: &[ConstructionSstEvidenceView<'_>],
) -> Result<(), CanonicalStoreError> {
    let family_by_name = families
        .iter()
        .map(|family| (family.family, family))
        .collect::<BTreeMap<_, _>>();
    let expected = canonical_staged_sst_families()
        .into_iter()
        .filter(|family| {
            family_by_name
                .get(family)
                .is_some_and(|evidence| evidence.row_count != 0)
        })
        .collect::<BTreeSet<_>>();
    let mut by_family = BTreeMap::<&str, Vec<&ConstructionSstEvidenceView<'_>>>::new();
    for staged in staged_ssts {
        if !expected.contains(staged.family)
            || !family_by_name.contains_key(staged.family)
            || staged.file_bytes == 0
            || staged.entry_count == 0
            || staged.first_key > staged.last_key
        {
            return Err(CanonicalStoreError::publication(
                "construction manifest staged SST evidence is invalid",
            ));
        }
        by_family.entry(staged.family).or_default().push(staged);
    }
    if by_family.keys().copied().collect::<BTreeSet<_>>() != expected {
        return Err(CanonicalStoreError::publication(
            "construction manifest staged SST evidence is incomplete",
        ));
    }
    for (family_name, files) in &mut by_family {
        files.sort_unstable_by_key(|file| file.ordinal);
        let family = family_by_name.get(family_name).ok_or_else(|| {
            CanonicalStoreError::publication("construction manifest staged SST family is unknown")
        })?;
        let entry_count = files.iter().try_fold(0_u64, |total, file| {
            total.checked_add(file.entry_count).ok_or_else(|| {
                CanonicalStoreError::publication(
                    "construction manifest staged SST entries exceed u64::MAX",
                )
            })
        })?;
        if entry_count != family.row_count
            || files.first().map(|file| file.first_key) != family.first_key
            || files.last().map(|file| file.last_key) != family.last_key
        {
            return Err(CanonicalStoreError::publication(
                "construction manifest staged SST evidence does not match its family evidence",
            ));
        }
        for (expected_ordinal, file) in files.iter().enumerate() {
            if file.ordinal
                != u64::try_from(expected_ordinal).map_err(|_| {
                    CanonicalStoreError::publication(
                        "construction manifest staged SST ordinal exceeds u64::MAX",
                    )
                })?
            {
                return Err(CanonicalStoreError::publication(
                    "construction manifest staged SST ordinals are not contiguous",
                ));
            }
        }
        if files
            .windows(2)
            .any(|pair| pair[0].last_key >= pair[1].first_key)
        {
            return Err(CanonicalStoreError::publication(
                "construction manifest staged SST key ranges overlap",
            ));
        }
    }
    Ok(())
}

fn canonical_construction_families() -> BTreeSet<&'static str> {
    use super::rocksdb::{
        BLOCK_BLOB_COLUMN_FAMILY, BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
        BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY, CHAIN_EPOCH_COLUMN_FAMILY,
        CHAIN_EVENT_COLUMN_FAMILY, COMPACT_BLOCK_COLUMN_FAMILY,
        DAILY_VALUE_POOL_BALANCE_COLUMN_FAMILY, DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
        MEMPOOL_EVENT_COLUMN_FAMILY, SUBTREE_ROOT_COLUMN_FAMILY, TRANSACTION_BLOB_COLUMN_FAMILY,
        TRANSACTION_LOCATION_COLUMN_FAMILY, TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    };
    BTreeSet::from([
        BLOCK_BLOB_COLUMN_FAMILY,
        BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
        BLOCK_HASH_INDEX_COLUMN_FAMILY,
        BLOCK_HEADER_COLUMN_FAMILY,
        super::block_replay::BLOCK_REPLAY_COLUMN_FAMILY,
        CHAIN_EPOCH_COLUMN_FAMILY,
        CHAIN_EVENT_COLUMN_FAMILY,
        COMPACT_BLOCK_COLUMN_FAMILY,
        DAILY_VALUE_POOL_BALANCE_COLUMN_FAMILY,
        DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
        MEMPOOL_EVENT_COLUMN_FAMILY,
        SUBTREE_ROOT_COLUMN_FAMILY,
        TRANSACTION_BLOB_COLUMN_FAMILY,
        TRANSACTION_LOCATION_COLUMN_FAMILY,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    ])
}

fn canonical_staged_sst_families() -> BTreeSet<&'static str> {
    use super::rocksdb::{
        BLOCK_BLOB_COLUMN_FAMILY, BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
        BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY, COMPACT_BLOCK_COLUMN_FAMILY,
        TRANSACTION_BLOB_COLUMN_FAMILY, TRANSACTION_LOCATION_COLUMN_FAMILY,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    };
    BTreeSet::from([
        BLOCK_BLOB_COLUMN_FAMILY,
        BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
        BLOCK_HASH_INDEX_COLUMN_FAMILY,
        BLOCK_HEADER_COLUMN_FAMILY,
        super::block_replay::BLOCK_REPLAY_COLUMN_FAMILY,
        COMPACT_BLOCK_COLUMN_FAMILY,
        TRANSACTION_BLOB_COLUMN_FAMILY,
        TRANSACTION_LOCATION_COLUMN_FAMILY,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    ])
}

fn shielded_protocols() -> [ShieldedProtocol; 3] {
    [
        ShieldedProtocol::Sapling,
        ShieldedProtocol::Orchard,
        ShieldedProtocol::Ironwood,
    ]
}

const fn protocol_tag(protocol: ShieldedProtocol) -> u8 {
    match protocol {
        ShieldedProtocol::Sapling => 1,
        ShieldedProtocol::Orchard => 2,
        ShieldedProtocol::Ironwood => 3,
        _ => 0,
    }
}

fn update_length_prefixed(digest: &mut Sha256, bytes: &[u8]) {
    digest.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    digest.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use zinder_core::{BlockHash, BlockHeight};

    #[test]
    fn manifest_digest_is_domain_separated_and_stable() {
        assert_eq!(manifest_digest(b"manifest"), manifest_digest(b"manifest"));
        assert_ne!(manifest_digest(b"manifest"), manifest_digest(b"manifest!"));
    }

    #[test]
    fn manifest_writer_never_replaces_an_existing_sidecar() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let path = manifest_path(temporary.path());
        write_manifest_bytes(&path, b"first")?;

        let error = write_manifest_bytes(&path, b"second")
            .err()
            .ok_or("manifest writer must refuse an existing sidecar")?;

        assert!(error.to_string().contains("already exists"));
        assert_eq!(read_manifest_bytes(&path)?, b"first");
        Ok(())
    }

    #[test]
    fn manifest_reader_rejects_unknown_json_fields() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let path = manifest_path(temporary.path());
        write_manifest_bytes(&path, br#"{"unexpected":true}"#)?;

        let error = read_construction_manifest_binding(temporary.path())
            .err()
            .ok_or("unknown sidecar field must fail closed")?;

        assert!(error.to_string().contains("unknown field"));
        Ok(())
    }

    #[test]
    fn manifest_and_control_reject_raw_blob_retention_tampering_in_both_directions_without_rewrite()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let transactions_plan = test_build_plan(crate::RawBlobRetention::Transactions)?;
        let all_plan = test_build_plan(crate::RawBlobRetention::All)?;
        let persisted_transactions = PersistedBuildPlan::from_plan(&transactions_plan);
        let persisted_all = PersistedBuildPlan::from_plan(&all_plan);
        let original_transactions = persisted_transactions.clone();
        let original_all = persisted_all.clone();

        for (manifest_plan, control_plan) in [
            (&persisted_transactions, &all_plan),
            (&persisted_all, &transactions_plan),
        ] {
            let error = validate_manifest_control_parts(
                temporary.path(),
                CanonicalStoreWorkload::Wallet,
                manifest_plan,
                CanonicalStoreWorkload::Wallet,
                control_plan,
            )
            .err()
            .ok_or("manifest/control raw-blob retention mismatch must fail closed")?;
            assert!(error.to_string().contains("differs from store control"));
        }

        assert_eq!(persisted_transactions, original_transactions);
        assert_eq!(persisted_all, original_all);
        Ok(())
    }

    #[test]
    fn manifest_and_control_compare_typed_workload_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let build_plan = test_build_plan(crate::RawBlobRetention::Transactions)?;
        let persisted_plan = PersistedBuildPlan::from_plan(&build_plan);

        assert!(
            validate_manifest_control_parts(
                temporary.path(),
                CanonicalStoreWorkload::Explorer,
                &persisted_plan,
                CanonicalStoreWorkload::Wallet,
                &build_plan,
            )
            .is_err()
        );
        Ok(())
    }

    fn test_build_plan(
        raw_blob_retention: crate::RawBlobRetention,
    ) -> Result<CanonicalStoreBuildPlan, Box<dyn std::error::Error>> {
        let activations =
            super::super::test_network_upgrade_activations(zinder_core::Network::ZcashTestnet)?;
        Ok(CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            raw_blob_retention,
            super::super::CanonicalReorgPolicy::new(100)?,
        )?)
    }
}
