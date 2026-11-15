//! Per-height tree-state checkpoints that let live following replay a fixture.
//!
//! Live canonical following compares every block's derived commitment-tree
//! checkpoint with the node's tree state at that height. A captured fixture
//! holds only block bytes, so tip replay keeps one node checkpoint for every
//! fixture height in a sidecar bound to the exact manifest, and
//! [`TipReplayNodeSource`] serves them in place of the node.

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Write,
    num::NonZeroU32,
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zinder_core::{
    BlockHeight, BlockId, CommitmentTreeCheckpoint, NetworkUpgradeActivations,
    NetworkUpgradeActivationsFingerprint, NetworkUpgradeActivationsFingerprintVersion,
    ShieldedProtocol, SubtreeRootIndex, SubtreeRootRange,
};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceChainSegment, SourceChainSegmentLimits,
    SourceError, SourceSubtreeRoots,
};

use crate::{
    canonical_fixture_replay::{
        CanonicalFixtureReplayCheckpointRecord, validate_digest_hex, validate_fixture_activations,
    },
    error::BenchError,
    fixture::{FixtureManifest, FixtureNodeSource, read_segment_blocks},
};

/// Sidecar file written beside a fixture manifest.
pub const TIP_REPLAY_CHECKPOINTS_FILE_NAME: &str = "tip-replay-checkpoints.json";
const TIP_REPLAY_CHECKPOINTS_TEMP_FILE_NAME: &str = "tip-replay-checkpoints.json.tmp";
const TIP_REPLAY_CHECKPOINTS_CONTRACT_IDENTITY: &str = "tip-replay-checkpoints";
const TIP_REPLAY_CHECKPOINTS_FORMAT_VERSION: u32 = 1;

/// The node's checkpoint after every fixture block, bound to one manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TipReplayCheckpoints {
    /// Stable sidecar contract identity.
    pub contract_identity: String,
    /// Sidecar format version.
    pub format_version: u32,
    /// SHA-256 identity of the exact fixture manifest.
    pub fixture_manifest_sha256: String,
    /// Fingerprint algorithm used for the captured activation table.
    pub network_upgrade_activations_fingerprint_version: u16,
    /// Fingerprint of the node-discovered activation table.
    pub network_upgrade_activations_fingerprint_hex: String,
    /// Block identity and commitment-tree state after each fixture block, in
    /// ascending height order.
    pub checkpoints: Vec<CanonicalFixtureReplayCheckpointRecord>,
}

impl TipReplayCheckpoints {
    /// Reads and admits the sidecar against its exact fixture manifest.
    pub fn read(fixture_directory: &Path, manifest: &FixtureManifest) -> Result<Self, BenchError> {
        read_with_file_digest(fixture_directory, manifest).map(|(sidecar, _)| sidecar)
    }

    /// Decodes every checkpoint, keyed by height.
    pub fn decode_checkpoints(
        &self,
    ) -> Result<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>, BenchError> {
        self.checkpoints
            .iter()
            .map(|record| {
                let checkpoint = record.decode("tip replay checkpoint")?;
                Ok((checkpoint.block_id.height, checkpoint))
            })
            .collect()
    }

    fn from_checkpoints(
        manifest: &FixtureManifest,
        activations: &NetworkUpgradeActivations,
        checkpoints: &[CommitmentTreeCheckpoint],
    ) -> Result<Self, BenchError> {
        let fingerprint = activations.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1);
        let sidecar = Self {
            contract_identity: TIP_REPLAY_CHECKPOINTS_CONTRACT_IDENTITY.to_owned(),
            format_version: TIP_REPLAY_CHECKPOINTS_FORMAT_VERSION,
            fixture_manifest_sha256: manifest.digest_sha256()?,
            network_upgrade_activations_fingerprint_version: fingerprint.version().value(),
            network_upgrade_activations_fingerprint_hex: hex::encode(fingerprint.as_bytes()),
            checkpoints: checkpoints
                .iter()
                .map(CanonicalFixtureReplayCheckpointRecord::from)
                .collect(),
        };
        sidecar.validate(manifest)?;
        Ok(sidecar)
    }

    fn validate(&self, manifest: &FixtureManifest) -> Result<(), BenchError> {
        if self.contract_identity != TIP_REPLAY_CHECKPOINTS_CONTRACT_IDENTITY {
            return Err(BenchError::fixture_format(format!(
                "tip replay checkpoints contract identity {:?} does not match {TIP_REPLAY_CHECKPOINTS_CONTRACT_IDENTITY:?}",
                self.contract_identity
            )));
        }
        if self.format_version != TIP_REPLAY_CHECKPOINTS_FORMAT_VERSION {
            return Err(BenchError::fixture_format(format!(
                "tip replay checkpoints format version {} does not match {TIP_REPLAY_CHECKPOINTS_FORMAT_VERSION}",
                self.format_version
            )));
        }
        validate_digest_hex(&self.fixture_manifest_sha256, "fixture manifest SHA-256")?;
        if self.fixture_manifest_sha256 != manifest.digest_sha256()? {
            return Err(BenchError::fixture_format(
                "tip replay checkpoints were captured for a different fixture manifest",
            ));
        }
        let expected_fingerprint = manifest
            .activations_typed()?
            .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1);
        if self.network_upgrade_activations_fingerprint_version
            != expected_fingerprint.version().value()
            || self.network_upgrade_activations_fingerprint_hex
                != hex::encode(expected_fingerprint.as_bytes())
        {
            return Err(BenchError::fixture_format(
                "tip replay checkpoints activation fingerprint does not match the fixture",
            ));
        }
        let checkpoints = self.decode_checkpoints()?;
        let expected_heights = manifest.from_height..=manifest.to_height;
        if checkpoints.len() != self.checkpoints.len()
            || u32::try_from(checkpoints.len()).ok() != Some(manifest.block_count)
            || !checkpoints
                .keys()
                .map(|height| height.value())
                .eq(expected_heights)
        {
            return Err(BenchError::fixture_format(
                "tip replay checkpoints must cover every fixture height exactly once",
            ));
        }
        let manifest_tip = manifest.tip_id()?;
        if checkpoints
            .get(&manifest_tip.height)
            .is_none_or(|checkpoint| checkpoint.block_id != manifest_tip)
        {
            return Err(BenchError::fixture_format(
                "the last tip replay checkpoint does not match the fixture tip",
            ));
        }
        Ok(())
    }

    fn write_new(&self, fixture_directory: &Path) -> Result<(), BenchError> {
        let temporary_path = fixture_directory.join(TIP_REPLAY_CHECKPOINTS_TEMP_FILE_NAME);
        let final_path = fixture_directory.join(TIP_REPLAY_CHECKPOINTS_FILE_NAME);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(|source| BenchError::io(&temporary_path, source))?;
        serde_json::to_writer_pretty(&mut file, self)?;
        file.write_all(b"\n")
            .map_err(|source| BenchError::io(&temporary_path, source))?;
        file.sync_all()
            .map_err(|source| BenchError::io(&temporary_path, source))?;
        if let Err(source) = std::fs::hard_link(&temporary_path, &final_path) {
            let _ = std::fs::remove_file(&temporary_path);
            if source.kind() == std::io::ErrorKind::AlreadyExists {
                return Err(existing_sidecar_error(&final_path));
            }
            return Err(BenchError::io(&final_path, source));
        }
        std::fs::remove_file(&temporary_path)
            .map_err(|source| BenchError::io(&temporary_path, source))?;
        File::open(fixture_directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| BenchError::io(fixture_directory, source))
    }
}

/// Captures the node checkpoint after every fixture block and writes the sidecar.
///
/// Requests start only after the manifest, activation fingerprint, segment
/// digests, and block linkage are admitted. Each returned checkpoint must name
/// the captured block at its height. An existing sidecar is never replaced.
pub async fn capture_tip_replay_checkpoints<S: NodeSource>(
    fixture_directory: &Path,
    source: &S,
    source_activations: &NetworkUpgradeActivations,
    fetch_concurrency: NonZeroU32,
) -> Result<TipReplayCheckpoints, BenchError> {
    let final_path = fixture_directory.join(TIP_REPLAY_CHECKPOINTS_FILE_NAME);
    if final_path
        .try_exists()
        .map_err(|source| BenchError::io(&final_path, source))?
    {
        return Err(existing_sidecar_error(&final_path));
    }
    let manifest = FixtureManifest::read(fixture_directory)?;
    validate_fixture_activations(&manifest, source_activations)?;
    let block_ids = connected_fixture_block_ids(fixture_directory, &manifest)?;
    let concurrency = usize::try_from(fetch_concurrency.get()).unwrap_or(usize::MAX);
    let checkpoints: Vec<CommitmentTreeCheckpoint> = stream::iter(block_ids)
        .map(|block_id| async move {
            let checkpoint = source
                .fetch_chain_checkpoint(block_id.height, source_activations)
                .await?;
            if checkpoint.block_id != block_id {
                return Err(BenchError::fixture_format(format!(
                    "node checkpoint at height {} names a block other than the captured one",
                    block_id.height.value()
                )));
            }
            Ok(checkpoint)
        })
        .buffered(concurrency)
        .try_collect()
        .await?;
    let sidecar =
        TipReplayCheckpoints::from_checkpoints(&manifest, source_activations, &checkpoints)?;
    sidecar.write_new(fixture_directory)?;
    Ok(sidecar)
}

/// Fixture source that also serves the admitted per-height checkpoints.
///
/// Blocks, the fixed tip, and subtree roots come from the fixture. A tree-state
/// checkpoint is served only for a fixture height and only for the fixture's
/// activation table, which is all canonical following asks for while it
/// appends the fixture blocks.
#[derive(Clone)]
pub struct TipReplayNodeSource {
    fixture_source: FixtureNodeSource,
    checkpoints: Arc<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>>,
    activations_fingerprint: NetworkUpgradeActivationsFingerprint,
    checkpoints_sha256: String,
}

impl TipReplayNodeSource {
    /// Opens the fixture and admits its tip replay checkpoints.
    pub fn open(fixture_directory: &Path, manifest: &FixtureManifest) -> Result<Self, BenchError> {
        let (sidecar, checkpoints_sha256) = read_with_file_digest(fixture_directory, manifest)?;
        Ok(Self {
            fixture_source: FixtureNodeSource::open(fixture_directory, manifest)?,
            checkpoints: Arc::new(sidecar.decode_checkpoints()?),
            activations_fingerprint: manifest
                .activations_typed()?
                .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1),
            checkpoints_sha256,
        })
    }

    /// Returns the SHA-256 of the admitted sidecar file bytes.
    #[must_use]
    pub fn checkpoints_sha256(&self) -> &str {
        &self.checkpoints_sha256
    }

    /// Returns the admitted checkpoint after the fixture block at `height`.
    #[must_use]
    pub fn checkpoint_at(&self, height: BlockHeight) -> Option<&CommitmentTreeCheckpoint> {
        self.checkpoints.get(&height)
    }
}

#[async_trait]
impl NodeSource for TipReplayNodeSource {
    fn capabilities(&self) -> NodeCapabilities {
        self.fixture_source.capabilities()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.fixture_source.fetch_block_at(height).await
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        self.fixture_source.fetch_chain_segment(limits).await
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        if network_upgrade_activations.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
            != self.activations_fingerprint
        {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "checkpoint activation table does not match the tip replay fixture",
            });
        }
        self.checkpoints
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "tip replay checkpoints cover only the fixture heights".to_owned(),
            })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.fixture_source.tip_id().await
    }

    async fn fetch_subtree_roots(
        &self,
        protocol: ShieldedProtocol,
        start_index: SubtreeRootIndex,
        max_entries: NonZeroU32,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.fixture_source
            .fetch_subtree_roots(protocol, start_index, max_entries)
            .await
    }

    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.fixture_source.fetch_subtree_root_range(range).await
    }
}

fn read_with_file_digest(
    fixture_directory: &Path,
    manifest: &FixtureManifest,
) -> Result<(TipReplayCheckpoints, String), BenchError> {
    let path = fixture_directory.join(TIP_REPLAY_CHECKPOINTS_FILE_NAME);
    let bytes = std::fs::read(&path).map_err(|source| BenchError::io(&path, source))?;
    let sidecar: TipReplayCheckpoints = serde_json::from_slice(&bytes)?;
    sidecar.validate(manifest)?;
    Ok((sidecar, hex::encode(Sha256::digest(&bytes))))
}

/// Returns every fixture block identity after checking segment digests and linkage.
fn connected_fixture_block_ids(
    fixture_directory: &Path,
    manifest: &FixtureManifest,
) -> Result<Vec<BlockId>, BenchError> {
    let network = manifest.network_typed()?;
    let mut block_ids = Vec::with_capacity(usize::try_from(manifest.block_count).unwrap_or(0));
    let mut previous: Option<BlockId> = None;
    for descriptor in &manifest.segments {
        for block in read_segment_blocks(fixture_directory, descriptor, network)? {
            if let Some(previous) = previous
                && (previous.height.next() != Some(block.height)
                    || previous.hash != block.parent_hash)
            {
                return Err(BenchError::fixture_format(format!(
                    "fixture blocks {} and {} are not an ordered connected pair",
                    previous.height.value(),
                    block.height.value()
                )));
            }
            let block_id = BlockId::new(block.height, block.hash);
            previous = Some(block_id);
            block_ids.push(block_id);
        }
    }
    if block_ids.first().map(|block_id| block_id.height.value()) != Some(manifest.from_height)
        || previous != Some(manifest.tip_id()?)
    {
        return Err(BenchError::fixture_format(
            "fixture blocks do not span the manifest range",
        ));
    }
    Ok(block_ids)
}

fn existing_sidecar_error(final_path: &Path) -> BenchError {
    BenchError::invalid_argument(format!(
        "tip replay checkpoints already exist at {}",
        final_path.display()
    ))
}
