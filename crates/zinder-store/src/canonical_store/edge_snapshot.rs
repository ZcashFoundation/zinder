//! Signed canonical checkpoints and private bootstrap staging.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    future::Future,
    io::{self, Read, Write},
    num::NonZeroU32,
    os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};

use ring::signature::{ED25519, Ed25519KeyPair, UnparsedPublicKey};
use rust_rocksdb::{BottommostLevelCompaction, CompactOptions, IteratorMode, WriteBatch};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zinder_core::{
    BlockHeight, BlockId, CommitmentTreeCheckpoint, CommitmentTreeFrontier,
    NetworkUpgradeActivations, NetworkUpgradeActivationsFingerprintVersion, SUBTREE_LEAF_COUNT,
    ShieldedProtocol, SubtreeRootArtifact, SubtreeRootIndex, SubtreeRootRange, TransactionId,
};

use crate::RocksDbResourceBudget;

use super::{
    CANONICAL_STORE_SCHEMA_VERSION, CanonicalReorgPolicy, CanonicalStoreError,
    CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalStore,
    block_replay::read_persisted_replay,
    construction_manifest::CANONICAL_CONSTRUCTION_MANIFEST_FILE_NAME,
    control::{decode_store_control, encode_ready_store_control_with_revisions},
    mempool_lifecycle::{MEMPOOL_EVENT_RETENTION_FLOOR_KEY, MEMPOOL_EVENT_SEQUENCE_KEY},
    publication::column_family,
    rocksdb::{MEMPOOL_EVENT_COLUMN_FAMILY, STORE_CONTROL_KEY},
};

/// Signed manifest filename.
pub const MANIFEST_FILE: &str = "manifest.v1.json";
/// Raw Ed25519 signature filename.
pub const SIGNATURE_FILE: &str = "manifest.v1.sig";
/// Maximum accepted manifest size in bytes.
pub const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
/// Default number of randomly sampled retained blocks.
pub const DEFAULT_SPOT_CHECK_BLOCKS: NonZeroU32 = NonZeroU32::MIN.saturating_add(31);

const FORMAT: &str = "zinder-edge-snapshot.v1";
const SIGNATURE_DOMAIN: &[u8] = b"zinder-edge-snapshot.v1\0";
const MAX_SNAPSHOT_ENTRIES: usize = 65_536;
const CANONICAL: &str = "canonical-rocksdb";
const SEALED: &str = "sealed-history";
const POOLS: [ShieldedProtocol; 3] = [
    ShieldedProtocol::Sapling,
    ShieldedProtocol::Orchard,
    ShieldedProtocol::Ironwood,
];

/// A snapshot was refused before publication, or publication could not be made durable.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EdgeSnapshotError {
    /// A snapshot contract was violated.
    #[error("edge snapshot refused: {0}")]
    Refused(String),
    /// A filesystem operation failed.
    #[error("edge snapshot filesystem operation failed: {0}")]
    Io(#[from] io::Error),
    /// The snapshot manifest could not be encoded or decoded.
    #[error("invalid edge snapshot manifest: {0}")]
    Manifest(#[from] serde_json::Error),
    /// Canonical store admission or a typed read failed.
    #[error(transparent)]
    Store(#[from] CanonicalStoreError),
}

fn refused(reason: impl Into<String>) -> EdgeSnapshotError {
    EdgeSnapshotError::Refused(reason.into())
}

/// Authenticated v1 snapshot inventory and consensus commitments.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeSnapshotManifest {
    format: String,
    anchors: ManifestAnchors,
    files: Vec<SnapshotFile>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotFile {
    path: String,
    bytes: u64,
    sha256: [u8; 32],
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestAnchors {
    canonical_schema_version: u16,
    network_id: u32,
    genesis_hash: [u8; 32],
    activation_fingerprint: [u8; 32],
    workload: CanonicalStoreWorkload,
    raw_blob_retention: RawBlobRetention,
    reorg_window_blocks: u32,
    history_floor: ManifestBlock,
    snapshot: ManifestBlock,
    trees: [Option<ManifestTree>; 3],
}

#[derive(Deserialize, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestBlock {
    height: u32,
    hash: [u8; 32],
}

impl From<BlockId> for ManifestBlock {
    fn from(block: BlockId) -> Self {
        Self {
            height: block.height.value(),
            hash: block.hash.as_bytes(),
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestTree {
    size: u32,
    root: [u8; 32],
    terminal_subtree: Option<ManifestSubtree>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestSubtree {
    index: u32,
    root: [u8; 32],
    completed_at: ManifestBlock,
}

impl EdgeSnapshotManifest {
    /// Verifies a detached signature over the exact manifest bytes and validates the inventory.
    ///
    /// # Errors
    ///
    /// Refuses unsupported formats, malformed manifests, and signatures that no trusted key verifies.
    pub fn verify(
        manifest: &[u8],
        signature: &[u8],
        trusted_keys: &[[u8; 32]],
    ) -> Result<Self, EdgeSnapshotError> {
        if manifest.len() as u64 > MAX_MANIFEST_BYTES || signature.len() != 64 {
            return Err(refused("invalid manifest or signature length"));
        }
        let message = signature_message(manifest);
        if !trusted_keys.iter().any(|key| {
            UnparsedPublicKey::new(&ED25519, key)
                .verify(&message, signature)
                .is_ok()
        }) {
            return Err(refused("manifest signature verification failed"));
        }
        let decoded: Self = serde_json::from_slice(manifest)?;
        decoded.validate_inventory()?;
        Ok(decoded)
    }

    /// Returns authenticated relative paths, lengths, and SHA-256 digests.
    pub fn files(&self) -> impl Iterator<Item = (&str, u64, [u8; 32])> {
        self.files
            .iter()
            .map(|file| (file.path.as_str(), file.bytes, file.sha256))
    }

    /// Refuses a signed snapshot whose store identity differs from the receiver's.
    ///
    /// # Errors
    ///
    /// Returns a refusal when the network, genesis, activation schedule, workload,
    /// raw blob retention, or replacement depth differs.
    pub fn validate_store_config(
        &self,
        config: &EdgeSnapshotStoreConfig<'_>,
    ) -> Result<(), EdgeSnapshotError> {
        let network = config.activations.network();
        let fingerprint = config
            .activations
            .fingerprint(NetworkUpgradeActivationsFingerprintVersion::CURRENT);
        let anchors = &self.anchors;
        if anchors.network_id != network.id()
            || anchors.genesis_hash != network.genesis_hash().as_bytes()
            || anchors.activation_fingerprint != fingerprint.as_bytes()
            || anchors.workload != config.workload
            || anchors.raw_blob_retention != config.raw_blob_retention
            || anchors.reorg_window_blocks != config.reorg_policy.reorg_window_blocks()
        {
            return Err(refused(
                "signed snapshot store identity differs from the receiver configuration",
            ));
        }
        Ok(())
    }

    fn validate_inventory(&self) -> Result<(), EdgeSnapshotError> {
        if self.format != FORMAT
            || self.anchors.canonical_schema_version != CANONICAL_STORE_SCHEMA_VERSION
        {
            return Err(refused("unsupported snapshot format"));
        }
        if self.files.len() > MAX_SNAPSHOT_ENTRIES {
            return Err(refused("snapshot inventory exceeds the entry limit"));
        }
        let mut paths = BTreeSet::new();
        for file in &self.files {
            let components = file.path.split('/').collect::<Vec<_>>();
            if components.len() > 16
                || !components.iter().all(|name| valid_component(name))
                || !match components.as_slice() {
                    [CANONICAL, name] => checkpoint_file(name),
                    [SEALED, _, ..] => true,
                    _ => false,
                }
                || !paths.insert(file.path.as_str())
            {
                return Err(refused("unsafe, unexpected, or duplicate snapshot path"));
            }
        }
        let directories = manifest_directories(paths.iter().copied());
        if paths.len() + directories.len() + 2 > MAX_SNAPSHOT_ENTRIES {
            return Err(refused("snapshot inventory exceeds the entry limit"));
        }
        if !paths.contains("canonical-rocksdb/CURRENT")
            || !paths.contains(
                format!("{CANONICAL}/{CANONICAL_CONSTRUCTION_MANIFEST_FILE_NAME}").as_str(),
            )
            || !paths
                .iter()
                .any(|path| path.starts_with("canonical-rocksdb/MANIFEST-"))
            || directories
                .iter()
                .any(|directory| paths.contains(directory))
        {
            return Err(refused("incomplete or conflicting snapshot inventory"));
        }
        Ok(())
    }
}

fn valid_component(name: &str) -> bool {
    name.len() <= 255
        && !matches!(name, "" | "." | "..")
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn checkpoint_file(name: &str) -> bool {
    let numbered = |suffix: &str| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit());
    matches!(
        name,
        "CURRENT" | "IDENTITY" | CANONICAL_CONSTRUCTION_MANIFEST_FILE_NAME
    ) || ["MANIFEST-", "OPTIONS-"]
        .iter()
        .any(|prefix| name.strip_prefix(prefix).is_some_and(numbered))
        || [".sst", ".blob", ".log"]
            .iter()
            .any(|suffix| name.strip_suffix(suffix).is_some_and(numbered))
}

fn signature_message(manifest: &[u8]) -> Vec<u8> {
    [SIGNATURE_DOMAIN, manifest].concat()
}

/// Exact store contract resolved by the receiving writer.
pub struct EdgeSnapshotStoreConfig<'a> {
    /// Activation schedule fetched from the configured validator.
    pub activations: &'a NetworkUpgradeActivations,
    /// Canonical workload expected by the writer.
    pub workload: CanonicalStoreWorkload,
    /// Raw block retention expected by the writer.
    pub raw_blob_retention: RawBlobRetention,
    /// Exact configured canonical replacement depth.
    pub reorg_policy: CanonicalReorgPolicy,
    /// Resource budget for cold admission.
    pub resource_budget: RocksDbResourceBudget,
}

impl EdgeSnapshotStoreConfig<'_> {
    fn open(&self, path: &Path) -> Result<RocksDbCanonicalStore, EdgeSnapshotError> {
        Ok(RocksDbCanonicalStore::open_ready(
            path,
            self.activations,
            self.workload,
            self.raw_blob_retention,
            self.reorg_policy,
            self.resource_budget,
        )?)
    }
}

/// Consensus evidence read directly from the admitted private store.
pub struct EdgeSnapshotAttestation {
    /// First retained block.
    pub history_floor: BlockId,
    /// Settled snapshot boundary and visible tip.
    pub checkpoint: CommitmentTreeCheckpoint,
    /// Terminal completed roots, ordered Sapling, Orchard, and Ironwood.
    pub terminal_subtrees: [Option<SubtreeRootArtifact>; 3],
    /// Unpredictably selected block commitments, including the snapshot boundary.
    pub blocks: Vec<EdgeSnapshotBlock>,
}

/// Canonical block identity and exact transaction sequence for a spot check.
pub struct EdgeSnapshotBlock {
    /// Block identity.
    pub block: BlockId,
    /// Transaction IDs in consensus order.
    pub transaction_ids: Vec<TransactionId>,
}

impl EdgeSnapshotAttestation {
    fn read(store: &RocksDbCanonicalStore) -> Result<Self, EdgeSnapshotError> {
        let epoch = store.chain_epoch()?;
        let visible_tip = BlockId::new(epoch.visible_tip_height, epoch.visible_tip_hash);
        let settled_tip = BlockId::new(epoch.settled_tip_height, epoch.settled_tip_hash);
        if visible_tip != settled_tip || !visible_tip.height.value().is_multiple_of(4096) {
            return Err(refused(
                "visible tip must be settled and aligned to 4096 blocks",
            ));
        }
        let activations = store
            .network_upgrade_activations()
            .ok_or_else(|| refused("missing admitted schedule"))?;
        let checkpoint = store
            .tree_state_checkpoint_at(visible_tip.height, activations)?
            .filter(|checkpoint| checkpoint.block_id == visible_tip)
            .ok_or_else(|| refused("missing snapshot tree state"))?;
        let mut terminal_subtrees = [None, None, None];
        for (slot, (protocol, frontier)) in terminal_subtrees
            .iter_mut()
            .zip(POOLS.into_iter().zip(frontiers(&checkpoint)))
        {
            if let Some(index) =
                frontier.and_then(|tree| (tree.tree_size() / SUBTREE_LEAF_COUNT).checked_sub(1))
            {
                let range =
                    SubtreeRootRange::new(protocol, SubtreeRootIndex::new(index), NonZeroU32::MIN);
                *slot = Some(
                    store
                        .subtree_roots(range)?
                        .pop()
                        .filter(|root| root.completing_block_height <= visible_tip.height)
                        .ok_or_else(|| refused("missing terminal completed subtree"))?,
                );
            }
        }
        Ok(Self {
            history_floor: store.ready_evidence().first_retained_block,
            checkpoint,
            terminal_subtrees,
            blocks: Vec::new(),
        })
    }

    fn manifest_anchors(
        &self,
        store: &RocksDbCanonicalStore,
    ) -> Result<ManifestAnchors, EdgeSnapshotError> {
        let schedule = store
            .network_upgrade_schedule()
            .ok_or_else(|| refused("missing admitted schedule"))?;
        let frontiers = frontiers(&self.checkpoint);
        Ok(ManifestAnchors {
            canonical_schema_version: CANONICAL_STORE_SCHEMA_VERSION,
            network_id: store.network().id(),
            genesis_hash: store.network().genesis_hash().as_bytes(),
            activation_fingerprint: schedule.activations_fingerprint().as_bytes(),
            workload: store.workload(),
            raw_blob_retention: store.raw_blob_retention(),
            reorg_window_blocks: store.reorg_policy().reorg_window_blocks(),
            history_floor: self.history_floor.into(),
            snapshot: self.checkpoint.block_id.into(),
            trees: std::array::from_fn(|index| {
                frontiers[index].map(|tree| ManifestTree {
                    size: tree.tree_size(),
                    root: tree.final_root().as_bytes(),
                    terminal_subtree: self.terminal_subtrees[index].as_ref().map(|root| {
                        ManifestSubtree {
                            index: root.subtree_index.value(),
                            root: root.root_hash.as_bytes(),
                            completed_at: BlockId::new(
                                root.completing_block_height,
                                root.completing_block_hash,
                            )
                            .into(),
                        }
                    }),
                })
            }),
        })
    }

    fn sample(
        &mut self,
        store: &RocksDbCanonicalStore,
        count: NonZeroU32,
    ) -> Result<(), EdgeSnapshotError> {
        let last = u64::from(self.checkpoint.block_id.height.value());
        let first = u64::from(self.history_floor.height.value());
        let span = last - first + 1;
        let count = u64::from(count.get()).min(span);
        // Floyd's algorithm draws distinct uniform offsets with O(K) storage.
        let mut offsets = BTreeSet::new();
        for upper in span - count..span {
            let offset = random_below(upper + 1)?;
            if !offsets.insert(offset) {
                offsets.insert(upper);
            }
        }
        offsets.insert(span - 1);
        for offset in offsets {
            let height = BlockHeight::new(
                u32::try_from(first + offset).map_err(|_| refused("invalid sampled height"))?,
            );
            let replay = read_persisted_replay(&store.bounded_open.db, height)?.replay;
            let facts = replay.facts();
            let header = store
                .block_header_at(height)?
                .ok_or_else(|| refused("missing sampled header"))?;
            if header != facts.block_header {
                return Err(refused("sampled header disagrees with canonical replay"));
            }
            self.blocks.push(EdgeSnapshotBlock {
                block: BlockId::new(height, header.block_hash),
                transaction_ids: facts
                    .transactions
                    .iter()
                    .map(|tx| tx.public_facts.transaction_id)
                    .collect(),
            });
        }
        Ok(())
    }
}

fn frontiers(checkpoint: &CommitmentTreeCheckpoint) -> [Option<&CommitmentTreeFrontier>; 3] {
    [
        checkpoint.frontiers.sapling(),
        checkpoint.frontiers.orchard(),
        checkpoint.frontiers.ironwood(),
    ]
}

fn entropy<const N: usize>() -> Result<[u8; N], EdgeSnapshotError> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes)
        .map_err(|error| refused(format!("OS randomness failed: {error}")))?;
    Ok(bytes)
}

fn random_below(bound: u64) -> Result<u64, EdgeSnapshotError> {
    let threshold = bound.wrapping_neg() % bound;
    loop {
        let draw = u64::from_ne_bytes(entropy()?);
        if draw >= threshold {
            return Ok(draw % bound);
        }
    }
}

fn reset_local_state(store: &RocksDbCanonicalStore) -> Result<(), EdgeSnapshotError> {
    store.clear_restored_retention_leases()?;
    let db = &store.bounded_open.db;
    let db_error = |source| CanonicalStoreError::RocksDbOperation {
        operation: "reset snapshot-local state",
        source,
    };
    let local_tables = || {
        db.live_files()
            .map(|files| {
                files
                    .into_iter()
                    .filter(|file| {
                        matches!(
                            file.column_family_name.as_str(),
                            "default" | MEMPOOL_EVENT_COLUMN_FAMILY
                        )
                    })
                    .map(|file| file.name)
                    .collect::<BTreeSet<_>>()
            })
            .map_err(db_error)
    };
    let inherited_tables = local_tables()?;
    let encoded = db
        .get(STORE_CONTROL_KEY)
        .map_err(db_error)?
        .ok_or_else(|| refused("missing store control"))?;
    let control = decode_store_control(db.path(), &encoded)?;
    let key = entropy()?;
    let control_bytes = encode_ready_store_control_with_revisions(
        control.workload,
        &control.build_plan,
        key,
        &store.ready_evidence(),
        control.revisions(),
    )
    .map_err(|error| refused(error.to_string()))?;
    let mut changes = WriteBatch::default();
    let events = column_family(db, MEMPOOL_EVENT_COLUMN_FAMILY)?;
    let first = db
        .iterator_cf(&events, IteratorMode::Start)
        .next()
        .transpose()
        .map_err(db_error)?;
    if let Some((first, _)) = first {
        let (last, _) = db
            .iterator_cf(&events, IteratorMode::End)
            .next()
            .transpose()
            .map_err(db_error)?
            .ok_or_else(|| refused("mempool event range changed during sanitization"))?;
        // Appending a zero byte places the exclusive end immediately after the last key.
        let mut end = last.into_vec();
        end.push(0);
        changes.delete_range_cf(&events, first.as_ref(), end.as_slice());
    }
    changes.delete(MEMPOOL_EVENT_SEQUENCE_KEY);
    changes.delete(MEMPOOL_EVENT_RETENTION_FLOOR_KEY);
    changes.put(STORE_CONTROL_KEY, control_bytes);
    db.write(&changes).map_err(db_error)?;
    db.flush().map_err(db_error)?;
    db.flush_cf(&events).map_err(db_error)?;
    let mut options = CompactOptions::default();
    options.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
    db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &options);
    db.compact_range_cf_opt(&events, None::<&[u8]>, None::<&[u8]>, &options);
    if !local_tables()?.is_disjoint(&inherited_tables) {
        return Err(refused(
            "compaction retained inherited control or mempool tables",
        ));
    }
    db.flush_wal(true).map_err(db_error)?;
    Ok(())
}

/// Exports a settled boundary from an admitted canonical owner.
///
/// The optional sealed-history directory contributes opaque files. Both the
/// checkpoint and the inventory are complete before the output becomes visible.
///
/// # Errors
///
/// Refuses an occupied output, an ineligible tip, an invalid PKCS#8 key, or
/// failed checkpoint, filesystem, or store admission operations.
pub fn export_edge_snapshot(
    store: &mut RocksDbCanonicalStore,
    output: &Path,
    signing_key: &Path,
    sealed_history: Option<&Path>,
) -> Result<(), EdgeSnapshotError> {
    let output = resolve_destination(output)?;
    let stage = sibling_path(&output, ".export.staging")?;
    let signing_path = signing_key.canonicalize()?;
    let key_file = open_regular(&signing_path)?;
    let metadata = key_file.metadata()?;
    let signing_identity = (metadata.dev(), metadata.ino());
    let sealed = sealed_history
        .map(|path| ExportHistory::inspect(path, &output, &stage, &signing_path, signing_identity))
        .transpose()?;
    // PKCS#8 v1 carries only the seed. Ring checks the embedded public key in v2.
    let key = Ed25519KeyPair::from_pkcs8_maybe_unchecked(&read_bounded(&key_file, 1024)?)
        .map_err(|_| refused("invalid Ed25519 PKCS#8 signing key"))?;
    let anchors = EdgeSnapshotAttestation::read(store)?.manifest_anchors(store)?;
    let work = Workspace::create(stage)?;
    let working_copy = work.0.join("working-copy");
    store.create_owner_checkpoint_physical(&working_copy)?;
    let config = EdgeSnapshotStoreConfig {
        activations: store
            .network_upgrade_activations()
            .ok_or_else(|| refused("missing admitted schedule"))?,
        workload: store.workload(),
        raw_blob_retention: store.raw_blob_retention(),
        reorg_policy: store.reorg_policy(),
        resource_budget: RocksDbResourceBudget::read_only_probe_defaults(),
    };
    let copy = config.open(&working_copy)?;
    reset_local_state(&copy)?;
    drop(copy);
    let mut copy = config.open(&working_copy)?;
    if EdgeSnapshotAttestation::read(&copy)?.manifest_anchors(&copy)? != anchors {
        return Err(refused("checkpoint anchors changed during export"));
    }
    let clean_bundle = work.0.join("bundle");
    create_private_dir(&clean_bundle)?;
    copy.create_owner_checkpoint_physical(clean_bundle.join(CANONICAL))?;
    drop(copy);
    let mut files = Vec::new();
    for (relative, path) in inventory(&clean_bundle)? {
        let (bytes, sha256) = transfer(&path, None, None)?;
        if Path::new(&relative).extension() == Some(std::ffi::OsStr::new("log")) && bytes != 0 {
            return Err(refused("exported checkpoint contains a nonempty WAL"));
        }
        files.push(SnapshotFile {
            path: relative,
            bytes,
            sha256,
        });
    }
    if let Some(sealed) = sealed {
        sealed.copy_to(&clean_bundle, &mut files)?;
    }
    let manifest = EdgeSnapshotManifest {
        format: FORMAT.to_owned(),
        anchors,
        files,
    };
    manifest.validate_inventory()?;
    let encoded = serde_json::to_vec(&manifest)?;
    if encoded.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(refused("snapshot manifest exceeds the size limit"));
    }
    fs::write(
        clean_bundle.join(SIGNATURE_FILE),
        key.sign(&signature_message(&encoded)).as_ref(),
    )?;
    fs::write(clean_bundle.join(MANIFEST_FILE), encoded)?;
    sync_tree(&clean_bundle)?;
    publish(&clean_bundle, &output)
}

struct ExportHistory<'a> {
    root: PathBuf,
    paths: Vec<String>,
    signing_path: &'a Path,
    signing_identity: (u64, u64),
}

impl<'a> ExportHistory<'a> {
    fn inspect(
        root: &Path,
        output: &Path,
        stage: &Path,
        signing_path: &'a Path,
        signing_identity: (u64, u64),
    ) -> Result<Self, EdgeSnapshotError> {
        if !fs::symlink_metadata(root)?.is_dir() {
            return Err(refused("sealed-history input must be a real directory"));
        }
        let root = root.canonicalize()?;
        require_disjoint(&[&root, output, stage])?;
        let paths = inventory(&root)?.into_keys().collect();
        let sealed = Self {
            root,
            paths,
            signing_path,
            signing_identity,
        };
        for relative in &sealed.paths {
            sealed.open_file(relative)?;
        }
        Ok(sealed)
    }

    fn open_file(&self, relative: &str) -> Result<File, EdgeSnapshotError> {
        let input = open_snapshot_file(&self.root, relative)?;
        let metadata = input.metadata()?;
        if self.root.join(relative) == self.signing_path
            || (metadata.dev(), metadata.ino()) == self.signing_identity
        {
            return Err(refused(
                "sealed-history input contains the publisher signing key",
            ));
        }
        Ok(input)
    }

    fn copy_to(
        &self,
        bundle: &Path,
        files: &mut Vec<SnapshotFile>,
    ) -> Result<(), EdgeSnapshotError> {
        for relative in &self.paths {
            let input = self.open_file(relative)?;
            let path = format!("{SEALED}/{relative}");
            let destination = bundle.join(&path);
            fs::create_dir_all(parent(&destination))?;
            let (bytes, sha256) = transfer_from(input, Some(&destination), None)?;
            files.push(SnapshotFile {
                path,
                bytes,
                sha256,
            });
        }
        Ok(())
    }
}

/// A canonical destination and its exclusively reserved private workspaces.
///
/// Dropping the reservation removes the workspaces it created. Destination
/// paths are checked again at publication.
pub struct EdgeSnapshotDestination {
    target: PathBuf,
    stage: Workspace,
    download: Option<Workspace>,
    sealed: Option<(PathBuf, Workspace)>,
}

impl EdgeSnapshotDestination {
    /// Checks every destination and reserves private scratch directories.
    ///
    /// Set `download` for an HTTP source that needs a private download directory.
    ///
    /// # Errors
    ///
    /// Refuses occupied or overlapping destinations and preexisting scratch directories.
    pub fn reserve(
        target: &Path,
        sealed_history_output: Option<&Path>,
        download: bool,
    ) -> Result<Self, EdgeSnapshotError> {
        let target = resolve_destination(target)?;
        let stage = sibling_path(&target, ".bootstrap.staging")?;
        let download = download
            .then(|| sibling_path(&target, ".bootstrap.download"))
            .transpose()?;
        let sealed = sealed_history_output.map(resolve_destination).transpose()?;
        let sealed_stage = sealed
            .as_deref()
            .map(|path| sibling_path(path, ".bootstrap.staging"))
            .transpose()?;
        let mut paths = vec![target.as_path(), stage.as_path()];
        paths.extend(download.as_deref());
        paths.extend(sealed.as_deref());
        paths.extend(sealed_stage.as_deref());
        require_disjoint(&paths)?;
        Ok(Self {
            target,
            stage: Workspace::create(stage)?,
            download: download.map(Workspace::create).transpose()?,
            sealed: sealed
                .zip(sealed_stage)
                .map(|(path, stage)| Ok::<_, EdgeSnapshotError>((path, Workspace::create(stage)?)))
                .transpose()?,
        })
    }

    /// Returns the reserved HTTP download directory when requested.
    #[must_use]
    pub fn download_path(&self) -> Option<&Path> {
        self.download
            .as_ref()
            .map(|workspace| workspace.0.as_path())
    }
}

/// Local inputs for verifying a downloaded or published snapshot.
pub struct EdgeSnapshotBootstrap<'a> {
    /// Immutable local directory containing the bundle.
    pub source: &'a Path,
    /// Acceptable raw Ed25519 publisher public keys.
    pub trusted_keys: &'a [[u8; 32]],
    /// Positive number of random retained blocks to verify.
    pub spot_check_blocks: NonZeroU32,
}

/// Verifies and privately admits a snapshot, awaits validator approval, and
/// publishes the canonical primary atomically.
///
/// The attestation callback must compare the supplied evidence with the
/// configured validator. A callback error aborts publication. A preexisting
/// staging directory is refused and can be examined after an interrupted run.
///
/// # Errors
///
/// Returns an error on invalid input, failed admission or attestation, or
/// filesystem failure. After successful rename, a directory sync error
/// reports uncertain durability and the target may already be visible.
pub async fn prepare_bootstrapped_store<F, E>(
    destination: EdgeSnapshotDestination,
    bootstrap: EdgeSnapshotBootstrap<'_>,
    config: EdgeSnapshotStoreConfig<'_>,
    attest: impl FnOnce(EdgeSnapshotAttestation) -> F,
) -> Result<(), E>
where
    F: Future<Output = Result<(), E>>,
    E: From<EdgeSnapshotError>,
{
    let stage = &destination.stage.0;
    let canonical = stage.join(CANONICAL);
    let manifest = stage_files(&bootstrap, stage, &config)?;
    let staged_store = config.open(&canonical)?;
    let mut evidence = EdgeSnapshotAttestation::read(&staged_store)?;
    if evidence.manifest_anchors(&staged_store)? != manifest.anchors {
        return Err(refused("signed anchors disagree with the staged store").into());
    }
    evidence.sample(&staged_store, bootstrap.spot_check_blocks)?;
    reset_local_state(&staged_store)?;
    drop(staged_store);
    drop(config.open(&canonical)?);
    attest(evidence).await?;
    fs::set_permissions(&canonical, fs::Permissions::from_mode(0o700))
        .map_err(EdgeSnapshotError::from)?;
    sync_tree(&canonical)?;
    commit_bootstrap(&destination, &canonical)?;
    Ok(())
}

fn stage_files(
    bootstrap: &EdgeSnapshotBootstrap<'_>,
    stage: &Path,
    config: &EdgeSnapshotStoreConfig<'_>,
) -> Result<EdgeSnapshotManifest, EdgeSnapshotError> {
    let verified = EdgeSnapshotManifest::verify(
        &read_bounded(
            open_snapshot_file(bootstrap.source, MANIFEST_FILE)?,
            MAX_MANIFEST_BYTES,
        )?,
        &read_bounded(open_snapshot_file(bootstrap.source, SIGNATURE_FILE)?, 64)?,
        bootstrap.trusted_keys,
    )?;
    verified.validate_store_config(config)?;
    validate_local_inventory(bootstrap.source, &verified)?;
    for (path, bytes, sha256) in verified.files() {
        let target = stage.join(path);
        fs::create_dir_all(parent(&target))?;
        let input = open_snapshot_file(bootstrap.source, path)?;
        if transfer_from(input, Some(&target), Some(bytes))? != (bytes, sha256) {
            return Err(refused(format!(
                "snapshot file failed length or digest verification: {path}"
            )));
        }
    }
    let current = read_bounded(open_regular(&stage.join(CANONICAL).join("CURRENT"))?, 255)?;
    let name = std::str::from_utf8(&current)
        .ok()
        .and_then(|line| line.strip_suffix('\n'));
    if !name.is_some_and(|name| {
        name.starts_with("MANIFEST-")
            && checkpoint_file(name)
            && verified
                .files()
                .any(|(path, _, _)| path.strip_prefix("canonical-rocksdb/") == Some(name))
    }) {
        return Err(refused(
            "checkpoint CURRENT must name a signed manifest file",
        ));
    }
    Ok(verified)
}

fn commit_bootstrap(
    destination: &EdgeSnapshotDestination,
    canonical: &Path,
) -> Result<(), EdgeSnapshotError> {
    let target = &destination.target;
    let sealed = destination.stage.0.join(SEALED);
    let Some((output, sidecar)) = &destination.sealed else {
        return publish(canonical, target);
    };
    require_empty_or_absent(output)?;
    if !sealed.is_dir() {
        return publish(canonical, target);
    }
    let files = sidecar.0.join(SEALED);
    create_private_dir(&files)?;
    for (relative, source) in inventory(&sealed)? {
        let target = files.join(relative);
        fs::create_dir_all(parent(&target))?;
        transfer(&source, Some(&target), None)?;
    }
    sync_tree(&files)?;
    rename_no_replace(&files, output)?;
    let commit = File::open(parent(output))
        .and_then(|directory| directory.sync_all())
        .map_err(EdgeSnapshotError::from)
        .and_then(|()| rename_no_replace(canonical, target));
    if let Err(error) = commit {
        fs::remove_dir_all(output)?;
        File::open(parent(output))?.sync_all()?;
        return Err(error);
    }
    File::open(parent(target))?.sync_all()?;
    Ok(())
}

fn read_bounded(input: impl Read, limit: u64) -> Result<Vec<u8>, EdgeSnapshotError> {
    let mut bytes = Vec::new();
    input.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(refused("snapshot metadata exceeds the size limit"));
    }
    Ok(bytes)
}

fn open_regular(path: &Path) -> Result<File, EdgeSnapshotError> {
    use rustix::fs::{Mode, OFlags, open};

    let file: File = open(
        path,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(io::Error::from)?
    .into();
    if !file.metadata()?.is_file() {
        return Err(refused("snapshot input is not a regular file"));
    }
    Ok(file)
}

// Resolve each component through a directory descriptor. Inventory checks
// alone cannot protect a later read if an input directory is replaced.
fn open_snapshot_file(root: &Path, relative: &str) -> Result<File, EdgeSnapshotError> {
    use rustix::fs::{Mode, OFlags, open, openat};

    let directories = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW;
    let mut directory = open(root, directories, Mode::empty()).map_err(io::Error::from)?;
    let mut components = relative.split('/').peekable();
    while let Some(name) = components.next() {
        if components.peek().is_none() {
            let file: File = openat(
                &directory,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(io::Error::from)?
            .into();
            if !file.metadata()?.is_file() {
                return Err(refused("snapshot input is not a regular file"));
            }
            return Ok(file);
        }
        directory =
            openat(&directory, name, directories, Mode::empty()).map_err(io::Error::from)?;
    }
    Err(refused("empty snapshot path"))
}

fn transfer(
    source: &Path,
    target: Option<&Path>,
    limit: Option<u64>,
) -> Result<(u64, [u8; 32]), EdgeSnapshotError> {
    transfer_from(open_regular(source)?, target, limit)
}

fn transfer_from(
    mut input: File,
    target: Option<&Path>,
    limit: Option<u64>,
) -> Result<(u64, [u8; 32]), EdgeSnapshotError> {
    let mut output = target.map(File::create_new).transpose()?;
    let mut buffer = vec![0; 128 * 1024];
    let mut hash = Sha256::new();
    let mut bytes = 0;
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        bytes += read as u64;
        if limit.is_some_and(|limit| bytes > limit) {
            return Err(refused("snapshot file exceeds its signed length"));
        }
        let chunk = &buffer[..read];
        hash.update(chunk);
        if let Some(output) = &mut output {
            output.write_all(chunk)?;
        }
    }
    Ok((bytes, hash.finalize().into()))
}

fn manifest_directories<'a>(paths: impl Iterator<Item = &'a str>) -> BTreeSet<&'a str> {
    paths
        .flat_map(|path| {
            std::iter::successors(path.rsplit_once('/').map(|(prefix, _)| prefix), |path| {
                path.rsplit_once('/').map(|(prefix, _)| prefix)
            })
        })
        .collect()
}

fn validate_local_inventory(
    root: &Path,
    manifest: &EdgeSnapshotManifest,
) -> Result<(), EdgeSnapshotError> {
    let mut files: BTreeSet<_> = manifest.files().map(|(path, _, _)| path).collect();
    let directories = manifest_directories(files.iter().copied());
    files.extend([MANIFEST_FILE, SIGNATURE_FILE]);
    walk_inventory(root, |relative, _, is_directory| {
        let listed = if is_directory {
            directories.contains(relative)
        } else {
            files.remove(relative)
        };
        if !listed {
            return Err(refused(format!("unlisted snapshot entry: {relative}")));
        }
        Ok(())
    })?;
    if !files.is_empty() {
        return Err(refused("snapshot directory is missing signed files"));
    }
    Ok(())
}

fn inventory(root: &Path) -> Result<BTreeMap<String, PathBuf>, EdgeSnapshotError> {
    let mut files = BTreeMap::new();
    walk_inventory(root, |relative, path, is_directory| {
        if !is_directory {
            files.insert(relative.to_owned(), path.to_path_buf());
        }
        Ok(())
    })?;
    Ok(files)
}

fn walk_inventory(
    root: &Path,
    mut visit: impl FnMut(&str, &Path, bool) -> Result<(), EdgeSnapshotError>,
) -> Result<(), EdgeSnapshotError> {
    if !fs::symlink_metadata(root)?.is_dir() {
        return Err(refused("snapshot input must be a real directory"));
    }
    let mut remaining = MAX_SNAPSHOT_ENTRIES;
    let mut directories = vec![(0, root.to_path_buf())];
    while let Some((depth, directory)) = directories.pop() {
        let mut entries = fs::read_dir(directory)?.peekable();
        if depth != 0 && entries.peek().is_none() {
            return Err(refused("empty snapshot directory"));
        }
        for entry in entries {
            remaining = remaining
                .checked_sub(1)
                .ok_or_else(|| refused("snapshot exceeds the entry limit"))?;
            let entry = entry?;
            if !entry.file_name().to_str().is_some_and(valid_component) {
                return Err(refused("unsafe snapshot filename"));
            }
            let path = entry.path();
            let kind = entry.file_type()?;
            let relative = path
                .strip_prefix(root)
                .ok()
                .and_then(Path::to_str)
                .ok_or_else(|| refused("invalid snapshot path"))?;
            if kind.is_file() {
                visit(relative, &path, false)?;
            } else if kind.is_dir() && depth < 15 {
                visit(relative, &path, true)?;
                directories.push((depth + 1, path));
            } else {
                return Err(refused(
                    "snapshot contains a symlink, special file, or excessive nesting",
                ));
            }
        }
    }
    Ok(())
}

fn sync_tree(root: &Path) -> Result<(), EdgeSnapshotError> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_tree(&entry.path())?;
        } else {
            open_regular(&entry.path())?.sync_all()?;
        }
    }
    File::open(root)?.sync_all()?;
    Ok(())
}

fn parent(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

fn create_private_dir(path: &Path) -> io::Result<()> {
    fs::DirBuilder::new().mode(0o700).create(path)
}

fn require_empty_or_absent(path: &Path) -> Result<(), EdgeSnapshotError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(metadata) if metadata.is_dir() && fs::read_dir(path)?.next().is_none() => Ok(()),
        Err(error) => Err(error.into()),
        Ok(_) => Err(refused(format!(
            "snapshot destination is occupied: {}",
            path.display()
        ))),
    }
}

fn rename_no_replace(source: &Path, target: &Path) -> Result<(), EdgeSnapshotError> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};
    require_empty_or_absent(target)?;
    match fs::remove_dir(target) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    renameat_with(CWD, source, CWD, target, RenameFlags::NOREPLACE).map_err(io::Error::from)?;
    Ok(())
}

fn publish(source: &Path, target: &Path) -> Result<(), EdgeSnapshotError> {
    rename_no_replace(source, target)?;
    File::open(parent(target))?.sync_all()?;
    Ok(())
}

struct Workspace(PathBuf);

fn resolve_destination(path: &Path) -> Result<PathBuf, EdgeSnapshotError> {
    let name = path
        .file_name()
        .ok_or_else(|| refused("missing destination filename"))?;
    let resolved = parent(path).canonicalize()?.join(name);
    require_empty_or_absent(&resolved)?;
    Ok(resolved)
}

fn sibling_path(path: &Path, suffix: &str) -> Result<PathBuf, EdgeSnapshotError> {
    let mut name = path
        .file_name()
        .ok_or_else(|| refused("missing destination filename"))?
        .to_os_string();
    name.push(suffix);
    Ok(parent(path).join(name))
}

fn require_disjoint(paths: &[&Path]) -> Result<(), EdgeSnapshotError> {
    for (index, path) in paths.iter().enumerate() {
        if paths[index + 1..]
            .iter()
            .any(|other| path.starts_with(other) || other.starts_with(path))
        {
            return Err(refused("snapshot paths must not overlap"));
        }
    }
    Ok(())
}

impl Workspace {
    fn create(path: PathBuf) -> Result<Self, EdgeSnapshotError> {
        create_private_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            tracing::warn!(path = %self.0.display(), %error, "failed to remove snapshot staging directory");
        }
    }
}

#[cfg(test)]
mod tests;
