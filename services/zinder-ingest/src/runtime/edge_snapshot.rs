//! Edge snapshot export and validator-attested bootstrap commands.

use std::{
    fs::{self, File},
    io::{self, Write},
    num::NonZeroU32,
    path::{Path, PathBuf},
    time::Duration,
};

use clap::{Args, Subcommand};
use reqwest::{Client, Url};
use sha2::{Digest, Sha256};
use zebra_chain::{block::Block, serialization::ZcashDeserializeInto};
use zinder_core::{
    BlockHeight, BlockId, Network, NetworkUpgradeActivations, SubtreeRootArtifact, TransactionId,
};
use zinder_source::{NodeSource, SourceError};
use zinder_store::{
    CanonicalReorgPolicy, RocksDbCanonicalStore,
    edge_snapshot::{
        DEFAULT_SPOT_CHECK_BLOCKS, EdgeSnapshotAttestation, EdgeSnapshotBootstrap,
        EdgeSnapshotDestination, EdgeSnapshotError, EdgeSnapshotManifest, EdgeSnapshotStoreConfig,
        MANIFEST_FILE, MAX_MANIFEST_BYTES, SIGNATURE_FILE, export_edge_snapshot,
        prepare_bootstrapped_store,
    },
};

use crate::configured_first_available_height;

use super::{
    IngestConfigError, IngestConfigOverrides, config, ingest_node_endpoint_for_target,
    validate_indexer_endpoint,
};

/// Edge-snapshot management commands for exporting and bootstrapping canonical stores.
#[derive(Debug, Subcommand)]
pub enum SnapshotCommand {
    /// Create a publisher-signed bundle from a settled canonical store.
    ExportEdgeSnapshot(ExportArgs),
    /// Verify a snapshot against a publisher key and the configured validator.
    BootstrapFromSnapshot(BootstrapArgs),
}

/// CLI arguments for exporting a signed canonical edge snapshot.
#[derive(Debug, Args)]
pub struct ExportArgs {
    /// Destination directory for the signed bundle.
    #[arg(long)]
    pub output_dir: PathBuf,
    /// File containing the DER-encoded Ed25519 PKCS#8 signing key.
    #[arg(long)]
    pub signing_key: PathBuf,
    /// Directory of opaque sealed-history files to include.
    #[arg(long)]
    pub include_sealed_history: Option<PathBuf>,
}

/// CLI arguments for bootstrapping a canonical store from a signed edge snapshot.
#[derive(Debug, Args)]
pub struct BootstrapArgs {
    /// Local bundle directory or HTTP(S) base URL.
    #[arg(long)]
    pub source: String,
    /// Trusted raw Ed25519 public key as 64 hexadecimal characters. Repeatable.
    #[arg(long, required = true, value_parser = parse_public_key)]
    pub public_key: Vec<[u8; 32]>,
    /// Number of random retained blocks to compare, plus the snapshot boundary.
    #[arg(long, default_value_t = DEFAULT_SPOT_CHECK_BLOCKS)]
    pub spot_check_blocks: NonZeroU32,
    /// Destination directory for verified opaque sealed-history files.
    #[arg(long)]
    pub sealed_history_output: Option<PathBuf>,
}

fn parse_public_key(key: &str) -> Result<[u8; 32], &'static str> {
    let mut bytes = [0; 32];
    hex::decode_to_slice(key, &mut bytes)
        .map_err(|_| "expected 64 hexadecimal characters for the Ed25519 public key")?;
    Ok(bytes)
}

/// Error returned while exporting or bootstrapping a canonical edge snapshot.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotCommandError {
    /// Snapshot bundle format or verification failed.
    #[error(transparent)]
    Snapshot(#[from] EdgeSnapshotError),
    /// Ingest configuration resolution failed.
    #[error(transparent)]
    Config(#[from] IngestConfigError),
    /// Canonical store read or creation failed.
    #[error(transparent)]
    Store(#[from] zinder_store::CanonicalStoreError),
    /// Upstream validator RPC failed.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// Local filesystem operation failed.
    #[error("snapshot I/O failed: {0}")]
    Io(#[from] io::Error),
    /// Remote bundle HTTP download failed.
    #[error("snapshot HTTP request failed: {0}")]
    Http(reqwest::Error),
    /// Validator attestation refused the candidate snapshot.
    #[error("snapshot refused: {0}")]
    Refused(&'static str),
}

impl From<reqwest::Error> for SnapshotCommandError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error.without_url())
    }
}

/// Executes an edge-snapshot export or validator-attested bootstrap command.
pub async fn run(
    command: SnapshotCommand,
    config_path: Option<PathBuf>,
    overrides: IngestConfigOverrides,
) -> Result<(), SnapshotCommandError> {
    let runtime = config::load_ingest_config(config_path, overrides)?.runtime_config;
    validate_indexer_endpoint(runtime.node_source, "node", &runtime.node)?;
    let validator = ingest_node_endpoint_for_target(runtime.node_source, &runtime.node)?;
    let activations = validator.fetch_network_upgrade_activations().await?;
    if activations.network() != runtime.node.network {
        return Err(SnapshotCommandError::Refused("validator network mismatch"));
    }
    let policy = CanonicalReorgPolicy::new(runtime.reorg_window_blocks)
        .map_err(crate::CanonicalWriterError::from)
        .map_err(IngestConfigError::from)?;
    let required_first_available_height =
        configured_first_available_height(runtime.run_overrides.checkpoint_height);
    let store_config = EdgeSnapshotStoreConfig {
        activations: &activations,
        raw_blob_retention: runtime.raw_blob_policy.to_retention(),
        reorg_policy: policy,
        required_first_available_height,
        resource_budget: runtime.canonical_rocksdb_budget,
    };
    match command {
        SnapshotCommand::ExportEdgeSnapshot(args) => {
            let mut store = RocksDbCanonicalStore::open_ready(
                &runtime.storage_path,
                &activations,
                store_config.raw_blob_retention,
                policy,
                store_config.resource_budget,
            )?;
            export_edge_snapshot(
                &mut store,
                &args.output_dir,
                &args.signing_key,
                args.include_sealed_history.as_deref(),
            )?;
        }
        SnapshotCommand::BootstrapFromSnapshot(args) => {
            bootstrap(
                &args,
                &runtime.storage_path,
                store_config,
                &validator,
                policy,
            )
            .await?;
        }
    }
    Ok(())
}

async fn bootstrap(
    args: &BootstrapArgs,
    target: &Path,
    store_config: EdgeSnapshotStoreConfig<'_>,
    validator: &impl NodeSource,
    policy: CanonicalReorgPolicy,
) -> Result<(), SnapshotCommandError> {
    let activations = store_config.activations;
    let required_first_available_height = store_config.required_first_available_height;
    let (destination, source) = prepare_source(args, target, &store_config).await?;
    prepare_bootstrapped_store(
        destination,
        EdgeSnapshotBootstrap {
            source: &source,
            trusted_keys: &args.public_key,
            spot_check_blocks: args.spot_check_blocks,
        },
        store_config,
        |evidence| {
            attest(
                validator,
                activations,
                policy,
                required_first_available_height,
                evidence,
            )
        },
    )
    .await
}

async fn attest(
    validator: &impl NodeSource,
    activations: &NetworkUpgradeActivations,
    policy: CanonicalReorgPolicy,
    required_first_available_height: BlockHeight,
    evidence: EdgeSnapshotAttestation,
) -> Result<(), SnapshotCommandError> {
    if evidence.history_floor.height > required_first_available_height {
        return Err(SnapshotCommandError::Refused(
            "snapshot history starts above the height the receiver requires",
        ));
    }
    let network = activations.network();
    let genesis = BlockId::new(BlockHeight::new(0), network.genesis_hash());
    require_block(validator, network, genesis).await?;
    if validator.fetch_network_upgrade_activations().await? != *activations {
        return Err(SnapshotCommandError::Refused(
            "validator activation schedule changed",
        ));
    }
    let snapshot = evidence.checkpoint.block_id;
    let minimum_tip = u64::from(snapshot.height.value()) + u64::from(policy.reorg_window_blocks());
    let tip = validator.tip_id().await?;
    if u64::from(tip.height.value()) < minimum_tip {
        return Err(SnapshotCommandError::Refused(
            "validator tip is inside the snapshot safety margin",
        ));
    }
    require_block(validator, network, snapshot).await?;
    require_block(validator, network, evidence.history_floor).await?;
    let observed = validator
        .fetch_chain_checkpoint_for_block(snapshot, activations)
        .await?;
    if observed.tip_metadata() != evidence.checkpoint.tip_metadata()
        || observed.frontiers.final_note_commitment_roots(snapshot)
            != evidence
                .checkpoint
                .frontiers
                .final_note_commitment_roots(snapshot)
    {
        return Err(SnapshotCommandError::Refused(
            "validator tree state differs",
        ));
    }
    attest_terminal_subtrees(validator, network, evidence.terminal_subtrees).await?;
    for block in evidence.blocks {
        let observed = require_block(validator, network, block.block).await?;
        let observed_ids = observed
            .transactions
            .iter()
            .map(|tx| TransactionId::from_bytes(tx.hash().0));
        if !observed_ids.eq(block.transaction_ids) {
            return Err(SnapshotCommandError::Refused(
                "validator spot-check transaction sequence differs",
            ));
        }
    }
    // The same best-chain fence must still cover all of the observations.
    require_block(validator, network, tip).await?;
    if validator.fetch_network_upgrade_activations().await? != *activations {
        return Err(SnapshotCommandError::Refused(
            "validator activation schedule changed",
        ));
    }
    Ok(())
}

async fn attest_terminal_subtrees(
    validator: &impl NodeSource,
    network: Network,
    subtrees: [Option<SubtreeRootArtifact>; 3],
) -> Result<(), SnapshotCommandError> {
    for root in subtrees.into_iter().flatten() {
        let index = root.subtree_index;
        let observed = validator
            .fetch_subtree_roots(root.protocol, index, NonZeroU32::MIN)
            .await?;
        if observed.protocol != root.protocol
            || observed.start_index != index
            || !matches!(observed.subtree_roots.as_slice(), [observed]
                if observed.subtree_index == index
                    && observed.root_hash == root.root_hash
                    && observed.completing_block_height == root.completing_block_height)
        {
            return Err(SnapshotCommandError::Refused(
                "validator terminal subtree root differs",
            ));
        }
        require_block(
            validator,
            network,
            BlockId::new(root.completing_block_height, root.completing_block_hash),
        )
        .await?;
    }
    Ok(())
}

async fn require_block(
    validator: &impl NodeSource,
    network: Network,
    expected: BlockId,
) -> Result<Block, SnapshotCommandError> {
    let block = validator.fetch_block_at(expected.height).await?;
    if block.network != network || block.height != expected.height || block.hash != expected.hash {
        return Err(SnapshotCommandError::Refused(
            "validator best-chain block identity differs",
        ));
    }
    let mut bytes = block.raw_block_bytes.as_slice();
    let decoded: Block = (&mut bytes)
        .zcash_deserialize_into()
        .map_err(|_| SnapshotCommandError::Refused("validator block could not be decoded"))?;
    if !bytes.is_empty() || decoded.hash().0 != expected.hash.as_bytes() {
        return Err(SnapshotCommandError::Refused(
            "validator block header differs",
        ));
    }
    Ok(decoded)
}

async fn prepare_source(
    args: &BootstrapArgs,
    target: &Path,
    config: &EdgeSnapshotStoreConfig<'_>,
) -> Result<(EdgeSnapshotDestination, PathBuf), SnapshotCommandError> {
    let remote = args.source.starts_with("http://") || args.source.starts_with("https://");
    let destination =
        EdgeSnapshotDestination::reserve(target, args.sealed_history_output.as_deref(), remote)?;
    let source = if let Some(directory) = destination.download_path() {
        let bundle = directory.join("bundle");
        fs::create_dir(&bundle)?;
        download_bundle(&args.source, &bundle, &args.public_key, config).await?;
        bundle
    } else {
        PathBuf::from(&args.source)
    };
    Ok((destination, source))
}

async fn download_bundle(
    source: &str,
    directory: &Path,
    keys: &[[u8; 32]],
    config: &EdgeSnapshotStoreConfig<'_>,
) -> Result<(), SnapshotCommandError> {
    let mut base = Url::parse(source)
        .map_err(|_| SnapshotCommandError::Refused("invalid snapshot base URL"))?;
    if !base.username().is_empty()
        || base.password().is_some()
        || base.query().is_some()
        || base.fragment().is_some()
    {
        return Err(SnapshotCommandError::Refused(
            "snapshot base URL must omit credentials, query, and fragment",
        ));
    }
    if !base.path().ends_with('/') {
        base.path_segments_mut()
            .map_err(|()| SnapshotCommandError::Refused("invalid snapshot base URL"))?
            .push("");
    }
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .read_timeout(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(20))
        .build()?;
    for (path, limit) in [(MANIFEST_FILE, MAX_MANIFEST_BYTES), (SIGNATURE_FILE, 64)] {
        download_file(&client, &base, directory, path, limit).await?;
    }
    let manifest = EdgeSnapshotManifest::verify(
        &fs::read(directory.join(MANIFEST_FILE))?,
        &fs::read(directory.join(SIGNATURE_FILE))?,
        keys,
    )?;
    manifest.validate_store_config(config)?;
    for (path, bytes, digest) in manifest.files() {
        if download_file(&client, &base, directory, path, bytes).await? != (bytes, digest) {
            return Err(SnapshotCommandError::Refused(
                "HTTP snapshot file differs from the signed inventory",
            ));
        }
    }
    Ok(())
}

async fn download_file(
    client: &Client,
    base: &Url,
    directory: &Path,
    path: &str,
    limit: u64,
) -> Result<(u64, [u8; 32]), SnapshotCommandError> {
    let url = base
        .join(path)
        .map_err(|_| SnapshotCommandError::Refused("invalid snapshot file URL"))?;
    let mut response = client.get(url).send().await?.error_for_status()?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(SnapshotCommandError::Refused(
            "HTTP snapshot source must return a complete file",
        ));
    }
    let destination = directory.join(path);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create_new(destination)?;
    let mut bytes = 0;
    let mut hash = Sha256::new();
    while let Some(chunk) = response.chunk().await? {
        bytes += chunk.len() as u64;
        if bytes > limit {
            return Err(SnapshotCommandError::Refused(
                "HTTP snapshot file exceeds its length limit",
            ));
        }
        hash.update(&chunk);
        file.write_all(&chunk)?;
    }
    Ok((bytes, hash.finalize().into()))
}

#[cfg(test)]
#[path = "edge_snapshot_tests.rs"]
mod tests;
