use std::{
    fs::{self, File},
    io::{self, Read, Write},
    num::NonZeroU32,
    path::{Path, PathBuf},
    time::Duration,
};

use clap::{Args, Subcommand};
use reqwest::{Client, Url};
use sha2::{Digest, Sha256};
use zebra_chain::{block::Block, serialization::ZcashDeserializeInto};
use zinder_core::{BlockHeight, BlockId, Network, NetworkUpgradeActivations, TransactionId};
use zinder_source::{NodeSource, SourceError};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RocksDbCanonicalStore,
    edge_snapshot::{
        DEFAULT_SPOT_CHECK_BLOCKS, EdgeSnapshotAttestation, EdgeSnapshotBootstrap,
        EdgeSnapshotError, EdgeSnapshotManifest, EdgeSnapshotStoreConfig, MANIFEST_FILE,
        MAX_MANIFEST_BYTES, SIGNATURE_FILE, export_edge_snapshot, prepare_bootstrapped_store,
    },
};

use crate::{IngestConfigError, IngestConfigOverrides, config, zebra_json_rpc_source_for_target};

#[derive(Subcommand)]
pub(crate) enum SnapshotCommand {
    /// Create a publisher-signed bundle from a settled canonical store.
    ExportEdgeSnapshot(ExportArgs),
    /// Verify a snapshot against a publisher key and the configured validator.
    BootstrapFromSnapshot(BootstrapArgs),
}

#[derive(Args)]
pub(crate) struct ExportArgs {
    /// Destination directory for the signed bundle.
    #[arg(long)]
    output_dir: PathBuf,
    /// File containing the DER-encoded Ed25519 PKCS#8 signing key.
    #[arg(long)]
    signing_key: PathBuf,
    /// Directory of opaque sealed-history files to include.
    #[arg(long)]
    include_sealed_history: Option<PathBuf>,
}

#[derive(Args)]
pub(crate) struct BootstrapArgs {
    /// Local bundle directory or HTTP(S) base URL.
    #[arg(long)]
    source: String,
    /// Trusted raw Ed25519 public key as 64 hexadecimal characters. Repeatable.
    #[arg(long, required = true, value_parser = parse_public_key)]
    public_key: Vec<[u8; 32]>,
    /// Number of random retained blocks to compare, plus the snapshot boundary.
    #[arg(long, default_value_t = DEFAULT_SPOT_CHECK_BLOCKS)]
    spot_check_blocks: NonZeroU32,
    /// Destination directory for verified opaque sealed-history files.
    #[arg(long)]
    sealed_history_output: Option<PathBuf>,
}

fn parse_public_key(key: &str) -> Result<[u8; 32], &'static str> {
    let mut bytes = [0; 32];
    hex::decode_to_slice(key, &mut bytes)
        .map_err(|_| "expected 64 hexadecimal characters for the Ed25519 public key")?;
    Ok(bytes)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotCommandError {
    #[error(transparent)]
    Snapshot(#[from] EdgeSnapshotError),
    #[error(transparent)]
    Config(#[from] IngestConfigError),
    #[error(transparent)]
    Store(#[from] zinder_store::CanonicalStoreError),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("snapshot I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("snapshot HTTP request failed: {0}")]
    Http(reqwest::Error),
    #[error("snapshot refused: {0}")]
    Refused(&'static str),
}

impl From<reqwest::Error> for SnapshotCommandError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error.without_url())
    }
}

pub(crate) async fn run(
    command: SnapshotCommand,
    config_path: Option<PathBuf>,
    overrides: IngestConfigOverrides,
) -> Result<(), SnapshotCommandError> {
    let runtime = config::load_ingest_config(config_path, overrides)?.runtime_config;
    let validator = zebra_json_rpc_source_for_target(runtime.node_source, &runtime.node)?;
    let activations = validator.fetch_network_upgrade_activations().await?;
    if activations.network() != runtime.node.network {
        return Err(SnapshotCommandError::Refused("validator network mismatch"));
    }
    let policy = CanonicalReorgPolicy::new(runtime.reorg_window_blocks)
        .map_err(zinder_ingest::CanonicalWriterError::from)
        .map_err(IngestConfigError::from)?;
    let store_config = EdgeSnapshotStoreConfig {
        activations: &activations,
        workload: CanonicalStoreWorkload::Wallet,
        raw_blob_retention: runtime.raw_blob_policy.to_retention(),
        reorg_policy: policy,
        resource_budget: runtime.canonical_rocksdb_budget,
    };
    match command {
        SnapshotCommand::ExportEdgeSnapshot(args) => {
            let mut store = RocksDbCanonicalStore::open_ready(
                &runtime.storage_path,
                &activations,
                store_config.workload,
                store_config.raw_blob_retention,
                policy,
                store_config.resource_budget,
            )?;
            let mut key = Vec::new();
            File::open(args.signing_key)?
                .take(1025)
                .read_to_end(&mut key)?;
            if key.len() > 1024 {
                return Err(SnapshotCommandError::Refused(
                    "signing key exceeds the size limit",
                ));
            }
            export_edge_snapshot(
                &mut store,
                &args.output_dir,
                &key,
                args.include_sealed_history.as_deref(),
            )?;
        }
        SnapshotCommand::BootstrapFromSnapshot(args) => {
            let download = if args.source.starts_with("http://")
                || args.source.starts_with("https://")
            {
                Some(download_bundle(&args.source, &runtime.storage_path, &args.public_key).await?)
            } else {
                None
            };
            let source = download
                .as_ref()
                .map_or_else(|| Path::new(&args.source), |download| &download.0);
            prepare_bootstrapped_store(
                EdgeSnapshotBootstrap {
                    source,
                    target: &runtime.storage_path,
                    trusted_keys: &args.public_key,
                    spot_check_blocks: args.spot_check_blocks,
                    sealed_history_output: args.sealed_history_output.as_deref(),
                },
                store_config,
                |evidence| attest(&validator, &activations, policy, evidence),
            )
            .await?;
        }
    }
    Ok(())
}

async fn attest(
    validator: &impl NodeSource,
    activations: &NetworkUpgradeActivations,
    policy: CanonicalReorgPolicy,
    evidence: EdgeSnapshotAttestation,
) -> Result<(), SnapshotCommandError> {
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
    for root in evidence.terminal_subtrees.into_iter().flatten() {
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

struct Download(PathBuf);

impl Drop for Download {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            tracing::warn!(%error, "failed to remove snapshot download directory");
        }
    }
}

async fn download_bundle(
    source: &str,
    target: &Path,
    keys: &[[u8; 32]],
) -> Result<Download, SnapshotCommandError> {
    use std::os::unix::fs::DirBuilderExt;
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
    let mut name = target.as_os_str().to_os_string();
    name.push(".bootstrap.download");
    let directory = PathBuf::from(name);
    fs::DirBuilder::new().mode(0o700).create(&directory)?;
    let download = Download(directory);
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .read_timeout(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(20))
        .build()?;
    for (path, limit) in [(MANIFEST_FILE, MAX_MANIFEST_BYTES), (SIGNATURE_FILE, 64)] {
        download_file(&client, &base, &download.0, path, limit).await?;
    }
    let manifest = EdgeSnapshotManifest::verify(
        &fs::read(download.0.join(MANIFEST_FILE))?,
        &fs::read(download.0.join(SIGNATURE_FILE))?,
        keys,
    )?;
    for (path, bytes, digest) in manifest.files() {
        if download_file(&client, &base, &download.0, path, bytes).await? != (bytes, digest) {
            return Err(SnapshotCommandError::Refused(
                "HTTP snapshot file differs from the signed inventory",
            ));
        }
    }
    Ok(download)
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
