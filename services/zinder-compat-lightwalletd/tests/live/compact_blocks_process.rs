//! Bounded live process validation for runtime compact-block serving.

#![allow(
    missing_docs,
    reason = "The ignored T3 test names describe the bounded process contract."
)]

use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use eyre::{Result, eyre};
use parking_lot::Mutex;
use prost::Message;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, process::Command, time::sleep};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::{Code, Request, Response, Status, transport::Server};
use zinder_core::{
    BlockHeight, BlockId, ChainTipMetadata, CommitmentTreeCheckpoint, CommitmentTreeFrontier,
    CommitmentTreeFrontiers, CompactBlockArtifact, CompactTransaction, CompactTransactionData,
    Network, ShieldedProtocol, TransactionId, TransactionLocation, UnixTimestampMillis,
    decode_canonical_block_replay,
    wire::{
        decode_rpc_block_hash_hex, encode_internal_block_hash, encode_internal_transaction_id,
        encode_zinder_native_chain_name,
    },
};
use zinder_proto::{
    compat::lightwalletd::{self, compact_tx_streamer_client::CompactTxStreamerClient},
    v1::ingest::{
        AcquireCanonicalProjectionBuildLeaseRequest, CanonicalEventPageRequest,
        CanonicalEventPageResponse, CanonicalProjectionBuildLeaseResponse, CanonicalWriterFence,
        CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
        CreateCanonicalOwnerCheckpointRequest, CreateCanonicalOwnerCheckpointResponse,
        ReadmitCanonicalOwnerCheckpointRequest, ReleaseCanonicalProjectionBuildLeaseRequest,
        ReleaseCanonicalProjectionBuildLeaseResponse, RenewCanonicalProjectionBuildLeaseRequest,
        canonical_control_server::{CanonicalControl, CanonicalControlServer},
    },
};
use zinder_source::{ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalEventFence, CanonicalLiveAppend,
    CanonicalLiveReplacement, CanonicalReorgPolicy, CanonicalReplacementBlock,
    CanonicalStoreBuildPlan, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget,
    TREE_STATE_CHECKPOINT_STRIDE,
};
use zinder_testkit::{
    ChainFixture, FixtureTransactionRows, encode_fixture_block_replay_with_raw_block,
    live::{init, optional_env, require_live_for},
    synthetic_transaction_public_facts,
};

const BINARY_PATH_ENV: &str = "ZINDER_TEST_COMPACT_BINARY_PATH";
const CANONICAL_PATH_ENV: &str = "ZINDER_TEST_COMPACT_CANONICAL_PATH";
const EVIDENCE_ROOT_ENV: &str = "ZINDER_TEST_COMPACT_EVIDENCE_ROOT";
const REORG_EVIDENCE_ROOT_ENV: &str = "ZINDER_TEST_COMPACT_REORG_EVIDENCE_ROOT";
const EXPECTED_RAW_BLOB_POLICY_ENV: &str = "ZINDER_TEST_COMPACT_EXPECTED_RAW_BLOB_POLICY";
const EXPECTED_HEIGHT_ENV: &str = "ZINDER_TEST_COMPACT_EXPECTED_HEIGHT";
const EXPECTED_RPC_HASH_ENV: &str = "ZINDER_TEST_COMPACT_EXPECTED_RPC_HASH";
const READY_TIMEOUT: Duration = Duration::from_mins(1);
const PROCESS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone)]
struct MutableCanonicalControl {
    status: std::sync::Arc<Mutex<CanonicalWriterStatusResponse>>,
}

#[tonic::async_trait]
impl CanonicalControl for MutableCanonicalControl {
    async fn writer_status(
        &self,
        _request: Request<CanonicalWriterStatusRequest>,
    ) -> Result<Response<CanonicalWriterStatusResponse>, Status> {
        Ok(Response::new(self.status.lock().clone()))
    }

    async fn event_page(
        &self,
        _request: Request<CanonicalEventPageRequest>,
    ) -> Result<Response<CanonicalEventPageResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }

    async fn create_owner_checkpoint(
        &self,
        _request: Request<CreateCanonicalOwnerCheckpointRequest>,
    ) -> Result<Response<CreateCanonicalOwnerCheckpointResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }

    async fn readmit_owner_checkpoint(
        &self,
        _request: Request<ReadmitCanonicalOwnerCheckpointRequest>,
    ) -> Result<Response<CreateCanonicalOwnerCheckpointResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }

    async fn acquire_projection_build_lease(
        &self,
        _request: Request<AcquireCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<CanonicalProjectionBuildLeaseResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }

    async fn renew_projection_build_lease(
        &self,
        _request: Request<RenewCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<CanonicalProjectionBuildLeaseResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }

    async fn release_projection_build_lease(
        &self,
        _request: Request<ReleaseCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<ReleaseCanonicalProjectionBuildLeaseResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }
}

#[derive(Serialize)]
struct InventoryEntry {
    path: String,
    bytes: u64,
    sha256: String,
}

#[derive(Serialize)]
struct ProcessReport {
    source: SourceReport,
    command: CommandReport,
    timings: TimingReport,
    protocol: ProtocolReport,
    isolation: IsolationReport,
    shutdown: ShutdownReport,
    non_claims: Vec<String>,
}

#[derive(Serialize)]
struct ReorgProcessReport {
    network: String,
    binary_source: String,
    initial_fence: WriterFenceReport,
    replacement_fence: WriterFenceReport,
    observed_replacement_hash: String,
    process_start_to_ready_ms: u128,
    replacement_commit_to_observed_ms: u128,
    shutdown: ShutdownReport,
    non_claims: Vec<String>,
}

#[derive(Serialize)]
struct SourceReport {
    network: String,
    raw_blob_policy: String,
    canonical_path: String,
    control_secondary_path: String,
    source_manifest_sha256_before: String,
    source_manifest_sha256_after: String,
    source_inventory_sha256_before: String,
    source_inventory_sha256_after: String,
    source_inventory_unchanged: bool,
    writer_fence: WriterFenceReport,
}

#[derive(Serialize)]
struct WriterFenceReport {
    chain_epoch_id: u64,
    event_sequence: u64,
    visible_tip_height: u32,
    visible_tip_hash: String,
    visible_block_count: u64,
    canonical_sequence_digest: String,
}

#[derive(Serialize)]
struct CommandReport {
    binary_source: String,
    command_shape: Vec<String>,
    environment_shape: Vec<String>,
    runtime_secondary_path: String,
    compat_addr: String,
    ops_addr: String,
}

#[derive(Serialize)]
struct TimingReport {
    process_start_to_ready_ms: u128,
    ready_to_four_rpc_completion_ms: u128,
}

#[derive(Serialize)]
struct ProtocolReport {
    tip_height: u32,
    tip_hash: String,
    lightd_info_taddr_support: bool,
    range_start_height: u32,
    ascending_heights: Vec<u64>,
    descending_heights: Vec<u64>,
    exact_compact_block_sha256: String,
    transparent_pool_compact_block_sha256: String,
    unsupported_transaction_code: String,
    unsupported_client_stream_code: String,
}

#[derive(Serialize)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "The report records independent filesystem isolation observations."
)]
struct IsolationReport {
    wallet_path_created: bool,
    materialized_view_path_created: bool,
    node_fallback_path_created: bool,
    source_manifest_file_present: bool,
}

#[derive(Serialize)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "The report records independent shutdown observations."
)]
struct ShutdownReport {
    sigterm_sent: bool,
    process_exit_success: bool,
    compat_port_stopped: bool,
    ops_port_stopped: bool,
    fake_control_stopped: bool,
}

struct SelectedBinary {
    path: PathBuf,
    source: &'static str,
    evidence_boundary: &'static str,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
#[allow(
    clippy::too_many_lines,
    reason = "The process gate records one bounded causal lifecycle and its evidence contract."
)]
async fn compact_blocks_process_serves_exact_contract() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest, Network::ZcashTestnet])? else {
        return Ok(());
    };
    let Some(canonical_text) = optional_env(CANONICAL_PATH_ENV)? else {
        return Ok(());
    };
    let Some(evidence_text) = optional_env(EVIDENCE_ROOT_ENV)? else {
        return Ok(());
    };
    let expected_raw_text = optional_env(EXPECTED_RAW_BLOB_POLICY_ENV)?
        .ok_or_else(|| eyre!("{EXPECTED_RAW_BLOB_POLICY_ENV} is required"))?;
    let expected_raw_blob_policy = RawBlobRetention::from_kebab_case(&expected_raw_text)
        .ok_or_else(|| eyre!("{EXPECTED_RAW_BLOB_POLICY_ENV} has an invalid value"))?;
    let expected_height = optional_env(EXPECTED_HEIGHT_ENV)?
        .map(|text| text.parse::<u32>())
        .transpose()?;
    let expected_rpc_hash = optional_env(EXPECTED_RPC_HASH_ENV)?
        .map(|text| decode_rpc_block_hash_hex(&text))
        .transpose()
        .map_err(|error| eyre!("{EXPECTED_RPC_HASH_ENV} is invalid: {error}"))?;
    let canonical_path = PathBuf::from(canonical_text);
    if !canonical_path.is_dir() {
        return Err(eyre!("{CANONICAL_PATH_ENV} is not a directory"));
    }
    let evidence_root = PathBuf::from(evidence_text);
    if evidence_root.exists() {
        return Err(eyre!("{EVIDENCE_ROOT_ENV} must name a fresh directory"));
    }
    fs::create_dir_all(&evidence_root)?;
    let source_before = inventory(&canonical_path)?;
    let source_manifest = canonical_path.join("canonical-construction-manifest.v4.json");
    let source_manifest_before = sha256_file(&source_manifest)?;

    let source = ZebraJsonRpcSource::with_options(
        env.target.network,
        env.target.json_rpc_addr.clone(),
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    let activations = source.fetch_network_upgrade_activations().await?;
    let control_secondary = evidence_root.join("control-secondary");
    let canonical_reader = RocksDbCanonicalSecondary::open_ready(
        &canonical_path,
        &control_secondary,
        &activations,
        CanonicalStoreWorkload::Wallet,
        expected_raw_blob_policy,
        CanonicalReorgPolicy::new(100)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let fence = canonical_reader.event_fence();
    if let Some(expected_height) = expected_height
        && expected_height != fence.visible_tip().height.value()
    {
        return Err(eyre!(
            "canonical tip height {} did not match expected height {expected_height}",
            fence.visible_tip().height.value()
        ));
    }
    if let Some(expected_rpc_hash) = expected_rpc_hash
        && expected_rpc_hash != fence.visible_tip().hash
    {
        return Err(eyre!(
            "canonical tip hash did not match {EXPECTED_RPC_HASH_ENV}"
        ));
    }
    let writer_status = writer_status_for_fence(fence, env.target.network);
    let tip_height = fence.visible_tip().height.value();
    let range_start_height = tip_height.saturating_sub(99).max(1);
    let expected_block = canonical_reader
        .compact_block_at(fence.visible_tip().height)?
        .ok_or_else(|| eyre!("canonical source has no compact tip artifact"))?;

    let control_listener = TcpListener::bind("127.0.0.1:0").await?;
    let control_addr = control_listener.local_addr()?;
    let control_shutdown = tokio_util::sync::CancellationToken::new();
    let control_shutdown_for_server = control_shutdown.clone();
    let control = MutableCanonicalControl {
        status: std::sync::Arc::new(Mutex::new(writer_status.clone())),
    };
    let control_server = tokio::spawn(async move {
        Server::builder()
            .add_service(CanonicalControlServer::new(control))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(control_listener),
                control_shutdown_for_server.cancelled_owned(),
            )
            .await
    });

    let compat_addr = free_loopback_addr()?;
    let ops_addr = free_loopback_addr()?;
    let runtime_secondary = evidence_root.join("runtime-secondary");
    let config_path = evidence_root.join("compact-process.toml");
    let config = process_config(
        &env,
        &canonical_path,
        &runtime_secondary,
        compat_addr,
        ops_addr,
        control_addr,
        expected_raw_blob_policy,
    );
    fs::write(&config_path, config)?;
    let binary = selected_binary()?;
    let process_started = Instant::now();
    let mut child = Command::new(&binary.path)
        .env_remove("ZINDER_NETWORK")
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    wait_until_ready(&mut child, ops_addr).await?;
    let ready_at = Instant::now();

    let endpoint = tonic::transport::Endpoint::new(format!("http://{compat_addr}"))?;
    let channel = endpoint.connect().await?;
    let mut client = CompactTxStreamerClient::new(channel);
    let info = client
        .get_lightd_info(lightwalletd::Empty::default())
        .await?
        .into_inner();
    assert!(!info.taddr_support);
    assert_eq!(
        info.block_height,
        u64::from(fence.visible_tip().height.value())
    );
    let latest = client
        .get_latest_block(lightwalletd::ChainSpec::default())
        .await?
        .into_inner();
    assert_eq!(latest.height, u64::from(fence.visible_tip().height.value()));
    assert_eq!(
        latest.hash,
        encode_internal_block_hash(fence.visible_tip().hash)
    );
    let block = client
        .get_block(lightwalletd::BlockId {
            height: u64::from(fence.visible_tip().height.value()),
            hash: Vec::new(),
        })
        .await?
        .into_inner();
    let expected_wire = expected_compact_block(&expected_block);
    assert_eq!(block.encode_to_vec(), expected_wire.encode_to_vec());
    let transparent_block = client
        .get_block_range(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: u64::from(fence.visible_tip().height.value()),
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: u64::from(fence.visible_tip().height.value()),
                hash: Vec::new(),
            }),
            pool_types: vec![lightwalletd::PoolType::Transparent as i32],
        })
        .await?
        .into_inner()
        .next()
        .await
        .ok_or_else(|| eyre!("transparent pool range returned no block"))??;
    let mut expected_transparent_block = expected_wire.clone();
    for transaction in &mut expected_transparent_block.vtx {
        transaction.spends.clear();
        transaction.outputs.clear();
        transaction.actions.clear();
        transaction.ironwood_actions.clear();
    }
    assert_eq!(
        transparent_block.encode_to_vec(),
        expected_transparent_block.encode_to_vec()
    );
    assert!(transparent_block.vtx.iter().all(|transaction| {
        transaction.spends.is_empty()
            && transaction.outputs.is_empty()
            && transaction.actions.is_empty()
            && transaction.ironwood_actions.is_empty()
    }));
    let mut descending = client
        .get_block_range(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: u64::from(tip_height),
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: u64::from(range_start_height),
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        })
        .await?
        .into_inner();
    let mut descending_heights = Vec::new();
    while let Some(item) = descending.next().await {
        descending_heights.push(item?.height);
    }
    assert_eq!(descending_heights.first().copied(), Some(latest.height));
    assert_eq!(
        descending_heights.last().copied(),
        Some(u64::from(range_start_height))
    );
    assert!(descending_heights.len() <= 100);
    let mut ascending = client
        .get_block_range(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: u64::from(range_start_height),
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: u64::from(tip_height),
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        })
        .await?
        .into_inner();
    let mut ascending_heights = Vec::new();
    while let Some(item) = ascending.next().await {
        ascending_heights.push(item?.height);
    }
    assert_eq!(
        ascending_heights.first().copied(),
        Some(u64::from(range_start_height))
    );
    assert_eq!(ascending_heights.last().copied(), Some(latest.height));
    assert!(ascending_heights.len() <= 100);
    let unsupported_transaction = client
        .get_transaction(lightwalletd::TxFilter::default())
        .await
        .err()
        .ok_or_else(|| eyre!("GetTransaction unexpectedly succeeded"))?;
    assert_eq!(unsupported_transaction.code(), Code::Unimplemented);
    let unsupported_stream = client
        .get_taddress_balance_stream(tokio_stream::iter(vec![lightwalletd::Address::default()]))
        .await
        .err()
        .ok_or_else(|| eyre!("GetTaddressBalanceStream unexpectedly succeeded"))?;
    assert_eq!(unsupported_stream.code(), Code::Unimplemented);
    let rpc_done_at = Instant::now();

    send_sigterm(&child).await?;
    let output = tokio::time::timeout(PROCESS_SHUTDOWN_TIMEOUT, child.wait_with_output()).await??;
    control_shutdown.cancel();
    control_server.await??;
    let compat_stopped = wait_for_port_closed(compat_addr).await;
    let ops_stopped = wait_for_port_closed(ops_addr).await;
    let source_after = inventory(&canonical_path)?;
    let source_manifest_after = sha256_file(&source_manifest)?;
    let source_before_hash = inventory_hash(&source_before);
    let source_after_hash = inventory_hash(&source_after);
    let wallet_path_created = evidence_root.join("wallet").exists();
    let materialized_view_path_created = evidence_root.join("materialized-views").exists();
    let node_fallback_path_created = ["node", "fallback", "upstream"]
        .iter()
        .any(|name| evidence_root.join(name).exists());
    let report = ProcessReport {
        source: SourceReport {
            network: encode_zinder_native_chain_name(env.target.network).to_owned(),
            raw_blob_policy: expected_raw_blob_policy.as_kebab_case().to_owned(),
            canonical_path: canonical_path.display().to_string(),
            control_secondary_path: control_secondary.display().to_string(),
            source_manifest_sha256_before: source_manifest_before,
            source_manifest_sha256_after: source_manifest_after,
            source_inventory_sha256_before: source_before_hash.clone(),
            source_inventory_sha256_after: source_after_hash.clone(),
            source_inventory_unchanged: source_before_hash == source_after_hash,
            writer_fence: WriterFenceReport {
                chain_epoch_id: writer_status
                    .fence
                    .as_ref()
                    .map_or(0, |fence| fence.chain_epoch_id),
                event_sequence: writer_status
                    .fence
                    .as_ref()
                    .map_or(0, |fence| fence.event_sequence),
                visible_tip_height: writer_status
                    .fence
                    .as_ref()
                    .map_or(0, |fence| fence.visible_tip_height),
                visible_tip_hash: writer_status
                    .fence
                    .as_ref()
                    .map_or_else(String::new, |fence| hex::encode(&fence.visible_tip_hash)),
                visible_block_count: writer_status
                    .fence
                    .as_ref()
                    .map_or(0, |fence| fence.visible_block_count),
                canonical_sequence_digest: writer_status
                    .fence
                    .as_ref()
                    .map_or_else(String::new, |fence| {
                        hex::encode(&fence.canonical_sequence_digest)
                    }),
            },
        },
        command: CommandReport {
            binary_source: binary.source.to_owned(),
            command_shape: vec![
                binary.path.display().to_string(),
                "--config <redacted-path>".to_owned(),
            ],
            environment_shape: vec![
                "ZINDER_NETWORK=<live-gate-only; removed from child>".to_owned(),
                format!(
                    "network.name={} (config)",
                    encode_zinder_native_chain_name(env.target.network)
                ),
                "ZINDER_NODE__JSON_RPC_ADDR=<configured>".to_owned(),
                "ZINDER_NODE__AUTH__METHOD=<configured>".to_owned(),
            ],
            runtime_secondary_path: runtime_secondary.display().to_string(),
            compat_addr: compat_addr.to_string(),
            ops_addr: ops_addr.to_string(),
        },
        timings: TimingReport {
            process_start_to_ready_ms: ready_at.duration_since(process_started).as_millis(),
            ready_to_four_rpc_completion_ms: rpc_done_at.duration_since(ready_at).as_millis(),
        },
        protocol: ProtocolReport {
            tip_height: fence.visible_tip().height.value(),
            tip_hash: hex::encode(fence.visible_tip().hash.as_bytes()),
            lightd_info_taddr_support: info.taddr_support,
            range_start_height,
            ascending_heights,
            descending_heights,
            exact_compact_block_sha256: sha256_bytes(&block.encode_to_vec()),
            transparent_pool_compact_block_sha256: sha256_bytes(&transparent_block.encode_to_vec()),
            unsupported_transaction_code: unsupported_transaction.code().to_string(),
            unsupported_client_stream_code: unsupported_stream.code().to_string(),
        },
        isolation: IsolationReport {
            wallet_path_created,
            materialized_view_path_created,
            node_fallback_path_created,
            source_manifest_file_present: source_manifest.is_file(),
        },
        shutdown: ShutdownReport {
            sigterm_sent: true,
            process_exit_success: output.status.success(),
            compat_port_stopped: compat_stopped,
            ops_port_stopped: ops_stopped,
            fake_control_stopped: control_shutdown.is_cancelled(),
        },
        non_claims: vec![
            "This is bounded local live process evidence from a preserved canonical artifact, not fresh end-to-end, capacity, or production evidence.".to_owned(),
            binary.evidence_boundary.to_owned(),
            "No wallet, materialized-view, mempool, or upstream fallback topology was exercised in compact mode.".to_owned(),
        ],
    };
    assert!(output.status.success());
    assert!(source_before_hash == source_after_hash);
    assert!(compat_stopped && ops_stopped && control_shutdown.is_cancelled());
    write_report_atomically(&evidence_root.join("report.json"), &report)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
#[allow(
    clippy::too_many_lines,
    reason = "The process gate keeps the primary replacement, writer-fence advance, serving observation, and shutdown in one causal proof."
)]
async fn compact_blocks_process_refreshes_after_shallow_reorg() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest, Network::ZcashTestnet])? else {
        return Ok(());
    };
    let Some(evidence_text) = optional_env(REORG_EVIDENCE_ROOT_ENV)? else {
        return Ok(());
    };
    let evidence_root = PathBuf::from(evidence_text);
    if evidence_root.exists() {
        return Err(eyre!(
            "{REORG_EVIDENCE_ROOT_ENV} must name a fresh directory"
        ));
    }
    fs::create_dir_all(&evidence_root)?;

    let source = ZebraJsonRpcSource::with_options(
        env.target.network,
        env.target.json_rpc_addr.clone(),
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    let activations = source.fetch_network_upgrade_activations().await?;
    let canonical_path = evidence_root.join("canonical-primary");
    let chain = ChainFixture::new(env.target.network).extend_blocks(4);
    let mut primary = build_reorg_process_primary(&canonical_path, &chain, &activations)?;
    let initial_fence = primary.event_fence();
    let initial_status = writer_status_for_fence(initial_fence, env.target.network);
    let shared_status = std::sync::Arc::new(Mutex::new(initial_status.clone()));

    let control_listener = TcpListener::bind("127.0.0.1:0").await?;
    let control_addr = control_listener.local_addr()?;
    let control_shutdown = tokio_util::sync::CancellationToken::new();
    let control_shutdown_for_server = control_shutdown.clone();
    let control = MutableCanonicalControl {
        status: std::sync::Arc::clone(&shared_status),
    };
    let control_server = tokio::spawn(async move {
        Server::builder()
            .add_service(CanonicalControlServer::new(control))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(control_listener),
                control_shutdown_for_server.cancelled_owned(),
            )
            .await
    });

    let compat_addr = free_loopback_addr()?;
    let ops_addr = free_loopback_addr()?;
    let runtime_secondary = evidence_root.join("runtime-secondary");
    let config_path = evidence_root.join("compact-reorg-process.toml");
    fs::write(
        &config_path,
        process_config(
            &env,
            &canonical_path,
            &runtime_secondary,
            compat_addr,
            ops_addr,
            control_addr,
            RawBlobRetention::None,
        ),
    )?;
    let binary = selected_binary()?;
    let process_started = Instant::now();
    let mut child = Command::new(&binary.path)
        .env_remove("ZINDER_NETWORK")
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    wait_until_ready(&mut child, ops_addr).await?;
    let ready_at = Instant::now();

    let endpoint = tonic::transport::Endpoint::new(format!("http://{compat_addr}"))?;
    let channel = endpoint.connect().await?;
    let mut client = CompactTxStreamerClient::new(channel);
    let initial = client
        .get_latest_block(lightwalletd::ChainSpec::default())
        .await?
        .into_inner();
    assert_eq!(
        initial.hash,
        encode_internal_block_hash(initial_fence.visible_tip().hash)
    );

    let replacement_chain = chain
        .fork_at(initial_fence.visible_tip().height)?
        .extend_blocks(1);
    let replacement_block = canonical_build_blocks(&replacement_chain, &activations)?
        .pop()
        .ok_or_else(|| eyre!("replacement fixture did not produce a tip block"))?;
    let replacement_started = Instant::now();
    let (next_primary, replacement_fence) = primary.commit_live_replacement(
        CanonicalLiveReplacement::new(
            initial_fence,
            vec![CanonicalReplacementBlock::new(
                replacement_block,
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_774_669_100_000),
        ),
        &activations,
    )?;
    primary = next_primary;
    let replacement_status = writer_status_for_fence(replacement_fence, env.target.network);
    *shared_status.lock() = replacement_status.clone();
    let observed = wait_for_latest_fence(&mut child, &mut client, replacement_fence).await?;
    let replacement_observed_at = Instant::now();
    assert_eq!(primary.event_fence(), replacement_fence);
    assert_ne!(
        replacement_fence.visible_tip().hash,
        initial_fence.visible_tip().hash
    );
    let replacement_block = client
        .get_block(lightwalletd::BlockId {
            height: u64::from(replacement_fence.visible_tip().height.value()),
            hash: Vec::new(),
        })
        .await?
        .into_inner();
    assert_eq!(replacement_block.hash, observed.hash);

    send_sigterm(&child).await?;
    let output = tokio::time::timeout(PROCESS_SHUTDOWN_TIMEOUT, child.wait_with_output()).await??;
    control_shutdown.cancel();
    control_server.await??;
    let compat_stopped = wait_for_port_closed(compat_addr).await;
    let ops_stopped = wait_for_port_closed(ops_addr).await;
    let report = ReorgProcessReport {
        network: encode_zinder_native_chain_name(env.target.network).to_owned(),
        binary_source: binary.source.to_owned(),
        initial_fence: writer_fence_report(&initial_status),
        replacement_fence: writer_fence_report(&replacement_status),
        observed_replacement_hash: hex::encode(observed.hash),
        process_start_to_ready_ms: ready_at.duration_since(process_started).as_millis(),
        replacement_commit_to_observed_ms: replacement_observed_at
            .duration_since(replacement_started)
            .as_millis(),
        shutdown: ShutdownReport {
            sigterm_sent: true,
            process_exit_success: output.status.success(),
            compat_port_stopped: compat_stopped,
            ops_port_stopped: ops_stopped,
            fake_control_stopped: control_shutdown.is_cancelled(),
        },
        non_claims: vec![
            "This gate proves a bounded local process refresh after one synthetic shallow canonical replacement; it is not a node-driven reorg, capacity, or production claim.".to_owned(),
            binary.evidence_boundary.to_owned(),
        ],
    };
    assert!(output.status.success());
    assert!(compat_stopped && ops_stopped && control_shutdown.is_cancelled());
    write_report_atomically(&evidence_root.join("report.json"), &report)?;
    Ok(())
}

fn selected_binary() -> Result<SelectedBinary> {
    let Some(path_text) = optional_env(BINARY_PATH_ENV)? else {
        return Ok(SelectedBinary {
            path: PathBuf::from(env!("CARGO_BIN_EXE_zinder-compat-lightwalletd")),
            source: "Cargo-provided test binary",
            evidence_boundary: "The binary was the Cargo-provided test binary, not a separately built release artifact.",
        });
    };
    let path = PathBuf::from(path_text);
    if !path.is_file() {
        return Err(eyre!("{BINARY_PATH_ENV} is not a file"));
    }
    Ok(SelectedBinary {
        path,
        source: "explicit binary path supplied by the test operator",
        evidence_boundary: "The explicit binary was built locally and is not an installed, signed, or production-deployed release artifact.",
    })
}

fn build_reorg_process_primary(
    path: &Path,
    chain: &ChainFixture,
    activations: &zinder_core::NetworkUpgradeActivations,
) -> Result<RocksDbCanonicalStore> {
    let tip_height = chain
        .tip_height()
        .ok_or_else(|| eyre!("reorg process fixture requires at least two blocks"))?;
    let baseline_chain = chain.fork_at(tip_height)?;
    let baseline_tip_block = baseline_chain
        .blocks()
        .last()
        .ok_or_else(|| eyre!("reorg process fixture requires at least two blocks"))?;
    let baseline_tip = BlockId::new(baseline_tip_block.height, baseline_tip_block.hash);
    let reorg_policy = CanonicalReorgPolicy::new(100)?;
    let build_plan = CanonicalStoreBuildPlan::complete(
        activations,
        baseline_tip_block.block_time_seconds.saturating_sub(1),
        baseline_tip,
        RawBlobRetention::None,
        reorg_policy,
    )?;
    let mut builder = RocksDbCanonicalBuilder::create_fresh(
        path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    builder.bulk_load_blocks(
        canonical_build_blocks(&baseline_chain, activations)?
            .into_iter()
            .map(Ok::<_, std::convert::Infallible>),
    )?;
    builder.load_subtree_roots(std::iter::empty())?;
    builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
        baseline_tip,
        baseline_tip_block.block_time_seconds,
        checkpoint_frontiers(activations, baseline_tip.height),
    ))?;
    let validated = builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        baseline_tip,
        UnixTimestampMillis::new(1_774_669_000_000),
    ))?;
    let primary = validated.publish_baseline(publication)?;
    let live_block = canonical_build_blocks(chain, activations)?
        .pop()
        .ok_or_else(|| eyre!("reorg process fixture did not produce a live block"))?;
    let expected_fence = primary.event_fence();
    let (primary, _) = primary.commit_live_append(
        CanonicalLiveAppend::new(
            expected_fence,
            live_block,
            Vec::new(),
            baseline_tip,
            UnixTimestampMillis::new(1_774_669_050_000),
        ),
        activations,
    )?;
    Ok(primary)
}

fn canonical_build_blocks(
    chain: &ChainFixture,
    activations: &zinder_core::NetworkUpgradeActivations,
) -> Result<Vec<CanonicalBuildBlock>> {
    if chain.raw_blob_retention() != RawBlobRetention::None {
        return Err(eyre!(
            "compact reorg process fixture requires raw-blob retention none"
        ));
    }
    let tip_height = chain
        .tip_height()
        .ok_or_else(|| eyre!("compact reorg process fixture requires a tip"))?;
    let mut blocks = Vec::with_capacity(chain.block_count());
    for fixture_block in chain.blocks() {
        let mut fixture_block = fixture_block.clone();
        if fixture_block.height == BlockHeight::new(1) {
            fixture_block.parent_hash = chain.network().genesis_hash();
        }
        let transaction_id = TransactionId::from_bytes(fixture_block.hash.as_bytes());
        let mut coinbase_facts = synthetic_transaction_public_facts(transaction_id, 0);
        coinbase_facts.is_coinbase = true;
        let coinbase = FixtureTransactionRows::from_public_facts(
            TransactionLocation::new(transaction_id, fixture_block.height, fixture_block.hash, 0),
            coinbase_facts,
        );
        let replay_envelope = encode_fixture_block_replay_with_raw_block(
            &fixture_block.block_header_artifact(),
            &fixture_block.raw_block_bytes,
            &[coinbase],
        );
        let facts = decode_canonical_block_replay(replay_envelope.as_bytes())?.into_facts();
        let compact_block = fixture_block.compact_block_artifact();
        let checkpoint_required = fixture_block.height == tip_height
            || fixture_block
                .height
                .value()
                .is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE);
        let tree_state_checkpoint = checkpoint_required.then(|| {
            CommitmentTreeCheckpoint::new(
                BlockId::new(fixture_block.height, fixture_block.hash),
                fixture_block.block_time_seconds,
                checkpoint_frontiers(activations, fixture_block.height),
            )
        });
        blocks.push(CanonicalBuildBlock {
            facts,
            replay_envelope,
            compact_block,
            tip_metadata: ChainTipMetadata::empty(),
            tree_state_checkpoint,
            block_final_note_commitment_roots: None,
            transaction_blobs: Vec::new(),
            block_blob: None,
        });
    }
    Ok(blocks)
}

fn checkpoint_frontiers(
    activations: &zinder_core::NetworkUpgradeActivations,
    height: BlockHeight,
) -> CommitmentTreeFrontiers {
    let active_frontier = |protocol: ShieldedProtocol| {
        activations
            .activation_height_by_name(protocol.activation_upgrade_name())
            .is_some_and(|activation_height| activation_height <= height)
            .then(|| CommitmentTreeFrontier::empty(protocol))
    };
    CommitmentTreeFrontiers::from_validated_parts(
        active_frontier(ShieldedProtocol::Sapling),
        active_frontier(ShieldedProtocol::Orchard),
        active_frontier(ShieldedProtocol::Ironwood),
    )
}

async fn wait_for_latest_fence(
    child: &mut tokio::process::Child,
    client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    expected: CanonicalEventFence,
) -> Result<lightwalletd::BlockId> {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(eyre!(
                "compat process exited before serving replacement: {status}"
            ));
        }
        if let Ok(response) = client
            .get_latest_block(lightwalletd::ChainSpec::default())
            .await
        {
            let latest = response.into_inner();
            if latest.height == u64::from(expected.visible_tip().height.value())
                && latest.hash == encode_internal_block_hash(expected.visible_tip().hash)
            {
                return Ok(latest);
            }
        }
        if started.elapsed() >= READY_TIMEOUT {
            return Err(eyre!(
                "compact process did not serve the replacement fence within {READY_TIMEOUT:?}"
            ));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn writer_fence_report(status: &CanonicalWriterStatusResponse) -> WriterFenceReport {
    WriterFenceReport {
        chain_epoch_id: status
            .fence
            .as_ref()
            .map_or(0, |fence| fence.chain_epoch_id),
        event_sequence: status
            .fence
            .as_ref()
            .map_or(0, |fence| fence.event_sequence),
        visible_tip_height: status
            .fence
            .as_ref()
            .map_or(0, |fence| fence.visible_tip_height),
        visible_tip_hash: status
            .fence
            .as_ref()
            .map_or_else(String::new, |fence| hex::encode(&fence.visible_tip_hash)),
        visible_block_count: status
            .fence
            .as_ref()
            .map_or(0, |fence| fence.visible_block_count),
        canonical_sequence_digest: status.fence.as_ref().map_or_else(String::new, |fence| {
            hex::encode(&fence.canonical_sequence_digest)
        }),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "The process fixture maps each independently reserved endpoint into TOML."
)]
fn process_config(
    env: &zinder_testkit::live::LiveTestEnv,
    canonical_path: &Path,
    runtime_secondary: &Path,
    compat_addr: SocketAddr,
    ops_addr: SocketAddr,
    control_addr: SocketAddr,
    raw_blob_policy: RawBlobRetention,
) -> String {
    format!(
        "[network]\nname = \"{}\"\n\n[node]\njson_rpc_addr = \"{}\"\nrequest_timeout_secs = {}\nmax_response_bytes = {}\n\n[storage]\npath = \"{}\"\nsecondary_path = \"{}\"\nraw_blob_policy = \"{}\"\n\n[ingest_control]\naddr = \"http://{}\"\n\n[compat]\nlisten_addr = \"{}\"\nreorg_window_blocks = 100\npair_convergence_attempts = 8\nserving = \"compact-blocks\"\n\n[ops]\nlisten_addr = \"{}\"\n\n[security]\nallow_public_bind = false\n",
        encode_zinder_native_chain_name(env.target.network),
        toml_string(&env.target.json_rpc_addr),
        env.target.request_timeout.as_secs(),
        env.target.max_response_bytes,
        toml_string(&canonical_path.display().to_string()),
        toml_string(&runtime_secondary.display().to_string()),
        raw_blob_policy,
        control_addr,
        compat_addr,
        ops_addr,
    )
}

fn toml_string(input: &str) -> String {
    input.replace('\\', "\\\\").replace('"', "\\\"")
}

fn writer_status_for_fence(
    fence: CanonicalEventFence,
    network: Network,
) -> CanonicalWriterStatusResponse {
    CanonicalWriterStatusResponse {
        network_name: zinder_core::wire::encode_zinder_native_chain_name(network).to_owned(),
        fence: Some(CanonicalWriterFence {
            chain_epoch_id: fence.chain_epoch_id().value(),
            event_sequence: fence.chain_event_sequence(),
            visible_tip_height: fence.visible_tip().height.value(),
            visible_tip_hash: fence.visible_tip().hash.as_bytes().to_vec(),
            visible_block_count: fence.sequence_digest().block_count(),
            canonical_sequence_digest: fence.sequence_digest().as_bytes().to_vec(),
        }),
        oldest_retained_event_sequence: 1,
        network_upgrade_schedule: None,
    }
}

fn expected_compact_block(block: &CompactBlockArtifact) -> lightwalletd::CompactBlock {
    let metadata = block.chain_metadata();
    lightwalletd::CompactBlock {
        height: u64::from(block.height().value()),
        hash: encode_internal_block_hash(block.block_hash()).to_vec(),
        prev_hash: encode_internal_block_hash(block.previous_block_hash()).to_vec(),
        time: block.time(),
        header: Vec::new(),
        vtx: block
            .transactions()
            .iter()
            .map(expected_compact_transaction)
            .collect(),
        chain_metadata: Some(lightwalletd::ChainMetadata {
            sapling_commitment_tree_size: metadata.sapling_commitment_tree_size,
            orchard_commitment_tree_size: metadata.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: metadata.ironwood_commitment_tree_size,
        }),
    }
}

fn expected_compact_transaction(transaction: &CompactTransaction) -> lightwalletd::CompactTx {
    expected_compact_data(
        transaction.index,
        transaction.transaction_id,
        &transaction.data,
    )
}

fn expected_compact_data(
    index: u64,
    transaction_id: zinder_core::TransactionId,
    transaction_data: &CompactTransactionData,
) -> lightwalletd::CompactTx {
    lightwalletd::CompactTx {
        index,
        txid: encode_internal_transaction_id(transaction_id).to_vec(),
        fee: transaction_data
            .fee_zat
            .and_then(|fee| u32::try_from(fee).ok())
            .unwrap_or_default(),
        spends: transaction_data
            .sapling_spends
            .iter()
            .map(|spend| lightwalletd::CompactSaplingSpend {
                nf: spend.nullifier.to_vec(),
            })
            .collect(),
        outputs: transaction_data
            .sapling_outputs
            .iter()
            .map(|output| lightwalletd::CompactSaplingOutput {
                cmu: output.commitment.to_vec(),
                ephemeral_key: output.ephemeral_key.to_vec(),
                ciphertext: output.ciphertext.to_vec(),
            })
            .collect(),
        actions: transaction_data
            .orchard_actions
            .iter()
            .map(|action| lightwalletd::CompactOrchardAction {
                nullifier: action.nullifier.to_vec(),
                cmx: action.commitment.to_vec(),
                ephemeral_key: action.ephemeral_key.to_vec(),
                ciphertext: action.ciphertext.to_vec(),
            })
            .collect(),
        ironwood_actions: transaction_data
            .ironwood_actions
            .iter()
            .map(|action| lightwalletd::CompactOrchardAction {
                nullifier: action.nullifier.to_vec(),
                cmx: action.commitment.to_vec(),
                ephemeral_key: action.ephemeral_key.to_vec(),
                ciphertext: action.ciphertext.to_vec(),
            })
            .collect(),
        vin: transaction_data
            .transparent_inputs
            .iter()
            .map(|input| lightwalletd::CompactTxIn {
                prevout_txid: encode_internal_transaction_id(input.previous_transaction_id)
                    .to_vec(),
                prevout_index: input.previous_output_index,
            })
            .collect(),
        vout: transaction_data
            .transparent_outputs
            .iter()
            .map(|output| lightwalletd::TxOut {
                script_pub_key: output.script_pub_key.clone(),
                value: output.value_zat,
            })
            .collect(),
    }
}

async fn wait_until_ready(child: &mut tokio::process::Child, address: SocketAddr) -> Result<()> {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(eyre!("compat process exited before readiness: {status}"));
        }
        if let Ok((status, body)) = get_readyz(address)
            && status == 200
            && body["status"] == "ready"
        {
            return Ok(());
        }
        if started.elapsed() >= READY_TIMEOUT {
            return Err(eyre!(
                "compact process did not become ready within {READY_TIMEOUT:?}"
            ));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn get_readyz(address: SocketAddr) -> Result<(u16, Value)> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(500))?;
    stream.set_read_timeout(Some(Duration::from_millis(500)))?;
    write!(
        stream,
        "GET /readyz HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes)?;
    let text = String::from_utf8(bytes)?;
    let (header, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| eyre!("readyz response omitted header separator"))?;
    let status = header
        .lines()
        .find(|line| line.starts_with("HTTP/"))
        .ok_or_else(|| eyre!("readyz response omitted HTTP status line"))?
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| eyre!("readyz response omitted HTTP status"))?
        .parse()?;
    Ok((status, serde_json::from_str(body)?))
}

fn free_loopback_addr() -> Result<SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}

async fn send_sigterm(child: &tokio::process::Child) -> Result<()> {
    let Some(pid) = child.id() else {
        return Err(eyre!("compat process did not expose a PID"));
    };
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .await?;
    if !status.success() {
        return Err(eyre!("kill -TERM failed for compat PID {pid}"));
    }
    Ok(())
}

async fn wait_for_port_closed(address: SocketAddr) -> bool {
    for _ in 0..40 {
        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

fn inventory(root: &Path) -> Result<Vec<InventoryEntry>> {
    let mut entries = Vec::new();
    inventory_dir(root, root, &mut entries)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(entries)
}

fn inventory_dir(root: &Path, directory: &Path, entries: &mut Vec<InventoryEntry>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            inventory_dir(root, &path, entries)?;
        } else if file_type.is_file() {
            let bytes = fs::read(&path)?;
            entries.push(InventoryEntry {
                path: path.strip_prefix(root)?.display().to_string(),
                bytes: u64::try_from(bytes.len())?,
                sha256: sha256_bytes(&bytes),
            });
        }
    }
    Ok(())
}

fn inventory_hash(entries: &[InventoryEntry]) -> String {
    let encoded = serde_json::to_vec(entries).unwrap_or_default();
    sha256_bytes(&encoded)
}

fn sha256_file(path: &Path) -> Result<String> {
    Ok(sha256_bytes(&fs::read(path)?))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    hex::encode(digest.finalize())
}

fn write_report_atomically(path: &Path, report: &impl Serialize) -> Result<()> {
    let encoded = serde_json::to_vec_pretty(report)?;
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let temporary = path.with_extension(format!("json.{nanos}.tmp"));
    fs::write(&temporary, encoded)?;
    fs::rename(temporary, path)?;
    Ok(())
}
