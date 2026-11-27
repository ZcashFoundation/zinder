//! Stateless multi-replica topology conformance and client isolation checks.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use base64::Engine as _;
use eyre::{ensure, eyre};
use parking_lot::RwLock;
use prost::Message as _;
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::{net::TcpListener, net::TcpStream, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use toml::Value as TomlValue;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;
use zinder_compat_cipherscan::{
    CipherscanMarketPriceEndpoints, CipherscanRestAdapter,
    runtime::{CipherscanConfig, serve_supervised},
};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::{BlockHeight, Network, TransactionId, wire::encode_zinder_native_chain_name};
use zinder_explorer::{ExplorerQueryGrpcAdapter, ExplorerServerInfoSettings};
use zinder_ingest::{MaterializedViewReplayConfig, MaterializedViewTailer, ReplayPrevoutCaches};
use zinder_materialized_views::{MaterializedViewStore, MaterializedViewStoreOptions};
use zinder_proto::{
    compat::lightwalletd::{self, compact_tx_streamer_client::CompactTxStreamerClient},
    v1::{
        explorer::{
            BlockDetailRequest, BlockSummariesInRangeRequest, ServerInfoRequest,
            block_detail_request, explorer_query_client::ExplorerQueryClient,
        },
        wallet::{VisibleTipBlockRequest, wallet_query_client::WalletQueryClient},
    },
};
use zinder_query::{
    AdmittedIngestControl, WalletEndpointMetadata, WalletQueryGrpcAdapter, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_runtime::{
    ClientAddressSource, HttpListenerLimits, OpsEndpointHandle, Readiness, RuntimeService,
    connect_zinder_grpc, serve_http, spawn_ops_endpoint_for,
};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreRequirements, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbResourceBudget,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
    IngestControlFixture, open_test_materialized_view_store_for_canonical,
    sample_regtest_upgrade_activations,
};

const TEST_REORG_WINDOW: u32 = 100;
const MAX_WS_HANDSHAKE_BYTES: usize = 8192;
const WS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const READYZ_TIMEOUT: Duration = Duration::from_secs(5);
const BANNED_CLIENT_DRIVERS: [&str; 14] = [
    "bb8-postgres",
    "deadpool-postgres",
    "diesel",
    "librocksdb-sys",
    "libsqlite3-sys",
    "postgres",
    "rocksdb",
    "rusqlite",
    "rust-rocksdb",
    "sqlx",
    "sqlx-core",
    "sqlx-postgres",
    "tokio-postgres",
    "zinder-store",
];

struct ReplicaAddresses {
    compat: SocketAddr,
    explorer: SocketAddr,
    cipherscan: SocketAddr,
    wallet: SocketAddr,
}

struct ReplicaStack {
    _secondary_dir: TempDir,
    addresses: ReplicaAddresses,
    tasks: Vec<JoinHandle<eyre::Result<()>>>,
}

fn build_fixture_chain() -> eyre::Result<ChainFixture> {
    let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    for height in 1_u32..=3_u32 {
        let block = chain
            .block_at(BlockHeight::new(height))
            .ok_or_else(|| eyre!("missing block at height {height}"))?
            .clone();
        let mut tx_bytes = [0x90_u8; 32];
        tx_bytes[..4].copy_from_slice(&height.to_be_bytes());
        chain = chain.with_transaction_rows(FixtureTransactionRows::coinbase(
            TransactionId::from_bytes(tx_bytes),
            block.height,
            block.hash,
            format!("topology-cb-{height}").into_bytes(),
        ));
    }
    Ok(chain)
}

fn prepare_materialized_views(canon_path: &Path) -> eyre::Result<(TempDir, MaterializedViewStore)> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mv_tail_dir = TempDir::new()?;
    let canon_sec = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        canon_path,
        mv_tail_dir.path().join("canon-sec"),
        Network::ZcashRegtest,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let mv_store = open_test_materialized_view_store_for_canonical(canon_path)?;
    MaterializedViewTailer {
        canonical: Arc::new(RwLock::new(canon_sec)),
        materialized_view_store: mv_store.clone(),
        config: MaterializedViewReplayConfig::DEFAULT,
        activations,
        reorg_window_blocks: TEST_REORG_WINDOW,
        chain_event_retention_window: Some(Duration::from_hours(168)),
        cursor_at_risk_warning: Duration::from_hours(24),
        prevout_caches: ReplayPrevoutCaches::default(),
        volume_space_guard: None,
    }
    .catch_up()?;
    Ok((mv_tail_dir, mv_store))
}

async fn spawn_wallet_and_compat(
    writer: &mut CanonicalWriterFixture,
    ingest_endpoint: &str,
    cancel: &CancellationToken,
) -> eyre::Result<(SocketAddr, SocketAddr, Vec<JoinHandle<eyre::Result<()>>>)> {
    let mut store_fixture = writer.serving_store_fixture()?;
    let (canon_reader, wallet_reader) = store_fixture.take_readers()?;
    let admitted =
        AdmittedIngestControl::connect(ingest_endpoint, None, Network::ZcashRegtest).await?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(zinder_query::RocksDbCanonicalReader::new(canon_reader)?),
        Arc::new(zinder_query::RocksDbWalletReader::new(wallet_reader)),
    )?);
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(pair),
        admitted,
    );

    let wallet_listener = TcpListener::bind("127.0.0.1:0").await?;
    let wallet_addr = wallet_listener.local_addr()?;
    let wallet_shutdown = cancel.clone();
    let wallet_adapter =
        WalletQueryGrpcAdapter::new(query.clone(), WalletEndpointMetadata::default());
    let wallet_task = tokio::spawn(async move {
        let _keep_fixture = store_fixture;
        Server::builder()
            .add_service(wallet_adapter.into_server())
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(wallet_listener),
                wallet_shutdown.cancelled_owned(),
            )
            .await?;
        Ok(())
    });

    let compat_listener = TcpListener::bind("127.0.0.1:0").await?;
    let compat_addr = compat_listener.local_addr()?;
    let compat_shutdown = cancel.clone();
    let compat_task = tokio::spawn(async move {
        zinder_runtime::public_grpc_server(ClientAddressSource::Connection)
            .accept_http1(true)
            .transport_layer(GrpcWebLayer::new())
            .add_service(LightwalletdGrpcAdapter::new(query).into_server())
            .serve_with_incoming_shutdown(compat_listener, compat_shutdown.cancelled_owned())
            .await?;
        Ok(())
    });
    Ok((wallet_addr, compat_addr, vec![wallet_task, compat_task]))
}

fn sample_market_price_endpoints() -> eyre::Result<CipherscanMarketPriceEndpoints> {
    Ok(CipherscanMarketPriceEndpoints::new(
        reqwest::Url::parse("http://127.0.0.1:9/current")?,
        "http://127.0.0.1:9/history/{date}".to_owned(),
    )?)
}

fn build_cipherscan_config(
    listen_addr: SocketAddr,
    explorer_addr: SocketAddr,
    wallet_addr: SocketAddr,
) -> eyre::Result<CipherscanConfig> {
    Ok(CipherscanConfig {
        network: Network::ZcashRegtest,
        listen_addr,
        ops_listen_addr: None,
        allow_public_bind: false,
        explorer_query_endpoint: format!("http://{explorer_addr}"),
        wallet_query_endpoint: format!("http://{wallet_addr}"),
        market_price_endpoints: sample_market_price_endpoints()?,
        bearer_token_path: None,
        bearer_token: None,
    })
}

async fn reserve_loopback_addr() -> eyre::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?)
}

async fn spawn_explorer_grpc(
    canon_path: &Path,
    secondary_dir: &Path,
    wallet_addr: SocketAddr,
    cancel: &CancellationToken,
) -> eyre::Result<(SocketAddr, JoinHandle<eyre::Result<()>>)> {
    let explorer_canon = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        canon_path,
        secondary_dir.join("explorer-canon"),
        Network::ZcashRegtest,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let mv_secondary = MaterializedViewStore::open_explorer_views_secondary(
        MaterializedViewStore::path_for_canonical(canon_path),
        secondary_dir.join("mv-secondary"),
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?;
    mv_secondary.try_catch_up()?;

    let explorer_adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    })
    .with_canonical_store(Arc::new(RwLock::new(explorer_canon)))
    .with_materialized_view_store(mv_secondary)
    .with_wallet_query_endpoint(format!("http://{wallet_addr}"))
    .with_prevout_resolution_online(true);
    let explorer_listener = TcpListener::bind("127.0.0.1:0").await?;
    let explorer_addr = explorer_listener.local_addr()?;
    let explorer_shutdown = cancel.clone();
    let explorer_task = tokio::spawn(async move {
        Server::builder()
            .add_service(explorer_adapter.into_server())
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(explorer_listener),
                explorer_shutdown.cancelled_owned(),
            )
            .await?;
        Ok(())
    });
    Ok((explorer_addr, explorer_task))
}

async fn spawn_explorer_and_cipherscan(
    canon_path: &Path,
    secondary_dir: &Path,
    wallet_addr: SocketAddr,
    cancel: &CancellationToken,
) -> eyre::Result<(SocketAddr, SocketAddr, Vec<JoinHandle<eyre::Result<()>>>)> {
    let (explorer_addr, explorer_task) =
        spawn_explorer_grpc(canon_path, secondary_dir, wallet_addr, cancel).await?;
    let (cipherscan_addr, cs_task) =
        spawn_cipherscan_http(explorer_addr, wallet_addr, cancel).await?;
    Ok((explorer_addr, cipherscan_addr, vec![explorer_task, cs_task]))
}

async fn spawn_cipherscan_http(
    explorer_addr: SocketAddr,
    wallet_addr: SocketAddr,
    cancel: &CancellationToken,
) -> eyre::Result<(SocketAddr, JoinHandle<eyre::Result<()>>)> {
    let explorer_channel = connect_zinder_grpc(&format!("http://{explorer_addr}"), None).await?;
    let wallet_channel = connect_zinder_grpc(&format!("http://{wallet_addr}"), None).await?;
    let adapter = CipherscanRestAdapter::new(
        Network::ZcashRegtest,
        explorer_channel,
        wallet_channel,
        sample_market_price_endpoints()?,
        cancel.clone(),
    )?;
    let router = adapter.router();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shutdown = cancel.clone();
    let task = tokio::spawn(async move {
        serve_http(
            listener,
            router,
            HttpListenerLimits::default(),
            shutdown.cancelled_owned(),
        )
        .await?;
        Ok(())
    });
    Ok((addr, task))
}

async fn spawn_replica_stack(
    writer: &mut CanonicalWriterFixture,
    canon_path: &Path,
    ingest_endpoint: &str,
    cancel: &CancellationToken,
) -> eyre::Result<ReplicaStack> {
    let secondary_dir = TempDir::new()?;
    let (wallet_addr, compat_addr, mut tasks) =
        spawn_wallet_and_compat(writer, ingest_endpoint, cancel).await?;
    let (explorer_addr, cipherscan_addr, mut extra_tasks) =
        spawn_explorer_and_cipherscan(canon_path, secondary_dir.path(), wallet_addr, cancel)
            .await?;
    tasks.append(&mut extra_tasks);
    Ok(ReplicaStack {
        _secondary_dir: secondary_dir,
        addresses: ReplicaAddresses {
            compat: compat_addr,
            explorer: explorer_addr,
            cipherscan: cipherscan_addr,
            wallet: wallet_addr,
        },
        tasks,
    })
}

fn encode_grpc_web_frame(payload: &[u8]) -> eyre::Result<Vec<u8>> {
    let length = u32::try_from(payload.len())?;
    let mut framed = Vec::with_capacity(5 + payload.len());
    framed.push(0);
    framed.extend_from_slice(&length.to_be_bytes());
    framed.extend_from_slice(payload);
    Ok(framed)
}

fn decode_first_grpc_web_message<M: prost::Message + Default>(bytes: &[u8]) -> eyre::Result<M> {
    ensure!(bytes.len() >= 5, "grpc-web response truncated");
    ensure!(bytes[0] == 0, "expected gRPC data frame header 0");
    let frame_len = usize::try_from(u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]))?;
    let payload_end = 5 + frame_len;
    ensure!(
        bytes.len() >= payload_end,
        "grpc-web frame extends past body"
    );
    Ok(M::decode(&bytes[5..payload_end])?)
}

async fn query_http2_and_grpc_web(addr: SocketAddr) -> eyre::Result<(u64, Vec<u8>)> {
    let mut client = CompactTxStreamerClient::connect(format!("http://{addr}")).await?;
    let lightd_info = client
        .get_lightd_info(lightwalletd::Empty {})
        .await?
        .into_inner();
    ensure!(lightd_info.block_height == 3);
    let tip = client
        .get_latest_block(lightwalletd::ChainSpec {})
        .await?
        .into_inner();
    ensure!(tip.height == 3);
    let block2 = client
        .get_block(lightwalletd::BlockId {
            height: 2,
            hash: Vec::new(),
        })
        .await?
        .into_inner();
    ensure!(block2.height == 2);

    let http_client = reqwest::Client::builder().http1_only().build()?;
    let request_bytes = encode_grpc_web_frame(&lightwalletd::ChainSpec {}.encode_to_vec())?;
    let bin_reply = http_client
        .post(format!(
            "http://{addr}/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock"
        ))
        .header("content-type", "application/grpc-web+proto")
        .header("x-grpc-web", "1")
        .body(request_bytes.clone())
        .send()
        .await?;
    ensure!(bin_reply.status().is_success());
    let bin_tip: lightwalletd::BlockId = decode_first_grpc_web_message(&bin_reply.bytes().await?)?;
    ensure!(bin_tip.height == tip.height && bin_tip.hash == tip.hash);

    let encoded_req = base64::engine::general_purpose::STANDARD.encode(&request_bytes);
    let text_reply = http_client
        .post(format!(
            "http://{addr}/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock"
        ))
        .header("content-type", "application/grpc-web-text+proto")
        .header("accept", "application/grpc-web-text+proto")
        .header("x-grpc-web", "1")
        .body(encoded_req)
        .send()
        .await?;
    ensure!(text_reply.status().is_success());
    let encoded_body = text_reply.bytes().await?;
    let mut raw_text_bytes = Vec::new();
    for quartet in encoded_body.chunks(4) {
        base64::engine::general_purpose::STANDARD.decode_vec(quartet, &mut raw_text_bytes)?;
    }
    let text_tip: lightwalletd::BlockId = decode_first_grpc_web_message(&raw_text_bytes)?;
    ensure!(text_tip.height == tip.height && text_tip.hash == tip.hash);
    Ok((tip.height, tip.hash))
}

async fn fetch_json(http: &reqwest::Client, url: &str) -> eyre::Result<JsonValue> {
    let body = http
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    Ok(serde_json::from_slice(&body)?)
}

struct ExplorerCipherscanSnapshot {
    block2_hash: String,
    summaries: Vec<(u32, String)>,
    summary_json: JsonValue,
    blockchain_json: JsonValue,
    mempool_json: JsonValue,
    reorg_stats_json: JsonValue,
}

impl PartialEq for ExplorerCipherscanSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.block2_hash == other.block2_hash
            && self.summaries == other.summaries
            && self.summary_json == other.summary_json
            && self.blockchain_json == other.blockchain_json
            && self.mempool_json == other.mempool_json
            && self.reorg_stats_json == other.reorg_stats_json
    }
}

async fn query_explorer_replica(
    explorer_addr: SocketAddr,
) -> eyre::Result<(String, Vec<(u32, String)>)> {
    let mut explorer = ExplorerQueryClient::connect(format!("http://{explorer_addr}")).await?;
    let server_res = explorer
        .server_info(ServerInfoRequest {})
        .await?
        .into_inner();
    let server_details = server_res
        .info
        .and_then(|descriptor| descriptor.common)
        .ok_or_else(|| eyre!("explorer server_info omitted common ServerInfo"))?;
    ensure!(server_details.network == "zcash-regtest");
    let block_res = explorer
        .block_detail(BlockDetailRequest {
            at_epoch_id: None,
            selector: Some(block_detail_request::Selector::BlockHeight(2)),
        })
        .await?
        .into_inner();
    let tip_height = block_res
        .freshness
        .as_ref()
        .and_then(|fresh| fresh.chain_view.as_ref())
        .and_then(|view| view.chain_epoch.as_ref())
        .and_then(|epoch| epoch.visible_tip.as_ref())
        .map_or(0, |tip| tip.height);
    ensure!(tip_height == 3);
    let block2_hash = block_res
        .summary
        .map(|summary| summary.block_hash)
        .ok_or_else(|| eyre!("explorer block_detail omitted summary"))?;

    let range_res = explorer
        .block_summaries_in_range(BlockSummariesInRangeRequest {
            start_height: 1,
            end_height: 3,
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    let summaries: Vec<(u32, String)> = range_res
        .summaries
        .into_iter()
        .map(|summary| (summary.block_height, summary.block_hash))
        .collect();
    ensure!(summaries.len() == 3);
    ensure!(summaries[1].1 == block2_hash);
    Ok((block2_hash, summaries))
}

async fn query_explorer_and_cipherscan(
    addresses: &ReplicaAddresses,
) -> eyre::Result<ExplorerCipherscanSnapshot> {
    let (block2_hash, summaries) = query_explorer_replica(addresses.explorer).await?;
    let http = reqwest::Client::new();
    let summary_json =
        fetch_json(&http, &format!("http://{}/api/info", addresses.cipherscan)).await?;
    let blockchain_json = fetch_json(
        &http,
        &format!("http://{}/api/blockchain-info", addresses.cipherscan),
    )
    .await?;
    let mempool_json = fetch_json(
        &http,
        &format!("http://{}/api/mempool", addresses.cipherscan),
    )
    .await?;
    let reorg_stats_json = fetch_json(
        &http,
        &format!("http://{}/api/uncles/stats", addresses.cipherscan),
    )
    .await?;
    verify_cipherscan_websocket(addresses.cipherscan).await?;
    Ok(ExplorerCipherscanSnapshot {
        block2_hash,
        summaries,
        summary_json,
        blockchain_json,
        mempool_json,
        reorg_stats_json,
    })
}

async fn read_http_response_head(stream: &TcpStream) -> eyre::Result<String> {
    let mut head = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 512];
    loop {
        stream.readable().await?;
        match stream.try_read(&mut chunk) {
            Ok(0) => {
                return Err(eyre!(
                    "unexpected EOF before WebSocket upgrade headers completed"
                ));
            }
            Ok(bytes_read) => {
                head.extend_from_slice(&chunk[..bytes_read]);
                ensure!(
                    head.len() <= MAX_WS_HANDSHAKE_BYTES,
                    "WebSocket upgrade response exceeded {MAX_WS_HANDSHAKE_BYTES} bytes"
                );
                if head.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(String::from_utf8_lossy(&head).into_owned())
}

async fn verify_cipherscan_websocket(cipherscan_addr: SocketAddr) -> eyre::Result<()> {
    let stream = TcpStream::connect(cipherscan_addr).await?;
    let req = format!(
        "GET / HTTP/1.1\r\nHost: {cipherscan_addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    let req_bytes = req.as_bytes();
    let mut written = 0;
    while written < req_bytes.len() {
        stream.writable().await?;
        match stream.try_write(&req_bytes[written..]) {
            Ok(bytes_sent) => written += bytes_sent,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(err) => return Err(err.into()),
        }
    }
    let headers = tokio::time::timeout(WS_HANDSHAKE_TIMEOUT, read_http_response_head(&stream))
        .await
        .map_err(|_| eyre!("timed out waiting for WebSocket upgrade response headers"))??;
    ensure!(
        headers.starts_with("HTTP/1.1 101 Switching Protocols"),
        "expected WebSocket 101 Switching Protocols, got: {headers}"
    );
    Ok(())
}

struct SupervisedCipherscan {
    listen_addr: SocketAddr,
    ops_addr: SocketAddr,
    readiness: Readiness,
    ops_handle: OpsEndpointHandle,
    cancel: CancellationToken,
    task: JoinHandle<eyre::Result<()>>,
}

async fn spawn_cipherscan_ops(
    readiness: Readiness,
) -> eyre::Result<(SocketAddr, OpsEndpointHandle)> {
    let ops_addr = reserve_loopback_addr().await?;
    let ops_handle = spawn_ops_endpoint_for(
        RuntimeService::CompatCipherscan,
        Some(ops_addr),
        env!("CARGO_PKG_VERSION"),
        encode_zinder_native_chain_name(Network::ZcashRegtest),
        readiness,
        Arc::from([]),
    )
    .await?
    .ok_or_else(|| eyre!("expected ops endpoint handle for Cipherscan conformance test"))?;
    Ok((ops_addr, ops_handle))
}

async fn verify_cipherscan_startup_gate_fails_closed(
    explorer_addr: SocketAddr,
    wallet_addr: SocketAddr,
    outage_label: &str,
) -> eyre::Result<()> {
    let listen_addr = reserve_loopback_addr().await?;
    let readiness = Readiness::default();
    let (ops_addr, ops_handle) = spawn_cipherscan_ops(readiness.clone()).await?;
    let http = reqwest::Client::new();
    let pre_start_readyz = http.get(format!("http://{ops_addr}/readyz")).send().await?;
    ensure!(
        pre_start_readyz.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "/readyz must start at 503 before Cipherscan connects, got {}",
        pre_start_readyz.status()
    );

    let config = build_cipherscan_config(listen_addr, explorer_addr, wallet_addr)?;
    let startup = serve_supervised(config, readiness.clone(), CancellationToken::new()).await;
    ensure!(
        startup.is_err(),
        "Cipherscan must fail closed during {outage_label}"
    );
    ensure!(
        !readiness.report().is_ready,
        "Cipherscan readiness must remain not_ready during {outage_label}"
    );
    let outage_readyz = http.get(format!("http://{ops_addr}/readyz")).send().await?;
    ensure!(
        outage_readyz.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "/readyz must return 503 during {outage_label}, got {}",
        outage_readyz.status()
    );
    ensure!(
        TcpStream::connect(listen_addr).await.is_err(),
        "Cipherscan HTTP traffic gate must stay closed during {outage_label}"
    );
    ops_handle.shutdown().await?;
    Ok(())
}

async fn spawn_supervised_cipherscan(
    explorer_addr: SocketAddr,
    wallet_addr: SocketAddr,
) -> eyre::Result<SupervisedCipherscan> {
    let listen_addr = reserve_loopback_addr().await?;
    let readiness = Readiness::default();
    let (ops_addr, ops_handle) = spawn_cipherscan_ops(readiness.clone()).await?;
    let config = build_cipherscan_config(listen_addr, explorer_addr, wallet_addr)?;
    let cancel = CancellationToken::new();
    let serve_readiness = readiness.clone();
    let serve_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        serve_supervised(config, serve_readiness, serve_cancel)
            .await
            .map_err(eyre::Report::from)
    });

    let http = reqwest::Client::new();
    let readyz_url = format!("http://{ops_addr}/readyz");
    tokio::time::timeout(READYZ_TIMEOUT, async {
        loop {
            ensure!(
                !task.is_finished(),
                "Cipherscan supervised server exited before becoming ready"
            );
            let resp = http.get(&readyz_url).send().await?;
            if resp.status() == reqwest::StatusCode::OK && readiness.report().is_ready {
                break Ok::<(), eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| eyre!("timed out waiting for Cipherscan /readyz to become 200 OK"))??;
    TcpStream::connect(listen_addr).await?;

    Ok(SupervisedCipherscan {
        listen_addr,
        ops_addr,
        readiness,
        ops_handle,
        cancel,
        task,
    })
}

async fn shutdown_supervised_cipherscan(cipherscan: SupervisedCipherscan) -> eyre::Result<()> {
    cipherscan.cancel.cancel();
    cipherscan.task.await??;
    ensure!(
        !cipherscan.readiness.report().is_ready,
        "Cipherscan readiness must drain on cancellation"
    );
    let drained = reqwest::Client::new()
        .get(format!("http://{}/readyz", cipherscan.ops_addr))
        .send()
        .await?;
    ensure!(
        drained.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "/readyz must return 503 after cancellation, got {}",
        drained.status()
    );
    cipherscan.ops_handle.shutdown().await?;
    Ok(())
}

async fn verify_cipherscan_runtime_wallet_outage(
    explorer_addr: SocketAddr,
    writer: &mut CanonicalWriterFixture,
    ingest_endpoint: &str,
) -> eyre::Result<()> {
    let wallet_cancel = CancellationToken::new();
    let (wallet_addr, _compat_addr, wallet_tasks) =
        spawn_wallet_and_compat(writer, ingest_endpoint, &wallet_cancel).await?;
    let cs = spawn_supervised_cipherscan(explorer_addr, wallet_addr).await?;
    let http = reqwest::Client::new();
    let pre_outage = http
        .get(format!("http://{}/api/info", cs.listen_addr))
        .send()
        .await?;
    ensure!(
        pre_outage.status() == reqwest::StatusCode::OK,
        "expected /api/info 200 OK when both upstreams are healthy, got {}",
        pre_outage.status()
    );

    wallet_cancel.cancel();
    for handle in wallet_tasks {
        handle.await??;
    }

    query_explorer_replica(explorer_addr).await?;
    let wallet_dependent_resp = http
        .get(format!("http://{}/api/blockchain-info", cs.listen_addr))
        .send()
        .await?;
    ensure!(
        wallet_dependent_resp.status().is_server_error(),
        "Cipherscan must fail closed when WalletQuery is unreachable, got {}",
        wallet_dependent_resp.status()
    );
    shutdown_supervised_cipherscan(cs).await
}

async fn verify_cipherscan_runtime_explorer_outage(
    canon_path: &Path,
    wallet_healthy_addr: SocketAddr,
) -> eyre::Result<()> {
    let secondary_dir = TempDir::new()?;
    let explorer_cancel = CancellationToken::new();
    let (explorer_addr, explorer_task) = spawn_explorer_grpc(
        canon_path,
        secondary_dir.path(),
        wallet_healthy_addr,
        &explorer_cancel,
    )
    .await?;
    let cs = spawn_supervised_cipherscan(explorer_addr, wallet_healthy_addr).await?;
    let http = reqwest::Client::new();
    let pre_outage = http
        .get(format!("http://{}/api/uncles/stats", cs.listen_addr))
        .send()
        .await?;
    ensure!(
        pre_outage.status() == reqwest::StatusCode::OK,
        "expected /api/uncles/stats 200 OK when both upstreams are healthy, got {}",
        pre_outage.status()
    );

    explorer_cancel.cancel();
    explorer_task.await??;

    let wallet_channel =
        connect_zinder_grpc(&format!("http://{wallet_healthy_addr}"), None).await?;
    let tip_response = WalletQueryClient::new(wallet_channel)
        .visible_tip_block(VisibleTipBlockRequest { at_epoch_id: None })
        .await?;
    ensure!(
        tip_response.into_inner().visible_tip_block.is_some(),
        "WalletQuery must stay healthy during ExplorerQuery-only outage"
    );

    let explorer_dependent_resp = http
        .get(format!("http://{}/api/uncles/stats", cs.listen_addr))
        .send()
        .await?;
    ensure!(
        explorer_dependent_resp.status().is_server_error(),
        "Cipherscan must fail closed when ExplorerQuery is unreachable, got {}",
        explorer_dependent_resp.status()
    );
    shutdown_supervised_cipherscan(cs).await
}

async fn verify_cipherscan_requires_both_planes(
    explorer_addr: SocketAddr,
    canon_path: &Path,
    wallet_healthy_addr: SocketAddr,
    writer: &mut CanonicalWriterFixture,
    ingest_endpoint: &str,
) -> eyre::Result<()> {
    let unreachable_addr = reserve_loopback_addr().await?;
    verify_cipherscan_startup_gate_fails_closed(explorer_addr, unreachable_addr, "wallet outage")
        .await?;
    verify_cipherscan_startup_gate_fails_closed(
        unreachable_addr,
        wallet_healthy_addr,
        "explorer outage",
    )
    .await?;
    verify_cipherscan_runtime_wallet_outage(explorer_addr, writer, ingest_endpoint).await?;
    verify_cipherscan_runtime_explorer_outage(canon_path, wallet_healthy_addr).await
}

#[tokio::test(flavor = "multi_thread")]
async fn two_stateless_reader_replicas_serve_consistent_grpc_grpc_web_explorer_and_cipherscan()
-> eyre::Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let chain = build_fixture_chain()?;
    let mut writer =
        CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
    let canon_path = writer.canonical_primary_path().to_path_buf();
    let (_mv_tail_dir, _mv_store) = prepare_materialized_views(&canon_path)?;
    let ingest_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let cancel = CancellationToken::new();

    let replica_a =
        spawn_replica_stack(&mut writer, &canon_path, ingest_fixture.endpoint(), &cancel).await?;
    let replica_b =
        spawn_replica_stack(&mut writer, &canon_path, ingest_fixture.endpoint(), &cancel).await?;

    let grpc_a = query_http2_and_grpc_web(replica_a.addresses.compat).await?;
    let grpc_b = query_http2_and_grpc_web(replica_b.addresses.compat).await?;
    ensure!(grpc_a == grpc_b);
    ensure!(replica_a.addresses.wallet != replica_b.addresses.wallet);

    let rest_a = query_explorer_and_cipherscan(&replica_a.addresses).await?;
    let rest_b = query_explorer_and_cipherscan(&replica_b.addresses).await?;
    ensure!(rest_a == rest_b);

    verify_cipherscan_requires_both_planes(
        replica_a.addresses.explorer,
        &canon_path,
        replica_a.addresses.wallet,
        &mut writer,
        ingest_fixture.endpoint(),
    )
    .await?;

    cancel.cancel();
    for handle in replica_a.tasks.into_iter().chain(replica_b.tasks) {
        handle.await??;
    }
    ingest_fixture.shutdown().await?;
    Ok(())
}

fn workspace_root_dir() -> eyre::Result<PathBuf> {
    if let Some(root) = std::env::var_os("NEXTEST_WORKSPACE_ROOT") {
        return Ok(PathBuf::from(root));
    }
    let manifest_dir = zinder_testkit::manifest_dir!();
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| eyre!("cannot locate workspace root from {manifest_dir:?}"))
}

fn extract_runtime_deps(manifest: &TomlValue) -> BTreeSet<String> {
    let mut deps = BTreeSet::new();
    if let Some(table) = manifest.get("dependencies").and_then(TomlValue::as_table) {
        collect_dep_table(table, &mut deps);
    }
    if let Some(targets) = manifest.get("target").and_then(TomlValue::as_table) {
        for cfg_entry in targets.values() {
            if let Some(table) = cfg_entry.get("dependencies").and_then(TomlValue::as_table) {
                collect_dep_table(table, &mut deps);
            }
        }
    }
    deps
}

fn collect_dep_table(table: &toml::map::Map<String, TomlValue>, out: &mut BTreeSet<String>) {
    for (alias, spec) in table {
        let package_name = spec
            .as_table()
            .and_then(|tbl| tbl.get("package"))
            .and_then(TomlValue::as_str)
            .unwrap_or(alias.as_str());
        out.insert(package_name.to_owned());
    }
}

fn load_workspace_manifests(
    workspace_root: &Path,
) -> eyre::Result<BTreeMap<String, BTreeSet<String>>> {
    let root_text = std::fs::read_to_string(workspace_root.join("Cargo.toml"))?;
    let root_toml: TomlValue = toml::from_str(&root_text)?;
    let members = root_toml
        .get("workspace")
        .and_then(|ws| ws.get("members"))
        .and_then(TomlValue::as_array)
        .ok_or_else(|| eyre!("root Cargo.toml missing workspace.members"))?;
    let mut workspace_deps = BTreeMap::new();
    for member in members {
        let member_rel = member
            .as_str()
            .ok_or_else(|| eyre!("workspace member entry is not a string"))?;
        let member_text =
            std::fs::read_to_string(workspace_root.join(member_rel).join("Cargo.toml"))?;
        let member_toml: TomlValue = toml::from_str(&member_text)?;
        let crate_name = member_toml
            .get("package")
            .and_then(|pkg| pkg.get("name"))
            .and_then(TomlValue::as_str)
            .ok_or_else(|| eyre!("{member_rel}/Cargo.toml missing package.name"))?;
        workspace_deps.insert(crate_name.to_owned(), extract_runtime_deps(&member_toml));
    }
    Ok(workspace_deps)
}

fn load_lockfile_graph(workspace_root: &Path) -> eyre::Result<BTreeMap<String, Vec<String>>> {
    let lock_text = std::fs::read_to_string(workspace_root.join("Cargo.lock"))?;
    let lock_toml: TomlValue = toml::from_str(&lock_text)?;
    let packages = lock_toml
        .get("package")
        .and_then(TomlValue::as_array)
        .ok_or_else(|| eyre!("Cargo.lock missing [[package]] entries"))?;
    let mut graph: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for pkg in packages {
        let Some(pkg_name) = pkg.get("name").and_then(TomlValue::as_str) else {
            continue;
        };
        let entry = graph.entry(pkg_name.to_owned()).or_default();
        if let Some(dep_list) = pkg.get("dependencies").and_then(TomlValue::as_array) {
            for dep in dep_list {
                if let Some(name) = dep.as_str().and_then(|raw| raw.split_whitespace().next()) {
                    entry.push(name.to_owned());
                }
            }
        }
    }
    Ok(graph)
}

#[test]
fn zinder_client_has_no_database_driver_dependencies() -> eyre::Result<()> {
    let workspace_root = workspace_root_dir()?;
    let workspace_deps = load_workspace_manifests(&workspace_root)?;
    let lock_graph = load_lockfile_graph(&workspace_root)?;

    let mut visited = BTreeSet::new();
    let mut queue = VecDeque::from([String::from("zinder-client")]);
    while let Some(current) = queue.pop_front() {
        if !visited.insert(current.clone()) {
            continue;
        }
        if let Some(runtime_edges) = workspace_deps.get(&current) {
            for edge in runtime_edges {
                if !visited.contains(edge) {
                    queue.push_back(edge.clone());
                }
            }
        } else if let Some(lock_edges) = lock_graph.get(&current) {
            for edge in lock_edges {
                if !visited.contains(edge) {
                    queue.push_back(edge.clone());
                }
            }
        }
    }

    let forbidden: Vec<&str> = BANNED_CLIENT_DRIVERS
        .iter()
        .copied()
        .filter(|driver| visited.contains(*driver))
        .collect();
    ensure!(
        forbidden.is_empty(),
        "zinder-client must have zero DB driver runtime dependencies, found: {forbidden:?}"
    );
    Ok(())
}
