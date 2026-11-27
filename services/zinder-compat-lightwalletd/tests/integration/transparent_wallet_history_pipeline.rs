#![allow(
    missing_docs,
    reason = "Integration test names describe the cold transparent-history pipeline contracts under test."
)]

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    convert::Infallible,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use eyre::{Result, eyre};
use http_body_util::{BodyExt as _, Full};
use hyper::{Request as HyperRequest, Response as HyperResponse, body::Bytes, server::conn::http1};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{Notify, Semaphore},
    task::JoinSet,
    time::timeout,
};
use tokio_stream::StreamExt as _;
use tonic::{Code, Request};
use zebra_chain::{
    parameters::NetworkKind as ZebraNetworkKind, transparent::Address as ZebraTransparentAddress,
};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::{
    BlockHash, BlockHeight, Network, NetworkUpgradeActivations, TransactionId,
    TransparentAddressScriptHash, TransparentOutPoint, TransparentUnspentOutput,
    wire::{
        encode_branch_id_hex, encode_internal_transaction_id, encode_rpc_block_hash_hex,
        encode_rpc_transaction_id_hex,
    },
};
use zinder_proto::compat::lightwalletd::{self, compact_tx_streamer_server::CompactTxStreamer};
use zinder_query::{
    ForwardedReadConfig, ForwardedReadLimit, OrderedBroadcaster, RocksDbCanonicalReader,
    RocksDbWalletReader, WalletQueryApi as _, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_store::RawBlobRetention;
use zinder_testkit::{ChainFixture, FixtureTransactionRows, WalletServingStoreFixture};

use crate::common::verified_node::one_upgrade_per_height;

#[path = "transparent_wallet_history_pipeline/gated_canonical.rs"]
mod gated_canonical;

pub(crate) type LeanWalletQuery =
    WalletServingQuery<Option<OrderedBroadcaster<ZebraJsonRpcSource>>>;
pub(crate) type LeanWalletAdapter = LightwalletdGrpcAdapter<LeanWalletQuery>;

#[derive(Clone)]
pub(crate) struct StoredTxEntry {
    pub(crate) height: BlockHeight,
    pub(crate) block_hash: BlockHash,
    pub(crate) raw_hex: String,
    pub(crate) delay: Duration,
}

pub(crate) struct AsyncZebraNode {
    address: SocketAddr,
    active_raw_tx: Arc<AtomicUsize>,
    pub(crate) peak_raw_tx: Arc<AtomicUsize>,
    gate_started: Arc<Notify>,
    gate_release: Arc<Semaphore>,
    timer: Arc<PreciseTimer>,
    shutdown: tokio_util::sync::CancellationToken,
}

pub(crate) struct NodeBehavior {
    pub(crate) entries: HashMap<String, StoredTxEntry>,
    pub(crate) tampered_txid_hex: Option<String>,
    pub(crate) stalled_txids: HashSet<String>,
}

struct PreciseTimer {
    inner: Arc<(parking_lot::Mutex<TimerQueue>, parking_lot::Condvar)>,
}

struct TimerQueue {
    deadlines: BTreeMap<(Instant, u64), tokio::sync::oneshot::Sender<()>>,
    next_id: u64,
    shutdown: bool,
}

impl PreciseTimer {
    fn start() -> Arc<Self> {
        let inner = Arc::new((
            parking_lot::Mutex::new(TimerQueue {
                deadlines: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
            }),
            parking_lot::Condvar::new(),
        ));
        let worker_inner = Arc::clone(&inner);
        std::thread::spawn(move || {
            let (lock, ready) = &*worker_inner;
            let mut queue = lock.lock();
            loop {
                if queue.shutdown {
                    return;
                }
                let now = Instant::now();
                while let Some((&(deadline, id), _)) = queue.deadlines.first_key_value() {
                    if deadline > now {
                        break;
                    }
                    if let Some(wake) = queue.deadlines.remove(&(deadline, id)) {
                        let _ = wake.send(());
                    }
                }
                if let Some((&(next_deadline, _), _)) = queue.deadlines.first_key_value() {
                    let _ = ready.wait_until(&mut queue, next_deadline);
                } else {
                    ready.wait(&mut queue);
                }
            }
        });
        Arc::new(Self { inner })
    }

    async fn sleep(&self, delay: Duration) {
        if delay.is_zero() {
            return;
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        {
            let (lock, ready) = &*self.inner;
            let mut queue = lock.lock();
            let id = queue.next_id;
            queue.next_id = queue.next_id.wrapping_add(1);
            queue.deadlines.insert((Instant::now() + delay, id), sender);
            drop(queue);
            ready.notify_one();
        }
        let _ = receiver.await;
    }

    fn stop(&self) {
        let (lock, ready) = &*self.inner;
        let mut queue = lock.lock();
        queue.shutdown = true;
        queue.deadlines.clear();
        drop(queue);
        ready.notify_all();
    }
}

impl AsyncZebraNode {
    pub(crate) async fn start(
        schedule: &NetworkUpgradeActivations,
        tip_hash: BlockHash,
        behavior: NodeBehavior,
    ) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let upgrades: serde_json::Map<String, Value> = schedule
            .activations()
            .iter()
            .map(|activation| {
                (
                    encode_branch_id_hex(activation.branch_id),
                    json!({
                        "name": activation.name,
                        "activationheight": activation.activation_height.value(),
                        "status": "active",
                    }),
                )
            })
            .collect();
        let active_raw_tx = Arc::new(AtomicUsize::new(0));
        let peak_raw_tx = Arc::new(AtomicUsize::new(0));
        let gate_started = Arc::new(Notify::new());
        let gate_release = Arc::new(Semaphore::new(0));
        let timer = PreciseTimer::start();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let shared = Arc::new(SharedNodeState {
            upgrades,
            tip_hash_hex: encode_rpc_block_hash_hex(tip_hash),
            behavior,
            active_raw_tx: Arc::clone(&active_raw_tx),
            peak_raw_tx: Arc::clone(&peak_raw_tx),
            gate_started: Arc::clone(&gate_started),
            gate_release: Arc::clone(&gate_release),
            timer: Arc::clone(&timer),
        });
        spawn_accept_loop(listener, shared, shutdown.clone());
        Ok(Self {
            address,
            active_raw_tx,
            peak_raw_tx,
            gate_started,
            gate_release,
            timer,
            shutdown,
        })
    }

    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    fn release_stalled(&self) {
        self.gate_release.close();
    }
}

fn spawn_accept_loop(
    listener: TcpListener,
    shared: Arc<SharedNodeState>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                () = shutdown.cancelled() => return,
                accepted = listener.accept() => accepted,
            };
            let Ok((tcp_stream, _)) = accepted else {
                return;
            };
            let _ = tcp_stream.set_nodelay(true);
            let conn_state = Arc::clone(&shared);
            let conn_shutdown = shutdown.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(tcp_stream);
                let service = hyper::service::service_fn(move |req| {
                    let call_state = Arc::clone(&conn_state);
                    async move { handle_rpc_http(req, &call_state).await }
                });
                let conn = http1::Builder::new()
                    .keep_alive(true)
                    .serve_connection(io, service);
                tokio::select! {
                    () = conn_shutdown.cancelled() => {}
                    _ = conn => {}
                }
            });
        }
    });
}

impl Drop for AsyncZebraNode {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.gate_release.close();
        self.timer.stop();
    }
}

struct SharedNodeState {
    upgrades: serde_json::Map<String, Value>,
    tip_hash_hex: String,
    behavior: NodeBehavior,
    active_raw_tx: Arc<AtomicUsize>,
    peak_raw_tx: Arc<AtomicUsize>,
    gate_started: Arc<Notify>,
    gate_release: Arc<Semaphore>,
    timer: Arc<PreciseTimer>,
}

struct ActiveCallGuard<'state>(&'state AtomicUsize);

impl<'state> ActiveCallGuard<'state> {
    fn enter(active: &'state AtomicUsize, peak: &AtomicUsize) -> Self {
        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(current, Ordering::SeqCst);
        Self(active)
    }
}

impl Drop for ActiveCallGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn handle_rpc_http(
    request: HyperRequest<hyper::body::Incoming>,
    state: &SharedNodeState,
) -> std::result::Result<HyperResponse<Full<Bytes>>, Infallible> {
    let body_bytes = match request.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return Ok(json_http_response(&json!({"jsonrpc": "2.0", "id": null}))),
    };
    let payload: Value = serde_json::from_slice(&body_bytes).unwrap_or(Value::Null);
    let request_id = payload.get("id").cloned().unwrap_or(Value::Null);
    let rpc_method = payload.get("method").and_then(Value::as_str).unwrap_or("");
    let params = payload.get("params").cloned().unwrap_or(Value::Null);
    let reply_body = dispatch_rpc(rpc_method, &params, &request_id, state).await;
    Ok(json_http_response(&reply_body))
}

async fn dispatch_rpc(
    rpc_method: &str,
    params: &Value,
    request_id: &Value,
    state: &SharedNodeState,
) -> Value {
    match rpc_method {
        "rpc.discover" => json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "openrpc": "1.3.2",
                "methods": [
                    {"name": "rpc.discover"},
                    {"name": "sendrawtransaction"},
                    {"name": "getstandardfee"},
                ],
            },
        }),
        "getblockchaininfo" => json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": { "upgrades": state.upgrades },
        }),
        "getblockhash" => json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": state.tip_hash_hex,
        }),
        "getrawtransaction" => handle_get_raw_transaction(params, request_id, state).await,
        other => json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": format!("unexpected method {other}")},
        }),
    }
}

async fn handle_get_raw_transaction(
    params: &Value,
    request_id: &Value,
    state: &SharedNodeState,
) -> Value {
    let _active_guard = ActiveCallGuard::enter(&state.active_raw_tx, &state.peak_raw_tx);
    let txid_hex = params
        .get(0)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if state.behavior.stalled_txids.contains(&txid_hex) {
        state.gate_started.notify_waiters();
        let _ = state.gate_release.acquire().await;
    }
    let Some(entry) = state.behavior.entries.get(&txid_hex) else {
        return json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -5, "message": "No such mempool or main chain transaction"},
        });
    };
    state.timer.sleep(entry.delay).await;
    let hex_payload = if state.behavior.tampered_txid_hex.as_deref() == Some(txid_hex.as_str()) {
        "deadbeef".to_owned()
    } else {
        entry.raw_hex.clone()
    };
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "txid": txid_hex,
            "hex": hex_payload,
            "height": entry.height.value(),
            "blockhash": encode_rpc_block_hash_hex(entry.block_hash),
        },
    })
}

fn json_http_response(body: &Value) -> HyperResponse<Full<Bytes>> {
    let serialized = serde_json::to_vec(body).unwrap_or_default();
    HyperResponse::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(serialized)))
        .unwrap_or_else(|_| HyperResponse::new(Full::new(Bytes::new())))
}

fn regtest_address(seed: u8) -> ZebraTransparentAddress {
    ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [seed; 20])
}

pub(crate) fn sequence_txid(address_seed: u8, sequence: u32) -> TransactionId {
    let mut bytes = [address_seed; 32];
    bytes[28..].copy_from_slice(&sequence.to_be_bytes());
    TransactionId::from_bytes(bytes)
}

fn sequence_payload(address_seed: u8, sequence: u32) -> Vec<u8> {
    format!("cold-history-addr-{address_seed:02x}-seq-{sequence:05}").into_bytes()
}

pub(crate) fn history_filter(
    address: String,
    start_height: u64,
    end_height: u64,
) -> lightwalletd::TransparentAddressBlockFilter {
    lightwalletd::TransparentAddressBlockFilter {
        address,
        range: Some(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: start_height,
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: end_height,
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }),
    }
}

pub(crate) struct MultiBlockFixture {
    pub(crate) store: WalletServingStoreFixture,
    pub(crate) schedule: NetworkUpgradeActivations,
    pub(crate) tip_hash: BlockHash,
    pub(crate) addresses: Vec<String>,
    pub(crate) expected_per_address: Vec<Vec<lightwalletd::RawTransaction>>,
    pub(crate) node_entries: HashMap<String, StoredTxEntry>,
}

pub(crate) fn build_multi_block_cold_fixture(
    block_count: u32,
    addresses_count: u8,
    txs_per_address: u32,
    delay_for: impl Fn(u8, u32) -> Duration,
) -> Result<MultiBlockFixture> {
    let schedule = one_upgrade_per_height()?;
    let mut chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(block_count);
    let tip_hash = chain
        .blocks()
        .last()
        .ok_or_else(|| eyre!("fixture must contain at least one block"))?
        .hash;
    let mut addresses = Vec::with_capacity(usize::from(addresses_count));
    let mut expected_per_address = Vec::with_capacity(usize::from(addresses_count));
    let mut node_entries = HashMap::new();
    let mut per_block_counts = vec![0_u32; usize::try_from(block_count)?];

    for addr_idx in 0..addresses_count {
        let seed = 0x40_u8.wrapping_add(addr_idx);
        let address = regtest_address(seed);
        let script = address.script().as_raw_bytes().to_vec();
        let script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
        let mut expected = Vec::with_capacity(usize::try_from(txs_per_address)?);
        for seq in 0..txs_per_address {
            let block_offset = (seq * block_count) / txs_per_address;
            let height_num = block_offset + 1;
            let block = chain
                .block_at(BlockHeight::new(height_num))
                .ok_or_else(|| eyre!("missing block {height_num}"))?
                .clone();
            let slot_idx = usize::try_from(block_offset)?;
            let tx_index = per_block_counts[slot_idx];
            per_block_counts[slot_idx] += 1;

            let txid = sequence_txid(seed, seq);
            let raw_bytes = sequence_payload(seed, seq);
            node_entries.insert(
                encode_rpc_transaction_id_hex(txid),
                StoredTxEntry {
                    height: block.height,
                    block_hash: block.hash,
                    raw_hex: hex::encode(&raw_bytes),
                    delay: delay_for(addr_idx, seq),
                },
            );
            expected.push(lightwalletd::RawTransaction {
                data: raw_bytes.clone(),
                height: u64::from(block.height.value()),
            });
            chain = chain
                .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
                    txid,
                    block.height,
                    block.hash,
                    tx_index,
                    raw_bytes,
                ))
                .with_address_output_index(TransparentUnspentOutput::new(
                    script_hash,
                    script.clone(),
                    TransparentOutPoint::new(txid, 0),
                    1,
                    block.height,
                    block.hash,
                ));
        }
        addresses.push(address.to_string());
        expected_per_address.push(expected);
    }

    let store = WalletServingStoreFixture::from_chain(&chain, &schedule)?;
    Ok(MultiBlockFixture {
        store,
        schedule,
        tip_hash,
        addresses,
        expected_per_address,
        node_entries,
    })
}

pub(crate) fn build_lean_adapter(
    store: &mut WalletServingStoreFixture,
    node_url: String,
    reads: ForwardedReadLimit,
) -> Result<(
    Arc<WalletServingReadPair>,
    LeanWalletQuery,
    LeanWalletAdapter,
)> {
    let (canonical, wallet) = store.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?);
    let source = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        node_url,
        NodeAuth::None,
        Duration::from_secs(10),
    )?;
    let query =
        WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::clone(&pair)))
            .with_node_broadcast(Some(OrderedBroadcaster::new(source, [])))
            .with_verified_cold_transactions(reads);
    let adapter = LightwalletdGrpcAdapter::from_admitted_compatibility_query(query.clone())?;
    Ok((pair, query, adapter))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_history_preserves_canonical_order_under_out_of_order_cold_replies_and_rejects_tampered_bytes()
-> Result<()> {
    const BLOCKS: u32 = 16;
    const TXS: u32 = 80;
    let mut fixture = build_multi_block_cold_fixture(BLOCKS, 1, TXS, |_, seq| {
        Duration::from_millis(u64::from((TXS - seq) % 7))
    })?;
    let node = AsyncZebraNode::start(
        &fixture.schedule,
        fixture.tip_hash,
        NodeBehavior {
            entries: fixture.node_entries.clone(),
            tampered_txid_hex: None,
            stalled_txids: HashSet::new(),
        },
    )
    .await?;
    let uncached_reads =
        ForwardedReadLimit::new(ForwardedReadConfig::resolve(None, None, None, Some(0))?);
    let (_pair, _query, adapter) =
        build_lean_adapter(&mut fixture.store, node.url(), uncached_reads)?;
    let filter = history_filter(fixture.addresses[0].clone(), 1, u64::from(BLOCKS));

    for deprecated in [true, false] {
        let mut stream = if deprecated {
            adapter
                .get_taddress_txids(Request::new(filter.clone()))
                .await?
                .into_inner()
        } else {
            adapter
                .get_taddress_transactions(Request::new(filter.clone()))
                .await?
                .into_inner()
        };
        let mut received = Vec::with_capacity(usize::try_from(TXS)?);
        while let Some(row) = stream.next().await {
            received.push(row?);
        }
        assert_eq!(received, fixture.expected_per_address[0]);
    }
    assert!(
        node.peak_raw_tx.load(Ordering::SeqCst) > 1 && node.peak_raw_tx.load(Ordering::SeqCst) <= 8,
        "expected bounded concurrent cold reads (2..=8), observed peak {}",
        node.peak_raw_tx.load(Ordering::SeqCst),
    );
    drop(node);

    assert_tampered_cold_row_fails_closed(BLOCKS, TXS).await
}

async fn assert_tampered_cold_row_fails_closed(blocks: u32, txs: u32) -> Result<()> {
    const TAMPERED_INDEX: u32 = 19;
    let mut fixture = build_multi_block_cold_fixture(blocks, 1, txs, |_, seq| {
        Duration::from_millis(u64::from((txs - seq) % 5))
    })?;
    let tampered_hex = encode_rpc_transaction_id_hex(sequence_txid(0x40, TAMPERED_INDEX));
    let node = AsyncZebraNode::start(
        &fixture.schedule,
        fixture.tip_hash,
        NodeBehavior {
            entries: fixture.node_entries,
            tampered_txid_hex: Some(tampered_hex),
            stalled_txids: HashSet::new(),
        },
    )
    .await?;
    let uncached_reads =
        ForwardedReadLimit::new(ForwardedReadConfig::resolve(None, None, None, Some(0))?);
    let (_pair, _query, adapter) =
        build_lean_adapter(&mut fixture.store, node.url(), uncached_reads)?;
    let filter = history_filter(fixture.addresses[0].clone(), 1, u64::from(blocks));

    for deprecated in [true, false] {
        let mut stream = if deprecated {
            adapter
                .get_taddress_txids(Request::new(filter.clone()))
                .await?
                .into_inner()
        } else {
            adapter
                .get_taddress_transactions(Request::new(filter.clone()))
                .await?
                .into_inner()
        };
        let mut prefix = Vec::new();
        let terminal_error = loop {
            match stream.next().await {
                Some(Ok(row)) => prefix.push(row),
                Some(Err(status)) => break status,
                None => return Err(eyre!("tampered stream completed without DATA_LOSS")),
            }
        };
        assert_eq!(terminal_error.code(), Code::DataLoss);
        assert_eq!(
            prefix.as_slice(),
            &fixture.expected_per_address[0][..usize::try_from(TAMPERED_INDEX)?],
        );
        assert!(
            stream.next().await.is_none(),
            "stream must end immediately after DATA_LOSS"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_concurrent_cold_history_streams_complete_without_refusals_and_preserve_transaction_floor()
-> Result<()> {
    const STREAMS: u8 = 4;
    const TXS_PER_STREAM: u32 = 120;
    const BLOCKS: u32 = 12;
    let mut fixture =
        build_multi_block_cold_fixture(BLOCKS, STREAMS, TXS_PER_STREAM, |addr, seq| {
            Duration::from_millis(1 + u64::from((u32::from(addr) + seq) % 2))
        })?;
    let node = AsyncZebraNode::start(
        &fixture.schedule,
        fixture.tip_hash,
        NodeBehavior {
            entries: fixture.node_entries,
            tampered_txid_hex: None,
            stalled_txids: HashSet::new(),
        },
    )
    .await?;
    let (_pair, _query, adapter) = build_lean_adapter(
        &mut fixture.store,
        node.url(),
        ForwardedReadLimit::default(),
    )?;

    let mut tasks = JoinSet::new();
    for (stream_idx, address) in fixture
        .addresses
        .iter()
        .enumerate()
        .take(usize::from(STREAMS))
    {
        let stream_adapter = adapter.clone();
        let filter = history_filter(address.clone(), 1, u64::from(BLOCKS));
        tasks.spawn(async move {
            let mut stream = stream_adapter
                .get_taddress_txids(Request::new(filter))
                .await?
                .into_inner();
            let mut rows = Vec::with_capacity(usize::try_from(TXS_PER_STREAM).unwrap_or(0));
            while let Some(row) = stream.next().await {
                rows.push(row?);
            }
            Ok::<_, tonic::Status>((stream_idx, rows))
        });
    }

    let point_txid = sequence_txid(0x40, TXS_PER_STREAM - 1);
    let point_reply = adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: None,
            index: 0,
            hash: encode_internal_transaction_id(point_txid).to_vec(),
        }))
        .await?
        .into_inner();
    assert_eq!(point_reply.data, sequence_payload(0x40, TXS_PER_STREAM - 1));

    while let Some(joined) = tasks.join_next().await {
        let (stream_idx, rows) = joined??;
        assert_eq!(rows, fixture.expected_per_address[stream_idx]);
    }
    let peak = node.peak_raw_tx.load(Ordering::SeqCst);
    assert!(
        (2..=17).contains(&peak),
        "expected History cap (16) plus 1 point lookup to bound upstream concurrency, observed peak {peak}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_client_stream_cancels_in_flight_cold_reads_and_frees_admission_and_pair()
-> Result<()> {
    const BLOCKS: u32 = 4;
    const TXS: u32 = 24;
    let mut fixture = build_multi_block_cold_fixture(BLOCKS, 2, TXS, |_, _| Duration::ZERO)?;
    let stalled_txids: HashSet<String> = (1..TXS)
        .map(|seq| encode_rpc_transaction_id_hex(sequence_txid(0x40, seq)))
        .collect();
    let node = AsyncZebraNode::start(
        &fixture.schedule,
        fixture.tip_hash,
        NodeBehavior {
            entries: fixture.node_entries,
            tampered_txid_hex: None,
            stalled_txids,
        },
    )
    .await?;
    let tight_limit = ForwardedReadLimit::new(ForwardedReadConfig::resolve(
        Some(4),
        Some(40),
        Some(8),
        Some(0),
    )?);
    let (pair, _query, adapter) = build_lean_adapter(&mut fixture.store, node.url(), tight_limit)?;
    let idle_owners = Arc::strong_count(&pair);

    let stalled_notified = node.gate_started.notified();
    let mut stream = adapter
        .get_taddress_txids(Request::new(history_filter(
            fixture.addresses[0].clone(),
            1,
            u64::from(BLOCKS),
        )))
        .await?
        .into_inner();
    let first = stream
        .next()
        .await
        .transpose()?
        .ok_or_else(|| eyre!("expected first row before gate"))?;
    assert_eq!(first, fixture.expected_per_address[0][0]);
    timeout(Duration::from_secs(5), stalled_notified).await?;
    assert!(Arc::strong_count(&pair) > idle_owners);

    assert_history_saturated(&adapter, fixture.addresses[1].clone(), u64::from(BLOCKS)).await?;

    drop(stream);
    wait_for_idle_pair(&pair, idle_owners, "dropped stream retained pair").await?;

    let mut recovered_stream = adapter
        .get_taddress_txids(Request::new(history_filter(
            fixture.addresses[1].clone(),
            1,
            u64::from(BLOCKS),
        )))
        .await?
        .into_inner();
    let recovered = recovered_stream
        .next()
        .await
        .transpose()?
        .ok_or_else(|| eyre!("expected row after slot release"))?;
    assert_eq!(recovered, fixture.expected_per_address[1][0]);
    drop(recovered_stream);
    node.release_stalled();
    timeout(Duration::from_secs(5), async {
        while node.active_raw_tx.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

async fn assert_history_saturated(
    adapter: &LeanWalletAdapter,
    address: String,
    end_height: u64,
) -> Result<()> {
    let mut refused_stream = adapter
        .get_taddress_txids(Request::new(history_filter(address, 1, end_height)))
        .await?
        .into_inner();
    let saturated = refused_stream
        .next()
        .await
        .ok_or_else(|| eyre!("expected refusal while slots are pinned"))?
        .err()
        .ok_or_else(|| eyre!("expected UNAVAILABLE while History cap is stalled"))?;
    assert_eq!(saturated.code(), Code::Unavailable);
    Ok(())
}

async fn wait_for_idle_pair(
    pair: &Arc<WalletServingReadPair>,
    idle_owners: usize,
    reason: &'static str,
) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        while Arc::strong_count(pair) > idle_owners {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| eyre!("{reason}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mid_stream_cold_failure_cancels_stalled_sibling_reads_and_releases_pair() -> Result<()> {
    const BLOCKS: u32 = 4;
    const TXS: u32 = 24;
    let mut fixture = build_multi_block_cold_fixture(BLOCKS, 1, TXS, |_, seq| {
        if seq == 1 {
            Duration::from_millis(25)
        } else {
            Duration::ZERO
        }
    })?;
    let tampered_hex = encode_rpc_transaction_id_hex(sequence_txid(0x40, 1));
    let stalled_txids: HashSet<String> = (2..TXS)
        .map(|seq| encode_rpc_transaction_id_hex(sequence_txid(0x40, seq)))
        .collect();
    let node = AsyncZebraNode::start(
        &fixture.schedule,
        fixture.tip_hash,
        NodeBehavior {
            entries: fixture.node_entries,
            tampered_txid_hex: Some(tampered_hex),
            stalled_txids,
        },
    )
    .await?;
    let (pair, _query, adapter) = build_lean_adapter(
        &mut fixture.store,
        node.url(),
        ForwardedReadLimit::default(),
    )?;
    let idle_owners = Arc::strong_count(&pair);

    let stalled_notified = node.gate_started.notified();
    let mut stream = adapter
        .get_taddress_txids(Request::new(history_filter(
            fixture.addresses[0].clone(),
            1,
            u64::from(BLOCKS),
        )))
        .await?
        .into_inner();
    let first = stream
        .next()
        .await
        .transpose()?
        .ok_or_else(|| eyre!("expected first row before stall"))?;
    assert_eq!(first, fixture.expected_per_address[0][0]);
    timeout(Duration::from_secs(5), stalled_notified).await?;
    assert!(Arc::strong_count(&pair) > idle_owners);

    let failure = timeout(Duration::from_secs(5), stream.next())
        .await?
        .ok_or_else(|| eyre!("expected terminal failure"))?
        .err()
        .ok_or_else(|| eyre!("expected tampered second row to fail"))?;
    assert_eq!(failure.code(), Code::DataLoss);
    assert!(stream.next().await.is_none());

    timeout(Duration::from_secs(5), async {
        while Arc::strong_count(&pair) > idle_owners {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| eyre!("terminal failure left stalled sibling cold reads pinning the pair"))?;
    node.release_stalled();
    Ok(())
}

#[tokio::test]
async fn default_transactions_in_order_fallback_starts_futures_on_demand() -> Result<()> {
    let mut fixture = build_multi_block_cold_fixture(4, 1, 24, |_, _| Duration::from_millis(5))?;
    let node = AsyncZebraNode::start(
        &fixture.schedule,
        fixture.tip_hash,
        NodeBehavior {
            entries: fixture.node_entries,
            tampered_txid_hex: None,
            stalled_txids: HashSet::new(),
        },
    )
    .await?;
    let uncached =
        ForwardedReadLimit::new(ForwardedReadConfig::resolve(None, None, None, Some(0))?);
    let (pair, query, _) = build_lean_adapter(&mut fixture.store, node.url(), uncached)?;
    let epoch_id = pair.chain_epoch().id;
    let ids: Vec<TransactionId> = (0..24).map(|seq| sequence_txid(0x40, seq)).collect();

    let mut fallback = zinder_query::TransactionsInOrder::empty();
    assert!(!fallback.has_remaining());
    assert!(fallback.next_batch(4).await.is_empty());

    let mut lazy = DefaultOrderedFallback::single(query)
        .transactions_in_order(ids, Some(epoch_id))
        .await?;
    assert_eq!(node.peak_raw_tx.load(Ordering::SeqCst), 0);
    let first_batch = lazy.next_batch(3).await;
    assert_eq!(first_batch.len(), 3);
    for pending in first_batch {
        let resolved = pending.await?;
        assert!(matches!(resolved.status, zinder_core::TxStatus::Mined(_)));
    }
    assert!(node.peak_raw_tx.load(Ordering::SeqCst) <= 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn low_budget_zero_wait_cold_history_stream_completes_without_self_refusal() -> Result<()> {
    const BLOCKS: u32 = 4;
    const TXS: u32 = 24;
    let mut fixture =
        build_multi_block_cold_fixture(BLOCKS, 1, TXS, |_, _| Duration::from_millis(2))?;
    let node = AsyncZebraNode::start(
        &fixture.schedule,
        fixture.tip_hash,
        NodeBehavior {
            entries: fixture.node_entries,
            tampered_txid_hex: None,
            stalled_txids: HashSet::new(),
        },
    )
    .await?;
    let low_budget = ForwardedReadLimit::new(ForwardedReadConfig::resolve(
        Some(4),
        Some(0),
        Some(8),
        Some(0),
    )?);
    let (_pair, _query, adapter) = build_lean_adapter(&mut fixture.store, node.url(), low_budget)?;
    let filter = history_filter(fixture.addresses[0].clone(), 1, u64::from(BLOCKS));

    for deprecated in [true, false] {
        let mut stream = if deprecated {
            adapter
                .get_taddress_txids(Request::new(filter.clone()))
                .await?
                .into_inner()
        } else {
            adapter
                .get_taddress_transactions(Request::new(filter.clone()))
                .await?
                .into_inner()
        };
        let mut received = Vec::with_capacity(usize::try_from(TXS)?);
        while let Some(row) = stream.next().await {
            received.push(row?);
        }
        assert_eq!(received, fixture.expected_per_address[0]);
    }
    assert!(
        node.peak_raw_tx.load(Ordering::SeqCst) <= 2,
        "expected History cap (2 at budget=4) to bound in-flight cold fetches, observed {}",
        node.peak_raw_tx.load(Ordering::SeqCst),
    );
    Ok(())
}

#[tokio::test]
async fn default_transactions_in_order_without_explicit_pin_locks_one_epoch_across_replacement()
-> Result<()> {
    let mut fixture_one = build_multi_block_cold_fixture(2, 1, 6, |_, _| Duration::ZERO)?;
    let (pair_two, query_two, _store_two) = build_live_appended_second_epoch_query()?;
    let node_one = AsyncZebraNode::start(
        &fixture_one.schedule,
        fixture_one.tip_hash,
        NodeBehavior {
            entries: fixture_one.node_entries,
            tampered_txid_hex: None,
            stalled_txids: HashSet::new(),
        },
    )
    .await?;
    let (pair_one, query_one, _) = build_lean_adapter(
        &mut fixture_one.store,
        node_one.url(),
        ForwardedReadLimit::default(),
    )?;
    let epoch_one = pair_one.chain_epoch().id;
    let epoch_two = pair_two.chain_epoch().id;
    assert_ne!(epoch_one, epoch_two);

    let wrapper = DefaultOrderedFallback::single(query_one.clone());
    let ids: Vec<TransactionId> = (0..6).map(|seq| sequence_txid(0x40, seq)).collect();
    let mut reader = wrapper.transactions_in_order(ids, None).await?;

    for pending in reader.next_batch(2).await {
        assert_eq!(pending.await?.chain_epoch.id, epoch_one);
    }

    wrapper.replace_retaining_previous(epoch_one, query_two);
    for pending in reader.next_batch(2).await {
        assert_eq!(
            pending.await?.chain_epoch.id,
            epoch_one,
            "reader opened with None must stay pinned to its initial epoch across replacement"
        );
    }

    wrapper.evict_retained();
    let evicted = reader
        .next_batch(2)
        .await
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("expected third batch"))?
        .await;
    assert!(matches!(
        evicted,
        Err(zinder_query::QueryError::ChainEpochPinUnavailable { chain_epoch_id })
            if chain_epoch_id == epoch_one
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gated_second_canonical_batch_does_not_stall_in_flight_delivery_or_cancellation()
-> Result<()> {
    assert_gated_second_batch_delivers_earlier_rows().await?;
    assert_gated_second_batch_releases_slots_on_drop().await
}

async fn assert_gated_second_batch_delivers_earlier_rows() -> Result<()> {
    const BLOCKS: u32 = 4;
    const TXS: u32 = 24;
    let mut fixture =
        build_multi_block_cold_fixture(BLOCKS, 1, TXS, |_, _| Duration::from_millis(2))?;
    let node = AsyncZebraNode::start(
        &fixture.schedule,
        fixture.tip_hash,
        NodeBehavior {
            entries: fixture.node_entries,
            tampered_txid_hex: None,
            stalled_txids: HashSet::new(),
        },
    )
    .await?;
    let uncached =
        ForwardedReadLimit::new(ForwardedReadConfig::resolve(None, None, None, Some(0))?);
    let gate = gated_canonical::BatchGate::holding_call(1);
    let (_pair, adapter) =
        build_gated_lean_adapter(&mut fixture.store, node.url(), uncached, gate.clone())?;

    let second_batch_started = gate.started();
    let mut stream = adapter
        .get_taddress_txids(Request::new(history_filter(
            fixture.addresses[0].clone(),
            1,
            u64::from(BLOCKS),
        )))
        .await?
        .into_inner();
    let mut received = Vec::with_capacity(usize::try_from(TXS)?);
    for _ in 0..16 {
        let row = timeout(Duration::from_secs(5), stream.next())
            .await?
            .ok_or_else(|| eyre!("expected first-batch row while second batch is gated"))??;
        received.push(row);
    }
    timeout(Duration::from_secs(5), second_batch_started).await?;
    assert_eq!(received.as_slice(), &fixture.expected_per_address[0][..16]);

    gate.release();
    while let Some(row) = timeout(Duration::from_secs(5), stream.next()).await? {
        received.push(row?);
    }
    assert_eq!(received, fixture.expected_per_address[0]);
    Ok(())
}

async fn assert_gated_second_batch_releases_slots_on_drop() -> Result<()> {
    const BLOCKS: u32 = 4;
    const TXS: u32 = 24;
    let mut fixture = build_multi_block_cold_fixture(BLOCKS, 2, TXS, |_, _| Duration::ZERO)?;
    let stalled_txids: HashSet<String> = (14..16)
        .map(|seq| encode_rpc_transaction_id_hex(sequence_txid(0x40, seq)))
        .collect();
    let node = AsyncZebraNode::start(
        &fixture.schedule,
        fixture.tip_hash,
        NodeBehavior {
            entries: fixture.node_entries,
            tampered_txid_hex: None,
            stalled_txids,
        },
    )
    .await?;
    let tight_limit = ForwardedReadLimit::new(ForwardedReadConfig::resolve(
        Some(6),
        Some(40),
        Some(8),
        Some(0),
    )?);
    let gate = gated_canonical::BatchGate::holding_call(1);
    let (pair, adapter) = build_gated_lean_adapter(
        &mut fixture.store,
        node.url(),
        tight_limit.clone(),
        gate.clone(),
    )?;
    let idle_owners = Arc::strong_count(&pair);

    let second_batch_started = gate.started();
    let node_stalled = node.gate_started.notified();
    let mut stream = adapter
        .get_taddress_txids(Request::new(history_filter(
            fixture.addresses[0].clone(),
            1,
            u64::from(BLOCKS),
        )))
        .await?
        .into_inner();
    for _ in 0..14 {
        let _ = timeout(Duration::from_secs(5), stream.next())
            .await?
            .ok_or_else(|| eyre!("expected unstalled prefix row"))??;
    }
    timeout(Duration::from_secs(5), second_batch_started).await?;
    timeout(Duration::from_secs(5), node_stalled).await?;
    assert!(Arc::strong_count(&pair) > idle_owners);

    drop(stream);
    wait_for_idle_pair(
        &pair,
        idle_owners,
        "gated refill or stalled cold reads retained pair",
    )
    .await?;

    let mut second_stream = adapter
        .get_taddress_txids(Request::new(history_filter(
            fixture.addresses[1].clone(),
            1,
            u64::from(BLOCKS),
        )))
        .await?
        .into_inner();
    let recovered = timeout(Duration::from_secs(5), second_stream.next())
        .await?
        .transpose()?
        .ok_or_else(|| eyre!("expected second address row after slot release"))?;
    assert_eq!(recovered, fixture.expected_per_address[1][0]);
    drop(second_stream);
    gate.release();
    node.release_stalled();
    Ok(())
}

fn build_gated_lean_adapter(
    store: &mut WalletServingStoreFixture,
    node_url: String,
    reads: ForwardedReadLimit,
    gate: Arc<gated_canonical::BatchGate>,
) -> Result<(Arc<WalletServingReadPair>, LeanWalletAdapter)> {
    let (canonical, wallet) = store.take_readers()?;
    let gated = gated_canonical::GatedBatchCanonicalReader::new(
        RocksDbCanonicalReader::new(canonical)?,
        gate,
    );
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(gated),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?);
    let source = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        node_url,
        NodeAuth::None,
        Duration::from_secs(10),
    )?;
    let query =
        WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::clone(&pair)))
            .with_node_broadcast(Some(OrderedBroadcaster::new(source, [])))
            .with_verified_cold_transactions(reads);
    Ok((
        pair,
        LightwalletdGrpcAdapter::from_admitted_compatibility_query(query)?,
    ))
}

fn build_live_appended_second_epoch_query() -> Result<(
    Arc<WalletServingReadPair>,
    LeanWalletQuery,
    WalletServingStoreFixture,
)> {
    let schedule = one_upgrade_per_height()?;
    let mut chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(2);
    let first_block = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("missing block 1"))?
        .clone();
    for seq in 0..6_u32 {
        chain = chain.with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            sequence_txid(0x40, seq),
            first_block.height,
            first_block.hash,
            seq,
            sequence_payload(0x40, seq),
        ));
    }
    let mut store = WalletServingStoreFixture::from_chain_after_live_append(&chain, &schedule)?;
    let (canonical, wallet) = store.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?);
    let query =
        WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::clone(&pair)))
            .with_node_broadcast(None);
    Ok((pair, query, store))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_get_transaction_succeeds_for_tx_queued_behind_saturated_history_slots() -> Result<()>
{
    const BLOCKS: u32 = 4;
    const TXS: u32 = 12;
    let mut fixture = build_multi_block_cold_fixture(BLOCKS, 2, TXS, |_, _| Duration::ZERO)?;
    let stalled_txids: HashSet<String> = (1..TXS)
        .map(|seq| encode_rpc_transaction_id_hex(sequence_txid(0x40, seq)))
        .collect();
    let node = AsyncZebraNode::start(
        &fixture.schedule,
        fixture.tip_hash,
        NodeBehavior {
            entries: fixture.node_entries,
            tampered_txid_hex: None,
            stalled_txids,
        },
    )
    .await?;
    let limit = ForwardedReadLimit::new(ForwardedReadConfig::resolve(
        Some(4),
        Some(250),
        Some(8),
        None,
    )?);
    let (_pair, _query, adapter) = build_lean_adapter(&mut fixture.store, node.url(), limit)?;

    let stalled_notified = node.gate_started.notified();
    let mut stalled_stream = adapter
        .get_taddress_txids(Request::new(history_filter(
            fixture.addresses[0].clone(),
            1,
            u64::from(BLOCKS),
        )))
        .await?
        .into_inner();
    let first = stalled_stream
        .next()
        .await
        .transpose()?
        .ok_or_else(|| eyre!("expected first row"))?;
    assert_eq!(first, fixture.expected_per_address[0][0]);
    timeout(Duration::from_secs(5), stalled_notified).await?;

    let mut queued_stream = adapter
        .get_taddress_txids(Request::new(history_filter(
            fixture.addresses[1].clone(),
            1,
            u64::from(BLOCKS),
        )))
        .await?
        .into_inner();
    tokio::time::sleep(Duration::from_millis(15)).await;

    let target_txid = sequence_txid(0x41, 0);
    let point_reply = timeout(
        Duration::from_secs(2),
        adapter.get_transaction(Request::new(lightwalletd::TxFilter {
            block: None,
            index: 0,
            hash: encode_internal_transaction_id(target_txid).to_vec(),
        })),
    )
    .await??
    .into_inner();
    assert_eq!(point_reply.data, sequence_payload(0x41, 0));

    let warmed_history_row = timeout(Duration::from_secs(2), queued_stream.next())
        .await?
        .transpose()?
        .ok_or_else(|| eyre!("expected queued history row warmed by point lookup"))?;
    assert_eq!(warmed_history_row, fixture.expected_per_address[1][0]);

    drop((queued_stream, stalled_stream));
    node.release_stalled();
    Ok(())
}

#[derive(Clone)]
struct DefaultOrderedFallback {
    capabilities: zinder_query::NativeWalletEndpointCapabilities,
    inner: Arc<parking_lot::Mutex<FallbackQueries>>,
}

struct FallbackQueries {
    current: LeanWalletQuery,
    retained: Option<(zinder_core::ChainEpochId, LeanWalletQuery)>,
}

impl DefaultOrderedFallback {
    fn single(current: LeanWalletQuery) -> Self {
        let capabilities = current.native_endpoint_capabilities().clone();
        Self {
            capabilities,
            inner: Arc::new(parking_lot::Mutex::new(FallbackQueries {
                current,
                retained: None,
            })),
        }
    }

    fn replace_retaining_previous(
        &self,
        previous_epoch: zinder_core::ChainEpochId,
        next_current: LeanWalletQuery,
    ) {
        let mut guard = self.inner.lock();
        let previous = std::mem::replace(&mut guard.current, next_current);
        guard.retained = Some((previous_epoch, previous));
    }

    fn evict_retained(&self) {
        self.inner.lock().retained = None;
    }

    fn select_for(&self, ep: Option<zinder_core::ChainEpochId>) -> LeanWalletQuery {
        let guard = self.inner.lock();
        if let (Some(pinned), Some((retained_epoch, retained_query))) = (ep, &guard.retained)
            && pinned == *retained_epoch
        {
            return retained_query.clone();
        }
        guard.current.clone()
    }
}

#[async_trait::async_trait]
impl zinder_query::WalletQueryApi for DefaultOrderedFallback {
    fn native_endpoint_capabilities(&self) -> &zinder_query::NativeWalletEndpointCapabilities {
        &self.capabilities
    }
    fn upstream_node_capabilities(&self) -> Option<zinder_query::UpstreamNodeCapabilities> {
        self.inner.lock().current.upstream_node_capabilities()
    }
    async fn network_upgrade_activations(
        &self,
    ) -> Result<NetworkUpgradeActivations, zinder_query::QueryError> {
        self.select_for(None).network_upgrade_activations().await
    }
    async fn visible_tip_block(
        &self,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::VisibleTipBlock, zinder_query::QueryError> {
        self.select_for(ep).visible_tip_block(ep).await
    }
    async fn settled_tip_block(
        &self,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::SettledTipBlock, zinder_query::QueryError> {
        self.select_for(ep).settled_tip_block(ep).await
    }
    async fn block_id_by_selector(
        &self,
        sel: zinder_core::BlockSelector,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::BlockIdAtEpoch, zinder_query::QueryError> {
        self.select_for(ep).block_id_by_selector(sel, ep).await
    }
    async fn block_header_by_selector(
        &self,
        sel: zinder_core::BlockSelector,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::BlockHeaderAtEpoch, zinder_query::QueryError> {
        self.select_for(ep).block_header_by_selector(sel, ep).await
    }
    async fn compact_block_at(
        &self,
        h: BlockHeight,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::CompactBlock, zinder_query::QueryError> {
        self.select_for(ep).compact_block_at(h, ep).await
    }
    async fn compact_blocks_in_range(
        &self,
        r: zinder_core::BlockHeightRange,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::CompactBlockRange, zinder_query::QueryError> {
        self.select_for(ep).compact_blocks_in_range(r, ep).await
    }
    async fn compact_blocks_in_range_up_to(
        &self,
        r: zinder_core::BlockHeightRange,
        ord: zinder_store::HeightOrder,
        max: usize,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::CompactBlockRange, zinder_query::QueryError> {
        self.select_for(ep)
            .compact_blocks_in_range_up_to(r, ord, max, ep)
            .await
    }
    async fn full_block_at(
        &self,
        h: BlockHeight,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::FullBlock, zinder_query::QueryError> {
        self.select_for(ep).full_block_at(h, ep).await
    }
    async fn full_blocks_in_range(
        &self,
        r: zinder_core::BlockHeightRange,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::FullBlockStream, zinder_query::QueryError> {
        self.select_for(ep).full_blocks_in_range(r, ep).await
    }
    async fn transaction(
        &self,
        id: TransactionId,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::TransactionStatus, zinder_query::QueryError> {
        self.select_for(ep).transaction(id, ep).await
    }
    async fn transaction_at_block_index(
        &self,
        h: BlockHeight,
        idx: u64,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::Transaction, zinder_query::QueryError> {
        self.select_for(ep)
            .transaction_at_block_index(h, idx, ep)
            .await
    }
    async fn raw_transaction(
        &self,
        id: TransactionId,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::RawTransaction, zinder_query::QueryError> {
        self.select_for(ep).raw_transaction(id, ep).await
    }
    async fn transparent_address_unspent_outputs(
        &self,
        req: zinder_query::TransparentAddressUnspentOutputsRequest,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::TransparentAddressUnspentOutputs, zinder_query::QueryError> {
        self.select_for(ep)
            .transparent_address_unspent_outputs(req, ep)
            .await
    }
    async fn transparent_address_tx_ids_in_range(
        &self,
        req: zinder_query::TransparentAddressTxIdsInRangeRequest,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::TransparentAddressTxIds, zinder_query::QueryError> {
        self.select_for(ep)
            .transparent_address_tx_ids_in_range(req, ep)
            .await
    }
    async fn transparent_outpoint_spend_status(
        &self,
        ops: Vec<TransparentOutPoint>,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::TransparentOutpointSpendStatuses, zinder_query::QueryError> {
        self.select_for(ep)
            .transparent_outpoint_spend_status(ops, ep)
            .await
    }
    async fn transparent_address_balance(
        &self,
        addrs: Vec<TransparentAddressScriptHash>,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_core::TransparentAddressBalance, zinder_query::QueryError> {
        self.select_for(ep)
            .transparent_address_balance(addrs, ep)
            .await
    }
    async fn tree_state_at(
        &self,
        h: BlockHeight,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::TreeState, zinder_query::QueryError> {
        self.select_for(ep).tree_state_at(h, ep).await
    }
    async fn latest_tree_state_checkpoint(
        &self,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::TreeState, zinder_query::QueryError> {
        self.select_for(ep).latest_tree_state_checkpoint(ep).await
    }
    async fn subtree_roots(
        &self,
        r: zinder_core::SubtreeRootRange,
        ep: Option<zinder_core::ChainEpochId>,
    ) -> Result<zinder_query::SubtreeRoots, zinder_query::QueryError> {
        self.select_for(ep).subtree_roots(r, ep).await
    }
    async fn chain_events(
        &self,
        cur: Option<zinder_store::StreamCursorTokenV1>,
        fam: zinder_store::ChainEventStreamFamily,
    ) -> Result<zinder_query::ChainEvents, zinder_query::QueryError> {
        self.select_for(None).chain_events(cur, fam).await
    }
    async fn resolve_chain_events_start(
        &self,
        st: zinder_store::EventStreamStartPosition,
        fam: zinder_store::ChainEventStreamFamily,
    ) -> Result<zinder_store::ChainEventStreamResume, zinder_query::QueryError> {
        self.select_for(None)
            .resolve_chain_events_start(st, fam)
            .await
    }
    async fn broadcast_transaction(
        &self,
        raw: zinder_core::RawTransactionBytes,
    ) -> Result<zinder_core::TransactionBroadcastOutcome, zinder_query::QueryError> {
        self.select_for(None).broadcast_transaction(raw).await
    }
    async fn standard_fee(&self) -> Result<zinder_core::StandardFee, zinder_query::QueryError> {
        self.select_for(None).standard_fee().await
    }
}
