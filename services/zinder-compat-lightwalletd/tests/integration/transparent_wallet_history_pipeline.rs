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

type LeanWalletQuery = WalletServingQuery<Option<OrderedBroadcaster<ZebraJsonRpcSource>>>;
type LeanWalletAdapter = LightwalletdGrpcAdapter<LeanWalletQuery>;

#[derive(Clone)]
struct StoredTxEntry {
    height: BlockHeight,
    block_hash: BlockHash,
    raw_hex: String,
    delay: Duration,
}

struct AsyncZebraNode {
    address: SocketAddr,
    active_raw_tx: Arc<AtomicUsize>,
    peak_raw_tx: Arc<AtomicUsize>,
    gate_started: Arc<Notify>,
    gate_release: Arc<Semaphore>,
    timer: Arc<PreciseTimer>,
    shutdown: tokio_util::sync::CancellationToken,
}

struct NodeBehavior {
    entries: HashMap<String, StoredTxEntry>,
    tampered_txid_hex: Option<String>,
    stalled_txids: HashSet<String>,
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
    async fn start(
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

    fn url(&self) -> String {
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

fn sequence_txid(address_seed: u8, sequence: u32) -> TransactionId {
    let mut bytes = [address_seed; 32];
    bytes[28..].copy_from_slice(&sequence.to_be_bytes());
    TransactionId::from_bytes(bytes)
}

fn sequence_payload(address_seed: u8, sequence: u32) -> Vec<u8> {
    format!("cold-history-addr-{address_seed:02x}-seq-{sequence:05}").into_bytes()
}

fn history_filter(
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

struct MultiBlockFixture {
    store: WalletServingStoreFixture,
    schedule: NetworkUpgradeActivations,
    tip_hash: BlockHash,
    addresses: Vec<String>,
    expected_per_address: Vec<Vec<lightwalletd::RawTransaction>>,
    node_entries: HashMap<String, StoredTxEntry>,
}

fn build_multi_block_cold_fixture(
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

fn build_lean_adapter(
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_address_cold_history_pipeline_beats_serial_round_trips() -> Result<()> {
    const BLOCKS: u32 = 17;
    const BUSY_TXS: u32 = 340;
    const RPC_LATENCY: Duration = Duration::from_millis(1);
    let mut fixture = build_multi_block_cold_fixture(BLOCKS, 1, BUSY_TXS, |_, _| RPC_LATENCY)?;
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
    let uncached_reads =
        ForwardedReadLimit::new(ForwardedReadConfig::resolve(None, None, None, Some(0))?);
    let (pair, query, adapter) =
        build_lean_adapter(&mut fixture.store, node.url(), uncached_reads)?;
    let epoch_id = pair.chain_epoch().id;

    // Warm node schedule/capability verification and HTTP keep-alive connection pool.
    let warm_filter = history_filter(fixture.addresses[0].clone(), 1, 1);
    let mut warm_stream = adapter
        .get_taddress_txids(Request::new(warm_filter))
        .await?
        .into_inner();
    while warm_stream.next().await.transpose()?.is_some() {}

    let serial_started = Instant::now();
    for seq in 0..BUSY_TXS {
        let status = query
            .transaction(sequence_txid(0x40, seq), Some(epoch_id))
            .await?;
        assert!(matches!(status.status, zinder_core::TxStatus::Mined(_)));
    }
    let serial_elapsed = serial_started.elapsed();

    let pipelined_started = Instant::now();
    let mut stream = adapter
        .get_taddress_txids(Request::new(history_filter(
            fixture.addresses[0].clone(),
            1,
            u64::from(BLOCKS),
        )))
        .await?
        .into_inner();
    let mut received = Vec::with_capacity(usize::try_from(BUSY_TXS)?);
    while let Some(row) = stream.next().await {
        received.push(row?);
    }
    let pipelined_elapsed = pipelined_started.elapsed();
    record_bench_report(
        BUSY_TXS,
        serial_elapsed,
        pipelined_elapsed,
        node.peak_raw_tx.load(Ordering::SeqCst),
    )?;

    assert_eq!(received, fixture.expected_per_address[0]);
    assert!(
        serial_elapsed >= Duration::from_millis(340),
        "expected 340 serial 1ms RPCs to take >= 340ms, took {serial_elapsed:?}",
    );
    let max_pipelined = if cfg!(debug_assertions) {
        Duration::from_millis(200)
    } else {
        Duration::from_millis(65)
    };
    assert!(
        pipelined_elapsed < max_pipelined && pipelined_elapsed * 4 < serial_elapsed,
        "expected pipelined 340 cold txs (< {max_pipelined:?} and > 4x faster than serial {serial_elapsed:?}), took {pipelined_elapsed:?}",
    );
    Ok(())
}

fn record_bench_report(
    busy_txs: u32,
    serial_elapsed: Duration,
    pipelined_elapsed: Duration,
    peak_in_flight: usize,
) -> Result<()> {
    tracing::info!(
        busy_txs,
        ?serial_elapsed,
        ?pipelined_elapsed,
        peak_in_flight,
        "busy-address cold transparent history benchmark completed"
    );
    if let Ok(report_path) = std::env::var("ZINDER_TEST_BENCH_REPORT_PATH") {
        std::fs::write(
            report_path,
            format!(
                "busy_txs={busy_txs}\nserial_ms={:.3}\npipelined_ms={:.3}\nspeedup_x={:.2}\npeak_in_flight={peak_in_flight}\n",
                serial_elapsed.as_secs_f64() * 1_000.0,
                pipelined_elapsed.as_secs_f64() * 1_000.0,
                serial_elapsed.as_secs_f64() / pipelined_elapsed.as_secs_f64(),
            ),
        )?;
    }
    Ok(())
}
