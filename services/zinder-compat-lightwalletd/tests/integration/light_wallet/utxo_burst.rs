use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use eyre::{Result, eyre};
use parking_lot::{Condvar, Mutex};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Barrier, task::JoinSet};
use tokio_stream::StreamExt as _;
use tonic::{Request, Status, transport::Channel};
use zinder_core::{BlockId, Network, TransactionId, wire::encode_rpc_transaction_id_hex};
use zinder_proto::compat::lightwalletd::{
    GetAddressUtxosArg, compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::CompactTxStreamer as _,
};
use zinder_query::{ForwardedReadConfig, ForwardedReadLimit, ForwardedResponseKind};
use zinder_runtime::{
    ClientAddressSource, DEFAULT_GRPC_RECEIVE_TIMEOUT, DEFAULT_GRPC_SEND_STALL_TIMEOUT,
    GrpcReceiveLimits, public_grpc_server_with,
};
use zinder_testkit::{ChainFixture, RpcReply, method};

use super::{
    ADDRESS, Fixture, balance_request, one_upgrade_per_height, populated_utxo_fixture_with,
    refused_for_admission, served_tip, utxo_request, utxos_at,
};

const SIMULATED_WALLETS: usize = 256;
const STREAMS_PER_WALLET: usize = 2;
const CONCURRENT_UTXO_CALLS: usize = SIMULATED_WALLETS * STREAMS_PER_WALLET;
const CLIENT_CHANNELS: usize = 32;
const DEFAULT_UTXO_NODE_SLOT_CAP: usize = 8;

struct ActiveRpcCounter {
    active: AtomicUsize,
    peak: AtomicUsize,
    total: AtomicUsize,
}

impl ActiveRpcCounter {
    const fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
        }
    }

    fn enter(self: &Arc<Self>) -> ActiveRpcGuard {
        let now_active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now_active, Ordering::SeqCst);
        self.total.fetch_add(1, Ordering::SeqCst);
        ActiveRpcGuard(Arc::clone(self))
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
}

struct ActiveRpcGuard(Arc<ActiveRpcCounter>);

impl Drop for ActiveRpcGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

fn address_for_call(call_index: usize) -> String {
    let mut hash = [0_u8; 20];
    let index_bytes = u64::try_from(call_index).unwrap_or(0).to_le_bytes();
    hash[..8].copy_from_slice(&index_bytes);
    zebra_chain::transparent::Address::from_pub_key_hash(
        zebra_chain::parameters::NetworkKind::Testnet,
        hash,
    )
    .to_string()
}

fn simulated_zebra_utxo_reply(params: &Value, tip: BlockId, call_number: usize) -> RpcReply {
    let requested_addr = params[0]["addresses"][0]
        .as_str()
        .unwrap_or(ADDRESS)
        .to_owned();
    let parsed_script = zinder_source::parse_transparent_address(&requested_addr)
        .map_or_default(|parsed| hex::encode(parsed.script().as_raw_bytes()));
    let delay = Duration::from_micros(if call_number.is_multiple_of(2) {
        500
    } else {
        1_000
    });
    std::thread::sleep(delay);

    let output_count = if requested_addr.ends_with('a')
        || requested_addr.ends_with('b')
        || requested_addr.ends_with('c')
        || requested_addr.ends_with('d')
    {
        3_u32
    } else {
        0_u32
    };
    let mut rows = Vec::with_capacity(usize::try_from(output_count).unwrap_or(0));
    for output_idx in (0..output_count).rev() {
        let mut raw_id = [0_u8; 32];
        raw_id[0] = u8::try_from(output_idx + 1).unwrap_or(1);
        let txid = encode_rpc_transaction_id_hex(TransactionId::from_bytes(raw_id));
        rows.push(json!({
            "address": requested_addr,
            "txid": txid,
            "outputIndex": output_idx,
            "script": parsed_script,
            "satoshis": u64::from(output_idx + 1) * 1_000,
            "height": 1 + (2 - output_idx) / 2,
        }));
    }
    utxos_at(tip, &Value::Array(rows))
}

fn burst_fixture() -> Result<(Fixture, Arc<ActiveRpcCounter>)> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
    let tip = chain
        .blocks()
        .last()
        .map(|block| BlockId::new(block.height, block.hash))
        .ok_or_else(|| eyre!("missing fixture tip"))?;
    let counter = Arc::new(ActiveRpcCounter::new());
    let rpc_counter = Arc::clone(&counter);
    let fixture = Fixture::from_chain(
        &chain,
        &one_upgrade_per_height()?,
        [method("getaddressutxos").reply_with(move |params| {
            let _guard = rpc_counter.enter();
            let seq = rpc_counter.total.load(Ordering::SeqCst);
            simulated_zebra_utxo_reply(params, tip, seq)
        })],
        Duration::from_secs(10),
    )?;
    Ok((fixture, counter))
}

async fn connect_channels(
    endpoint: &str,
    count: usize,
) -> Result<Vec<CompactTxStreamerClient<Channel>>> {
    let mut clients = Vec::with_capacity(count);
    for _ in 0..count {
        clients.push(CompactTxStreamerClient::connect(endpoint.to_owned()).await?);
    }
    Ok(clients)
}

async fn run_vizor_transport_burst(clients: &[CompactTxStreamerClient<Channel>]) -> Result<usize> {
    let barrier = Arc::new(Barrier::new(SIMULATED_WALLETS));
    let mut tasks = JoinSet::new();
    for wallet_index in 0..SIMULATED_WALLETS {
        let client_template = clients[wallet_index % clients.len()].clone();
        let start_gate = Arc::clone(&barrier);
        tasks.spawn(async move {
            start_gate.wait().await;
            let first_call = wallet_index * STREAMS_PER_WALLET;
            let second_call = first_call + 1;
            let pair: [usize; 2] = tokio::try_join!(
                drain_one_utxo_stream(client_template.clone(), address_for_call(first_call)),
                drain_one_utxo_stream(client_template, address_for_call(second_call)),
            )?
            .into();
            Ok::<[usize; 2], Status>(pair)
        });
    }
    let mut non_empty = 0_usize;
    while let Some(joined) = tasks.join_next().await {
        let pair = joined?.map_err(|status| eyre!("UTXO stream burst failed: {status:?}"))?;
        for received in pair {
            if received > 0 {
                assert_eq!(received, 3);
                non_empty += 1;
            }
        }
    }
    Ok(non_empty)
}

async fn drain_one_utxo_stream(
    mut client: CompactTxStreamerClient<Channel>,
    addr: String,
) -> std::result::Result<usize, Status> {
    let mut stream = client
        .get_address_utxos_stream(GetAddressUtxosArg {
            addresses: vec![addr.clone()],
            start_height: 1,
            max_entries: 0,
        })
        .await?
        .into_inner();
    let expected = [(1, Some(3), 2), (1, Some(2), 1), (2, Some(1), 0)];
    let mut count = 0;
    while let Some(reply) = stream.message().await? {
        assert_eq!(
            expected.get(count).copied(),
            Some((reply.height, reply.txid.first().copied(), reply.index)),
        );
        assert_eq!(reply.address, addr);
        count += 1;
    }
    Ok(count)
}

async fn run_transport_unary_and_balance_burst(
    clients: &[CompactTxStreamerClient<Channel>],
) -> Result<()> {
    let barrier = Arc::new(Barrier::new(CONCURRENT_UTXO_CALLS));
    let mut tasks = JoinSet::new();
    for call_index in 0..CONCURRENT_UTXO_CALLS {
        let mut client = clients[call_index % clients.len()].clone();
        let start_gate = Arc::clone(&barrier);
        let addr = address_for_call(call_index);
        tasks.spawn(async move {
            start_gate.wait().await;
            if call_index.is_multiple_of(2) {
                let reply = client
                    .get_address_utxos(GetAddressUtxosArg {
                        addresses: vec![addr],
                        start_height: 1,
                        max_entries: 0,
                    })
                    .await?;
                Ok::<usize, Status>(reply.into_inner().address_utxos.len())
            } else {
                let reply = client
                    .get_taddress_balance(zinder_proto::compat::lightwalletd::AddressList {
                        addresses: vec![addr],
                    })
                    .await?;
                let zat = usize::try_from(reply.into_inner().value_zat).unwrap_or(0);
                Ok(zat)
            }
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined?.map_err(|status| eyre!("unary/balance burst failed: {status:?}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vizor_burst_of_512_utxo_streams_completes_without_unavailable_and_caps_node_concurrency()
-> Result<()> {
    let (fixture, counter) = burst_fixture()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    // Widen per-IP gRPC receive slots from 32 to 512 so 256 barrier-synchronized
    // loopback tasks test ForwardedReadLimit admission rather than per-IP throttling.
    let receive_slots =
        NonZeroUsize::new(CONCURRENT_UTXO_CALLS).ok_or_else(|| eyre!("zero burst size"))?;
    let server = public_grpc_server_with(
        GrpcReceiveLimits::new(receive_slots, DEFAULT_GRPC_RECEIVE_TIMEOUT),
        DEFAULT_GRPC_SEND_STALL_TIMEOUT,
        ClientAddressSource::Connection,
    )
    .add_service(fixture.adapter.into_server())
    .serve_with_incoming(listener);

    let check = async {
        let clients = connect_channels(&endpoint, CLIENT_CHANNELS).await?;
        let started = Instant::now();
        let non_empty = run_vizor_transport_burst(&clients).await?;
        let transport_elapsed = started.elapsed();
        assert!(non_empty > 0, "expected mixed empty and non-empty replies");
        assert!(
            counter.peak() <= DEFAULT_UTXO_NODE_SLOT_CAP,
            "peak active Zebra UTXO RPCs {} exceeded slot cap {DEFAULT_UTXO_NODE_SLOT_CAP}",
            counter.peak()
        );
        assert!(counter.peak() >= 2, "expected concurrent Zebra UTXO RPCs");
        assert_eq!(counter.active(), 0);
        assert!(
            transport_elapsed < Duration::from_secs(5),
            "256-wallet transport burst took too long: {transport_elapsed:?}"
        );

        run_transport_unary_and_balance_burst(&clients).await?;
        assert!(
            counter.peak() <= DEFAULT_UTXO_NODE_SLOT_CAP,
            "peak active Zebra UTXO RPCs {} exceeded slot cap {DEFAULT_UTXO_NODE_SLOT_CAP}",
            counter.peak()
        );
        assert_eq!(counter.active(), 0);
        Ok::<(), eyre::Report>(())
    };

    tokio::select! {
        stopped = server => Err(eyre!("gRPC server stopped during UTXO burst: {stopped:?}")),
        outcome = tokio::time::timeout(Duration::from_secs(30), check) => outcome?,
    }
}

#[tokio::test]
async fn single_row_utxo_stream_releases_response_permit_upon_yielding_final_row() -> Result<()> {
    let one = NonZeroU32::MIN;
    let reads = ForwardedReadLimit::new(ForwardedReadConfig::default().with_response_limit(
        ForwardedResponseKind::UtxoResponse,
        one,
        0,
    ));
    let fixture = populated_utxo_fixture_with(2, 1, super::UtxoNode::Zakura, reads)?;
    let final_height = || {
        let mut request = utxo_request(1);
        request.get_mut().start_height = 6;
        request
    };
    let mut single_row = fixture
        .adapter
        .get_address_utxos_stream(final_height())
        .await?
        .into_inner();

    let while_buffered = fixture
        .adapter
        .get_address_utxos(final_height())
        .await
        .err()
        .ok_or_else(|| eyre!("admitted second UTXO response while single row was buffered"))?;
    refused_for_admission(&while_buffered);

    let first_reply = single_row
        .next()
        .await
        .transpose()?
        .ok_or_else(|| eyre!("missing single UTXO row"))?;
    assert_eq!(first_reply.address, ADDRESS);

    let admitted_after_final_row = fixture
        .adapter
        .get_address_utxos(final_height())
        .await?
        .into_inner();
    assert_eq!(admitted_after_final_row.address_utxos.len(), 1);
    assert!(single_row.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn stalled_large_utxo_streams_bound_buffered_responses_and_fail_closed() -> Result<()> {
    let two = NonZeroU32::MIN.saturating_add(1);
    let reads = ForwardedReadLimit::new(
        ForwardedReadConfig::resolve(None, Some(400), Some(2), None)?.with_response_limit(
            ForwardedResponseKind::UtxoResponse,
            two,
            2,
        ),
    );
    let fixture = populated_utxo_fixture_with(5_000, 1, super::UtxoNode::Zakura, reads)?;
    let adapter = &fixture.adapter;

    let mut stalled_a = adapter
        .get_address_utxos_stream(utxo_request(5_000))
        .await?
        .into_inner();
    let mut stalled_b = adapter
        .get_address_utxos_stream(utxo_request(5_000))
        .await?
        .into_inner();
    assert!(stalled_a.next().await.transpose()?.is_some());
    assert!(stalled_b.next().await.transpose()?.is_some());

    let node_calls_before = fixture.node.requests_for("getaddressutxos")?.len();
    let waiter_a = tokio::spawn({
        let cloned = adapter.clone();
        async move { cloned.get_address_utxos_stream(utxo_request(1)).await }
    });
    let waiter_b = tokio::spawn({
        let cloned = adapter.clone();
        async move { cloned.get_address_utxos_stream(utxo_request(1)).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let overflow = adapter
        .get_address_utxos_stream(utxo_request(1))
        .await
        .err()
        .ok_or_else(|| eyre!("fifth request bypassed full UTXO response queue"))?;
    refused_for_admission(&overflow);

    let timed_out_a = waiter_a
        .await?
        .err()
        .ok_or_else(|| eyre!("waiter A succeeded while response slots stayed stalled"))?;
    let timed_out_b = waiter_b
        .await?
        .err()
        .ok_or_else(|| eyre!("waiter B succeeded while response slots stayed stalled"))?;
    refused_for_admission(&timed_out_a);
    refused_for_admission(&timed_out_b);
    assert_eq!(
        fixture.node.requests_for("getaddressutxos")?.len(),
        node_calls_before,
        "queued or overflowed UTXO response waiters must not call the node"
    );

    let balance = adapter.get_taddress_balance(balance_request()).await?;
    assert_eq!(balance.into_inner().value_zat, 5_001);

    drop((stalled_a, stalled_b));
    let mut request = utxo_request(1);
    request.get_mut().start_height = 6;
    let recovered = adapter.get_address_utxos(request).await?.into_inner();
    assert_eq!(recovered.address_utxos.len(), 1);
    Ok(())
}

#[tokio::test]
async fn stalled_upstream_node_fails_closed_at_wait_deadline_without_leaking_permits() -> Result<()>
{
    let tip = served_tip(1)?;
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let node_gate = Arc::clone(&gate);
    let counter = Arc::new(ActiveRpcCounter::new());
    let rpc_counter = Arc::clone(&counter);
    let reads = ForwardedReadLimit::new(ForwardedReadConfig::resolve(
        Some(3),
        Some(400),
        Some(2),
        None,
    )?);
    let fixture = Fixture::with_node_timeout_and_reads(
        1,
        [method("getaddressutxos").reply_with(move |_| {
            let _guard = rpc_counter.enter();
            let (open, signal) = &*node_gate;
            let mut released = open.lock();
            signal.wait_while_for(&mut released, |state| !*state, Duration::from_secs(5));
            utxos_at(tip, &json!([]))
        })],
        Duration::from_secs(5),
        reads,
    )?;

    let blocked = tokio::spawn({
        let adapter = fixture.adapter.clone();
        async move { adapter.get_address_utxos_stream(utxo_request(0)).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while counter.active() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let waiter_a = tokio::spawn({
        let adapter = fixture.adapter.clone();
        async move { adapter.get_address_utxos_stream(utxo_request(0)).await }
    });
    let waiter_b = tokio::spawn({
        let adapter = fixture.adapter.clone();
        async move { adapter.get_address_utxos_stream(utxo_request(0)).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let overflow = fixture
        .adapter
        .get_address_utxos_stream(utxo_request(0))
        .await
        .err()
        .ok_or_else(|| eyre!("fourth request bypassed full UTXO node-call queue"))?;
    refused_for_admission(&overflow);

    let timed_out_a = waiter_a
        .await?
        .err()
        .ok_or_else(|| eyre!("waiter A bypassed stalled node slot"))?;
    let timed_out_b = waiter_b
        .await?
        .err()
        .ok_or_else(|| eyre!("waiter B bypassed stalled node slot"))?;
    refused_for_admission(&timed_out_a);
    refused_for_admission(&timed_out_b);
    assert_eq!(counter.peak(), 1, "stalled node must hold at most 1 slot");

    {
        let (open, signal) = &*gate;
        *open.lock() = true;
        signal.notify_all();
    }
    let mut recovered_first = blocked.await??.into_inner();
    assert!(recovered_first.next().await.is_none());

    let mut after_recovery = fixture
        .adapter
        .get_address_utxos_stream(Request::new(GetAddressUtxosArg {
            addresses: vec![ADDRESS.to_owned()],
            start_height: 1,
            max_entries: 0,
        }))
        .await?
        .into_inner();
    assert!(after_recovery.next().await.is_none());
    assert_eq!(counter.active(), 0);
    Ok(())
}
