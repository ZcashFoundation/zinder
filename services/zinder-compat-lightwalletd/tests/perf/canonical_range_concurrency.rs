//! High-concurrency 2-worker benchmark and regression gate for canonical block-range streams.

use std::{
    collections::HashSet,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant},
};

use eyre::{Result, ensure, eyre};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint as TonicEndpoint};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::{
    BlockHeight, BlockId, CompactBlockArtifact, CompactChainMetadata, CompactSaplingSpend,
    CompactTransaction, CompactTransactionData, Network, TransactionId,
};
use zinder_proto::compat::lightwalletd::{
    BlockId as ProtoBlockId, BlockRange, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::{
    RocksDbCanonicalReader, RocksDbWalletReader, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_runtime::{
    ClientAddressSource, DEFAULT_GRPC_RECEIVE_TIMEOUT, DEFAULT_GRPC_SEND_STALL_TIMEOUT,
    GrpcReceiveLimits, WalletRpcMetricsLayer, public_grpc_server_with,
};
use zinder_store::RawBlobRetention;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
    WalletServingStoreFixture, sample_regtest_upgrade_activations,
};

const TOTAL_BLOCKS: u32 = 4_050;
const SETTLED_BLOCKS: u32 = 4_000;

#[cfg(debug_assertions)]
const MAX_BURST_MS: u128 = 30_000;
#[cfg(not(debug_assertions))]
const MAX_BURST_MS: u128 = 10_000;

struct BenchFixture {
    _store: WalletServingStoreFixture,
    query: WalletServingQuery<()>,
}

#[derive(Clone, Copy, Default)]
struct ThreadIo {
    preads: u64,
    reads: u64,
    writes: u64,
}

impl ThreadIo {
    const fn delta(self, prior: Self) -> Self {
        Self {
            preads: self.preads.saturating_sub(prior.preads),
            reads: self.reads.saturating_sub(prior.reads),
            writes: self.writes.saturating_sub(prior.writes),
        }
    }
}

fn sapling_spend(seed: u8) -> CompactSaplingSpend {
    CompactSaplingSpend {
        nullifier: [seed; 32],
    }
}

fn spend_count(height: u32) -> usize {
    if height.is_multiple_of(8) {
        24
    } else if height.is_multiple_of(2) {
        6
    } else {
        0
    }
}

fn build_mixed_chain() -> Result<ChainFixture> {
    let mut chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(TOTAL_BLOCKS);
    let metadata = CompactChainMetadata {
        sapling_commitment_tree_size: 0,
        orchard_commitment_tree_size: 0,
        ironwood_commitment_tree_size: 0,
    };
    for height_num in 2..=TOTAL_BLOCKS {
        let spends = spend_count(height_num);
        if spends == 0 {
            continue;
        }
        let seed = height_num.to_le_bytes()[0];
        let mut txid_bytes = [seed; 32];
        txid_bytes[..4].copy_from_slice(&height_num.to_le_bytes());
        let txid = TransactionId::from_bytes(txid_bytes);
        let height = BlockHeight::new(height_num);
        let block = chain
            .block_at(height)
            .ok_or_else(|| eyre!("missing fixture block at {height_num}"))?;
        let mut rows =
            FixtureTransactionRows::from_raw_transaction(txid, height, block.hash, 0, [seed]);
        rows.facts.public_facts.counts.sapling_spend_count = u32::try_from(spends)?;
        let compact = CompactBlockArtifact::new(
            BlockId::new(height, block.hash),
            block.parent_hash,
            block.block_time_seconds,
            vec![CompactTransaction {
                index: 0,
                transaction_id: txid,
                data: CompactTransactionData {
                    fee_zat: Some(5_000),
                    sapling_spends: vec![sapling_spend(seed); spends],
                    ..CompactTransactionData::default()
                },
            }],
            metadata,
        )?;
        chain = chain
            .with_transaction_rows(rows)
            .with_compact_block_artifact(compact);
    }
    Ok(chain)
}

fn build_bench_fixture() -> Result<BenchFixture> {
    let chain = build_mixed_chain()?;
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(SETTLED_BLOCKS)),
        &sample_regtest_upgrade_activations(),
    )?;
    let mut store = writer.into_serving_store_fixture()?;
    let (mut canonical, wallet) = store.take_readers()?;
    canonical.try_catch_up()?;
    let reader = Arc::new(RocksDbCanonicalReader::new(canonical)?);
    let pair = WalletServingReadPair::new(reader, Arc::new(RocksDbWalletReader::new(wallet)))?;
    let query =
        WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(pair)));
    Ok(BenchFixture {
        _store: store,
        query,
    })
}

#[cfg(target_os = "linux")]
fn current_tid() -> Option<u64> {
    let link = std::fs::read_link("/proc/thread-self").ok()?;
    link.file_name()?.to_str()?.parse().ok()
}

#[cfg(not(target_os = "linux"))]
fn current_tid() -> Option<u64> {
    None
}

fn sample_server_io(tids: &Mutex<HashSet<u64>>) -> ThreadIo {
    let mut totals = ThreadIo::default();
    for &tid in tids.lock().iter() {
        let Ok(contents) = std::fs::read_to_string(format!("/proc/self/task/{tid}/io")) else {
            continue;
        };
        let mut thread_r = 0_u64;
        let mut thread_w = 0_u64;
        for line in contents.lines() {
            if let Some(rest) = line.strip_prefix("syscr: ")
                && let Ok(parsed) = rest.trim().parse::<u64>()
            {
                thread_r = parsed;
            } else if let Some(rest) = line.strip_prefix("syscw: ")
                && let Ok(parsed) = rest.trim().parse::<u64>()
            {
                thread_w = parsed;
            }
        }
        if thread_w == 0 {
            totals.preads = totals.preads.saturating_add(thread_r);
        }
        totals.reads = totals.reads.saturating_add(thread_r);
        totals.writes = totals.writes.saturating_add(thread_w);
    }
    totals
}

struct TwoWorkerServer {
    addr: SocketAddr,
    tids: Arc<Mutex<HashSet<u64>>>,
    shutdown: CancellationToken,
    thread: Option<std::thread::JoinHandle<Result<()>>>,
}

impl TwoWorkerServer {
    fn start(query: WalletServingQuery<()>) -> Result<Self> {
        let tids = Arc::new(Mutex::new(HashSet::new()));
        let recorded_tids = Arc::clone(&tids);
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let (addr_tx, addr_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .on_thread_start(move || {
                    if let Some(tid) = current_tid() {
                        recorded_tids.lock().insert(tid);
                    }
                })
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
                addr_tx
                    .send(listener.local_addr()?)
                    .map_err(|_| eyre!("addr receiver dropped"))?;
                let max_streams = NonZeroU32::new(200).ok_or_else(|| eyre!("zero max streams"))?;
                let receive_slots =
                    NonZeroUsize::new(256).ok_or_else(|| eyre!("zero receive slots"))?;
                let adapter = LightwalletdGrpcAdapter::new(query)
                    .with_max_concurrent_block_range_streams(max_streams);
                let router = public_grpc_server_with(
                    GrpcReceiveLimits::new(receive_slots, DEFAULT_GRPC_RECEIVE_TIMEOUT),
                    DEFAULT_GRPC_SEND_STALL_TIMEOUT,
                    ClientAddressSource::Connection,
                )
                .layer(WalletRpcMetricsLayer::Lightwalletd)
                .add_service(adapter.into_server());
                router
                    .serve_with_incoming_shutdown(listener, stop.cancelled_owned())
                    .await?;
                Ok(())
            })
        });
        let addr = addr_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|_| eyre!("two-worker server failed to bind"))?;
        Ok(Self {
            addr,
            tids,
            shutdown,
            thread: Some(thread),
        })
    }

    fn io_snapshot(&self) -> ThreadIo {
        sample_server_io(&self.tids)
    }

    fn stop(mut self) -> Result<()> {
        self.shutdown.cancel();
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| eyre!("two-worker server panicked"))??;
        }
        Ok(())
    }
}

impl Drop for TwoWorkerServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn range_request(start: u64, end: u64) -> BlockRange {
    BlockRange {
        start: Some(ProtoBlockId {
            height: start,
            hash: Vec::new(),
        }),
        end: Some(ProtoBlockId {
            height: end,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    }
}

async fn connect_clients(
    addr: SocketAddr,
    count: usize,
) -> Result<Vec<CompactTxStreamerClient<Channel>>> {
    let mut clients = Vec::with_capacity(count);
    for _ in 0..count {
        let channel = TonicEndpoint::from_shared(format!("http://{addr}"))?
            .initial_stream_window_size(4 * 1024 * 1024)
            .initial_connection_window_size(16 * 1024 * 1024)
            .connect()
            .await?;
        clients.push(CompactTxStreamerClient::new(channel));
    }
    Ok(clients)
}

async fn run_concurrent_ranges(
    clients: &[CompactTxStreamerClient<Channel>],
    requests: usize,
    (start, end): (u64, u64),
) -> Result<usize> {
    let expected = usize::try_from(end - start + 1)?;
    let mut tasks = tokio::task::JoinSet::new();
    for idx in 0..requests {
        let mut client = clients[idx % clients.len()].clone();
        tasks.spawn(async move {
            let mut stream = client
                .get_block_range(range_request(start, end))
                .await?
                .into_inner();
            let mut count = 0_usize;
            while let Some(block) = stream.message().await? {
                ensure!(block.height >= start && block.height <= end);
                count += 1;
            }
            ensure!(count == expected, "expected {expected} blocks, got {count}");
            Ok::<usize, eyre::Report>(count)
        });
    }
    let mut delivered = 0_usize;
    while let Some(joined) = tasks.join_next().await {
        delivered += joined??;
    }
    Ok(delivered)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_worker_canonical_block_ranges_scale_under_high_concurrency() -> Result<()> {
    let fixture = build_bench_fixture()?;
    let server = TwoWorkerServer::start(fixture.query)?;
    let clients = connect_clients(server.addr, 32).await?;

    // Warm up segment validation verdicts and blocking threads once.
    let warmed = run_concurrent_ranges(&clients, 1, (1, u64::from(SETTLED_BLOCKS))).await?;
    assert_eq!(warmed, usize::try_from(SETTLED_BLOCKS)?);

    let io_before_burst = server.io_snapshot();
    let started_burst = Instant::now();
    let burst_blocks = run_concurrent_ranges(&clients, 200, (1, 1_000)).await?;
    let burst_elapsed = started_burst.elapsed();
    let io_after_burst = server.io_snapshot();
    assert_eq!(burst_blocks, 200_000);

    let started_wide = Instant::now();
    let wide_blocks = run_concurrent_ranges(&clients, 100, (1, u64::from(SETTLED_BLOCKS))).await?;
    let wide_elapsed = started_wide.elapsed();
    let io_after_wide = server.io_snapshot();
    assert_eq!(wide_blocks, 100 * usize::try_from(SETTLED_BLOCKS)?);

    let burst_secs = burst_elapsed.as_secs_f64();
    let wide_secs = wide_elapsed.as_secs_f64();
    let wide_total = f64::from(u32::try_from(wide_blocks)?);
    let burst_io = io_after_burst.delta(io_before_burst);
    let wide_io = io_after_wide.delta(io_after_burst);

    #[allow(
        clippy::print_stderr,
        reason = "perf gate reports 2-worker throughput and syscall counts"
    )]
    {
        eprintln!(
            "canonical_range_2w_burst200x1k elapsed_ms={} req_s={:.1} blk_s={:.0} preads={} syscr={} syscw={}",
            burst_elapsed.as_millis(),
            200.0 / burst_secs,
            200_000.0 / burst_secs,
            burst_io.preads,
            burst_io.reads,
            burst_io.writes,
        );
        eprintln!(
            "canonical_range_2w_wide100x4k elapsed_ms={} req_s={:.1} blk_s={:.0} preads={} syscr={} syscw={}",
            wide_elapsed.as_millis(),
            100.0 / wide_secs,
            wide_total / wide_secs,
            wide_io.preads,
            wide_io.reads,
            wide_io.writes,
        );
    }

    assert!(
        burst_elapsed.as_millis() <= MAX_BURST_MS,
        "200x1000 burst took {burst_elapsed:?} (budget {MAX_BURST_MS} ms)"
    );
    server.stop()
}
