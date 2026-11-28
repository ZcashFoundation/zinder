//! T2 performance benchmark for pipelined wallet-mode `GetTaddressTxids` cold reads.

use std::{
    collections::HashSet,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use eyre::Result;
use tokio_stream::StreamExt as _;
use tonic::Request;
use zinder_proto::compat::lightwalletd::compact_tx_streamer_server::CompactTxStreamer as _;
use zinder_query::{ForwardedReadConfig, ForwardedReadLimit, WalletQueryApi as _};

use crate::integration::transparent_wallet_history_pipeline::{
    AsyncZebraNode, NodeBehavior, build_lean_adapter, build_multi_block_cold_fixture,
    history_filter, sequence_txid,
};

#[cfg(debug_assertions)]
const MAX_PIPELINED_DURATION: Duration = Duration::from_millis(1_000);
#[cfg(not(debug_assertions))]
const MAX_PIPELINED_DURATION: Duration = Duration::from_millis(200);

#[cfg(debug_assertions)]
const MIN_SPEEDUP_FACTOR: u32 = 2;
#[cfg(not(debug_assertions))]
const MIN_SPEEDUP_FACTOR: u32 = 3;

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
    let peak_in_flight = node.peak_raw_tx.load(Ordering::SeqCst);
    record_bench_report(BUSY_TXS, serial_elapsed, pipelined_elapsed, peak_in_flight)?;

    assert_eq!(received, fixture.expected_per_address[0]);
    assert!(
        serial_elapsed >= Duration::from_millis(340),
        "expected 340 serial 1ms RPCs to take >= 340ms, took {serial_elapsed:?}",
    );
    assert!(
        pipelined_elapsed < MAX_PIPELINED_DURATION
            && pipelined_elapsed * MIN_SPEEDUP_FACTOR < serial_elapsed,
        "expected pipelined 340 cold txs (< {MAX_PIPELINED_DURATION:?} and > {MIN_SPEEDUP_FACTOR}x faster than serial {serial_elapsed:?}), took {pipelined_elapsed:?}",
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
