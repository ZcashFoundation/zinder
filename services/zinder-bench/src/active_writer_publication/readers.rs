//! Concurrent lightwalletd-compatibility gRPC reader fleet synchronized over
//! level-triggered `watch` channels and shared [`WorkloadTrace`].

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use prost::Message as _;
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};
use zinder_proto::compat::lightwalletd::{
    AddressList, BlockId, BlockRange, ChainSpec, Empty, GetAddressUtxosArg, GetMempoolTxRequest,
    TxFilter, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_runtime::AuthenticatedChannel;

use super::{
    mempool::BenchmarkMempoolSurface,
    pipelines::{ModeServingHandle, ServedState, WriterProgress},
    report::{
        LatencySummaryMs, MempoolTierMetrics, ProtocolErrorCounts, StreamLatencyReport,
        WalletRpcMetrics, WriterMode,
    },
    trace::WorkloadTrace,
};
use crate::BenchError;

const CLIENT_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
const MAX_ERROR_SAMPLES: usize = 8;
const MAX_TRANSIENT_RETRIES: u32 = 8;

fn transient_retry_backoff(attempt: u32) -> Duration {
    Duration::from_millis(u64::from(attempt.saturating_add(1).min(5)) * 10)
}

#[derive(Default)]
pub(crate) struct ErrorCollector {
    pub(crate) unexpected_grpc_errors: u64,
    pub(crate) unexpected_error_samples: Vec<String>,
    pub(crate) transient_retry_count: u64,
}

impl ErrorCollector {
    fn record_unexpected(&mut self, context: &str, status: &Status) {
        self.unexpected_grpc_errors = self.unexpected_grpc_errors.saturating_add(1);
        if self.unexpected_error_samples.len() < MAX_ERROR_SAMPLES {
            self.unexpected_error_samples.push(format!(
                "{context}: {:?}: {}",
                status.code(),
                status.message()
            ));
        }
    }

    pub(crate) fn to_report(&self) -> ProtocolErrorCounts {
        ProtocolErrorCounts {
            unexpected_grpc_errors: self.unexpected_grpc_errors,
            unexpected_error_samples: self.unexpected_error_samples.clone(),
            transient_retry_count: self.transient_retry_count,
        }
    }
}

/// Shared synchronization and trace passed to every concurrent reader task.
#[derive(Clone)]
pub(crate) struct ReaderFleetContext {
    pub(crate) mode: WriterMode,
    pub(crate) wallet_serving: bool,
    pub(crate) trace: WorkloadTrace,
    pub(crate) sample_v5_tx: Arc<Vec<u8>>,
    pub(crate) served_rx: watch::Receiver<ServedState>,
    pub(crate) writer_rx: watch::Receiver<WriterProgress>,
    pub(crate) commit_instants: Arc<Mutex<BTreeMap<u32, Instant>>>,
    pub(crate) cancel: CancellationToken,
    pub(crate) error_collector: Arc<Mutex<ErrorCollector>>,
    pub(crate) quiescent_rotator: Option<ModeServingHandle>,
    pub(crate) rotation_requester: Option<ModeServingHandle>,
}

/// Aggregated measurements returned by the reader fleet before final-tip parity.
pub(crate) struct ReaderFleetOutput {
    pub(crate) latest_block_rpc_ms: LatencySummaryMs,
    pub(crate) commit_to_served_tip_ms: LatencySummaryMs,
    pub(crate) catalog_range: StreamLatencyReport,
    pub(crate) tail_cold_after_advance: StreamLatencyReport,
    pub(crate) tail_warm_repeat: StreamLatencyReport,
    pub(crate) peak_tail_response_bytes: u64,
    pub(crate) unpaired_tail_probes: u64,
    pub(crate) slow_boundary_range: StreamLatencyReport,
    pub(crate) tree_state_ms: LatencySummaryMs,
    pub(crate) wallet_rpc_ms: WalletRpcMetrics,
    pub(crate) mempool_churn: Vec<MempoolTierMetrics>,
}

/// Structured handle managing reader tasks and clean shutdown on abort/error.
pub(crate) struct ReaderFleetHandle {
    cancel: CancellationToken,
    task: JoinHandle<Result<ReaderFleetOutput, BenchError>>,
}

impl ReaderFleetHandle {
    pub(crate) async fn join(self) -> Result<ReaderFleetOutput, BenchError> {
        self.task
            .await
            .map_err(|err| BenchError::canonical_read(err.to_string()))?
    }

    pub(crate) async fn cancel_and_drain(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

/// Connects a `CompactTxStreamerClient` with a raised decoding ceiling.
pub(crate) fn streamer_client(
    channel: AuthenticatedChannel,
) -> CompactTxStreamerClient<AuthenticatedChannel> {
    CompactTxStreamerClient::new(channel)
        .max_decoding_message_size(CLIENT_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(CLIENT_MAX_MESSAGE_BYTES)
}

/// Spawns all concurrent reader tasks over `channel`.
pub(crate) fn spawn_reader_fleet(
    channel: AuthenticatedChannel,
    ctx: ReaderFleetContext,
    mempool: Arc<BenchmarkMempoolSurface>,
) -> ReaderFleetHandle {
    let cancel = ctx.cancel.clone();
    let task = tokio::spawn(async move {
        let tip_task = tokio::spawn(run_tip_observer(channel.clone(), ctx.clone()));
        let catalog_task = tokio::spawn(run_catalog_reader(channel.clone(), ctx.clone()));
        let tail_task = tokio::spawn(run_tail_refill_reader(channel.clone(), ctx.clone()));
        let slow_task = tokio::spawn(run_slow_boundary_reader(channel.clone(), ctx.clone()));
        let tree_task = tokio::spawn(run_tree_state_reader(channel.clone(), ctx.clone()));
        let wallet_task = tokio::spawn(run_wallet_rpc_reader(channel.clone(), ctx.clone()));
        let mempool_task = tokio::spawn(run_mempool_readers(channel, ctx, mempool));

        let (tip_res, cat_res, tail_res, slow_res, tree_res, wal_res, mempool_res) = tokio::join!(
            tip_task,
            catalog_task,
            tail_task,
            slow_task,
            tree_task,
            wallet_task,
            mempool_task,
        );

        let (latest_block_rpc_ms, commit_to_served_tip_ms) =
            tip_res.map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let catalog_range = cat_res.map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let tail_out = tail_res.map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let slow_boundary_range =
            slow_res.map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let tree_state_ms = tree_res.map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let wallet_rpc_ms = wal_res.map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let mempool_churn =
            mempool_res.map_err(|err| BenchError::canonical_read(err.to_string()))??;

        Ok(ReaderFleetOutput {
            latest_block_rpc_ms,
            commit_to_served_tip_ms,
            catalog_range,
            tail_cold_after_advance: tail_out.cold.finish(),
            tail_warm_repeat: tail_out.warm.finish(),
            peak_tail_response_bytes: tail_out.peak_response_bytes,
            unpaired_tail_probes: tail_out.unpaired_tail_probes,
            slow_boundary_range,
            tree_state_ms,
            wallet_rpc_ms,
            mempool_churn,
        })
    });
    ReaderFleetHandle { cancel, task }
}

async fn watch_served_changed(rx: &mut watch::Receiver<ServedState>, open: &mut bool) -> bool {
    if !*open {
        std::future::pending::<()>().await;
        return false;
    }
    if rx.changed().await.is_err() {
        *open = false;
        return false;
    }
    true
}

async fn await_next_tip_poll(ctx: &mut ReaderFleetContext, served_open: &mut bool) -> bool {
    tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => false,
        changed = watch_served_changed(&mut ctx.served_rx, served_open) => {
            changed || ctx.served_rx.borrow().serving_caught_up_done
        }
        () = tokio::time::sleep(Duration::from_millis(2)) => true,
    }
}

async fn run_tip_observer(
    channel: AuthenticatedChannel,
    mut ctx: ReaderFleetContext,
) -> (LatencySummaryMs, LatencySummaryMs) {
    let mut client = streamer_client(channel);
    let mut rpc_latencies = Vec::new();
    let mut visibility_lags = Vec::new();
    let mut round = 0_u32;
    let mut last_observed_tip = 0_u32;
    let mut last_tip_observed_at = Instant::now();
    let mut served_open = true;

    loop {
        if ctx.cancel.is_cancelled() {
            break;
        }
        let Some((elapsed, height)) = fetch_latest_block_with_retry(&mut client, &ctx).await else {
            break;
        };
        let observed_at = Instant::now();
        rpc_latencies.push(elapsed);
        if height > last_observed_tip {
            last_observed_tip = height;
            last_tip_observed_at = observed_at;
        }
        if ctx.mode != WriterMode::QuiescentControl {
            drain_committed_delays(
                &ctx.commit_instants,
                last_observed_tip,
                last_tip_observed_at,
                &mut visibility_lags,
            );
        }

        round = round.saturating_add(1);
        if should_stop_reader(&mut ctx, round)
            && (ctx.mode == WriterMode::QuiescentControl
                || last_observed_tip >= ctx.trace.final_tip_height)
        {
            if ctx.mode != WriterMode::QuiescentControl {
                drain_committed_delays(
                    &ctx.commit_instants,
                    last_observed_tip,
                    last_tip_observed_at,
                    &mut visibility_lags,
                );
            }
            break;
        }
        if !await_next_tip_poll(&mut ctx, &mut served_open).await {
            break;
        }
    }

    (
        LatencySummaryMs::from_durations(&rpc_latencies),
        LatencySummaryMs::from_durations(&visibility_lags),
    )
}

async fn fetch_latest_block_with_retry(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    ctx: &ReaderFleetContext,
) -> Option<(Duration, u32)> {
    let started = Instant::now();
    for attempt in 0..=MAX_TRANSIENT_RETRIES {
        let rpc_res = tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => return None,
            rpc = client.get_latest_block(ChainSpec {}) => rpc,
        };
        match rpc_res {
            Ok(resp) => {
                let height = u32::try_from(resp.into_inner().height).unwrap_or(u32::MAX);
                return Some((started.elapsed(), height));
            }
            Err(status) if is_transient_retryable(&status) && attempt < MAX_TRANSIENT_RETRIES => {
                if status.code() == Code::Unavailable
                    && let Some(requester) = ctx.rotation_requester.as_ref()
                {
                    requester.request_rotation();
                }
                {
                    let mut collector = ctx.error_collector.lock();
                    collector.transient_retry_count =
                        collector.transient_retry_count.saturating_add(1);
                }
                tokio::select! {
                    biased;
                    () = ctx.cancel.cancelled() => return None,
                    () = tokio::time::sleep(transient_retry_backoff(attempt)) => {}
                }
            }
            Err(status) => {
                if !ctx.cancel.is_cancelled() {
                    ctx.error_collector
                        .lock()
                        .record_unexpected("GetLatestBlock", &status);
                }
                return None;
            }
        }
    }
    None
}

fn drain_committed_delays(
    commit_instants: &Mutex<BTreeMap<u32, Instant>>,
    served_height: u32,
    observed_at: Instant,
    lag_samples: &mut Vec<Duration>,
) {
    let mut instants = commit_instants.lock();
    let due: Vec<u32> = instants
        .range(..=served_height)
        .map(|(&height, _)| height)
        .collect();
    for height in due {
        if let Some(committed_at) = instants.remove(&height) {
            lag_samples.push(observed_at.saturating_duration_since(committed_at));
        }
    }
}

async fn run_catalog_reader(
    channel: AuthenticatedChannel,
    mut ctx: ReaderFleetContext,
) -> StreamLatencyReport {
    let mut client = streamer_client(channel);
    let mut tracker = StreamStatsTracker::default();
    let mut round = 0_u32;

    loop {
        if ctx.cancel.is_cancelled() {
            break;
        }
        let tip = ctx.served_rx.borrow().tip_height.max(1);
        let (start_height, end_height) = ctx.trace.catalog_range(tip);
        tracker
            .probe_range(
                &mut client,
                RangeProbe {
                    label: "GetBlockRange(catalog)",
                    request: range_request(start_height, end_height),
                    frame_delay: Duration::ZERO,
                },
                &ctx,
            )
            .await;

        round = round.saturating_add(1);
        if should_stop_reader(&mut ctx, round) {
            break;
        }
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => break,
            () = tokio::time::sleep(Duration::from_millis(2)) => {}
        }
    }

    tracker.finish()
}

#[derive(Default)]
struct TailTrackers {
    cold: StreamStatsTracker,
    warm: StreamStatsTracker,
    peak_response_bytes: u64,
    unpaired_tail_probes: u64,
}

async fn run_tail_refill_reader(
    channel: AuthenticatedChannel,
    mut ctx: ReaderFleetContext,
) -> TailTrackers {
    let mut client = streamer_client(channel);
    let mut trackers = TailTrackers::default();

    if ctx.mode == WriterMode::QuiescentControl {
        run_quiescent_tail_loop(&mut client, &ctx, &mut trackers).await;
    } else {
        run_active_tail_loop(&mut client, &mut ctx, &mut trackers).await;
    }
    trackers
}

async fn run_quiescent_tail_loop(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    ctx: &ReaderFleetContext,
    trackers: &mut TailTrackers,
) {
    let mut verified_rounds = 0_u32;
    let mut attempts = 0_u32;
    let max_attempts = ctx.trace.quiescent_rounds.saturating_mul(4).max(4);
    while verified_rounds < ctx.trace.quiescent_rounds
        && attempts < max_attempts
        && !ctx.cancel.is_cancelled()
    {
        if attempts > 0
            && let Some(rotator) = ctx.quiescent_rotator.as_ref()
        {
            let rotated = rotator.rotate_generation_for_cold_tail(&ctx.cancel).await;
            if !matches!(rotated, Ok((_, _, seq)) if seq > 0) || ctx.cancel.is_cancelled() {
                break;
            }
        }
        let tip = ctx
            .served_rx
            .borrow()
            .tip_height
            .max(ctx.trace.base_tip_height);
        let (start, end) = ctx.trace.descending_tail_range(tip);
        if probe_tail_pair(client, ctx, trackers, start, end).await {
            verified_rounds = verified_rounds.saturating_add(1);
        }

        attempts = attempts.saturating_add(1);
        if verified_rounds < ctx.trace.quiescent_rounds && attempts < max_attempts {
            tokio::select! {
                biased;
                () = ctx.cancel.cancelled() => break,
                () = tokio::time::sleep(Duration::from_millis(2)) => {}
            }
        }
    }
}

async fn run_active_tail_loop(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    ctx: &mut ReaderFleetContext,
    trackers: &mut TailTrackers,
) {
    let mut last_tip = ctx.trace.base_tip_height;
    loop {
        let served = *ctx.served_rx.borrow_and_update();
        if ctx.cancel.is_cancelled() || served.serving_caught_up_done {
            break;
        }
        if served.tip_height > last_tip {
            last_tip = last_tip.max(served.tip_height);
            let (start, end) = ctx.trace.descending_tail_range(served.tip_height);
            let _ = probe_tail_pair(client, ctx, trackers, start, end).await;
            continue;
        }
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => break,
            res = ctx.served_rx.changed() => {
                if res.is_err() {
                    break;
                }
            }
            () = tokio::time::sleep(Duration::from_millis(2)) => {}
        }
    }
}

async fn probe_tail_pair(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    ctx: &ReaderFleetContext,
    trackers: &mut TailTrackers,
    start: u32,
    end: u32,
) -> bool {
    let req = range_request(start, end);
    let before = *ctx.served_rx.borrow();
    let Some(cold) = measure_range_sample(
        client,
        RangeProbe {
            label: "GetBlockRange(tail-cold)",
            request: req.clone(),
            frame_delay: Duration::ZERO,
        },
        ctx,
    )
    .await
    else {
        return false;
    };
    let mid = *ctx.served_rx.borrow();
    if !tail_sample_is_stable(&cold, before, mid, ctx) {
        trackers.unpaired_tail_probes = trackers.unpaired_tail_probes.saturating_add(1);
        return false;
    }

    let Some(warm) = measure_range_sample(
        client,
        RangeProbe {
            label: "GetBlockRange(tail-warm)",
            request: req,
            frame_delay: Duration::ZERO,
        },
        ctx,
    )
    .await
    else {
        return false;
    };
    let after = *ctx.served_rx.borrow();
    if !tail_sample_is_stable(&warm, before, after, ctx)
        || cold.frames != warm.frames
        || cold.payload_bytes != warm.payload_bytes
    {
        trackers.unpaired_tail_probes = trackers.unpaired_tail_probes.saturating_add(1);
        return false;
    }

    trackers.cold.record_sample(&cold);
    trackers.warm.record_sample(&warm);
    trackers.peak_response_bytes = trackers
        .peak_response_bytes
        .max(cold.payload_bytes.max(warm.payload_bytes));
    true
}

fn tail_sample_is_stable(
    sample: &StreamSample,
    expected: ServedState,
    observed: ServedState,
    ctx: &ReaderFleetContext,
) -> bool {
    sample.retries == 0
        && (ctx.mode == WriterMode::QuiescentControl || !observed.serving_caught_up_done)
        && observed.epoch_id == expected.epoch_id
        && observed.generation_seq == expected.generation_seq
}

async fn run_slow_boundary_reader(
    channel: AuthenticatedChannel,
    mut ctx: ReaderFleetContext,
) -> StreamLatencyReport {
    let mut client = streamer_client(channel);
    let mut tracker = StreamStatsTracker::default();
    let frame_delay = ctx.trace.slow_reader_frame_delay();
    let mut round = 0_u32;

    loop {
        if ctx.cancel.is_cancelled() {
            break;
        }
        let tip = ctx.served_rx.borrow().tip_height;
        let (start, end) = ctx.trace.slow_boundary_range(tip);
        tracker
            .probe_range(
                &mut client,
                RangeProbe {
                    label: "GetBlockRange(slow-boundary)",
                    request: range_request(start, end),
                    frame_delay,
                },
                &ctx,
            )
            .await;

        round = round.saturating_add(1);
        if should_stop_reader(&mut ctx, round) {
            break;
        }
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => break,
            () = tokio::time::sleep(Duration::from_millis(4)) => {}
        }
    }
    tracker.finish()
}

async fn run_tree_state_reader(
    channel: AuthenticatedChannel,
    mut ctx: ReaderFleetContext,
) -> LatencySummaryMs {
    let client = streamer_client(channel);
    let mut samples = Vec::new();
    let settled_probe = ctx.trace.tree_state_settled_height;
    let mut round = 0_u32;

    loop {
        let current_tip = ctx.served_rx.borrow().tip_height.max(settled_probe);
        for target_height in [settled_probe, current_tip] {
            record_unary_sample(
                &client,
                "GetTreeState",
                &ctx,
                &mut samples,
                |mut cli| async move {
                    cli.get_tree_state(BlockId {
                        height: u64::from(target_height),
                        hash: Vec::new(),
                    })
                    .await
                },
            )
            .await;
            if ctx.cancel.is_cancelled() {
                return LatencySummaryMs::from_durations(&samples);
            }
        }
        round = round.saturating_add(1);
        if should_stop_reader(&mut ctx, round) {
            break;
        }
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => break,
            () = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    LatencySummaryMs::from_durations(&samples)
}

async fn run_wallet_rpc_reader(
    channel: AuthenticatedChannel,
    mut ctx: ReaderFleetContext,
) -> WalletRpcMetrics {
    if !ctx.wallet_serving || ctx.trace.wallet_addresses.is_empty() {
        return WalletRpcMetrics::default();
    }
    let client = streamer_client(channel);
    let mut balance_durs = Vec::new();
    let mut utxo_durs = Vec::new();
    let mut tx_durs = Vec::new();
    let mut round = 0_u32;

    loop {
        if ctx.cancel.is_cancelled() {
            break;
        }
        probe_wallet_rpc_round(
            &client,
            &ctx,
            round,
            (&mut balance_durs, &mut utxo_durs, &mut tx_durs),
        )
        .await;

        round = round.saturating_add(1);
        if should_stop_reader(&mut ctx, round) {
            break;
        }
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => break,
            () = tokio::time::sleep(Duration::from_millis(4)) => {}
        }
    }

    WalletRpcMetrics {
        taddress_balance_ms: LatencySummaryMs::from_durations(&balance_durs),
        address_utxos_ms: LatencySummaryMs::from_durations(&utxo_durs),
        get_transaction_ms: LatencySummaryMs::from_durations(&tx_durs),
    }
}

async fn probe_wallet_rpc_round(
    client: &CompactTxStreamerClient<AuthenticatedChannel>,
    ctx: &ReaderFleetContext,
    round: u32,
    (balance_durs, utxo_durs, tx_durs): (
        &mut Vec<Duration>,
        &mut Vec<Duration>,
        &mut Vec<Duration>,
    ),
) {
    let served_tip = ctx.served_rx.borrow().tip_height;
    let addr_idx = usize::try_from(round).unwrap_or(0) % ctx.trace.wallet_addresses.len();
    let address = ctx.trace.wallet_addresses[addr_idx].clone();
    let balance_addr = address.clone();

    record_unary_sample(
        client,
        "GetTaddressBalance",
        ctx,
        balance_durs,
        |mut cli| {
            let addr = balance_addr.clone();
            async move {
                cli.get_taddress_balance(AddressList {
                    addresses: vec![addr],
                })
                .await
            }
        },
    )
    .await;
    record_unary_sample(client, "GetAddressUtxos", ctx, utxo_durs, |mut cli| {
        let addr = address.clone();
        async move {
            cli.get_address_utxos(GetAddressUtxosArg {
                addresses: vec![addr],
                start_height: 1,
                max_entries: 64,
            })
            .await
        }
    })
    .await;
    if let Some(txid) = ctx.trace.txid_visible_at(round, served_tip) {
        record_unary_sample(
            client,
            "GetTransaction",
            ctx,
            tx_durs,
            |mut cli| async move {
                cli.get_transaction(TxFilter {
                    block: None,
                    index: 0,
                    hash: txid.to_vec(),
                })
                .await
            },
        )
        .await;
    }
}

async fn record_unary_sample<T, F, Fut>(
    client: &CompactTxStreamerClient<AuthenticatedChannel>,
    label: &str,
    ctx: &ReaderFleetContext,
    samples: &mut Vec<Duration>,
    mut make_rpc: F,
) where
    F: FnMut(CompactTxStreamerClient<AuthenticatedChannel>) -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    let started = Instant::now();
    for attempt in 0..=MAX_TRANSIENT_RETRIES {
        let rpc = make_rpc(client.clone());
        let res = tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => return,
            res = rpc => res,
        };
        match res {
            Ok(_) => {
                samples.push(started.elapsed());
                return;
            }
            Err(status) if is_transient_retryable(&status) && attempt < MAX_TRANSIENT_RETRIES => {
                if status.code() == Code::Unavailable
                    && let Some(requester) = ctx.rotation_requester.as_ref()
                {
                    requester.request_rotation();
                }
                {
                    let mut collector = ctx.error_collector.lock();
                    collector.transient_retry_count =
                        collector.transient_retry_count.saturating_add(1);
                }
                tokio::select! {
                    biased;
                    () = ctx.cancel.cancelled() => return,
                    () = tokio::time::sleep(transient_retry_backoff(attempt)) => {}
                }
            }
            Err(status) => {
                if !ctx.cancel.is_cancelled() {
                    ctx.error_collector.lock().record_unexpected(label, &status);
                }
                return;
            }
        }
    }
}

fn is_transient_retryable(status: &Status) -> bool {
    status.code() == Code::Unavailable
        || (status.code() == Code::FailedPrecondition
            && status.message().contains("chain-epoch pin is unavailable"))
}

async fn run_mempool_readers(
    channel: AuthenticatedChannel,
    ctx: ReaderFleetContext,
    mempool: Arc<BenchmarkMempoolSurface>,
) -> Result<Vec<MempoolTierMetrics>, BenchError> {
    let mut client = streamer_client(channel);
    let watcher_cancel = ctx.cancel.child_token();
    let mut tier_reports = Vec::with_capacity(ctx.trace.mempool_churn_sizes.len());
    let mut pending_watchers = Vec::with_capacity(ctx.trace.mempool_churn_sizes.len());

    for (index, &batch_size) in ctx.trace.mempool_churn_sizes.iter().enumerate() {
        if ctx.cancel.is_cancelled() {
            break;
        }
        let tier = run_mempool_tier(
            &mut client,
            (index, batch_size),
            (&ctx, &mempool, &watcher_cancel),
            &mut pending_watchers,
        )
        .await?;
        tier_reports.push(tier);
    }

    // Wait briefly for in-flight tip-change closures before signalling clean stream teardown.
    if ctx.mode != WriterMode::QuiescentControl && !ctx.cancel.is_cancelled() {
        await_writer_commits_done(&ctx).await;
    }
    watcher_cancel.cancel();
    drain_mempool_watchers(pending_watchers, &mut tier_reports).await;
    Ok(tier_reports)
}

async fn await_commit_progress_or_done(ctx: &ReaderFleetContext, target_commits: u32) {
    let mut rx = ctx.writer_rx.clone();
    loop {
        let prog = *rx.borrow_and_update();
        if ctx.cancel.is_cancelled()
            || prog.commits_completed >= target_commits
            || prog.commits_done
        {
            break;
        }
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => break,
            res = rx.changed() => if res.is_err() { break; }
        }
    }
}

async fn await_writer_commits_done(ctx: &ReaderFleetContext) {
    let mut rx = ctx.served_rx.clone();
    loop {
        let state = *rx.borrow_and_update();
        if ctx.cancel.is_cancelled() || state.serving_caught_up_done {
            break;
        }
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => break,
            res = rx.changed() => if res.is_err() { break; }
        }
    }
}

async fn run_mempool_tier(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    (index, batch_size): (usize, u32),
    (ctx, mempool, watcher_cancel): (
        &ReaderFleetContext,
        &BenchmarkMempoolSurface,
        &CancellationToken,
    ),
    pending_watchers: &mut Vec<(usize, JoinHandle<u64>)>,
) -> Result<MempoolTierMetrics, BenchError> {
    let required_commits = u32::try_from(index).unwrap_or(u32::MAX);
    if ctx.mode != WriterMode::QuiescentControl && required_commits > 0 {
        await_commit_progress_or_done(ctx, required_commits).await;
    }

    let before = *ctx.writer_rx.borrow();
    let commits_before = before.commits_completed;
    let active_at_start = !before.commits_done;
    let round_id = u32::try_from(index + 1).unwrap_or(1);
    let mut tracker = StreamStatsTracker::default();
    mempool.apply_churn_batch(batch_size, round_id, ctx.sample_v5_tx.as_slice())?;

    if !ctx.cancel.is_cancelled() {
        pending_watchers.push((
            index,
            spawn_mempool_stream_watcher(
                client.clone(),
                Arc::clone(&ctx.error_collector),
                watcher_cancel.clone(),
            ),
        ));
    }

    let passes = if batch_size >= 500 { 2 } else { 3 };
    for pass in 0..passes {
        if ctx.cancel.is_cancelled() {
            break;
        }
        if ctx.mode != WriterMode::QuiescentControl && pass + 1 == passes {
            await_commit_progress_or_done(ctx, commits_before.saturating_add(1)).await;
        }
        probe_mempool_tx(client, &mut tracker, ctx).await;
    }

    let after = *ctx.writer_rx.borrow();
    let commits_observed_during_tier = after.commits_completed.saturating_sub(commits_before);
    let writer_overlap_verified = ctx.mode != WriterMode::QuiescentControl
        && active_at_start
        && (commits_observed_during_tier > 0 || !after.commits_done);

    Ok(MempoolTierMetrics {
        batch_size,
        mempool_tx_stream: tracker.finish(),
        mempool_stream_closures: 0,
        writer_overlap_verified,
        commits_observed_during_tier,
    })
}

async fn drain_mempool_watchers(
    pending_watchers: Vec<(usize, JoinHandle<u64>)>,
    tier_reports: &mut [MempoolTierMetrics],
) {
    for (tier_index, handle) in pending_watchers {
        let closures = handle.await.unwrap_or(0);
        if let Some(report) = tier_reports.get_mut(tier_index) {
            report.mempool_stream_closures = closures;
        }
    }
}

fn spawn_mempool_stream_watcher(
    mut stream_client: CompactTxStreamerClient<AuthenticatedChannel>,
    error_collector: Arc<Mutex<ErrorCollector>>,
    cancel: CancellationToken,
) -> JoinHandle<u64> {
    tokio::spawn(async move {
        let mut closures = 0_u64;
        let open_res = tokio::select! {
            biased;
            () = cancel.cancelled() => return 0,
            resp = stream_client.get_mempool_stream(Empty {}) => resp,
        };
        match open_res {
            Ok(resp) => {
                let mut stream = resp.into_inner();
                loop {
                    let msg = tokio::select! {
                        biased;
                        () = cancel.cancelled() => break,
                        frame = stream.message() => frame,
                    };
                    match msg {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            closures = closures.saturating_add(1);
                            break;
                        }
                        Err(status) if !cancel.is_cancelled() => {
                            error_collector
                                .lock()
                                .record_unexpected("GetMempoolStream", &status);
                            break;
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(status) if !cancel.is_cancelled() => error_collector
                .lock()
                .record_unexpected("GetMempoolStream", &status),
            Err(_) => {}
        }
        closures
    })
}

async fn probe_mempool_tx(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    tracker: &mut StreamStatsTracker,
    ctx: &ReaderFleetContext,
) {
    let started = Instant::now();
    let request = GetMempoolTxRequest {
        exclude_txid_suffixes: Vec::new(),
        pool_types: Vec::new(),
    };
    let response = tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => return,
        resp = client.get_mempool_tx(request) => resp,
    };
    match response {
        Ok(resp) => {
            let mut stream = resp.into_inner();
            let mut first_elapsed = None;
            let mut frames = 0_u64;
            let mut bytes = 0_u64;
            loop {
                let msg = tokio::select! {
                    biased;
                    () = ctx.cancel.cancelled() => return,
                    entry = stream.message() => entry,
                };
                match msg {
                    Ok(Some(tx)) => {
                        if first_elapsed.is_none() {
                            first_elapsed = Some(started.elapsed());
                        }
                        frames = frames.saturating_add(1);
                        bytes = bytes.saturating_add(u64::try_from(tx.encoded_len()).unwrap_or(0));
                    }
                    Ok(None) => break,
                    Err(status) if !ctx.cancel.is_cancelled() => {
                        ctx.error_collector
                            .lock()
                            .record_unexpected("GetMempoolTx", &status);
                        return;
                    }
                    Err(_) => return,
                }
            }
            tracker.record_sample(&StreamSample {
                first_elapsed,
                total_elapsed: started.elapsed(),
                frames,
                payload_bytes: bytes,
                retries: 0,
            });
        }
        Err(status) if !ctx.cancel.is_cancelled() => ctx
            .error_collector
            .lock()
            .record_unexpected("GetMempoolTx", &status),
        Err(_) => {}
    }
}

#[derive(Clone, Copy)]
struct StreamSample {
    first_elapsed: Option<Duration>,
    total_elapsed: Duration,
    frames: u64,
    payload_bytes: u64,
    retries: u32,
}

#[derive(Default)]
struct StreamStatsTracker {
    first_frame: Vec<Duration>,
    completion: Vec<Duration>,
    frames_delivered: u64,
    payload_bytes_delivered: u64,
}

struct RangeProbe<'a> {
    label: &'a str,
    request: BlockRange,
    frame_delay: Duration,
}

impl StreamStatsTracker {
    async fn probe_range(
        &mut self,
        client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
        probe: RangeProbe<'_>,
        ctx: &ReaderFleetContext,
    ) {
        if let Some(sample) = measure_range_sample(client, probe, ctx).await {
            self.record_sample(&sample);
        }
    }

    fn record_sample(&mut self, sample: &StreamSample) {
        if let Some(first_elapsed) = sample.first_elapsed {
            self.first_frame.push(first_elapsed);
        }
        self.completion.push(sample.total_elapsed);
        self.frames_delivered = self.frames_delivered.saturating_add(sample.frames);
        self.payload_bytes_delivered = self
            .payload_bytes_delivered
            .saturating_add(sample.payload_bytes);
    }

    fn finish(&self) -> StreamLatencyReport {
        StreamLatencyReport {
            first_frame_ms: LatencySummaryMs::from_durations(&self.first_frame),
            completion_ms: LatencySummaryMs::from_durations(&self.completion),
            frames_delivered: self.frames_delivered,
            payload_bytes_delivered: self.payload_bytes_delivered,
        }
    }
}

async fn measure_range_sample(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    probe: RangeProbe<'_>,
    ctx: &ReaderFleetContext,
) -> Option<StreamSample> {
    let logical_started = Instant::now();
    for attempt in 0..=MAX_TRANSIENT_RETRIES {
        let stream_fut = try_single_range_stream(
            client,
            probe.request.clone(),
            probe.frame_delay,
            logical_started,
        );
        let step = tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => return None,
            res = stream_fut => res,
        };
        match step {
            Ok(mut sample) => {
                sample.retries = attempt;
                return Some(sample);
            }
            Err(status) if is_transient_retryable(&status) && attempt < MAX_TRANSIENT_RETRIES => {
                if status.code() == Code::Unavailable
                    && let Some(requester) = ctx.rotation_requester.as_ref()
                {
                    requester.request_rotation();
                }
                {
                    let mut collector = ctx.error_collector.lock();
                    collector.transient_retry_count =
                        collector.transient_retry_count.saturating_add(1);
                }
                tokio::select! {
                    biased;
                    () = ctx.cancel.cancelled() => return None,
                    () = tokio::time::sleep(transient_retry_backoff(attempt)) => {}
                }
            }
            Err(status) => {
                if !ctx.cancel.is_cancelled() {
                    ctx.error_collector
                        .lock()
                        .record_unexpected(probe.label, &status);
                }
                return None;
            }
        }
    }
    None
}

async fn try_single_range_stream(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    request: BlockRange,
    frame_delay: Duration,
    logical_started: Instant,
) -> Result<StreamSample, Status> {
    let mut stream = client
        .get_block_range(tonic::Request::new(request))
        .await?
        .into_inner();
    let mut first_elapsed = None;
    let mut frames = 0_u64;
    let mut payload_bytes = 0_u64;

    while let Some(block) = stream.message().await? {
        if first_elapsed.is_none() {
            first_elapsed = Some(logical_started.elapsed());
        }
        frames = frames.saturating_add(1);
        payload_bytes =
            payload_bytes.saturating_add(u64::try_from(block.encoded_len()).unwrap_or(0));
        if !frame_delay.is_zero() {
            tokio::time::sleep(frame_delay).await;
        }
    }

    Ok(StreamSample {
        first_elapsed,
        total_elapsed: logical_started.elapsed(),
        frames,
        payload_bytes,
        retries: 0,
    })
}

fn should_stop_reader(ctx: &mut ReaderFleetContext, round: u32) -> bool {
    if ctx.cancel.is_cancelled() {
        return true;
    }
    if ctx.mode == WriterMode::QuiescentControl {
        return round >= ctx.trace.quiescent_rounds;
    }
    let served = *ctx.served_rx.borrow_and_update();
    served.serving_caught_up_done && served.tip_height >= ctx.trace.final_tip_height
}

fn range_request(start: u32, end: u32) -> BlockRange {
    BlockRange {
        start: Some(BlockId {
            height: u64::from(start),
            hash: Vec::new(),
        }),
        end: Some(BlockId {
            height: u64::from(end),
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        error::Error,
        future::Future as _,
        task::{Context, Waker},
        time::{Duration, Instant},
    };

    use parking_lot::Mutex;
    use tokio::sync::watch;
    use tokio_util::sync::CancellationToken;

    use super::{
        ErrorCollector, ReaderFleetContext, ServedState, TailTrackers, WriterMode, WriterProgress,
        await_commit_progress_or_done, await_next_tip_poll, drain_committed_delays,
        run_active_tail_loop, streamer_client,
    };
    use crate::active_writer_publication::{ActiveWriterPublicationConfig, trace::WorkloadTrace};

    fn test_ctx(
        writer_rx: watch::Receiver<WriterProgress>,
        served_rx: watch::Receiver<ServedState>,
    ) -> ReaderFleetContext {
        let config = ActiveWriterPublicationConfig {
            base_blocks: 20,
            live_blocks: 6,
            reorg_window_blocks: 2,
            segment_blocks: std::num::NonZeroU32::new(16).unwrap_or(std::num::NonZeroU32::MIN),
            mempool_churn_sizes: vec![10],
            ..ActiveWriterPublicationConfig::default()
        };
        let trace = WorkloadTrace::build(&config, 16, Vec::new(), Vec::new(), Vec::new());
        ReaderFleetContext {
            mode: WriterMode::IngestActive,
            wallet_serving: false,
            trace,
            sample_v5_tx: std::sync::Arc::new(vec![0_u8; 32]),
            served_rx,
            writer_rx,
            commit_instants: std::sync::Arc::default(),
            cancel: CancellationToken::new(),
            error_collector: std::sync::Arc::new(
                parking_lot::Mutex::new(ErrorCollector::default()),
            ),
            quiescent_rotator: None,
            rotation_requester: None,
        }
    }

    #[tokio::test]
    async fn await_commit_progress_or_done_wakes_via_level_triggered_watch()
    -> Result<(), Box<dyn Error>> {
        let (writer_tx, writer_rx) = watch::channel(WriterProgress::default());
        let (_served_tx, served_rx) = watch::channel(ServedState::default());
        let ctx = test_ctx(writer_rx, served_rx);

        let noop_waker = Waker::noop();
        {
            let mut wait_commit = std::pin::pin!(await_commit_progress_or_done(&ctx, 1));
            let mut cx = Context::from_waker(noop_waker);
            assert!(wait_commit.as_mut().poll(&mut cx).is_pending());
            writer_tx.send_modify(|prog| prog.commits_completed = 1);
            tokio::time::timeout(Duration::from_secs(1), wait_commit).await?;
        }

        // Update state BEFORE polling a new waiter: level-triggered watch returns Ready immediately.
        writer_tx.send_modify(|prog| prog.commits_done = true);
        let mut already_done = std::pin::pin!(await_commit_progress_or_done(&ctx, 99));
        let mut cx = Context::from_waker(noop_waker);
        assert!(already_done.as_mut().poll(&mut cx).is_ready());
        Ok(())
    }

    #[tokio::test]
    async fn coalesced_serving_caught_up_done_stops_active_tail_loop_before_idle_probe()
    -> Result<(), Box<dyn Error>> {
        let (_writer_tx, writer_rx) = watch::channel(WriterProgress::default());
        let (_served_tx, served_rx) = watch::channel(ServedState {
            tip_height: 26,
            epoch_id: 2,
            generation_seq: 2,
            serving_caught_up_done: true,
        });
        let mut ctx = test_ctx(writer_rx, served_rx);
        let channel = tonic::service::interceptor::InterceptedService::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            zinder_runtime::BearerTokenClientInterceptor::new(None)?,
        );
        let mut client = streamer_client(channel);
        let mut trackers = TailTrackers::default();

        tokio::time::timeout(
            Duration::from_secs(1),
            run_active_tail_loop(&mut client, &mut ctx, &mut trackers),
        )
        .await?;
        assert_eq!(trackers.cold.finish().completion_ms.samples, 0);
        assert_eq!(trackers.warm.finish().completion_ms.samples, 0);
        assert_eq!(ctx.error_collector.lock().unexpected_grpc_errors, 0);
        Ok(())
    }

    #[test]
    fn drain_committed_delays_handles_post_observation_commit_instants() {
        let base = Instant::now();
        let commit_instants = Mutex::new(BTreeMap::from([
            (10_u32, base),
            (11_u32, base + Duration::from_millis(12)),
        ]));
        let observed_at = base + Duration::from_millis(7);
        let mut lags = Vec::new();

        drain_committed_delays(&commit_instants, 10, observed_at, &mut lags);
        assert_eq!(lags, vec![Duration::from_millis(7)]);
        assert!(commit_instants.lock().contains_key(&11));

        // Height 11 observed at `observed_at` before the writer records the
        // post-commit histogram entry at `base + 12ms`, then drained on exit.
        drain_committed_delays(&commit_instants, 11, observed_at, &mut lags);
        assert_eq!(lags, vec![Duration::from_millis(7), Duration::ZERO]);
        assert!(commit_instants.lock().is_empty());
    }

    #[tokio::test]
    async fn await_next_tip_poll_continues_after_served_sender_drops_when_done()
    -> Result<(), Box<dyn Error>> {
        let (_writer_tx, writer_rx) = watch::channel(WriterProgress::default());
        let (served_tx, served_rx) = watch::channel(ServedState::default());
        let mut ctx = test_ctx(writer_rx, served_rx);
        served_tx.send_modify(|state| state.serving_caught_up_done = true);
        drop(served_tx);
        let _ = *ctx.served_rx.borrow_and_update();

        let mut served_open = true;
        let first = tokio::time::timeout(
            Duration::from_secs(1),
            await_next_tip_poll(&mut ctx, &mut served_open),
        )
        .await?;
        assert!(first);
        assert!(!served_open);

        let second = tokio::time::timeout(
            Duration::from_secs(1),
            await_next_tip_poll(&mut ctx, &mut served_open),
        )
        .await?;
        assert!(second);
        assert!(!served_open);

        let (_writer_tx2, writer_rx2) = watch::channel(WriterProgress::default());
        let (abnormal_tx, abnormal_rx) = watch::channel(ServedState::default());
        let mut abnormal_ctx = test_ctx(writer_rx2, abnormal_rx);
        drop(abnormal_tx);
        let mut abnormal_open = true;
        let abnormal = tokio::time::timeout(
            Duration::from_secs(1),
            await_next_tip_poll(&mut abnormal_ctx, &mut abnormal_open),
        )
        .await?;
        assert!(!abnormal);
        assert!(!abnormal_open);
        Ok(())
    }
}
