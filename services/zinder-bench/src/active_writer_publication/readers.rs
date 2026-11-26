//! Concurrent lightwalletd-compatibility gRPC reader fleet and final-tip
//! byte-level reply parity verifier.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::BenchError;
use parking_lot::Mutex;
use prost::Message as _;
use sha2::{Digest, Sha256};
use tokio::{sync::Notify, task::JoinHandle};
use tonic::Code;
use zinder_compat_lightwalletd::PublishedShieldedHistory;
use zinder_proto::compat::lightwalletd::{
    BlockId, BlockRange, ChainSpec, Empty, GetMempoolTxRequest,
    compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_runtime::AuthenticatedChannel;

use super::{
    mempool::BenchmarkMempoolSurface,
    report::{
        LatencySummaryMs, MempoolTierMetrics, ModeParityDigests, ProtocolErrorCounts,
        StreamLatencyReport, TailRefillMetrics, WriterMode, update_length_prefixed_digest,
    },
};

const CLIENT_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
const MAX_ERROR_SAMPLES: usize = 8;
const MAX_TRANSIENT_RANGE_RETRIES: u32 = 3;
const MAX_QUIESCENT_PAIR_ATTEMPTS: u32 = 6;

/// Shared synchronization and geometry passed to the concurrent reader fleet.
#[derive(Clone)]
pub(crate) struct ReaderFleetSpec {
    pub(crate) mode: WriterMode,
    pub(crate) base_tip_height: u32,
    pub(crate) final_tip_height: u32,
    pub(crate) reorg_window_blocks: u32,
    pub(crate) segment_boundary_height: u32,
    pub(crate) slow_reader_frame_delay: Duration,
    pub(crate) mempool_churn_sizes: Vec<u32>,
    pub(crate) quiescent_rounds: u32,
    pub(crate) sample_v5_tx: Arc<Vec<u8>>,
}

/// Shared coordinator between writer commits and reader measurements.
pub(crate) struct FleetSignals {
    pub(crate) writer_done: AtomicBool,
    pub(crate) highest_served_tip: AtomicU32,
    pub(crate) current_epoch_id: AtomicU64,
    pub(crate) tip_published_wake: Notify,
    pub(crate) commit_instants: Mutex<BTreeMap<u32, Instant>>,
    pub(crate) error_collector: Mutex<ErrorCollector>,
}

#[derive(Default)]
pub(crate) struct ErrorCollector {
    pub(crate) unexpected_grpc_errors: u64,
    pub(crate) unexpected_error_samples: Vec<String>,
    pub(crate) transient_retry_count: u64,
}

impl ErrorCollector {
    fn record_unexpected(&mut self, context: &str, status: &tonic::Status) {
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

impl FleetSignals {
    pub(crate) fn new(initial_tip: u32, initial_epoch_id: u64) -> Arc<Self> {
        Arc::new(Self {
            writer_done: AtomicBool::new(false),
            highest_served_tip: AtomicU32::new(initial_tip),
            current_epoch_id: AtomicU64::new(initial_epoch_id),
            tip_published_wake: Notify::new(),
            commit_instants: Mutex::new(BTreeMap::new()),
            error_collector: Mutex::new(ErrorCollector::default()),
        })
    }

    pub(crate) fn record_commit(&self, height: u32) {
        self.commit_instants.lock().insert(height, Instant::now());
    }
}

/// Aggregated measurements from all concurrent gRPC reader tasks.
pub(crate) struct ReaderFleetOutput {
    pub(crate) commit_to_served_tip_ms: LatencySummaryMs,
    pub(crate) catalog_range: StreamLatencyReport,
    pub(crate) tail_refill: TailRefillMetrics,
    pub(crate) slow_boundary_range: StreamLatencyReport,
    pub(crate) tree_state_ms: LatencySummaryMs,
    pub(crate) mempool_churn: Vec<MempoolTierMetrics>,
}

/// Parameters identifying the parity probe target arm at final tip.
pub(crate) struct ParityProbeRequest<'a> {
    pub(crate) mode: WriterMode,
    pub(crate) label: &'a str,
    pub(crate) base_tip_height: u32,
    pub(crate) final_tip_height: u32,
    pub(crate) segment_boundary_height: u32,
}

/// Connects a `CompactTxStreamerClient` with a raised decoding ceiling.
pub(crate) fn streamer_client(
    channel: AuthenticatedChannel,
) -> CompactTxStreamerClient<AuthenticatedChannel> {
    CompactTxStreamerClient::new(channel)
        .max_decoding_message_size(CLIENT_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(CLIENT_MAX_MESSAGE_BYTES)
}

/// Spawns all concurrent reader tasks against `channel` and returns a single
/// join handle yielding [`ReaderFleetOutput`].
pub(crate) fn spawn_reader_fleet(
    channel: AuthenticatedChannel,
    spec: ReaderFleetSpec,
    signals: Arc<FleetSignals>,
    mempool: Arc<BenchmarkMempoolSurface>,
    history: Arc<PublishedShieldedHistory>,
) -> JoinHandle<Result<ReaderFleetOutput, BenchError>> {
    tokio::spawn(async move {
        let tip_task = tokio::spawn(run_tip_poller(
            channel.clone(),
            spec.clone(),
            Arc::clone(&signals),
        ));
        let catalog_task = tokio::spawn(run_catalog_reader(
            channel.clone(),
            spec.clone(),
            Arc::clone(&signals),
        ));
        let tail_task = tokio::spawn(run_tail_refill_reader(
            channel.clone(),
            spec.clone(),
            Arc::clone(&signals),
            history,
        ));
        let slow_task = tokio::spawn(run_slow_boundary_reader(
            channel.clone(),
            spec.clone(),
            Arc::clone(&signals),
        ));
        let tree_task = tokio::spawn(run_tree_state_reader(
            channel.clone(),
            spec.clone(),
            Arc::clone(&signals),
        ));
        let mempool_task = tokio::spawn(run_mempool_readers(
            channel,
            spec,
            Arc::clone(&signals),
            mempool,
        ));

        let commit_to_served_tip_ms = tip_task
            .await
            .map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let catalog_range = catalog_task
            .await
            .map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let tail_refill = tail_task
            .await
            .map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let slow_boundary_range = slow_task
            .await
            .map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let tree_state_ms = tree_task
            .await
            .map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let mempool_churn = mempool_task
            .await
            .map_err(|err| BenchError::canonical_read(err.to_string()))??;

        Ok(ReaderFleetOutput {
            commit_to_served_tip_ms,
            catalog_range,
            tail_refill,
            slow_boundary_range,
            tree_state_ms,
            mempool_churn,
        })
    })
}

async fn run_tip_poller(
    channel: AuthenticatedChannel,
    spec: ReaderFleetSpec,
    signals: Arc<FleetSignals>,
) -> LatencySummaryMs {
    let mut client = streamer_client(channel);
    let mut lag_samples = Vec::new();
    let rounds = if spec.mode == WriterMode::QuiescentControl {
        spec.quiescent_rounds
    } else {
        u32::MAX
    };

    for _ in 0..rounds {
        let poll_started = Instant::now();
        match client.get_latest_block(ChainSpec {}).await {
            Ok(resp) => {
                let block = resp.into_inner();
                let height = u32::try_from(block.height).unwrap_or(u32::MAX);
                signals
                    .highest_served_tip
                    .fetch_max(height, Ordering::AcqRel);
                if spec.mode == WriterMode::QuiescentControl {
                    lag_samples.push(poll_started.elapsed());
                } else {
                    drain_committed_delays(&signals, height, Instant::now(), &mut lag_samples);
                }
                if height >= spec.final_tip_height && signals.writer_done.load(Ordering::Acquire) {
                    break;
                }
            }
            Err(status) => signals
                .error_collector
                .lock()
                .record_unexpected("GetLatestBlock", &status),
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    LatencySummaryMs::from_durations(&lag_samples)
}

fn drain_committed_delays(
    signals: &FleetSignals,
    served_height: u32,
    observed_at: Instant,
    lag_samples: &mut Vec<Duration>,
) {
    let mut instants = signals.commit_instants.lock();
    let due_heights: Vec<u32> = instants
        .range(..=served_height)
        .map(|(&height, _)| height)
        .collect();
    for height in due_heights {
        if let Some(committed_at) = instants.remove(&height) {
            lag_samples.push(observed_at.saturating_duration_since(committed_at));
        }
    }
}

async fn run_catalog_reader(
    channel: AuthenticatedChannel,
    spec: ReaderFleetSpec,
    signals: Arc<FleetSignals>,
) -> StreamLatencyReport {
    let mut client = streamer_client(channel);
    let mut tracker = StreamStatsTracker::default();
    let base_settled = spec
        .base_tip_height
        .saturating_sub(spec.reorg_window_blocks)
        .max(1);
    let catalog_span = 128_u32.min(base_settled.saturating_sub(1));
    let mut round = 0_u32;

    loop {
        let settled_tip = signals
            .highest_served_tip
            .load(Ordering::Acquire)
            .saturating_sub(spec.reorg_window_blocks)
            .max(1);
        let end_height = settled_tip.min(spec.segment_boundary_height);
        let start_height = end_height.saturating_sub(catalog_span).max(1);
        tracker
            .probe_range(
                &mut client,
                RangeProbe {
                    label: "GetBlockRange(catalog)",
                    request: range_request(start_height, end_height),
                    frame_delay: Duration::ZERO,
                },
                &signals.error_collector,
            )
            .await;

        round = round.saturating_add(1);
        if should_stop_reader(&spec, &signals, round) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    tracker.finish()
}

#[derive(Default)]
struct TailTrackers {
    cold: StreamStatsTracker,
    warm: StreamStatsTracker,
    peak_response_bytes: u64,
    excluded_cross_epoch_pairs: u64,
}

enum TailPairOutcome {
    Recorded,
    CrossEpochExcluded,
    Unverified,
}

async fn run_tail_refill_reader(
    channel: AuthenticatedChannel,
    spec: ReaderFleetSpec,
    signals: Arc<FleetSignals>,
    history: Arc<PublishedShieldedHistory>,
) -> TailRefillMetrics {
    let mut client = streamer_client(channel);
    let mut trackers = TailTrackers::default();
    let start_tel = history.encoded_tail_telemetry();

    if spec.mode == WriterMode::QuiescentControl {
        run_quiescent_tail_loop(&mut client, &spec, &signals, &history, &mut trackers).await;
    } else {
        run_active_tail_loop(&mut client, &spec, &signals, &history, &mut trackers).await;
    }

    let end_tel = history.encoded_tail_telemetry();
    let fills = end_tel.cache_fills.saturating_sub(start_tel.cache_fills);
    let hits = end_tel.cache_hits.saturating_sub(start_tel.cache_hits);
    let evictions = end_tel
        .byte_cap_evictions
        .saturating_sub(start_tel.byte_cap_evictions);
    let capped_fills = end_tel
        .byte_capped_fills
        .saturating_sub(start_tel.byte_capped_fills);

    TailRefillMetrics {
        cold_after_advance: trackers.cold.finish(),
        warm_repeat: trackers.warm.finish(),
        peak_tail_response_bytes: trackers.peak_response_bytes,
        peak_encoded_tail_bytes: end_tel.peak_cached_bytes,
        encoded_tail_byte_cap_evictions: evictions,
        encoded_tail_cache_fills: fills,
        encoded_tail_cache_hits: hits,
        excluded_cross_epoch_pairs: trackers.excluded_cross_epoch_pairs,
        byte_cap_exceeded: capped_fills > 0 || evictions > 0,
    }
}

async fn run_quiescent_tail_loop(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    spec: &ReaderFleetSpec,
    signals: &FleetSignals,
    history: &PublishedShieldedHistory,
    trackers: &mut TailTrackers,
) {
    let tail_offset = 24_u32.min(spec.base_tip_height.saturating_sub(1));
    let mut round = 0_u32;

    while round < spec.quiescent_rounds {
        let current_tip = signals.highest_served_tip.load(Ordering::Acquire);
        let tail_start = current_tip.saturating_sub(tail_offset).max(1);
        let request = range_request(tail_start, current_tip);

        for _ in 0..MAX_QUIESCENT_PAIR_ATTEMPTS {
            history.clear_encoded_tail();
            if matches!(
                probe_tail_pair(client, signals, history, trackers, request.clone()).await,
                TailPairOutcome::Recorded
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        round = round.saturating_add(1);
        if round < spec.quiescent_rounds {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

async fn run_active_tail_loop(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    spec: &ReaderFleetSpec,
    signals: &FleetSignals,
    history: &PublishedShieldedHistory,
    trackers: &mut TailTrackers,
) {
    let mut last_tip = 0_u32;
    let tail_offset = 24_u32.min(spec.base_tip_height.saturating_sub(1));
    let mut round = 0_u32;

    loop {
        let current_tip = signals.highest_served_tip.load(Ordering::Acquire);
        if current_tip > last_tip {
            last_tip = current_tip;
            let tail_start = current_tip.saturating_sub(tail_offset).max(1);
            match probe_tail_pair(
                client,
                signals,
                history,
                trackers,
                range_request(tail_start, current_tip),
            )
            .await
            {
                TailPairOutcome::Recorded => round = round.saturating_add(1),
                TailPairOutcome::CrossEpochExcluded => {
                    trackers.excluded_cross_epoch_pairs =
                        trackers.excluded_cross_epoch_pairs.saturating_add(1);
                }
                TailPairOutcome::Unverified => {}
            }
        }

        if should_stop_reader(spec, signals, round) {
            break;
        }
        tokio::select! {
            () = signals.tip_published_wake.notified() => {}
            () = tokio::time::sleep(Duration::from_millis(3)) => {}
        }
    }

    if trackers.cold.completion.is_empty() {
        let final_tip = signals.highest_served_tip.load(Ordering::Acquire);
        let tail_start = final_tip.saturating_sub(tail_offset).max(1);
        let request = range_request(tail_start, final_tip);
        for _ in 0..MAX_QUIESCENT_PAIR_ATTEMPTS {
            history.clear_encoded_tail();
            if matches!(
                probe_tail_pair(client, signals, history, trackers, request.clone()).await,
                TailPairOutcome::Recorded
            ) {
                break;
            }
        }
    }
}

async fn probe_tail_pair(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    signals: &FleetSignals,
    history: &PublishedShieldedHistory,
    trackers: &mut TailTrackers,
    request: BlockRange,
) -> TailPairOutcome {
    let epoch_before = signals.current_epoch_id.load(Ordering::Acquire);
    let tel_before = history.encoded_tail_telemetry();
    let cold_sample = measure_range_sample(
        client,
        RangeProbe {
            label: "GetBlockRange(tail-cold)",
            request: request.clone(),
            frame_delay: Duration::ZERO,
        },
        &signals.error_collector,
    )
    .await;
    let epoch_mid = signals.current_epoch_id.load(Ordering::Acquire);
    let tel_mid = history.encoded_tail_telemetry();

    let warm_sample = measure_range_sample(
        client,
        RangeProbe {
            label: "GetBlockRange(tail-warm)",
            request,
            frame_delay: Duration::ZERO,
        },
        &signals.error_collector,
    )
    .await;
    let epoch_after = signals.current_epoch_id.load(Ordering::Acquire);
    let tel_after = history.encoded_tail_telemetry();

    let (Some(cold), Some(warm)) = (cold_sample, warm_sample) else {
        return TailPairOutcome::Unverified;
    };
    trackers.peak_response_bytes = trackers
        .peak_response_bytes
        .max(cold.payload_bytes)
        .max(warm.payload_bytes);

    let epoch_changed = epoch_before != epoch_mid || epoch_after != epoch_mid;
    let warm_rebuilt = tel_after.cache_rebuilds > tel_mid.cache_rebuilds;
    let tail_epoch_changed = tel_mid.current_epoch_id != tel_after.current_epoch_id;
    if epoch_changed || warm_rebuilt || tail_epoch_changed {
        return TailPairOutcome::CrossEpochExcluded;
    }
    let cold_filled = tel_mid.cache_rebuilds > tel_before.cache_rebuilds;
    let warm_hit =
        tel_after.cache_hits > tel_mid.cache_hits && tel_after.current_epoch_id.is_some();
    if !cold_filled || !warm_hit {
        return TailPairOutcome::Unverified;
    }

    trackers.cold.record_sample(&cold);
    trackers.warm.record_sample(&warm);
    TailPairOutcome::Recorded
}

async fn run_slow_boundary_reader(
    channel: AuthenticatedChannel,
    spec: ReaderFleetSpec,
    signals: Arc<FleetSignals>,
) -> StreamLatencyReport {
    let mut client = streamer_client(channel);
    let mut tracker = StreamStatsTracker::default();
    let boundary_start = spec.segment_boundary_height.saturating_sub(6).max(1);
    let target_offset = spec.final_tip_height.saturating_sub(boundary_start);
    let fixed_offset = target_offset.min(spec.base_tip_height.saturating_sub(1));
    let mut round = 0_u32;

    loop {
        let current_tip = signals
            .highest_served_tip
            .load(Ordering::Acquire)
            .max(spec.base_tip_height)
            .max(1);
        let start_height = current_tip.saturating_sub(fixed_offset).max(1);
        tracker
            .probe_range(
                &mut client,
                RangeProbe {
                    label: "GetBlockRange(slow-boundary)",
                    request: range_request(start_height, current_tip),
                    frame_delay: spec.slow_reader_frame_delay,
                },
                &signals.error_collector,
            )
            .await;
        round = round.saturating_add(1);
        if should_stop_reader(&spec, &signals, round) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(4)).await;
    }
    tracker.finish()
}

async fn run_tree_state_reader(
    channel: AuthenticatedChannel,
    spec: ReaderFleetSpec,
    signals: Arc<FleetSignals>,
) -> LatencySummaryMs {
    let mut client = streamer_client(channel);
    let mut samples = Vec::new();
    let settled_probe = spec.base_tip_height.saturating_sub(10).max(1);
    let mut round = 0_u32;

    loop {
        let current_tip = signals
            .highest_served_tip
            .load(Ordering::Acquire)
            .max(settled_probe);
        for target_height in [settled_probe, current_tip] {
            let started = Instant::now();
            match client
                .get_tree_state(BlockId {
                    height: u64::from(target_height),
                    hash: Vec::new(),
                })
                .await
            {
                Ok(_) => samples.push(started.elapsed()),
                Err(status) => signals
                    .error_collector
                    .lock()
                    .record_unexpected("GetTreeState", &status),
            }
        }
        round = round.saturating_add(1);
        if should_stop_reader(&spec, &signals, round) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    LatencySummaryMs::from_durations(&samples)
}

async fn run_mempool_readers(
    channel: AuthenticatedChannel,
    spec: ReaderFleetSpec,
    signals: Arc<FleetSignals>,
    mempool: Arc<BenchmarkMempoolSurface>,
) -> Result<Vec<MempoolTierMetrics>, BenchError> {
    let mut client = streamer_client(channel);
    let mut tier_reports = Vec::with_capacity(spec.mempool_churn_sizes.len());

    for (index, &batch_size) in spec.mempool_churn_sizes.iter().enumerate() {
        let round_id = u32::try_from(index + 1).unwrap_or(1);
        let mut tracker = StreamStatsTracker::default();
        mempool.apply_churn_batch(batch_size, round_id, &spec.sample_v5_tx)?;

        let stream_task = (spec.mode != WriterMode::QuiescentControl)
            .then(|| spawn_mempool_stream_watcher(client.clone(), Arc::clone(&signals)));

        let passes = if batch_size >= 500 { 2 } else { 3 };
        for _ in 0..passes {
            probe_mempool_tx(&mut client, &mut tracker, &signals.error_collector).await;
        }

        let closures = if let Some(handle) = stream_task {
            handle.await.unwrap_or(0)
        } else {
            0
        };

        tier_reports.push(MempoolTierMetrics {
            batch_size,
            mempool_tx_stream: tracker.finish(),
            mempool_stream_closures: closures,
        });
    }

    mempool.apply_churn_batch(0, 9_999, &spec.sample_v5_tx)?;
    Ok(tier_reports)
}

fn spawn_mempool_stream_watcher(
    mut stream_client: CompactTxStreamerClient<AuthenticatedChannel>,
    err_ref: Arc<FleetSignals>,
) -> JoinHandle<u64> {
    tokio::spawn(async move {
        let mut closures = 0_u64;
        match stream_client.get_mempool_stream(Empty {}).await {
            Ok(resp) => {
                let mut stream = resp.into_inner();
                let _ = tokio::time::timeout(Duration::from_millis(300), async {
                    loop {
                        match stream.message().await {
                            Ok(Some(_)) => {}
                            Ok(None) => {
                                closures = closures.saturating_add(1);
                                break;
                            }
                            Err(status) => {
                                err_ref
                                    .error_collector
                                    .lock()
                                    .record_unexpected("GetMempoolStream", &status);
                                break;
                            }
                        }
                    }
                })
                .await;
            }
            Err(status) => err_ref
                .error_collector
                .lock()
                .record_unexpected("GetMempoolStream", &status),
        }
        closures
    })
}

async fn probe_mempool_tx(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    tracker: &mut StreamStatsTracker,
    errors: &Mutex<ErrorCollector>,
) {
    let started = Instant::now();
    let request = GetMempoolTxRequest {
        exclude_txid_suffixes: Vec::new(),
        pool_types: Vec::new(),
    };
    let response = client.get_mempool_tx(request).await;
    match response {
        Ok(resp) => {
            let mut stream = resp.into_inner();
            let mut first_elapsed = None;
            let mut frames = 0_u64;
            let mut bytes = 0_u64;
            loop {
                match stream.message().await {
                    Ok(Some(tx)) => {
                        if first_elapsed.is_none() {
                            first_elapsed = Some(started.elapsed());
                        }
                        frames = frames.saturating_add(1);
                        bytes = bytes
                            .saturating_add(u64::try_from(tx.encode_to_vec().len()).unwrap_or(0));
                    }
                    Ok(None) => break,
                    Err(status) => {
                        errors.lock().record_unexpected("GetMempoolTx", &status);
                        return;
                    }
                }
            }
            tracker.record_sample(&StreamSample {
                first_elapsed,
                total_elapsed: started.elapsed(),
                frames,
                payload_bytes: bytes,
            });
        }
        Err(status) => errors.lock().record_unexpected("GetMempoolTx", &status),
    }
}

/// Probes settled catalog, boundary-crossing, tail, and tree-state replies at the
/// final tip and returns deterministic SHA-256 digests for parity checking.
pub(crate) async fn collect_final_tip_parity(
    channel: AuthenticatedChannel,
    req: &ParityProbeRequest<'_>,
) -> Result<ModeParityDigests, BenchError> {
    let mut client = streamer_client(channel);
    let settled_start = req.base_tip_height.saturating_sub(128).max(1);
    let settled_end = req.segment_boundary_height.min(req.final_tip_height);
    let boundary_start = req.segment_boundary_height.saturating_sub(6).max(1);
    let tail_start = req
        .segment_boundary_height
        .saturating_add(1)
        .min(req.final_tip_height);

    let settled_catalog_range_sha256 =
        hash_block_range(&mut client, settled_start, settled_end).await?;
    let boundary_crossing_range_sha256 =
        hash_block_range(&mut client, boundary_start, req.final_tip_height).await?;
    let uncovered_tail_range_sha256 =
        hash_block_range(&mut client, tail_start, req.final_tip_height).await?;
    let tree_states_sha256 = hash_tree_states(
        &mut client,
        &[
            settled_start,
            req.segment_boundary_height,
            req.final_tip_height,
        ],
    )
    .await?;

    Ok(ModeParityDigests {
        mode: req.mode,
        label: req.label.to_owned(),
        settled_catalog_range_sha256,
        boundary_crossing_range_sha256,
        uncovered_tail_range_sha256,
        tree_states_sha256,
    })
}

async fn hash_block_range(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    start: u32,
    end: u32,
) -> Result<String, BenchError> {
    let mut stream = client
        .get_block_range(range_request(start, end))
        .await
        .map_err(|status| BenchError::canonical_read(status.to_string()))?
        .into_inner();
    let mut hasher = Sha256::new();
    while let Some(block) = stream
        .message()
        .await
        .map_err(|status| BenchError::canonical_read(status.to_string()))?
    {
        update_length_prefixed_digest(&mut hasher, &block.encode_to_vec());
    }
    Ok(hex::encode(hasher.finalize()))
}

async fn hash_tree_states(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    heights: &[u32],
) -> Result<String, BenchError> {
    let mut hasher = Sha256::new();
    for &height in heights {
        let state = client
            .get_tree_state(BlockId {
                height: u64::from(height),
                hash: Vec::new(),
            })
            .await
            .map_err(|status| BenchError::canonical_read(status.to_string()))?
            .into_inner();
        update_length_prefixed_digest(&mut hasher, &state.encode_to_vec());
    }
    Ok(hex::encode(hasher.finalize()))
}

#[derive(Clone, Copy)]
struct StreamSample {
    first_elapsed: Option<Duration>,
    total_elapsed: Duration,
    frames: u64,
    payload_bytes: u64,
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
        errors: &Mutex<ErrorCollector>,
    ) -> u64 {
        let Some(sample) = measure_range_sample(client, probe, errors).await else {
            return 0;
        };
        self.record_sample(&sample);
        sample.payload_bytes
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
    errors: &Mutex<ErrorCollector>,
) -> Option<StreamSample> {
    for attempt in 0..=MAX_TRANSIENT_RANGE_RETRIES {
        match try_single_range_stream(client, probe.request.clone(), probe.frame_delay).await {
            Ok(sample) => return Some(sample),
            Err(status)
                if status.code() == Code::Unavailable && attempt < MAX_TRANSIENT_RANGE_RETRIES =>
            {
                {
                    let mut collector = errors.lock();
                    collector.transient_retry_count =
                        collector.transient_retry_count.saturating_add(1);
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Err(status) => {
                errors.lock().record_unexpected(probe.label, &status);
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
) -> Result<StreamSample, tonic::Status> {
    let started = Instant::now();
    let mut stream = client.get_block_range(request).await?.into_inner();
    let mut first_elapsed = None;
    let mut frames = 0_u64;
    let mut payload_bytes = 0_u64;

    while let Some(block) = stream.message().await? {
        if first_elapsed.is_none() {
            first_elapsed = Some(started.elapsed());
        }
        frames = frames.saturating_add(1);
        payload_bytes =
            payload_bytes.saturating_add(u64::try_from(block.encode_to_vec().len()).unwrap_or(0));
        if !frame_delay.is_zero() {
            tokio::time::sleep(frame_delay).await;
        }
    }

    Ok(StreamSample {
        first_elapsed,
        total_elapsed: started.elapsed(),
        frames,
        payload_bytes,
    })
}

fn should_stop_reader(spec: &ReaderFleetSpec, signals: &FleetSignals, round: u32) -> bool {
    if spec.mode == WriterMode::QuiescentControl {
        return round >= spec.quiescent_rounds;
    }
    signals.writer_done.load(Ordering::Acquire)
        && signals.highest_served_tip.load(Ordering::Acquire) >= spec.final_tip_height
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
