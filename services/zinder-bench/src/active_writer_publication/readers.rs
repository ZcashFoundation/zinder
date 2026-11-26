//! Concurrent lightwalletd-compatibility gRPC reader fleet and final-tip
//! byte-level reply parity verifier.

use std::{
    collections::BTreeMap,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::BenchError;
use parking_lot::Mutex;
use prost::{
    Message as _,
    bytes::{Buf as _, Bytes},
};
use sha2::{Digest, Sha256};
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tonic::{
    Code, Status,
    client::Grpc,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    codegen::http::uri::PathAndQuery,
};
use zinder_compat_lightwalletd::{
    EncodedTailLookupEvent, EncodedTailLookupOutcome, EncodedTailTelemetry,
    PublishedShieldedHistory,
};
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
const MAX_QUIESCENT_PAIR_ATTEMPTS: u32 = 8;
const GET_BLOCK_RANGE_PATH: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange";
const GET_TREE_STATE_PATH: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTreeState";

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
    pub(crate) writer_commits_done: AtomicBool,
    pub(crate) fleet_cancel: CancellationToken,
    pub(crate) highest_served_tip: AtomicU32,
    pub(crate) current_epoch_id: AtomicU64,
    pub(crate) commits_completed: AtomicU32,
    pub(crate) tip_published_wake: Notify,
    pub(crate) commit_wake: Notify,
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

impl FleetSignals {
    pub(crate) fn new(initial_tip: u32, initial_epoch_id: u64) -> Arc<Self> {
        Arc::new(Self {
            writer_done: AtomicBool::new(false),
            writer_commits_done: AtomicBool::new(false),
            fleet_cancel: CancellationToken::new(),
            highest_served_tip: AtomicU32::new(initial_tip),
            current_epoch_id: AtomicU64::new(initial_epoch_id),
            commits_completed: AtomicU32::new(0),
            tip_published_wake: Notify::new(),
            commit_wake: Notify::new(),
            commit_instants: Mutex::new(BTreeMap::new()),
            error_collector: Mutex::new(ErrorCollector::default()),
        })
    }

    pub(crate) fn record_commit(&self, height: u32) {
        self.commit_instants.lock().insert(height, Instant::now());
        self.commits_completed.fetch_add(1, Ordering::AcqRel);
        self.commit_wake.notify_waiters();
    }

    pub(crate) fn mark_writer_commits_done(&self) {
        self.writer_commits_done.store(true, Ordering::Release);
        self.commit_wake.notify_waiters();
        self.tip_published_wake.notify_waiters();
    }
}

/// Aggregated measurements from all concurrent gRPC reader tasks.
pub(crate) struct ReaderFleetOutput {
    pub(crate) commit_to_served_tip_ms: LatencySummaryMs,
    pub(crate) catalog_range: StreamLatencyReport,
    pub(crate) catalog_range_unpublished: StreamLatencyReport,
    pub(crate) catalog_range_published: StreamLatencyReport,
    pub(crate) tail_refill: TailRefillMetrics,
    pub(crate) slow_boundary_range: StreamLatencyReport,
    pub(crate) tree_state_ms: LatencySummaryMs,
    pub(crate) mempool_churn: Vec<MempoolTierMetrics>,
}

/// Structured handle managing the reader fleet lifecycle and child drainage.
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

/// Separate channels and request-scoped tail telemetry shared with the fleet.
pub(crate) struct ReaderFleetChannels {
    pub(crate) fleet: AuthenticatedChannel,
    pub(crate) tail: AuthenticatedChannel,
    pub(crate) tail_lookup_events: Arc<Mutex<Vec<EncodedTailLookupEvent>>>,
}

/// Parameters identifying the parity probe target arm at final tip.
pub(crate) struct ParityProbeRequest<'a> {
    pub(crate) mode: WriterMode,
    pub(crate) label: &'a str,
    pub(crate) base_tip_height: u32,
    pub(crate) final_tip_height: u32,
    pub(crate) segment_boundary_height: u32,
}

#[derive(Default)]
struct CatalogReaderOutput {
    combined: StreamLatencyReport,
    unpublished: StreamLatencyReport,
    published: StreamLatencyReport,
}

/// Connects a `CompactTxStreamerClient` with a raised decoding ceiling.
pub(crate) fn streamer_client(
    channel: AuthenticatedChannel,
) -> CompactTxStreamerClient<AuthenticatedChannel> {
    CompactTxStreamerClient::new(channel)
        .max_decoding_message_size(CLIENT_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(CLIENT_MAX_MESSAGE_BYTES)
}

/// Spawns all concurrent reader tasks against `channels` and returns a
/// structured handle that joins or cancels and drains every child task cleanly.
pub(crate) fn spawn_reader_fleet(
    channels: ReaderFleetChannels,
    spec: ReaderFleetSpec,
    signals: Arc<FleetSignals>,
    mempool: Arc<BenchmarkMempoolSurface>,
    history: Arc<PublishedShieldedHistory>,
) -> ReaderFleetHandle {
    let cancel = signals.fleet_cancel.clone();
    let ReaderFleetChannels {
        fleet,
        tail,
        tail_lookup_events,
    } = channels;
    let task = tokio::spawn(async move {
        let tip_task = tokio::spawn(run_tip_poller(
            fleet.clone(),
            spec.clone(),
            Arc::clone(&signals),
        ));
        let catalog_task = tokio::spawn(run_catalog_reader(
            fleet.clone(),
            spec.clone(),
            Arc::clone(&signals),
            Arc::clone(&history),
        ));
        let tail_task = tokio::spawn(run_tail_refill_reader(
            (tail, tail_lookup_events),
            spec.clone(),
            Arc::clone(&signals),
            history,
        ));
        let slow_task = tokio::spawn(run_slow_boundary_reader(
            fleet.clone(),
            spec.clone(),
            Arc::clone(&signals),
        ));
        let tree_task = tokio::spawn(run_tree_state_reader(
            fleet.clone(),
            spec.clone(),
            Arc::clone(&signals),
        ));
        let mempool_task = tokio::spawn(run_mempool_readers(
            fleet,
            spec,
            Arc::clone(&signals),
            mempool,
        ));

        let (tip_res, cat_res, tail_res, slow_res, tree_res, mempool_res) = tokio::join!(
            tip_task,
            catalog_task,
            tail_task,
            slow_task,
            tree_task,
            mempool_task,
        );

        let commit_to_served_tip_ms =
            tip_res.map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let catalog_out = cat_res.map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let tail_refill = tail_res.map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let slow_boundary_range =
            slow_res.map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let tree_state_ms = tree_res.map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let mempool_churn =
            mempool_res.map_err(|err| BenchError::canonical_read(err.to_string()))??;

        Ok(ReaderFleetOutput {
            commit_to_served_tip_ms,
            catalog_range: catalog_out.combined,
            catalog_range_unpublished: catalog_out.unpublished,
            catalog_range_published: catalog_out.published,
            tail_refill,
            slow_boundary_range,
            tree_state_ms,
            mempool_churn,
        })
    });
    ReaderFleetHandle { cancel, task }
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
        let rpc_res = tokio::select! {
            biased;
            () = signals.fleet_cancel.cancelled() => break,
            rpc = client.get_latest_block(ChainSpec {}) => rpc,
        };
        match rpc_res {
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
            Err(status) if !signals.fleet_cancel.is_cancelled() => signals
                .error_collector
                .lock()
                .record_unexpected("GetLatestBlock", &status),
            Err(_) => break,
        }
        tokio::select! {
            () = signals.fleet_cancel.cancelled() => break,
            () = tokio::time::sleep(Duration::from_millis(2)) => {}
        }
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
    history: Arc<PublishedShieldedHistory>,
) -> CatalogReaderOutput {
    let mut client = streamer_client(channel);
    let mut combined = StreamStatsTracker::default();
    let mut unpublished = StreamStatsTracker::default();
    let mut published = StreamStatsTracker::default();
    let base_settled = spec
        .base_tip_height
        .saturating_sub(spec.reorg_window_blocks)
        .max(1);
    let catalog_span = 128_u32.min(base_settled.saturating_sub(1));
    let mut round = 0_u32;

    loop {
        if signals.fleet_cancel.is_cancelled() {
            break;
        }
        let settled_tip = signals
            .highest_served_tip
            .load(Ordering::Acquire)
            .saturating_sub(spec.reorg_window_blocks)
            .max(1);
        let end_height = settled_tip.min(spec.segment_boundary_height);
        let start_height = end_height.saturating_sub(catalog_span).max(1);
        let before_end = history.encoded_tail_telemetry().catalog_end_height;
        let sample_opt = measure_range_sample(
            &mut client,
            RangeProbe {
                label: "GetBlockRange(catalog)",
                request: range_request(start_height, end_height),
                frame_delay: Duration::ZERO,
            },
            &signals,
        )
        .await;
        let after_end = history.encoded_tail_telemetry().catalog_end_height;

        if let Some(sample) = sample_opt {
            combined.record_sample(&sample);
            record_catalog_phase_sample(
                before_end,
                after_end,
                end_height,
                &sample,
                (&mut unpublished, &mut published),
            );
        }

        round = round.saturating_add(1);
        if should_stop_reader(&spec, &signals, round) {
            break;
        }
        tokio::select! {
            () = signals.fleet_cancel.cancelled() => break,
            () = tokio::time::sleep(Duration::from_millis(2)) => {}
        }
    }

    CatalogReaderOutput {
        combined: combined.finish(),
        unpublished: unpublished.finish(),
        published: published.finish(),
    }
}

fn record_catalog_phase_sample(
    before_end: Option<u32>,
    after_end: Option<u32>,
    end_height: u32,
    sample: &StreamSample,
    (unpublished, published): (&mut StreamStatsTracker, &mut StreamStatsTracker),
) {
    let covered_before = before_end.is_some_and(|cat_end| cat_end >= end_height);
    let covered_after = after_end.is_some_and(|cat_end| cat_end >= end_height);
    match (covered_before, covered_after) {
        (false, false) => unpublished.record_sample(sample),
        (true, true) => published.record_sample(sample),
        _ => {}
    }
}

#[derive(Default)]
struct TailTrackers {
    cold: StreamStatsTracker,
    warm: StreamStatsTracker,
    peak_response_bytes: u64,
    excluded_cross_epoch_pairs: u64,
}

#[derive(Clone, Copy)]
struct TailProbeSnap {
    telemetry: EncodedTailTelemetry,
}

#[derive(Clone, Copy)]
struct TailProbeContext<'a> {
    signals: &'a FleetSignals,
    history: &'a PublishedShieldedHistory,
    tail_events: &'a Mutex<Vec<EncodedTailLookupEvent>>,
}

#[derive(Clone, Copy)]
struct TailPairObservation<'a> {
    mid: TailProbeSnap,
    after: TailProbeSnap,
    cold_events: &'a [EncodedTailLookupEvent],
    warm_events: &'a [EncodedTailLookupEvent],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TailPairOutcome {
    Recorded,
    CrossEpochExcluded,
    Unverified,
}

async fn run_tail_refill_reader(
    (channel, tail_events): (
        AuthenticatedChannel,
        Arc<Mutex<Vec<EncodedTailLookupEvent>>>,
    ),
    spec: ReaderFleetSpec,
    signals: Arc<FleetSignals>,
    history: Arc<PublishedShieldedHistory>,
) -> TailRefillMetrics {
    let mut client = streamer_client(channel);
    let mut trackers = TailTrackers::default();
    let start_tel = history.encoded_tail_telemetry();
    let tail_ctx = TailProbeContext {
        signals: &signals,
        history: &history,
        tail_events: &tail_events,
    };

    if spec.mode == WriterMode::QuiescentControl {
        run_quiescent_tail_loop(&mut client, &spec, tail_ctx, &mut trackers).await;
    } else {
        run_active_tail_loop(&mut client, &spec, tail_ctx, &mut trackers).await;
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
    tail_ctx: TailProbeContext<'_>,
    trackers: &mut TailTrackers,
) {
    let tail_offset = 24_u32.min(spec.base_tip_height.saturating_sub(1));
    let mut round = 0_u32;

    while round < spec.quiescent_rounds && !tail_ctx.signals.fleet_cancel.is_cancelled() {
        let current_tip = tail_ctx.signals.highest_served_tip.load(Ordering::Acquire);
        let tail_start = current_tip.saturating_sub(tail_offset).max(1);
        let request = range_request(tail_start, current_tip);

        for _ in 0..MAX_QUIESCENT_PAIR_ATTEMPTS {
            tail_ctx.history.clear_encoded_tail();
            if probe_tail_pair(client, tail_ctx, trackers, request.clone()).await
                == TailPairOutcome::Recorded
            {
                break;
            }
            tokio::select! {
                () = tail_ctx.signals.fleet_cancel.cancelled() => return,
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
        round = round.saturating_add(1);
        if round < spec.quiescent_rounds {
            tokio::select! {
                () = tail_ctx.signals.fleet_cancel.cancelled() => break,
                () = tokio::time::sleep(Duration::from_millis(2)) => {}
            }
        }
    }
}

async fn run_active_tail_loop(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    spec: &ReaderFleetSpec,
    tail_ctx: TailProbeContext<'_>,
    trackers: &mut TailTrackers,
) {
    let mut last_tip = spec.base_tip_height;
    let tail_offset = 24_u32.min(spec.base_tip_height.saturating_sub(1));

    loop {
        let tip_wake = tail_ctx.signals.tip_published_wake.notified();
        tokio::pin!(tip_wake);
        tip_wake.as_mut().enable();
        if tail_ctx.signals.fleet_cancel.is_cancelled()
            || tail_ctx.signals.writer_commits_done.load(Ordering::Acquire)
        {
            break;
        }
        let observed_tip = tail_ctx.signals.highest_served_tip.load(Ordering::Acquire);
        if observed_tip > last_tip {
            last_tip = observed_tip;
            let tail_start = observed_tip.saturating_sub(tail_offset).max(1);
            if probe_tail_pair(
                client,
                tail_ctx,
                trackers,
                range_request(tail_start, observed_tip),
            )
            .await
                == TailPairOutcome::CrossEpochExcluded
            {
                trackers.excluded_cross_epoch_pairs =
                    trackers.excluded_cross_epoch_pairs.saturating_add(1);
            }
            continue;
        }

        tokio::select! {
            biased;
            () = tail_ctx.signals.fleet_cancel.cancelled() => break,
            () = &mut tip_wake => {}
            () = tokio::time::sleep(Duration::from_millis(2)) => {}
        }
    }
}

fn snap_tail(history: &PublishedShieldedHistory) -> TailProbeSnap {
    TailProbeSnap {
        telemetry: history.encoded_tail_telemetry(),
    }
}

fn classify_tail_pair(obs: TailPairObservation<'_>) -> TailPairOutcome {
    let Some(&first_cold) = obs.cold_events.first() else {
        return TailPairOutcome::Unverified;
    };
    if obs.warm_events.is_empty() {
        return TailPairOutcome::Unverified;
    }

    let target_epoch = first_cold.epoch_id;
    let target_gen = first_cold.catalog_generation;
    let same_epoch_and_gen = obs
        .cold_events
        .iter()
        .chain(obs.warm_events.iter())
        .all(|ev| ev.epoch_id == target_epoch && ev.catalog_generation == target_gen);
    let snap_catalog_stable = obs.mid.telemetry.catalog_generation == target_gen
        && obs.after.telemetry.catalog_generation == target_gen;
    let warm_mutated_cache = obs.warm_events.iter().any(|ev| {
        matches!(
            ev.outcome,
            EncodedTailLookupOutcome::Rebuild | EncodedTailLookupOutcome::Prepend
        )
    });
    if !same_epoch_and_gen || !snap_catalog_stable || warm_mutated_cache {
        return TailPairOutcome::CrossEpochExcluded;
    }

    let cold_rebuilt = first_cold.outcome == EncodedTailLookupOutcome::Rebuild
        && obs.cold_events.iter().all(|ev| {
            matches!(
                ev.outcome,
                EncodedTailLookupOutcome::Rebuild | EncodedTailLookupOutcome::Hit
            )
        });
    let warm_hit = obs
        .warm_events
        .iter()
        .all(|ev| ev.outcome == EncodedTailLookupOutcome::Hit);
    if cold_rebuilt && warm_hit {
        TailPairOutcome::Recorded
    } else {
        TailPairOutcome::Unverified
    }
}

async fn probe_tail_pair(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    tail_ctx: TailProbeContext<'_>,
    trackers: &mut TailTrackers,
    request: BlockRange,
) -> TailPairOutcome {
    let cold_out = measure_observed_range_sample(
        client,
        RangeProbe {
            label: "GetBlockRange(tail-cold)",
            request: request.clone(),
            frame_delay: Duration::ZERO,
        },
        tail_ctx.signals,
        tail_ctx.tail_events,
    )
    .await;
    let mid = snap_tail(tail_ctx.history);

    let warm_out = measure_observed_range_sample(
        client,
        RangeProbe {
            label: "GetBlockRange(tail-warm)",
            request,
            frame_delay: Duration::ZERO,
        },
        tail_ctx.signals,
        tail_ctx.tail_events,
    )
    .await;
    let after = snap_tail(tail_ctx.history);

    let (Some((cold, cold_events)), Some((warm, warm_events))) = (cold_out, warm_out) else {
        return TailPairOutcome::Unverified;
    };
    trackers.peak_response_bytes = trackers
        .peak_response_bytes
        .max(cold.payload_bytes)
        .max(warm.payload_bytes);

    let outcome = classify_tail_pair(TailPairObservation {
        mid,
        after,
        cold_events: &cold_events,
        warm_events: &warm_events,
    });
    if outcome == TailPairOutcome::Recorded {
        trackers.cold.record_sample(&cold);
        trackers.warm.record_sample(&warm);
    }
    outcome
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
        if signals.fleet_cancel.is_cancelled() {
            break;
        }
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
                &signals,
            )
            .await;
        round = round.saturating_add(1);
        if should_stop_reader(&spec, &signals, round) {
            break;
        }
        tokio::select! {
            () = signals.fleet_cancel.cancelled() => break,
            () = tokio::time::sleep(Duration::from_millis(4)) => {}
        }
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
            let rpc_res = tokio::select! {
                biased;
                () = signals.fleet_cancel.cancelled() => return LatencySummaryMs::from_durations(&samples),
                rpc = client.get_tree_state(BlockId {
                    height: u64::from(target_height),
                    hash: Vec::new(),
                }) => rpc,
            };
            match rpc_res {
                Ok(_) => samples.push(started.elapsed()),
                Err(status) if !signals.fleet_cancel.is_cancelled() => signals
                    .error_collector
                    .lock()
                    .record_unexpected("GetTreeState", &status),
                Err(_) => return LatencySummaryMs::from_durations(&samples),
            }
        }
        round = round.saturating_add(1);
        if should_stop_reader(&spec, &signals, round) {
            break;
        }
        tokio::select! {
            () = signals.fleet_cancel.cancelled() => break,
            () = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
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
    let mut pending_watchers = Vec::with_capacity(spec.mempool_churn_sizes.len());

    for (index, &batch_size) in spec.mempool_churn_sizes.iter().enumerate() {
        if signals.fleet_cancel.is_cancelled() {
            break;
        }
        let tier = run_mempool_tier(
            &mut client,
            (index, batch_size),
            (&spec, &signals, &mempool),
            &mut pending_watchers,
        )
        .await?;
        tier_reports.push(tier);
    }

    drain_mempool_watchers(pending_watchers, &signals, &mut tier_reports).await;
    mempool.apply_churn_batch(0, 9_999, &spec.sample_v5_tx)?;
    Ok(tier_reports)
}

async fn run_mempool_tier(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    (index, batch_size): (usize, u32),
    (spec, signals, mempool): (
        &ReaderFleetSpec,
        &Arc<FleetSignals>,
        &BenchmarkMempoolSurface,
    ),
    pending_watchers: &mut Vec<(usize, JoinHandle<u64>)>,
) -> Result<MempoolTierMetrics, BenchError> {
    let required_commits = u32::try_from(index).unwrap_or(u32::MAX);
    while spec.mode != WriterMode::QuiescentControl
        && signals.commits_completed.load(Ordering::Acquire) < required_commits
        && !signals.writer_commits_done.load(Ordering::Acquire)
    {
        tokio::select! {
            biased;
            () = signals.fleet_cancel.cancelled() => break,
            () = signals.commit_wake.notified() => {}
        }
    }

    let commits_before = signals.commits_completed.load(Ordering::Acquire);
    let active_at_start = !signals.writer_commits_done.load(Ordering::Acquire);
    let round_id = u32::try_from(index + 1).unwrap_or(1);
    let mut tracker = StreamStatsTracker::default();
    mempool.apply_churn_batch(batch_size, round_id, &spec.sample_v5_tx)?;

    if spec.mode != WriterMode::QuiescentControl && !signals.fleet_cancel.is_cancelled() {
        pending_watchers.push((
            index,
            spawn_mempool_stream_watcher(client.clone(), Arc::clone(signals)),
        ));
    }

    let passes = if batch_size >= 500 { 2 } else { 3 };
    for pass in 0..passes {
        if signals.fleet_cancel.is_cancelled() {
            break;
        }
        if spec.mode != WriterMode::QuiescentControl
            && pass + 1 == passes
            && signals.commits_completed.load(Ordering::Acquire) == commits_before
            && !signals.writer_commits_done.load(Ordering::Acquire)
        {
            tokio::select! {
                biased;
                () = signals.fleet_cancel.cancelled() => break,
                () = signals.commit_wake.notified() => {}
            }
        }
        probe_mempool_tx(client, &mut tracker, signals).await;
    }

    let commits_after = signals.commits_completed.load(Ordering::Acquire);
    let active_at_end = !signals.writer_commits_done.load(Ordering::Acquire);
    let commits_observed_during_tier = commits_after.saturating_sub(commits_before);
    let writer_overlap_verified = spec.mode != WriterMode::QuiescentControl
        && active_at_start
        && (commits_observed_during_tier > 0 || active_at_end);

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
    signals: &FleetSignals,
    tier_reports: &mut [MempoolTierMetrics],
) {
    for (tier_index, handle) in pending_watchers {
        if signals.fleet_cancel.is_cancelled() {
            handle.abort();
        }
        let closures = handle.await.unwrap_or(0);
        if let Some(report) = tier_reports.get_mut(tier_index) {
            report.mempool_stream_closures = closures;
        }
    }
}

fn spawn_mempool_stream_watcher(
    mut stream_client: CompactTxStreamerClient<AuthenticatedChannel>,
    err_ref: Arc<FleetSignals>,
) -> JoinHandle<u64> {
    tokio::spawn(async move {
        let mut closures = 0_u64;
        let open_res = tokio::select! {
            biased;
            () = err_ref.fleet_cancel.cancelled() => return 0,
            resp = stream_client.get_mempool_stream(Empty {}) => resp,
        };
        match open_res {
            Ok(resp) => {
                let mut stream = resp.into_inner();
                let _ = tokio::time::timeout(Duration::from_millis(300), async {
                    loop {
                        let msg = tokio::select! {
                            biased;
                            () = err_ref.fleet_cancel.cancelled() => break,
                            frame = stream.message() => frame,
                        };
                        match msg {
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
            Err(status) if !err_ref.fleet_cancel.is_cancelled() => err_ref
                .error_collector
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
    signals: &FleetSignals,
) {
    let started = Instant::now();
    let request = GetMempoolTxRequest {
        exclude_txid_suffixes: Vec::new(),
        pool_types: Vec::new(),
    };
    let response = tokio::select! {
        biased;
        () = signals.fleet_cancel.cancelled() => return,
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
                    () = signals.fleet_cancel.cancelled() => return,
                    entry = stream.message() => entry,
                };
                match msg {
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
                        if !signals.fleet_cancel.is_cancelled() {
                            signals
                                .error_collector
                                .lock()
                                .record_unexpected("GetMempoolTx", &status);
                        }
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
        Err(status) if !signals.fleet_cancel.is_cancelled() => signals
            .error_collector
            .lock()
            .record_unexpected("GetMempoolTx", &status),
        Err(_) => {}
    }
}

struct RawReplyCodec<Req>(PhantomData<fn(Req)>);

impl<Req> Default for RawReplyCodec<Req> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

struct ProtoRequestEncoder<Req>(PhantomData<fn(Req)>);
struct RawBytesDecoder;

impl<Req: prost::Message> Encoder for ProtoRequestEncoder<Req> {
    type Item = Req;
    type Error = Status;

    fn encode(&mut self, request: Req, buffer: &mut EncodeBuf<'_>) -> Result<(), Status> {
        request
            .encode(buffer)
            .map_err(|err| Status::internal(err.to_string()))
    }
}

impl Decoder for RawBytesDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, frame: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(frame.copy_to_bytes(frame.remaining())))
    }
}

impl<Req: prost::Message + Default + Send + Sync + 'static> Codec for RawReplyCodec<Req> {
    type Encode = Req;
    type Decode = Bytes;
    type Encoder = ProtoRequestEncoder<Req>;
    type Decoder = RawBytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        ProtoRequestEncoder(PhantomData)
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawBytesDecoder
    }
}

/// Probes settled catalog, boundary-crossing, tail, and tree-state replies at the
/// final tip using raw response-frame payloads for wire-byte parity checking.
pub(crate) async fn collect_final_tip_parity(
    channel: AuthenticatedChannel,
    req: &ParityProbeRequest<'_>,
) -> Result<ModeParityDigests, BenchError> {
    let mut grpc = Grpc::new(channel)
        .max_decoding_message_size(CLIENT_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(CLIENT_MAX_MESSAGE_BYTES);
    let settled_start = req.base_tip_height.saturating_sub(128).max(1);
    let settled_end = req.segment_boundary_height.min(req.final_tip_height);
    let boundary_start = req.segment_boundary_height.saturating_sub(6).max(1);
    let tail_start = req
        .segment_boundary_height
        .saturating_add(1)
        .min(req.final_tip_height);

    let settled_catalog_range_sha256 =
        hash_block_range(&mut grpc, settled_start, settled_end).await?;
    let boundary_crossing_range_sha256 =
        hash_block_range(&mut grpc, boundary_start, req.final_tip_height).await?;
    let uncovered_tail_range_sha256 =
        hash_block_range(&mut grpc, tail_start, req.final_tip_height).await?;
    let tree_states_sha256 = hash_tree_states(
        &mut grpc,
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
    grpc: &mut Grpc<AuthenticatedChannel>,
    start: u32,
    end: u32,
) -> Result<String, BenchError> {
    grpc.ready()
        .await
        .map_err(|err| BenchError::canonical_read(err.to_string()))?;
    let mut stream = grpc
        .server_streaming(
            tonic::Request::new(range_request(start, end)),
            PathAndQuery::from_static(GET_BLOCK_RANGE_PATH),
            RawReplyCodec::<BlockRange>::default(),
        )
        .await
        .map_err(|status| BenchError::canonical_read(status.to_string()))?
        .into_inner();
    let mut hasher = Sha256::new();
    while let Some(raw_frame) = stream
        .message()
        .await
        .map_err(|status| BenchError::canonical_read(status.to_string()))?
    {
        update_length_prefixed_digest(&mut hasher, &raw_frame);
    }
    Ok(hex::encode(hasher.finalize()))
}

async fn hash_tree_states(
    grpc: &mut Grpc<AuthenticatedChannel>,
    heights: &[u32],
) -> Result<String, BenchError> {
    let mut hasher = Sha256::new();
    for &height in heights {
        grpc.ready()
            .await
            .map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let raw_payload = grpc
            .unary(
                tonic::Request::new(BlockId {
                    height: u64::from(height),
                    hash: Vec::new(),
                }),
                PathAndQuery::from_static(GET_TREE_STATE_PATH),
                RawReplyCodec::<BlockId>::default(),
            )
            .await
            .map_err(|status| BenchError::canonical_read(status.to_string()))?
            .into_inner();
        update_length_prefixed_digest(&mut hasher, &raw_payload);
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
        signals: &FleetSignals,
    ) -> u64 {
        let Some(sample) = measure_range_sample(client, probe, signals).await else {
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
    signals: &FleetSignals,
) -> Option<StreamSample> {
    measure_range_sample_with_events(client, probe, signals, None)
        .await
        .map(|(sample, _)| sample)
}

async fn measure_observed_range_sample(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    probe: RangeProbe<'_>,
    signals: &FleetSignals,
    tail_events: &Mutex<Vec<EncodedTailLookupEvent>>,
) -> Option<(StreamSample, Vec<EncodedTailLookupEvent>)> {
    measure_range_sample_with_events(client, probe, signals, Some(tail_events)).await
}

async fn measure_range_sample_with_events(
    client: &mut CompactTxStreamerClient<AuthenticatedChannel>,
    probe: RangeProbe<'_>,
    signals: &FleetSignals,
    tail_events: Option<&Mutex<Vec<EncodedTailLookupEvent>>>,
) -> Option<(StreamSample, Vec<EncodedTailLookupEvent>)> {
    for attempt in 0..=MAX_TRANSIENT_RANGE_RETRIES {
        if let Some(events) = tail_events {
            events.lock().clear();
        }
        let stream_fut = try_single_range_stream(client, probe.request.clone(), probe.frame_delay);
        let step = tokio::select! {
            biased;
            () = signals.fleet_cancel.cancelled() => return None,
            res = stream_fut => res,
        };
        match step {
            Ok(sample) => {
                let recorded =
                    tail_events.map_or_else(Vec::new, |events| std::mem::take(&mut *events.lock()));
                return Some((sample, recorded));
            }
            Err(status)
                if status.code() == Code::Unavailable && attempt < MAX_TRANSIENT_RANGE_RETRIES =>
            {
                {
                    let mut collector = signals.error_collector.lock();
                    collector.transient_retry_count =
                        collector.transient_retry_count.saturating_add(1);
                }
                tokio::select! {
                    () = signals.fleet_cancel.cancelled() => return None,
                    () = tokio::time::sleep(Duration::from_millis(2)) => {}
                }
            }
            Err(status) => {
                if !signals.fleet_cancel.is_cancelled() {
                    signals
                        .error_collector
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
) -> Result<StreamSample, Status> {
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
    if signals.fleet_cancel.is_cancelled() {
        return true;
    }
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

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        error::Error,
        marker::PhantomData,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::Duration,
    };

    use parking_lot::Mutex;
    use prost::{
        Message as _,
        bytes::{Buf as _, BufMut as _, Bytes},
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{
        Request, Response, Status,
        body::Body,
        client::Grpc,
        codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
        server::{NamedService, ServerStreamingService, UnaryService},
        transport::Server,
    };
    use zinder_compat_lightwalletd::{
        EncodedTailLookupEvent, EncodedTailLookupOutcome, EncodedTailTelemetry,
    };
    use zinder_proto::compat::lightwalletd::{BlockId, BlockRange, CompactBlock, TreeState};
    use zinder_runtime::connect_zinder_grpc_with_timeout;

    use super::{
        CLIENT_MAX_MESSAGE_BYTES, TailPairObservation, TailPairOutcome, TailProbeSnap,
        classify_tail_pair, hash_block_range, hash_tree_states,
    };

    #[test]
    fn classify_tail_pair_rejects_same_epoch_catalog_extension_mid_pair() {
        let base_tel = EncodedTailTelemetry {
            cache_hits: 0,
            cache_fills: 1,
            cache_rebuilds: 1,
            byte_capped_fills: 0,
            byte_cap_evictions: 0,
            peak_cached_bytes: 1_024,
            current_cached_bytes: 1_024,
            current_epoch_id: Some(42),
            catalog_generation: 1,
            catalog_end_height: Some(64),
        };
        let mid = TailProbeSnap {
            telemetry: base_tel,
        };
        let after = TailProbeSnap {
            telemetry: EncodedTailTelemetry {
                cache_hits: 1,
                catalog_generation: 2,
                catalog_end_height: Some(68),
                ..base_tel
            },
        };
        let cold_events = [EncodedTailLookupEvent {
            catalog_generation: 1,
            catalog_end_height: Some(64),
            epoch_id: 42,
            outcome: EncodedTailLookupOutcome::Rebuild,
        }];
        let warm_events = [EncodedTailLookupEvent {
            catalog_generation: 2,
            catalog_end_height: Some(68),
            epoch_id: 42,
            outcome: EncodedTailLookupOutcome::Hit,
        }];
        assert_eq!(
            classify_tail_pair(TailPairObservation {
                mid,
                after,
                cold_events: &cold_events,
                warm_events: &warm_events,
            }),
            TailPairOutcome::CrossEpochExcluded
        );
    }

    #[test]
    fn classify_tail_pair_rejects_concurrent_rebuild_without_this_probe_fill() {
        let base_tel = EncodedTailTelemetry {
            cache_hits: 1,
            cache_fills: 1,
            cache_rebuilds: 1,
            byte_capped_fills: 0,
            byte_cap_evictions: 0,
            peak_cached_bytes: 1_024,
            current_cached_bytes: 1_024,
            current_epoch_id: Some(42),
            catalog_generation: 1,
            catalog_end_height: Some(64),
        };
        let mid = TailProbeSnap {
            telemetry: base_tel,
        };
        let after = TailProbeSnap {
            telemetry: EncodedTailTelemetry {
                cache_hits: 2,
                ..base_tel
            },
        };
        let cold_events = [EncodedTailLookupEvent {
            catalog_generation: 1,
            catalog_end_height: Some(64),
            epoch_id: 42,
            outcome: EncodedTailLookupOutcome::Hit,
        }];
        let warm_events = [EncodedTailLookupEvent {
            catalog_generation: 1,
            catalog_end_height: Some(64),
            epoch_id: 42,
            outcome: EncodedTailLookupOutcome::Hit,
        }];
        assert_eq!(
            classify_tail_pair(TailPairObservation {
                mid,
                after,
                cold_events: &cold_events,
                warm_events: &warm_events,
            }),
            TailPairOutcome::Unverified
        );
    }

    struct RawSendCodec<Req>(PhantomData<fn(Req)>);

    impl<Req> Default for RawSendCodec<Req> {
        fn default() -> Self {
            Self(PhantomData)
        }
    }

    struct BytesEncoder;
    struct ProtoRequestDecoder<Req>(PhantomData<fn(Req)>);

    impl Encoder for BytesEncoder {
        type Item = Bytes;
        type Error = Status;

        fn encode(&mut self, bytes: Bytes, buf: &mut EncodeBuf<'_>) -> Result<(), Status> {
            buf.put_slice(&bytes);
            Ok(())
        }
    }

    impl<Req: prost::Message + Default> Decoder for ProtoRequestDecoder<Req> {
        type Item = Req;
        type Error = Status;

        fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Req>, Status> {
            let chunk = buf.copy_to_bytes(buf.remaining());
            Req::decode(chunk)
                .map(Some)
                .map_err(|err| Status::invalid_argument(err.to_string()))
        }
    }

    impl<Req: prost::Message + Default + Send + Sync + 'static> Codec for RawSendCodec<Req> {
        type Encode = Bytes;
        type Decode = Req;
        type Encoder = BytesEncoder;
        type Decoder = ProtoRequestDecoder<Req>;

        fn encoder(&mut self) -> Self::Encoder {
            BytesEncoder
        }

        fn decoder(&mut self) -> Self::Decoder {
            ProtoRequestDecoder(PhantomData)
        }
    }

    #[derive(Clone)]
    struct FixedRawStreamer {
        block_frames: Arc<Mutex<VecDeque<Vec<Bytes>>>>,
        tree_replies: Arc<Mutex<VecDeque<Bytes>>>,
    }

    impl NamedService for FixedRawStreamer {
        const NAME: &'static str = "cash.z.wallet.sdk.rpc.CompactTxStreamer";
    }

    struct RangeSvc(Vec<Bytes>);
    impl ServerStreamingService<BlockRange> for RangeSvc {
        type Response = Bytes;
        type ResponseStream =
            Pin<Box<dyn tokio_stream::Stream<Item = Result<Bytes, Status>> + Send>>;
        type Future = std::future::Ready<Result<Response<Self::ResponseStream>, Status>>;

        fn call(&mut self, _request: Request<BlockRange>) -> Self::Future {
            let items: Vec<Result<Bytes, Status>> = self.0.iter().cloned().map(Ok).collect();
            let stream: Self::ResponseStream = Box::pin(tokio_stream::iter(items));
            std::future::ready(Ok(Response::new(stream)))
        }
    }

    struct TreeSvc(Bytes);
    impl UnaryService<BlockId> for TreeSvc {
        type Response = Bytes;
        type Future = std::future::Ready<Result<Response<Bytes>, Status>>;

        fn call(&mut self, _request: Request<BlockId>) -> Self::Future {
            std::future::ready(Ok(Response::new(self.0.clone())))
        }
    }

    impl tower::Service<tonic::codegen::http::Request<Body>> for FixedRawStreamer {
        type Response = tonic::codegen::http::Response<Body>;
        type Error = std::convert::Infallible;
        type Future =
            Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: tonic::codegen::http::Request<Body>) -> Self::Future {
            let path = req.uri().path().to_owned();
            let block_frames = Arc::clone(&self.block_frames);
            let tree_replies = Arc::clone(&self.tree_replies);
            Box::pin(async move {
                match path.as_str() {
                    super::GET_BLOCK_RANGE_PATH => {
                        let frames = block_frames.lock().pop_front().unwrap_or_default();
                        let mut grpc =
                            tonic::server::Grpc::new(RawSendCodec::<BlockRange>::default());
                        Ok(grpc.server_streaming(RangeSvc(frames), req).await)
                    }
                    super::GET_TREE_STATE_PATH => {
                        let reply = tree_replies.lock().pop_front().unwrap_or_default();
                        let mut grpc = tonic::server::Grpc::new(RawSendCodec::<BlockId>::default());
                        Ok(grpc.unary(TreeSvc(reply), req).await)
                    }
                    _ => Ok(tonic::Status::unimplemented("unsupported test path").into_http()),
                }
            })
        }
    }

    fn encode_compact_block_reordered(height: u64, hash: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut canonical = Vec::new();
        prost::encoding::uint64::encode(2, &height, &mut canonical);
        prost::encoding::bytes::encode(3, &hash.to_vec(), &mut canonical);

        let mut reordered = Vec::new();
        prost::encoding::bytes::encode(3, &hash.to_vec(), &mut reordered);
        prost::encoding::uint64::encode(2, &height, &mut reordered);
        (canonical, reordered)
    }

    fn encode_tree_state_reordered(network: &str, height: u64) -> (Vec<u8>, Vec<u8>) {
        let mut canonical = Vec::new();
        prost::encoding::string::encode(1, &network.to_owned(), &mut canonical);
        prost::encoding::uint64::encode(2, &height, &mut canonical);

        let mut reordered = Vec::new();
        prost::encoding::uint64::encode(2, &height, &mut reordered);
        prost::encoding::string::encode(1, &network.to_owned(), &mut reordered);
        (canonical, reordered)
    }

    #[tokio::test]
    async fn raw_wire_parity_distinguishes_semantically_equal_byte_distinct_protobuf_payloads()
    -> Result<(), Box<dyn Error>> {
        let (block_canon, block_reordered) = encode_compact_block_reordered(42, &[7_u8; 32]);
        assert_ne!(block_canon, block_reordered);
        assert_eq!(
            CompactBlock::decode(block_canon.as_slice())?,
            CompactBlock::decode(block_reordered.as_slice())?
        );

        let (tree_canon, tree_reordered) = encode_tree_state_reordered("regtest", 42);
        assert_ne!(tree_canon, tree_reordered);
        assert_eq!(
            TreeState::decode(tree_canon.as_slice())?,
            TreeState::decode(tree_reordered.as_slice())?
        );

        let service = FixedRawStreamer {
            block_frames: Arc::new(Mutex::new(VecDeque::from([
                vec![Bytes::from(block_canon)],
                vec![Bytes::from(block_reordered)],
            ]))),
            tree_replies: Arc::new(Mutex::new(VecDeque::from([
                Bytes::from(tree_canon),
                Bytes::from(tree_reordered),
            ]))),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        let channel = connect_zinder_grpc_with_timeout(
            &format!("http://{addr}"),
            None,
            Duration::from_secs(2),
        )
        .await?;
        let mut grpc = Grpc::new(channel).max_decoding_message_size(CLIENT_MAX_MESSAGE_BYTES);

        let range_hash_a = hash_block_range(&mut grpc, 42, 42).await?;
        let range_hash_b = hash_block_range(&mut grpc, 42, 42).await?;
        assert_ne!(range_hash_a, range_hash_b);

        let tree_hash_a = hash_tree_states(&mut grpc, &[42]).await?;
        let tree_hash_b = hash_tree_states(&mut grpc, &[42]).await?;
        assert_ne!(tree_hash_a, tree_hash_b);

        let _ = shutdown_tx.send(());
        let _ = server_task.await;
        Ok(())
    }
}
