//! The single in-process writer: canonical commits plus the wallet stage.

use std::{
    num::{NonZeroU32, NonZeroU64},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use rust_rocksdb::DB;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use zinder_bench::BenchError;
use zinder_core::{BlockHeight, BlockId, Network, NetworkUpgradeActivations};
use zinder_ingest::{
    CanonicalFollowConfig, SpikeFollowStep, spike_prepare_follow_step,
    spike_prepare_suffix_replacement,
};
use zinder_store::{
    CanonicalEventHistoryRequest, CanonicalLiveAppend, CanonicalLiveReplacement,
    CanonicalStoreReadyEvidence, RocksDbCanonicalStore, SpikeBatchKind, SpikeSnapshot,
};
use zinder_wallet_projection::WalletCanonicalSourceIdentity;
use zinder_wallet_rocksdb::RocksDbWalletFollowingStore;

use super::{
    observer::SpikeObserver,
    output::{Outputs, RunClock},
    plan::{Phase, ReplacementSpec},
    source::PacedReplaySource,
    stats::process_memory,
};

/// Everything shared by the writer, readers, sampler, and index builder.
pub(crate) struct Shared<'db> {
    pub(crate) canonical_db: &'db DB,
    pub(crate) wallet_db: Option<&'db DB>,
    pub(crate) canonical_dir: PathBuf,
    pub(crate) wallet_dir: Option<PathBuf>,
    pub(crate) staging_dir: PathBuf,
    pub(crate) network: Network,
    pub(crate) source: Arc<PacedReplaySource>,
    pub(crate) observer: Arc<SpikeObserver>,
    pub(crate) outputs: &'db Outputs,
    pub(crate) clock: &'db RunClock,
    pub(crate) runtime: tokio::runtime::Handle,
    pub(crate) cancel: CancellationToken,
    pub(crate) active_readers: AtomicU32,
    /// 0 idle, 1 SST build, 2 ingest, 3 catch-up, 4 switch, 5 drop.
    pub(crate) attach_stage: AtomicU32,
    pub(crate) first_retained_height: u32,
    pub(crate) wallet_attached: AtomicBool,
}

/// Owned writer state that moves into each phase's writer thread.
pub(crate) struct WriterState {
    pub(crate) canonical: Option<RocksDbCanonicalStore>,
    pub(crate) wallet: Option<RocksDbWalletFollowingStore>,
    pub(crate) activations: Arc<NetworkUpgradeActivations>,
    pub(crate) follow_config: CanonicalFollowConfig,
    pub(crate) request_timeout: Duration,
    pub(crate) wallet_max_transition_bytes: NonZeroU64,
}

impl WriterState {
    pub(crate) fn canonical(&self) -> &RocksDbCanonicalStore {
        self.canonical
            .as_ref()
            .expect("canonical store is present between commits")
    }

    pub(crate) fn tip_height(&self) -> u32 {
        self.canonical().event_fence().visible_tip().height.value()
    }
}

/// A snapshot taken by the writer between commits, with its exact fence.
pub(crate) struct CanonicalSnapshotGrant<'db> {
    pub(crate) snapshot: SpikeSnapshot<'db>,
    pub(crate) ready: CanonicalStoreReadyEvidence,
    pub(crate) network: Network,
}

/// Work other threads ask the writer to perform between commits.
pub(crate) enum WriterRequest<'db> {
    CanonicalSnapshot(mpsc::Sender<CanonicalSnapshotGrant<'db>>),
    Run(Box<dyn FnOnce(&mut WriterState) + Send + 'db>),
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CommitRecord {
    pub(crate) t: f64,
    pub(crate) phase: String,
    pub(crate) kind: &'static str,
    pub(crate) height: u32,
    pub(crate) catch_up: bool,
    pub(crate) prepare_s: f64,
    pub(crate) canonical_commit_s: f64,
    pub(crate) canonical_write_s: Option<f64>,
    pub(crate) canonical_base_bytes: Option<usize>,
    pub(crate) canonical_ballast_bytes: Option<usize>,
    pub(crate) canonical_batch_bytes: Option<usize>,
    pub(crate) wallet_apply_s: Option<f64>,
    pub(crate) wallet_write_s: Option<f64>,
    pub(crate) wallet_base_bytes: Option<usize>,
    pub(crate) wallet_ballast_bytes: Option<usize>,
    pub(crate) wallet_batch_bytes: Option<usize>,
    pub(crate) total_commit_s: f64,
    pub(crate) active_readers: u32,
    pub(crate) attach_stage: u32,
    pub(crate) pacing_lag_s: f64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ReplacementRecord {
    pub(crate) t: f64,
    pub(crate) displaced: u32,
    pub(crate) kept: u32,
    pub(crate) first_height: u32,
    pub(crate) last_height: u32,
    pub(crate) previous_tip: u32,
    pub(crate) new_tip: u32,
    pub(crate) prepare_s: f64,
    pub(crate) canonical_commit_s: f64,
    pub(crate) canonical_write_s: Option<f64>,
    pub(crate) canonical_batch_bytes: Option<usize>,
    pub(crate) wallet_apply_s: Option<f64>,
    pub(crate) wallet_write_s: Option<f64>,
    pub(crate) wallet_batch_bytes: Option<usize>,
    pub(crate) wallet_error: Option<String>,
    pub(crate) rss_before_bytes: Option<u64>,
    pub(crate) rss_after_bytes: Option<u64>,
    pub(crate) peak_rss_after_bytes: Option<u64>,
    pub(crate) active_readers: u32,
}

#[derive(Default)]
pub(crate) struct WriterPhaseOutcome {
    pub(crate) commits: Vec<CommitRecord>,
    pub(crate) replacements: Vec<ReplacementRecord>,
    pub(crate) source_exhausted: bool,
    pub(crate) start_tip: u32,
    pub(crate) end_tip: u32,
    pub(crate) wallet_detached: Option<String>,
}

struct WalletApply {
    seconds: f64,
    events: u32,
    write: Option<super::observer::LastWrite>,
}

/// Applies every retained canonical event after the wallet cursor.
pub(crate) fn apply_wallet_events(
    state: &mut WriterState,
    shared: &Shared<'_>,
) -> Result<Option<(f64, u32)>, BenchError> {
    let applied = apply_wallet_events_inner(state, shared)?;
    Ok(applied.map(|applied| (applied.seconds, applied.events)))
}

fn apply_wallet_events_inner(
    state: &mut WriterState,
    shared: &Shared<'_>,
) -> Result<Option<WalletApply>, BenchError> {
    let Some(wallet) = state.wallet.as_mut() else {
        return Ok(None);
    };
    let canonical = state
        .canonical
        .as_ref()
        .expect("canonical store is present between commits");
    let started = Instant::now();
    let mut events = 0;
    let mut last_write = None;
    loop {
        let evidence = wallet.ready_evidence().clone();
        let cursor = evidence.source_position.event_cursor.as_bytes();
        let retained = canonical.canonical_event_history(CanonicalEventHistoryRequest::new(
            Some(&cursor),
            NonZeroU32::MIN,
        ))?;
        let Some(event) = retained.first().copied() else {
            break;
        };
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(&evidence);
        let epoch = canonical.chain_epoch_at(event.resulting_epoch_id())?;
        let settled = BlockId::new(epoch.settled_tip_height, epoch.settled_tip_hash);
        let rows = canonical.spike_scan_canonical_replay_range(event.committed_range())?;
        wallet.apply_canonical_event_range(
            source,
            event,
            event.resulting_fence(),
            settled,
            state.wallet_max_transition_bytes,
            rows,
        )?;
        last_write = shared.observer.take_last(SpikeBatchKind::WalletTransition);
        events += 1;
    }
    Ok(Some(WalletApply {
        seconds: started.elapsed().as_secs_f64(),
        events,
        write: last_write,
    }))
}

pub(crate) fn handle_request<'db>(
    state: &mut WriterState,
    shared: &Shared<'db>,
    request: WriterRequest<'db>,
) {
    match request {
        WriterRequest::CanonicalSnapshot(reply) => {
            let grant = CanonicalSnapshotGrant {
                snapshot: shared.canonical_db.snapshot(),
                ready: state.canonical().ready_evidence(),
                network: state.canonical().network(),
            };
            let _ = reply.send(grant);
        }
        WriterRequest::Run(work) => work(state),
    }
}

fn drain_requests<'db>(
    state: &mut WriterState,
    shared: &Shared<'db>,
    requests: &mpsc::Receiver<WriterRequest<'db>>,
) {
    while let Ok(request) = requests.try_recv() {
        handle_request(state, shared, request);
    }
}

/// Runs the writer for one phase and returns its commit evidence.
pub(crate) fn run_writer_phase<'db>(
    state: &mut WriterState,
    shared: &Shared<'db>,
    phase: &Phase,
    requests: &mpsc::Receiver<WriterRequest<'db>>,
) -> Result<WriterPhaseOutcome, BenchError> {
    let mut outcome = WriterPhaseOutcome {
        start_tip: state.tip_height(),
        ..WriterPhaseOutcome::default()
    };
    shared.observer.set_multiplier(phase.density_multiplier);
    let phase_started = Instant::now();
    let deadline = phase
        .duration_seconds
        .map(|seconds| phase_started + Duration::from_secs(seconds));
    let interval = Duration::from_millis(phase.block_interval_ms);
    let mut next_reveal = phase_started;
    let mut committed = 0_u32;
    let mut replacements = phase.replacements.clone();
    replacements.sort_by(|left, right| left.at_seconds.total_cmp(&right.at_seconds));
    let mut replacements = std::collections::VecDeque::from(replacements);

    'phase: loop {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline)
            || phase.blocks.is_some_and(|limit| committed >= limit)
        {
            break;
        }
        loop {
            if let Some(spec) = replacements.front()
                && phase_started.elapsed().as_secs_f64() >= spec.at_seconds
            {
                let spec = spec.clone();
                replacements.pop_front();
                let record = commit_replacement(state, shared, &spec)?;
                shared.outputs.events.write(&record);
                outcome.replacements.push(record);
                continue;
            }
            let now = Instant::now();
            if deadline.is_some_and(|deadline| now >= deadline) {
                break 'phase;
            }
            if now >= next_reveal {
                break;
            }
            let mut wait = next_reveal - now;
            if let Some(deadline) = deadline {
                wait = wait.min(deadline.saturating_duration_since(now));
            }
            if let Some(spec) = replacements.front() {
                let due = phase_started + Duration::from_secs_f64(spec.at_seconds);
                wait = wait.min(due.saturating_duration_since(now));
            }
            match requests.recv_timeout(wait.max(Duration::from_millis(1))) {
                Ok(request) => handle_request(state, shared, request),
                Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {}
            }
        }
        let lag = Instant::now().saturating_duration_since(next_reveal);
        if shared.source.reveal_next().is_none() {
            outcome.source_exhausted = true;
            shared.clock.event(
                shared.outputs,
                "source_exhausted",
                serde_json::json!({ "end_height": shared.source.end_height() }),
            );
            break;
        }
        next_reveal += interval;
        if next_reveal < Instant::now() {
            next_reveal = Instant::now();
        }
        loop {
            drain_requests(state, shared, requests);
            let prepare_started = Instant::now();
            let step = shared.runtime.block_on(spike_prepare_follow_step(
                state.canonical(),
                shared.source.as_ref(),
                Arc::clone(&state.activations),
                state.follow_config.clone(),
                &shared.cancel,
            ));
            let step = match step {
                Ok(step) => step,
                Err(error) => {
                    return Err(BenchError::invalid_argument(format!(
                        "spike follow preparation failed: {error}"
                    )));
                }
            };
            let prepare_s = prepare_started.elapsed().as_secs_f64();
            match step {
                SpikeFollowStep::AtTip => break,
                SpikeFollowStep::Append(append) => {
                    let catch_up = state.tip_height() + 1 < shared.source.revealed_height();
                    let record = commit_append(
                        state,
                        shared,
                        *append,
                        prepare_s,
                        catch_up,
                        lag.as_secs_f64(),
                        &mut outcome,
                    )?;
                    shared.outputs.commits.write(&record);
                    outcome.commits.push(record);
                    committed += 1;
                }
                SpikeFollowStep::Replacement(replacement) => {
                    shared.clock.event(
                        shared.outputs,
                        "unexpected_source_replacement",
                        serde_json::json!({ "tip": state.tip_height() }),
                    );
                    commit_prepared_replacement(state, shared, *replacement)?;
                }
            }
            if phase.blocks.is_some_and(|limit| committed >= limit) {
                break;
            }
        }
    }
    outcome.end_tip = state.tip_height();
    Ok(outcome)
}

fn commit_append(
    state: &mut WriterState,
    shared: &Shared<'_>,
    append: CanonicalLiveAppend,
    prepare_s: f64,
    catch_up: bool,
    pacing_lag_s: f64,
    outcome: &mut WriterPhaseOutcome,
) -> Result<CommitRecord, BenchError> {
    let started = Instant::now();
    let store = state.canonical.take().expect("canonical store is present");
    let (store, fence) = store.commit_live_append(append, &state.activations)?;
    let canonical_commit_s = started.elapsed().as_secs_f64();
    state.canonical = Some(store);
    let canonical_write = shared.observer.take_last(SpikeBatchKind::CanonicalAppend);
    let height = fence.visible_tip().height.value();
    shared.source.set_writer_tip(height);
    let wallet = match apply_wallet_events_inner(state, shared) {
        Ok(wallet) => wallet,
        Err(error) => {
            detach_wallet(state, shared, &error.to_string(), outcome);
            None
        }
    };
    let total_commit_s = started.elapsed().as_secs_f64();
    Ok(CommitRecord {
        t: shared.clock.t(),
        phase: shared.clock.phase().to_owned(),
        kind: "append",
        height,
        catch_up,
        prepare_s,
        canonical_commit_s,
        canonical_write_s: canonical_write.map(|write| write.write_seconds),
        canonical_base_bytes: canonical_write.map(|write| write.base_bytes),
        canonical_ballast_bytes: canonical_write.map(|write| write.ballast_bytes),
        canonical_batch_bytes: canonical_write.map(|write| write.final_bytes),
        wallet_apply_s: wallet.as_ref().map(|wallet| wallet.seconds),
        wallet_write_s: wallet
            .as_ref()
            .and_then(|wallet| wallet.write.map(|write| write.write_seconds)),
        wallet_base_bytes: wallet
            .as_ref()
            .and_then(|wallet| wallet.write.map(|write| write.base_bytes)),
        wallet_ballast_bytes: wallet
            .as_ref()
            .and_then(|wallet| wallet.write.map(|write| write.ballast_bytes)),
        wallet_batch_bytes: wallet
            .as_ref()
            .and_then(|wallet| wallet.write.map(|write| write.final_bytes)),
        total_commit_s,
        active_readers: shared.active_readers.load(Ordering::SeqCst),
        attach_stage: shared.attach_stage.load(Ordering::SeqCst),
        pacing_lag_s,
    })
}

fn detach_wallet(
    state: &mut WriterState,
    shared: &Shared<'_>,
    reason: &str,
    outcome: &mut WriterPhaseOutcome,
) {
    state.wallet = None;
    shared.wallet_attached.store(false, Ordering::SeqCst);
    outcome.wallet_detached = Some(reason.to_owned());
    shared.clock.event(
        shared.outputs,
        "wallet_detached",
        serde_json::json!({ "reason": reason }),
    );
}

fn commit_prepared_replacement(
    state: &mut WriterState,
    shared: &Shared<'_>,
    replacement: CanonicalLiveReplacement,
) -> Result<(), BenchError> {
    let store = state.canonical.take().expect("canonical store is present");
    let (store, fence) = store.commit_live_replacement(replacement, &state.activations)?;
    state.canonical = Some(store);
    shared
        .source
        .set_writer_tip(fence.visible_tip().height.value());
    let _ = shared
        .observer
        .take_last(SpikeBatchKind::CanonicalReplacement);
    apply_wallet_events_inner(state, shared)?;
    Ok(())
}

fn commit_replacement(
    state: &mut WriterState,
    shared: &Shared<'_>,
    spec: &ReplacementSpec,
) -> Result<ReplacementRecord, BenchError> {
    let previous_tip = state.tip_height();
    let first_height = previous_tip + 1 - spec.displaced;
    let last_height = first_height + spec.kept - 1;
    shared.clock.event(
        shared.outputs,
        "replacement_prepare_started",
        serde_json::json!({ "first_height": first_height, "last_height": last_height, "previous_tip": previous_tip }),
    );
    let prepare_started = Instant::now();
    let replacement = shared
        .runtime
        .block_on(spike_prepare_suffix_replacement(
            state.canonical(),
            shared.source.as_ref(),
            Arc::clone(&state.activations),
            state.request_timeout,
            &shared.cancel,
            BlockHeight::new(first_height),
            BlockHeight::new(last_height),
        ))
        .map_err(|error| {
            BenchError::invalid_argument(format!("spike replacement preparation failed: {error}"))
        })?;
    let prepare_s = prepare_started.elapsed().as_secs_f64();
    let before = process_memory();
    let started = Instant::now();
    let store = state.canonical.take().expect("canonical store is present");
    let (store, fence) = store.commit_live_replacement(replacement, &state.activations)?;
    let canonical_commit_s = started.elapsed().as_secs_f64();
    state.canonical = Some(store);
    let canonical_write = shared
        .observer
        .take_last(SpikeBatchKind::CanonicalReplacement);
    let new_tip = fence.visible_tip().height.value();
    shared.source.set_writer_tip(new_tip);
    let (wallet, wallet_error) = match apply_wallet_events_inner(state, shared) {
        Ok(wallet) => (wallet, None),
        Err(error) => {
            let reason = error.to_string();
            let mut ignored = WriterPhaseOutcome::default();
            detach_wallet(state, shared, &reason, &mut ignored);
            (None, Some(reason))
        }
    };
    let after = process_memory();
    Ok(ReplacementRecord {
        t: shared.clock.t(),
        displaced: spec.displaced,
        kept: spec.kept,
        first_height,
        last_height,
        previous_tip,
        new_tip,
        prepare_s,
        canonical_commit_s,
        canonical_write_s: canonical_write.map(|write| write.write_seconds),
        canonical_batch_bytes: canonical_write.map(|write| write.final_bytes),
        wallet_apply_s: wallet.as_ref().map(|wallet| wallet.seconds),
        wallet_write_s: wallet
            .as_ref()
            .and_then(|wallet| wallet.write.map(|write| write.write_seconds)),
        wallet_batch_bytes: wallet
            .as_ref()
            .and_then(|wallet| wallet.write.map(|write| write.final_bytes)),
        wallet_error,
        rss_before_bytes: before.rss_bytes,
        rss_after_bytes: after.rss_bytes,
        peak_rss_after_bytes: after.peak_rss_bytes,
        active_readers: shared.active_readers.load(Ordering::SeqCst),
    })
}
