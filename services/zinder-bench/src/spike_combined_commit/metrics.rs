//! Latency summaries, write-stall attribution, property sampling, and tickers.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use rust_rocksdb::{
    DB, Options,
    event_listener::{
        CompactionJobInfo, DBWriteStallCondition, EventListener, FlushJobInfo, WriteStallInfo,
    },
    statistics::Ticker,
};
use serde::Serialize;

/// Nearest-rank percentile summary of durations, in milliseconds.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct LatencySummary {
    pub(crate) count: usize,
    pub(crate) mean_ms: f64,
    pub(crate) p50_ms: f64,
    pub(crate) p90_ms: f64,
    pub(crate) p99_ms: f64,
    pub(crate) p999_ms: f64,
    pub(crate) max_ms: f64,
    pub(crate) total_seconds: f64,
}

/// Nearest-rank percentile: the value at rank `ceil(p * n)`.
pub(crate) fn nearest_rank(sorted: &[f64], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (percentile * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

pub(crate) fn summarize_seconds(values: &[f64]) -> LatencySummary {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let total: f64 = sorted.iter().sum();
    let count = sorted.len();
    LatencySummary {
        count,
        mean_ms: if count == 0 {
            0.0
        } else {
            total / count as f64 * 1000.0
        },
        p50_ms: nearest_rank(&sorted, 0.50) * 1000.0,
        p90_ms: nearest_rank(&sorted, 0.90) * 1000.0,
        p99_ms: nearest_rank(&sorted, 0.99) * 1000.0,
        p999_ms: nearest_rank(&sorted, 0.999) * 1000.0,
        max_ms: sorted.last().copied().unwrap_or(0.0) * 1000.0,
        total_seconds: total,
    }
}

/// Nearest-rank summary of a byte or count series.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct ValueSummary {
    pub(crate) count: usize,
    pub(crate) mean: f64,
    pub(crate) p50: f64,
    pub(crate) p99: f64,
    pub(crate) max: f64,
    pub(crate) total: f64,
}

pub(crate) fn summarize_values(values: &[f64]) -> ValueSummary {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let total: f64 = sorted.iter().sum();
    let count = sorted.len();
    ValueSummary {
        count,
        mean: if count == 0 { 0.0 } else { total / count as f64 },
        p50: nearest_rank(&sorted, 0.50),
        p99: nearest_rank(&sorted, 0.99),
        max: sorted.last().copied().unwrap_or(0.0),
        total,
    }
}

/// Records `OnStallConditionsChanged`, flush, and compaction callbacks.
pub(crate) struct StallRecorder {
    started: Instant,
    state: Mutex<StallState>,
}

#[derive(Default)]
struct StallState {
    per_cf: BTreeMap<String, CfStall>,
    stopped_cfs: BTreeSet<String>,
    delayed_cfs: BTreeSet<String>,
    db_stop_since: Option<Instant>,
    db_delay_since: Option<Instant>,
    db_stopped: Duration,
    db_delayed: Duration,
    db_stop_entries: u64,
    db_delay_entries: u64,
    flushes: BTreeMap<String, FlushTotals>,
    compactions: BTreeMap<String, CompactionTotals>,
    transitions: Vec<StallTransition>,
}

struct CfStall {
    condition: DBWriteStallCondition,
    since: Instant,
    delayed: Duration,
    stopped: Duration,
    delayed_entries: u64,
    stopped_entries: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct FlushTotals {
    pub(crate) count: u64,
    pub(crate) triggered_writes_slowdown: u64,
    pub(crate) triggered_writes_stop: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct CompactionTotals {
    pub(crate) count: u64,
    pub(crate) input_bytes: u64,
    pub(crate) output_bytes: u64,
    pub(crate) elapsed_seconds: f64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct StallTransition {
    pub(crate) at_seconds: f64,
    pub(crate) column_family: String,
    pub(crate) previous: &'static str,
    pub(crate) current: &'static str,
}

const MAX_RECORDED_TRANSITIONS: usize = 20_000;

const fn condition_name(condition: DBWriteStallCondition) -> &'static str {
    match condition {
        DBWriteStallCondition::KDelayed => "delayed",
        DBWriteStallCondition::KStopped => "stopped",
        DBWriteStallCondition::KNormal => "normal",
    }
}

impl StallRecorder {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            state: Mutex::new(StallState::default()),
        })
    }

    fn on_change(
        &self,
        column_family: String,
        previous: DBWriteStallCondition,
        current: DBWriteStallCondition,
    ) {
        let now = Instant::now();
        let mut state = self.state.lock();
        let entry = state
            .per_cf
            .entry(column_family.clone())
            .or_insert(CfStall {
                condition: DBWriteStallCondition::KNormal,
                since: now,
                delayed: Duration::ZERO,
                stopped: Duration::ZERO,
                delayed_entries: 0,
                stopped_entries: 0,
            });
        let elapsed = now.saturating_duration_since(entry.since);
        match entry.condition {
            DBWriteStallCondition::KDelayed => entry.delayed += elapsed,
            DBWriteStallCondition::KStopped => entry.stopped += elapsed,
            DBWriteStallCondition::KNormal => {}
        }
        entry.condition = current;
        entry.since = now;
        match current {
            DBWriteStallCondition::KDelayed => entry.delayed_entries += 1,
            DBWriteStallCondition::KStopped => entry.stopped_entries += 1,
            DBWriteStallCondition::KNormal => {}
        }
        state.stopped_cfs.remove(&column_family);
        state.delayed_cfs.remove(&column_family);
        match current {
            DBWriteStallCondition::KStopped => {
                state.stopped_cfs.insert(column_family.clone());
            }
            DBWriteStallCondition::KDelayed => {
                state.delayed_cfs.insert(column_family.clone());
            }
            DBWriteStallCondition::KNormal => {}
        }
        state.update_db(now);
        if state.transitions.len() < MAX_RECORDED_TRANSITIONS {
            state.transitions.push(StallTransition {
                at_seconds: now.saturating_duration_since(self.started).as_secs_f64(),
                column_family,
                previous: condition_name(previous),
                current: condition_name(current),
            });
        }
    }

    pub(crate) fn summary(&self) -> StallSummary {
        let now = Instant::now();
        let state = self.state.lock();
        let mut per_cf = BTreeMap::new();
        for (name, stall) in &state.per_cf {
            let open = now.saturating_duration_since(stall.since);
            let (delayed, stopped) = match stall.condition {
                DBWriteStallCondition::KDelayed => (stall.delayed + open, stall.stopped),
                DBWriteStallCondition::KStopped => (stall.delayed, stall.stopped + open),
                DBWriteStallCondition::KNormal => (stall.delayed, stall.stopped),
            };
            per_cf.insert(
                name.clone(),
                CfStallSummary {
                    delayed_seconds: delayed.as_secs_f64(),
                    stopped_seconds: stopped.as_secs_f64(),
                    delayed_entries: stall.delayed_entries,
                    stopped_entries: stall.stopped_entries,
                    final_condition: condition_name(stall.condition),
                },
            );
        }
        let db_stopped = state.db_stopped
            + state
                .db_stop_since
                .map_or(Duration::ZERO, |since| now.saturating_duration_since(since));
        let db_delayed = state.db_delayed
            + state
                .db_delay_since
                .map_or(Duration::ZERO, |since| now.saturating_duration_since(since));
        StallSummary {
            db_stopped_condition_seconds: db_stopped.as_secs_f64(),
            db_delayed_condition_seconds: db_delayed.as_secs_f64(),
            db_stop_entries: state.db_stop_entries,
            db_delay_entries: state.db_delay_entries,
            per_column_family: per_cf,
            flushes: state.flushes.clone(),
            compactions: state.compactions.clone(),
            transitions_recorded: state.transitions.len(),
        }
    }

    pub(crate) fn transitions(&self) -> Vec<StallTransition> {
        self.state.lock().transitions.clone()
    }
}

impl StallState {
    fn update_db(&mut self, now: Instant) {
        let stopped = !self.stopped_cfs.is_empty();
        let delayed = !stopped && !self.delayed_cfs.is_empty();
        match (self.db_stop_since, stopped) {
            (None, true) => {
                self.db_stop_since = Some(now);
                self.db_stop_entries += 1;
            }
            (Some(since), false) => {
                self.db_stopped += now.saturating_duration_since(since);
                self.db_stop_since = None;
            }
            _ => {}
        }
        match (self.db_delay_since, delayed) {
            (None, true) => {
                self.db_delay_since = Some(now);
                self.db_delay_entries += 1;
            }
            (Some(since), false) => {
                self.db_delayed += now.saturating_duration_since(since);
                self.db_delay_since = None;
            }
            _ => {}
        }
    }
}

/// Stall-condition time per family and database-wide.
///
/// The database-wide write controller stops every writer while any family is
/// stopped and delays every writer while any family is delayed, so the
/// database figures bound the time writers could have waited.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct StallSummary {
    pub(crate) db_stopped_condition_seconds: f64,
    pub(crate) db_delayed_condition_seconds: f64,
    pub(crate) db_stop_entries: u64,
    pub(crate) db_delay_entries: u64,
    pub(crate) per_column_family: BTreeMap<String, CfStallSummary>,
    pub(crate) flushes: BTreeMap<String, FlushTotals>,
    pub(crate) compactions: BTreeMap<String, CompactionTotals>,
    pub(crate) transitions_recorded: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct CfStallSummary {
    pub(crate) delayed_seconds: f64,
    pub(crate) stopped_seconds: f64,
    pub(crate) delayed_entries: u64,
    pub(crate) stopped_entries: u64,
    pub(crate) final_condition: &'static str,
}

fn family_name(name: Option<Vec<u8>>) -> String {
    name.map_or_else(
        || "<unknown>".to_owned(),
        |bytes| String::from_utf8_lossy(&bytes).to_string(),
    )
}

/// Event listener attached to the writer database through the open hook.
pub(crate) struct StallListener(pub(crate) Arc<StallRecorder>);

impl EventListener for StallListener {
    fn on_stall_conditions_changed(&self, info: &WriteStallInfo) {
        self.0
            .on_change(family_name(info.cf_name()), info.prev(), info.cur());
    }

    fn on_flush_completed(&self, info: &FlushJobInfo) {
        let mut state = self.0.state.lock();
        let totals = state.flushes.entry(family_name(info.cf_name())).or_default();
        totals.count += 1;
        totals.triggered_writes_slowdown += u64::from(info.triggered_writes_slowdown());
        totals.triggered_writes_stop += u64::from(info.triggered_writes_stop());
    }

    fn on_compaction_completed(&self, info: &CompactionJobInfo) {
        let mut state = self.0.state.lock();
        let totals = state
            .compactions
            .entry(family_name(info.cf_name()))
            .or_default();
        totals.count += 1;
        totals.input_bytes += info.total_input_bytes();
        totals.output_bytes += info.total_output_bytes();
        totals.elapsed_seconds += info.elapsed_micros() as f64 / 1_000_000.0;
    }
}

/// Installs the stall listener on every later primary open in this process.
pub(crate) fn install_stall_listener(recorder: &Arc<StallRecorder>) {
    let recorder = Arc::clone(recorder);
    zinder_store::spike_set_primary_options_hook(Some(Box::new(move |options: &mut Options| {
        options.add_event_listener(StallListener(Arc::clone(&recorder)));
    })));
}

/// Per-family property maxima sampled on an interval.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct CfPropertyMax {
    pub(crate) max_l0_files: u64,
    pub(crate) max_pending_compaction_bytes: u64,
    pub(crate) max_immutable_memtables: u64,
    pub(crate) compaction_pending_samples: u64,
    pub(crate) final_l0_files: u64,
    pub(crate) final_pending_compaction_bytes: u64,
    pub(crate) final_total_sst_bytes: u64,
    pub(crate) final_live_data_bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct PropertySamples {
    pub(crate) interval_ms: u64,
    pub(crate) samples: u64,
    pub(crate) write_stopped_samples: u64,
    pub(crate) delayed_write_rate_samples: u64,
    pub(crate) max_actual_delayed_write_rate: u64,
    pub(crate) per_column_family: BTreeMap<String, CfPropertyMax>,
}

/// Background thread sampling `RocksDB` properties.
pub(crate) struct PropertySampler {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<PropertySamples>>,
}

impl PropertySampler {
    pub(crate) fn spawn(db: Arc<DB>, families: Vec<String>, interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut samples = PropertySamples {
                interval_ms: interval.as_millis() as u64,
                ..PropertySamples::default()
            };
            loop {
                sample_once(&db, &families, &mut samples, false);
                if thread_stop.load(Ordering::Relaxed) {
                    sample_once(&db, &families, &mut samples, true);
                    return samples;
                }
                std::thread::sleep(interval);
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    pub(crate) fn finish(mut self) -> PropertySamples {
        self.stop.store(true, Ordering::Relaxed);
        self.handle
            .take()
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default()
    }
}

fn sample_once(db: &DB, families: &[String], samples: &mut PropertySamples, is_final: bool) {
    samples.samples += 1;
    if db
        .property_int_value("rocksdb.is-write-stopped")
        .ok()
        .flatten()
        .unwrap_or(0)
        > 0
    {
        samples.write_stopped_samples += 1;
    }
    let delayed = db
        .property_int_value("rocksdb.actual-delayed-write-rate")
        .ok()
        .flatten()
        .unwrap_or(0);
    if delayed > 0 {
        samples.delayed_write_rate_samples += 1;
        samples.max_actual_delayed_write_rate = samples.max_actual_delayed_write_rate.max(delayed);
    }
    for name in families {
        let Some(handle) = db.cf_handle(name) else {
            continue;
        };
        let read = |property: &str| {
            db.property_int_value_cf(&handle, property)
                .ok()
                .flatten()
                .unwrap_or(0)
        };
        let l0 = read("rocksdb.num-files-at-level0");
        let pending = read("rocksdb.estimate-pending-compaction-bytes");
        let immutable = read("rocksdb.num-immutable-mem-table");
        let compaction_pending = read("rocksdb.compaction-pending");
        let entry = samples.per_column_family.entry(name.clone()).or_default();
        entry.max_l0_files = entry.max_l0_files.max(l0);
        entry.max_pending_compaction_bytes = entry.max_pending_compaction_bytes.max(pending);
        entry.max_immutable_memtables = entry.max_immutable_memtables.max(immutable);
        entry.compaction_pending_samples += u64::from(compaction_pending > 0);
        if is_final {
            entry.final_l0_files = l0;
            entry.final_pending_compaction_bytes = pending;
            entry.final_total_sst_bytes = read("rocksdb.total-sst-files-size");
            entry.final_live_data_bytes = read("rocksdb.estimate-live-data-size");
        }
    }
}

const TICKERS: [(Ticker, &str); 16] = [
    (Ticker::StallMicros, "rocksdb.stall.micros"),
    (Ticker::BytesWritten, "rocksdb.bytes.written"),
    (Ticker::NumberKeysWritten, "rocksdb.number.keys.written"),
    (Ticker::WalFileBytes, "rocksdb.wal.bytes"),
    (Ticker::WalFileSynced, "rocksdb.wal.synced"),
    (Ticker::WriteWithWal, "rocksdb.write.wal"),
    (Ticker::WriteDoneBySelf, "rocksdb.write.self"),
    (Ticker::WriteDoneByOther, "rocksdb.write.other"),
    (Ticker::FlushWriteBytes, "rocksdb.flush.write.bytes"),
    (Ticker::CompactWriteBytes, "rocksdb.compact.write.bytes"),
    (Ticker::CompactReadBytes, "rocksdb.compact.read.bytes"),
    (Ticker::NumberKeysRead, "rocksdb.number.keys.read"),
    (Ticker::BytesRead, "rocksdb.bytes.read"),
    (Ticker::BlockCacheMiss, "rocksdb.block.cache.miss"),
    (Ticker::MemtableHit, "rocksdb.memtable.hit"),
    (Ticker::MemtableMiss, "rocksdb.memtable.miss"),
];

pub(crate) fn ticker_snapshot(options: &Options) -> BTreeMap<&'static str, u64> {
    TICKERS
        .iter()
        .map(|(ticker, name)| (*name, options.get_ticker_count(*ticker)))
        .collect()
}

pub(crate) fn ticker_delta(
    before: &BTreeMap<&'static str, u64>,
    after: &BTreeMap<&'static str, u64>,
) -> BTreeMap<&'static str, u64> {
    after
        .iter()
        .map(|(name, value)| {
            (
                *name,
                value.saturating_sub(before.get(name).copied().unwrap_or(0)),
            )
        })
        .collect()
}

/// Current resident set size from `/proc/self/status`.
pub(crate) fn current_rss_bytes() -> Option<u64> {
    proc_status_kib("VmRSS:").map(|kib| kib * 1024)
}

/// Peak resident set size (`VmHWM`) from `/proc/self/status`.
pub(crate) fn peak_rss_bytes() -> Option<u64> {
    proc_status_kib("VmHWM:").map(|kib| kib * 1024)
}

fn proc_status_kib(prefix: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix(prefix)
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|field| field.parse::<u64>().ok())
    })
}

/// Resets the calling thread's `RocksDB` perf context.
pub(crate) struct PerfProbe {
    context: rust_rocksdb::perf::PerfContext,
}

/// Write-path perf counters for one commit (nanoseconds).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PerfWrite {
    pub(crate) write_delay_ns: u64,
    pub(crate) write_wal_ns: u64,
    pub(crate) write_memtable_ns: u64,
    pub(crate) write_pre_and_post_process_ns: u64,
}

impl PerfProbe {
    pub(crate) fn enable_for_thread() -> Self {
        rust_rocksdb::perf::set_perf_stats(
            rust_rocksdb::perf::PerfStatsLevel::EnableTimeExceptForMutex,
        );
        Self {
            context: rust_rocksdb::perf::PerfContext::default(),
        }
    }

    pub(crate) fn reset(&mut self) {
        self.context.reset();
    }

    pub(crate) fn read(&self) -> PerfWrite {
        use rust_rocksdb::perf::PerfMetric;
        PerfWrite {
            write_delay_ns: self.context.metric(PerfMetric::WriteDelayTime),
            write_wal_ns: self.context.metric(PerfMetric::WriteWalTime),
            write_memtable_ns: self.context.metric(PerfMetric::WriteMemtableTime),
            write_pre_and_post_process_ns: self
                .context
                .metric(PerfMetric::WritePreAndPostProcessTime),
        }
    }
}
