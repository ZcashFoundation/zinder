//! Compares two tip replay reports of the same fixture and start state.

use std::{collections::BTreeMap, path::PathBuf};

use clap::Args;
use serde::Serialize;
use serde_json::Value;
use zinder_bench::BenchError;

use super::{
    measurement::{LatencySummary, PERCENTILE_METHOD, StratumSummary},
    report::{REPORT_CONTRACT_IDENTITY, REPORT_FORMAT_VERSION, TipReplayReport},
};

const COMPARISON_CONTRACT_IDENTITY: &str = "rocksdb-tip-replay-comparison";
const COMPARISON_FORMAT_VERSION: u32 = 1;

/// CLI contract for comparing a baseline and a candidate tip replay report.
#[derive(Args)]
pub(crate) struct CompareTipReplayReportsArgs {
    /// Report measured before the change.
    #[arg(long)]
    baseline: PathBuf,
    /// Report measured after the change.
    #[arg(long)]
    candidate: PathBuf,
    /// Write the comparison to this absent path instead of stdout.
    #[arg(long)]
    report: Option<PathBuf>,
}

#[derive(Serialize)]
struct TipReplayComparison {
    contract_identity: &'static str,
    comparison_format_version: u32,
    percentile_method: &'static str,
    baseline: ComparedReport,
    candidate: ComparedReport,
    workload: ComparedWorkload,
    /// Every build, host, and configuration setting that differs between the reports.
    setting_differences: Vec<SettingDifference>,
    process: BTreeMap<&'static str, MetricChange>,
    /// Pooled timers by stratum, then by timer name.
    timers: BTreeMap<&'static str, StratumChange>,
}

#[derive(Serialize)]
struct ComparedReport {
    path: String,
    software_revision: String,
    label: Option<String>,
    repeat_count: u32,
}

#[derive(Serialize)]
struct ComparedWorkload {
    network: String,
    fixture_manifest_sha256: String,
    from_height: u32,
    to_height: u32,
    block_count: u32,
}

#[derive(Serialize)]
struct SettingDifference {
    setting: String,
    baseline: Value,
    candidate: Value,
}

#[derive(Serialize)]
struct MetricChange {
    baseline: f64,
    candidate: f64,
    /// `(candidate / baseline - 1) * 100`; `null` when the baseline is zero.
    change_percent: Option<f64>,
}

impl MetricChange {
    fn new(baseline: f64, candidate: f64) -> Self {
        Self {
            baseline,
            candidate,
            change_percent: (baseline != 0.0).then(|| (candidate / baseline - 1.0) * 100.0),
        }
    }
}

#[derive(Serialize)]
struct StratumChange {
    baseline_block_count: u32,
    candidate_block_count: u32,
    /// Present when both reports have blocks in this stratum.
    timers: Option<BTreeMap<&'static str, TimerChange>>,
    /// Mean per-block wallet-store ticker deltas by `RocksDB` ticker name;
    /// present when both reports collected statistics over blocks in this stratum.
    wallet_ticker_mean_per_block: Option<BTreeMap<String, MetricChange>>,
}

/// Changes of one timer's summary statistics, in milliseconds.
#[derive(Serialize)]
struct TimerChange {
    p50: MetricChange,
    p99: MetricChange,
    p999: MetricChange,
    max: MetricChange,
    mean: MetricChange,
    total: MetricChange,
}

impl TimerChange {
    fn new(baseline: &LatencySummary, candidate: &LatencySummary) -> Self {
        Self {
            p50: MetricChange::new(baseline.p50_ms, candidate.p50_ms),
            p99: MetricChange::new(baseline.p99_ms, candidate.p99_ms),
            p999: MetricChange::new(baseline.p999_ms, candidate.p999_ms),
            max: MetricChange::new(baseline.max_ms, candidate.max_ms),
            mean: MetricChange::new(baseline.mean_ms, candidate.mean_ms),
            total: MetricChange::new(baseline.total_ms, candidate.total_ms),
        }
    }
}

/// Compares two reports and emits the comparison.
pub(crate) fn run_compare_tip_replay_reports(
    args: &CompareTipReplayReportsArgs,
) -> Result<(), BenchError> {
    if let Some(path) = args.report.as_deref() {
        super::scratch::resolve_absent_path(path, "report")?;
    }
    let baseline = read_report(&args.baseline)?;
    let candidate = read_report(&args.candidate)?;
    require_same_workload(&baseline, &candidate)?;
    let comparison = TipReplayComparison {
        contract_identity: COMPARISON_CONTRACT_IDENTITY,
        comparison_format_version: COMPARISON_FORMAT_VERSION,
        percentile_method: PERCENTILE_METHOD,
        baseline: compared_report(&args.baseline, &baseline),
        candidate: compared_report(&args.candidate, &candidate),
        workload: ComparedWorkload {
            network: baseline.inputs.network.clone(),
            fixture_manifest_sha256: baseline.inputs.fixture.manifest_sha256.clone(),
            from_height: baseline.inputs.fixture.from_height,
            to_height: baseline.inputs.fixture.to_height,
            block_count: baseline.inputs.fixture.block_count,
        },
        setting_differences: setting_differences(&baseline, &candidate)?,
        process: process_changes(&baseline, &candidate),
        timers: timer_changes(&baseline, &candidate),
    };
    crate::emit_report(&comparison, args.report.as_deref())
}

fn read_report(path: &std::path::Path) -> Result<TipReplayReport, BenchError> {
    let bytes = std::fs::read(path).map_err(|source| BenchError::io(path, source))?;
    let report: TipReplayReport = serde_json::from_slice(&bytes)?;
    if report.contract_identity != REPORT_CONTRACT_IDENTITY
        || report.report_format_version != REPORT_FORMAT_VERSION
    {
        return Err(BenchError::report_format(format!(
            "{} is not a version {REPORT_FORMAT_VERSION} {REPORT_CONTRACT_IDENTITY} report",
            path.display()
        )));
    }
    Ok(report)
}

/// Timings are comparable only for the same blocks, replayed as many times,
/// from the same start state, and summarized by the same method.
///
/// The pooled summaries count every block of every repeat, so their totals
/// and block counts scale with the repeat count. Stratum rules decide which
/// blocks each stratum holds, and the comparison names [`PERCENTILE_METHOD`]
/// as the method of every percentile it compares. A change that alters the
/// final state is a correctness failure.
fn require_same_workload(
    baseline: &TipReplayReport,
    candidate: &TipReplayReport,
) -> Result<(), BenchError> {
    for (role, report) in [("baseline", baseline), ("candidate", candidate)] {
        let method = &report.method.percentile_method;
        if method != PERCENTILE_METHOD {
            return Err(BenchError::report_format(format!(
                "the {role} percentile method {method} is not the supported {PERCENTILE_METHOD} method"
            )));
        }
    }
    let (baseline_statistics, candidate_statistics) = (
        baseline.configuration.wallet_statistics.collected,
        candidate.configuration.wallet_statistics.collected,
    );
    if baseline_statistics != candidate_statistics {
        return Err(BenchError::report_format(format!(
            "the baseline wallet statistics collection ({baseline_statistics}) differs from the candidate ({candidate_statistics}); collecting statistics changes the wallet store's options and can change timings"
        )));
    }
    if baseline.method.stratum_rules != candidate.method.stratum_rules {
        return Err(BenchError::report_format(
            "the baseline and candidate stratum rules differ, so their strata hold different blocks",
        ));
    }
    let (baseline_repeats, candidate_repeats) = (
        baseline.configuration.repeat_count,
        candidate.configuration.repeat_count,
    );
    if baseline_repeats != candidate_repeats {
        return Err(BenchError::report_format(format!(
            "the baseline repeat count {baseline_repeats} differs from the candidate repeat count {candidate_repeats}; pooled totals and block counts scale with --repeat"
        )));
    }
    let (baseline_inputs, candidate_inputs) = (&baseline.inputs, &candidate.inputs);
    if baseline_inputs.fixture.manifest_sha256 != candidate_inputs.fixture.manifest_sha256
        || baseline_inputs.fixture.tip_replay_checkpoints_sha256
            != candidate_inputs.fixture.tip_replay_checkpoints_sha256
    {
        return Err(BenchError::report_format(
            "the reports replay different fixtures",
        ));
    }
    if baseline_inputs.canonical_store.start != candidate_inputs.canonical_store.start
        || baseline_inputs.wallet_store.start != candidate_inputs.wallet_store.start
    {
        return Err(BenchError::report_format(
            "the reports start from different canonical or wallet states",
        ));
    }
    let final_state = |report: &TipReplayReport| {
        report
            .repeats
            .first()
            .map(|repeat| (repeat.final_canonical.clone(), repeat.final_wallet.clone()))
    };
    if final_state(baseline).is_none() || final_state(baseline) != final_state(candidate) {
        return Err(BenchError::report_format(
            "the reports end in different canonical or wallet states",
        ));
    }
    Ok(())
}

fn compared_report(path: &std::path::Path, report: &TipReplayReport) -> ComparedReport {
    ComparedReport {
        path: path.display().to_string(),
        software_revision: report.provenance.software_revision.clone(),
        label: report.provenance.label.clone(),
        repeat_count: report.configuration.repeat_count,
    }
}

fn setting_differences(
    baseline: &TipReplayReport,
    candidate: &TipReplayReport,
) -> Result<Vec<SettingDifference>, BenchError> {
    let settings = |report: &TipReplayReport| -> Result<BTreeMap<String, Value>, BenchError> {
        let mut flattened = BTreeMap::new();
        flatten(
            "software_revision",
            &Value::from(report.provenance.software_revision.clone()),
            &mut flattened,
        );
        flatten(
            "build",
            &serde_json::to_value(&report.provenance.build)?,
            &mut flattened,
        );
        flatten(
            "host",
            &serde_json::to_value(&report.provenance.host)?,
            &mut flattened,
        );
        flatten(
            "configuration",
            &serde_json::to_value(&report.configuration)?,
            &mut flattened,
        );
        flattened.remove("configuration.scratch_directory");
        Ok(flattened)
    };
    let (baseline, candidate) = (settings(baseline)?, settings(candidate)?);
    let mut names: Vec<&String> = baseline.keys().chain(candidate.keys()).collect();
    names.sort_unstable();
    names.dedup();
    Ok(names
        .into_iter()
        .filter_map(|name| {
            let (baseline, candidate) = (
                baseline.get(name).cloned().unwrap_or(Value::Null),
                candidate.get(name).cloned().unwrap_or(Value::Null),
            );
            (baseline != candidate).then(|| SettingDifference {
                setting: name.clone(),
                baseline,
                candidate,
            })
        })
        .collect())
}

fn flatten(prefix: &str, node: &Value, flattened: &mut BTreeMap<String, Value>) {
    if let Value::Object(fields) = node {
        for (name, field) in fields {
            flatten(&format!("{prefix}.{name}"), field, flattened);
        }
    } else {
        flattened.insert(prefix.to_owned(), node.clone());
    }
}

fn process_changes(
    baseline: &TipReplayReport,
    candidate: &TipReplayReport,
) -> BTreeMap<&'static str, MetricChange> {
    let mean = |report: &TipReplayReport, metric: fn(&super::report::RepeatReport) -> f64| {
        let total: f64 = report.repeats.iter().map(metric).sum();
        let count = u32::try_from(report.repeats.len())
            .unwrap_or(u32::MAX)
            .max(1);
        total / f64::from(count)
    };
    let mut changes = BTreeMap::new();
    changes.insert(
        "replay_wall_seconds_mean",
        MetricChange::new(
            mean(baseline, |repeat| repeat.replay_wall_seconds),
            mean(candidate, |repeat| repeat.replay_wall_seconds),
        ),
    );
    changes.insert(
        "replay_process_cpu_seconds_mean",
        MetricChange::new(
            mean(baseline, |repeat| repeat.replay_process_cpu_seconds),
            mean(candidate, |repeat| repeat.replay_process_cpu_seconds),
        ),
    );
    if let (Some(baseline_peak), Some(candidate_peak)) = (
        baseline.process.peak_rss_bytes,
        candidate.process.peak_rss_bytes,
    ) {
        changes.insert(
            "peak_rss_mebibytes",
            MetricChange::new(mebibytes(baseline_peak), mebibytes(candidate_peak)),
        );
    }
    changes
}

fn mebibytes(bytes: u64) -> f64 {
    f64::from(u32::try_from(bytes / 1024).unwrap_or(u32::MAX)) / 1024.0
}

fn timer_changes(
    baseline: &TipReplayReport,
    candidate: &TipReplayReport,
) -> BTreeMap<&'static str, StratumChange> {
    baseline
        .aggregate
        .named_strata()
        .into_iter()
        .zip(candidate.aggregate.named_strata())
        .map(|((name, baseline), (_, candidate))| (name, stratum_change(baseline, candidate)))
        .collect()
}

fn stratum_change(baseline: &StratumSummary, candidate: &StratumSummary) -> StratumChange {
    StratumChange {
        baseline_block_count: baseline.block_count,
        candidate_block_count: candidate.block_count,
        timers: baseline.timers.as_ref().zip(candidate.timers.as_ref()).map(
            |(baseline, candidate)| {
                baseline
                    .named_timers()
                    .into_iter()
                    .zip(candidate.named_timers())
                    .map(|((name, baseline), (_, candidate))| {
                        (name, TimerChange::new(baseline, candidate))
                    })
                    .collect()
            },
        ),
        wallet_ticker_mean_per_block: baseline
            .wallet_tickers
            .as_ref()
            .zip(candidate.wallet_tickers.as_ref())
            .map(|(baseline, candidate)| {
                baseline
                    .0
                    .iter()
                    .filter_map(|(name, baseline)| {
                        let candidate = candidate.0.get(name)?;
                        Some((
                            name.clone(),
                            MetricChange::new(baseline.mean_per_block, candidate.mean_per_block),
                        ))
                    })
                    .collect()
            }),
    }
}
