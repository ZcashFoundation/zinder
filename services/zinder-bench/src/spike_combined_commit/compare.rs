//! Cross-report checks: combined against its canonical-only baseline.

use std::path::PathBuf;

use clap::Args;
use serde::Serialize;
use serde_json::Value;
use zinder_bench::BenchError;

use super::{NETWORK_LABEL_TESTNET, replay::ThresholdCheck};

/// Compare inputs.
#[derive(Args)]
pub(crate) struct CompareArgs {
    /// Canonical-only replay report.
    #[arg(long)]
    baseline: PathBuf,
    /// Combined replay report over the same range and settings.
    #[arg(long)]
    combined: PathBuf,
    /// Write the JSON report here (refuses to overwrite) instead of stdout.
    #[arg(long)]
    pub(crate) report: Option<PathBuf>,
}

#[derive(Serialize)]
pub(crate) struct PhaseRatio {
    pub(crate) metric: String,
    pub(crate) baseline: f64,
    pub(crate) combined: f64,
    pub(crate) ratio: Option<f64>,
}

/// Compare report.
#[derive(Serialize)]
pub(crate) struct CompareReport {
    pub(crate) spike: &'static str,
    pub(crate) action: &'static str,
    pub(crate) network_label: &'static str,
    pub(crate) baseline: String,
    pub(crate) combined: String,
    pub(crate) same_range: bool,
    pub(crate) same_density_and_pressure: bool,
    pub(crate) ratios: Vec<PhaseRatio>,
    pub(crate) thresholds: Vec<ThresholdCheck>,
}

fn read(path: &PathBuf) -> Result<Value, BenchError> {
    let bytes = std::fs::read(path).map_err(|source| BenchError::io(path, source))?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn number(value: &Value, pointer: &str) -> f64 {
    value.pointer(pointer).and_then(Value::as_f64).unwrap_or(f64::NAN)
}

/// Runs the comparison.
pub(crate) fn run_compare(args: &CompareArgs) -> Result<CompareReport, BenchError> {
    let baseline = read(&args.baseline)?;
    let combined = read(&args.combined)?;
    if baseline.pointer("/mode").and_then(Value::as_str) != Some("canonical-only")
        || combined.pointer("/mode").and_then(Value::as_str) != Some("combined")
    {
        return Err(BenchError::invalid_argument(
            "--baseline must be a canonical-only replay report and --combined a combined one",
        ));
    }
    let same_range = baseline.pointer("/h0") == combined.pointer("/h0")
        && baseline.pointer("/through") == combined.pointer("/through");
    let same_density_and_pressure = baseline.pointer("/density_multiplier")
        == combined.pointer("/density_multiplier")
        && baseline.pointer("/pressure/lanes") == combined.pointer("/pressure/lanes");
    let mut ratios = Vec::new();
    for pointer in [
        "/latency/commit_total/p50_ms",
        "/latency/commit_total/p99_ms",
        "/latency/commit_total/p999_ms",
        "/latency/commit_total/max_ms",
        "/latency/synced_write/p50_ms",
        "/latency/synced_write/p99_ms",
        "/blocks_per_second",
        "/commit_path_blocks_per_second",
        "/batch/total_bytes/p50",
        "/batch/total_bytes/p99",
        "/batch/total_bytes/max",
        "/rss/peak_vmhwm_bytes",
        "/write_amplification/user_bytes_written",
        "/stall_rates/ticker_stall_seconds",
    ] {
        let base = number(&baseline, pointer);
        let comb = number(&combined, pointer);
        ratios.push(PhaseRatio {
            metric: pointer.trim_start_matches('/').replace('/', "."),
            baseline: base,
            combined: comb,
            ratio: (base.is_finite() && base > 0.0 && comb.is_finite()).then(|| comb / base),
        });
    }
    let base_p99 = number(&baseline, "/latency/commit_total/p99_ms");
    let comb_p99 = number(&combined, "/latency/commit_total/p99_ms");
    let thresholds = vec![
        ThresholdCheck {
            check: "tip commit p99 with wallet rows at most 1 s",
            threshold: "<= 1000 ms".to_owned(),
            observed: format!("{comb_p99:.3} ms"),
            passed: comb_p99 <= 1000.0,
        },
        ThresholdCheck {
            check: "tip commit p99 with wallet rows at most 2x canonical-only p99",
            threshold: format!("<= {:.3} ms (2 x {base_p99:.3} ms)", 2.0 * base_p99),
            observed: format!("{comb_p99:.3} ms ({:.2}x)", comb_p99 / base_p99),
            passed: comb_p99 <= 2.0 * base_p99,
        },
    ];
    Ok(CompareReport {
        spike: "A-combined-commit",
        action: "compare",
        network_label: NETWORK_LABEL_TESTNET,
        baseline: args.baseline.display().to_string(),
        combined: args.combined.display().to_string(),
        same_range,
        same_density_and_pressure,
        ratios,
        thresholds,
    })
}
