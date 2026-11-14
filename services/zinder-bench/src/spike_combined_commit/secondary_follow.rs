//! A canonical secondary in its own process, as a reader runtime runs it.
//!
//! The replay and reorg arms spawn this action so the reader's memory is not
//! counted in the writer's peak RSS. It opens a secondary on the primary,
//! catches up on an interval until the stop file appears, then measures one
//! final catch-up and checks that the wallet control it reads names exactly
//! the canonical fence it admitted.

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use clap::Args;
use serde::{Deserialize, Serialize};
use zinder_bench::BenchError;

use super::{
    NETWORK_LABEL_TESTNET,
    layout::{Layout, absolute, open_secondary, wallet_family_budget},
    metrics::{LatencySummary, peak_rss_bytes, summarize_seconds},
    replay::{FenceView, finish_secondary},
    spike_error,
};

/// Secondary follower inputs.
#[derive(Args)]
pub(crate) struct SecondaryFollowArgs {
    /// Network name.
    #[arg(long, default_value = "zcash-testnet")]
    network: String,
    /// Fixture directory (for the activation table).
    #[arg(long)]
    fixture: PathBuf,
    /// Primary database path.
    #[arg(long)]
    primary: PathBuf,
    /// Secondary metadata directory.
    #[arg(long)]
    secondary_dir: PathBuf,
    /// Catch-up interval while the writer runs (0 = only the final catch-up).
    #[arg(long, default_value_t = 1000)]
    poll_ms: u64,
    /// The follower stops polling once this file exists.
    #[arg(long)]
    stop_file: PathBuf,
    /// The follower writes this file after it opened and caught up once.
    #[arg(long)]
    ready_file: PathBuf,
    /// Report path.
    #[arg(long)]
    report: PathBuf,
    /// Canonical reorg window.
    #[arg(long, default_value_t = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS)]
    reorg_window_blocks: u32,
}

/// One on-request catch-up measured by the follower.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct CatchUpResponse {
    pub(crate) seconds: f64,
    pub(crate) tip_height: u32,
    pub(crate) event_sequence: u64,
    pub(crate) wallet_control_matches_fence: Option<bool>,
}

fn control_file(stop_file: &Path, name: &str) -> PathBuf {
    stop_file.with_file_name(name)
}

/// Secondary follower report.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct SecondaryFollowReport {
    pub(crate) network_label: String,
    pub(crate) primary: String,
    pub(crate) secondary_dir: String,
    pub(crate) open_seconds: f64,
    pub(crate) poll_ms: u64,
    pub(crate) periodic_catch_up_count: usize,
    pub(crate) periodic_catch_up_p50_ms: f64,
    pub(crate) periodic_catch_up_p99_ms: f64,
    pub(crate) periodic_catch_up_max_ms: f64,
    pub(crate) final_catch_up_seconds: f64,
    pub(crate) final_tip_height: u32,
    pub(crate) final_event_sequence: u64,
    pub(crate) wallet_control_matches_fence: Option<bool>,
    pub(crate) peak_vmhwm_bytes: Option<u64>,
    /// Catch-up calls that returned an error (the follower records and retries them).
    #[serde(default)]
    pub(crate) catch_up_errors: u64,
    /// The first catch-up error, with the time it happened.
    #[serde(default)]
    pub(crate) first_catch_up_error: Option<String>,
}

/// Catch-up errors seen by the follower.
#[derive(Default)]
struct ErrorLog {
    count: u64,
    first: Option<String>,
}

impl ErrorLog {
    fn record(&mut self, context: &str, error: &BenchError) {
        self.count += 1;
        let line = format!(
            "{} {context}: {error}",
            chrono_like_now()
        );
        tracing::warn!(
            target: "zinder::bench::spike",
            event = "spike_secondary_catch_up_error",
            count = self.count,
            error = %line,
            "secondary catch-up failed; retrying on the next poll"
        );
        if self.first.is_none() {
            self.first = Some(line);
        }
    }
}

fn chrono_like_now() -> String {
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("unix_ms={}", since.as_millis())
}

/// Retries one measured catch-up a few times, recording each failure.
fn finish_with_retry(
    secondary: &mut zinder_store::RocksDbCanonicalSecondary,
    combined: bool,
    errors: &mut ErrorLog,
) -> Result<(f64, FenceView, bool, Option<bool>), BenchError> {
    let mut attempt = 0;
    loop {
        let fence_before = secondary.event_fence();
        match finish_secondary(secondary, fence_before, combined) {
            Ok(outcome) => return Ok(outcome),
            Err(error) if attempt < 20 => {
                errors.record("measured catch-up", &error);
                attempt += 1;
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(error) => return Err(error),
        }
    }
}

/// Runs the follower until the stop file appears.
pub(crate) fn run_secondary_follow(args: &SecondaryFollowArgs) -> Result<(), BenchError> {
    let network = zinder_core::wire::decode_zinder_native_chain_name(&args.network)
        .map_err(|source| BenchError::invalid_argument(source.to_string()))?;
    let manifest = zinder_bench::fixture::FixtureManifest::read(&args.fixture)?;
    if manifest.network_typed()? != network {
        return Err(BenchError::invalid_argument("fixture network differs"));
    }
    let activations = manifest.activations_typed()?;
    let families = rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), &args.primary)
        .map_err(|source| spike_error("list column families", source))?;
    let combined = families
        .iter()
        .any(|name| name == zinder_wallet_rocksdb::SPIKE_WALLET_CONTROL_COLUMN_FAMILY);
    Layout {
        wallet: combined,
        mirrors: families.iter().any(|name| name.starts_with("mirror_")),
    }
    .register(wallet_family_budget());
    let started = Instant::now();
    let mut secondary = open_secondary(
        &args.primary,
        &args.secondary_dir,
        &activations,
        args.reorg_window_blocks,
    )?;
    let open_seconds = started.elapsed().as_secs_f64();
    std::fs::write(&args.ready_file, b"ready")
        .map_err(|source| BenchError::io(&args.ready_file, source))?;
    let mut durations = Vec::new();
    let poll = Duration::from_millis(args.poll_ms);
    let request_file = control_file(&args.stop_file, "request");
    let mut last_poll = Instant::now();
    let mut errors = ErrorLog::default();
    while !args.stop_file.exists() {
        if let Ok(encoded) = std::fs::read_to_string(&request_file) {
            let index = encoded.trim().to_owned();
            let (seconds, fence, _, wallet_matches) =
                finish_with_retry(&mut secondary, combined, &mut errors)?;
            let response = CatchUpResponse {
                seconds,
                tip_height: fence.tip_height,
                event_sequence: fence.event_sequence,
                wallet_control_matches_fence: wallet_matches,
            };
            let target = control_file(&args.stop_file, &format!("response-{index}.json"));
            let staging = control_file(&args.stop_file, &format!("response-{index}.tmp"));
            std::fs::write(&staging, serde_json::to_vec(&response)?)
                .map_err(|source| BenchError::io(&staging, source))?;
            std::fs::rename(&staging, &target).map_err(|source| BenchError::io(&target, source))?;
            std::fs::remove_file(&request_file)
                .map_err(|source| BenchError::io(&request_file, source))?;
            continue;
        }
        if args.poll_ms > 0 && last_poll.elapsed() >= poll {
            last_poll = Instant::now();
            let catch_up_started = Instant::now();
            match secondary.try_catch_up() {
                Ok(_) => durations.push(catch_up_started.elapsed().as_secs_f64()),
                Err(error) => errors.record("periodic catch-up", &BenchError::from(error)),
            }
            continue;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let (final_seconds, fence, _, wallet_matches) =
        finish_with_retry(&mut secondary, combined, &mut errors)?;
    let periodic: LatencySummary = summarize_seconds(&durations);
    let report = SecondaryFollowReport {
        network_label: NETWORK_LABEL_TESTNET.to_owned(),
        primary: absolute(&args.primary).display().to_string(),
        secondary_dir: absolute(&args.secondary_dir).display().to_string(),
        open_seconds,
        poll_ms: args.poll_ms,
        periodic_catch_up_count: periodic.count,
        periodic_catch_up_p50_ms: periodic.p50_ms,
        periodic_catch_up_p99_ms: periodic.p99_ms,
        periodic_catch_up_max_ms: periodic.max_ms,
        final_catch_up_seconds: final_seconds,
        final_tip_height: fence.tip_height,
        final_event_sequence: fence.event_sequence,
        wallet_control_matches_fence: wallet_matches,
        peak_vmhwm_bytes: peak_rss_bytes(),
        catch_up_errors: errors.count,
        first_catch_up_error: errors.first,
    };
    let encoded = serde_json::to_vec_pretty(&report)?;
    std::fs::write(&args.report, encoded).map_err(|source| BenchError::io(&args.report, source))
}

/// A follower child process owned by an arm.
pub(crate) struct SecondaryChild {
    child: Child,
    stop_file: PathBuf,
    report: PathBuf,
    requests: u64,
}

impl SecondaryChild {
    /// Spawns the follower and waits until it has opened.
    pub(crate) fn spawn(
        fixture: &Path,
        network: &str,
        primary: &Path,
        secondary_dir: &Path,
        poll_ms: u64,
        reorg_window_blocks: u32,
    ) -> Result<Self, BenchError> {
        std::fs::create_dir_all(secondary_dir)
            .map_err(|source| BenchError::io(secondary_dir, source))?;
        let control = secondary_dir.with_extension("control");
        std::fs::create_dir_all(&control).map_err(|source| BenchError::io(&control, source))?;
        let stop_file = control.join("stop");
        let ready_file = control.join("ready");
        let report = control.join("secondary-follow.json");
        let _ = std::fs::remove_file(&stop_file);
        let _ = std::fs::remove_file(&ready_file);
        let _ = std::fs::remove_file(&report);
        let exe = std::env::current_exe().map_err(|source| spike_error("current exe", source))?;
        let child = Command::new(exe)
            .args(["spike-combined-commit", "secondary-follow", "--network", network])
            .arg("--fixture")
            .arg(fixture)
            .arg("--primary")
            .arg(primary)
            .arg("--secondary-dir")
            .arg(secondary_dir)
            .arg("--poll-ms")
            .arg(poll_ms.to_string())
            .arg("--stop-file")
            .arg(&stop_file)
            .arg("--ready-file")
            .arg(&ready_file)
            .arg("--report")
            .arg(&report)
            .arg("--reorg-window-blocks")
            .arg(reorg_window_blocks.to_string())
            .stdin(Stdio::null())
            .spawn()
            .map_err(|source| spike_error("spawn secondary follower", source))?;
        let mut owned = Self {
            child,
            stop_file,
            report,
            requests: 0,
        };
        let deadline = Instant::now() + Duration::from_secs(600);
        while !ready_file.exists() {
            if let Some(status) = owned
                .child
                .try_wait()
                .map_err(|source| spike_error("secondary follower wait", source))?
            {
                return Err(BenchError::spike(format!(
                    "secondary follower exited before opening: {status}"
                )));
            }
            if Instant::now() > deadline {
                return Err(BenchError::spike("secondary follower did not open"));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(owned)
    }

    /// Asks the follower for one timed catch-up and waits for its answer.
    pub(crate) fn request_catch_up(&mut self) -> Result<CatchUpResponse, BenchError> {
        self.requests += 1;
        let index = self.requests;
        let response = control_file(&self.stop_file, &format!("response-{index}.json"));
        let request = control_file(&self.stop_file, "request");
        let staging = control_file(&self.stop_file, "request.tmp");
        std::fs::write(&staging, index.to_string()).map_err(|source| BenchError::io(&staging, source))?;
        std::fs::rename(&staging, &request).map_err(|source| BenchError::io(&request, source))?;
        let deadline = Instant::now() + Duration::from_secs(1800);
        loop {
            if let Ok(bytes) = std::fs::read(&response) {
                return Ok(serde_json::from_slice(&bytes)?);
            }
            if let Some(status) = self
                .child
                .try_wait()
                .map_err(|source| spike_error("secondary follower wait", source))?
            {
                return Err(BenchError::spike(format!(
                    "secondary follower exited during a catch-up request: {status}"
                )));
            }
            if Instant::now() > deadline {
                return Err(BenchError::spike("secondary catch-up request timed out"));
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// Signals the follower, waits, and returns its report.
    pub(crate) fn finish(mut self) -> Result<SecondaryFollowReport, BenchError> {
        std::fs::write(&self.stop_file, b"stop")
            .map_err(|source| BenchError::io(&self.stop_file, source))?;
        let status = self
            .child
            .wait()
            .map_err(|source| spike_error("secondary follower wait", source))?;
        if !status.success() {
            return Err(BenchError::spike(format!(
                "secondary follower failed: {status}"
            )));
        }
        let bytes = std::fs::read(&self.report).map_err(|source| BenchError::io(&self.report, source))?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub(crate) fn report_path(&self) -> &Path {
        &self.report
    }
}

/// Converts a child report into the arm's secondary section.
pub(crate) fn fence_matches(report: &SecondaryFollowReport, primary: &FenceView) -> bool {
    report.final_tip_height == primary.tip_height
        && report.final_event_sequence == primary.event_sequence
}
