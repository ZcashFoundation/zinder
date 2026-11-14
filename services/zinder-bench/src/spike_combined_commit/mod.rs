//! Spike A: wallet rows in the canonical synced `WriteBatch`, one database.
//!
//! Throwaway benchmark harness. Every arm reuses the production canonical
//! commit (`PreparedLiveAppendCommit`, `PreparedLiveReplacement`, the synced
//! write, and the canonical readback) and the production wallet transition
//! planner (`apply_block_rows`, `rollback_range`, `advance_settled_tip`). The
//! only new mechanism is the glue that lets both write into one batch.
#![allow(
    dead_code,
    unreachable_pub,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "throwaway spike harness; skipped repository lint gate by design"
)]

pub(crate) mod compare;
pub(crate) mod layout;
pub(crate) mod metrics;
pub(crate) mod mirror;
pub(crate) mod pipeline;
pub(crate) mod prepare;
pub(crate) mod reorg;
pub(crate) mod replay;
pub(crate) mod verify;

use std::{fs::OpenOptions, io::Write, path::PathBuf};

use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use zinder_bench::{BenchError, fixture::FixtureManifest};
use zinder_core::{Network, NetworkUpgradeActivations, wire::decode_zinder_native_chain_name};

/// Label attached to every report; all inputs on this host are Testnet.
pub(crate) const NETWORK_LABEL_TESTNET: &str = "Testnet";

/// Spike A command family.
#[derive(Args)]
pub(crate) struct SpikeCombinedCommitArgs {
    #[command(subcommand)]
    action: SpikeAction,
}

#[derive(Subcommand)]
enum SpikeAction {
    /// Build canonical-only and combined golden stores at H0 from READY H0 stores.
    Prepare(prepare::PrepareArgs),
    /// Arm 1, Arm 3, and the density arm: replay H0+1..T at one synced commit per block.
    Replay(replay::ReplayArgs),
    /// Arm 2: repeated 1000-block replacements at the tip.
    Reorg(reorg::ReorgArgs),
    /// Compare the combined wallet digest with a reference wallet built by the projector path.
    Verify(verify::VerifyArgs),
    /// Compare a combined replay report with its canonical-only baseline report.
    Compare(compare::CompareArgs),
}

/// Whether wallet rows join the canonical batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CommitMode {
    /// Canonical rows only (today's canonical writer).
    CanonicalOnly,
    /// Canonical plus wallet rows in one synced batch, one database.
    Combined,
}

/// Inputs shared by every action.
#[derive(Args, Clone, Debug)]
pub(crate) struct CommonArgs {
    /// Network name.
    #[arg(long, default_value = "zcash-testnet")]
    pub(crate) network: String,
    /// Fixture directory holding blocks H0+1..T and the node's activation table.
    #[arg(long)]
    pub(crate) fixture: PathBuf,
    /// Write the JSON report here (refuses to overwrite) instead of stdout.
    #[arg(long)]
    pub(crate) report: Option<PathBuf>,
    /// Canonical reorg window the stores were built with.
    #[arg(long, default_value_t = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS)]
    pub(crate) reorg_window_blocks: u32,
    /// Writer resource budget for the combined database.
    #[arg(long, value_enum, default_value_t = layout::BudgetProfile::Summed)]
    pub(crate) budget_profile: layout::BudgetProfile,
    /// Override `max_background_jobs` for the writer database.
    #[arg(long)]
    pub(crate) max_background_jobs: Option<i32>,
    /// `RocksDB` statistics level for the writer database.
    #[arg(long, value_enum, default_value_t = layout::StatisticsLevelArg::Tickers)]
    pub(crate) statistics_level: layout::StatisticsLevelArg,
    /// Free-form label copied into the report.
    #[arg(long)]
    pub(crate) label: Option<String>,
}

impl CommonArgs {
    pub(crate) fn network(&self) -> Result<Network, BenchError> {
        decode_zinder_native_chain_name(&self.network)
            .map_err(|source| BenchError::invalid_argument(source.to_string()))
    }

    pub(crate) fn manifest_and_activations(
        &self,
    ) -> Result<(FixtureManifest, NetworkUpgradeActivations), BenchError> {
        let manifest = FixtureManifest::read(&self.fixture)?;
        if manifest.network_typed()? != self.network()? {
            return Err(BenchError::invalid_argument(
                "fixture network differs from --network",
            ));
        }
        let activations = manifest.activations_typed()?;
        Ok((manifest, activations))
    }
}

/// Runs one Spike A action and emits its report.
pub(crate) async fn run_spike_combined_commit(
    args: SpikeCombinedCommitArgs,
) -> Result<(), BenchError> {
    match args.action {
        SpikeAction::Prepare(args) => {
            let report_path = args.common.report.clone();
            let report = prepare::run_prepare(args).await?;
            write_report(&report, report_path.as_deref())
        }
        SpikeAction::Replay(args) => {
            let report_path = args.common.report.clone();
            let report = replay::run_replay(args).await?;
            write_report(&report, report_path.as_deref())
        }
        SpikeAction::Reorg(args) => {
            let report_path = args.common.report.clone();
            let report = reorg::run_reorg(args).await?;
            write_report(&report, report_path.as_deref())
        }
        SpikeAction::Compare(args) => {
            let report_path = args.report.clone();
            let report = compare::run_compare(&args)?;
            write_report(&report, report_path.as_deref())
        }
        SpikeAction::Verify(args) => {
            let report_path = args.common.report.clone();
            let report = verify::run_verify(args)?;
            let passed = report.digest_equal && report.combined_rows_match_evidence;
            write_report(&report, report_path.as_deref())?;
            if passed {
                Ok(())
            } else {
                Err(BenchError::spike("wallet digest verification failed"))
            }
        }
    }
}

fn write_report(report: &impl Serialize, path: Option<&std::path::Path>) -> Result<(), BenchError> {
    let encoded = serde_json::to_vec_pretty(report)?;
    match path {
        Some(path) => {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|source| BenchError::io(path, source))?;
            file.write_all(&encoded)
                .map_err(|source| BenchError::io(path, source))?;
            file.sync_all().map_err(|source| BenchError::io(path, source))
        }
        None => {
            print_report(&encoded);
            Ok(())
        }
    }
}

#[allow(clippy::print_stdout, reason = "the JSON report is the structured output")]
fn print_report(encoded: &[u8]) {
    println!("{}", String::from_utf8_lossy(encoded));
}

pub(crate) fn spike_error(context: &str, error: impl std::fmt::Display) -> BenchError {
    BenchError::spike(format!("{context}: {error}"))
}

pub(crate) fn hex32(bytes: [u8; 32]) -> String {
    hex::encode(bytes)
}
