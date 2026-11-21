//! Measure stock, ordinary-prefilter, and shifted-key scans of one prepared corpus.
use std::{io::Write as _, num::NonZeroUsize, path::PathBuf, time::Instant};

use clap::Parser;
use eyre::Result;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use wallet_scan_experiment::ScanComparison;

#[derive(Parser)]
struct Args {
    /// Corpus written by export-wallet-scan-corpus.
    corpus: PathBuf,
    /// Number of actions per pool in a trial-decryption batch.
    #[arg(long, default_value = "128")]
    batch_size: NonZeroUsize,
    /// Each cycle runs all six orderings of the three arms.
    #[arg(long, default_value = "1")]
    cycles: NonZeroUsize,
}

#[cfg(target_os = "linux")]
fn cpu_seconds() -> Option<f64> {
    let now = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    std::time::Duration::try_from(now)
        .ok()
        .map(|d| d.as_secs_f64())
}

#[cfg(not(target_os = "linux"))]
const fn cpu_seconds() -> Option<f64> {
    None
}

#[derive(Serialize)]
struct Sample {
    cycle: usize,
    order: usize,
    arm: &'static str,
    elapsed_ms: f64,
    process_cpu_ms: Option<f64>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let encoded = std::fs::read(args.corpus)?;
    let corpus_sha256 = hex::encode(Sha256::digest(&encoded));
    let executable_sha256 = hex::encode(Sha256::digest(std::fs::read(std::env::current_exe()?)?));
    let comparison = ScanComparison::new(&encoded, args.batch_size.get())?;
    let arms = ["stock", "ordinary-prefilter", "hinted"];
    for arm in arms {
        comparison.scan(arm)?;
    }
    let mut samples = Vec::new();
    let orders = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for cycle in 0..args.cycles.get() {
        for (order, sequence) in orders.iter().enumerate() {
            for &index in sequence {
                let arm = arms[index];
                let cpu_start = cpu_seconds();
                let start = Instant::now();
                comparison.scan(arm)?;
                let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                let process_cpu_ms = cpu_seconds()
                    .zip(cpu_start)
                    .map(|(end, begin)| (end - begin) * 1000.0);
                samples.push(Sample {
                    cycle,
                    order,
                    arm,
                    elapsed_ms,
                    process_cpu_ms,
                });
            }
        }
    }
    let diagnostics: serde_json::Value = serde_json::from_str(comparison.diagnostics())?;
    let mut out = std::io::stdout().lock();
    serde_json::to_writer_pretty(
        &mut out,
        &serde_json::json!({
            "corpus_sha256": corpus_sha256, "executable_sha256": executable_sha256,
            "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
            "batch_size": args.batch_size, "cycles": args.cycles, "diagnostics": diagnostics, "samples": samples
        }),
    )?;
    writeln!(out)?;
    Ok(())
}
