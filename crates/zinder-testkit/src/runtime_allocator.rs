//! Probe that a service binary allocates through jemalloc.
//!
//! On Linux, `zinder-runtime` links jemalloc with unprefixed symbols, so it
//! replaces the C library `malloc` that `RocksDB` allocates through. jemalloc
//! reads `MALLOC_CONF` only when it owns those symbols, which makes its
//! statistics report under `MALLOC_CONF=stats_print:true` evidence that the
//! native allocations of the process use it. The probe cannot tell whether the
//! binary also declares `zinder_runtime::RuntimeAllocator` as its global
//! allocator, because linking `zinder-runtime` alone produces the same report.

use std::{path::Path, process::Command};

use eyre::{Result, WrapErr as _, ensure};

/// Line jemalloc writes before the statistics it prints at exit.
const JEMALLOC_STATISTICS_BANNER: &str = "___ Begin jemalloc statistics ___";

/// Fails unless the binary at `executable` allocates through jemalloc in
/// place of the C library allocator.
///
/// Runs `executable --version` with jemalloc asked to print its statistics at
/// exit. Linux builds install jemalloc; other platforms keep the system
/// allocator and fail this check.
pub fn check_binary_allocates_through_jemalloc(executable: impl AsRef<Path>) -> Result<()> {
    let executable = executable.as_ref();
    let output = Command::new(executable)
        .arg("--version")
        .env("MALLOC_CONF", "stats_print:true")
        .output()
        .wrap_err_with(|| format!("run {} --version", executable.display()))?;
    ensure!(
        output.status.success(),
        "{} --version exited with {}",
        executable.display(),
        output.status
    );
    ensure!(
        String::from_utf8_lossy(&output.stderr).contains(JEMALLOC_STATISTICS_BANNER),
        "{} printed no jemalloc statistics, so the C library allocator still serves it",
        executable.display()
    );
    Ok(())
}
