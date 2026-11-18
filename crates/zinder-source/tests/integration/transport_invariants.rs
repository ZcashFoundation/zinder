#![allow(
    missing_docs,
    reason = "Integration test names describe the transport-policy invariants under test."
)]

use std::fs;
use std::path::{Path, PathBuf};

use eyre::{Result, ensure, eyre};

/// The two files that own Zebra and intra-Zinder transport construction.
///
/// `zinder-runtime::transport` owns intra-Zinder gRPC channels;
/// `zinder-source::transport` owns every long-lived client to a Zebra
/// full node. ADR-0007 codifies the split.
const CORE_TRANSPORT_OWNERSHIP: &[&str] = &[
    "crates/zinder-runtime/src/transport.rs",
    "crates/zinder-source/src/transport.rs",
];

/// Service-owned transports that call external product dependencies.
const EXTERNAL_PRODUCT_HTTP_TRANSPORT_OWNERSHIP: &[&str] =
    &["services/zinder-compat-cipherscan/src/market_price.rs"];

/// gRPC endpoint owners: the core transports, and the benchmark load client,
/// which calls lightwalletd servers outside Zinder without keep-alive pings.
const GRPC_ENDPOINT_OWNERSHIP: &[&str] = &[
    "crates/zinder-runtime/src/transport.rs",
    "crates/zinder-source/src/transport.rs",
    "services/zinder-bench/src/wallet_load/transport.rs",
];

const SCANNED_DIRECTORIES: &[&str] = &["crates", "services"];

/// Floor for the Rust sources one scan root must yield. The tree holds several
/// times this many, so only a missing or emptied tree falls below it.
const MINIMUM_SCANNED_FILES_PER_DIRECTORY: usize = 50;

/// Patterns that may only appear inside the transport modules.
///
/// Each entry pairs a code fragment with a short reason that is
/// included in the failure message so an offending change knows which
/// helper to call instead.
struct BannedPattern {
    needle: &'static str,
    explanation: &'static str,
    allowed_owners: &'static [&'static str],
}

const BANNED_TRANSPORT_PATTERNS: &[BannedPattern] = &[
    BannedPattern {
        needle: "Endpoint::from_shared",
        explanation: "use zinder_runtime::connect_zinder_grpc for intra-Zinder channels, \
             zinder_source::connect_zebra_indexer_channel for Zebra Indexer gRPC, \
             or zinder_runtime::validate_zinder_grpc_endpoint to validate a URL at config load",
        allowed_owners: GRPC_ENDPOINT_OWNERSHIP,
    },
    BannedPattern {
        needle: "HttpClientBuilder",
        explanation: "use zinder_source::build_zebra_json_rpc_client to construct the Zebra \
             JSON-RPC client; ResilientClient wraps it so the underlying connection \
             rebuilds after transport-class failures",
        allowed_owners: CORE_TRANSPORT_OWNERSHIP,
    },
    BannedPattern {
        needle: "reqwest::Client::builder",
        explanation: "Zebra HTTP clients belong in zinder_source::transport; external-product \
             HTTP clients must live in an explicitly owned service transport module",
        allowed_owners: EXTERNAL_PRODUCT_HTTP_TRANSPORT_OWNERSHIP,
    },
];

/// Walks Rust source files under `directory`, excluding `target`,
/// `.tmp`, and per-crate `tests/` subtrees.
///
/// Test files build mock transports and routinely construct
/// `Endpoint::from_shared` directly to exercise misconfigured paths
/// (e.g. wrong port, bad token). Forcing those through the transport
/// module would obscure the test intent without preventing the
/// regression the invariant exists to prevent: production code drifting
/// off the canonical seam.
fn production_source_files(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    collect_production_source_files(directory, &mut paths)?;
    ensure_scan_saw_source_tree(directory, paths.len())?;
    paths.sort();
    Ok(paths)
}

fn collect_production_source_files(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            if path
                .file_name()
                .is_some_and(|name| name == "target" || name == ".tmp" || name == "tests")
            {
                continue;
            }
            collect_production_source_files(&path, paths)?;
        } else if file_type.is_file() && path.extension().is_some_and(|extension| extension == "rs")
        {
            paths.push(path);
        }
    }
    Ok(())
}

/// Yields source lines that are not Rust line comments.
///
/// Module-level docs and inline doc comments often quote the banned
/// identifier (`Endpoint::from_shared`, `HttpClientBuilder`) for
/// pedagogical reasons. The check only flags emissions in real code.
fn code_lines(source: &str) -> impl Iterator<Item = &str> {
    source
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
}

#[test]
fn transport_construction_lives_only_in_transport_modules() -> Result<()> {
    let root = workspace_root()?;
    let mut offenders: Vec<(PathBuf, String, &'static BannedPattern)> = Vec::new();
    for directory in SCANNED_DIRECTORIES {
        for source_path in production_source_files(&root.join(directory))? {
            let contents = fs::read_to_string(&source_path)?;
            for line in code_lines(&contents) {
                for pattern in BANNED_TRANSPORT_PATTERNS {
                    let is_allowed_owner = pattern
                        .allowed_owners
                        .iter()
                        .any(|owner| root.join(owner) == source_path);
                    if line.contains(pattern.needle) && !is_allowed_owner {
                        offenders.push((source_path.clone(), line.trim().to_owned(), pattern));
                    }
                }
            }
        }
    }

    if offenders.is_empty() {
        return Ok(());
    }

    let formatted_offenders = offenders
        .iter()
        .map(|(path, line, pattern)| {
            format!(
                "  {}:\n    found: {line}\n    pattern: {}\n    instead: {}",
                path.display(),
                pattern.needle,
                pattern.explanation
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    Err(eyre!(
        "transport construction must live in its explicitly owning module \
         (see ADR-0007). Offending production-source sites:\n{formatted_offenders}",
    ))
}

/// Resolves the workspace root from the manifest directory of this test's crate.
///
/// Nextest sets `CARGO_MANIFEST_DIR` at run time, rewritten by
/// `--workspace-remap` when the tests run from an archive. The compile-time
/// value names the build machine's checkout, so it is only the fallback for
/// runners that do not set the variable.
fn workspace_root() -> Result<PathBuf> {
    let manifest_directory = std::env::var_os("CARGO_MANIFEST_DIR")
        .map_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")), PathBuf::from);
    manifest_directory
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| eyre!("CARGO_MANIFEST_DIR has no grandparent: {manifest_directory:?}"))
}

/// Fails when a scan root holds too few Rust sources to be the workspace tree.
///
/// A scan that finds nothing would pass every invariant vacuously, for
/// example when archived tests run without the checkout beside them.
fn ensure_scan_saw_source_tree(directory: &Path, scanned_file_count: usize) -> Result<()> {
    ensure!(
        scanned_file_count >= MINIMUM_SCANNED_FILES_PER_DIRECTORY,
        "scanned {scanned_file_count} Rust sources under {}, expected at least \
         {MINIMUM_SCANNED_FILES_PER_DIRECTORY}; the invariant tests need the workspace source tree \
         at the path nextest reports in CARGO_MANIFEST_DIR",
        directory.display(),
    );
    Ok(())
}
