//! One-shot wallet rebuild from an existing authenticated canonical store.
#![allow(
    clippy::all,
    clippy::pedantic,
    reason = "spike: RSS and staging sampling added for the in-process serving rebuild arm"
)]

use std::{path::PathBuf, time::Duration};

use clap::Args;
use serde::Serialize;
use zinder_bench::BenchError;
use zinder_core::wire::decode_zinder_native_chain_name;
use zinder_source::{CookieSource, NodeAuth, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalStore,
    RocksDbResourceBudget,
};
use zinder_wallet_rocksdb::{RocksDbWalletBuildOptions, build_wallet_from_canonical};

const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_SUPPORTED_REORG_DEPTH: u32 = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS;
const WALLET_OUTPOINT_SORT_MEMORY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER: u64 = 1024 * 1024 * 1024;
const WALLET_TEMPORARY_FILE_BYTES_PER_SORTER: u64 = 64 * 1024 * 1024 * 1024;
const WALLET_SST_TARGET_LOGICAL_BYTES: u64 = 128 * 1024 * 1024;
const WALLET_ACCOUNTED_REORG_UNDO_BYTES: u64 = 512 * 1024 * 1024;

/// CLI contract for rebuilding one wallet store at a READY canonical fence.
#[derive(Args)]
pub(crate) struct RocksDbWalletRebuildArgs {
    /// Network name, such as zcash-testnet.
    #[arg(long)]
    network: String,
    /// Zebra JSON-RPC base URL used only to authenticate network activations.
    #[arg(long = "json-rpc-addr")]
    json_rpc_addr: String,
    /// Optional node cookie file path.
    #[arg(long = "node-auth-cookie")]
    node_auth_cookie: Option<PathBuf>,
    /// Existing READY canonical store.
    #[arg(long = "canonical-store")]
    canonical_store: PathBuf,
    /// Fresh destination for the wallet store.
    #[arg(long = "wallet-store")]
    wallet_store: PathBuf,
    /// Persisted raw-blob retention contract expected on the canonical store.
    #[arg(
        long = "raw-blob-retention",
        default_value = "transactions",
        value_parser = parse_raw_blob_retention
    )]
    raw_blob_retention: RawBlobRetention,
    /// Per-request source timeout in seconds.
    #[arg(long = "request-timeout-secs", default_value_t = DEFAULT_REQUEST_TIMEOUT_SECONDS)]
    request_timeout_seconds: u64,
    /// Maximum accepted source response body.
    #[arg(long, default_value_t = DEFAULT_MAX_RESPONSE_BYTES)]
    max_response_bytes: u64,
    /// Number of exact-tip wallet undo rows retained for reorg handling.
    #[arg(long, default_value_t = DEFAULT_SUPPORTED_REORG_DEPTH)]
    supported_reorg_depth: u32,
}

/// Reviewer-scannable evidence from one wallet-only rebuild.
#[derive(Serialize)]
pub(crate) struct RocksDbWalletRebuildReport {
    canonical_tip_height: u32,
    canonical_epoch: u64,
    canonical_event_sequence: u64,
    wallet_tip_height: u32,
    wallet_epoch: u64,
    wallet_event_sequence: u64,
    wallet_projection_digest_hex: String,
    scanned_block_count: u64,
    scanned_transaction_count: u64,
    historical_prevout_read_count: u64,
    store_initialization_seconds: f64,
    canonical_scan_seconds: f64,
    outpoint_sort_seconds: f64,
    outpoint_merge_seconds: f64,
    secondary_row_derivation_seconds: f64,
    logical_evidence_seconds: f64,
    row_load_seconds: f64,
    flush_and_cold_reopen_seconds: f64,
    cold_validation_seconds: f64,
    cold_validation_primary_output_scan_seconds: f64,
    cold_validation_address_index_and_balance_readback_seconds: f64,
    cold_validation_address_transaction_readback_seconds: f64,
    cold_validation_reorg_undo_readback_seconds: f64,
    ready_publication_seconds: f64,
    total_seconds: f64,
    /// Spike: process peak RSS (VmHWM) after the build.
    peak_rss_bytes: Option<u64>,
    /// Spike: largest sampled VmRSS during the build.
    max_sampled_rss_bytes: u64,
    /// Spike: largest sampled size of the projection staging sibling directory.
    max_sampled_staging_bytes: u64,
    /// Spike: size of the canonical store directory.
    canonical_store_bytes: u64,
    /// Spike: size of the finished wallet store directory.
    wallet_store_bytes: u64,
    staged_output_count: u64,
    staged_spend_count: u64,
    logical_row_bytes: u64,
    sst_file_bytes: u64,
    sst_file_count: u64,
    row_counts: RowCounts,
    /// Spike: external-sort evidence (compare with the documented Mainnet outpoint sorter size).
    outpoint_sort: SortEvidence,
    address_index_sort: SortEvidence,
    address_transaction_sort: SortEvidence,
}

/// Spike: one external sorter's evidence.
#[derive(Serialize)]
struct SortEvidence {
    record_count: u64,
    initial_run_count: u64,
    merge_pass_count: u64,
    peak_accounted_memory_bytes: u64,
    peak_temporary_file_bytes: u64,
    final_run_file_bytes: u64,
}

impl From<zinder_rocksdb_bulk_load::VariableValueSortEvidence> for SortEvidence {
    fn from(evidence: zinder_rocksdb_bulk_load::VariableValueSortEvidence) -> Self {
        Self {
            record_count: evidence.record_count,
            initial_run_count: evidence.initial_run_count,
            merge_pass_count: evidence.merge_pass_count,
            peak_accounted_memory_bytes: evidence.peak_accounted_memory_bytes,
            peak_temporary_file_bytes: evidence.peak_temporary_file_bytes,
            final_run_file_bytes: evidence.final_run_file_bytes,
        }
    }
}

/// Spike: published row counts per wallet family.
#[derive(Serialize)]
struct RowCounts {
    transparent_unspent_output: u64,
    transparent_unspent_output_by_address: u64,
    transparent_spent_output: u64,
    transparent_address_transaction: u64,
    transparent_address_balance: u64,
    reorg_undo: u64,
}

fn directory_bytes(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(metadata) if metadata.is_dir() => directory_bytes(&entry.path()),
            Ok(metadata) => metadata.len(),
            Err(_) => 0,
        })
        .sum()
}

fn current_rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .trim()
                    .trim_end_matches("kB")
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
        })
        .map_or(0, |kib| kib * 1024)
}

/// Rebuilds one wallet projection from the exact admitted canonical fence.
pub(crate) async fn run_rocksdb_wallet_rebuild(
    args: RocksDbWalletRebuildArgs,
) -> Result<RocksDbWalletRebuildReport, BenchError> {
    validate_rebuild_args(&args)?;

    let network = decode_zinder_native_chain_name(&args.network)
        .map_err(|error| BenchError::invalid_argument(error.to_string()))?;
    let node_auth = args.node_auth_cookie.map_or(NodeAuth::None, |path| {
        NodeAuth::Cookie(CookieSource::File(path))
    });
    let source = ZebraJsonRpcSource::with_options(
        network,
        args.json_rpc_addr,
        node_auth,
        ZebraJsonRpcSourceOptions {
            request_timeout: Duration::from_secs(args.request_timeout_seconds),
            max_response_bytes: std::num::NonZeroU64::new(args.max_response_bytes).ok_or_else(
                || BenchError::invalid_argument("max response bytes must be non-zero"),
            )?,
            broadcast_timeout: None,
        },
    )?;
    let activations = source
        .discover_network_upgrade_activations("zinder-bench-wallet-rebuild")
        .await?;
    let canonical_store = RocksDbCanonicalStore::open_ready(
        &args.canonical_store,
        &activations,
        CanonicalStoreWorkload::Wallet,
        args.raw_blob_retention,
        CanonicalReorgPolicy::new(args.supported_reorg_depth)?,
        RocksDbResourceBudget::canonical_reader_defaults(),
    )?;
    let canonical_ready = canonical_store.ready_evidence();
    let staging_path = {
        let mut name = args.wallet_store.as_os_str().to_owned();
        name.push(".projection-load-staging");
        std::path::PathBuf::from(name)
    };
    let sampler_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let stop = std::sync::Arc::clone(&sampler_stop);
        let staging_path = staging_path.clone();
        std::thread::spawn(move || {
            let (mut max_rss, mut max_staging) = (0_u64, 0_u64);
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                max_rss = max_rss.max(current_rss_bytes());
                max_staging = max_staging.max(directory_bytes(&staging_path));
                std::thread::sleep(Duration::from_millis(500));
            }
            (max_rss, max_staging)
        })
    };
    let outcome = build_wallet_from_canonical(
        &canonical_store,
        &args.wallet_store,
        RocksDbWalletBuildOptions {
            resource_budget: RocksDbResourceBudget::materialized_view_writer_defaults(),
            max_outpoint_sort_memory_bytes: WALLET_OUTPOINT_SORT_MEMORY_BYTES,
            max_secondary_sort_memory_bytes_per_sorter:
                WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER,
            max_temporary_file_bytes_per_sorter: WALLET_TEMPORARY_FILE_BYTES_PER_SORTER,
            sst_target_logical_bytes: WALLET_SST_TARGET_LOGICAL_BYTES,
            max_accounted_reorg_undo_bytes: WALLET_ACCOUNTED_REORG_UNDO_BYTES,
            supported_reorg_depth: args.supported_reorg_depth,
        },
    );
    sampler_stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let (max_sampled_rss_bytes, max_sampled_staging_bytes) = sampler.join().unwrap_or((0, 0));
    let outcome = outcome?;
    let mut report = rebuild_report(&canonical_ready, &outcome.report);
    report.peak_rss_bytes = zinder_bench::rss::peak_rss().bytes;
    report.max_sampled_rss_bytes = max_sampled_rss_bytes;
    report.max_sampled_staging_bytes = max_sampled_staging_bytes;
    report.canonical_store_bytes = directory_bytes(&args.canonical_store);
    report.wallet_store_bytes = directory_bytes(&args.wallet_store);
    Ok(report)
}

fn rebuild_report(
    canonical_ready: &zinder_store::CanonicalStoreReadyEvidence,
    report: &zinder_wallet_rocksdb::RocksDbWalletBuildReport,
) -> RocksDbWalletRebuildReport {
    let wallet_source = report.source_position;
    let durations = report.phase_durations;
    let validation_phases = durations.cold_validation_phases;

    RocksDbWalletRebuildReport {
        canonical_tip_height: canonical_ready.visible_tip.height.value(),
        canonical_epoch: canonical_ready.visible_epoch.value(),
        canonical_event_sequence: canonical_ready.visible_event_sequence,
        wallet_tip_height: wallet_source.tip.height.value(),
        wallet_epoch: wallet_source.chain_epoch_id.value(),
        wallet_event_sequence: wallet_source.event_sequence,
        wallet_projection_digest_hex: hex::encode(report.projection_digest.as_bytes()),
        scanned_block_count: report.scanned_block_count,
        scanned_transaction_count: report.scanned_transaction_count,
        historical_prevout_read_count: report.historical_prevout_read_count,
        store_initialization_seconds: durations.store_initialization.as_secs_f64(),
        canonical_scan_seconds: durations.canonical_scan.as_secs_f64(),
        outpoint_sort_seconds: durations.outpoint_sort.as_secs_f64(),
        outpoint_merge_seconds: durations.outpoint_merge.as_secs_f64(),
        secondary_row_derivation_seconds: durations.secondary_row_derivation.as_secs_f64(),
        logical_evidence_seconds: durations.logical_evidence.as_secs_f64(),
        row_load_seconds: durations.row_load.as_secs_f64(),
        flush_and_cold_reopen_seconds: durations.flush_and_cold_reopen.as_secs_f64(),
        cold_validation_seconds: durations.cold_validation.as_secs_f64(),
        cold_validation_primary_output_scan_seconds: validation_phases
            .primary_output_scan
            .as_secs_f64(),
        cold_validation_address_index_and_balance_readback_seconds: validation_phases
            .address_index_and_balance_readback
            .as_secs_f64(),
        cold_validation_address_transaction_readback_seconds: validation_phases
            .address_transaction_readback
            .as_secs_f64(),
        cold_validation_reorg_undo_readback_seconds: validation_phases
            .reorg_undo_readback
            .as_secs_f64(),
        ready_publication_seconds: durations.ready_publication.as_secs_f64(),
        total_seconds: durations.total.as_secs_f64(),
        peak_rss_bytes: None,
        max_sampled_rss_bytes: 0,
        max_sampled_staging_bytes: 0,
        canonical_store_bytes: 0,
        wallet_store_bytes: 0,
        staged_output_count: report.staged_output_count,
        staged_spend_count: report.staged_spend_count,
        logical_row_bytes: report.logical_row_bytes,
        sst_file_bytes: report.sst_file_bytes,
        sst_file_count: report.sst_file_count,
        row_counts: RowCounts {
            transparent_unspent_output: report.row_counts.transparent_unspent_output_count,
            transparent_unspent_output_by_address: report
                .row_counts
                .transparent_unspent_output_by_address_count,
            transparent_spent_output: report.row_counts.transparent_spent_output_count,
            transparent_address_transaction: report
                .row_counts
                .transparent_address_transaction_count,
            transparent_address_balance: report.row_counts.transparent_address_balance_count,
            reorg_undo: report.row_counts.reorg_undo_count,
        },
        outpoint_sort: report.outpoint_sort.into(),
        address_index_sort: report.address_index_sort.into(),
        address_transaction_sort: report.address_transaction_sort.into(),
    }
}

fn parse_raw_blob_retention(encoded: &str) -> Result<RawBlobRetention, String> {
    RawBlobRetention::from_kebab_case(encoded)
        .ok_or_else(|| "--raw-blob-retention must be one of none, transactions, or all".to_owned())
}

fn validate_rebuild_args(args: &RocksDbWalletRebuildArgs) -> Result<(), BenchError> {
    if args.request_timeout_seconds == 0 {
        return Err(BenchError::invalid_argument(
            "--request-timeout-secs must be greater than zero",
        ));
    }
    if args.max_response_bytes == 0 {
        return Err(BenchError::invalid_argument(
            "--max-response-bytes must be greater than zero",
        ));
    }
    if args.canonical_store == args.wallet_store {
        return Err(BenchError::invalid_argument(
            "--canonical-store and --wallet-store must be different paths",
        ));
    }

    Ok(())
}
