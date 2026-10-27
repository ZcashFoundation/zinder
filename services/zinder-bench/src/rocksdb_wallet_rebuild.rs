//! One-shot wallet rebuild from an existing authenticated canonical store.

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
    ready_publication_seconds: f64,
    total_seconds: f64,
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
    )?;
    let report = outcome.report;
    let wallet_source = report.source_position;
    let durations = report.phase_durations;

    Ok(RocksDbWalletRebuildReport {
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
        ready_publication_seconds: durations.ready_publication.as_secs_f64(),
        total_seconds: durations.total.as_secs_f64(),
    })
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
