//! Command-line entry point for the fixed-range benchmark harness.

use std::{
    fs::OpenOptions,
    io::Write,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroU64},
    path::PathBuf,
    process::ExitCode,
    time::Duration,
};

use clap::{Args, Parser, Subcommand};
use zinder_bench::{
    BenchError,
    canonical_fixture_replay::capture_canonical_fixture_replay_plan,
    canonical_fixture_transport_server::{
        CanonicalFixtureTransportServerConfig, run_canonical_fixture_transport_server,
    },
    capture::{CaptureConfig, capture_fixed_range},
    fixture::FixtureManifest,
};
use zinder_core::{BlockHeight, Network, wire::decode_zinder_native_chain_name};
use zinder_source::{CookieSource, NodeAuth, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};

#[path = "canonical_replay_storage/command.rs"]
mod canonical_replay_storage_command;
mod rocksdb_canonical_fixture_replay;
mod rocksdb_compact_block_range;
mod rocksdb_raw_blob_retention_comparison;
mod rocksdb_storage_lifecycle;
mod rocksdb_wallet_rebuild;

use canonical_replay_storage_command::{CanonicalReplayStorageArgs, run_canonical_replay_storage};
use rocksdb_canonical_fixture_replay::{
    RocksDbCanonicalFixtureReplayArgs, run_rocksdb_canonical_fixture_replay,
};
use rocksdb_compact_block_range::{RocksDbCompactBlockRangeArgs, run_rocksdb_compact_block_range};
use rocksdb_raw_blob_retention_comparison::{
    RocksDbRawBlobRetentionComparisonArgs, run_rocksdb_raw_blob_retention_comparison,
};
use rocksdb_storage_lifecycle::{RocksDbStorageLifecycleArgs, run_rocksdb_storage_lifecycle};
use rocksdb_wallet_rebuild::{RocksDbWalletRebuildArgs, run_rocksdb_wallet_rebuild};

const DEFAULT_FROM_HEIGHT: u32 = 150_000;
const DEFAULT_TO_HEIGHT: u32 = 200_000;
const DEFAULT_SEGMENT_BLOCKS: u32 = 1_000;
const DEFAULT_FETCH_CONCURRENCY: u32 = 16;
const DEFAULT_CAPTURE_PREPARE_CONCURRENCY: u32 = 10;
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Parser)]
#[command(name = "zinder-bench")]
#[command(about = "Zinder fixed-range storage benchmark harness")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Capture raw source payloads for a block range into a fixture directory.
    Capture(CaptureArgs),
    /// Capture predecessor and fixed-tip checkpoints for canonical fixture replay.
    #[command(name = "capture-canonical-fixture-checkpoints")]
    CaptureCanonicalFixtureCheckpoints(CaptureCanonicalFixtureCheckpointsArgs),
    /// Persist and read back backend-neutral canonical replay records.
    #[command(name = "canonical-replay-storage")]
    CanonicalReplayStorage(CanonicalReplayStorageArgs),
    /// Replay an authenticated fixture into a fresh canonical `RocksDB` store.
    #[command(name = "rocksdb-canonical-fixture-replay")]
    RocksDbCanonicalFixtureReplay(RocksDbCanonicalFixtureReplayArgs),
    /// Compare transaction-only and complete raw-blob retention over one fixture.
    #[command(name = "rocksdb-raw-blob-retention-comparison")]
    RocksDbRawBlobRetentionComparison(RocksDbRawBlobRetentionComparisonArgs),
    /// Serve an immutable canonical fixture through JSON-RPC and indexer gRPC.
    #[command(name = "serve-canonical-fixture-transports")]
    ServeCanonicalFixtureTransports(ServeCanonicalFixtureTransportsArgs),
    /// Build and cold-admit complete canonical and wallet stores.
    #[command(name = "rocksdb-storage-lifecycle")]
    RocksDbStorageLifecycle(RocksDbStorageLifecycleArgs),
    /// Rebuild and cold-admit a wallet store from an existing READY canonical store.
    #[command(name = "rocksdb-wallet-rebuild")]
    RocksDbWalletRebuild(RocksDbWalletRebuildArgs),
    /// Measure version-1 compact-block ranges through an admitted secondary.
    #[command(name = "rocksdb-compact-block-range")]
    RocksDbCompactBlockRange(RocksDbCompactBlockRangeArgs),
}

#[derive(Args)]
struct ServeCanonicalFixtureTransportsArgs {
    /// Directory containing the immutable fixture manifest and segments.
    #[arg(long)]
    fixture: PathBuf,
    /// Local JSON-RPC listener for the current batched source.
    #[arg(long, default_value = "127.0.0.1:19432")]
    json_rpc_listen_addr: SocketAddr,
    /// Local Zebra indexer gRPC listener for unary `GetBlock`.
    #[arg(long, default_value = "127.0.0.1:19430")]
    indexer_grpc_listen_addr: SocketAddr,
    /// Fixed delay per JSON request or batch and per unary gRPC call.
    #[arg(long, default_value_t = 0)]
    response_delay_millis: u64,
    /// Maximum JSON-RPC or protobuf response bytes.
    #[arg(long, default_value_t = DEFAULT_MAX_RESPONSE_BYTES)]
    max_response_bytes: u64,
    /// Write the shutdown report to this path instead of stdout.
    #[arg(long)]
    report: Option<PathBuf>,
}

#[derive(Args)]
struct CaptureArgs {
    /// Network name, such as zcash-mainnet.
    #[arg(long)]
    network: String,
    /// Zebra JSON-RPC base URL.
    #[arg(long = "json-rpc-addr")]
    json_rpc_addr: String,
    /// Optional node cookie file path.
    #[arg(long = "node-auth-cookie")]
    node_auth_cookie: Option<PathBuf>,
    /// First block height to capture.
    #[arg(long = "from-height", default_value_t = DEFAULT_FROM_HEIGHT)]
    from_height: u32,
    /// Last block height to capture.
    #[arg(long = "to-height", default_value_t = DEFAULT_TO_HEIGHT)]
    to_height: u32,
    /// Blocks written per segment file.
    #[arg(long = "segment-blocks", default_value_t = DEFAULT_SEGMENT_BLOCKS)]
    segment_blocks: u32,
    /// Concurrent block fetches issued against the node.
    #[arg(long = "fetch-concurrency", default_value_t = DEFAULT_FETCH_CONCURRENCY)]
    fetch_concurrency: u32,
    /// Concurrent block-local preparations used to measure captured blocks.
    #[arg(
        long = "prepare-concurrency",
        default_value_t = DEFAULT_CAPTURE_PREPARE_CONCURRENCY
    )]
    prepare_concurrency: u32,
    /// Per-request source timeout in seconds.
    #[arg(long = "request-timeout-secs", default_value_t = DEFAULT_REQUEST_TIMEOUT_SECS)]
    request_timeout_secs: u64,
    /// Maximum JSON-RPC response body size in bytes.
    #[arg(long = "max-response-bytes", default_value_t = DEFAULT_MAX_RESPONSE_BYTES)]
    max_response_bytes: u64,
    /// Destination fixture directory.
    #[arg(long = "out")]
    out: PathBuf,
}

#[derive(Args)]
struct CaptureCanonicalFixtureCheckpointsArgs {
    /// Existing captured fixture directory to augment.
    #[arg(long)]
    fixture: PathBuf,
    /// Network name, such as zcash-mainnet.
    #[arg(long)]
    network: String,
    /// Zebra JSON-RPC base URL.
    #[arg(long = "json-rpc-addr")]
    json_rpc_addr: String,
    /// Optional node cookie file path.
    #[arg(long = "node-auth-cookie")]
    node_auth_cookie: Option<PathBuf>,
    /// Per-request source timeout in seconds.
    #[arg(long = "request-timeout-secs", default_value_t = DEFAULT_REQUEST_TIMEOUT_SECS)]
    request_timeout_secs: u64,
    /// Maximum JSON-RPC response body size in bytes.
    #[arg(long = "max-response-bytes", default_value_t = DEFAULT_MAX_RESPONSE_BYTES)]
    max_response_bytes: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(
                target: "zinder::bench",
                event = "bench_run_failed",
                error = %error,
                "benchmark run failed"
            );
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), BenchError> {
    match cli.command {
        Command::Capture(args) => run_capture(args).await,
        Command::CaptureCanonicalFixtureCheckpoints(args) => {
            run_capture_canonical_fixture_checkpoints(args).await
        }
        Command::CanonicalReplayStorage(args) => {
            let output = run_canonical_replay_storage(args).await?;
            emit_report(&output.report, output.report_path.as_deref())?;
            output.report.validate()
        }
        Command::RocksDbCanonicalFixtureReplay(args) => {
            let output = run_rocksdb_canonical_fixture_replay(args).await?;
            emit_report(&output.report, output.report_path.as_deref())?;
            output.report.validate()
        }
        Command::RocksDbRawBlobRetentionComparison(args) => {
            let output = run_rocksdb_raw_blob_retention_comparison(args).await?;
            emit_report(&output.report, output.report_path.as_deref())
        }
        Command::ServeCanonicalFixtureTransports(args) => {
            let max_response_bytes = u32::try_from(args.max_response_bytes).map_err(|_| {
                BenchError::invalid_argument("--max-response-bytes must fit in u32")
            })?;
            let report =
                run_canonical_fixture_transport_server(CanonicalFixtureTransportServerConfig {
                    fixture_directory: args.fixture,
                    json_rpc_listen_addr: args.json_rpc_listen_addr,
                    indexer_grpc_listen_addr: args.indexer_grpc_listen_addr,
                    response_delay: Duration::from_millis(args.response_delay_millis),
                    max_response_bytes,
                })
                .await?;
            let encoded = serde_json::to_vec_pretty(&report)?;
            if let Some(path) = args.report.as_deref() {
                create_report_file(path, &encoded)?;
            } else {
                write_report_to_stdout(&encoded);
            }
            Ok(())
        }
        Command::RocksDbStorageLifecycle(args) => {
            let output = run_rocksdb_storage_lifecycle(args).await?;
            emit_report(&output.report, output.report_path.as_deref())?;
            output.report.validate()
        }
        Command::RocksDbWalletRebuild(args) => {
            let output = run_rocksdb_wallet_rebuild(args).await?;
            let encoded = serde_json::to_vec_pretty(&output)?;
            write_report_to_stdout(&encoded);
            Ok(())
        }
        Command::RocksDbCompactBlockRange(args) => {
            let output = run_rocksdb_compact_block_range(args)?;
            emit_report(&output.report, output.report_path.as_deref())
        }
    }
}

async fn run_capture_canonical_fixture_checkpoints(
    args: CaptureCanonicalFixtureCheckpointsArgs,
) -> Result<(), BenchError> {
    let network = parse_network(&args.network)?;
    let manifest = FixtureManifest::read(&args.fixture)?;
    if manifest.network_typed()? != network {
        return Err(BenchError::invalid_argument(
            "--network does not match the captured fixture network",
        ));
    }
    let node_auth = args.node_auth_cookie.map_or(NodeAuth::None, |path| {
        NodeAuth::Cookie(CookieSource::File(path))
    });
    let source = ZebraJsonRpcSource::with_options(
        network,
        &args.json_rpc_addr,
        node_auth,
        ZebraJsonRpcSourceOptions {
            request_timeout: Duration::from_secs(args.request_timeout_secs),
            max_response_bytes: require_nonzero_u64(args.max_response_bytes, "max-response-bytes")?,
            broadcast_timeout: None,
        },
    )?;
    let activations = source
        .discover_network_upgrade_activations("zinder-bench")
        .await?;
    let replay_plan =
        capture_canonical_fixture_replay_plan(&args.fixture, &source, &activations).await?;
    tracing::info!(
        target: "zinder::bench",
        event = "canonical_fixture_checkpoints_captured",
        fixture_manifest_sha256 = replay_plan.fixture_manifest_sha256,
        predecessor_height = replay_plan.history_predecessor.block_id.height,
        source_tip_height = replay_plan.source_tip_checkpoint.block_id.height,
        "canonical fixture checkpoints captured"
    );
    Ok(())
}

async fn run_capture(args: CaptureArgs) -> Result<(), BenchError> {
    let network = parse_network(&args.network)?;
    let node_auth = args.node_auth_cookie.map_or(NodeAuth::None, |path| {
        NodeAuth::Cookie(CookieSource::File(path))
    });
    let config = CaptureConfig {
        network,
        json_rpc_addr: args.json_rpc_addr,
        node_auth,
        from_height: BlockHeight::new(args.from_height),
        to_height: BlockHeight::new(args.to_height),
        segment_blocks: require_nonzero_u32(args.segment_blocks, "segment-blocks")?,
        fetch_concurrency: require_nonzero_u32(args.fetch_concurrency, "fetch-concurrency")?,
        prepare_concurrency: require_nonzero_u32(args.prepare_concurrency, "prepare-concurrency")?,
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        max_response_bytes: require_nonzero_u64(args.max_response_bytes, "max-response-bytes")?,
        output_directory: args.out,
    };
    let manifest = capture_fixed_range(config).await?;
    tracing::info!(
        target: "zinder::bench",
        event = "capture_complete",
        from_height = manifest.from_height,
        to_height = manifest.to_height,
        block_count = manifest.block_count,
        transaction_count = manifest.workload_density.transaction_count,
        transparent_input_count = manifest.workload_density.transparent_input_count,
        transparent_output_count = manifest.workload_density.transparent_output_count,
        segment_count = manifest.segments.len(),
        "capture complete"
    );
    Ok(())
}

fn emit_report(
    report: &impl serde::Serialize,
    report_path: Option<&std::path::Path>,
) -> Result<(), BenchError> {
    let encoded = serde_json::to_vec_pretty(report)?;
    if let Some(path) = report_path {
        create_report_file(path, &encoded)?;
    } else {
        write_report_to_stdout(&encoded);
    }
    Ok(())
}

fn create_report_file(path: &std::path::Path, encoded: &[u8]) -> Result<(), BenchError> {
    let mut report_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| BenchError::io(path, source))?;
    report_file
        .write_all(encoded)
        .map_err(|source| BenchError::io(path, source))?;
    report_file
        .sync_all()
        .map_err(|source| BenchError::io(path, source))
}

#[allow(
    clippy::print_stdout,
    reason = "the JSON report is the tool's structured output stream"
)]
fn write_report_to_stdout(encoded: &[u8]) {
    let rendered = String::from_utf8_lossy(encoded);
    println!("{rendered}");
}

fn parse_network(name: &str) -> Result<Network, BenchError> {
    decode_zinder_native_chain_name(name)
        .map_err(|source| BenchError::invalid_argument(source.to_string()))
}

fn require_nonzero_u32(candidate: u32, flag: &str) -> Result<NonZeroU32, BenchError> {
    NonZeroU32::new(candidate)
        .ok_or_else(|| BenchError::invalid_argument(format!("--{flag} must be greater than zero")))
}

fn require_nonzero_u64(candidate: u64, flag: &str) -> Result<NonZeroU64, BenchError> {
    NonZeroU64::new(candidate)
        .ok_or_else(|| BenchError::invalid_argument(format!("--{flag} must be greater than zero")))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use clap::Parser;
    use tempfile::tempdir;

    use super::{Cli, Command, create_report_file};

    #[test]
    fn rocksdb_storage_lifecycle_command_spelling_is_stable() -> Result<(), Box<dyn Error>> {
        let cli = Cli::try_parse_from([
            "zinder-bench",
            "rocksdb-storage-lifecycle",
            "--network",
            "zcash-testnet",
            "--json-rpc-addr",
            "http://zebra:18232",
            "--canonical-store",
            "canonical",
            "--wallet-store",
            "wallet",
            "--cpu-limit-cores",
            "10",
            "--memory-limit-bytes",
            "10737418240",
        ])?;

        assert!(matches!(cli.command, Command::RocksDbStorageLifecycle(_)));
        Ok(())
    }

    #[test]
    fn rocksdb_canonical_fixture_replay_command_spelling_is_stable() -> Result<(), Box<dyn Error>> {
        let cli = Cli::try_parse_from([
            "zinder-bench",
            "rocksdb-canonical-fixture-replay",
            "--fixture",
            "fixture",
            "--canonical-store",
            "canonical",
        ])?;

        assert!(matches!(
            cli.command,
            Command::RocksDbCanonicalFixtureReplay(_)
        ));
        Ok(())
    }

    #[test]
    fn rocksdb_raw_blob_retention_comparison_command_spelling_is_stable()
    -> Result<(), Box<dyn Error>> {
        let cli = Cli::try_parse_from([
            "zinder-bench",
            "rocksdb-raw-blob-retention-comparison",
            "--fixture",
            "fixture",
            "--transactions-canonical-store",
            "transactions-canonical",
            "--transactions-secondary-root",
            "transactions-secondary",
            "--all-canonical-store",
            "all-canonical",
            "--all-secondary-root",
            "all-secondary",
        ])?;

        assert!(matches!(
            cli.command,
            Command::RocksDbRawBlobRetentionComparison(_)
        ));
        Ok(())
    }

    #[test]
    fn capture_canonical_fixture_checkpoints_command_spelling_is_stable()
    -> Result<(), Box<dyn Error>> {
        let cli = Cli::try_parse_from([
            "zinder-bench",
            "capture-canonical-fixture-checkpoints",
            "--fixture",
            "fixture",
            "--network",
            "zcash-mainnet",
            "--json-rpc-addr",
            "http://zebra:8232",
        ])?;

        assert!(matches!(
            cli.command,
            Command::CaptureCanonicalFixtureCheckpoints(_)
        ));
        Ok(())
    }

    #[test]
    fn rocksdb_wallet_rebuild_command_spelling_is_stable() -> Result<(), Box<dyn Error>> {
        let cli = Cli::try_parse_from([
            "zinder-bench",
            "rocksdb-wallet-rebuild",
            "--network",
            "zcash-testnet",
            "--json-rpc-addr",
            "http://zebra:18232",
            "--canonical-store",
            "canonical",
            "--wallet-store",
            "wallet",
            "--raw-blob-retention",
            "all",
        ])?;

        assert!(matches!(cli.command, Command::RocksDbWalletRebuild(_)));
        Ok(())
    }

    #[test]
    fn rocksdb_compact_block_range_command_spelling_is_stable() -> Result<(), Box<dyn Error>> {
        let cli = Cli::try_parse_from([
            "zinder-bench",
            "rocksdb-compact-block-range",
            "--fixture",
            "fixture",
            "--canonical-store",
            "canonical",
            "--secondary-root",
            "secondary",
            "--software-revision",
            "5079515",
        ])?;

        assert!(matches!(cli.command, Command::RocksDbCompactBlockRange(_)));
        Ok(())
    }

    #[tokio::test]
    async fn capture_rejects_zero_prepare_concurrency_before_source_io()
    -> Result<(), Box<dyn Error>> {
        let cli = Cli::try_parse_from([
            "zinder-bench",
            "capture",
            "--network",
            "zcash-mainnet",
            "--json-rpc-addr",
            "http://zebra.invalid:8232",
            "--prepare-concurrency",
            "0",
            "--out",
            "fixture",
        ])?;

        let Some(error) = super::run(cli).await.err() else {
            return Err("zero capture preparation concurrency must be rejected".into());
        };
        assert_eq!(
            error.to_string(),
            "invalid argument: --prepare-concurrency must be greater than zero"
        );
        Ok(())
    }

    #[test]
    fn capture_defaults_prepare_concurrency_to_cpu_envelope() -> Result<(), Box<dyn Error>> {
        let cli = Cli::try_parse_from([
            "zinder-bench",
            "capture",
            "--network",
            "zcash-mainnet",
            "--json-rpc-addr",
            "http://zebra:8232",
            "--out",
            "fixture",
        ])?;
        let Command::Capture(args) = cli.command else {
            return Err("expected capture command".into());
        };

        assert_eq!(args.prepare_concurrency, 10);
        Ok(())
    }

    #[test]
    fn report_file_creation_refuses_to_replace_existing_evidence() -> Result<(), Box<dyn Error>> {
        let directory = tempdir()?;
        let report_path = directory.path().join("report.json");
        create_report_file(&report_path, b"first")?;

        assert!(create_report_file(&report_path, b"second").is_err());
        assert_eq!(std::fs::read(&report_path)?, b"first");
        Ok(())
    }
}
