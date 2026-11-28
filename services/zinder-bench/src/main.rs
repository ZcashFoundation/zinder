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
    active_writer_publication::{
        ActiveWriterPublicationConfig, run_active_writer_publication, validate_config,
    },
    canonical_fixture_replay::capture_canonical_fixture_replay_plan,
    canonical_fixture_transport_server::{
        CanonicalFixtureTransportServerConfig, run_canonical_fixture_transport_server,
    },
    capture::{CaptureConfig, capture_fixed_range},
    fixture::FixtureManifest,
    tip_replay_checkpoints::capture_tip_replay_checkpoints,
};
use zinder_core::{BlockHeight, Network, wire::decode_zinder_native_chain_name};
use zinder_source::{CookieSource, NodeAuth, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_store::RocksDbIoMode;

mod build_provenance;
#[path = "canonical_replay_storage/command.rs"]
mod canonical_replay_storage_command;
#[path = "postgres_segmented/command.rs"]
mod postgres_segmented_command;
mod rocksdb_canonical_fixture_replay;
mod rocksdb_compact_block_range;
mod rocksdb_raw_blob_retention_comparison;
mod rocksdb_read_bench;
mod rocksdb_storage_lifecycle;
mod rocksdb_store_properties;
mod rocksdb_subtree_roots;
mod rocksdb_tip_replay;
mod rocksdb_tree_state_replay;
mod rocksdb_wallet_footprint;
mod rocksdb_wallet_rebuild;
mod wallet_load;

use canonical_replay_storage_command::{CanonicalReplayStorageArgs, run_canonical_replay_storage};
use rocksdb_canonical_fixture_replay::{
    RocksDbCanonicalFixtureReplayArgs, run_rocksdb_canonical_fixture_replay,
};
use rocksdb_compact_block_range::run_rocksdb_compact_block_range;
use rocksdb_raw_blob_retention_comparison::{
    RocksDbRawBlobRetentionComparisonArgs, run_rocksdb_raw_blob_retention_comparison,
};
use rocksdb_read_bench::ReadStoreArgs;
use rocksdb_storage_lifecycle::{RocksDbStorageLifecycleArgs, run_rocksdb_storage_lifecycle};
use rocksdb_tip_replay::{
    CompareTipReplayReportsArgs, RocksDbTipReorgArgs, RocksDbTipReplayArgs,
    run_compare_tip_replay_reports, run_rocksdb_tip_reorg, run_rocksdb_tip_replay,
};
use rocksdb_tree_state_replay::{RocksDbTreeStateReplayArgs, run_rocksdb_tree_state_replay};
use rocksdb_wallet_rebuild::{RocksDbWalletRebuildArgs, run_rocksdb_wallet_rebuild};
use wallet_load::{
    LightwalletdWalletLoadArgs, RawRangeProbeArgs, run_lightwalletd_wallet_load,
    run_raw_range_probe,
};

#[global_allocator]
static GLOBAL_ALLOCATOR: zinder_runtime::RuntimeAllocator = zinder_runtime::RuntimeAllocator;

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
    /// Measure per-CF wallet `RocksDB` footprint, cold readback, and warm transparent query latency.
    #[command(name = "rocksdb-wallet-footprint")]
    RocksDbWalletFootprint(rocksdb_wallet_footprint::RocksDbWalletFootprintArgs),
    /// Measure unbounded ranges and byte-bounded serving chunks on a READY store.
    #[command(name = "rocksdb-compact-block-range")]
    RocksDbCompactBlockRange(ReadStoreArgs),
    /// Measure cold and warm reads of a subtree-root page.
    #[command(name = "rocksdb-subtree-roots")]
    RocksDbSubtreeRoots(rocksdb_subtree_roots::SubtreeArgs),
    /// Inspect canonical column-family and SST properties through a READY secondary.
    #[command(name = "rocksdb-store-properties")]
    RocksDbStoreProperties(ReadStoreArgs),
    /// Measure tree-state reconstructions at wallet anchor heights.
    #[command(name = "rocksdb-tree-state-replay")]
    RocksDbTreeStateReplay(RocksDbTreeStateReplayArgs),
    /// Capture the node's tree-state checkpoint for every fixture height.
    #[command(name = "capture-tip-replay-checkpoints")]
    CaptureTipReplayCheckpoints(CaptureTipReplayCheckpointsArgs),
    /// Replay a fixture through production canonical appends and wallet transitions.
    #[command(name = "rocksdb-tip-replay")]
    RocksDbTipReplay(RocksDbTipReplayArgs),
    /// Compare a baseline and a candidate tip replay report.
    #[command(name = "compare-tip-replay-reports")]
    CompareTipReplayReports(CompareTipReplayReportsArgs),
    /// Replay a deep reorg at the tip through production following and the wallet transition.
    #[command(name = "rocksdb-tip-reorg")]
    RocksDbTipReorg(RocksDbTipReorgArgs),
    /// Drive wallet-shaped sync load at a running lightwalletd-compatible endpoint.
    #[command(name = "lightwalletd-wallet-load")]
    LightwalletdWalletLoad(LightwalletdWalletLoadArgs),
    /// Verify raw `GetBlockRange` frames against an oracle and time them.
    #[command(name = "raw-range-probe")]
    RawRangeProbe(RawRangeProbeArgs),
    /// Benchmark reader publication across four matched active-writer states.
    #[command(name = "active-writer-publication")]
    ActiveWriterPublication(ActiveWriterPublicationArgs),
    /// Run the segmented PostgreSQL canonical + wallet build, tip-follow, mempool churn, and pooled-reader benchmark arm.
    #[command(name = "postgres-segmented")]
    PostgresSegmented(postgres_segmented_command::PostgresSegmentedArgs),
}

#[derive(Args)]
struct ActiveWriterPublicationArgs {
    /// Number of pre-built base blocks before live commits begin.
    #[arg(long = "base-blocks", default_value_t = 4_092)]
    base_blocks: u32,
    /// Number of live blocks committed while readers are active.
    #[arg(long = "live-blocks", default_value_t = 16)]
    live_blocks: u32,
    /// Settlement reorg window depth in blocks.
    #[arg(long = "reorg-window-blocks", default_value_t = 4)]
    reorg_window_blocks: u32,
    /// Settled shielded catalog segment size in blocks.
    #[arg(long = "segment-blocks", default_value_t = 4_096)]
    segment_blocks: u32,
    /// Cadence (in blocks) for commitment-dense shielded blocks.
    #[arg(long = "dense-block-cadence", default_value_t = 4)]
    dense_block_cadence: u32,
    /// Synthetic V5 Orchard transactions attached to each dense block.
    #[arg(long = "dense-txs-per-block", default_value_t = 1)]
    dense_txs_per_block: u32,
    /// Number of Orchard actions embedded in each dense shielded transaction.
    #[arg(long = "dense-orchard-actions-per-tx", default_value_t = 2)]
    dense_orchard_actions_per_tx: u32,
    /// Width in blocks (>= 2) of descending `GetBlockRange(tip, floor)` uncovered-tail probes.
    #[arg(long = "tail-probe-blocks", default_value_t = 24)]
    tail_probe_blocks: u32,
    /// Milliseconds between consecutive live block commits.
    #[arg(long = "commit-interval-millis", default_value_t = 8)]
    commit_interval_millis: u64,
    /// Milliseconds slept per frame by the slow boundary-crossing reader.
    #[arg(long = "slow-reader-frame-delay-millis", default_value_t = 3)]
    slow_reader_frame_delay_millis: u64,
    /// Mempool churn transaction counts (comma-separated or repeated).
    #[arg(long = "mempool-churn-size", value_delimiter = ',', default_values_t = vec![50, 500])]
    mempool_churn_sizes: Vec<u32>,
    /// Minimum request rounds for the `quiescent_control` baseline arm.
    #[arg(long = "quiescent-rounds", default_value_t = 6)]
    quiescent_rounds: u32,
    /// Optional scratch directory parent for temporary benchmark stores.
    #[arg(long = "scratch-dir")]
    scratch_dir: Option<PathBuf>,
    /// Requested `RocksDB` I/O mode (`direct` or `buffered`).
    #[arg(long = "io-mode", default_value = "direct", value_parser = parse_io_mode)]
    io_mode: RocksDbIoMode,
    /// Optional worker count for signed-catalog publication.
    #[arg(long = "publish-workers")]
    publish_workers: Option<u32>,
    /// Fail unless at least one arm triggers an `EncodedTail` `64 MiB` byte-cap eviction.
    #[arg(long = "require-tail-eviction", default_value_t = false)]
    require_tail_eviction: bool,
    /// Write the JSON report to this absent path instead of stdout.
    #[arg(long)]
    report: Option<PathBuf>,
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

#[derive(Args)]
struct CaptureTipReplayCheckpointsArgs {
    /// Existing captured fixture directory to augment.
    #[arg(long)]
    fixture: PathBuf,
    /// Network name, such as zcash-testnet.
    #[arg(long)]
    network: String,
    /// Zebra JSON-RPC base URL.
    #[arg(long = "json-rpc-addr")]
    json_rpc_addr: String,
    /// Optional node cookie file path.
    #[arg(long = "node-auth-cookie")]
    node_auth_cookie: Option<PathBuf>,
    /// Concurrent `z_gettreestate` requests issued against the node.
    #[arg(long = "fetch-concurrency", default_value_t = DEFAULT_FETCH_CONCURRENCY)]
    fetch_concurrency: u32,
    /// Per-request source timeout in seconds.
    #[arg(long = "request-timeout-secs", default_value_t = DEFAULT_REQUEST_TIMEOUT_SECS)]
    request_timeout_secs: u64,
    /// Maximum JSON-RPC response body size in bytes.
    #[arg(long = "max-response-bytes", default_value_t = DEFAULT_MAX_RESPONSE_BYTES)]
    max_response_bytes: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(error) = zinder_bench::recorder::install_tracing_subscriber() {
        tracing::error!(
            target: "zinder::bench",
            event = "tracing_init_failed",
            error = %error,
            "failed to initialize benchmark tracing subscriber"
        );
        return ExitCode::FAILURE;
    }
    let cli = Cli::parse();
    if !zinder_runtime::host_cpu_meets_compiled_baseline() {
        return ExitCode::FAILURE;
    }
    if !zinder_runtime::host_cpu_meets_pasta_backend_baseline() {
        return ExitCode::FAILURE;
    }
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
            run_serve_canonical_fixture_transports_command(args).await
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
        Command::RocksDbWalletFootprint(args) => {
            let output = rocksdb_wallet_footprint::run(&args)?;
            emit_report(&output, args.output_json.as_deref())
        }
        Command::RocksDbCompactBlockRange(args) => {
            let report = run_rocksdb_compact_block_range(&args).await?;
            emit_report(&report, args.report.as_deref())
        }
        Command::RocksDbSubtreeRoots(args) => {
            let report = rocksdb_subtree_roots::run(&args).await?;
            emit_report(&report, args.store.report.as_deref())
        }
        Command::RocksDbStoreProperties(args) => {
            let report = rocksdb_store_properties::run(&args)?;
            emit_report(&report, args.report.as_deref())
        }
        Command::RocksDbTreeStateReplay(args) => {
            let report = run_rocksdb_tree_state_replay(&args)?;
            emit_report(&report, args.store.report.as_deref())
        }
        Command::CaptureTipReplayCheckpoints(args) => {
            run_capture_tip_replay_checkpoints(args).await
        }
        Command::RocksDbTipReplay(args) => run_rocksdb_tip_replay(args).await,
        Command::CompareTipReplayReports(args) => run_compare_tip_replay_reports(&args),
        Command::RocksDbTipReorg(args) => run_rocksdb_tip_reorg(args).await,
        Command::LightwalletdWalletLoad(args) => run_wallet_load_command(args).await,
        Command::RawRangeProbe(args) => run_raw_range_probe_command(args).await,
        Command::ActiveWriterPublication(args) => {
            Box::pin(run_active_writer_publication_command(args)).await
        }
        Command::PostgresSegmented(args) => {
            let output =
                Box::pin(postgres_segmented_command::run_postgres_segmented_cli(args)).await?;
            emit_report(&output.report, output.report_path.as_deref())
        }
    }
}

async fn run_serve_canonical_fixture_transports_command(
    args: ServeCanonicalFixtureTransportsArgs,
) -> Result<(), BenchError> {
    let max_response_bytes = u32::try_from(args.max_response_bytes)
        .map_err(|_| BenchError::invalid_argument("--max-response-bytes must fit in u32"))?;
    let report = run_canonical_fixture_transport_server(CanonicalFixtureTransportServerConfig {
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

fn parse_io_mode(text: &str) -> Result<RocksDbIoMode, String> {
    match text {
        "direct" => Ok(RocksDbIoMode::Direct),
        "buffered" => Ok(RocksDbIoMode::Buffered),
        _ => Err("expected direct or buffered".to_owned()),
    }
}

async fn run_active_writer_publication_command(
    args: ActiveWriterPublicationArgs,
) -> Result<(), BenchError> {
    if let Some(path) = args.report.as_deref()
        && path.exists()
    {
        return Err(BenchError::io(
            path,
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "report output path already exists",
            ),
        ));
    }
    let segment_blocks = require_nonzero_u32(args.segment_blocks, "segment-blocks")?;
    let publish_workers = args
        .publish_workers
        .map(|workers| require_nonzero_u32(workers, "publish-workers"))
        .transpose()?;
    let config = ActiveWriterPublicationConfig {
        base_blocks: args.base_blocks,
        live_blocks: args.live_blocks,
        reorg_window_blocks: args.reorg_window_blocks,
        segment_blocks,
        dense_block_cadence: args.dense_block_cadence,
        dense_txs_per_block: args.dense_txs_per_block,
        dense_orchard_actions_per_tx: args.dense_orchard_actions_per_tx,
        tail_probe_blocks: args.tail_probe_blocks,
        commit_interval: Duration::from_millis(args.commit_interval_millis),
        slow_reader_frame_delay: Duration::from_millis(args.slow_reader_frame_delay_millis),
        mempool_churn_sizes: args.mempool_churn_sizes,
        quiescent_rounds: args.quiescent_rounds,
        scratch_dir: args.scratch_dir,
        io_mode: args.io_mode,
        publish_workers,
        require_tail_eviction: args.require_tail_eviction,
    };
    validate_config(&config)?;
    let report = run_active_writer_publication(&config).await?;
    emit_report(&report, args.report.as_deref())?;
    report.validate(&config)
}

async fn run_wallet_load_command(args: LightwalletdWalletLoadArgs) -> Result<(), BenchError> {
    let output = run_lightwalletd_wallet_load(args).await?;
    emit_report(&output.report, output.report_path.as_deref())
}

async fn run_raw_range_probe_command(args: RawRangeProbeArgs) -> Result<(), BenchError> {
    let output = run_raw_range_probe(args).await?;
    emit_report(&output.report, output.report_path.as_deref())?;
    output.require_verified()
}

async fn run_capture_tip_replay_checkpoints(
    args: CaptureTipReplayCheckpointsArgs,
) -> Result<(), BenchError> {
    let network = parse_network(&args.network)?;
    let manifest = FixtureManifest::read(&args.fixture)?;
    if manifest.network_typed()? != network {
        return Err(BenchError::invalid_argument(
            "--network does not match the captured fixture network",
        ));
    }
    let fetch_concurrency = require_nonzero_u32(args.fetch_concurrency, "fetch-concurrency")?;
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
            ..ZebraJsonRpcSourceOptions::default()
        },
    )?;
    let activations =
        zinder_source::discover_network_upgrade_activations(&source, "zinder-bench").await?;
    let checkpoints =
        capture_tip_replay_checkpoints(&args.fixture, &source, &activations, fetch_concurrency)
            .await?;
    tracing::info!(
        target: "zinder::bench",
        event = "tip_replay_checkpoints_captured",
        fixture_manifest_sha256 = checkpoints.fixture_manifest_sha256,
        checkpoint_count = checkpoints.checkpoints.len(),
        "tip replay checkpoints captured"
    );
    Ok(())
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
            ..ZebraJsonRpcSourceOptions::default()
        },
    )?;
    let activations =
        zinder_source::discover_network_upgrade_activations(&source, "zinder-bench").await?;
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
        for coverage in ["every-block", "cadence-and-tip"] {
            assert!(matches!(
                Cli::try_parse_from([
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
                    "--tree-state-checkpoint-coverage",
                    coverage,
                ])?
                .command,
                Command::RocksDbStorageLifecycle(_)
            ));
        }
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
        for coverage in ["every-block", "cadence-and-tip"] {
            assert!(matches!(
                Cli::try_parse_from([
                    "zinder-bench",
                    "rocksdb-canonical-fixture-replay",
                    "--fixture",
                    "fixture",
                    "--canonical-store",
                    "canonical",
                    "--tree-state-checkpoint-coverage",
                    coverage,
                ])?
                .command,
                Command::RocksDbCanonicalFixtureReplay(_)
            ));
        }
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
            "--none-canonical-store",
            "none-canonical",
            "--none-secondary-root",
            "none-secondary",
            "--all-canonical-store",
            "all-canonical",
            "--all-secondary-root",
            "all-secondary",
            "--tree-state-checkpoint-coverage",
            "cadence-and-tip",
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
        let base = [
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
        ];
        let cli = Cli::try_parse_from(base)?;
        assert!(matches!(cli.command, Command::RocksDbCompactBlockRange(_)));
        assert!(
            Cli::try_parse_from(base.into_iter().chain([
                "--canonical-io-mode",
                "direct",
                "--evict-page-cache"
            ]))
            .is_ok()
        );
        assert!(
            Cli::try_parse_from(base.into_iter().chain([
                "--canonical-io-mode",
                "buffered",
                "--evict-page-cache"
            ]))
            .is_ok()
        );
        assert!(
            Cli::try_parse_from(base.into_iter().chain(["--canonical-io-mode", "invalid"]))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn rocksdb_tree_state_replay_command_spelling_is_stable() -> Result<(), Box<dyn Error>> {
        let cli = Cli::try_parse_from([
            "zinder-bench",
            "rocksdb-tree-state-replay",
            "--fixture",
            "fixture",
            "--canonical-store",
            "canonical",
            "--secondary-root",
            "secondary",
            "--software-revision",
            "5079515",
            "--from-height",
            "1710000",
            "--to-height",
            "1850000",
            "--max-anchors",
            "128",
        ])?;

        assert!(matches!(cli.command, Command::RocksDbTreeStateReplay(_)));
        Ok(())
    }

    #[test]
    fn explicit_tree_state_heights_are_in_help_and_conflict_with_sampling()
    -> Result<(), Box<dyn Error>> {
        let base = [
            "zinder-bench",
            "rocksdb-tree-state-replay",
            "--fixture",
            "fixture",
            "--canonical-store",
            "canonical",
            "--secondary-root",
            "secondary",
            "--software-revision",
            "revision",
            "--heights-file",
            "frontiers.txt",
        ];
        assert!(Cli::try_parse_from(base).is_ok());
        for flag in ["--from-height", "--to-height", "--max-anchors"] {
            assert!(Cli::try_parse_from(base.into_iter().chain([flag, "100"])).is_err());
        }
        let help = Cli::try_parse_from(["zinder-bench", "rocksdb-tree-state-replay", "--help"])
            .err()
            .ok_or("help should exit")?;
        assert!(help.to_string().contains("--heights-file"));
        Ok(())
    }

    #[test]
    fn lightwalletd_wallet_load_command_spelling_is_stable() -> Result<(), Box<dyn Error>> {
        let load = |workload: &str| {
            Cli::try_parse_from([
                "zinder-bench",
                "lightwalletd-wallet-load",
                "--endpoint",
                "https://wallet.example:443",
                "--workload",
                workload,
                "--wallets",
                "20",
                "--from-height",
                "3424811",
            ])
        };

        for workload in ["slipstream", "vizor", "classic-sdk", "point-reads"] {
            assert!(matches!(
                load(workload)?.command,
                Command::LightwalletdWalletLoad(_)
            ));
        }
        assert!(load("zingo").is_err());
        Ok(())
    }

    #[test]
    fn raw_range_probe_command_spellings_are_stable() -> Result<(), Box<dyn Error>> {
        let probe = |extra: &[&str]| {
            let mut arguments = vec!["zinder-bench", "raw-range-probe", "--endpoint", "http://a"];
            arguments.extend_from_slice(extra);
            Cli::try_parse_from(arguments)
        };
        assert!(matches!(
            probe(&[
                "--oracle",
                "oracle.json",
                "--clients",
                "4",
                "--server-pid",
                "7",
                "--server-executable",
                "reader"
            ])?
            .command,
            Command::RawRangeProbe(_)
        ));
        assert!(matches!(
            probe(&[
                "--oracle-endpoint",
                "http://b",
                "--range",
                "20:10",
                "--both-directions",
                "--write-oracle",
                "oracle.json",
            ])?
            .command,
            Command::RawRangeProbe(_)
        ));
        // The oracle is required, and a file excludes recording one.
        assert!(probe(&[]).is_err());
        assert!(probe(&["--oracle", "o.json", "--oracle-endpoint", "http://b"]).is_err());
        assert!(probe(&["--oracle-endpoint", "http://b"]).is_err());
        assert!(probe(&["--oracle", "o.json", "--clients", "0"]).is_err());
        assert!(probe(&["--oracle", "o.json", "--server-counter", "x"]).is_err());
        let metrics = [
            "--oracle",
            "o.json",
            "--server-metrics-url",
            "http://m/metrics",
        ];
        let with = |extra: &[&str]| probe(&[&metrics[..], extra].concat());
        let witness = ["--server-freshness-counter", "w_total{status=ok}"];
        assert!(with(&[&["--rocksdb-multiget-counters"][..], &witness].concat()).is_ok());
        assert!(with(&[&["--server-counter", "a=b_total{k=v}"][..], &witness].concat()).is_ok());
        // Counters without a freshness witness, or a witness without counters, could read as zero.
        assert!(with(&["--rocksdb-multiget-counters"]).is_err());
        assert!(with(&witness).is_err());
        assert!(with(&["--server-counter", "a{b}"]).is_err());
        Ok(())
    }

    #[test]
    fn tip_replay_command_spellings_are_stable() -> Result<(), Box<dyn Error>> {
        let replay = Cli::try_parse_from([
            "zinder-bench",
            "rocksdb-tip-replay",
            "--fixture",
            "fixture",
            "--canonical-store",
            "canonical",
            "--wallet-store",
            "wallet",
            "--scratch-dir",
            "scratch",
            "--report",
            "report.json",
            "--software-revision",
            "5079515",
            "--repeat",
            "3",
        ])?;
        let checkpoints = Cli::try_parse_from([
            "zinder-bench",
            "capture-tip-replay-checkpoints",
            "--fixture",
            "fixture",
            "--network",
            "zcash-testnet",
            "--json-rpc-addr",
            "http://zebra:18232",
        ])?;
        let compare = Cli::try_parse_from([
            "zinder-bench",
            "compare-tip-replay-reports",
            "--baseline",
            "baseline.json",
            "--candidate",
            "candidate.json",
        ])?;

        assert!(matches!(replay.command, Command::RocksDbTipReplay(_)));
        let tip_replay_with = |extra: [&str; 2]| {
            Cli::try_parse_from(
                [
                    "zinder-bench",
                    "rocksdb-tip-replay",
                    "--fixture",
                    "fixture",
                    "--canonical-store",
                    "canonical",
                    "--wallet-store",
                    "wallet",
                    "--scratch-dir",
                    "scratch",
                    "--report",
                    "report.json",
                    "--software-revision",
                    "5079515",
                ]
                .into_iter()
                .chain(extra),
            )
        };
        assert!(tip_replay_with(["--wallet-io-mode", "buffered"]).is_ok());
        assert!(tip_replay_with(["--canonical-io-mode", "direct"]).is_ok());
        assert!(tip_replay_with(["--wallet-io-mode", "mmap"]).is_err());
        assert!(tip_replay_with(["--wallet-rocksdb-statistics", "--keep-scratch"]).is_ok());
        assert!(matches!(
            checkpoints.command,
            Command::CaptureTipReplayCheckpoints(_)
        ));
        assert!(matches!(
            compare.command,
            Command::CompareTipReplayReports(_)
        ));
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

    #[test]
    fn active_writer_publication_command_spelling_is_stable() -> Result<(), Box<dyn Error>> {
        let parsed = Cli::try_parse_from([
            "zinder-bench",
            "active-writer-publication",
            "--base-blocks",
            "4092",
            "--live-blocks",
            "16",
            "--report",
            "publication.json",
        ])?;
        assert!(matches!(
            parsed.command,
            Command::ActiveWriterPublication(_)
        ));
        Ok(())
    }

    #[test]
    fn active_writer_publication_parses_comma_separated_and_repeated_mempool_churn_sizes()
    -> Result<(), Box<dyn Error>> {
        for argv in [
            vec![
                "zinder-bench",
                "active-writer-publication",
                "--mempool-churn-size",
                "50,500",
            ],
            vec![
                "zinder-bench",
                "active-writer-publication",
                "--mempool-churn-size",
                "50",
                "--mempool-churn-size",
                "500",
            ],
        ] {
            let parsed = Cli::try_parse_from(argv)?;
            assert!(matches!(
                parsed.command,
                Command::ActiveWriterPublication(ref args)
                    if args.mempool_churn_sizes == [50, 500]
            ));
        }
        Ok(())
    }

    #[test]
    fn active_writer_publication_parses_extended_flags() -> Result<(), Box<dyn Error>> {
        let parsed = Cli::try_parse_from([
            "zinder-bench",
            "active-writer-publication",
            "--scratch-dir",
            "/tmp/zinder-bench-scratch",
            "--io-mode",
            "buffered",
            "--tail-probe-blocks",
            "64",
            "--dense-orchard-actions-per-tx",
            "16",
            "--publish-workers",
            "2",
            "--require-tail-eviction",
        ])?;
        let Command::ActiveWriterPublication(args) = parsed.command else {
            return Err("unexpected subcommand parsed".into());
        };
        assert_eq!(
            args.scratch_dir.as_deref(),
            Some(std::path::Path::new("/tmp/zinder-bench-scratch"))
        );
        assert_eq!(args.io_mode, zinder_store::RocksDbIoMode::Buffered);
        assert_eq!(args.tail_probe_blocks, 64);
        assert_eq!(args.dense_orchard_actions_per_tx, 16);
        assert_eq!(args.publish_workers, Some(2));
        assert!(args.require_tail_eviction);
        Ok(())
    }

    #[tokio::test]
    async fn active_writer_publication_rejects_existing_report_before_running()
    -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let existing_report = dir.path().join("existing.json");
        create_report_file(&existing_report, b"preexisting")?;
        let parsed = Cli::try_parse_from([
            "zinder-bench",
            "active-writer-publication",
            "--live-blocks",
            "0",
            "--report",
            existing_report
                .to_str()
                .ok_or("temp report path must be utf8")?,
        ])?;
        let Command::ActiveWriterPublication(args) = parsed.command else {
            return Err("unexpected subcommand parsed".into());
        };
        let err = super::run_active_writer_publication_command(args)
            .await
            .err()
            .ok_or("existing report path must reject before config validation or execution")?;
        assert!(err.to_string().contains("already exists"));
        Ok(())
    }

    #[tokio::test]
    async fn postgres_segmented_parses_and_rejects_existing_report() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let existing_report = dir.path().join("existing-segmented.json");
        create_report_file(&existing_report, b"preexisting")?;
        let parsed = Cli::try_parse_from([
            "zinder-bench",
            "postgres-segmented",
            "--fixture",
            "/tmp/nonexistent-fixture",
            "--reader-mode",
            "pgbouncer-transaction",
            "--full-blocks",
            "--report",
            existing_report
                .to_str()
                .ok_or("temp report path must be utf8")?,
        ])?;
        let Command::PostgresSegmented(args) = parsed.command else {
            return Err("unexpected subcommand parsed".into());
        };
        let err = super::postgres_segmented_command::run_postgres_segmented_cli(args)
            .await
            .err()
            .ok_or("existing report path must reject before running postgres-segmented")?;
        assert!(err.to_string().contains("already exists"));
        Ok(())
    }
}
