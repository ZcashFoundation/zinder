//! CLI adapter for `zinder-bench postgres-segmented`.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use clap::{Args, ValueEnum};
use zinder_bench::{
    BenchError,
    fixture::FixtureManifest,
    postgres_segmented::{
        PostgresSegmentedConfig,
        reader::ReaderConnectionMode,
        run_postgres_segmented_benchmark,
        schema::{
            DEFAULT_LIVE_TAIL_MAX_BLOCKS, DEFAULT_POSTGRES_SEGMENTED_SCHEMA,
            DEFAULT_SETTLED_SEGMENT_BLOCKS, MAX_SETTLED_SEGMENT_BYTES,
        },
    },
    report::{
        BenchmarkReport, FixtureCachePolicy, FixtureSummary, PostgresBenchmarkRuntimeEvidence,
        PostgresSegmentedBaselineInput, PostgresSegmentedReportInputs,
        build_postgres_segmented_storage_report, is_immutable_image_reference,
        is_valid_benchmark_trial_id,
    },
    rss::peak_rss,
};
use zinder_core::UnixTimestampMillis;

const DEFAULT_POSTGRES_DATABASE_URL_ENV: &str = "ZINDER_BENCH_POSTGRES_DATABASE_URL";

/// Cli reader connection mode mapped cleanly to [`ReaderConnectionMode`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub(super) enum CliReaderMode {
    /// Readers connect directly to PostgreSQL without an external pooler.
    #[default]
    Direct,
    /// Readers connect through `PgBouncer` in transaction pooling mode.
    PgbouncerTransaction,
}

impl From<CliReaderMode> for ReaderConnectionMode {
    fn from(mode: CliReaderMode) -> Self {
        match mode {
            CliReaderMode::Direct => Self::Direct,
            CliReaderMode::PgbouncerTransaction => Self::PgbouncerTransaction,
        }
    }
}

/// CLI options for `zinder-bench postgres-segmented`.
#[derive(Args)]
pub(super) struct PostgresSegmentedArgs {
    /// Captured fixture directory containing `manifest.json` and `.seg` files.
    #[arg(long)]
    fixture: PathBuf,
    /// Environment variable carrying the primary writer PostgreSQL URL.
    #[arg(long, default_value = DEFAULT_POSTGRES_DATABASE_URL_ENV)]
    database_url_env: String,
    /// Optional environment variable carrying a separate reader/PgBouncer URL.
    #[arg(long)]
    reader_database_url_env: Option<String>,
    /// Reader connection pooler mode (`direct` or `pgbouncer-transaction`).
    #[arg(long, value_enum, default_value_t = CliReaderMode::Direct)]
    reader_mode: CliReaderMode,
    /// Target PostgreSQL schema isolated for this benchmark run.
    #[arg(long, default_value = DEFAULT_POSTGRES_SEGMENTED_SCHEMA)]
    schema: String,
    /// Also persist and verify `ZSBLKB01` raw consensus block segments.
    #[arg(long, default_value_t = false)]
    full_blocks: bool,
    /// Maximum byte size allowed for any single settled segment (`<= 256 MiB`).
    #[arg(long, default_value_t = MAX_SETTLED_SEGMENT_BYTES)]
    max_segment_bytes: u64,
    /// Target blocks sealed per settled segment batch.
    #[arg(long, default_value_t = DEFAULT_SETTLED_SEGMENT_BLOCKS)]
    settled_segment_blocks: u32,
    /// Maximum blocks retained in `{schema}.canonical_live_tail`.
    #[arg(long, default_value_t = DEFAULT_LIVE_TAIL_MAX_BLOCKS)]
    live_tail_max_blocks: u32,
    /// Trailing blocks replayed one-by-one across uncontended and reader-contended phases.
    #[arg(long)]
    tip_follow_blocks: Option<u32>,
    /// Concurrent `REPEATABLE READ READ ONLY` reader workers active during contended tip-follow.
    #[arg(long, default_value_t = 4)]
    concurrent_readers: u32,
    /// Synthetic mempool transactions admitted and evicted per tip block.
    #[arg(long, default_value_t = 8)]
    mempool_churn_txs_per_block: u32,
    /// Optional `RocksDB` baseline report JSON (`rocksdb-canonical-fixture-replay`, `rocksdb-storage-lifecycle`, or `rocksdb-compact-block-range`) for ADR-0004 Decision 7 comparison.
    #[arg(long)]
    rocksdb_baseline_report: Option<PathBuf>,
    /// CPU limit applied to the benchmark client container, in logical cores.
    #[arg(long)]
    client_cpu_limit_cores: Option<f64>,
    /// Memory limit applied to the benchmark client container, in bytes.
    #[arg(long)]
    client_memory_limit_bytes: Option<u64>,
    /// CPU limit applied to the database container, in logical cores.
    #[arg(long)]
    database_cpu_limit_cores: Option<f64>,
    /// Memory limit applied to the database container, in bytes.
    #[arg(long)]
    database_memory_limit_bytes: Option<u64>,
    /// Immutable image reference for the measured database container.
    #[arg(long)]
    database_image_reference: Option<String>,
    /// Write the JSON report to this path instead of stdout (fails if file already exists).
    #[arg(long)]
    report: Option<PathBuf>,
    /// Source revision of the measured binary.
    #[arg(long)]
    software_revision: Option<String>,
    /// Campaign trial identity; requires `--fixture-cache-policy`.
    #[arg(long)]
    trial_id: Option<String>,
    /// Controlled fixture-cache treatment; requires `--trial-id`.
    #[arg(long, value_enum)]
    fixture_cache_policy: Option<FixtureCachePolicy>,
    /// Stable operator label for the complete benchmark arm.
    #[arg(long)]
    runner_id: Option<String>,
    /// Aggregate CPU limit for the complete benchmark arm.
    #[arg(long)]
    cpu_limit_cores: Option<f64>,
    /// Aggregate memory limit for the complete benchmark arm, in bytes.
    #[arg(long)]
    memory_limit_bytes: Option<u64>,
    /// Stable operator-defined storage performance class.
    #[arg(long)]
    storage_class: Option<String>,
    /// Immutable container image reference for the measured binary.
    #[arg(long)]
    image_reference: Option<String>,
}

pub(super) struct PostgresSegmentedCommandOutput {
    pub report: BenchmarkReport,
    pub report_path: Option<PathBuf>,
}

pub(super) async fn run_postgres_segmented_cli(
    args: PostgresSegmentedArgs,
) -> Result<PostgresSegmentedCommandOutput, BenchError> {
    let run_started_at_unix_millis = UnixTimestampMillis::now().value();
    if let Some(ref report_path) = args.report
        && report_path.exists()
    {
        return Err(BenchError::invalid_argument(format!(
            "report path {} already exists; refuse to overwrite existing artifact",
            report_path.display()
        )));
    }

    validate_cli_provenance_and_partitions(&args, run_started_at_unix_millis)?;
    let baseline = match args.rocksdb_baseline_report.as_deref() {
        Some(path) => Some(read_rocksdb_baseline_report(path)?),
        None => None,
    };

    let writer_url = read_env_url(&args.database_url_env, "--database-url-env")?;
    let reader_url = args
        .reader_database_url_env
        .as_deref()
        .map(|var_name| read_env_url(var_name, "--reader-database-url-env"))
        .transpose()?;

    let manifest = FixtureManifest::read(&args.fixture)?;
    let fixture_summary = FixtureSummary::try_from(&manifest)?;
    let run_report = run_postgres_segmented_benchmark(PostgresSegmentedConfig {
        fixture_directory: args.fixture,
        database_url: writer_url,
        reader_database_url: reader_url,
        schema_name: args.schema,
        reader_mode: args.reader_mode.into(),
        full_blocks: args.full_blocks,
        max_segment_bytes: args.max_segment_bytes,
        settled_segment_blocks: args.settled_segment_blocks,
        live_tail_max_blocks: args.live_tail_max_blocks,
        tip_follow_blocks: args.tip_follow_blocks,
        concurrent_readers: args.concurrent_readers,
        mempool_churn_txs_per_block: args.mempool_churn_txs_per_block,
    })
    .await?;

    let inputs = PostgresSegmentedReportInputs {
        run: run_report,
        baseline,
        runtime_evidence: PostgresBenchmarkRuntimeEvidence {
            database_image_reference: args.database_image_reference,
            client_cpu_limit_cores: args.client_cpu_limit_cores,
            client_memory_limit_bytes: args.client_memory_limit_bytes,
            database_cpu_limit_cores: args.database_cpu_limit_cores,
            database_memory_limit_bytes: args.database_memory_limit_bytes,
        },
        benchmark_client_peak_rss: peak_rss(),
        software_revision: args.software_revision,
        trial_id: args.trial_id,
        fixture_cache_policy: args.fixture_cache_policy,
        run_started_at_unix_millis,
        run_completed_at_unix_millis: UnixTimestampMillis::now().value(),
        runner_id: args.runner_id,
        cpu_limit_cores: args.cpu_limit_cores,
        memory_limit_bytes: args.memory_limit_bytes,
        storage_class: args.storage_class,
        image_reference: args.image_reference,
    };

    let report = build_postgres_segmented_storage_report(fixture_summary, inputs);
    report.validate()?;
    Ok(PostgresSegmentedCommandOutput {
        report,
        report_path: args.report,
    })
}

fn validate_cli_provenance_and_partitions(
    args: &PostgresSegmentedArgs,
    _started: u64,
) -> Result<(), BenchError> {
    validate_positive_f64(args.cpu_limit_cores, "--cpu-limit-cores")?;
    validate_positive_f64(args.client_cpu_limit_cores, "--client-cpu-limit-cores")?;
    validate_positive_f64(args.database_cpu_limit_cores, "--database-cpu-limit-cores")?;
    validate_positive_u64(args.memory_limit_bytes, "--memory-limit-bytes")?;
    validate_positive_u64(
        args.client_memory_limit_bytes,
        "--client-memory-limit-bytes",
    )?;
    validate_positive_u64(
        args.database_memory_limit_bytes,
        "--database-memory-limit-bytes",
    )?;

    match (&args.trial_id, args.fixture_cache_policy) {
        (None, None) => {}
        (Some(trial_id), Some(_)) if is_valid_benchmark_trial_id(trial_id) => {}
        (Some(_), Some(_)) => {
            return Err(BenchError::invalid_argument(
                "--trial-id must be nonblank ASCII without control chars or path separators",
            ));
        }
        _ => {
            return Err(BenchError::invalid_argument(
                "--trial-id and --fixture-cache-policy must be supplied together",
            ));
        }
    }

    if args
        .image_reference
        .as_deref()
        .is_some_and(|r| !is_immutable_image_reference(r))
    {
        return Err(BenchError::invalid_argument(
            "--image-reference must be a sha256 image ID or digest-pinned reference",
        ));
    }
    if args
        .database_image_reference
        .as_deref()
        .is_some_and(|r| !is_immutable_image_reference(r))
    {
        return Err(BenchError::invalid_argument(
            "--database-image-reference must be a sha256 image ID or digest-pinned reference",
        ));
    }

    validate_resource_partition_totals(args)
}

fn validate_resource_partition_totals(args: &PostgresSegmentedArgs) -> Result<(), BenchError> {
    match (
        args.cpu_limit_cores,
        args.client_cpu_limit_cores,
        args.database_cpu_limit_cores,
    ) {
        (None, None, None) => {}
        (Some(agg), Some(cli), Some(db))
            if (cli + db - agg).abs() <= f64::EPSILON * agg.max(1.0) => {}
        (Some(_), Some(_), Some(_)) => {
            return Err(BenchError::invalid_argument(
                "--client-cpu-limit-cores plus --database-cpu-limit-cores must equal --cpu-limit-cores",
            ));
        }
        _ => {
            return Err(BenchError::invalid_argument(
                "CPU resource partition requires --cpu-limit-cores, --client-cpu-limit-cores, and --database-cpu-limit-cores together",
            ));
        }
    }
    match (
        args.memory_limit_bytes,
        args.client_memory_limit_bytes,
        args.database_memory_limit_bytes,
    ) {
        (None, None, None) => {}
        (Some(agg), Some(cli), Some(db)) if cli.checked_add(db) == Some(agg) => {}
        (Some(_), Some(_), Some(_)) => {
            return Err(BenchError::invalid_argument(
                "--client-memory-limit-bytes plus --database-memory-limit-bytes must equal --memory-limit-bytes",
            ));
        }
        _ => {
            return Err(BenchError::invalid_argument(
                "Memory resource partition requires --memory-limit-bytes, --client-memory-limit-bytes, and --database-memory-limit-bytes together",
            ));
        }
    }
    Ok(())
}

fn validate_positive_f64(candidate: Option<f64>, flag: &str) -> Result<(), BenchError> {
    if let Some(number) = candidate
        && (!number.is_finite() || number <= 0.0)
    {
        return Err(BenchError::invalid_argument(format!(
            "{flag} must be finite and greater than zero"
        )));
    }
    Ok(())
}

fn validate_positive_u64(candidate: Option<u64>, flag: &str) -> Result<(), BenchError> {
    if candidate == Some(0) {
        return Err(BenchError::invalid_argument(format!(
            "{flag} must be greater than zero"
        )));
    }
    Ok(())
}

fn read_env_url(env_var: &str, flag: &str) -> Result<String, BenchError> {
    if env_var.trim().is_empty() {
        return Err(BenchError::invalid_argument(format!(
            "{flag} must name a nonblank environment variable"
        )));
    }
    env::var(env_var).map_err(|_| {
        BenchError::invalid_argument(format!(
            "environment variable {env_var} must contain the PostgreSQL URL"
        ))
    })
}

fn json_req_f64(doc: &serde_json::Value, ptr: &str) -> Result<f64, BenchError> {
    doc.pointer(ptr)
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| BenchError::report_format(format!("baseline missing {ptr}")))
}

fn json_req_u64(doc: &serde_json::Value, ptr: &str) -> Result<u64, BenchError> {
    doc.pointer(ptr)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| BenchError::report_format(format!("baseline missing {ptr}")))
}

fn read_rocksdb_baseline_report(path: &Path) -> Result<PostgresSegmentedBaselineInput, BenchError> {
    let raw = fs::read(path).map_err(|err| BenchError::io(path, err))?;
    let json_doc: serde_json::Value = serde_json::from_slice(&raw)
        .map_err(|err| BenchError::report_format(format!("invalid baseline report JSON: {err}")))?;
    let measurement_kind = json_doc
        .get("measurement_kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| BenchError::report_format("baseline report missing measurement_kind"))?;

    match measurement_kind {
        "rocksdb-storage-lifecycle" => {
            let total_secs = json_req_f64(&json_doc, "/phase_durations/total_seconds")?;
            let can_bytes =
                json_req_u64(&json_doc, "/canonical_storage_ready/physical_store_bytes")?;
            let wal_bytes = json_req_u64(&json_doc, "/wallet_storage_ready/physical_store_bytes")?;
            Ok(PostgresSegmentedBaselineInput {
                build_seconds: Some(total_secs),
                storage_bytes: Some(can_bytes.saturating_add(wal_bytes)),
                streaming_blocks_per_second: None,
            })
        }
        "rocksdb-canonical-fixture-replay" => Ok(PostgresSegmentedBaselineInput {
            build_seconds: Some(json_req_f64(&json_doc, "/source_load/wall_clock_seconds")?),
            storage_bytes: Some(json_req_u64(
                &json_doc,
                "/canonical_ready/physical_store_bytes",
            )?),
            streaming_blocks_per_second: json_doc
                .pointer("/source_load/blocks_per_second")
                .and_then(serde_json::Value::as_f64),
        }),
        "canonical-replay-storage" => Ok(PostgresSegmentedBaselineInput {
            build_seconds: Some(json_req_f64(&json_doc, "/round_trip/wall_clock_seconds")?),
            storage_bytes: Some(json_req_u64(
                &json_doc,
                "/round_trip/physical_storage_bytes",
            )?),
            streaming_blocks_per_second: json_doc
                .pointer("/round_trip/blocks_per_second")
                .and_then(serde_json::Value::as_f64),
        }),
        other => Err(BenchError::report_format(format!(
            "unsupported baseline report measurement_kind `{other}`"
        ))),
    }
}
