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
        BenchmarkReport, FixtureCachePolicy, FixtureSummary, PostgresSegmentedBaselineInput,
        PostgresSegmentedReportInputs, PostgresSegmentedRuntimeEvidence,
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
    /// Optional `RocksDB` lifecycle baseline report JSON (`rocksdb-storage-lifecycle`) matching the fixture network, range, retention, and canonical-plus-wallet scope for ADR-0004 Decision 7 build/storage comparison.
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
    /// CPU limit applied to the connection pooler container (`PgBouncer`), in logical cores.
    #[arg(long)]
    pooler_cpu_limit_cores: Option<f64>,
    /// Memory limit applied to the connection pooler container (`PgBouncer`), in bytes.
    #[arg(long)]
    pooler_memory_limit_bytes: Option<u64>,
    /// Immutable image reference for the measured connection pooler container (`PgBouncer`).
    #[arg(long)]
    pooler_image_reference: Option<String>,
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
    let manifest = FixtureManifest::read(&args.fixture)?;
    let fixture_summary = FixtureSummary::try_from(&manifest)?;
    let baseline = args
        .rocksdb_baseline_report
        .as_deref()
        .map(|path| {
            read_rocksdb_baseline_report(
                path,
                &BaselineExpectation {
                    fixture: &fixture_summary,
                    full_blocks: args.full_blocks,
                },
            )
        })
        .transpose()?;

    let writer_url = read_env_url(&args.database_url_env, "--database-url-env")?;
    let reader_url = args
        .reader_database_url_env
        .as_deref()
        .map(|var_name| read_env_url(var_name, "--reader-database-url-env"))
        .transpose()?;

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
        runtime_evidence: PostgresSegmentedRuntimeEvidence {
            database_image_reference: args.database_image_reference,
            pooler_image_reference: args.pooler_image_reference,
            client_cpu_limit_cores: args.client_cpu_limit_cores,
            client_memory_limit_bytes: args.client_memory_limit_bytes,
            database_cpu_limit_cores: args.database_cpu_limit_cores,
            database_memory_limit_bytes: args.database_memory_limit_bytes,
            pooler_cpu_limit_cores: args.pooler_cpu_limit_cores,
            pooler_memory_limit_bytes: args.pooler_memory_limit_bytes,
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
    validate_positive_f64(args.pooler_cpu_limit_cores, "--pooler-cpu-limit-cores")?;
    validate_positive_u64(args.memory_limit_bytes, "--memory-limit-bytes")?;
    validate_positive_u64(
        args.client_memory_limit_bytes,
        "--client-memory-limit-bytes",
    )?;
    validate_positive_u64(
        args.database_memory_limit_bytes,
        "--database-memory-limit-bytes",
    )?;
    validate_positive_u64(
        args.pooler_memory_limit_bytes,
        "--pooler-memory-limit-bytes",
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

    for (flag, img_opt) in [
        ("--image-reference", args.image_reference.as_deref()),
        (
            "--database-image-reference",
            args.database_image_reference.as_deref(),
        ),
        (
            "--pooler-image-reference",
            args.pooler_image_reference.as_deref(),
        ),
    ] {
        if img_opt.is_some_and(|r| !is_immutable_image_reference(r)) {
            return Err(BenchError::invalid_argument(format!(
                "{flag} must be a sha256 image ID or digest-pinned reference"
            )));
        }
    }

    validate_resource_partition_totals(args)
}

fn validate_resource_partition_totals(args: &PostgresSegmentedArgs) -> Result<(), BenchError> {
    if args.reader_mode == CliReaderMode::Direct
        && (args.pooler_cpu_limit_cores.is_some()
            || args.pooler_memory_limit_bytes.is_some()
            || args.pooler_image_reference.is_some())
    {
        return Err(BenchError::invalid_argument(
            "--pooler-* flags are only valid with --reader-mode pgbouncer-transaction",
        ));
    }

    validate_cpu_budget_partition(args)?;
    validate_memory_budget_partition(args)
}

fn validate_cpu_budget_partition(args: &PostgresSegmentedArgs) -> Result<(), BenchError> {
    let any_cpu = args.cpu_limit_cores.is_some()
        || args.client_cpu_limit_cores.is_some()
        || args.database_cpu_limit_cores.is_some()
        || args.pooler_cpu_limit_cores.is_some();
    if !any_cpu {
        return Ok(());
    }
    match args.reader_mode {
        CliReaderMode::Direct => match (
            args.cpu_limit_cores,
            args.client_cpu_limit_cores,
            args.database_cpu_limit_cores,
        ) {
            (Some(agg), Some(cli), Some(db))
                if (cli + db - agg).abs() <= f64::EPSILON * agg.max(1.0) => {}
            (Some(_), Some(_), Some(_)) => {
                return Err(BenchError::invalid_argument(
                    "--client-cpu-limit-cores plus --database-cpu-limit-cores must equal --cpu-limit-cores",
                ));
            }
            _ => {
                return Err(BenchError::invalid_argument(
                    "Direct CPU partition requires --cpu-limit-cores, --client-cpu-limit-cores, and --database-cpu-limit-cores together",
                ));
            }
        },
        CliReaderMode::PgbouncerTransaction => match (
            args.cpu_limit_cores,
            args.client_cpu_limit_cores,
            args.database_cpu_limit_cores,
            args.pooler_cpu_limit_cores,
        ) {
            (Some(agg), Some(cli), Some(db), Some(pool))
                if (cli + db + pool - agg).abs() <= f64::EPSILON * agg.max(1.0) => {}
            (Some(_), Some(_), Some(_), Some(_)) => {
                return Err(BenchError::invalid_argument(
                    "--client-cpu-limit-cores + --database-cpu-limit-cores + --pooler-cpu-limit-cores must equal --cpu-limit-cores",
                ));
            }
            _ => {
                return Err(BenchError::invalid_argument(
                    "Pooled CPU partition requires --cpu-limit-cores, --client-cpu-limit-cores, --database-cpu-limit-cores, and --pooler-cpu-limit-cores together",
                ));
            }
        },
    }
    Ok(())
}

fn validate_memory_budget_partition(args: &PostgresSegmentedArgs) -> Result<(), BenchError> {
    let any_mem = args.memory_limit_bytes.is_some()
        || args.client_memory_limit_bytes.is_some()
        || args.database_memory_limit_bytes.is_some()
        || args.pooler_memory_limit_bytes.is_some();
    if !any_mem {
        return Ok(());
    }
    match args.reader_mode {
        CliReaderMode::Direct => match (
            args.memory_limit_bytes,
            args.client_memory_limit_bytes,
            args.database_memory_limit_bytes,
        ) {
            (Some(agg), Some(cli), Some(db)) if cli.checked_add(db) == Some(agg) => {}
            (Some(_), Some(_), Some(_)) => {
                return Err(BenchError::invalid_argument(
                    "--client-memory-limit-bytes plus --database-memory-limit-bytes must equal --memory-limit-bytes",
                ));
            }
            _ => {
                return Err(BenchError::invalid_argument(
                    "Direct memory partition requires --memory-limit-bytes, --client-memory-limit-bytes, and --database-memory-limit-bytes together",
                ));
            }
        },
        CliReaderMode::PgbouncerTransaction => match (
            args.memory_limit_bytes,
            args.client_memory_limit_bytes,
            args.database_memory_limit_bytes,
            args.pooler_memory_limit_bytes,
        ) {
            (Some(agg), Some(cli), Some(db), Some(pool))
                if cli.checked_add(db).and_then(|s| s.checked_add(pool)) == Some(agg) => {}
            (Some(_), Some(_), Some(_), Some(_)) => {
                return Err(BenchError::invalid_argument(
                    "--client-memory-limit-bytes + --database-memory-limit-bytes + --pooler-memory-limit-bytes must equal --memory-limit-bytes",
                ));
            }
            _ => {
                return Err(BenchError::invalid_argument(
                    "Pooled memory partition requires --memory-limit-bytes, --client-memory-limit-bytes, --database-memory-limit-bytes, and --pooler-memory-limit-bytes together",
                ));
            }
        },
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

struct BaselineExpectation<'a> {
    fixture: &'a FixtureSummary,
    full_blocks: bool,
}

fn json_req_f64(doc: &serde_json::Value, ptr: &str) -> Result<f64, BenchError> {
    let number = doc
        .pointer(ptr)
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| BenchError::report_format(format!("baseline missing {ptr}")))?;
    if !number.is_finite() || number <= 0.0 {
        return Err(BenchError::report_format(format!(
            "baseline {ptr} must be finite and positive"
        )));
    }
    Ok(number)
}

fn json_req_u64(doc: &serde_json::Value, ptr: &str) -> Result<u64, BenchError> {
    doc.pointer(ptr)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| BenchError::report_format(format!("baseline missing {ptr}")))
}

fn json_req_str<'a>(doc: &'a serde_json::Value, ptr: &str) -> Result<&'a str, BenchError> {
    doc.pointer(ptr)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| BenchError::report_format(format!("baseline missing {ptr}")))
}

fn json_req_bool(doc: &serde_json::Value, ptr: &str) -> Result<bool, BenchError> {
    doc.pointer(ptr)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| BenchError::report_format(format!("baseline missing {ptr}")))
}

fn is_lowercase_sha256_hex(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn read_rocksdb_baseline_report(
    path: &Path,
    expected: &BaselineExpectation<'_>,
) -> Result<PostgresSegmentedBaselineInput, BenchError> {
    let raw = fs::read(path).map_err(|err| BenchError::io(path, err))?;
    let json_doc: serde_json::Value = serde_json::from_slice(&raw)
        .map_err(|err| BenchError::report_format(format!("invalid baseline report JSON: {err}")))?;
    if json_req_str(&json_doc, "/contract_identity")? != "benchmark-report"
        || json_req_u64(&json_doc, "/report_format_version")? != 1
    {
        return Err(BenchError::report_format(
            "baseline report contract_identity or report_format_version is unsupported",
        ));
    }
    match json_req_str(&json_doc, "/measurement_kind")? {
        "rocksdb-storage-lifecycle" => parse_rocksdb_lifecycle_baseline(&json_doc, expected),
        "rocksdb-canonical-fixture-replay" => {
            if json_req_str(&json_doc, "/storage_candidate/canonical_engine")? != "rocksdb" {
                return Err(BenchError::report_format(
                    "baseline report canonical_engine is not rocksdb",
                ));
            }
            Err(BenchError::report_format(
                "rocksdb-canonical-fixture-replay is canonical-only and lacks wallet projection; supply a matching rocksdb-storage-lifecycle baseline",
            ))
        }
        "canonical-replay-storage" => {
            let candidate_engine = json_req_str(&json_doc, "/storage_candidate/canonical_engine")?;
            let storage_engine = json_req_str(&json_doc, "/round_trip/storage/engine")?;
            if candidate_engine != "rocksdb" || storage_engine != "rocksdb" {
                return Err(BenchError::report_format(format!(
                    "canonical-replay-storage baseline uses engine `{candidate_engine}/{storage_engine}`, not rocksdb"
                )));
            }
            Err(BenchError::report_format(
                "canonical-replay-storage is a block-local canonical-only report without wallet projection; supply a matching rocksdb-storage-lifecycle baseline",
            ))
        }
        other => Err(BenchError::report_format(format!(
            "unsupported baseline report measurement_kind `{other}`"
        ))),
    }
}

fn parse_rocksdb_lifecycle_baseline(
    json_doc: &serde_json::Value,
    expected: &BaselineExpectation<'_>,
) -> Result<PostgresSegmentedBaselineInput, BenchError> {
    if json_req_str(json_doc, "/storage_candidate/id")? != "rocksdb-storage-lifecycle"
        || json_req_str(json_doc, "/storage_candidate/canonical_engine")? != "rocksdb"
        || json_req_str(json_doc, "/storage_candidate/topology")? != "rocksdb"
    {
        return Err(BenchError::report_format(
            "baseline storage_candidate is not rocksdb-storage-lifecycle",
        ));
    }
    if expected.full_blocks {
        return Err(BenchError::report_format(
            "rocksdb-storage-lifecycle baseline retains RawBlobRetention::None and is incompatible with --full-blocks",
        ));
    }
    validate_lifecycle_baseline_dataset(json_doc, expected.fixture)?;

    let total_secs = json_req_f64(json_doc, "/phase_durations/total_seconds")?;
    let can_bytes = json_req_u64(json_doc, "/canonical_storage_ready/physical_store_bytes")?;
    let wal_bytes = json_req_u64(json_doc, "/wallet_storage_ready/physical_store_bytes")?;
    let total_bytes = can_bytes
        .checked_add(wal_bytes)
        .filter(|&sum| sum > 0)
        .ok_or_else(|| BenchError::report_format("baseline storage bytes must be positive"))?;

    Ok(PostgresSegmentedBaselineInput {
        build_seconds: Some(total_secs),
        storage_bytes: Some(total_bytes),
        streaming_blocks_per_second: None,
        network: expected.fixture.network.clone(),
        first_height: expected.fixture.from_height,
        tip_height: expected.fixture.to_height,
        block_count: u64::from(expected.fixture.block_count),
        sequence_digest_sha256: expected
            .fixture
            .canonical_block_facts_digest_evidence
            .sequence_digest_sha256
            .clone(),
        full_blocks: false,
        includes_wallet_projection: true,
    })
}

fn validate_lifecycle_baseline_dataset(
    json_doc: &serde_json::Value,
    fixture: &FixtureSummary,
) -> Result<(), BenchError> {
    if json_req_str(json_doc, "/canonical_storage_ready/scope")? != "canonical-storage-ready"
        || json_req_str(json_doc, "/wallet_storage_ready/scope")? != "wallet-storage-ready"
        || !json_req_bool(
            json_doc,
            "/canonical_storage_ready/cold_reopen_evidence_match",
        )?
        || !json_req_bool(json_doc, "/wallet_storage_ready/cold_reopen_evidence_match")?
        || !json_req_bool(json_doc, "/wallet_storage_ready/canonical_fence_match")?
    {
        return Err(BenchError::report_format(
            "baseline lifecycle report lacks verified canonical and wallet READY evidence",
        ));
    }
    let network = json_req_str(json_doc, "/source/network")?;
    let first_height = json_req_u64(
        json_doc,
        "/canonical_storage_ready/first_retained_block/height",
    )?;
    let tip_height = json_req_u64(json_doc, "/canonical_storage_ready/visible_tip/height")?;
    let tip_hash = json_req_str(json_doc, "/canonical_storage_ready/visible_tip/hash_hex")?;
    let block_count = json_req_u64(json_doc, "/canonical_storage_ready/block_count")?;
    let wallet_blocks = json_req_u64(json_doc, "/wallet_storage_ready/scanned_block_count")?;
    let can_digest = json_req_str(json_doc, "/canonical_storage_ready/sequence_digest/sha256")?;
    let wal_digest = json_req_str(
        json_doc,
        "/wallet_storage_ready/source_sequence_digest/sha256",
    )?;
    let expected_digest = &fixture
        .canonical_block_facts_digest_evidence
        .sequence_digest_sha256;

    if network != fixture.network
        || first_height != u64::from(fixture.from_height)
        || tip_height != u64::from(fixture.to_height)
        || tip_hash != fixture.tip_hash_hex
        || block_count != u64::from(fixture.block_count)
        || wallet_blocks != block_count
        || !is_lowercase_sha256_hex(can_digest)
        || can_digest != *expected_digest
        || wal_digest != *expected_digest
    {
        return Err(BenchError::report_format(
            "baseline lifecycle report network, height range, block count, tip hash, or sequence digest disagrees with fixture",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BaselineExpectation, CliReaderMode, PostgresSegmentedArgs, read_rocksdb_baseline_report,
        validate_cli_provenance_and_partitions,
    };
    use clap::Parser;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use zinder_bench::fixture::{
        CanonicalBlockFactsDigestEvidence, FIXTURE_CONTRACT_IDENTITY, FIXTURE_FORMAT_VERSION,
        WorkloadDensity,
    };
    use zinder_bench::report::FixtureSummary;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        args: PostgresSegmentedArgs,
    }

    fn test_fixture_summary() -> FixtureSummary {
        FixtureSummary {
            contract_identity: FIXTURE_CONTRACT_IDENTITY.to_owned(),
            fixture_format_version: FIXTURE_FORMAT_VERSION,
            canonical_artifact_schema_version: 2,
            canonical_block_facts_digest_evidence: CanonicalBlockFactsDigestEvidence {
                block_digest_version: 1,
                sequence_digest_version: 1,
                block_count: 10,
                sequence_digest_sha256: "22".repeat(32),
            },
            tip_hash_hex: "11".repeat(32),
            digest_sha256: "33".repeat(32),
            network: "zcash-regtest".to_owned(),
            from_height: 1,
            to_height: 10,
            block_count: 10,
            workload_density: WorkloadDensity {
                block_count: 10,
                raw_block_bytes: 1_400,
                transaction_count: 10,
                transparent_input_count: 0,
                transparent_output_count: 10,
                blocks_with_transparent_inputs: 0,
                blocks_with_transparent_outputs: 10,
                max_raw_block_bytes_per_block: 140,
                max_transactions_per_block: 1,
                max_transparent_inputs_per_block: 0,
                max_transparent_outputs_per_block: 1,
            },
            segment_count: 1,
        }
    }

    fn valid_lifecycle_baseline_json() -> serde_json::Value {
        serde_json::json!({
            "contract_identity": "benchmark-report",
            "report_format_version": 1,
            "measurement_kind": "rocksdb-storage-lifecycle",
            "storage_candidate": {
                "id": "rocksdb-storage-lifecycle",
                "canonical_engine": "rocksdb",
                "topology": "rocksdb"
            },
            "source": { "network": "zcash-regtest" },
            "phase_durations": { "total_seconds": 12.5 },
            "canonical_storage_ready": {
                "scope": "canonical-storage-ready",
                "first_retained_block": { "height": 1, "hash_hex": "aa".repeat(32) },
                "visible_tip": { "height": 10, "hash_hex": "11".repeat(32) },
                "block_count": 10,
                "sequence_digest": { "sha256": "22".repeat(32) },
                "physical_store_bytes": 800_000_u64,
                "cold_reopen_evidence_match": true
            },
            "wallet_storage_ready": {
                "scope": "wallet-storage-ready",
                "scanned_block_count": 10,
                "source_sequence_digest": { "sha256": "22".repeat(32) },
                "physical_store_bytes": 187_654_u64,
                "cold_reopen_evidence_match": true,
                "canonical_fence_match": true
            }
        })
    }

    fn write_json_temp(doc: &serde_json::Value) -> Result<NamedTempFile, crate::BenchError> {
        let mut file =
            NamedTempFile::new().map_err(|err| crate::BenchError::io("baseline-test.json", err))?;
        file.write_all(doc.to_string().as_bytes())
            .map_err(|err| crate::BenchError::io("write baseline json", err))?;
        Ok(file)
    }

    #[test]
    fn matching_lifecycle_baseline_parses_canonical_plus_wallet_metrics()
    -> Result<(), crate::BenchError> {
        let fixture = test_fixture_summary();
        let file = write_json_temp(&valid_lifecycle_baseline_json())?;
        let parsed = read_rocksdb_baseline_report(
            file.path(),
            &BaselineExpectation {
                fixture: &fixture,
                full_blocks: false,
            },
        )?;
        assert_eq!(parsed.build_seconds, Some(12.5));
        assert_eq!(parsed.storage_bytes, Some(987_654));
        assert_eq!(parsed.streaming_blocks_per_second, None);
        assert!(parsed.includes_wallet_projection);
        assert!(!parsed.full_blocks);
        Ok(())
    }

    #[test]
    fn baseline_rejects_postgres_diagnostic_and_canonical_only_reports()
    -> Result<(), crate::BenchError> {
        let fixture = test_fixture_summary();
        let expected = BaselineExpectation {
            fixture: &fixture,
            full_blocks: false,
        };

        let pg_diag = write_json_temp(&serde_json::json!({
            "contract_identity": "benchmark-report",
            "report_format_version": 1,
            "measurement_kind": "canonical-replay-storage",
            "storage_candidate": {
                "id": "postgres-canonical-replay-storage",
                "canonical_engine": "postgres",
                "topology": "postgres-diagnostic"
            },
            "round_trip": {
                "wall_clock_seconds": 4.25,
                "physical_storage_bytes": 123_456_u64,
                "storage": { "engine": "postgres" }
            }
        }))?;
        let pg_err = read_rocksdb_baseline_report(pg_diag.path(), &expected)
            .err()
            .ok_or_else(|| crate::BenchError::report_format("pg diagnostic must fail"))?;
        assert!(pg_err.to_string().contains("not rocksdb"));

        let rocks_replay = write_json_temp(&serde_json::json!({
            "contract_identity": "benchmark-report",
            "report_format_version": 1,
            "measurement_kind": "rocksdb-canonical-fixture-replay",
            "storage_candidate": {
                "id": "rocksdb-canonical-fixture-replay",
                "canonical_engine": "rocksdb",
                "topology": "rocksdb"
            },
            "total_seconds": 12.5,
            "physical_store_bytes": 987_654_u64
        }))?;
        let replay_err = read_rocksdb_baseline_report(rocks_replay.path(), &expected)
            .err()
            .ok_or_else(|| crate::BenchError::report_format("canonical-only report must fail"))?;
        assert!(replay_err.to_string().contains("canonical-only"));
        Ok(())
    }

    #[test]
    fn lifecycle_baseline_rejects_mismatched_network_range_and_retention()
    -> Result<(), crate::BenchError> {
        let fixture = test_fixture_summary();
        let expected = BaselineExpectation {
            fixture: &fixture,
            full_blocks: false,
        };

        let mut wrong_net = valid_lifecycle_baseline_json();
        wrong_net["source"]["network"] = serde_json::json!("zcash-testnet");
        assert!(
            read_rocksdb_baseline_report(write_json_temp(&wrong_net)?.path(), &expected).is_err()
        );

        let mut wrong_range = valid_lifecycle_baseline_json();
        wrong_range["canonical_storage_ready"]["visible_tip"]["height"] = serde_json::json!(20);
        assert!(
            read_rocksdb_baseline_report(write_json_temp(&wrong_range)?.path(), &expected).is_err()
        );

        let mut wrong_digest = valid_lifecycle_baseline_json();
        wrong_digest["canonical_storage_ready"]["sequence_digest"]["sha256"] =
            serde_json::json!("99".repeat(32));
        assert!(
            read_rocksdb_baseline_report(write_json_temp(&wrong_digest)?.path(), &expected)
                .is_err()
        );

        let full_blocks_expected = BaselineExpectation {
            fixture: &fixture,
            full_blocks: true,
        };
        assert!(
            read_rocksdb_baseline_report(
                write_json_temp(&valid_lifecycle_baseline_json())?.path(),
                &full_blocks_expected
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn pgbouncer_resource_partition_requires_pooler_and_accepts_matched_envelope()
    -> Result<(), crate::BenchError> {
        let valid = TestCli::try_parse_from([
            "test",
            "--fixture",
            "/tmp/fx",
            "--reader-mode",
            "pgbouncer-transaction",
            "--cpu-limit-cores",
            "8.0",
            "--memory-limit-bytes",
            "17179869184",
            "--client-cpu-limit-cores",
            "1.5",
            "--client-memory-limit-bytes",
            "8053063680",
            "--database-cpu-limit-cores",
            "6.0",
            "--database-memory-limit-bytes",
            "8589934592",
            "--pooler-cpu-limit-cores",
            "0.5",
            "--pooler-memory-limit-bytes",
            "536870912",
        ])
        .map_err(|err| crate::BenchError::invalid_argument(err.to_string()))?;
        assert_eq!(valid.args.reader_mode, CliReaderMode::PgbouncerTransaction);
        validate_cli_provenance_and_partitions(&valid.args, 0)?;

        let mut missing_pooler = valid.args;
        missing_pooler.pooler_cpu_limit_cores = None;
        assert!(validate_cli_provenance_and_partitions(&missing_pooler, 0).is_err());

        missing_pooler.pooler_cpu_limit_cores = Some(1.0);
        assert!(validate_cli_provenance_and_partitions(&missing_pooler, 0).is_err());
        Ok(())
    }
}
