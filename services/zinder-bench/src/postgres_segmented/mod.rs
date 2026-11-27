//! Segmented `PostgreSQL` build, tip-following, mempool churn, and pooled
//! concurrent reader benchmark arm (ADR-0004 Decision 7 diagnostic harness).

pub mod database;
pub mod loader;
pub mod reader;
pub mod schema;
pub mod segment_builder;
pub mod storage_footprint;
pub mod tip_follow;
pub mod wallet_projection;

use std::fmt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use zinder_core::{BlockHash, BlockHeight, Network};

use crate::canonical_replay_storage::postgres::PostgresServerSettings;
use crate::error::BenchError;
use crate::fixture::FixtureManifest;

use database::{
    PostgresConnection, WalLsn, close_postgres, connect_postgres, measure_wal_lsn,
    read_server_settings, reject_existing_schema, validate_required_posture, wal_bytes_delta,
};
use loader::{
    BulkLoadTimings, PreparedBenchmarkChain, SchemaSeedMetadata, load_bulk_prefix,
    mark_schema_completed, prepare_all_fixture_blocks,
};
use reader::{
    ConcurrentReaderPoolConfig, ConcurrentReaderSummary, PersistedValidationSummary,
    ReaderConnectionMode, ValidationExpectations, spawn_concurrent_readers,
    validate_persisted_state_read_only,
};
use schema::{MAX_SETTLED_SEGMENT_BYTES, validate_schema_name};
use segment_builder::PreparedSegmentedBlock;
use storage_footprint::{StorageFootprintSummary, measure_storage_footprint};
use tip_follow::{
    LatencyPercentilesMillis, TipFollowParameters, TipFollowRunMetrics, TipFollowState,
};
use wallet_projection::ProvisionalWalletProjection;

/// Inputs configuring one segmented `PostgreSQL` benchmark run.
///
/// Connection strings remain redacted in [`fmt::Debug`].
#[derive(Clone)]
pub struct PostgresSegmentedConfig {
    /// Captured fixture directory containing `manifest.json` and `.seg` files.
    pub fixture_directory: PathBuf,
    /// Writer `PostgreSQL` connection URL (direct primary connection).
    pub database_url: String,
    /// Optional separate connection URL for concurrent readers (e.g. `PgBouncer` transaction pool).
    pub reader_database_url: Option<String>,
    /// Target `PostgreSQL` schema isolated for this benchmark run.
    pub schema_name: String,
    /// Declared reader pooling mode.
    pub reader_mode: ReaderConnectionMode,
    /// Retain raw consensus block blobs (`ZSBLKB01` + `canonical_live_tail.raw_block_bytes`).
    pub full_blocks: bool,
    /// Maximum byte size permitted for any single settled segment (`<= 256 MiB`).
    pub max_segment_bytes: u64,
    /// Number of contiguous blocks sealed per settled segment batch.
    pub settled_segment_blocks: u32,
    /// Maximum rows retained in `{schema}.canonical_live_tail`.
    pub live_tail_max_blocks: u32,
    /// Optional explicit number of trailing blocks replayed one-by-one during tip-follow phases.
    pub tip_follow_blocks: Option<u32>,
    /// Number of concurrent read-only workers spawned during the contended tip-follow phase.
    pub concurrent_readers: u32,
    /// Synthetic mempool transactions admitted+evicted per tip block.
    pub mempool_churn_txs_per_block: u32,
}

impl fmt::Debug for PostgresSegmentedConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresSegmentedConfig")
            .field("fixture_directory", &self.fixture_directory)
            .field("database_url", &"<redacted>")
            .field(
                "reader_database_url",
                &self.reader_database_url.as_ref().map(|_| "<redacted>"),
            )
            .field("schema_name", &self.schema_name)
            .field("reader_mode", &self.reader_mode)
            .field("full_blocks", &self.full_blocks)
            .field("max_segment_bytes", &self.max_segment_bytes)
            .field("settled_segment_blocks", &self.settled_segment_blocks)
            .field("live_tail_max_blocks", &self.live_tail_max_blocks)
            .field("tip_follow_blocks", &self.tip_follow_blocks)
            .field("concurrent_readers", &self.concurrent_readers)
            .field(
                "mempool_churn_txs_per_block",
                &self.mempool_churn_txs_per_block,
            )
            .finish()
    }
}

/// Phase-level wall-clock timings captured across build, tip-following, validation, and measurement.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PostgresSegmentedBuildTimings {
    /// End-to-end duration across all phases.
    pub wall_clock_seconds: f64,
    /// Fixture parse, canonical construction, and replay encoding duration.
    pub prepare_seconds: f64,
    /// Settled segment sealing duration (`ZSHRNG02`, `ZSTSUP01`, optional `ZSBLKB01`).
    pub seal_seconds: f64,
    /// Binary `COPY` persistence duration for bulk tables.
    pub bulk_copy_seconds: f64,
    /// PK/index construction and `ANALYZE` duration.
    pub finalize_seconds: f64,
    /// Uncontended tip-follow phase duration (`0` concurrent readers).
    pub uncontended_tip_follow_seconds: f64,
    /// Contended tip-follow phase duration (`N` concurrent readers active).
    pub contended_tip_follow_seconds: f64,
    /// Fresh-connection validation duration.
    pub fresh_validation_seconds: f64,
    /// Relation/TOAST/index/WAL footprint query duration.
    pub footprint_measurement_seconds: f64,
}

/// Complete domain output produced by [`run_postgres_segmented_benchmark`].
#[derive(Clone, Debug)]
pub struct PostgresSegmentedRunReport {
    /// Isolated `PostgreSQL` schema written and validated.
    pub schema_name: String,
    /// Schema format version (`2`).
    pub schema_version: u16,
    /// Target chain network.
    pub network: Network,
    /// Whether `ZSBLKB01` raw blocks were retained.
    pub full_blocks: bool,
    /// Settled segment byte cap used.
    pub max_segment_bytes: u64,
    /// Target settled segment span in blocks.
    pub settled_segment_blocks: u32,
    /// Upper bound on `{schema}.canonical_live_tail` rows.
    pub live_tail_max_blocks: u32,
    /// Declared reader pooling topology.
    pub reader_mode: ReaderConnectionMode,
    /// Configured number of concurrent readers during contended tip-follow.
    pub concurrent_readers: u32,
    /// Configured mempool churn entries per block.
    pub mempool_churn_txs_per_block: u32,
    /// Blocks loaded in initial bulk prefix.
    pub bulk_block_count: u64,
    /// Blocks committed incrementally during tip-following phases.
    pub tip_follow_block_count: u64,
    /// Total blocks persisted (`bulk_block_count + tip_follow_block_count`).
    pub total_block_count: u64,
    /// First persisted height.
    pub first_height: BlockHeight,
    /// Final persisted tip height.
    pub tip_height: BlockHeight,
    /// Final persisted tip block hash.
    pub tip_hash: BlockHash,
    /// Validated backend-neutral sequence digest hex over all committed blocks.
    pub persisted_sequence_digest_hex: String,
    /// Reconstructed and verified settled blocks from `ZSHRNG02` + `ZSTSUP01`.
    pub settled_blocks_verified: u64,
    /// Verified live-tail blocks from `canonical_live_tail`.
    pub live_tail_blocks_verified: u64,
    /// Verified raw consensus blocks from `ZSBLKB01`.
    pub raw_blocks_verified: u64,
    /// Observed `PostgreSQL` configuration and durability posture.
    pub server_settings: PostgresServerSettings,
    /// Phase timings.
    pub timings: PostgresSegmentedBuildTimings,
    /// Uncontended/contended tip-follow latencies and mempool/sealing totals.
    pub tip_follow: TipFollowRunMetrics,
    /// Concurrent pooled reader workload and streaming throughput metrics.
    pub reader_workload: ConcurrentReaderSummary,
    /// Reader transaction latency percentiles in milliseconds.
    pub reader_latencies_ms: LatencyPercentilesMillis,
    /// Non-double-counted relation/TOAST/index/WAL footprint.
    pub footprint: StorageFootprintSummary,
}

struct PreparedRunInputs {
    network: Network,
    manifest: FixtureManifest,
    manifest_sha256: String,
    chain: PreparedBenchmarkChain,
    prepare_duration: Duration,
    bulk_count: usize,
    tip_count: usize,
}

struct BulkSessionOutcome {
    writer_conn: PostgresConnection,
    server_settings: PostgresServerSettings,
    bulk_timings: BulkLoadTimings,
    initial_max_seg_bytes: u64,
    wal_after_bulk: WalLsn,
    bulk_wal_bytes: u64,
    wallet_projection: ProvisionalWalletProjection,
}

struct TipAndFootprintOutcome {
    uncontended_duration: Duration,
    contended_duration: Duration,
    footprint_duration: Duration,
    tip_follow: TipFollowRunMetrics,
    reader_workload: ConcurrentReaderSummary,
    reader_latencies_ms: LatencyPercentilesMillis,
    footprint: StorageFootprintSummary,
    wallet_utxo_rows: u64,
    wallet_address_tx_rows: u64,
}

struct ContendedPhaseContext<'a> {
    config: &'a PostgresSegmentedConfig,
    network: Network,
    tip_state: &'a mut TipFollowState,
    wallet_projection: &'a mut ProvisionalWalletProjection,
}

/// Runs the segmented `PostgreSQL` build, tip-following, mempool churn, pooled reader
/// workload, footprint measurement, and fresh-connection validation end-to-end.
pub async fn run_postgres_segmented_benchmark(
    config: PostgresSegmentedConfig,
) -> Result<PostgresSegmentedRunReport, BenchError> {
    let run_started = Instant::now();
    validate_run_config(&config)?;

    let prepared = load_and_split_fixture(&config).await?;
    let mut bulk = execute_writer_bulk_phase(&config, &prepared, prepared.bulk_count).await?;
    let tip_outcome = execute_tip_and_footprint_phase(&config, &prepared, &mut bulk).await?;

    let expectations = build_validation_expectations(&config, &prepared, &tip_outcome);
    let validation_url = config
        .reader_database_url
        .as_deref()
        .unwrap_or(&config.database_url);
    let (validation, fresh_val_duration) =
        run_fresh_reader_validation(validation_url, &expectations).await?;

    let first_block = prepared
        .chain
        .blocks
        .first()
        .ok_or_else(|| BenchError::fixture_format("fixture chain is empty"))?;
    let last_block = prepared
        .chain
        .blocks
        .last()
        .ok_or_else(|| BenchError::fixture_format("fixture chain is empty"))?;

    Ok(assemble_run_report(
        &config,
        &prepared,
        first_block.height,
        last_block,
        bulk.server_settings,
        PostgresSegmentedBuildTimings {
            wall_clock_seconds: run_started.elapsed().as_secs_f64(),
            prepare_seconds: prepared.prepare_duration.as_secs_f64(),
            seal_seconds: bulk.bulk_timings.seal_duration.as_secs_f64(),
            bulk_copy_seconds: bulk.bulk_timings.copy_duration.as_secs_f64(),
            finalize_seconds: bulk.bulk_timings.finalize_duration.as_secs_f64(),
            uncontended_tip_follow_seconds: tip_outcome.uncontended_duration.as_secs_f64(),
            contended_tip_follow_seconds: tip_outcome.contended_duration.as_secs_f64(),
            fresh_validation_seconds: fresh_val_duration.as_secs_f64(),
            footprint_measurement_seconds: tip_outcome.footprint_duration.as_secs_f64(),
        },
        tip_outcome,
        validation,
    ))
}

async fn load_and_split_fixture(
    config: &PostgresSegmentedConfig,
) -> Result<PreparedRunInputs, BenchError> {
    let manifest = FixtureManifest::read(&config.fixture_directory)?;
    let network = manifest.network_typed()?;
    let manifest_sha256 = manifest.digest_sha256()?;
    let (chain, prepare_duration) =
        prepare_all_fixture_blocks(&config.fixture_directory, &manifest, config.full_blocks)
            .await?;
    let tip_count = resolve_tip_follow_block_count(chain.blocks.len(), config.tip_follow_blocks)?;
    let bulk_count = chain.blocks.len() - tip_count;
    Ok(PreparedRunInputs {
        network,
        manifest,
        manifest_sha256,
        chain,
        prepare_duration,
        bulk_count,
        tip_count,
    })
}

async fn execute_writer_bulk_phase(
    config: &PostgresSegmentedConfig,
    prepared: &PreparedRunInputs,
    bulk_count: usize,
) -> Result<BulkSessionOutcome, BenchError> {
    let mut writer_conn =
        connect_postgres(&config.database_url, "connecting postgres writer").await?;
    reject_existing_schema(&writer_conn.client, &config.schema_name).await?;
    let server_settings = read_server_settings(&writer_conn.client).await?;
    validate_required_posture(&server_settings)?;

    let wal_start = measure_wal_lsn(&writer_conn.client).await?;
    let mut wallet_projection = ProvisionalWalletProjection::new();
    let bulk_blocks = &prepared.chain.blocks[..bulk_count];
    let seed_meta = SchemaSeedMetadata {
        schema_name: &config.schema_name,
        network: prepared.network,
        full_blocks: config.full_blocks,
        max_segment_bytes: config.max_segment_bytes,
        settled_segment_blocks: config.settled_segment_blocks,
        live_tail_max_blocks: config.live_tail_max_blocks,
        fixture_format_version: prepared.manifest.fixture_format_version,
        fixture_manifest_sha256: &prepared.manifest_sha256,
        expected_sequence_digest_hex: &prepared.chain.full_sequence_digest_hex,
    };
    let (bulk_timings, initial_max_seg_bytes) = load_bulk_prefix(
        &mut writer_conn.client,
        &seed_meta,
        bulk_blocks,
        &prepared.chain.subtree_roots,
        &mut wallet_projection,
    )
    .await?;
    let wal_after_bulk = measure_wal_lsn(&writer_conn.client).await?;
    let bulk_wal_bytes = wal_bytes_delta(&writer_conn.client, &wal_start, &wal_after_bulk).await?;

    Ok(BulkSessionOutcome {
        writer_conn,
        server_settings,
        bulk_timings,
        initial_max_seg_bytes,
        wal_after_bulk,
        bulk_wal_bytes,
        wallet_projection,
    })
}

async fn execute_tip_and_footprint_phase(
    config: &PostgresSegmentedConfig,
    prepared: &PreparedRunInputs,
    bulk: &mut BulkSessionOutcome,
) -> Result<TipAndFootprintOutcome, BenchError> {
    let (bulk_blocks, tip_blocks) = prepared.chain.blocks.split_at(prepared.bulk_count);
    let (uncontended_tips, contended_tips) = split_tip_follow_phases(tip_blocks);
    let first_block = prepared
        .chain
        .blocks
        .first()
        .ok_or_else(|| BenchError::fixture_format("fixture chain is empty"))?;
    let mut tip_state = TipFollowState::new(
        TipFollowParameters {
            network: prepared.network,
            schema_name: config.schema_name.clone(),
            first_height: first_block.height,
            full_blocks: config.full_blocks,
            max_segment_bytes: config.max_segment_bytes,
            settled_segment_blocks: config.settled_segment_blocks,
            live_tail_max_blocks: config.live_tail_max_blocks,
            mempool_churn_txs_per_block: config.mempool_churn_txs_per_block,
        },
        initial_tail_slice(bulk_blocks, config.live_tail_max_blocks),
        bulk.initial_max_seg_bytes,
    );

    let uncontended_started = Instant::now();
    let uncontended_stats = tip_state
        .advance_blocks(
            &mut bulk.writer_conn.client,
            uncontended_tips,
            &mut bulk.wallet_projection,
        )
        .await?;
    let uncontended_duration = uncontended_started.elapsed();

    let (contended_stats, contended_duration, reader_workload) = run_contended_phase(
        ContendedPhaseContext {
            config,
            network: prepared.network,
            tip_state: &mut tip_state,
            wallet_projection: &mut bulk.wallet_projection,
        },
        &mut bulk.writer_conn.client,
        contended_tips,
    )
    .await?;

    mark_schema_completed(&bulk.writer_conn.client, &config.schema_name).await?;
    let wal_end = measure_wal_lsn(&bulk.writer_conn.client).await?;
    let tip_follow_wal_bytes =
        wal_bytes_delta(&bulk.writer_conn.client, &bulk.wal_after_bulk, &wal_end).await?;

    let fp_started = Instant::now();
    let footprint = measure_storage_footprint(
        &bulk.writer_conn.client,
        &config.schema_name,
        bulk.bulk_wal_bytes,
        tip_follow_wal_bytes,
    )
    .await?;
    let footprint_duration = fp_started.elapsed();
    let wallet_utxo_rows =
        u64::try_from(bulk.wallet_projection.sorted_utxo_rows().len()).unwrap_or(0);
    let wallet_address_tx_rows =
        u64::try_from(bulk.wallet_projection.address_tx_rows().len()).unwrap_or(0);

    let reader_latencies_ms =
        LatencyPercentilesMillis::from_samples(&reader_workload.tx_latencies_ms);
    let tip_follow = tip_state.into_metrics(uncontended_stats, contended_stats);

    Ok(TipAndFootprintOutcome {
        uncontended_duration,
        contended_duration,
        footprint_duration,
        tip_follow,
        reader_workload,
        reader_latencies_ms,
        footprint,
        wallet_utxo_rows,
        wallet_address_tx_rows,
    })
}

fn build_validation_expectations(
    config: &PostgresSegmentedConfig,
    prepared: &PreparedRunInputs,
    tip_outcome: &TipAndFootprintOutcome,
) -> ValidationExpectations {
    let first_height = prepared
        .chain
        .blocks
        .first()
        .map_or(BlockHeight::new(0), |b| b.height);
    let (tip_height, tip_hash) = prepared.chain.blocks.last().map_or(
        (BlockHeight::new(0), BlockHash::from_bytes([0_u8; 32])),
        |b| (b.height, b.block_hash),
    );
    ValidationExpectations {
        schema_name: config.schema_name.clone(),
        network: prepared.network,
        full_blocks: config.full_blocks,
        max_segment_bytes: config.max_segment_bytes,
        live_tail_max_blocks: config.live_tail_max_blocks,
        expected_first_height: first_height,
        expected_tip_height: tip_height,
        expected_tip_hash: tip_hash,
        expected_sequence_digest_hex: prepared.chain.full_sequence_digest_hex.clone(),
        expected_transaction_location_rows: prepared.chain.total_tx_count,
        expected_block_hash_index_rows: u64::try_from(prepared.chain.blocks.len()).unwrap_or(0),
        expected_tree_state_checkpoint_rows: prepared.chain.total_checkpoint_count,
        expected_subtree_root_rows: u64::try_from(prepared.chain.subtree_roots.len()).unwrap_or(0),
        expected_wallet_utxo_rows: tip_outcome.wallet_utxo_rows,
        expected_wallet_address_tx_rows: tip_outcome.wallet_address_tx_rows,
    }
}

fn validate_run_config(config: &PostgresSegmentedConfig) -> Result<(), BenchError> {
    validate_schema_name(&config.schema_name)?;
    if config.max_segment_bytes == 0 || config.max_segment_bytes > MAX_SETTLED_SEGMENT_BYTES {
        return Err(BenchError::invalid_argument(format!(
            "--max-segment-bytes must be within 1..={MAX_SETTLED_SEGMENT_BYTES}"
        )));
    }
    if config.settled_segment_blocks == 0 {
        return Err(BenchError::invalid_argument(
            "--settled-segment-blocks must be at least 1",
        ));
    }
    if config.live_tail_max_blocks == 0 {
        return Err(BenchError::invalid_argument(
            "--live-tail-max-blocks must be at least 1",
        ));
    }
    if config.reader_database_url.is_none()
        && config.reader_mode == ReaderConnectionMode::PgbouncerTransaction
    {
        return Err(BenchError::invalid_argument(
            "--reader-mode pgbouncer-transaction requires --reader-database-url-env or distinct reader URL",
        ));
    }
    Ok(())
}

fn resolve_tip_follow_block_count(
    total_blocks: usize,
    explicit: Option<u32>,
) -> Result<usize, BenchError> {
    if total_blocks < 2 {
        return Err(BenchError::invalid_argument(
            "postgres-segmented requires a fixture with at least 2 blocks",
        ));
    }
    match explicit {
        Some(count) => {
            let count_usize = usize::try_from(count)
                .map_err(|_| BenchError::invalid_argument("--tip-follow-blocks exceeds usize"))?;
            if count_usize >= total_blocks {
                return Err(BenchError::invalid_argument(format!(
                    "--tip-follow-blocks ({count_usize}) must leave at least 1 bulk block from {total_blocks} total blocks"
                )));
            }
            Ok(count_usize)
        }
        None if total_blocks >= 64 => Ok(32),
        None => Ok((total_blocks / 3).max(1)),
    }
}

fn split_tip_follow_phases(
    tip_blocks: &[PreparedSegmentedBlock],
) -> (&[PreparedSegmentedBlock], &[PreparedSegmentedBlock]) {
    if tip_blocks.len() <= 1 {
        return (tip_blocks, &[]);
    }
    tip_blocks.split_at(tip_blocks.len().div_ceil(2))
}

fn initial_tail_slice(
    bulk_blocks: &[PreparedSegmentedBlock],
    live_tail_max_blocks: u32,
) -> Vec<PreparedSegmentedBlock> {
    let max_tail = usize::try_from(live_tail_max_blocks).unwrap_or(128);
    let settled_count = bulk_blocks.len().saturating_sub(max_tail);
    bulk_blocks[settled_count..].to_vec()
}

async fn run_contended_phase(
    ctx: ContendedPhaseContext<'_>,
    writer_client: &mut tokio_postgres::Client,
    contended_tips: &[PreparedSegmentedBlock],
) -> Result<(LatencyPercentilesMillis, Duration, ConcurrentReaderSummary), BenchError> {
    if contended_tips.is_empty() && ctx.config.concurrent_readers == 0 {
        return Ok((
            LatencyPercentilesMillis::default(),
            Duration::ZERO,
            ConcurrentReaderSummary::default(),
        ));
    }
    let reader_url = ctx
        .config
        .reader_database_url
        .as_deref()
        .unwrap_or(&ctx.config.database_url);
    let started = Instant::now();
    let pool = if ctx.config.concurrent_readers > 0 {
        Some(
            spawn_concurrent_readers(ConcurrentReaderPoolConfig {
                database_url: reader_url.to_owned(),
                schema_name: ctx.config.schema_name.clone(),
                reader_count: ctx.config.concurrent_readers,
                network: ctx.network,
                connection_mode: ctx.config.reader_mode,
            })
            .await?,
        )
    } else {
        None
    };

    let commit_outcome = ctx
        .tip_state
        .advance_blocks(writer_client, contended_tips, ctx.wallet_projection)
        .await;
    let reader_outcome = match pool {
        Some(active_pool) => Some(active_pool.stop_and_collect().await),
        None => None,
    };

    let stats = commit_outcome?;
    let summary = match reader_outcome {
        Some(res) => res?,
        None => ConcurrentReaderSummary::default(),
    };
    Ok((stats, started.elapsed(), summary))
}

async fn run_fresh_reader_validation(
    database_url: &str,
    expectations: &ValidationExpectations,
) -> Result<(PersistedValidationSummary, Duration), BenchError> {
    let started = Instant::now();
    let conn = connect_postgres(database_url, "connecting fresh validation reader").await?;
    let validation = validate_persisted_state_read_only(&conn.client, expectations).await;
    let close_res = close_postgres(conn).await;
    let summary = validation?;
    close_res?;
    Ok((summary, started.elapsed()))
}

#[allow(
    clippy::too_many_arguments,
    reason = "final assembly helper packages immutable completed stage outputs into report struct"
)]
fn assemble_run_report(
    config: &PostgresSegmentedConfig,
    prepared: &PreparedRunInputs,
    first_height: BlockHeight,
    last_block: &PreparedSegmentedBlock,
    server_settings: PostgresServerSettings,
    timings: PostgresSegmentedBuildTimings,
    tip_outcome: TipAndFootprintOutcome,
    validation: PersistedValidationSummary,
) -> PostgresSegmentedRunReport {
    PostgresSegmentedRunReport {
        schema_name: config.schema_name.clone(),
        schema_version: schema::POSTGRES_SEGMENTED_SCHEMA_VERSION,
        network: prepared.network,
        full_blocks: config.full_blocks,
        max_segment_bytes: config.max_segment_bytes,
        settled_segment_blocks: config.settled_segment_blocks,
        live_tail_max_blocks: config.live_tail_max_blocks,
        reader_mode: config.reader_mode,
        concurrent_readers: config.concurrent_readers,
        mempool_churn_txs_per_block: config.mempool_churn_txs_per_block,
        bulk_block_count: u64::try_from(prepared.bulk_count).unwrap_or(0),
        tip_follow_block_count: u64::try_from(prepared.tip_count).unwrap_or(0),
        total_block_count: u64::try_from(prepared.chain.blocks.len()).unwrap_or(0),
        first_height,
        tip_height: last_block.height,
        tip_hash: last_block.block_hash,
        persisted_sequence_digest_hex: validation.observed_sequence_digest_hex,
        settled_blocks_verified: validation.settled_blocks_verified,
        live_tail_blocks_verified: validation.live_tail_blocks_verified,
        raw_blocks_verified: validation.raw_blocks_verified,
        server_settings,
        timings,
        tip_follow: tip_outcome.tip_follow,
        reader_workload: tip_outcome.reader_workload,
        reader_latencies_ms: tip_outcome.reader_latencies_ms,
        footprint: tip_outcome.footprint,
    }
}
