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
use crate::fixture::{FixtureManifest, FixtureNodeSource};

use database::{
    PostgresConnection, WalLsn, close_postgres, connect_postgres, measure_wal_lsn,
    read_server_settings, reject_existing_schema, validate_required_posture, wal_bytes_delta,
};
use loader::{
    BulkStreamConfig, BulkStreamOutcome, WriterFenceHandle, execute_streaming_bulk_load,
    mark_schema_completed,
};
use reader::{
    ConcurrentReaderPoolConfig, ConcurrentReaderSummary, PersistedValidationSummary,
    ReaderConnectionMode, ValidationExpectations, spawn_concurrent_readers,
    validate_persisted_state_read_only,
};
use schema::{BENCHMARK_LEASE_DURATION_MS, MAX_SETTLED_SEGMENT_BYTES, validate_schema_name};
use storage_footprint::{StorageFootprintSummary, measure_storage_footprint};
use tip_follow::{
    LatencyPercentilesMillis, TipAdvanceTarget, TipFollowParameters, TipFollowRunMetrics,
    TipFollowState,
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
    /// Bulk-prefix fixture parse, canonical construction, and replay encoding duration.
    pub prepare_seconds: f64,
    /// Tip-follow block fetch, canonical preparation, and replay encoding duration.
    pub tip_prepare_seconds: f64,
    /// Schema and table creation DDL duration.
    pub schema_create_seconds: f64,
    /// Provisional wallet projection construction and final UTXO sort duration.
    pub wallet_projection_seconds: f64,
    /// Settled segment sealing duration (`ZSHRNG02`, `ZSTSUP01`, optional `ZSBLKB01`).
    pub seal_seconds: f64,
    /// Binary `COPY` persistence duration for bulk tables.
    pub bulk_copy_seconds: f64,
    /// PK/index construction, `ANALYZE`, and post-finalize writer-term acquisition duration.
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

struct PreparedSourceInputs {
    network: Network,
    manifest: FixtureManifest,
    manifest_sha256: String,
    source: FixtureNodeSource,
    source_open_duration: Duration,
    bulk_count: usize,
    tip_count: usize,
}

struct BulkSessionOutcome {
    writer_conn: PostgresConnection,
    server_settings: PostgresServerSettings,
    bulk_stream: BulkStreamOutcome,
    wal_after_bulk: WalLsn,
    bulk_wal_bytes: u64,
    wallet_projection: ProvisionalWalletProjection,
}

struct TipAndFootprintOutcome {
    uncontended_duration: Duration,
    contended_duration: Duration,
    footprint_duration: Duration,
    extra_prepare_duration: Duration,
    tip_follow: TipFollowRunMetrics,
    reader_workload: ConcurrentReaderSummary,
    reader_latencies_ms: LatencyPercentilesMillis,
    footprint: StorageFootprintSummary,
    first_height: BlockHeight,
    tip_height: BlockHeight,
    tip_hash: BlockHash,
    oracle_sequence_digest_hex: String,
    total_tx_count: u64,
    total_checkpoint_count: u64,
    subtree_root_count: u64,
    wallet_utxo_rows: u64,
    wallet_address_tx_rows: u64,
}

struct ContendedPhaseContext<'a> {
    config: &'a PostgresSegmentedConfig,
    network: Network,
    tip_state: &'a mut TipFollowState,
    advance_target: &'a mut TipAdvanceTarget<'a>,
}

/// Runs the segmented `PostgreSQL` build, tip-following, mempool churn, pooled reader
/// workload, footprint measurement, and fresh-connection validation end-to-end.
pub async fn run_postgres_segmented_benchmark(
    config: PostgresSegmentedConfig,
) -> Result<PostgresSegmentedRunReport, BenchError> {
    let run_started = Instant::now();
    validate_run_config(&config)?;

    let prepared = open_streaming_source(&config).await?;
    let mut bulk = execute_writer_bulk_phase(&config, &prepared).await?;
    let tip_outcome = execute_tip_and_footprint_phase(&config, &prepared, &mut bulk).await?;

    let expectations = build_validation_expectations(&config, &prepared, &tip_outcome);
    let validation_url = config
        .reader_database_url
        .as_deref()
        .unwrap_or(&config.database_url);
    let (validation, fresh_val_duration) =
        run_fresh_reader_validation(validation_url, &expectations).await?;

    Ok(assemble_run_report(
        &config,
        &prepared,
        bulk.server_settings,
        &bulk.bulk_stream,
        run_started.elapsed(),
        fresh_val_duration,
        tip_outcome,
        validation,
    ))
}

async fn open_streaming_source(
    config: &PostgresSegmentedConfig,
) -> Result<PreparedSourceInputs, BenchError> {
    let manifest = FixtureManifest::read(&config.fixture_directory)?;
    let network = manifest.network_typed()?;
    let manifest_sha256 = manifest.digest_sha256()?;
    let total_blocks = usize::try_from(manifest.block_count)
        .map_err(|_| BenchError::fixture_format("manifest.block_count exceeds usize"))?;
    let tip_count = resolve_tip_follow_block_count(total_blocks, config.tip_follow_blocks)?;
    if tip_count < 2 && config.concurrent_readers > 0 {
        return Err(BenchError::invalid_argument(
            "--concurrent-readers must be 0 when fewer than 2 tip-follow blocks are configured (no contended tip-follow phase)",
        ));
    }
    let bulk_count = total_blocks - tip_count;

    let dir = config.fixture_directory.clone();
    let manifest_for_open = manifest.clone();
    let started = Instant::now();
    let source =
        tokio::task::spawn_blocking(move || FixtureNodeSource::open(&dir, &manifest_for_open))
            .await
            .map_err(|err| {
                database::candidate_error(format!("opening streaming fixture source: {err}"))
            })??;
    let source_open_duration = started.elapsed();

    Ok(PreparedSourceInputs {
        network,
        manifest,
        manifest_sha256,
        source,
        source_open_duration,
        bulk_count,
        tip_count,
    })
}

async fn execute_writer_bulk_phase(
    config: &PostgresSegmentedConfig,
    prepared: &PreparedSourceInputs,
) -> Result<BulkSessionOutcome, BenchError> {
    let mut writer_conn =
        connect_postgres(&config.database_url, "connecting postgres writer").await?;
    reject_existing_schema(&writer_conn.client, &config.schema_name).await?;
    let server_settings = read_server_settings(&writer_conn.client).await?;
    validate_required_posture(&server_settings)?;

    let wal_start = measure_wal_lsn(&writer_conn.client).await?;
    let mut wallet_projection = ProvisionalWalletProjection::new();
    let stream_cfg = BulkStreamConfig {
        manifest: &prepared.manifest,
        manifest_sha256: &prepared.manifest_sha256,
        schema_name: &config.schema_name,
        full_blocks: config.full_blocks,
        max_segment_bytes: config.max_segment_bytes,
        settled_segment_blocks: config.settled_segment_blocks,
        live_tail_max_blocks: config.live_tail_max_blocks,
        bulk_block_count: prepared.bulk_count,
        seed_lease_duration_ms: BENCHMARK_LEASE_DURATION_MS,
        post_finalize_lease_duration_ms: BENCHMARK_LEASE_DURATION_MS,
    };
    let bulk_stream = execute_streaming_bulk_load(
        &mut writer_conn.client,
        &prepared.source,
        &stream_cfg,
        &mut wallet_projection,
    )
    .await?;
    let wal_after_bulk = measure_wal_lsn(&writer_conn.client).await?;
    let bulk_wal_bytes = wal_bytes_delta(&writer_conn.client, &wal_start, &wal_after_bulk).await?;

    Ok(BulkSessionOutcome {
        writer_conn,
        server_settings,
        bulk_stream,
        wal_after_bulk,
        bulk_wal_bytes,
        wallet_projection,
    })
}

async fn execute_tip_and_footprint_phase(
    config: &PostgresSegmentedConfig,
    prepared: &PreparedSourceInputs,
    bulk: &mut BulkSessionOutcome,
) -> Result<TipAndFootprintOutcome, BenchError> {
    let initial_tail = std::mem::take(&mut bulk.bulk_stream.initial_tail);
    let first_height = BlockHeight::new(prepared.manifest.from_height);
    let mut tip_state = TipFollowState::new(
        TipFollowParameters {
            network: prepared.network,
            schema_name: config.schema_name.clone(),
            fence: WriterFenceHandle {
                writer_epoch: bulk.bulk_stream.writer_epoch,
                lease_duration_ms: BENCHMARK_LEASE_DURATION_MS,
            },
            first_height,
            full_blocks: config.full_blocks,
            max_segment_bytes: config.max_segment_bytes,
            settled_segment_blocks: config.settled_segment_blocks,
            live_tail_max_blocks: config.live_tail_max_blocks,
            mempool_churn_txs_per_block: config.mempool_churn_txs_per_block,
        },
        initial_tail,
        bulk.bulk_stream.initial_max_segment_bytes,
    );

    let (uncontended_range, contended_range) =
        split_tip_follow_heights(&prepared.manifest, prepared.bulk_count, prepared.tip_count)?;

    let mut advance_target = TipAdvanceTarget {
        source: &prepared.source,
        prepare_state: &mut bulk.bulk_stream.prepare_state,
        wallet_projection: &mut bulk.wallet_projection,
    };

    let uncontended_started = Instant::now();
    let uncontended_stats = tip_state
        .advance_height_range(
            &mut bulk.writer_conn.client,
            &mut advance_target,
            uncontended_range,
        )
        .await?;
    let uncontended_duration = uncontended_started.elapsed();

    let (contended_stats, contended_duration, reader_workload) = run_contended_phase(
        ContendedPhaseContext {
            config,
            network: prepared.network,
            tip_state: &mut tip_state,
            advance_target: &mut advance_target,
        },
        &mut bulk.writer_conn.client,
        contended_range,
    )
    .await?;

    mark_schema_completed(&bulk.writer_conn.client, &config.schema_name).await?;
    measure_final_tip_and_footprint(
        config,
        bulk,
        (uncontended_duration, contended_duration),
        tip_state,
        (uncontended_stats, contended_stats),
        reader_workload,
        first_height,
    )
    .await
}

type TipPhaseHeightRanges = (Option<(u32, u32)>, Option<(u32, u32)>);

#[allow(
    clippy::too_many_arguments,
    reason = "packages finished tip-follow metrics and queries final WAL/relation footprint"
)]
async fn measure_final_tip_and_footprint(
    config: &PostgresSegmentedConfig,
    bulk: &BulkSessionOutcome,
    durations: (Duration, Duration),
    tip_state: TipFollowState,
    latencies: (LatencyPercentilesMillis, LatencyPercentilesMillis),
    reader_workload: ConcurrentReaderSummary,
    first_height: BlockHeight,
) -> Result<TipAndFootprintOutcome, BenchError> {
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
    let extra_prepare_duration = tip_state.extra_prepare_duration();
    let reader_latencies_ms =
        LatencyPercentilesMillis::from_samples(&reader_workload.tx_latencies_ms);
    let tip_follow = tip_state.into_metrics(latencies.0, latencies.1);

    let (tip_height, tip_hash, _) =
        bulk.bulk_stream
            .prepare_state
            .last_block_meta
            .ok_or_else(|| {
                database::candidate_error("missing tip block identity after benchmark completion")
            })?;
    let oracle_sequence_digest_hex = hex::encode(
        bulk.bulk_stream
            .prepare_state
            .sequence_builder
            .clone()
            .finish()
            .as_bytes(),
    );

    Ok(TipAndFootprintOutcome {
        uncontended_duration: durations.0,
        contended_duration: durations.1,
        footprint_duration,
        extra_prepare_duration,
        tip_follow,
        reader_workload,
        reader_latencies_ms,
        footprint,
        first_height,
        tip_height,
        tip_hash,
        oracle_sequence_digest_hex,
        total_tx_count: bulk.bulk_stream.prepare_state.total_tx_count,
        total_checkpoint_count: bulk.bulk_stream.prepare_state.total_checkpoint_count,
        subtree_root_count: bulk.bulk_stream.subtree_root_count,
        wallet_utxo_rows: bulk.wallet_projection.utxo_count(),
        wallet_address_tx_rows: bulk.wallet_projection.address_tx_count(),
    })
}

fn split_tip_follow_heights(
    manifest: &FixtureManifest,
    bulk_count: usize,
    tip_count: usize,
) -> Result<TipPhaseHeightRanges, BenchError> {
    if tip_count == 0 {
        return Ok((None, None));
    }
    let bulk_u32 = u32::try_from(bulk_count)
        .map_err(|_| BenchError::invalid_argument("bulk_count exceeds u32"))?;
    let tip_u32 = u32::try_from(tip_count)
        .map_err(|_| BenchError::invalid_argument("tip_count exceeds u32"))?;
    let tip_start = manifest.from_height.saturating_add(bulk_u32);
    let tip_end = tip_start.saturating_add(tip_u32 - 1);
    if tip_u32 == 1 {
        return Ok((Some((tip_start, tip_start)), None));
    }
    let first_half_len = tip_u32.div_ceil(2);
    let first_end = tip_start.saturating_add(first_half_len - 1);
    let second_start = first_end.saturating_add(1);
    Ok((Some((tip_start, first_end)), Some((second_start, tip_end))))
}

fn build_validation_expectations(
    config: &PostgresSegmentedConfig,
    prepared: &PreparedSourceInputs,
    tip_outcome: &TipAndFootprintOutcome,
) -> ValidationExpectations {
    ValidationExpectations {
        schema_name: config.schema_name.clone(),
        network: prepared.network,
        full_blocks: config.full_blocks,
        max_segment_bytes: config.max_segment_bytes,
        live_tail_max_blocks: config.live_tail_max_blocks,
        expected_first_height: tip_outcome.first_height,
        expected_tip_height: tip_outcome.tip_height,
        expected_tip_hash: tip_outcome.tip_hash,
        expected_sequence_digest_hex: tip_outcome.oracle_sequence_digest_hex.clone(),
        expected_transaction_location_rows: tip_outcome.total_tx_count,
        expected_block_hash_index_rows: u64::from(prepared.manifest.block_count),
        expected_tree_state_checkpoint_rows: tip_outcome.total_checkpoint_count,
        expected_subtree_root_rows: tip_outcome.subtree_root_count,
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

async fn run_contended_phase(
    ctx: ContendedPhaseContext<'_>,
    writer_client: &mut tokio_postgres::Client,
    contended_range: Option<(u32, u32)>,
) -> Result<(LatencyPercentilesMillis, Duration, ConcurrentReaderSummary), BenchError> {
    if contended_range.is_none() {
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
    let mut pool = if ctx.config.concurrent_readers > 0 {
        Some(
            spawn_concurrent_readers(ConcurrentReaderPoolConfig {
                database_url: reader_url.to_owned(),
                schema_name: ctx.config.schema_name.clone(),
                reader_count: ctx.config.concurrent_readers,
                network: ctx.network,
                connection_mode: ctx.config.reader_mode,
                live_tail_max_blocks: ctx.config.live_tail_max_blocks,
            })
            .await?,
        )
    } else {
        None
    };

    if let Some(ref mut active_pool) = pool
        && let Err(err) = active_pool.begin_writer_phase().await
    {
        if let Some(failed_pool) = pool {
            let _ = failed_pool.stop_and_collect().await?;
        }
        return Err(err);
    }

    let started = Instant::now();
    let commit_outcome = ctx
        .tip_state
        .advance_height_range(writer_client, ctx.advance_target, contended_range)
        .await;
    let contended_elapsed = started.elapsed();
    let reader_outcome = match pool {
        Some(active_pool) => Some(active_pool.stop_and_collect().await),
        None => None,
    };

    let stats = commit_outcome?;
    let summary = match reader_outcome {
        Some(res) => res?,
        None => ConcurrentReaderSummary::default(),
    };
    Ok((stats, contended_elapsed, summary))
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
    prepared: &PreparedSourceInputs,
    server_settings: PostgresServerSettings,
    bulk_stream: &BulkStreamOutcome,
    wall_clock_duration: Duration,
    fresh_val_duration: Duration,
    tip_outcome: TipAndFootprintOutcome,
    validation: PersistedValidationSummary,
) -> PostgresSegmentedRunReport {
    let bulk_prepare =
        (prepared.source_open_duration + bulk_stream.timings.prepare_duration).as_secs_f64();
    let tip_prepare = tip_outcome.extra_prepare_duration.as_secs_f64();
    let timings = PostgresSegmentedBuildTimings {
        wall_clock_seconds: wall_clock_duration.as_secs_f64(),
        prepare_seconds: bulk_prepare,
        tip_prepare_seconds: tip_prepare,
        schema_create_seconds: bulk_stream.timings.schema_create_duration.as_secs_f64(),
        wallet_projection_seconds: bulk_stream.timings.wallet_projection_duration.as_secs_f64(),
        seal_seconds: bulk_stream.timings.seal_duration.as_secs_f64(),
        bulk_copy_seconds: bulk_stream.timings.copy_duration.as_secs_f64(),
        finalize_seconds: bulk_stream.timings.finalize_duration.as_secs_f64(),
        uncontended_tip_follow_seconds: tip_outcome.uncontended_duration.as_secs_f64(),
        contended_tip_follow_seconds: tip_outcome.contended_duration.as_secs_f64(),
        fresh_validation_seconds: fresh_val_duration.as_secs_f64(),
        footprint_measurement_seconds: tip_outcome.footprint_duration.as_secs_f64(),
    };
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
        total_block_count: u64::from(prepared.manifest.block_count),
        first_height: tip_outcome.first_height,
        tip_height: tip_outcome.tip_height,
        tip_hash: tip_outcome.tip_hash,
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
