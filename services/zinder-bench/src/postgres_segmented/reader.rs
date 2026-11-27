//! Pooler-safe concurrent PostgreSQL readers and fresh-connection end-to-end
//! oracle validation using positioned `substring()` reads and Zinder segment codecs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, IsolationLevel, Transaction};
use zinder_core::wire::{decode_internal_block_hash, decode_zinder_native_chain_name};
use zinder_core::{
    BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId,
    CanonicalBlockFactsDigestVersion, CanonicalBlockFactsSequenceDigestBuilder,
    CanonicalBlockFactsSequenceDigestVersion, CompactBlockArtifact, Network,
    decode_canonical_block_replay,
};
use zinder_store::{
    ByteRange, CURRENT_ARTIFACT_SCHEMA_VERSION, HeightOrder, RawBlockCodec, RawBlockRowIndexSlice,
    RawBlockSegmentHeader, RawBlockSegmentIdentity, SettledRawBlockSegmentIndex,
    SettledShieldedSegmentIndex, SettledSupplementSegmentIndex, ShieldedFetchedSlab,
    ShieldedIndexSlice, ShieldedProjectionSelection, ShieldedSegmentHeader,
    ShieldedSegmentIdentity, SupplementRowIndexSlice, SupplementSegmentHeader,
    SupplementSegmentIdentity, decode_compact_block_artifact, reconstruct_canonical_block_facts,
    reconstruct_compact_block_artifact,
};

use crate::error::BenchError;

use super::database::{
    candidate_error, close_postgres, connect_postgres, i32_to_u32, i64_to_u32, i64_to_u64,
    postgres_error, slice_to_32_bytes, substring_bounds, u32_to_i64,
};
use super::schema::{
    POSTGRES_SEGMENTED_CONTRACT_IDENTITY, POSTGRES_SEGMENTED_SCHEMA_VERSION, validate_schema_name,
};

const SEGMENT_HEADER_LEN: usize = 128;
const READER_RANGE_WINDOW_BLOCKS: u32 = 16;

/// Pooler topology declared for concurrent reader connections.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReaderConnectionMode {
    /// Readers connect directly to PostgreSQL without an external transaction pooler.
    #[default]
    Direct,
    /// Readers connect through a transaction-mode pooler such as `PgBouncer` (`pool_mode=transaction`).
    PgbouncerTransaction,
}

impl ReaderConnectionMode {
    /// Stable lowercase identifier stamped into benchmark reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::PgbouncerTransaction => "pgbouncer-transaction",
        }
    }
}

/// Configuration shared by concurrent reader workers.
#[derive(Clone)]
pub struct ConcurrentReaderPoolConfig {
    /// `PostgreSQL` or `PgBouncer` connection string used exclusively for reads.
    pub database_url: String,
    /// Validated schema name holding benchmark tables.
    pub schema_name: String,
    /// Number of concurrent reader tasks to spawn.
    pub reader_count: u32,
    /// Expected chain network name matching `{schema}.schema_meta`.
    pub network: Network,
    /// Declared connection mode for observability.
    pub connection_mode: ReaderConnectionMode,
    /// Maximum allowed rows in `{schema}.canonical_live_tail` checked on each snapshot.
    pub live_tail_max_blocks: u32,
}

#[derive(Clone)]
struct ReaderLoopParams {
    schema_name: String,
    network: Network,
    worker_index: u32,
    live_tail_max_blocks: u32,
}

/// Streaming throughput and activity metrics observed by one reader worker.
#[derive(Clone, Debug, Default)]
pub struct ReaderWorkerThroughput {
    /// Zero-based reader worker ordinal.
    pub worker_index: u32,
    /// Completed read-only transactions.
    pub completed_transactions: u64,
    /// Verified settled compact-block range decodes.
    pub settled_range_reads: u64,
    /// Total compact blocks decoded across settled segments and live tail.
    pub blocks_decoded: u64,
    /// Total payload bytes read over `substring()` and live-tail reads.
    pub bytes_fetched: u64,
    /// Active wall-clock time spent reading by this worker.
    pub elapsed_seconds: f64,
    /// Mean blocks per second streamed by this individual reader.
    pub blocks_per_second: f64,
    /// Mean payload bytes per second streamed by this individual reader.
    pub bytes_per_second: f64,
    /// Individual transaction latencies collected by this worker.
    pub tx_latencies_ms: Vec<f64>,
}

/// Aggregated concurrent reader execution metrics collected across all workers.
#[derive(Clone, Debug, Default)]
pub struct ConcurrentReaderSummary {
    /// Total read-only transactions completed cleanly.
    pub completed_transactions: u64,
    /// Settled shielded range decodes served via positioned `substring()`.
    pub settled_range_reads: u64,
    /// Individual compact blocks decoded across settled and live-tail reads.
    pub blocks_decoded: u64,
    /// Exact payload bytes pulled over `substring()` and live-tail queries.
    pub bytes_fetched: u64,
    /// Per-reader mean streaming throughput in blocks/sec.
    pub per_reader_blocks_per_second_mean: f64,
    /// Per-reader mean streaming throughput in bytes/sec.
    pub per_reader_bytes_per_second_mean: f64,
    /// Aggregate blocks/sec across concurrent readers during contended window.
    pub aggregate_blocks_per_second: f64,
    /// Aggregate bytes/sec across concurrent readers during contended window.
    pub aggregate_bytes_per_second: f64,
    /// Per-transaction latencies in milliseconds across all workers.
    pub tx_latencies_ms: Vec<f64>,
    /// Individual worker summaries.
    pub per_reader_stats: Vec<ReaderWorkerThroughput>,
}

/// Expected persisted counts and digests checked by `validate_persisted_state_read_only`.
pub struct ValidationExpectations {
    /// Schema name to inspect.
    pub schema_name: String,
    /// Expected Zcash network.
    pub network: Network,
    /// Expected full-block retention flag (`ZSBLKB01` presence).
    pub full_blocks: bool,
    /// Expected maximum settled segment byte cap.
    pub max_segment_bytes: u64,
    /// Expected live-tail upper bound in blocks.
    pub live_tail_max_blocks: u32,
    /// Expected first committed block height.
    pub expected_first_height: BlockHeight,
    /// Expected highest committed block height.
    pub expected_tip_height: BlockHeight,
    /// Expected highest committed block hash.
    pub expected_tip_hash: BlockHash,
    /// Expected canonical sequence digest hex over all blocks.
    pub expected_sequence_digest_hex: String,
    /// Expected row count in `{schema}.transaction_location`.
    pub expected_transaction_location_rows: u64,
    /// Expected row count in `{schema}.block_hash_index`.
    pub expected_block_hash_index_rows: u64,
    /// Expected row count in `{schema}.tree_state_checkpoint`.
    pub expected_tree_state_checkpoint_rows: u64,
    /// Expected row count in `{schema}.subtree_root`.
    pub expected_subtree_root_rows: u64,
    /// Expected UTXO row count in `{schema}.wallet_utxo`.
    pub expected_wallet_utxo_rows: u64,
    /// Expected address-activity row count in `{schema}.wallet_address_tx`.
    pub expected_wallet_address_tx_rows: u64,
}

/// Validated metadata and block counts observed on a fresh connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedValidationSummary {
    /// Confirmed canonical sequence digest hex matching `schema_meta`.
    pub observed_sequence_digest_hex: String,
    /// Number of settled blocks reconstructed from `ZSHRNG02` + `ZSTSUP01`.
    pub settled_blocks_verified: u64,
    /// Number of live-tail blocks verified from `canonical_live_tail`.
    pub live_tail_blocks_verified: u64,
    /// Number of settled raw blocks verified from `ZSBLKB01`.
    pub raw_blocks_verified: u64,
}

struct SegmentRowDescriptor {
    start_height: BlockHeight,
    end_height: BlockHeight,
    artifact_schema_version: u16,
    segment_sha256: [u8; 32],
    index_sha256: [u8; 32],
    segment_byte_len: u64,
    header_bytes: [u8; SEGMENT_HEADER_LEN],
}

struct ValidationCursor {
    sequence_builder: CanonicalBlockFactsSequenceDigestBuilder,
    prior_hash: Option<BlockHash>,
}

/// Active reader pool handle stopped cooperatively at the end of contended tip-follow.
pub struct RunningReaderPool {
    stop_signal: Arc<AtomicBool>,
    started_at: Instant,
    workers: Vec<JoinHandle<Result<ReaderWorkerThroughput, BenchError>>>,
}

impl RunningReaderPool {
    /// Signals all concurrent reader tasks to stop and aggregates throughput + latency metrics.
    pub async fn stop_and_collect(self) -> Result<ConcurrentReaderSummary, BenchError> {
        self.stop_signal.store(true, Ordering::Release);
        let wall_secs = self.started_at.elapsed().as_secs_f64().max(1e-6);
        let mut combined = ConcurrentReaderSummary::default();

        for worker in self.workers {
            let stats = worker.await.map_err(|join_err| {
                candidate_error(format!("concurrent reader worker panicked: {join_err}"))
            })??;
            combined.completed_transactions = combined
                .completed_transactions
                .saturating_add(stats.completed_transactions);
            combined.settled_range_reads = combined
                .settled_range_reads
                .saturating_add(stats.settled_range_reads);
            combined.blocks_decoded = combined.blocks_decoded.saturating_add(stats.blocks_decoded);
            combined.bytes_fetched = combined.bytes_fetched.saturating_add(stats.bytes_fetched);
            combined
                .tx_latencies_ms
                .extend(stats.tx_latencies_ms.iter().copied());
            combined.per_reader_stats.push(stats);
        }

        if !combined.per_reader_stats.is_empty() {
            let count = f64::from(u32::try_from(combined.per_reader_stats.len()).unwrap_or(1));
            let blocks_rate_sum: f64 = combined
                .per_reader_stats
                .iter()
                .map(|w| w.blocks_per_second)
                .sum();
            let bytes_rate_sum: f64 = combined
                .per_reader_stats
                .iter()
                .map(|w| w.bytes_per_second)
                .sum();
            combined.per_reader_blocks_per_second_mean = blocks_rate_sum / count;
            combined.per_reader_bytes_per_second_mean = bytes_rate_sum / count;
        }
        let total_blocks = u64_to_f64_lossy(combined.blocks_decoded);
        let total_bytes = u64_to_f64_lossy(combined.bytes_fetched);
        combined.aggregate_blocks_per_second = total_blocks / wall_secs;
        combined.aggregate_bytes_per_second = total_bytes / wall_secs;
        Ok(combined)
    }
}

/// Spawns `config.reader_count` independent read-only workers targeting `config.database_url`.
///
/// Readers use transaction-local `REPEATABLE READ READ ONLY` blocks, execute zero
/// session-pinned SQL commands (`SET`, advisory locks, temp tables, `LISTEN`),
/// and stream/decode blocks from `canonical_settled_segment` + `canonical_live_tail`.
pub async fn spawn_concurrent_readers(
    config: ConcurrentReaderPoolConfig,
) -> Result<RunningReaderPool, BenchError> {
    validate_schema_name(&config.schema_name)?;
    let stop_signal = Arc::new(AtomicBool::new(false));
    let started_at = Instant::now();
    let mut workers = Vec::with_capacity(usize::try_from(config.reader_count).unwrap_or(0));

    for worker_index in 0..config.reader_count {
        let mut connection =
            connect_postgres(&config.database_url, "connecting concurrent reader").await?;
        let worker_stop = Arc::clone(&stop_signal);
        let loop_params = ReaderLoopParams {
            schema_name: config.schema_name.clone(),
            network: config.network,
            worker_index,
            live_tail_max_blocks: config.live_tail_max_blocks,
        };
        let handle = tokio::spawn(async move {
            let outcome = run_reader_loop(&mut connection.client, &loop_params, &worker_stop).await;
            let close_outcome = close_postgres(connection).await;
            let summary = outcome?;
            close_outcome?;
            Ok(summary)
        });
        workers.push(handle);
    }

    Ok(RunningReaderPool {
        stop_signal,
        started_at,
        workers,
    })
}

async fn run_reader_loop(
    client: &mut Client,
    params: &ReaderLoopParams,
    stop_signal: &AtomicBool,
) -> Result<ReaderWorkerThroughput, BenchError> {
    let worker_started = Instant::now();
    let mut stats = ReaderWorkerThroughput {
        worker_index: params.worker_index,
        ..ReaderWorkerThroughput::default()
    };
    let mut iteration = 0_u64;

    loop {
        let iter_started = Instant::now();
        let iter_stats = execute_reader_iteration(client, params, iteration).await?;
        stats.completed_transactions = stats.completed_transactions.saturating_add(1);
        stats.settled_range_reads = stats
            .settled_range_reads
            .saturating_add(iter_stats.settled_reads);
        stats.blocks_decoded = stats
            .blocks_decoded
            .saturating_add(iter_stats.blocks_decoded);
        stats.bytes_fetched = stats.bytes_fetched.saturating_add(iter_stats.bytes_fetched);
        stats
            .tx_latencies_ms
            .push(iter_started.elapsed().as_secs_f64() * 1000.0);
        iteration = iteration.saturating_add(1);
        if stop_signal.load(Ordering::Acquire) {
            break;
        }
        tokio::task::yield_now().await;
    }

    let elapsed_secs = worker_started.elapsed().as_secs_f64().max(1e-6);
    stats.elapsed_seconds = elapsed_secs;
    stats.blocks_per_second = u64_to_f64_lossy(stats.blocks_decoded) / elapsed_secs;
    stats.bytes_per_second = u64_to_f64_lossy(stats.bytes_fetched) / elapsed_secs;
    Ok(stats)
}

struct IterationStats {
    settled_reads: u64,
    blocks_decoded: u64,
    bytes_fetched: u64,
}

async fn execute_reader_iteration(
    client: &mut Client,
    params: &ReaderLoopParams,
    iteration: u64,
) -> Result<IterationStats, BenchError> {
    let schema = &params.schema_name;
    let txn = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(|err| postgres_error("starting read-only reader transaction", &err))?;

    let meta_row = txn
        .query_one(
            &format!(
                "SELECT settled_tip_height, canonical_tip_height FROM {schema}.schema_meta WHERE singleton"
            ),
            &[],
        )
        .await
        .map_err(|err| postgres_error("reading schema_meta in reader", &err))?;
    let settled_tip: Option<i64> = meta_row.get(0);
    let canonical_tip: i64 = meta_row.get(1);
    if let Some(st) = settled_tip
        && canonical_tip < st
    {
        return Err(candidate_error(
            "reader observed canonical_tip_height < settled_tip_height",
        ));
    }

    let mut stats = IterationStats {
        settled_reads: 0,
        blocks_decoded: 0,
        bytes_fetched: 0,
    };

    let seg_count: i64 = txn
        .query_one(
            &format!(
                "SELECT COUNT(*)::BIGINT FROM {schema}.canonical_settled_segment WHERE segment_kind = 'shielded'"
            ),
            &[],
        )
        .await
        .map_err(|err| postgres_error("counting shielded segments in reader", &err))?
        .get(0);
    if seg_count > 0 {
        let modulo = u64::try_from(seg_count).unwrap_or(1).max(1);
        let row_offset =
            i64::try_from((iteration + u64::from(params.worker_index)) % modulo).unwrap_or(0);
        let (blocks, fetched) =
            read_one_shielded_range_slice(&txn, schema, params.network, row_offset).await?;
        stats.settled_reads = 1;
        stats.blocks_decoded = stats.blocks_decoded.saturating_add(blocks);
        stats.bytes_fetched = stats.bytes_fetched.saturating_add(fetched);
    }

    read_live_tail_and_index_probes(&txn, schema, params.live_tail_max_blocks, &mut stats).await?;

    txn.commit()
        .await
        .map_err(|err| postgres_error("committing read-only reader transaction", &err))?;
    Ok(stats)
}

async fn read_live_tail_and_index_probes(
    txn: &Transaction<'_>,
    schema: &str,
    live_tail_max_blocks: u32,
    stats: &mut IterationStats,
) -> Result<(), BenchError> {
    let tail_count = i64_to_u64(
        txn.query_one(
            &format!("SELECT COUNT(*)::BIGINT FROM {schema}.canonical_live_tail"),
            &[],
        )
        .await
        .map_err(|err| postgres_error("counting canonical_live_tail in reader", &err))?
        .get(0),
        "reader.canonical_live_tail_count",
    )?;
    if tail_count > u64::from(live_tail_max_blocks) {
        return Err(candidate_error(format!(
            "reader observed canonical_live_tail ({tail_count} rows) exceeding live_tail_max_blocks ({live_tail_max_blocks})"
        )));
    }

    let tail_rows = txn
        .query(
            &format!(
                "SELECT compact_block_bytes FROM {schema}.canonical_live_tail ORDER BY height DESC LIMIT 8"
            ),
            &[],
        )
        .await
        .map_err(|err| postgres_error("reading canonical_live_tail in reader", &err))?;
    for row in tail_rows {
        let cb_bytes: &[u8] = row.get(0);
        let _decoded = decode_compact_block_artifact(cb_bytes)
            .map_err(|err| candidate_error(format!("reader live-tail decode failed: {err}")))?;
        stats.blocks_decoded = stats.blocks_decoded.saturating_add(1);
        stats.bytes_fetched = stats
            .bytes_fetched
            .saturating_add(u64::try_from(cb_bytes.len()).unwrap_or(0));
    }

    let _hash_probe = txn
        .query_opt(
            &format!("SELECT height FROM {schema}.block_hash_index LIMIT 1"),
            &[],
        )
        .await
        .map_err(|err| postgres_error("probing block_hash_index in reader", &err))?;
    let _tx_probe = txn
        .query_opt(
            &format!("SELECT block_height FROM {schema}.transaction_location LIMIT 1"),
            &[],
        )
        .await
        .map_err(|err| postgres_error("probing transaction_location in reader", &err))?;
    let _wallet_probe = txn
        .query_opt(
            &format!(
                "SELECT script_hash, SUM(value_zatoshis)::BIGINT FROM {schema}.wallet_utxo WHERE spent_at_height IS NULL GROUP BY script_hash LIMIT 1"
            ),
            &[],
        )
        .await
        .map_err(|err| postgres_error("probing wallet_utxo in reader", &err))?;
    Ok(())
}

async fn read_one_shielded_range_slice(
    txn: &Transaction<'_>,
    schema: &str,
    network: Network,
    row_offset: i64,
) -> Result<(u64, u64), BenchError> {
    let desc = fetch_shielded_descriptor_by_offset(txn, schema, row_offset).await?;
    let end_hash = fetch_block_hash_by_height_txn(txn, schema, desc.end_height).await?;
    let expected = ShieldedSegmentIdentity {
        network,
        artifact_schema_version: desc.artifact_schema_version,
        start_height: desc.start_height,
        end_block: BlockId::new(desc.end_height, end_hash),
        sha256: desc.segment_sha256,
        index_sha256: desc.index_sha256,
        fs_verity_sha256: None,
    };

    let header =
        ShieldedSegmentHeader::decode(&desc.header_bytes, &expected, desc.segment_byte_len)
            .map_err(|err| candidate_error(format!("shielded segment header decode: {err}")))?;
    let idx_range = header
        .index_and_trailer_range()
        .map_err(|err| candidate_error(format!("shielded index range: {err}")))?;
    let idx_bytes = fetch_settled_slice_txn(txn, schema, "shielded", &desc, idx_range).await?;
    let fetched_bytes = u64::try_from(desc.header_bytes.len() + idx_bytes.len()).unwrap_or(0);

    let index = SettledShieldedSegmentIndex::decode_index(
        &desc.header_bytes,
        &idx_bytes,
        &expected,
        desc.segment_byte_len,
        false,
    )
    .map_err(|err| candidate_error(format!("shielded index decode: {err}")))?;

    let span_end = desc
        .start_height
        .value()
        .saturating_add(READER_RANGE_WINDOW_BLOCKS - 1)
        .min(desc.end_height.value());
    let plan = index
        .plan_block_range(
            BlockHeightRange::inclusive(desc.start_height, BlockHeight::new(span_end)),
            ShieldedProjectionSelection::DEFAULT_SHIELDED,
        )
        .map_err(|err| candidate_error(format!("shielded range plan: {err}")))?;

    let (decoded, extra_bytes) =
        decode_shielded_plan_steps_txn(txn, schema, &desc, &index, &plan).await?;
    Ok((decoded, fetched_bytes.saturating_add(extra_bytes)))
}

async fn decode_shielded_plan_steps_txn(
    txn: &Transaction<'_>,
    schema: &str,
    desc: &SegmentRowDescriptor,
    index: &SettledShieldedSegmentIndex,
    plan: &zinder_store::ShieldedRangePlan,
) -> Result<(u64, u64), BenchError> {
    let mut fetched_bytes = 0_u64;
    let verified_buf = match plan.needed_index_byte_range() {
        Some(range) => {
            let slice = fetch_settled_slice_txn(txn, schema, "shielded", desc, range).await?;
            fetched_bytes = fetched_bytes.saturating_add(u64::try_from(slice.len()).unwrap_or(0));
            Some((range.offset, slice))
        }
        None => None,
    };
    let verified_view = match verified_buf.as_ref() {
        Some((off, bytes)) => Some(
            index
                .authenticate_index_slice(ShieldedIndexSlice::new(*off, bytes))
                .map_err(|err| candidate_error(format!("shielded index slice auth: {err}")))?,
        ),
        None => None,
    };

    let mut decoded_blocks = 0_u64;
    for step in plan.steps() {
        let slab_bytes = match step.slab_byte_range() {
            Some(slab_range) => {
                let slab =
                    fetch_settled_slice_txn(txn, schema, "shielded", desc, slab_range).await?;
                fetched_bytes =
                    fetched_bytes.saturating_add(u64::try_from(slab.len()).unwrap_or(0));
                Some(slab)
            }
            None => None,
        };
        let decoded = index
            .verify_and_decode_slab(
                step,
                ShieldedFetchedSlab::new(slab_bytes, verified_view),
                ShieldedProjectionSelection::DEFAULT_SHIELDED,
                HeightOrder::Ascending,
            )
            .map_err(|err| candidate_error(format!("shielded slab decode: {err}")))?;
        decoded_blocks = decoded_blocks.saturating_add(u64::try_from(decoded.len()).unwrap_or(0));
    }
    Ok((decoded_blocks, fetched_bytes))
}

async fn fetch_shielded_descriptor_by_offset(
    txn: &Transaction<'_>,
    schema: &str,
    row_offset: i64,
) -> Result<SegmentRowDescriptor, BenchError> {
    let header_len_pg = i32::try_from(SEGMENT_HEADER_LEN).unwrap_or(128);
    let desc_row = txn
        .query_one(
            &format!(
                r"
SELECT
    start_height, end_height, artifact_schema_version,
    segment_sha256, index_sha256, segment_byte_len,
    substring(segment_bytes FROM 1 FOR $1)
FROM {schema}.canonical_settled_segment
WHERE segment_kind = 'shielded'
ORDER BY start_height ASC
OFFSET $2 LIMIT 1
"
            ),
            &[&header_len_pg, &row_offset],
        )
        .await
        .map_err(|err| postgres_error("reading shielded segment header", &err))?;
    parse_segment_descriptor(&desc_row)
}

async fn fetch_block_hash_by_height_txn(
    txn: &Transaction<'_>,
    schema: &str,
    height: BlockHeight,
) -> Result<BlockHash, BenchError> {
    let row = txn
        .query_one(
            &format!("SELECT block_hash FROM {schema}.block_header WHERE height = $1"),
            &[&u32_to_i64(height.value())],
        )
        .await
        .map_err(|err| postgres_error("reading segment end block hash", &err))?;
    decode_internal_block_hash(row.get::<_, &[u8]>(0))
        .map_err(|err| candidate_error(format!("invalid segment end block hash: {err}")))
}

async fn fetch_settled_slice_txn(
    txn: &Transaction<'_>,
    schema: &str,
    segment_kind: &str,
    desc: &SegmentRowDescriptor,
    range: ByteRange,
) -> Result<Vec<u8>, BenchError> {
    let (pg_off, pg_len) = substring_bounds(range.offset, range.len)?;
    let start_pg = u32_to_i64(desc.start_height.value());
    let end_pg = u32_to_i64(desc.end_height.value());
    let slice: Vec<u8> = txn
        .query_one(
            &format!(
                "SELECT substring(segment_bytes FROM $1 FOR $2) FROM {schema}.canonical_settled_segment WHERE segment_kind = $3 AND start_height = $4 AND end_height = $5"
            ),
            &[&pg_off, &pg_len, &segment_kind, &start_pg, &end_pg],
        )
        .await
        .map_err(|err| postgres_error("fetching settled segment slice in reader", &err))?
        .get(0);
    Ok(slice)
}

async fn fetch_settled_slice_client(
    client: &Client,
    schema: &str,
    segment_kind: &str,
    bounds: (BlockHeight, BlockHeight),
    range: ByteRange,
) -> Result<Vec<u8>, BenchError> {
    let (pg_off, pg_len) = substring_bounds(range.offset, range.len)?;
    let start_pg = u32_to_i64(bounds.0.value());
    let end_pg = u32_to_i64(bounds.1.value());
    let slice: Vec<u8> = client
        .query_one(
            &format!(
                "SELECT substring(segment_bytes FROM $1 FOR $2) FROM {schema}.canonical_settled_segment WHERE segment_kind = $3 AND start_height = $4 AND end_height = $5"
            ),
            &[&pg_off, &pg_len, &segment_kind, &start_pg, &end_pg],
        )
        .await
        .map_err(|err| postgres_error("fetching settled segment slice for validation", &err))?
        .get(0);
    Ok(slice)
}

fn parse_segment_descriptor(row: &tokio_postgres::Row) -> Result<SegmentRowDescriptor, BenchError> {
    let start_h = BlockHeight::new(i64_to_u32(row.get(0), "start_height")?);
    let end_h = BlockHeight::new(i64_to_u32(row.get(1), "end_height")?);
    let schema_ver = u16::try_from(i32_to_u32(row.get(2), "artifact_schema_version")?)
        .map_err(|_| candidate_error("artifact_schema_version exceeds u16"))?;
    let segment_sha256 = slice_to_32_bytes(row.get(3), "segment_sha256")?;
    let index_sha256 = slice_to_32_bytes(row.get(4), "index_sha256")?;
    let segment_byte_len = i64_to_u64(row.get(5), "segment_byte_len")?;
    let header_slice: &[u8] = row.get(6);
    let header_bytes: [u8; SEGMENT_HEADER_LEN] = header_slice.try_into().map_err(|_| {
        candidate_error(format!(
            "segment header must be {SEGMENT_HEADER_LEN} bytes, found {}",
            header_slice.len()
        ))
    })?;
    Ok(SegmentRowDescriptor {
        start_height: start_h,
        end_height: end_h,
        artifact_schema_version: schema_ver,
        segment_sha256,
        index_sha256,
        segment_byte_len,
        header_bytes,
    })
}

/// Validates persisted metadata, bounded live tail, settled segments (`ZSHRNG02`,
/// `ZSTSUP01`, optional `ZSBLKB01`), and the full `CanonicalBlockFactsSequenceDigest`
/// oracle over a fresh read-only connection.
pub async fn validate_persisted_state_read_only(
    client: &Client,
    expected: &ValidationExpectations,
) -> Result<PersistedValidationSummary, BenchError> {
    let schema = validate_schema_name(&expected.schema_name)?;
    let (settled_tip, canonical_tip) = validate_schema_metadata(client, schema, expected).await?;
    validate_secondary_table_counts(client, schema, expected).await?;
    validate_transaction_location_segments(client, schema, settled_tip).await?;

    let mut cursor = ValidationCursor {
        sequence_builder: CanonicalBlockFactsSequenceDigestBuilder::new(
            CanonicalBlockFactsSequenceDigestVersion::CURRENT,
        ),
        prior_hash: None,
    };

    let (settled_blocks_verified, raw_blocks_verified) =
        verify_all_settled_segments(client, schema, expected, settled_tip, &mut cursor).await?;
    let live_tail_blocks_verified = verify_live_tail_sequence(
        client,
        schema,
        expected,
        (settled_tip, canonical_tip),
        &mut cursor,
    )
    .await?;

    let observed_hex = hex::encode(cursor.sequence_builder.finish().as_bytes());
    if observed_hex != expected.expected_sequence_digest_hex {
        return Err(BenchError::canonical_replay_storage_sequence_mismatch(
            format!(
                "postgres-segmented reconstructed sequence digest {observed_hex} disagrees with oracle {}",
                expected.expected_sequence_digest_hex
            ),
        ));
    }

    Ok(PersistedValidationSummary {
        observed_sequence_digest_hex: observed_hex,
        settled_blocks_verified,
        live_tail_blocks_verified,
        raw_blocks_verified,
    })
}

async fn validate_schema_metadata(
    client: &Client,
    schema: &str,
    expected: &ValidationExpectations,
) -> Result<(Option<BlockHeight>, BlockHeight), BenchError> {
    let meta = client
        .query_one(
            &format!(
                r"
SELECT
    contract_identity, schema_version, network, artifact_schema_version,
    full_blocks, max_segment_bytes, live_tail_max_blocks, completed,
    first_height, settled_tip_height, canonical_tip_height,
    canonical_tip_hash, expected_sequence_digest
FROM {schema}.schema_meta
WHERE singleton
"
            ),
            &[],
        )
        .await
        .map_err(|err| postgres_error("reading schema_meta for validation", &err))?;

    validate_meta_identity_and_flags(&meta, expected)?;
    let first_height = BlockHeight::new(i64_to_u32(meta.get(8), "first_height")?);
    if first_height != expected.expected_first_height {
        return Err(candidate_error("schema_meta.first_height mismatch"));
    }
    let settled_tip_opt: Option<i64> = meta.get(9);
    let settled_tip = settled_tip_opt
        .map(|raw| i64_to_u32(raw, "settled_tip_height").map(BlockHeight::new))
        .transpose()?;
    let canonical_tip = BlockHeight::new(i64_to_u32(meta.get(10), "canonical_tip_height")?);
    let canonical_tip_hash = decode_internal_block_hash(meta.get::<_, &[u8]>(11))
        .map_err(|err| candidate_error(format!("invalid canonical_tip_hash: {err}")))?;
    if canonical_tip != expected.expected_tip_height
        || canonical_tip_hash != expected.expected_tip_hash
    {
        return Err(candidate_error(
            "persisted canonical tip does not match fixture tip",
        ));
    }
    let stored_seq_digest: String = meta.get(12);
    if stored_seq_digest != expected.expected_sequence_digest_hex {
        return Err(BenchError::canonical_replay_storage_sequence_mismatch(
            "schema_meta.expected_sequence_digest disagrees with expected oracle digest",
        ));
    }
    Ok((settled_tip, canonical_tip))
}

fn validate_meta_identity_and_flags(
    meta: &tokio_postgres::Row,
    expected: &ValidationExpectations,
) -> Result<(), BenchError> {
    let identity: String = meta.get(0);
    if identity != POSTGRES_SEGMENTED_CONTRACT_IDENTITY {
        return Err(candidate_error(format!(
            "unexpected schema_meta contract_identity `{identity}`"
        )));
    }
    let schema_ver = u16::try_from(i32_to_u32(meta.get(1), "schema_version")?)
        .map_err(|_| candidate_error("schema_version exceeds u16"))?;
    if schema_ver != POSTGRES_SEGMENTED_SCHEMA_VERSION {
        return Err(candidate_error(format!(
            "unexpected schema_version {schema_ver}"
        )));
    }
    let stored_network = decode_zinder_native_chain_name(meta.get::<_, &str>(2))
        .map_err(|err| candidate_error(format!("invalid stored chain name: {err}")))?;
    if stored_network != expected.network {
        return Err(candidate_error(
            "stored network disagrees with benchmark expectation",
        ));
    }
    let artifact_ver = u16::try_from(i32_to_u32(meta.get(3), "artifact_schema_version")?)
        .map_err(|_| candidate_error("artifact_schema_version exceeds u16"))?;
    if artifact_ver != CURRENT_ARTIFACT_SCHEMA_VERSION.value() {
        return Err(candidate_error("artifact_schema_version mismatch"));
    }
    if meta.get::<_, bool>(4) != expected.full_blocks {
        return Err(candidate_error(
            "stored full_blocks disagrees with expectation",
        ));
    }
    if i64_to_u64(meta.get(5), "max_segment_bytes")? != expected.max_segment_bytes {
        return Err(candidate_error(
            "stored max_segment_bytes disagrees with config",
        ));
    }
    if i64_to_u32(meta.get(6), "live_tail_max_blocks")? != expected.live_tail_max_blocks {
        return Err(candidate_error(
            "stored live_tail_max_blocks disagrees with config",
        ));
    }
    if !meta.get::<_, bool>(7) {
        return Err(BenchError::acceptance_completion_mismatch(
            "postgres-segmented schema_meta.completed is false",
        ));
    }
    Ok(())
}

async fn validate_secondary_table_counts(
    client: &Client,
    schema: &str,
    expected: &ValidationExpectations,
) -> Result<(), BenchError> {
    let counts = client
        .query_one(
            &format!(
                r"
SELECT
    (SELECT COUNT(*)::BIGINT FROM {schema}.transaction_location),
    (SELECT COUNT(*)::BIGINT FROM {schema}.block_hash_index),
    (SELECT COUNT(*)::BIGINT FROM {schema}.tree_state_checkpoint),
    (SELECT COUNT(*)::BIGINT FROM {schema}.subtree_root),
    (SELECT COUNT(*)::BIGINT FROM {schema}.wallet_utxo),
    (SELECT COUNT(*)::BIGINT FROM {schema}.wallet_address_tx),
    (SELECT COUNT(*)::BIGINT FROM {schema}.canonical_live_tail)
"
            ),
            &[],
        )
        .await
        .map_err(|err| postgres_error("validating table row counts", &err))?;

    let checks = [
        (
            "transaction_location",
            i64_to_u64(counts.get(0), "transaction_location")?,
            expected.expected_transaction_location_rows,
        ),
        (
            "block_hash_index",
            i64_to_u64(counts.get(1), "block_hash_index")?,
            expected.expected_block_hash_index_rows,
        ),
        (
            "tree_state_checkpoint",
            i64_to_u64(counts.get(2), "tree_state_checkpoint")?,
            expected.expected_tree_state_checkpoint_rows,
        ),
        (
            "subtree_root",
            i64_to_u64(counts.get(3), "subtree_root")?,
            expected.expected_subtree_root_rows,
        ),
        (
            "wallet_utxo",
            i64_to_u64(counts.get(4), "wallet_utxo")?,
            expected.expected_wallet_utxo_rows,
        ),
        (
            "wallet_address_tx",
            i64_to_u64(counts.get(5), "wallet_address_tx")?,
            expected.expected_wallet_address_tx_rows,
        ),
    ];
    for (table, observed, wanted) in checks {
        if observed != wanted {
            return Err(candidate_error(format!(
                "validated row count mismatch for `{table}`: expected {wanted}, observed {observed}"
            )));
        }
    }
    let live_tail_count = i64_to_u64(counts.get(6), "canonical_live_tail")?;
    if live_tail_count > u64::from(expected.live_tail_max_blocks) {
        return Err(candidate_error(format!(
            "canonical_live_tail ({live_tail_count} rows) exceeds live_tail_max_blocks ({})",
            expected.live_tail_max_blocks
        )));
    }
    Ok(())
}

async fn validate_transaction_location_segments(
    client: &Client,
    schema: &str,
    settled_tip: Option<BlockHeight>,
) -> Result<(), BenchError> {
    let settled_tip_pg = settled_tip.map(|h| u32_to_i64(h.value()));
    let mismatch = client
        .query_one(
            &format!(
                r"
SELECT
    (SELECT COUNT(*)::BIGINT
     FROM {schema}.transaction_location tl
     LEFT JOIN {schema}.canonical_live_tail lt
       ON lt.height = tl.block_height AND lt.block_hash = tl.block_hash
     WHERE tl.segment_kind = 0
       AND (lt.height IS NULL OR ($1::BIGINT IS NOT NULL AND tl.block_height <= $1))) AS bad_live,
    (SELECT COUNT(*)::BIGINT
     FROM {schema}.transaction_location tl
     LEFT JOIN {schema}.canonical_live_tail lt
       ON lt.height = tl.block_height AND lt.block_hash = tl.block_hash
     WHERE tl.segment_kind = 1
       AND (lt.height IS NOT NULL OR $1::BIGINT IS NULL OR tl.block_height > $1)) AS bad_settled,
    (SELECT COUNT(*)::BIGINT
     FROM {schema}.transaction_location
     WHERE segment_kind NOT IN (0, 1)) AS invalid_kind
"
            ),
            &[&settled_tip_pg],
        )
        .await
        .map_err(|err| postgres_error("validating transaction_location segment_kind", &err))?;

    let bad_live = i64_to_u64(mismatch.get(0), "bad_live")?;
    let bad_settled = i64_to_u64(mismatch.get(1), "bad_settled")?;
    let invalid_kind = i64_to_u64(mismatch.get(2), "invalid_kind")?;
    if bad_live != 0 || bad_settled != 0 || invalid_kind != 0 {
        return Err(candidate_error(format!(
            "transaction_location segment_kind mismatch with canonical_live_tail/settled_tip (bad_live={bad_live}, bad_settled={bad_settled}, invalid_kind={invalid_kind})"
        )));
    }
    Ok(())
}

async fn verify_all_settled_segments(
    client: &Client,
    schema: &str,
    expected: &ValidationExpectations,
    settled_tip: Option<BlockHeight>,
    cursor: &mut ValidationCursor,
) -> Result<(u64, u64), BenchError> {
    let header_len_pg = i32::try_from(SEGMENT_HEADER_LEN).unwrap_or(128);
    let shielded_rows = client
        .query(
            &format!(
                r"
SELECT
    start_height, end_height, artifact_schema_version,
    segment_sha256, index_sha256, segment_byte_len,
    substring(segment_bytes FROM 1 FOR $1)
FROM {schema}.canonical_settled_segment
WHERE segment_kind = 'shielded'
ORDER BY start_height ASC
"
            ),
            &[&header_len_pg],
        )
        .await
        .map_err(|err| postgres_error("listing settled shielded segments", &err))?;

    let mut settled_blocks_verified = 0_u64;
    let mut raw_blocks_verified = 0_u64;
    let mut next_expected_height = expected.expected_first_height.value();

    for shielded_row in &shielded_rows {
        let desc = parse_segment_descriptor(shielded_row)?;
        if desc.start_height.value() != next_expected_height
            || desc.segment_byte_len > expected.max_segment_bytes
        {
            return Err(candidate_error(format!(
                "settled segment boundary or byte limit violated at start_height {}",
                desc.start_height.value()
            )));
        }
        let verified_count =
            verify_one_settled_span(client, schema, expected, &desc, cursor).await?;
        settled_blocks_verified = settled_blocks_verified.saturating_add(verified_count);
        if expected.full_blocks {
            raw_blocks_verified = raw_blocks_verified.saturating_add(verified_count);
        }
        next_expected_height = desc.end_height.value().saturating_add(1);
    }

    match settled_tip {
        None if settled_blocks_verified != 0 => {
            return Err(candidate_error(
                "unexpected settled segments when settled_tip_height IS NULL",
            ));
        }
        Some(tip_h)
            if settled_blocks_verified == 0
                || next_expected_height.saturating_sub(1) != tip_h.value() =>
        {
            return Err(candidate_error(format!(
                "settled segments end at {} while schema_meta reports {}",
                next_expected_height.saturating_sub(1),
                tip_h.value()
            )));
        }
        _ => {}
    }

    Ok((settled_blocks_verified, raw_blocks_verified))
}

async fn verify_one_settled_span(
    client: &Client,
    schema: &str,
    expected: &ValidationExpectations,
    shielded_desc: &SegmentRowDescriptor,
    cursor: &mut ValidationCursor,
) -> Result<u64, BenchError> {
    let start_h_pg = u32_to_i64(shielded_desc.start_height.value());
    let end_h_pg = u32_to_i64(shielded_desc.end_height.value());
    let end_hash = fetch_block_hash_client(client, schema, end_h_pg).await?;
    let end_block = BlockId::new(shielded_desc.end_height, end_hash);

    let compact_blocks = decode_shielded_segment_via_substring(
        client,
        schema,
        expected.network,
        shielded_desc,
        end_block,
    )
    .await?;

    let sup_desc = fetch_segment_descriptor_client(
        client,
        schema,
        "supplement",
        shielded_desc.start_height,
        shielded_desc.end_height,
    )
    .await?;
    if sup_desc.segment_byte_len > expected.max_segment_bytes {
        return Err(candidate_error(
            "supplement segment exceeds max_segment_bytes",
        ));
    }
    let supplement_records = decode_supplement_segment_via_substring(
        client,
        schema,
        expected.network,
        &sup_desc,
        end_block,
    )
    .await?;
    if compact_blocks.len() != supplement_records.len() {
        return Err(candidate_error(
            "shielded and supplement block counts differ",
        ));
    }

    let raw_digests = if expected.full_blocks {
        Some(
            verify_raw_block_segment_via_substring(
                client,
                schema,
                expected,
                shielded_desc.start_height,
                end_block,
            )
            .await?,
        )
    } else {
        None
    };

    let headers = load_block_headers_in_range(client, schema, start_h_pg, end_h_pg).await?;
    if headers.len() != compact_blocks.len() {
        return Err(candidate_error(
            "block_header rows missing inside settled segment range",
        ));
    }

    verify_settled_span_blocks(
        &compact_blocks,
        &supplement_records,
        &headers,
        raw_digests.as_deref(),
        cursor,
    )?;
    Ok(u64::try_from(compact_blocks.len()).unwrap_or(0))
}

fn verify_settled_span_blocks(
    compact_blocks: &[Vec<u8>],
    supplement_records: &[zinder_store::BlockSupplementRecord],
    headers: &[BlockHeaderArtifact],
    raw_digests: Option<&[[u8; 32]]>,
    cursor: &mut ValidationCursor,
) -> Result<(), BenchError> {
    for (idx, ((shielded_payload, sup_record), header)) in compact_blocks
        .iter()
        .zip(supplement_records.iter())
        .zip(headers.iter())
        .enumerate()
    {
        let reconstructed_cb = reconstruct_compact_block_artifact(shielded_payload, sup_record)
            .map_err(|err| candidate_error(format!("reconstruct compact block failed: {err}")))?;
        let facts = reconstruct_canonical_block_facts(header.clone(), sup_record)
            .map_err(|err| candidate_error(format!("reconstruct canonical facts failed: {err}")))?;

        verify_chain_continuity(&mut cursor.prior_hash, &reconstructed_cb)?;
        if let Some(digests) = raw_digests {
            let expected_sha = digests
                .get(idx)
                .ok_or_else(|| candidate_error("missing raw block digest in ZSBLKB01 check"))?;
            if *expected_sha != facts.serialized_bytes_digest.as_bytes() {
                return Err(candidate_error(format!(
                    "ZSBLKB01 decompressed SHA-256 differs from canonical_facts at height {}",
                    header.height.value()
                )));
            }
        }
        let block_digest = facts.digest(CanonicalBlockFactsDigestVersion::V1);
        cursor
            .sequence_builder
            .try_append(block_digest)
            .map_err(|err| candidate_error(format!("sequence digest append: {err}")))?;
    }
    Ok(())
}

async fn fetch_block_hash_client(
    client: &Client,
    schema: &str,
    height_pg: i64,
) -> Result<BlockHash, BenchError> {
    let row = client
        .query_one(
            &format!("SELECT block_hash FROM {schema}.block_header WHERE height = $1"),
            &[&height_pg],
        )
        .await
        .map_err(|err| postgres_error("reading block_header hash", &err))?;
    decode_internal_block_hash(row.get(0))
        .map_err(|err| candidate_error(format!("invalid block_header hash: {err}")))
}

async fn fetch_segment_descriptor_client(
    client: &Client,
    schema: &str,
    kind: &str,
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> Result<SegmentRowDescriptor, BenchError> {
    let header_len_pg = i32::try_from(SEGMENT_HEADER_LEN).unwrap_or(128);
    let row = client
        .query_one(
            &format!(
                r"
SELECT
    start_height, end_height, artifact_schema_version,
    segment_sha256, index_sha256, segment_byte_len,
    substring(segment_bytes FROM 1 FOR $1)
FROM {schema}.canonical_settled_segment
WHERE segment_kind = $2 AND start_height = $3 AND end_height = $4
"
            ),
            &[
                &header_len_pg,
                &kind,
                &u32_to_i64(start_height.value()),
                &u32_to_i64(end_height.value()),
            ],
        )
        .await
        .map_err(|err| postgres_error("reading canonical_settled_segment descriptor", &err))?;
    parse_segment_descriptor(&row)
}

async fn decode_shielded_segment_via_substring(
    client: &Client,
    schema: &str,
    network: Network,
    desc: &SegmentRowDescriptor,
    end_block: BlockId,
) -> Result<Vec<Vec<u8>>, BenchError> {
    let expected = ShieldedSegmentIdentity {
        network,
        artifact_schema_version: desc.artifact_schema_version,
        start_height: desc.start_height,
        end_block,
        sha256: desc.segment_sha256,
        index_sha256: desc.index_sha256,
        fs_verity_sha256: None,
    };
    let header =
        ShieldedSegmentHeader::decode(&desc.header_bytes, &expected, desc.segment_byte_len)
            .map_err(|err| candidate_error(format!("shielded header validation: {err}")))?;
    let idx_range = header
        .index_and_trailer_range()
        .map_err(|err| candidate_error(format!("shielded index range: {err}")))?;
    let idx_bytes = fetch_settled_slice_client(
        client,
        schema,
        "shielded",
        (desc.start_height, desc.end_height),
        idx_range,
    )
    .await?;
    let index = SettledShieldedSegmentIndex::decode_index(
        &desc.header_bytes,
        &idx_bytes,
        &expected,
        desc.segment_byte_len,
        false,
    )
    .map_err(|err| candidate_error(format!("shielded index validation: {err}")))?;

    let plan = index
        .plan_block_range(
            BlockHeightRange::inclusive(desc.start_height, desc.end_height),
            ShieldedProjectionSelection::DEFAULT_SHIELDED,
        )
        .map_err(|err| candidate_error(format!("shielded full plan: {err}")))?;
    decode_shielded_plan_steps_client(client, schema, desc, &index, &plan).await
}

async fn decode_shielded_plan_steps_client(
    client: &Client,
    schema: &str,
    desc: &SegmentRowDescriptor,
    index: &SettledShieldedSegmentIndex,
    plan: &zinder_store::ShieldedRangePlan,
) -> Result<Vec<Vec<u8>>, BenchError> {
    let auth_buf = match plan.needed_index_byte_range() {
        Some(range) => Some((
            range.offset,
            fetch_settled_slice_client(
                client,
                schema,
                "shielded",
                (desc.start_height, desc.end_height),
                range,
            )
            .await?,
        )),
        None => None,
    };
    let verified_idx = match auth_buf.as_ref() {
        Some((off, bytes)) => Some(
            index
                .authenticate_index_slice(ShieldedIndexSlice::new(*off, bytes))
                .map_err(|err| candidate_error(format!("shielded auth slice: {err}")))?,
        ),
        None => None,
    };

    let mut payloads = Vec::with_capacity(plan.block_count());
    for step in plan.steps() {
        let slab_bytes = match step.slab_byte_range() {
            Some(range) => Some(
                fetch_settled_slice_client(
                    client,
                    schema,
                    "shielded",
                    (desc.start_height, desc.end_height),
                    range,
                )
                .await?,
            ),
            None => None,
        };
        let blocks = index
            .verify_and_decode_slab(
                step,
                ShieldedFetchedSlab::new(slab_bytes, verified_idx),
                ShieldedProjectionSelection::DEFAULT_SHIELDED,
                HeightOrder::Ascending,
            )
            .map_err(|err| candidate_error(format!("shielded slab validation: {err}")))?;
        for encoded_block in blocks {
            payloads.push(encoded_block.payload().to_vec());
        }
    }
    Ok(payloads)
}

async fn decode_supplement_segment_via_substring(
    client: &Client,
    schema: &str,
    network: Network,
    desc: &SegmentRowDescriptor,
    end_block: BlockId,
) -> Result<Vec<zinder_store::BlockSupplementRecord>, BenchError> {
    let expected = SupplementSegmentIdentity {
        network,
        artifact_schema_version: desc.artifact_schema_version,
        start_height: desc.start_height,
        end_block,
        sha256: desc.segment_sha256,
        index_sha256: desc.index_sha256,
        fs_verity_sha256: None,
    };
    let header =
        SupplementSegmentHeader::decode(&desc.header_bytes, &expected, desc.segment_byte_len)
            .map_err(|err| candidate_error(format!("supplement header validation: {err}")))?;
    let idx_range = header
        .indexes_and_trailer_range()
        .map_err(|err| candidate_error(format!("supplement index range: {err}")))?;
    let idx_bytes = fetch_settled_slice_client(
        client,
        schema,
        "supplement",
        (desc.start_height, desc.end_height),
        idx_range,
    )
    .await?;
    let index = SettledSupplementSegmentIndex::decode_index(
        &desc.header_bytes,
        &idx_bytes,
        &expected,
        desc.segment_byte_len,
        false,
    )
    .map_err(|err| candidate_error(format!("supplement index validation: {err}")))?;

    let plan = index
        .plan_block_range(desc.start_height, desc.end_height)
        .map_err(|err| candidate_error(format!("supplement plan: {err}")))?;
    let rows_range = plan.row_index_range();
    let rows_bytes = fetch_settled_slice_client(
        client,
        schema,
        "supplement",
        (desc.start_height, desc.end_height),
        rows_range,
    )
    .await?;
    let verified_rows = index
        .authenticate_row_index_slice(SupplementRowIndexSlice::new(rows_range.offset, &rows_bytes))
        .map_err(|err| candidate_error(format!("supplement auth row slice: {err}")))?;

    let mut records = Vec::new();
    for step in plan.chunks() {
        let chunk_bytes = fetch_settled_slice_client(
            client,
            schema,
            "supplement",
            (desc.start_height, desc.end_height),
            step.compressed_range(),
        )
        .await?;
        let decoded = index
            .verify_and_decode_chunk(step, &chunk_bytes, verified_rows)
            .map_err(|err| candidate_error(format!("supplement chunk verify: {err}")))?;
        records.extend(decoded);
    }
    Ok(records)
}

async fn decode_raw_block_index_client(
    client: &Client,
    schema: &str,
    expected: &ValidationExpectations,
    start_height: BlockHeight,
    end_block: BlockId,
) -> Result<SettledRawBlockSegmentIndex, BenchError> {
    let desc = fetch_segment_descriptor_client(
        client,
        schema,
        "raw_block",
        start_height,
        end_block.height,
    )
    .await?;
    if desc.segment_byte_len > expected.max_segment_bytes {
        return Err(candidate_error(
            "raw block segment exceeds max_segment_bytes",
        ));
    }
    let raw_identity = RawBlockSegmentIdentity {
        network: expected.network,
        codec: RawBlockCodec::Zstd,
        start_height,
        end_block,
        sha256: desc.segment_sha256,
        index_sha256: desc.index_sha256,
        fs_verity_sha256: None,
    };
    let header =
        RawBlockSegmentHeader::decode(&desc.header_bytes, &raw_identity, desc.segment_byte_len)
            .map_err(|err| candidate_error(format!("raw header validation: {err}")))?;
    let idx_range = header
        .index_and_trailer_range()
        .map_err(|err| candidate_error(format!("raw index range: {err}")))?;
    let idx_bytes = fetch_settled_slice_client(
        client,
        schema,
        "raw_block",
        (start_height, end_block.height),
        idx_range,
    )
    .await?;
    SettledRawBlockSegmentIndex::decode_index(
        &desc.header_bytes,
        &idx_bytes,
        &raw_identity,
        desc.segment_byte_len,
        false,
    )
    .map_err(|err| candidate_error(format!("raw block index decode: {err}")))
}

async fn verify_raw_block_segment_via_substring(
    client: &Client,
    schema: &str,
    expected: &ValidationExpectations,
    start_height: BlockHeight,
    end_block: BlockId,
) -> Result<Vec<[u8; 32]>, BenchError> {
    let index =
        decode_raw_block_index_client(client, schema, expected, start_height, end_block).await?;

    let row_idx_range = index
        .plan_row_index_range(start_height, end_block.height)
        .map_err(|err| candidate_error(format!("raw block row index plan: {err}")))?;
    let rows_bytes = fetch_settled_slice_client(
        client,
        schema,
        "raw_block",
        (start_height, end_block.height),
        row_idx_range,
    )
    .await?;
    let verified_rows = index
        .authenticate_row_index_slice(RawBlockRowIndexSlice::new(
            row_idx_range.offset,
            &rows_bytes,
        ))
        .map_err(|err| candidate_error(format!("raw block auth row slice: {err}")))?;

    let mut digests = Vec::new();
    for height_val in start_height.value()..=end_block.height.value() {
        let plan = index
            .plan_block_read(BlockHeight::new(height_val))
            .map_err(|err| candidate_error(format!("raw block read plan: {err}")))?;
        let frame = fetch_settled_slice_client(
            client,
            schema,
            "raw_block",
            (start_height, end_block.height),
            plan.slab_byte_range(),
        )
        .await?;
        let raw_block = index
            .verify_and_decode_block(&plan, verified_rows, &frame)
            .map_err(|err| candidate_error(format!("raw block frame verify: {err}")))?;
        digests.push(Sha256::digest(&raw_block).into());
    }
    Ok(digests)
}

async fn load_block_headers_in_range(
    client: &Client,
    schema: &str,
    start_height: i64,
    end_height: i64,
) -> Result<Vec<BlockHeaderArtifact>, BenchError> {
    let rows = client
        .query(
            &format!(
                r"
SELECT
    height, block_hash, previous_hash, version, time_unix,
    bits, nonce, merkle_root, final_sapling_root, block_size_bytes
FROM {schema}.block_header
WHERE height BETWEEN $1 AND $2
ORDER BY height ASC
"
            ),
            &[&start_height, &end_height],
        )
        .await
        .map_err(|err| postgres_error("loading block_header range", &err))?;

    let mut headers = Vec::with_capacity(rows.len());
    for row in rows {
        headers.push(BlockHeaderArtifact::new(
            BlockHeight::new(i64_to_u32(row.get(0), "header.height")?),
            decode_internal_block_hash(row.get(1))
                .map_err(|err| candidate_error(format!("invalid header block_hash: {err}")))?,
            decode_internal_block_hash(row.get(2))
                .map_err(|err| candidate_error(format!("invalid header previous_hash: {err}")))?,
            slice_to_32_bytes(row.get(7), "header.merkle_root")?,
            slice_to_32_bytes(row.get(8), "header.final_sapling_root")?,
            row.get(4),
            i64_to_u32(row.get(5), "header.bits")?,
            slice_to_32_bytes(row.get(6), "header.nonce")?,
            i64_to_u32(row.get(3), "header.version")?,
            i64_to_u64(row.get(9), "header.block_size_bytes")?,
        ));
    }
    Ok(headers)
}

async fn verify_live_tail_sequence(
    client: &Client,
    schema: &str,
    expected: &ValidationExpectations,
    tip_bounds: (Option<BlockHeight>, BlockHeight),
    cursor: &mut ValidationCursor,
) -> Result<u64, BenchError> {
    let (settled_tip, canonical_tip) = tip_bounds;
    let rows = client
        .query(
            &format!(
                r"
SELECT
    height, block_hash, previous_hash,
    compact_block_bytes, canonical_facts_bytes, raw_block_bytes
FROM {schema}.canonical_live_tail
ORDER BY height ASC
"
            ),
            &[],
        )
        .await
        .map_err(|err| postgres_error("loading canonical_live_tail rows", &err))?;

    let mut next_height = settled_tip.map_or_else(
        || expected.expected_first_height.value(),
        |h| h.value().saturating_add(1),
    );
    let mut verified_count = 0_u64;
    for row in rows {
        let height = BlockHeight::new(i64_to_u32(row.get(0), "tail.height")?);
        if height.value() != next_height {
            return Err(candidate_error(format!(
                "canonical_live_tail gap: expected height {next_height}, found {}",
                height.value()
            )));
        }
        verify_single_live_tail_row(&row, height, expected.full_blocks, cursor)?;
        next_height = next_height.saturating_add(1);
        verified_count = verified_count.saturating_add(1);
    }

    let expect_tail = settled_tip.is_none_or(|st| st < canonical_tip);
    if expect_tail && next_height.saturating_sub(1) != canonical_tip.value() {
        return Err(candidate_error(format!(
            "canonical_live_tail stops at {} instead of canonical_tip_height {}",
            next_height.saturating_sub(1),
            canonical_tip.value()
        )));
    }
    Ok(verified_count)
}

fn verify_single_live_tail_row(
    row: &tokio_postgres::Row,
    height: BlockHeight,
    full_blocks: bool,
    cursor: &mut ValidationCursor,
) -> Result<(), BenchError> {
    let block_hash = decode_internal_block_hash(row.get(1))
        .map_err(|err| candidate_error(format!("invalid tail block_hash: {err}")))?;
    let prev_hash = decode_internal_block_hash(row.get(2))
        .map_err(|err| candidate_error(format!("invalid tail previous_hash: {err}")))?;
    let compact_block = decode_compact_block_artifact(row.get(3))
        .map_err(|err| candidate_error(format!("invalid tail compact_block: {err}")))?;
    if compact_block.height() != height
        || compact_block.block_hash() != block_hash
        || compact_block.previous_block_hash() != prev_hash
    {
        return Err(candidate_error(
            "tail compact_block identity disagrees with row metadata",
        ));
    }
    verify_chain_continuity(&mut cursor.prior_hash, &compact_block)?;

    let replay = decode_canonical_block_replay(row.get(4))
        .map_err(|err| candidate_error(format!("invalid tail replay envelope: {err}")))?;
    let raw_opt: Option<Vec<u8>> = row.get(5);
    if full_blocks {
        let raw_bytes = raw_opt.ok_or_else(|| {
            candidate_error(format!(
                "missing raw_block_bytes in canonical_live_tail at height {}",
                height.value()
            ))
        })?;
        let raw_sha: [u8; 32] = Sha256::digest(&raw_bytes).into();
        if raw_sha != replay.facts().serialized_bytes_digest.as_bytes() {
            return Err(candidate_error("tail raw_block_bytes SHA-256 mismatch"));
        }
    }
    cursor
        .sequence_builder
        .try_append(replay.reference_digest())
        .map_err(|err| candidate_error(format!("tail sequence digest append: {err}")))
}

fn verify_chain_continuity(
    prior_hash: &mut Option<BlockHash>,
    compact_block: &CompactBlockArtifact,
) -> Result<(), BenchError> {
    if let Some(expected_prev) = *prior_hash
        && compact_block.previous_block_hash() != expected_prev
    {
        return Err(candidate_error(format!(
            "chain continuity broken at height {}",
            compact_block.height().value()
        )));
    }
    *prior_hash = Some(compact_block.block_hash());
    Ok(())
}

#[allow(
    clippy::cast_precision_loss,
    reason = "u64 byte/block counters convert cleanly to f64 for throughput rates"
)]
fn u64_to_f64_lossy(value_in: u64) -> f64 {
    value_in as f64
}

#[cfg(test)]
mod tests {
    use super::ReaderConnectionMode;

    #[test]
    fn reader_connection_mode_strings_match_cli_contract() {
        assert_eq!(ReaderConnectionMode::Direct.as_str(), "direct");
        assert_eq!(
            ReaderConnectionMode::PgbouncerTransaction.as_str(),
            "pgbouncer-transaction"
        );
    }
}
