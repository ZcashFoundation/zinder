//! Streaming bulk fixture preparation, binary `COPY` ingestion, finalized index
//! creation, fenced lease management, and atomic incremental tip+settlement commits.

use std::num::NonZeroU32;
use std::time::{Duration, Instant};

use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Transaction};
use zinder_core::wire::{
    encode_address_script_hash, encode_internal_block_hash, encode_internal_transaction_id,
    encode_zinder_native_chain_name,
};
use zinder_core::{
    BlockHash, BlockHeight, CanonicalBlockFactsDigestVersion,
    CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion, Network,
    NetworkUpgradeActivations, ShieldedProtocol, SubtreeRootArtifact,
    encode_canonical_block_replay,
};
use zinder_ingest::{
    CommitmentTreeSizes, RawBlobPolicy, position_canonical_block, prepare_canonical_block,
};
use zinder_source::{
    NodeSource, SourceBlock, SourceChainCursor, SourceChainSegmentLimits, SourceChainUpdate,
};
use zinder_store::TREE_STATE_CHECKPOINT_STRIDE;

use crate::error::BenchError;
use crate::fixture::{FixtureManifest, FixtureNodeSource};

use super::database::{candidate_error, postgres_error, u32_to_i64, u64_to_i64, usize_to_i64};
use super::schema::{
    BENCHMARK_WRITER_ID, POSTGRES_SEGMENTED_CONTRACT_IDENTITY, POSTGRES_SEGMENTED_SCHEMA_VERSION,
    build_analyze_sql, build_create_schema_sql, build_finalize_schema_sql,
};
use super::segment_builder::{
    PreparedSegmentedBlock, SealedSegmentBundle, SealedSegmentBytes, encode_compact_block_for_tail,
    extract_consensus_tx_slices, seal_settled_segments,
};
use super::wallet_projection::{
    ProvisionalWalletProjection, WalletAddressTxRow, WalletBlockDelta, WalletUtxoRow,
};

const STREAM_FETCH_WINDOW_BLOCKS: u32 = 64;
const STREAM_MAX_BATCH_BLOCKS: usize = 2_048;
const STREAM_SOURCE_RESPONSE_CAP_BYTES: u64 = 256 * 1024 * 1024;
const TRANSACTION_LOCATION_KIND_LIVE: i16 = 0;
const TRANSACTION_LOCATION_KIND_SETTLED: i16 = 1;

/// Active fenced writer ownership term passed into tip-commit and settlement transactions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterFenceHandle {
    /// Expected monotonic `writer_epoch` held in `{schema}.canonical_writer_fence`.
    pub writer_epoch: i64,
    /// Lease extension duration applied on successful fencing renewal, in milliseconds.
    pub lease_duration_ms: i64,
}

/// Pre-sealed settled-segment bundle trio applied atomically inside a fenced commit.
#[derive(Clone, Debug)]
pub struct PendingSealTransition {
    /// Sealed `ZSHRNG02`, `ZSTSUP01`, and optional `ZSBLKB01` segments.
    pub bundles: Vec<SealedSegmentBundle>,
    /// Inclusive highest block height covered by `bundles`.
    pub settled_height: BlockHeight,
    /// Block hash at `settled_height` recorded in `chain_event`.
    pub settled_hash: BlockHash,
    /// Previous block hash at `settled_height` recorded in `chain_event`.
    pub settled_prev: BlockHash,
    /// Number of tail blocks evicted from `{schema}.canonical_live_tail`.
    pub seal_count: usize,
    /// Largest single settled segment payload across `bundles`.
    pub max_segment_bytes_observed: u64,
}

/// Breakdown of bulk-load build durations before steady-state tip-following.
#[derive(Clone, Debug, Default)]
pub struct BulkLoadTimings {
    /// Source-open, fixture block decode, canonical construction, and replay encoding.
    pub prepare_duration: Duration,
    /// DDL schema and table creation time (`CREATE SCHEMA`, `CREATE TABLE`, `STORAGE EXTERNAL`).
    pub schema_create_duration: Duration,
    /// Provisional wallet projection construction and final UTXO sort duration.
    pub wallet_projection_duration: Duration,
    /// Blocking `ZSHRNG02`, `ZSTSUP01`, and optional `ZSBLKB01` segment sealing duration.
    pub seal_duration: Duration,
    /// Binary `COPY` stream time into unindexed bulk tables.
    pub copy_duration: Duration,
    /// Primary key/index creation, `ANALYZE`, and post-finalize writer term acquisition.
    pub finalize_duration: Duration,
}

/// Configuration driving streaming bulk ingest without retaining all fixture blocks in RAM.
pub struct BulkStreamConfig<'a> {
    /// Parsed fixture manifest.
    pub manifest: &'a FixtureManifest,
    /// Normalized SHA-256 hex digest of `manifest`.
    pub manifest_sha256: &'a str,
    /// Target isolated schema name.
    pub schema_name: &'a str,
    /// Retain raw consensus block payloads (`ZSBLKB01` + `raw_block_bytes`).
    pub full_blocks: bool,
    /// Maximum single settled segment size (`<= 256 MiB`).
    pub max_segment_bytes: u64,
    /// Target block span per settled segment batch.
    pub settled_segment_blocks: u32,
    /// Upper bound on `{schema}.canonical_live_tail` rows.
    pub live_tail_max_blocks: u32,
    /// Number of initial blocks loaded by the streaming bulk phase.
    pub bulk_block_count: usize,
    /// Initial seed lease duration applied during bulk control-row creation.
    pub seed_lease_duration_ms: i64,
    /// Fresh lease duration granted when acquiring the post-finalize writer term.
    pub post_finalize_lease_duration_ms: i64,
}

/// Incremental block-preparation accumulator shared across bulk streaming and tip-follow.
pub struct StreamingPrepareState {
    /// Network consensus upgrade schedule.
    pub activations: NetworkUpgradeActivations,
    /// Cumulative commitment tree sizes tracked sequentially across blocks.
    pub tree_sizes: CommitmentTreeSizes,
    /// Whether `ZSBLKB01` raw block slicing is enabled.
    pub full_blocks: bool,
    /// First chain height (governs checkpoint cadence).
    pub first_height: BlockHeight,
    /// Streaming sequence digest builder updated per connected block.
    pub sequence_builder: CanonicalBlockFactsSequenceDigestBuilder,
    /// Total canonical transactions processed so far.
    pub total_tx_count: u64,
    /// Total strided tree-state checkpoints created so far.
    pub total_checkpoint_count: u64,
    /// First block identity (`height`, `hash`, `previous_hash`).
    pub first_block_meta: Option<(BlockHeight, BlockHash, BlockHash)>,
    /// Most recent block identity (`height`, `hash`, `previous_hash`).
    pub last_block_meta: Option<(BlockHeight, BlockHash, BlockHash)>,
}

impl StreamingPrepareState {
    /// Initializes an empty accumulator rooted at `first_height`.
    #[must_use]
    pub fn new(
        activations: NetworkUpgradeActivations,
        full_blocks: bool,
        first_height: BlockHeight,
    ) -> Self {
        Self {
            activations,
            tree_sizes: CommitmentTreeSizes {
                sapling: 0,
                orchard: 0,
                ironwood: 0,
            },
            full_blocks,
            first_height,
            sequence_builder: CanonicalBlockFactsSequenceDigestBuilder::new(
                CanonicalBlockFactsSequenceDigestVersion::CURRENT,
            ),
            total_tx_count: 0,
            total_checkpoint_count: 0,
            first_block_meta: None,
            last_block_meta: None,
        }
    }

    /// Prepares one [`SourceBlock`] into [`PreparedSegmentedBlock`] and advances digest/counts.
    pub fn prepare_source_block(
        &mut self,
        source_block: &SourceBlock,
    ) -> Result<PreparedSegmentedBlock, BenchError> {
        let prepared = prepare_single_block(
            source_block,
            &self.activations,
            &mut self.tree_sizes,
            self.full_blocks,
        )?;
        self.sequence_builder
            .try_append(prepared.facts.digest(CanonicalBlockFactsDigestVersion::V1))
            .map_err(|err| candidate_error(format!("sequence builder append: {err}")))?;
        self.total_tx_count = self
            .total_tx_count
            .saturating_add(u64::try_from(prepared.facts.transactions.len()).unwrap_or(0));
        if should_checkpoint_height(self.first_height, prepared.height) {
            self.total_checkpoint_count = self.total_checkpoint_count.saturating_add(1);
        }
        let meta = (
            prepared.height,
            prepared.block_hash,
            prepared.previous_block_hash,
        );
        if self.first_block_meta.is_none() {
            self.first_block_meta = Some(meta);
        }
        self.last_block_meta = Some(meta);
        Ok(prepared)
    }
}

/// Outputs produced by [`execute_streaming_bulk_load`] to initialize tip-following.
pub struct BulkStreamOutcome {
    /// Phased build durations measured across streaming preparation, DDL, wallet, seal, COPY, finalize.
    pub timings: BulkLoadTimings,
    /// Unsealed live-tail blocks remaining at the end of bulk ingest (`<= live_tail_max_blocks`).
    pub initial_tail: Vec<PreparedSegmentedBlock>,
    /// Largest single settled segment byte length produced during bulk sealing.
    pub initial_max_segment_bytes: u64,
    /// Active fenced `writer_epoch` acquired after `FINALIZE` + `ANALYZE`.
    pub writer_epoch: i64,
    /// Cumulative preparation state ready to continue over tip-follow blocks.
    pub prepare_state: StreamingPrepareState,
    /// Number of subtree roots copied during bulk ingest.
    pub subtree_root_count: u64,
}

/// Seeded schema metadata parameters inserted into `{schema}.schema_meta`.
pub struct SchemaSeedMetadata<'a> {
    /// Validated schema name.
    pub schema_name: &'a str,
    /// Network identifier.
    pub network: Network,
    /// Whether `ZSBLKB01` + `raw_block_bytes` retention is enabled.
    pub full_blocks: bool,
    /// Configured settled segment byte cap.
    pub max_segment_bytes: u64,
    /// Configured settled segment block span.
    pub settled_segment_blocks: u32,
    /// Configured live-tail upper bound in blocks.
    pub live_tail_max_blocks: u32,
    /// Fixture manifest schema revision.
    pub fixture_format_version: u32,
    /// Fixture manifest SHA-256 hex digest.
    pub fixture_manifest_sha256: &'a str,
    /// Expected full-run canonical sequence digest hex.
    pub expected_sequence_digest_hex: &'a str,
}

/// All atomic updates applied during a single incremental tip-follow block commit.
pub struct TipCommitPayload<'a> {
    /// Active fenced writer term verified inside the tip transaction.
    pub fence: WriterFenceHandle,
    /// Whether raw block retention is active.
    pub full_blocks: bool,
    /// First chain height (governs checkpoint cadence).
    pub first_height: BlockHeight,
    /// Prepared block being appended to the tip.
    pub block: &'a PreparedSegmentedBlock,
    /// Pre-validated wallet UTXO/address changes.
    pub wallet_delta: &'a WalletBlockDelta,
    /// Mempool transaction IDs evicted atomically as mined in this block.
    pub evicted_mempool_txids: &'a [[u8; 32]],
    /// Optional pre-sealed settled transition committed atomically with the tip append.
    pub pending_seal: Option<&'a PendingSealTransition>,
}

/// Deterministic checkpoint predicate: persists at `first_height` and every
/// `TREE_STATE_CHECKPOINT_STRIDE` multiple.
#[must_use]
pub const fn should_checkpoint_height(first_height: BlockHeight, height: BlockHeight) -> bool {
    height.value() == first_height.value()
        || height.value().is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE)
}

/// Fetches and prepares contiguous fixture blocks `[start_height..=end_height]`
/// from `source` using bounded worker calls.
pub async fn prepare_block_range_from_source(
    source: &FixtureNodeSource,
    prepare_state: &mut StreamingPrepareState,
    start_height: u32,
    end_height: u32,
) -> Result<Vec<PreparedSegmentedBlock>, BenchError> {
    let mut prepared_blocks = Vec::new();
    if start_height > end_height {
        return Ok(prepared_blocks);
    }
    let mut cursor_height = start_height;
    while cursor_height <= end_height {
        let remaining = end_height - cursor_height + 1;
        let max_connected = remaining.min(STREAM_FETCH_WINDOW_BLOCKS);
        let limit_count =
            NonZeroU32::new(max_connected).unwrap_or(const { NonZeroU32::new(1).unwrap() });
        let limits = SourceChainSegmentLimits::new(
            SourceChainCursor::before_height(BlockHeight::new(cursor_height)),
            limit_count,
            STREAM_SOURCE_RESPONSE_CAP_BYTES,
            STREAM_SOURCE_RESPONSE_CAP_BYTES,
        );
        let seg = source
            .fetch_chain_segment(limits)
            .await
            .map_err(|err| candidate_error(format!("fixture segment fetch: {err}")))?;
        let mut received = 0_u32;
        for update in seg.updates() {
            if let SourceChainUpdate::ConnectedBlock { block, .. } = update {
                let prepared = prepare_state.prepare_source_block(block)?;
                prepared_blocks.push(prepared);
                received = received.saturating_add(1);
            }
        }
        if received == 0 {
            return Err(candidate_error(format!(
                "fixture source returned zero connected blocks at height {cursor_height}"
            )));
        }
        cursor_height = cursor_height.saturating_add(received);
    }
    Ok(prepared_blocks)
}

fn prepare_single_block(
    source_block: &SourceBlock,
    activations: &NetworkUpgradeActivations,
    tree_sizes: &mut CommitmentTreeSizes,
    full_blocks: bool,
) -> Result<PreparedSegmentedBlock, BenchError> {
    let raw_policy = if full_blocks {
        RawBlobPolicy::All
    } else {
        RawBlobPolicy::None
    };
    let prepared = prepare_canonical_block(source_block, activations, raw_policy)
        .map_err(|err| candidate_error(format!("prepare canonical block: {err}")))?;
    let positioned = position_canonical_block(prepared, tree_sizes)
        .map_err(|err| candidate_error(format!("position canonical block: {err}")))?;

    let raw_tx_slices = if full_blocks {
        let block_blob = positioned
            .retained_raw_blobs
            .block_blob
            .as_ref()
            .ok_or_else(|| {
                candidate_error("expected retained block_blob when RawBlobPolicy::All")
            })?;
        extract_consensus_tx_slices(
            &block_blob.raw_block_bytes,
            &positioned.retained_raw_blobs.transaction_blobs,
        )?
    } else {
        Vec::new()
    };
    let raw_block_bytes = if full_blocks {
        positioned
            .retained_raw_blobs
            .block_blob
            .map(|blob| blob.raw_block_bytes)
    } else {
        None
    };

    let compact_block_bytes = encode_compact_block_for_tail(&positioned.compact_block);
    let compact_block_proto_len = compact_block_bytes.len();
    let canonical_facts_bytes = encode_canonical_block_replay(
        &positioned.facts,
        positioned.replay_envelope.format_version(),
        CanonicalBlockFactsDigestVersion::V1,
    )
    .into_bytes();

    Ok(PreparedSegmentedBlock {
        height: positioned.facts.block_header.height,
        block_hash: positioned.facts.block_header.block_hash,
        previous_block_hash: positioned.facts.block_header.parent_hash,
        compact_block: positioned.compact_block,
        compact_block_proto_len,
        compact_block_bytes,
        facts: positioned.facts,
        canonical_facts_bytes,
        raw_block_bytes,
        raw_tx_slices,
    })
}

fn estimated_prepared_block_bytes(block: &PreparedSegmentedBlock) -> u64 {
    let raw_len = block.raw_block_bytes.as_ref().map_or(0, Vec::len);
    u64::try_from(
        block.compact_block_bytes.len()
            + block.canonical_facts_bytes.len()
            + raw_len
            + block.raw_tx_slices.len() * 16,
    )
    .unwrap_or(u64::MAX)
}

/// Streams settled batches + initial tail from `source` into `PostgreSQL` without
/// keeping full fixture blocks resident in RAM, timing every build stage accurately.
pub async fn execute_streaming_bulk_load(
    client: &mut Client,
    source: &FixtureNodeSource,
    cfg: &BulkStreamConfig<'_>,
    wallet_projection: &mut ProvisionalWalletProjection,
) -> Result<BulkStreamOutcome, BenchError> {
    if cfg.bulk_block_count == 0 {
        return Err(BenchError::invalid_argument(
            "bulk prefix must contain at least 1 block",
        ));
    }
    let network = cfg.manifest.network_typed()?;
    let activations = cfg.manifest.activations_typed()?;
    let subtree_roots = cfg.manifest.subtree_root_artifacts()?;
    let first_height = BlockHeight::new(cfg.manifest.from_height);
    let mut prepare_state = StreamingPrepareState::new(activations, cfg.full_blocks, first_height);
    let mut timings = BulkLoadTimings::default();

    let schema_started = Instant::now();
    client
        .batch_execute(&build_create_schema_sql(cfg.schema_name)?)
        .await
        .map_err(|err| postgres_error("creating postgres-segmented schema", &err))?;
    timings.schema_create_duration = schema_started.elapsed();

    let txn = client
        .transaction()
        .await
        .map_err(|err| postgres_error("starting bulk load transaction", &err))?;

    let mut ctx = StreamingTxnContext {
        txn: &txn,
        source,
        cfg,
        network,
        prepare_state: &mut prepare_state,
        wallet_projection,
        timings: &mut timings,
        max_segment_bytes_observed: 0,
        settled_tip: None,
    };

    let bulk_u32 = u32::try_from(cfg.bulk_block_count)
        .map_err(|_| BenchError::invalid_argument("bulk_block_count exceeds u32"))?;
    let settled_count = cfg
        .bulk_block_count
        .saturating_sub(usize::try_from(cfg.live_tail_max_blocks).unwrap_or(128));
    let settled_end_h = u32::try_from(settled_count)
        .ok()
        .filter(|&c| c > 0)
        .map(|c| cfg.manifest.from_height + c - 1);
    let tail_start_h = settled_end_h.map_or(cfg.manifest.from_height, |s| s + 1);
    let bulk_end_h = cfg.manifest.from_height + bulk_u32 - 1;

    if let Some(settled_end) = settled_end_h {
        stream_and_copy_settled_prefix(&mut ctx, cfg.manifest.from_height, settled_end).await?;
    }
    let (initial_tail, first_tail_bounds) =
        stream_and_copy_tail_and_finalize_bulk(&mut ctx, tail_start_h, bulk_end_h, &subtree_roots)
            .await?;

    let settled_tip = ctx.settled_tip;
    let max_segment_bytes_observed = ctx.max_segment_bytes_observed;
    seed_and_commit_bulk_txn(
        txn,
        SeedAndCommitBulkArgs {
            cfg,
            network,
            settled_tip,
            first_tail_bounds,
            timings: &mut timings,
        },
    )
    .await?;

    let writer_epoch = finalize_schema_and_acquire_term(client, cfg, &mut timings).await?;

    Ok(BulkStreamOutcome {
        timings,
        initial_tail,
        initial_max_segment_bytes: max_segment_bytes_observed,
        writer_epoch,
        prepare_state,
        subtree_root_count: u64::try_from(subtree_roots.len()).unwrap_or(0),
    })
}

struct SeedAndCommitBulkArgs<'a> {
    cfg: &'a BulkStreamConfig<'a>,
    network: Network,
    settled_tip: Option<BlockHeight>,
    first_tail_bounds: (BlockHeight, BlockHash, BlockHash, BlockHeight, BlockHash),
    timings: &'a mut BulkLoadTimings,
}

async fn seed_and_commit_bulk_txn(
    txn: Transaction<'_>,
    args: SeedAndCommitBulkArgs<'_>,
) -> Result<(), BenchError> {
    let copy_seed_started = Instant::now();
    let seed_meta = SchemaSeedMetadata {
        schema_name: args.cfg.schema_name,
        network: args.network,
        full_blocks: args.cfg.full_blocks,
        max_segment_bytes: args.cfg.max_segment_bytes,
        settled_segment_blocks: args.cfg.settled_segment_blocks,
        live_tail_max_blocks: args.cfg.live_tail_max_blocks,
        fixture_format_version: args.cfg.manifest.fixture_format_version,
        fixture_manifest_sha256: args.cfg.manifest_sha256,
        expected_sequence_digest_hex: &args
            .cfg
            .manifest
            .canonical_block_facts_digest_evidence
            .sequence_digest_sha256,
    };
    seed_control_tables(
        &txn,
        &seed_meta,
        args.settled_tip,
        args.cfg.seed_lease_duration_ms,
        args.first_tail_bounds,
    )
    .await?;
    txn.commit()
        .await
        .map_err(|err| postgres_error("committing bulk COPY payload", &err))?;
    args.timings.copy_duration += copy_seed_started.elapsed();
    Ok(())
}

struct StreamingTxnContext<'a, 't> {
    txn: &'a Transaction<'t>,
    source: &'a FixtureNodeSource,
    cfg: &'a BulkStreamConfig<'a>,
    network: Network,
    prepare_state: &'a mut StreamingPrepareState,
    wallet_projection: &'a mut ProvisionalWalletProjection,
    timings: &'a mut BulkLoadTimings,
    max_segment_bytes_observed: u64,
    settled_tip: Option<BlockHeight>,
}

async fn stream_and_copy_settled_prefix(
    ctx: &mut StreamingTxnContext<'_, '_>,
    start_height: u32,
    end_height: u32,
) -> Result<(), BenchError> {
    let max_batch_blocks = usize::try_from(ctx.cfg.settled_segment_blocks)
        .unwrap_or(1_000)
        .clamp(1, STREAM_MAX_BATCH_BLOCKS);
    let max_batch_bytes = ctx.cfg.max_segment_bytes;
    let mut batch_blocks = Vec::new();
    let mut batch_est_bytes = 0_u64;
    let mut cursor_h = start_height;

    while cursor_h <= end_height {
        let slots_left = max_batch_blocks.saturating_sub(batch_blocks.len()).max(1);
        let win_u32 = u32::try_from(slots_left)
            .unwrap_or(STREAM_FETCH_WINDOW_BLOCKS)
            .min(STREAM_FETCH_WINDOW_BLOCKS);
        let chunk_end = cursor_h.saturating_add(win_u32 - 1).min(end_height);

        let prep_started = Instant::now();
        let chunk =
            prepare_block_range_from_source(ctx.source, ctx.prepare_state, cursor_h, chunk_end)
                .await?;
        ctx.timings.prepare_duration += prep_started.elapsed();

        let wallet_started = Instant::now();
        for prepared in &chunk {
            let _ = ctx.wallet_projection.apply_block(&prepared.facts)?;
        }
        ctx.timings.wallet_projection_duration += wallet_started.elapsed();

        for prepared in chunk {
            batch_est_bytes =
                batch_est_bytes.saturating_add(estimated_prepared_block_bytes(&prepared));
            batch_blocks.push(prepared);
        }
        cursor_h = chunk_end.saturating_add(1);

        if batch_blocks.len() >= max_batch_blocks
            || batch_est_bytes >= max_batch_bytes
            || cursor_h > end_height
        {
            let batch = std::mem::take(&mut batch_blocks);
            batch_est_bytes = 0;
            flush_settled_batch(ctx, batch).await?;
        }
    }
    Ok(())
}

async fn flush_settled_batch(
    ctx: &mut StreamingTxnContext<'_, '_>,
    batch: Vec<PreparedSegmentedBlock>,
) -> Result<(), BenchError> {
    if batch.is_empty() {
        return Ok(());
    }
    if let Some(last) = batch.last() {
        ctx.settled_tip = Some(last.height);
    }

    let schema = ctx.cfg.schema_name;
    let first_height = ctx.prepare_state.first_height;
    let copy_idx_started = Instant::now();
    copy_canonical_indexes(
        ctx.txn,
        schema,
        first_height,
        &batch,
        TRANSACTION_LOCATION_KIND_SETTLED,
    )
    .await?;
    let addr_rows = ctx.wallet_projection.drain_address_tx_rows();
    copy_wallet_address_txs(ctx.txn, schema, &addr_rows).await?;
    ctx.timings.copy_duration += copy_idx_started.elapsed();

    let seal_started = Instant::now();
    let sealed = seal_settled_segments(
        ctx.network,
        batch,
        ctx.cfg.settled_segment_blocks,
        ctx.cfg.full_blocks,
        ctx.cfg.max_segment_bytes,
    )
    .await?;
    ctx.timings.seal_duration += seal_started.elapsed();
    ctx.max_segment_bytes_observed = ctx
        .max_segment_bytes_observed
        .max(sealed.max_segment_bytes_observed);

    let copy_seg_started = Instant::now();
    copy_settled_bundles(ctx.txn, schema, ctx.cfg.full_blocks, &sealed.bundles).await?;
    ctx.timings.copy_duration += copy_seg_started.elapsed();
    Ok(())
}

async fn stream_and_copy_tail_and_finalize_bulk(
    ctx: &mut StreamingTxnContext<'_, '_>,
    tail_start_h: u32,
    bulk_end_h: u32,
    subtree_roots: &[SubtreeRootArtifact],
) -> Result<
    (
        Vec<PreparedSegmentedBlock>,
        (BlockHeight, BlockHash, BlockHash, BlockHeight, BlockHash),
    ),
    BenchError,
> {
    let prep_started = Instant::now();
    let initial_tail =
        prepare_block_range_from_source(ctx.source, ctx.prepare_state, tail_start_h, bulk_end_h)
            .await?;
    ctx.timings.prepare_duration += prep_started.elapsed();

    let wallet_started = Instant::now();
    for prepared in &initial_tail {
        let _ = ctx.wallet_projection.apply_block(&prepared.facts)?;
    }
    let sorted_utxos = ctx.wallet_projection.sorted_utxo_rows();
    let remaining_addr_rows = ctx.wallet_projection.drain_address_tx_rows();
    ctx.timings.wallet_projection_duration += wallet_started.elapsed();

    let schema = ctx.cfg.schema_name;
    let first_height = ctx.prepare_state.first_height;
    let copy_started = Instant::now();
    copy_canonical_live_tail(ctx.txn, schema, ctx.cfg.full_blocks, &initial_tail).await?;
    copy_canonical_indexes(
        ctx.txn,
        schema,
        first_height,
        &initial_tail,
        TRANSACTION_LOCATION_KIND_LIVE,
    )
    .await?;
    copy_subtree_roots(ctx.txn, schema, subtree_roots).await?;
    copy_wallet_utxos(ctx.txn, schema, &sorted_utxos).await?;
    copy_wallet_address_txs(ctx.txn, schema, &remaining_addr_rows).await?;
    ctx.timings.copy_duration += copy_started.elapsed();

    let (first_h, first_hash, first_prev) = ctx
        .prepare_state
        .first_block_meta
        .ok_or_else(|| candidate_error("missing first block identity after bulk stream"))?;
    let (tip_h, tip_hash, _tip_prev) = ctx
        .prepare_state
        .last_block_meta
        .ok_or_else(|| candidate_error("missing tip block identity after bulk stream"))?;

    Ok((
        initial_tail,
        (first_h, first_hash, first_prev, tip_h, tip_hash),
    ))
}

async fn finalize_schema_and_acquire_term(
    client: &Client,
    cfg: &BulkStreamConfig<'_>,
    timings: &mut BulkLoadTimings,
) -> Result<i64, BenchError> {
    let finalize_started = Instant::now();
    client
        .batch_execute(&build_finalize_schema_sql(cfg.schema_name)?)
        .await
        .map_err(|err| postgres_error("finalizing segmented schema indexes", &err))?;
    client
        .batch_execute(&build_analyze_sql(cfg.schema_name)?)
        .await
        .map_err(|err| postgres_error("analyzing segmented schema tables", &err))?;
    let writer_epoch = acquire_post_finalize_writer_term(
        client,
        cfg.schema_name,
        1,
        cfg.post_finalize_lease_duration_ms,
    )
    .await?;
    timings.finalize_duration = finalize_started.elapsed();
    Ok(writer_epoch)
}

/// Atomically advances `writer_epoch` after bulk finalization and grants a fresh lease.
///
/// Replaces the seed term without requiring `lease_expires_at > clock_timestamp()` so
/// slow bulk builds recover cleanly while still rejecting superseded epochs/writers.
pub async fn acquire_post_finalize_writer_term(
    client: &Client,
    schema_name: &str,
    expected_epoch: i64,
    lease_duration_ms: i64,
) -> Result<i64, BenchError> {
    if lease_duration_ms <= 0 {
        return Err(BenchError::invalid_argument(
            "writer lease duration must be positive",
        ));
    }
    let bumped_epoch = expected_epoch
        .checked_add(1)
        .ok_or_else(|| candidate_error("writer_epoch overflow during post-finalize renewal"))?;
    let row = client
        .query_opt(
            &format!(
                r"
UPDATE {schema_name}.canonical_writer_fence
SET
    writer_epoch = $1,
    lease_expires_at = clock_timestamp() + ($2::BIGINT * INTERVAL '1 millisecond'),
    updated_at = clock_timestamp()
WHERE singleton
  AND writer_epoch = $3
  AND writer_id = $4
RETURNING writer_epoch
"
            ),
            &[
                &bumped_epoch,
                &lease_duration_ms,
                &expected_epoch,
                &BENCHMARK_WRITER_ID,
            ],
        )
        .await
        .map_err(|err| postgres_error("acquiring post-finalize writer fence term", &err))?;
    let granted = row.ok_or_else(|| {
        candidate_error("post-finalize writer fence superseded before acquiring tip term")
    })?;
    Ok(granted.get(0))
}

async fn seed_control_tables(
    txn: &Transaction<'_>,
    meta: &SchemaSeedMetadata<'_>,
    settled_tip: Option<BlockHeight>,
    seed_lease_duration_ms: i64,
    bounds: (BlockHeight, BlockHash, BlockHash, BlockHeight, BlockHash),
) -> Result<(), BenchError> {
    let schema = meta.schema_name;
    let network_name = encode_zinder_native_chain_name(meta.network);
    insert_schema_meta_row(
        txn,
        meta,
        network_name,
        settled_tip,
        (bounds.0, bounds.3, bounds.4),
    )
    .await?;
    insert_fence_and_epoch(
        txn,
        schema,
        network_name,
        seed_lease_duration_ms,
        (bounds.2, bounds.3, bounds.4),
    )
    .await
}

async fn insert_schema_meta_row(
    txn: &Transaction<'_>,
    meta: &SchemaSeedMetadata<'_>,
    network_name: &str,
    settled_tip: Option<BlockHeight>,
    tip_bounds: (BlockHeight, BlockHeight, BlockHash),
) -> Result<(), BenchError> {
    let (first_height, canonical_tip_height, canonical_tip_hash) = tip_bounds;
    let schema = meta.schema_name;
    let settled_tip_pg = settled_tip.map(|h| u32_to_i64(h.value()));
    let tip_hash_wire = encode_internal_block_hash(canonical_tip_hash);
    txn.execute(
        &format!(
            r"
INSERT INTO {schema}.schema_meta (
    singleton, contract_identity, schema_version, network, artifact_schema_version,
    full_blocks, max_segment_bytes, settled_segment_blocks, live_tail_max_blocks,
    completed, first_height, settled_tip_height, canonical_tip_height,
    canonical_tip_hash, expected_sequence_digest, fixture_format_version, fixture_manifest_sha256
) VALUES (TRUE, $1, $2, $3, $4, $5, $6, $7, $8, FALSE, $9, $10, $11, $12, $13, $14, $15)
"
        ),
        &[
            &POSTGRES_SEGMENTED_CONTRACT_IDENTITY,
            &i32::from(POSTGRES_SEGMENTED_SCHEMA_VERSION),
            &network_name,
            &i32::from(zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value()),
            &meta.full_blocks,
            &u64_to_i64(meta.max_segment_bytes, "max_segment_bytes")?,
            &u32_to_i64(meta.settled_segment_blocks),
            &u32_to_i64(meta.live_tail_max_blocks),
            &u32_to_i64(first_height.value()),
            &settled_tip_pg,
            &u32_to_i64(canonical_tip_height.value()),
            &tip_hash_wire.as_slice(),
            &meta.expected_sequence_digest_hex,
            &i32::try_from(meta.fixture_format_version)
                .map_err(|_| candidate_error("fixture_format_version exceeds i32"))?,
            &meta.fixture_manifest_sha256,
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting schema_meta", &err))?;
    Ok(())
}

async fn insert_fence_and_epoch(
    txn: &Transaction<'_>,
    schema: &str,
    network_name: &str,
    seed_lease_duration_ms: i64,
    chain_bounds: (BlockHash, BlockHeight, BlockHash),
) -> Result<(), BenchError> {
    let (genesis_prev_hash, canonical_tip_height, canonical_tip_hash) = chain_bounds;
    txn.execute(
        &format!(
            r"
INSERT INTO {schema}.canonical_writer_fence (
    singleton, writer_epoch, writer_id, lease_expires_at, updated_at
) VALUES (
    TRUE, 1, $1,
    clock_timestamp() + ($2::BIGINT * INTERVAL '1 millisecond'),
    clock_timestamp()
)
"
        ),
        &[&BENCHMARK_WRITER_ID, &seed_lease_duration_ms],
    )
    .await
    .map_err(|err| postgres_error("seeding canonical_writer_fence", &err))?;

    txn.execute(
        &format!(
            "INSERT INTO {schema}.chain_epoch (epoch, network, genesis_hash) VALUES (1, $1, $2)"
        ),
        &[
            &network_name,
            &encode_internal_block_hash(genesis_prev_hash).as_slice(),
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting initial chain_epoch", &err))?;

    txn.execute(
        &format!(
            "INSERT INTO {schema}.chain_event (epoch, event_kind, height, block_hash, previous_hash) VALUES (1, 'bulk_seed', $1, $2, $3)"
        ),
        &[
            &u32_to_i64(canonical_tip_height.value()),
            &encode_internal_block_hash(canonical_tip_hash).as_slice(),
            &encode_internal_block_hash(genesis_prev_hash).as_slice(),
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting bulk_seed chain_event", &err))?;
    Ok(())
}

/// Inserts settled bundles (`ZSHRNG02`, `ZSTSUP01`, optional `ZSBLKB01`) into
/// `{schema}.canonical_settled_segment` via binary `COPY`.
pub async fn copy_settled_bundles(
    txn: &Transaction<'_>,
    schema: &str,
    full_blocks: bool,
    bundles: &[SealedSegmentBundle],
) -> Result<(), BenchError> {
    if bundles.is_empty() {
        return Ok(());
    }
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.canonical_settled_segment (segment_kind, start_height, end_height, artifact_schema_version, compression_codec, chunk_blocks, per_block_framing, segment_sha256, index_sha256, segment_byte_len, segment_bytes) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting canonical_settled_segment COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::TEXT,
            Type::INT8,
            Type::INT8,
            Type::INT4,
            Type::TEXT,
            Type::INT4,
            Type::BOOL,
            Type::BYTEA,
            Type::BYTEA,
            Type::INT8,
            Type::BYTEA,
        ],
    );
    futures_util::pin_mut!(writer);

    let artifact_ver = i32::from(zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value());
    let sup_chunks = i32::from(zinder_store::SETTLED_SUPPLEMENT_DEFAULT_CHUNK_BLOCKS);
    for bundle in bundles {
        write_settled_row(
            writer.as_mut(),
            SettledSegmentSpec {
                kind: "shielded",
                artifact_ver,
                codec: "none",
                chunk_blocks: None,
                per_block_framing: false,
            },
            &bundle.shielded,
        )
        .await?;
        write_settled_row(
            writer.as_mut(),
            SettledSegmentSpec {
                kind: "supplement",
                artifact_ver,
                codec: "zstd",
                chunk_blocks: Some(sup_chunks),
                per_block_framing: false,
            },
            &bundle.supplement,
        )
        .await?;
        if full_blocks && let Some(raw_seg) = bundle.raw_block.as_ref() {
            write_settled_row(
                writer.as_mut(),
                SettledSegmentSpec {
                    kind: "raw_block",
                    artifact_ver,
                    codec: "zstd",
                    chunk_blocks: None,
                    per_block_framing: true,
                },
                raw_seg,
            )
            .await?;
        }
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing canonical_settled_segment COPY", &err))?;
    Ok(())
}

struct SettledSegmentSpec {
    kind: &'static str,
    artifact_ver: i32,
    codec: &'static str,
    chunk_blocks: Option<i32>,
    per_block_framing: bool,
}

async fn write_settled_row(
    writer: std::pin::Pin<&mut BinaryCopyInWriter>,
    spec: SettledSegmentSpec,
    seg: &SealedSegmentBytes,
) -> Result<(), BenchError> {
    let len_pg = usize_to_i64(seg.bytes.len(), "segment_bytes")?;
    writer
        .write(&[
            &spec.kind,
            &u32_to_i64(seg.start_height.value()),
            &u32_to_i64(seg.end_height.value()),
            &spec.artifact_ver,
            &spec.codec,
            &spec.chunk_blocks,
            &spec.per_block_framing,
            &seg.segment_sha256.as_slice(),
            &seg.index_sha256.as_slice(),
            &len_pg,
            &seg.bytes.as_slice(),
        ])
        .await
        .map_err(|err| postgres_error("writing canonical_settled_segment COPY row", &err))
}

async fn copy_canonical_live_tail(
    txn: &Transaction<'_>,
    schema: &str,
    full_blocks: bool,
    tail_blocks: &[PreparedSegmentedBlock],
) -> Result<(), BenchError> {
    if tail_blocks.is_empty() {
        return Ok(());
    }
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.canonical_live_tail (height, block_hash, previous_hash, epoch, compact_block_bytes, canonical_facts_bytes, raw_block_bytes) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting canonical_live_tail COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::INT8,
            Type::BYTEA,
            Type::BYTEA,
            Type::INT8,
            Type::BYTEA,
            Type::BYTEA,
            Type::BYTEA,
        ],
    );
    futures_util::pin_mut!(writer);
    for block in tail_blocks {
        let raw_opt = if full_blocks {
            block.raw_block_bytes.as_deref()
        } else {
            None
        };
        writer
            .as_mut()
            .write(&[
                &u32_to_i64(block.height.value()),
                &encode_internal_block_hash(block.block_hash).as_slice(),
                &encode_internal_block_hash(block.previous_block_hash).as_slice(),
                &1_i64,
                &block.compact_block_bytes.as_slice(),
                &block.canonical_facts_bytes.as_slice(),
                &raw_opt,
            ])
            .await
            .map_err(|err| postgres_error("writing canonical_live_tail COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing canonical_live_tail COPY", &err))?;
    Ok(())
}

async fn copy_canonical_indexes(
    txn: &Transaction<'_>,
    schema: &str,
    first_height: BlockHeight,
    blocks: &[PreparedSegmentedBlock],
    tx_segment_kind: i16,
) -> Result<(), BenchError> {
    if blocks.is_empty() {
        return Ok(());
    }
    copy_block_headers(txn, schema, blocks).await?;
    copy_block_hash_index(txn, schema, blocks).await?;
    copy_tree_state_checkpoints(txn, schema, first_height, blocks).await?;
    copy_transaction_locations(txn, schema, blocks, tx_segment_kind).await
}

async fn copy_block_headers(
    txn: &Transaction<'_>,
    schema: &str,
    blocks: &[PreparedSegmentedBlock],
) -> Result<(), BenchError> {
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.block_header (height, block_hash, previous_hash, epoch, version, time_unix, bits, nonce, merkle_root, final_sapling_root, sapling_commitment_tree_size, orchard_commitment_tree_size, ironwood_commitment_tree_size, block_size_bytes, transaction_count) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting block_header COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::INT8,
            Type::BYTEA,
            Type::BYTEA,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::BYTEA,
            Type::BYTEA,
            Type::BYTEA,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::INT8,
        ],
    );
    futures_util::pin_mut!(writer);
    for block in blocks {
        let hdr = &block.facts.block_header;
        let meta = block.compact_block.chain_metadata();
        let tx_count = usize_to_i64(block.facts.transactions.len(), "transaction_count")?;
        writer
            .as_mut()
            .write(&[
                &u32_to_i64(hdr.height.value()),
                &encode_internal_block_hash(hdr.block_hash).as_slice(),
                &encode_internal_block_hash(hdr.parent_hash).as_slice(),
                &1_i64,
                &u32_to_i64(hdr.version),
                &hdr.block_time,
                &u32_to_i64(hdr.bits),
                &hdr.nonce.as_slice(),
                &hdr.merkle_root_hash.as_slice(),
                &hdr.commitment_bytes.as_slice(),
                &u32_to_i64(meta.sapling_commitment_tree_size),
                &u32_to_i64(meta.orchard_commitment_tree_size),
                &u32_to_i64(meta.ironwood_commitment_tree_size),
                &u64_to_i64(hdr.block_size_bytes, "block_size_bytes")?,
                &tx_count,
            ])
            .await
            .map_err(|err| postgres_error("writing block_header COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing block_header COPY", &err))?;
    Ok(())
}

async fn copy_block_hash_index(
    txn: &Transaction<'_>,
    schema: &str,
    blocks: &[PreparedSegmentedBlock],
) -> Result<(), BenchError> {
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.block_hash_index (block_hash, height) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting block_hash_index COPY", &err))?;
    let writer = BinaryCopyInWriter::new(sink, &[Type::BYTEA, Type::INT8]);
    futures_util::pin_mut!(writer);
    for block in blocks {
        writer
            .as_mut()
            .write(&[
                &encode_internal_block_hash(block.block_hash).as_slice(),
                &u32_to_i64(block.height.value()),
            ])
            .await
            .map_err(|err| postgres_error("writing block_hash_index COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing block_hash_index COPY", &err))?;
    Ok(())
}

async fn copy_tree_state_checkpoints(
    txn: &Transaction<'_>,
    schema: &str,
    first_height: BlockHeight,
    blocks: &[PreparedSegmentedBlock],
) -> Result<(), BenchError> {
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.tree_state_checkpoint (height, block_hash, sapling_tree_size, orchard_tree_size, ironwood_tree_size, commitment_bytes) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting tree_state_checkpoint COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::INT8,
            Type::BYTEA,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::BYTEA,
        ],
    );
    futures_util::pin_mut!(writer);
    for block in blocks {
        if !should_checkpoint_height(first_height, block.height) {
            continue;
        }
        let meta = block.compact_block.chain_metadata();
        let commitment = block.facts.block_header.commitment_bytes;
        writer
            .as_mut()
            .write(&[
                &u32_to_i64(block.height.value()),
                &encode_internal_block_hash(block.block_hash).as_slice(),
                &u32_to_i64(meta.sapling_commitment_tree_size),
                &u32_to_i64(meta.orchard_commitment_tree_size),
                &u32_to_i64(meta.ironwood_commitment_tree_size),
                &commitment.as_slice(),
            ])
            .await
            .map_err(|err| postgres_error("writing tree_state_checkpoint COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing tree_state_checkpoint COPY", &err))?;
    Ok(())
}

async fn copy_transaction_locations(
    txn: &Transaction<'_>,
    schema: &str,
    blocks: &[PreparedSegmentedBlock],
    tx_segment_kind: i16,
) -> Result<(), BenchError> {
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.transaction_location (txid, block_height, block_hash, tx_index, segment_kind) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting transaction_location COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[Type::BYTEA, Type::INT8, Type::BYTEA, Type::INT8, Type::INT2],
    );
    futures_util::pin_mut!(writer);
    for block in blocks {
        let height_pg = u32_to_i64(block.height.value());
        let block_hash_wire = encode_internal_block_hash(block.block_hash);
        for (tx_pos, tx) in block.facts.transactions.iter().enumerate() {
            let tx_idx_pg = usize_to_i64(tx_pos, "tx_index")?;
            let txid_wire = encode_internal_transaction_id(tx.public_facts.transaction_id);
            writer
                .as_mut()
                .write(&[
                    &txid_wire.as_slice(),
                    &height_pg,
                    &block_hash_wire.as_slice(),
                    &tx_idx_pg,
                    &tx_segment_kind,
                ])
                .await
                .map_err(|err| postgres_error("writing transaction_location COPY row", &err))?;
        }
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing transaction_location COPY", &err))?;
    Ok(())
}

async fn copy_subtree_roots(
    txn: &Transaction<'_>,
    schema: &str,
    subtree_roots: &[SubtreeRootArtifact],
) -> Result<(), BenchError> {
    if subtree_roots.is_empty() {
        return Ok(());
    }
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.subtree_root (pool, subtree_index, root_hash, completing_block_height, completing_block_hash) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting subtree_root COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[Type::INT2, Type::INT8, Type::BYTEA, Type::INT8, Type::BYTEA],
    );
    futures_util::pin_mut!(writer);
    for root in subtree_roots {
        let pool_pg: i16 = match root.protocol {
            ShieldedProtocol::Sapling => 1,
            ShieldedProtocol::Orchard => 2,
            ShieldedProtocol::Ironwood => 3,
            _ => {
                return Err(candidate_error(format!(
                    "unsupported shielded protocol variant for subtree root {}",
                    root.subtree_index.value()
                )));
            }
        };
        let completing_hash_wire = encode_internal_block_hash(root.completing_block_hash);
        let root_hash_bytes = root.root_hash.as_bytes();
        writer
            .as_mut()
            .write(&[
                &pool_pg,
                &u32_to_i64(root.subtree_index.value()),
                &root_hash_bytes.as_slice(),
                &u32_to_i64(root.completing_block_height.value()),
                &completing_hash_wire.as_slice(),
            ])
            .await
            .map_err(|err| postgres_error("writing subtree_root COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing subtree_root COPY", &err))?;
    Ok(())
}

async fn copy_wallet_utxos(
    txn: &Transaction<'_>,
    schema: &str,
    utxos: &[WalletUtxoRow],
) -> Result<(), BenchError> {
    if utxos.is_empty() {
        return Ok(());
    }
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.wallet_utxo (script_hash, txid, output_index, block_height, value_zatoshis, spent_by_txid, spent_at_height) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting wallet_utxo COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::BYTEA,
            Type::BYTEA,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::BYTEA,
            Type::INT8,
        ],
    );
    futures_util::pin_mut!(writer);
    for utxo in utxos {
        let script_wire = encode_address_script_hash(utxo.script_hash);
        let txid_wire = encode_internal_transaction_id(utxo.txid);
        let spent_txid_wire = utxo.spent_by_txid.map(encode_internal_transaction_id);
        let spent_height_pg = utxo.spent_at_height.map(|h| u32_to_i64(h.value()));
        writer
            .as_mut()
            .write(&[
                &script_wire.as_slice(),
                &txid_wire.as_slice(),
                &u32_to_i64(utxo.output_index),
                &u32_to_i64(utxo.block_height.value()),
                &utxo.zatoshis,
                &spent_txid_wire.as_ref().map(<[u8; 32]>::as_slice),
                &spent_height_pg,
            ])
            .await
            .map_err(|err| postgres_error("writing wallet_utxo COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing wallet_utxo COPY", &err))?;
    Ok(())
}

async fn copy_wallet_address_txs(
    txn: &Transaction<'_>,
    schema: &str,
    address_txs: &[WalletAddressTxRow],
) -> Result<(), BenchError> {
    if address_txs.is_empty() {
        return Ok(());
    }
    let sink = txn
        .copy_in(&format!(
            "COPY {schema}.wallet_address_tx (script_hash, block_height, tx_index, txid, net_delta_zatoshis) FROM STDIN BINARY"
        ))
        .await
        .map_err(|err| postgres_error("starting wallet_address_tx COPY", &err))?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[Type::BYTEA, Type::INT8, Type::INT8, Type::BYTEA, Type::INT8],
    );
    futures_util::pin_mut!(writer);
    for row in address_txs {
        let script_wire = encode_address_script_hash(row.script_hash);
        let txid_wire = encode_internal_transaction_id(row.txid);
        writer
            .as_mut()
            .write(&[
                &script_wire.as_slice(),
                &u32_to_i64(row.block_height.value()),
                &u32_to_i64(row.tx_index),
                &txid_wire.as_slice(),
                &row.net_delta_zatoshis,
            ])
            .await
            .map_err(|err| postgres_error("writing wallet_address_tx COPY row", &err))?;
    }
    writer
        .finish()
        .await
        .map_err(|err| postgres_error("finishing wallet_address_tx COPY", &err))?;
    Ok(())
}

/// Atomically persists a tip block, fence renewal, and optional pre-sealed tail transition.
///
/// Updates canonical index tables, wallet delta, mempool evictions, `canonical_live_tail`,
/// and (when `pending_seal` is present) `canonical_settled_segment` eviction in one txn.
pub async fn persist_tip_block_in_txn(
    txn: &Transaction<'_>,
    schema: &str,
    payload: &TipCommitPayload<'_>,
) -> Result<(), BenchError> {
    renew_writer_fence(txn, schema, payload.fence).await?;
    let block = payload.block;
    insert_single_header_and_indexes(txn, schema, payload.first_height, block).await?;
    insert_single_wallet_delta(txn, schema, payload.wallet_delta).await?;
    evict_mined_mempool_in_txn(txn, schema, block.height, payload.evicted_mempool_txids).await?;
    insert_live_tail_and_advance_tip(txn, schema, payload.full_blocks, block).await?;
    if let Some(seal) = payload.pending_seal {
        apply_sealed_tail_transition_in_txn(txn, schema, payload.full_blocks, seal).await?;
    }
    Ok(())
}

async fn renew_writer_fence(
    txn: &Transaction<'_>,
    schema: &str,
    fence: WriterFenceHandle,
) -> Result<(), BenchError> {
    let renewed = txn
        .query_opt(
            &format!(
                r"
UPDATE {schema}.canonical_writer_fence
SET
    lease_expires_at = clock_timestamp() + ($1::BIGINT * INTERVAL '1 millisecond'),
    updated_at = clock_timestamp()
WHERE singleton
  AND writer_epoch = $2
  AND writer_id = $3
  AND lease_expires_at > clock_timestamp()
RETURNING writer_epoch
"
            ),
            &[
                &fence.lease_duration_ms,
                &fence.writer_epoch,
                &BENCHMARK_WRITER_ID,
            ],
        )
        .await
        .map_err(|err| postgres_error("renewing canonical_writer_fence", &err))?;
    if renewed.is_none() {
        return Err(candidate_error(
            "canonical_writer_fence lease expired or fenced by another epoch",
        ));
    }
    Ok(())
}

async fn insert_single_header_and_indexes(
    txn: &Transaction<'_>,
    schema: &str,
    first_height: BlockHeight,
    block: &PreparedSegmentedBlock,
) -> Result<(), BenchError> {
    let hdr = &block.facts.block_header;
    let meta = block.compact_block.chain_metadata();
    let tx_count = usize_to_i64(block.facts.transactions.len(), "transaction_count")?;
    let height_pg = u32_to_i64(hdr.height.value());
    let block_hash_wire = encode_internal_block_hash(hdr.block_hash);

    txn.execute(
        &format!(
            r"
INSERT INTO {schema}.block_header (
    height, block_hash, previous_hash, epoch, version, time_unix, bits,
    nonce, merkle_root, final_sapling_root, sapling_commitment_tree_size,
    orchard_commitment_tree_size, ironwood_commitment_tree_size,
    block_size_bytes, transaction_count
) VALUES ($1, $2, $3, 1, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
"
        ),
        &[
            &height_pg,
            &block_hash_wire.as_slice(),
            &encode_internal_block_hash(hdr.parent_hash).as_slice(),
            &u32_to_i64(hdr.version),
            &hdr.block_time,
            &u32_to_i64(hdr.bits),
            &hdr.nonce.as_slice(),
            &hdr.merkle_root_hash.as_slice(),
            &hdr.commitment_bytes.as_slice(),
            &u32_to_i64(meta.sapling_commitment_tree_size),
            &u32_to_i64(meta.orchard_commitment_tree_size),
            &u32_to_i64(meta.ironwood_commitment_tree_size),
            &u64_to_i64(hdr.block_size_bytes, "block_size_bytes")?,
            &tx_count,
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting tip block_header", &err))?;

    txn.execute(
        &format!("INSERT INTO {schema}.block_hash_index (block_hash, height) VALUES ($1, $2)"),
        &[&block_hash_wire.as_slice(), &height_pg],
    )
    .await
    .map_err(|err| postgres_error("inserting tip block_hash_index", &err))?;

    if should_checkpoint_height(first_height, block.height) {
        txn.execute(
            &format!(
                "INSERT INTO {schema}.tree_state_checkpoint (height, block_hash, sapling_tree_size, orchard_tree_size, ironwood_tree_size, commitment_bytes) VALUES ($1, $2, $3, $4, $5, $6)"
            ),
            &[
                &height_pg,
                &block_hash_wire.as_slice(),
                &u32_to_i64(meta.sapling_commitment_tree_size),
                &u32_to_i64(meta.orchard_commitment_tree_size),
                &u32_to_i64(meta.ironwood_commitment_tree_size),
                &hdr.commitment_bytes.as_slice(),
            ],
        )
        .await
        .map_err(|err| postgres_error("inserting tip tree_state_checkpoint", &err))?;
    }

    for (tx_pos, tx) in block.facts.transactions.iter().enumerate() {
        let txid_wire = encode_internal_transaction_id(tx.public_facts.transaction_id);
        txn.execute(
            &format!(
                "INSERT INTO {schema}.transaction_location (txid, block_height, block_hash, tx_index, segment_kind) VALUES ($1, $2, $3, $4, 0)"
            ),
            &[
                &txid_wire.as_slice(),
                &height_pg,
                &block_hash_wire.as_slice(),
                &usize_to_i64(tx_pos, "tx_index")?,
            ],
        )
        .await
        .map_err(|err| postgres_error("inserting tip transaction_location", &err))?;
    }
    Ok(())
}

async fn evict_mined_mempool_in_txn(
    txn: &Transaction<'_>,
    schema: &str,
    height: BlockHeight,
    txids: &[[u8; 32]],
) -> Result<(), BenchError> {
    if txids.is_empty() {
        return Ok(());
    }
    let height_pg = u32_to_i64(height.value());
    for txid in txids {
        txn.execute(
            &format!("DELETE FROM {schema}.mempool_active_entry WHERE txid = $1"),
            &[&txid.as_slice()],
        )
        .await
        .map_err(|err| postgres_error("evicting mined mempool_active_entry", &err))?;
        txn.execute(
            &format!(
                "INSERT INTO {schema}.mempool_event (event_kind, txid, block_height) VALUES ('evict_mined', $1, $2)"
            ),
            &[&txid.as_slice(), &height_pg],
        )
        .await
        .map_err(|err| postgres_error("inserting evict_mined mempool_event", &err))?;
    }
    Ok(())
}

async fn insert_live_tail_and_advance_tip(
    txn: &Transaction<'_>,
    schema: &str,
    full_blocks: bool,
    block: &PreparedSegmentedBlock,
) -> Result<(), BenchError> {
    let raw_opt = if full_blocks {
        block.raw_block_bytes.as_deref()
    } else {
        None
    };
    let height_pg = u32_to_i64(block.height.value());
    let block_hash_wire = encode_internal_block_hash(block.block_hash);
    let prev_hash_wire = encode_internal_block_hash(block.previous_block_hash);

    txn.execute(
        &format!(
            "INSERT INTO {schema}.canonical_live_tail (height, block_hash, previous_hash, epoch, compact_block_bytes, canonical_facts_bytes, raw_block_bytes) VALUES ($1, $2, $3, 1, $4, $5, $6)"
        ),
        &[
            &height_pg,
            &block_hash_wire.as_slice(),
            &prev_hash_wire.as_slice(),
            &block.compact_block_bytes.as_slice(),
            &block.canonical_facts_bytes.as_slice(),
            &raw_opt,
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting tip block into canonical_live_tail", &err))?;

    txn.execute(
        &format!(
            "INSERT INTO {schema}.chain_event (epoch, event_kind, height, block_hash, previous_hash) VALUES (1, 'tip_append', $1, $2, $3)"
        ),
        &[
            &height_pg,
            &block_hash_wire.as_slice(),
            &prev_hash_wire.as_slice(),
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting tip_append chain_event", &err))?;

    txn.execute(
        &format!(
            "UPDATE {schema}.schema_meta SET canonical_tip_height = $1, canonical_tip_hash = $2 WHERE singleton"
        ),
        &[&height_pg, &block_hash_wire.as_slice()],
    )
    .await
    .map_err(|err| postgres_error("updating canonical tip metadata", &err))?;
    Ok(())
}

async fn insert_single_wallet_delta(
    txn: &Transaction<'_>,
    schema: &str,
    delta: &WalletBlockDelta,
) -> Result<(), BenchError> {
    for utxo in &delta.created_utxos {
        txn.execute(
            &format!(
                "INSERT INTO {schema}.wallet_utxo (script_hash, txid, output_index, block_height, value_zatoshis, spent_by_txid, spent_at_height) VALUES ($1, $2, $3, $4, $5, NULL, NULL)"
            ),
            &[
                &encode_address_script_hash(utxo.script_hash).as_slice(),
                &encode_internal_transaction_id(utxo.txid).as_slice(),
                &u32_to_i64(utxo.output_index),
                &u32_to_i64(utxo.block_height.value()),
                &utxo.zatoshis,
            ],
        )
        .await
        .map_err(|err| postgres_error("inserting tip wallet_utxo", &err))?;
    }
    for spend in &delta.spent_utxos {
        txn.execute(
            &format!(
                "UPDATE {schema}.wallet_utxo SET spent_by_txid = $1, spent_at_height = $2 WHERE txid = $3 AND output_index = $4 AND spent_at_height IS NULL"
            ),
            &[
                &encode_internal_transaction_id(spend.spent_by_txid).as_slice(),
                &u32_to_i64(spend.spent_at_height.value()),
                &encode_internal_transaction_id(spend.outpoint.transaction_id).as_slice(),
                &u32_to_i64(spend.outpoint.output_index),
            ],
        )
        .await
        .map_err(|err| postgres_error("marking spent wallet_utxo", &err))?;
    }
    for addr_row in &delta.address_tx_rows {
        txn.execute(
            &format!(
                "INSERT INTO {schema}.wallet_address_tx (script_hash, block_height, tx_index, txid, net_delta_zatoshis) VALUES ($1, $2, $3, $4, $5)"
            ),
            &[
                &encode_address_script_hash(addr_row.script_hash).as_slice(),
                &u32_to_i64(addr_row.block_height.value()),
                &u32_to_i64(addr_row.tx_index),
                &encode_internal_transaction_id(addr_row.txid).as_slice(),
                &addr_row.net_delta_zatoshis,
            ],
        )
        .await
        .map_err(|err| postgres_error("inserting tip wallet_address_tx", &err))?;
    }
    Ok(())
}

/// Marks `schema_meta.completed = TRUE` once all bulk, tip-follow, and sealing phases finish.
pub async fn mark_schema_completed(client: &Client, schema_name: &str) -> Result<(), BenchError> {
    let rows = client
        .execute(
            &format!("UPDATE {schema_name}.schema_meta SET completed = TRUE WHERE singleton"),
            &[],
        )
        .await
        .map_err(|err| postgres_error("marking schema_meta.completed", &err))?;
    if rows != 1 {
        return Err(candidate_error(
            "schema_meta singleton missing during completion",
        ));
    }
    Ok(())
}

/// Fences, seals, and atomically commits `seal` inside its own transaction when
/// invoked standalone (also called in-transaction from `persist_tip_block_in_txn`).
pub async fn persist_sealed_tail_transition(
    client: &mut Client,
    schema: &str,
    fence: WriterFenceHandle,
    full_blocks: bool,
    seal: &PendingSealTransition,
) -> Result<(), BenchError> {
    let txn = client
        .transaction()
        .await
        .map_err(|err| postgres_error("starting settled transition txn", &err))?;
    renew_writer_fence(&txn, schema, fence).await?;
    apply_sealed_tail_transition_in_txn(&txn, schema, full_blocks, seal).await?;
    txn.commit()
        .await
        .map_err(|err| postgres_error("committing settled segment transition", &err))?;
    Ok(())
}

async fn apply_sealed_tail_transition_in_txn(
    txn: &Transaction<'_>,
    schema: &str,
    full_blocks: bool,
    seal: &PendingSealTransition,
) -> Result<(), BenchError> {
    copy_settled_bundles(txn, schema, full_blocks, &seal.bundles).await?;

    let settled_h_pg = u32_to_i64(seal.settled_height.value());
    txn.execute(
        &format!("DELETE FROM {schema}.canonical_live_tail WHERE height <= $1"),
        &[&settled_h_pg],
    )
    .await
    .map_err(|err| postgres_error("evicting sealed canonical_live_tail rows", &err))?;

    txn.execute(
        &format!(
            "UPDATE {schema}.transaction_location SET segment_kind = 1 WHERE block_height <= $1 AND segment_kind = 0"
        ),
        &[&settled_h_pg],
    )
    .await
    .map_err(|err| postgres_error("promoting transaction_location segment_kind", &err))?;

    txn.execute(
        &format!(
            "INSERT INTO {schema}.chain_event (epoch, event_kind, height, block_hash, previous_hash) VALUES (1, 'segment_seal', $1, $2, $3)"
        ),
        &[
            &settled_h_pg,
            &encode_internal_block_hash(seal.settled_hash).as_slice(),
            &encode_internal_block_hash(seal.settled_prev).as_slice(),
        ],
    )
    .await
    .map_err(|err| postgres_error("inserting segment_seal chain_event", &err))?;

    txn.execute(
        &format!("UPDATE {schema}.schema_meta SET settled_tip_height = $1 WHERE singleton"),
        &[&settled_h_pg],
    )
    .await
    .map_err(|err| postgres_error("updating settled_tip_height", &err))?;
    Ok(())
}
