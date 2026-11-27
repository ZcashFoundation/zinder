//! `ExplorerQuery.BlockProductionSeries` and `BlockProductionInTimeRange` handlers.

use std::{collections::BTreeMap, sync::Arc};

use tonic::{Request, Response, Status};
use zinder_core::{
    BlockHash, BlockHeight, ChainEpoch,
    wire::{
        decode_height_key_ascending, decode_internal_block_hash, encode_height_key_ascending,
        encode_internal_block_hash, encode_rpc_block_hash_hex,
    },
};
use zinder_materialized_views::{
    BLOCK_PRODUCTION_TIME_CONSUMER_NAME, BLOCK_PRODUCTION_TIME_MAX_PAGE_SIZE,
    BLOCK_SUMMARY_COLUMN_FAMILY, BlockProductionTimeBackfillCoverage, BlockProductionTimeConsumer,
    BlockProductionTimeCursor, BlockProductionTimePageRequest, BlockProductionTimeRow,
    MaterializedViewState, MaterializedViewStore,
};
use zinder_proto::capabilities::{
    EXPLORER_BLOCK_PRODUCTION_SERIES_V2, EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1,
};
use zinder_proto::v1::{
    explorer::{
        BlockProductionInTimeRangeRequest, BlockProductionInTimeRangeResponse,
        BlockProductionPoint, BlockProductionSeriesRequest, BlockProductionSeriesResponse,
        BlockProductionTimeRangeCoverage, BlockProductionTimeRangeReadFence, BlockSummary,
    },
    wallet::BlockTip,
};
use zinder_store::chain_epoch_message;

use super::block_paid_fees::{FeeHydrationBudget, hydrate_coinbase_and_paid_fees_with_budget};
use super::canonical_read::{
    SharedCanonicalSecondary, internal_canonical_error, join_block_summary_with_header,
    maybe_catch_up_for_epoch, resolve_locked_canonical_epoch,
};
use super::clamp_max_entries;
use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};

const MAX_BLOCK_PRODUCTION_SERIES_BLOCKS: u32 = 1024;
const DEFAULT_BLOCK_PRODUCTION_TIME_RANGE_ENTRIES: u32 = 256;
const TIME_CURSOR_PREFIX: &[u8; 4] = b"zbt1";
const INNER_TIME_CURSOR_LEN: usize = 8 + 4 + 32;
const TIME_CURSOR_LEN: usize = 4 + 8 + 8 + 8 + 8 + 4 + 32 + INNER_TIME_CURSOR_LEN;

/// Shared dependencies for block-production handlers.
pub(crate) struct BlockProductionContext<'store> {
    pub(crate) canonical_store: &'store SharedCanonicalSecondary,
    pub(crate) materialized_view_store: &'store MaterializedViewStore,
    pub(crate) upstream_observation_cache: &'store UpstreamObservationCache,
}

/// Serves `ExplorerQuery.BlockProductionSeries`.
pub(crate) async fn query_block_production_series(
    context: BlockProductionContext<'_>,
    request: Request<BlockProductionSeriesRequest>,
) -> Result<Response<BlockProductionSeriesResponse>, Status> {
    let inner = request.into_inner();
    let expected_count =
        validate_block_production_series_range(inner.start_height, inner.end_height)?;
    let start_height = inner.start_height;
    let end_height = inner.end_height;

    let canonical_store = Arc::clone(context.canonical_store);
    let mv_store = context.materialized_view_store.clone();
    let at_epoch_id = inner.at_epoch_id;
    let (epoch, points) = tokio::task::spawn_blocking(move || {
        read_block_production_series_sync(
            &canonical_store,
            &mv_store,
            at_epoch_id,
            start_height,
            end_height,
        )
    })
    .await
    .map_err(|error| ExplorerError::internal(format!("canonical read task failed: {error}")))??;

    let covered_block_count = u32::try_from(points.len())
        .map_err(|_| ExplorerError::internal("block-production series point count exceeds u32"))?;
    let missing_block_count = expected_count.saturating_sub(covered_block_count);
    let freshness = attach_upstream_observation(
        context.upstream_observation_cache,
        build_explorer_freshness(
            Some(context.materialized_view_store),
            EXPLORER_BLOCK_PRODUCTION_SERIES_V2,
            Some(chain_epoch_message(epoch)),
            0,
        )?,
    )
    .await;

    Ok(Response::new(BlockProductionSeriesResponse {
        freshness: Some(freshness),
        start_height,
        end_height,
        covered_block_count,
        missing_block_count,
        points,
    }))
}

/// Serves `ExplorerQuery.BlockProductionInTimeRange`.
pub(crate) async fn query_block_production_in_time_range(
    context: BlockProductionContext<'_>,
    request: Request<BlockProductionInTimeRangeRequest>,
) -> Result<Response<BlockProductionInTimeRangeResponse>, Status> {
    let inner = request.into_inner();
    let start_time = inner.start_time_unix_seconds;
    let end_time = inner.end_time_unix_seconds;
    if end_time <= start_time {
        return Err(ExplorerError::invalid_request(
            "end_time_unix_seconds must be greater than start_time_unix_seconds",
        )
        .into());
    }
    let max_entries_cap = u32::try_from(BLOCK_PRODUCTION_TIME_MAX_PAGE_SIZE).unwrap_or(u32::MAX);
    let max_entries = clamp_max_entries(
        inner.max_entries,
        DEFAULT_BLOCK_PRODUCTION_TIME_RANGE_ENTRIES,
        max_entries_cap,
    );
    let decoded_cursor = decode_time_cursor(&inner.from_cursor, start_time, end_time)?;
    let pinned_epoch_id = resolve_time_range_epoch_id(decoded_cursor.as_ref(), inner.at_epoch_id)?;

    let canonical_store = Arc::clone(context.canonical_store);
    let mv_store = context.materialized_view_store.clone();
    let page_outcome = tokio::task::spawn_blocking(move || {
        read_block_production_in_time_range_sync(TimeRangeReadParams {
            canonical_store: &canonical_store,
            mv_store: &mv_store,
            at_epoch_id: pinned_epoch_id,
            start_time,
            end_time,
            max_entries,
            cursor: decoded_cursor.as_ref(),
        })
    })
    .await
    .map_err(|error| ExplorerError::internal(format!("canonical read task failed: {error}")))??;

    let freshness = attach_upstream_observation(
        context.upstream_observation_cache,
        build_explorer_freshness(
            Some(context.materialized_view_store),
            EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1,
            Some(chain_epoch_message(page_outcome.epoch)),
            0,
        )?,
    )
    .await;

    Ok(Response::new(BlockProductionInTimeRangeResponse {
        freshness: Some(freshness),
        points: page_outcome.points,
        next_cursor: page_outcome.next_cursor,
        covered_block_count: page_outcome.covered_block_count,
        missing_block_count: page_outcome.missing_block_count,
        missing_coinbase_count: page_outcome.missing_coinbase_count,
        missing_paid_fee_count: page_outcome.missing_paid_fee_count,
        coverage: Some(page_outcome.coverage),
        read_fence: Some(page_outcome.read_fence),
    }))
}

fn validate_block_production_series_range(
    start_height: u32,
    end_height: u32,
) -> Result<u32, Status> {
    if end_height < start_height {
        return Err(ExplorerError::invalid_request("end_height must be >= start_height").into());
    }
    let span = u64::from(end_height) - u64::from(start_height) + 1;
    if span > u64::from(MAX_BLOCK_PRODUCTION_SERIES_BLOCKS) {
        return Err(ExplorerError::invalid_request(format!(
            "requested span {span} blocks exceeds the per-request cap of \
             {MAX_BLOCK_PRODUCTION_SERIES_BLOCKS}",
        ))
        .into());
    }
    u32::try_from(span)
        .map_err(|_| ExplorerError::invalid_request("block range overflows u32").into())
}

fn read_block_production_series_sync(
    canonical_store: &SharedCanonicalSecondary,
    mv_store: &MaterializedViewStore,
    at_epoch_id: Option<u64>,
    start_height: u32,
    end_height: u32,
) -> Result<(ChainEpoch, Vec<BlockProductionPoint>), Status> {
    let (epoch, mut matched) = read_series_candidates(
        canonical_store,
        mv_store,
        at_epoch_id,
        start_height,
        end_height,
    )?;
    let mut budget = FeeHydrationBudget::default();
    let coinbases = hydrate_coinbase_and_paid_fees_with_budget(
        canonical_store,
        epoch,
        &mut matched,
        &mut budget,
    )?;
    let points = matched
        .into_iter()
        .zip(coinbases)
        .map(|((_, _, bits, summary), coinbase)| BlockProductionPoint {
            summary: Some(summary),
            bits,
            coinbase,
        })
        .collect();
    Ok((epoch, points))
}

type MatchedBlockRow = (BlockHeight, BlockHash, u32, BlockSummary);

fn read_series_candidates(
    canonical_store: &SharedCanonicalSecondary,
    mv_store: &MaterializedViewStore,
    at_epoch_id: Option<u64>,
    start_height: u32,
    end_height: u32,
) -> Result<(ChainEpoch, Vec<MatchedBlockRow>), Status> {
    maybe_catch_up_for_epoch(canonical_store, at_epoch_id)?;
    let guard = canonical_store.read();
    let epoch = resolve_locked_canonical_epoch(&guard, at_epoch_id)?;
    let visible_tip = epoch.visible_tip_height.value();
    let clamped_end = end_height.min(visible_tip);
    if start_height > clamped_end {
        drop(guard);
        return Ok((epoch, Vec::new()));
    }
    let heights: Vec<BlockHeight> = (start_height..=clamped_end).map(BlockHeight::new).collect();
    let headers = guard
        .block_headers_at(&heights)
        .map_err(internal_canonical_error)?;
    drop(guard);

    let mv_payloads = read_mv_summary_payloads_in_range(mv_store, start_height, clamped_end)?;
    let mut matched = Vec::with_capacity(heights.len());
    for (height, maybe_header) in heights.into_iter().zip(headers) {
        let Some(header) = maybe_header else {
            continue;
        };
        let Some(payload) = mv_payloads.get(&height.value()) else {
            continue;
        };
        let Some(summary) = join_block_summary_with_header(payload, &header, visible_tip)? else {
            continue;
        };
        matched.push((height, header.block_hash, header.bits, summary));
    }
    Ok((epoch, matched))
}

fn read_mv_summary_payloads_in_range(
    mv_store: &MaterializedViewStore,
    start_height: u32,
    end_height: u32,
) -> Result<BTreeMap<u32, Vec<u8>>, Status> {
    let start_key = encode_height_key_ascending(BlockHeight::new(start_height));
    let end_key = encode_height_key_ascending(BlockHeight::new(end_height));
    let entries = mv_store
        .range_iterate_consumer(
            BLOCK_SUMMARY_COLUMN_FAMILY,
            &start_key,
            &end_key,
            MAX_BLOCK_PRODUCTION_SERIES_BLOCKS as usize,
        )
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let mut payloads = BTreeMap::new();
    for (key, payload) in entries {
        let height = decode_height_key_ascending(&key)
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
        payloads.insert(height.value(), payload);
    }
    Ok(payloads)
}

#[derive(Clone, Copy)]
struct TimeRangeReadParams<'a> {
    canonical_store: &'a SharedCanonicalSecondary,
    mv_store: &'a MaterializedViewStore,
    at_epoch_id: Option<u64>,
    start_time: i64,
    end_time: i64,
    max_entries: u32,
    cursor: Option<&'a DecodedTimeCursor>,
}

const MAX_TIME_RANGE_SCANNED_CANDIDATES: usize = 4_096;

struct TimeRangePageOutcome {
    epoch: ChainEpoch,
    points: Vec<BlockProductionPoint>,
    next_cursor: Vec<u8>,
    covered_block_count: u32,
    missing_block_count: u32,
    missing_coinbase_count: u32,
    missing_paid_fee_count: u32,
    coverage: BlockProductionTimeRangeCoverage,
    read_fence: BlockProductionTimeRangeReadFence,
}

fn read_block_production_in_time_range_sync(
    params: TimeRangeReadParams<'_>,
) -> Result<TimeRangePageOutcome, Status> {
    if !params
        .mv_store
        .has_consumer(BLOCK_PRODUCTION_TIME_CONSUMER_NAME)
    {
        return Err(ExplorerError::unsupported(
            "BlockProductionInTimeRange is omitted by the selected materialized-view workload",
        )
        .into());
    }
    let snapshot_read = read_time_range_snapshot_and_candidates(&params)?;
    let mut matched = snapshot_read.matched;
    let mut budget = FeeHydrationBudget::default();
    let coinbases = hydrate_coinbase_and_paid_fees_with_budget(
        params.canonical_store,
        snapshot_read.epoch,
        &mut matched,
        &mut budget,
    )?;
    let (points, covered_block_count, missing_coinbase_count, missing_paid_fee_count) =
        collect_hydrated_time_range_points(matched, coinbases)?;
    let (coverage, read_fence) = build_time_range_coverage_and_fence(TimeRangeCoverageInput {
        first_retained: snapshot_read.first_retained,
        epoch: snapshot_read.epoch,
        mv_state: snapshot_read.mv_state,
        mv_coverage: snapshot_read.mv_coverage,
        missing_block_count: snapshot_read.missing_block_count,
        mv_matches_canonical_epoch: snapshot_read.mv_matches_canonical_epoch,
        scan_exhausted: snapshot_read.scan_exhausted,
    });
    let next_cursor = snapshot_read.next_cursor.map_or_else(Vec::new, |cursor| {
        encode_time_cursor(params.start_time, params.end_time, &read_fence, cursor)
    });

    Ok(TimeRangePageOutcome {
        epoch: snapshot_read.epoch,
        points,
        next_cursor,
        covered_block_count,
        missing_block_count: snapshot_read.missing_block_count,
        missing_coinbase_count,
        missing_paid_fee_count,
        coverage,
        read_fence,
    })
}

struct TimeRangeSnapshotRead {
    epoch: ChainEpoch,
    first_retained: BlockHeight,
    mv_state: MaterializedViewState,
    mv_coverage: Option<BlockProductionTimeBackfillCoverage>,
    matched: Vec<(BlockHeight, BlockHash, u32, BlockSummary)>,
    missing_block_count: u32,
    mv_matches_canonical_epoch: bool,
    scan_exhausted: bool,
    next_cursor: Option<BlockProductionTimeCursor>,
}

fn read_time_range_snapshot_and_candidates(
    params: &TimeRangeReadParams<'_>,
) -> Result<TimeRangeSnapshotRead, Status> {
    maybe_catch_up_for_epoch(params.canonical_store, params.at_epoch_id)?;
    let guard = params.canonical_store.read();
    let epoch = resolve_locked_canonical_epoch(&guard, params.at_epoch_id)?;
    let first_retained = guard.history_bounds().first_available_height();

    let snapshot = params.mv_store.read_snapshot();
    let mv_state = snapshot
        .consumer_state(BLOCK_PRODUCTION_TIME_CONSUMER_NAME)
        .map_err(|error| ExplorerError::internal(error.to_string()))?
        .ok_or_else(|| {
            ExplorerError::unsatisfied_precondition(
                "block-production-time materialized view has not committed state yet",
            )
        })?;
    validate_time_cursor_fence(params.cursor, epoch, mv_state)?;
    let page = BlockProductionTimeConsumer::read_page_snapshot(
        &snapshot,
        BlockProductionTimePageRequest {
            start_time_unix_seconds: params.start_time,
            end_time_unix_seconds: params.end_time,
            after: params.cursor.map(|cursor| cursor.time_cursor),
            maximum_height: Some(epoch.visible_tip_height),
            limit: params.max_entries as usize,
            max_scanned_rows: Some(MAX_TIME_RANGE_SCANNED_CANDIDATES),
        },
    )
    .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let mv_coverage = BlockProductionTimeConsumer::coverage_snapshot(&snapshot)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let (matched, missing_block_count) =
        collect_time_range_candidates(&guard, &snapshot, epoch, &page.rows)?;
    drop(snapshot);

    let canonical_tip_hash = guard
        .block_header_at(epoch.visible_tip_height)
        .map_err(internal_canonical_error)?
        .map(|header| header.block_hash);
    drop(guard);

    let mv_matches_canonical_epoch = mv_state.chain_epoch_id == epoch.id
        && mv_state.tip_height == epoch.visible_tip_height
        && canonical_tip_hash == Some(mv_state.tip_hash);
    Ok(TimeRangeSnapshotRead {
        epoch,
        first_retained,
        mv_state,
        mv_coverage,
        matched,
        missing_block_count,
        mv_matches_canonical_epoch,
        scan_exhausted: page.scan_exhausted,
        next_cursor: page.next_cursor,
    })
}

fn collect_hydrated_time_range_points(
    matched: Vec<(BlockHeight, BlockHash, u32, BlockSummary)>,
    coinbases: Vec<Option<zinder_proto::v1::explorer::CoinbaseTransactionSummary>>,
) -> Result<(Vec<BlockProductionPoint>, u32, u32, u32), Status> {
    let mut points = Vec::with_capacity(matched.len());
    let mut missing_coinbase_count = 0_u32;
    let mut missing_paid_fee_count = 0_u32;
    for ((_, _, bits, summary), coinbase) in matched.into_iter().zip(coinbases) {
        if coinbase.is_none() {
            missing_coinbase_count = missing_coinbase_count.saturating_add(1);
        }
        if summary.paid_fees_collected_zat.is_none() {
            missing_paid_fee_count = missing_paid_fee_count.saturating_add(1);
        }
        points.push(BlockProductionPoint {
            summary: Some(summary),
            bits,
            coinbase,
        });
    }
    let covered_block_count = u32::try_from(points.len())
        .map_err(|_| ExplorerError::internal("block-production page point count exceeds u32"))?;
    Ok((
        points,
        covered_block_count,
        missing_coinbase_count,
        missing_paid_fee_count,
    ))
}

fn collect_time_range_candidates(
    secondary: &zinder_store::RocksDbCanonicalSecondary,
    snapshot: &zinder_materialized_views::MaterializedViewStoreReadSnapshot<'_>,
    epoch: ChainEpoch,
    rows: &[BlockProductionTimeRow],
) -> Result<(Vec<MatchedBlockRow>, u32), Status> {
    let heights: Vec<BlockHeight> = rows.iter().map(|row| row.block_height).collect();
    let headers = secondary
        .block_headers_at(&heights)
        .map_err(internal_canonical_error)?;
    let summary_keys: Vec<[u8; 4]> = heights
        .iter()
        .copied()
        .map(encode_height_key_ascending)
        .collect();
    let summary_payloads = snapshot
        .multi_get_consumer(BLOCK_SUMMARY_COLUMN_FAMILY, &summary_keys)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;

    let visible_tip = epoch.visible_tip_height.value();
    let mut matched = Vec::with_capacity(rows.len());
    let mut missing_block_count = 0_u32;
    for ((row, maybe_header), maybe_payload) in rows.iter().zip(headers).zip(summary_payloads) {
        let joined = match (maybe_header, maybe_payload) {
            (Some(header), Some(payload)) => {
                let within_epoch = row.block_height <= epoch.visible_tip_height;
                let hash_matches = header.block_hash == row.block_hash;
                let time_matches = header.block_time == row.block_time_unix_seconds;
                if within_epoch && hash_matches && time_matches {
                    join_block_summary_with_header(&payload, &header, visible_tip)?
                        .map(|summary| (row.block_height, header.block_hash, header.bits, summary))
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(entry) = joined {
            matched.push(entry);
        } else {
            missing_block_count = missing_block_count.saturating_add(1);
        }
    }
    Ok((matched, missing_block_count))
}

#[derive(Clone, Copy)]
struct TimeRangeCoverageInput {
    first_retained: BlockHeight,
    epoch: ChainEpoch,
    mv_state: MaterializedViewState,
    mv_coverage: Option<BlockProductionTimeBackfillCoverage>,
    missing_block_count: u32,
    mv_matches_canonical_epoch: bool,
    scan_exhausted: bool,
}

fn build_time_range_coverage_and_fence(
    input: TimeRangeCoverageInput,
) -> (
    BlockProductionTimeRangeCoverage,
    BlockProductionTimeRangeReadFence,
) {
    let TimeRangeCoverageInput {
        first_retained,
        epoch,
        mv_state,
        mv_coverage,
        missing_block_count,
        mv_matches_canonical_epoch,
        scan_exhausted,
    } = input;
    let requested_range_complete = !scan_exhausted
        && missing_block_count == 0
        && mv_matches_canonical_epoch
        && first_retained == BlockHeight::new(1)
        && mv_coverage.is_some_and(|cov| {
            cov.complete_from_height == BlockHeight::new(1)
                && cov.complete_through_height >= epoch.visible_tip_height
        });
    let coverage = BlockProductionTimeRangeCoverage {
        complete_from_height: mv_coverage.map(|cov| cov.complete_from_height.value()),
        complete_through_height: mv_coverage.map(|cov| cov.complete_through_height.value()),
        complete_from_time_unix_seconds: mv_coverage.map(|cov| cov.complete_from_time_unix_seconds),
        complete_through_time_unix_seconds: mv_coverage
            .map(|cov| cov.complete_through_time_unix_seconds),
        requested_range_complete,
    };
    let read_fence = BlockProductionTimeRangeReadFence {
        chain_epoch_id: epoch.id.value(),
        materialized_view_revision: mv_state.revision,
        materialized_view_tip: Some(BlockTip {
            height: mv_state.tip_height.value(),
            hash: encode_rpc_block_hash_hex(mv_state.tip_hash),
        }),
    };
    (coverage, read_fence)
}

fn resolve_time_range_epoch_id(
    cursor: Option<&DecodedTimeCursor>,
    at_epoch_id: Option<u64>,
) -> Result<Option<u64>, Status> {
    let Some(cursor) = cursor else {
        return Ok(at_epoch_id);
    };
    if at_epoch_id.is_some_and(|requested| requested != cursor.chain_epoch_id) {
        return Err(ExplorerError::invalid_request(
            "at_epoch_id conflicts with block-production continuation cursor epoch",
        )
        .into());
    }
    Ok(Some(cursor.chain_epoch_id))
}

fn validate_time_cursor_fence(
    cursor: Option<&DecodedTimeCursor>,
    epoch: ChainEpoch,
    mv_state: MaterializedViewState,
) -> Result<(), Status> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.chain_epoch_id != epoch.id.value()
        || cursor.materialized_view_revision != mv_state.revision
        || cursor.materialized_view_tip_height != mv_state.tip_height.value()
        || cursor.materialized_view_tip_hash != mv_state.tip_hash
    {
        return Err(ExplorerError::unsatisfied_precondition(
            "block-production-time cursor was invalidated by a materialized-view change",
        )
        .into());
    }
    Ok(())
}

struct DecodedTimeCursor {
    chain_epoch_id: u64,
    materialized_view_revision: u64,
    materialized_view_tip_height: u32,
    materialized_view_tip_hash: BlockHash,
    time_cursor: BlockProductionTimeCursor,
}

fn encode_time_cursor(
    start_time: i64,
    end_time: i64,
    fence: &BlockProductionTimeRangeReadFence,
    time_cursor: BlockProductionTimeCursor,
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(TIME_CURSOR_LEN);
    encoded.extend_from_slice(TIME_CURSOR_PREFIX);
    encoded.extend_from_slice(&start_time.to_be_bytes());
    encoded.extend_from_slice(&end_time.to_be_bytes());
    encoded.extend_from_slice(&fence.chain_epoch_id.to_be_bytes());
    encoded.extend_from_slice(&fence.materialized_view_revision.to_be_bytes());
    let tip_height = fence
        .materialized_view_tip
        .as_ref()
        .map_or(0, |tip| tip.height);
    encoded.extend_from_slice(&tip_height.to_be_bytes());
    let tip_hash = fence
        .materialized_view_tip
        .as_ref()
        .and_then(|tip| zinder_core::wire::decode_rpc_block_hash_hex(&tip.hash).ok())
        .map_or([0_u8; 32], encode_internal_block_hash);
    encoded.extend_from_slice(&tip_hash);
    encoded.extend_from_slice(time_cursor.as_bytes());
    encoded
}

fn decode_time_cursor(
    raw: &[u8],
    start_time: i64,
    end_time: i64,
) -> Result<Option<DecodedTimeCursor>, Status> {
    if raw.is_empty() {
        return Ok(None);
    }
    if raw.len() != TIME_CURSOR_LEN || raw.get(..4) != Some(TIME_CURSOR_PREFIX) {
        return Err(ExplorerError::invalid_request(
            "block-production-time continuation cursor has unexpected encoding",
        )
        .into());
    }
    let cursor_start = read_cursor_i64(raw, 4)?;
    let cursor_end = read_cursor_i64(raw, 12)?;
    if cursor_start != start_time || cursor_end != end_time {
        return Err(ExplorerError::invalid_request(
            "block-production-time continuation cursor time bounds do not match request",
        )
        .into());
    }
    let chain_epoch_id = read_cursor_u64(raw, 20)?;
    let materialized_view_revision = read_cursor_u64(raw, 28)?;
    let materialized_view_tip_height = read_cursor_u32(raw, 36)?;
    let tip_hash_bytes = read_cursor_array::<32>(raw, 40)?;
    let materialized_view_tip_hash = decode_internal_block_hash(&tip_hash_bytes)
        .map_err(|error| ExplorerError::invalid_request(error.to_string()))?;
    let time_cursor = BlockProductionTimeCursor::from_bytes(&raw[72..])
        .map_err(|error| ExplorerError::invalid_request(error.to_string()))?;
    Ok(Some(DecodedTimeCursor {
        chain_epoch_id,
        materialized_view_revision,
        materialized_view_tip_height,
        materialized_view_tip_hash,
        time_cursor,
    }))
}

fn read_cursor_i64(raw: &[u8], offset: usize) -> Result<i64, Status> {
    Ok(i64::from_be_bytes(read_cursor_array(raw, offset)?))
}

fn read_cursor_u64(raw: &[u8], offset: usize) -> Result<u64, Status> {
    Ok(u64::from_be_bytes(read_cursor_array(raw, offset)?))
}

fn read_cursor_u32(raw: &[u8], offset: usize) -> Result<u32, Status> {
    Ok(u32::from_be_bytes(read_cursor_array(raw, offset)?))
}

fn read_cursor_array<const N: usize>(raw: &[u8], offset: usize) -> Result<[u8; N], Status> {
    raw.get(offset..offset.saturating_add(N))
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| {
            ExplorerError::invalid_request("block-production-time continuation cursor truncated")
                .into()
        })
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use zinder_core::{
        ArtifactSchemaVersion, ChainEpochId, ChainTipMetadata, Network, UnixTimestampMillis,
    };

    use super::*;

    #[test]
    fn exhausted_scan_budget_forces_incomplete_time_range_coverage() {
        let tip_hash = BlockHash::from_bytes([0x22; 32]);
        let epoch = ChainEpoch {
            id: ChainEpochId::new(1),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(10),
            visible_tip_hash: tip_hash,
            settled_tip_height: BlockHeight::new(1),
            settled_tip_hash: BlockHash::from_bytes([0x11; 32]),
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            created_at: UnixTimestampMillis::new(1_750_000_000_000),
        };
        let mv_state = MaterializedViewState {
            chain_epoch_id: ChainEpochId::new(1),
            tip_height: BlockHeight::new(10),
            tip_hash,
            revision: 1,
            coverage: None,
        };
        let mv_coverage = Some(BlockProductionTimeBackfillCoverage::new(
            BlockHeight::new(1),
            BlockHeight::new(10),
            100,
            200,
        ));
        let (complete_cov, _) = build_time_range_coverage_and_fence(TimeRangeCoverageInput {
            first_retained: BlockHeight::new(1),
            epoch,
            mv_state,
            mv_coverage,
            missing_block_count: 0,
            mv_matches_canonical_epoch: true,
            scan_exhausted: false,
        });
        assert!(complete_cov.requested_range_complete);

        let (exhausted_cov, _) = build_time_range_coverage_and_fence(TimeRangeCoverageInput {
            first_retained: BlockHeight::new(1),
            epoch,
            mv_state,
            mv_coverage,
            missing_block_count: 0,
            mv_matches_canonical_epoch: true,
            scan_exhausted: true,
        });
        assert!(!exhausted_cov.requested_range_complete);
    }
}
