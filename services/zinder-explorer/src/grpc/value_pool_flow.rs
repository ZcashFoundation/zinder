//! Value-pool flow history and aggregate summary handlers.
//!
//! Every surface reads the neutral canonical event projection at request time.
//! History pages preserve its newest-first order; summaries fold the same rows
//! without introducing a second durable aggregate.

use std::collections::BTreeMap;

use tonic::{Request, Response, Status};
use zinder_core::wire::encode_rpc_transaction_id_hex;
use zinder_materialized_views::{
    MaterializedViewStore, ValuePoolFlowBackfillCoverage,
    ValuePoolFlowDirection as ProjectedDirection, ValuePoolFlowEvent as ProjectedEvent,
    ValuePoolFlowHistoryConsumer, ValuePoolFlowHistoryRow, ValuePoolFlowPool as ProjectedPool,
    ValuePoolFlowTailCoverage,
};
use zinder_proto::capabilities::{
    EXPLORER_VALUE_POOL_FLOW_AMOUNT_THRESHOLD_SUMMARY_V1,
    EXPLORER_VALUE_POOL_FLOW_EVENTS_IN_RANGE_V1, EXPLORER_VALUE_POOL_FLOW_HISTORY_V1,
    EXPLORER_VALUE_POOL_FLOW_ROUNDED_AMOUNT_SUMMARY_V1, EXPLORER_VALUE_POOL_FLOW_SUMMARY_V1,
};
use zinder_proto::v1::explorer::{
    TransactionIntrinsicValueBalances, ValuePoolFlowAmountThresholdSummaryRequest,
    ValuePoolFlowAmountThresholdSummaryResponse, ValuePoolFlowAmountThresholdSummaryRow,
    ValuePoolFlowCoverage, ValuePoolFlowDirection, ValuePoolFlowEvent,
    ValuePoolFlowEventsInRangeRequest, ValuePoolFlowEventsInRangeResponse, ValuePoolFlowFilter,
    ValuePoolFlowHistoryRequest, ValuePoolFlowHistoryResponse, ValuePoolFlowPool,
    ValuePoolFlowRoundedAmountSummaryRequest, ValuePoolFlowRoundedAmountSummaryResponse,
    ValuePoolFlowRoundedAmountSummaryRow, ValuePoolFlowSummaryBucket, ValuePoolFlowSummaryRequest,
    ValuePoolFlowSummaryResolution, ValuePoolFlowSummaryResponse,
};
use zinder_proto::v1::wallet::{VisibleTipBlockRequest, wallet_query_client::WalletQueryClient};
use zinder_runtime::AuthenticatedChannel;

use super::clamp_max_entries;
use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
    indexed_tip_matches_chain_epoch,
};

const DEFAULT_HISTORY_PAGE_SIZE: u32 = 64;
const MAX_HISTORY_PAGE_SIZE: u32 = 256;
const MAX_HISTORY_SCANNED_EVENTS: usize = 100_000;
const DEFAULT_RANGE_MAX_EVENTS: u32 = 64;
const MAX_RANGE_MAX_EVENTS: u32 = 1_024;
const MAX_SUMMARY_SCANNED_EVENTS: usize = 500_000;
pub(super) const MAX_AMOUNT_THRESHOLDS: usize = 32;
const DEFAULT_ROUNDED_AMOUNT_ROWS: u32 = 50;
const MAX_ROUNDED_AMOUNT_ROWS: u32 = 100;
const CURSOR_PREFIX: &[u8; 4] = b"zvf1";
const CURSOR_FILTER_LEN: usize = 10;
const CURSOR_LEN: usize = CURSOR_PREFIX.len()
    + CURSOR_FILTER_LEN
    + zinder_materialized_views::VALUE_POOL_FLOW_HISTORY_KEY_LEN;

/// Executes one `ExplorerQuery.ValuePoolFlowHistory` request.
pub(crate) async fn query_value_pool_flow_history(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<ValuePoolFlowHistoryRequest>,
) -> Result<Response<ValuePoolFlowHistoryResponse>, Status> {
    let request = request.into_inner();
    let page_size = clamp_max_entries(
        request.page_size,
        DEFAULT_HISTORY_PAGE_SIZE,
        MAX_HISTORY_PAGE_SIZE,
    );
    let filter = FlowFilter::try_from(request.filter.unwrap_or_default())?;
    let anchor = if request.cursor.is_empty() {
        None
    } else {
        Some(decode_cursor(&request.cursor, &filter)?)
    };
    let backfill_coverage =
        ValuePoolFlowHistoryConsumer::backfill_coverage(materialized_view_store)
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let tail_coverage = ValuePoolFlowHistoryConsumer::tail_coverage(materialized_view_store)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let chain_epoch = fetch_current_chain_epoch(wallet_client).await?;
    let visible_tip_height = visible_tip_height(&chain_epoch)?;
    let count_domain_complete = should_compute_total_matching_events(
        request.include_total_count,
        backfill_coverage,
        tail_coverage,
        visible_tip_height,
    );
    let page =
        read_history_page_blocking(materialized_view_store, page_size, anchor, filter).await?;
    let total_matching_events =
        history_total_count(materialized_view_store, filter, count_domain_complete).await?;
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_VALUE_POOL_FLOW_HISTORY_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;

    let next_cursor = if page.has_more {
        page.cursor_key
            .as_ref()
            .map_or_else(Vec::new, |key| encode_cursor(key, &filter))
    } else {
        Vec::new()
    };
    Ok(Response::new(ValuePoolFlowHistoryResponse {
        freshness: Some(freshness),
        events: page
            .rows
            .into_iter()
            .map(|row| map_event(row.event))
            .collect::<Result<_, _>>()?,
        next_cursor,
        has_more: page.has_more,
        total_matching_events,
        scanned_event_count: page.scanned_event_count,
        scan_limit_reached: page.scan_limit_reached,
        coverage: Some(map_coverage(backfill_coverage, tail_coverage, false)),
    }))
}

/// Executes one `ExplorerQuery.ValuePoolFlowEventsInRange` request.
pub(crate) async fn query_value_pool_flow_events_in_range(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<ValuePoolFlowEventsInRangeRequest>,
) -> Result<Response<ValuePoolFlowEventsInRangeResponse>, Status> {
    const MAX_EPOCH_STABILIZATION_ATTEMPTS: usize = 3;

    let request = request.into_inner();
    validate_flow_events_in_range_request(&request)?;
    let max_events = clamp_max_entries(
        request.max_events,
        DEFAULT_RANGE_MAX_EVENTS,
        MAX_RANGE_MAX_EVENTS,
    );
    let filter = FlowFilter::try_from(ValuePoolFlowFilter {
        directions: request.directions,
        pools: request.pools,
        minimum_amount_zat: request.minimum_amount_zat,
    })?;
    for _ in 0..MAX_EPOCH_STABILIZATION_ATTEMPTS {
        let chain_epoch = fetch_current_chain_epoch(wallet_client).await?;
        let visible_tip_height = visible_tip_height(&chain_epoch)?;
        let backfill_coverage =
            ValuePoolFlowHistoryConsumer::backfill_coverage(materialized_view_store)
                .map_err(|error| ExplorerError::internal(error.to_string()))?;
        let tail_coverage = ValuePoolFlowHistoryConsumer::tail_coverage(materialized_view_store)
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
        let range = read_flow_events_in_range_blocking(
            materialized_view_store,
            FlowEventsInRangeQuery {
                start_time_unix_seconds: request.start_time_unix_seconds,
                end_time_unix_seconds: request.end_time_unix_seconds,
                filter,
                maximum_amount_zat: request.maximum_amount_zat,
                max_events,
            },
        )
        .await?;
        let observed_chain_epoch = fetch_current_chain_epoch(wallet_client).await?;
        if observed_chain_epoch != chain_epoch {
            continue;
        }
        let freshness = attach_upstream_observation(
            upstream_observation_cache,
            build_explorer_freshness(
                Some(materialized_view_store),
                EXPLORER_VALUE_POOL_FLOW_EVENTS_IN_RANGE_V1,
                Some(chain_epoch.clone()),
                0,
            )?,
        )
        .await;
        let requested_range_complete =
            coverage_reaches_visible_tip(backfill_coverage, tail_coverage, visible_tip_height)
                && indexed_tip_matches_chain_epoch(&freshness, &chain_epoch);

        return Ok(Response::new(ValuePoolFlowEventsInRangeResponse {
            freshness: Some(freshness),
            events: range
                .events
                .into_iter()
                .map(map_event)
                .collect::<Result<_, _>>()?,
            scanned_event_count: range.scanned_event_count,
            scan_limit_reached: range.scan_limit_reached,
            event_limit_reached: range.event_limit_reached,
            coverage: Some(map_coverage(
                backfill_coverage,
                tail_coverage,
                requested_range_complete,
            )),
        }));
    }

    Err(ExplorerError::upstream_unreachable(
        "chain epoch changed while reading value-pool flow events",
    )
    .into())
}

/// Executes one `ExplorerQuery.ValuePoolFlowSummary` request.
pub(crate) async fn query_value_pool_flow_summary(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<ValuePoolFlowSummaryRequest>,
) -> Result<Response<ValuePoolFlowSummaryResponse>, Status> {
    let request = request.into_inner();
    if request.start_time_unix_seconds >= request.end_time_unix_seconds {
        return Err(ExplorerError::invalid_request(
            "start_time_unix_seconds must be less than end_time_unix_seconds",
        )
        .into());
    }
    let resolution = SummaryResolution::try_from(request.resolution)?;
    let pools = PoolFilter::try_from(request.pools)?;
    let backfill_coverage =
        ValuePoolFlowHistoryConsumer::backfill_coverage(materialized_view_store)
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let tail_coverage = ValuePoolFlowHistoryConsumer::tail_coverage(materialized_view_store)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let chain_epoch = fetch_current_chain_epoch(wallet_client).await?;
    let visible_tip_height = visible_tip_height(&chain_epoch)?;
    let requested_range_complete =
        coverage_reaches_visible_tip(backfill_coverage, tail_coverage, visible_tip_height);
    let buckets = read_summary_buckets_blocking(
        materialized_view_store,
        request.start_time_unix_seconds,
        request.end_time_unix_seconds,
        pools,
        resolution,
    )
    .await?;
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_VALUE_POOL_FLOW_SUMMARY_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;
    Ok(Response::new(ValuePoolFlowSummaryResponse {
        freshness: Some(freshness),
        buckets,
        coverage: Some(map_coverage(
            backfill_coverage,
            tail_coverage,
            requested_range_complete,
        )),
    }))
}

/// Executes one `ExplorerQuery.ValuePoolFlowAmountThresholdSummary` request.
pub(crate) async fn query_value_pool_flow_amount_threshold_summary(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<ValuePoolFlowAmountThresholdSummaryRequest>,
) -> Result<Response<ValuePoolFlowAmountThresholdSummaryResponse>, Status> {
    let request = request.into_inner();
    if request.start_time_unix_seconds >= request.end_time_unix_seconds {
        return Err(ExplorerError::invalid_request(
            "start_time_unix_seconds must be less than end_time_unix_seconds",
        )
        .into());
    }
    validate_minimum_amounts(&request.minimum_amounts_zat)?;
    let pools = PoolFilter::try_from(request.pools)?;
    let backfill_coverage =
        ValuePoolFlowHistoryConsumer::backfill_coverage(materialized_view_store)
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let tail_coverage = ValuePoolFlowHistoryConsumer::tail_coverage(materialized_view_store)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let chain_epoch = fetch_current_chain_epoch(wallet_client).await?;
    let visible_tip_height = visible_tip_height(&chain_epoch)?;
    let requested_range_complete =
        coverage_reaches_visible_tip(backfill_coverage, tail_coverage, visible_tip_height);
    let thresholds = read_amount_threshold_summary_blocking(
        materialized_view_store,
        request.start_time_unix_seconds,
        request.end_time_unix_seconds,
        pools,
        request.minimum_amounts_zat,
    )
    .await?;
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_VALUE_POOL_FLOW_AMOUNT_THRESHOLD_SUMMARY_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;

    Ok(Response::new(ValuePoolFlowAmountThresholdSummaryResponse {
        freshness: Some(freshness),
        thresholds,
        coverage: Some(map_coverage(
            backfill_coverage,
            tail_coverage,
            requested_range_complete,
        )),
    }))
}

/// Executes one `ExplorerQuery.ValuePoolFlowRoundedAmountSummary` request.
pub(crate) async fn query_value_pool_flow_rounded_amount_summary(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<ValuePoolFlowRoundedAmountSummaryRequest>,
) -> Result<Response<ValuePoolFlowRoundedAmountSummaryResponse>, Status> {
    let request = request.into_inner();
    validate_rounded_amount_summary_request(&request)?;
    let pools = PoolFilter::try_from(request.pools)?;
    let max_rows = clamp_max_entries(
        request.max_rows,
        DEFAULT_ROUNDED_AMOUNT_ROWS,
        MAX_ROUNDED_AMOUNT_ROWS,
    );
    let backfill_coverage =
        ValuePoolFlowHistoryConsumer::backfill_coverage(materialized_view_store)
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let tail_coverage = ValuePoolFlowHistoryConsumer::tail_coverage(materialized_view_store)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let chain_epoch = fetch_current_chain_epoch(wallet_client).await?;
    let visible_tip_height = visible_tip_height(&chain_epoch)?;
    let requested_range_complete =
        coverage_reaches_visible_tip(backfill_coverage, tail_coverage, visible_tip_height);
    let rows = read_rounded_amount_summary_blocking(
        materialized_view_store,
        request.start_time_unix_seconds,
        request.end_time_unix_seconds,
        pools,
        request.minimum_raw_amount_zat,
        request.maximum_raw_amount_zat,
        request.rounding_quantum_zat,
        request.minimum_event_count,
        max_rows,
    )
    .await?;
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_VALUE_POOL_FLOW_ROUNDED_AMOUNT_SUMMARY_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;

    Ok(Response::new(ValuePoolFlowRoundedAmountSummaryResponse {
        freshness: Some(freshness),
        rows,
        coverage: Some(map_coverage(
            backfill_coverage,
            tail_coverage,
            requested_range_complete,
        )),
    }))
}

async fn fetch_current_chain_epoch(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
) -> Result<zinder_proto::v1::wallet::ChainEpoch, Status> {
    wallet_client
        .visible_tip_block(Request::new(VisibleTipBlockRequest { at_epoch_id: None }))
        .await?
        .into_inner()
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.chain_view.chain_epoch missing").into()
        })
}

fn visible_tip_height(chain_epoch: &zinder_proto::v1::wallet::ChainEpoch) -> Result<u32, Status> {
    chain_epoch
        .visible_tip
        .as_ref()
        .map(|tip| tip.height)
        .ok_or_else(|| ExplorerError::internal("ChainEpoch.visible_tip missing").into())
}

async fn history_total_count(
    materialized_view_store: &MaterializedViewStore,
    filter: FlowFilter,
    count_domain_complete: bool,
) -> Result<Option<u64>, Status> {
    if !count_domain_complete {
        return Ok(None);
    }
    let materialized_view_store = materialized_view_store.clone();
    let count = tokio::task::spawn_blocking(move || -> Result<u64, Status> {
        materialized_view_store
            .count_consumer_rows_matching(
                zinder_materialized_views::VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY,
                |key, payload| {
                    let event = ValuePoolFlowHistoryConsumer::decode_event(key, payload)
                        .map_err(|error| error.to_string())?;
                    filter.matches(&event).map_err(|error| error.to_string())
                },
            )
            .map_err(|error| Status::from(ExplorerError::internal(error.to_string())))
    })
    .await
    .map_err(|error| ExplorerError::internal(format!("value-pool flow count failed: {error}")))??;
    Ok(Some(count))
}

async fn read_history_page_blocking(
    materialized_view_store: &MaterializedViewStore,
    page_size: u32,
    anchor: Option<[u8; zinder_materialized_views::VALUE_POOL_FLOW_HISTORY_KEY_LEN]>,
    filter: FlowFilter,
) -> Result<HistoryPage, Status> {
    let materialized_view_store = materialized_view_store.clone();
    tokio::task::spawn_blocking(move || {
        read_history_page(
            &materialized_view_store,
            page_size,
            anchor.as_ref(),
            &filter,
        )
    })
    .await
    .map_err(|error| {
        ExplorerError::internal(format!("value-pool flow history scan failed: {error}"))
    })?
}

struct FlowEventsInRange {
    events: Vec<ProjectedEvent>,
    scanned_event_count: u32,
    scan_limit_reached: bool,
    event_limit_reached: bool,
}

#[derive(Clone, Copy)]
struct FlowEventsInRangeQuery {
    start_time_unix_seconds: i64,
    end_time_unix_seconds: i64,
    filter: FlowFilter,
    maximum_amount_zat: Option<u64>,
    max_events: u32,
}

async fn read_flow_events_in_range_blocking(
    materialized_view_store: &MaterializedViewStore,
    query: FlowEventsInRangeQuery,
) -> Result<FlowEventsInRange, Status> {
    let materialized_view_store = materialized_view_store.clone();
    tokio::task::spawn_blocking(move || {
        let range_events = ValuePoolFlowHistoryConsumer::events_in_time_range(
            &materialized_view_store,
            query.start_time_unix_seconds,
            query.end_time_unix_seconds,
            MAX_HISTORY_SCANNED_EVENTS.saturating_add(1),
        )
        .map_err(|error| Status::from(ExplorerError::internal(error.to_string())))?;
        select_flow_events_in_range(
            range_events,
            query.filter,
            query.maximum_amount_zat,
            query.max_events,
        )
    })
    .await
    .map_err(|error| {
        ExplorerError::internal(format!("value-pool flow range scan failed: {error}"))
    })?
}

fn select_flow_events_in_range(
    mut range_events: Vec<ProjectedEvent>,
    filter: FlowFilter,
    maximum_amount_zat: Option<u64>,
    max_events: u32,
) -> Result<FlowEventsInRange, Status> {
    let scan_limit_reached = range_events.len() > MAX_HISTORY_SCANNED_EVENTS;
    range_events.truncate(MAX_HISTORY_SCANNED_EVENTS);
    let scanned_event_count = u32::try_from(range_events.len()).unwrap_or(u32::MAX);
    let max_events = usize::try_from(max_events).unwrap_or(usize::MAX);
    let mut selected_events = Vec::with_capacity(max_events);
    let mut event_limit_reached = false;
    for event in range_events {
        if !filter
            .matches(&event)
            .map_err(|error| Status::from(ExplorerError::internal(error.to_string())))?
        {
            continue;
        }
        let amount_zat = event
            .amount_zat()
            .map_err(|error| Status::from(ExplorerError::internal(error.to_string())))?;
        if maximum_amount_zat.is_some_and(|maximum| amount_zat > maximum) {
            continue;
        }
        if selected_events.len() >= max_events {
            event_limit_reached = true;
            continue;
        }
        selected_events.push(event);
    }
    Ok(FlowEventsInRange {
        events: selected_events,
        scanned_event_count,
        scan_limit_reached,
        event_limit_reached,
    })
}

async fn read_summary_buckets_blocking(
    materialized_view_store: &MaterializedViewStore,
    start_time_unix_seconds: i64,
    end_time_unix_seconds: i64,
    pools: PoolFilter,
    resolution: SummaryResolution,
) -> Result<Vec<ValuePoolFlowSummaryBucket>, Status> {
    let materialized_view_store = materialized_view_store.clone();
    tokio::task::spawn_blocking(move || {
        let events = ValuePoolFlowHistoryConsumer::events_in_time_range(
            &materialized_view_store,
            start_time_unix_seconds,
            end_time_unix_seconds,
            MAX_SUMMARY_SCANNED_EVENTS.saturating_add(1),
        )
        .map_err(|error| Status::from(ExplorerError::internal(error.to_string())))?;
        if events.len() > MAX_SUMMARY_SCANNED_EVENTS {
            return Err(ExplorerError::invalid_request(
                "value-pool flow summary range exceeds the server event scan limit",
            )
            .into());
        }
        summarize_events(events, pools, resolution)
    })
    .await
    .map_err(|error| {
        ExplorerError::internal(format!("value-pool flow summary scan failed: {error}"))
    })?
}

async fn read_amount_threshold_summary_blocking(
    materialized_view_store: &MaterializedViewStore,
    start_time_unix_seconds: i64,
    end_time_unix_seconds: i64,
    pools: PoolFilter,
    minimum_amounts_zat: Vec<u64>,
) -> Result<Vec<ValuePoolFlowAmountThresholdSummaryRow>, Status> {
    let materialized_view_store = materialized_view_store.clone();
    tokio::task::spawn_blocking(move || {
        summarize_amount_thresholds(
            &materialized_view_store,
            start_time_unix_seconds,
            end_time_unix_seconds,
            pools,
            minimum_amounts_zat,
        )
    })
    .await
    .map_err(|error| {
        ExplorerError::internal(format!(
            "value-pool flow amount-threshold summary scan failed: {error}"
        ))
    })?
}

#[allow(
    clippy::too_many_arguments,
    reason = "Arguments mirror the bounded native request."
)]
async fn read_rounded_amount_summary_blocking(
    materialized_view_store: &MaterializedViewStore,
    start_time_unix_seconds: i64,
    end_time_unix_seconds: i64,
    pools: PoolFilter,
    minimum_raw_amount_zat: u64,
    maximum_raw_amount_zat: Option<u64>,
    rounding_quantum_zat: u64,
    minimum_event_count: u64,
    max_rows: u32,
) -> Result<Vec<ValuePoolFlowRoundedAmountSummaryRow>, Status> {
    let materialized_view_store = materialized_view_store.clone();
    tokio::task::spawn_blocking(move || {
        let events = ValuePoolFlowHistoryConsumer::events_in_time_range(
            &materialized_view_store,
            start_time_unix_seconds,
            end_time_unix_seconds,
            MAX_SUMMARY_SCANNED_EVENTS.saturating_add(1),
        )
        .map_err(|error| Status::from(ExplorerError::internal(error.to_string())))?;
        if events.len() > MAX_SUMMARY_SCANNED_EVENTS {
            return Err(ExplorerError::invalid_request(
                "value-pool rounded-amount summary range exceeds the server event scan limit",
            )
            .into());
        }
        summarize_rounded_amounts(
            events,
            pools,
            minimum_raw_amount_zat,
            maximum_raw_amount_zat,
            rounding_quantum_zat,
            minimum_event_count,
            max_rows,
        )
    })
    .await
    .map_err(|error| {
        ExplorerError::internal(format!(
            "value-pool rounded-amount summary scan failed: {error}"
        ))
    })?
}

struct HistoryPage {
    rows: Vec<ValuePoolFlowHistoryRow>,
    cursor_key: Option<[u8; zinder_materialized_views::VALUE_POOL_FLOW_HISTORY_KEY_LEN]>,
    has_more: bool,
    scanned_event_count: u32,
    scan_limit_reached: bool,
}

fn read_history_page(
    materialized_view_store: &MaterializedViewStore,
    page_size: u32,
    anchor: Option<&[u8; zinder_materialized_views::VALUE_POOL_FLOW_HISTORY_KEY_LEN]>,
    filter: &FlowFilter,
) -> Result<HistoryPage, Status> {
    let rows = ValuePoolFlowHistoryConsumer::read_page_after(
        materialized_view_store,
        anchor,
        MAX_HISTORY_SCANNED_EVENTS,
    )
    .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let mut selected_rows = Vec::with_capacity(usize::try_from(page_size).unwrap_or(usize::MAX));
    let mut last_scanned_key = None;
    let mut has_extra_event = false;
    let mut scanned_event_count = 0_u32;
    for row in rows {
        scanned_event_count = scanned_event_count.saturating_add(1);
        last_scanned_key = Some(*row.continuation_key());
        if !filter
            .matches(&row.event)
            .map_err(|error| ExplorerError::internal(error.to_string()))?
        {
            continue;
        }
        if u32::try_from(selected_rows.len()).unwrap_or(u32::MAX) >= page_size {
            has_extra_event = true;
            break;
        }
        selected_rows.push(row);
    }
    let scan_limit_reached = !has_extra_event
        && usize::try_from(scanned_event_count).unwrap_or(usize::MAX) == MAX_HISTORY_SCANNED_EVENTS;
    let has_more = has_extra_event || scan_limit_reached;
    let cursor_key = if has_extra_event {
        selected_rows.last().map(|row| *row.continuation_key())
    } else if scan_limit_reached {
        last_scanned_key
    } else {
        None
    };
    Ok(HistoryPage {
        rows: selected_rows,
        cursor_key,
        has_more,
        scanned_event_count,
        scan_limit_reached,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FlowFilter {
    direction_mask: u8,
    pool_mask: u8,
    minimum_amount_zat: u64,
}

impl TryFrom<ValuePoolFlowFilter> for FlowFilter {
    type Error = Status;

    fn try_from(filter: ValuePoolFlowFilter) -> Result<Self, Self::Error> {
        Ok(Self {
            direction_mask: direction_mask(filter.directions)?,
            pool_mask: pool_mask(filter.pools)?,
            minimum_amount_zat: filter.minimum_amount_zat,
        })
    }
}

impl FlowFilter {
    fn matches(
        self,
        event: &ProjectedEvent,
    ) -> Result<bool, zinder_materialized_views::ValuePoolFlowHistoryConsumerError> {
        if event.is_coinbase() {
            return Ok(false);
        }
        let direction = event.direction()?;
        let pool = event.pool();
        Ok(
            (self.direction_mask == 0 || self.direction_mask & direction_bit(direction) != 0)
                && (self.pool_mask == 0 || self.pool_mask & pool_bit(pool) != 0)
                && event.amount_zat()? >= self.minimum_amount_zat,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PoolFilter(u8);

impl TryFrom<Vec<i32>> for PoolFilter {
    type Error = Status;

    fn try_from(pools: Vec<i32>) -> Result<Self, Self::Error> {
        Ok(Self(pool_mask(pools)?))
    }
}

impl PoolFilter {
    fn matches(self, event: &ProjectedEvent) -> bool {
        self.0 == 0 || self.0 & pool_bit(event.pool()) != 0
    }
}

fn validate_minimum_amounts(minimum_amounts_zat: &[u64]) -> Result<(), Status> {
    if minimum_amounts_zat.len() > MAX_AMOUNT_THRESHOLDS {
        return Err(ExplorerError::invalid_request(
            "minimum_amounts_zat cannot contain more than 32 thresholds",
        )
        .into());
    }
    if minimum_amounts_zat
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err(ExplorerError::invalid_request(
            "minimum_amounts_zat must be strictly increasing and unique",
        )
        .into());
    }
    Ok(())
}

fn validate_flow_events_in_range_request(
    request: &ValuePoolFlowEventsInRangeRequest,
) -> Result<(), Status> {
    if request.start_time_unix_seconds >= request.end_time_unix_seconds {
        return Err(ExplorerError::invalid_request(
            "start_time_unix_seconds must be less than end_time_unix_seconds",
        )
        .into());
    }
    if request
        .maximum_amount_zat
        .is_some_and(|maximum| maximum < request.minimum_amount_zat)
    {
        return Err(ExplorerError::invalid_request(
            "maximum_amount_zat must be greater than or equal to minimum_amount_zat",
        )
        .into());
    }
    Ok(())
}

fn validate_rounded_amount_summary_request(
    request: &ValuePoolFlowRoundedAmountSummaryRequest,
) -> Result<(), Status> {
    if request.start_time_unix_seconds >= request.end_time_unix_seconds {
        return Err(ExplorerError::invalid_request(
            "start_time_unix_seconds must be less than end_time_unix_seconds",
        )
        .into());
    }
    if request.rounding_quantum_zat == 0 {
        return Err(ExplorerError::invalid_request("rounding_quantum_zat must be positive").into());
    }
    if request
        .maximum_raw_amount_zat
        .is_some_and(|maximum| maximum <= request.minimum_raw_amount_zat)
    {
        return Err(ExplorerError::invalid_request(
            "maximum_raw_amount_zat must be greater than minimum_raw_amount_zat",
        )
        .into());
    }
    Ok(())
}

fn direction_mask(directions: Vec<i32>) -> Result<u8, Status> {
    directions.into_iter().try_fold(0_u8, |mask, encoded| {
        let direction = ValuePoolFlowDirection::try_from(encoded)
            .map_err(|_| ExplorerError::invalid_request("unknown value-pool flow direction"))?;
        match direction {
            ValuePoolFlowDirection::Unspecified => Err(ExplorerError::invalid_request(
                "unspecified value-pool flow direction cannot be used as a filter",
            )
            .into()),
            ValuePoolFlowDirection::Shield => Ok(mask | direction_bit(ProjectedDirection::Shield)),
            ValuePoolFlowDirection::Deshield => {
                Ok(mask | direction_bit(ProjectedDirection::Deshield))
            }
        }
    })
}

fn pool_mask(pools: Vec<i32>) -> Result<u8, Status> {
    pools.into_iter().try_fold(0_u8, |mask, encoded| {
        let pool = ValuePoolFlowPool::try_from(encoded)
            .map_err(|_| ExplorerError::invalid_request("unknown value-pool flow pool"))?;
        let pool = match pool {
            ValuePoolFlowPool::Unspecified => {
                return Err(ExplorerError::invalid_request(
                    "unspecified value-pool flow pool cannot be used as a filter",
                )
                .into());
            }
            ValuePoolFlowPool::Sprout => ProjectedPool::Sprout,
            ValuePoolFlowPool::Sapling => ProjectedPool::Sapling,
            ValuePoolFlowPool::Orchard => ProjectedPool::Orchard,
            ValuePoolFlowPool::Ironwood => ProjectedPool::Ironwood,
            ValuePoolFlowPool::Mixed => ProjectedPool::Mixed,
        };
        Ok(mask | pool_bit(pool))
    })
}

const fn direction_bit(direction: ProjectedDirection) -> u8 {
    match direction {
        ProjectedDirection::Shield => 1,
        ProjectedDirection::Deshield => 2,
    }
}

const fn pool_bit(pool: ProjectedPool) -> u8 {
    match pool {
        ProjectedPool::Sprout => 1,
        ProjectedPool::Sapling => 2,
        ProjectedPool::Orchard => 4,
        ProjectedPool::Ironwood => 8,
        ProjectedPool::Mixed => 16,
    }
}

fn encode_cursor(
    anchor: &[u8; zinder_materialized_views::VALUE_POOL_FLOW_HISTORY_KEY_LEN],
    filter: &FlowFilter,
) -> Vec<u8> {
    let mut cursor = Vec::with_capacity(CURSOR_LEN);
    cursor.extend_from_slice(CURSOR_PREFIX);
    cursor.push(filter.direction_mask);
    cursor.push(filter.pool_mask);
    cursor.extend_from_slice(&filter.minimum_amount_zat.to_be_bytes());
    cursor.extend_from_slice(anchor);
    cursor
}

fn decode_cursor(
    cursor: &[u8],
    filter: &FlowFilter,
) -> Result<[u8; zinder_materialized_views::VALUE_POOL_FLOW_HISTORY_KEY_LEN], Status> {
    if cursor.len() != CURSOR_LEN || cursor.get(..CURSOR_PREFIX.len()) != Some(CURSOR_PREFIX) {
        return Err(ExplorerError::invalid_request("invalid value-pool flow cursor").into());
    }
    let direction_mask = cursor[CURSOR_PREFIX.len()];
    let pool_mask = cursor[CURSOR_PREFIX.len() + 1];
    let minimum_amount_offset = CURSOR_PREFIX.len() + 2;
    let minimum_amount_zat = u64::from_be_bytes(
        cursor[minimum_amount_offset..minimum_amount_offset + 8]
            .try_into()
            .map_err(|_| ExplorerError::invalid_request("invalid value-pool flow cursor"))?,
    );
    if (direction_mask, pool_mask, minimum_amount_zat)
        != (
            filter.direction_mask,
            filter.pool_mask,
            filter.minimum_amount_zat,
        )
    {
        return Err(ExplorerError::invalid_request(
            "value-pool flow cursor filter does not match request filter",
        )
        .into());
    }
    cursor[CURSOR_PREFIX.len() + CURSOR_FILTER_LEN..]
        .try_into()
        .map_err(|_| ExplorerError::invalid_request("invalid value-pool flow cursor").into())
}

fn map_event(event: ProjectedEvent) -> Result<ValuePoolFlowEvent, Status> {
    let direction = match event
        .direction()
        .map_err(|error| ExplorerError::internal(error.to_string()))?
    {
        ProjectedDirection::Shield => ValuePoolFlowDirection::Shield,
        ProjectedDirection::Deshield => ValuePoolFlowDirection::Deshield,
    };
    let pool = match event.pool() {
        ProjectedPool::Sprout => ValuePoolFlowPool::Sprout,
        ProjectedPool::Sapling => ValuePoolFlowPool::Sapling,
        ProjectedPool::Orchard => ValuePoolFlowPool::Orchard,
        ProjectedPool::Ironwood => ValuePoolFlowPool::Ironwood,
        ProjectedPool::Mixed => ValuePoolFlowPool::Mixed,
    };
    Ok(ValuePoolFlowEvent {
        transaction_id: encode_rpc_transaction_id_hex(event.transaction_id),
        block_height: event.block_height.value(),
        block_time_unix_seconds: event.block_time_unix_seconds,
        transaction_index_in_block: event.transaction_index_in_block,
        direction: direction as i32,
        pool: pool as i32,
        amount_zat: event
            .amount_zat()
            .map_err(|error| ExplorerError::internal(error.to_string()))?,
        pool_balances: Some(TransactionIntrinsicValueBalances {
            sprout_zat: event.pool_balances.sprout_zat,
            sapling_zat: event.pool_balances.sapling_zat,
            orchard_zat: event.pool_balances.orchard_zat,
            ironwood_zat: event.pool_balances.ironwood_zat,
        }),
    })
}

fn map_coverage(
    backfill: Option<ValuePoolFlowBackfillCoverage>,
    tail: Option<ValuePoolFlowTailCoverage>,
    requested_range_complete: bool,
) -> ValuePoolFlowCoverage {
    ValuePoolFlowCoverage {
        historical_from_height: backfill.map(|coverage| coverage.complete_from_height.value()),
        historical_through_height: backfill
            .map(|coverage| coverage.complete_through_height.value()),
        historical_from_time_unix_seconds: backfill
            .map(|coverage| coverage.complete_from_time_unix_seconds),
        historical_through_time_unix_seconds: backfill
            .map(|coverage| coverage.complete_through_time_unix_seconds),
        live_tail_from_height: tail.map(|coverage| coverage.boundary_height.value()),
        live_tail_through_height: tail
            .and_then(|coverage| coverage.complete_through_height)
            .map(zinder_core::BlockHeight::value),
        live_tail_through_time_unix_seconds: tail
            .and_then(|coverage| coverage.complete_through_time_unix_seconds),
        requested_range_complete,
    }
}

fn coverage_reaches_visible_tip(
    backfill: Option<ValuePoolFlowBackfillCoverage>,
    tail: Option<ValuePoolFlowTailCoverage>,
    visible_tip_height: u32,
) -> bool {
    let Some(backfill) = backfill else {
        return false;
    };
    if backfill.complete_from_height != zinder_core::BlockHeight::new(1) {
        return false;
    }
    if backfill.complete_through_height.value() >= visible_tip_height {
        return true;
    }
    let Some(tail) = tail else {
        return false;
    };
    backfill.complete_through_height.next() == Some(tail.boundary_height)
        && tail
            .complete_through_height
            .is_some_and(|through| through.value() >= visible_tip_height)
}

fn should_compute_total_matching_events(
    include_total_count: bool,
    backfill: Option<ValuePoolFlowBackfillCoverage>,
    tail: Option<ValuePoolFlowTailCoverage>,
    visible_tip_height: u32,
) -> bool {
    include_total_count && coverage_reaches_visible_tip(backfill, tail, visible_tip_height)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SummaryResolution {
    Hour,
    Day,
}

impl TryFrom<i32> for SummaryResolution {
    type Error = Status;

    fn try_from(encoded: i32) -> Result<Self, Self::Error> {
        match ValuePoolFlowSummaryResolution::try_from(encoded).map_err(|_| {
            ExplorerError::invalid_request("unknown value-pool flow summary resolution")
        })? {
            ValuePoolFlowSummaryResolution::Unspecified => Err(ExplorerError::invalid_request(
                "value-pool flow summary resolution is required",
            )
            .into()),
            ValuePoolFlowSummaryResolution::Hour => Ok(Self::Hour),
            ValuePoolFlowSummaryResolution::Day => Ok(Self::Day),
        }
    }
}

impl SummaryResolution {
    const fn seconds(self) -> i64 {
        match self {
            Self::Hour => 60 * 60,
            Self::Day => 24 * 60 * 60,
        }
    }
}

#[derive(Default)]
struct BucketTotals {
    shield_event_count: u64,
    deshield_event_count: u64,
    shield_amount_zat: u64,
    deshield_amount_zat: u64,
}

fn summarize_events(
    events: Vec<ProjectedEvent>,
    pools: PoolFilter,
    resolution: SummaryResolution,
) -> Result<Vec<ValuePoolFlowSummaryBucket>, Status> {
    let mut totals = BTreeMap::<i64, BucketTotals>::new();
    for event in events
        .into_iter()
        .filter(|event| !event.is_coinbase() && pools.matches(event))
    {
        let bucket_start_time_unix_seconds = event
            .block_time_unix_seconds
            .div_euclid(resolution.seconds())
            * resolution.seconds();
        let amount_zat = event
            .amount_zat()
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
        let bucket = totals.entry(bucket_start_time_unix_seconds).or_default();
        match event
            .direction()
            .map_err(|error| ExplorerError::internal(error.to_string()))?
        {
            ProjectedDirection::Shield => {
                bucket.shield_event_count =
                    bucket.shield_event_count.checked_add(1).ok_or_else(|| {
                        ExplorerError::internal("value-pool flow shield event count overflow")
                    })?;
                bucket.shield_amount_zat = bucket
                    .shield_amount_zat
                    .checked_add(amount_zat)
                    .ok_or_else(|| {
                        ExplorerError::internal("value-pool flow shield amount overflow")
                    })?;
            }
            ProjectedDirection::Deshield => {
                bucket.deshield_event_count =
                    bucket.deshield_event_count.checked_add(1).ok_or_else(|| {
                        ExplorerError::internal("value-pool flow deshield event count overflow")
                    })?;
                bucket.deshield_amount_zat = bucket
                    .deshield_amount_zat
                    .checked_add(amount_zat)
                    .ok_or_else(|| {
                        ExplorerError::internal("value-pool flow deshield amount overflow")
                    })?;
            }
        }
    }
    Ok(totals
        .into_iter()
        .map(
            |(bucket_start_time_unix_seconds, totals)| ValuePoolFlowSummaryBucket {
                bucket_start_time_unix_seconds,
                shield_event_count: totals.shield_event_count,
                deshield_event_count: totals.deshield_event_count,
                shield_amount_zat: totals.shield_amount_zat,
                deshield_amount_zat: totals.deshield_amount_zat,
            },
        )
        .collect())
}

fn summarize_amount_thresholds(
    materialized_view_store: &MaterializedViewStore,
    start_time_unix_seconds: i64,
    end_time_unix_seconds: i64,
    pools: PoolFilter,
    minimum_amounts_zat: Vec<u64>,
) -> Result<Vec<ValuePoolFlowAmountThresholdSummaryRow>, Status> {
    if minimum_amounts_zat.is_empty() {
        return Ok(Vec::new());
    }
    let mut thresholds = minimum_amounts_zat
        .into_iter()
        .map(
            |minimum_amount_zat| ValuePoolFlowAmountThresholdSummaryRow {
                minimum_amount_zat,
                shield_event_count: 0,
                deshield_event_count: 0,
                shield_amount_zat: 0,
                deshield_amount_zat: 0,
            },
        )
        .collect::<Vec<_>>();
    ValuePoolFlowHistoryConsumer::visit_events_in_time_range(
        materialized_view_store,
        start_time_unix_seconds,
        end_time_unix_seconds,
        |event| {
            if event.is_coinbase() || !pools.matches(&event) {
                return Ok(());
            }
            let amount_zat = event.amount_zat().map_err(|error| error.to_string())?;
            let direction = event.direction().map_err(|error| error.to_string())?;
            for threshold in thresholds
                .iter_mut()
                .take_while(|threshold| threshold.minimum_amount_zat <= amount_zat)
            {
                add_amount_threshold_event(threshold, direction, amount_zat)
                    .map_err(str::to_owned)?;
            }
            Ok(())
        },
    )
    .map_err(|error| ExplorerError::internal(error.to_string()))?;
    Ok(thresholds)
}

#[derive(Default)]
struct RoundedAmountTotals {
    shield_event_count: u64,
    deshield_event_count: u64,
}

#[allow(
    clippy::too_many_arguments,
    reason = "Arguments mirror the bounded native request."
)]
fn summarize_rounded_amounts(
    events: Vec<ProjectedEvent>,
    pools: PoolFilter,
    minimum_raw_amount_zat: u64,
    maximum_raw_amount_zat: Option<u64>,
    rounding_quantum_zat: u64,
    minimum_event_count: u64,
    max_rows: u32,
) -> Result<Vec<ValuePoolFlowRoundedAmountSummaryRow>, Status> {
    let mut totals = BTreeMap::<u64, RoundedAmountTotals>::new();
    for event in events
        .into_iter()
        .filter(|event| !event.is_coinbase() && pools.matches(event))
    {
        let amount_zat = event
            .amount_zat()
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
        if amount_zat < minimum_raw_amount_zat
            || maximum_raw_amount_zat.is_some_and(|maximum| amount_zat >= maximum)
        {
            continue;
        }
        let rounded_amount_zat = round_amount_to_quantum(amount_zat, rounding_quantum_zat)?;
        let bucket = totals.entry(rounded_amount_zat).or_default();
        let count = match event
            .direction()
            .map_err(|error| ExplorerError::internal(error.to_string()))?
        {
            ProjectedDirection::Shield => &mut bucket.shield_event_count,
            ProjectedDirection::Deshield => &mut bucket.deshield_event_count,
        };
        *count = count.checked_add(1).ok_or_else(|| {
            ExplorerError::internal("value-pool rounded-amount event count overflow")
        })?;
    }

    let mut rows = totals
        .into_iter()
        .map(|(rounded_amount_zat, totals)| {
            let total_event_count = totals
                .shield_event_count
                .checked_add(totals.deshield_event_count)
                .ok_or_else(|| {
                    ExplorerError::internal("value-pool rounded-amount total count overflow")
                })?;
            Ok((
                total_event_count,
                ValuePoolFlowRoundedAmountSummaryRow {
                    rounded_amount_zat,
                    shield_event_count: totals.shield_event_count,
                    deshield_event_count: totals.deshield_event_count,
                },
            ))
        })
        .collect::<Result<Vec<_>, Status>>()?;
    rows.retain(|(total_event_count, _)| *total_event_count >= minimum_event_count);
    rows.sort_unstable_by(|(left_count, left), (right_count, right)| {
        right_count
            .cmp(left_count)
            .then_with(|| left.rounded_amount_zat.cmp(&right.rounded_amount_zat))
    });
    rows.truncate(usize::try_from(max_rows).unwrap_or(usize::MAX));
    Ok(rows.into_iter().map(|(_, row)| row).collect())
}

fn round_amount_to_quantum(amount_zat: u64, quantum_zat: u64) -> Result<u64, Status> {
    let quotient = amount_zat / quantum_zat;
    let remainder = amount_zat % quantum_zat;
    let rounds_up = remainder >= quantum_zat / 2 + quantum_zat % 2;
    let rounded_quotient = quotient
        .checked_add(u64::from(rounds_up))
        .ok_or_else(|| ExplorerError::internal("value-pool rounded amount quotient overflow"))?;
    rounded_quotient
        .checked_mul(quantum_zat)
        .ok_or_else(|| ExplorerError::internal("value-pool rounded amount overflow").into())
}

fn add_amount_threshold_event(
    row: &mut ValuePoolFlowAmountThresholdSummaryRow,
    direction: ProjectedDirection,
    amount_zat: u64,
) -> Result<(), &'static str> {
    match direction {
        ProjectedDirection::Shield => {
            let event_count = row
                .shield_event_count
                .checked_add(1)
                .ok_or("value-pool flow threshold shield event count overflow")?;
            let amount_sum = row
                .shield_amount_zat
                .checked_add(amount_zat)
                .ok_or("value-pool flow threshold shield amount overflow")?;
            row.shield_event_count = event_count;
            row.shield_amount_zat = amount_sum;
        }
        ProjectedDirection::Deshield => {
            let event_count = row
                .deshield_event_count
                .checked_add(1)
                .ok_or("value-pool flow threshold deshield event count overflow")?;
            let amount_sum = row
                .deshield_amount_zat
                .checked_add(amount_zat)
                .ok_or("value-pool flow threshold deshield amount overflow")?;
            row.deshield_event_count = event_count;
            row.deshield_amount_zat = amount_sum;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use tempfile::tempdir;
    use tonic::Code;
    use zinder_core::{BlockHeight, TransactionId, TransactionIntrinsicValueBalances};
    use zinder_materialized_views::{MaterializedViewStoreOptions, VALUE_POOL_FLOW_HISTORY_SCHEMA};
    use zinder_store::RocksDbResourceBudget;

    use super::*;

    fn event(time: i64, balances: TransactionIntrinsicValueBalances) -> ProjectedEvent {
        ProjectedEvent {
            transaction_id: TransactionId::from_bytes([7; 32]),
            block_height: BlockHeight::new(10),
            block_time_unix_seconds: time,
            transaction_index_in_block: 1,
            pool_balances: balances,
        }
    }

    fn coinbase_event(time: i64, balances: TransactionIntrinsicValueBalances) -> ProjectedEvent {
        ProjectedEvent {
            transaction_index_in_block: 0,
            ..event(time, balances)
        }
    }

    #[test]
    fn cursor_rejects_filter_changes() {
        let filter = FlowFilter {
            direction_mask: direction_bit(ProjectedDirection::Shield),
            pool_mask: pool_bit(ProjectedPool::Sapling),
            minimum_amount_zat: 12,
        };
        let cursor = encode_cursor(
            &[3; zinder_materialized_views::VALUE_POOL_FLOW_HISTORY_KEY_LEN],
            &filter,
        );
        assert!(decode_cursor(&cursor, &filter).is_ok());
        assert!(
            decode_cursor(
                &cursor,
                &FlowFilter {
                    minimum_amount_zat: 13,
                    ..filter
                },
            )
            .is_err()
        );
    }

    #[test]
    fn summary_uses_utc_hour_boundaries_and_pool_filters() -> Result<(), Status> {
        let buckets = summarize_events(
            vec![
                coinbase_event(
                    7_199,
                    TransactionIntrinsicValueBalances::new(0, -1_250_000_000, 0, 0),
                ),
                event(7_199, TransactionIntrinsicValueBalances::new(0, -7, 0, 0)),
                event(7_200, TransactionIntrinsicValueBalances::new(0, 0, 11, 0)),
                event(7_300, TransactionIntrinsicValueBalances::new(0, -13, 0, 0)),
            ],
            PoolFilter(pool_bit(ProjectedPool::Sapling)),
            SummaryResolution::Hour,
        )?;
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].bucket_start_time_unix_seconds, 3_600);
        assert_eq!(buckets[0].shield_amount_zat, 7);
        assert_eq!(buckets[1].bucket_start_time_unix_seconds, 7_200);
        assert_eq!(buckets[1].shield_amount_zat, 13);
        assert_eq!(buckets[1].deshield_amount_zat, 0);
        Ok(())
    }

    #[test]
    fn history_filters_exclude_coinbase_flow_rows() {
        let filter = FlowFilter {
            direction_mask: 0,
            pool_mask: 0,
            minimum_amount_zat: 0,
        };

        assert!(
            filter
                .matches(&coinbase_event(
                    7_199,
                    TransactionIntrinsicValueBalances::new(0, 0, 0, -125_000_000),
                ))
                .is_ok_and(|matches| !matches)
        );
        assert!(
            filter
                .matches(&event(
                    7_199,
                    TransactionIntrinsicValueBalances::new(0, 0, -125_000_000, 0),
                ))
                .is_ok_and(|matches| matches)
        );
    }

    #[test]
    fn flow_event_range_validation_requires_ordered_time_and_amount_bounds() {
        let valid = ValuePoolFlowEventsInRangeRequest {
            start_time_unix_seconds: 10,
            end_time_unix_seconds: 20,
            minimum_amount_zat: 100,
            maximum_amount_zat: Some(200),
            ..Default::default()
        };
        assert!(validate_flow_events_in_range_request(&valid).is_ok());
        assert!(
            validate_flow_events_in_range_request(&ValuePoolFlowEventsInRangeRequest {
                end_time_unix_seconds: 10,
                ..valid.clone()
            })
            .is_err()
        );
        assert!(
            validate_flow_events_in_range_request(&ValuePoolFlowEventsInRangeRequest {
                maximum_amount_zat: Some(99),
                ..valid
            })
            .is_err()
        );
    }

    #[test]
    fn flow_event_range_completeness_requires_the_indexed_tip_identity() {
        let visible_tip = zinder_proto::v1::wallet::BlockTip {
            height: 42,
            hash: "00".repeat(32),
        };
        let freshness = zinder_proto::v1::explorer::ExplorerFreshness {
            chain_view: Some(zinder_proto::v1::wallet::ChainView {
                indexed_tip: Some(zinder_proto::v1::wallet::IndexedTip {
                    tip: Some(visible_tip.clone()),
                    block_time_unix_seconds: 100,
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let matching_epoch = zinder_proto::v1::wallet::ChainEpoch {
            visible_tip: Some(visible_tip.clone()),
            ..Default::default()
        };
        assert!(indexed_tip_matches_chain_epoch(&freshness, &matching_epoch));

        let displaced_epoch = zinder_proto::v1::wallet::ChainEpoch {
            visible_tip: Some(zinder_proto::v1::wallet::BlockTip {
                hash: "11".repeat(32),
                ..visible_tip
            }),
            ..Default::default()
        };
        assert!(!indexed_tip_matches_chain_epoch(
            &freshness,
            &displaced_epoch
        ));
    }

    #[test]
    fn flow_event_range_preserves_inclusive_amount_bounds_and_reports_result_limits()
    -> Result<(), Status> {
        let filter = FlowFilter {
            direction_mask: direction_bit(ProjectedDirection::Deshield),
            pool_mask: pool_bit(ProjectedPool::Sapling),
            minimum_amount_zat: 100,
        };
        let selected = select_flow_events_in_range(
            vec![
                coinbase_event(100, TransactionIntrinsicValueBalances::new(0, 150, 0, 0)),
                event(101, TransactionIntrinsicValueBalances::new(0, -150, 0, 0)),
                event(102, TransactionIntrinsicValueBalances::new(0, 100, 0, 0)),
                event(103, TransactionIntrinsicValueBalances::new(0, 200, 0, 0)),
                event(104, TransactionIntrinsicValueBalances::new(0, 201, 0, 0)),
            ],
            filter,
            Some(200),
            1,
        )?;

        assert_eq!(selected.scanned_event_count, 5);
        assert!(!selected.scan_limit_reached);
        assert!(selected.event_limit_reached);
        assert_eq!(selected.events.len(), 1);
        assert!(
            selected.events[0]
                .amount_zat()
                .is_ok_and(|amount| amount == 100)
        );
        Ok(())
    }

    #[test]
    fn amount_threshold_validation_requires_strict_order_and_enforces_the_cap() {
        assert!(validate_minimum_amounts(&[]).is_ok());
        assert!(validate_minimum_amounts(&[0, 10, 20]).is_ok());
        assert!(validate_minimum_amounts(&[10, 10]).is_err());
        assert!(validate_minimum_amounts(&[20, 10]).is_err());
        assert!(validate_minimum_amounts(&[0; MAX_AMOUNT_THRESHOLDS + 1]).is_err());
    }

    #[test]
    fn amount_threshold_totals_are_checked_for_count_and_sum_overflow() {
        let mut threshold = ValuePoolFlowAmountThresholdSummaryRow {
            minimum_amount_zat: 10,
            ..Default::default()
        };
        assert!(add_amount_threshold_event(&mut threshold, ProjectedDirection::Shield, 12).is_ok());
        assert!(
            add_amount_threshold_event(&mut threshold, ProjectedDirection::Deshield, 20).is_ok()
        );
        assert_eq!(threshold.shield_event_count, 1);
        assert_eq!(threshold.shield_amount_zat, 12);
        assert_eq!(threshold.deshield_event_count, 1);
        assert_eq!(threshold.deshield_amount_zat, 20);

        threshold.shield_event_count = u64::MAX;
        assert!(add_amount_threshold_event(&mut threshold, ProjectedDirection::Shield, 1).is_err());
        threshold.shield_event_count = 0;
        threshold.shield_amount_zat = u64::MAX;
        assert!(add_amount_threshold_event(&mut threshold, ProjectedDirection::Shield, 1).is_err());
    }

    #[test]
    fn rounded_amount_validation_requires_a_range_and_positive_quantum() {
        let valid = ValuePoolFlowRoundedAmountSummaryRequest {
            start_time_unix_seconds: 10,
            end_time_unix_seconds: 20,
            minimum_raw_amount_zat: 100,
            maximum_raw_amount_zat: Some(200),
            rounding_quantum_zat: 10,
            ..Default::default()
        };
        assert!(validate_rounded_amount_summary_request(&valid).is_ok());
        assert!(
            validate_rounded_amount_summary_request(&ValuePoolFlowRoundedAmountSummaryRequest {
                rounding_quantum_zat: 0,
                ..valid.clone()
            })
            .is_err()
        );
        assert!(
            validate_rounded_amount_summary_request(&ValuePoolFlowRoundedAmountSummaryRequest {
                maximum_raw_amount_zat: Some(100),
                ..valid
            })
            .is_err()
        );
    }

    #[test]
    fn amount_rounding_uses_nearest_quantum_with_positive_ties_up() -> Result<(), Status> {
        assert_eq!(round_amount_to_quantum(1_499_999, 1_000_000)?, 1_000_000);
        assert_eq!(round_amount_to_quantum(1_500_000, 1_000_000)?, 2_000_000);
        assert_eq!(round_amount_to_quantum(1_500_001, 1_000_000)?, 2_000_000);
        assert_eq!(round_amount_to_quantum(7, 5)?, 5);
        assert_eq!(round_amount_to_quantum(8, 5)?, 10);
        assert!(round_amount_to_quantum(u64::MAX, 2).is_err());
        Ok(())
    }

    #[test]
    fn rounded_amount_summary_filters_sorts_and_preserves_direction_counts() -> Result<(), Status> {
        let mut events = Vec::new();
        events.extend((0..3).map(|_| {
            event(
                7_200,
                TransactionIntrinsicValueBalances::new(0, -2_500_000, 0, 0),
            )
        }));
        events.extend((0..2).map(|_| {
            event(
                7_200,
                TransactionIntrinsicValueBalances::new(0, -1_499_999, 0, 0),
            )
        }));
        events.extend((0..2).map(|_| {
            event(
                7_200,
                TransactionIntrinsicValueBalances::new(0, 1_500_000, 0, 0),
            )
        }));
        events.push(event(
            7_200,
            TransactionIntrinsicValueBalances::new(0, -500_000, 0, 0),
        ));

        let rows = summarize_rounded_amounts(
            events,
            PoolFilter(0),
            1_000_000,
            Some(3_000_000),
            1_000_000,
            2,
            3,
        )?;

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].rounded_amount_zat, 3_000_000);
        assert_eq!(rows[0].shield_event_count, 3);
        assert_eq!(rows[1].rounded_amount_zat, 1_000_000);
        assert_eq!(rows[1].shield_event_count, 2);
        assert_eq!(rows[2].rounded_amount_zat, 2_000_000);
        assert_eq!(rows[2].deshield_event_count, 2);
        Ok(())
    }

    #[test]
    fn full_domain_coverage_requires_joined_heights_through_visible_tip() {
        let backfill = ValuePoolFlowBackfillCoverage::new(
            BlockHeight::new(1),
            BlockHeight::new(100),
            1_700_000_000,
            1_700_100_000,
        );
        let tail = ValuePoolFlowTailCoverage {
            boundary_height: BlockHeight::new(101),
            complete_through_height: Some(BlockHeight::new(105)),
            complete_through_time_unix_seconds: Some(1_700_105_000),
        };
        let coverage = map_coverage(Some(backfill), Some(tail), true);
        assert!(coverage.requested_range_complete);

        let gap = ValuePoolFlowTailCoverage {
            boundary_height: BlockHeight::new(102),
            ..tail
        };
        assert!(!coverage_reaches_visible_tip(
            Some(backfill),
            Some(gap),
            105
        ));
    }

    #[test]
    fn non_monotonic_times_cannot_make_partial_height_coverage_complete() {
        let backfill = ValuePoolFlowBackfillCoverage::new(
            BlockHeight::new(1),
            BlockHeight::new(100),
            3_000,
            5_000,
        );
        let incomplete_tail = ValuePoolFlowTailCoverage {
            boundary_height: BlockHeight::new(101),
            complete_through_height: Some(BlockHeight::new(104)),
            complete_through_time_unix_seconds: Some(4_000),
        };

        assert!(!coverage_reaches_visible_tip(
            Some(backfill),
            Some(incomplete_tail),
            105,
        ));

        let complete_tail = ValuePoolFlowTailCoverage {
            complete_through_height: Some(BlockHeight::new(105)),
            complete_through_time_unix_seconds: Some(2_500),
            ..incomplete_tail
        };
        assert!(coverage_reaches_visible_tip(
            Some(backfill),
            Some(complete_tail),
            105,
        ));
    }

    #[test]
    fn exact_total_requires_request_and_full_domain_coverage() {
        let backfill = ValuePoolFlowBackfillCoverage::new(
            BlockHeight::new(1),
            BlockHeight::new(100),
            1_000,
            2_000,
        );
        let tail = ValuePoolFlowTailCoverage {
            boundary_height: BlockHeight::new(101),
            complete_through_height: Some(BlockHeight::new(105)),
            complete_through_time_unix_seconds: Some(2_500),
        };
        assert!(should_compute_total_matching_events(
            true,
            Some(backfill),
            Some(tail),
            105,
        ));
        assert!(!should_compute_total_matching_events(
            false,
            Some(backfill),
            Some(tail),
            105,
        ));
        assert!(!should_compute_total_matching_events(
            true,
            Some(backfill),
            Some(tail),
            106,
        ));
    }

    #[tokio::test]
    async fn blocking_scans_preserve_store_error_mapping_and_skip_inexact_counts()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let directory = tempdir()?;
        let store = MaterializedViewStore::open(
            directory.path(),
            MaterializedViewStoreOptions {
                sync_writes: false,
                consumers: &[VALUE_POOL_FLOW_HISTORY_SCHEMA],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                reorg_policy: None,
            },
        )?;
        let filter = FlowFilter {
            direction_mask: 0,
            pool_mask: 0,
            minimum_amount_zat: 0,
        };

        assert_eq!(history_total_count(&store, filter, false).await?, None);

        drop(store);
        let unconfigured_directory = tempdir()?;
        let unconfigured_store = MaterializedViewStore::open(
            unconfigured_directory.path(),
            MaterializedViewStoreOptions {
                sync_writes: false,
                consumers: &[],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                reorg_policy: None,
            },
        )?;
        let history_error = read_history_page_blocking(&unconfigured_store, 1, None, filter)
            .await
            .err()
            .ok_or("history scan unexpectedly succeeded")?;
        assert_eq!(history_error.code(), Code::Internal);
        let summary_error = read_summary_buckets_blocking(
            &unconfigured_store,
            1,
            2,
            PoolFilter(0),
            SummaryResolution::Hour,
        )
        .await
        .err()
        .ok_or("summary scan unexpectedly succeeded")?;
        assert_eq!(summary_error.code(), Code::Internal);
        assert!(
            read_amount_threshold_summary_blocking(
                &unconfigured_store,
                1,
                2,
                PoolFilter(0),
                Vec::new(),
            )
            .await?
            .is_empty()
        );
        let threshold_error = read_amount_threshold_summary_blocking(
            &unconfigured_store,
            1,
            2,
            PoolFilter(0),
            vec![0],
        )
        .await
        .err()
        .ok_or("amount-threshold summary scan unexpectedly succeeded")?;
        assert_eq!(threshold_error.code(), Code::Internal);
        Ok(())
    }
}
