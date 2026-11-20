//! `ExplorerQuery.OverviewSnapshot` handler.
//!
//! Composes one coherent point-in-time bundle that overview consumers
//! used to assemble from six independent RPCs (`MempoolSummary` +
//! `FeeSummary` + `ValuePoolSummary` + `BlockSummariesInRange` +
//! `RecentTransactions` + `MempoolEventCounts`). Consumer-side fan-out
//! lets per-card freshness diverge: the tile claims height N while the
//! list shows height N+50 because the calls land on different RPC
//! windows. This handler instead anchors every sub-field to the same
//! `WalletQuery.VisibleTipBlock` tip and reads every materialized-view
//! column-family in one pass, so the response carries one
//! `ExplorerFreshness`. The bundle's snapshot identity is
//! `freshness.chain_view.chain_epoch`; consumers compare its
//! `chain_epoch_id` and `visible_tip` across responses.

use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message as _;
use tonic::{Code, Request, Response, Status};
use tonic_types::StatusExt as _;
use zinder_core::{
    BlockHeight,
    explorer_reasons::{MEMPOOL_EVENT_COUNTS_NOT_INDEXED, WALLET_QUERY_CAPABILITY_NOT_SUPPORTED},
    wire::encode_height_key_ascending,
};
use zinder_materialized_views::{
    BLOCK_SUMMARY_COLUMN_FAMILY, MaterializedViewStore, TRANSACTION_HISTORY_COLUMN_FAMILY,
};
use zinder_proto::ZINDER_ERROR_DOMAIN;
use zinder_proto::capabilities::{
    EXPLORER_OVERVIEW_SNAPSHOT_V1, WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
    WALLET_SNAPSHOT_MEMPOOL_V3,
};
use zinder_proto::v1::explorer::{
    BlockSummary, BlockSummaryRecord, OverviewFeeSummary, OverviewMempool, OverviewSnapshotRequest,
    OverviewSnapshotResponse, TransactionHistoryEntry, UnavailableField, UnavailableReason,
};
use zinder_proto::v1::ops::ErrorReason;
use zinder_proto::v1::wallet::{
    self, ChainValuePoolsAtTipRequest, MempoolSnapshotRequest, VisibleTipBlockRequest,
    wallet_query_client::WalletQueryClient,
};
use zinder_proto::wire::{
    OVERVIEW_MEMPOOL_EVENTS_FIELD_PATH, OVERVIEW_MEMPOOL_FIELD_PATH,
    OVERVIEW_VALUE_POOLS_FIELD_PATH,
};
use zinder_runtime::AuthenticatedChannel;

use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use super::mempool_event_counts::{clamp_window, read_mempool_event_counts};
use super::transaction_history::decode_history_entry;

/// Range cap on `recent_blocks_limit`.
const MIN_RECENT_BLOCKS_LIMIT: u32 = 1;
/// Server-side ceiling on `recent_blocks_limit`.
const MAX_RECENT_BLOCKS_LIMIT: u32 = 16;
/// Server default when `recent_blocks_limit == 0`.
const DEFAULT_RECENT_BLOCKS_LIMIT: u32 = 8;

/// Range cap on `recent_transactions_limit`.
const MIN_RECENT_TRANSACTIONS_LIMIT: u32 = 1;
/// Server-side ceiling on `recent_transactions_limit`.
const MAX_RECENT_TRANSACTIONS_LIMIT: u32 = 64;
/// Server default when `recent_transactions_limit == 0`.
const DEFAULT_RECENT_TRANSACTIONS_LIMIT: u32 = 32;

/// Range cap on `fee_summary_block_count`.
const MIN_FEE_SUMMARY_BLOCK_COUNT: u32 = 1;
/// Server-side ceiling matches the per-request cap of `FeeSummary`.
const MAX_FEE_SUMMARY_BLOCK_COUNT: u32 = 256;
/// Server default when `fee_summary_block_count == 0`.
const DEFAULT_FEE_SUMMARY_BLOCK_COUNT: u32 = 50;

/// Cap on entries the bundle's mempool aggregation hydrates from the
/// wallet `MempoolSnapshot`. Mirrors the cap in the per-feature mempool
/// handlers.
const MAX_MEMPOOL_SNAPSHOT_ENTRIES: u32 = 4_096;

/// Executes one `ExplorerQuery.OverviewSnapshot` request.
pub(crate) async fn query_overview_snapshot(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<OverviewSnapshotRequest>,
) -> Result<Response<OverviewSnapshotResponse>, Status> {
    let limits = RequestLimits::from_request(request.into_inner());
    let anchor = anchor_to_wallet_tip(wallet_client).await?;
    let block_records =
        read_block_summary_records(materialized_view_store, anchor.tip_height, &limits)?;
    let recent_blocks =
        collect_recent_blocks(&block_records, limits.recent_blocks, anchor.tip_height);
    let fee_summary = aggregate_fee_summary(&block_records, limits.fee_summary_blocks);
    let tip_block_time_unix_seconds = recent_blocks
        .first()
        .map_or(0, |summary| summary.block_time_unix_seconds);
    let recent_transactions =
        read_recent_transactions(materialized_view_store, limits.recent_transactions)?;
    let mempool_events =
        read_mempool_event_counts(materialized_view_store, limits.mempool_window_seconds)?;
    let mempool = aggregate_mempool_summary(wallet_client).await?;
    let available_value_pools = read_value_pools(wallet_client).await?;
    let mut freshness = build_explorer_freshness(
        Some(materialized_view_store),
        EXPLORER_OVERVIEW_SNAPSHOT_V1,
        Some(anchor.chain_epoch),
        0,
    )?;
    if mempool.is_none() {
        freshness
            .unavailable
            .push(build_wallet_capability_unavailable_field(
                OVERVIEW_MEMPOOL_FIELD_PATH,
            ));
    }
    if mempool_events.is_none() {
        freshness.unavailable.push(UnavailableField {
            field_path: OVERVIEW_MEMPOOL_EVENTS_FIELD_PATH.to_owned(),
            reason: UnavailableReason::UnavailableNotIndexed as i32,
            human_reason: MEMPOOL_EVENT_COUNTS_NOT_INDEXED.to_owned(),
        });
    }
    let value_pools = if let Some(value_pools) = available_value_pools {
        value_pools
    } else {
        freshness
            .unavailable
            .push(build_wallet_capability_unavailable_field(
                OVERVIEW_VALUE_POOLS_FIELD_PATH,
            ));
        Vec::new()
    };
    let freshness = attach_upstream_observation(upstream_observation_cache, freshness).await;
    Ok(Response::new(OverviewSnapshotResponse {
        freshness: Some(freshness),
        tip_block_time_unix_seconds,
        mempool,
        mempool_events,
        fee_summary: Some(fee_summary),
        value_pools,
        recent_blocks,
        recent_transactions,
    }))
}

/// Server-clamped request limits derived from the caller's inputs.
#[derive(Clone, Copy)]
struct RequestLimits {
    recent_blocks: u32,
    recent_transactions: u32,
    mempool_window_seconds: u32,
    fee_summary_blocks: u32,
}

impl RequestLimits {
    fn from_request(request: OverviewSnapshotRequest) -> Self {
        Self {
            recent_blocks: clamp(
                request.recent_blocks_limit,
                DEFAULT_RECENT_BLOCKS_LIMIT,
                MIN_RECENT_BLOCKS_LIMIT,
                MAX_RECENT_BLOCKS_LIMIT,
            ),
            recent_transactions: clamp(
                request.recent_transactions_limit,
                DEFAULT_RECENT_TRANSACTIONS_LIMIT,
                MIN_RECENT_TRANSACTIONS_LIMIT,
                MAX_RECENT_TRANSACTIONS_LIMIT,
            ),
            mempool_window_seconds: clamp_window(request.mempool_window_seconds),
            fee_summary_blocks: clamp(
                request.fee_summary_block_count,
                DEFAULT_FEE_SUMMARY_BLOCK_COUNT,
                MIN_FEE_SUMMARY_BLOCK_COUNT,
                MAX_FEE_SUMMARY_BLOCK_COUNT,
            ),
        }
    }

    fn block_window_count(&self) -> u32 {
        self.recent_blocks.max(self.fee_summary_blocks)
    }
}

/// Wallet-anchored snapshot identity (chain epoch + canonical tip height).
struct WalletAnchor {
    chain_epoch: wallet::ChainEpoch,
    tip_height: u32,
}

async fn anchor_to_wallet_tip(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
) -> Result<WalletAnchor, Status> {
    let response = wallet_client
        .visible_tip_block(Request::new(VisibleTipBlockRequest { at_epoch_id: None }))
        .await?
        .into_inner();
    let chain_epoch = response
        .chain_view
        .clone()
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.chain_view.chain_epoch missing")
        })?;
    let tip_height = response
        .visible_tip_block
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.visible_tip_block missing")
        })?
        .height;
    Ok(WalletAnchor {
        chain_epoch,
        tip_height,
    })
}

fn read_block_summary_records(
    materialized_view_store: &MaterializedViewStore,
    canonical_tip_height: u32,
    limits: &RequestLimits,
) -> Result<Vec<BlockSummaryRecord>, Status> {
    let window = limits.block_window_count();
    let start_height = canonical_tip_height.saturating_sub(window.saturating_sub(1));
    let start_key = encode_height_key_ascending(BlockHeight::new(start_height));
    let end_key = encode_height_key_ascending(BlockHeight::new(canonical_tip_height));
    let cap = usize::try_from(window).unwrap_or(MAX_FEE_SUMMARY_BLOCK_COUNT as usize);
    let entries = materialized_view_store
        .range_iterate_consumer(BLOCK_SUMMARY_COLUMN_FAMILY, &start_key, &end_key, cap)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let mut records = Vec::with_capacity(entries.len());
    for (_, payload) in entries {
        let record = BlockSummaryRecord::decode(payload.as_slice()).map_err(|error| {
            ExplorerError::internal(format!("BlockSummaryRecord decode failed: {error}"))
        })?;
        records.push(record);
    }
    Ok(records)
}

fn collect_recent_blocks(
    records: &[BlockSummaryRecord],
    limit: u32,
    canonical_tip_height: u32,
) -> Vec<BlockSummary> {
    let cap = usize::try_from(limit).unwrap_or(MAX_RECENT_BLOCKS_LIMIT as usize);
    let mut summaries: Vec<BlockSummary> = records
        .iter()
        .rev()
        .filter_map(|record| record.summary.clone())
        .take(cap)
        .collect();
    for summary in &mut summaries {
        summary.confirmations = canonical_tip_height
            .saturating_sub(summary.block_height)
            .saturating_add(1);
        summary.is_canonical = true;
    }
    summaries
}

fn aggregate_fee_summary(records: &[BlockSummaryRecord], limit: u32) -> OverviewFeeSummary {
    let cap = usize::try_from(limit).unwrap_or(MAX_FEE_SUMMARY_BLOCK_COUNT as usize);
    let mut block_count: u32 = 0;
    let mut transaction_count: u32 = 0;
    let mut total_fee_zat: u64 = 0;
    let mut min_fee_zat: Option<u64> = None;
    let mut max_fee_zat: Option<u64> = None;
    for record in records.iter().rev().take(cap) {
        block_count = block_count.saturating_add(1);
        transaction_count = transaction_count.saturating_add(record.fee_transaction_count);
        if let Some(summary) = &record.summary {
            total_fee_zat = total_fee_zat.saturating_add(summary.fees_collected_zat);
        }
        if record.fee_transaction_count > 0 {
            min_fee_zat = Some(
                min_fee_zat.map_or(record.min_zip317_conventional_fee_zat, |prior| {
                    prior.min(record.min_zip317_conventional_fee_zat)
                }),
            );
            max_fee_zat = Some(
                max_fee_zat.map_or(record.max_zip317_conventional_fee_zat, |prior| {
                    prior.max(record.max_zip317_conventional_fee_zat)
                }),
            );
        }
    }
    OverviewFeeSummary {
        block_count,
        transaction_count,
        total_zip317_conventional_fee_zat: total_fee_zat,
        min_zip317_conventional_fee_zat: min_fee_zat.unwrap_or(0),
        max_zip317_conventional_fee_zat: max_fee_zat.unwrap_or(0),
    }
}

fn read_recent_transactions(
    materialized_view_store: &MaterializedViewStore,
    limit: u32,
) -> Result<Vec<TransactionHistoryEntry>, Status> {
    let start_key = [0u8; zinder_materialized_views::TRANSACTION_HISTORY_KEY_LEN];
    let end_key = [0xFFu8; zinder_materialized_views::TRANSACTION_HISTORY_KEY_LEN];
    let cap = usize::try_from(limit).unwrap_or(MAX_RECENT_TRANSACTIONS_LIMIT as usize);
    let rows = materialized_view_store
        .range_iterate_consumer(TRANSACTION_HISTORY_COLUMN_FAMILY, &start_key, &end_key, cap)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let mut entries = Vec::with_capacity(rows.len());
    for (key, payload) in rows {
        entries.push(decode_history_entry(&key, &payload)?);
    }
    Ok(entries)
}

async fn aggregate_mempool_summary(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
) -> Result<Option<OverviewMempool>, Status> {
    // Mempool is an optional Overview field. Structural absence is
    // represented through the response freshness envelope; transient
    // transport failures still fail the request.
    let snapshot = match wallet_client
        .mempool_snapshot(Request::new(MempoolSnapshotRequest {
            max_entries: MAX_MEMPOOL_SNAPSHOT_ENTRIES,
            from_cursor: Vec::new(),
        }))
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) if is_endpoint_capability_unavailable(&status, WALLET_SNAPSHOT_MEMPOOL_V3) => {
            return Ok(None);
        }
        Err(status) => return Err(status),
    };
    let now_millis = current_unix_millis();
    let mut transaction_count: u32 = 0;
    let mut total_size_bytes: u64 = 0;
    let mut oldest_first_seen: Option<u64> = None;
    let mut newest_first_seen: Option<u64> = None;
    for entry in &snapshot.entries {
        transaction_count = transaction_count.saturating_add(1);
        let entry_size = u64::try_from(entry.raw_transaction_bytes.len()).unwrap_or(u64::MAX);
        total_size_bytes = total_size_bytes.saturating_add(entry_size);
        oldest_first_seen = Some(
            oldest_first_seen.map_or(entry.first_seen_unix_millis, |prior| {
                prior.min(entry.first_seen_unix_millis)
            }),
        );
        newest_first_seen = Some(
            newest_first_seen.map_or(entry.first_seen_unix_millis, |prior| {
                prior.max(entry.first_seen_unix_millis)
            }),
        );
    }
    Ok(Some(OverviewMempool {
        transaction_count,
        total_size_bytes,
        oldest_entry_age_millis: oldest_first_seen
            .map_or(0, |seen| now_millis.saturating_sub(seen)),
        newest_entry_age_millis: newest_first_seen
            .map_or(0, |seen| now_millis.saturating_sub(seen)),
    }))
}

async fn read_value_pools(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
) -> Result<Option<Vec<wallet::ChainValuePool>>, Status> {
    // Value pools are optional in the Overview bundle. An absent capability
    // is distinguished from an observed empty pool list by
    // `ExplorerFreshness.unavailable`.
    match wallet_client
        .chain_value_pools_at_tip(Request::new(ChainValuePoolsAtTipRequest {}))
        .await
    {
        Ok(response) => Ok(Some(response.into_inner().pools)),
        Err(status)
            if is_endpoint_capability_unavailable(
                &status,
                WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
            ) =>
        {
            Ok(None)
        }
        Err(status) => Err(status),
    }
}

fn is_endpoint_capability_unavailable(status: &Status, capability: &str) -> bool {
    if status.code() != Code::FailedPrecondition {
        return false;
    }

    let details = status.get_error_details();
    let has_matching_error_info = details.error_info().is_some_and(|error_info| {
        error_info.domain == ZINDER_ERROR_DOMAIN
            && error_info.reason == ErrorReason::EndpointCapabilityUnavailable.as_str_name()
    });
    if !has_matching_error_info {
        return false;
    }

    details
        .precondition_failure()
        .is_some_and(|precondition_failure| {
            matches!(precondition_failure.violations.as_slice(), [violation] if {
                violation.r#type == ErrorReason::EndpointCapabilityUnavailable.as_str_name()
                    && violation.subject == capability
            })
        })
}

fn build_wallet_capability_unavailable_field(field_path: &str) -> UnavailableField {
    UnavailableField {
        field_path: field_path.to_owned(),
        reason: UnavailableReason::UnavailableUpstreamNotSupported as i32,
        human_reason: WALLET_QUERY_CAPABILITY_NOT_SUPPORTED.to_owned(),
    }
}

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

const fn clamp(requested: u32, default: u32, min: u32, max: u32) -> u32 {
    let target = if requested == 0 { default } else { requested };
    if target < min {
        min
    } else if target > max {
        max
    } else {
        target
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tonic_types::ErrorDetails;

    use super::*;

    fn status_with_capability_detail(
        code: Code,
        error_domain: &str,
        error_reason: &str,
        violation_type: &str,
        capability: &str,
    ) -> Status {
        let mut details = ErrorDetails::with_precondition_failure_violation(
            violation_type,
            capability,
            "test precondition",
        );
        details.set_error_info(error_reason, error_domain, HashMap::new());
        Status::with_error_details(code, "test status", details)
    }

    #[test]
    fn endpoint_capability_unavailable_requires_exact_structured_details() {
        let endpoint_reason = ErrorReason::EndpointCapabilityUnavailable.as_str_name();
        let status = status_with_capability_detail(
            Code::FailedPrecondition,
            ZINDER_ERROR_DOMAIN,
            endpoint_reason,
            endpoint_reason,
            WALLET_SNAPSHOT_MEMPOOL_V3,
        );

        assert!(is_endpoint_capability_unavailable(
            &status,
            WALLET_SNAPSHOT_MEMPOOL_V3
        ));
    }

    #[test]
    fn transient_unavailable_is_not_structural_capability_absence() {
        let endpoint_reason = ErrorReason::EndpointCapabilityUnavailable.as_str_name();
        let status = status_with_capability_detail(
            Code::Unavailable,
            ZINDER_ERROR_DOMAIN,
            endpoint_reason,
            endpoint_reason,
            WALLET_SNAPSHOT_MEMPOOL_V3,
        );

        assert!(!is_endpoint_capability_unavailable(
            &status,
            WALLET_SNAPSHOT_MEMPOOL_V3
        ));
    }

    #[test]
    fn endpoint_capability_unavailable_requires_zinder_error_domain() {
        let endpoint_reason = ErrorReason::EndpointCapabilityUnavailable.as_str_name();
        let status = status_with_capability_detail(
            Code::FailedPrecondition,
            "foreign.example",
            endpoint_reason,
            endpoint_reason,
            WALLET_SNAPSHOT_MEMPOOL_V3,
        );

        assert!(!is_endpoint_capability_unavailable(
            &status,
            WALLET_SNAPSHOT_MEMPOOL_V3
        ));
    }

    #[test]
    fn endpoint_capability_unavailable_requires_matching_error_reason() {
        let endpoint_reason = ErrorReason::EndpointCapabilityUnavailable.as_str_name();
        let status = status_with_capability_detail(
            Code::FailedPrecondition,
            ZINDER_ERROR_DOMAIN,
            ErrorReason::DependencyNotConfigured.as_str_name(),
            endpoint_reason,
            WALLET_SNAPSHOT_MEMPOOL_V3,
        );

        assert!(!is_endpoint_capability_unavailable(
            &status,
            WALLET_SNAPSHOT_MEMPOOL_V3
        ));
    }

    #[test]
    fn endpoint_capability_unavailable_requires_matching_precondition_type() {
        let endpoint_reason = ErrorReason::EndpointCapabilityUnavailable.as_str_name();
        let status = status_with_capability_detail(
            Code::FailedPrecondition,
            ZINDER_ERROR_DOMAIN,
            endpoint_reason,
            ErrorReason::DependencyNotConfigured.as_str_name(),
            WALLET_SNAPSHOT_MEMPOOL_V3,
        );

        assert!(!is_endpoint_capability_unavailable(
            &status,
            WALLET_SNAPSHOT_MEMPOOL_V3
        ));
    }

    #[test]
    fn endpoint_capability_unavailable_requires_exact_capability_subject() {
        let endpoint_reason = ErrorReason::EndpointCapabilityUnavailable.as_str_name();
        let status = status_with_capability_detail(
            Code::FailedPrecondition,
            ZINDER_ERROR_DOMAIN,
            endpoint_reason,
            endpoint_reason,
            WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
        );

        assert!(!is_endpoint_capability_unavailable(
            &status,
            WALLET_SNAPSHOT_MEMPOOL_V3
        ));
    }

    #[test]
    fn endpoint_capability_unavailable_rejects_composite_preconditions() {
        let endpoint_reason = ErrorReason::EndpointCapabilityUnavailable.as_str_name();
        let mut details = ErrorDetails::with_precondition_failure_violation(
            endpoint_reason,
            WALLET_SNAPSHOT_MEMPOOL_V3,
            "test precondition",
        );
        details.add_precondition_failure_violation(
            ErrorReason::DependencyNotConfigured.as_str_name(),
            "unrelated dependency",
            "unrelated precondition",
        );
        details.set_error_info(endpoint_reason, ZINDER_ERROR_DOMAIN, HashMap::new());
        let status = Status::with_error_details(Code::FailedPrecondition, "test status", details);

        assert!(!is_endpoint_capability_unavailable(
            &status,
            WALLET_SNAPSHOT_MEMPOOL_V3
        ));
    }

    #[test]
    fn endpoint_capability_unavailable_requires_structured_details() {
        let status = Status::failed_precondition("missing structured details");

        assert!(!is_endpoint_capability_unavailable(
            &status,
            WALLET_SNAPSHOT_MEMPOOL_V3
        ));
    }
}
