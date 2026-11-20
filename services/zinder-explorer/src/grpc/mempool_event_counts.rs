//! `ExplorerQuery.MempoolEventCounts` handler.
//!
//! Reads the per-second counter rows written by
//! [`zinder_materialized_views::MempoolEventCountsConsumer`]
//! and aggregates them across the requested window.

use std::time::{SystemTime, UNIX_EPOCH};

use tonic::{Request, Response, Status};
use zinder_core::explorer_reasons::MEMPOOL_EVENT_COUNTS_NOT_INDEXED;
use zinder_proto::capabilities::EXPLORER_MEMPOOL_EVENT_COUNTS_V1;
use zinder_proto::v1::explorer::{
    MempoolEventCountsRequest, MempoolEventCountsResponse, OverviewMempoolEvents,
};
use zinder_proto::v1::wallet::{VisibleTipBlockRequest, wallet_query_client::WalletQueryClient};
use zinder_runtime::AuthenticatedChannel;

use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use zinder_materialized_views::{
    MEMPOOL_EVENT_COUNTS_COLUMN_FAMILY, MaterializedViewStore, MempoolEventCountsConsumer,
};

/// Minimum window size accepted by the handler.
const MIN_WINDOW_SECONDS: u32 = 60;

/// Maximum window size accepted by the handler.
const MAX_WINDOW_SECONDS: u32 = 3_600;

/// Default window when the caller passes `window_seconds = 0`.
const DEFAULT_WINDOW_SECONDS: u32 = 300;

/// Executes one `MempoolEventCounts` request.
pub(crate) async fn query_mempool_event_counts(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<MempoolEventCountsRequest>,
) -> Result<Response<MempoolEventCountsResponse>, Status> {
    let inner = request.into_inner();
    let window_seconds = clamp_window(inner.window_seconds);
    let counts = read_mempool_event_counts(materialized_view_store, window_seconds)?
        .ok_or_else(|| ExplorerError::not_materialized(MEMPOOL_EVENT_COUNTS_NOT_INDEXED))?;

    let latest = wallet_client
        .visible_tip_block(Request::new(VisibleTipBlockRequest { at_epoch_id: None }))
        .await?
        .into_inner();
    let chain_epoch = latest
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.chain_view.chain_epoch missing")
        })?;

    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_MEMPOOL_EVENT_COUNTS_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;
    Ok(Response::new(MempoolEventCountsResponse {
        freshness: Some(freshness),
        window_seconds,
        added_count: counts.added_count,
        mined_count: counts.mined_count,
        invalidated_count: counts.invalidated_count,
    }))
}

pub(super) fn read_mempool_event_counts(
    materialized_view_store: &MaterializedViewStore,
    window_seconds: u32,
) -> Result<Option<OverviewMempoolEvents>, Status> {
    let now_seconds = current_unix_seconds();
    let window_start = now_seconds.saturating_sub(u64::from(window_seconds - 1));
    let start_key = MempoolEventCountsConsumer::key_for_second(window_start);
    let end_key = MempoolEventCountsConsumer::key_for_second(now_seconds);
    let entries = materialized_view_store
        .range_iterate_consumer(
            MEMPOOL_EVENT_COUNTS_COLUMN_FAMILY,
            &start_key,
            &end_key,
            window_seconds as usize,
        )
        .map_err(|error| ExplorerError::internal(error.to_string()))?;

    if entries.is_empty() {
        return Ok(None);
    }
    let mut added_count = 0u32;
    let mut mined_count = 0u32;
    let mut invalidated_count = 0u32;
    for (_, payload) in entries {
        let (added, mined, invalidated) = MempoolEventCountsConsumer::decode_row(&payload)
            .ok_or_else(|| ExplorerError::internal("invalid mempool event counts row"))?;
        added_count = added_count.saturating_add(added);
        mined_count = mined_count.saturating_add(mined);
        invalidated_count = invalidated_count.saturating_add(invalidated);
    }

    Ok(Some(OverviewMempoolEvents {
        window_seconds,
        added_count,
        mined_count,
        invalidated_count,
    }))
}

pub(super) const fn clamp_window(requested: u32) -> u32 {
    let target = if requested == 0 {
        DEFAULT_WINDOW_SECONDS
    } else {
        requested
    };
    if target < MIN_WINDOW_SECONDS {
        MIN_WINDOW_SECONDS
    } else if target > MAX_WINDOW_SECONDS {
        MAX_WINDOW_SECONDS
    } else {
        target
    }
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zinder_materialized_views::MaterializedViewStoreOptions;
    use zinder_store::RocksDbResourceBudget;

    #[test]
    fn counts_require_a_decodable_bucket_in_the_window() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let store = MaterializedViewStore::open_explorer_views(
            directory.path(),
            MaterializedViewStoreOptions {
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                ..Default::default()
            },
        )?;
        let stale = MempoolEventCountsConsumer::key_for_second(1);
        let recent = MempoolEventCountsConsumer::key_for_second(current_unix_seconds());
        store.put_consumer(MEMPOOL_EVENT_COUNTS_COLUMN_FAMILY, &stale, &[0; 12])?;
        assert!(read_mempool_event_counts(&store, DEFAULT_WINDOW_SECONDS)?.is_none());
        store.put_consumer(MEMPOOL_EVENT_COUNTS_COLUMN_FAMILY, &recent, &[0; 12])?;
        assert_eq!(
            read_mempool_event_counts(&store, DEFAULT_WINDOW_SECONDS)?,
            Some(OverviewMempoolEvents {
                window_seconds: DEFAULT_WINDOW_SECONDS,
                ..Default::default()
            })
        );
        store.put_consumer(MEMPOOL_EVENT_COUNTS_COLUMN_FAMILY, &recent, &[0; 1])?;
        assert!(
            matches!(read_mempool_event_counts(&store, DEFAULT_WINDOW_SECONDS), Err(status) if status.code() == tonic::Code::Internal)
        );
        Ok(())
    }
}
