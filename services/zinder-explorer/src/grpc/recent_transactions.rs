//! `ExplorerQuery.RecentTransactions` handler.
//!
//! Streams the newest-first view materialized by
//! [`zinder_materialized_views::RecentTransactionsConsumer`]
//! out of the consumer-owned `recent_transactions` column family. Joins
//! the per-tx `transaction_fees` rows in a single `multi_get` so the page
//! cost is one prefix scan plus one batched lookup.

use std::pin::Pin;

use prost::Message as _;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use zinder_core::wire::decode_rpc_transaction_id_hex;
use zinder_core::{PrivacyShape, TransactionId};
use zinder_proto::capabilities::EXPLORER_TRANSACTION_RECENT_V1;
use zinder_proto::v1::explorer::{
    RecentTransactionEntry, RecentTransactionsChunk, RecentTransactionsRequest,
};
use zinder_proto::v1::wallet::{VisibleTipBlockRequest, wallet_query_client::WalletQueryClient};
use zinder_proto::wire::decode_privacy_shape;
use zinder_runtime::AuthenticatedChannel;

use super::clamp_max_entries;
use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use zinder_materialized_views::{
    MaterializedViewStore, RECENT_TRANSACTIONS_COLUMN_FAMILY, TransactionFeesConsumer,
};

/// Server-side maximum entries the handler ever returns in one stream.
const MAX_RECENT_TRANSACTIONS_PER_REQUEST: u32 = 1024;

/// Default `max_entries` when the caller passes zero.
const DEFAULT_RECENT_TRANSACTIONS: u32 = 64;

/// Length of one row key in the materialized view (`reverse_height` + position).
const ROW_KEY_LEN: usize = 8;

/// Stream type returned by the RPC.
pub(crate) type RecentTransactionsStream =
    Pin<Box<dyn Stream<Item = Result<RecentTransactionsChunk, Status>> + Send + 'static>>;

/// Executes one `ExplorerQuery.RecentTransactions` request.
#[allow(
    clippy::too_many_lines,
    reason = "the streaming handler keeps admission, hydration, and response construction together"
)]
pub(crate) async fn query_recent_transactions(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<RecentTransactionsRequest>,
) -> Result<Response<RecentTransactionsStream>, Status> {
    let inner = request.into_inner();
    let max_entries = clamp_max_entries(
        inner.max_entries,
        DEFAULT_RECENT_TRANSACTIONS,
        MAX_RECENT_TRANSACTIONS_PER_REQUEST,
    );
    let cursor_start: Option<[u8; ROW_KEY_LEN]> = if inner.from_cursor.is_empty() {
        None
    } else {
        Some(
            inner
                .from_cursor
                .as_slice()
                .try_into()
                .map_err(|_| ExplorerError::invalid_request("from_cursor must be 8 bytes"))?,
        )
    };
    let start_key = cursor_start.unwrap_or([0u8; ROW_KEY_LEN]);
    let end_key = [0xFFu8; ROW_KEY_LEN];
    // Cursor rows are exclusive, so request one extra row to skip the
    // resume row without short-changing the page.
    let scan_cap = (max_entries as usize).saturating_add(usize::from(cursor_start.is_some()));
    let rows = materialized_view_store
        .range_iterate_consumer(
            RECENT_TRANSACTIONS_COLUMN_FAMILY,
            &start_key,
            &end_key,
            scan_cap,
        )
        .map_err(|error| ExplorerError::internal(error.to_string()))?;

    let mut entries: Vec<RecentTransactionEntry> = Vec::with_capacity(rows.len());
    let mut last_key: Option<[u8; ROW_KEY_LEN]> = None;
    for (key, payload) in rows {
        let key_array: [u8; ROW_KEY_LEN] = key
            .as_slice()
            .try_into()
            .map_err(|_| ExplorerError::internal("recent_transactions row key not 8 bytes"))?;
        if cursor_start.is_some() && key_array == start_key {
            continue;
        }
        let entry = RecentTransactionEntry::decode(payload.as_slice())
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
        entries.push(entry);
        last_key = Some(key_array);
        if u32::try_from(entries.len()).unwrap_or(u32::MAX) >= max_entries {
            break;
        }
    }

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
    join_paid_fees(materialized_view_store, &mut entries)?;
    let cursor = last_key.map_or_else(Vec::new, |key| key.to_vec());
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_TRANSACTION_RECENT_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;
    let chunk = RecentTransactionsChunk {
        freshness: Some(freshness),
        cursor,
        entries,
    };
    let stream = tokio_stream::iter(std::iter::once(Ok(chunk)));
    Ok(Response::new(Box::pin(stream)))
}

/// Hydrates `entries[*].paid_fee_zat` from the `transaction_fees` materialized view
/// in a single batched read.
///
/// Coinbase rows are skipped (no fee record exists). Missing fee records
/// leave `paid_fee_zat` unset; that's the explicit "not available" signal
/// per ADR-0018.
fn join_paid_fees(
    materialized_view_store: &MaterializedViewStore,
    entries: &mut [RecentTransactionEntry],
) -> Result<(), Status> {
    let lookup_targets: Vec<(TransactionId, PrivacyShape)> = entries
        .iter()
        .filter(|entry| !entry.is_coinbase)
        .map(|entry| {
            let privacy_shape =
                decode_privacy_shape(entry.privacy_shape).unwrap_or(PrivacyShape::Unclassified);
            decode_rpc_transaction_id_hex(&entry.transaction_id)
                .map(|transaction_id| (transaction_id, privacy_shape))
        })
        .collect::<Result<_, _>>()
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    if lookup_targets.is_empty() {
        return Ok(());
    }
    let records =
        TransactionFeesConsumer::read_fees_records_many(materialized_view_store, &lookup_targets)
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
    for entry in entries.iter_mut() {
        if entry.is_coinbase {
            continue;
        }
        let Ok(transaction_id) = decode_rpc_transaction_id_hex(&entry.transaction_id) else {
            continue;
        };
        if let Some(record) = records.get(&transaction_id) {
            entry.paid_fee_zat = record.paid_fee_zat;
        }
    }
    Ok(())
}
