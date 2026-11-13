//! `ExplorerQuery.TransactionHistory` handler.
//!
//! Reads the preserved transaction-history materialized view through bounded,
//! filter-aware, bidirectional pages. The physical column-family name is an
//! internal storage detail, not public vocabulary.

use std::{cmp::Reverse, collections::HashMap, fmt, sync::Arc};

use prost::Message as _;
use thiserror::Error;
use tonic::{Request, Response, Status};
use zinder_core::wire::{
    decode_height_key_descending, decode_in_block_position, decode_rpc_block_hash_hex,
    decode_rpc_transaction_id_hex, encode_internal_transaction_id, encode_rpc_block_hash_hex,
};
use zinder_core::{BlockHeight, PrivacyShape, TransactionId};
use zinder_proto::capabilities::EXPLORER_TRANSACTION_HISTORY_V2;
use zinder_proto::v1::explorer::{
    ShieldedProtocol, TransactionFeesRecord, TransactionHistoryCountScope,
    TransactionHistoryCoverage, TransactionHistoryDirection, TransactionHistoryEntry,
    TransactionHistoryFilter, TransactionHistoryReadFence, TransactionHistoryRequest,
    TransactionHistoryResponse, transaction_history_request,
};
use zinder_proto::v1::wallet::{VisibleTipBlockRequest, wallet_query_client::WalletQueryClient};
use zinder_proto::wire::decode_privacy_shape;
use zinder_runtime::AuthenticatedChannel;
use zinder_store::chain_epoch_from_message;

use super::clamp_max_entries;
use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use zinder_materialized_views::{
    MaterializedViewState, MaterializedViewStore, MaterializedViewStoreError,
    MaterializedViewStoreReadSnapshot, TRANSACTION_FEES_COLUMN_FAMILY,
    TRANSACTION_HISTORY_COLUMN_FAMILY, TRANSACTION_HISTORY_CONSUMER_NAME,
    TRANSACTION_HISTORY_KEY_LEN, TransactionHistoryConsumer,
};

/// Typed lifecycle state for the optional transaction-history materialized view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionHistoryMaterializedViewReadiness {
    /// The selected workload deliberately omits transaction history.
    Omitted,
    /// The materialized view is selected and schema-compatible but has no durable position yet.
    Materializing,
    /// The materialized view has a durable epoch, tip, revision, and optional verified coverage.
    Available(MaterializedViewState),
}

impl TransactionHistoryMaterializedViewReadiness {
    pub(crate) const fn is_available(self) -> bool {
        matches!(self, Self::Available(_))
    }

    pub(crate) fn require_available(
        self,
    ) -> Result<(), TransactionHistoryMaterializedViewReadError> {
        match self {
            Self::Available(_) => Ok(()),
            Self::Omitted => Err(TransactionHistoryMaterializedViewReadError::Omitted),
            Self::Materializing => Err(TransactionHistoryMaterializedViewReadError::Materializing),
        }
    }
}

/// Failures returned by the typed transaction-history materialized-view seam.
#[derive(Debug, Error)]
pub(crate) enum TransactionHistoryMaterializedViewReadError {
    #[error("transaction-history materialized view is omitted by the selected workload")]
    Omitted,
    #[error("transaction-history materialized view has not recorded a durable position")]
    Materializing,
    #[error("transaction-history materialized-view storage read failed: {0}")]
    Storage(#[source] MaterializedViewStoreError),
    #[error("transaction-history materialized-view request failed: {0}")]
    Request(Status),
}

impl TransactionHistoryMaterializedViewReadError {
    pub(crate) fn into_status(self) -> Status {
        match self {
            Self::Omitted => ExplorerError::unsupported(
                "TransactionHistory is omitted by the selected materialized-view workload",
            )
            .into(),
            Self::Materializing => ExplorerError::unsatisfied_precondition(
                "transaction-history materialized-view state is not available",
            )
            .into(),
            Self::Storage(error) => ExplorerError::internal(error.to_string()).into(),
            Self::Request(status) => status,
        }
    }
}

/// Typed read boundary for the optional transaction-history materialized view.
pub(crate) trait TransactionHistoryMaterializedViewReadApi:
    fmt::Debug + Send + Sync + 'static
{
    fn readiness(
        &self,
    ) -> Result<
        TransactionHistoryMaterializedViewReadiness,
        TransactionHistoryMaterializedViewReadError,
    >;

    fn read_snapshot(
        &self,
        request: &TransactionHistorySnapshotRequest,
    ) -> Result<TransactionHistorySnapshotRead, TransactionHistoryMaterializedViewReadError>;
}

/// Current `RocksDB` implementation of the transaction-history read boundary.
#[derive(Clone, Debug)]
pub(crate) struct TransactionHistoryMaterializedViewReader {
    store: MaterializedViewStore,
}

impl TransactionHistoryMaterializedViewReader {
    pub(crate) const fn new(store: MaterializedViewStore) -> Self {
        Self { store }
    }
}

impl TransactionHistoryMaterializedViewReadApi for TransactionHistoryMaterializedViewReader {
    fn readiness(
        &self,
    ) -> Result<
        TransactionHistoryMaterializedViewReadiness,
        TransactionHistoryMaterializedViewReadError,
    > {
        if !self.store.has_consumer(TRANSACTION_HISTORY_CONSUMER_NAME) {
            return Ok(TransactionHistoryMaterializedViewReadiness::Omitted);
        }
        self.store
            .try_catch_up()
            .map_err(TransactionHistoryMaterializedViewReadError::Storage)?;
        let state = self
            .store
            .consumer_state(TRANSACTION_HISTORY_CONSUMER_NAME)
            .map_err(TransactionHistoryMaterializedViewReadError::Storage)?;
        Ok(state.map_or(
            TransactionHistoryMaterializedViewReadiness::Materializing,
            TransactionHistoryMaterializedViewReadiness::Available,
        ))
    }

    fn read_snapshot(
        &self,
        request: &TransactionHistorySnapshotRequest,
    ) -> Result<TransactionHistorySnapshotRead, TransactionHistoryMaterializedViewReadError> {
        self.readiness()?.require_available()?;
        read_transaction_history_snapshot(&self.store, request)
            .map_err(TransactionHistoryMaterializedViewReadError::Request)
    }
}

/// Server-side maximum entries returned in one page.
const MAX_TRANSACTION_HISTORY_PAGE_SIZE: u32 = 256;

/// Default page size when the caller passes zero.
const DEFAULT_TRANSACTION_HISTORY_PAGE_SIZE: u32 = 64;

/// Bound on rows inspected to fill one filtered page.
const MAX_TRANSACTION_HISTORY_SCANNED_ENTRIES: u32 = 100_000;

/// Bound on one block's transaction rows.
const MAX_TRANSACTION_HISTORY_ENTRIES_PER_BLOCK: usize = 100_000;

const CURSOR_PREFIX: &[u8; 4] = b"zch2";
const CURSOR_FILTER_LEN: usize = 8;
const CURSOR_BLOCK_HASH_LEN: usize = 64;
const CURSOR_FENCE_LEN: usize = 8 + 8 + 4 + CURSOR_BLOCK_HASH_LEN;
const CURSOR_LEN: usize =
    CURSOR_PREFIX.len() + 4 + 4 + CURSOR_BLOCK_HASH_LEN + CURSOR_FILTER_LEN + CURSOR_FENCE_LEN;

/// Dependencies for one typed transaction-history public read.
pub(crate) struct TransactionHistoryContext<'store> {
    pub(crate) materialized_view_reader: Arc<dyn TransactionHistoryMaterializedViewReadApi>,
    pub(crate) materialized_view_store: Option<&'store MaterializedViewStore>,
    pub(crate) upstream_observation_cache: &'store UpstreamObservationCache,
}

/// Executes one `ExplorerQuery.TransactionHistory` request.
#[allow(
    clippy::too_many_lines,
    reason = "The handler keeps one materialized-view snapshot, its epoch resolution, and response fence together."
)]
pub(crate) async fn transaction_history(
    context: TransactionHistoryContext<'_>,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    request: Request<TransactionHistoryRequest>,
) -> Result<Response<TransactionHistoryResponse>, Status> {
    let inner = request.into_inner();
    let page_size = clamp_max_entries(
        inner.page_size,
        DEFAULT_TRANSACTION_HISTORY_PAGE_SIZE,
        MAX_TRANSACTION_HISTORY_PAGE_SIZE,
    );
    let direction = transaction_history_direction(inner.direction)?;
    let filter = HistoryFilter::try_from(inner.filter.unwrap_or_default())?;
    let snapshot_request = TransactionHistorySnapshotRequest {
        page_size,
        direction,
        filter: filter.clone(),
        request_fence: inner.read_fence,
        start: inner.start,
        include_total_count: inner.include_total_count,
    };
    let snapshot_reader = Arc::clone(&context.materialized_view_reader);
    let snapshot_result =
        tokio::task::spawn_blocking(move || snapshot_reader.read_snapshot(&snapshot_request))
            .await
            .map_err(|error| {
                ExplorerError::internal(format!("transaction-history snapshot failed: {error}"))
            })?;
    let snapshot_read =
        snapshot_result.map_err(TransactionHistoryMaterializedViewReadError::into_status)?;

    let chain_epoch = resolve_transaction_history_chain_epoch(
        wallet_client,
        snapshot_read.materialized_view_state,
    )
    .await?;
    let page = snapshot_read.page;
    let freshness = attach_upstream_observation(
        context.upstream_observation_cache,
        build_explorer_freshness(
            context.materialized_view_store,
            EXPLORER_TRANSACTION_HISTORY_V2,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;
    let read_fence = materialized_view_read_fence(snapshot_read.materialized_view_state);
    let coverage = materialized_view_coverage(snapshot_read.materialized_view_state);
    let response = TransactionHistoryResponse {
        freshness: Some(freshness),
        older_cursor: history_page_cursor(
            &page,
            direction,
            TransactionHistoryDirection::Older,
            &filter,
            &read_fence,
        ),
        newer_cursor: history_page_cursor(
            &page,
            direction,
            TransactionHistoryDirection::Newer,
            &filter,
            &read_fence,
        ),
        entries: page.entries,
        has_older: page.has_older,
        has_newer: page.has_newer,
        total_matching_transactions: snapshot_read.total_matching_transactions,
        scanned_entry_count: page.scanned_entry_count,
        scan_limit_reached: page.scan_limit_reached,
        read_fence: Some(read_fence),
        coverage,
        count_scope: transaction_history_count_scope(
            snapshot_read.total_matching_transactions,
            snapshot_read.materialized_view_state,
        ) as i32,
    };
    Ok(Response::new(response))
}

async fn resolve_transaction_history_chain_epoch(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    materialized_view_state: MaterializedViewState,
) -> Result<zinder_proto::v1::wallet::ChainEpoch, Status> {
    let latest = wallet_client
        .visible_tip_block(Request::new(VisibleTipBlockRequest {
            at_epoch_id: Some(materialized_view_state.chain_epoch_id.value()),
        }))
        .await?
        .into_inner();
    let chain_epoch = latest
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.chain_view.chain_epoch missing")
        })?;
    let wallet_epoch = chain_epoch_from_message(chain_epoch.clone())
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    if wallet_epoch.id != materialized_view_state.chain_epoch_id {
        return Err(ExplorerError::unsatisfied_precondition(
            "transaction-history materialized-view epoch is unavailable from WalletQuery",
        )
        .into());
    }
    Ok(chain_epoch)
}

pub(crate) struct TransactionHistorySnapshotRequest {
    page_size: u32,
    direction: TransactionHistoryDirection,
    filter: HistoryFilter,
    request_fence: Option<TransactionHistoryReadFence>,
    start: Option<transaction_history_request::Start>,
    include_total_count: bool,
}

pub(crate) struct TransactionHistorySnapshotRead {
    page: HistoryPage,
    materialized_view_state: MaterializedViewState,
    total_matching_transactions: Option<u64>,
}

fn read_transaction_history_snapshot(
    materialized_view_store: &MaterializedViewStore,
    request: &TransactionHistorySnapshotRequest,
) -> Result<TransactionHistorySnapshotRead, Status> {
    let snapshot = materialized_view_store.read_snapshot();
    let materialized_view_state = snapshot
        .consumer_state(TRANSACTION_HISTORY_CONSUMER_NAME)
        .map_err(|error| ExplorerError::internal(error.to_string()))?
        .ok_or_else(|| {
            ExplorerError::unsatisfied_precondition(
                "transaction-history materialized-view state is not available",
            )
        })?;
    validate_request_read_fence(request.request_fence.as_ref(), materialized_view_state)?;
    let anchor = resolve_transaction_history_anchor(
        &snapshot,
        request.start.clone(),
        request.direction,
        &request.filter,
        materialized_view_state,
    )?;
    let mut page = read_transaction_history_page(
        &snapshot,
        request.page_size,
        request.direction,
        anchor.as_ref(),
        &request.filter,
    )?;
    join_projected_paid_fees(&snapshot, &mut page.entries)?;
    let total_matching_transactions = (request.include_total_count
        && full_history_coverage(materialized_view_state))
    .then(|| transaction_history_total_count(&snapshot, &request.filter))
    .transpose()?;
    drop(snapshot);
    Ok(TransactionHistorySnapshotRead {
        page,
        materialized_view_state,
        total_matching_transactions,
    })
}

fn count_matching_transaction_history_rows(
    snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    filter: &HistoryFilter,
) -> Result<u64, Status> {
    snapshot
        .count_consumer_rows_matching(TRANSACTION_HISTORY_COLUMN_FAMILY, |_key, payload| {
            let record = TransactionHistoryCountRecord::decode(payload)
                .map_err(|error| error.to_string())?;
            Ok(filter.matches_fields(
                record.is_coinbase,
                record.privacy_shape,
                record.component_counts.as_ref(),
            ))
        })
        .map_err(|error| ExplorerError::internal(error.to_string()).into())
}

fn transaction_history_total_count(
    snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    filter: &HistoryFilter,
) -> Result<u64, Status> {
    if filter.is_unfiltered() {
        snapshot
            .consumer_row_count(TRANSACTION_HISTORY_COLUMN_FAMILY)
            .map_err(|error| ExplorerError::internal(error.to_string()).into())
    } else {
        count_matching_transaction_history_rows(snapshot, filter)
    }
}

fn resolve_transaction_history_anchor(
    snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    start: Option<transaction_history_request::Start>,
    direction: TransactionHistoryDirection,
    filter: &HistoryFilter,
    materialized_view_state: MaterializedViewState,
) -> Result<Option<HistoryAnchor>, Status> {
    match start {
        Some(transaction_history_request::Start::Cursor(cursor)) => {
            let (anchor, cursor_filter, cursor_fence) = decode_history_cursor(&cursor)?;
            if cursor_filter != *filter {
                return Err(ExplorerError::invalid_request(
                    "transaction-history cursor filter does not match request filter",
                )
                .into());
            }
            if cursor_fence != materialized_view_read_fence(materialized_view_state) {
                return Err(ExplorerError::unsatisfied_precondition(
                    "transaction-history cursor was invalidated by a materialized-view change",
                )
                .into());
            }
            validate_history_anchor(snapshot, anchor, true)
        }
        Some(transaction_history_request::Start::Anchor(anchor)) => validate_history_anchor(
            snapshot,
            HistoryAnchor {
                block_height: BlockHeight::new(anchor.block_height),
                transaction_index: anchor.transaction_index,
                block_hash: String::new(),
            },
            false,
        ),
        None => {
            if direction == TransactionHistoryDirection::Newer {
                return Err(ExplorerError::invalid_request(
                    "newer transaction-history pages require a cursor or anchor",
                )
                .into());
            }
            Ok(None)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HistoryAnchor {
    block_height: BlockHeight,
    transaction_index: u32,
    block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HistoryFilter {
    is_coinbase: Option<bool>,
    privacy_shape_mask: u16,
    shielded_protocol_mask: u8,
    minimum_shielded_component_count: u32,
}

/// Minimal persisted-row view used only by exact filtered counts.
///
/// Tags match `TransactionHistoryEntry`; prost skips transaction IDs, block
/// hashes, timestamps, and fee fields that cannot affect a history filter.
#[derive(Clone, PartialEq, prost::Message)]
struct TransactionHistoryCountRecord {
    #[prost(bool, tag = "5")]
    is_coinbase: bool,
    #[prost(int32, tag = "6")]
    privacy_shape: i32,
    #[prost(message, optional, tag = "7")]
    component_counts: Option<zinder_proto::v1::explorer::TransactionComponentCounts>,
}

impl TryFrom<TransactionHistoryFilter> for HistoryFilter {
    type Error = Status;

    fn try_from(filter: TransactionHistoryFilter) -> Result<Self, Self::Error> {
        let mut privacy_shape_mask = 0_u16;
        for encoded_shape in filter.privacy_shapes {
            let shape = zinder_proto::v1::explorer::PrivacyShape::try_from(encoded_shape)
                .map_err(|_| ExplorerError::invalid_request("unknown privacy shape filter"))?;
            if shape == zinder_proto::v1::explorer::PrivacyShape::Unspecified {
                return Err(ExplorerError::invalid_request(
                    "unspecified privacy shape cannot be used as a filter",
                )
                .into());
            }
            privacy_shape_mask |= 1_u16 << (shape as u16);
        }
        let mut shielded_protocol_mask = 0_u8;
        for encoded_protocol in filter.contains_any_protocol {
            let protocol = ShieldedProtocol::try_from(encoded_protocol)
                .map_err(|_| ExplorerError::invalid_request("unknown shielded protocol filter"))?;
            if protocol == ShieldedProtocol::Unspecified {
                return Err(ExplorerError::invalid_request(
                    "unspecified shielded protocol cannot be used as a filter",
                )
                .into());
            }
            shielded_protocol_mask |= 1_u8 << (protocol as u8);
        }
        Ok(Self {
            is_coinbase: filter.is_coinbase,
            privacy_shape_mask,
            shielded_protocol_mask,
            minimum_shielded_component_count: filter.minimum_shielded_component_count,
        })
    }
}

struct HistoryPage {
    entries: Vec<TransactionHistoryEntry>,
    has_older: bool,
    has_newer: bool,
    scanned_entry_count: u32,
    scan_limit_reached: bool,
    scan_progress: Option<TransactionHistoryEntry>,
}

impl HistoryPage {
    fn empty() -> Self {
        Self {
            entries: Vec::new(),
            has_older: false,
            has_newer: false,
            scanned_entry_count: 0,
            scan_limit_reached: false,
            scan_progress: None,
        }
    }
}

fn history_page_cursor(
    page: &HistoryPage,
    scan_direction: TransactionHistoryDirection,
    cursor_direction: TransactionHistoryDirection,
    filter: &HistoryFilter,
    read_fence: &TransactionHistoryReadFence,
) -> Vec<u8> {
    let page_boundary = if cursor_direction == TransactionHistoryDirection::Older {
        page.entries.last()
    } else {
        page.entries.first()
    };
    let entry = if page.scan_limit_reached && scan_direction == cursor_direction {
        page.scan_progress.as_ref().or(page_boundary)
    } else {
        page_boundary
    };
    entry.map_or_else(Vec::new, |entry| {
        encode_history_cursor(entry, filter, read_fence)
    })
}

fn transaction_history_direction(encoded: i32) -> Result<TransactionHistoryDirection, Status> {
    let direction = TransactionHistoryDirection::try_from(encoded)
        .map_err(|_| ExplorerError::invalid_request("unknown transaction-history direction"))?;
    Ok(match direction {
        TransactionHistoryDirection::Unspecified | TransactionHistoryDirection::Older => {
            TransactionHistoryDirection::Older
        }
        TransactionHistoryDirection::Newer => TransactionHistoryDirection::Newer,
    })
}

fn read_transaction_history_page(
    snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    page_size: u32,
    direction: TransactionHistoryDirection,
    anchor: Option<&HistoryAnchor>,
    filter: &HistoryFilter,
) -> Result<HistoryPage, Status> {
    let Some((minimum_height, maximum_height)) = transaction_history_height_bounds(snapshot)?
    else {
        return Ok(HistoryPage::empty());
    };
    let mut height = anchor.map_or_else(|| maximum_height, |anchor| anchor.block_height);
    let mut entries =
        Vec::with_capacity(usize::try_from(page_size.saturating_add(1)).unwrap_or(usize::MAX));
    let mut scanned_entry_count = 0_u32;
    let mut last_scanned_entry = None;
    let mut is_anchor_height = anchor.is_some();
    let requested_entry_count = page_size.saturating_add(1);

    loop {
        let block_entries = transaction_history_entries_in_direction(snapshot, height, direction)?;
        for entry in block_entries {
            if is_anchor_height && !entry_is_after_anchor(&entry, anchor, direction) {
                continue;
            }
            scanned_entry_count = scanned_entry_count.saturating_add(1);
            let is_match = filter.matches(&entry);
            if is_match {
                entries.push(entry.clone());
                if u32::try_from(entries.len()).unwrap_or(u32::MAX) >= requested_entry_count {
                    last_scanned_entry = Some(entry);
                    break;
                }
            }
            last_scanned_entry = Some(entry);
            if scanned_entry_count >= MAX_TRANSACTION_HISTORY_SCANNED_ENTRIES {
                break;
            }
        }
        if u32::try_from(entries.len()).unwrap_or(u32::MAX) >= requested_entry_count
            || scanned_entry_count >= MAX_TRANSACTION_HISTORY_SCANNED_ENTRIES
        {
            break;
        }
        let next_height = adjacent_history_height(height, direction);
        let Some(next_height) = next_height else {
            break;
        };
        if next_height < minimum_height || next_height > maximum_height {
            break;
        }
        height = next_height;
        is_anchor_height = false;
    }

    let has_extra_entry = u32::try_from(entries.len()).unwrap_or(u32::MAX) > page_size;
    entries.truncate(usize::try_from(page_size).unwrap_or(usize::MAX));
    if direction == TransactionHistoryDirection::Newer {
        entries.reverse();
    }
    let scan_limit_reached = scanned_entry_count >= MAX_TRANSACTION_HISTORY_SCANNED_ENTRIES;
    let scan_progress = if scan_limit_reached && !has_extra_entry {
        last_scanned_entry
    } else {
        None
    };
    Ok(HistoryPage {
        entries,
        has_older: if direction == TransactionHistoryDirection::Older {
            has_extra_entry || scan_limit_reached
        } else {
            anchor.is_some()
        },
        has_newer: if direction == TransactionHistoryDirection::Newer {
            has_extra_entry || scan_limit_reached
        } else {
            anchor.is_some()
        },
        scanned_entry_count,
        scan_limit_reached,
        scan_progress,
    })
}

fn transaction_history_entries_in_direction(
    snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    height: BlockHeight,
    direction: TransactionHistoryDirection,
) -> Result<Vec<TransactionHistoryEntry>, Status> {
    let mut entries = transaction_history_entries_at_height(snapshot, height)?;
    if direction == TransactionHistoryDirection::Older {
        entries.sort_unstable_by_key(|entry| Reverse(entry.transaction_index));
    } else {
        entries.sort_unstable_by_key(|entry| entry.transaction_index);
    }
    Ok(entries)
}

fn adjacent_history_height(
    height: BlockHeight,
    direction: TransactionHistoryDirection,
) -> Option<BlockHeight> {
    if direction == TransactionHistoryDirection::Older {
        height.value().checked_sub(1).map(BlockHeight::new)
    } else {
        height.next()
    }
}

fn transaction_history_height_bounds(
    snapshot: &MaterializedViewStoreReadSnapshot<'_>,
) -> Result<Option<(BlockHeight, BlockHeight)>, Status> {
    let maximum_height = snapshot
        .last_materialized_height_descending(TRANSACTION_HISTORY_COLUMN_FAMILY)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let Some(maximum_height) = maximum_height else {
        return Ok(None);
    };
    let minimum_key = snapshot
        .last_consumer_key(TRANSACTION_HISTORY_COLUMN_FAMILY)
        .map_err(|error| ExplorerError::internal(error.to_string()))?
        .ok_or_else(|| ExplorerError::internal("transaction-history bounds disagree"))?;
    let minimum_height = decode_history_key(&minimum_key)?.0;
    Ok(Some((minimum_height, maximum_height)))
}

fn transaction_history_entries_at_height(
    snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    height: BlockHeight,
) -> Result<Vec<TransactionHistoryEntry>, Status> {
    let start_key = TransactionHistoryConsumer::key_for_row(height, 0);
    let end_key = TransactionHistoryConsumer::key_for_row(height, u32::MAX);
    let rows = snapshot
        .range_iterate_consumer(
            TRANSACTION_HISTORY_COLUMN_FAMILY,
            &start_key,
            &end_key,
            MAX_TRANSACTION_HISTORY_ENTRIES_PER_BLOCK.saturating_add(1),
        )
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    if rows.len() > MAX_TRANSACTION_HISTORY_ENTRIES_PER_BLOCK {
        return Err(ExplorerError::internal(
            "transaction-history block exceeds the per-block read bound",
        )
        .into());
    }
    rows.into_iter()
        .map(|(key, payload)| decode_history_entry(&key, &payload))
        .collect()
}

pub(super) fn decode_history_entry(
    key: &[u8],
    payload: &[u8],
) -> Result<TransactionHistoryEntry, Status> {
    let (block_height, transaction_index) = decode_history_key(key)?;
    let mut entry = TransactionHistoryEntry::decode(payload)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    if entry.block_height != block_height.value() {
        return Err(ExplorerError::internal(
            "transaction-history row height does not match its key",
        )
        .into());
    }
    if entry.transaction_index != 0 && entry.transaction_index != transaction_index {
        return Err(ExplorerError::internal(
            "transaction-history row index does not match its key",
        )
        .into());
    }
    decode_rpc_block_hash_hex(&entry.block_hash)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    entry.transaction_index = transaction_index;
    Ok(entry)
}

fn decode_history_key(key: &[u8]) -> Result<(BlockHeight, u32), Status> {
    let key: [u8; TRANSACTION_HISTORY_KEY_LEN] = key
        .try_into()
        .map_err(|_| ExplorerError::internal("transaction-history row key must be 8 bytes"))?;
    let block_height = decode_height_key_descending(&key[..4])
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let transaction_index = decode_in_block_position(&key[4..])
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    Ok((block_height, transaction_index))
}

fn entry_is_after_anchor(
    entry: &TransactionHistoryEntry,
    anchor: Option<&HistoryAnchor>,
    direction: TransactionHistoryDirection,
) -> bool {
    let Some(anchor) = anchor else {
        return true;
    };
    if direction == TransactionHistoryDirection::Older {
        entry.transaction_index < anchor.transaction_index
    } else {
        entry.transaction_index > anchor.transaction_index
    }
}

fn validate_history_anchor(
    snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    mut anchor: HistoryAnchor,
    require_matching_hash: bool,
) -> Result<Option<HistoryAnchor>, Status> {
    let key =
        TransactionHistoryConsumer::key_for_row(anchor.block_height, anchor.transaction_index);
    let payload = snapshot
        .get_consumer(TRANSACTION_HISTORY_COLUMN_FAMILY, &key)
        .map_err(|error| ExplorerError::internal(error.to_string()))?
        .ok_or_else(|| {
            ExplorerError::unsatisfied_precondition(
                "transaction-history cursor or anchor is no longer canonical",
            )
        })?;
    let entry = decode_history_entry(&key, &payload)?;
    if require_matching_hash && anchor.block_hash != entry.block_hash {
        return Err(ExplorerError::unsatisfied_precondition(
            "transaction-history cursor was invalidated by a chain change",
        )
        .into());
    }
    anchor.block_hash = entry.block_hash;
    Ok(Some(anchor))
}

impl HistoryFilter {
    fn is_unfiltered(&self) -> bool {
        self.is_coinbase.is_none()
            && self.privacy_shape_mask == 0
            && self.shielded_protocol_mask == 0
            && self.minimum_shielded_component_count == 0
    }

    fn matches(&self, entry: &TransactionHistoryEntry) -> bool {
        self.matches_fields(
            entry.is_coinbase,
            entry.privacy_shape,
            entry.component_counts.as_ref(),
        )
    }

    fn matches_fields(
        &self,
        is_coinbase: bool,
        privacy_shape: i32,
        component_counts: Option<&zinder_proto::v1::explorer::TransactionComponentCounts>,
    ) -> bool {
        if self
            .is_coinbase
            .is_some_and(|expected_coinbase| is_coinbase != expected_coinbase)
        {
            return false;
        }
        if self.privacy_shape_mask != 0 {
            let Ok(shape) = zinder_proto::v1::explorer::PrivacyShape::try_from(privacy_shape)
            else {
                return false;
            };
            if self.privacy_shape_mask & (1_u16 << (shape as u16)) == 0 {
                return false;
            }
        }
        let Some(counts) = component_counts else {
            return self.shielded_protocol_mask == 0 && self.minimum_shielded_component_count == 0;
        };
        if self.shielded_protocol_mask != 0 && !self.selected_protocol_is_present(counts) {
            return false;
        }
        self.minimum_shielded_component_count == 0
            || maximum_shielded_component_count(counts) >= self.minimum_shielded_component_count
    }

    fn selected_protocol_is_present(
        &self,
        counts: &zinder_proto::v1::explorer::TransactionComponentCounts,
    ) -> bool {
        (self.protocol_selected(ShieldedProtocol::Sprout) && counts.sprout_joinsplit_count > 0)
            || (self.protocol_selected(ShieldedProtocol::Sapling)
                && (counts.sapling_spend_count > 0 || counts.sapling_output_count > 0))
            || (self.protocol_selected(ShieldedProtocol::Orchard)
                && counts.orchard_action_count > 0)
            || (self.protocol_selected(ShieldedProtocol::Ironwood)
                && counts.ironwood_action_count > 0)
    }

    fn protocol_selected(&self, protocol: ShieldedProtocol) -> bool {
        self.shielded_protocol_mask & (1_u8 << (protocol as u8)) != 0
    }
}

fn maximum_shielded_component_count(
    counts: &zinder_proto::v1::explorer::TransactionComponentCounts,
) -> u32 {
    counts
        .sapling_spend_count
        .max(counts.sapling_output_count)
        .max(counts.orchard_action_count)
        .max(counts.ironwood_action_count)
        .max(counts.sprout_joinsplit_count)
}

fn encode_history_cursor(
    entry: &TransactionHistoryEntry,
    filter: &HistoryFilter,
    read_fence: &TransactionHistoryReadFence,
) -> Vec<u8> {
    let mut cursor = Vec::with_capacity(CURSOR_LEN);
    cursor.extend_from_slice(CURSOR_PREFIX);
    cursor.extend_from_slice(&entry.block_height.to_be_bytes());
    cursor.extend_from_slice(&entry.transaction_index.to_be_bytes());
    cursor.extend_from_slice(entry.block_hash.as_bytes());
    cursor.extend_from_slice(&filter.cursor_bytes());
    cursor.extend_from_slice(&read_fence.chain_epoch_id.to_be_bytes());
    cursor.extend_from_slice(&read_fence.materialized_view_revision.to_be_bytes());
    cursor.extend_from_slice(&read_fence.materialized_view_tip_height.to_be_bytes());
    cursor.extend_from_slice(read_fence.materialized_view_tip_hash.as_bytes());
    cursor
}

fn decode_history_cursor(
    cursor: &[u8],
) -> Result<(HistoryAnchor, HistoryFilter, TransactionHistoryReadFence), Status> {
    if cursor.len() != CURSOR_LEN || cursor.get(..CURSOR_PREFIX.len()) != Some(CURSOR_PREFIX) {
        return Err(
            ExplorerError::invalid_request("transaction-history cursor shape is invalid").into(),
        );
    }
    let block_height = read_cursor_u32(cursor, CURSOR_PREFIX.len())?;
    let transaction_index = read_cursor_u32(cursor, CURSOR_PREFIX.len() + 4)?;
    let hash_start = CURSOR_PREFIX.len() + 8;
    let hash_end = hash_start + CURSOR_BLOCK_HASH_LEN;
    let block_hash = std::str::from_utf8(&cursor[hash_start..hash_end])
        .map_err(|_| ExplorerError::invalid_request("transaction-history cursor hash is invalid"))?
        .to_owned();
    decode_rpc_block_hash_hex(&block_hash).map_err(|_| {
        ExplorerError::invalid_request("transaction-history cursor hash is invalid")
    })?;
    let filter_end = hash_end + CURSOR_FILTER_LEN;
    let filter = HistoryFilter::from_cursor_bytes(&cursor[hash_end..filter_end])?;
    let fence = TransactionHistoryReadFence {
        chain_epoch_id: read_cursor_u64(cursor, filter_end)?,
        materialized_view_revision: read_cursor_u64(cursor, filter_end + 8)?,
        materialized_view_tip_height: read_cursor_u32(cursor, filter_end + 16)?,
        materialized_view_tip_hash: decode_cursor_hash(cursor, filter_end + 20)?,
    };
    Ok((
        HistoryAnchor {
            block_height: BlockHeight::new(block_height),
            transaction_index,
            block_hash,
        },
        filter,
        fence,
    ))
}

impl HistoryFilter {
    fn cursor_bytes(&self) -> [u8; CURSOR_FILTER_LEN] {
        let mut bytes = [0_u8; CURSOR_FILTER_LEN];
        bytes[0] = self
            .is_coinbase
            .map_or(0, |is_coinbase| if is_coinbase { 2 } else { 1 });
        bytes[1..3].copy_from_slice(&self.privacy_shape_mask.to_be_bytes());
        bytes[3] = self.shielded_protocol_mask;
        bytes[4..8].copy_from_slice(&self.minimum_shielded_component_count.to_be_bytes());
        bytes
    }

    fn from_cursor_bytes(bytes: &[u8]) -> Result<Self, Status> {
        let bytes: [u8; CURSOR_FILTER_LEN] = bytes.try_into().map_err(|_| {
            ExplorerError::invalid_request("transaction-history cursor filter is invalid")
        })?;
        let is_coinbase = match bytes[0] {
            0 => None,
            1 => Some(false),
            2 => Some(true),
            _ => {
                return Err(ExplorerError::invalid_request(
                    "transaction-history cursor coinbase filter is invalid",
                )
                .into());
            }
        };
        Ok(Self {
            is_coinbase,
            privacy_shape_mask: u16::from_be_bytes([bytes[1], bytes[2]]),
            shielded_protocol_mask: bytes[3],
            minimum_shielded_component_count: u32::from_be_bytes([
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
        })
    }
}

fn read_cursor_u32(cursor: &[u8], offset: usize) -> Result<u32, Status> {
    let bytes: [u8; 4] = cursor
        .get(offset..offset.saturating_add(4))
        .ok_or_else(|| ExplorerError::invalid_request("transaction-history cursor is truncated"))?
        .try_into()
        .map_err(|_| ExplorerError::invalid_request("transaction-history cursor is truncated"))?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_cursor_u64(cursor: &[u8], offset: usize) -> Result<u64, Status> {
    let bytes: [u8; 8] = cursor
        .get(offset..offset.saturating_add(8))
        .ok_or_else(|| ExplorerError::invalid_request("transaction-history cursor is truncated"))?
        .try_into()
        .map_err(|_| ExplorerError::invalid_request("transaction-history cursor is truncated"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn decode_cursor_hash(cursor: &[u8], offset: usize) -> Result<String, Status> {
    let hash_end = offset + CURSOR_BLOCK_HASH_LEN;
    let hash = std::str::from_utf8(cursor.get(offset..hash_end).ok_or_else(|| {
        ExplorerError::invalid_request("transaction-history cursor is truncated")
    })?)
    .map_err(|_| ExplorerError::invalid_request("transaction-history cursor hash is invalid"))?
    .to_owned();
    decode_rpc_block_hash_hex(&hash).map_err(|_| {
        ExplorerError::invalid_request("transaction-history cursor hash is invalid")
    })?;
    Ok(hash)
}

fn materialized_view_read_fence(
    materialized_view_state: MaterializedViewState,
) -> TransactionHistoryReadFence {
    TransactionHistoryReadFence {
        chain_epoch_id: materialized_view_state.chain_epoch_id.value(),
        materialized_view_revision: materialized_view_state.revision,
        materialized_view_tip_height: materialized_view_state.tip_height.value(),
        materialized_view_tip_hash: encode_rpc_block_hash_hex(materialized_view_state.tip_hash),
    }
}

fn validate_request_read_fence(
    request_fence: Option<&TransactionHistoryReadFence>,
    materialized_view_state: MaterializedViewState,
) -> Result<(), Status> {
    if request_fence
        .is_some_and(|fence| fence != &materialized_view_read_fence(materialized_view_state))
    {
        return Err(ExplorerError::unsatisfied_precondition(
            "transaction-history read fence does not match the current materialized view",
        )
        .into());
    }
    Ok(())
}

fn materialized_view_coverage(
    materialized_view_state: MaterializedViewState,
) -> Option<TransactionHistoryCoverage> {
    materialized_view_state
        .coverage
        .map(|coverage| TransactionHistoryCoverage {
            complete_from_height: coverage.complete_from_height.value(),
            complete_through_height: coverage.complete_through_height.value(),
            complete_through_hash: encode_rpc_block_hash_hex(coverage.complete_through_hash),
        })
}

fn transaction_history_count_scope(
    total_matching_transactions: Option<u64>,
    materialized_view_state: MaterializedViewState,
) -> TransactionHistoryCountScope {
    if total_matching_transactions.is_some() && full_history_coverage(materialized_view_state) {
        TransactionHistoryCountScope::FullHistory
    } else {
        TransactionHistoryCountScope::Unspecified
    }
}

fn full_history_coverage(materialized_view_state: MaterializedViewState) -> bool {
    materialized_view_state.coverage.is_some_and(|coverage| {
        coverage.complete_from_height == BlockHeight::new(1)
            && coverage.complete_through_height == materialized_view_state.tip_height
            && coverage.complete_through_hash == materialized_view_state.tip_hash
    })
}

/// Hydrates `entries[*].paid_fee_zat` from the read snapshot's fee materialized view.
///
/// Coinbase rows are skipped (no fee record exists). Missing fee records leave
/// `paid_fee_zat` unset; that is the explicit "not available" signal.
fn join_projected_paid_fees(
    snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    entries: &mut [TransactionHistoryEntry],
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
    let keys: Vec<_> = lookup_targets
        .iter()
        .map(|(transaction_id, _)| encode_internal_transaction_id(*transaction_id))
        .collect();
    let values = snapshot
        .multi_get_consumer(TRANSACTION_FEES_COLUMN_FAMILY, &keys)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let mut records = HashMap::with_capacity(values.len());
    for ((transaction_id, privacy_shape), maybe_bytes) in lookup_targets.iter().copied().zip(values)
    {
        if let Some(bytes) = maybe_bytes
            && let Ok(mut record) = TransactionFeesRecord::decode(bytes.as_slice())
        {
            if privacy_shape != PrivacyShape::TransparentOnly {
                record.paid_fee_zat = None;
            }
            records.insert(transaction_id, record);
        }
    }
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

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use zinder_core::{BlockHash, ChainEpochId};
    use zinder_materialized_views::{
        MaterializedViewCoverage, MaterializedViewStoreOptions, TRANSPARENT_OUTPOINT_SPEND_SCHEMA,
    };
    use zinder_store::RocksDbResourceBudget;

    use super::*;

    fn test_options() -> MaterializedViewStoreOptions {
        MaterializedViewStoreOptions {
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            ..MaterializedViewStoreOptions::default()
        }
    }

    #[test]
    fn typed_readiness_distinguishes_omitted_materializing_and_verified_states()
    -> Result<(), Box<dyn std::error::Error>> {
        let partial_path = tempdir()?;
        let partial_store = MaterializedViewStore::open(
            partial_path.path(),
            MaterializedViewStoreOptions {
                consumers: &[TRANSPARENT_OUTPOINT_SPEND_SCHEMA],
                ..test_options()
            },
        )?;
        let partial_reader = TransactionHistoryMaterializedViewReader::new(partial_store);
        assert_eq!(
            partial_reader.readiness()?,
            TransactionHistoryMaterializedViewReadiness::Omitted
        );

        let explorer_path = tempdir()?;
        let explorer_store =
            MaterializedViewStore::open_explorer_views(explorer_path.path(), test_options())?;
        let explorer_reader = TransactionHistoryMaterializedViewReader::new(explorer_store.clone());
        assert_eq!(
            explorer_reader.readiness()?,
            TransactionHistoryMaterializedViewReadiness::Materializing
        );

        let partial_state = MaterializedViewState {
            chain_epoch_id: ChainEpochId::new(7),
            tip_height: BlockHeight::new(20),
            tip_hash: BlockHash::from_bytes([0x20; 32]),
            revision: 1,
            coverage: None,
        };
        explorer_store.put_consumer_state(TRANSACTION_HISTORY_CONSUMER_NAME, partial_state)?;
        assert!(explorer_reader.readiness()?.is_available());

        let checkpoint_state = MaterializedViewState {
            revision: 2,
            coverage: Some(MaterializedViewCoverage {
                complete_from_height: BlockHeight::new(8),
                complete_through_height: partial_state.tip_height,
                complete_through_hash: partial_state.tip_hash,
            }),
            ..partial_state
        };
        explorer_store.put_consumer_state(TRANSACTION_HISTORY_CONSUMER_NAME, checkpoint_state)?;
        assert!(explorer_reader.readiness()?.is_available());
        assert!(!full_history_coverage(checkpoint_state));

        let complete_state = MaterializedViewState {
            revision: 3,
            coverage: Some(MaterializedViewCoverage {
                complete_from_height: BlockHeight::new(1),
                complete_through_height: partial_state.tip_height,
                complete_through_hash: partial_state.tip_hash,
            }),
            ..partial_state
        };
        explorer_store.put_consumer_state(TRANSACTION_HISTORY_CONSUMER_NAME, complete_state)?;
        assert!(explorer_reader.readiness()?.is_available());
        assert!(full_history_coverage(complete_state));
        Ok(())
    }

    fn history_entry(block_height: u32, transaction_index: u32) -> TransactionHistoryEntry {
        TransactionHistoryEntry {
            block_height,
            transaction_index,
            block_hash: "00".repeat(32),
            ..TransactionHistoryEntry::default()
        }
    }

    #[test]
    fn scan_limit_cursor_advances_across_an_empty_filtered_page() -> Result<(), Status> {
        let page = HistoryPage {
            entries: Vec::new(),
            has_older: true,
            has_newer: false,
            scanned_entry_count: MAX_TRANSACTION_HISTORY_SCANNED_ENTRIES,
            scan_limit_reached: true,
            scan_progress: Some(history_entry(42, 7)),
        };
        let filter = HistoryFilter {
            is_coinbase: None,
            privacy_shape_mask: 0,
            shielded_protocol_mask: 0,
            minimum_shielded_component_count: 0,
        };
        let read_fence = TransactionHistoryReadFence {
            chain_epoch_id: 1,
            materialized_view_revision: 2,
            materialized_view_tip_height: 42,
            materialized_view_tip_hash: "00".repeat(32),
        };

        let cursor = history_page_cursor(
            &page,
            TransactionHistoryDirection::Older,
            TransactionHistoryDirection::Older,
            &filter,
            &read_fence,
        );
        let (anchor, decoded_filter, decoded_fence) = decode_history_cursor(&cursor)?;
        assert_eq!(anchor.block_height, BlockHeight::new(42));
        assert_eq!(anchor.transaction_index, 7);
        assert_eq!(decoded_filter, filter);
        assert_eq!(decoded_fence, read_fence);
        assert!(
            history_page_cursor(
                &page,
                TransactionHistoryDirection::Older,
                TransactionHistoryDirection::Newer,
                &filter,
                &read_fence,
            )
            .is_empty()
        );
        Ok(())
    }
}
