//! `ExplorerQuery` block-view handlers.
//!
//! Both reads project the materialized `BlockSummaryRecord` payloads written
//! by [`zinder_materialized_views::BlockSummaryConsumer`] into the
//! public wire shapes. The handlers wrap reads in the cross-cutting
//! [`ExplorerFreshness`] envelope per
//! [ADR-0011](../../../docs/adrs/0011-explorer-freshness-envelope.md) and
//! compute `materialized_view_cursor_lag_blocks` against the wallet plane's visible
//! tip.

use prost::Message as _;
use tonic::{Request, Response, Status};
use zinder_core::{BlockHeight, wire::encode_height_key_ascending};
use zinder_proto::capabilities::{EXPLORER_BLOCK_DETAIL_V1, EXPLORER_BLOCK_SUMMARY_V1};
use zinder_proto::v1::explorer::{
    BlockDetailRequest, BlockDetailResponse, BlockSummariesInRangeRequest,
    BlockSummariesInRangeResponse, BlockSummary, BlockSummaryRecord, block_detail_request,
};
use zinder_proto::v1::wallet::{
    self, BlockSelector, VisibleTipBlockRequest, block_selector,
    wallet_query_client::WalletQueryClient,
};
use zinder_runtime::AuthenticatedChannel;

use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use zinder_materialized_views::{BLOCK_SUMMARY_COLUMN_FAMILY, MaterializedViewStore};

/// Hard cap on the number of block summaries one range request returns.
///
/// The wire response is a single repeated field; a multi-million-row request
/// would blow up the gRPC buffer. The cap mirrors the bounded-page rule used
/// across other explorer reads.
const MAX_BLOCK_SUMMARIES_PER_REQUEST: u32 = 1024;

struct MaterializedBlockView {
    summary: zinder_proto::v1::explorer::BlockSummary,
    transaction_ids: Vec<String>,
    chain_epoch: wallet::ChainEpoch,
}

pub(crate) async fn query_block_summaries_in_range(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<BlockSummariesInRangeRequest>,
) -> Result<Response<BlockSummariesInRangeResponse>, Status> {
    let inner = request.into_inner();
    let start_height = inner.start_height;
    let end_height = inner.end_height;
    validate_block_view_range(start_height, end_height)?;

    let (chain_epoch, canonical_tip) = read_canonical_tip(wallet_client).await?;
    let mut summaries =
        read_materialized_block_summaries(materialized_view_store, start_height, end_height)?;
    for summary in &mut summaries {
        annotate_request_time_fields(summary, canonical_tip);
    }

    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_BLOCK_SUMMARY_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;

    Ok(Response::new(BlockSummariesInRangeResponse {
        freshness: Some(freshness),
        summaries,
    }))
}

fn validate_block_view_range(start_height: u32, end_height: u32) -> Result<(), Status> {
    if end_height < start_height {
        return Err(ExplorerError::invalid_request("end_height must be >= start_height").into());
    }
    let span = u64::from(end_height) - u64::from(start_height) + 1;
    if span > u64::from(MAX_BLOCK_SUMMARIES_PER_REQUEST) {
        return Err(ExplorerError::invalid_request(format!(
            "requested span {span} blocks exceeds the per-request cap of \
             {MAX_BLOCK_SUMMARIES_PER_REQUEST}",
        ))
        .into());
    }
    Ok(())
}

fn read_materialized_block_summaries(
    materialized_view_store: &MaterializedViewStore,
    start_height: u32,
    end_height: u32,
) -> Result<Vec<BlockSummary>, Status> {
    read_materialized_block_records(materialized_view_store, start_height, end_height)?
        .into_iter()
        .map(|record| {
            record
                .summary
                .ok_or_else(|| ExplorerError::internal("BlockSummaryRecord.summary missing").into())
        })
        .collect()
}

fn read_materialized_block_records(
    materialized_view_store: &MaterializedViewStore,
    start_height: u32,
    end_height: u32,
) -> Result<Vec<BlockSummaryRecord>, Status> {
    let start_key = encode_height_key_ascending(BlockHeight::new(start_height));
    let end_key = encode_height_key_ascending(BlockHeight::new(end_height));
    materialized_view_store
        .range_iterate_consumer(
            BLOCK_SUMMARY_COLUMN_FAMILY,
            &start_key,
            &end_key,
            MAX_BLOCK_SUMMARIES_PER_REQUEST as usize,
        )
        .map_err(|error| ExplorerError::internal(error.to_string()))?
        .into_iter()
        .map(|(_, payload)| {
            BlockSummaryRecord::decode(payload.as_slice()).map_err(|error| {
                ExplorerError::internal(format!("BlockSummaryRecord decode failed: {error}")).into()
            })
        })
        .collect()
}

pub(crate) async fn query_block_detail(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<BlockDetailRequest>,
) -> Result<Response<BlockDetailResponse>, Status> {
    let inner = request.into_inner();
    let materialized =
        read_materialized_block_view(materialized_view_store, wallet_client, &inner).await?;
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_BLOCK_DETAIL_V1,
            Some(materialized.chain_epoch),
            0,
        )?,
    )
    .await;
    Ok(Response::new(BlockDetailResponse {
        freshness: Some(freshness),
        summary: Some(materialized.summary),
        transaction_ids: materialized.transaction_ids,
    }))
}

async fn read_materialized_block_view(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    request: &BlockDetailRequest,
) -> Result<MaterializedBlockView, Status> {
    let height = resolve_block_height(wallet_client, request).await?;
    let key = encode_height_key_ascending(BlockHeight::new(height));
    let payload = materialized_view_store
        .get_consumer(BLOCK_SUMMARY_COLUMN_FAMILY, &key)
        .map_err(|error| ExplorerError::internal(error.to_string()))?
        .ok_or_else(|| {
            ExplorerError::not_materialized(format!(
                "BlockSummary is not materialized for height {height}"
            ))
        })?;
    let record = BlockSummaryRecord::decode(payload.as_slice()).map_err(|error| {
        ExplorerError::internal(format!("BlockSummaryRecord decode failed: {error}"))
    })?;
    let mut summary = record
        .summary
        .ok_or_else(|| ExplorerError::internal("BlockSummaryRecord.summary missing"))?;
    let (chain_epoch, canonical_tip) = read_canonical_tip(wallet_client).await?;
    annotate_request_time_fields(&mut summary, canonical_tip);

    Ok(MaterializedBlockView {
        summary,
        transaction_ids: record.transaction_ids,
        chain_epoch,
    })
}

fn annotate_request_time_fields(
    summary: &mut zinder_proto::v1::explorer::BlockSummary,
    canonical_tip: u32,
) {
    summary.confirmations = canonical_tip
        .saturating_sub(summary.block_height)
        .saturating_add(1);
    summary.is_canonical = summary.block_height <= canonical_tip;
}

async fn resolve_block_height(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    request: &BlockDetailRequest,
) -> Result<u32, Status> {
    match request
        .selector
        .as_ref()
        .ok_or_else(|| ExplorerError::invalid_request("BlockDetailRequest.selector is required"))?
    {
        block_detail_request::Selector::BlockHeight(height) => Ok(*height),
        block_detail_request::Selector::BlockHash(hash) => {
            let selector = BlockSelector {
                selector: Some(block_selector::Selector::Hash(hash.clone())),
            };
            let response = wallet_client
                .block_id_by_selector(Request::new(wallet::BlockSelectorRequest {
                    selector: Some(selector),
                    at_epoch_id: request.at_epoch_id,
                }))
                .await?
                .into_inner();
            let block_id = response
                .block_id
                .ok_or_else(|| ExplorerError::internal("BlockIdResponse.block_id missing"))?;
            Ok(block_id.height)
        }
    }
}

async fn read_canonical_tip(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
) -> Result<(wallet::ChainEpoch, u32), Status> {
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
    let canonical_tip = latest
        .visible_tip_block
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.visible_tip_block missing")
        })?
        .height;
    Ok((chain_epoch, canonical_tip))
}
