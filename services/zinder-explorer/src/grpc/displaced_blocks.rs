//! `ExplorerQuery.DisplacedBlockHistory` and `DisplacedBlockDetail` handlers.

use std::{num::NonZeroU32, sync::Arc};

use tonic::{Request, Response, Status};
use zinder_core::{
    BlockHash, BlockHeight, ChainEpoch, DisplacedBlock, DisplacedBlockArchiveCoverage,
    wire::{
        decode_internal_block_hash, decode_rpc_block_hash_hex, encode_internal_block_hash,
        encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex,
    },
};
use zinder_materialized_views::MaterializedViewStore;
use zinder_proto::capabilities::{
    EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1, EXPLORER_CHAIN_DISPLACED_BLOCK_HISTORY_V1,
};
use zinder_proto::v1::explorer::{
    DisplacedBlockArchiveCoverage as WireCoverage, DisplacedBlockCanonicalCounterpart,
    DisplacedBlockCoinbaseOutput, DisplacedBlockDetailRequest, DisplacedBlockDetailResponse,
    DisplacedBlockHistoryEntry, DisplacedBlockHistoryRequest, DisplacedBlockHistoryResponse,
    DisplacedBlockSummary,
};
use zinder_store::{
    ArchivedDisplacedBlockSummary, CanonicalStoreError, DisplacedBlockCursor, chain_epoch_message,
};

use super::canonical_read::{
    SharedCanonicalSecondary, encode_canonical_counterpart, internal_canonical_error,
};
use super::clamp_max_entries;
use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};

const DEFAULT_DISPLACED_BLOCK_HISTORY_PAGE_SIZE: u32 = 64;
const MAX_DISPLACED_BLOCK_HISTORY_PAGE_SIZE: u32 = 4_096;
const CURSOR_PREFIX: &[u8; 4] = b"zdb1";
const CURSOR_LEN: usize = 4 + 8 + 4 + 32;

/// Shared dependencies for displaced-block handlers.
pub(crate) struct DisplacedBlocksContext<'store> {
    pub(crate) canonical_store: &'store SharedCanonicalSecondary,
    pub(crate) materialized_view_store: Option<&'store MaterializedViewStore>,
    pub(crate) upstream_observation_cache: &'store UpstreamObservationCache,
}

/// Serves one `ExplorerQuery.DisplacedBlockHistory` page.
pub(crate) async fn query_displaced_block_history(
    context: DisplacedBlocksContext<'_>,
    request: Request<DisplacedBlockHistoryRequest>,
) -> Result<Response<DisplacedBlockHistoryResponse>, Status> {
    let inner = request.into_inner();
    let page_size = clamp_max_entries(
        inner.page_size,
        DEFAULT_DISPLACED_BLOCK_HISTORY_PAGE_SIZE,
        MAX_DISPLACED_BLOCK_HISTORY_PAGE_SIZE,
    );
    let page_size_nz = NonZeroU32::new(page_size)
        .ok_or_else(|| ExplorerError::internal("displaced-block page size clamped to zero"))?;
    let after = decode_displaced_cursor(&inner.cursor)?;

    let store = Arc::clone(context.canonical_store);
    let (epoch, entries, next_cursor, has_more, total_count, coverage) =
        tokio::task::spawn_blocking(move || {
            read_displaced_history_page(&store, after.as_ref(), page_size_nz)
        })
        .await
        .map_err(|error| {
            ExplorerError::internal(format!("displaced-block history task failed: {error}"))
        })??;

    let freshness = attach_upstream_observation(
        context.upstream_observation_cache,
        build_explorer_freshness(
            context.materialized_view_store,
            EXPLORER_CHAIN_DISPLACED_BLOCK_HISTORY_V1,
            Some(chain_epoch_message(epoch)),
            0,
        )?,
    )
    .await;

    Ok(Response::new(DisplacedBlockHistoryResponse {
        freshness: Some(freshness),
        entries,
        next_cursor,
        has_more,
        total_count,
        coverage,
    }))
}

/// Serves one `ExplorerQuery.DisplacedBlockDetail` lookup.
pub(crate) async fn query_displaced_block_detail(
    context: DisplacedBlocksContext<'_>,
    request: Request<DisplacedBlockDetailRequest>,
) -> Result<Response<DisplacedBlockDetailResponse>, Status> {
    let inner = request.into_inner();
    let block_hash = decode_rpc_block_hash_hex(&inner.block_hash)
        .map_err(|error| ExplorerError::invalid_request(error.to_string()))?;

    let store = Arc::clone(context.canonical_store);
    let requested_hash_hex = inner.block_hash;
    let (epoch, block, counterpart, coverage) = tokio::task::spawn_blocking(move || {
        read_displaced_detail(&store, block_hash, &requested_hash_hex)
    })
    .await
    .map_err(|error| {
        ExplorerError::internal(format!("displaced-block detail task failed: {error}"))
    })??;

    let freshness = attach_upstream_observation(
        context.upstream_observation_cache,
        build_explorer_freshness(
            context.materialized_view_store,
            EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1,
            Some(chain_epoch_message(epoch)),
            0,
        )?,
    )
    .await;

    Ok(Response::new(DisplacedBlockDetailResponse {
        freshness: Some(freshness),
        block: Some(encode_full_displaced_block_summary(&block)),
        current_canonical_block: counterpart,
        raw_block_bytes: block.raw_block_bytes,
        coverage,
    }))
}

type HistoryPageRead = (
    ChainEpoch,
    Vec<DisplacedBlockHistoryEntry>,
    Vec<u8>,
    bool,
    u64,
    Option<WireCoverage>,
);

fn read_displaced_history_page(
    store: &SharedCanonicalSecondary,
    after: Option<&DisplacedBlockCursor>,
    limit: NonZeroU32,
) -> Result<HistoryPageRead, Status> {
    let guard = store.read();
    let epoch = guard.chain_epoch().map_err(internal_canonical_error)?;
    let page = match guard.displaced_block_summary_page(after, limit) {
        Ok(page) => page,
        Err(CanonicalStoreError::DisplacedArchiveInvalid { reason })
            if after.is_some() && reason.contains("archive cursor") =>
        {
            return Err(ExplorerError::invalid_request(reason).into());
        }
        Err(error) => return Err(internal_canonical_error(error)),
    };
    let total_count = guard
        .displaced_block_count()
        .map_err(internal_canonical_error)?;
    let coverage = guard
        .displaced_block_archive_coverage()
        .map_err(internal_canonical_error)?
        .map(encode_archive_coverage);

    let mut entries = Vec::with_capacity(page.blocks.len());
    for displaced in page.blocks {
        let counterpart = read_canonical_counterpart(&guard, displaced.header.height, &epoch)?;
        entries.push(DisplacedBlockHistoryEntry {
            block: Some(encode_displaced_block_summary(&displaced)),
            current_canonical_block: counterpart,
        });
    }
    drop(guard);
    let next_cursor = page
        .next_cursor
        .as_ref()
        .map_or_else(Vec::new, encode_displaced_cursor);
    Ok((
        epoch,
        entries,
        next_cursor,
        page.has_more,
        total_count,
        coverage,
    ))
}

fn read_displaced_detail(
    store: &SharedCanonicalSecondary,
    block_hash: BlockHash,
    requested_hash_hex: &str,
) -> Result<
    (
        ChainEpoch,
        DisplacedBlock,
        Option<DisplacedBlockCanonicalCounterpart>,
        Option<WireCoverage>,
    ),
    Status,
> {
    let guard = store.read();
    let epoch = guard.chain_epoch().map_err(internal_canonical_error)?;
    let Some(block) = guard
        .displaced_block_by_hash(block_hash)
        .map_err(internal_canonical_error)?
    else {
        return Err(ExplorerError::not_materialized(format!(
            "displaced block {requested_hash_hex} is absent from the archive",
        ))
        .into());
    };
    let counterpart = read_canonical_counterpart(&guard, block.header.height, &epoch)?;
    let coverage = guard
        .displaced_block_archive_coverage()
        .map_err(internal_canonical_error)?
        .map(encode_archive_coverage);
    drop(guard);
    Ok((epoch, block, counterpart, coverage))
}

fn read_canonical_counterpart(
    secondary: &zinder_store::RocksDbCanonicalSecondary,
    height: BlockHeight,
    epoch: &ChainEpoch,
) -> Result<Option<DisplacedBlockCanonicalCounterpart>, Status> {
    if height > epoch.visible_tip_height {
        return Ok(None);
    }
    let Some(facts) = secondary
        .block_replay_facts_at(height)
        .map_err(internal_canonical_error)?
    else {
        return Ok(None);
    };
    encode_canonical_counterpart(&facts).map(Some)
}

struct DisplacedSummaryFields<'a> {
    block_hash: BlockHash,
    header: &'a zinder_core::BlockHeaderArtifact,
    transaction_ids: &'a [zinder_core::TransactionId],
    coinbase_outputs: &'a [zinder_core::DisplacedBlockCoinbaseOutput],
    displacement_event_sequence: u64,
    displacement_epoch_id: u64,
    displaced_at_millis: u64,
}

fn encode_displaced_block_summary(block: &ArchivedDisplacedBlockSummary) -> DisplacedBlockSummary {
    encode_displaced_summary_fields(&DisplacedSummaryFields {
        block_hash: block.block_hash,
        header: &block.header,
        transaction_ids: &block.transaction_ids,
        coinbase_outputs: &block.coinbase_outputs,
        displacement_event_sequence: block.displacement_event_sequence,
        displacement_epoch_id: block.displacement_epoch.value(),
        displaced_at_millis: block.displaced_at.value(),
    })
}

fn encode_full_displaced_block_summary(block: &DisplacedBlock) -> DisplacedBlockSummary {
    encode_displaced_summary_fields(&DisplacedSummaryFields {
        block_hash: block.block_hash,
        header: &block.header,
        transaction_ids: &block.transaction_ids,
        coinbase_outputs: &block.coinbase_outputs,
        displacement_event_sequence: block.displacement_event_sequence,
        displacement_epoch_id: block.displacement_epoch.value(),
        displaced_at_millis: block.displaced_at.value(),
    })
}

fn encode_displaced_summary_fields(fields: &DisplacedSummaryFields<'_>) -> DisplacedBlockSummary {
    DisplacedBlockSummary {
        block_height: fields.header.height.value(),
        block_hash: encode_rpc_block_hash_hex(fields.block_hash),
        previous_block_hash: encode_rpc_block_hash_hex(fields.header.parent_hash),
        block_time_unix_seconds: fields.header.block_time,
        total_size_bytes: fields.header.block_size_bytes,
        difficulty_bits: fields.header.bits,
        transaction_ids: fields
            .transaction_ids
            .iter()
            .copied()
            .map(encode_rpc_transaction_id_hex)
            .collect(),
        coinbase_outputs: fields
            .coinbase_outputs
            .iter()
            .map(|output| DisplacedBlockCoinbaseOutput {
                output_index: output.output_index,
                value_zat: output.value_zat,
                script_pub_key: output.script_pub_key.clone(),
            })
            .collect(),
        displacement_event_sequence: fields.displacement_event_sequence,
        displacement_epoch_id: fields.displacement_epoch_id,
        displaced_at_millis: fields.displaced_at_millis,
    }
}

const fn encode_archive_coverage(coverage: DisplacedBlockArchiveCoverage) -> WireCoverage {
    WireCoverage {
        activation_event_sequence: coverage.activation_event_sequence,
        activation_epoch_id: coverage.activation_epoch.value(),
        activated_at_millis: coverage.activated_at.value(),
    }
}

fn encode_displaced_cursor(cursor: &DisplacedBlockCursor) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(CURSOR_LEN);
    encoded.extend_from_slice(CURSOR_PREFIX);
    encoded.extend_from_slice(&cursor.event_sequence().to_be_bytes());
    encoded.extend_from_slice(&cursor.height().value().to_be_bytes());
    encoded.extend_from_slice(&encode_internal_block_hash(cursor.block_hash()));
    encoded
}

fn decode_displaced_cursor(encoded: &[u8]) -> Result<Option<DisplacedBlockCursor>, Status> {
    if encoded.is_empty() {
        return Ok(None);
    }
    let bytes: &[u8; CURSOR_LEN] = encoded.try_into().map_err(|_| {
        ExplorerError::invalid_request("displaced-block cursor has unexpected byte length")
    })?;
    if &bytes[..4] != CURSOR_PREFIX {
        return Err(
            ExplorerError::invalid_request("displaced-block cursor version mismatch").into(),
        );
    }
    let event_sequence = u64::from_be_bytes(copy_array(&bytes[4..12])?);
    let height = u32::from_be_bytes(copy_array(&bytes[12..16])?);
    let block_hash_bytes: [u8; 32] = copy_array(&bytes[16..48])?;
    let block_hash = decode_internal_block_hash(&block_hash_bytes)
        .map_err(|error| ExplorerError::invalid_request(error.to_string()))?;
    Ok(Some(DisplacedBlockCursor::from_position(
        event_sequence,
        BlockHeight::new(height),
        block_hash,
    )))
}

fn copy_array<const N: usize>(slice: &[u8]) -> Result<[u8; N], Status> {
    slice
        .try_into()
        .map_err(|_| ExplorerError::invalid_request("displaced-block cursor is truncated").into())
}
