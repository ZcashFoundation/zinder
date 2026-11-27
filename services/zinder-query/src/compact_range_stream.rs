//! Bounded native compact ranges read against one admitted serving pair.

use std::{future::Future, sync::Arc};

use tokio::sync::mpsc;
use tracing::Instrument as _;
use zinder_core::{
    BlockHeight, BlockHeightRange, ChainEpochId, CompactBlockArtifact, CompactBlockNullifiers,
    CompactPool,
};
use zinder_store::{
    EncodedCompactBlockOutput, EncodedCompactBlockPayload, EncodedCompactBlockRangeRequest,
    HeightOrder, ShieldedProjectionSelection,
};

use crate::{
    COMPACT_BLOCK_STREAM_CHANNEL_CAPACITY, CompactBlockStream, DEFAULT_MAX_COMPACT_BLOCK_RANGE,
    QueryError, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
    grpc::CompactBlockStreamSlot, serving_read, validate_block_range,
};

#[derive(Clone, Copy)]
pub(crate) struct CompactRangeRead {
    range: BlockHeightRange,
    order: HeightOrder,
    max_bytes: usize,
}

impl CompactRangeRead {
    pub(crate) const fn new(range: BlockHeightRange, order: HeightOrder, max_bytes: usize) -> Self {
        Self {
            range,
            order,
            max_bytes,
        }
    }

    fn after(self, last: BlockHeight) -> Option<Self> {
        let range = match self.order {
            HeightOrder::Ascending if last < self.range.end => {
                BlockHeightRange::inclusive(last.next()?, self.range.end)
            }
            HeightOrder::Descending if last > self.range.start => {
                BlockHeightRange::inclusive(self.range.start, BlockHeight::new(last.value() - 1))
            }
            HeightOrder::Ascending | HeightOrder::Descending => return None,
        };
        Some(Self { range, ..self })
    }
}

pub(crate) struct CompactRangePage<Block> {
    last_height: Option<BlockHeight>,
    blocks: Vec<Block>,
}

pub(crate) async fn open<Block, Read, PageFuture>(
    slot: WalletServingPairSlot,
    first: CompactRangeRead,
    at_epoch_id: Option<ChainEpochId>,
    read_page: Read,
) -> Result<CompactBlockStream<Block>, QueryError>
where
    Block: Send + 'static,
    Read: Fn(Arc<WalletServingReadPair>, CompactRangeRead) -> PageFuture
        + Copy
        + Send
        + Sync
        + 'static,
    PageFuture: Future<Output = Result<CompactRangePage<Block>, QueryError>> + Send,
{
    let requested_blocks = Some(first.range.into_iter().len());
    let stream_slot = CompactBlockStreamSlot::current();
    serving_read::record("compact_blocks_in_range", requested_blocks, async {
        validate_block_range(first.range, DEFAULT_MAX_COMPACT_BLOCK_RANGE)?;
        let first_read_slot = stream_slot.clone();
        let first_pair_slot = slot.clone();
        // Keep admission until the first storage read ends even if the RPC
        // opening the stream is cancelled.
        let first_read = tokio::spawn(
            async move {
                let _stream_slot = first_read_slot;
                let pin = serving_read::SlotPin::new(&first_pair_slot, at_epoch_id);
                serving_read::read_at(pin, move |pair| async move {
                    let chain_epoch = WalletServingQuery::<()>::chain_epoch(&pair, at_epoch_id)?;
                    let page = read_page(Arc::clone(&pair), first).await?;
                    Ok((pair, chain_epoch, page))
                })
                .await
            }
            .in_current_span(),
        );
        let (pair, chain_epoch, page) =
            first_read
                .await
                .map_err(|error| QueryError::BlockingTaskFailed {
                    reason: error.to_string(),
                })??;
        let (blocks, receiver) = mpsc::channel(COMPACT_BLOCK_STREAM_CHANNEL_CAPACITY);
        tokio::spawn(
            async move {
                let _stream_slot = stream_slot;
                drive_range(slot, pair, first, read_page, page, blocks).await;
            }
            .in_current_span(),
        );
        Ok(CompactBlockStream {
            chain_epoch,
            blocks: receiver,
        })
    })
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "The driver owns the captured pair, read position, and channel for this stream."
)]
async fn drive_range<Block, Read, PageFuture>(
    slot: WalletServingPairSlot,
    mut pair: Arc<WalletServingReadPair>,
    mut read: CompactRangeRead,
    read_page: Read,
    mut page: CompactRangePage<Block>,
    blocks: mpsc::Sender<Result<Block, QueryError>>,
) where
    Block: Send + 'static,
    Read: Fn(Arc<WalletServingReadPair>, CompactRangeRead) -> PageFuture + Copy + Send + Sync,
    PageFuture: Future<Output = Result<CompactRangePage<Block>, QueryError>> + Send,
{
    let chain_epoch_id = pair.canonical_fence().chain_epoch_id();
    loop {
        let next = page.last_height.and_then(|last| read.after(last));
        let sent = tokio::select! {
            biased;
            () = pair.reads_revoked() => {
                let error = pair.revoked_read_error();
                drop(pair);
                let _ = blocks.send(Err(error)).await;
                return;
            }
            sent = send_page(&blocks, page.blocks) => sent,
        };
        let Some(next) = next.filter(|_| sent) else {
            return;
        };
        read = next;
        let next_page =
            serving_read::read_pair(&slot, pair, Some(chain_epoch_id), move |pair| async move {
                let page = read_page(Arc::clone(&pair), read).await?;
                Ok((pair, page))
            })
            .await;
        match next_page {
            Ok((read_pair, read_page)) => {
                pair = read_pair;
                page = read_page;
            }
            Err(error) => {
                let _ = blocks.send(Err(error)).await;
                return;
            }
        }
    }
}

async fn send_page<Block>(
    sender: &mpsc::Sender<Result<Block, QueryError>>,
    blocks: Vec<Block>,
) -> bool {
    for block in blocks {
        if sender.send(Ok(block)).await.is_err() {
            return false;
        }
    }
    true
}

pub(crate) async fn read_blocks(
    pair: Arc<WalletServingReadPair>,
    read: CompactRangeRead,
) -> Result<CompactRangePage<CompactBlockArtifact>, QueryError> {
    let mut blocks = pair
        .canonical()
        .compact_blocks_in_range_up_to(read.range, read.order, read.max_bytes)
        .await
        .map_err(QueryError::canonical_read)?;
    if read.order == HeightOrder::Descending {
        blocks.reverse();
    }
    let last_height = blocks.last().map(CompactBlockArtifact::height);
    Ok(CompactRangePage {
        blocks,
        last_height,
    })
}

pub(crate) fn nullifier_selection(pools: &[CompactPool]) -> ShieldedProjectionSelection {
    if pools.is_empty() {
        return ShieldedProjectionSelection::DEFAULT_NULLIFIERS;
    }
    ShieldedProjectionSelection::new(
        pools.contains(&CompactPool::Sapling),
        pools.contains(&CompactPool::Orchard),
        pools.contains(&CompactPool::Ironwood),
        true,
    )
}

pub(crate) async fn read_nullifiers(
    pair: Arc<WalletServingReadPair>,
    read: CompactRangeRead,
    selection: ShieldedProjectionSelection,
) -> Result<CompactRangePage<CompactBlockNullifiers>, QueryError> {
    let encoded = pair
        .canonical()
        .encoded_compact_block_range_up_to(
            EncodedCompactBlockRangeRequest {
                range: read.range,
                order: read.order,
                selection,
                output: EncodedCompactBlockOutput::Blocks,
                max_bytes: read.max_bytes,
                enforce_wire_fee_limit: false,
            },
            None,
        )
        .await
        .map_err(QueryError::canonical_read)?;
    let Some(encoded) = encoded else {
        return Err(QueryError::CompactBlockPayloadMalformed {
            height: read.range.start,
            reason: "a nonempty compact range returned no blocks".to_owned(),
        });
    };
    let EncodedCompactBlockPayload::Blocks(blocks) = encoded.payload else {
        return Err(QueryError::CompactBlockPayloadMalformed {
            height: encoded.first_link.height,
            reason: "a nullifier block read returned batched frames".to_owned(),
        });
    };
    let blocks = blocks
        .into_iter()
        .map(|block| {
            zinder_proto::wire::decode_projected_block_nullifiers(&block.payload()).map_err(
                |error| QueryError::CompactBlockPayloadMalformed {
                    height: encoded.first_link.height,
                    reason: error.to_string(),
                },
            )
        })
        .collect::<Result<_, _>>()?;
    Ok(CompactRangePage {
        blocks,
        last_height: Some(encoded.last_link.height),
    })
}
