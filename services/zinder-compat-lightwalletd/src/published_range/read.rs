use std::{sync::Arc, time::Instant};

use prost::{Message, bytes::Bytes};
use tonic::Status;
use tonic_types::StatusExt as _;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpoch, CompactBlockArtifact,
};
use zinder_proto::{ZINDER_ERROR_DOMAIN, v1::ops::ErrorReason};

use super::PublishedShieldedCatalog;
use crate::{
    block_methods::{
        ChunkMessages, ChunkRead, ChunkReadFailure, ChunkReadOutcome, ChunkView, CompactBlockReads,
        RangeSource, view_reaches,
    },
    block_range::RangeBlock,
    compact_block_messages::CompactBlockShape,
    sealed_history::SealedBlock,
};

#[derive(Clone)]
pub(crate) struct PublishedRangeRead {
    history: Arc<PublishedShieldedCatalog>,
    shape: CompactBlockShape,
    is_descending: bool,
}

pub(crate) struct ReadBlock {
    id: BlockId,
    parent: BlockHash,
    payload: Payload,
}

enum Payload {
    Canonical(CompactBlockArtifact),
    Published(Bytes),
}

impl From<CompactBlockArtifact> for ReadBlock {
    fn from(block: CompactBlockArtifact) -> Self {
        Self {
            id: BlockId::new(block.height(), block.block_hash()),
            parent: block.previous_block_hash(),
            payload: Payload::Canonical(block),
        }
    }
}

impl From<SealedBlock> for ReadBlock {
    fn from(row: SealedBlock) -> Self {
        Self {
            id: row.id,
            parent: row.parent,
            payload: Payload::Published(row.payload),
        }
    }
}

impl RangeBlock for ReadBlock {
    fn height(&self) -> BlockHeight {
        self.id.height
    }
    fn block_hash(&self) -> BlockHash {
        self.id.hash
    }
    fn previous_block_hash(&self) -> BlockHash {
        self.parent
    }
}

impl PublishedRangeRead {
    pub(crate) fn new(
        history: Arc<PublishedShieldedCatalog>,
        shape: CompactBlockShape,
        is_descending: bool,
    ) -> Self {
        Self {
            history,
            shape,
            is_descending,
        }
    }

    fn encoded_span(
        &self,
        chunk: BlockHeightRange,
        order: zinder_store::HeightOrder,
        tail: &super::tail::EncodedTail,
    ) -> (BlockHeightRange, bool) {
        use zinder_store::HeightOrder;
        let catalog = self.history.bounds();
        let start = match order {
            HeightOrder::Ascending => chunk.start,
            HeightOrder::Descending => chunk.end,
        };
        let mut spans = vec![catalog];
        if let Some(live) = tail.bounds() {
            if catalog.end.next().is_some_and(|next| next >= live.start) {
                spans[0].end = catalog.end.max(live.end);
            } else {
                spans.push(live);
            }
        }
        let mut span = chunk;
        for covered in spans {
            if (covered.start..=covered.end).contains(&start) {
                span.start = span.start.max(covered.start);
                span.end = span.end.min(covered.end);
                return (span, true);
            }
            match order {
                HeightOrder::Ascending if covered.start > start => {
                    span.end = span.end.min(BlockHeight::new(covered.start.value() - 1));
                }
                HeightOrder::Descending if covered.end < start => {
                    span.start = span.start.max(BlockHeight::new(covered.end.value() + 1));
                }
                HeightOrder::Ascending | HeightOrder::Descending => {}
            }
        }
        (span, false)
    }

    fn encoded_rows(
        &self,
        span: BlockHeightRange,
        order: zinder_store::HeightOrder,
        max_bytes: usize,
        tail: Option<&super::tail::EncodedTail>,
    ) -> std::io::Result<Vec<SealedBlock>> {
        use zinder_store::HeightOrder;
        let end = self.history.bounds().end;
        if span.end <= end {
            return self.history.read_chunk(span, order, max_bytes, true);
        }
        let tail = tail.ok_or_else(|| std::io::Error::other("missing encoded tail"))?;
        if span.start > end {
            return Ok(tail.select(span, order, max_bytes, true));
        }
        let prefix = BlockHeightRange::inclusive(span.start, end);
        let suffix = BlockHeightRange::inclusive(BlockHeight::new(end.value() + 1), span.end);
        let (mut first, first_span) = match order {
            HeightOrder::Ascending => (
                self.history.read_chunk(prefix, order, max_bytes, true)?,
                prefix,
            ),
            HeightOrder::Descending => (tail.select(suffix, order, max_bytes, true), suffix),
        };
        let room = first.iter().fold(max_bytes, |budget, row| {
            budget.saturating_sub(row.native_bytes)
        });
        if room == 0
            || first.len() as u64
                != u64::from(first_span.end.value() - first_span.start.value()) + 1
        {
            return Ok(first);
        }
        match order {
            HeightOrder::Ascending => first.extend(tail.select(suffix, order, room, false)),
            HeightOrder::Descending => {
                let mut earlier = self.history.read_chunk(prefix, order, room, false)?;
                earlier.append(&mut first);
                first = earlier;
            }
        }
        Ok(first)
    }

    fn covers_selected_history(&self, chunk: BlockHeightRange) -> bool {
        let coverage = self.history.bounds();
        coverage.start <= chunk.start
            && chunk.end <= coverage.end
            && CompactBlockShape::for_range(&[]).ok() == Some(self.shape)
    }
}

fn pin_unavailable(status: &Status) -> bool {
    status
        .get_error_details()
        .error_info()
        .is_some_and(|detail| {
            detail.domain == ZINDER_ERROR_DOMAIN
                && detail.reason == ErrorReason::ChainEpochPinUnavailable.as_str_name()
        })
}

fn task_failure(error: &tokio::task::JoinError) -> ChunkReadFailure {
    ChunkReadFailure::Failed(Status::internal(error.to_string()))
}

async fn native_chunk<Reads, Output, Convert>(
    reads: &Reads,
    read: ChunkRead<Reads::View>,
    convert: Convert,
) -> ChunkReadOutcome<Output>
where
    Reads: CompactBlockReads,
    Output: Send + 'static,
    Convert: FnOnce(ChainEpoch, Vec<ReadBlock>) -> Output + Send + 'static,
{
    reads
        .read_compact_block_chunk(read, move |epoch, blocks| {
            convert(epoch, blocks.into_iter().map(Into::into).collect())
        })
        .await
}

impl<Reads: CompactBlockReads> RangeSource<Reads> for PublishedRangeRead {
    type Block = ReadBlock;
    type Message = Bytes;

    fn messages(&self, mut blocks: Vec<ReadBlock>) -> Result<ChunkMessages<Bytes>, Status> {
        if self.is_descending {
            blocks.reverse();
        }
        blocks
            .into_iter()
            .map(|block| match block.payload {
                Payload::Published(bytes) => Ok(Ok(bytes)),
                Payload::Canonical(block) => {
                    Ok(Ok(self.shape.message(block)?.encode_to_vec().into()))
                }
            })
            .collect()
    }

    async fn read<Output, Convert>(
        &self,
        reads: &Reads,
        mut read: ChunkRead<Reads::View>,
        convert: Convert,
    ) -> ChunkReadOutcome<Output>
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, Vec<ReadBlock>) -> Output + Send + 'static,
    {
        if CompactBlockShape::for_range(&[]).ok() != Some(self.shape) {
            return native_chunk(reads, read, convert).await;
        }
        let (epoch, started, anchor, tail) = {
            let current;
            let view = match &read.view {
                ChunkView::Captured(view) => view,
                ChunkView::Current => {
                    current = match reads.capture_view().await {
                        Ok(view) => view,
                        Err(status) => {
                            return ChunkReadOutcome::unread(ChunkReadFailure::Failed(status));
                        }
                    };
                    &current
                }
            };
            let epoch = Reads::view_chain_epoch(view);
            if !view_reaches(&epoch, read.chunk) {
                return ChunkReadOutcome::unread(ChunkReadFailure::TipBelowChunk(epoch));
            }
            let started = Instant::now();
            // A replacement with the same settled tip can have a higher retention floor.
            let tail = self.history.tail.load_full().filter(|tail| {
                tail.epoch == epoch
                    && reads.range_fence(view) == Some(tail.fence)
                    && read.chunk.start >= tail.fence.1.first_available_height()
            });
            let anchor = if let Some(live) = &tail {
                let (span, encoded) = self.encoded_span(read.chunk, read.order, live);
                read.chunk = span;
                if !encoded {
                    return native_chunk(reads, read, convert).await;
                }
                Some(Ok(()))
            } else if self.covers_selected_history(read.chunk)
                && reads.block_hash_at(view, read.chunk.start).await.is_ok()
            {
                Some(self.history.terminal().check_view(reads, view).await)
            } else {
                None
            };
            (epoch, started, anchor, tail)
        };
        match anchor {
            Some(Ok(())) => {}
            None => return native_chunk(reads, read, convert).await,
            Some(Err(status)) => {
                if matches!(read.view, ChunkView::Current) && pin_unavailable(&status) {
                    return native_chunk(reads, read, convert).await;
                }
                return ChunkReadOutcome {
                    read_duration: Some(started.elapsed()),
                    converted: Err(ChunkReadFailure::Failed(status)),
                };
            }
        }
        drop(read.view);
        let source = self.clone();
        tokio::task::spawn_blocking(move || {
            let rows = source.encoded_rows(read.chunk, read.order, read.max_bytes, tail.as_deref());
            let read_duration = Some(started.elapsed());
            let converted = rows
                .map(|rows| convert(epoch, rows.into_iter().map(Into::into).collect()))
                .map_err(|error| ChunkReadFailure::Failed(Status::data_loss(error.to_string())));
            ChunkReadOutcome {
                read_duration,
                converted,
            }
        })
        .await
        .unwrap_or_else(|error| ChunkReadOutcome::unread(task_failure(&error)))
    }
}
