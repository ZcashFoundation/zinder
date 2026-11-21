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

    fn covers_selected_history(&self, chunk: BlockHeightRange) -> bool {
        let coverage = self.history.bounds();
        coverage.start <= chunk.start
            && chunk.end <= coverage.end
            && CompactBlockShape::for_range(&[]).ok() == Some(self.shape)
    }
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
        read: ChunkRead<Reads::View>,
        convert: Convert,
    ) -> ChunkReadOutcome<Output>
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, Vec<ReadBlock>) -> Output + Send + 'static,
    {
        if !self.covers_selected_history(read.chunk) {
            return native_chunk(reads, read, convert).await;
        }
        let (epoch, started, anchor) = {
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
            let anchor = if reads.block_hash_at(view, read.chunk.start).await.is_ok() {
                Some(self.history.terminal().check_view(reads, view).await)
            } else {
                None
            };
            (epoch, started, anchor)
        };
        match anchor {
            Some(Ok(())) => {}
            None => return native_chunk(reads, read, convert).await,
            Some(Err(status)) => {
                if matches!(read.view, ChunkView::Current)
                    && status
                        .get_error_details()
                        .error_info()
                        .is_some_and(|detail| {
                            detail.domain == ZINDER_ERROR_DOMAIN
                                && detail.reason
                                    == ErrorReason::ChainEpochPinUnavailable.as_str_name()
                        })
                {
                    return native_chunk(reads, read, convert).await;
                }
                return ChunkReadOutcome {
                    read_duration: Some(started.elapsed()),
                    converted: Err(ChunkReadFailure::Failed(status)),
                };
            }
        }
        drop(read.view);
        let history = self.history.clone();
        tokio::task::spawn_blocking(move || {
            let rows = history.read_chunk(read.chunk, read.order, read.max_bytes);
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
