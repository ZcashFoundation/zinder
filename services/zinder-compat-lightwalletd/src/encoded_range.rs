//! Canonical encoded compact-block range sources backed directly by
//! `ZSHRNG02` settled segments and `live_tail_block`, with canonical fallback
//! for transparent or custom views.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio_util::sync::CancellationToken;
use tonic::Status;
use zinder_core::{BlockHeightRange, ChainEpoch, CompactBlockArtifact};
use zinder_proto::compat::lightwalletd::{EncodedBlock, EncodedBlockBatcher, EncodedBlockFrame};
use zinder_store::{
    EncodedCompactBlockLink, EncodedCompactBlockOutput, EncodedCompactBlockPayload,
    EncodedCompactBlockRange, EncodedCompactBlockRangeRequest,
};

use crate::block_methods::{
    ChunkMessages, ChunkRead, ChunkReadFailure, ChunkReadOutcome, CompactBlockReads,
    EncodedChunkReadOutcome, RangeChunk, RangeSource,
};
use crate::block_range::BlockRangeStreamLimit;
use crate::compact_block_messages::CompactBlockShape;

enum EncodedChunkPayload {
    Direct(EncodedCompactBlockRange),
    Fallback(Vec<CompactBlockArtifact>),
}

/// Chunk yielded by [`EncodedRangeRead`] or [`EncodedFrameRangeRead`].
pub(crate) struct EncodedRangeChunk {
    payload: EncodedChunkPayload,
    /// Boundary links in ascending height order (`[lowest, highest]`).
    links: [EncodedCompactBlockLink; 2],
    link_len: usize,
}

impl EncodedRangeChunk {
    fn direct(range: EncodedCompactBlockRange) -> Self {
        let links = if range.first_link.height <= range.last_link.height {
            [range.first_link, range.last_link]
        } else {
            [range.last_link, range.first_link]
        };
        Self {
            payload: EncodedChunkPayload::Direct(range),
            links,
            link_len: 2,
        }
    }

    fn fallback(blocks: Vec<CompactBlockArtifact>) -> Self {
        let link = |block: &CompactBlockArtifact| EncodedCompactBlockLink {
            height: block.height(),
            block_hash: block.block_hash(),
            previous_block_hash: block.previous_block_hash(),
        };
        if let (Some(lowest), Some(highest)) = (blocks.first().map(link), blocks.last().map(link)) {
            Self {
                payload: EncodedChunkPayload::Fallback(blocks),
                links: [lowest, highest],
                link_len: 2,
            }
        } else {
            let placeholder = EncodedCompactBlockLink {
                height: zinder_core::BlockHeight::new(0),
                block_hash: zinder_core::BlockHash::from_bytes([0; 32]),
                previous_block_hash: zinder_core::BlockHash::from_bytes([0; 32]),
            };
            Self {
                payload: EncodedChunkPayload::Fallback(blocks),
                links: [placeholder, placeholder],
                link_len: 0,
            }
        }
    }
}

impl RangeChunk for EncodedRangeChunk {
    type Block = EncodedCompactBlockLink;

    fn kept_blocks(&self) -> usize {
        match &self.payload {
            EncodedChunkPayload::Direct(range) => range.block_count,
            EncodedChunkPayload::Fallback(blocks) => blocks.len(),
        }
    }

    fn endpoints(&self) -> &[Self::Block] {
        &self.links[..self.link_len]
    }
}

/// Range source producing individual protobuf-framed [`EncodedBlock`] messages.
#[derive(Clone)]
pub(crate) struct EncodedRangeRead {
    shape: CompactBlockShape,
    is_descending: bool,
    read_limit: BlockRangeStreamLimit,
}

impl EncodedRangeRead {
    pub(crate) const fn new(
        shape: CompactBlockShape,
        is_descending: bool,
        read_limit: BlockRangeStreamLimit,
    ) -> Self {
        Self {
            shape,
            is_descending,
            read_limit,
        }
    }
}

impl<Reads: CompactBlockReads> RangeSource<Reads> for EncodedRangeRead {
    type Chunk = EncodedRangeChunk;
    type Message = EncodedBlock;

    fn encoded_range_read_limit(
        &self,
        _chunk: BlockHeightRange,
        _retry: Option<BlockHeightRange>,
    ) -> Option<&BlockRangeStreamLimit> {
        self.shape
            .uses_canonical_encoded_range()
            .then_some(&self.read_limit)
    }

    fn messages(&self, chunk: Self::Chunk) -> Result<ChunkMessages<Self::Message>, Status> {
        match chunk.payload {
            EncodedChunkPayload::Direct(range) => match range.payload {
                EncodedCompactBlockPayload::Blocks(blocks) => {
                    Ok(blocks.into_iter().map(Ok).collect())
                }
                EncodedCompactBlockPayload::BatchedFrames(_) => Err(Status::internal(
                    "canonical encoded range returned batched frames for unbatched request",
                )),
            },
            EncodedChunkPayload::Fallback(mut blocks) => {
                if self.is_descending {
                    blocks.reverse();
                }
                blocks
                    .into_iter()
                    .map(|block| self.shape.encoded(block).map(Ok))
                    .collect()
            }
        }
    }

    fn read<Output, Convert>(
        &self,
        reads: &Reads,
        read: ChunkRead<Reads::View>,
        convert: Convert,
    ) -> impl Future<Output = ChunkReadOutcome<Output>> + Send
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, Self::Chunk) -> Output + Send + 'static,
    {
        CanonicalEncodedMode {
            shape: self.shape,
            batched_frames: false,
            opening_frame: false,
        }
        .read_chunk(reads, read, convert)
    }
}

/// Range source producing pre-batched [`EncodedBlockFrame`] slices for uncompressed streaming.
#[derive(Clone)]
pub(crate) struct EncodedFrameRangeRead {
    shape: CompactBlockShape,
    is_descending: bool,
    is_opening: Arc<AtomicBool>,
    read_limit: BlockRangeStreamLimit,
}

impl EncodedFrameRangeRead {
    pub(crate) fn new(
        shape: CompactBlockShape,
        is_descending: bool,
        read_limit: BlockRangeStreamLimit,
    ) -> Self {
        Self {
            shape,
            is_descending,
            is_opening: Arc::new(AtomicBool::new(true)),
            read_limit,
        }
    }
}

impl<Reads: CompactBlockReads> RangeSource<Reads> for EncodedFrameRangeRead {
    type Chunk = EncodedRangeChunk;
    type Message = EncodedBlockFrame;

    fn encoded_range_read_limit(
        &self,
        _chunk: BlockHeightRange,
        _retry: Option<BlockHeightRange>,
    ) -> Option<&BlockRangeStreamLimit> {
        self.shape
            .uses_canonical_encoded_range()
            .then_some(&self.read_limit)
    }

    fn messages(&self, chunk: Self::Chunk) -> Result<ChunkMessages<Self::Message>, Status> {
        match chunk.payload {
            EncodedChunkPayload::Direct(range) => match range.payload {
                EncodedCompactBlockPayload::BatchedFrames(frames) => {
                    let _ = self.is_opening.swap(false, Ordering::AcqRel);
                    Ok(frames.into_iter().map(Ok).collect())
                }
                EncodedCompactBlockPayload::Blocks(_) => Err(Status::internal(
                    "canonical encoded range returned single blocks for batched frame request",
                )),
            },
            EncodedChunkPayload::Fallback(mut blocks) => {
                if self.is_descending {
                    blocks.reverse();
                }
                let opening = self.is_opening.swap(false, Ordering::AcqRel);
                let mut batcher = EncodedBlockBatcher::new(opening);
                for block in blocks {
                    batcher.push_block(self.shape.encoded(block)?);
                }
                Ok(batcher.finish().into_iter().map(Ok).collect())
            }
        }
    }

    fn read<Output, Convert>(
        &self,
        reads: &Reads,
        read: ChunkRead<Reads::View>,
        convert: Convert,
    ) -> impl Future<Output = ChunkReadOutcome<Output>> + Send
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, Self::Chunk) -> Output + Send + 'static,
    {
        CanonicalEncodedMode {
            shape: self.shape,
            batched_frames: true,
            opening_frame: self.is_opening.load(Ordering::Acquire),
        }
        .read_chunk(reads, read, convert)
    }
}

#[derive(Clone, Copy)]
struct CanonicalEncodedMode {
    shape: CompactBlockShape,
    batched_frames: bool,
    opening_frame: bool,
}

impl CanonicalEncodedMode {
    async fn read_chunk<Reads, Output, Convert>(
        self,
        reads: &Reads,
        read: ChunkRead<Reads::View>,
        convert: Convert,
    ) -> ChunkReadOutcome<Output>
    where
        Reads: CompactBlockReads,
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, EncodedRangeChunk) -> Output + Send + 'static,
    {
        if read.cancel.is_cancelled() {
            return cancelled_outcome(None);
        }
        let Some((selection, include_transparent)) = self.shape.encoded_range_selection() else {
            return reads
                .read_compact_block_chunk(read, move |epoch, blocks| {
                    convert(epoch, EncodedRangeChunk::fallback(blocks))
                })
                .await;
        };
        let ChunkRead {
            chunk,
            order,
            max_bytes,
            view,
            cancel,
        } = read;
        let output = if self.batched_frames {
            EncodedCompactBlockOutput::BatchedFrames {
                opening_frame: self.opening_frame,
            }
        } else {
            EncodedCompactBlockOutput::Blocks
        };
        let request = EncodedCompactBlockRangeRequest {
            range: chunk,
            order,
            selection,
            include_transparent,
            output,
            max_bytes,
            enforce_wire_fee_limit: true,
        };
        let cancel_bridge = CancelBridge::new(&cancel);
        let outcome = reads
            .read_encoded_compact_block_chunk(view, request, Arc::clone(&cancel_bridge.flag))
            .await;
        drop(cancel_bridge);
        match outcome {
            EncodedChunkReadOutcome::Direct {
                read_at,
                range,
                read_duration,
            } => {
                if cancel.is_cancelled() {
                    return cancelled_outcome(Some(read_duration));
                }
                ChunkReadOutcome {
                    read_duration: Some(read_duration),
                    converted: Ok(convert(read_at, EncodedRangeChunk::direct(range))),
                }
            }
            EncodedChunkReadOutcome::Fallback(fallback_view) => {
                let fallback_read = ChunkRead {
                    chunk,
                    order,
                    max_bytes,
                    view: fallback_view,
                    cancel,
                };
                reads
                    .read_compact_block_chunk(fallback_read, move |epoch, blocks| {
                        convert(epoch, EncodedRangeChunk::fallback(blocks))
                    })
                    .await
            }
            EncodedChunkReadOutcome::Failed {
                read_duration,
                failure,
            } => ChunkReadOutcome {
                read_duration,
                converted: Err(failure),
            },
        }
    }
}

fn cancelled_outcome<Output>(
    read_duration: Option<std::time::Duration>,
) -> ChunkReadOutcome<Output> {
    ChunkReadOutcome {
        read_duration,
        converted: Err(ChunkReadFailure::Failed(Status::cancelled(
            "block-range stream cancelled",
        ))),
    }
}

struct CancelBridge {
    flag: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl CancelBridge {
    fn new(token: &CancellationToken) -> Self {
        let flag = Arc::new(AtomicBool::new(token.is_cancelled()));
        if token.is_cancelled() {
            return Self { flag, task: None };
        }
        let token = token.clone();
        let task_flag = Arc::clone(&flag);
        let task = tokio::spawn(async move {
            token.cancelled().await;
            task_flag.store(true, Ordering::Release);
        });
        Self {
            flag,
            task: Some(task),
        }
    }
}

impl Drop for CancelBridge {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
