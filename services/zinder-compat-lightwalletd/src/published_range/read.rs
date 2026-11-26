use std::{io::ErrorKind, sync::Arc, time::Instant};

use tokio_util::sync::CancellationToken;
use tonic::Status;
use tonic_types::StatusExt as _;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpoch, CompactBlockArtifact,
};
use zinder_proto::{
    ZINDER_ERROR_DOMAIN,
    compat::lightwalletd::{EncodedBlock, EncodedBlockBatcher, EncodedBlockFrame},
    v1::ops::ErrorReason,
};
use zinder_store::{HeightOrder, ShieldedProjectionSelection};

use super::{
    PublishedShieldedCatalog,
    tail::{EncodedTail, Fence, ShieldedTailBuffer, TailLookupOutcome, TailReadLimits},
};
use crate::{
    block_methods::{
        ChunkMessages, ChunkRead, ChunkReadFailure, ChunkReadOutcome, ChunkView, CompactBlockReads,
        RangeChunk, RangeSource, view_reaches,
    },
    block_range::RangeBlock,
    compact_block_messages::{CompactBlockShape, PublishedProjection},
    sealed_history::{BlockLink, ChunkSpan, SealedBlock, WireProjectionScratch},
};

#[derive(Clone)]
pub(crate) struct PublishedRangeRead {
    history: Option<Arc<PublishedShieldedCatalog>>,
    tail: ShieldedTailBuffer,
    limits: crate::block_range::BlockRangeStreamLimit,
    shape: CompactBlockShape,
    is_descending: bool,
}

#[derive(Clone)]
pub(crate) struct PublishedBatchedRangeRead(PublishedRangeRead);

pub(crate) enum PrebatchedChunk {
    Empty,
    Encoded {
        kept_blocks: usize,
        endpoints: [BlockLink; 2],
        frames: Vec<EncodedBlockFrame>,
    },
    Canonical {
        is_first_chunk: bool,
        endpoints: [BlockLink; 2],
        blocks: Vec<CompactBlockArtifact>,
    },
}

impl PrebatchedChunk {
    fn from_canonical(is_first_chunk: bool, blocks: Vec<CompactBlockArtifact>) -> Self {
        let (Some(first), Some(last)) = (blocks.first(), blocks.last()) else {
            return Self::Empty;
        };
        let endpoints = [
            BlockLink {
                id: BlockId::new(first.height(), first.block_hash()),
                parent: first.previous_block_hash(),
            },
            BlockLink {
                id: BlockId::new(last.height(), last.block_hash()),
                parent: last.previous_block_hash(),
            },
        ];
        Self::Canonical {
            is_first_chunk,
            endpoints,
            blocks,
        }
    }
}

impl RangeChunk for PrebatchedChunk {
    type Block = BlockLink;

    fn kept_blocks(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Encoded { kept_blocks, .. } => *kept_blocks,
            Self::Canonical { blocks, .. } => blocks.len(),
        }
    }

    fn endpoints(&self) -> &[BlockLink] {
        match self {
            Self::Empty => &[],
            Self::Encoded { endpoints, .. } | Self::Canonical { endpoints, .. } => endpoints,
        }
    }
}

impl PublishedBatchedRangeRead {
    pub(crate) fn new(
        history: Option<Arc<PublishedShieldedCatalog>>,
        tail: ShieldedTailBuffer,
        shape: CompactBlockShape,
        is_descending: bool,
        limits: crate::block_range::BlockRangeStreamLimit,
    ) -> Self {
        Self(PublishedRangeRead::new(
            history,
            tail,
            shape,
            is_descending,
            limits,
        ))
    }
}

pub(crate) struct ReadBlock {
    id: BlockId,
    parent: BlockHash,
    payload: Payload,
}

enum Payload {
    Canonical(CompactBlockArtifact),
    Published(EncodedBlock),
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
            payload: Payload::Published(row.message),
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

enum AnchorPlan {
    UseEncoded(Option<Arc<EncodedTail>>),
    FallbackNative,
    Failed(Status),
}

impl PublishedRangeRead {
    pub(crate) fn new(
        history: Option<Arc<PublishedShieldedCatalog>>,
        tail: ShieldedTailBuffer,
        shape: CompactBlockShape,
        is_descending: bool,
        limits: crate::block_range::BlockRangeStreamLimit,
    ) -> Self {
        Self {
            history,
            tail,
            limits,
            shape,
            is_descending,
        }
    }

    /// Checks the whole offer. A descending offer that starts below the
    /// catalog can join it only from a live tail above the catalog.
    fn reads_published_files(&self, chunk: BlockHeightRange) -> bool {
        self.shape.uses_published_range()
            && self.history.as_ref().is_some_and(|catalog| {
                let coverage = catalog.bounds();
                (coverage.start..=coverage.end).contains(&chunk.start)
                    || (self.is_descending
                        && chunk.start < coverage.start
                        && coverage.end < chunk.end)
            })
    }

    fn order(&self) -> HeightOrder {
        if self.is_descending {
            HeightOrder::Descending
        } else {
            HeightOrder::Ascending
        }
    }

    fn encoded_span(
        &self,
        chunk: BlockHeightRange,
        order: HeightOrder,
        tail: &EncodedTail,
    ) -> Option<BlockHeightRange> {
        let first = match order {
            HeightOrder::Ascending => chunk.start,
            HeightOrder::Descending => chunk.end,
        };
        let mut catalog = self.history.as_ref().map(|catalog| catalog.bounds());
        let mut live = tail.bounds();
        if let (Some(prefix), Some(suffix)) = (&mut catalog, live)
            && prefix.end.next().is_some_and(|next| next >= suffix.start)
        {
            prefix.end = prefix.end.max(suffix.end);
            live = None;
        }
        [catalog, live]
            .into_iter()
            .flatten()
            .find(|covered| (covered.start..=covered.end).contains(&first))
            .map(|covered| {
                BlockHeightRange::inclusive(
                    chunk.start.max(covered.start),
                    chunk.end.min(covered.end),
                )
            })
    }

    fn encoded_rows(
        &self,
        projection: PublishedProjection,
        (span, order, max_bytes): (BlockHeightRange, HeightOrder, usize),
        (tail, cancel): (Option<&EncodedTail>, &CancellationToken),
    ) -> std::io::Result<Vec<SealedBlock>> {
        let mut rows =
            self.raw_slab_rows(projection.slab(), (span, order, max_bytes), (tail, cancel))?;
        if let Some(selection) = projection.selection() {
            project_sealed_blocks(&mut rows, selection, Some(cancel))?;
        }
        Ok(rows)
    }

    fn raw_slab_rows(
        &self,
        slab: crate::sealed_history::SealedSlab,
        (span, order, max_bytes): (BlockHeightRange, HeightOrder, usize),
        (tail, cancel): (Option<&EncodedTail>, &CancellationToken),
    ) -> std::io::Result<Vec<SealedBlock>> {
        if cancel.is_cancelled() {
            return Err(std::io::Error::new(
                ErrorKind::Interrupted,
                "block-range stream cancelled",
            ));
        }
        let Some(history) = &self.history else {
            let live = tail.ok_or_else(|| std::io::Error::other("missing encoded tail"))?;
            return Ok(live.select(slab, span, order, max_bytes, true));
        };
        let end = history.bounds().end;
        if span.end <= end {
            return history.read_chunk(slab, span, order, max_bytes, (true, cancel));
        }
        let live = tail.ok_or_else(|| std::io::Error::other("missing encoded tail"))?;
        if span.start > end {
            return Ok(live.select(slab, span, order, max_bytes, true));
        }
        let prefix = BlockHeightRange::inclusive(span.start, end);
        let suffix = BlockHeightRange::inclusive(BlockHeight::new(end.value() + 1), span.end);
        let (mut first, first_span) = match order {
            HeightOrder::Ascending => (
                history.read_chunk(slab, prefix, order, max_bytes, (true, cancel))?,
                prefix,
            ),
            HeightOrder::Descending => (live.select(slab, suffix, order, max_bytes, true), suffix),
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
            HeightOrder::Ascending => first.extend(live.select(slab, suffix, order, room, false)),
            HeightOrder::Descending => {
                let mut earlier = history.read_chunk(slab, prefix, order, room, (false, cancel))?;
                earlier.append(&mut first);
                first = earlier;
            }
        }
        Ok(first)
    }

    fn encoded_frames(
        &self,
        projection: PublishedProjection,
        (span, order, max_bytes, is_first_chunk): (BlockHeightRange, HeightOrder, usize, bool),
        (tail, cancel): (Option<&EncodedTail>, &CancellationToken),
    ) -> std::io::Result<PrebatchedChunk> {
        if cancel.is_cancelled() {
            return Err(std::io::Error::new(
                ErrorKind::Interrupted,
                "block-range stream cancelled",
            ));
        }
        let mut scratch = WireProjectionScratch::default();
        let mut batcher = EncodedBlockBatcher::new(is_first_chunk);
        let chunk_span = match &self.history {
            None => {
                let live = tail.ok_or_else(|| std::io::Error::other("missing encoded tail"))?;
                live.append_projected_frames(
                    projection,
                    (span, order),
                    (max_bytes, true),
                    (cancel, &mut scratch, &mut batcher),
                )?
            }
            Some(history) if span.end <= history.bounds().end => history.append_chunk_frames(
                projection,
                (span, order),
                (max_bytes, true),
                (cancel, &mut scratch, &mut batcher),
            )?,
            Some(history) if span.start > history.bounds().end => {
                let live = tail.ok_or_else(|| std::io::Error::other("missing encoded tail"))?;
                live.append_projected_frames(
                    projection,
                    (span, order),
                    (max_bytes, true),
                    (cancel, &mut scratch, &mut batcher),
                )?
            }
            Some(history) => Self::cross_boundary_frames(
                history,
                tail.ok_or_else(|| std::io::Error::other("missing encoded tail"))?,
                (projection, span, order, max_bytes),
                (cancel, &mut scratch, &mut batcher),
            )?,
        };
        let Some(chunk_span) = chunk_span else {
            return Ok(PrebatchedChunk::Empty);
        };
        Ok(PrebatchedChunk::Encoded {
            kept_blocks: chunk_span.kept_blocks,
            endpoints: chunk_span.endpoints(),
            frames: batcher.finish(),
        })
    }

    fn cross_boundary_frames(
        history: &PublishedShieldedCatalog,
        live: &EncodedTail,
        (projection, span, order, max_bytes): (
            PublishedProjection,
            BlockHeightRange,
            HeightOrder,
            usize,
        ),
        (cancel, scratch, batcher): (
            &CancellationToken,
            &mut WireProjectionScratch,
            &mut EncodedBlockBatcher,
        ),
    ) -> std::io::Result<Option<ChunkSpan>> {
        let end = history.bounds().end;
        let prefix = BlockHeightRange::inclusive(span.start, end);
        let suffix = BlockHeightRange::inclusive(BlockHeight::new(end.value() + 1), span.end);
        let (first_span, first_range) = match order {
            HeightOrder::Ascending => (
                history.append_chunk_frames(
                    projection,
                    (prefix, order),
                    (max_bytes, true),
                    (cancel, scratch, batcher),
                )?,
                prefix,
            ),
            HeightOrder::Descending => (
                live.append_projected_frames(
                    projection,
                    (suffix, order),
                    (max_bytes, true),
                    (cancel, scratch, batcher),
                )?,
                suffix,
            ),
        };
        let Some(first_span) = first_span else {
            return Ok(None);
        };
        let room = max_bytes.saturating_sub(first_span.kept_native_bytes);
        let whole_first = first_span.kept_blocks as u64
            == u64::from(first_range.end.value() - first_range.start.value()) + 1;
        if room == 0 || !whole_first {
            return Ok(Some(first_span));
        }
        let second_span = match order {
            HeightOrder::Ascending => live.append_projected_frames(
                projection,
                (suffix, order),
                (room, false),
                (cancel, scratch, batcher),
            )?,
            HeightOrder::Descending => history.append_chunk_frames(
                projection,
                (prefix, order),
                (room, false),
                (cancel, scratch, batcher),
            )?,
        };
        Ok(Some(
            second_span.map_or(first_span, |second| first_span.merge(second)),
        ))
    }

    fn covers_selected_history(&self, chunk: BlockHeightRange) -> bool {
        let Some(history) = &self.history else {
            return false;
        };
        let coverage = history.bounds();
        coverage.start <= chunk.start
            && chunk.end <= coverage.end
            && self.shape.uses_published_range()
    }

    async fn resolve_plan<Reads: CompactBlockReads>(
        &self,
        reads: &Reads,
        (view, epoch, fence, cancel): (&Reads::View, ChainEpoch, Option<Fence>, &CancellationToken),
        (chunk, max_bytes): (&mut BlockHeightRange, usize),
    ) -> AnchorPlan {
        let Some(fence) = fence else {
            if !self.covers_selected_history(*chunk) {
                return AnchorPlan::FallbackNative;
            }
            if reads.block_hash_at(view, chunk.start).await.is_err() {
                return AnchorPlan::FallbackNative;
            }
            let Some(history) = &self.history else {
                return AnchorPlan::FallbackNative;
            };
            return match history.terminal().check_view(reads, view, cancel).await {
                Ok(()) => AnchorPlan::UseEncoded(None),
                Err(status) => AnchorPlan::Failed(status),
            };
        };
        if chunk.start < fence.1.first_available_height() {
            return AnchorPlan::FallbackNative;
        }
        if let Some(history) = &self.history {
            let covered = self.covers_selected_history(*chunk);
            if chunk.end <= history.bounds().end {
                if !covered {
                    return AnchorPlan::FallbackNative;
                }
                return match history.terminal().check_view(reads, view, cancel).await {
                    Ok(()) => AnchorPlan::UseEncoded(None),
                    Err(status) => AnchorPlan::Failed(status),
                };
            }
            match history.terminal().check_view(reads, view, cancel).await {
                Ok(()) => {}
                Err(status) if status.code() == tonic::Code::Cancelled => {
                    return AnchorPlan::Failed(status);
                }
                Err(_) => return AnchorPlan::FallbackNative,
            }
            let catalog_end = (
                history.bounds().end,
                history.terminal().segment.identity().end,
            );
            return self
                .plan_with_tail(
                    reads,
                    (view, epoch, fence, cancel),
                    (chunk, max_bytes),
                    Some(catalog_end),
                )
                .await;
        }
        self.plan_with_tail(
            reads,
            (view, epoch, fence, cancel),
            (chunk, max_bytes),
            None,
        )
        .await
    }

    #[cfg(test)]
    pub(super) async fn plan_can_read_signed_files<Reads: CompactBlockReads>(
        &self,
        reads: &Reads,
        mut chunk: BlockHeightRange,
    ) -> Result<bool, Status> {
        let view = reads.capture_view().await?;
        let plan = self
            .resolve_plan(
                reads,
                (
                    &view,
                    Reads::view_chain_epoch(&view),
                    reads.range_fence(&view),
                    &CancellationToken::new(),
                ),
                (&mut chunk, usize::MAX),
            )
            .await;
        match plan {
            AnchorPlan::UseEncoded(_) => Ok(self.history.as_ref().is_some_and(|catalog| {
                let bounds = catalog.bounds();
                chunk.start <= bounds.end && bounds.start <= chunk.end
            })),
            AnchorPlan::FallbackNative => Ok(false),
            AnchorPlan::Failed(status) => Err(status),
        }
    }

    async fn plan_with_tail<Reads: CompactBlockReads>(
        &self,
        reads: &Reads,
        (view, epoch, fence, cancel): (&Reads::View, ChainEpoch, Fence, &CancellationToken),
        (chunk, max_bytes): (&mut BlockHeightRange, usize),
        catalog_end: Option<(BlockHeight, BlockId)>,
    ) -> AnchorPlan {
        let Some(window_floor) =
            ShieldedTailBuffer::window_floor(epoch, fence.1, catalog_end.map(|(end, _)| end))
        else {
            super::tail::record_lookup(TailLookupOutcome::Bypass);
            return if catalog_end.is_some() && self.covers_selected_history(*chunk) {
                AnchorPlan::UseEncoded(None)
            } else {
                AnchorPlan::FallbackNative
            };
        };
        if chunk.end < window_floor {
            super::tail::record_lookup(TailLookupOutcome::Bypass);
            return if catalog_end.is_some() && self.covers_selected_history(*chunk) {
                AnchorPlan::UseEncoded(None)
            } else {
                AnchorPlan::FallbackNative
            };
        }
        match self
            .tail
            .load_or_refresh_with_outcome(
                (reads, view, cancel),
                catalog_end,
                *chunk,
                TailReadLimits {
                    ascending_bytes: (self.order() == HeightOrder::Ascending).then_some(max_bytes),
                    refresh_wait: self.limits.max_wait(),
                },
            )
            .await
        {
            Ok((Some(live), outcome)) => {
                self.encoded_span(*chunk, self.order(), &live).map_or_else(
                    || {
                        super::tail::record_lookup(TailLookupOutcome::FallbackNative);
                        AnchorPlan::FallbackNative
                    },
                    |span| {
                        super::tail::record_lookup(outcome);
                        *chunk = span;
                        AnchorPlan::UseEncoded(Some(live))
                    },
                )
            }
            Ok((None, outcome)) => {
                let recorded = if outcome == TailLookupOutcome::Bypass {
                    TailLookupOutcome::Bypass
                } else {
                    TailLookupOutcome::FallbackNative
                };
                super::tail::record_lookup(recorded);
                AnchorPlan::FallbackNative
            }
            // Cancellation and an expired pin end this read as the native path
            // would. A tail that fails to build spans heights beyond this
            // chunk, so the native read reports any fault at the chunk that
            // holds it, as a reader without the tail does.
            Err(status) if status.code() == tonic::Code::Cancelled || pin_unavailable(&status) => {
                AnchorPlan::Failed(status)
            }
            Err(status) => {
                super::tail::record_lookup(TailLookupOutcome::FallbackNative);
                tracing::warn!(%status, "serving the uncovered tail from canonical storage");
                AnchorPlan::FallbackNative
            }
        }
    }
}

pub(super) fn pin_unavailable(status: &Status) -> bool {
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

async fn native_batched_chunk<Reads, Output, Convert>(
    reads: &Reads,
    read: ChunkRead<Reads::View>,
    convert: Convert,
) -> ChunkReadOutcome<Output>
where
    Reads: CompactBlockReads,
    Output: Send + 'static,
    Convert: FnOnce(ChainEpoch, PrebatchedChunk) -> Output + Send + 'static,
{
    let is_first_chunk = matches!(read.view, ChunkView::Captured(_));
    reads
        .read_compact_block_chunk(read, move |epoch, blocks| {
            convert(
                epoch,
                PrebatchedChunk::from_canonical(is_first_chunk, blocks),
            )
        })
        .await
}

impl<Reads: CompactBlockReads> RangeSource<Reads> for PublishedRangeRead {
    type Chunk = Vec<ReadBlock>;
    type Message = EncodedBlock;

    fn published_read_limit(
        &self,
        chunk: BlockHeightRange,
        retry: Option<BlockHeightRange>,
    ) -> Option<&crate::block_range::BlockRangeStreamLimit> {
        (self.reads_published_files(chunk)
            || retry.is_some_and(|checked| self.reads_published_files(checked)))
        .then_some(&self.limits)
    }

    fn messages(&self, mut blocks: Vec<ReadBlock>) -> Result<ChunkMessages<EncodedBlock>, Status> {
        if self.is_descending {
            blocks.reverse();
        }
        blocks
            .into_iter()
            .map(|block| match block.payload {
                Payload::Published(message) => Ok(Ok(message)),
                Payload::Canonical(block) => Ok(Ok(self.shape.encoded(block)?)),
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
        if read.cancel.is_cancelled() {
            return cancelled_outcome(None);
        }
        let Some(projection) = self.shape.published_projection() else {
            return native_chunk(reads, read, convert).await;
        };
        let (epoch, started, plan) = {
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
            let fence = reads.range_fence(view);
            let plan = self
                .resolve_plan(
                    reads,
                    (view, epoch, fence, &read.cancel),
                    (&mut read.chunk, read.max_bytes),
                )
                .await;
            (epoch, started, plan)
        };
        let tail = match plan {
            AnchorPlan::UseEncoded(tail) => tail,
            AnchorPlan::FallbackNative => return native_chunk(reads, read, convert).await,
            AnchorPlan::Failed(status) => {
                if matches!(read.view, ChunkView::Current) && pin_unavailable(&status) {
                    return native_chunk(reads, read, convert).await;
                }
                return ChunkReadOutcome {
                    read_duration: Some(started.elapsed()),
                    converted: Err(ChunkReadFailure::Failed(status)),
                };
            }
        };
        drop(read.view);
        self.finish_encoded_read(
            (epoch, started, projection),
            (read.chunk, read.order, read.max_bytes, read.cancel),
            tail,
            convert,
        )
        .await
    }
}

const PACKED_PROJECTION_SLAB_BYTES: usize = 64 * 1024;
const STANDALONE_PROJECTED_FRAME_BYTES: usize = 32 * 1024;
const MIN_PACKED_PROJECTION_CAPACITY: usize = 4 * 1024;

struct ProjectedSlabPacker {
    frame_scratch: Vec<u8>,
    tx_scratch: Vec<u8>,
    slab_bytes: Vec<u8>,
    pending: Vec<(usize, std::ops::Range<usize>)>,
}

impl ProjectedSlabPacker {
    const fn new() -> Self {
        Self {
            frame_scratch: Vec::new(),
            tx_scratch: Vec::new(),
            slab_bytes: Vec::new(),
            pending: Vec::new(),
        }
    }

    fn project_row(
        &mut self,
        rows: &mut [SealedBlock],
        row_index: usize,
        selection: ShieldedProjectionSelection,
    ) -> std::io::Result<()> {
        let payload = rows[row_index].message.payload();
        if zinder_store::is_passthrough_shielded_block_wire(&payload, selection)
            .map_err(projection_io_error)?
        {
            let passthrough = rows[row_index].message.as_length_prefixed().clone();
            if passthrough.len() >= STANDALONE_PROJECTED_FRAME_BYTES {
                return Ok(());
            }
            return self.pack_short_frame(rows, row_index, &passthrough);
        }
        zinder_store::project_shielded_block_wire_with_scratch(
            &payload,
            selection,
            &mut self.frame_scratch,
            &mut self.tx_scratch,
        )
        .map_err(projection_io_error)?;
        let frame_len = self.frame_scratch.len();
        if frame_len >= STANDALONE_PROJECTED_FRAME_BYTES {
            let standalone = prost::bytes::Bytes::copy_from_slice(&self.frame_scratch);
            rows[row_index].message = EncodedBlock::from_length_prefixed_in(standalone, frame_len)
                .map_err(|_| invalid_projected_frame())?;
            return Ok(());
        }
        let staged = std::mem::take(&mut self.frame_scratch);
        let outcome = self.pack_short_frame(rows, row_index, &staged);
        self.frame_scratch = staged;
        outcome
    }

    fn pack_short_frame(
        &mut self,
        rows: &mut [SealedBlock],
        row_index: usize,
        frame: &[u8],
    ) -> std::io::Result<()> {
        let frame_len = frame.len();
        if !self.slab_bytes.is_empty()
            && self.slab_bytes.len() + frame_len > PACKED_PROJECTION_SLAB_BYTES
        {
            self.flush(rows)?;
        }
        let needed = self.slab_bytes.len() + frame_len;
        if needed > self.slab_bytes.capacity() {
            let target = needed
                .max(MIN_PACKED_PROJECTION_CAPACITY)
                .next_power_of_two()
                .min(PACKED_PROJECTION_SLAB_BYTES);
            self.slab_bytes
                .reserve_exact(target - self.slab_bytes.len());
        }
        let start = self.slab_bytes.len();
        self.slab_bytes.extend_from_slice(frame);
        self.pending.push((row_index, start..start + frame_len));
        Ok(())
    }

    fn flush(&mut self, rows: &mut [SealedBlock]) -> std::io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let packed_vec = std::mem::take(&mut self.slab_bytes);
        let allocation_len = packed_vec.capacity();
        let packed = prost::bytes::Bytes::from(packed_vec);
        for (row_index, span) in self.pending.drain(..) {
            rows[row_index].message =
                EncodedBlock::from_length_prefixed_in(packed.slice(span), allocation_len)
                    .map_err(|_| invalid_projected_frame())?;
        }
        Ok(())
    }
}

fn project_sealed_blocks(
    rows: &mut [SealedBlock],
    selection: ShieldedProjectionSelection,
    cancel: Option<&CancellationToken>,
) -> std::io::Result<()> {
    let mut packer = ProjectedSlabPacker::new();
    for row_index in 0..rows.len() {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(std::io::Error::new(
                ErrorKind::Interrupted,
                "block-range stream cancelled",
            ));
        }
        packer.project_row(rows, row_index, selection)?;
    }
    packer.flush(rows)
}

fn projection_io_error(error: zinder_store::SettledShieldedSegmentError) -> std::io::Error {
    std::io::Error::new(ErrorKind::InvalidData, error)
}

fn invalid_projected_frame() -> std::io::Error {
    std::io::Error::new(
        ErrorKind::InvalidData,
        "projected shielded frame is not a valid gRPC message",
    )
}

impl PublishedRangeRead {
    async fn finish_encoded_read<Output, Convert>(
        &self,
        (epoch, started, projection): (ChainEpoch, Instant, PublishedProjection),
        (chunk, order, max_bytes, cancel): (
            BlockHeightRange,
            HeightOrder,
            usize,
            CancellationToken,
        ),
        tail: Option<Arc<EncodedTail>>,
        convert: Convert,
    ) -> ChunkReadOutcome<Output>
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, Vec<ReadBlock>) -> Output + Send + 'static,
    {
        if cancel.is_cancelled() {
            return cancelled_outcome(Some(started.elapsed()));
        }
        let touches_disk = self.reads_published_files(chunk);
        if !touches_disk
            && projection.selection().is_none()
            && let Some(live) = tail.as_deref()
        {
            let rows = Ok(live.select(projection.slab(), chunk, order, max_bytes, true));
            return finish_encoded_rows(epoch, Some(started.elapsed()), rows, &cancel, convert);
        }
        let source = self.clone();
        tokio::task::spawn_blocking(move || {
            if cancel.is_cancelled() {
                return cancelled_outcome(Some(started.elapsed()));
            }
            let rows = source.encoded_rows(
                projection,
                (chunk, order, max_bytes),
                (tail.as_deref(), &cancel),
            );
            finish_encoded_rows(epoch, Some(started.elapsed()), rows, &cancel, convert)
        })
        .await
        .unwrap_or_else(|error| ChunkReadOutcome::unread(task_failure(&error)))
    }

    async fn finish_encoded_frames<Output, Convert>(
        &self,
        (epoch, started, projection): (ChainEpoch, Instant, PublishedProjection),
        (chunk, order, max_bytes, is_first_chunk, cancel): (
            BlockHeightRange,
            HeightOrder,
            usize,
            bool,
            CancellationToken,
        ),
        tail: Option<Arc<EncodedTail>>,
        convert: Convert,
    ) -> ChunkReadOutcome<Output>
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, PrebatchedChunk) -> Output + Send + 'static,
    {
        if cancel.is_cancelled() {
            return cancelled_outcome(Some(started.elapsed()));
        }
        let touches_disk = self.reads_published_files(chunk);
        if !touches_disk
            && projection.selection().is_none()
            && let Some(live) = tail.as_deref()
        {
            let mut batcher = EncodedBlockBatcher::new(is_first_chunk);
            let prebatched = Ok(live
                .append_frames(
                    projection.slab(),
                    (chunk, order),
                    (max_bytes, true),
                    &mut batcher,
                )
                .map_or(PrebatchedChunk::Empty, |chunk_span| {
                    PrebatchedChunk::Encoded {
                        kept_blocks: chunk_span.kept_blocks,
                        endpoints: chunk_span.endpoints(),
                        frames: batcher.finish(),
                    }
                }));
            return finish_prebatched_chunk(
                epoch,
                Some(started.elapsed()),
                prebatched,
                &cancel,
                convert,
            );
        }
        let source = self.clone();
        tokio::task::spawn_blocking(move || {
            if cancel.is_cancelled() {
                return cancelled_outcome(Some(started.elapsed()));
            }
            let frames = source.encoded_frames(
                projection,
                (chunk, order, max_bytes, is_first_chunk),
                (tail.as_deref(), &cancel),
            );
            finish_prebatched_chunk(epoch, Some(started.elapsed()), frames, &cancel, convert)
        })
        .await
        .unwrap_or_else(|error| ChunkReadOutcome::unread(task_failure(&error)))
    }
}

impl<Reads: CompactBlockReads> RangeSource<Reads> for PublishedBatchedRangeRead {
    type Chunk = PrebatchedChunk;
    type Message = EncodedBlockFrame;

    fn published_read_limit(
        &self,
        chunk: BlockHeightRange,
        retry: Option<BlockHeightRange>,
    ) -> Option<&crate::block_range::BlockRangeStreamLimit> {
        <PublishedRangeRead as RangeSource<Reads>>::published_read_limit(&self.0, chunk, retry)
    }

    fn messages(&self, chunk: PrebatchedChunk) -> Result<ChunkMessages<EncodedBlockFrame>, Status> {
        match chunk {
            PrebatchedChunk::Empty => Ok(Vec::new()),
            PrebatchedChunk::Encoded { frames, .. } => Ok(frames.into_iter().map(Ok).collect()),
            PrebatchedChunk::Canonical {
                is_first_chunk,
                mut blocks,
                ..
            } => {
                if self.0.is_descending {
                    blocks.reverse();
                }
                let mut batcher = EncodedBlockBatcher::new(is_first_chunk);
                for block in blocks {
                    batcher.push_block(self.0.shape.encoded(block)?);
                }
                Ok(batcher.finish().into_iter().map(Ok).collect())
            }
        }
    }

    async fn read<Output, Convert>(
        &self,
        reads: &Reads,
        mut read: ChunkRead<Reads::View>,
        convert: Convert,
    ) -> ChunkReadOutcome<Output>
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, PrebatchedChunk) -> Output + Send + 'static,
    {
        if read.cancel.is_cancelled() {
            return cancelled_outcome(None);
        }
        let Some(projection) = self.0.shape.published_projection() else {
            return native_batched_chunk(reads, read, convert).await;
        };
        let is_first_chunk = matches!(read.view, ChunkView::Captured(_));
        let (epoch, started, plan) = {
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
            let fence = reads.range_fence(view);
            let plan = self
                .0
                .resolve_plan(
                    reads,
                    (view, epoch, fence, &read.cancel),
                    (&mut read.chunk, read.max_bytes),
                )
                .await;
            (epoch, started, plan)
        };
        let tail = match plan {
            AnchorPlan::UseEncoded(tail) => tail,
            AnchorPlan::FallbackNative => return native_batched_chunk(reads, read, convert).await,
            AnchorPlan::Failed(status) => {
                if matches!(read.view, ChunkView::Current) && pin_unavailable(&status) {
                    return native_batched_chunk(reads, read, convert).await;
                }
                return ChunkReadOutcome {
                    read_duration: Some(started.elapsed()),
                    converted: Err(ChunkReadFailure::Failed(status)),
                };
            }
        };
        drop(read.view);
        self.0
            .finish_encoded_frames(
                (epoch, started, projection),
                (
                    read.chunk,
                    read.order,
                    read.max_bytes,
                    is_first_chunk,
                    read.cancel,
                ),
                tail,
                convert,
            )
            .await
    }
}

fn finish_prebatched_chunk<Output, Convert>(
    epoch: ChainEpoch,
    read_duration: Option<std::time::Duration>,
    chunk: std::io::Result<PrebatchedChunk>,
    cancel: &CancellationToken,
    convert: Convert,
) -> ChunkReadOutcome<Output>
where
    Convert: FnOnce(ChainEpoch, PrebatchedChunk) -> Output,
{
    if cancel.is_cancelled() {
        return cancelled_outcome(read_duration);
    }
    let converted = chunk.map(|chunk| convert(epoch, chunk)).map_err(|error| {
        let status = if error.kind() == ErrorKind::Interrupted {
            Status::cancelled(error.to_string())
        } else {
            Status::data_loss(error.to_string())
        };
        ChunkReadFailure::Failed(status)
    });
    ChunkReadOutcome {
        read_duration,
        converted,
    }
}

fn finish_encoded_rows<Output, Convert>(
    epoch: ChainEpoch,
    read_duration: Option<std::time::Duration>,
    rows: std::io::Result<Vec<SealedBlock>>,
    cancel: &CancellationToken,
    convert: Convert,
) -> ChunkReadOutcome<Output>
where
    Convert: FnOnce(ChainEpoch, Vec<ReadBlock>) -> Output,
{
    if cancel.is_cancelled() {
        return cancelled_outcome(read_duration);
    }
    let converted = rows
        .map(|rows| convert(epoch, rows.into_iter().map(Into::into).collect()))
        .map_err(|error| {
            let status = if error.kind() == ErrorKind::Interrupted {
                Status::cancelled(error.to_string())
            } else {
                Status::data_loss(error.to_string())
            };
            ChunkReadFailure::Failed(status)
        });
    ChunkReadOutcome {
        read_duration,
        converted,
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use eyre::{Result, eyre};
    use prost::Message as _;
    use tonic::Code;
    use zinder_core::{
        ArtifactSchemaVersion, ChainEpochId, ChainTipMetadata, CompactChainMetadata,
        CompactSaplingOutput, CompactShieldedAction, CompactTransaction, CompactTransactionData,
        Network, TransactionId, UnixTimestampMillis,
    };
    use zinder_proto::compat::lightwalletd::{BoundedChunkRetention, CompactBlock, PoolType};

    use super::*;
    use crate::sealed_history::SealedSlab;

    fn sample_orchard_action(seed: u8) -> CompactShieldedAction {
        CompactShieldedAction {
            nullifier: [seed; 32],
            commitment: [seed.wrapping_add(1); 32],
            ephemeral_key: [seed.wrapping_add(2); 32],
            ciphertext: [seed.wrapping_add(3); 52],
        }
    }

    fn sample_sapling_output(seed: u8) -> CompactSaplingOutput {
        CompactSaplingOutput {
            commitment: [seed; 32],
            ephemeral_key: [seed.wrapping_add(1); 32],
            ciphertext: [seed.wrapping_add(2); 52],
        }
    }

    fn sample_artifact(
        height_num: u32,
        orchard_count: usize,
        with_sapling: bool,
    ) -> Result<CompactBlockArtifact> {
        let txs = if orchard_count == 0 && !with_sapling {
            Vec::new()
        } else {
            let mut raw_id = [0x42; 32];
            raw_id[..4].copy_from_slice(&height_num.to_le_bytes());
            let actions = (0..orchard_count)
                .map(|idx| sample_orchard_action(u8::try_from(idx % 251).unwrap_or(0)))
                .collect();
            let sapling_outputs = if with_sapling {
                vec![sample_sapling_output(0x7a)]
            } else {
                Vec::new()
            };
            vec![CompactTransaction {
                index: 1,
                transaction_id: TransactionId::from_bytes(raw_id),
                data: CompactTransactionData {
                    fee_zat: Some(10_000),
                    sapling_outputs,
                    orchard_actions: actions,
                    ..Default::default()
                },
            }]
        };
        let hash_byte = u8::try_from(height_num).unwrap_or(1);
        Ok(CompactBlockArtifact::new(
            BlockId::new(
                BlockHeight::new(height_num),
                BlockHash::from_bytes([hash_byte; 32]),
            ),
            BlockHash::from_bytes([hash_byte.saturating_sub(1); 32]),
            1_700_000_000 + height_num,
            txs,
            CompactChainMetadata {
                sapling_commitment_tree_size: u32::from(with_sapling),
                orchard_commitment_tree_size: u32::try_from(orchard_count)?,
                ironwood_commitment_tree_size: 0,
            },
        )?)
    }

    fn sealed_shielded_row(block: &CompactBlockArtifact) -> Result<SealedBlock> {
        let message = CompactBlockShape::DEFAULT_RANGE
            .encoded(block.clone())
            .map_err(|status| eyre!(status.to_string()))?;
        Ok(SealedBlock {
            id: BlockId::new(block.height(), block.block_hash()),
            parent: block.previous_block_hash(),
            native_bytes: message.payload_len(),
            message,
        })
    }

    fn assert_projected_parity(
        rows: &[SealedBlock],
        artifacts: &[CompactBlockArtifact],
        orchard_shape: CompactBlockShape,
    ) -> Result<()> {
        for (row, artifact) in rows.iter().zip(artifacts) {
            let expected = orchard_shape
                .encoded(artifact.clone())
                .map_err(|status| eyre!(status.to_string()))?;
            let frame = row.message.as_length_prefixed();
            assert_eq!(frame, expected.as_length_prefixed());
            assert_eq!(frame[0], 0);
            let prefix_len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
            assert_eq!(prefix_len, row.message.payload_len());
            assert_eq!(
                CompactBlock::decode(row.message.payload())?,
                CompactBlock::decode(expected.payload())?
            );
        }
        Ok(())
    }

    fn wrap_in_large_raw_slab(row: &mut SealedBlock) -> Result<prost::bytes::Bytes> {
        let mut backing = vec![0_u8; BoundedChunkRetention::MAX_ALLOCATION_BYTES];
        let framed = row.message.as_length_prefixed();
        backing[..framed.len()].copy_from_slice(framed);
        let alloc = backing.capacity();
        let slab = prost::bytes::Bytes::from(backing);
        row.message = EncodedBlock::from_length_prefixed_in(slab.slice(..framed.len()), alloc)
            .map_err(|_| eyre!("invalid test frame"))?;
        Ok(slab)
    }

    #[test]
    fn slab_packer_handles_32k_standalone_64k_rollover_and_passthrough_rows() -> Result<()> {
        let orchard_shape = CompactBlockShape::for_range(&[PoolType::Orchard as i32])
            .map_err(|status| eyre!(status.to_string()))?;
        let projection = orchard_shape
            .published_projection()
            .ok_or_else(|| eyre!("expected orchard published projection"))?;
        assert_eq!(projection.slab(), SealedSlab::Shielded);
        let selection = projection
            .selection()
            .ok_or_else(|| eyre!("expected wire selection"))?;

        let artifacts = [
            sample_artifact(1, 205, true)?,
            sample_artifact(2, 0, false)?,
            sample_artifact(3, 205, true)?,
            sample_artifact(4, 206, true)?,
            sample_artifact(5, 0, false)?,
            sample_artifact(6, 20, true)?,
        ];
        let mut step_rows: Vec<SealedBlock> = artifacts
            .iter()
            .map(sealed_shielded_row)
            .collect::<Result<_>>()?;
        let large_passthrough_msg = EncodedBlock::encode(&CompactBlock {
            height: 7,
            header: vec![0xaa; 33_000],
            ..CompactBlock::default()
        })?;
        let large_passthrough_ptr = large_passthrough_msg.as_length_prefixed().as_ptr();
        step_rows.push(SealedBlock {
            id: BlockId::new(BlockHeight::new(7), BlockHash::from_bytes([7; 32])),
            parent: BlockHash::from_bytes([6; 32]),
            native_bytes: large_passthrough_msg.payload_len(),
            message: large_passthrough_msg,
        });
        let raw_512k_slab = wrap_in_large_raw_slab(&mut step_rows[1])?;
        assert!(!raw_512k_slab.is_unique());
        let mut batch_rows = step_rows.clone();

        let mut packer = ProjectedSlabPacker::new();
        packer.project_row(&mut step_rows, 0, selection)?;
        let len0 = packer.slab_bytes.len();
        assert!((32_500..STANDALONE_PROJECTED_FRAME_BYTES).contains(&len0));
        assert_eq!(
            packer.slab_bytes.capacity(),
            STANDALONE_PROJECTED_FRAME_BYTES
        );
        packer.project_row(&mut step_rows, 1, selection)?;
        let len1 = step_rows[1].message.as_length_prefixed().len();
        assert_eq!(packer.pending.len(), 2);
        packer.project_row(&mut step_rows, 2, selection)?;
        assert_eq!(packer.slab_bytes.len(), len0 * 2 + len1);
        let slab1_alloc = packer.slab_bytes.capacity();
        assert_eq!(slab1_alloc, PACKED_PROJECTION_SLAB_BYTES);

        packer.project_row(&mut step_rows, 3, selection)?;
        assert!(
            step_rows[3].message.as_length_prefixed().len() >= STANDALONE_PROJECTED_FRAME_BYTES
        );
        assert_eq!(packer.pending.len(), 3);
        packer.project_row(&mut step_rows, 4, selection)?;
        assert_eq!(packer.pending.len(), 1);
        assert_eq!(packer.slab_bytes.capacity(), MIN_PACKED_PROJECTION_CAPACITY);
        packer.project_row(&mut step_rows, 5, selection)?;
        assert_eq!(packer.pending.len(), 2);
        packer.project_row(&mut step_rows, 6, selection)?;
        assert_eq!(packer.pending.len(), 2);
        packer.flush(&mut step_rows)?;

        project_sealed_blocks(&mut batch_rows, selection, None)?;
        assert!(
            raw_512k_slab.is_unique(),
            "repacking short passthrough rows must release the 512 KiB raw slab"
        );
        assert_projected_parity(&step_rows[..6], &artifacts, orchard_shape)?;
        assert_projected_parity(&batch_rows[..6], &artifacts, orchard_shape)?;
        assert_slab_layout(&step_rows, (len0, len1, slab1_alloc), large_passthrough_ptr);
        Ok(())
    }

    fn assert_slab_layout(
        step_rows: &[SealedBlock],
        (len0, len1, slab1_alloc): (usize, usize, usize),
        large_passthrough_ptr: *const u8,
    ) {
        let ptr0 = step_rows[0].message.as_length_prefixed().as_ptr();
        let ptr1 = step_rows[1].message.as_length_prefixed().as_ptr();
        let ptr2 = step_rows[2].message.as_length_prefixed().as_ptr();
        let ptr3 = step_rows[3].message.as_length_prefixed().as_ptr();
        let ptr4 = step_rows[4].message.as_length_prefixed().as_ptr();
        let ptr6 = step_rows[6].message.as_length_prefixed().as_ptr();
        assert_eq!(ptr0.wrapping_add(len0), ptr1);
        assert_eq!(ptr1.wrapping_add(len1), ptr2);
        assert!(std::ptr::eq(ptr6, large_passthrough_ptr));
        let slab1_span = (ptr0 as usize)..(ptr0.wrapping_add(slab1_alloc) as usize);
        assert!(!slab1_span.contains(&(ptr3 as usize)));
        assert!(!slab1_span.contains(&(ptr4 as usize)));
    }

    #[test]
    fn cancellation_after_catalog_read_skips_conversion() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let converted_called = AtomicBool::new(false);
        let epoch = ChainEpoch {
            id: ChainEpochId::new(1),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(1),
            visible_tip_hash: BlockHash::from_bytes([1; 32]),
            settled_tip_height: BlockHeight::new(1),
            settled_tip_hash: BlockHash::from_bytes([1; 32]),
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            created_at: UnixTimestampMillis::new(0),
        };
        let outcome = finish_encoded_rows(epoch, None, Ok(Vec::new()), &cancel, |_, _| {
            converted_called.store(true, Ordering::Relaxed);
        });
        assert!(!converted_called.load(Ordering::Relaxed));
        assert!(matches!(
            outcome.converted,
            Err(ChunkReadFailure::Failed(status)) if status.code() == Code::Cancelled
        ));
    }
}
