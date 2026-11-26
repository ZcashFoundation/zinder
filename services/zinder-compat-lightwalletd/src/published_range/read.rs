use std::{io::ErrorKind, sync::Arc, time::Instant};

use tokio_util::sync::CancellationToken;
use tonic::Status;
use tonic_types::StatusExt as _;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpoch, CompactBlockArtifact,
};
use zinder_proto::{ZINDER_ERROR_DOMAIN, compat::lightwalletd::EncodedBlock, v1::ops::ErrorReason};
use zinder_store::{HeightOrder, ShieldedProjectionSelection};

use super::{
    PublishedShieldedCatalog,
    tail::{EncodedTail, Fence, ShieldedTailBuffer, TailReadLimits},
};
use crate::{
    block_methods::{
        ChunkMessages, ChunkRead, ChunkReadFailure, ChunkReadOutcome, ChunkView, CompactBlockReads,
        RangeSource, view_reaches,
    },
    block_range::RangeBlock,
    compact_block_messages::{CompactBlockShape, PublishedProjection},
    sealed_history::SealedBlock,
};

#[derive(Clone)]
pub(crate) struct PublishedRangeRead {
    history: Option<Arc<PublishedShieldedCatalog>>,
    tail: ShieldedTailBuffer,
    limits: crate::block_range::BlockRangeStreamLimit,
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
            return if catalog_end.is_some() && self.covers_selected_history(*chunk) {
                AnchorPlan::UseEncoded(None)
            } else {
                AnchorPlan::FallbackNative
            };
        };
        if chunk.end < window_floor {
            return if catalog_end.is_some() && self.covers_selected_history(*chunk) {
                AnchorPlan::UseEncoded(None)
            } else {
                AnchorPlan::FallbackNative
            };
        }
        match self
            .tail
            .load_or_refresh(
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
            Ok(Some(live)) => self.encoded_span(*chunk, self.order(), &live).map_or_else(
                || AnchorPlan::FallbackNative,
                |span| {
                    *chunk = span;
                    AnchorPlan::UseEncoded(Some(live))
                },
            ),
            Ok(None) => AnchorPlan::FallbackNative,
            // Cancellation and an expired pin end this read as the native path
            // would. A tail that fails to build spans heights beyond this
            // chunk, so the native read reports any fault at the chunk that
            // holds it, as a reader without the tail does.
            Err(status) if status.code() == tonic::Code::Cancelled || pin_unavailable(&status) => {
                AnchorPlan::Failed(status)
            }
            Err(status) => {
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

impl<Reads: CompactBlockReads> RangeSource<Reads> for PublishedRangeRead {
    type Block = ReadBlock;
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
            return Ok(());
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
        if !self.slab_bytes.is_empty()
            && self.slab_bytes.len() + frame_len > PACKED_PROJECTION_SLAB_BYTES
        {
            self.flush(rows)?;
        }
        if self.slab_bytes.capacity() == 0 {
            let initial = frame_len
                .max(MIN_PACKED_PROJECTION_CAPACITY)
                .next_power_of_two()
                .min(PACKED_PROJECTION_SLAB_BYTES);
            self.slab_bytes = Vec::with_capacity(initial);
        }
        let start = self.slab_bytes.len();
        self.slab_bytes.extend_from_slice(&self.frame_scratch);
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

pub(super) fn project_sealed_blocks(
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
        if !touches_disk && let Some(live) = tail.as_deref() {
            let rows =
                live.select_projected(projection, (chunk, order, max_bytes), (true, Some(&cancel)));
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

    use tonic::Code;
    use zinder_core::{
        ArtifactSchemaVersion, ChainEpochId, ChainTipMetadata, Network, UnixTimestampMillis,
    };

    use super::*;

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
