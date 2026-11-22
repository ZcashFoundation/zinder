use std::{collections::VecDeque, sync::Arc};

use arc_swap::ArcSwapOption;
use prost::Message as _;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CanonicalHistoryBounds, ChainEpoch,
    CompactBlockArtifact,
};
use zinder_query::{CanonicalReader, CompactBlockSource};
use zinder_store::{CanonicalEventFence, HeightOrder};

use super::PublishedShieldedCatalog;
use crate::{
    block_methods::CompactBlockReads, compact_block_messages::CompactBlockShape,
    sealed_history::SealedBlock,
};

const TAIL_BLOCKS: u32 = 5_096;
const TAIL_BYTES: usize = 64 * 1024 * 1024;
const ENCODE_BATCH_BLOCKS: u32 = 1_000;

pub(super) type Fence = (CanonicalEventFence, CanonicalHistoryBounds);

#[derive(Clone, Default)]
pub(crate) struct ShieldedTailBuffer(Arc<ShieldedTailBufferInner>);

#[derive(Default)]
struct ShieldedTailBufferInner {
    current: ArcSwapOption<EncodedTail>,
    refresh_lock: tokio::sync::Mutex<()>,
}

pub(super) struct EncodedTail {
    pub(super) epoch: ChainEpoch,
    pub(super) fence: Fence,
    /// The admitted data the rows were read or verified against. A tail
    /// prepared without one only seeds a later verified rebuild.
    source: Option<CompactBlockSource>,
    rows: VecDeque<SealedBlock>,
    bytes: usize,
    byte_capped: bool,
    /// The most payload bytes the build of this tail held at once, counting
    /// the rows it keeps and the storage chunk it was converting.
    build_peak_bytes: usize,
}

trait TailSource: Sync {
    fn epoch_and_fence(&self) -> (ChainEpoch, Fence);

    fn source(&self) -> Option<&CompactBlockSource>;

    fn cancel(&self) -> &CancellationToken;

    /// Returns whether this source still reads the view current now.
    fn is_current(&self) -> impl Future<Output = bool> + Send;

    fn block_hash_at(
        &self,
        height: BlockHeight,
    ) -> impl Future<Output = Result<Option<BlockHash>, Status>> + Send;

    fn read_rows(
        &self,
        range: BlockHeightRange,
        max_bytes: usize,
    ) -> impl Future<Output = Result<Option<Vec<(CompactBlockArtifact, usize)>>, Status>> + Send;
}

struct CanonicalTailSource<'a> {
    reader: &'a dyn CanonicalReader,
    cancel: CancellationToken,
}

impl TailSource for CanonicalTailSource<'_> {
    fn epoch_and_fence(&self) -> (ChainEpoch, Fence) {
        (
            self.reader.chain_epoch(),
            (self.reader.event_fence(), self.reader.history_bounds()),
        )
    }

    fn source(&self) -> Option<&CompactBlockSource> {
        None
    }

    fn cancel(&self) -> &CancellationToken {
        &self.cancel
    }

    fn is_current(&self) -> impl Future<Output = bool> + Send {
        std::future::ready(true)
    }

    async fn block_hash_at(&self, height: BlockHeight) -> Result<Option<BlockHash>, Status> {
        self.reader
            .block_header_at(height)
            .await
            .map(|row| row.map(|header| header.block_hash))
            .map_err(|error| Status::unavailable(error.to_string()))
    }

    async fn read_rows(
        &self,
        range: BlockHeightRange,
        max_bytes: usize,
    ) -> Result<Option<Vec<(CompactBlockArtifact, usize)>>, Status> {
        self.reader
            .compact_block_rows_with_lengths(range, max_bytes)
            .await
            .map(Some)
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}

struct ReadsTailSource<'a, Reads: CompactBlockReads> {
    reads: &'a Reads,
    view: &'a Reads::View,
    fence: Fence,
    source: CompactBlockSource,
    cancel: &'a CancellationToken,
}

impl<Reads: CompactBlockReads> ReadsTailSource<'_, Reads> {
    /// Returns whether `view` still reads this source's fence and data.
    fn reads_same_source(&self, view: &Reads::View) -> bool {
        self.reads
            .tail_source(view)
            .is_some_and(|(fence, source)| fence == self.fence && source.is(&self.source))
    }
}

impl<Reads: CompactBlockReads> TailSource for ReadsTailSource<'_, Reads> {
    fn epoch_and_fence(&self) -> (ChainEpoch, Fence) {
        (Reads::view_chain_epoch(self.view), self.fence)
    }

    fn source(&self) -> Option<&CompactBlockSource> {
        Some(&self.source)
    }

    fn cancel(&self) -> &CancellationToken {
        self.cancel
    }

    async fn is_current(&self) -> bool {
        self.reads.capture_view().await.is_ok_and(|current| {
            Reads::view_chain_epoch(&current) == Reads::view_chain_epoch(self.view)
                && self.reads_same_source(&current)
        })
    }

    async fn block_hash_at(&self, height: BlockHeight) -> Result<Option<BlockHash>, Status> {
        match self.reads.block_hash_at(self.view, height).await {
            Ok(hash) => Ok(Some(hash)),
            Err(status) if super::read::pin_unavailable(&status) => Err(status),
            Err(status)
                if matches!(
                    status.code(),
                    Code::NotFound
                        | Code::OutOfRange
                        | Code::InvalidArgument
                        | Code::FailedPrecondition
                ) =>
            {
                Ok(None)
            }
            Err(status) => Err(status),
        }
    }

    async fn read_rows(
        &self,
        range: BlockHeightRange,
        max_bytes: usize,
    ) -> Result<Option<Vec<(CompactBlockArtifact, usize)>>, Status> {
        let rows = self
            .reads
            .read_compact_block_rows_with_lengths(self.view, range, max_bytes)
            .await?;
        // Epoch pins match by identifier, and a republished pair keeps it.
        // Cached rows must come from the data that still answers this view.
        if rows.is_some() && !self.reads_same_source(self.view) {
            return Err(Status::aborted(
                "the canonical view changed during the live-tail read",
            ));
        }
        Ok(rows)
    }
}

fn cancelled() -> Status {
    Status::cancelled("block-range stream cancelled")
}

fn check_cancel(source: &impl TailSource) -> Result<(), Status> {
    if source.cancel().is_cancelled() {
        return Err(cancelled());
    }
    Ok(())
}

fn payload_bytes<'a>(rows: impl IntoIterator<Item = &'a SealedBlock>) -> usize {
    rows.into_iter().map(|row| row.payload.len()).sum()
}

impl ShieldedTailBuffer {
    pub(super) fn load_full(&self) -> Option<Arc<EncodedTail>> {
        self.0.current.load_full()
    }

    pub(super) fn after_published(&self, end: BlockHeight) -> Self {
        let trimmed = self.load_full().map(|tail| tail.after_published(end));
        Self(Arc::new(ShieldedTailBufferInner {
            current: ArcSwapOption::from(trimmed),
            refresh_lock: tokio::sync::Mutex::new(()),
        }))
    }

    pub(super) fn window_floor(
        epoch: ChainEpoch,
        bounds: CanonicalHistoryBounds,
        catalog_end: Option<BlockHeight>,
    ) -> Option<BlockHeight> {
        let min_floor = BlockHeight::new(
            epoch
                .visible_tip_height
                .value()
                .saturating_sub(TAIL_BLOCKS - 1),
        )
        .max(bounds.first_available_height());
        let floor = match catalog_end {
            Some(end) => min_floor.max(end.next()?),
            None => min_floor,
        };
        (floor <= epoch.visible_tip_height).then_some(floor)
    }

    pub(super) async fn load_or_refresh<Reads: CompactBlockReads>(
        &self,
        (reads, view, cancel): (&Reads, &Reads::View, &CancellationToken),
        catalog: Option<(BlockHeight, BlockId)>,
        requested: BlockHeightRange,
    ) -> Result<Option<Arc<EncodedTail>>, Status> {
        let Some((fence, source)) = reads.tail_source(view) else {
            return Ok(None);
        };
        let source = ReadsTailSource {
            reads,
            view,
            fence,
            source,
            cancel,
        };
        self.refresh_with_limit(&source, catalog, (Some(requested), TAIL_BYTES))
            .await
    }

    async fn refresh_with_limit<Source: TailSource>(
        &self,
        source: &Source,
        catalog: Option<(BlockHeight, BlockId)>,
        (requested, max_tail_bytes): (Option<BlockHeightRange>, usize),
    ) -> Result<Option<Arc<EncodedTail>>, Status> {
        let cancel = source.cancel();
        check_cancel(source)?;
        let (epoch, fence) = source.epoch_and_fence();
        let Some(window_floor) = Self::window_floor(epoch, fence.1, catalog.map(|(end, _)| end))
        else {
            return Ok(None);
        };
        let requested_floor = match requested {
            Some(chunk) if chunk.end < window_floor => return Ok(None),
            Some(chunk) => chunk.start.max(window_floor),
            None => window_floor,
        };
        let existing = self.load_full();
        if let Some(existing) = existing
            .as_ref()
            .filter(|tail| tail.satisfies(source, requested_floor))
        {
            return Ok(Some(existing.clone()));
        }
        // A stale view must not spend the refresh lock rebuilding rows it may
        // not install.
        if existing.is_some() && !source.is_current().await {
            return Ok(None);
        }
        check_cancel(source)?;
        let _guard = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(cancelled()),
            guard = self.0.refresh_lock.lock() => guard,
        };
        check_cancel(source)?;
        let previous = self.load_full();
        if let Some(existing) = &previous
            && existing.satisfies(source, requested_floor)
        {
            return Ok(Some(existing.clone()));
        }
        let updated = match &previous {
            Some(existing) if existing.serves(source) => {
                prepend_missing(source, existing, requested_floor, max_tail_bytes).await?
            }
            _ => {
                build_or_extend(
                    source,
                    previous.as_deref(),
                    (window_floor, requested_floor, max_tail_bytes),
                    catalog,
                )
                .await?
            }
        };
        let Some(next_tail) = updated else {
            return Ok(None);
        };
        // Rows built for a view that stopped being current during the build
        // serve this request only.
        if source.is_current().await {
            self.0
                .current
                .compare_and_swap(&previous, Some(next_tail.clone()));
        }
        Ok(Some(next_tail))
    }
}

impl EncodedTail {
    pub(super) fn after_published(self: Arc<Self>, end: BlockHeight) -> Arc<Self> {
        let covered = self.rows.partition_point(|row| row.id.height <= end);
        if covered == 0 {
            return self;
        }
        let rows: VecDeque<_> = self.rows.range(covered..).cloned().collect();
        let bytes = payload_bytes(&rows);
        Arc::new(Self {
            epoch: self.epoch,
            fence: self.fence,
            source: self.source.clone(),
            bytes,
            byte_capped: false,
            rows,
            build_peak_bytes: bytes,
        })
    }

    pub(super) fn bounds(&self) -> Option<BlockHeightRange> {
        Some(BlockHeightRange::inclusive(
            self.rows.front()?.id.height,
            self.rows.back()?.id.height,
        ))
    }

    /// Returns whether these rows were read from `source`'s view and data.
    /// A source without data identity only matches to skip preparing again.
    fn serves(&self, source: &impl TailSource) -> bool {
        let (epoch, fence) = source.epoch_and_fence();
        self.epoch == epoch
            && self.fence == fence
            && source
                .source()
                .is_none_or(|source| self.source.as_ref().is_some_and(|own| own.is(source)))
    }

    fn satisfies(&self, source: &impl TailSource, requested_floor: BlockHeight) -> bool {
        self.serves(source)
            && (self.byte_capped
                || source.source().is_none()
                || self
                    .bounds()
                    .is_some_and(|bounds| bounds.start <= requested_floor))
    }

    fn with_rows(&self, rows: VecDeque<SealedBlock>, byte_capped: bool, peak: usize) -> Self {
        Self {
            epoch: self.epoch,
            fence: self.fence,
            source: self.source.clone(),
            bytes: payload_bytes(&rows),
            byte_capped,
            rows,
            build_peak_bytes: peak,
        }
    }

    pub(super) fn select(
        &self,
        span: BlockHeightRange,
        order: HeightOrder,
        budget: usize,
        allow_first: bool,
    ) -> Vec<SealedBlock> {
        let Some(bounds) = self.bounds() else {
            return Vec::new();
        };
        let mut rows = self.rows.range(
            (span.start.value() - bounds.start.value()) as usize
                ..=(span.end.value() - bounds.start.value()) as usize,
        );
        let mut selected = Vec::new();
        let mut room = budget;
        while let Some(row) = match order {
            HeightOrder::Ascending => rows.next(),
            HeightOrder::Descending => rows.next_back(),
        } {
            if row.native_bytes > room && !(allow_first && selected.is_empty()) {
                break;
            }
            selected.push(row.clone());
            room = room.saturating_sub(row.native_bytes);
        }
        if order == HeightOrder::Descending {
            selected.reverse();
        }
        selected
    }

    /// Returns the rows below `source`'s tip that its view still holds with
    /// the same bytes.
    ///
    /// Block hashes link the rows, so a matching hash keeps every row below
    /// it. Tree sizes come from the store's seed checkpoint rather than the
    /// block, so the highest kept row must also read back byte for byte;
    /// later rows only add their own commitments to it.
    async fn verified_prefix<Source: TailSource>(
        &self,
        source: &Source,
        first: BlockHeight,
    ) -> Result<VecDeque<SealedBlock>, Status> {
        let (epoch, fence) = source.epoch_and_fence();
        if epoch.network != self.epoch.network
            || epoch.artifact_schema_version != self.epoch.artifact_schema_version
            || (self.fence.0.visible_tip() == fence.0.visible_tip()
                && self.fence.0.sequence_digest() != fence.0.sequence_digest())
        {
            return Ok(VecDeque::new());
        }
        let left = self.rows.partition_point(|row| row.id.height < first);
        let mut hi = self
            .rows
            .partition_point(|row| row.id.height <= epoch.visible_tip_height);
        let mut lo = left.min(hi);
        while lo < hi {
            check_cancel(source)?;
            let mid = if hi == self.rows.len() && lo == left {
                hi - 1
            } else {
                lo + (hi - lo) / 2
            };
            let id = self.rows[mid].id;
            if source
                .block_hash_at(id.height)
                .await?
                .is_some_and(|hash| hash == id.hash)
            {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let kept: VecDeque<_> = self.rows.range(left.min(lo)..lo).cloned().collect();
        let Some(top) = kept.back() else {
            return Ok(kept);
        };
        check_cancel(source)?;
        let reread = encode_range(
            source,
            BlockHeightRange::inclusive(top.id.height, top.id.height),
        )
        .await?;
        let identical = reread
            .as_deref()
            .and_then(<[SealedBlock]>::first)
            .is_some_and(|row| {
                row.id == top.id
                    && row.parent == top.parent
                    && row.native_bytes == top.native_bytes
                    && row.payload == top.payload
            });
        Ok(if identical { kept } else { VecDeque::new() })
    }

    fn push_with_limit(&mut self, row: SealedBlock, max_tail_bytes: usize) {
        self.bytes += row.payload.len();
        self.rows.push_back(row);
        while self.rows.len() > 1 && self.bytes > max_tail_bytes {
            if let Some(evicted) = self.rows.pop_front() {
                self.bytes -= evicted.payload.len();
                self.byte_capped = true;
            }
        }
        if self.bytes > max_tail_bytes {
            self.byte_capped = true;
        }
    }
}

async fn build_or_extend<Source: TailSource>(
    source: &Source,
    previous: Option<&EncodedTail>,
    (window_floor, requested_floor, max_tail_bytes): (BlockHeight, BlockHeight, usize),
    catalog: Option<(BlockHeight, BlockId)>,
) -> Result<Option<Arc<EncodedTail>>, Status> {
    let (epoch, fence) = source.epoch_and_fence();
    let retained = match previous {
        Some(prior) => prior.verified_prefix(source, window_floor).await?,
        None => VecDeque::new(),
    };
    // A retained capped front keeps the cap, so a floor below it does not
    // re-encode a batch that cannot fit.
    let byte_capped = previous.is_some_and(|prior| {
        prior.byte_capped
            && retained.front().map(|row| row.id) == prior.rows.front().map(|row| row.id)
    });
    let mut start_floor = requested_floor;
    if let Some((catalog_end, _)) = catalog
        && previous.is_none()
        && requested_floor == window_floor
        && let Some(after_catalog) = catalog_end.next()
    {
        start_floor = start_floor.max(after_catalog);
    }
    let next = retained
        .back()
        .map_or(Some(start_floor), |row| row.id.height.next());
    let mut prior = retained.back().map(|row| row.id).or_else(|| {
        catalog.and_then(|(end, id)| (Some(end.next()?) == Some(start_floor)).then_some(id))
    });
    let bytes = payload_bytes(&retained);
    let mut tail = EncodedTail {
        epoch,
        fence,
        source: source.source().cloned(),
        bytes,
        byte_capped,
        rows: retained,
        build_peak_bytes: bytes,
    };
    if let Some(first) = next
        && first <= epoch.visible_tip_height
    {
        let span = BlockHeightRange::inclusive(first, epoch.visible_tip_height);
        let mut peak = tail.build_peak_bytes;
        let mut keep = |chunk: Vec<SealedBlock>| {
            peak = peak.max(tail.bytes + payload_bytes(&chunk));
            for row in chunk {
                tail.push_with_limit(row, max_tail_bytes);
            }
        };
        match encode_span(source, span, prior, &mut keep).await? {
            SpanEnd::Unsupported => return Ok(None),
            SpanEnd::Through(last) => prior = last.or(prior),
        }
        tail.build_peak_bytes = peak;
    }
    if prior.is_some_and(|id| {
        id.height == epoch.visible_tip_height && id.hash != epoch.visible_tip_hash
    }) {
        return Err(Status::data_loss(
            "encoded tail differs from the visible tip",
        ));
    }
    if !tail.byte_capped
        && tail
            .bounds()
            .is_some_and(|bounds| bounds.start > requested_floor)
    {
        return prepend_missing(source, &tail, requested_floor, max_tail_bytes).await;
    }
    Ok(Some(Arc::new(tail)))
}

async fn prepend_missing<Source: TailSource>(
    source: &Source,
    existing: &EncodedTail,
    target_floor: BlockHeight,
    max_tail_bytes: usize,
) -> Result<Option<Arc<EncodedTail>>, Status> {
    let unchanged = || {
        Ok(Some(Arc::new(existing.with_rows(
            existing.rows.clone(),
            existing.byte_capped,
            existing.build_peak_bytes,
        ))))
    };
    let Some(bounds) = existing.bounds() else {
        return unchanged();
    };
    if bounds.start <= target_floor || existing.byte_capped {
        return unchanged();
    }
    let Some(end_val) = bounds.start.value().checked_sub(1) else {
        return unchanged();
    };
    read_prepend_batches(source, existing, (target_floor, end_val), max_tail_bytes).await
}

/// The highest contiguous rows of one prepend batch that fit a byte budget.
struct PrependWindow {
    rows: VecDeque<SealedBlock>,
    bytes: usize,
    budget: usize,
    /// The last row read, which must link to the rows above the batch.
    top: Option<SealedBlock>,
    dropped: bool,
}

impl PrependWindow {
    fn push(&mut self, row: SealedBlock) {
        self.top = Some(row.clone());
        self.bytes += row.payload.len();
        self.rows.push_back(row);
        while self.bytes > self.budget {
            let Some(evicted) = self.rows.pop_front() else {
                break;
            };
            self.bytes -= evicted.payload.len();
            self.dropped = true;
        }
    }
}

async fn read_prepend_batches<Source: TailSource>(
    source: &Source,
    existing: &EncodedTail,
    (target_floor, mut cursor): (BlockHeight, u32),
    max_tail_bytes: usize,
) -> Result<Option<Arc<EncodedTail>>, Status> {
    let mut remaining_bytes = max_tail_bytes.saturating_sub(existing.bytes);
    let mut kept_bytes = 0;
    let mut peak = existing.bytes;
    let mut batches: Vec<VecDeque<SealedBlock>> = Vec::new();
    let mut expected_height = existing.rows.front().map_or(0, |row| row.id.height.value());
    let mut expected_parent = existing.rows.front().map(|row| row.parent);
    let mut byte_capped =
        existing.byte_capped || (remaining_bytes == 0 && cursor >= target_floor.value());
    while cursor >= target_floor.value() && remaining_bytes > 0 {
        check_cancel(source)?;
        let batch_start = cursor
            .saturating_sub(ENCODE_BATCH_BLOCKS - 1)
            .max(target_floor.value());
        let span =
            BlockHeightRange::inclusive(BlockHeight::new(batch_start), BlockHeight::new(cursor));
        let mut window = PrependWindow {
            rows: VecDeque::new(),
            bytes: 0,
            budget: remaining_bytes,
            top: None,
            dropped: false,
        };
        let held = existing.bytes + kept_bytes;
        let mut keep = |chunk: Vec<SealedBlock>| {
            peak = peak.max(held + window.bytes + payload_bytes(&chunk));
            for row in chunk {
                window.push(row);
            }
        };
        if matches!(
            encode_span(source, span, None, &mut keep).await?,
            SpanEnd::Unsupported
        ) {
            return Ok(None);
        }
        let Some(top) = &window.top else {
            return Err(Status::data_loss("missing canonical tail block"));
        };
        if top.id.height.value() + 1 != expected_height
            || expected_parent.is_some_and(|parent| top.id.hash != parent)
        {
            return Err(Status::data_loss("discontinuous canonical tail"));
        }
        byte_capped |= window.dropped;
        let Some(front) = window.rows.front() else {
            break;
        };
        expected_height = front.id.height.value();
        expected_parent = Some(front.parent);
        remaining_bytes -= window.bytes;
        kept_bytes += window.bytes;
        batches.push(window.rows);
        if byte_capped || batch_start == 0 || batch_start <= target_floor.value() {
            break;
        }
        cursor = batch_start - 1;
    }
    let mut rows = VecDeque::with_capacity(
        batches.iter().map(VecDeque::len).sum::<usize>() + existing.rows.len(),
    );
    for batch in batches.into_iter().rev() {
        rows.extend(batch);
    }
    rows.extend(existing.rows.iter().cloned());
    Ok(Some(Arc::new(existing.with_rows(rows, byte_capped, peak))))
}

enum SpanEnd {
    /// The source cannot supply stored row lengths.
    Unsupported,
    /// Every row was handed over; holds the last row's identity.
    Through(Option<BlockId>),
}

/// Reads `span` in ascending storage chunks and hands each linked chunk to
/// `keep`, so a build holds its kept rows and at most one chunk at a time.
///
/// Each chunk is capped at [`crate::BLOCK_RANGE_CHUNK_MAX_BYTES`] of stored
/// bytes, except that a single row larger than the cap is read alone.
async fn encode_span<Source: TailSource>(
    source: &Source,
    span: BlockHeightRange,
    mut prior: Option<BlockId>,
    keep: &mut impl FnMut(Vec<SealedBlock>),
) -> Result<SpanEnd, Status> {
    let mut next = Some(span.start);
    while let Some(height) = next.filter(|height| *height <= span.end) {
        check_cancel(source)?;
        let last =
            BlockHeight::new(height.value().saturating_add(ENCODE_BATCH_BLOCKS - 1)).min(span.end);
        let Some(rows) = encode_range(source, BlockHeightRange::inclusive(height, last)).await?
        else {
            return Ok(SpanEnd::Unsupported);
        };
        for row in &rows {
            if next != Some(row.id.height)
                || row.id.height > last
                || prior.is_some_and(|id| id.hash != row.parent)
            {
                return Err(Status::data_loss("discontinuous canonical tail"));
            }
            prior = Some(row.id);
            next = row.id.height.next();
        }
        keep(rows);
    }
    Ok(SpanEnd::Through(prior))
}

impl PublishedShieldedCatalog {
    pub(super) async fn refresh_tail(&self, reader: &dyn CanonicalReader) -> Result<(), Status> {
        self.verify_source(reader).await?;
        let source = CanonicalTailSource {
            reader,
            cancel: CancellationToken::new(),
        };
        let terminal = (self.bounds().end, self.terminal().segment.identity().end);
        let _ = self
            .tail
            .refresh_with_limit(&source, Some(terminal), (None, TAIL_BYTES))
            .await?;
        Ok(())
    }
}

async fn encode_range<Source: TailSource>(
    source: &Source,
    range: BlockHeightRange,
) -> Result<Option<Vec<SealedBlock>>, Status> {
    let Some(blocks) = source
        .read_rows(range, crate::BLOCK_RANGE_CHUNK_MAX_BYTES)
        .await?
    else {
        return Ok(None);
    };
    if blocks.is_empty() {
        return Err(Status::data_loss("missing canonical tail block"));
    }
    let rows = tokio::task::spawn_blocking(move || {
        let shape = CompactBlockShape::for_range(&[])?;
        blocks
            .into_iter()
            .map(|(block, native_bytes)| {
                Ok(SealedBlock {
                    id: BlockId::new(block.height(), block.block_hash()),
                    parent: block.previous_block_hash(),
                    native_bytes,
                    payload: shape.message(block)?.encode_to_vec().into(),
                })
            })
            .collect::<Result<Vec<_>, Status>>()
    })
    .await
    .map_err(|error| Status::internal(error.to_string()))??;
    Ok(Some(rows))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use parking_lot::Mutex;
    use zinder_core::{
        ArtifactSchemaVersion, CanonicalBlockFactsSequenceDigest,
        CanonicalBlockFactsSequenceDigestVersion, ChainEpochId, ChainTipMetadata,
        CompactChainMetadata, Network, UnixTimestampMillis,
    };

    use super::*;

    struct MockTailSource {
        epoch: ChainEpoch,
        fence: Fence,
        blocks: Vec<CompactBlockArtifact>,
        reads: Mutex<Vec<BlockHeightRange>>,
        unsupported: bool,
        current: Arc<AtomicBool>,
        cancel: CancellationToken,
        cancel_on_read: bool,
        /// Marks the view stale when a read starts.
        stale_on_read: bool,
        native_row_bytes: usize,
        owner: Arc<()>,
        source: CompactBlockSource,
    }

    fn mock_hash(height: u32, branch: u8) -> BlockHash {
        let mut raw = [branch; 32];
        raw[..4].copy_from_slice(&height.to_le_bytes());
        BlockHash::from_bytes(raw)
    }

    fn build_source(tip: u32, branch: u8) -> MockTailSource {
        let mut blocks = Vec::with_capacity(usize::try_from(tip).unwrap_or(0) + 1);
        for height in 0..=tip {
            let prev = if height == 0 {
                BlockHash::from_bytes([0; 32])
            } else {
                mock_hash(height - 1, branch)
            };
            let id = BlockId::new(BlockHeight::new(height), mock_hash(height, branch));
            blocks.push(CompactBlockArtifact::empty(
                id,
                prev,
                height,
                CompactChainMetadata {
                    sapling_commitment_tree_size: height,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            ));
        }
        let tip_id = BlockId::new(BlockHeight::new(tip), mock_hash(tip, branch));
        let epoch = ChainEpoch {
            id: ChainEpochId::new(u64::from(tip) + u64::from(branch) * 100),
            network: Network::ZcashRegtest,
            visible_tip_height: tip_id.height,
            visible_tip_hash: tip_id.hash,
            settled_tip_height: tip_id.height,
            settled_tip_hash: tip_id.hash,
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            created_at: UnixTimestampMillis::new(0),
        };
        let digest = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            u64::from(tip) + 1,
            [branch; 32],
        );
        let fence = (
            CanonicalEventFence::new(epoch.id, u64::from(tip), tip_id, digest),
            CanonicalHistoryBounds::complete(),
        );
        let owner = Arc::new(());
        MockTailSource {
            epoch,
            fence,
            blocks,
            reads: Mutex::new(Vec::new()),
            unsupported: false,
            current: Arc::new(AtomicBool::new(true)),
            cancel: CancellationToken::new(),
            cancel_on_read: false,
            stale_on_read: false,
            native_row_bytes: 64,
            source: CompactBlockSource::of(&owner),
            owner,
        }
    }

    impl TailSource for MockTailSource {
        fn epoch_and_fence(&self) -> (ChainEpoch, Fence) {
            (self.epoch, self.fence)
        }

        fn source(&self) -> Option<&CompactBlockSource> {
            Some(&self.source)
        }

        fn cancel(&self) -> &CancellationToken {
            &self.cancel
        }

        fn is_current(&self) -> impl Future<Output = bool> + Send {
            std::future::ready(self.current.load(Ordering::SeqCst))
        }

        fn block_hash_at(
            &self,
            height: BlockHeight,
        ) -> impl Future<Output = Result<Option<BlockHash>, Status>> + Send {
            let hash = self
                .blocks
                .get(usize::try_from(height.value()).unwrap_or(usize::MAX))
                .map(CompactBlockArtifact::block_hash);
            std::future::ready(Ok(hash))
        }

        fn read_rows(
            &self,
            range: BlockHeightRange,
            max_bytes: usize,
        ) -> impl Future<Output = Result<Option<Vec<(CompactBlockArtifact, usize)>>, Status>> + Send
        {
            self.reads.lock().push(range);
            if self.cancel_on_read {
                self.cancel.cancel();
            }
            if self.stale_on_read {
                self.current.store(false, Ordering::SeqCst);
            }
            if self.unsupported {
                return std::future::ready(Ok(None));
            }
            let mut stored = 0_usize;
            let out: Vec<_> = (range.start.value()..=range.end.value())
                .filter_map(|height| {
                    self.blocks
                        .get(usize::try_from(height).unwrap_or(usize::MAX))
                        .cloned()
                })
                .take_while(|_| {
                    let fits = stored == 0 || stored + self.native_row_bytes <= max_bytes;
                    stored += self.native_row_bytes;
                    fits
                })
                .map(|block| (block, self.native_row_bytes))
                .collect();
            std::future::ready(Ok(Some(out)))
        }
    }

    #[tokio::test]
    async fn cold_start_is_lazy_and_backward_prepend_reuses_suffix_and_caps_bytes()
    -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let source = build_source(10, 1);
        let span = |s, e| BlockHeightRange::inclusive(BlockHeight::new(s), BlockHeight::new(e));

        let first = buffer
            .refresh_with_limit(&source, None, (Some(span(8, 10)), TAIL_BYTES))
            .await?
            .ok_or_else(|| eyre::eyre!("expected cold tail"))?;
        assert_eq!(first.bounds(), Some(span(8, 10)));
        assert_eq!(*source.reads.lock(), vec![span(8, 10)]);

        let repeat = buffer
            .refresh_with_limit(&source, None, (Some(span(9, 10)), TAIL_BYTES))
            .await?
            .ok_or_else(|| eyre::eyre!("expected hit"))?;
        assert!(Arc::ptr_eq(&first, &repeat));
        assert_eq!(source.reads.lock().len(), 1);

        let expanded = buffer
            .refresh_with_limit(&source, None, (Some(span(5, 10)), TAIL_BYTES))
            .await?
            .ok_or_else(|| eyre::eyre!("expected backward prepend"))?;
        assert_eq!(expanded.bounds(), Some(span(5, 10)));
        assert_eq!(*source.reads.lock(), vec![span(8, 10), span(5, 7)]);
        assert_eq!(
            expanded.rows[3].payload.as_ptr(),
            first.rows[0].payload.as_ptr()
        );

        let single_row_bytes = first.rows[0].payload.len();
        let capped_budget = expanded.bytes + single_row_bytes;
        let capped = buffer
            .refresh_with_limit(&source, None, (Some(span(1, 10)), capped_budget))
            .await?
            .ok_or_else(|| eyre::eyre!("expected byte-capped tail"))?;
        assert_eq!(capped.bounds(), Some(span(4, 10)));
        assert!(capped.byte_capped);
        assert!(capped.satisfies(&source, BlockHeight::new(1)));

        let precancelled = build_source(10, 1);
        precancelled.cancel.cancel();
        let cancelled = buffer
            .refresh_with_limit(&precancelled, None, (Some(span(1, 10)), TAIL_BYTES))
            .await;
        assert!(matches!(cancelled, Err(status) if status.code() == Code::Cancelled));
        Ok(())
    }

    #[tokio::test]
    async fn tip_extension_reuses_prefix_and_unavailable_rows_fall_back() -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let mut no_lengths = build_source(5, 1);
        no_lengths.unsupported = true;
        let span = |s, e| BlockHeightRange::inclusive(BlockHeight::new(s), BlockHeight::new(e));
        assert!(
            buffer
                .refresh_with_limit(&no_lengths, None, (Some(span(3, 5)), TAIL_BYTES))
                .await?
                .is_none()
        );

        let initial = build_source(5, 1);
        let tail_one = buffer
            .refresh_with_limit(&initial, None, (Some(span(3, 5)), TAIL_BYTES))
            .await?
            .ok_or_else(|| eyre::eyre!("expected initial tail"))?;
        let extended = build_source(6, 1);
        let tail_two = buffer
            .refresh_with_limit(&extended, None, (Some(span(3, 6)), TAIL_BYTES))
            .await?
            .ok_or_else(|| eyre::eyre!("expected extended tail"))?;
        assert_eq!(tail_two.bounds(), Some(span(3, 6)));
        assert_eq!(*extended.reads.lock(), vec![span(5, 5), span(6, 6)]);
        assert_eq!(
            tail_two.rows[0].payload.as_ptr(),
            tail_one.rows[0].payload.as_ptr()
        );
        Ok(())
    }

    fn span(first: u32, last: u32) -> BlockHeightRange {
        BlockHeightRange::inclusive(BlockHeight::new(first), BlockHeight::new(last))
    }

    #[tokio::test]
    async fn a_stale_view_does_not_displace_the_current_tail() -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let advanced = build_source(6, 1);
        let current = buffer
            .refresh_with_limit(&advanced, None, (Some(span(3, 6)), TAIL_BYTES))
            .await?
            .ok_or_else(|| eyre::eyre!("expected current tail"))?;

        let stale = build_source(5, 1);
        stale.current.store(false, Ordering::SeqCst);
        assert!(
            buffer
                .refresh_with_limit(&stale, None, (Some(span(3, 5)), TAIL_BYTES))
                .await?
                .is_none()
        );
        assert!(stale.reads.lock().is_empty());
        assert!(
            buffer
                .load_full()
                .is_some_and(|tail| Arc::ptr_eq(&tail, &current))
        );

        let reorged = build_source(6, 2);
        let replaced = buffer
            .refresh_with_limit(&reorged, None, (Some(span(3, 6)), TAIL_BYTES))
            .await?
            .ok_or_else(|| eyre::eyre!("expected replacement tail"))?;
        assert_eq!(replaced.epoch, reorged.epoch);
        assert!(
            buffer
                .load_full()
                .is_some_and(|tail| Arc::ptr_eq(&tail, &replaced))
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_stops_tail_builds_and_prepends_between_batches() -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let mut source = build_source(2_500, 1);
        source.cancel_on_read = true;
        let built = buffer
            .refresh_with_limit(&source, None, (Some(span(1, 2_500)), TAIL_BYTES))
            .await;
        assert!(matches!(built, Err(status) if status.code() == Code::Cancelled));
        assert_eq!(*source.reads.lock(), vec![span(1, 1_000)]);
        assert!(buffer.load_full().is_none());

        let mut source = build_source(2_500, 1);
        buffer
            .refresh_with_limit(&source, None, (Some(span(2_000, 2_500)), TAIL_BYTES))
            .await?
            .ok_or_else(|| eyre::eyre!("expected suffix tail"))?;
        source.cancel_on_read = true;
        let prepended = buffer
            .refresh_with_limit(&source, None, (Some(span(1, 2_500)), TAIL_BYTES))
            .await;
        assert!(matches!(prepended, Err(status) if status.code() == Code::Cancelled));
        assert_eq!(
            *source.reads.lock(),
            vec![span(2_000, 2_500), span(1_000, 1_999)]
        );
        assert_eq!(
            buffer.load_full().and_then(|tail| tail.bounds()),
            Some(span(2_000, 2_500))
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_tip_extension_keeps_the_byte_cap_of_its_retained_front() -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let source = build_source(10, 1);
        let suffix = buffer
            .refresh_with_limit(&source, None, (Some(span(5, 10)), TAIL_BYTES))
            .await?
            .ok_or_else(|| eyre::eyre!("expected suffix tail"))?;
        let row = suffix.rows[0].payload.len();
        let capped = buffer
            .refresh_with_limit(&source, None, (Some(span(1, 10)), suffix.bytes + row / 2))
            .await?
            .ok_or_else(|| eyre::eyre!("expected capped tail"))?;
        assert!(capped.byte_capped);
        assert_eq!(capped.bounds(), Some(span(5, 10)));

        let advanced = build_source(11, 1);
        let extended = buffer
            .refresh_with_limit(
                &advanced,
                None,
                (Some(span(1, 11)), suffix.bytes + row + row / 2),
            )
            .await?
            .ok_or_else(|| eyre::eyre!("expected extended tail"))?;
        assert_eq!(extended.bounds(), Some(span(5, 11)));
        assert!(extended.byte_capped);
        assert_eq!(*advanced.reads.lock(), vec![span(10, 10), span(11, 11)]);
        Ok(())
    }

    #[derive(Clone)]
    struct FencedReads {
        source: Arc<MockTailSource>,
        fence: Arc<Mutex<Option<Fence>>>,
        /// The admitted data the fence reads, replaced by a republication.
        source_owner: Arc<Mutex<Option<Arc<()>>>>,
        republished: Option<Fence>,
        pin_lost: bool,
    }

    impl FencedReads {
        fn new(source: MockTailSource) -> Self {
            Self {
                fence: Arc::new(Mutex::new(Some(source.fence))),
                source_owner: Arc::new(Mutex::new(Some(source.owner.clone()))),
                source: Arc::new(source),
                republished: None,
                pin_lost: false,
            }
        }
    }

    impl CompactBlockReads for FencedReads {
        type View = ChainEpoch;

        fn capture_view(&self) -> impl Future<Output = Result<ChainEpoch, Status>> + Send {
            std::future::ready(Ok(self.source.epoch))
        }

        fn view_chain_epoch(view: &ChainEpoch) -> ChainEpoch {
            *view
        }

        fn range_fence(&self, _view: &ChainEpoch) -> Option<Fence> {
            *self.fence.lock()
        }

        fn tail_source(&self, _view: &ChainEpoch) -> Option<(Fence, CompactBlockSource)> {
            let source = self
                .source_owner
                .lock()
                .as_ref()
                .map(CompactBlockSource::of)?;
            Some(((*self.fence.lock())?, source))
        }

        fn view_activations(
            &self,
            _view: &ChainEpoch,
        ) -> impl Future<Output = Result<zinder_core::NetworkUpgradeActivations, Status>> + Send
        {
            std::future::ready(Ok(zinder_testkit::sample_regtest_upgrade_activations()))
        }

        fn tip_block_time(
            &self,
            _view: &ChainEpoch,
        ) -> impl Future<Output = Result<i64, Status>> + Send {
            std::future::ready(Ok(0))
        }

        fn read_compact_block(
            &self,
            _view: &ChainEpoch,
            _block: crate::block_methods::RequestedBlock,
        ) -> impl Future<Output = Result<CompactBlockArtifact, Status>> + Send {
            std::future::ready(Err(Status::unimplemented("the test serves no block")))
        }

        async fn block_hash_at(
            &self,
            view: &ChainEpoch,
            height: BlockHeight,
        ) -> Result<BlockHash, Status> {
            if self.pin_lost {
                return Err(zinder_query::status_from_query_error(
                    &zinder_query::QueryError::ChainEpochPinUnavailable {
                        chain_epoch_id: view.id,
                    },
                ));
            }
            self.source
                .block_hash_at(height)
                .await?
                .ok_or_else(|| Status::not_found("missing fixture block"))
        }

        async fn read_compact_block_rows_with_lengths(
            &self,
            _view: &ChainEpoch,
            range: BlockHeightRange,
            max_bytes: usize,
        ) -> Result<Option<Vec<(CompactBlockArtifact, usize)>>, Status> {
            let rows = self.source.read_rows(range, max_bytes).await;
            if let Some(republished) = self.republished {
                *self.fence.lock() = Some(republished);
            }
            rows
        }

        fn read_compact_block_chunk<Output, Convert>(
            &self,
            _read: crate::block_methods::ChunkRead<ChainEpoch>,
            _convert: Convert,
        ) -> impl Future<Output = crate::block_methods::ChunkReadOutcome<Output>> + Send
        where
            Output: Send + 'static,
            Convert: FnOnce(ChainEpoch, Vec<CompactBlockArtifact>) -> Output + Send + 'static,
        {
            std::future::ready(crate::block_methods::ChunkReadOutcome::unread(
                crate::block_methods::ChunkReadFailure::Failed(Status::unimplemented(
                    "the test serves no block range",
                )),
            ))
        }
    }

    #[tokio::test]
    async fn a_republished_pair_during_a_tail_read_installs_no_rows() -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let mut reads = FencedReads::new(build_source(6, 1));
        reads.republished = Some(build_source(6, 2).fence);
        let epoch = reads.source.epoch;
        let cancel = CancellationToken::new();
        let refreshed = buffer
            .load_or_refresh((&reads, &epoch, &cancel), None, span(3, 6))
            .await;
        assert!(matches!(refreshed, Err(status) if status.code() == Code::Aborted));
        assert!(buffer.load_full().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn an_expired_pin_is_not_read_as_a_reorg() -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let reads = FencedReads::new(build_source(6, 1));
        let epoch = reads.source.epoch;
        let cancel = CancellationToken::new();
        buffer
            .load_or_refresh((&reads, &epoch, &cancel), None, span(3, 6))
            .await?
            .ok_or_else(|| eyre::eyre!("expected initial tail"))?;

        let mut advanced = FencedReads::new(build_source(7, 1));
        advanced.pin_lost = true;
        let epoch = advanced.source.epoch;
        let refreshed = buffer
            .load_or_refresh((&advanced, &epoch, &cancel), None, span(3, 7))
            .await;
        assert!(matches!(refreshed, Err(status) if super::super::read::pin_unavailable(&status)));
        assert!(advanced.source.reads.lock().is_empty());
        Ok(())
    }
}
