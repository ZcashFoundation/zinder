use std::{collections::VecDeque, sync::Arc};

use arc_swap::ArcSwapOption;
use prost::Message as _;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CanonicalHistoryBounds, ChainEpoch,
    CompactBlockArtifact,
};
use zinder_query::{CompactBlockFence, CompactBlockSource};
use zinder_store::{CanonicalEventFence, HeightOrder};

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
    /// The admitted serving generation the rows were read from. Rows are
    /// reused only while requests read that same immutable generation.
    source: CompactBlockSource,
    rows: VecDeque<SealedBlock>,
    bytes: usize,
    byte_capped: bool,
    held: HeldBytes,
}

/// The most payload bytes a tail build held at once: the rows it keeps,
/// including rows carried over from the tail it extends, plus the storage
/// chunk it is converting. Only tests record it.
#[derive(Clone, Copy, Default)]
struct HeldBytes {
    #[cfg(test)]
    peak: usize,
}

impl HeldBytes {
    #[cfg_attr(
        not(test),
        expect(
            clippy::unused_self,
            clippy::needless_pass_by_ref_mut,
            reason = "only tests record the peak"
        )
    )]
    fn observe(&mut self, held: usize) {
        #[cfg(test)]
        {
            self.peak = self.peak.max(held);
        }
        #[cfg(not(test))]
        let _ = held;
    }
}

trait TailSource: Sync {
    fn epoch_and_fence(&self) -> (ChainEpoch, Fence);

    fn source(&self) -> &CompactBlockSource;

    fn cancel(&self) -> &CancellationToken;

    /// Returns whether this source still reads the view current now.
    fn is_current(&self) -> impl Future<Output = bool> + Send;

    fn read_rows(
        &self,
        range: BlockHeightRange,
        max_bytes: usize,
    ) -> impl Future<Output = Result<Option<Vec<(CompactBlockArtifact, usize)>>, Status>> + Send;
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
        self.reads.tail_source(view).is_some_and(|current| {
            (current.event_fence, current.history_bounds) == self.fence
                && current.source.is(&self.source)
        })
    }
}

impl<Reads: CompactBlockReads> TailSource for ReadsTailSource<'_, Reads> {
    fn epoch_and_fence(&self) -> (ChainEpoch, Fence) {
        (Reads::view_chain_epoch(self.view), self.fence)
    }

    fn source(&self) -> &CompactBlockSource {
        &self.source
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
        let Some(CompactBlockFence {
            event_fence,
            history_bounds,
            source,
        }) = reads.tail_source(view)
        else {
            return Ok(None);
        };
        let source = ReadsTailSource {
            reads,
            view,
            fence: (event_fence, history_bounds),
            source,
            cancel,
        };
        self.refresh_with_limit(&source, catalog, (requested, TAIL_BYTES))
            .await
    }

    async fn refresh_with_limit<Source: TailSource>(
        &self,
        source: &Source,
        catalog: Option<(BlockHeight, BlockId)>,
        (requested, max_tail_bytes): (BlockHeightRange, usize),
    ) -> Result<Option<Arc<EncodedTail>>, Status> {
        let cancel = source.cancel();
        check_cancel(source)?;
        let (epoch, fence) = source.epoch_and_fence();
        let Some(window_floor) = Self::window_floor(epoch, fence.1, catalog.map(|(end, _)| end))
        else {
            return Ok(None);
        };
        if requested.end < window_floor {
            return Ok(None);
        }
        let requested_floor = requested.start.max(window_floor);
        let existing = self.load_full();
        if let Some(existing) = existing
            .as_ref()
            .filter(|tail| tail.satisfies(source, requested_floor))
        {
            return Ok(Some(existing.clone()));
        }
        if !fills(existing.as_deref(), source, requested) {
            return Ok(None);
        }
        // A stale view must not spend the refresh lock building rows it may
        // not install.
        if !source.is_current().await {
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
        // The view may have changed while this miss waited for the lock.
        if !source.is_current().await {
            return Ok(None);
        }
        check_cancel(source)?;
        if !fills(previous.as_deref(), source, requested) {
            return Ok(None);
        }
        // Another generation's rows are never reused: two admitted stores can
        // share block hashes while holding different compact bytes.
        let updated = match &previous {
            Some(existing) if existing.serves(source) => {
                prepend_missing(source, existing, requested_floor, max_tail_bytes).await?
            }
            _ => {
                build(
                    source,
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

/// Returns whether filling `requested` reads only rows the tail keeps.
///
/// The tail always ends at the visible tip. A request that reaches the tip
/// starts it, and one that reaches its lowest row extends it downward. Any
/// other request reads canonical storage, so a short older range neither
/// reads the heights between it and the tail nor replaces the tail.
fn fills(
    tail: Option<&EncodedTail>,
    source: &impl TailSource,
    requested: BlockHeightRange,
) -> bool {
    match tail
        .filter(|tail| tail.serves(source))
        .and_then(EncodedTail::bounds)
    {
        Some(bounds) => requested.end.value().saturating_add(1) >= bounds.start.value(),
        None => requested.end >= source.epoch_and_fence().0.visible_tip_height,
    }
}

impl EncodedTail {
    pub(super) fn after_published(self: Arc<Self>, end: BlockHeight) -> Arc<Self> {
        let covered = self.rows.partition_point(|row| row.id.height <= end);
        if covered == 0 {
            return self;
        }
        let rows: VecDeque<_> = self.rows.range(covered..).cloned().collect();
        Arc::new(self.with_rows(rows, false, self.held))
    }

    pub(super) fn bounds(&self) -> Option<BlockHeightRange> {
        Some(BlockHeightRange::inclusive(
            self.rows.front()?.id.height,
            self.rows.back()?.id.height,
        ))
    }

    /// Returns whether these rows were read from `source`'s view and data.
    fn serves(&self, source: &impl TailSource) -> bool {
        let (epoch, fence) = source.epoch_and_fence();
        self.epoch == epoch && self.fence == fence && self.source.is(source.source())
    }

    fn satisfies(&self, source: &impl TailSource, requested_floor: BlockHeight) -> bool {
        self.serves(source)
            && (self.byte_capped
                || self
                    .bounds()
                    .is_some_and(|bounds| bounds.start <= requested_floor))
    }

    fn with_rows(&self, rows: VecDeque<SealedBlock>, byte_capped: bool, held: HeldBytes) -> Self {
        Self {
            epoch: self.epoch,
            fence: self.fence,
            source: self.source.clone(),
            bytes: payload_bytes(&rows),
            byte_capped,
            rows,
            held,
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

    /// Appends `row`, evicting the lowest rows past `max_tail_bytes`. The
    /// newest row stays even when it alone exceeds the budget.
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

/// Reads the requested part of the window from `source` alone.
async fn build<Source: TailSource>(
    source: &Source,
    (window_floor, requested_floor, max_tail_bytes): (BlockHeight, BlockHeight, usize),
    catalog: Option<(BlockHeight, BlockId)>,
) -> Result<Option<Arc<EncodedTail>>, Status> {
    let (epoch, fence) = source.epoch_and_fence();
    let mut start_floor = requested_floor;
    if let Some((catalog_end, _)) = catalog
        && requested_floor == window_floor
        && let Some(after_catalog) = catalog_end.next()
    {
        start_floor = start_floor.max(after_catalog);
    }
    let mut prior =
        catalog.and_then(|(end, id)| (Some(end.next()?) == Some(start_floor)).then_some(id));
    let mut tail = EncodedTail {
        epoch,
        fence,
        source: source.source().clone(),
        bytes: 0,
        byte_capped: false,
        rows: VecDeque::new(),
        held: HeldBytes::default(),
    };
    if start_floor <= epoch.visible_tip_height {
        let span = BlockHeightRange::inclusive(start_floor, epoch.visible_tip_height);
        let mut held = HeldBytes::default();
        let mut keep = |chunk: Vec<SealedBlock>| {
            held.observe(tail.bytes + payload_bytes(&chunk));
            for row in chunk {
                tail.push_with_limit(row, max_tail_bytes);
            }
        };
        match encode_span(source, span, prior, &mut keep).await? {
            SpanEnd::Unsupported => return Ok(None),
            SpanEnd::Through(last) => prior = last.or(prior),
        }
        tail.held = held;
    }
    if prior.is_some_and(|id| {
        id.height == epoch.visible_tip_height && id.hash != epoch.visible_tip_hash
    }) {
        return Err(Status::data_loss(
            "encoded tail differs from the visible tip",
        ));
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
            existing.held,
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
    top: Option<(BlockId, BlockHash)>,
    dropped: bool,
}

impl PrependWindow {
    fn push(&mut self, row: SealedBlock) {
        self.top = Some((row.id, row.parent));
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
    let mut held = existing.held;
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
        let retained = existing.bytes + kept_bytes;
        let mut keep = |chunk: Vec<SealedBlock>| {
            held.observe(retained + window.bytes + payload_bytes(&chunk));
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
        let Some((top, _)) = window.top else {
            return Err(Status::data_loss("missing canonical tail block"));
        };
        if top.height.value() + 1 != expected_height
            || expected_parent.is_some_and(|parent| top.hash != parent)
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
    Ok(Some(Arc::new(existing.with_rows(rows, byte_capped, held))))
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
    use std::{
        num::NonZeroU32,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use eyre::OptionExt as _;
    use parking_lot::Mutex;
    use tokio_stream::StreamExt as _;
    use tonic::Code;
    use zinder_core::{
        ArtifactSchemaVersion, CanonicalBlockFactsSequenceDigest,
        CanonicalBlockFactsSequenceDigestVersion, ChainEpochId, ChainTipMetadata,
        CompactChainMetadata, Network, UnixTimestampMillis,
    };
    use zinder_proto::compat::lightwalletd;

    use super::*;
    use crate::block_methods::{
        BlockMethods, ChunkRead, ChunkReadFailure, ChunkReadOutcome, ChunkView, RangeSource as _,
    };

    struct MockTailSource {
        epoch: ChainEpoch,
        fence: Fence,
        blocks: Vec<CompactBlockArtifact>,
        reads: Mutex<Vec<BlockHeightRange>>,
        unsupported: bool,
        current: Arc<AtomicBool>,
        cancel: CancellationToken,
        cancel_on_read: bool,
        read_gate: Option<Arc<tokio::sync::Notify>>,
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

    fn mock_block(height: u32, branch: u8, sapling_size: u32) -> CompactBlockArtifact {
        let parent = if height == 0 {
            BlockHash::from_bytes([0; 32])
        } else {
            mock_hash(height - 1, branch)
        };
        CompactBlockArtifact::empty(
            BlockId::new(BlockHeight::new(height), mock_hash(height, branch)),
            parent,
            height,
            CompactChainMetadata {
                sapling_commitment_tree_size: sapling_size,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )
    }

    fn build_source(tip: u32, branch: u8) -> MockTailSource {
        let blocks = (0..=tip)
            .map(|height| mock_block(height, branch, height))
            .collect();
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
            read_gate: None,
            stale_on_read: false,
            native_row_bytes: 64,
            source: CompactBlockSource::of(&owner),
            owner,
        }
    }

    impl MockTailSource {
        fn stored_rows(
            &self,
            range: BlockHeightRange,
            max_bytes: usize,
        ) -> Vec<(CompactBlockArtifact, usize)> {
            let mut stored = 0_usize;
            (range.start.value()..=range.end.value())
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
                .collect()
        }

        fn largest_payload(&self) -> eyre::Result<usize> {
            let shape = CompactBlockShape::for_range(&[])?;
            let mut largest = 0;
            for block in &self.blocks {
                largest = largest.max(shape.message(block.clone())?.encode_to_vec().len());
            }
            Ok(largest)
        }
    }

    impl TailSource for MockTailSource {
        fn epoch_and_fence(&self) -> (ChainEpoch, Fence) {
            (self.epoch, self.fence)
        }

        fn source(&self) -> &CompactBlockSource {
            &self.source
        }

        fn cancel(&self) -> &CancellationToken {
            &self.cancel
        }

        fn is_current(&self) -> impl Future<Output = bool> + Send {
            std::future::ready(self.current.load(Ordering::SeqCst))
        }

        async fn read_rows(
            &self,
            range: BlockHeightRange,
            max_bytes: usize,
        ) -> Result<Option<Vec<(CompactBlockArtifact, usize)>>, Status> {
            self.reads.lock().push(range);
            if let Some(gate) = &self.read_gate {
                gate.notified().await;
            }
            if self.cancel_on_read {
                self.cancel.cancel();
            }
            if self.stale_on_read {
                self.current.store(false, Ordering::SeqCst);
            }
            if self.unsupported {
                return Ok(None);
            }
            Ok(Some(self.stored_rows(range, max_bytes)))
        }
    }

    fn span(first: u32, last: u32) -> BlockHeightRange {
        BlockHeightRange::inclusive(BlockHeight::new(first), BlockHeight::new(last))
    }

    async fn fill(
        buffer: &ShieldedTailBuffer,
        source: &MockTailSource,
        requested: BlockHeightRange,
        budget: usize,
    ) -> eyre::Result<Arc<EncodedTail>> {
        buffer
            .refresh_with_limit(source, None, (requested, budget))
            .await?
            .ok_or_else(|| eyre::eyre!("expected a tail for {requested:?}"))
    }

    #[tokio::test]
    async fn cold_start_is_lazy_and_backward_prepend_reuses_its_own_rows_and_caps_bytes()
    -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let source = build_source(10, 1);

        let first = fill(&buffer, &source, span(8, 10), TAIL_BYTES).await?;
        assert_eq!(first.bounds(), Some(span(8, 10)));
        assert_eq!(*source.reads.lock(), vec![span(8, 10)]);

        let repeat = fill(&buffer, &source, span(9, 10), TAIL_BYTES).await?;
        assert!(Arc::ptr_eq(&first, &repeat));
        assert_eq!(source.reads.lock().len(), 1);

        let expanded = fill(&buffer, &source, span(5, 10), TAIL_BYTES).await?;
        assert_eq!(expanded.bounds(), Some(span(5, 10)));
        assert_eq!(*source.reads.lock(), vec![span(8, 10), span(5, 7)]);
        assert_eq!(
            expanded.rows[3].payload.as_ptr(),
            first.rows[0].payload.as_ptr()
        );

        let single_row_bytes = first.rows[0].payload.len();
        let capped = fill(
            &buffer,
            &source,
            span(1, 10),
            expanded.bytes + single_row_bytes,
        )
        .await?;
        assert_eq!(capped.bounds(), Some(span(4, 10)));
        assert!(capped.byte_capped);
        assert!(capped.satisfies(&source, BlockHeight::new(1)));

        let unsupported = {
            let mut source = build_source(5, 1);
            source.unsupported = true;
            source
        };
        assert!(
            ShieldedTailBuffer::default()
                .refresh_with_limit(&unsupported, None, (span(3, 5), TAIL_BYTES))
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_new_generation_refills_only_the_requested_window() -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let initial = build_source(500, 1);
        let first = fill(&buffer, &initial, span(401, 500), TAIL_BYTES).await?;
        assert_eq!(
            fill(&buffer, &initial, span(401, 500), TAIL_BYTES)
                .await?
                .rows
                .len(),
            100
        );
        assert_eq!(*initial.reads.lock(), vec![span(401, 500)]);

        let advanced = build_source(501, 1);
        let refilled = fill(&buffer, &advanced, span(402, 501), TAIL_BYTES).await?;
        assert_eq!(refilled.bounds(), Some(span(402, 501)));
        assert_eq!(*advanced.reads.lock(), vec![span(402, 501)]);
        assert!(first.rows.iter().all(|old| {
            refilled
                .rows
                .iter()
                .all(|new| old.payload.as_ptr() != new.payload.as_ptr())
        }));

        let replaced = build_source(501, 2);
        let replacement = fill(&buffer, &replaced, span(402, 501), TAIL_BYTES).await?;
        assert_eq!(replacement.epoch, replaced.epoch);
        assert_eq!(*replaced.reads.lock(), vec![span(402, 501)]);
        Ok(())
    }

    #[tokio::test]
    async fn rows_from_another_generation_are_never_served_at_an_equal_fence() -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let original = build_source(10, 1);
        let mut other = build_source(10, 1);
        other.blocks[6] = mock_block(6, 1, 600);
        assert_eq!(original.epoch_and_fence(), other.epoch_and_fence());

        let served = fill(&buffer, &original, span(3, 10), TAIL_BYTES).await?;
        let replacement = fill(&buffer, &other, span(3, 10), TAIL_BYTES).await?;
        assert_eq!(*other.reads.lock(), vec![span(3, 10)]);
        let shape = CompactBlockShape::for_range(&[])?;
        let expected = shape.message(other.blocks[6].clone())?.encode_to_vec();
        assert_eq!(replacement.rows[3].payload.as_ref(), expected.as_slice());
        assert_ne!(served.rows[3].payload, replacement.rows[3].payload);
        Ok(())
    }

    #[tokio::test]
    async fn a_stale_view_does_not_displace_the_current_tail() -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let advanced = build_source(6, 1);
        let current = fill(&buffer, &advanced, span(3, 6), TAIL_BYTES).await?;

        let stale = build_source(5, 1);
        stale.current.store(false, Ordering::SeqCst);
        assert!(
            buffer
                .refresh_with_limit(&stale, None, (span(3, 5), TAIL_BYTES))
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
        let replaced = fill(&buffer, &reorged, span(3, 6), TAIL_BYTES).await?;
        assert!(
            buffer
                .load_full()
                .is_some_and(|tail| Arc::ptr_eq(&tail, &replaced))
        );
        Ok(())
    }

    fn poll_once<F: Future>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    #[tokio::test]
    async fn a_stale_cold_miss_skips_the_refresh_queue() {
        let buffer = ShieldedTailBuffer::default();
        let stale = build_source(5, 1);
        stale.current.store(false, Ordering::SeqCst);
        let _guard = buffer.0.refresh_lock.lock().await;
        let read = buffer.refresh_with_limit(&stale, None, (span(1, 5), TAIL_BYTES));
        tokio::pin!(read);
        assert!(matches!(
            poll_once(read.as_mut()),
            std::task::Poll::Ready(Ok(None))
        ));
        assert!(stale.reads.lock().is_empty());
    }

    #[tokio::test]
    async fn a_queued_miss_that_turns_stale_skips_its_storage_reads() -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let source = build_source(5, 1);
        let guard = buffer.0.refresh_lock.lock().await;
        let read = buffer.refresh_with_limit(&source, None, (span(1, 5), TAIL_BYTES));
        tokio::pin!(read);
        assert!(poll_once(read.as_mut()).is_pending());
        source.current.store(false, Ordering::SeqCst);
        drop(guard);
        assert!(read.await?.is_none());
        assert!(source.reads.lock().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn a_view_that_turns_stale_during_its_fill_serves_only_its_request() -> eyre::Result<()> {
        let buffer = ShieldedTailBuffer::default();
        let current = fill(&buffer, &build_source(6, 1), span(3, 6), TAIL_BYTES).await?;

        let mut overtaken = build_source(7, 1);
        overtaken.stale_on_read = true;
        let private = fill(&buffer, &overtaken, span(3, 7), TAIL_BYTES).await?;
        assert_eq!(private.bounds(), Some(span(3, 7)));
        assert!(
            buffer
                .load_full()
                .is_some_and(|tail| Arc::ptr_eq(&tail, &current))
        );

        let mut first_fill = build_source(7, 1);
        first_fill.stale_on_read = true;
        let empty = ShieldedTailBuffer::default();
        fill(&empty, &first_fill, span(3, 7), TAIL_BYTES).await?;
        assert!(empty.load_full().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_stops_tail_builds_and_prepends_between_storage_chunks() -> eyre::Result<()>
    {
        let buffer = ShieldedTailBuffer::default();
        let mut source = build_source(2_500, 1);
        source.cancel_on_read = true;
        let built = buffer
            .refresh_with_limit(&source, None, (span(1, 2_500), TAIL_BYTES))
            .await;
        assert!(matches!(built, Err(status) if status.code() == Code::Cancelled));
        assert_eq!(*source.reads.lock(), vec![span(1, 1_000)]);
        assert!(buffer.load_full().is_none());

        let mut source = build_source(2_500, 1);
        fill(&buffer, &source, span(2_000, 2_500), TAIL_BYTES).await?;
        source.cancel_on_read = true;
        let prepended = buffer
            .refresh_with_limit(&source, None, (span(1, 2_500), TAIL_BYTES))
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

        let precancelled = build_source(10, 1);
        precancelled.cancel.cancel();
        let cancelled = buffer
            .refresh_with_limit(&precancelled, None, (span(1, 10), TAIL_BYTES))
            .await;
        assert!(matches!(cancelled, Err(status) if status.code() == Code::Cancelled));
        assert!(precancelled.reads.lock().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn a_queued_refresh_proceeds_when_a_cancelled_storage_chunk_finishes() -> eyre::Result<()>
    {
        let buffer = ShieldedTailBuffer::default();
        let gate = Arc::new(tokio::sync::Notify::new());
        let mut source = build_source(2_500, 1);
        source.read_gate = Some(gate.clone());
        let follower = build_source(5, 2);
        let owner = buffer.refresh_with_limit(&source, None, (span(1, 2_500), TAIL_BYTES));
        let next = buffer.refresh_with_limit(&follower, None, (span(1, 5), TAIL_BYTES));
        tokio::pin!(owner, next);
        assert!(poll_once(owner.as_mut()).is_pending());
        assert_eq!(*source.reads.lock(), vec![span(1, 1_000)]);
        assert!(poll_once(next.as_mut()).is_pending());
        source.cancel.cancel();
        assert!(poll_once(owner.as_mut()).is_pending());
        assert!(poll_once(next.as_mut()).is_pending());
        assert!(follower.reads.lock().is_empty());
        gate.notify_one();
        assert!(matches!(owner.await, Err(status) if status.code() == Code::Cancelled));
        let installed = next.await?.ok_or_eyre("missing refreshed tail")?;
        assert!(
            buffer
                .load_full()
                .is_some_and(|tail| Arc::ptr_eq(&tail, &installed))
        );
        assert_eq!(*source.reads.lock(), vec![span(1, 1_000)]);
        assert_eq!(*follower.reads.lock(), vec![span(1, 5)]);
        Ok(())
    }

    /// Stores each block as a quarter of a storage chunk, so a read holds four.
    fn quarter_chunk_rows(tip: u32) -> MockTailSource {
        let mut source = build_source(tip, 1);
        source.native_row_bytes = crate::BLOCK_RANGE_CHUNK_MAX_BYTES / 4;
        source
    }

    #[tokio::test]
    async fn a_build_holds_its_byte_budget_plus_one_storage_chunk() -> eyre::Result<()> {
        let source = quarter_chunk_rows(2_500);
        let row = source.largest_payload()?;
        let budget = 10 * row;
        let tail = fill(
            &ShieldedTailBuffer::default(),
            &source,
            span(1, 2_500),
            budget,
        )
        .await?;
        assert!(tail.byte_capped);
        assert!(tail.bytes <= budget);
        assert_eq!(
            tail.bounds().map(|bounds| bounds.end),
            Some(BlockHeight::new(2_500))
        );
        assert!(
            tail.held.peak <= budget + 4 * row,
            "{} > {}",
            tail.held.peak,
            budget + 4 * row
        );
        assert_eq!(source.reads.lock().len(), 2_500 / 4);
        Ok(())
    }

    #[tokio::test]
    async fn a_prepend_holds_the_retained_tail_its_budget_and_one_storage_chunk() -> eyre::Result<()>
    {
        let buffer = ShieldedTailBuffer::default();
        let source = quarter_chunk_rows(2_500);
        let row = source.largest_payload()?;
        let suffix = fill(&buffer, &source, span(2_001, 2_500), TAIL_BYTES).await?;
        let budget = suffix.bytes + 10 * row;
        let prepended = fill(&buffer, &source, span(1, 2_500), budget).await?;
        assert!(prepended.byte_capped);
        assert!(prepended.bytes <= budget);
        assert_eq!(
            prepended.bounds().map(|bounds| bounds.end),
            Some(BlockHeight::new(2_500))
        );
        assert!(
            prepended.held.peak <= budget + 4 * row,
            "{} > {}",
            prepended.held.peak,
            budget + 4 * row
        );
        Ok(())
    }

    /// Canonical reads over one mock chain, as a serving mode answers them.
    #[derive(Clone)]
    struct FencedReads {
        source: Arc<MockTailSource>,
        fence: Arc<Mutex<Option<Fence>>>,
        /// The admitted data the fence reads, replaced by a republication.
        source_owner: Arc<Mutex<Arc<()>>>,
        republished: Option<Fence>,
        pin_lost: bool,
        tail_enabled: bool,
        native_reads: Arc<AtomicUsize>,
    }

    impl FencedReads {
        fn new(source: MockTailSource) -> Self {
            Self {
                fence: Arc::new(Mutex::new(Some(source.fence))),
                source_owner: Arc::new(Mutex::new(source.owner.clone())),
                source: Arc::new(source),
                republished: None,
                pin_lost: false,
                tail_enabled: true,
                native_reads: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn pin_status(view: &ChainEpoch) -> Status {
            zinder_query::status_from_query_error(
                &zinder_query::QueryError::ChainEpochPinUnavailable {
                    chain_epoch_id: view.id,
                },
            )
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

        fn tail_source(&self, _view: &ChainEpoch) -> Option<CompactBlockFence> {
            let (event_fence, history_bounds) = (*self.fence.lock())?;
            self.tail_enabled.then(|| CompactBlockFence {
                event_fence,
                history_bounds,
                source: CompactBlockSource::of(&self.source_owner.lock()),
            })
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

        async fn read_compact_block_rows_with_lengths(
            &self,
            view: &ChainEpoch,
            range: BlockHeightRange,
            max_bytes: usize,
        ) -> Result<Option<Vec<(CompactBlockArtifact, usize)>>, Status> {
            if self.pin_lost {
                return Err(Self::pin_status(view));
            }
            let rows = self.source.read_rows(range, max_bytes).await;
            if let Some(republished) = self.republished {
                *self.fence.lock() = Some(republished);
                *self.source_owner.lock() = Arc::new(());
            }
            rows
        }

        fn read_compact_block_chunk<Output, Convert>(
            &self,
            read: ChunkRead<ChainEpoch>,
            convert: Convert,
        ) -> impl Future<Output = ChunkReadOutcome<Output>> + Send
        where
            Output: Send + 'static,
            Convert: FnOnce(ChainEpoch, Vec<CompactBlockArtifact>) -> Output + Send + 'static,
        {
            self.native_reads.fetch_add(1, Ordering::SeqCst);
            let epoch = match read.view {
                ChunkView::Captured(view) => view,
                ChunkView::Current => self.source.epoch,
            };
            let mut blocks: Vec<_> = self
                .source
                .stored_rows(read.chunk, usize::MAX)
                .into_iter()
                .map(|(block, _)| block)
                .collect();
            let fits = (read.max_bytes / self.source.native_row_bytes).max(1);
            if blocks.len() > fits {
                if read.order == HeightOrder::Descending {
                    blocks.drain(..blocks.len() - fits);
                } else {
                    blocks.truncate(fits);
                }
            }
            std::future::ready(ChunkReadOutcome {
                read_duration: None,
                converted: Ok(convert(epoch, blocks)),
            })
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

        let mut same_fence = FencedReads::new(build_source(6, 1));
        same_fence.republished = Some(same_fence.source.fence);
        let refreshed = buffer
            .load_or_refresh((&same_fence, &epoch, &cancel), None, span(3, 6))
            .await;
        assert!(matches!(refreshed, Err(status) if status.code() == Code::Aborted));
        assert!(buffer.load_full().is_none());
        Ok(())
    }

    fn published_read() -> eyre::Result<super::super::PublishedRangeRead> {
        Ok(super::super::PublishedRangeRead::new(
            None,
            ShieldedTailBuffer::default(),
            CompactBlockShape::for_range(&[])?,
            false,
            crate::block_range::BlockRangeStreamLimit::new(NonZeroU32::MIN),
        ))
    }

    fn chunk_read(view: ChunkView<ChainEpoch>) -> ChunkRead<ChainEpoch> {
        ChunkRead {
            chunk: span(3, 6),
            order: HeightOrder::Ascending,
            max_bytes: crate::BLOCK_RANGE_CHUNK_MAX_BYTES,
            view,
            cancel: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn an_expired_pin_during_the_tail_fill_reaches_the_existing_retry() -> eyre::Result<()> {
        let mut reads = FencedReads::new(build_source(6, 1));
        reads.pin_lost = true;

        let current = published_read()?
            .read(&reads, chunk_read(ChunkView::Current), |_, blocks| {
                blocks.len()
            })
            .await;
        assert!(matches!(current.converted, Ok(4)));
        assert_eq!(reads.native_reads.load(Ordering::SeqCst), 1);

        let captured = published_read()?
            .read(
                &reads,
                chunk_read(ChunkView::Captured(reads.source.epoch)),
                |_, blocks| blocks.len(),
            )
            .await;
        assert!(matches!(
            captured.converted,
            Err(ChunkReadFailure::Failed(status)) if super::super::read::pin_unavailable(&status)
        ));
        assert_eq!(reads.native_reads.load(Ordering::SeqCst), 1);
        Ok(())
    }

    /// Streams `first..=last` through the shared block methods and returns
    /// every frame with the status that ended the stream, if any.
    async fn streamed(
        reads: &FencedReads,
        first: u64,
        last: u64,
    ) -> eyre::Result<(Vec<prost::bytes::Bytes>, Option<Code>)> {
        let block = |height| {
            Some(lightwalletd::BlockId {
                height,
                hash: Vec::new(),
            })
        };
        let request = lightwalletd::BlockRange {
            start: block(first),
            end: block(last),
            pool_types: Vec::new(),
        };
        let opened = BlockMethods::default()
            .published_block_range(reads, &request, (None, ShieldedTailBuffer::default()))
            .await;
        let mut stream = match opened {
            Ok(stream) => stream,
            Err(status) => return Ok((Vec::new(), Some(status.code()))),
        };
        let mut frames = Vec::new();
        while let Some(frame) = stream.next().await {
            match frame {
                Ok(frame) => frames.push(frame),
                Err(status) => return Ok((frames, Some(status.code()))),
            }
        }
        Ok((frames, None))
    }

    /// Streams `1..=60` over `faulty` with and without the tail, asserts both
    /// send the same frames and end the same way, and returns that outcome.
    ///
    /// Each stored block is 100 KiB, so the first chunk holds two blocks and
    /// each later chunk twenty: 1..=2, 3..=22, 23..=42, and 43..=60.
    async fn tail_fault_outcome(
        mut faulty: MockTailSource,
    ) -> eyre::Result<(Vec<prost::bytes::Bytes>, Option<Code>)> {
        faulty.native_row_bytes = 100 * 1024;
        let with_tail = FencedReads::new(faulty);
        let mut without_tail = with_tail.clone();
        without_tail.tail_enabled = false;
        let native = streamed(&without_tail, 1, 60).await?;
        let tailed = streamed(&with_tail, 1, 60).await?;
        assert!(!with_tail.source.reads.lock().is_empty());
        assert_eq!(tailed, native);
        Ok(native)
    }

    fn unlinked_block(height: u32) -> CompactBlockArtifact {
        CompactBlockArtifact::empty(
            BlockId::new(BlockHeight::new(height), mock_hash(height, 1)),
            mock_hash(height - 1, 7),
            height,
            CompactChainMetadata {
                sapling_commitment_tree_size: height,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )
    }

    #[tokio::test]
    async fn an_unlinked_row_at_a_chunk_boundary_ends_where_the_native_reader_does()
    -> eyre::Result<()> {
        let mut faulty = build_source(60, 1);
        faulty.blocks[43] = unlinked_block(43);
        let (frames, ending) = tail_fault_outcome(faulty).await?;
        assert_eq!((frames.len(), ending), (42, Some(Code::Aborted)));
        Ok(())
    }

    #[tokio::test]
    async fn an_unlinked_row_inside_a_chunk_streams_as_the_native_reader_does() -> eyre::Result<()>
    {
        let mut faulty = build_source(60, 1);
        faulty.blocks[45] = unlinked_block(45);
        let (frames, ending) = tail_fault_outcome(faulty).await?;
        assert_eq!((frames.len(), ending), (60, None));
        Ok(())
    }

    #[tokio::test]
    async fn a_stored_tip_that_differs_from_the_visible_tip_streams_as_the_native_reader_does()
    -> eyre::Result<()> {
        let mut faulty = build_source(60, 1);
        faulty.blocks[60] = mock_block(60, 7, 60);
        let (frames, ending) = tail_fault_outcome(faulty).await?;
        assert_eq!((frames.len(), ending), (60, None));
        Ok(())
    }
}
