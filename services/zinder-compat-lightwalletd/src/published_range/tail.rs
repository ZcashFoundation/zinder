use std::{collections::VecDeque, sync::Arc};

use arc_swap::ArcSwapOption;
use prost::Message as _;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CanonicalHistoryBounds, ChainEpoch,
    CompactBlockArtifact,
};
use zinder_query::CanonicalReader;
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
    rows: VecDeque<SealedBlock>,
    bytes: usize,
    byte_capped: bool,
}

trait TailSource: Sync {
    fn epoch_and_fence(&self) -> (ChainEpoch, Fence);

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
}

impl TailSource for CanonicalTailSource<'_> {
    fn epoch_and_fence(&self) -> (ChainEpoch, Fence) {
        (
            self.reader.chain_epoch(),
            (self.reader.event_fence(), self.reader.history_bounds()),
        )
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
}

impl<Reads: CompactBlockReads> TailSource for ReadsTailSource<'_, Reads> {
    fn epoch_and_fence(&self) -> (ChainEpoch, Fence) {
        (Reads::view_chain_epoch(self.view), self.fence)
    }

    async fn block_hash_at(&self, height: BlockHeight) -> Result<Option<BlockHash>, Status> {
        match self.reads.block_hash_at(self.view, height).await {
            Ok(hash) => Ok(Some(hash)),
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

    fn read_rows(
        &self,
        range: BlockHeightRange,
        max_bytes: usize,
    ) -> impl Future<Output = Result<Option<Vec<(CompactBlockArtifact, usize)>>, Status>> + Send
    {
        self.reads
            .read_compact_block_rows_with_lengths(self.view, range, max_bytes)
    }
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
        (reads, view, fence, cancel): (&Reads, &Reads::View, Fence, &CancellationToken),
        catalog: Option<(BlockHeight, BlockId)>,
        requested: BlockHeightRange,
    ) -> Result<Option<Arc<EncodedTail>>, Status> {
        let source = ReadsTailSource { reads, view, fence };
        self.refresh_with_limit(&source, catalog, (Some(requested), TAIL_BYTES, cancel))
            .await
    }

    async fn refresh_with_source<Source: TailSource>(
        &self,
        source: &Source,
        catalog: Option<(BlockHeight, BlockId)>,
        requested: Option<BlockHeightRange>,
    ) -> Result<Option<Arc<EncodedTail>>, Status> {
        let no_cancel = CancellationToken::new();
        self.refresh_with_limit(source, catalog, (requested, TAIL_BYTES, &no_cancel))
            .await
    }

    async fn refresh_with_limit<Source: TailSource>(
        &self,
        source: &Source,
        catalog: Option<(BlockHeight, BlockId)>,
        (requested, max_tail_bytes, cancel): (Option<BlockHeightRange>, usize, &CancellationToken),
    ) -> Result<Option<Arc<EncodedTail>>, Status> {
        if cancel.is_cancelled() {
            return Err(Status::cancelled("block-range stream cancelled"));
        }
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
        if let Some(existing) = self
            .load_full()
            .filter(|tail| tail.satisfies(epoch, fence, requested_floor))
        {
            return Ok(Some(existing));
        }
        if cancel.is_cancelled() {
            return Err(Status::cancelled("block-range stream cancelled"));
        }
        let _guard = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(Status::cancelled("block-range stream cancelled")),
            guard = self.0.refresh_lock.lock() => guard,
        };
        if cancel.is_cancelled() {
            return Err(Status::cancelled("block-range stream cancelled"));
        }
        let previous = self.load_full();
        if let Some(existing) = &previous
            && existing.satisfies(epoch, fence, requested_floor)
        {
            return Ok(Some(existing.clone()));
        }
        let updated = match &previous {
            Some(existing) if existing.epoch == epoch && existing.fence == fence => {
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
        if let Some(next_tail) = updated {
            self.0
                .current
                .compare_and_swap(&previous, Some(next_tail.clone()));
            return Ok(Some(next_tail));
        }
        Ok(None)
    }
}

impl EncodedTail {
    pub(super) fn after_published(self: Arc<Self>, end: BlockHeight) -> Arc<Self> {
        let covered = self.rows.partition_point(|row| row.id.height <= end);
        if covered == 0 {
            return self;
        }
        let rows: VecDeque<_> = self.rows.range(covered..).cloned().collect();
        Arc::new(Self {
            epoch: self.epoch,
            fence: self.fence,
            bytes: rows.iter().map(|row| row.payload.len()).sum(),
            byte_capped: false,
            rows,
        })
    }

    pub(super) fn bounds(&self) -> Option<BlockHeightRange> {
        Some(BlockHeightRange::inclusive(
            self.rows.front()?.id.height,
            self.rows.back()?.id.height,
        ))
    }

    fn satisfies(&self, epoch: ChainEpoch, fence: Fence, requested_floor: BlockHeight) -> bool {
        self.epoch == epoch
            && self.fence == fence
            && (self.byte_capped
                || self
                    .bounds()
                    .is_some_and(|bounds| bounds.start <= requested_floor))
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

    async fn retained_prefix<Source: TailSource>(
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
        Ok(self.rows.range(left.min(lo)..lo).cloned().collect())
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
        Some(prior) => prior.retained_prefix(source, window_floor).await?,
        None => VecDeque::new(),
    };
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
    let mut tail = EncodedTail {
        epoch,
        fence,
        bytes: retained.iter().map(|row| row.payload.len()).sum(),
        byte_capped: false,
        rows: retained,
    };
    if let Some(first) = next
        && first <= epoch.visible_tip_height
    {
        let span = BlockHeightRange::inclusive(first, epoch.visible_tip_height);
        let Some(rows) = encode_span(source, span, prior).await? else {
            return Ok(None);
        };
        if let Some(last) = rows.last() {
            prior = Some(last.id);
        }
        for row in rows {
            tail.push_with_limit(row, max_tail_bytes);
        }
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
    let Some(bounds) = existing.bounds() else {
        return Ok(Some(Arc::new(EncodedTail {
            epoch: existing.epoch,
            fence: existing.fence,
            rows: existing.rows.clone(),
            bytes: existing.bytes,
            byte_capped: existing.byte_capped,
        })));
    };
    if bounds.start <= target_floor || existing.byte_capped {
        return Ok(Some(Arc::new(EncodedTail {
            epoch: existing.epoch,
            fence: existing.fence,
            rows: existing.rows.clone(),
            bytes: existing.bytes,
            byte_capped: existing.byte_capped,
        })));
    }
    let Some(end_val) = bounds.start.value().checked_sub(1) else {
        return Ok(Some(Arc::new(EncodedTail {
            epoch: existing.epoch,
            fence: existing.fence,
            rows: existing.rows.clone(),
            bytes: existing.bytes,
            byte_capped: existing.byte_capped,
        })));
    };
    read_prepend_batches(source, existing, (target_floor, end_val), max_tail_bytes).await
}

async fn read_prepend_batches<Source: TailSource>(
    source: &Source,
    existing: &EncodedTail,
    (target_floor, mut cursor): (BlockHeight, u32),
    max_tail_bytes: usize,
) -> Result<Option<Arc<EncodedTail>>, Status> {
    let mut remaining_bytes = max_tail_bytes.saturating_sub(existing.bytes);
    let mut batches: Vec<Vec<SealedBlock>> = Vec::new();
    let mut expected_height = existing.rows.front().map_or(0, |row| row.id.height.value());
    let mut expected_parent = existing.rows.front().map(|row| row.parent);
    let mut byte_capped =
        existing.byte_capped || (remaining_bytes == 0 && cursor >= target_floor.value());
    while cursor >= target_floor.value() && remaining_bytes > 0 {
        let batch_start = cursor
            .saturating_sub(ENCODE_BATCH_BLOCKS - 1)
            .max(target_floor.value());
        let span =
            BlockHeightRange::inclusive(BlockHeight::new(batch_start), BlockHeight::new(cursor));
        let Some(mut batch) = encode_span(source, span, None).await? else {
            return Ok(None);
        };
        let Some(last_row) = batch.last() else {
            return Err(Status::data_loss("missing canonical tail block"));
        };
        if last_row.id.height.value() + 1 != expected_height
            || expected_parent.is_some_and(|parent| last_row.id.hash != parent)
        {
            return Err(Status::data_loss("discontinuous canonical tail"));
        }
        let mut keep_from = batch.len();
        for (idx, row) in batch.iter().enumerate().rev() {
            if row.payload.len() > remaining_bytes {
                byte_capped = true;
                break;
            }
            remaining_bytes -= row.payload.len();
            keep_from = idx;
        }
        let kept = batch.split_off(keep_from);
        if kept.is_empty() {
            break;
        }
        expected_height = kept[0].id.height.value();
        expected_parent = Some(kept[0].parent);
        batches.push(kept);
        if byte_capped || batch_start == 0 || batch_start <= target_floor.value() {
            break;
        }
        cursor = batch_start - 1;
    }
    let mut rows =
        VecDeque::with_capacity(batches.iter().map(Vec::len).sum::<usize>() + existing.rows.len());
    for batch in batches.into_iter().rev() {
        rows.extend(batch);
    }
    rows.extend(existing.rows.iter().cloned());
    let bytes = rows.iter().map(|row| row.payload.len()).sum();
    Ok(Some(Arc::new(EncodedTail {
        epoch: existing.epoch,
        fence: existing.fence,
        rows,
        bytes,
        byte_capped,
    })))
}

async fn encode_span<Source: TailSource>(
    source: &Source,
    span: BlockHeightRange,
    mut prior: Option<BlockId>,
) -> Result<Option<Vec<SealedBlock>>, Status> {
    let mut out =
        Vec::with_capacity(usize::try_from(span.end.value() - span.start.value() + 1).unwrap_or(0));
    let mut next = Some(span.start);
    while let Some(height) = next.filter(|height| *height <= span.end) {
        let last =
            BlockHeight::new(height.value().saturating_add(ENCODE_BATCH_BLOCKS - 1)).min(span.end);
        let Some(rows) = encode_range(source, BlockHeightRange::inclusive(height, last)).await?
        else {
            return Ok(None);
        };
        for row in rows {
            if next != Some(row.id.height)
                || row.id.height > last
                || prior.is_some_and(|id| id.hash != row.parent)
            {
                return Err(Status::data_loss("discontinuous canonical tail"));
            }
            prior = Some(row.id);
            next = row.id.height.next();
            out.push(row);
        }
    }
    Ok(Some(out))
}

impl PublishedShieldedCatalog {
    pub(super) async fn refresh_tail(&self, reader: &dyn CanonicalReader) -> Result<(), Status> {
        self.verify_source(reader).await?;
        let source = CanonicalTailSource { reader };
        let terminal = (self.bounds().end, self.terminal().segment.identity().end);
        let _ = self
            .tail
            .refresh_with_source(&source, Some(terminal), None)
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
        MockTailSource {
            epoch,
            fence,
            blocks,
            reads: Mutex::new(Vec::new()),
            unsupported: false,
        }
    }

    impl TailSource for MockTailSource {
        fn epoch_and_fence(&self) -> (ChainEpoch, Fence) {
            (self.epoch, self.fence)
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
            _max_bytes: usize,
        ) -> impl Future<Output = Result<Option<Vec<(CompactBlockArtifact, usize)>>, Status>> + Send
        {
            self.reads.lock().push(range);
            if self.unsupported {
                return std::future::ready(Ok(None));
            }
            let out: Vec<_> = (range.start.value()..=range.end.value())
                .filter_map(|height| {
                    self.blocks
                        .get(usize::try_from(height).unwrap_or(usize::MAX))
                        .cloned()
                })
                .map(|block| (block, 64))
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
        let no_cancel = CancellationToken::new();

        let first = buffer
            .refresh_with_limit(&source, None, (Some(span(8, 10)), TAIL_BYTES, &no_cancel))
            .await?
            .ok_or_else(|| eyre::eyre!("expected cold tail"))?;
        assert_eq!(first.bounds(), Some(span(8, 10)));
        assert_eq!(*source.reads.lock(), vec![span(8, 10)]);

        let repeat = buffer
            .refresh_with_limit(&source, None, (Some(span(9, 10)), TAIL_BYTES, &no_cancel))
            .await?
            .ok_or_else(|| eyre::eyre!("expected hit"))?;
        assert!(Arc::ptr_eq(&first, &repeat));
        assert_eq!(source.reads.lock().len(), 1);

        let expanded = buffer
            .refresh_with_limit(&source, None, (Some(span(5, 10)), TAIL_BYTES, &no_cancel))
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
            .refresh_with_limit(
                &source,
                None,
                (Some(span(1, 10)), capped_budget, &no_cancel),
            )
            .await?
            .ok_or_else(|| eyre::eyre!("expected byte-capped tail"))?;
        assert_eq!(capped.bounds(), Some(span(4, 10)));
        assert!(capped.byte_capped);
        assert!(capped.satisfies(source.epoch, source.fence, BlockHeight::new(1)));

        let precancelled = CancellationToken::new();
        precancelled.cancel();
        let cancelled = buffer
            .refresh_with_limit(
                &source,
                None,
                (Some(span(1, 10)), TAIL_BYTES, &precancelled),
            )
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
        let no_cancel = CancellationToken::new();
        assert!(
            buffer
                .refresh_with_limit(
                    &no_lengths,
                    None,
                    (Some(span(3, 5)), TAIL_BYTES, &no_cancel)
                )
                .await?
                .is_none()
        );

        let initial = build_source(5, 1);
        let tail_one = buffer
            .refresh_with_limit(&initial, None, (Some(span(3, 5)), TAIL_BYTES, &no_cancel))
            .await?
            .ok_or_else(|| eyre::eyre!("expected initial tail"))?;
        let extended = build_source(6, 1);
        let tail_two = buffer
            .refresh_with_limit(&extended, None, (Some(span(3, 6)), TAIL_BYTES, &no_cancel))
            .await?
            .ok_or_else(|| eyre::eyre!("expected extended tail"))?;
        assert_eq!(tail_two.bounds(), Some(span(3, 6)));
        assert_eq!(*extended.reads.lock(), vec![span(6, 6)]);
        assert_eq!(
            tail_two.rows[0].payload.as_ptr(),
            tail_one.rows[0].payload.as_ptr()
        );
        Ok(())
    }
}
