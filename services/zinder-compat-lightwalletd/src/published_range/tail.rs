use std::{collections::VecDeque, sync::Arc};

use prost::Message as _;
use tonic::Status;
use zinder_core::{BlockHeight, BlockHeightRange, BlockId, CanonicalHistoryBounds, ChainEpoch};
use zinder_query::CanonicalReader;
use zinder_store::{CanonicalEventFence, HeightOrder};

use super::PublishedShieldedCatalog;
use crate::{compact_block_messages::CompactBlockShape, sealed_history::SealedBlock};

const TAIL_BLOCKS: u32 = 5_096;
const TAIL_BYTES: usize = 64 * 1024 * 1024;

pub(super) type Fence = (CanonicalEventFence, CanonicalHistoryBounds);

pub(super) struct EncodedTail {
    pub(super) epoch: ChainEpoch,
    pub(super) fence: Fence,
    rows: VecDeque<SealedBlock>,
    bytes: usize,
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
            rows,
        })
    }

    pub(super) fn bounds(&self) -> Option<BlockHeightRange> {
        Some(BlockHeightRange::inclusive(
            self.rows.front()?.id.height,
            self.rows.back()?.id.height,
        ))
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

    async fn retained_prefix(
        &self,
        source: &dyn CanonicalReader,
        first: BlockHeight,
    ) -> Result<VecDeque<SealedBlock>, Status> {
        let epoch = source.chain_epoch();
        if epoch.network != self.epoch.network
            || epoch.artifact_schema_version != self.epoch.artifact_schema_version
            || (self.fence.0.visible_tip() == source.event_fence().visible_tip()
                && self.fence.0.sequence_digest() != source.event_fence().sequence_digest())
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
            let header = source
                .block_header_at(id.height)
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            if header.is_some_and(|header| header.block_hash == id.hash) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(self.rows.range(left.min(lo)..lo).cloned().collect())
    }

    fn push(&mut self, row: SealedBlock) {
        self.bytes += row.payload.len();
        self.rows.push_back(row);
        while self.bytes > TAIL_BYTES {
            if let Some(evicted) = self.rows.pop_front() {
                self.bytes -= evicted.payload.len();
            }
        }
    }
}

impl PublishedShieldedCatalog {
    pub(super) async fn refresh_tail(&self, source: &dyn CanonicalReader) -> Result<(), Status> {
        let epoch = source.chain_epoch();
        let fence = (source.event_fence(), source.history_bounds());
        let previous = self.tail.load_full();
        if previous
            .as_ref()
            .is_some_and(|tail| tail.epoch == epoch && tail.fence == fence)
        {
            return Ok(());
        }
        self.verify_source(source).await?;
        let start = self.bounds().end.next();
        let first = BlockHeight::new(
            epoch
                .visible_tip_height
                .value()
                .saturating_sub(TAIL_BLOCKS - 1),
        )
        .max(fence.1.first_available_height())
        .max(start.unwrap_or(epoch.visible_tip_height));
        let retained = match &previous {
            Some(prior) => prior.retained_prefix(source, first).await?,
            None => VecDeque::new(),
        };
        let mut next = retained
            .back()
            .map_or(Some(first), |row| row.id.height.next());
        let mut prior = retained
            .back()
            .map(|row| row.id)
            .or_else(|| (start == Some(first)).then(|| self.terminal().segment.identity().end));
        let mut tail = EncodedTail {
            epoch,
            fence,
            bytes: retained.iter().map(|row| row.payload.len()).sum(),
            rows: retained,
        };
        while let Some(height) =
            next.filter(|height| start.is_some() && *height <= epoch.visible_tip_height)
        {
            let last =
                BlockHeight::new(height.value().saturating_add(999)).min(epoch.visible_tip_height);
            let rows = encode_range(source, BlockHeightRange::inclusive(height, last)).await?;
            for row in rows {
                if next != Some(row.id.height)
                    || row.id.height > last
                    || prior.is_some_and(|id| id.hash != row.parent)
                {
                    return Err(Status::data_loss("discontinuous canonical tail"));
                }
                prior = Some(row.id);
                next = row.id.height.next();
                tail.push(row);
            }
        }
        if prior.is_some_and(|id| {
            id.height == epoch.visible_tip_height && id.hash != epoch.visible_tip_hash
        }) {
            return Err(Status::data_loss(
                "encoded tail differs from the visible tip",
            ));
        }
        self.tail
            .compare_and_swap(&previous, Some(std::sync::Arc::new(tail)));
        Ok(())
    }
}

async fn encode_range(
    source: &dyn CanonicalReader,
    range: BlockHeightRange,
) -> Result<Vec<SealedBlock>, Status> {
    let blocks = source
        .compact_block_rows_with_lengths(range, crate::BLOCK_RANGE_CHUNK_MAX_BYTES)
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    if blocks.is_empty() {
        return Err(Status::data_loss("missing canonical tail block"));
    }
    tokio::task::spawn_blocking(move || {
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
    .map_err(|error| Status::internal(error.to_string()))?
}
