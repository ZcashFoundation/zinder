use std::{io::ErrorKind, sync::Arc, time::Instant};

use prost::{Message, bytes::Bytes};
use tokio_util::sync::CancellationToken;
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
        limits: crate::block_range::BlockRangeStreamLimit,
    ) -> Self {
        Self {
            history,
            limits,
            shape,
            is_descending,
        }
    }

    fn encoded_span(
        &self,
        chunk: BlockHeightRange,
        order: zinder_store::HeightOrder,
        tail: &super::tail::EncodedTail,
    ) -> Option<BlockHeightRange> {
        use zinder_store::HeightOrder;
        let first = match order {
            HeightOrder::Ascending => chunk.start,
            HeightOrder::Descending => chunk.end,
        };
        let mut catalog = self.history.bounds();
        let mut live = tail.bounds();
        if let Some(suffix) = live
            && catalog.end.next().is_some_and(|next| next >= suffix.start)
        {
            catalog.end = catalog.end.max(suffix.end);
            live = None;
        }
        [Some(catalog), live]
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
        span: BlockHeightRange,
        order: zinder_store::HeightOrder,
        max_bytes: usize,
        (tail, cancel): (Option<&super::tail::EncodedTail>, &CancellationToken),
    ) -> std::io::Result<Vec<SealedBlock>> {
        use zinder_store::HeightOrder;
        if cancel.is_cancelled() {
            return Err(std::io::Error::new(
                ErrorKind::Interrupted,
                "block-range stream cancelled",
            ));
        }
        let end = self.history.bounds().end;
        if span.end <= end {
            return self
                .history
                .read_chunk(span, order, max_bytes, (true, cancel));
        }
        let tail = tail.ok_or_else(|| std::io::Error::other("missing encoded tail"))?;
        if span.start > end {
            return Ok(tail.select(span, order, max_bytes, true));
        }
        let prefix = BlockHeightRange::inclusive(span.start, end);
        let suffix = BlockHeightRange::inclusive(BlockHeight::new(end.value() + 1), span.end);
        let (mut first, first_span) = match order {
            HeightOrder::Ascending => (
                self.history
                    .read_chunk(prefix, order, max_bytes, (true, cancel))?,
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
                let mut earlier = self
                    .history
                    .read_chunk(prefix, order, room, (false, cancel))?;
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

    fn published_read_limit(&self) -> Option<&crate::block_range::BlockRangeStreamLimit> {
        Some(&self.limits)
    }

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
        if read.cancel.is_cancelled() {
            return cancelled_outcome(None);
        }
        read.max_bytes = self.limits.published_chunk_budget(read.max_bytes);
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
            let fence = reads.range_fence(view);
            // A replacement with the same settled tip can have a higher retention floor.
            let tail = self.history.tail.load_full().filter(|tail| {
                tail.epoch == epoch
                    && fence == Some(tail.fence)
                    && read.chunk.start >= tail.fence.1.first_available_height()
            });
            let anchor = self
                .resolve_anchor(
                    reads,
                    (view, fence, &read.cancel),
                    &mut read.chunk,
                    tail.as_deref(),
                )
                .await;
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
        self.spawn_encoded_read(
            (epoch, started),
            (read.chunk, read.order, read.max_bytes, read.cancel),
            tail,
            convert,
        )
        .await
    }
}

impl PublishedRangeRead {
    async fn resolve_anchor<Reads: CompactBlockReads>(
        &self,
        reads: &Reads,
        (view, fence, cancel): (&Reads::View, Option<super::tail::Fence>, &CancellationToken),
        chunk: &mut BlockHeightRange,
        tail: Option<&super::tail::EncodedTail>,
    ) -> Option<Result<(), Status>> {
        if let Some(live) = tail {
            let span = self.encoded_span(
                *chunk,
                if self.is_descending {
                    zinder_store::HeightOrder::Descending
                } else {
                    zinder_store::HeightOrder::Ascending
                },
                live,
            )?;
            *chunk = span;
            return Some(Ok(()));
        }
        if !self.covers_selected_history(*chunk) {
            return None;
        }
        let retained = match fence {
            Some((_, bounds)) => chunk.start >= bounds.first_available_height(),
            None => reads.block_hash_at(view, chunk.start).await.is_ok(),
        };
        if retained {
            Some(
                self.history
                    .terminal()
                    .check_view(reads, view, cancel)
                    .await,
            )
        } else {
            None
        }
    }

    async fn spawn_encoded_read<Output, Convert>(
        &self,
        (epoch, started): (ChainEpoch, Instant),
        (chunk, order, max_bytes, cancel): (
            BlockHeightRange,
            zinder_store::HeightOrder,
            usize,
            CancellationToken,
        ),
        tail: Option<Arc<super::tail::EncodedTail>>,
        convert: Convert,
    ) -> ChunkReadOutcome<Output>
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, Vec<ReadBlock>) -> Output + Send + 'static,
    {
        if cancel.is_cancelled() {
            return cancelled_outcome(Some(started.elapsed()));
        }
        let source = self.clone();
        tokio::task::spawn_blocking(move || {
            if cancel.is_cancelled() {
                return cancelled_outcome(Some(started.elapsed()));
            }
            let rows = source.encoded_rows(chunk, order, max_bytes, (tail.as_deref(), &cancel));
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
