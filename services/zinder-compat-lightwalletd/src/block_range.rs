//! Chunk planning, chain continuity, and the concurrency limit of lightwalletd
//! block-range streams.
//!
//! A `GetBlockRange` is served as a sequence of chunk reads. A chunk holds at
//! most 1,000 blocks and at most [`BLOCK_RANGE_CHUNK_MAX_BYTES`] of stored
//! compact blocks, or one block when that block alone is larger, so a range
//! of large blocks is read and sent in smaller chunks. Each chunk after the
//! first is read at the chain view current when the server reads it, so a
//! stream never holds a serving pair or a compact-blocks generation while it
//! waits for its client. A later chunk continues the stream only when it
//! links by hash to the last block the stream sent. A block hash commits to
//! its parent, so that one link proves the blocks sent and the blocks to come
//! form one chain. Otherwise a reorg replaced a block the stream needs, and
//! the stream ends after the blocks it already sent with the code lightwalletd
//! returns for the same case: `ABORTED` for a broken link, and `OUT_OF_RANGE`
//! when the visible tip fell below a height still to send. ADR-0038,
//! "Compatibility block-range streams", records the contract.
//!
//! A stream holds one slot of a process-wide [`BlockRangeStreamLimit`] from
//! its admission until its response is dropped and every chunk read of the
//! stream has finished, so the number of slots bounds how many streams hold chunks
//! at once.

use std::{
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::sync::{
    OwnedSemaphorePermit, Semaphore,
    mpsc::{self, error::SendError},
};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::Status;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, ChainEpoch, ChainEpochId, CompactBlockArtifact,
};
use zinder_query::DEFAULT_MAX_COMPACT_BLOCK_RANGE;
use zinder_store::HeightOrder;

/// Stored compact-block bytes one block-range chunk keeps at most.
///
/// A chunk keeps one block even when that block alone is larger. The stored
/// encoding is the whole compact block before pool filtering, which is what a
/// chunk read materializes, so `GetBlockRangeNullifiers` reads the same chunks
/// as `GetBlockRange`.
pub const BLOCK_RANGE_CHUNK_MAX_BYTES: usize = 2 * 1024 * 1024;

/// Default for the process-wide limit on concurrent `GetBlockRange` and
/// `GetBlockRangeNullifiers` streams.
pub const DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS: NonZeroU32 =
    NonZeroU32::MIN.saturating_add(99);

/// Plans the chunk reads of one block-range stream in wire order.
///
/// [`Self::next_chunk`] offers up to 1,000 heights at the end of the
/// remaining range that the stream sends first. A byte-bounded read keeps a
/// run of them from that end, and [`Self::advance`] moves past the blocks it
/// kept, so the next chunk starts at the next height to send.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockRangeChunks {
    /// Heights no chunk has covered yet, or `None` once every height has.
    remaining: Option<BlockHeightRange>,
    is_descending: bool,
}

impl BlockRangeChunks {
    /// Plans the chunks of `range` in the order the stream sends them.
    #[must_use]
    pub const fn new(range: BlockHeightRange, is_descending: bool) -> Self {
        let remaining = if range.start.value() <= range.end.value() {
            Some(range)
        } else {
            None
        };
        Self {
            remaining,
            is_descending,
        }
    }

    /// Returns whether a chunk remains to be read.
    #[must_use]
    pub const fn has_remaining(&self) -> bool {
        self.remaining.is_some()
    }

    /// Returns the height order of the stream, which every chunk read walks.
    #[must_use]
    pub const fn order(&self) -> HeightOrder {
        if self.is_descending {
            HeightOrder::Descending
        } else {
            HeightOrder::Ascending
        }
    }

    /// Returns the heights the next chunk read may cover: up to 1,000 at the
    /// end of the remaining range that the stream sends first.
    #[must_use]
    pub fn next_chunk(&self) -> Option<BlockHeightRange> {
        let remaining = self.remaining?;
        let span = DEFAULT_MAX_COMPACT_BLOCK_RANGE.get().saturating_sub(1);
        Some(if self.is_descending {
            let start = remaining
                .end
                .value()
                .saturating_sub(span)
                .max(remaining.start.value());
            BlockHeightRange::inclusive(BlockHeight::new(start), remaining.end)
        } else {
            let end = remaining
                .start
                .value()
                .saturating_add(span)
                .min(remaining.end.value());
            BlockHeightRange::inclusive(remaining.start, BlockHeight::new(end))
        })
    }

    /// Moves past `read_blocks` heights at the end of the remaining range
    /// that the stream sends first.
    ///
    /// A read that kept no block leaves the plan unchanged. Its caller must
    /// end the stream, as the continuity check of an empty chunk does.
    pub fn advance(&mut self, read_blocks: usize) {
        let Some(remaining) = self.remaining else {
            return;
        };
        let span = remaining.end.value() - remaining.start.value();
        match u32::try_from(read_blocks) {
            Ok(0) => {}
            Ok(read_blocks) if read_blocks <= span => {
                self.remaining = Some(if self.is_descending {
                    BlockHeightRange::inclusive(
                        remaining.start,
                        BlockHeight::new(remaining.end.value() - read_blocks),
                    )
                } else {
                    BlockHeightRange::inclusive(
                        BlockHeight::new(remaining.start.value() + read_blocks),
                        remaining.end,
                    )
                });
            }
            _ => self.remaining = None,
        }
    }
}

/// Gauge of the block-range streams that hold a slot.
const OPEN_STREAMS_GAUGE: &str = "zinder_compat_block_range_streams_open";

/// Process-wide limit on concurrent `GetBlockRange` and
/// `GetBlockRangeNullifiers` streams.
///
/// Clones share one limit. A stream takes a slot after its request passes
/// validation. It returns the slot when the stream ends, fails, or the client
/// goes away, and no chunk read of the stream is still running.
#[derive(Clone, Debug)]
pub struct BlockRangeStreamLimit {
    slots: Arc<Semaphore>,
    max_streams: NonZeroU32,
}

impl BlockRangeStreamLimit {
    /// Creates a limit that admits at most `max_streams` streams at once.
    #[must_use]
    pub fn new(max_streams: NonZeroU32) -> Self {
        let permits = usize::try_from(max_streams.get())
            .unwrap_or(Semaphore::MAX_PERMITS)
            .min(Semaphore::MAX_PERMITS);
        // Publishes the gauge before the first stream opens. Setting it to
        // zero would discard the slots that other limits in the process hold.
        metrics::gauge!(OPEN_STREAMS_GAUGE).increment(0.0);
        Self {
            slots: Arc::new(Semaphore::new(permits)),
            max_streams,
        }
    }

    /// Returns the most streams this limit admits at once.
    #[must_use]
    pub const fn max_streams(&self) -> NonZeroU32 {
        self.max_streams
    }

    /// Takes a slot for one new stream.
    ///
    /// # Errors
    ///
    /// Returns `RESOURCE_EXHAUSTED` when every slot is taken, and counts the
    /// refusal in `zinder_compat_block_range_stream_refusals_total`.
    pub fn try_open(&self) -> Result<BlockRangeStreamSlot, Status> {
        let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() else {
            metrics::counter!("zinder_compat_block_range_stream_refusals_total").increment(1);
            return Err(Status::resource_exhausted(format!(
                "this server already serves its limit of {} concurrent block-range streams",
                self.max_streams
            )));
        };
        Ok(BlockRangeStreamSlot {
            _open_stream: Arc::new(OpenStream::new(permit)),
        })
    }
}

/// A taken slot, counted in `zinder_compat_block_range_streams_open` until it
/// is dropped.
///
/// The gauge moves by one on each open and close rather than taking a count,
/// so concurrent opens and closes cannot leave it at a stale value.
#[derive(Debug)]
struct OpenStream {
    _permit: OwnedSemaphorePermit,
}

impl OpenStream {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        metrics::gauge!(OPEN_STREAMS_GAUGE).increment(1.0);
        Self { _permit: permit }
    }
}

impl Drop for OpenStream {
    fn drop(&mut self) {
        metrics::gauge!(OPEN_STREAMS_GAUGE).decrement(1.0);
    }
}

/// A block-range response: the blocks a stream sends, in wire order.
pub type BlockRangeResponse<Message> = Pin<Box<dyn Stream<Item = Message> + Send + 'static>>;

/// One stream's slot of a [`BlockRangeStreamLimit`].
///
/// The slot returns to the limit once every holder has dropped it: the
/// response, the [`BlockRangeSink`] of the stream's reader for a stream of
/// more than one chunk, and a read run with [`Self::hold_during`] that has
/// not finished.
#[derive(Clone, Debug)]
pub struct BlockRangeStreamSlot {
    _open_stream: Arc<OpenStream>,
}

impl BlockRangeStreamSlot {
    /// Runs `read` on a task of its own that holds this slot until `read`
    /// has finished and its output is dropped, and returns that output.
    ///
    /// A store read runs on the blocking pool, which finishes it even after
    /// its caller is cancelled. A client that cancels its request during
    /// `read` therefore cannot return the slot while `read` still holds the
    /// chunk it reads.
    ///
    /// # Errors
    ///
    /// Returns the error of `read`, or `UNAVAILABLE` when the task that runs
    /// `read` panics.
    pub async fn hold_during<Output, Read>(&self, read: Read) -> Result<Output, Status>
    where
        Output: Send + 'static,
        Read: Future<Output = Result<Output, Status>> + Send + 'static,
    {
        let stream_slot = self.clone();
        let (outcome, _stream_slot) = tokio::spawn(async move { (read.await, stream_slot) })
            .await
            .map_err(|error| Status::unavailable(format!("block-range read failed: {error}")))?;
        outcome
    }

    /// Returns a response that sends `messages` and holds this slot until it
    /// is dropped.
    #[must_use]
    pub fn hold_for<Message: Send + 'static>(
        self,
        messages: Vec<Message>,
    ) -> BlockRangeResponse<Message> {
        self.hold_for_stream(Box::pin(tokio_stream::iter(messages)))
    }

    /// Returns a response that sends `first_messages` and then what `reader`
    /// queues, at most `capacity` messages ahead of the client, and spawns
    /// `reader` to read the later chunks.
    ///
    /// The response and the reader's sink each hold this slot, so the reader
    /// keeps the slot while it reads, even after the client has gone away.
    #[must_use]
    pub fn hold_with_reader<Message, Reader>(
        self,
        first_messages: Vec<Message>,
        capacity: usize,
        reader: impl FnOnce(BlockRangeSink<Message>) -> Reader,
    ) -> BlockRangeResponse<Message>
    where
        Message: Send + 'static,
        Reader: Future<Output = ()> + Send + 'static,
    {
        let (messages, queued_messages) = mpsc::channel(capacity);
        tokio::spawn(reader(BlockRangeSink {
            messages,
            _stream_slot: self.clone(),
        }));
        self.hold_for_stream(Box::pin(
            tokio_stream::iter(first_messages).chain(ReceiverStream::new(queued_messages)),
        ))
    }

    fn hold_for_stream<Message: 'static>(
        self,
        stream: BlockRangeResponse<Message>,
    ) -> BlockRangeResponse<Message> {
        Box::pin(SlotHoldingStream {
            stream,
            _slot: self,
        })
    }
}

/// Where the reader of a stream's later chunks queues their messages.
///
/// It holds the stream's slot until the reader drops it.
#[derive(Debug)]
pub struct BlockRangeSink<Message> {
    messages: mpsc::Sender<Message>,
    _stream_slot: BlockRangeStreamSlot,
}

impl<Message> BlockRangeSink<Message> {
    /// Queues `message` for the client, waiting while the queue is full.
    ///
    /// # Errors
    ///
    /// Returns the message when the client has dropped the response.
    pub async fn send(&self, message: Message) -> Result<(), SendError<Message>> {
        self.messages.send(message).await
    }
}

struct SlotHoldingStream<S> {
    stream: S,
    _slot: BlockRangeStreamSlot,
}

impl<S: Stream + Unpin> Stream for SlotHoldingStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<S::Item>> {
        Pin::new(&mut self.stream).poll_next(context)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// The last block a lightwalletd block-range stream sent, and the chain view
/// it was read at.
///
/// Both compatibility serving modes check every chunk after the first
/// against this value, so they end a stream for the same reasons and record
/// the same metric and log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockRangeContinuity {
    is_descending: bool,
    last_sent_height: BlockHeight,
    /// An ascending chunk must name this hash as its first block's parent.
    last_sent_hash: BlockHash,
    /// A descending chunk must end with the block that has this hash.
    last_sent_parent_hash: BlockHash,
    /// Epoch of the view the last sent chunk was read at.
    sent_chain_epoch_id: ChainEpochId,
}

/// Why a block-range stream ended before its last requested height.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReorgEndingCause {
    /// The next chunk does not link by hash to the last block sent.
    ChainReplaced,
    /// The current view's visible tip is below a height still to send.
    TipLowered,
}

impl ReorgEndingCause {
    const fn label(self) -> &'static str {
        match self {
            Self::ChainReplaced => "chain_replaced",
            Self::TipLowered => "tip_lowered",
        }
    }
}

impl BlockRangeContinuity {
    /// Returns the continuity after the stream's first chunk.
    ///
    /// `blocks` are the chunk's compact blocks in ascending height order, read
    /// at `chain_epoch_id`. The stream sends them in its own direction, so the
    /// last block sent is the highest when ascending and the lowest when
    /// descending.
    ///
    /// # Errors
    ///
    /// Returns `UNAVAILABLE` when `blocks` is empty. A chunk read keeps at
    /// least one block or fails, so an empty chunk means the store broke that
    /// contract.
    pub fn after_first_chunk(
        blocks: &[CompactBlockArtifact],
        chain_epoch_id: ChainEpochId,
        is_descending: bool,
    ) -> Result<Self, Status> {
        let last_sent = if is_descending {
            blocks.first()
        } else {
            blocks.last()
        };
        let last_sent = last_sent.ok_or_else(|| {
            Status::unavailable("compact-block range read returned no blocks for its chunk")
        })?;
        Ok(Self {
            is_descending,
            last_sent_height: last_sent.height(),
            last_sent_hash: last_sent.block_hash(),
            last_sent_parent_hash: last_sent.previous_block_hash(),
            sent_chain_epoch_id: chain_epoch_id,
        })
    }

    /// Checks that `view` still serves every height of `next_chunk`.
    ///
    /// Call it before reading `next_chunk` at `view`, so a reorg that lowered
    /// the visible tip ends the stream with lightwalletd's `OUT_OF_RANGE`
    /// rather than the `NOT_FOUND` of a read above the tip.
    ///
    /// # Errors
    ///
    /// Returns `OUT_OF_RANGE` when `next_chunk` ends above the view's visible
    /// tip.
    pub fn check_view_reaches(
        &self,
        next_chunk: BlockHeightRange,
        view: &ChainEpoch,
    ) -> Result<(), Status> {
        if next_chunk.end > view.visible_tip_height {
            self.record_reorg_ending(ReorgEndingCause::TipLowered, next_chunk, view.id);
            return Err(Status::out_of_range(format!(
                "block {} is newer than latest block {} after a reorg",
                next_chunk.end.value(),
                view.visible_tip_height.value(),
            )));
        }
        Ok(())
    }

    /// Accepts `blocks`, the compact blocks a read of `next_chunk` at
    /// `read_at` kept from the end the stream sends first, in ascending height
    /// order, when they continue the chain the stream has sent, and moves past
    /// them.
    ///
    /// Ascending, the chunk's lowest block must sit one height above the last
    /// block sent and name it as its parent. Descending, the chunk's highest
    /// block must sit one height below the last block sent and be its parent.
    ///
    /// # Errors
    ///
    /// Returns `ABORTED` when the chunk does not link.
    pub fn advance(
        &mut self,
        next_chunk: BlockHeightRange,
        read_at: &ChainEpoch,
        blocks: &[CompactBlockArtifact],
    ) -> Result<(), Status> {
        if !self.is_continued_by(blocks) {
            self.record_reorg_ending(ReorgEndingCause::ChainReplaced, next_chunk, read_at.id);
            return Err(Status::aborted(
                "block-range stream ended at a chain discontinuity: a reorg replaced the chain it \
                 was sending",
            ));
        }
        if read_at.id != self.sent_chain_epoch_id {
            tracing::debug!(
                target: "zinder::compat_lightwalletd",
                event = "compat_block_range_continued_at_newer_epoch",
                last_sent_height = self.last_sent_height.value(),
                sent_chain_epoch = self.sent_chain_epoch_id.value(),
                current_chain_epoch = read_at.id.value(),
                next_chunk_start = next_chunk.start.value(),
                next_chunk_end = next_chunk.end.value(),
                "block-range stream continued at a newer chain view"
            );
        }
        *self = Self::after_first_chunk(blocks, read_at.id, self.is_descending)?;
        Ok(())
    }

    fn is_continued_by(&self, blocks: &[CompactBlockArtifact]) -> bool {
        if self.is_descending {
            blocks.last().is_some_and(|next| {
                next.height().next() == Some(self.last_sent_height)
                    && next.block_hash() == self.last_sent_parent_hash
            })
        } else {
            blocks.first().is_some_and(|next| {
                self.last_sent_height.next() == Some(next.height())
                    && next.previous_block_hash() == self.last_sent_hash
            })
        }
    }

    fn record_reorg_ending(
        &self,
        cause: ReorgEndingCause,
        next_chunk: BlockHeightRange,
        current_chain_epoch_id: ChainEpochId,
    ) {
        metrics::counter!(
            "zinder_compat_block_range_reorg_endings_total",
            "cause" => cause.label()
        )
        .increment(1);
        tracing::info!(
            target: "zinder::compat_lightwalletd",
            event = "compat_block_range_ended_by_reorg",
            cause = cause.label(),
            last_sent_height = self.last_sent_height.value(),
            sent_chain_epoch = self.sent_chain_epoch_id.value(),
            current_chain_epoch = current_chain_epoch_id.value(),
            next_chunk_start = next_chunk.start.value(),
            next_chunk_end = next_chunk.end.value(),
            "block-range stream ended because a reorg replaced the chain it was sending"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use tonic::Code;
    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpoch,
        ChainEpochId, ChainTipMetadata, CompactBlockArtifact, CompactChainMetadata, Network,
        UnixTimestampMillis,
    };

    use zinder_store::HeightOrder;

    use super::{BlockRangeChunks, BlockRangeContinuity, BlockRangeStreamLimit};

    const SENT_EPOCH: ChainEpochId = ChainEpochId::new(4);
    const CURRENT_EPOCH: ChainEpochId = ChainEpochId::new(5);

    /// Hash of the block at `height` on `branch`; branch 0 is the entry chain.
    fn block_hash(height: u32, branch: u8) -> BlockHash {
        let mut bytes = [0_u8; 32];
        bytes[..4].copy_from_slice(&height.to_le_bytes());
        bytes[4] = branch;
        BlockHash::from_bytes(bytes)
    }

    fn block_at(height: u32, hash: BlockHash, parent_hash: BlockHash) -> CompactBlockArtifact {
        CompactBlockArtifact::empty(
            BlockId::new(BlockHeight::new(height), hash),
            parent_hash,
            0,
            CompactChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )
    }

    /// Blocks `start..=end` in ascending order of the chain that follows
    /// branch 0 below `fork_height` and `branch` from it.
    fn chain(start: u32, end: u32, branch: u8, fork_height: u32) -> Vec<CompactBlockArtifact> {
        let branch_at = |height: u32| if height >= fork_height { branch } else { 0 };
        (start..=end)
            .map(|height| {
                let parent_height = height.saturating_sub(1);
                block_at(
                    height,
                    block_hash(height, branch_at(height)),
                    block_hash(parent_height, branch_at(parent_height)),
                )
            })
            .collect()
    }

    fn view(visible_tip_height: u32) -> ChainEpoch {
        ChainEpoch {
            id: CURRENT_EPOCH,
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(visible_tip_height),
            visible_tip_hash: block_hash(visible_tip_height, 0),
            settled_tip_height: BlockHeight::new(1),
            settled_tip_hash: block_hash(1, 0),
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            created_at: UnixTimestampMillis::new(0),
        }
    }

    fn range(start: u32, end: u32) -> BlockHeightRange {
        BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end))
    }

    fn assert_chain_discontinuity(outcome: Result<(), tonic::Status>) {
        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::Aborted)
        );
    }

    #[test]
    fn ascending_continues_only_from_the_last_sent_block() -> Result<(), tonic::Status> {
        let sent = chain(1, 10, 0, u32::MAX);
        let mut continuity = BlockRangeContinuity::after_first_chunk(&sent, SENT_EPOCH, false)?;

        // A fork just above the last sent block keeps the sent chain.
        continuity.advance(range(11, 20), &view(30), &chain(11, 20, 1, 11))?;
        // The next chunk must follow the new last block, not the first chunk's.
        continuity.advance(range(21, 30), &view(30), &chain(21, 30, 1, 11))?;

        let mut fork_at_last_sent =
            BlockRangeContinuity::after_first_chunk(&sent, SENT_EPOCH, false)?;
        assert_chain_discontinuity(fork_at_last_sent.advance(
            range(11, 20),
            &view(20),
            &chain(11, 20, 2, 10),
        ));
        let mut fork_below = BlockRangeContinuity::after_first_chunk(&sent, SENT_EPOCH, false)?;
        assert_chain_discontinuity(fork_below.advance(
            range(11, 20),
            &view(20),
            &chain(11, 20, 2, 9),
        ));
        Ok(())
    }

    #[test]
    fn descending_continues_only_into_the_parent_of_the_last_sent_block()
    -> Result<(), tonic::Status> {
        // Sent 30 down to 21: the last block sent is 21.
        let sent = chain(21, 30, 0, u32::MAX);
        let mut continuity = BlockRangeContinuity::after_first_chunk(&sent, SENT_EPOCH, true)?;

        // A fork at the last sent height keeps its parent, so the stream
        // continues on the chain it has sent.
        continuity.advance(range(11, 20), &view(30), &chain(11, 20, 1, 21))?;
        continuity.advance(range(1, 10), &view(30), &chain(1, 10, 1, 21))?;

        let mut fork_below = BlockRangeContinuity::after_first_chunk(&sent, SENT_EPOCH, true)?;
        assert_chain_discontinuity(fork_below.advance(
            range(11, 20),
            &view(30),
            &chain(11, 20, 2, 20),
        ));
        Ok(())
    }

    #[test]
    fn a_chunk_at_the_wrong_height_does_not_link() -> Result<(), tonic::Status> {
        let ascending = chain(1, 10, 0, u32::MAX);
        let mut skipped = BlockRangeContinuity::after_first_chunk(&ascending, SENT_EPOCH, false)?;
        assert_chain_discontinuity(skipped.advance(
            range(12, 20),
            &view(20),
            &chain(12, 20, 0, u32::MAX),
        ));

        let descending = chain(21, 30, 0, u32::MAX);
        let mut overlapping =
            BlockRangeContinuity::after_first_chunk(&descending, SENT_EPOCH, true)?;
        assert_chain_discontinuity(overlapping.advance(
            range(12, 21),
            &view(30),
            &chain(12, 21, 0, u32::MAX),
        ));

        // A block that names the last sent block as its parent but claims
        // another height does not link either.
        let mut misnumbered =
            BlockRangeContinuity::after_first_chunk(&ascending, SENT_EPOCH, false)?;
        assert_chain_discontinuity(misnumbered.advance(
            range(11, 11),
            &view(20),
            &[block_at(12, block_hash(12, 0), block_hash(10, 0))],
        ));
        let mut misnumbered_parent =
            BlockRangeContinuity::after_first_chunk(&descending, SENT_EPOCH, true)?;
        assert_chain_discontinuity(misnumbered_parent.advance(
            range(20, 20),
            &view(30),
            &[block_at(19, block_hash(20, 0), block_hash(18, 0))],
        ));

        let mut empty = BlockRangeContinuity::after_first_chunk(&ascending, SENT_EPOCH, false)?;
        assert_chain_discontinuity(empty.advance(range(11, 20), &view(20), &[]));
        Ok(())
    }

    #[test]
    fn a_view_below_the_next_chunk_ends_the_stream_out_of_range() -> Result<(), tonic::Status> {
        let continuity =
            BlockRangeContinuity::after_first_chunk(&chain(1, 10, 0, u32::MAX), SENT_EPOCH, false)?;
        continuity.check_view_reaches(range(11, 20), &view(20))?;

        let outcome = continuity.check_view_reaches(range(11, 20), &view(19));
        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::OutOfRange)
        );
        Ok(())
    }

    #[test]
    fn an_empty_first_chunk_is_unavailable() {
        let outcome = BlockRangeContinuity::after_first_chunk(&[], SENT_EPOCH, false);
        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::Unavailable)
        );
    }

    /// Plans every chunk of `chunks`, assuming each read keeps the whole
    /// chunk it was offered.
    fn drain(chunks: &mut BlockRangeChunks) -> Vec<BlockHeightRange> {
        std::iter::from_fn(|| {
            let chunk = chunks.next_chunk()?;
            chunks.advance(chunk.into_iter().len());
            Some(chunk)
        })
        .collect()
    }

    #[test]
    fn chunks_hold_at_most_one_store_read_and_preserve_direction() {
        let mut ascending = BlockRangeChunks::new(range(1, 2_500), false);
        assert_eq!(ascending.order(), HeightOrder::Ascending);
        assert_eq!(
            drain(&mut ascending),
            vec![range(1, 1_000), range(1_001, 2_000), range(2_001, 2_500)]
        );
        assert!(!ascending.has_remaining());

        let mut descending = BlockRangeChunks::new(range(1, 2_500), true);
        assert_eq!(descending.order(), HeightOrder::Descending);
        assert_eq!(
            drain(&mut descending),
            vec![range(1_501, 2_500), range(501, 1_500), range(1, 500)]
        );

        let mut single = BlockRangeChunks::new(range(7, 7), true);
        assert_eq!(drain(&mut single), vec![range(7, 7)]);
        let mut top = BlockRangeChunks::new(range(u32::MAX - 1, u32::MAX), false);
        assert_eq!(drain(&mut top), vec![range(u32::MAX - 1, u32::MAX)]);
        let mut whole = BlockRangeChunks::new(range(0, u32::MAX), true);
        assert_eq!(whole.next_chunk(), Some(range(u32::MAX - 999, u32::MAX)));
        whole.advance(usize::MAX);
        assert!(!whole.has_remaining());
    }

    #[test]
    fn the_next_chunk_starts_after_the_blocks_a_read_kept() {
        let mut ascending = BlockRangeChunks::new(range(1, 3_000), false);
        assert_eq!(ascending.next_chunk(), Some(range(1, 1_000)));
        // A byte-bounded read kept 120 of the 1,000 heights it was offered.
        ascending.advance(120);
        assert_eq!(ascending.next_chunk(), Some(range(121, 1_120)));
        ascending.advance(1);
        assert_eq!(ascending.next_chunk(), Some(range(122, 1_121)));
        // A read that kept nothing leaves the plan where it was.
        ascending.advance(0);
        assert_eq!(ascending.next_chunk(), Some(range(122, 1_121)));
        ascending.advance(2_878);
        assert_eq!(ascending.next_chunk(), Some(range(3_000, 3_000)));
        ascending.advance(1);
        assert_eq!(ascending.next_chunk(), None);

        let mut descending = BlockRangeChunks::new(range(1, 3_000), true);
        assert_eq!(descending.next_chunk(), Some(range(2_001, 3_000)));
        descending.advance(120);
        assert_eq!(descending.next_chunk(), Some(range(1_881, 2_880)));
        descending.advance(2_880);
        assert!(!descending.has_remaining());
    }

    #[test]
    fn the_stream_limit_refuses_past_its_slots_until_one_is_returned() -> Result<(), tonic::Status>
    {
        let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN.saturating_add(1));
        let response_side = limit.try_open()?;
        let other_stream = limit.try_open()?;
        assert_eq!(
            limit.try_open().err().map(|status| status.code()),
            Some(Code::ResourceExhausted)
        );

        // A slot shared by a response and its reader returns only when both
        // are dropped.
        let reader_side = response_side.clone();
        drop(response_side);
        assert!(limit.try_open().is_err());
        drop(reader_side);
        let reopened = limit.try_open()?;
        drop((reopened, other_stream));
        Ok(())
    }

    #[test]
    fn the_open_streams_gauge_counts_each_slot_until_its_last_holder_drops_it()
    -> Result<(), tonic::Status> {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let open_streams = || {
            handle.render().lines().find_map(|line| {
                line.strip_prefix("zinder_compat_block_range_streams_open ")?
                    .parse::<f64>()
                    .ok()
            })
        };
        metrics::with_local_recorder(&recorder, || {
            let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN.saturating_add(1));
            assert_eq!(open_streams(), Some(0.0));
            let response_side = limit.try_open()?;
            let other_stream = limit.try_open()?;
            let reader_side = response_side.clone();
            // A limit created while streams are open leaves their count alone.
            let _other_limit = BlockRangeStreamLimit::new(NonZeroU32::MIN);
            assert_eq!(open_streams(), Some(2.0));

            drop((response_side, other_stream));
            assert_eq!(open_streams(), Some(1.0));
            drop(reader_side);
            assert_eq!(open_streams(), Some(0.0));
            Ok(())
        })
    }
}
