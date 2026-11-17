//! Chain continuity across the chunk reads of one lightwalletd block-range
//! stream.
//!
//! A `GetBlockRange` wider than one store read is served as a sequence of
//! chunk reads. Each chunk after the first is read at the chain view current
//! when the server reads it, so a stream never holds a serving pair or a
//! compact-blocks generation while it waits for its client. A later chunk
//! continues the stream only when it links by hash to the last block the
//! stream sent. A block hash commits to its parent, so that one link proves
//! the blocks sent and the blocks to come form one chain. Otherwise a reorg
//! replaced a block the stream needs, and the stream ends with
//! `CHAIN_EPOCH_PIN_UNAVAILABLE` after the blocks it already sent. ADR-0038,
//! "Compatibility block-range streams", records the contract.

use tonic::Status;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, ChainEpoch, ChainEpochId, CompactBlockArtifact,
};
use zinder_query::{QueryError, status_from_query_error};

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
    /// Returns `UNAVAILABLE` when `blocks` is empty. A compact-block range
    /// read returns every requested height or fails, so an empty chunk means
    /// the store broke that contract.
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
    /// the visible tip ends the stream with the reorg status rather than the
    /// `NOT_FOUND` of a read above the tip.
    ///
    /// # Errors
    ///
    /// Returns `FAILED_PRECONDITION` with `CHAIN_EPOCH_PIN_UNAVAILABLE`, naming
    /// the epoch of the last chunk sent, when `next_chunk` ends above the
    /// view's visible tip.
    pub fn check_view_reaches(
        &self,
        next_chunk: BlockHeightRange,
        view: &ChainEpoch,
    ) -> Result<(), Status> {
        if next_chunk.end > view.visible_tip_height {
            return Err(self.reorg_ending(ReorgEndingCause::TipLowered, next_chunk, view.id));
        }
        Ok(())
    }

    /// Accepts `blocks`, the compact blocks of `next_chunk` in ascending
    /// height order read at `read_at`, when they continue the chain the stream
    /// has sent, and moves past them.
    ///
    /// Ascending, the chunk's lowest block must sit one height above the last
    /// block sent and name it as its parent. Descending, the chunk's highest
    /// block must sit one height below the last block sent and be its parent.
    ///
    /// # Errors
    ///
    /// Returns `FAILED_PRECONDITION` with `CHAIN_EPOCH_PIN_UNAVAILABLE`, naming
    /// the epoch of the last chunk sent, when the chunk does not link.
    pub fn advance(
        &mut self,
        next_chunk: BlockHeightRange,
        read_at: &ChainEpoch,
        blocks: &[CompactBlockArtifact],
    ) -> Result<(), Status> {
        if !self.is_continued_by(blocks) {
            return Err(self.reorg_ending(ReorgEndingCause::ChainReplaced, next_chunk, read_at.id));
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

    fn reorg_ending(
        &self,
        cause: ReorgEndingCause,
        next_chunk: BlockHeightRange,
        current_chain_epoch_id: ChainEpochId,
    ) -> Status {
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
        status_from_query_error(&QueryError::ChainEpochPinUnavailable {
            chain_epoch_id: self.sent_chain_epoch_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use tonic::Code;
    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpoch,
        ChainEpochId, ChainTipMetadata, CompactBlockArtifact, CompactChainMetadata, Network,
        UnixTimestampMillis,
    };

    use super::BlockRangeContinuity;

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

    fn assert_reorg_ending(outcome: Result<(), tonic::Status>) {
        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::FailedPrecondition)
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
        assert_reorg_ending(fork_at_last_sent.advance(
            range(11, 20),
            &view(20),
            &chain(11, 20, 2, 10),
        ));
        let mut fork_below = BlockRangeContinuity::after_first_chunk(&sent, SENT_EPOCH, false)?;
        assert_reorg_ending(fork_below.advance(range(11, 20), &view(20), &chain(11, 20, 2, 9)));
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
        assert_reorg_ending(fork_below.advance(range(11, 20), &view(30), &chain(11, 20, 2, 20)));
        Ok(())
    }

    #[test]
    fn a_chunk_at_the_wrong_height_does_not_link() -> Result<(), tonic::Status> {
        let ascending = chain(1, 10, 0, u32::MAX);
        let mut skipped = BlockRangeContinuity::after_first_chunk(&ascending, SENT_EPOCH, false)?;
        assert_reorg_ending(skipped.advance(range(12, 20), &view(20), &chain(12, 20, 0, u32::MAX)));

        let descending = chain(21, 30, 0, u32::MAX);
        let mut overlapping =
            BlockRangeContinuity::after_first_chunk(&descending, SENT_EPOCH, true)?;
        assert_reorg_ending(overlapping.advance(
            range(12, 21),
            &view(30),
            &chain(12, 21, 0, u32::MAX),
        ));

        // A block that names the last sent block as its parent but claims
        // another height does not link either.
        let mut misnumbered =
            BlockRangeContinuity::after_first_chunk(&ascending, SENT_EPOCH, false)?;
        assert_reorg_ending(misnumbered.advance(
            range(11, 11),
            &view(20),
            &[block_at(12, block_hash(12, 0), block_hash(10, 0))],
        ));
        let mut misnumbered_parent =
            BlockRangeContinuity::after_first_chunk(&descending, SENT_EPOCH, true)?;
        assert_reorg_ending(misnumbered_parent.advance(
            range(20, 20),
            &view(30),
            &[block_at(19, block_hash(20, 0), block_hash(18, 0))],
        ));

        let mut empty = BlockRangeContinuity::after_first_chunk(&ascending, SENT_EPOCH, false)?;
        assert_reorg_ending(empty.advance(range(11, 20), &view(20), &[]));
        Ok(())
    }

    #[test]
    fn a_view_below_the_next_chunk_ends_the_stream_naming_the_sent_epoch()
    -> Result<(), tonic::Status> {
        let continuity =
            BlockRangeContinuity::after_first_chunk(&chain(1, 10, 0, u32::MAX), SENT_EPOCH, false)?;
        continuity.check_view_reaches(range(11, 20), &view(20))?;

        let Err(status) = continuity.check_view_reaches(range(11, 20), &view(19)) else {
            return Err(tonic::Status::internal(
                "a view below the chunk must end the stream",
            ));
        };
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(
            status.message().contains("ChainEpochId(4)"),
            "the ending must name the sent epoch: {}",
            status.message()
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
}
