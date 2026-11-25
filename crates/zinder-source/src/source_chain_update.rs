//! Source chain update values.
//!
//! These types are the source-crate boundary between upstream-node observations
//! and canonical ingest. Adapters map their transport-specific calls into this
//! shape before downstream code decides how to commit canonical state.

use std::num::NonZeroU32;

use zinder_core::{BlockHeight, BlockId};

use crate::SourceBlock;

/// Cursor position in the ordered source-chain feed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceChainCursor {
    position: SourceChainCursorPosition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceChainCursorPosition {
    /// No connected block has been accepted at this height or later.
    BeforeHeight(BlockHeight),
    /// The feed position is immediately after this block.
    AtBlock(BlockId),
}

/// Caller-authenticated upstream best-chain tip context for bounded historical
/// segment fetches.
///
/// Segments strictly below `tip_height - reorg_window_blocks` lie in settled
/// non-reorgable history, so adapters may resolve their range directly from
/// this bound while preserving parent-hash linkage verification and reorg
/// tip checks at or inside the reorg window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceKnownTip {
    /// Upstream best-chain height already authenticated by the caller.
    pub tip_height: BlockHeight,
    /// Reorg window depth protecting unsettled blocks at and above
    /// `tip_height - reorg_window_blocks`.
    pub reorg_window_blocks: u32,
}

impl SourceKnownTip {
    /// Creates an upstream best-chain tip context.
    #[must_use]
    pub const fn new(tip_height: BlockHeight, reorg_window_blocks: u32) -> Self {
        Self {
            tip_height,
            reorg_window_blocks,
        }
    }

    /// Returns the exclusive settled upper height strictly below
    /// `tip_height - reorg_window_blocks` when the entire requested range
    /// `[start_height, start_height + max_connected_blocks - 1]` stays strictly
    /// below that cutoff.
    ///
    /// Returns `None` on short chains (`tip_height <= reorg_window_blocks`) or
    /// when the range touches or enters the reorg window, forcing the adapter
    /// to observe the upstream tip explicitly.
    #[must_use]
    pub const fn settled_upper_bound_below_window(
        self,
        start_height: BlockHeight,
        max_connected_blocks: NonZeroU32,
    ) -> Option<BlockHeight> {
        let Some(settled_cutoff) = self
            .tip_height
            .value()
            .checked_sub(self.reorg_window_blocks)
        else {
            return None;
        };
        let Some(settled_upper_bound) = settled_cutoff.checked_sub(1) else {
            return None;
        };
        let requested_end = start_height
            .value()
            .saturating_add(max_connected_blocks.get().saturating_sub(1));
        if requested_end <= settled_upper_bound {
            Some(BlockHeight::new(settled_upper_bound))
        } else {
            None
        }
    }
}

/// Resource limits for one bounded source-chain segment request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceChainSegmentLimits {
    /// Cursor immediately before the requested segment.
    pub cursor: SourceChainCursor,
    /// Maximum connected blocks accepted from this request.
    pub max_connected_blocks: NonZeroU32,
    /// Desired response payload size for adaptive source sizing.
    pub target_response_bytes: u64,
    /// Hard response payload limit for the source adapter.
    pub max_response_bytes: u64,
    /// Optional caller-authenticated upstream tip bound for settled history.
    pub known_tip: Option<SourceKnownTip>,
}

impl SourceChainSegmentLimits {
    /// Creates source segment request limits.
    #[must_use]
    pub const fn new(
        cursor: SourceChainCursor,
        max_connected_blocks: NonZeroU32,
        target_response_bytes: u64,
        max_response_bytes: u64,
    ) -> Self {
        Self {
            cursor,
            max_connected_blocks,
            target_response_bytes,
            max_response_bytes,
            known_tip: None,
        }
    }

    /// Attaches caller-authenticated upstream tip context for skipping
    /// redundant settled-history tip polls.
    #[must_use]
    pub const fn with_known_tip(mut self, known_tip: SourceKnownTip) -> Self {
        self.known_tip = Some(known_tip);
        self
    }

    /// Attaches optional caller-authenticated upstream tip context.
    #[must_use]
    pub const fn with_optional_known_tip(mut self, known_tip: Option<SourceKnownTip>) -> Self {
        self.known_tip = known_tip;
        self
    }
}

impl SourceChainCursor {
    /// Cursor before the first non-genesis block currently committed by Zinder.
    #[must_use]
    pub const fn before_first_block() -> Self {
        Self {
            position: SourceChainCursorPosition::BeforeHeight(BlockHeight::new(1)),
        }
    }

    /// Cursor before a specific block height.
    #[must_use]
    pub const fn before_height(height: BlockHeight) -> Self {
        Self {
            position: SourceChainCursorPosition::BeforeHeight(height),
        }
    }

    /// Cursor immediately after a connected block.
    #[must_use]
    pub const fn at_block(block_id: BlockId) -> Self {
        Self {
            position: SourceChainCursorPosition::AtBlock(block_id),
        }
    }

    /// Returns the next height this cursor can connect, when representable.
    #[must_use]
    pub const fn next_connected_height(self) -> Option<BlockHeight> {
        match self.position {
            SourceChainCursorPosition::BeforeHeight(height) => Some(height),
            SourceChainCursorPosition::AtBlock(block_id) => block_id.height.next(),
        }
    }

    /// Returns the block identity this cursor follows, when it is block-anchored.
    #[must_use]
    pub const fn block_id(self) -> Option<BlockId> {
        match self.position {
            SourceChainCursorPosition::BeforeHeight(_) => None,
            SourceChainCursorPosition::AtBlock(block_id) => Some(block_id),
        }
    }

    /// Returns the crate-internal cursor position.
    pub(crate) const fn position(self) -> SourceChainCursorPosition {
        self.position
    }
}

/// One ordered source-chain update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceChainUpdate {
    /// A block connected to the source best chain.
    ConnectedBlock {
        /// Cursor after this update is applied.
        cursor: SourceChainCursor,
        /// Source-observed block payload.
        block: SourceBlock,
    },
    /// A previously connected block left the source best chain.
    RevertedBlock {
        /// Cursor after this update is applied.
        cursor: SourceChainCursor,
        /// Block identity that should no longer be treated as connected.
        block_id: BlockId,
    },
    /// Source-observed settled tip advanced (the upstream node reports a new
    /// height past its reorg window).
    SettledTip {
        /// Cursor after this update is applied.
        cursor: SourceChainCursor,
        /// Highest block the source reports as past its reorg window.
        tip_id: BlockId,
    },
}

/// Bounded ordered source-chain updates fetched in one adapter call.
///
/// Bulk catch-up uses this as an internal source-boundary optimization. The
/// update vocabulary remains [`SourceChainUpdate`], so source-specific batching
/// converges before canonical ingest sees it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SourceChainSegment {
    updates: Vec<SourceChainUpdate>,
    stats: SourceChainSegmentStats,
}

/// Resource-density sample observed while fetching a source-chain segment.
///
/// The values are advisory control-plane inputs for bulk catch-up. They do
/// not participate in canonical consensus or storage state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SourceChainSegmentStats {
    connected_blocks: u32,
    response_payload_bytes: u64,
    split_count: u32,
}

impl SourceChainSegment {
    /// Builds a segment from already ordered updates.
    #[must_use]
    pub fn new(updates: impl Into<Vec<SourceChainUpdate>>) -> Self {
        Self {
            updates: updates.into(),
            stats: SourceChainSegmentStats::default(),
        }
    }

    /// Builds a segment from connected best-chain blocks.
    #[must_use]
    pub fn connected_blocks(blocks: impl IntoIterator<Item = SourceBlock>) -> Self {
        let blocks = blocks.into_iter().collect::<Vec<_>>();
        let stats = SourceChainSegmentStats::from_connected_blocks(&blocks);
        Self::connected_blocks_with_stats(blocks, stats)
    }

    /// Builds a segment from connected best-chain blocks and adapter-observed
    /// resource-density stats.
    #[must_use]
    pub fn connected_blocks_with_stats(
        blocks: impl IntoIterator<Item = SourceBlock>,
        stats: SourceChainSegmentStats,
    ) -> Self {
        let blocks = blocks.into_iter().collect::<Vec<_>>();
        let stats = stats.with_connected_blocks(blocks.len());
        let updates = blocks
            .into_iter()
            .map(SourceChainUpdate::connected_block)
            .collect();
        Self { updates, stats }
    }

    /// Returns the ordered updates in this segment.
    #[must_use]
    pub fn updates(&self) -> &[SourceChainUpdate] {
        &self.updates
    }

    /// Returns the resource-density stats observed while fetching this segment.
    #[must_use]
    pub const fn stats(&self) -> SourceChainSegmentStats {
        self.stats
    }

    /// Consumes the segment and returns its ordered updates.
    #[must_use]
    pub fn into_updates(self) -> Vec<SourceChainUpdate> {
        self.updates
    }

    /// Returns true when no source update was available.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    /// Returns the number of updates in this segment.
    #[must_use]
    pub fn len(&self) -> usize {
        self.updates.len()
    }
}

impl SourceChainSegmentStats {
    /// Builds stats for connected blocks when the adapter did not measure
    /// transport-specific response size.
    #[must_use]
    pub fn from_connected_blocks(blocks: &[SourceBlock]) -> Self {
        let response_payload_bytes = blocks
            .iter()
            .map(source_block_payload_bytes)
            .fold(0_u64, u64::saturating_add);
        Self {
            connected_blocks: usize_to_u32_saturating(blocks.len()),
            response_payload_bytes,
            split_count: 0,
        }
    }

    /// Builds stats from an adapter-observed response payload byte count.
    #[must_use]
    pub const fn from_response_payload_bytes(response_payload_bytes: u64) -> Self {
        Self {
            connected_blocks: 0,
            response_payload_bytes,
            split_count: 0,
        }
    }

    /// Returns the number of connected blocks in the segment.
    #[must_use]
    pub const fn connected_blocks(self) -> u32 {
        self.connected_blocks
    }

    /// Returns the adapter-observed response payload byte count.
    #[must_use]
    pub const fn response_payload_bytes(self) -> u64 {
        self.response_payload_bytes
    }

    /// Returns the number of range splits required to satisfy the request.
    #[must_use]
    pub const fn split_count(self) -> u32 {
        self.split_count
    }

    /// Returns a copy with the connected block count set from the final segment.
    #[must_use]
    pub fn with_connected_blocks(mut self, connected_blocks: usize) -> Self {
        self.connected_blocks = usize_to_u32_saturating(connected_blocks);
        self
    }

    /// Returns a copy with additional split attempts recorded.
    #[must_use]
    pub fn with_added_splits(mut self, split_count: u32) -> Self {
        self.split_count = self.split_count.saturating_add(split_count);
        self
    }

    /// Returns a copy with additional response payload bytes recorded.
    #[must_use]
    pub fn with_added_response_payload_bytes(mut self, response_payload_bytes: u64) -> Self {
        self.response_payload_bytes = self
            .response_payload_bytes
            .saturating_add(response_payload_bytes);
        self
    }
}

impl SourceChainUpdate {
    /// Builds a connected-block source update.
    #[must_use]
    pub fn connected_block(block: SourceBlock) -> Self {
        let cursor = SourceChainCursor::at_block(BlockId::new(block.height, block.hash));
        Self::ConnectedBlock { cursor, block }
    }

    /// Builds a reverted-block source update.
    #[must_use]
    pub const fn reverted_block(block_id: BlockId) -> Self {
        Self::RevertedBlock {
            cursor: SourceChainCursor::before_height(block_id.height),
            block_id,
        }
    }

    /// Builds a settled-tip source update.
    #[must_use]
    pub const fn settled_tip(cursor: SourceChainCursor, tip_id: BlockId) -> Self {
        Self::SettledTip { cursor, tip_id }
    }

    /// Returns the feed cursor after this update is applied.
    #[must_use]
    pub const fn cursor(&self) -> SourceChainCursor {
        match self {
            Self::ConnectedBlock { cursor, .. }
            | Self::RevertedBlock { cursor, .. }
            | Self::SettledTip { cursor, .. } => *cursor,
        }
    }
}

fn source_block_payload_bytes(block: &SourceBlock) -> u64 {
    usize_to_u64_saturating(block.raw_block_bytes.len()).saturating_mul(2)
}

fn usize_to_u32_saturating(amount: usize) -> u32 {
    u32::try_from(amount).unwrap_or(u32::MAX)
}

fn usize_to_u64_saturating(amount: usize) -> u64 {
    u64::try_from(amount).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use zinder_core::{BlockHash, Network};

    use crate::SourceBlockHeader;

    use super::*;

    #[test]
    fn connected_block_update_advances_cursor_to_block_id() {
        let block_hash = BlockHash::from_bytes([1; 32]);
        let source_block = SourceBlock::new(
            SourceBlockHeader {
                network: Network::ZcashRegtest,
                height: BlockHeight::new(7),
                hash: block_hash,
                parent_hash: BlockHash::from_bytes([2; 32]),
                block_time_seconds: 10,
            },
            Vec::<u8>::new(),
        );

        let update = SourceChainUpdate::connected_block(source_block);

        assert_eq!(
            update.cursor(),
            SourceChainCursor::at_block(BlockId::new(BlockHeight::new(7), block_hash)),
        );
    }

    #[test]
    fn reverted_block_update_rewinds_before_reverted_height() {
        let block_id = BlockId::new(BlockHeight::new(9), BlockHash::from_bytes([3; 32]));

        let update = SourceChainUpdate::reverted_block(block_id);

        assert_eq!(
            update.cursor(),
            SourceChainCursor::before_height(BlockHeight::new(9)),
        );
    }

    #[test]
    fn connected_block_segment_preserves_update_order() {
        let first_hash = BlockHash::from_bytes([1; 32]);
        let second_hash = BlockHash::from_bytes([2; 32]);
        let first_block = SourceBlock::new(
            SourceBlockHeader {
                network: Network::ZcashRegtest,
                height: BlockHeight::new(7),
                hash: first_hash,
                parent_hash: BlockHash::from_bytes([0; 32]),
                block_time_seconds: 10,
            },
            Vec::<u8>::new(),
        );
        let second_block = SourceBlock::new(
            SourceBlockHeader {
                network: Network::ZcashRegtest,
                height: BlockHeight::new(8),
                hash: second_hash,
                parent_hash: first_hash,
                block_time_seconds: 11,
            },
            Vec::<u8>::new(),
        );

        let segment = SourceChainSegment::connected_blocks([first_block, second_block]);

        assert_eq!(segment.len(), 2);
        assert_eq!(
            segment.updates()[0].cursor(),
            SourceChainCursor::at_block(BlockId::new(BlockHeight::new(7), first_hash)),
        );
        assert_eq!(
            segment.updates()[1].cursor(),
            SourceChainCursor::at_block(BlockId::new(BlockHeight::new(8), second_hash)),
        );
    }

    #[test]
    fn connected_block_segment_records_raw_block_wire_density() {
        let block = SourceBlock::new(
            SourceBlockHeader {
                network: Network::ZcashRegtest,
                height: BlockHeight::new(7),
                hash: BlockHash::from_bytes([1; 32]),
                parent_hash: BlockHash::from_bytes([0; 32]),
                block_time_seconds: 10,
            },
            vec![0xaa, 0xbb, 0xcc],
        );

        let segment = SourceChainSegment::connected_blocks([block]);

        assert_eq!(segment.stats().connected_blocks(), 1);
        assert_eq!(segment.stats().response_payload_bytes(), 6);
    }
}
