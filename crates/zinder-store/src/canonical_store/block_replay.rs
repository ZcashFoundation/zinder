use std::{
    borrow::Cow,
    collections::VecDeque,
    time::{Duration, Instant},
};

use rust_rocksdb::{DB, ReadOptions};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFacts,
    CanonicalBlockFactsDigest, CanonicalBlockFactsDigestVersion, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion,
    CanonicalBlockReplayFormatVersion, ValidatedCanonicalBlockReplay,
    wire::decode_height_key_ascending,
};

use super::{
    CanonicalBlockLoadEvidence, CanonicalSequenceCheckpoint, CanonicalStoreError,
    CanonicalStoreReadyEvidence,
    block_load::encode_block_position,
    construction_manifest::CanonicalConstructionFamilyEvidence,
    rocksdb::{
        LIVE_TAIL_BLOCK_COLUMN_FAMILY, LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY,
        SETTLED_SEGMENT_INDEX_COLUMN_FAMILY,
    },
    settled_segments::{
        layout::SettledSegmentIndexEntry,
        store_access::{self, SegmentStoreContext},
    },
};

/// Maximum rows returned by one incremental replay-range scan.
///
/// Projectors page retained events and apply committed ranges one at a time.
/// This ceiling prevents a broad event or caller request from turning the
/// incremental path back into an unbounded historical scan.
pub const MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS: u32 = 4_096;

const MAX_SETTLED_REPLAY_BATCH_BLOCKS: u32 = 255;

/// One cache-bypassing, forward scan of the READY canonical replay state.
///
/// Every yielded block is decoded and semantically validated across settled
/// external segments and the unsealed live tail. Reaching the end also
/// authenticates the exact first block, visible tip, current block count, parent
/// links, every block-local replay digest, and the complete visible sequence
/// digest recorded by canonical publication.
pub struct CanonicalReplayScan<'a> {
    ctx: SegmentStoreContext<'a>,
    remaining_range: Option<BlockHeightRange>,
    buffered: VecDeque<(BlockHeight, ValidatedCanonicalBlockReplay)>,
    ready_evidence: CanonicalStoreReadyEvidence,
    previous_block: Option<(BlockHeight, BlockHash)>,
    observed_block_count: u64,
    sequence_digest_builder: Option<CanonicalBlockFactsSequenceDigestBuilder>,
    finished: bool,
}

/// Summary of a complete canonical replay reconstructed from disjoint slices.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservedCanonicalReplaySequence {
    /// First block observed across the stitched slices.
    pub first_block: BlockId,
    /// Last block observed across the stitched slices.
    pub tip: BlockId,
    /// Total blocks observed across the stitched slices.
    pub block_count: u64,
    /// Ordered sequence digest computed across every block in height order.
    pub sequence_digest: CanonicalBlockFactsSequenceDigest,
}

/// Read-only `Sync` handle for scanning disjoint height slices of one READY
/// canonical replay fence concurrently.
///
/// Each [`CanonicalReplaySliceScan`] decodes and verifies its slice's internal
/// contiguity and parent links across settled segments and the unsealed live
/// tail without populating the `RocksDB` block cache. Callers that reconstruct a
/// full history across multiple slices must verify the stitched boundaries and
/// call [`Self::verify_complete_sequence`] before trusting the scan as a
/// complete READY replay.
#[derive(Clone, Copy)]
pub struct CanonicalReplaySliceSource<'a> {
    ctx: SegmentStoreContext<'a>,
    ready_evidence: CanonicalStoreReadyEvidence,
}

impl<'a> CanonicalReplaySliceSource<'a> {
    pub(super) const fn new(
        ctx: SegmentStoreContext<'a>,
        ready_evidence: &CanonicalStoreReadyEvidence,
    ) -> Self {
        Self {
            ctx,
            ready_evidence: *ready_evidence,
        }
    }

    /// Returns the admitted READY evidence pinned by this slice source.
    #[must_use]
    pub const fn ready_evidence(&self) -> CanonicalStoreReadyEvidence {
        self.ready_evidence
    }

    /// Opens one cache-bypassing forward scan over `range`.
    ///
    /// `range` must be non-empty and lie within the admitted retained range.
    pub fn scan_slice(
        &self,
        range: BlockHeightRange,
    ) -> Result<CanonicalReplaySliceScan<'a>, CanonicalStoreError> {
        CanonicalReplaySliceScan::new(self.ctx, &self.ready_evidence, range)
    }

    /// Verifies that a stitched set of slices matches the complete READY fence
    /// and that no replay rows or settled segments exist past the visible tip.
    pub fn verify_complete_sequence(
        &self,
        observed: ObservedCanonicalReplaySequence,
    ) -> Result<(), CanonicalStoreError> {
        let expected_block_count = expected_visible_block_count(&self.ready_evidence)?;
        if observed.first_block != self.ready_evidence.first_retained_block {
            return Err(CanonicalStoreError::block_replay_sequence(
                "replay scan first block does not match READY",
            ));
        }
        if self.ready_evidence.visible_block_count != expected_block_count
            || observed.block_count != expected_block_count
        {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "READY visible range contains {expected_block_count} blocks but replay scan observed {}",
                observed.block_count
            )));
        }
        if observed.tip != self.ready_evidence.visible_tip {
            return Err(CanonicalStoreError::block_replay_sequence(
                "replay scan tip does not match READY",
            ));
        }
        if observed.sequence_digest.as_bytes() != self.ready_evidence.visible_sequence_digest {
            return Err(CanonicalStoreError::block_replay_sequence(
                "replay scan sequence digest does not match READY",
            ));
        }
        verify_no_rows_past_tip(self.ctx, self.ready_evidence.visible_tip.height)
    }
}

fn verify_no_rows_past_tip(
    ctx: SegmentStoreContext<'_>,
    visible_tip_height: BlockHeight,
) -> Result<(), CanonicalStoreError> {
    if let Some(after_tip) = visible_tip_height.next() {
        check_family_has_no_rows_from(ctx.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY, after_tip)?;
        check_family_has_no_rows_from(ctx.db, LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY, after_tip)?;
    }
    let settled = match ctx.sealed_segments {
        Some(entries) => Cow::Borrowed(entries),
        None => Cow::Owned(store_access::read_all_settled_segment_entries(ctx.db)?),
    };
    if let Some(last) = settled.last()
        && last.end_block.height > visible_tip_height
    {
        return Err(CanonicalStoreError::block_replay_sequence(
            "settled segment index extends beyond the visible fence",
        ));
    }
    Ok(())
}

fn check_family_has_no_rows_from(
    db: &DB,
    family_name: &'static str,
    from_height: BlockHeight,
) -> Result<(), CanonicalStoreError> {
    let family = db.cf_handle(family_name).ok_or_else(|| {
        CanonicalStoreError::block_replay_sequence(format!("{family_name} column family is absent"))
    })?;
    let mut iterator = db.raw_iterator_cf_opt(&family, store_access::uncached_read_options());
    iterator.seek(encode_block_position(from_height));
    iterator
        .status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "canonical replay fence check",
            source,
        })?;
    if iterator.valid() {
        return Err(CanonicalStoreError::block_replay_sequence(
            "replay scan contains rows beyond the visible fence",
        ));
    }
    Ok(())
}

/// One cache-bypassing forward scan over a height-bounded slice of replay blocks.
///
/// Every yielded row is decoded, version-checked, height-checked, and linked to
/// its immediate predecessor within the slice. Reaching `None` verifies that
/// every height in the requested slice was present with no gaps or trailing
/// rows.
pub struct CanonicalReplaySliceScan<'a> {
    ctx: SegmentStoreContext<'a>,
    remaining_range: Option<BlockHeightRange>,
    buffered: VecDeque<(BlockHeight, ValidatedCanonicalBlockReplay)>,
    ready_evidence: CanonicalStoreReadyEvidence,
    range: BlockHeightRange,
    expected_block_count: u64,
    previous_block: Option<(BlockHeight, BlockHash)>,
    observed_block_count: u64,
    finished: bool,
}

impl<'a> CanonicalReplaySliceScan<'a> {
    fn new(
        ctx: SegmentStoreContext<'a>,
        ready_evidence: &CanonicalStoreReadyEvidence,
        range: BlockHeightRange,
    ) -> Result<Self, CanonicalStoreError> {
        if range.start > range.end {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "requested replay slice {}..={} must not be empty",
                range.start.value(),
                range.end.value(),
            )));
        }
        if range.start < ready_evidence.first_retained_block.height
            || range.end > ready_evidence.visible_tip.height
        {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "requested replay slice {}..={} is outside admitted canonical range {}..={}",
                range.start.value(),
                range.end.value(),
                ready_evidence.first_retained_block.height.value(),
                ready_evidence.visible_tip.height.value(),
            )));
        }
        let expected_block_count = u64::from(range.end.value() - range.start.value()) + 1;
        Ok(Self {
            ctx,
            remaining_range: Some(range),
            buffered: VecDeque::new(),
            ready_evidence: *ready_evidence,
            range,
            expected_block_count,
            previous_block: None,
            observed_block_count: 0,
            finished: false,
        })
    }

    fn finish_slice(&self) -> Result<(), CanonicalStoreError> {
        if self.observed_block_count != self.expected_block_count {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "replay slice {}..={} contains {} blocks but observed {}",
                self.range.start.value(),
                self.range.end.value(),
                self.expected_block_count,
                self.observed_block_count,
            )));
        }
        let Some((last_height, last_hash)) = self.previous_block else {
            return Err(CanonicalStoreError::block_replay_sequence(
                "replay slice observed no blocks",
            ));
        };
        if last_height != self.range.end {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "replay slice ended at height {} instead of {}",
                last_height.value(),
                self.range.end.value(),
            )));
        }
        if self.range.end == self.ready_evidence.visible_tip.height
            && last_hash != self.ready_evidence.visible_tip.hash
        {
            return Err(CanonicalStoreError::block_replay_sequence(
                "replay scan tip does not match READY",
            ));
        }
        Ok(())
    }

    fn validate_slice_position(
        &self,
        height: BlockHeight,
        block_hash: BlockHash,
        parent_hash: BlockHash,
    ) -> Result<(), CanonicalStoreError> {
        match self.previous_block {
            None if height == self.range.start
                && (height != self.ready_evidence.first_retained_block.height
                    || block_hash == self.ready_evidence.first_retained_block.hash) =>
            {
                Ok(())
            }
            Some((previous_height, previous_hash))
                if previous_height.next() == Some(height)
                    && height <= self.range.end
                    && parent_hash == previous_hash =>
            {
                Ok(())
            }
            None | Some(_) => Err(CanonicalStoreError::block_replay_sequence(format!(
                "replay slice observed an unexpected block at height {}",
                height.value()
            ))),
        }
    }
}

impl Iterator for CanonicalReplaySliceScan<'_> {
    type Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let next_block =
            next_buffered_replay(self.ctx, &mut self.remaining_range, &mut self.buffered);
        let (height, replay) = match next_block {
            Ok(Some(block)) => block,
            Ok(None) => {
                self.finished = true;
                return self.finish_slice().err().map(Err);
            }
            Err(err) => {
                self.finished = true;
                return Some(Err(err));
            }
        };
        let validated = (|| {
            let facts = replay.facts();
            self.validate_slice_position(
                height,
                facts.block_header.block_hash,
                facts.block_header.parent_hash,
            )?;
            self.observed_block_count =
                self.observed_block_count.checked_add(1).ok_or_else(|| {
                    CanonicalStoreError::block_replay_sequence(
                        "replay slice count exceeds u64::MAX",
                    )
                })?;
            if self.observed_block_count > self.expected_block_count {
                return Err(CanonicalStoreError::block_replay_sequence(
                    "replay slice contains rows beyond its upper bound",
                ));
            }
            self.previous_block = Some((height, facts.block_header.block_hash));
            Ok(replay)
        })();
        if validated.is_err() {
            self.finished = true;
        }
        Some(validated)
    }
}

fn expected_visible_block_count(
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<u64, CanonicalStoreError> {
    let distance = ready_evidence
        .visible_tip
        .height
        .value()
        .checked_sub(ready_evidence.first_retained_block.height.value())
        .ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence(
                "READY visible range cannot produce a block count",
            )
        })?;
    Ok(u64::from(distance) + 1)
}

/// Bounded, connected replay rows inside one admitted canonical fence.
///
/// Unlike [`CanonicalReplayScan`], this iterator does not authenticate the
/// complete historical prefix. The secondary or primary must already have
/// admitted its READY publication. Every requested row is still decoded,
/// version-checked, height-checked, and connected to the preceding canonical
/// row so an incremental projection never accepts a gap or detached suffix.
pub struct CanonicalReplayRangeScan<'a> {
    ctx: SegmentStoreContext<'a>,
    remaining_range: Option<BlockHeightRange>,
    buffered: VecDeque<(BlockHeight, ValidatedCanonicalBlockReplay)>,
    previous_hash: Option<BlockHash>,
    first_retained_block: BlockId,
    finished: bool,
}

impl<'a> CanonicalReplayRangeScan<'a> {
    pub(super) fn new(
        ctx: SegmentStoreContext<'a>,
        ready_evidence: &CanonicalStoreReadyEvidence,
        range: BlockHeightRange,
    ) -> Result<Self, CanonicalStoreError> {
        validate_incremental_replay_range_bound(range)?;
        if range.start <= range.end
            && (range.start < ready_evidence.first_retained_block.height
                || range.end > ready_evidence.visible_tip.height)
        {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "requested replay range {}..={} is outside admitted canonical range {}..={}",
                range.start.value(),
                range.end.value(),
                ready_evidence.first_retained_block.height.value(),
                ready_evidence.visible_tip.height.value(),
            )));
        }
        let previous_hash = if range.start <= range.end
            && range.start > ready_evidence.first_retained_block.height
        {
            let predecessor_height = BlockHeight::new(range.start.value() - 1);
            Some(
                read_persisted_replay(ctx, predecessor_height)?
                    .replay
                    .facts()
                    .block_header
                    .block_hash,
            )
        } else {
            None
        };
        Ok(Self {
            ctx,
            remaining_range: (range.start <= range.end).then_some(range),
            buffered: VecDeque::new(),
            previous_hash,
            first_retained_block: ready_evidence.first_retained_block,
            finished: false,
        })
    }
}

pub(super) fn validate_incremental_replay_range_bound(
    range: BlockHeightRange,
) -> Result<(), CanonicalStoreError> {
    let requested_block_count =
        (u64::from(range.end.value()) + 1).saturating_sub(u64::from(range.start.value()));
    if requested_block_count > u64::from(MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS) {
        return Err(CanonicalStoreError::block_replay_sequence(format!(
            "requested replay range has {requested_block_count} blocks; maximum is {MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS}"
        )));
    }
    Ok(())
}

impl Iterator for CanonicalReplayRangeScan<'_> {
    type Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let (height, replay) =
            match next_buffered_replay(self.ctx, &mut self.remaining_range, &mut self.buffered) {
                Ok(Some(block)) => block,
                Ok(None) => {
                    self.finished = true;
                    return None;
                }
                Err(err) => {
                    self.finished = true;
                    return Some(Err(err));
                }
            };
        let validated = (|| {
            let header = &replay.facts().block_header;
            match self.previous_hash {
                Some(previous_hash) if header.parent_hash == previous_hash => {}
                None if height == self.first_retained_block.height
                    && header.block_hash == self.first_retained_block.hash => {}
                Some(_) | None => {
                    return Err(CanonicalStoreError::block_replay_sequence(format!(
                        "requested replay range is detached at height {}",
                        height.value()
                    )));
                }
            }
            self.previous_hash = Some(header.block_hash);
            Ok(replay)
        })();
        if validated.is_err() {
            self.finished = true;
        }
        Some(validated)
    }
}

impl<'a> CanonicalReplayScan<'a> {
    pub(super) fn new(
        ctx: SegmentStoreContext<'a>,
        ready_evidence: &CanonicalStoreReadyEvidence,
    ) -> Result<Self, CanonicalStoreError> {
        let _ = expected_visible_block_count(ready_evidence)?;
        let range = BlockHeightRange::inclusive(
            ready_evidence.first_retained_block.height,
            ready_evidence.visible_tip.height,
        );
        Ok(Self {
            ctx,
            remaining_range: Some(range),
            buffered: VecDeque::new(),
            ready_evidence: *ready_evidence,
            previous_block: None,
            observed_block_count: 0,
            sequence_digest_builder: Some(CanonicalBlockFactsSequenceDigestBuilder::new(
                CanonicalBlockFactsSequenceDigestVersion::V1,
            )),
            finished: false,
        })
    }

    fn finish_scan(&mut self) -> Result<(), CanonicalStoreError> {
        let expected_block_count = expected_visible_block_count(&self.ready_evidence)?;
        if self.ready_evidence.visible_block_count != expected_block_count
            || self.observed_block_count != expected_block_count
        {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "READY visible range contains {expected_block_count} blocks but replay scan observed {}",
                self.observed_block_count
            )));
        }
        let observed_tip = self.previous_block;
        let expected_tip = (
            self.ready_evidence.visible_tip.height,
            self.ready_evidence.visible_tip.hash,
        );
        if observed_tip != Some(expected_tip) {
            return Err(CanonicalStoreError::block_replay_sequence(
                "replay scan tip does not match READY",
            ));
        }
        let digest_builder = self.sequence_digest_builder.take().ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence(
                "canonical replay scan sequence digest was already finalized",
            )
        })?;
        if digest_builder.finish().as_bytes() != self.ready_evidence.visible_sequence_digest {
            return Err(CanonicalStoreError::block_replay_sequence(
                "replay scan sequence digest does not match READY",
            ));
        }
        Ok(())
    }

    fn validate_replay_position(
        &self,
        height: BlockHeight,
        block_hash: BlockHash,
        parent_hash: BlockHash,
    ) -> Result<(), CanonicalStoreError> {
        match self.previous_block {
            None if height == self.ready_evidence.first_retained_block.height
                && block_hash == self.ready_evidence.first_retained_block.hash =>
            {
                Ok(())
            }
            Some((previous_height, previous_hash))
                if previous_height.next() == Some(height) && parent_hash == previous_hash =>
            {
                Ok(())
            }
            None | Some(_) => Err(CanonicalStoreError::block_replay_sequence(format!(
                "replay scan observed an unexpected block at height {}",
                height.value()
            ))),
        }
    }
}

impl Iterator for CanonicalReplayScan<'_> {
    type Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let (height, replay) =
            match next_buffered_replay(self.ctx, &mut self.remaining_range, &mut self.buffered) {
                Ok(Some(block)) => block,
                Ok(None) => {
                    self.finished = true;
                    return self.finish_scan().err().map(Err);
                }
                Err(err) => {
                    self.finished = true;
                    return Some(Err(err));
                }
            };
        let validated = (|| {
            let facts = replay.facts();
            self.validate_replay_position(
                height,
                facts.block_header.block_hash,
                facts.block_header.parent_hash,
            )?;
            self.observed_block_count =
                self.observed_block_count.checked_add(1).ok_or_else(|| {
                    CanonicalStoreError::block_replay_sequence("replay scan count exceeds u64::MAX")
                })?;
            if self.observed_block_count > self.ready_evidence.visible_block_count {
                return Err(CanonicalStoreError::block_replay_sequence(
                    "replay scan contains rows beyond the visible fence",
                ));
            }
            let digest_builder = self.sequence_digest_builder.as_mut().ok_or_else(|| {
                CanonicalStoreError::block_replay_sequence(
                    "canonical replay scan sequence digest was finalized early",
                )
            })?;
            digest_builder
                .try_append(replay.reference_digest())
                .map_err(|source| CanonicalStoreError::block_replay_sequence(source.to_string()))?;
            self.previous_block = Some((height, facts.block_header.block_hash));
            Ok(replay)
        })();
        if validated.is_err() {
            self.finished = true;
        }
        Some(validated)
    }
}

fn next_buffered_replay(
    ctx: SegmentStoreContext<'_>,
    remaining_range: &mut Option<BlockHeightRange>,
    buffered: &mut VecDeque<(BlockHeight, ValidatedCanonicalBlockReplay)>,
) -> Result<Option<(BlockHeight, ValidatedCanonicalBlockReplay)>, CanonicalStoreError> {
    if let Some(front) = buffered.pop_front() {
        return Ok(Some(front));
    }
    let Some(range) = *remaining_range else {
        return Ok(None);
    };
    if let Some(entry) =
        store_access::resolve_settled_segment_for_height(ctx.db, ctx.sealed_segments, range.start)?
    {
        let batch_limit = BlockHeight::new(
            range
                .start
                .value()
                .saturating_add(MAX_SETTLED_REPLAY_BATCH_BLOCKS - 1),
        );
        let batch_end = range.end.min(entry.end_block.height).min(batch_limit);
        let readers = store_access::open_replay_segment_readers(ctx.db, ctx.segment_cache, &entry)?;
        let rows = store_access::reconstruct_replay_range_from_segment(
            ctx.db,
            &readers,
            BlockHeightRange::inclusive(range.start, batch_end),
        )?;
        *remaining_range = batch_end
            .next()
            .filter(|next| *next <= range.end)
            .map(|start| BlockHeightRange::inclusive(start, range.end));
        buffered.extend(rows);
        return Ok(buffered.pop_front());
    }
    let encoded = store_access::read_live_tail_raw_bytes_with_options(
        ctx.db,
        range.start,
        &store_access::uncached_read_options(),
    )?
    .ok_or_else(|| {
        CanonicalStoreError::block_replay_sequence(format!(
            "canonical replay at height {} is absent",
            range.start.value()
        ))
    })?;
    let step = store_access::replay_step_from_live_tail_bytes(range.start, &encoded)?;
    validate_replay_versions(step.replay.format_version(), step.replay.reference_digest())?;
    *remaining_range = range
        .start
        .next()
        .filter(|next| *next <= range.end)
        .map(|start| BlockHeightRange::inclusive(start, range.end));
    Ok(Some((range.start, step.replay)))
}

/// Cache-bypassing semantic proof of the persisted replay state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PersistedBlockReplayEvidence {
    pub(super) first_height: BlockHeight,
    pub(super) first_parent_hash: BlockHash,
    pub(super) first_hash: BlockHash,
    pub(super) tip_height: BlockHeight,
    pub(super) tip_hash: BlockHash,
    pub(super) block_count: u64,
    pub(super) settled_segment_index_count: u64,
    pub(super) settled_segment_index_logical_bytes: u64,
    pub(super) live_tail_block_count: u64,
    pub(super) live_tail_block_logical_bytes: u64,
    pub(super) logical_replay_bytes: u64,
    pub(super) replay_format_version: CanonicalBlockReplayFormatVersion,
    pub(super) block_digest_version: CanonicalBlockFactsDigestVersion,
    pub(super) sequence_digest_version: CanonicalBlockFactsSequenceDigestVersion,
    pub(super) sequence_digest: CanonicalBlockFactsSequenceDigest,
    pub(super) settled_segment_index_evidence: CanonicalConstructionFamilyEvidence,
    pub(super) live_tail_block_evidence: CanonicalConstructionFamilyEvidence,
    pub(super) elapsed: Duration,
}

pub(super) struct PersistedBlockReplayValidation {
    pub(super) evidence: PersistedBlockReplayEvidence,
    pub(super) retained_sequence_checkpoints: VecDeque<CanonicalSequenceCheckpoint>,
}

/// Decodes every persisted replay row without populating the block cache.
#[cfg(test)]
pub(super) fn validate_persisted_block_replays(
    ctx: SegmentStoreContext<'_>,
) -> Result<PersistedBlockReplayEvidence, CanonicalStoreError> {
    Ok(validate_persisted_block_replays_with_checkpoints(ctx, 0)?.evidence)
}

pub(super) fn validate_persisted_block_replays_with_checkpoints(
    ctx: SegmentStoreContext<'_>,
    retained_checkpoint_count: usize,
) -> Result<PersistedBlockReplayValidation, CanonicalStoreError> {
    let started_at = Instant::now();
    let mut state = ReplayValidationState::new(retained_checkpoint_count);
    let (
        settled_segment_index_count,
        settled_segment_index_logical_bytes,
        settled_segment_index_evidence,
    ) = validate_settled_segments_replay(ctx, &mut state)?;
    let (live_tail_block_count, live_tail_block_logical_bytes, live_tail_block_evidence) =
        validate_live_tail_replay(ctx.db, &mut state)?;
    let finished = state.sequence.map(ReplaySequence::finish).ok_or_else(|| {
        CanonicalStoreError::block_replay_sequence(
            "persisted canonical replay state must not be empty",
        )
    })?;
    let evidence = PersistedBlockReplayEvidence {
        first_height: finished.first_height,
        first_parent_hash: finished.first_parent_hash,
        first_hash: finished.first_hash,
        tip_height: finished.tip_height,
        tip_hash: finished.tip_hash,
        block_count: finished.block_count,
        settled_segment_index_count,
        settled_segment_index_logical_bytes,
        live_tail_block_count,
        live_tail_block_logical_bytes,
        logical_replay_bytes: finished.logical_replay_bytes,
        replay_format_version: CanonicalBlockReplayFormatVersion::V1,
        block_digest_version: CanonicalBlockFactsDigestVersion::V1,
        sequence_digest_version: CanonicalBlockFactsSequenceDigestVersion::V1,
        sequence_digest: finished.sequence_digest,
        settled_segment_index_evidence,
        live_tail_block_evidence,
        elapsed: started_at.elapsed(),
    };
    Ok(PersistedBlockReplayValidation {
        evidence,
        retained_sequence_checkpoints: state.retained_checkpoints,
    })
}

fn validate_settled_segments_replay(
    ctx: SegmentStoreContext<'_>,
    state: &mut ReplayValidationState,
) -> Result<(u64, u64, CanonicalConstructionFamilyEvidence), CanonicalStoreError> {
    let settled_family = ctx
        .db
        .cf_handle(SETTLED_SEGMENT_INDEX_COLUMN_FAMILY)
        .ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence(
                "settled_segment_index column family is absent",
            )
        })?;
    let mut iterator = ctx
        .db
        .raw_iterator_cf_opt(&settled_family, store_access::uncached_read_options());
    iterator.seek_to_first();
    let mut family_evidence =
        CanonicalConstructionFamilyEvidence::accumulator(SETTLED_SEGMENT_INDEX_COLUMN_FAMILY);
    let mut count = 0_u64;
    let mut logical_bytes = 0_u64;
    while iterator.valid() {
        let Some((key, encoded)) = iterator.item() else {
            break;
        };
        family_evidence.observe(key, encoded)?;
        let row_bytes = checked_key_value_bytes(key.len(), encoded.len())?;
        logical_bytes = logical_bytes.checked_add(row_bytes).ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence(
                "settled_segment_index logical bytes exceed u64::MAX",
            )
        })?;
        count = count.checked_add(1).ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence("settled segment count exceeds u64::MAX")
        })?;
        iterator.next();
    }
    iterator
        .status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "settled_segment_index readback iteration",
            source,
        })?;
    let entries = store_access::read_all_settled_segment_entries(ctx.db)?;
    if let Some(expected) = ctx.sealed_segments
        && expected != entries.as_slice()
    {
        return Err(CanonicalStoreError::block_replay_sequence(
            "settled_segment_index differs from cold-admitted segment list",
        ));
    }
    for entry in &entries {
        validate_single_settled_segment_replay(ctx, entry, state)?;
    }
    Ok((count, logical_bytes, family_evidence.finish()))
}

fn validate_single_settled_segment_replay(
    ctx: SegmentStoreContext<'_>,
    entry: &SettledSegmentIndexEntry,
    state: &mut ReplayValidationState,
) -> Result<(), CanonicalStoreError> {
    let readers = store_access::open_replay_segment_readers(ctx.db, ctx.segment_cache, entry)?;
    let mut next_height = Some(entry.start_height);
    while let Some(chunk_start) = next_height.filter(|height| *height <= entry.end_block.height) {
        let limit = BlockHeight::new(
            chunk_start
                .value()
                .saturating_add(MAX_SETTLED_REPLAY_BATCH_BLOCKS - 1),
        );
        let chunk_end = entry.end_block.height.min(limit);
        let rows = store_access::reconstruct_replay_range_from_segment(
            ctx.db,
            &readers,
            BlockHeightRange::inclusive(chunk_start, chunk_end),
        )?;
        for (height, replay) in rows {
            validate_replay_versions(replay.format_version(), replay.reference_digest())?;
            let step = store_access::replay_step_from_validated(replay)?;
            let facts = step.replay.facts();
            state.append(ReplaySequenceRow {
                height,
                block_hash: facts.block_header.block_hash,
                parent_hash: facts.block_header.parent_hash,
                reference_digest: step.replay.reference_digest(),
                replay_bytes: step.logical_replay_bytes,
            })?;
        }
        next_height = chunk_end.next();
    }
    let observed = state
        .sequence
        .as_ref()
        .map(ReplaySequence::sequence_checkpoint);
    if observed != Some(entry.end_sequence_checkpoint) {
        return Err(CanonicalStoreError::block_replay_sequence(format!(
            "settled segment {}..={} sequence checkpoint mismatch",
            entry.start_height.value(),
            entry.end_block.height.value(),
        )));
    }
    Ok(())
}

fn validate_live_tail_replay(
    db: &DB,
    state: &mut ReplayValidationState,
) -> Result<(u64, u64, CanonicalConstructionFamilyEvidence), CanonicalStoreError> {
    let live_tail_family = db.cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY).ok_or_else(|| {
        CanonicalStoreError::block_replay_sequence("live_tail_block column family is absent")
    })?;
    let mut iterator =
        db.raw_iterator_cf_opt(&live_tail_family, store_access::uncached_read_options());
    iterator.seek_to_first();
    let mut family_evidence =
        CanonicalConstructionFamilyEvidence::accumulator(LIVE_TAIL_BLOCK_COLUMN_FAMILY);
    let mut count = 0_u64;
    let mut logical_bytes = 0_u64;
    while iterator.valid() {
        let Some((key, encoded)) = iterator.item() else {
            break;
        };
        let height = decode_height_key_ascending(key).map_err(|source| {
            CanonicalStoreError::BlockReplayKeyInvalid {
                reason: source.to_string(),
            }
        })?;
        family_evidence.observe(key, encoded)?;
        let row_bytes = checked_key_value_bytes(key.len(), encoded.len())?;
        logical_bytes = logical_bytes.checked_add(row_bytes).ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence(
                "live_tail_block logical bytes exceed u64::MAX",
            )
        })?;
        let step = store_access::replay_step_from_live_tail_bytes(height, encoded)?;
        validate_replay_versions(step.replay.format_version(), step.replay.reference_digest())?;
        let facts = step.replay.facts();
        state.append(ReplaySequenceRow {
            height,
            block_hash: facts.block_header.block_hash,
            parent_hash: facts.block_header.parent_hash,
            reference_digest: step.replay.reference_digest(),
            replay_bytes: step.logical_replay_bytes,
        })?;
        count = count.checked_add(1).ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence("live_tail_block count exceeds u64::MAX")
        })?;
        iterator.next();
    }
    iterator
        .status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "live_tail_block readback iteration",
            source,
        })?;
    Ok((count, logical_bytes, family_evidence.finish()))
}

fn checked_key_value_bytes(key_len: usize, value_len: usize) -> Result<u64, CanonicalStoreError> {
    key_len
        .checked_add(value_len)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence(
                "replay family key and value bytes exceed u64::MAX",
            )
        })
}

pub(super) fn resume_persisted_sequence_checkpoint(
    ctx: SegmentStoreContext<'_>,
    checkpoint: CanonicalSequenceCheckpoint,
    through: BlockHeight,
    maximum_replay_blocks: u32,
) -> Result<CanonicalSequenceCheckpoint, CanonicalStoreError> {
    let replay_block_count = through
        .value()
        .checked_sub(checkpoint.through().height.value())
        .ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence(
                "sequence checkpoint cannot resume backwards",
            )
        })?;
    if replay_block_count > maximum_replay_blocks {
        return Err(CanonicalStoreError::block_replay_sequence(format!(
            "sequence checkpoint tail has {replay_block_count} blocks; maximum is {maximum_replay_blocks}"
        )));
    }
    let mut resumed = checkpoint;
    while let Some(height) = resumed
        .through()
        .height
        .next()
        .filter(|height| *height <= through)
    {
        resumed = super::replay_tail::authenticate_next_replay(ctx, resumed, height)?;
    }
    Ok(resumed)
}

pub(super) fn read_replay_facts_at_with_cache(
    ctx: SegmentStoreContext<'_>,
    ready_evidence: &CanonicalStoreReadyEvidence,
    height: BlockHeight,
) -> Result<Option<CanonicalBlockFacts>, CanonicalStoreError> {
    if height < ready_evidence.first_retained_block.height
        || height > ready_evidence.visible_tip.height
    {
        return Ok(None);
    }
    Ok(Some(
        read_persisted_replay(ctx, height)?.replay.into_facts(),
    ))
}

/// Replay rows one batched lookup of [`visit_replay_facts_at_heights_with_cache`] holds
/// at once.
const REPLAY_FACTS_BATCH_ROWS: usize = 32;

/// Visits the canonical facts at several heights in height order.
pub(super) fn visit_replay_facts_at_heights_with_cache<E>(
    ctx: SegmentStoreContext<'_>,
    ready_evidence: &CanonicalStoreReadyEvidence,
    heights: &[BlockHeight],
    mut visit: impl FnMut(usize, Option<CanonicalBlockFacts>) -> Result<(), E>,
) -> Result<(), E>
where
    E: From<CanonicalStoreError>,
{
    let mut retained = Vec::with_capacity(heights.len());
    for (index, height) in heights.iter().enumerate() {
        if *height < ready_evidence.first_retained_block.height
            || *height > ready_evidence.visible_tip.height
        {
            visit(index, None)?;
        } else {
            retained.push((*height, index));
        }
    }
    retained.sort_unstable();
    let uncached = store_access::uncached_read_options();
    let mut live_batch = Vec::with_capacity(REPLAY_FACTS_BATCH_ROWS);
    let mut cursor = 0;
    while cursor < retained.len() {
        let (height, _) = retained[cursor];
        if let Some(entry) =
            store_access::resolve_settled_segment_for_height(ctx.db, ctx.sealed_segments, height)?
        {
            if !live_batch.is_empty() {
                visit_live_tail_facts_batch(ctx.db, &live_batch, &uncached, &mut visit)?;
                live_batch.clear();
            }
            let readers =
                store_access::open_replay_segment_readers(ctx.db, ctx.segment_cache, &entry)?;
            while cursor < retained.len() && retained[cursor].0 <= entry.end_block.height {
                let (target_height, orig_index) = retained[cursor];
                let replay =
                    store_access::reconstruct_replay_from_segment(ctx.db, &readers, target_height)?;
                visit(orig_index, Some(replay.into_facts()))?;
                cursor += 1;
            }
        } else {
            live_batch.push(retained[cursor]);
            cursor += 1;
            if live_batch.len() >= REPLAY_FACTS_BATCH_ROWS {
                visit_live_tail_facts_batch(ctx.db, &live_batch, &uncached, &mut visit)?;
                live_batch.clear();
            }
        }
    }
    if !live_batch.is_empty() {
        visit_live_tail_facts_batch(ctx.db, &live_batch, &uncached, &mut visit)?;
    }
    Ok(())
}

fn visit_live_tail_facts_batch<E>(
    db: &DB,
    batch: &[(BlockHeight, usize)],
    read_options: &ReadOptions,
    visit: &mut impl FnMut(usize, Option<CanonicalBlockFacts>) -> Result<(), E>,
) -> Result<(), E>
where
    E: From<CanonicalStoreError>,
{
    let batch_heights = batch.iter().map(|(height, _)| *height).collect::<Vec<_>>();
    let rows = store_access::fetch_live_tail_batch_pinned(db, &batch_heights, read_options)?;
    for ((height, orig_index), row) in batch.iter().zip(rows) {
        let slice = row?.ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence(format!(
                "canonical replay at height {} is absent",
                height.value()
            ))
        })?;
        let step = store_access::replay_step_from_live_tail_bytes(*height, &slice)?;
        validate_replay_versions(step.replay.format_version(), step.replay.reference_digest())?;
        visit(*orig_index, Some(step.replay.into_facts()))?;
    }
    Ok(())
}

#[derive(Debug)]
pub(in crate::canonical_store) struct PersistedReplayStep {
    pub(in crate::canonical_store) replay: zinder_core::ValidatedCanonicalBlockReplay,
    pub(in crate::canonical_store) logical_replay_bytes: u64,
}

pub(super) fn read_persisted_replay(
    ctx: SegmentStoreContext<'_>,
    height: BlockHeight,
) -> Result<PersistedReplayStep, CanonicalStoreError> {
    store_access::read_replay_step_split(ctx, height)
}

impl PersistedBlockReplayEvidence {
    pub(super) fn has_same_sequence(&self, prepared: &CanonicalBlockLoadEvidence) -> bool {
        self.first_height == prepared.first_height
            && self.first_parent_hash == prepared.first_parent_hash
            && self.first_hash == prepared.first_hash
            && self.tip_height == prepared.tip_height
            && self.tip_hash == prepared.tip_hash
            && self.block_count == prepared.block_count
            && self.settled_segment_index_count == prepared.settled_segment_index_count
            && self.settled_segment_index_logical_bytes
                == prepared.settled_segment_index_logical_bytes
            && self.live_tail_block_count == prepared.live_tail_block_count
            && self.live_tail_block_logical_bytes == prepared.live_tail_block_logical_bytes
            && self.logical_replay_bytes == prepared.logical_replay_bytes
            && self.replay_format_version == prepared.replay_format_version
            && self.block_digest_version == prepared.block_digest_version
            && self.sequence_digest_version == prepared.sequence_digest_version
            && self.sequence_digest == prepared.sequence_digest
    }
}

struct ReplayValidationState {
    sequence: Option<ReplaySequence>,
    retained_checkpoints: VecDeque<CanonicalSequenceCheckpoint>,
    retained_checkpoint_count: usize,
}

impl ReplayValidationState {
    const fn new(retained_checkpoint_count: usize) -> Self {
        Self {
            sequence: None,
            retained_checkpoints: VecDeque::new(),
            retained_checkpoint_count,
        }
    }

    fn append(&mut self, row: ReplaySequenceRow) -> Result<(), CanonicalStoreError> {
        if let Some(existing) = self.sequence.as_mut() {
            existing.append(row)?;
        } else {
            self.sequence = Some(ReplaySequence::new(row)?);
        }
        if self.retained_checkpoint_count > 0
            && let Some(active) = self.sequence.as_ref()
        {
            self.retained_checkpoints
                .push_back(active.sequence_checkpoint());
            if self.retained_checkpoints.len() > self.retained_checkpoint_count {
                self.retained_checkpoints.pop_front();
            }
        }
        Ok(())
    }
}

struct ReplaySequence {
    first_height: BlockHeight,
    first_parent_hash: BlockHash,
    first_hash: BlockHash,
    tip_height: BlockHeight,
    tip_hash: BlockHash,
    block_count: u64,
    logical_replay_bytes: u64,
    digest_builder: CanonicalBlockFactsSequenceDigestBuilder,
}

#[derive(Clone, Copy)]
struct ReplaySequenceRow {
    height: BlockHeight,
    block_hash: BlockHash,
    parent_hash: BlockHash,
    reference_digest: CanonicalBlockFactsDigest,
    replay_bytes: u64,
}

struct FinishedReplaySequence {
    first_height: BlockHeight,
    first_parent_hash: BlockHash,
    first_hash: BlockHash,
    tip_height: BlockHeight,
    tip_hash: BlockHash,
    block_count: u64,
    logical_replay_bytes: u64,
    sequence_digest: CanonicalBlockFactsSequenceDigest,
}

impl ReplaySequence {
    fn new(row: ReplaySequenceRow) -> Result<Self, CanonicalStoreError> {
        let mut digest_builder = CanonicalBlockFactsSequenceDigestBuilder::new(
            CanonicalBlockFactsSequenceDigestVersion::V1,
        );
        digest_builder
            .try_append(row.reference_digest)
            .map_err(|source| CanonicalStoreError::block_replay_sequence(source.to_string()))?;
        Ok(Self {
            first_height: row.height,
            first_parent_hash: row.parent_hash,
            first_hash: row.block_hash,
            tip_height: row.height,
            tip_hash: row.block_hash,
            block_count: 1,
            logical_replay_bytes: row.replay_bytes,
            digest_builder,
        })
    }

    fn append(&mut self, row: ReplaySequenceRow) -> Result<(), CanonicalStoreError> {
        let expected_height = self.tip_height.next().ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence("block height overflow after u32::MAX")
        })?;
        if row.height != expected_height {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "expected height {}, observed {}",
                expected_height.value(),
                row.height.value()
            )));
        }
        if row.parent_hash != self.tip_hash {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "block {} parent does not match the preceding block hash",
                row.height.value()
            )));
        }
        self.block_count = self.block_count.checked_add(1).ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence("block count exceeds u64::MAX")
        })?;
        self.logical_replay_bytes = self
            .logical_replay_bytes
            .checked_add(row.replay_bytes)
            .ok_or_else(|| {
                CanonicalStoreError::block_replay_sequence(
                    "logical replay byte count exceeds u64::MAX",
                )
            })?;
        self.digest_builder
            .try_append(row.reference_digest)
            .map_err(|source| CanonicalStoreError::block_replay_sequence(source.to_string()))?;
        self.tip_height = row.height;
        self.tip_hash = row.block_hash;
        Ok(())
    }

    fn sequence_checkpoint(&self) -> CanonicalSequenceCheckpoint {
        CanonicalSequenceCheckpoint::from_admitted_parts(
            BlockId::new(self.tip_height, self.tip_hash),
            self.block_count,
            self.digest_builder.clone().finish(),
            self.logical_replay_bytes,
        )
    }

    fn finish(self) -> FinishedReplaySequence {
        FinishedReplaySequence {
            first_height: self.first_height,
            first_parent_hash: self.first_parent_hash,
            first_hash: self.first_hash,
            tip_height: self.tip_height,
            tip_hash: self.tip_hash,
            block_count: self.block_count,
            logical_replay_bytes: self.logical_replay_bytes,
            sequence_digest: self.digest_builder.finish(),
        }
    }
}

fn validate_replay_versions(
    format_version: CanonicalBlockReplayFormatVersion,
    reference_digest: CanonicalBlockFactsDigest,
) -> Result<(), CanonicalStoreError> {
    if format_version != CanonicalBlockReplayFormatVersion::V1
        || reference_digest.version() != CanonicalBlockFactsDigestVersion::V1
    {
        return Err(CanonicalStoreError::block_replay_sequence(
            "replay and semantic digest contracts must both be version 1",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::canonical_store::live_replacement_tests::{canonical_block, published_store};

    #[test]
    fn incremental_replay_range_enforces_the_exact_span_limit() {
        let maximum_end = BlockHeight::new(MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS - 1);
        assert!(
            validate_incremental_replay_range_bound(BlockHeightRange::inclusive(
                BlockHeight::new(0),
                maximum_end,
            ))
            .is_ok()
        );
        assert!(
            validate_incremental_replay_range_bound(BlockHeightRange::inclusive(
                BlockHeight::new(0),
                BlockHeight::new(MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS),
            ))
            .is_err()
        );
        assert!(
            validate_incremental_replay_range_bound(BlockHeightRange::empty_at(BlockHeight::new(
                10
            ),))
            .is_ok()
        );
    }

    #[test]
    fn replay_slice_source_reconstructs_and_verifies_the_complete_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store = published_store(
            &temporary.path().join("canonical"),
            2,
            BlockHeight::new(8),
            BlockHeight::new(6),
        )?;
        let ready = store.ready_evidence();
        let slice_source = store.canonical_replay_slice_source();

        // Empty or out-of-bounds slices must be refused immediately.
        assert!(
            slice_source
                .scan_slice(BlockHeightRange::empty_at(BlockHeight::new(2)))
                .is_err()
        );
        assert!(
            slice_source
                .scan_slice(BlockHeightRange::inclusive(
                    BlockHeight::new(1),
                    BlockHeight::new(9),
                ))
                .is_err()
        );

        let slices = [
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(3)),
            BlockHeightRange::inclusive(BlockHeight::new(4), BlockHeight::new(6)),
            BlockHeightRange::inclusive(BlockHeight::new(7), BlockHeight::new(8)),
        ];
        let mut sequence_digest = CanonicalBlockFactsSequenceDigestBuilder::new(
            CanonicalBlockFactsSequenceDigestVersion::V1,
        );
        let mut observed_blocks = Vec::new();
        for range in slices {
            for replay in slice_source.scan_slice(range)? {
                let replay = replay?;
                sequence_digest.try_append(replay.reference_digest())?;
                let header = &replay.facts().block_header;
                observed_blocks.push(BlockId::new(header.height, header.block_hash));
            }
        }
        let serial_blocks = store
            .scan_canonical_replay()?
            .map(|replay| {
                replay.map(|validated| {
                    let header = &validated.facts().block_header;
                    BlockId::new(header.height, header.block_hash)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(observed_blocks, serial_blocks);

        let observed = ObservedCanonicalReplaySequence {
            first_block: *observed_blocks.first().ok_or("missing first block")?,
            tip: *observed_blocks.last().ok_or("missing tip block")?,
            block_count: u64::try_from(observed_blocks.len())?,
            sequence_digest: sequence_digest.finish(),
        };
        slice_source.verify_complete_sequence(observed)?;

        // Mismatched count, tip, or sequence digest must be rejected.
        let mut bad_count = observed;
        bad_count.block_count -= 1;
        assert!(slice_source.verify_complete_sequence(bad_count).is_err());

        let mut bad_first = observed;
        bad_first.first_block = BlockId::new(
            ready.first_retained_block.height,
            BlockHash::from_bytes([0xff; 32]),
        );
        assert!(slice_source.verify_complete_sequence(bad_first).is_err());

        let mut bad_tip = observed;
        bad_tip.tip = BlockId::new(ready.visible_tip.height, BlockHash::from_bytes([0xee; 32]));
        assert!(slice_source.verify_complete_sequence(bad_tip).is_err());

        let mut bad_digest = observed;
        bad_digest.sequence_digest =
            CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                CanonicalBlockFactsSequenceDigestVersion::V1,
                observed.block_count,
                [0xdd; 32],
            );
        assert!(slice_source.verify_complete_sequence(bad_digest).is_err());
        Ok(())
    }

    fn exhaust_slice(
        slice_source: &CanonicalReplaySliceSource<'_>,
        range: BlockHeightRange,
    ) -> Result<usize, CanonicalStoreError> {
        let mut observed = 0usize;
        for replay in slice_source.scan_slice(range)? {
            replay?;
            observed += 1;
        }
        Ok(observed)
    }

    fn collect_slices(
        slice_source: &CanonicalReplaySliceSource<'_>,
        slices: &[BlockHeightRange],
    ) -> Result<ObservedCanonicalReplaySequence, Box<dyn std::error::Error>> {
        let mut sequence_digest = CanonicalBlockFactsSequenceDigestBuilder::new(
            CanonicalBlockFactsSequenceDigestVersion::V1,
        );
        let mut first_block = None;
        let mut tip = None;
        let mut block_count = 0u64;
        for &range in slices {
            for replay in slice_source.scan_slice(range)? {
                let replay = replay?;
                sequence_digest.try_append(replay.reference_digest())?;
                let header = &replay.facts().block_header;
                let block_id = BlockId::new(header.height, header.block_hash);
                if first_block.is_none() {
                    first_block = Some(block_id);
                }
                tip = Some(block_id);
                block_count += 1;
            }
        }
        Ok(ObservedCanonicalReplaySequence {
            first_block: first_block.ok_or("missing first block")?,
            tip: tip.ok_or("missing tip block")?,
            block_count,
            sequence_digest: sequence_digest.finish(),
        })
    }

    #[test]
    fn replay_slice_source_rejects_deleted_and_detached_stored_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::super::live_tail::{LiveTailBlockRecord, LiveTailEncodeOptions};
        use super::super::rocksdb::LIVE_TAIL_BLOCK_COLUMN_FAMILY;
        use zinder_core::wire::encode_height_key_ascending;

        let temporary = TempDir::new()?;
        let slice_1_3 = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(3));

        let missing_internal = published_store(
            &temporary.path().join("missing-internal"),
            2,
            BlockHeight::new(8),
            BlockHeight::new(6),
        )?;
        let internal_cf = missing_internal
            .bounded_open
            .db
            .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
            .ok_or("missing live_tail_block column family")?;
        missing_internal.bounded_open.db.delete_cf(
            &internal_cf,
            encode_height_key_ascending(BlockHeight::new(2)),
        )?;
        assert!(matches!(
            exhaust_slice(&missing_internal.canonical_replay_slice_source(), slice_1_3),
            Err(CanonicalStoreError::BlockReplaySequenceInvalid { .. })
        ));

        let missing_final = published_store(
            &temporary.path().join("missing-final"),
            2,
            BlockHeight::new(8),
            BlockHeight::new(6),
        )?;
        let final_cf = missing_final
            .bounded_open
            .db
            .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
            .ok_or("missing live_tail_block column family")?;
        missing_final
            .bounded_open
            .db
            .delete_cf(&final_cf, encode_height_key_ascending(BlockHeight::new(3)))?;
        assert!(matches!(
            exhaust_slice(&missing_final.canonical_replay_slice_source(), slice_1_3),
            Err(CanonicalStoreError::BlockReplaySequenceInvalid { .. })
        ));

        let detached = published_store(
            &temporary.path().join("detached-parent"),
            2,
            BlockHeight::new(8),
            BlockHeight::new(6),
        )?;
        let mut wrong_parent = detached
            .ready_evidence()
            .first_retained_block
            .hash
            .as_bytes();
        wrong_parent[0] ^= 0xff;
        let replacement = canonical_block(BlockHeight::new(2), [0x22; 32], wrong_parent, 2);
        let (encoded_replacement, _) = LiveTailBlockRecord::encode_from_compact_block_v1(
            &replacement.compact_block,
            Some(replacement.facts.clone()),
            LiveTailEncodeOptions {
                include_supplement: true,
                retain_canonical_facts: true,
            },
        )?;
        let detached_cf = detached
            .bounded_open
            .db
            .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
            .ok_or("missing live_tail_block column family")?;
        detached.bounded_open.db.put_cf(
            &detached_cf,
            encode_height_key_ascending(BlockHeight::new(2)),
            encoded_replacement,
        )?;
        assert!(matches!(
            exhaust_slice(&detached.canonical_replay_slice_source(), slice_1_3),
            Err(CanonicalStoreError::BlockReplaySequenceInvalid { .. })
        ));
        Ok(())
    }

    #[test]
    fn replay_slice_source_rejects_trailing_row_beyond_ready_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::super::live_tail::{LiveTailBlockRecord, LiveTailEncodeOptions};
        use super::super::rocksdb::LIVE_TAIL_BLOCK_COLUMN_FAMILY;
        use zinder_core::wire::encode_height_key_ascending;

        let temporary = TempDir::new()?;
        let store = published_store(
            &temporary.path().join("trailing-fence"),
            2,
            BlockHeight::new(8),
            BlockHeight::new(6),
        )?;
        let ready = store.ready_evidence();
        let trailing = canonical_block(
            BlockHeight::new(9),
            [0x99; 32],
            ready.visible_tip.hash.as_bytes(),
            9,
        );
        let (encoded_trailing, _) = LiveTailBlockRecord::encode_from_compact_block_v1(
            &trailing.compact_block,
            Some(trailing.facts.clone()),
            LiveTailEncodeOptions {
                include_supplement: true,
                retain_canonical_facts: true,
            },
        )?;
        let replay_cf = store
            .bounded_open
            .db
            .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
            .ok_or("missing live_tail_block column family")?;
        store.bounded_open.db.put_cf(
            &replay_cf,
            encode_height_key_ascending(BlockHeight::new(9)),
            encoded_trailing,
        )?;

        let slices = [
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(3)),
            BlockHeightRange::inclusive(BlockHeight::new(4), BlockHeight::new(6)),
            BlockHeightRange::inclusive(BlockHeight::new(7), BlockHeight::new(8)),
        ];
        let slice_source = store.canonical_replay_slice_source();
        let observed = collect_slices(&slice_source, &slices)?;
        let outcome = slice_source.verify_complete_sequence(observed);
        assert!(matches!(
            outcome,
            Err(CanonicalStoreError::BlockReplaySequenceInvalid { ref reason })
                if reason.contains("beyond the visible fence")
        ));
        Ok(())
    }
}
