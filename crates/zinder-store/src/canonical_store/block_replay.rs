use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use rust_rocksdb::{BoundColumnFamily, DB, DBRawIteratorWithThreadMode, ReadOptions};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockHeightRangeIter, BlockId, CanonicalBlockFacts,
    CanonicalBlockFactsDigest, CanonicalBlockFactsDigestVersion, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion,
    CanonicalBlockReplayFormatVersion, ValidatedCanonicalBlockReplay,
    decode_canonical_block_replay,
    wire::{decode_height_key_ascending, encode_height_key_ascending},
};

use super::{
    CanonicalBlockLoadEvidence, CanonicalSequenceCheckpoint, CanonicalStoreError,
    CanonicalStoreReadyEvidence, construction_manifest::CanonicalConstructionFamilyEvidence,
};

pub(super) const BLOCK_REPLAY_COLUMN_FAMILY: &str = "block_replay";

/// Maximum rows returned by one incremental replay-range scan.
///
/// Projectors page retained events and apply committed ranges one at a time.
/// This ceiling prevents a broad event or caller request from turning the
/// incremental path back into an unbounded historical scan.
pub const MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS: u32 = 4_096;

/// One cache-bypassing, forward scan of the READY canonical replay family.
///
/// Every yielded envelope is decoded and semantically validated. Reaching the
/// end also authenticates the exact first block, visible tip, current row
/// count, parent links, every block-local replay digest, and the complete
/// visible sequence digest recorded by canonical publication.
pub struct CanonicalReplayScan<'a> {
    iterator: DBRawIteratorWithThreadMode<'a, DB>,
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
/// contiguity and parent links without populating the block cache. Callers
/// that reconstruct a full history across multiple slices must verify the
/// stitched boundaries and call [`Self::verify_complete_sequence`] before
/// trusting the scan as a complete READY replay.
#[derive(Clone, Copy)]
pub struct CanonicalReplaySliceSource<'a> {
    db: &'a DB,
    ready_evidence: CanonicalStoreReadyEvidence,
}

impl<'a> CanonicalReplaySliceSource<'a> {
    pub(super) const fn new(db: &'a DB, ready_evidence: &CanonicalStoreReadyEvidence) -> Self {
        Self {
            db,
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
        CanonicalReplaySliceScan::new(self.db, &self.ready_evidence, range)
    }

    /// Verifies that a stitched set of slices matches the complete READY fence
    /// and that no replay rows exist past the visible tip.
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
        if let Some(after_tip) = self.ready_evidence.visible_tip.height.next() {
            let block_replay = block_replay_column_family(self.db)?;
            let mut read_options = ReadOptions::default();
            read_options.fill_cache(false);
            let mut iterator = self.db.raw_iterator_cf_opt(&block_replay, read_options);
            iterator.seek(encode_height_key_ascending(after_tip));
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
        }
        Ok(())
    }
}

/// One cache-bypassing forward scan over a height-bounded slice of `block_replay`.
///
/// Every yielded row is decoded, version-checked, height-checked, and linked to
/// its immediate predecessor within the slice. Reaching `None` verifies that
/// every height in the requested slice was present with no gaps or trailing
/// rows.
pub struct CanonicalReplaySliceScan<'a> {
    iterator: DBRawIteratorWithThreadMode<'a, DB>,
    ready_evidence: CanonicalStoreReadyEvidence,
    range: BlockHeightRange,
    expected_block_count: u64,
    previous_block: Option<(BlockHeight, BlockHash)>,
    observed_block_count: u64,
    finished: bool,
}

impl<'a> CanonicalReplaySliceScan<'a> {
    fn new(
        db: &'a DB,
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
        let block_replay = block_replay_column_family(db)?;
        let mut read_options = ReadOptions::default();
        read_options.fill_cache(false);
        read_options.set_readahead_size(2 * 1024 * 1024);
        if let Some(upper_height) = range.end.next() {
            read_options.set_iterate_upper_bound(encode_height_key_ascending(upper_height));
        }
        let mut iterator = db.raw_iterator_cf_opt(&block_replay, read_options);
        iterator.seek(encode_height_key_ascending(range.start));
        Ok(Self {
            iterator,
            ready_evidence: *ready_evidence,
            range,
            expected_block_count,
            previous_block: None,
            observed_block_count: 0,
            finished: false,
        })
    }

    fn finish_slice(&self) -> Result<(), CanonicalStoreError> {
        self.iterator
            .status()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "canonical replay slice scan",
                source,
            })?;
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
        if !self.iterator.valid() {
            self.finished = true;
            return self.finish_slice().err().map(Err);
        }
        let decoded_replay = self.iterator.item().map_or_else(
            || {
                Err(CanonicalStoreError::block_replay_sequence(
                    "canonical replay slice iterator was valid without a row",
                ))
            },
            |(key, encoded_replay)| decode_persisted_replay(key, encoded_replay),
        );
        let replay = (|| {
            let (height, replay) = decoded_replay?;
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
        self.iterator.next();
        if replay.is_err() {
            self.finished = true;
        }
        Some(replay)
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
    db: &'a DB,
    heights: BlockHeightRangeIter,
    previous_hash: Option<BlockHash>,
    first_retained_block: BlockId,
    finished: bool,
}

impl<'a> CanonicalReplayRangeScan<'a> {
    pub(super) fn new(
        db: &'a DB,
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
                read_persisted_replay(db, predecessor_height)?
                    .replay
                    .facts()
                    .block_header
                    .block_hash,
            )
        } else {
            None
        };
        Ok(Self {
            db,
            heights: range.into_iter(),
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
        let Some(height) = self.heights.next() else {
            self.finished = true;
            return None;
        };
        let replay = (|| {
            let replay = read_persisted_replay(self.db, height)?.replay;
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
        if replay.is_err() {
            self.finished = true;
        }
        Some(replay)
    }
}

impl<'a> CanonicalReplayScan<'a> {
    pub(super) fn new(
        db: &'a DB,
        ready_evidence: &CanonicalStoreReadyEvidence,
    ) -> Result<Self, CanonicalStoreError> {
        let block_replay = block_replay_column_family(db)?;
        let mut read_options = ReadOptions::default();
        read_options.fill_cache(false);
        read_options.set_readahead_size(2 * 1024 * 1024);
        let mut iterator = db.raw_iterator_cf_opt(&block_replay, read_options);
        iterator.seek(encode_height_key_ascending(
            ready_evidence.first_retained_block.height,
        ));
        Ok(Self {
            iterator,
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
        self.iterator
            .status()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "canonical replay scan",
                source,
            })?;
        let expected_block_count = u64::from(
            self.ready_evidence
                .visible_tip
                .height
                .value()
                .checked_sub(self.ready_evidence.first_retained_block.height.value())
                .and_then(|distance| distance.checked_add(1))
                .ok_or_else(|| {
                    CanonicalStoreError::block_replay_sequence(
                        "READY visible range cannot produce a block count",
                    )
                })?,
        );
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
        if !self.iterator.valid() {
            self.finished = true;
            return self.finish_scan().err().map(Err);
        }
        let decoded_replay = self.iterator.item().map_or_else(
            || {
                Err(CanonicalStoreError::block_replay_sequence(
                    "canonical replay iterator was valid without a row",
                ))
            },
            |(key, encoded_replay)| decode_persisted_replay(key, encoded_replay),
        );
        let replay = (|| {
            let (height, replay) = decoded_replay?;
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
        self.iterator.next();
        if replay.is_err() {
            self.finished = true;
        }
        Some(replay)
    }
}

/// Cache-bypassing semantic proof of the persisted replay family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PersistedBlockReplayEvidence {
    pub(super) first_height: BlockHeight,
    pub(super) first_parent_hash: BlockHash,
    pub(super) first_hash: BlockHash,
    pub(super) tip_height: BlockHeight,
    pub(super) tip_hash: BlockHash,
    pub(super) block_count: u64,
    pub(super) logical_family_bytes: u64,
    pub(super) logical_replay_bytes: u64,
    pub(super) replay_format_version: CanonicalBlockReplayFormatVersion,
    pub(super) block_digest_version: CanonicalBlockFactsDigestVersion,
    pub(super) sequence_digest_version: CanonicalBlockFactsSequenceDigestVersion,
    pub(super) sequence_digest: CanonicalBlockFactsSequenceDigest,
    pub(super) family_evidence: CanonicalConstructionFamilyEvidence,
    pub(super) elapsed: Duration,
}

pub(super) struct PersistedBlockReplayValidation {
    pub(super) evidence: PersistedBlockReplayEvidence,
    pub(super) retained_sequence_checkpoints: VecDeque<CanonicalSequenceCheckpoint>,
}

/// Decodes every persisted replay row without populating the block cache.
#[cfg(test)]
pub(super) fn validate_persisted_block_replays(
    db: &DB,
) -> Result<PersistedBlockReplayEvidence, CanonicalStoreError> {
    Ok(validate_persisted_block_replays_with_checkpoints(db, 0)?.evidence)
}

pub(super) fn validate_persisted_block_replays_with_checkpoints(
    db: &DB,
    retained_checkpoint_count: usize,
) -> Result<PersistedBlockReplayValidation, CanonicalStoreError> {
    let started_at = Instant::now();
    let block_replay = block_replay_column_family(db)?;
    let mut read_options = ReadOptions::default();
    read_options.fill_cache(false);
    read_options.set_readahead_size(2 * 1024 * 1024);
    let mut iterator = db.raw_iterator_cf_opt(&block_replay, read_options);
    iterator.seek_to_first();
    let mut sequence: Option<ReplaySequence> = None;
    let mut family_evidence =
        CanonicalConstructionFamilyEvidence::accumulator(BLOCK_REPLAY_COLUMN_FAMILY);
    let mut retained_sequence_checkpoints = VecDeque::new();
    while iterator.valid() {
        let Some((key, encoded_replay)) = iterator.item() else {
            iterator
                .status()
                .map_err(|source| CanonicalStoreError::RocksDbOperation {
                    operation: "block replay readback iteration",
                    source,
                })?;
            break;
        };
        let (height, replay) = decode_persisted_replay(key, encoded_replay)?;
        family_evidence.observe(key, encoded_replay)?;
        let facts = replay.facts();
        let replay_bytes = u64::try_from(encoded_replay.len()).map_err(|_| {
            CanonicalStoreError::block_replay_sequence("replay byte length exceeds u64::MAX")
        })?;
        let logical_family_bytes = key
            .len()
            .checked_add(encoded_replay.len())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| {
                CanonicalStoreError::block_replay_sequence(
                    "replay family key and value bytes exceed u64::MAX",
                )
            })?;
        let row = ReplaySequenceRow {
            height,
            block_hash: facts.block_header.block_hash,
            parent_hash: facts.block_header.parent_hash,
            reference_digest: replay.reference_digest(),
            logical_family_bytes,
            replay_bytes,
        };
        if let Some(sequence) = sequence.as_mut() {
            sequence.append(row)?;
        } else {
            sequence = Some(ReplaySequence::new(row)?);
        }
        let sequence = sequence.as_ref().ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence("replay sequence state is absent")
        })?;
        if retained_checkpoint_count > 0 {
            retained_sequence_checkpoints.push_back(sequence.sequence_checkpoint());
            if retained_sequence_checkpoints.len() > retained_checkpoint_count {
                retained_sequence_checkpoints.pop_front();
            }
        }
        iterator.next();
    }
    iterator
        .status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "block replay readback iteration",
            source,
        })?;
    let mut evidence = sequence.map(ReplaySequence::finish).ok_or_else(|| {
        CanonicalStoreError::block_replay_sequence(
            "persisted block replay family must not be empty",
        )
    })?;
    evidence.family_evidence = family_evidence.finish();
    evidence.elapsed = started_at.elapsed();
    Ok(PersistedBlockReplayValidation {
        evidence,
        retained_sequence_checkpoints,
    })
}

pub(super) fn resume_persisted_sequence_checkpoint(
    db: &DB,
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
        resumed = super::replay_tail::authenticate_next_replay(db, resumed, height)?;
    }
    Ok(resumed)
}

pub(super) fn read_replay_facts_at(
    db: &DB,
    ready_evidence: &CanonicalStoreReadyEvidence,
    height: BlockHeight,
) -> Result<Option<CanonicalBlockFacts>, CanonicalStoreError> {
    if height < ready_evidence.first_retained_block.height
        || height > ready_evidence.visible_tip.height
    {
        return Ok(None);
    }
    Ok(Some(read_persisted_replay(db, height)?.replay.into_facts()))
}

/// Replay rows one batched lookup of [`visit_replay_facts_at_heights`] holds
/// at once.
///
/// Rows of transparent-dense blocks reach hundreds of kilobytes, so the bound
/// keeps a reader's memory to a few megabytes however many heights it visits,
/// while adjacent rows inside one batch still coalesce into one device read.
const REPLAY_FACTS_BATCH_ROWS: usize = 32;

/// Visits the canonical facts at several heights in height order.
///
/// `visit` receives each height's position in `heights` with its facts, or
/// with `None` for a height outside the admitted retained range, as
/// [`read_replay_facts_at`] answers. Rows are read in batched lookups of at
/// most [`REPLAY_FACTS_BATCH_ROWS`] and bypass the block cache: a random
/// historical row is rarely read twice, and admitting it would evict the
/// smaller index and transaction-location blocks that every lookup reuses.
pub(super) fn visit_replay_facts_at_heights<E>(
    db: &DB,
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
    let family = block_replay_column_family(db)?;
    let mut read_options = ReadOptions::default();
    read_options.fill_cache(false);
    for batch in retained.chunks(REPLAY_FACTS_BATCH_ROWS) {
        let keys = batch
            .iter()
            .map(|(height, _)| encode_height_key_ascending(*height))
            .collect::<Vec<_>>();
        let rows = db.batched_multi_get_cf_slice_opt(
            &family,
            keys.iter().map(<[u8; 4]>::as_slice),
            true,
            &read_options,
        );
        for ((key, (height, index)), row) in keys.iter().zip(batch).zip(rows) {
            let encoded_replay = row
                .map_err(|source| CanonicalStoreError::RocksDbOperation {
                    operation: "canonical replay batch read",
                    source,
                })?
                .ok_or_else(|| {
                    CanonicalStoreError::block_replay_sequence(format!(
                        "canonical replay at height {} is absent",
                        height.value()
                    ))
                })?;
            let (_, replay) = decode_persisted_replay(key, &encoded_replay)?;
            visit(*index, Some(replay.into_facts()))?;
        }
    }
    Ok(())
}

pub(super) struct PersistedReplayStep {
    pub(super) replay: ValidatedCanonicalBlockReplay,
    pub(super) logical_replay_bytes: u64,
}

pub(super) fn read_persisted_replay(
    db: &DB,
    height: BlockHeight,
) -> Result<PersistedReplayStep, CanonicalStoreError> {
    let key = encode_height_key_ascending(height);
    let encoded_replay = db
        .get_cf(&block_replay_column_family(db)?, key)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "canonical replay checkpoint read",
            source,
        })?
        .ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence(format!(
                "canonical replay at height {} is absent",
                height.value()
            ))
        })?;
    let (_, replay) = decode_persisted_replay(&key, &encoded_replay)?;
    let logical_replay_bytes = u64::try_from(encoded_replay.len()).map_err(|_| {
        CanonicalStoreError::block_replay_sequence("replay byte length exceeds u64::MAX")
    })?;
    Ok(PersistedReplayStep {
        replay,
        logical_replay_bytes,
    })
}

fn decode_persisted_replay(
    key: &[u8],
    encoded_replay: &[u8],
) -> Result<(BlockHeight, ValidatedCanonicalBlockReplay), CanonicalStoreError> {
    let height = decode_height_key_ascending(key).map_err(|source| {
        CanonicalStoreError::BlockReplayKeyInvalid {
            reason: source.to_string(),
        }
    })?;
    let replay = decode_canonical_block_replay(encoded_replay)
        .map_err(|source| CanonicalStoreError::block_replay_invalid(height, source.to_string()))?;
    validate_replay_versions(replay.format_version(), replay.reference_digest())?;
    if replay.facts().block_header.height != height {
        return Err(CanonicalStoreError::block_replay_invalid(
            height,
            format!(
                "row contains height {}",
                replay.facts().block_header.height.value()
            ),
        ));
    }
    Ok((height, replay))
}

impl PersistedBlockReplayEvidence {
    pub(super) fn has_same_sequence(&self, prepared: &CanonicalBlockLoadEvidence) -> bool {
        let replay_counts_match = self.block_count == prepared.block_count
            && prepared.block_count == prepared.block_replay_count;
        let replay_logical_bytes_match =
            self.logical_family_bytes == prepared.block_replay_logical_bytes;
        self.first_height == prepared.first_height
            && self.first_parent_hash == prepared.first_parent_hash
            && self.first_hash == prepared.first_hash
            && self.tip_height == prepared.tip_height
            && self.tip_hash == prepared.tip_hash
            && replay_counts_match
            && replay_logical_bytes_match
            && self.replay_format_version == prepared.replay_format_version
            && self.block_digest_version == prepared.block_digest_version
            && self.sequence_digest_version == prepared.sequence_digest_version
            && self.sequence_digest == prepared.sequence_digest
    }
}

fn block_replay_column_family(db: &DB) -> Result<Arc<BoundColumnFamily<'_>>, CanonicalStoreError> {
    db.cf_handle(BLOCK_REPLAY_COLUMN_FAMILY).ok_or_else(|| {
        CanonicalStoreError::block_replay_sequence("block_replay column family is absent")
    })
}

struct ReplaySequence {
    first_height: BlockHeight,
    first_parent_hash: BlockHash,
    first_hash: BlockHash,
    tip_height: BlockHeight,
    tip_hash: BlockHash,
    block_count: u64,
    logical_family_bytes: u64,
    logical_replay_bytes: u64,
    digest_builder: CanonicalBlockFactsSequenceDigestBuilder,
}

#[derive(Clone, Copy)]
struct ReplaySequenceRow {
    height: BlockHeight,
    block_hash: BlockHash,
    parent_hash: BlockHash,
    reference_digest: CanonicalBlockFactsDigest,
    logical_family_bytes: u64,
    replay_bytes: u64,
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
            logical_family_bytes: row.logical_family_bytes,
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
        self.logical_family_bytes = self
            .logical_family_bytes
            .checked_add(row.logical_family_bytes)
            .ok_or_else(|| {
                CanonicalStoreError::block_replay_sequence(
                    "replay family logical byte count exceeds u64::MAX",
                )
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

    fn finish(self) -> PersistedBlockReplayEvidence {
        PersistedBlockReplayEvidence {
            first_height: self.first_height,
            first_parent_hash: self.first_parent_hash,
            first_hash: self.first_hash,
            tip_height: self.tip_height,
            tip_hash: self.tip_hash,
            block_count: self.block_count,
            logical_family_bytes: self.logical_family_bytes,
            logical_replay_bytes: self.logical_replay_bytes,
            replay_format_version: CanonicalBlockReplayFormatVersion::V1,
            block_digest_version: CanonicalBlockFactsDigestVersion::V1,
            sequence_digest_version: CanonicalBlockFactsSequenceDigestVersion::V1,
            sequence_digest: self.digest_builder.finish(),
            family_evidence: CanonicalConstructionFamilyEvidence::accumulator(
                BLOCK_REPLAY_COLUMN_FAMILY,
            )
            .finish(),
            elapsed: Duration::ZERO,
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
            .cf_handle(BLOCK_REPLAY_COLUMN_FAMILY)
            .ok_or("missing block_replay column family")?;
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
            .cf_handle(BLOCK_REPLAY_COLUMN_FAMILY)
            .ok_or("missing block_replay column family")?;
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
        let detached_cf = detached
            .bounded_open
            .db
            .cf_handle(BLOCK_REPLAY_COLUMN_FAMILY)
            .ok_or("missing block_replay column family")?;
        detached.bounded_open.db.put_cf(
            &detached_cf,
            encode_height_key_ascending(BlockHeight::new(2)),
            replacement.replay_envelope.as_bytes(),
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
        let replay_cf = store
            .bounded_open
            .db
            .cf_handle(BLOCK_REPLAY_COLUMN_FAMILY)
            .ok_or("missing block_replay column family")?;
        store.bounded_open.db.put_cf(
            &replay_cf,
            encode_height_key_ascending(BlockHeight::new(9)),
            trailing.replay_envelope.as_bytes(),
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
