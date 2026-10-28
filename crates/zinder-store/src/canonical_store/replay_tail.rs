//! Incrementally authenticated replay rows of the unsettled canonical suffix.
//!
//! Every READY publication must be explained by the persisted replay rows from
//! the settled sequence checkpoint through the visible tip. Re-deriving that
//! suffix on every transition costs one row read per unsettled block, which
//! scales with the reorg window. This tail keeps the sequence checkpoint of
//! every unsettled height it has already derived from persisted rows, so a
//! transition authenticates only the rows it changed and chains them onto
//! checkpoints derived earlier.
//!
//! Rows above the settled tip change only through the canonical writer's
//! atomic transitions, so an incrementally extended tail equals the tail a
//! complete re-derivation would produce. A holder still re-derives the whole
//! suffix once the rows it authenticated incrementally reach one reorg window,
//! which bounds how long an out-of-band change to an unchanged row can go
//! unnoticed while keeping the amortized cost per block constant.

use std::collections::VecDeque;

use rust_rocksdb::DB;
use zinder_core::{BlockHeight, BlockId, CanonicalBlockFactsSequenceDigestBuilder};

use super::{
    CanonicalSequenceCheckpoint, CanonicalStoreError, block_replay::read_persisted_replay,
};

/// Sequence checkpoints from the settled tip through the visible tip, each
/// derived from persisted replay rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AuthenticatedReplayTail {
    settled: CanonicalSequenceCheckpoint,
    unsettled: VecDeque<CanonicalSequenceCheckpoint>,
    rows_since_full_authentication: u64,
}

impl AuthenticatedReplayTail {
    /// Derives every unsettled checkpoint from the persisted rows above
    /// `settled` through `through`.
    pub(super) fn authenticate(
        db: &DB,
        settled: CanonicalSequenceCheckpoint,
        through: BlockHeight,
        maximum_replay_blocks: u32,
    ) -> Result<Self, CanonicalStoreError> {
        let mut tail = Self {
            settled,
            unsettled: VecDeque::new(),
            rows_since_full_authentication: 0,
        };
        tail.extend_through(db, through, maximum_replay_blocks)?;
        tail.rows_since_full_authentication = 0;
        Ok(tail)
    }

    /// Returns the authenticated settled checkpoint.
    pub(super) const fn settled(&self) -> CanonicalSequenceCheckpoint {
        self.settled
    }

    /// Returns the authenticated checkpoint through the visible tip.
    pub(super) fn tip(&self) -> CanonicalSequenceCheckpoint {
        self.unsettled.back().copied().unwrap_or(self.settled)
    }

    /// Returns every authenticated checkpoint from the settled tip upward.
    #[cfg(test)]
    pub(super) fn checkpoints(&self) -> Vec<CanonicalSequenceCheckpoint> {
        std::iter::once(self.settled)
            .chain(self.unsettled.iter().copied())
            .collect()
    }

    /// Returns whether the rows authenticated incrementally since the last
    /// complete derivation have reached one reorg window.
    pub(super) fn full_authentication_due(&self, maximum_replay_blocks: u32) -> bool {
        self.rows_since_full_authentication >= u64::from(maximum_replay_blocks)
    }

    /// Authenticates the rows a transition wrote above `retained_height`,
    /// whose checkpoint it keeps, through `through`.
    ///
    /// An append retains the previous visible tip; a replacement retains its
    /// common parent and discards the checkpoints of every replaced height.
    pub(super) fn reauthenticate_above(
        &mut self,
        db: &DB,
        retained_height: BlockHeight,
        through: BlockHeight,
        maximum_replay_blocks: u32,
    ) -> Result<(), CanonicalStoreError> {
        while self
            .unsettled
            .back()
            .is_some_and(|checkpoint| checkpoint.through().height > retained_height)
        {
            self.unsettled.pop_back();
        }
        if self.tip().through().height != retained_height {
            return Err(CanonicalStoreError::block_replay_sequence(format!(
                "transition retains height {} outside the authenticated replay tail",
                retained_height.value()
            )));
        }
        self.extend_through(db, through, maximum_replay_blocks)
    }

    /// Advances the settled boundary to `settled`, which must equal the
    /// checkpoint this tail derived for its height.
    pub(super) fn settle(
        &mut self,
        settled: CanonicalSequenceCheckpoint,
    ) -> Result<(), CanonicalStoreError> {
        while self.settled.through().height < settled.through().height {
            let Some(next) = self.unsettled.pop_front() else {
                break;
            };
            self.settled = next;
        }
        if self.settled != settled {
            return Err(CanonicalStoreError::block_replay_sequence(
                "persisted settled checkpoint differs from the authenticated replay tail",
            ));
        }
        Ok(())
    }

    /// Derives the rows above the current tip through `through`, reading at
    /// most `maximum_replay_blocks` rows.
    fn extend_through(
        &mut self,
        db: &DB,
        through: BlockHeight,
        maximum_replay_blocks: u32,
    ) -> Result<(), CanonicalStoreError> {
        let mut previous = self.tip();
        let replay_block_count = through
            .value()
            .checked_sub(previous.through().height.value())
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
        while let Some(height) = previous
            .through()
            .height
            .next()
            .filter(|height| *height <= through)
        {
            previous = authenticate_next_replay(db, previous, height)?;
            self.unsettled.push_back(previous);
            self.rows_since_full_authentication =
                self.rows_since_full_authentication.saturating_add(1);
        }
        Ok(())
    }
}

/// Extends `previous` by the persisted replay row at `height`, which must
/// link to it by parent hash.
pub(super) fn authenticate_next_replay(
    db: &DB,
    previous: CanonicalSequenceCheckpoint,
    height: BlockHeight,
) -> Result<CanonicalSequenceCheckpoint, CanonicalStoreError> {
    let persisted = read_persisted_replay(db, height)?;
    let facts = persisted.replay.facts();
    if facts.block_header.parent_hash != previous.through().hash {
        return Err(CanonicalStoreError::block_replay_sequence(format!(
            "block {} does not extend the sequence checkpoint",
            height.value()
        )));
    }
    let mut digest_builder =
        CanonicalBlockFactsSequenceDigestBuilder::resume_from_prefix(previous.sequence_digest());
    digest_builder
        .try_append(persisted.replay.reference_digest())
        .map_err(|source| CanonicalStoreError::block_replay_sequence(source.to_string()))?;
    let retained_block_count = previous
        .retained_block_count()
        .checked_add(1)
        .ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence("checkpoint block count exceeds u64::MAX")
        })?;
    let logical_replay_bytes = previous
        .logical_replay_bytes()
        .checked_add(persisted.logical_replay_bytes)
        .ok_or_else(|| {
            CanonicalStoreError::block_replay_sequence("checkpoint replay bytes exceed u64::MAX")
        })?;
    Ok(CanonicalSequenceCheckpoint::from_admitted_parts(
        BlockId::new(height, facts.block_header.block_hash),
        retained_block_count,
        digest_builder.finish(),
        logical_replay_bytes,
    ))
}
