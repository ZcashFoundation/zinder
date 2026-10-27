//! Atomic canonical suffix replacement.

use std::collections::HashSet;

use rust_rocksdb::{DB, WriteBatch, WriteOptions};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFactsDigestVersion,
    CanonicalBlockFactsSequenceDigestBuilder, ChainEpochId, CommitmentTreeAccumulator,
    CommitmentTreeCheckpoint, CompactBlockArtifact, NetworkUpgradeActivations, ShieldedProtocol,
    SubtreeRootArtifact, SubtreeRootHash, UnixTimestampMillis,
    wire::{encode_internal_block_hash, encode_internal_transaction_id},
};

use super::{
    CanonicalBuildBlock, CanonicalBuildSubtreeRoot, CanonicalEventFence, CanonicalStoreError,
    CanonicalStoreReadyEvidence, CanonicalStoreWorkload, RocksDbCanonicalStore,
    block_load::{
        decode_block_final_note_commitment_roots, encode_block_position,
        encode_transaction_position, read_persisted_tip_metadata, validate_live_block,
    },
    block_replay::{
        BLOCK_REPLAY_COLUMN_FAMILY, read_persisted_replay, resume_persisted_sequence_checkpoint,
    },
    control::encode_ready_store_control_with_revisions,
    displaced_archive::{
        CanonicalDisplacedBlock, DisplacedArchiveEvent, PreparedDisplacedArchiveWrite,
    },
    live_commit::{PreparedLiveBlockRows, event_fence_from_ready, next_live_fence},
    publication::{
        column_family, encode_live_chain_epoch, encode_live_event,
        validate_live_replacement_publication, validate_ready_sequence_checkpoint,
    },
    rocksdb::{
        BLOCK_BLOB_COLUMN_FAMILY, BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
        BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY, CHAIN_EPOCH_COLUMN_FAMILY,
        CHAIN_EVENT_COLUMN_FAMILY, COMPACT_BLOCK_COLUMN_FAMILY, STORE_CONTROL_KEY,
        SUBTREE_ROOT_COLUMN_FAMILY, TRANSACTION_BLOB_COLUMN_FAMILY,
        TRANSACTION_LOCATION_COLUMN_FAMILY, TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    },
    subtree_load::{encode_subtree_root_key, required_subtree_root_ranges},
};

/// One replacement block and its source-authenticated completed subtree roots.
#[derive(Debug)]
pub struct CanonicalReplacementBlock {
    block: CanonicalBuildBlock,
    subtree_roots: Vec<CanonicalBuildSubtreeRoot>,
}

impl CanonicalReplacementBlock {
    /// Creates one ordered replacement block.
    #[must_use]
    pub const fn new(
        block: CanonicalBuildBlock,
        subtree_roots: Vec<CanonicalBuildSubtreeRoot>,
    ) -> Self {
        Self {
            block,
            subtree_roots,
        }
    }
}

/// A nonempty canonical suffix replacement against one authenticated fence.
#[derive(Debug)]
pub struct CanonicalLiveReplacement {
    expected_fence: CanonicalEventFence,
    blocks: Vec<CanonicalReplacementBlock>,
    created_at: UnixTimestampMillis,
}

impl CanonicalLiveReplacement {
    /// Creates one replacement command. Admission rejects an empty or non-suffix range.
    #[must_use]
    pub const fn new(
        expected_fence: CanonicalEventFence,
        blocks: Vec<CanonicalReplacementBlock>,
        created_at: UnixTimestampMillis,
    ) -> Self {
        Self {
            expected_fence,
            blocks,
            created_at,
        }
    }
}

impl RocksDbCanonicalStore {
    /// Reconstructs the exact local tree checkpoint at a candidate replacement parent.
    ///
    /// The candidate must be part of the admitted canonical suffix at or above
    /// the authenticated settled anchor. Reconstruction reads at most the
    /// persisted reorg window and never contacts an upstream source.
    pub fn replacement_parent_checkpoint(
        &self,
        parent: BlockId,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, CanonicalStoreError> {
        validate_activation_identity(self, network_upgrade_activations)?;
        let anchor = self.append_anchor()?;
        if parent.height < anchor.settled_tip().height
            || parent.height >= anchor.event_fence().visible_tip().height
        {
            return Err(CanonicalStoreError::live_commit(
                "replacement parent must be within the admitted canonical suffix before the visible tip",
            ));
        }
        let distance = anchor
            .event_fence()
            .visible_tip()
            .height
            .value()
            .checked_sub(parent.height.value())
            .ok_or_else(|| CanonicalStoreError::live_commit("replacement parent exceeds tip"))?;
        if distance > self.reorg_policy().reorg_window_blocks() {
            return Err(CanonicalStoreError::live_commit(format!(
                "replacement parent is {distance} blocks behind the visible tip; maximum is {}",
                self.reorg_policy().reorg_window_blocks()
            )));
        }
        checkpoint_at(self, parent, network_upgrade_activations)
    }

    /// Atomically archives and replaces the complete unsettled canonical suffix.
    ///
    /// The method consumes the admitted handle. Every canonical deletion and
    /// replacement row, permanent displaced-archive row, epoch, reorg event,
    /// and READY control record is committed by one synced batch. Only exact
    /// readback returns a new admitted handle.
    pub fn commit_live_replacement(
        mut self,
        replacement: CanonicalLiveReplacement,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<(Self, CanonicalEventFence), CanonicalStoreError> {
        let prepared =
            PreparedLiveReplacement::new(&self, replacement, network_upgrade_activations)?;
        let mut batch = WriteBatch::default();
        prepared
            .deletions
            .delete_from(&self.bounded_open.db, &mut batch)?;
        for block_rows in &prepared.replacement_rows {
            block_rows.put_into(&self.bounded_open.db, &mut batch)?;
        }
        prepared.archive.put_into(&self, &mut batch)?;
        batch.put_cf(
            &column_family(&self.bounded_open.db, CHAIN_EPOCH_COLUMN_FAMILY)?,
            prepared.outcome.chain_epoch_id().value().to_be_bytes(),
            &prepared.encoded_epoch,
        );
        batch.put_cf(
            &column_family(&self.bounded_open.db, CHAIN_EVENT_COLUMN_FAMILY)?,
            prepared.outcome.chain_event_sequence().to_be_bytes(),
            &prepared.encoded_event,
        );
        batch.put(STORE_CONTROL_KEY, &prepared.encoded_control);
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(false);
        write_options.set_sync(true);
        abort_at_live_replacement_failpoint("before_atomic_write");
        self.bounded_open
            .db
            .write_opt(&batch, &write_options)
            .map_err(
                |source| CanonicalStoreError::LiveCommitWriteOutcomeUnknown {
                    path: self.bounded_open.db.path().to_path_buf(),
                    source,
                },
            )?;
        abort_at_live_replacement_failpoint("after_atomic_write");
        prepared.validate_readback(&self).map_err(|source| {
            CanonicalStoreError::LiveCommitCompletedButUnverified {
                path: self.bounded_open.db.path().to_path_buf(),
                reason: source.to_string(),
            }
        })?;
        self.ready_evidence = prepared.ready_evidence;
        Ok((self, prepared.outcome))
    }
}

struct PreparedLiveReplacement {
    deletions: PreparedCanonicalDeletions,
    replacement_rows: Vec<PreparedLiveBlockRows>,
    archive: PreparedDisplacedArchiveWrite,
    encoded_epoch: Vec<u8>,
    encoded_event: Vec<u8>,
    encoded_control: Vec<u8>,
    ready_evidence: CanonicalStoreReadyEvidence,
    previous_epoch_id: ChainEpochId,
    reverted_range: BlockHeightRange,
    committed_range: BlockHeightRange,
    outcome: CanonicalEventFence,
}

impl PreparedLiveReplacement {
    #[expect(
        clippy::too_many_lines,
        reason = "one preparation pass binds every deletion, replacement, archive, event, and READY invariant before the atomic batch"
    )]
    fn new(
        store: &RocksDbCanonicalStore,
        replacement: CanonicalLiveReplacement,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<Self, CanonicalStoreError> {
        validate_activation_identity(store, network_upgrade_activations)?;
        if replacement.expected_fence != store.event_fence() {
            return Err(CanonicalStoreError::live_commit(
                "expected canonical event fence is stale",
            ));
        }
        let anchor = store.append_anchor()?;
        let first = replacement.blocks.first().ok_or_else(|| {
            CanonicalStoreError::live_commit(
                "canonical replacement must contain at least one block",
            )
        })?;
        let first_height = first.block.facts.block_header.height;
        if first_height <= anchor.settled_tip().height
            || first_height > store.ready_evidence.visible_tip.height
        {
            return Err(CanonicalStoreError::live_commit(
                "canonical replacement must begin after the settled tip within the visible suffix",
            ));
        }
        let reverted_range =
            BlockHeightRange::inclusive(first_height, store.ready_evidence.visible_tip.height);
        let displaced_count = inclusive_count(reverted_range)?;
        if displaced_count > store.reorg_policy().reorg_window_blocks() {
            return Err(CanonicalStoreError::live_commit(format!(
                "canonical replacement displaces {displaced_count} blocks; maximum is {}",
                store.reorg_policy().reorg_window_blocks()
            )));
        }
        let parent_height =
            BlockHeight::new(first_height.value().checked_sub(1).ok_or_else(|| {
                CanonicalStoreError::live_commit(
                    "canonical replacement cannot begin at height zero",
                )
            })?);
        let parent_replay = read_persisted_replay(&store.bounded_open.db, parent_height)?;
        let parent_id = BlockId::new(
            parent_height,
            parent_replay.replay.facts().block_header.block_hash,
        );
        let parent_metadata = read_persisted_tip_metadata(&store.bounded_open.db, parent_id)?;
        validate_replacement_blocks(
            store,
            &replacement.blocks,
            parent_id,
            anchor.settled_tip(),
            network_upgrade_activations,
        )?;
        let last = replacement.blocks.last().ok_or_else(|| {
            CanonicalStoreError::live_commit("canonical replacement lost its final block")
        })?;
        let visible_tip = BlockId::new(
            last.block.facts.block_header.height,
            last.block.facts.block_header.block_hash,
        );
        let committed_range = BlockHeightRange::inclusive(first_height, visible_tip.height);
        let visible_lag = visible_tip
            .height
            .value()
            .checked_sub(anchor.settled_tip().height.value())
            .ok_or_else(|| {
                CanonicalStoreError::live_commit("replacement tip precedes settlement")
            })?;
        if visible_lag > store.reorg_policy().reorg_window_blocks() {
            return Err(CanonicalStoreError::live_commit(format!(
                "replacement tip exceeds its {}-block settlement window",
                store.reorg_policy().reorg_window_blocks()
            )));
        }

        let (chain_epoch_id, chain_event_sequence) = next_live_fence(&store.ready_evidence)?;
        let displaced = capture_displaced_blocks(store, reverted_range)?;
        let old_tip_metadata =
            read_persisted_tip_metadata(&store.bounded_open.db, store.ready_evidence.visible_tip)?;
        let deletions = PreparedCanonicalDeletions::new(
            store,
            reverted_range,
            parent_metadata,
            old_tip_metadata,
            &displaced,
        )?;
        let archive = PreparedDisplacedArchiveWrite::new(
            store,
            DisplacedArchiveEvent {
                event_sequence: chain_event_sequence,
                displacement_epoch: chain_epoch_id,
                reverted_range,
                displaced_at: replacement.created_at,
            },
            displaced,
        )?;
        let visible_sequence = replacement_visible_sequence(
            store,
            anchor.settled_tip(),
            parent_height,
            &replacement.blocks,
        )?;
        let ready_evidence = CanonicalStoreReadyEvidence {
            visible_tip,
            visible_epoch: chain_epoch_id,
            visible_event_sequence: chain_event_sequence,
            visible_block_count: visible_sequence.retained_block_count(),
            visible_sequence_digest: visible_sequence.sequence_digest().as_bytes(),
            visible_logical_replay_bytes: visible_sequence.logical_replay_bytes(),
            ..store.ready_evidence
        };
        let encoded_control = encode_ready_store_control_with_revisions(
            store.workload,
            &store.build_plan,
            store.cursor_auth_key,
            &ready_evidence,
            store.control_revisions(),
        )
        .map_err(|source| CanonicalStoreError::live_commit(source.to_string()))?;
        let tip_metadata = last.block.tip_metadata;
        let mut previous_metadata = parent_metadata;
        let mut replacement_rows = Vec::with_capacity(replacement.blocks.len());
        for replacement_block in replacement.blocks {
            let next_metadata = replacement_block.block.tip_metadata;
            replacement_rows.push(PreparedLiveBlockRows::from_block(
                replacement_block.block,
                previous_metadata,
                replacement_block.subtree_roots,
            )?);
            previous_metadata = next_metadata;
        }
        let previous_epoch_id = store.ready_evidence.visible_epoch;
        let outcome = event_fence_from_ready(&ready_evidence);
        Ok(Self {
            deletions,
            replacement_rows,
            archive,
            encoded_epoch: encode_live_chain_epoch(
                visible_tip,
                anchor.settled_tip(),
                tip_metadata,
                replacement.created_at,
            )
            .to_vec(),
            encoded_event: encode_live_event(
                chain_epoch_id,
                previous_epoch_id,
                Some(reverted_range),
                committed_range,
                outcome,
            )
            .to_vec(),
            encoded_control,
            ready_evidence,
            previous_epoch_id,
            reverted_range,
            committed_range,
            outcome,
        })
    }

    fn validate_readback(&self, store: &RocksDbCanonicalStore) -> Result<(), CanonicalStoreError> {
        let replacement_keys = self
            .replacement_rows
            .iter()
            .flat_map(|rows| rows.rows.iter().map(|row| (row.family, row.key.as_slice())))
            .collect::<HashSet<_>>();
        self.deletions
            .validate_readback(&store.bounded_open.db, &replacement_keys)?;
        for rows in &self.replacement_rows {
            rows.validate_readback(&store.bounded_open.db)?;
        }
        self.archive.validate_readback(store)?;
        let observed_epoch = store
            .bounded_open
            .db
            .get_cf(
                &column_family(&store.bounded_open.db, CHAIN_EPOCH_COLUMN_FAMILY)?,
                self.outcome.chain_epoch_id().value().to_be_bytes(),
            )
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "replacement chain epoch readback",
                source,
            })?;
        let observed_event = store
            .bounded_open
            .db
            .get_cf(
                &column_family(&store.bounded_open.db, CHAIN_EVENT_COLUMN_FAMILY)?,
                self.outcome.chain_event_sequence().to_be_bytes(),
            )
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "replacement chain event readback",
                source,
            })?;
        let observed_control = store
            .bounded_open
            .db
            .get(STORE_CONTROL_KEY)
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "replacement READY control readback",
                source,
            })?;
        if observed_epoch.as_deref() != Some(self.encoded_epoch.as_slice())
            || observed_event.as_deref() != Some(self.encoded_event.as_slice())
            || observed_control.as_deref() != Some(self.encoded_control.as_slice())
        {
            return Err(CanonicalStoreError::live_commit(
                "epoch, reorg event, or READY control differs after atomic replacement write",
            ));
        }
        validate_live_replacement_publication(
            &store.bounded_open.db,
            &self.ready_evidence,
            self.previous_epoch_id,
            self.reverted_range,
            self.committed_range,
        )?;
        validate_ready_sequence_checkpoint(
            &store.bounded_open.db,
            store.reorg_policy(),
            &self.ready_evidence,
        )
    }
}

fn validate_activation_identity(
    store: &RocksDbCanonicalStore,
    activations: &NetworkUpgradeActivations,
) -> Result<(), CanonicalStoreError> {
    if !store.activation_table_is_admitted(activations) {
        return Err(CanonicalStoreError::live_commit(
            "live replacement activation table differs from the admitted store identity",
        ));
    }
    Ok(())
}

fn validate_replacement_blocks(
    store: &RocksDbCanonicalStore,
    blocks: &[CanonicalReplacementBlock],
    parent_id: BlockId,
    settled_tip: BlockId,
    activations: &NetworkUpgradeActivations,
) -> Result<(), CanonicalStoreError> {
    let parent_checkpoint = checkpoint_at(store, parent_id, activations)?;
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        parent_id.height,
        &parent_checkpoint.frontiers,
        activations,
    )
    .map_err(|source| {
        CanonicalStoreError::live_commit(format!(
            "replacement parent frontier cannot seed following: {source}"
        ))
    })?;
    let mut expected_parent = parent_id;
    for replacement in blocks {
        validate_live_block(
            store.workload,
            store.build_plan.raw_blob_retention(),
            &replacement.block,
        )?;
        let header = &replacement.block.facts.block_header;
        let expected_height = expected_parent.height.next().ok_or_else(|| {
            CanonicalStoreError::live_commit("replacement height exceeds u32::MAX")
        })?;
        if header.height != expected_height || header.parent_hash != expected_parent.hash {
            return Err(CanonicalStoreError::live_commit(format!(
                "replacement block {} does not extend its admitted parent",
                header.height.value()
            )));
        }
        append_compact_commitments(
            &mut accumulator,
            header.height,
            &replacement.block.compact_block,
        )?;
        let checkpoint = replacement
            .block
            .tree_state_checkpoint
            .as_ref()
            .ok_or_else(|| {
                CanonicalStoreError::live_commit(
                    "replacement block has no exact transition tree checkpoint",
                )
            })?;
        let derived_frontiers = accumulator.validated_frontiers().map_err(|source| {
            CanonicalStoreError::live_commit(format!(
                "replacement commitment frontiers cannot be authenticated: {source}"
            ))
        })?;
        let block_id = BlockId::new(header.height, header.block_hash);
        if checkpoint.block_id != block_id
            || checkpoint.tip_metadata() != replacement.block.tip_metadata
            || checkpoint.frontiers != derived_frontiers
            || accumulator.tip_metadata() != replacement.block.tip_metadata
        {
            return Err(CanonicalStoreError::live_commit(
                "replacement checkpoint is not derived from its admitted parent frontier",
            ));
        }
        expected_parent = block_id;
    }
    if expected_parent.height < settled_tip.height {
        return Err(CanonicalStoreError::live_commit(
            "replacement visible tip precedes the settled anchor",
        ));
    }
    Ok(())
}

fn checkpoint_at(
    store: &RocksDbCanonicalStore,
    target: BlockId,
    activations: &NetworkUpgradeActivations,
) -> Result<CommitmentTreeCheckpoint, CanonicalStoreError> {
    let checkpoint = store
        .tree_state_checkpoint_at_or_before(target.height)?
        .ok_or_else(|| {
            CanonicalStoreError::live_commit("replacement parent has no rewind checkpoint")
        })?;
    if checkpoint.block_id.height == target.height {
        if checkpoint.block_id != target {
            return Err(CanonicalStoreError::live_commit(
                "replacement parent checkpoint has the wrong canonical hash",
            ));
        }
        return Ok(checkpoint);
    }
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        checkpoint.block_id.height,
        &checkpoint.frontiers,
        activations,
    )
    .map_err(|source| CanonicalStoreError::live_commit(source.to_string()))?;
    let mut height = checkpoint.block_id.height.next();
    while height.is_some_and(|height| height <= target.height) {
        let current = height.ok_or_else(|| {
            CanonicalStoreError::live_commit("replacement parent checkpoint height overflow")
        })?;
        let compact = store.compact_block_at(current)?.ok_or_else(|| {
            CanonicalStoreError::live_commit("replacement parent compact block is absent")
        })?;
        append_compact_commitments(&mut accumulator, current, &compact)?;
        height = current.next();
    }
    let header = store.block_header_at(target.height)?.ok_or_else(|| {
        CanonicalStoreError::live_commit("replacement parent block header is absent")
    })?;
    if header.block_hash != target.hash {
        return Err(CanonicalStoreError::live_commit(
            "replacement parent header has the wrong canonical hash",
        ));
    }
    let block_time_seconds = u32::try_from(header.block_time).map_err(|_| {
        CanonicalStoreError::live_commit("replacement parent block time is outside u32")
    })?;
    Ok(CommitmentTreeCheckpoint::new(
        target,
        block_time_seconds,
        accumulator
            .validated_frontiers()
            .map_err(|source| CanonicalStoreError::live_commit(source.to_string()))?,
    ))
}

fn append_compact_commitments(
    accumulator: &mut CommitmentTreeAccumulator,
    height: BlockHeight,
    compact: &CompactBlockArtifact,
) -> Result<(), CanonicalStoreError> {
    let mut sapling = Vec::new();
    let mut orchard = Vec::new();
    let mut ironwood = Vec::new();
    for transaction in compact.transactions() {
        for output in &transaction.data.sapling_outputs {
            sapling.push(commitment_bytes(
                height,
                ShieldedProtocol::Sapling,
                &output.commitment,
            )?);
        }
        for action in &transaction.data.orchard_actions {
            orchard.push(commitment_bytes(
                height,
                ShieldedProtocol::Orchard,
                &action.commitment,
            )?);
        }
        for action in &transaction.data.ironwood_actions {
            ironwood.push(commitment_bytes(
                height,
                ShieldedProtocol::Ironwood,
                &action.commitment,
            )?);
        }
    }
    accumulator
        .append_block_commitments(height, &sapling, &orchard, &ironwood)
        .map_err(|source| CanonicalStoreError::live_commit(source.to_string()))
}

fn commitment_bytes(
    height: BlockHeight,
    protocol: ShieldedProtocol,
    bytes: &[u8],
) -> Result<[u8; 32], CanonicalStoreError> {
    bytes.try_into().map_err(|_| {
        CanonicalStoreError::live_commit(format!(
            "replacement block {} has a {protocol:?} commitment with {} bytes",
            height.value(),
            bytes.len()
        ))
    })
}

fn replacement_visible_sequence(
    store: &RocksDbCanonicalStore,
    settled_tip: BlockId,
    parent_height: BlockHeight,
    blocks: &[CanonicalReplacementBlock],
) -> Result<super::CanonicalSequenceCheckpoint, CanonicalStoreError> {
    let prefix = resume_persisted_sequence_checkpoint(
        &store.bounded_open.db,
        store.ready_evidence.sequence_checkpoint,
        parent_height,
        store.reorg_policy().reorg_window_blocks(),
    )?;
    if prefix.through().height < settled_tip.height {
        return Err(CanonicalStoreError::live_commit(
            "replacement sequence prefix precedes settled anchor",
        ));
    }
    let mut digest =
        CanonicalBlockFactsSequenceDigestBuilder::resume_from_prefix(prefix.sequence_digest());
    let mut count = prefix.retained_block_count();
    let mut logical_bytes = prefix.logical_replay_bytes();
    let mut through = prefix.through();
    for replacement in blocks {
        digest
            .try_append(
                replacement
                    .block
                    .facts
                    .digest(CanonicalBlockFactsDigestVersion::V1),
            )
            .map_err(|source| CanonicalStoreError::live_commit(source.to_string()))?;
        count = count.checked_add(1).ok_or_else(|| {
            CanonicalStoreError::live_commit("replacement block count exceeds u64")
        })?;
        logical_bytes = logical_bytes
            .checked_add(
                u64::try_from(replacement.block.replay_envelope.as_bytes().len()).map_err(
                    |_| CanonicalStoreError::live_commit("replacement replay bytes exceed u64"),
                )?,
            )
            .ok_or_else(|| {
                CanonicalStoreError::live_commit("replacement replay bytes exceed u64")
            })?;
        through = BlockId::new(
            replacement.block.facts.block_header.height,
            replacement.block.facts.block_header.block_hash,
        );
    }
    Ok(super::CanonicalSequenceCheckpoint::from_admitted_parts(
        through,
        count,
        digest.finish(),
        logical_bytes,
    ))
}

fn capture_displaced_blocks(
    store: &RocksDbCanonicalStore,
    reverted_range: BlockHeightRange,
) -> Result<Vec<CanonicalDisplacedBlock>, CanonicalStoreError> {
    reverted_range
        .into_iter()
        .map(|height| {
            let replay = read_family_required(
                &store.bounded_open.db,
                BLOCK_REPLAY_COLUMN_FAMILY,
                &encode_block_position(height),
                "displaced canonical replay read",
            )?;
            let validated = read_persisted_replay(&store.bounded_open.db, height)?;
            let block_id = BlockId::new(height, validated.replay.facts().block_header.block_hash);
            let raw_block_bytes = read_family_optional(
                &store.bounded_open.db,
                BLOCK_BLOB_COLUMN_FAMILY,
                &encode_block_position(height),
                "displaced raw block read",
            )?;
            let final_note_commitment_roots = read_family_optional(
                &store.bounded_open.db,
                BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
                &encode_block_position(height),
                "displaced final roots read",
            )?
            .map(|encoded| {
                decode_block_final_note_commitment_roots(height, block_id.hash, &encoded)
            })
            .transpose()
            .map_err(|source| CanonicalStoreError::live_commit(source.to_string()))?;
            if raw_block_bytes.is_some()
                != store.build_plan.raw_blob_retention().retains_block_blobs()
            {
                return Err(CanonicalStoreError::live_commit(
                    "canonical suffix raw block rows do not match admitted retention",
                ));
            }
            match store.workload {
                CanonicalStoreWorkload::Wallet if final_note_commitment_roots.is_some() => {
                    return Err(CanonicalStoreError::live_commit(
                        "wallet canonical suffix contains explorer-only rows",
                    ));
                }
                CanonicalStoreWorkload::Explorer if final_note_commitment_roots.is_none() => {
                    return Err(CanonicalStoreError::live_commit(
                        "explorer canonical suffix is missing archive facts",
                    ));
                }
                CanonicalStoreWorkload::Wallet | CanonicalStoreWorkload::Explorer => {}
            }
            Ok(CanonicalDisplacedBlock {
                block_id,
                replay_bytes: replay,
                raw_block_bytes,
                final_note_commitment_roots,
            })
        })
        .collect()
}

struct PreparedCanonicalDeletions {
    rows: Vec<(&'static str, Vec<u8>)>,
}

impl PreparedCanonicalDeletions {
    fn new(
        store: &RocksDbCanonicalStore,
        reverted_range: BlockHeightRange,
        parent_metadata: zinder_core::ChainTipMetadata,
        old_tip_metadata: zinder_core::ChainTipMetadata,
        displaced: &[CanonicalDisplacedBlock],
    ) -> Result<Self, CanonicalStoreError> {
        let mut rows = Vec::new();
        for (height, block) in reverted_range.into_iter().zip(displaced) {
            let persisted = read_persisted_replay(&store.bounded_open.db, height)?;
            let facts = persisted.replay.facts();
            if facts.block_header.block_hash != block.block_id.hash {
                return Err(CanonicalStoreError::live_commit(
                    "displaced replay changed while preparing canonical deletion",
                ));
            }
            let height_key = encode_block_position(height).to_vec();
            for family in [
                BLOCK_HEADER_COLUMN_FAMILY,
                BLOCK_REPLAY_COLUMN_FAMILY,
                COMPACT_BLOCK_COLUMN_FAMILY,
                BLOCK_BLOB_COLUMN_FAMILY,
                TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
                BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
            ] {
                rows.push((family, height_key.clone()));
            }
            rows.push((
                BLOCK_HASH_INDEX_COLUMN_FAMILY,
                encode_internal_block_hash(facts.block_header.block_hash).to_vec(),
            ));
            for (index, transaction) in facts.transactions.iter().enumerate() {
                let index = u32::try_from(index).map_err(|_| {
                    CanonicalStoreError::live_commit("displaced transaction index exceeds u32")
                })?;
                rows.push((
                    TRANSACTION_BLOB_COLUMN_FAMILY,
                    encode_transaction_position(height, index).to_vec(),
                ));
                rows.push((
                    TRANSACTION_LOCATION_COLUMN_FAMILY,
                    encode_internal_transaction_id(transaction.public_facts.transaction_id)
                        .to_vec(),
                ));
            }
        }
        for range in required_subtree_root_ranges(parent_metadata, old_tip_metadata)? {
            for index in range {
                let probe = SubtreeRootArtifact::new(
                    range.protocol,
                    index,
                    SubtreeRootHash::from_bytes([0; 32]),
                    BlockHeight::new(0),
                    zinder_core::BlockHash::from_bytes([0; 32]),
                );
                rows.push((
                    SUBTREE_ROOT_COLUMN_FAMILY,
                    encode_subtree_root_key(&probe).to_vec(),
                ));
            }
        }
        Ok(Self { rows })
    }

    fn delete_from(&self, db: &DB, batch: &mut WriteBatch) -> Result<(), CanonicalStoreError> {
        for (family, key) in &self.rows {
            batch.delete_cf(&column_family(db, family)?, key);
        }
        Ok(())
    }

    fn validate_readback(
        &self,
        db: &DB,
        replacement_keys: &HashSet<(&'static str, &[u8])>,
    ) -> Result<(), CanonicalStoreError> {
        for (family, key) in &self.rows {
            if replacement_keys.contains(&(*family, key.as_slice())) {
                continue;
            }
            if read_family_optional(db, family, key, "canonical replacement deletion readback")?
                .is_some()
            {
                return Err(CanonicalStoreError::live_commit(format!(
                    "stale {family} row remains after canonical replacement"
                )));
            }
        }
        Ok(())
    }
}

fn read_family_required(
    db: &DB,
    family: &'static str,
    key: &[u8],
    operation: &'static str,
) -> Result<Vec<u8>, CanonicalStoreError> {
    read_family_optional(db, family, key, operation)?
        .ok_or_else(|| CanonicalStoreError::live_commit(format!("required {family} row is absent")))
}

fn read_family_optional(
    db: &DB,
    family: &'static str,
    key: &[u8],
    operation: &'static str,
) -> Result<Option<Vec<u8>>, CanonicalStoreError> {
    db.get_cf(&column_family(db, family)?, key)
        .map_err(|source| CanonicalStoreError::RocksDbOperation { operation, source })
}

fn inclusive_count(range: BlockHeightRange) -> Result<u32, CanonicalStoreError> {
    range
        .end
        .value()
        .checked_sub(range.start.value())
        .and_then(|difference| difference.checked_add(1))
        .ok_or_else(|| CanonicalStoreError::live_commit("canonical replacement range is empty"))
}

#[cfg(test)]
fn abort_at_live_replacement_failpoint(expected: &str) {
    if std::env::var("ZINDER_TEST_CANONICAL_LIVE_REPLACEMENT_FAILPOINT").as_deref() == Ok(expected)
    {
        std::process::abort();
    }
}

#[cfg(not(test))]
const fn abort_at_live_replacement_failpoint(_expected: &str) {}
