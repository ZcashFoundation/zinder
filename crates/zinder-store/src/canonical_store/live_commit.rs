use rust_rocksdb::{DB, WriteBatch, WriteOptions};
use zinder_core::{
    BlockHeight, BlockId, CanonicalBlockFactsDigestVersion, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion,
    ChainEpochId, CommitmentTreeAccumulator, CommitmentTreeCheckpoint, NetworkUpgradeActivations,
    ShieldedProtocol, SubtreeRootArtifact, TransactionLocation, UnixTimestampMillis,
};

use super::{
    CanonicalBuildBlock, CanonicalBuildSubtreeRoot, CanonicalSequenceCheckpoint,
    CanonicalStoreError, CanonicalStoreReadyEvidence, RocksDbCanonicalStore,
    block_load::{
        encode_block_final_note_commitment_roots, encode_block_hash_location, encode_block_header,
        encode_block_position, encode_transaction_location, encode_transaction_position,
        encode_tree_state_checkpoint, read_persisted_tip_checkpoint, read_persisted_tip_metadata,
        validate_live_block,
    },
    block_replay::{BLOCK_REPLAY_COLUMN_FAMILY, resume_persisted_sequence_checkpoint},
    control::encode_ready_store_control_with_revisions,
    publication::{
        column_family, encode_live_append_event, encode_live_chain_epoch, read_chain_epoch_tips,
        validate_live_append_publication, validate_ready_sequence_checkpoint, validate_settled_tip,
    },
    rocksdb::{
        BLOCK_BLOB_COLUMN_FAMILY, BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
        BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY, CHAIN_EPOCH_COLUMN_FAMILY,
        CHAIN_EVENT_COLUMN_FAMILY, COMPACT_BLOCK_COLUMN_FAMILY, STORE_CONTROL_KEY,
        TRANSACTION_BLOB_COLUMN_FAMILY, TRANSACTION_LOCATION_COLUMN_FAMILY,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    },
    subtree_load::{
        encode_subtree_root_key, encode_subtree_root_value, required_subtree_root_ranges,
    },
};

/// One block-local append prepared for an atomic READY canonical transition.
///
/// The command contains no historical wallet outputs, resolved prevouts, or
/// projection rows. The canonical store validates its parent against the
/// authenticated visible fence and writes only canonical families.
#[derive(Debug)]
pub struct CanonicalLiveAppend {
    expected_fence: CanonicalEventFence,
    block: CanonicalBuildBlock,
    subtree_roots: Vec<CanonicalBuildSubtreeRoot>,
    settled_tip: BlockId,
    created_at: UnixTimestampMillis,
}

impl CanonicalLiveAppend {
    /// Creates one live append command with explicit settlement and observation time.
    #[must_use]
    pub const fn new(
        expected_fence: CanonicalEventFence,
        block: CanonicalBuildBlock,
        subtree_roots: Vec<CanonicalBuildSubtreeRoot>,
        settled_tip: BlockId,
        created_at: UnixTimestampMillis,
    ) -> Self {
        Self {
            expected_fence,
            block,
            subtree_roots,
            settled_tip,
            created_at,
        }
    }
}

/// Authenticated canonical source fence for projection construction and following.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalEventFence {
    chain_epoch_id: ChainEpochId,
    chain_event_sequence: u64,
    visible_tip: BlockId,
    sequence_digest: CanonicalBlockFactsSequenceDigest,
}

impl CanonicalEventFence {
    pub(super) const fn from_persisted_event(
        chain_epoch_id: ChainEpochId,
        chain_event_sequence: u64,
        visible_tip: BlockId,
        visible_block_count: u64,
        sequence_digest: [u8; 32],
    ) -> Self {
        Self {
            chain_epoch_id,
            chain_event_sequence,
            visible_tip,
            sequence_digest: CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                CanonicalBlockFactsSequenceDigestVersion::V1,
                visible_block_count,
                sequence_digest,
            ),
        }
    }
    /// Returns the durable epoch containing this transition.
    #[must_use]
    pub const fn chain_epoch_id(self) -> ChainEpochId {
        self.chain_epoch_id
    }

    /// Returns the durable event sequence that produced the epoch.
    #[must_use]
    pub const fn chain_event_sequence(self) -> u64 {
        self.chain_event_sequence
    }

    /// Returns the visible canonical tip after the commit.
    #[must_use]
    pub const fn visible_tip(self) -> BlockId {
        self.visible_tip
    }

    /// Returns the ordered digest of every fact block through `visible_tip`.
    #[must_use]
    pub const fn sequence_digest(self) -> CanonicalBlockFactsSequenceDigest {
        self.sequence_digest
    }
}

/// Authenticated state required to prepare the next live canonical append.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalAppendAnchor {
    event_fence: CanonicalEventFence,
    settled_tip: BlockId,
    tip_checkpoint: CommitmentTreeCheckpoint,
}

impl CanonicalAppendAnchor {
    /// Returns the exact canonical fence this anchor authenticates.
    #[must_use]
    pub const fn event_fence(&self) -> CanonicalEventFence {
        self.event_fence
    }

    /// Returns the current durable settlement boundary.
    #[must_use]
    pub const fn settled_tip(&self) -> BlockId {
        self.settled_tip
    }

    /// Returns the visible-tip commitment-tree checkpoint used to derive the next block.
    #[must_use]
    pub const fn tip_checkpoint(&self) -> &CommitmentTreeCheckpoint {
        &self.tip_checkpoint
    }
}

impl RocksDbCanonicalStore {
    /// Atomically appends one block-local canonical fact set and publishes its fence.
    ///
    /// The synced `RocksDB` batch contains every block family row, the next
    /// `ChainEpoch`, the corresponding `ChainEvent`, and the updated READY
    /// control record. This path performs no wallet-history or historical
    /// prevout reads. The method consumes the admitted store so an uncertain or
    /// unverified write outcome cannot leave a reusable handle with a stale
    /// fence; only successful readback returns the next admitted store.
    pub fn commit_live_append(
        mut self,
        append: CanonicalLiveAppend,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<(Self, CanonicalEventFence), CanonicalStoreError> {
        let prepared = PreparedLiveAppendCommit::new(&self, append, network_upgrade_activations)?;
        let mut batch = WriteBatch::default();
        prepared
            .block_rows
            .put_into(&self.bounded_open.db, &mut batch)?;
        batch.put_cf(
            &column_family(&self.bounded_open.db, CHAIN_EPOCH_COLUMN_FAMILY)?,
            prepared.outcome.chain_epoch_id.value().to_be_bytes(),
            &prepared.encoded_epoch,
        );
        batch.put_cf(
            &column_family(&self.bounded_open.db, CHAIN_EVENT_COLUMN_FAMILY)?,
            prepared.outcome.chain_event_sequence.to_be_bytes(),
            &prepared.encoded_event,
        );
        batch.put(STORE_CONTROL_KEY, &prepared.encoded_control);
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(false);
        write_options.set_sync(true);
        abort_at_live_commit_failpoint("before_atomic_write");
        self.bounded_open
            .db
            .write_opt(&batch, &write_options)
            .map_err(
                |source| CanonicalStoreError::LiveCommitWriteOutcomeUnknown {
                    path: self.bounded_open.db.path().to_path_buf(),
                    source,
                },
            )?;
        abort_at_live_commit_failpoint("after_atomic_write");
        validate_live_commit_readback(&self.bounded_open.db, self.reorg_policy(), &prepared)
            .map_err(
                |source| CanonicalStoreError::LiveCommitCompletedButUnverified {
                    path: self.bounded_open.db.path().to_path_buf(),
                    reason: source.to_string(),
                },
            )?;
        self.ready_evidence = prepared.ready_evidence;
        Ok((self, prepared.outcome))
    }

    /// Returns the exact source fence authenticated by the READY control record.
    #[must_use]
    pub fn event_fence(&self) -> CanonicalEventFence {
        event_fence_from_ready(&self.ready_evidence)
    }

    /// Reads the authenticated fence, settled tip, and exact visible-tip tree checkpoint.
    pub fn append_anchor(&self) -> Result<CanonicalAppendAnchor, CanonicalStoreError> {
        let event_fence = self.event_fence();
        let (visible_tip, settled_tip) =
            read_chain_epoch_tips(&self.bounded_open.db, event_fence.chain_epoch_id())?;
        if visible_tip != event_fence.visible_tip() {
            return Err(CanonicalStoreError::live_commit(
                "persisted epoch no longer matches the admitted READY fence",
            ));
        }
        let tip_checkpoint = read_persisted_tip_checkpoint(&self.bounded_open.db, visible_tip)?;
        if tip_checkpoint.tip_metadata()
            != read_persisted_tip_metadata(&self.bounded_open.db, visible_tip)?
        {
            return Err(CanonicalStoreError::live_commit(
                "visible-tip checkpoint differs from persisted compact-block tree positions",
            ));
        }
        Ok(CanonicalAppendAnchor {
            event_fence,
            settled_tip,
            tip_checkpoint,
        })
    }
}

struct PreparedLiveAppendCommit {
    block_rows: PreparedLiveBlockRows,
    encoded_epoch: Vec<u8>,
    encoded_event: Vec<u8>,
    encoded_control: Vec<u8>,
    ready_evidence: CanonicalStoreReadyEvidence,
    previous_epoch_id: ChainEpochId,
    outcome: CanonicalEventFence,
}

impl PreparedLiveAppendCommit {
    fn new(
        store: &RocksDbCanonicalStore,
        append: CanonicalLiveAppend,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<Self, CanonicalStoreError> {
        if !store.activation_table_is_admitted(network_upgrade_activations) {
            return Err(CanonicalStoreError::live_commit(
                "live append activation table differs from the admitted store identity",
            ));
        }
        let anchor = store.append_anchor()?;
        if append.expected_fence != anchor.event_fence() {
            return Err(CanonicalStoreError::live_commit(
                "expected canonical event fence is stale",
            ));
        }
        validate_live_block(
            store.workload,
            store.build_plan.raw_blob_retention(),
            &append.block,
        )?;
        let visible_tip = validate_live_block_extension(&store.ready_evidence, &append.block)?;
        validate_live_checkpoint_transition(&anchor, &append.block, network_upgrade_activations)?;
        let tip_metadata = append.block.tip_metadata;
        let previous_epoch_id = store.ready_evidence.visible_epoch;
        let settlement = LiveSettlementTransition {
            reorg_policy: store.reorg_policy(),
            previous_settled_tip: anchor.settled_tip(),
            settled_tip: append.settled_tip,
            visible_tip,
        };
        validate_live_settled_tip(&store.bounded_open.db, &store.ready_evidence, settlement)?;
        let sequence_checkpoint = advance_settled_sequence_checkpoint(
            &store.bounded_open.db,
            &store.ready_evidence,
            settlement,
            &append.block,
        )?;
        let (chain_epoch_id, chain_event_sequence) = next_live_fence(&store.ready_evidence)?;
        let (visible_block_count, visible_sequence_digest, visible_logical_replay_bytes) =
            advance_visible_sequence(&store.ready_evidence, &append.block)?;
        let ready_evidence = CanonicalStoreReadyEvidence {
            visible_tip,
            visible_epoch: chain_epoch_id,
            visible_event_sequence: chain_event_sequence,
            visible_block_count,
            visible_sequence_digest,
            visible_logical_replay_bytes,
            sequence_checkpoint,
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
        Ok(Self {
            block_rows: PreparedLiveBlockRows::from_block(
                append.block,
                anchor.tip_checkpoint().tip_metadata(),
                append.subtree_roots,
            )?,
            encoded_epoch: encode_live_chain_epoch(
                visible_tip,
                append.settled_tip,
                tip_metadata,
                append.created_at,
            )
            .to_vec(),
            encoded_event: encode_live_append_event(
                previous_epoch_id,
                event_fence_from_ready(&ready_evidence),
            )
            .to_vec(),
            encoded_control,
            ready_evidence,
            previous_epoch_id,
            outcome: event_fence_from_ready(&ready_evidence),
        })
    }
}

fn validate_live_checkpoint_transition(
    anchor: &CanonicalAppendAnchor,
    block: &CanonicalBuildBlock,
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<(), CanonicalStoreError> {
    let checkpoint = block.tree_state_checkpoint.as_ref().ok_or_else(|| {
        CanonicalStoreError::live_commit("live append has no transition-tip tree checkpoint")
    })?;
    let mut sapling = Vec::new();
    let mut orchard = Vec::new();
    let mut ironwood = Vec::new();
    for transaction in block.compact_block.transactions() {
        for output in &transaction.data.sapling_outputs {
            sapling.push(compact_commitment_bytes(
                block.facts.block_header.height,
                ShieldedProtocol::Sapling,
                &output.commitment,
            )?);
        }
        for action in &transaction.data.orchard_actions {
            orchard.push(compact_commitment_bytes(
                block.facts.block_header.height,
                ShieldedProtocol::Orchard,
                &action.commitment,
            )?);
        }
        for action in &transaction.data.ironwood_actions {
            ironwood.push(compact_commitment_bytes(
                block.facts.block_header.height,
                ShieldedProtocol::Ironwood,
                &action.commitment,
            )?);
        }
    }
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        anchor.tip_checkpoint.block_id.height,
        &anchor.tip_checkpoint.frontiers,
        network_upgrade_activations,
    )
    .map_err(|source| {
        CanonicalStoreError::live_commit(format!(
            "persisted visible-tip frontier cannot seed live following: {source}"
        ))
    })?;
    accumulator
        .append_block_commitments(
            block.facts.block_header.height,
            &sapling,
            &orchard,
            &ironwood,
        )
        .map_err(|source| {
            CanonicalStoreError::live_commit(format!(
                "live block commitments cannot advance the persisted frontier: {source}"
            ))
        })?;
    let derived_frontiers = accumulator.validated_frontiers().map_err(|source| {
        CanonicalStoreError::live_commit(format!(
            "live block commitment frontiers cannot be authenticated: {source}"
        ))
    })?;
    if accumulator.tip_metadata() != block.tip_metadata || derived_frontiers != checkpoint.frontiers
    {
        return Err(CanonicalStoreError::live_commit(
            "live transition-tip checkpoint is not derived from the persisted frontier",
        ));
    }
    Ok(())
}

fn compact_commitment_bytes(
    height: BlockHeight,
    protocol: ShieldedProtocol,
    bytes: &[u8],
) -> Result<[u8; 32], CanonicalStoreError> {
    bytes.try_into().map_err(|_| {
        CanonicalStoreError::live_commit(format!(
            "block {} has a {protocol:?} commitment with {} bytes",
            height.value(),
            bytes.len()
        ))
    })
}

pub(super) fn event_fence_from_ready(
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> CanonicalEventFence {
    CanonicalEventFence {
        chain_epoch_id: ready_evidence.visible_epoch,
        chain_event_sequence: ready_evidence.visible_event_sequence,
        visible_tip: ready_evidence.visible_tip,
        sequence_digest: CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            ready_evidence.sequence_digest_version,
            ready_evidence.visible_block_count,
            ready_evidence.visible_sequence_digest,
        ),
    }
}

fn advance_visible_sequence(
    ready_evidence: &CanonicalStoreReadyEvidence,
    block: &CanonicalBuildBlock,
) -> Result<(u64, [u8; 32], u64), CanonicalStoreError> {
    let admitted_prefix = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
        ready_evidence.sequence_digest_version,
        ready_evidence.visible_block_count,
        ready_evidence.visible_sequence_digest,
    );
    let mut digest_builder =
        CanonicalBlockFactsSequenceDigestBuilder::resume_from_prefix(admitted_prefix);
    digest_builder
        .try_append(block.facts.digest(CanonicalBlockFactsDigestVersion::V1))
        .map_err(|source| CanonicalStoreError::live_commit(source.to_string()))?;
    let visible_digest = digest_builder.finish();
    let replay_bytes = u64::try_from(block.replay_envelope.as_bytes().len())
        .map_err(|_| CanonicalStoreError::live_commit("live replay bytes exceed u64::MAX"))?;
    let visible_logical_replay_bytes = ready_evidence
        .visible_logical_replay_bytes
        .checked_add(replay_bytes)
        .ok_or_else(|| CanonicalStoreError::live_commit("visible replay bytes exceed u64::MAX"))?;
    Ok((
        visible_digest.block_count(),
        visible_digest.as_bytes(),
        visible_logical_replay_bytes,
    ))
}

fn validate_live_block_extension(
    ready_evidence: &CanonicalStoreReadyEvidence,
    block: &CanonicalBuildBlock,
) -> Result<BlockId, CanonicalStoreError> {
    let previous_tip = ready_evidence.visible_tip;
    let expected_height = previous_tip
        .height
        .next()
        .ok_or_else(|| CanonicalStoreError::live_commit("visible tip height is u32::MAX"))?;
    let block_header = &block.facts.block_header;
    if block_header.height != expected_height || block_header.parent_hash != previous_tip.hash {
        return Err(CanonicalStoreError::live_commit(format!(
            "block {} does not extend visible tip {}",
            block_header.height.value(),
            previous_tip.height.value()
        )));
    }
    Ok(BlockId::new(block_header.height, block_header.block_hash))
}

pub(super) fn next_live_fence(
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<(ChainEpochId, u64), CanonicalStoreError> {
    let chain_epoch_id = ready_evidence
        .visible_epoch
        .value()
        .checked_add(1)
        .map(ChainEpochId::new)
        .ok_or_else(|| CanonicalStoreError::live_commit("chain epoch exceeds u64::MAX"))?;
    let chain_event_sequence = ready_evidence
        .visible_event_sequence
        .checked_add(1)
        .ok_or_else(|| CanonicalStoreError::live_commit("chain event exceeds u64::MAX"))?;
    if chain_event_sequence != chain_epoch_id.value() {
        return Err(CanonicalStoreError::live_commit(
            "chain epoch and event sequence do not share the version-1 fence",
        ));
    }
    Ok((chain_epoch_id, chain_event_sequence))
}

fn validate_live_settled_tip(
    db: &DB,
    ready_evidence: &CanonicalStoreReadyEvidence,
    transition: LiveSettlementTransition,
) -> Result<(), CanonicalStoreError> {
    let LiveSettlementTransition {
        reorg_policy,
        previous_settled_tip,
        settled_tip,
        visible_tip,
    } = transition;
    if ready_evidence.sequence_checkpoint.through() != previous_settled_tip {
        return Err(CanonicalStoreError::live_commit(
            "admitted sequence checkpoint does not match the previous settled tip",
        ));
    }
    if settled_tip.height < previous_settled_tip.height || settled_tip.height > visible_tip.height {
        return Err(CanonicalStoreError::live_commit(
            "settled tip must advance monotonically within visible history",
        ));
    }
    let settled_lag = visible_tip
        .height
        .value()
        .checked_sub(settled_tip.height.value())
        .ok_or_else(|| CanonicalStoreError::live_commit("settled tip exceeds visible tip"))?;
    if settled_lag > reorg_policy.reorg_window_blocks() {
        return Err(CanonicalStoreError::live_commit(format!(
            "visible tip exceeds its {}-block settlement window",
            reorg_policy.reorg_window_blocks()
        )));
    }
    let settlement_advance = settled_tip
        .height
        .value()
        .checked_sub(previous_settled_tip.height.value())
        .ok_or_else(|| CanonicalStoreError::live_commit("settled tip regressed"))?;
    if settlement_advance > reorg_policy.reorg_window_blocks() {
        return Err(CanonicalStoreError::live_commit(format!(
            "settlement advance requires replaying {settlement_advance} blocks; maximum is {}",
            reorg_policy.reorg_window_blocks()
        )));
    }
    if settled_tip.height == previous_settled_tip.height
        && settled_tip.hash != previous_settled_tip.hash
    {
        return Err(CanonicalStoreError::live_commit(
            "unchanged settled height must retain its canonical hash",
        ));
    }
    if settled_tip.height == visible_tip.height {
        if settled_tip.hash != visible_tip.hash {
            return Err(CanonicalStoreError::live_commit(
                "settled visible tip has the wrong canonical hash",
            ));
        }
        return Ok(());
    }
    let mut visible_ready_evidence = *ready_evidence;
    visible_ready_evidence.visible_tip = visible_tip;
    validate_settled_tip(db, &visible_ready_evidence, settled_tip)
}

#[derive(Clone, Copy)]
struct LiveSettlementTransition {
    reorg_policy: super::CanonicalReorgPolicy,
    previous_settled_tip: BlockId,
    settled_tip: BlockId,
    visible_tip: BlockId,
}

fn advance_settled_sequence_checkpoint(
    db: &DB,
    ready_evidence: &CanonicalStoreReadyEvidence,
    transition: LiveSettlementTransition,
    block: &CanonicalBuildBlock,
) -> Result<CanonicalSequenceCheckpoint, CanonicalStoreError> {
    let LiveSettlementTransition {
        reorg_policy,
        settled_tip,
        visible_tip,
        ..
    } = transition;
    let previous = ready_evidence.sequence_checkpoint;
    if settled_tip == previous.through() {
        return Ok(previous);
    }
    let persisted_through = settled_tip.height.min(ready_evidence.visible_tip.height);
    let mut checkpoint = resume_persisted_sequence_checkpoint(
        db,
        previous,
        persisted_through,
        reorg_policy.reorg_window_blocks(),
    )?;
    if settled_tip.height == visible_tip.height {
        let mut digest_builder = CanonicalBlockFactsSequenceDigestBuilder::resume_from_prefix(
            checkpoint.sequence_digest(),
        );
        digest_builder
            .try_append(block.facts.digest(CanonicalBlockFactsDigestVersion::V1))
            .map_err(|source| CanonicalStoreError::live_commit(source.to_string()))?;
        let retained_block_count = checkpoint
            .retained_block_count()
            .checked_add(1)
            .ok_or_else(|| CanonicalStoreError::live_commit("checkpoint count exceeds u64::MAX"))?;
        let replay_bytes = u64::try_from(block.replay_envelope.as_bytes().len())
            .map_err(|_| CanonicalStoreError::live_commit("live replay bytes exceed u64::MAX"))?;
        let logical_replay_bytes = checkpoint
            .logical_replay_bytes()
            .checked_add(replay_bytes)
            .ok_or_else(|| {
                CanonicalStoreError::live_commit("checkpoint replay bytes exceed u64::MAX")
            })?;
        checkpoint = CanonicalSequenceCheckpoint::from_admitted_parts(
            visible_tip,
            retained_block_count,
            digest_builder.finish(),
            logical_replay_bytes,
        );
    }
    if checkpoint.through() != settled_tip {
        return Err(CanonicalStoreError::live_commit(
            "settled sequence checkpoint does not end at the requested settled tip",
        ));
    }
    Ok(checkpoint)
}

pub(super) struct PreparedLiveBlockRows {
    pub(super) rows: Vec<PreparedLiveRow>,
}

impl PreparedLiveBlockRows {
    pub(super) fn from_block(
        block: CanonicalBuildBlock,
        previous_tip_metadata: zinder_core::ChainTipMetadata,
        subtree_roots: Vec<CanonicalBuildSubtreeRoot>,
    ) -> Result<Self, CanonicalStoreError> {
        let CanonicalBuildBlock {
            facts,
            replay_envelope,
            compact_block,
            tip_metadata,
            tree_state_checkpoint,
            block_final_note_commitment_roots,
            transaction_blobs,
            block_blob,
            ..
        } = block;
        let height = facts.block_header.height;
        let height_key = encode_block_position(height).to_vec();
        let mut rows = vec![
            PreparedLiveRow::new(
                BLOCK_HEADER_COLUMN_FAMILY,
                height_key.clone(),
                encode_block_header(&facts.block_header).to_vec(),
            ),
            PreparedLiveRow::new(
                BLOCK_REPLAY_COLUMN_FAMILY,
                height_key.clone(),
                replay_envelope.into_bytes(),
            ),
            PreparedLiveRow::new(
                COMPACT_BLOCK_COLUMN_FAMILY,
                height_key.clone(),
                crate::encode_compact_block_artifact(&compact_block),
            ),
        ];
        let block_hash_record = encode_block_hash_location(facts.block_header.block_hash, height);
        rows.push(PreparedLiveRow::new(
            BLOCK_HASH_INDEX_COLUMN_FAMILY,
            block_hash_record[..32].to_vec(),
            block_hash_record[32..].to_vec(),
        ));
        append_transaction_rows(&mut rows, &facts, transaction_blobs)?;
        if let Some(block_blob) = block_blob {
            rows.push(PreparedLiveRow::new(
                BLOCK_BLOB_COLUMN_FAMILY,
                height_key.clone(),
                block_blob.raw_block_bytes,
            ));
        }
        if let Some(checkpoint) = tree_state_checkpoint {
            rows.push(PreparedLiveRow::new(
                TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
                height_key.clone(),
                encode_tree_state_checkpoint(checkpoint.block_time_seconds, &checkpoint.frontiers),
            ));
        }
        if let Some(roots) = block_final_note_commitment_roots {
            rows.push(PreparedLiveRow::new(
                BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
                height_key,
                encode_block_final_note_commitment_roots(&roots),
            ));
        }
        PreparedLiveSubtreeRoots {
            previous_tip_metadata,
            visible_tip_metadata: tip_metadata,
            completing_block: BlockId::new(height, facts.block_header.block_hash),
            source_roots: subtree_roots,
        }
        .append_to(&mut rows)?;
        Ok(Self { rows })
    }

    pub(super) fn put_into(
        &self,
        db: &DB,
        batch: &mut WriteBatch,
    ) -> Result<(), CanonicalStoreError> {
        for row in &self.rows {
            batch.put_cf(
                &column_family(db, row.family)?,
                &row.key,
                &row.encoded_value,
            );
        }
        Ok(())
    }

    pub(super) fn validate_readback(&self, db: &DB) -> Result<(), CanonicalStoreError> {
        for row in &self.rows {
            let observed = db
                .get_cf(&column_family(db, row.family)?, &row.key)
                .map_err(|source| CanonicalStoreError::RocksDbOperation {
                    operation: "live canonical row readback",
                    source,
                })?;
            if observed.as_deref() != Some(row.encoded_value.as_slice()) {
                return Err(CanonicalStoreError::live_commit(format!(
                    "{} live row differs after atomic write",
                    row.family
                )));
            }
        }
        Ok(())
    }
}

fn append_transaction_rows(
    rows: &mut Vec<PreparedLiveRow>,
    facts: &zinder_core::CanonicalBlockFacts,
    transaction_blobs: Vec<zinder_core::TransactionBlobArtifact>,
) -> Result<(), CanonicalStoreError> {
    let height = facts.block_header.height;
    for (transaction_index, transaction) in facts.transactions.iter().enumerate() {
        let transaction_index = u32::try_from(transaction_index).map_err(|_| {
            CanonicalStoreError::live_commit(format!(
                "block {} transaction count exceeds u32::MAX",
                height.value()
            ))
        })?;
        let location_record = encode_transaction_location(TransactionLocation::new(
            transaction.public_facts.transaction_id,
            height,
            facts.block_header.block_hash,
            transaction_index,
        ));
        rows.push(PreparedLiveRow::new(
            TRANSACTION_LOCATION_COLUMN_FAMILY,
            location_record[..32].to_vec(),
            location_record[32..].to_vec(),
        ));
    }
    for (transaction_index, transaction_blob) in transaction_blobs.into_iter().enumerate() {
        let transaction_index = u32::try_from(transaction_index).map_err(|_| {
            CanonicalStoreError::live_commit(format!(
                "block {} transaction blob count exceeds u32::MAX",
                height.value()
            ))
        })?;
        rows.push(PreparedLiveRow::new(
            TRANSACTION_BLOB_COLUMN_FAMILY,
            encode_transaction_position(height, transaction_index).to_vec(),
            transaction_blob.raw_transaction_bytes,
        ));
    }
    Ok(())
}

struct PreparedLiveSubtreeRoots {
    previous_tip_metadata: zinder_core::ChainTipMetadata,
    visible_tip_metadata: zinder_core::ChainTipMetadata,
    completing_block: BlockId,
    source_roots: Vec<CanonicalBuildSubtreeRoot>,
}

impl PreparedLiveSubtreeRoots {
    fn append_to(self, rows: &mut Vec<PreparedLiveRow>) -> Result<(), CanonicalStoreError> {
        let Self {
            previous_tip_metadata,
            visible_tip_metadata,
            completing_block,
            source_roots,
        } = self;
        let mut source_roots = source_roots.into_iter();
        for required_range in
            required_subtree_root_ranges(previous_tip_metadata, visible_tip_metadata)?
        {
            for expected_index in required_range {
                let source_root = source_roots.next().ok_or_else(|| {
                    CanonicalStoreError::live_commit(format!(
                        "missing {:?} live subtree root at index {}",
                        required_range.protocol,
                        expected_index.value()
                    ))
                })?;
                Self::append_root(
                    rows,
                    completing_block,
                    required_range.protocol,
                    expected_index,
                    source_root,
                )?;
            }
        }
        if let Some(extra) = source_roots.next() {
            return Err(CanonicalStoreError::live_commit(format!(
                "unexpected {:?} live subtree root at index {}",
                extra.protocol,
                extra.subtree_index.value()
            )));
        }
        Ok(())
    }

    fn append_root(
        rows: &mut Vec<PreparedLiveRow>,
        completing_block: BlockId,
        expected_protocol: zinder_core::ShieldedProtocol,
        expected_index: zinder_core::SubtreeRootIndex,
        source_root: CanonicalBuildSubtreeRoot,
    ) -> Result<(), CanonicalStoreError> {
        if source_root.protocol != expected_protocol
            || source_root.subtree_index != expected_index
            || source_root.completing_block_height != completing_block.height
        {
            return Err(CanonicalStoreError::live_commit(format!(
                "live subtree root does not match {expected_protocol:?} index {} completed at height {}",
                expected_index.value(),
                completing_block.height.value()
            )));
        }
        let artifact = SubtreeRootArtifact::new(
            source_root.protocol,
            source_root.subtree_index,
            source_root.root_hash,
            completing_block.height,
            completing_block.hash,
        );
        rows.push(PreparedLiveRow::new(
            super::rocksdb::SUBTREE_ROOT_COLUMN_FAMILY,
            encode_subtree_root_key(&artifact),
            encode_subtree_root_value(&artifact),
        ));
        Ok(())
    }
}

pub(super) struct PreparedLiveRow {
    pub(super) family: &'static str,
    pub(super) key: Vec<u8>,
    pub(super) encoded_value: Vec<u8>,
}

impl PreparedLiveRow {
    fn new(
        family: &'static str,
        key: impl Into<Vec<u8>>,
        encoded_value: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            family,
            key: key.into(),
            encoded_value: encoded_value.into(),
        }
    }
}

fn validate_live_commit_readback(
    db: &DB,
    reorg_policy: super::CanonicalReorgPolicy,
    prepared: &PreparedLiveAppendCommit,
) -> Result<(), CanonicalStoreError> {
    prepared.block_rows.validate_readback(db)?;
    let observed_epoch = db
        .get_cf(
            &column_family(db, CHAIN_EPOCH_COLUMN_FAMILY)?,
            prepared.ready_evidence.visible_epoch.value().to_be_bytes(),
        )
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "live chain epoch readback",
            source,
        })?;
    let observed_event = db
        .get_cf(
            &column_family(db, CHAIN_EVENT_COLUMN_FAMILY)?,
            prepared.ready_evidence.visible_event_sequence.to_be_bytes(),
        )
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "live chain event readback",
            source,
        })?;
    let observed_control =
        db.get(STORE_CONTROL_KEY)
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "live READY control readback",
                source,
            })?;
    if observed_epoch.as_deref() != Some(prepared.encoded_epoch.as_slice())
        || observed_event.as_deref() != Some(prepared.encoded_event.as_slice())
        || observed_control.as_deref() != Some(prepared.encoded_control.as_slice())
    {
        return Err(CanonicalStoreError::live_commit(
            "epoch, event, or READY control differs after atomic live write",
        ));
    }
    validate_live_append_publication(db, &prepared.ready_evidence, prepared.previous_epoch_id)?;
    validate_ready_sequence_checkpoint(db, reorg_policy, &prepared.ready_evidence)
}

#[cfg(test)]
fn abort_at_live_commit_failpoint(expected: &str) {
    if std::env::var("ZINDER_TEST_CANONICAL_LIVE_COMMIT_FAILPOINT").as_deref() == Ok(expected) {
        std::process::abort();
    }
}

#[cfg(not(test))]
const fn abort_at_live_commit_failpoint(_expected: &str) {}

#[cfg(test)]
mod tests {
    use zinder_core::{
        BlockHash, BlockHeight, ChainTipMetadata, SUBTREE_LEAF_COUNT, ShieldedProtocol,
        SubtreeRootHash, SubtreeRootIndex,
    };

    use super::*;

    #[test]
    fn live_subtree_rows_cover_every_newly_completed_index_exactly()
    -> Result<(), Box<dyn std::error::Error>> {
        let height = BlockHeight::new(3);
        let hash = BlockHash::from_bytes([3; 32]);
        let source_root = CanonicalBuildSubtreeRoot {
            protocol: ShieldedProtocol::Sapling,
            subtree_index: SubtreeRootIndex::new(0),
            root_hash: SubtreeRootHash::from_bytes([7; 32]),
            completing_block_height: height,
        };
        let mut rows = Vec::new();

        PreparedLiveSubtreeRoots {
            previous_tip_metadata: ChainTipMetadata::new(0, 0, 0),
            visible_tip_metadata: ChainTipMetadata::new(SUBTREE_LEAF_COUNT, 0, 0),
            completing_block: BlockId::new(height, hash),
            source_roots: vec![source_root],
        }
        .append_to(&mut rows)?;

        let artifact = SubtreeRootArtifact::new(
            source_root.protocol,
            source_root.subtree_index,
            source_root.root_hash,
            height,
            hash,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].family,
            super::super::rocksdb::SUBTREE_ROOT_COLUMN_FAMILY
        );
        assert_eq!(rows[0].key, encode_subtree_root_key(&artifact));
        assert_eq!(rows[0].encoded_value, encode_subtree_root_value(&artifact));
        Ok(())
    }
}
