mod codec;

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    time::Instant,
};

use metrics::Histogram;

use rust_rocksdb::{DB, IngestExternalFileOptions, IteratorMode, Options};
use zinder_core::{
    BlockBlobArtifact, BlockFinalNoteCommitmentRoots, BlockHash, BlockHeight, CanonicalBlockFacts,
    CanonicalBlockFactsDigestVersion, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion,
    CanonicalBlockReplayEnvelope, CanonicalBlockReplayFormatVersion, ChainTipMetadata,
    CommitmentTreeCheckpoint, CommitmentTreeFrontiers, CompactBlockArtifact,
    NetworkUpgradeActivations, SerializedBytesDigest, TransactionBlobArtifact, TransactionLocation,
};
use zinder_rocksdb_bulk_load::{
    FixedRecordSorter, OrderedSstWriter, SstFileEvidence, SstFileSet, fixed_record_capacity,
};

use crate::RawBlobRetention;

use self::codec::{BLOCK_HASH_INDEX_RECORD_LEN, TRANSACTION_LOCATION_RECORD_LEN};

pub(super) use self::codec::{
    BLOCK_HEADER_VALUE_LEN, StoredTreeStateRoots, decode_block_final_note_commitment_roots,
    decode_served_tree_state_checkpoint, decode_stored_tree_state_roots,
    decode_tree_state_checkpoint, encode_block_final_note_commitment_roots,
    encode_block_hash_location, encode_block_header, encode_block_position,
    encode_transaction_location, encode_transaction_position, encode_tree_state_checkpoint,
};

use super::{
    CanonicalSequenceCheckpoint, CanonicalStoreBuildError, CanonicalStoreBuildPlan,
    CanonicalStoreError, CanonicalStoreWorkload, TREE_STATE_CHECKPOINT_STRIDE,
    block_replay::BLOCK_REPLAY_COLUMN_FAMILY,
    construction_manifest::CanonicalConstructionFamilyEvidence,
    frontier_presence_mismatch,
    rocksdb::{
        BLOCK_BLOB_COLUMN_FAMILY, BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
        BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY, COMPACT_BLOCK_COLUMN_FAMILY,
        TRANSACTION_BLOB_COLUMN_FAMILY, TRANSACTION_LOCATION_COLUMN_FAMILY,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    },
};

/// Required tree-state checkpoint coverage for a fresh canonical build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TreeStateCheckpointCoverage {
    /// Store checkpoints on the global cadence and at the build tip.
    CadenceAndTip,
    /// Store a checkpoint for every loaded block.
    EveryBlock,
}

impl TreeStateCheckpointCoverage {
    pub(super) fn requires_checkpoint(
        self,
        height: BlockHeight,
        build_tip_height: BlockHeight,
    ) -> bool {
        match self {
            Self::CadenceAndTip => tree_state_checkpoint_required(height, build_tip_height),
            Self::EveryBlock => true,
        }
    }

    const fn max_persisted_gap(self) -> u32 {
        match self {
            Self::CadenceAndTip => TREE_STATE_CHECKPOINT_STRIDE,
            Self::EveryBlock => 1,
        }
    }
}

pub(super) const CANONICAL_BLOCK_SST_TARGET_LOGICAL_BYTES: u64 = 256 * 1024 * 1024;
pub(super) const REVERSE_INDEX_SORT_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const BLOCK_LOAD_STAGE_DURATION_METRIC: &str =
    "zinder_store_canonical_block_load_stage_duration_seconds";
const BLOCK_LOAD_FINISH_DURATION_METRIC: &str =
    "zinder_store_canonical_block_load_finish_duration_seconds";

/// All source-derived values for one block in a fresh canonical construction.
///
/// This value deliberately does not implement `Clone`: raw transaction and
/// block bytes have one owner and move directly into bounded SST staging.
#[derive(Debug)]
pub struct CanonicalBuildBlock {
    /// Backend-neutral semantic facts used by every later projection.
    pub facts: CanonicalBlockFacts,
    /// Stable version-1 recovery representation of `facts`.
    pub replay_envelope: CanonicalBlockReplayEnvelope,
    /// Wallet protocol bytes derived while the source block is already parsed.
    pub compact_block: CompactBlockArtifact,
    /// Commitment-tree positions after applying this block.
    pub tip_metadata: ChainTipMetadata,
    /// Typed commitment-tree state when required by the build's checkpoint coverage,
    /// or for every live block.
    pub tree_state_checkpoint: Option<CommitmentTreeCheckpoint>,
    /// Per-block final roots retained only by the explorer workload.
    pub block_final_note_commitment_roots: Option<BlockFinalNoteCommitmentRoots>,
    /// Raw transactions in exact block order.
    pub transaction_blobs: Vec<TransactionBlobArtifact>,
    /// Raw block bytes required by the explorer workload.
    pub block_blob: Option<BlockBlobArtifact>,
}

/// Prepared identity and bounded-load measurements for ingested block families.
///
/// This value proves what was staged and that every staged SST was accepted by
/// `RocksDB`; it is not cache-bypassing persisted readback evidence and cannot
/// by itself publish a canonical store as READY.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalBlockLoadEvidence {
    /// First retained block height.
    pub first_height: BlockHeight,
    /// Parent of the first retained block.
    pub first_parent_hash: BlockHash,
    /// First retained block hash.
    pub first_hash: BlockHash,
    /// Last retained block height.
    pub tip_height: BlockHeight,
    /// Last retained block hash.
    pub tip_hash: BlockHash,
    /// Commitment-tree positions after the last retained block.
    pub tip_metadata: ChainTipMetadata,
    /// Number of contiguous source blocks.
    pub block_count: u64,
    /// Number of source transactions.
    pub transaction_count: u64,
    /// Number of height-addressed header rows.
    pub block_header_count: u64,
    /// Number of hash-addressed block-location rows.
    pub block_hash_index_count: u64,
    /// Number of height-addressed semantic replay rows.
    pub block_replay_count: u64,
    /// Number of height-addressed compact block rows.
    pub compact_block_count: u64,
    /// Number of transaction-id-addressed location rows.
    pub transaction_location_count: u64,
    /// Number of position-addressed raw transaction rows.
    pub transaction_blob_count: u64,
    /// Number of height-addressed raw block rows.
    pub block_blob_count: u64,
    /// Number of typed commitment-tree checkpoints, including the history predecessor.
    pub tree_state_checkpoint_count: u64,
    /// Number of height-addressed final note-commitment-root rows.
    pub block_final_note_commitment_roots_count: u64,
    /// Header-family key and value bytes submitted to the SST writers.
    pub block_header_logical_bytes: u64,
    /// Block-hash-index key and value bytes submitted to the SST writers.
    pub block_hash_index_logical_bytes: u64,
    /// Replay-family key and value bytes submitted to the SST writers.
    pub block_replay_logical_bytes: u64,
    /// Compact-block key and value bytes submitted to the SST writers.
    pub compact_block_logical_bytes: u64,
    /// Transaction-location key and value bytes submitted to the SST writers.
    pub transaction_location_logical_bytes: u64,
    /// Transaction-blob key and value bytes submitted to the SST writers.
    pub transaction_blob_logical_bytes: u64,
    /// Block-blob key and value bytes submitted to the SST writers.
    pub block_blob_logical_bytes: u64,
    /// Tree-checkpoint key and value bytes submitted to the SST writers.
    pub tree_state_checkpoint_logical_bytes: u64,
    /// Final-root key and value bytes submitted to the SST writers.
    pub block_final_note_commitment_roots_logical_bytes: u64,
    /// Total key and value bytes submitted to the SST writers.
    pub logical_bytes: u64,
    /// Physical bytes occupied by every staged SST.
    pub sst_file_bytes: u64,
    /// Number of staged SST files.
    pub sst_file_count: u64,
    /// Canonical replay-envelope contract validated for every block.
    pub replay_format_version: CanonicalBlockReplayFormatVersion,
    /// Semantic fact-digest contract validated for every block.
    pub block_digest_version: CanonicalBlockFactsDigestVersion,
    /// Ordered fact-sequence digest contract.
    pub sequence_digest_version: CanonicalBlockFactsSequenceDigestVersion,
    /// Ordered digest of every block's semantic facts.
    pub sequence_digest: CanonicalBlockFactsSequenceDigest,
}

pub(super) struct PreparedCanonicalBlockLoad {
    families: Vec<PreparedColumnFamily>,
    pub(super) evidence: CanonicalBlockLoadEvidence,
    trusted_fresh_evidence: CanonicalTrustedFreshBlockEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CanonicalTrustedFreshBlockEvidence {
    pub(super) family_evidence: Vec<CanonicalConstructionFamilyEvidence>,
    pub(super) retained_sequence_checkpoints: VecDeque<CanonicalSequenceCheckpoint>,
    pub(super) logical_replay_bytes: u64,
}

/// Per-file physical evidence emitted by the bounded staging writers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CanonicalStagedSstEvidence {
    pub(super) family: &'static str,
    pub(super) file: SstFileEvidence,
}

#[derive(Clone, Copy)]
pub(super) struct CanonicalBlockSstConfig<'options> {
    pub(super) staging_path: &'options Path,
    pub(super) options: &'options Options,
    pub(super) workload: CanonicalStoreWorkload,
    pub(super) build_plan: &'options CanonicalStoreBuildPlan,
    /// The activation table the build plan fingerprints, which decides the
    /// pools each checkpoint carries a frontier for.
    pub(super) network_upgrade_activations: &'options NetworkUpgradeActivations,
    pub(super) tree_state_checkpoint_coverage: TreeStateCheckpointCoverage,
    pub(super) sst_target_logical_bytes: u64,
    pub(super) reverse_index_sort_memory_bytes: usize,
}

struct PreparedColumnFamily {
    name: &'static str,
    paths: Vec<PathBuf>,
    staged_ssts: Vec<CanonicalStagedSstEvidence>,
    family_evidence: CanonicalConstructionFamilyEvidence,
}

pub(super) fn write_canonical_block_ssts<SourceError>(
    config: CanonicalBlockSstConfig<'_>,
    blocks: impl IntoIterator<Item = Result<CanonicalBuildBlock, SourceError>>,
) -> Result<PreparedCanonicalBlockLoad, CanonicalStoreBuildError<SourceError>> {
    let mut stager = CanonicalBlockSstStager::new(config)?;
    for source_block in blocks {
        let block = source_block.map_err(|source| CanonicalStoreBuildError::Source { source })?;
        stager.stage(block)?;
    }
    Ok(stager.finish()?)
}

struct CanonicalBlockSstStager<'options> {
    config: CanonicalBlockSstConfig<'options>,
    header_writer: OrderedSstWriter<'options>,
    replay_writer: OrderedSstWriter<'options>,
    compact_writer: OrderedSstWriter<'options>,
    transaction_blob_writer: OrderedSstWriter<'options>,
    block_blob_writer: OrderedSstWriter<'options>,
    tree_state_checkpoint_writer: OrderedSstWriter<'options>,
    block_final_note_commitment_roots_writer: OrderedSstWriter<'options>,
    block_hash_sorter: FixedRecordSorter<BLOCK_HASH_INDEX_RECORD_LEN>,
    transaction_location_sorter: FixedRecordSorter<TRANSACTION_LOCATION_RECORD_LEN>,
    sequence: Option<BlockSequence>,
    predecessor_checkpoint_logical_bytes: u64,
    stage_timers: BlockStageTimers,
}

/// Per-block durations of the serial store stage, resolved once per load.
struct BlockStageTimers {
    validate: Histogram,
    encode: Histogram,
    sequence: Histogram,
    write: Histogram,
}

impl BlockStageTimers {
    fn register() -> Self {
        Self {
            validate: metrics::histogram!(BLOCK_LOAD_STAGE_DURATION_METRIC, "stage" => "validate"),
            encode: metrics::histogram!(BLOCK_LOAD_STAGE_DURATION_METRIC, "stage" => "encode"),
            sequence: metrics::histogram!(BLOCK_LOAD_STAGE_DURATION_METRIC, "stage" => "sequence"),
            write: metrics::histogram!(BLOCK_LOAD_STAGE_DURATION_METRIC, "stage" => "write"),
        }
    }
}

/// Runs one once-per-load finish step for `family` and records the duration
/// of a successful step.
fn timed_finish_step<T, E>(
    step: &'static str,
    family: &'static str,
    finish: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let started_at = Instant::now();
    let finished = finish()?;
    metrics::histogram!(BLOCK_LOAD_FINISH_DURATION_METRIC, "step" => step, "family" => family)
        .record(started_at.elapsed());
    Ok(finished)
}

impl<'options> CanonicalBlockSstStager<'options> {
    fn new(config: CanonicalBlockSstConfig<'options>) -> Result<Self, CanonicalStoreError> {
        if config.sst_target_logical_bytes == 0 {
            return Err(CanonicalStoreError::block_load_sequence(
                "SST target logical bytes must be greater than zero",
            ));
        }
        let block_hash_capacity = fixed_record_capacity::<BLOCK_HASH_INDEX_RECORD_LEN>(
            config.reverse_index_sort_memory_bytes,
        )?;
        let transaction_location_capacity = fixed_record_capacity::<TRANSACTION_LOCATION_RECORD_LEN>(
            config.reverse_index_sort_memory_bytes,
        )?;
        let mut tree_state_checkpoint_writer = config.ordered_writer("tree-state-checkpoint")?;
        let predecessor = config.build_plan.history_predecessor();
        let predecessor_key = encode_block_position(predecessor.block_id.height);
        let predecessor_value =
            encode_tree_state_checkpoint(predecessor.block_time_seconds, &predecessor.frontiers);
        tree_state_checkpoint_writer.put(&predecessor_key, &predecessor_value)?;
        let predecessor_checkpoint_logical_bytes =
            checked_row_bytes(predecessor_key.len(), predecessor_value.len())?;
        Ok(Self {
            header_writer: config.ordered_writer("block-header")?,
            replay_writer: config.ordered_writer("block-replay")?,
            compact_writer: config.ordered_writer("compact-block")?,
            transaction_blob_writer: config.ordered_writer("transaction-blob")?,
            block_blob_writer: config.ordered_writer("block-blob")?,
            tree_state_checkpoint_writer,
            block_final_note_commitment_roots_writer: config
                .ordered_writer("block-final-note-commitment-roots")?,
            block_hash_sorter: FixedRecordSorter::new(
                config.staging_path,
                "block-hash-index",
                block_hash_capacity,
            )?,
            transaction_location_sorter: FixedRecordSorter::new(
                config.staging_path,
                "transaction-location",
                transaction_location_capacity,
            )?,
            config,
            sequence: None,
            predecessor_checkpoint_logical_bytes,
            stage_timers: BlockStageTimers::register(),
        })
    }

    fn stage(&mut self, block: CanonicalBuildBlock) -> Result<(), CanonicalStoreError> {
        let started_at = Instant::now();
        validate_build_block(
            self.config.workload,
            self.config.build_plan,
            self.config.network_upgrade_activations,
            self.config.tree_state_checkpoint_coverage,
            &block,
        )?;
        let validated_at = Instant::now();
        self.stage_timers
            .validate
            .record(validated_at.duration_since(started_at));
        let encoded = EncodedBlockValues::encode(&block);
        let encoded_at = Instant::now();
        self.stage_timers
            .encode
            .record(encoded_at.duration_since(validated_at));
        let row = BlockSequenceRow::from_validated_block(&block, &encoded)?;
        if let Some(sequence) = &mut self.sequence {
            sequence.append(row)?;
        } else {
            let retained_checkpoint_count = usize::try_from(
                u64::from(self.config.build_plan.reorg_policy().reorg_window_blocks()) + 1,
            )
            .map_err(|_| CanonicalStoreError::block_load_sequence("reorg window exceeds usize"))?;
            self.sequence = Some(BlockSequence::new(row, retained_checkpoint_count)?);
        }
        let sequenced_at = Instant::now();
        self.stage_timers
            .sequence
            .record(sequenced_at.duration_since(encoded_at));
        write_build_block(
            block,
            encoded,
            &mut self.header_writer,
            &mut self.replay_writer,
            &mut self.compact_writer,
            &mut self.transaction_blob_writer,
            &mut self.block_blob_writer,
            &mut self.tree_state_checkpoint_writer,
            &mut self.block_final_note_commitment_roots_writer,
            &mut self.block_hash_sorter,
            &mut self.transaction_location_sorter,
        )?;
        self.stage_timers.write.record(sequenced_at.elapsed());
        Ok(())
    }

    fn finish(self) -> Result<PreparedCanonicalBlockLoad, CanonicalStoreError> {
        let sequence = self.sequence.ok_or_else(|| {
            CanonicalStoreError::block_load_sequence(
                "a canonical block load must contain at least one block",
            )
        })?;
        let options = self.config.options;
        let sst_target_logical_bytes = self.config.sst_target_logical_bytes;
        let artifacts = CanonicalBlockSstArtifacts {
            header: timed_finish_step("sst_finish", BLOCK_HEADER_COLUMN_FAMILY, || {
                self.header_writer.finish()
            })?,
            block_hash: timed_finish_step("sort_merge", BLOCK_HASH_INDEX_COLUMN_FAMILY, || {
                self.block_hash_sorter
                    .finish::<32>(options, sst_target_logical_bytes)
            })?,
            replay: timed_finish_step("sst_finish", BLOCK_REPLAY_COLUMN_FAMILY, || {
                self.replay_writer.finish()
            })?,
            compact: timed_finish_step("sst_finish", COMPACT_BLOCK_COLUMN_FAMILY, || {
                self.compact_writer.finish()
            })?,
            transaction_location: timed_finish_step(
                "sort_merge",
                TRANSACTION_LOCATION_COLUMN_FAMILY,
                || {
                    self.transaction_location_sorter
                        .finish::<32>(options, sst_target_logical_bytes)
                },
            )?,
            transaction_blob: timed_finish_step(
                "sst_finish",
                TRANSACTION_BLOB_COLUMN_FAMILY,
                || self.transaction_blob_writer.finish(),
            )?,
            block_blob: timed_finish_step("sst_finish", BLOCK_BLOB_COLUMN_FAMILY, || {
                self.block_blob_writer.finish()
            })?,
            tree_state_checkpoint: timed_finish_step(
                "sst_finish",
                TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
                || self.tree_state_checkpoint_writer.finish(),
            )?,
            block_final_note_commitment_roots: timed_finish_step(
                "sst_finish",
                BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
                || self.block_final_note_commitment_roots_writer.finish(),
            )?,
        };
        prepare_canonical_block_load(
            sequence,
            artifacts,
            self.predecessor_checkpoint_logical_bytes,
        )
    }
}

impl<'options> CanonicalBlockSstConfig<'options> {
    fn ordered_writer(
        self,
        artifact_label: &'static str,
    ) -> Result<OrderedSstWriter<'options>, CanonicalStoreError> {
        Ok(OrderedSstWriter::new(
            self.staging_path,
            artifact_label,
            self.options,
            self.sst_target_logical_bytes,
        )?)
    }
}

struct CanonicalBlockSstArtifacts {
    header: SstFileSet,
    block_hash: SstFileSet,
    replay: SstFileSet,
    compact: SstFileSet,
    transaction_location: SstFileSet,
    transaction_blob: SstFileSet,
    block_blob: SstFileSet,
    tree_state_checkpoint: SstFileSet,
    block_final_note_commitment_roots: SstFileSet,
}

fn prepare_canonical_block_load(
    sequence: BlockSequence,
    artifacts: CanonicalBlockSstArtifacts,
    predecessor_checkpoint_logical_bytes: u64,
) -> Result<PreparedCanonicalBlockLoad, CanonicalStoreError> {
    let CanonicalBlockSstArtifacts {
        header,
        block_hash,
        replay,
        compact,
        transaction_location,
        transaction_blob,
        block_blob,
        tree_state_checkpoint,
        block_final_note_commitment_roots,
    } = artifacts;
    let sst_file_bytes = [
        header.file_bytes,
        block_hash.file_bytes,
        replay.file_bytes,
        compact.file_bytes,
        transaction_location.file_bytes,
        transaction_blob.file_bytes,
        block_blob.file_bytes,
        tree_state_checkpoint.file_bytes,
        block_final_note_commitment_roots.file_bytes,
    ]
    .into_iter()
    .try_fold(0_u64, checked_add_sst_bytes)?;
    let families = vec![
        PreparedColumnFamily::new(BLOCK_HEADER_COLUMN_FAMILY, header),
        PreparedColumnFamily::new(BLOCK_HASH_INDEX_COLUMN_FAMILY, block_hash),
        PreparedColumnFamily::new(BLOCK_REPLAY_COLUMN_FAMILY, replay),
        PreparedColumnFamily::new(COMPACT_BLOCK_COLUMN_FAMILY, compact),
        PreparedColumnFamily::new(TRANSACTION_LOCATION_COLUMN_FAMILY, transaction_location),
        PreparedColumnFamily::new(TRANSACTION_BLOB_COLUMN_FAMILY, transaction_blob),
        PreparedColumnFamily::new(BLOCK_BLOB_COLUMN_FAMILY, block_blob),
        PreparedColumnFamily::new(TREE_STATE_CHECKPOINT_COLUMN_FAMILY, tree_state_checkpoint),
        PreparedColumnFamily::new(
            BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
            block_final_note_commitment_roots,
        ),
    ];
    let sst_file_count = families.iter().try_fold(0_u64, |count, family| {
        let family_count = u64::try_from(family.paths.len()).map_err(|_| {
            CanonicalStoreError::block_load_sequence("SST file count exceeds u64::MAX")
        })?;
        count.checked_add(family_count).ok_or_else(|| {
            CanonicalStoreError::block_load_sequence("SST file count exceeds u64::MAX")
        })
    })?;
    let family_evidence = families
        .iter()
        .map(|family| family.family_evidence.clone())
        .collect();

    let finished_sequence = sequence.finish(
        sst_file_bytes,
        sst_file_count,
        predecessor_checkpoint_logical_bytes,
    )?;
    Ok(PreparedCanonicalBlockLoad {
        families,
        evidence: finished_sequence.evidence,
        trusted_fresh_evidence: CanonicalTrustedFreshBlockEvidence {
            family_evidence,
            retained_sequence_checkpoints: finished_sequence.retained_sequence_checkpoints,
            logical_replay_bytes: finished_sequence.logical_replay_bytes,
        },
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "each argument is one independently staged physical block family"
)]
fn write_build_block(
    block: CanonicalBuildBlock,
    encoded: EncodedBlockValues,
    header_writer: &mut OrderedSstWriter<'_>,
    replay_writer: &mut OrderedSstWriter<'_>,
    compact_writer: &mut OrderedSstWriter<'_>,
    transaction_blob_writer: &mut OrderedSstWriter<'_>,
    block_blob_writer: &mut OrderedSstWriter<'_>,
    tree_state_checkpoint_writer: &mut OrderedSstWriter<'_>,
    block_final_note_commitment_roots_writer: &mut OrderedSstWriter<'_>,
    block_hash_sorter: &mut FixedRecordSorter<BLOCK_HASH_INDEX_RECORD_LEN>,
    transaction_location_sorter: &mut FixedRecordSorter<TRANSACTION_LOCATION_RECORD_LEN>,
) -> Result<(), CanonicalStoreError> {
    let CanonicalBuildBlock {
        facts,
        replay_envelope,
        transaction_blobs,
        block_blob,
        ..
    } = block;
    let height = facts.block_header.height;
    let height_key = encode_block_position(height);
    let header_value = encode_block_header(&facts.block_header);
    header_writer.put(&height_key, &header_value)?;
    replay_writer.put(&height_key, replay_envelope.as_bytes())?;
    compact_writer.put(&height_key, &encoded.compact_block)?;

    let block_hash_record = encode_block_hash_location(facts.block_header.block_hash, height);
    block_hash_sorter.push(block_hash_record)?;

    for (transaction_index, transaction) in facts.transactions.iter().enumerate() {
        let transaction_index = u32::try_from(transaction_index).map_err(|_| {
            CanonicalStoreError::block_load_sequence(format!(
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
        transaction_location_sorter.push(location_record)?;
    }

    for (transaction_index, transaction_blob) in transaction_blobs.into_iter().enumerate() {
        let transaction_index = u32::try_from(transaction_index).map_err(|_| {
            CanonicalStoreError::block_load_sequence(format!(
                "block {} transaction blob count exceeds u32::MAX",
                height.value()
            ))
        })?;
        let transaction_key = encode_transaction_position(height, transaction_index);
        transaction_blob_writer.put(&transaction_key, &transaction_blob.raw_transaction_bytes)?;
    }

    if let Some(block_blob) = block_blob {
        block_blob_writer.put(&height_key, &block_blob.raw_block_bytes)?;
    }
    if let Some(checkpoint_value) = encoded.tree_state_checkpoint {
        tree_state_checkpoint_writer.put(&height_key, &checkpoint_value)?;
    }
    if let Some(roots_value) = encoded.block_final_note_commitment_roots {
        block_final_note_commitment_roots_writer.put(&height_key, &roots_value)?;
    }
    Ok(())
}

/// Stored values encoded once per block for both the logical-byte accounting
/// and the SST writers.
struct EncodedBlockValues {
    compact_block: Vec<u8>,
    tree_state_checkpoint: Option<Vec<u8>>,
    block_final_note_commitment_roots: Option<Vec<u8>>,
}

impl EncodedBlockValues {
    fn encode(block: &CanonicalBuildBlock) -> Self {
        Self {
            compact_block: crate::encode_compact_block_artifact(&block.compact_block),
            tree_state_checkpoint: block.tree_state_checkpoint.as_ref().map(|checkpoint| {
                encode_tree_state_checkpoint(checkpoint.block_time_seconds, &checkpoint.frontiers)
            }),
            block_final_note_commitment_roots: block
                .block_final_note_commitment_roots
                .as_ref()
                .map(encode_block_final_note_commitment_roots),
        }
    }
}

pub(super) fn ingest_canonical_block_ssts(
    db: &DB,
    prepared: PreparedCanonicalBlockLoad,
) -> Result<IngestedCanonicalBlockLoad, CanonicalStoreError> {
    let mut staged_ssts = Vec::new();
    for family in prepared.families {
        if family.paths.is_empty() {
            continue;
        }
        let column_family = db.cf_handle(family.name).ok_or_else(|| {
            CanonicalStoreError::block_load_sequence(format!(
                "{} column family is absent",
                family.name
            ))
        })?;
        let mut options = IngestExternalFileOptions::default();
        options.set_move_files(true);
        options.set_snapshot_consistency(true);
        options.set_allow_global_seqno(false);
        options.set_allow_blocking_flush(false);
        timed_finish_step("ingest", family.name, || {
            db.ingest_external_file_cf_opts(&column_family, &options, family.paths)
        })
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "canonical block-family external SST ingestion",
            source,
        })?;
        staged_ssts.extend(family.staged_ssts);
    }
    Ok(IngestedCanonicalBlockLoad {
        evidence: prepared.evidence,
        staged_ssts,
        trusted_fresh_evidence: prepared.trusted_fresh_evidence,
    })
}

pub(super) struct IngestedCanonicalBlockLoad {
    pub(super) evidence: CanonicalBlockLoadEvidence,
    pub(super) staged_ssts: Vec<CanonicalStagedSstEvidence>,
    pub(super) trusted_fresh_evidence: CanonicalTrustedFreshBlockEvidence,
}

pub(super) fn canonical_block_families_are_empty(db: &DB) -> Result<bool, CanonicalStoreError> {
    for name in [
        BLOCK_HEADER_COLUMN_FAMILY,
        BLOCK_HASH_INDEX_COLUMN_FAMILY,
        BLOCK_REPLAY_COLUMN_FAMILY,
        COMPACT_BLOCK_COLUMN_FAMILY,
        TRANSACTION_LOCATION_COLUMN_FAMILY,
        TRANSACTION_BLOB_COLUMN_FAMILY,
        BLOCK_BLOB_COLUMN_FAMILY,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
        BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
    ] {
        let column_family = db.cf_handle(name).ok_or_else(|| {
            CanonicalStoreError::block_load_sequence(format!("{name} column family is absent"))
        })?;
        let mut iterator = db.raw_iterator_cf(&column_family);
        iterator.seek_to_first();
        if iterator.valid() {
            return Ok(false);
        }
        iterator
            .status()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "canonical block-family empty-state validation",
                source,
            })?;
    }
    Ok(true)
}

pub(super) fn validate_persisted_commitment_tree_families(
    db: &DB,
    workload: CanonicalStoreWorkload,
    build_plan: &CanonicalStoreBuildPlan,
    tree_state_checkpoint_coverage: TreeStateCheckpointCoverage,
    evidence: &CanonicalBlockLoadEvidence,
) -> Result<(), CanonicalStoreError> {
    validate_persisted_tree_state_checkpoints(
        db,
        build_plan,
        tree_state_checkpoint_coverage,
        evidence,
    )?;
    validate_persisted_block_final_note_commitment_roots(db, workload, build_plan, evidence)
}

pub(super) fn validate_source_tip_checkpoint(
    db: &DB,
    build_plan: &CanonicalStoreBuildPlan,
    evidence: &CanonicalBlockLoadEvidence,
    source_checkpoint: &CommitmentTreeCheckpoint,
) -> Result<(), CanonicalStoreError> {
    let expected_tip = build_plan.build_tip();
    if source_checkpoint.block_id != expected_tip
        || source_checkpoint.tip_metadata() != evidence.tip_metadata
    {
        return Err(CanonicalStoreError::source_tip_checkpoint(
            "source checkpoint identity or tree positions differ from the fixed canonical tip",
        ));
    }
    let family = db
        .cf_handle(TREE_STATE_CHECKPOINT_COLUMN_FAMILY)
        .ok_or_else(|| {
            CanonicalStoreError::source_tip_checkpoint(
                "tree_state_checkpoint column family is absent",
            )
        })?;
    let encoded_checkpoint = db
        .get_cf(&family, encode_block_position(expected_tip.height))
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "read fixed-tip tree-state checkpoint",
            source,
        })?
        .ok_or_else(|| {
            CanonicalStoreError::source_tip_checkpoint("fixed-tip tree-state checkpoint is absent")
        })?;
    let (block_time_seconds, frontiers) = decode_tree_state_checkpoint(&encoded_checkpoint)
        .map_err(|source| {
            CanonicalStoreError::source_tip_checkpoint(format!(
                "fixed-tip tree-state checkpoint is invalid: {source}"
            ))
        })?;
    if block_time_seconds != source_checkpoint.block_time_seconds
        || frontiers != source_checkpoint.frontiers
    {
        return Err(CanonicalStoreError::source_tip_checkpoint(
            "persisted fixed-tip checkpoint differs from the source-authenticated checkpoint",
        ));
    }
    Ok(())
}

pub(super) fn read_persisted_tip_metadata(
    db: &DB,
    tip: zinder_core::BlockId,
) -> Result<ChainTipMetadata, CanonicalStoreError> {
    let family = db.cf_handle(COMPACT_BLOCK_COLUMN_FAMILY).ok_or_else(|| {
        CanonicalStoreError::block_load_sequence("compact_block column family is absent")
    })?;
    let encoded_compact_block = db
        .get_cf(&family, encode_block_position(tip.height))
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "read published tip compact block",
            source,
        })?
        .ok_or_else(|| {
            CanonicalStoreError::block_load_sequence("published tip compact block is absent")
        })?;
    let compact =
        crate::decode_compact_block_artifact(encoded_compact_block.as_slice()).map_err(|_| {
            CanonicalStoreError::block_load_sequence("published tip compact block is invalid")
        })?;
    let metadata = compact.chain_metadata();
    if compact.height() != tip.height || compact.block_hash() != tip.hash {
        return Err(CanonicalStoreError::block_load_sequence(
            "published tip compact block has the wrong canonical identity",
        ));
    }
    Ok(ChainTipMetadata::new(
        metadata.sapling_commitment_tree_size,
        metadata.orchard_commitment_tree_size,
        metadata.ironwood_commitment_tree_size,
    ))
}

pub(super) fn read_persisted_tip_checkpoint(
    db: &DB,
    tip: zinder_core::BlockId,
) -> Result<CommitmentTreeCheckpoint, CanonicalStoreError> {
    let family = db
        .cf_handle(TREE_STATE_CHECKPOINT_COLUMN_FAMILY)
        .ok_or_else(|| {
            CanonicalStoreError::live_commit("tree_state_checkpoint column family is absent")
        })?;
    let encoded_checkpoint = db
        .get_cf(&family, encode_block_position(tip.height))
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "read live append tip checkpoint",
            source,
        })?
        .ok_or_else(|| {
            CanonicalStoreError::live_commit(
                "authenticated visible tip has no persisted tree-state checkpoint",
            )
        })?;
    let (block_time_seconds, frontiers) = decode_tree_state_checkpoint(&encoded_checkpoint)
        .map_err(|source| {
            CanonicalStoreError::live_commit(format!(
                "authenticated visible-tip checkpoint is invalid: {source}"
            ))
        })?;
    Ok(CommitmentTreeCheckpoint::new(
        tip,
        block_time_seconds,
        frontiers,
    ))
}

fn validate_persisted_tree_state_checkpoints(
    db: &DB,
    build_plan: &CanonicalStoreBuildPlan,
    coverage: TreeStateCheckpointCoverage,
    evidence: &CanonicalBlockLoadEvidence,
) -> Result<(), CanonicalStoreError> {
    let family = db
        .cf_handle(TREE_STATE_CHECKPOINT_COLUMN_FAMILY)
        .ok_or_else(|| {
            CanonicalStoreError::block_load_sequence(
                "tree_state_checkpoint column family is absent",
            )
        })?;
    let predecessor = build_plan.history_predecessor();
    let mut previous_height: Option<BlockHeight> = None;
    let mut row_count = 0_u64;
    let mut tip_frontiers = None;
    for row in db.iterator_cf(&family, IteratorMode::Start) {
        let (key, encoded_checkpoint) =
            row.map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "tree-state checkpoint persisted readback",
                source,
            })?;
        let height = zinder_core::wire::decode_height_key_ascending(&key).map_err(|source| {
            CanonicalStoreError::block_load_sequence(format!(
                "tree_state_checkpoint has an invalid height key: {source}"
            ))
        })?;
        let (block_time_seconds, frontiers) = decode_tree_state_checkpoint(&encoded_checkpoint)
            .map_err(|source| {
                CanonicalStoreError::block_load_sequence(format!(
                    "tree_state_checkpoint at height {} is invalid: {source}",
                    height.value()
                ))
            })?;
        match previous_height {
            None if height != predecessor.block_id.height
                || block_time_seconds != predecessor.block_time_seconds
                || frontiers != predecessor.frontiers =>
            {
                return Err(CanonicalStoreError::block_load_sequence(
                    "first persisted tree-state checkpoint does not match the history predecessor",
                ));
            }
            Some(previous) => validate_persisted_checkpoint_step(
                coverage,
                previous,
                height,
                build_plan.build_tip().height,
            )?,
            None => {}
        }
        if height == build_plan.build_tip().height {
            tip_frontiers = Some(frontiers);
        }
        previous_height = Some(height);
        row_count = row_count.checked_add(1).ok_or_else(|| {
            CanonicalStoreError::block_load_sequence(
                "persisted tree-state checkpoint count exceeds u64::MAX",
            )
        })?;
    }
    validate_tree_state_checkpoint_readback(
        row_count,
        previous_height,
        tip_frontiers.as_ref(),
        build_plan,
        evidence,
    )
}

fn validate_persisted_checkpoint_step(
    coverage: TreeStateCheckpointCoverage,
    previous: BlockHeight,
    height: BlockHeight,
    build_tip_height: BlockHeight,
) -> Result<(), CanonicalStoreError> {
    let gap = height
        .value()
        .checked_sub(previous.value())
        .ok_or_else(|| {
            CanonicalStoreError::block_load_sequence(
                "persisted tree-state checkpoint heights do not increase",
            )
        })?;
    let max_gap = coverage.max_persisted_gap();
    if gap == 0 || gap > max_gap {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "persisted tree-state checkpoint gap {gap} exceeds {max_gap} blocks"
        )));
    }
    if height > build_tip_height || !coverage.requires_checkpoint(height, build_tip_height) {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "persisted tree-state checkpoint at height {} is outside the selected construction coverage",
            height.value()
        )));
    }
    Ok(())
}

fn validate_tree_state_checkpoint_readback(
    row_count: u64,
    last_height: Option<BlockHeight>,
    tip_frontiers: Option<&CommitmentTreeFrontiers>,
    build_plan: &CanonicalStoreBuildPlan,
    evidence: &CanonicalBlockLoadEvidence,
) -> Result<(), CanonicalStoreError> {
    if row_count == evidence.tree_state_checkpoint_count
        && last_height == Some(build_plan.build_tip().height)
        && tip_frontiers.map(CommitmentTreeFrontiers::tip_metadata) == Some(evidence.tip_metadata)
    {
        return Ok(());
    }
    Err(CanonicalStoreError::BlockLoadReadbackMismatch)
}

fn validate_persisted_block_final_note_commitment_roots(
    db: &DB,
    workload: CanonicalStoreWorkload,
    build_plan: &CanonicalStoreBuildPlan,
    evidence: &CanonicalBlockLoadEvidence,
) -> Result<(), CanonicalStoreError> {
    let family = db
        .cf_handle(BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY)
        .ok_or_else(|| {
            CanonicalStoreError::block_load_sequence(
                "block_final_note_commitment_roots column family is absent",
            )
        })?;
    let mut row_count = 0_u64;
    for row in db.iterator_cf(&family, IteratorMode::Start) {
        let (key, encoded_roots) = row.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "block final note-commitment roots persisted readback",
            source,
        })?;
        let height = zinder_core::wire::decode_height_key_ascending(&key).map_err(|source| {
            CanonicalStoreError::block_load_sequence(format!(
                "block_final_note_commitment_roots has an invalid height key: {source}"
            ))
        })?;
        if height < build_plan.history_bounds().first_available_height()
            || height > build_plan.build_tip().height
        {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block_final_note_commitment_roots height {} is outside retained history",
                height.value()
            )));
        }
        decode_block_final_note_commitment_roots(
            height,
            BlockHash::from_bytes([0; 32]),
            &encoded_roots,
        )
        .map_err(|source| {
            CanonicalStoreError::block_load_sequence(format!(
                "block_final_note_commitment_roots at height {} is invalid: {source}",
                height.value()
            ))
        })?;
        row_count = row_count.checked_add(1).ok_or_else(|| {
            CanonicalStoreError::block_load_sequence(
                "persisted block final note-commitment roots count exceeds u64::MAX",
            )
        })?;
    }
    if row_count != evidence.block_final_note_commitment_roots_count
        || (workload == CanonicalStoreWorkload::Wallet && row_count != 0)
    {
        return Err(CanonicalStoreError::BlockLoadReadbackMismatch);
    }
    Ok(())
}

pub(super) fn validate_build_block(
    workload: CanonicalStoreWorkload,
    build_plan: &CanonicalStoreBuildPlan,
    network_upgrade_activations: &NetworkUpgradeActivations,
    tree_state_checkpoint_coverage: TreeStateCheckpointCoverage,
    block: &CanonicalBuildBlock,
) -> Result<(), CanonicalStoreError> {
    validate_block(workload, build_plan.raw_blob_retention(), block)?;
    validate_tree_state_checkpoint_pools(network_upgrade_activations, block)?;
    validate_tree_state_checkpoint(
        build_plan.build_tip().height,
        tree_state_checkpoint_coverage,
        block,
    )
}

/// Requires a loaded checkpoint to carry a frontier for exactly the pools
/// active at its height.
///
/// A live commit derives its checkpoint by seeding an accumulator that checks
/// the same rule, so only the bulk loader, which takes checkpoints as given,
/// checks it here.
fn validate_tree_state_checkpoint_pools(
    network_upgrade_activations: &NetworkUpgradeActivations,
    block: &CanonicalBuildBlock,
) -> Result<(), CanonicalStoreError> {
    let Some(checkpoint) = &block.tree_state_checkpoint else {
        return Ok(());
    };
    frontier_presence_mismatch(
        network_upgrade_activations,
        checkpoint.block_id.height,
        &checkpoint.frontiers,
    )
    .map_or(Ok(()), |mismatch| {
        Err(CanonicalStoreError::block_load_sequence(format!(
            "block {} tree-state checkpoint {}",
            checkpoint.block_id.height.value(),
            mismatch.describe()
        )))
    })
}

pub(super) fn validate_live_block(
    workload: CanonicalStoreWorkload,
    raw_blob_retention: RawBlobRetention,
    block: &CanonicalBuildBlock,
) -> Result<(), CanonicalStoreError> {
    validate_block(workload, raw_blob_retention, block)?;
    validate_tree_state_checkpoint(
        block.facts.block_header.height,
        TreeStateCheckpointCoverage::EveryBlock,
        block,
    )
}

fn validate_block(
    workload: CanonicalStoreWorkload,
    raw_blob_retention: RawBlobRetention,
    block: &CanonicalBuildBlock,
) -> Result<(), CanonicalStoreError> {
    let header = &block.facts.block_header;
    let height = header.height;
    if block.replay_envelope.format_version() != CanonicalBlockReplayFormatVersion::V1
        || block.replay_envelope.reference_digest().version()
            != CanonicalBlockFactsDigestVersion::V1
    {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "block {} does not use the version-1 replay and fact-digest contracts",
            height.value()
        )));
    }
    if block.replay_envelope.block_height() != height
        || block.replay_envelope.block_hash() != header.block_hash
        || block.replay_envelope.parent_hash() != header.parent_hash
        || block.replay_envelope.reference_digest()
            != block.facts.digest(CanonicalBlockFactsDigestVersion::V1)
    {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "block {} replay envelope does not match its canonical facts",
            height.value()
        )));
    }
    if block.compact_block.height() != height
        || block.compact_block.block_hash() != header.block_hash
    {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "block {} compact artifact does not match its canonical facts",
            height.value()
        )));
    }
    validate_compact_block_payload(block)?;
    validate_transaction_blobs(raw_blob_retention, block)?;
    validate_block_blob(raw_blob_retention, block)?;
    validate_block_final_note_commitment_roots(workload, block)
}

pub(super) fn validate_compact_block_payload(
    block: &CanonicalBuildBlock,
) -> Result<(), CanonicalStoreError> {
    let header = &block.facts.block_header;
    let compact = &block.compact_block;
    let metadata = compact.chain_metadata();
    if compact.height() != header.height
        || compact.block_hash() != header.block_hash
        || compact.previous_block_hash() != header.parent_hash
        || u32::try_from(header.block_time).ok() != Some(compact.time())
        || metadata.sapling_commitment_tree_size != block.tip_metadata.sapling_commitment_tree_size
        || metadata.orchard_commitment_tree_size != block.tip_metadata.orchard_commitment_tree_size
        || metadata.ironwood_commitment_tree_size
            != block.tip_metadata.ironwood_commitment_tree_size
    {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "block {} compact payload identity or tree positions do not match canonical facts",
            header.height.value()
        )));
    }
    for compact_transaction in compact.transactions() {
        let transaction = usize::try_from(compact_transaction.index)
            .ok()
            .and_then(|index| block.facts.transactions.get(index))
            .ok_or_else(|| {
                CanonicalStoreError::block_load_sequence(format!(
                    "block {} compact transaction index {} is absent from canonical facts",
                    header.height.value(),
                    compact_transaction.index
                ))
            })?;
        if transaction.public_facts.transaction_id != compact_transaction.transaction_id {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} compact transaction {} has the wrong transaction id",
                header.height.value(),
                compact_transaction.index
            )));
        }
    }
    Ok(())
}

fn validate_transaction_blobs(
    raw_blob_retention: RawBlobRetention,
    block: &CanonicalBuildBlock,
) -> Result<(), CanonicalStoreError> {
    let height = block.facts.block_header.height;
    let expected_blob_count = if raw_blob_retention.retains_transaction_blobs() {
        block.facts.transactions.len()
    } else {
        0
    };
    if block.transaction_blobs.len() != expected_blob_count {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "block {} requires {} raw transaction blobs under {} retention but contains {}",
            height.value(),
            expected_blob_count,
            raw_blob_retention,
            block.transaction_blobs.len()
        )));
    }
    for (transaction_index, (transaction, blob)) in block
        .facts
        .transactions
        .iter()
        .zip(&block.transaction_blobs)
        .enumerate()
    {
        let transaction_index = u32::try_from(transaction_index).map_err(|_| {
            CanonicalStoreError::block_load_sequence(format!(
                "block {} transaction count exceeds u32::MAX",
                height.value()
            ))
        })?;
        let location = blob.location;
        if location.transaction_id != transaction.public_facts.transaction_id
            || location.block_height != height
            || location.block_hash != block.facts.block_header.block_hash
            || location.tx_index_in_block != transaction_index
        {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} transaction {transaction_index} raw blob has the wrong location",
                height.value()
            )));
        }
        if SerializedBytesDigest::from_serialized_bytes(&blob.raw_transaction_bytes)
            != transaction.serialized_bytes_digest
        {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} transaction {transaction_index} raw bytes do not match canonical facts",
                height.value()
            )));
        }
    }
    Ok(())
}

fn validate_block_blob(
    raw_blob_retention: RawBlobRetention,
    block: &CanonicalBuildBlock,
) -> Result<(), CanonicalStoreError> {
    let header = &block.facts.block_header;
    let height = header.height;
    match (raw_blob_retention.retains_block_blobs(), &block.block_blob) {
        (false, Some(_)) => {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} unexpectedly contains a raw block under {} retention",
                height.value(),
                raw_blob_retention
            )));
        }
        (true, None) => {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} is missing its raw block under {} retention",
                height.value(),
                raw_blob_retention
            )));
        }
        (true, Some(blob))
            if blob.height != height
                || blob.block_hash != header.block_hash
                || blob.parent_hash != header.parent_hash =>
        {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} raw block identity does not match its canonical facts",
                height.value()
            )));
        }
        (true, Some(blob))
            if SerializedBytesDigest::from_serialized_bytes(&blob.raw_block_bytes)
                != block.facts.serialized_bytes_digest =>
        {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} raw bytes do not match canonical facts",
                height.value()
            )));
        }
        (false, None) | (true, Some(_)) => {}
    }
    Ok(())
}

fn validate_tree_state_checkpoint(
    transition_tip_height: BlockHeight,
    coverage: TreeStateCheckpointCoverage,
    block: &CanonicalBuildBlock,
) -> Result<(), CanonicalStoreError> {
    let header = &block.facts.block_header;
    let height = header.height;
    let checkpoint_required = coverage.requires_checkpoint(height, transition_tip_height);
    match (checkpoint_required, &block.tree_state_checkpoint) {
        (true, None) => {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} is missing its required tree-state checkpoint",
                height.value()
            )));
        }
        (false, Some(_)) => {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} has a tree-state checkpoint outside the selected construction coverage",
                height.value()
            )));
        }
        (true, Some(checkpoint))
            if checkpoint.block_id.height != height
                || checkpoint.block_id.hash != header.block_hash
                || i64::from(checkpoint.block_time_seconds) != header.block_time
                || checkpoint.tip_metadata() != block.tip_metadata =>
        {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} tree-state checkpoint does not match its canonical identity and tree positions",
                height.value()
            )));
        }
        (true, Some(_)) | (false, None) => {}
    }
    Ok(())
}

fn tree_state_checkpoint_required(height: BlockHeight, build_tip_height: BlockHeight) -> bool {
    height == build_tip_height || height.value().is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE)
}

fn validate_block_final_note_commitment_roots(
    workload: CanonicalStoreWorkload,
    block: &CanonicalBuildBlock,
) -> Result<(), CanonicalStoreError> {
    let header = &block.facts.block_header;
    let height = header.height;
    match (workload, block.block_final_note_commitment_roots) {
        (CanonicalStoreWorkload::Wallet, Some(_)) => {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "wallet block {} unexpectedly contains explorer final note-commitment roots",
                height.value()
            )));
        }
        (CanonicalStoreWorkload::Explorer, Some(roots))
            if roots.height != height || roots.block_hash != header.block_hash =>
        {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "explorer block {} final note-commitment roots do not match its canonical identity",
                height.value()
            )));
        }
        (CanonicalStoreWorkload::Wallet, None)
        | (CanonicalStoreWorkload::Explorer, None | Some(_)) => {}
    }
    Ok(())
}

struct BlockSequence {
    first_height: BlockHeight,
    first_parent_hash: BlockHash,
    first_hash: BlockHash,
    tip_height: BlockHeight,
    tip_hash: BlockHash,
    tip_metadata: ChainTipMetadata,
    block_count: u64,
    transaction_count: u64,
    transaction_blob_count: u64,
    block_blob_count: u64,
    tree_state_checkpoint_count: u64,
    block_final_note_commitment_roots_count: u64,
    family_logical_bytes: BlockFamilyLogicalBytes,
    logical_replay_bytes: u64,
    sequence_digest: CanonicalBlockFactsSequenceDigestBuilder,
    retained_checkpoint_count: usize,
    retained_sequence_checkpoints: VecDeque<CanonicalSequenceCheckpoint>,
}

#[derive(Clone, Copy)]
struct BlockSequenceRow {
    height: BlockHeight,
    block_hash: BlockHash,
    parent_hash: BlockHash,
    tip_metadata: ChainTipMetadata,
    transaction_count: u64,
    transaction_blob_count: u64,
    has_block_blob: bool,
    has_tree_state_checkpoint: bool,
    has_block_final_note_commitment_roots: bool,
    family_logical_bytes: BlockFamilyLogicalBytes,
    replay_value_bytes: u64,
    facts_digest: zinder_core::CanonicalBlockFactsDigest,
}

#[derive(Clone, Copy)]
struct BlockFamilyLogicalBytes {
    block_header: u64,
    block_hash_index: u64,
    block_replay: u64,
    compact_block: u64,
    transaction_location: u64,
    transaction_blob: u64,
    block_blob: u64,
    tree_state_checkpoint: u64,
    block_final_note_commitment_roots: u64,
}

impl BlockSequenceRow {
    /// Measures a block that already passed [`validate_build_block`], which
    /// requires its replay envelope's reference digest to equal its facts
    /// digest, so the envelope's digest stands in for recomputing it.
    fn from_validated_block(
        block: &CanonicalBuildBlock,
        encoded: &EncodedBlockValues,
    ) -> Result<Self, CanonicalStoreError> {
        let transaction_count = u64::try_from(block.facts.transactions.len()).map_err(|_| {
            CanonicalStoreError::block_load_sequence("transaction count exceeds u64::MAX")
        })?;
        let transaction_blob_count =
            u64::try_from(block.transaction_blobs.len()).map_err(|_| {
                CanonicalStoreError::block_load_sequence("transaction blob count exceeds u64::MAX")
            })?;
        let mut transaction_location_logical_bytes = 0;
        for _ in &block.facts.transactions {
            transaction_location_logical_bytes =
                checked_add_row_bytes(transaction_location_logical_bytes, 32, 40)?;
        }
        let mut transaction_blob_logical_bytes = 0;
        for transaction_blob in &block.transaction_blobs {
            transaction_blob_logical_bytes = checked_add_row_bytes(
                transaction_blob_logical_bytes,
                8,
                transaction_blob.raw_transaction_bytes.len(),
            )?;
        }
        let block_blob_logical_bytes = block.block_blob.as_ref().map_or(Ok(0), |block_blob| {
            checked_row_bytes(4, block_blob.raw_block_bytes.len())
        })?;
        let tree_state_checkpoint_logical_bytes = encoded
            .tree_state_checkpoint
            .as_ref()
            .map_or(Ok(0), |checkpoint_value| {
                checked_row_bytes(4, checkpoint_value.len())
            })?;
        let block_final_note_commitment_roots_logical_bytes = encoded
            .block_final_note_commitment_roots
            .as_ref()
            .map_or(Ok(0), |roots_value| checked_row_bytes(4, roots_value.len()))?;
        Ok(Self {
            height: block.facts.block_header.height,
            block_hash: block.facts.block_header.block_hash,
            parent_hash: block.facts.block_header.parent_hash,
            tip_metadata: block.tip_metadata,
            transaction_count,
            transaction_blob_count,
            has_block_blob: block.block_blob.is_some(),
            has_tree_state_checkpoint: block.tree_state_checkpoint.is_some(),
            has_block_final_note_commitment_roots: block
                .block_final_note_commitment_roots
                .is_some(),
            family_logical_bytes: BlockFamilyLogicalBytes {
                block_header: checked_row_bytes(4, BLOCK_HEADER_VALUE_LEN)?,
                block_hash_index: checked_row_bytes(32, 4)?,
                block_replay: checked_row_bytes(4, block.replay_envelope.as_bytes().len())?,
                compact_block: checked_row_bytes(4, encoded.compact_block.len())?,
                transaction_location: transaction_location_logical_bytes,
                transaction_blob: transaction_blob_logical_bytes,
                block_blob: block_blob_logical_bytes,
                tree_state_checkpoint: tree_state_checkpoint_logical_bytes,
                block_final_note_commitment_roots: block_final_note_commitment_roots_logical_bytes,
            },
            replay_value_bytes: u64::try_from(block.replay_envelope.as_bytes().len()).map_err(
                |_| CanonicalStoreError::block_load_sequence("replay byte length exceeds u64::MAX"),
            )?,
            facts_digest: block.replay_envelope.reference_digest(),
        })
    }
}

impl BlockFamilyLogicalBytes {
    fn checked_add(self, row: Self) -> Result<Self, CanonicalStoreError> {
        Ok(Self {
            block_header: checked_add_logical_bytes(self.block_header, row.block_header)?,
            block_hash_index: checked_add_logical_bytes(
                self.block_hash_index,
                row.block_hash_index,
            )?,
            block_replay: checked_add_logical_bytes(self.block_replay, row.block_replay)?,
            compact_block: checked_add_logical_bytes(self.compact_block, row.compact_block)?,
            transaction_location: checked_add_logical_bytes(
                self.transaction_location,
                row.transaction_location,
            )?,
            transaction_blob: checked_add_logical_bytes(
                self.transaction_blob,
                row.transaction_blob,
            )?,
            block_blob: checked_add_logical_bytes(self.block_blob, row.block_blob)?,
            tree_state_checkpoint: checked_add_logical_bytes(
                self.tree_state_checkpoint,
                row.tree_state_checkpoint,
            )?,
            block_final_note_commitment_roots: checked_add_logical_bytes(
                self.block_final_note_commitment_roots,
                row.block_final_note_commitment_roots,
            )?,
        })
    }

    fn checked_sum(self) -> Result<u64, CanonicalStoreError> {
        [
            self.block_header,
            self.block_hash_index,
            self.block_replay,
            self.compact_block,
            self.transaction_location,
            self.transaction_blob,
            self.block_blob,
            self.tree_state_checkpoint,
            self.block_final_note_commitment_roots,
        ]
        .into_iter()
        .try_fold(0_u64, checked_add_logical_bytes)
    }
}

impl BlockSequence {
    fn new(
        row: BlockSequenceRow,
        retained_checkpoint_count: usize,
    ) -> Result<Self, CanonicalStoreError> {
        let mut sequence_digest = CanonicalBlockFactsSequenceDigestBuilder::new(
            CanonicalBlockFactsSequenceDigestVersion::V1,
        );
        sequence_digest.try_append(row.facts_digest).map_err(|_| {
            CanonicalStoreError::block_load_sequence("block sequence count exceeds u64::MAX")
        })?;
        let mut sequence = Self {
            first_height: row.height,
            first_parent_hash: row.parent_hash,
            first_hash: row.block_hash,
            tip_height: row.height,
            tip_hash: row.block_hash,
            tip_metadata: row.tip_metadata,
            block_count: 1,
            transaction_count: row.transaction_count,
            transaction_blob_count: row.transaction_blob_count,
            block_blob_count: u64::from(row.has_block_blob),
            tree_state_checkpoint_count: u64::from(row.has_tree_state_checkpoint),
            block_final_note_commitment_roots_count: u64::from(
                row.has_block_final_note_commitment_roots,
            ),
            family_logical_bytes: row.family_logical_bytes,
            logical_replay_bytes: row.replay_value_bytes,
            sequence_digest,
            retained_checkpoint_count,
            retained_sequence_checkpoints: VecDeque::new(),
        };
        sequence.retain_checkpoint();
        Ok(sequence)
    }

    fn append(&mut self, row: BlockSequenceRow) -> Result<(), CanonicalStoreError> {
        if self.tip_height.next() != Some(row.height) {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "height {} does not immediately follow height {}",
                row.height.value(),
                self.tip_height.value()
            )));
        }
        if row.parent_hash != self.tip_hash {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} parent does not match the preceding block",
                row.height.value()
            )));
        }
        if row.tip_metadata.sapling_commitment_tree_size
            < self.tip_metadata.sapling_commitment_tree_size
            || row.tip_metadata.orchard_commitment_tree_size
                < self.tip_metadata.orchard_commitment_tree_size
            || row.tip_metadata.ironwood_commitment_tree_size
                < self.tip_metadata.ironwood_commitment_tree_size
        {
            return Err(CanonicalStoreError::block_load_sequence(format!(
                "block {} commitment-tree positions move backwards",
                row.height.value()
            )));
        }
        self.block_count = self.block_count.checked_add(1).ok_or_else(|| {
            CanonicalStoreError::block_load_sequence("block count exceeds u64::MAX")
        })?;
        self.append_family_counts(&row)?;
        self.family_logical_bytes = self
            .family_logical_bytes
            .checked_add(row.family_logical_bytes)?;
        self.logical_replay_bytes = self
            .logical_replay_bytes
            .checked_add(row.replay_value_bytes)
            .ok_or_else(|| {
                CanonicalStoreError::block_load_sequence("replay bytes exceed u64::MAX")
            })?;
        self.sequence_digest
            .try_append(row.facts_digest)
            .map_err(|_| {
                CanonicalStoreError::block_load_sequence("block sequence count exceeds u64::MAX")
            })?;
        self.tip_height = row.height;
        self.tip_hash = row.block_hash;
        self.tip_metadata = row.tip_metadata;
        self.retain_checkpoint();
        Ok(())
    }

    fn append_family_counts(&mut self, row: &BlockSequenceRow) -> Result<(), CanonicalStoreError> {
        self.transaction_count = self
            .transaction_count
            .checked_add(row.transaction_count)
            .ok_or_else(|| {
                CanonicalStoreError::block_load_sequence("transaction count exceeds u64::MAX")
            })?;
        self.transaction_blob_count = self
            .transaction_blob_count
            .checked_add(row.transaction_blob_count)
            .ok_or_else(|| {
                CanonicalStoreError::block_load_sequence("transaction blob count exceeds u64::MAX")
            })?;
        self.block_blob_count = self
            .block_blob_count
            .checked_add(u64::from(row.has_block_blob))
            .ok_or_else(|| {
                CanonicalStoreError::block_load_sequence("block blob count exceeds u64::MAX")
            })?;
        self.tree_state_checkpoint_count = self
            .tree_state_checkpoint_count
            .checked_add(u64::from(row.has_tree_state_checkpoint))
            .ok_or_else(|| {
                CanonicalStoreError::block_load_sequence(
                    "tree-state checkpoint count exceeds u64::MAX",
                )
            })?;
        self.block_final_note_commitment_roots_count = self
            .block_final_note_commitment_roots_count
            .checked_add(u64::from(row.has_block_final_note_commitment_roots))
            .ok_or_else(|| {
                CanonicalStoreError::block_load_sequence(
                    "block final note-commitment roots count exceeds u64::MAX",
                )
            })?;
        Ok(())
    }

    fn retain_checkpoint(&mut self) {
        self.retained_sequence_checkpoints.push_back(
            CanonicalSequenceCheckpoint::from_admitted_parts(
                zinder_core::BlockId::new(self.tip_height, self.tip_hash),
                self.block_count,
                self.sequence_digest.clone().finish(),
                self.logical_replay_bytes,
            ),
        );
        if self.retained_sequence_checkpoints.len() > self.retained_checkpoint_count {
            self.retained_sequence_checkpoints.pop_front();
        }
    }

    fn finish(
        self,
        sst_file_bytes: u64,
        sst_file_count: u64,
        predecessor_checkpoint_logical_bytes: u64,
    ) -> Result<FinishedBlockSequence, CanonicalStoreError> {
        let mut family_logical_bytes = self.family_logical_bytes;
        family_logical_bytes.tree_state_checkpoint = checked_add_logical_bytes(
            family_logical_bytes.tree_state_checkpoint,
            predecessor_checkpoint_logical_bytes,
        )?;
        let logical_bytes = family_logical_bytes.checked_sum()?;
        let evidence = CanonicalBlockLoadEvidence {
            first_height: self.first_height,
            first_parent_hash: self.first_parent_hash,
            first_hash: self.first_hash,
            tip_height: self.tip_height,
            tip_hash: self.tip_hash,
            tip_metadata: self.tip_metadata,
            block_count: self.block_count,
            transaction_count: self.transaction_count,
            block_header_count: self.block_count,
            block_hash_index_count: self.block_count,
            block_replay_count: self.block_count,
            compact_block_count: self.block_count,
            transaction_location_count: self.transaction_count,
            transaction_blob_count: self.transaction_blob_count,
            block_blob_count: self.block_blob_count,
            tree_state_checkpoint_count: self
                .tree_state_checkpoint_count
                .checked_add(1)
                .ok_or_else(|| {
                    CanonicalStoreError::block_load_sequence(
                        "tree-state checkpoint count exceeds u64::MAX",
                    )
                })?,
            block_final_note_commitment_roots_count: self.block_final_note_commitment_roots_count,
            block_header_logical_bytes: family_logical_bytes.block_header,
            block_hash_index_logical_bytes: family_logical_bytes.block_hash_index,
            block_replay_logical_bytes: family_logical_bytes.block_replay,
            compact_block_logical_bytes: family_logical_bytes.compact_block,
            transaction_location_logical_bytes: family_logical_bytes.transaction_location,
            transaction_blob_logical_bytes: family_logical_bytes.transaction_blob,
            block_blob_logical_bytes: family_logical_bytes.block_blob,
            tree_state_checkpoint_logical_bytes: family_logical_bytes.tree_state_checkpoint,
            block_final_note_commitment_roots_logical_bytes: family_logical_bytes
                .block_final_note_commitment_roots,
            logical_bytes,
            sst_file_bytes,
            sst_file_count,
            replay_format_version: CanonicalBlockReplayFormatVersion::V1,
            block_digest_version: CanonicalBlockFactsDigestVersion::V1,
            sequence_digest_version: CanonicalBlockFactsSequenceDigestVersion::V1,
            sequence_digest: self.sequence_digest.finish(),
        };
        Ok(FinishedBlockSequence {
            evidence,
            retained_sequence_checkpoints: self.retained_sequence_checkpoints,
            logical_replay_bytes: self.logical_replay_bytes,
        })
    }
}

struct FinishedBlockSequence {
    evidence: CanonicalBlockLoadEvidence,
    retained_sequence_checkpoints: VecDeque<CanonicalSequenceCheckpoint>,
    logical_replay_bytes: u64,
}

impl PreparedColumnFamily {
    fn new(name: &'static str, files: SstFileSet) -> Self {
        let family_evidence = CanonicalConstructionFamilyEvidence::from_ordered_writer(
            name,
            files.logical_family_evidence,
        );
        let staged_ssts = files
            .files
            .into_iter()
            .map(|file| CanonicalStagedSstEvidence { family: name, file })
            .collect();
        Self {
            name,
            paths: files.paths,
            staged_ssts,
            family_evidence,
        }
    }
}

fn checked_row_bytes(key_len: usize, value_len: usize) -> Result<u64, CanonicalStoreError> {
    let row_len = key_len.checked_add(value_len).ok_or_else(|| {
        CanonicalStoreError::block_load_sequence("logical row byte count exceeds usize::MAX")
    })?;
    u64::try_from(row_len).map_err(|_| {
        CanonicalStoreError::block_load_sequence("logical row byte count exceeds u64::MAX")
    })
}

fn checked_add_row_bytes(
    total: u64,
    key_len: usize,
    value_len: usize,
) -> Result<u64, CanonicalStoreError> {
    total
        .checked_add(checked_row_bytes(key_len, value_len)?)
        .ok_or_else(|| {
            CanonicalStoreError::block_load_sequence("logical byte count exceeds u64::MAX")
        })
}

fn checked_add_sst_bytes(total: u64, file_bytes: u64) -> Result<u64, CanonicalStoreError> {
    total.checked_add(file_bytes).ok_or_else(|| {
        CanonicalStoreError::block_load_sequence("physical SST byte count exceeds u64::MAX")
    })
}

fn checked_add_logical_bytes(total: u64, bytes: u64) -> Result<u64, CanonicalStoreError> {
    total.checked_add(bytes).ok_or_else(|| {
        CanonicalStoreError::block_load_sequence("logical byte count exceeds u64::MAX")
    })
}
