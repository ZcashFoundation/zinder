//! Aligned stride accumulator, `<= 256 MiB` segment presplitting, atomic
//! multi-module segment sealing under `.staging/`, and crash-recovery cleanup.

use std::{
    fmt,
    fs::File,
    io::{self, BufReader, BufWriter, Read as _, Seek as _, SeekFrom, Write as _},
    path::{Path, PathBuf},
};

use zinder_core::{
    BlockHeight, BlockId, CanonicalBlockFacts, CanonicalBlockFactsSequenceDigestBuilder,
    CanonicalBlockFactsSequenceDigestVersion, CanonicalBlockReplayFormatVersion,
    CompactBlockArtifact, Network, encode_canonical_block_replay, wire::encode_internal_block_hash,
};

use crate::{
    segment_verity,
    settled_raw_block_segment::{
        DEFAULT_SETTLED_RAW_BLOCK_SEGMENT_MAX_SLAB_BYTES, MAX_RAW_COMPRESSED_PAYLOAD_BYTES,
        MAX_RAW_SEGMENT_TX_ENTRIES, MAX_RAW_SLAB_BYTES, RAW_FRAME_PREFIX_LEN, RAW_HEADER_LEN,
        RAW_ROW_INDEX_ENTRY_LEN, RAW_TRAILER_LEN, RAW_TX_SLICE_ENTRY_LEN, RawBlockCodec,
        RawBlockSegmentIdentity, SettledRawBlockSegmentWriter,
    },
    settled_shielded_segment::{
        DEFAULT_SETTLED_SHIELDED_SEGMENT_MAX_SLAB_BYTES, MAX_SHIELDED_SLAB_BYTES,
        SHIELDED_FRAME_PREFIX_LEN, SHIELDED_HEADER_LEN, SHIELDED_ROW_INDEX_ENTRY_LEN,
        SHIELDED_TRAILER_LEN, SettledShieldedSegmentWriter, ShieldedSegmentIdentity,
    },
    settled_supplement_segment::{
        BlockEnvelopeIdentity, DEFAULT_SETTLED_SUPPLEMENT_SEGMENT_MAX_BYTES,
        SUPPLEMENT_CHUNK_INDEX_ENTRY_LEN, SUPPLEMENT_HEADER_LEN, SUPPLEMENT_ROW_INDEX_ENTRY_LEN,
        SUPPLEMENT_TRAILER_LEN, SettledSupplementSegmentWriteOptions,
        SettledSupplementSegmentWriter, SupplementCompressionCodec, SupplementSegmentIdentity,
        encode_block_record,
    },
};

use super::super::{
    CanonicalSequenceCheckpoint,
    live_tail::{LiveTailBlockRecord, LiveTailRawBlock},
};
use super::{
    EntryKind, SettledSegmentsError, VerifiedDir,
    layout::{
        SEGMENTS_DIR_NAME, SEGMENTS_STAGING_DIR_NAME, SettledSegmentIndexEntry, SettledSegmentKind,
        SettledSegmentModules, aligned_segment_end_height, format_segment_filename,
    },
};

/// Default number of contiguous blocks sealed per aligned settled segment stride.
pub(crate) const DEFAULT_SETTLED_SEGMENT_BLOCKS: u32 = 4_096;
/// Default sealed segment byte cap (`256 MiB`) across `ZSHRNG02`, `ZSTSUP01`, and `ZSBLKB01`.
pub(crate) const DEFAULT_MAX_SETTLED_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_STAGING_SPOOL_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAX_SPOOL_RECORD_BYTES: usize = 64 * 1024 * 1024;
const SPOOL_FRAME_HEADER_LEN: usize = 16;
const ZSTD_BOUND_PER_FRAME_HEADROOM: usize = 64;

const _: () =
    assert!(DEFAULT_MAX_SETTLED_SEGMENT_BYTES == DEFAULT_SETTLED_SHIELDED_SEGMENT_MAX_SLAB_BYTES);
const _: () =
    assert!(DEFAULT_MAX_SETTLED_SEGMENT_BYTES == DEFAULT_SETTLED_SUPPLEMENT_SEGMENT_MAX_BYTES);
const _: () =
    assert!(DEFAULT_MAX_SETTLED_SEGMENT_BYTES == DEFAULT_SETTLED_RAW_BLOCK_SEGMENT_MAX_SLAB_BYTES);

/// Sealer configuration controlling stride alignment, compression, and optional modules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SettledSegmentSealerConfig {
    /// Network identity recorded in segment headers and index entries.
    pub(crate) network: Network,
    /// Schema version of compact block artifacts stored in `ZSHRNG02`/`ZSTSUP01`.
    pub(crate) artifact_schema_version: u16,
    /// Aligned block stride per settled segment (`DEFAULT_SETTLED_SEGMENT_BLOCKS` by default).
    pub(crate) segment_blocks: u32,
    /// Optional external modules enabled alongside `ZSHRNG02`.
    pub(crate) modules: SettledSegmentModules,
    /// Encoding options for `ZSTSUP01` when `modules.supplement` is `true`.
    pub(crate) supplement_options: SettledSupplementSegmentWriteOptions,
    /// Payload codec for `ZSBLKB01` when `modules.raw_bodies` is `true`.
    pub(crate) raw_block_codec: RawBlockCodec,
    /// Explicit byte ceiling enforced across one active `.staging/` spool.
    pub(crate) max_spool_bytes: u64,
    /// Maximum total `ZSHRNG02` segment byte size before presplitting (default `256 MiB`).
    pub(crate) max_shielded_segment_bytes: u64,
    /// Maximum total `ZSTSUP01` segment byte size before presplitting (default `256 MiB`).
    pub(crate) max_supplement_segment_bytes: u64,
    /// Maximum total `ZSBLKB01` segment byte size before presplitting (default `256 MiB`).
    pub(crate) max_raw_segment_bytes: u64,
    /// Explicit ceiling on `ZSBLKB01` transaction slice entries before presplitting.
    pub(crate) max_raw_tx_entries: usize,
}

impl SettledSegmentSealerConfig {
    /// Creates a sealer configuration using the default 4,096-block stride, `Zstd`,
    /// and `256 MiB` total segment byte caps.
    #[must_use]
    pub(crate) fn new(
        network: Network,
        artifact_schema_version: u16,
        modules: SettledSegmentModules,
    ) -> Self {
        Self {
            network,
            artifact_schema_version,
            segment_blocks: DEFAULT_SETTLED_SEGMENT_BLOCKS,
            modules,
            supplement_options: SettledSupplementSegmentWriteOptions::default(),
            raw_block_codec: RawBlockCodec::Zstd,
            max_spool_bytes: DEFAULT_MAX_STAGING_SPOOL_BYTES,
            max_shielded_segment_bytes: DEFAULT_MAX_SETTLED_SEGMENT_BYTES,
            max_supplement_segment_bytes: DEFAULT_MAX_SETTLED_SEGMENT_BYTES,
            max_raw_segment_bytes: DEFAULT_MAX_SETTLED_SEGMENT_BYTES,
            max_raw_tx_entries: MAX_RAW_SEGMENT_TX_ENTRIES,
        }
    }

    /// Overrides the aligned `segment_blocks` stride (useful for compact tests).
    #[must_use]
    pub(crate) const fn with_segment_blocks(mut self, segment_blocks: u32) -> Self {
        self.segment_blocks = segment_blocks;
        self
    }

    /// Overrides `ZSTSUP01` chunking/compression options.
    #[must_use]
    pub(crate) const fn with_supplement_options(
        mut self,
        supplement_options: SettledSupplementSegmentWriteOptions,
    ) -> Self {
        self.supplement_options = supplement_options;
        self
    }

    /// Overrides `ZSBLKB01` compression codec.
    #[must_use]
    pub(crate) const fn with_raw_block_codec(mut self, raw_block_codec: RawBlockCodec) -> Self {
        self.raw_block_codec = raw_block_codec;
        self
    }

    /// Overrides the maximum `.staging/` spool byte budget in unit tests.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn with_max_spool_bytes(mut self, max_spool_bytes: u64) -> Self {
        self.max_spool_bytes = max_spool_bytes;
        self
    }

    /// Overrides the `ZSHRNG02` segment byte rollover cap in unit tests.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn with_max_shielded_segment_bytes(
        mut self,
        max_shielded_segment_bytes: u64,
    ) -> Self {
        self.max_shielded_segment_bytes = max_shielded_segment_bytes;
        self
    }

    /// Overrides the `ZSTSUP01` segment byte rollover cap in unit tests.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn with_max_supplement_segment_bytes(
        mut self,
        max_supplement_segment_bytes: u64,
    ) -> Self {
        self.max_supplement_segment_bytes = max_supplement_segment_bytes;
        self
    }

    /// Overrides the `ZSBLKB01` segment byte rollover cap in unit tests.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn with_max_raw_segment_bytes(mut self, max_raw_segment_bytes: u64) -> Self {
        self.max_raw_segment_bytes = max_raw_segment_bytes;
        self
    }

    /// Overrides the `ZSBLKB01` transaction slice rollover cap in unit tests.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn with_max_raw_tx_entries(mut self, max_raw_tx_entries: usize) -> Self {
        self.max_raw_tx_entries = max_raw_tx_entries;
        self
    }

    fn validate(&self) -> Result<(), SettledSegmentsError> {
        if self.segment_blocks == 0 {
            return Err(SettledSegmentsError::InvalidLayout(
                "sealer segment_blocks stride must be positive".to_owned(),
            ));
        }
        if self.max_spool_bytes == 0 {
            return Err(SettledSegmentsError::InvalidLayout(
                "sealer max_spool_bytes budget must be positive".to_owned(),
            ));
        }
        if self.max_shielded_segment_bytes == 0
            || self.max_shielded_segment_bytes > MAX_SHIELDED_SLAB_BYTES
        {
            return Err(SettledSegmentsError::InvalidLayout(
                "sealer max_shielded_segment_bytes is outside format limits".to_owned(),
            ));
        }
        if self.max_supplement_segment_bytes == 0
            || self.max_supplement_segment_bytes > u64::from(u32::MAX)
        {
            return Err(SettledSegmentsError::InvalidLayout(
                "sealer max_supplement_segment_bytes is outside format limits".to_owned(),
            ));
        }
        if self.max_raw_segment_bytes == 0 || self.max_raw_segment_bytes > MAX_RAW_SLAB_BYTES {
            return Err(SettledSegmentsError::InvalidLayout(
                "sealer max_raw_segment_bytes is outside format limits".to_owned(),
            ));
        }
        if self.max_raw_tx_entries == 0 || self.max_raw_tx_entries > MAX_RAW_SEGMENT_TX_ENTRIES {
            return Err(SettledSegmentsError::InvalidLayout(
                "sealer max_raw_tx_entries is outside format limits".to_owned(),
            ));
        }
        Ok(())
    }
}

/// One contiguous canonical block supplied to [`SettledSegmentSealer::push_block`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SettledSegmentBlockInput {
    /// Structured compact block artifact written into `ZSHRNG02`/`ZSTSUP01`.
    pub(crate) compact_block: CompactBlockArtifact,
    /// Canonical uncompressed native byte budget tracked in `ZSHRNG02` row index.
    pub(crate) native_bytes: usize,
    /// Full canonical block facts advancing `CanonicalSequenceCheckpoint` and Tag 2 metadata.
    pub(crate) canonical_facts: CanonicalBlockFacts,
    /// Optional raw consensus block payload and tx slices required when `raw_bodies` is enabled.
    pub(crate) raw_block: Option<LiveTailRawBlock>,
}

/// Accumulates contiguous canonical blocks into a bounded `.staging/` spool,
/// advances `CanonicalSequenceCheckpoint` incrementally, presplits before
/// `<= 256 MiB` segment caps, and seals/commits atomically.
#[derive(Debug)]
pub(crate) struct SettledSegmentSealer {
    store_root: PathBuf,
    config: SettledSegmentSealerConfig,
    prior_checkpoint: Option<CanonicalSequenceCheckpoint>,
    active_stride: Option<ActiveStrideState>,
    poisoned: bool,
}

struct ActiveStrideState {
    start_height: BlockHeight,
    target_end: BlockHeight,
    last_block_id: BlockId,
    block_count: usize,
    shielded_slab_bytes: u64,
    supplement_uncompressed_bytes: u64,
    raw_slab_bytes: u64,
    raw_tx_entries: usize,
    spool: StagingBlockSpool,
    digest_builder: CanonicalBlockFactsSequenceDigestBuilder,
    retained_count: u64,
    logical_replay_bytes: u64,
}

impl ActiveStrideState {
    fn should_presplit_for(
        &self,
        spooled: &SpooledBlockRecord,
        config: &SettledSegmentSealerConfig,
    ) -> bool {
        let Some(next_blocks) = self
            .block_count
            .checked_add(1)
            .and_then(|count| u64::try_from(count).ok())
        else {
            return true;
        };
        let next_shielded_slab = self
            .shielded_slab_bytes
            .saturating_add(spooled.shielded_frame_bytes);
        if next_shielded_slab > MAX_SHIELDED_SLAB_BYTES
            || predicted_shielded_segment_bytes(next_blocks, next_shielded_slab)
                > config.max_shielded_segment_bytes
        {
            return true;
        }
        if config.modules.supplement {
            let next_sup_uncompressed = self
                .supplement_uncompressed_bytes
                .saturating_add(spooled.supplement_uncompressed_bytes);
            if predicted_supplement_segment_bytes(
                next_blocks,
                next_sup_uncompressed,
                config.supplement_options,
            ) > config.max_supplement_segment_bytes
            {
                return true;
            }
        }
        if config.modules.raw_bodies {
            let next_raw_slab = self.raw_slab_bytes.saturating_add(spooled.raw_frame_bytes);
            let next_raw_txs = self.raw_tx_entries.saturating_add(spooled.raw_tx_entries);
            if next_raw_slab > MAX_RAW_SLAB_BYTES
                || next_raw_txs > config.max_raw_tx_entries
                || predicted_raw_segment_bytes(next_blocks, next_raw_slab, next_raw_txs)
                    > config.max_raw_segment_bytes
            {
                return true;
            }
        }
        self.spool.would_exceed_budget(
            spooled.encoded_record.len(),
            spooled.encoded_raw_block.as_deref().map_or(0, <[u8]>::len),
        )
    }
}

impl fmt::Debug for ActiveStrideState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveStrideState")
            .field("start_height", &self.start_height)
            .field("target_end", &self.target_end)
            .field("last_block_id", &self.last_block_id)
            .field("block_count", &self.block_count)
            .field("retained_count", &self.retained_count)
            .field("logical_replay_bytes", &self.logical_replay_bytes)
            .finish_non_exhaustive()
    }
}

impl SettledSegmentSealer {
    /// Creates a new sealer rooted at `store_root`, optionally resuming after `prior_checkpoint`.
    pub(crate) fn new(
        store_root: impl AsRef<Path>,
        config: SettledSegmentSealerConfig,
        prior_checkpoint: Option<CanonicalSequenceCheckpoint>,
    ) -> Result<Self, SettledSegmentsError> {
        config.validate()?;
        if let Some(cp) = prior_checkpoint {
            validate_prior_checkpoint(cp)?;
        }
        let root_path = store_root.as_ref().to_path_buf();
        let _ = VerifiedDir::open_existing_optional(&root_path, &[])?;
        Ok(Self {
            store_root: root_path,
            config,
            prior_checkpoint,
            active_stride: None,
            poisoned: false,
        })
    }

    /// Returns the number of unsealed blocks currently staged for the active stride.
    #[must_use]
    pub(crate) const fn pending_len(&self) -> usize {
        match &self.active_stride {
            Some(active) => active.block_count,
            None => 0,
        }
    }

    /// Returns the latest committed checkpoint, if at least one stride has been sealed.
    #[must_use]
    pub(crate) const fn prior_checkpoint(&self) -> Option<CanonicalSequenceCheckpoint> {
        self.prior_checkpoint
    }

    /// Returns whether the sealer is poisoned after a post-seal or append failure.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Pushes the next contiguous canonical block. Returns zero or more sealed
    /// segment entries whenever the block completes its aligned stride or forces a
    /// size/transaction presplit of the prior active segment.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "Ingest API accepts SettledSegmentBlockInput by value."
    )]
    pub(crate) fn push_block(
        &mut self,
        block: SettledSegmentBlockInput,
    ) -> Result<Vec<SettledSegmentIndexEntry>, SettledSegmentsError> {
        self.push_block_internal(&block, false)
    }

    /// Test helper simulating an I/O or budget failure immediately after a cap rollover seals.
    #[cfg(test)]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "Test helper mirrors push_block ownership signature."
    )]
    pub(crate) fn push_block_with_injected_append_failure(
        &mut self,
        block: SettledSegmentBlockInput,
    ) -> Result<Vec<SettledSegmentIndexEntry>, SettledSegmentsError> {
        self.push_block_internal(&block, true)
    }

    fn push_block_internal(
        &mut self,
        block: &SettledSegmentBlockInput,
        fail_append_for_test: bool,
    ) -> Result<Vec<SettledSegmentIndexEntry>, SettledSegmentsError> {
        if self.poisoned {
            return Err(SettledSegmentsError::ChainLinkage(
                "settled segment sealer is poisoned after a post-seal or append failure; reconstruct from the caller's durable checkpoint"
                    .to_owned(),
            ));
        }
        self.validate_incoming_block(block)?;
        let block_id = BlockId::new(
            block.compact_block.height(),
            block.compact_block.block_hash(),
        );
        let checkpoint_step = self.next_incremental_checkpoint(&block.canonical_facts)?;
        let spooled = encode_spool_record(block, &self.config)?;
        validate_single_block_fits_segment_caps(&spooled, &self.config)?;

        let mut sealed = Vec::new();
        if let Some(active) = &self.active_stride
            && active.should_presplit_for(&spooled, &self.config)
        {
            let prior_active = self.active_stride.take().ok_or_else(|| {
                SettledSegmentsError::ChainLinkage(
                    "missing active stride state on cap presplit".to_owned(),
                )
            })?;
            match self.seal_active_stride(prior_active) {
                Ok(entry) => sealed.push(entry),
                Err(err) => {
                    self.poisoned = true;
                    return Err(err);
                }
            }
        }
        #[cfg(not(test))]
        let _ = fail_append_for_test;
        let post_result: Result<(), SettledSegmentsError> = (|| {
            #[cfg(test)]
            if fail_append_for_test {
                return Err(SettledSegmentsError::Io(io::Error::other(
                    "injected post-rollover append failure",
                )));
            }
            let target_end = self.append_to_active_stride(block_id, checkpoint_step, &spooled)?;
            if block_id.height >= target_end {
                let active = self.active_stride.take().ok_or_else(|| {
                    SettledSegmentsError::ChainLinkage(
                        "missing active stride state on seal".to_owned(),
                    )
                })?;
                sealed.push(self.seal_active_stride(active)?);
            }
            Ok(())
        })();
        if let Err(err) = post_result {
            self.active_stride = None;
            self.poisoned = true;
            return Err(err);
        }
        Ok(sealed)
    }

    fn next_incremental_checkpoint(
        &self,
        facts: &CanonicalBlockFacts,
    ) -> Result<IncrementalCheckpointStep, SettledSegmentsError> {
        let (mut builder, retained_count, logical_replay_bytes) =
            self.active_stride.as_ref().map_or_else(
                || init_sequence_accumulator(self.prior_checkpoint),
                |active| {
                    (
                        active.digest_builder.clone(),
                        active.retained_count,
                        active.logical_replay_bytes,
                    )
                },
            );
        let envelope = encode_canonical_block_replay(
            facts,
            CanonicalBlockReplayFormatVersion::V1,
            zinder_core::CanonicalBlockFactsDigestVersion::V1,
        );
        builder
            .try_append(envelope.reference_digest())
            .map_err(|_| {
                SettledSegmentsError::ChainLinkage(
                    "canonical block sequence count overflow".to_owned(),
                )
            })?;
        let next_retained = retained_count.checked_add(1).ok_or_else(|| {
            SettledSegmentsError::ChainLinkage("retained_block_count overflow".to_owned())
        })?;
        let replay_len = u64::try_from(envelope.as_bytes().len()).map_err(|_| {
            SettledSegmentsError::InvalidFormat("replay envelope length exceeds u64".to_owned())
        })?;
        let next_bytes = logical_replay_bytes
            .checked_add(replay_len)
            .ok_or_else(|| {
                SettledSegmentsError::ChainLinkage("logical_replay_bytes overflow".to_owned())
            })?;
        Ok(IncrementalCheckpointStep {
            digest_builder: builder,
            retained_count: next_retained,
            logical_replay_bytes: next_bytes,
        })
    }

    fn append_to_active_stride(
        &mut self,
        block_id: BlockId,
        checkpoint_step: IncrementalCheckpointStep,
        spooled: &SpooledBlockRecord,
    ) -> Result<BlockHeight, SettledSegmentsError> {
        if self.active_stride.is_none() {
            let dirs = prepare_segment_directories(&self.store_root, self.config.modules)?;
            let target_end =
                aligned_segment_end_height(block_id.height, self.config.segment_blocks)?;
            let spool = StagingBlockSpool::create(&dirs.staging, self.config.max_spool_bytes)?;
            let (base_builder, base_retained, base_bytes) =
                init_sequence_accumulator(self.prior_checkpoint);
            self.active_stride = Some(ActiveStrideState {
                start_height: block_id.height,
                target_end,
                last_block_id: block_id,
                block_count: 0,
                shielded_slab_bytes: 0,
                supplement_uncompressed_bytes: 0,
                raw_slab_bytes: 0,
                raw_tx_entries: 0,
                spool,
                digest_builder: base_builder,
                retained_count: base_retained,
                logical_replay_bytes: base_bytes,
            });
        }
        let Some(active) = self.active_stride.as_mut() else {
            return Err(SettledSegmentsError::ChainLinkage(
                "missing active stride after initialization".to_owned(),
            ));
        };
        if let Err(err) = active.spool.push_frame(
            spooled.native_bytes,
            &spooled.encoded_record,
            spooled.encoded_raw_block.as_deref(),
        ) {
            self.active_stride = None;
            return Err(err);
        }
        active.last_block_id = block_id;
        active.block_count += 1;
        active.shielded_slab_bytes = active
            .shielded_slab_bytes
            .saturating_add(spooled.shielded_frame_bytes);
        active.supplement_uncompressed_bytes = active
            .supplement_uncompressed_bytes
            .saturating_add(spooled.supplement_uncompressed_bytes);
        active.raw_slab_bytes = active
            .raw_slab_bytes
            .saturating_add(spooled.raw_frame_bytes);
        active.raw_tx_entries = active.raw_tx_entries.saturating_add(spooled.raw_tx_entries);
        active.digest_builder = checkpoint_step.digest_builder;
        active.retained_count = checkpoint_step.retained_count;
        active.logical_replay_bytes = checkpoint_step.logical_replay_bytes;
        Ok(active.target_end)
    }

    fn validate_incoming_block(
        &self,
        block: &SettledSegmentBlockInput,
    ) -> Result<(), SettledSegmentsError> {
        let compact = &block.compact_block;
        let header = &block.canonical_facts.block_header;
        if header.height != compact.height()
            || header.block_hash != compact.block_hash()
            || header.parent_hash != compact.previous_block_hash()
            || header.block_time != i64::from(compact.time())
        {
            return Err(SettledSegmentsError::ChainLinkage(format!(
                "canonical_facts header disagrees with compact block at height {}",
                compact.height().value()
            )));
        }
        if let Some(prev_id) = self.last_committed_or_pending_block_id() {
            let expected_height = prev_id.height.value().checked_add(1).ok_or_else(|| {
                SettledSegmentsError::ChainLinkage("block height overflow".to_owned())
            })?;
            if compact.height().value() != expected_height
                || compact.previous_block_hash() != prev_id.hash
            {
                return Err(SettledSegmentsError::ChainLinkage(format!(
                    "non-contiguous block {} with parent {:?}",
                    compact.height().value(),
                    compact.previous_block_hash()
                )));
            }
        }
        validate_compact_facts_alignment(compact, &block.canonical_facts)?;
        validate_raw_input(
            &block.canonical_facts,
            block.raw_block.as_ref(),
            self.config.modules,
        )
    }

    fn last_committed_or_pending_block_id(&self) -> Option<BlockId> {
        self.active_stride
            .as_ref()
            .map(|active| active.last_block_id)
            .or_else(|| {
                self.prior_checkpoint
                    .map(CanonicalSequenceCheckpoint::through)
            })
    }

    fn seal_active_stride(
        &mut self,
        active: ActiveStrideState,
    ) -> Result<SettledSegmentIndexEntry, SettledSegmentsError> {
        if active.block_count == 0 {
            return Err(SettledSegmentsError::ChainLinkage(
                "cannot seal empty pending stride".to_owned(),
            ));
        }
        let dirs = prepare_segment_directories(&self.store_root, self.config.modules)?;
        let staged = stage_stride_files(&dirs.staging, &self.config, active)?;
        let end_checkpoint = staged.end_checkpoint;
        let entry = promote_staged_stride(
            &dirs,
            staged.start_height,
            staged.end_block,
            &staged,
            end_checkpoint,
        )?;
        self.prior_checkpoint = Some(end_checkpoint);
        Ok(entry)
    }
}

/// Removes orphaned temporary files in `<store_root>/segments/.staging/` on reopen recovery,
/// refusing any symlinks or non-regular entries.
pub(crate) fn cleanup_stale_staging_files(
    store_root: &Path,
) -> Result<usize, SettledSegmentsError> {
    let Some(root_dir) = VerifiedDir::open_existing_optional(store_root, &[])? else {
        return Ok(0);
    };
    let Some(segments_dir) = root_dir.open_child_dir_optional(SEGMENTS_DIR_NAME)? else {
        return Ok(0);
    };
    let Some(staging_dir) = segments_dir.open_child_dir_optional(SEGMENTS_STAGING_DIR_NAME)? else {
        return Ok(0);
    };
    let mut removed = 0_usize;
    for entry_name in staging_dir.list_entry_names()? {
        if staging_dir.stat_entry_nofollow(&entry_name)? != Some(EntryKind::RegularFile) {
            return Err(SettledSegmentsError::InvalidLayout(format!(
                "refusing non-regular entry {entry_name:?} inside segments/.staging"
            )));
        }
        staging_dir.unlink_file(&entry_name)?;
        removed += 1;
    }
    if removed > 0 {
        staging_dir.sync()?;
    }
    Ok(removed)
}

fn predicted_shielded_segment_bytes(block_count: u64, slab_bytes: u64) -> u64 {
    let fixed = u64::try_from(SHIELDED_HEADER_LEN + SHIELDED_TRAILER_LEN).unwrap_or(u64::MAX);
    let row_bytes =
        block_count.saturating_mul(u64::try_from(SHIELDED_ROW_INDEX_ENTRY_LEN).unwrap_or(u64::MAX));
    fixed.saturating_add(slab_bytes).saturating_add(row_bytes)
}

fn predicted_supplement_segment_bytes(
    block_count: u64,
    uncompressed_bytes: u64,
    options: SettledSupplementSegmentWriteOptions,
) -> u64 {
    let chunks = block_count.div_ceil(u64::from(options.chunk_blocks));
    let payload_bound = match options.codec {
        SupplementCompressionCodec::None => uncompressed_bytes,
        SupplementCompressionCodec::Zstd => uncompressed_bytes
            .saturating_add(uncompressed_bytes >> 8)
            .saturating_add(
                chunks.saturating_mul(u64::try_from(ZSTD_BOUND_PER_FRAME_HEADROOM).unwrap_or(64)),
            ),
    };
    let fixed = u64::try_from(SUPPLEMENT_HEADER_LEN + SUPPLEMENT_TRAILER_LEN).unwrap_or(u64::MAX);
    let chunk_index_bytes =
        chunks.saturating_mul(u64::try_from(SUPPLEMENT_CHUNK_INDEX_ENTRY_LEN).unwrap_or(u64::MAX));
    let row_index_bytes = block_count
        .saturating_mul(u64::try_from(SUPPLEMENT_ROW_INDEX_ENTRY_LEN).unwrap_or(u64::MAX));
    fixed
        .saturating_add(payload_bound)
        .saturating_add(chunk_index_bytes)
        .saturating_add(row_index_bytes)
}

fn predicted_raw_segment_bytes(
    block_count: u64,
    raw_slab_bytes: u64,
    raw_tx_entries: usize,
) -> u64 {
    let fixed = u64::try_from(RAW_HEADER_LEN + RAW_TRAILER_LEN).unwrap_or(u64::MAX);
    let tx_bytes = u64::try_from(raw_tx_entries)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(RAW_TX_SLICE_ENTRY_LEN).unwrap_or(u64::MAX));
    let row_bytes =
        block_count.saturating_mul(u64::try_from(RAW_ROW_INDEX_ENTRY_LEN).unwrap_or(u64::MAX));
    fixed
        .saturating_add(raw_slab_bytes)
        .saturating_add(tx_bytes)
        .saturating_add(row_bytes)
}

struct IncrementalCheckpointStep {
    digest_builder: CanonicalBlockFactsSequenceDigestBuilder,
    retained_count: u64,
    logical_replay_bytes: u64,
}

struct StagingBlockSpool {
    staging_dir: VerifiedDir,
    temp_name: String,
    writer: Option<BufWriter<File>>,
    bytes_written: u64,
    max_bytes: u64,
}

impl StagingBlockSpool {
    fn would_exceed_budget(&self, encoded_record_len: usize, encoded_raw_len: usize) -> bool {
        let Some(frame_bytes) = spool_frame_total_bytes(encoded_record_len, encoded_raw_len) else {
            return true;
        };
        self.bytes_written
            .checked_add(frame_bytes)
            .is_none_or(|next_bytes| next_bytes > self.max_bytes)
    }

    fn create(staging_dir: &VerifiedDir, max_bytes: u64) -> Result<Self, SettledSegmentsError> {
        let (temp_name, temp_file) = staging_dir.create_temp_file("spool")?;
        Ok(Self {
            staging_dir: staging_dir.try_clone()?,
            temp_name,
            writer: Some(BufWriter::new(temp_file)),
            bytes_written: 0,
            max_bytes,
        })
    }

    fn push_frame(
        &mut self,
        native_bytes: usize,
        encoded_record: &[u8],
        encoded_raw_block: Option<&[u8]>,
    ) -> Result<(), SettledSegmentsError> {
        let record_len_u32 = u32::try_from(encoded_record.len()).map_err(|_| {
            SettledSegmentsError::InvalidFormat("spool record length exceeds u32".to_owned())
        })?;
        let raw_slice = encoded_raw_block.unwrap_or(&[]);
        let raw_len_u32 = u32::try_from(raw_slice.len()).map_err(|_| {
            SettledSegmentsError::InvalidFormat("spool raw block length exceeds u32".to_owned())
        })?;
        let native_u64 = u64::try_from(native_bytes).map_err(|_| {
            SettledSegmentsError::InvalidFormat("native_bytes exceeds u64".to_owned())
        })?;
        let frame_bytes = spool_frame_total_bytes(encoded_record.len(), raw_slice.len())
            .ok_or_else(|| {
                SettledSegmentsError::InvalidFormat("spool frame byte count overflow".to_owned())
            })?;
        let next_bytes = self.bytes_written.checked_add(frame_bytes).ok_or_else(|| {
            SettledSegmentsError::InvalidFormat("staging spool byte total overflow".to_owned())
        })?;
        if next_bytes > self.max_bytes {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "staging spool bytes ({next_bytes}) exceed configured budget ({})",
                self.max_bytes
            )));
        }
        let mut writer = self.writer.take().ok_or_else(|| {
            SettledSegmentsError::Io(io::Error::other("staging spool writer poisoned"))
        })?;
        writer.write_all(&record_len_u32.to_be_bytes())?;
        writer.write_all(&raw_len_u32.to_be_bytes())?;
        writer.write_all(&native_u64.to_be_bytes())?;
        writer.write_all(encoded_record)?;
        if !raw_slice.is_empty() {
            writer.write_all(raw_slice)?;
        }
        self.writer = Some(writer);
        self.bytes_written = next_bytes;
        Ok(())
    }

    fn into_reader(mut self) -> Result<SpoolStreamReader, SettledSegmentsError> {
        let writer = self.writer.take().ok_or_else(|| {
            SettledSegmentsError::Io(io::Error::other("staging spool writer poisoned"))
        })?;
        let mut file = writer
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        file.seek(SeekFrom::Start(0))?;
        let staging_dir = self.staging_dir.try_clone()?;
        let temp_name = std::mem::take(&mut self.temp_name);
        Ok(SpoolStreamReader {
            staging_dir,
            temp_name,
            reader: Some(BufReader::new(file)),
        })
    }
}

impl Drop for StagingBlockSpool {
    fn drop(&mut self) {
        drop(self.writer.take());
        if !self.temp_name.is_empty() {
            let _ = self.staging_dir.unlink_file(&self.temp_name);
        }
    }
}

fn spool_frame_total_bytes(encoded_record_len: usize, encoded_raw_len: usize) -> Option<u64> {
    let rec_u32 = u32::try_from(encoded_record_len).ok()?;
    let raw_u32 = u32::try_from(encoded_raw_len).ok()?;
    u64::try_from(SPOOL_FRAME_HEADER_LEN)
        .ok()?
        .checked_add(u64::from(rec_u32))?
        .checked_add(u64::from(raw_u32))
}

struct SpoolStreamReader {
    staging_dir: VerifiedDir,
    temp_name: String,
    reader: Option<BufReader<File>>,
}

impl SpoolStreamReader {
    fn read_next_frame(
        &mut self,
    ) -> Result<(usize, LiveTailBlockRecord, Option<LiveTailRawBlock>), SettledSegmentsError> {
        let Some(reader) = self.reader.as_mut() else {
            return Err(SettledSegmentsError::Io(io::Error::other(
                "spool stream reader already closed",
            )));
        };
        let mut hdr = [0_u8; SPOOL_FRAME_HEADER_LEN];
        reader.read_exact(&mut hdr)?;
        let record_len = usize::try_from(u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]))
            .map_err(|_| {
                SettledSegmentsError::InvalidFormat("spool record length exceeds usize".to_owned())
            })?;
        let raw_len = usize::try_from(u32::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]))
            .map_err(|_| {
                SettledSegmentsError::InvalidFormat(
                    "spool raw block length exceeds usize".to_owned(),
                )
            })?;
        if record_len > MAX_SPOOL_RECORD_BYTES || raw_len > MAX_SPOOL_RECORD_BYTES {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "spool frame lengths ({record_len}, {raw_len}) exceed limit {MAX_SPOOL_RECORD_BYTES}"
            )));
        }
        let native_u64 = u64::from_be_bytes([
            hdr[8], hdr[9], hdr[10], hdr[11], hdr[12], hdr[13], hdr[14], hdr[15],
        ]);
        let native_bytes = usize::try_from(native_u64).map_err(|_| {
            SettledSegmentsError::InvalidFormat("spooled native_bytes exceeds usize".to_owned())
        })?;
        let mut payload = vec![0_u8; record_len];
        reader.read_exact(&mut payload)?;
        let record = LiveTailBlockRecord::decode_v1(&payload)?;
        let raw_block = if raw_len > 0 {
            let mut raw_payload = vec![0_u8; raw_len];
            reader.read_exact(&mut raw_payload)?;
            Some(LiveTailRawBlock::decode_v1(&raw_payload)?)
        } else {
            None
        };
        Ok((native_bytes, record, raw_block))
    }

    fn finish_eof(mut self) -> Result<(), SettledSegmentsError> {
        let Some(reader) = self.reader.as_mut() else {
            return Err(SettledSegmentsError::Io(io::Error::other(
                "spool stream reader already closed",
            )));
        };
        let mut extra = [0_u8; 1];
        if reader.read(&mut extra)? != 0 {
            return Err(SettledSegmentsError::InvalidFormat(
                "staging spool contains unexpected trailing bytes".to_owned(),
            ));
        }
        drop(self.reader.take());
        if !self.temp_name.is_empty() {
            let name = std::mem::take(&mut self.temp_name);
            let _ = self.staging_dir.unlink_file(&name);
        }
        Ok(())
    }
}

impl Drop for SpoolStreamReader {
    fn drop(&mut self) {
        drop(self.reader.take());
        if !self.temp_name.is_empty() {
            let _ = self.staging_dir.unlink_file(&self.temp_name);
        }
    }
}

struct StagedStrideArtifacts {
    start_height: BlockHeight,
    end_block: BlockId,
    modules: SettledSegmentModules,
    shielded_staged_name: String,
    shielded_identity: ShieldedSegmentIdentity,
    supplement_staged_name: Option<String>,
    supplement_identity: Option<SupplementSegmentIdentity>,
    raw_block_staged_name: Option<String>,
    raw_block_identity: Option<RawBlockSegmentIdentity>,
    end_checkpoint: CanonicalSequenceCheckpoint,
}

struct StagingCleanupGuard {
    targets: Vec<(VerifiedDir, String)>,
}

impl StagingCleanupGuard {
    const fn new() -> Self {
        Self {
            targets: Vec::new(),
        }
    }

    fn track(&mut self, dir: &VerifiedDir, name: String) -> Result<(), SettledSegmentsError> {
        self.targets.push((dir.try_clone()?, name));
        Ok(())
    }

    fn disarm(&mut self) {
        self.targets.clear();
    }
}

impl Drop for StagingCleanupGuard {
    fn drop(&mut self) {
        for (dir, name) in &self.targets {
            let _ = dir.unlink_file(name);
        }
    }
}

fn validate_prior_checkpoint(
    checkpoint: CanonicalSequenceCheckpoint,
) -> Result<(), SettledSegmentsError> {
    if checkpoint.retained_block_count() == 0
        || checkpoint.sequence_digest().version() != CanonicalBlockFactsSequenceDigestVersion::V1
        || checkpoint.sequence_digest().block_count() != checkpoint.retained_block_count()
        || checkpoint.logical_replay_bytes() == 0
    {
        return Err(SettledSegmentsError::ChainLinkage(
            "invalid prior sequence checkpoint supplied to sealer".to_owned(),
        ));
    }
    Ok(())
}

fn init_sequence_accumulator(
    prior: Option<CanonicalSequenceCheckpoint>,
) -> (CanonicalBlockFactsSequenceDigestBuilder, u64, u64) {
    prior.map_or_else(
        || {
            (
                CanonicalBlockFactsSequenceDigestBuilder::new(
                    CanonicalBlockFactsSequenceDigestVersion::V1,
                ),
                0_u64,
                0_u64,
            )
        },
        |cp| {
            (
                CanonicalBlockFactsSequenceDigestBuilder::resume_from_prefix(cp.sequence_digest()),
                cp.retained_block_count(),
                cp.logical_replay_bytes(),
            )
        },
    )
}

fn validate_compact_facts_alignment(
    compact: &CompactBlockArtifact,
    facts: &CanonicalBlockFacts,
) -> Result<(), SettledSegmentsError> {
    let compact_txs = compact.transactions();
    if compact_txs.len() > facts.transactions.len() {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "block {} compact tx count {} exceeds canonical_facts tx count {}",
            compact.height().value(),
            compact_txs.len(),
            facts.transactions.len()
        )));
    }
    let mut prev_index: Option<u64> = None;
    for tx in compact_txs {
        if prev_index.is_some_and(|prev| tx.index <= prev) {
            return Err(SettledSegmentsError::ChainLinkage(format!(
                "block {} compact transaction indexes are not strictly ascending",
                compact.height().value()
            )));
        }
        prev_index = Some(tx.index);
        let idx = usize::try_from(tx.index).map_err(|_| {
            SettledSegmentsError::ChainLinkage(
                "compact transaction index exceeds host address space".to_owned(),
            )
        })?;
        let Some(fact_tx) = facts.transactions.get(idx) else {
            return Err(SettledSegmentsError::ChainLinkage(format!(
                "block {} canonical_facts missing compact transaction at index {}",
                compact.height().value(),
                tx.index
            )));
        };
        if fact_tx.public_facts.transaction_id != tx.transaction_id {
            return Err(SettledSegmentsError::ChainLinkage(format!(
                "block {} canonical_facts txid disagrees with compact transaction at index {}",
                compact.height().value(),
                tx.index
            )));
        }
    }
    Ok(())
}

struct SpooledBlockRecord {
    native_bytes: usize,
    encoded_record: Vec<u8>,
    encoded_raw_block: Option<Vec<u8>>,
    shielded_frame_bytes: u64,
    supplement_uncompressed_bytes: u64,
    raw_frame_bytes: u64,
    raw_tx_entries: usize,
}

fn encode_spool_record(
    block: &SettledSegmentBlockInput,
    config: &SettledSegmentSealerConfig,
) -> Result<SpooledBlockRecord, SettledSegmentsError> {
    let mut record = LiveTailBlockRecord::from_compact_block(
        &block.compact_block,
        config.modules.supplement,
        config
            .modules
            .supplement
            .then(|| block.canonical_facts.clone()),
    )?;
    record.canonical_facts = None;
    let shielded_frame_bytes =
        u64::try_from(SHIELDED_FRAME_PREFIX_LEN.saturating_add(record.shielded_payload.len()))
            .map_err(|_| {
                SettledSegmentsError::InvalidFormat("shielded frame length exceeds u64".to_owned())
            })?;
    let supplement_uncompressed_bytes = match &record.supplement {
        Some(sup) => {
            let identity = BlockEnvelopeIdentity {
                height: record.block_id.height,
                block_hash_wire: encode_internal_block_hash(record.block_id.hash),
                prev_hash_wire: encode_internal_block_hash(record.previous_block_hash),
                block_time: record.block_time,
            };
            let (encoded_sup, _) = encode_block_record(identity, sup)?;
            u64::try_from(encoded_sup.len()).map_err(|_| {
                SettledSegmentsError::InvalidFormat(
                    "supplement block record length exceeds u64".to_owned(),
                )
            })?
        }
        None => 0,
    };
    let (encoded_raw_block, raw_frame_bytes, raw_tx_entries) = match &block.raw_block {
        Some(raw) if config.modules.raw_bodies => {
            let raw_len = raw.raw_block_bytes.len();
            let stored_bound = match config.raw_block_codec {
                RawBlockCodec::None => raw_len,
                RawBlockCodec::Zstd => raw_len
                    .saturating_add(raw_len >> 8)
                    .saturating_add(ZSTD_BOUND_PER_FRAME_HEADROOM)
                    .min(MAX_RAW_COMPRESSED_PAYLOAD_BYTES),
            };
            let frame_bytes = u64::try_from(RAW_FRAME_PREFIX_LEN.saturating_add(stored_bound))
                .map_err(|_| {
                    SettledSegmentsError::InvalidFormat("raw frame length exceeds u64".to_owned())
                })?;
            let encoded_raw = raw.encode_v1()?;
            (Some(encoded_raw), frame_bytes, raw.tx_slices.len())
        }
        _ => (None, 0, 0),
    };
    let encoded_record = record.encode_v1()?;
    Ok(SpooledBlockRecord {
        native_bytes: block.native_bytes,
        encoded_record,
        encoded_raw_block,
        shielded_frame_bytes,
        supplement_uncompressed_bytes,
        raw_frame_bytes,
        raw_tx_entries,
    })
}

fn validate_single_block_fits_segment_caps(
    spooled: &SpooledBlockRecord,
    config: &SettledSegmentSealerConfig,
) -> Result<(), SettledSegmentsError> {
    let raw_len = spooled.encoded_raw_block.as_deref().map_or(0, <[u8]>::len);
    let Some(spool_frame_bytes) = spool_frame_total_bytes(spooled.encoded_record.len(), raw_len)
    else {
        return Err(SettledSegmentsError::InvalidFormat(
            "spool frame byte count overflow".to_owned(),
        ));
    };
    if spool_frame_bytes > config.max_spool_bytes {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "single block spool frame ({spool_frame_bytes} bytes) exceeds configured budget ({})",
            config.max_spool_bytes
        )));
    }
    let shielded_total = predicted_shielded_segment_bytes(1, spooled.shielded_frame_bytes);
    if spooled.shielded_frame_bytes > MAX_SHIELDED_SLAB_BYTES
        || shielded_total > config.max_shielded_segment_bytes
    {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "single block shielded segment ({shielded_total} bytes) exceeds segment cap ({})",
            config.max_shielded_segment_bytes
        )));
    }
    if config.modules.supplement {
        let sup_total = predicted_supplement_segment_bytes(
            1,
            spooled.supplement_uncompressed_bytes,
            config.supplement_options,
        );
        if sup_total > config.max_supplement_segment_bytes {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "single block supplement segment ({sup_total} bytes) exceeds segment cap ({})",
                config.max_supplement_segment_bytes
            )));
        }
    }
    if config.modules.raw_bodies {
        let raw_total =
            predicted_raw_segment_bytes(1, spooled.raw_frame_bytes, spooled.raw_tx_entries);
        if spooled.raw_frame_bytes > MAX_RAW_SLAB_BYTES || raw_total > config.max_raw_segment_bytes
        {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "single block raw segment ({raw_total} bytes) exceeds segment cap ({})",
                config.max_raw_segment_bytes
            )));
        }
        if spooled.raw_tx_entries > config.max_raw_tx_entries {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "single block raw tx count ({}) exceeds segment cap ({})",
                spooled.raw_tx_entries, config.max_raw_tx_entries
            )));
        }
    }
    Ok(())
}

fn validate_raw_input(
    facts: &CanonicalBlockFacts,
    raw_block: Option<&LiveTailRawBlock>,
    modules: SettledSegmentModules,
) -> Result<(), SettledSegmentsError> {
    if modules.raw_bodies && raw_block.is_none() {
        return Err(SettledSegmentsError::ChainLinkage(format!(
            "raw_bodies module is active but block {} lacks raw_block",
            facts.block_header.height.value()
        )));
    }
    if let Some(raw) = raw_block {
        raw.validate()?;
        raw.validate_against_facts(facts)?;
    }
    Ok(())
}

struct PreparedDirs {
    staging: VerifiedDir,
    shielded: VerifiedDir,
    supplement: Option<VerifiedDir>,
    raw_block: Option<VerifiedDir>,
}

fn prepare_segment_directories(
    store_root: &Path,
    modules: SettledSegmentModules,
) -> Result<PreparedDirs, SettledSegmentsError> {
    let root = VerifiedDir::create_or_open(store_root, &[])?;
    let segments = root.create_or_open_child_dir(SEGMENTS_DIR_NAME)?;
    let staging = segments.create_or_open_child_dir(SEGMENTS_STAGING_DIR_NAME)?;
    let shielded = segments.create_or_open_child_dir(SettledSegmentKind::Shielded.subdir_name())?;
    let supplement = modules
        .supplement
        .then(|| segments.create_or_open_child_dir(SettledSegmentKind::Supplement.subdir_name()))
        .transpose()?;
    let raw_block = modules
        .raw_bodies
        .then(|| segments.create_or_open_child_dir(SettledSegmentKind::RawBlock.subdir_name()))
        .transpose()?;
    Ok(PreparedDirs {
        staging,
        shielded,
        supplement,
        raw_block,
    })
}

struct StagedWriters {
    shielded_name: String,
    shielded_writer: SettledShieldedSegmentWriter,
    supplement_name: Option<String>,
    supplement_writer: Option<SettledSupplementSegmentWriter>,
    raw_block_name: Option<String>,
    raw_block_writer: Option<SettledRawBlockSegmentWriter>,
}

impl StagedWriters {
    fn open(
        staging_dir: &VerifiedDir,
        config: &SettledSegmentSealerConfig,
        start_height: BlockHeight,
        end_block: BlockId,
        cleanup: &mut StagingCleanupGuard,
    ) -> Result<Self, SettledSegmentsError> {
        let (shielded_name, shielded_writer) =
            open_staged_shielded_writer(staging_dir, config, start_height, end_block, cleanup)?;
        let (supplement_name, supplement_writer) = if config.modules.supplement {
            let (name, writer) = open_staged_supplement_writer(
                staging_dir,
                config,
                start_height,
                end_block,
                cleanup,
            )?;
            (Some(name), Some(writer))
        } else {
            (None, None)
        };
        let (raw_block_name, raw_block_writer) = if config.modules.raw_bodies {
            let (name, writer) = open_staged_raw_block_writer(
                staging_dir,
                config,
                start_height,
                end_block,
                cleanup,
            )?;
            (Some(name), Some(writer))
        } else {
            (None, None)
        };

        Ok(Self {
            shielded_name,
            shielded_writer,
            supplement_name,
            supplement_writer,
            raw_block_name,
            raw_block_writer,
        })
    }

    fn push_spooled_block(
        &mut self,
        native_bytes: usize,
        record: &LiveTailBlockRecord,
        raw_block: Option<&LiveTailRawBlock>,
    ) -> Result<(), SettledSegmentsError> {
        let compact = record.reconstruct_compact_block()?;
        if self.supplement_writer.is_some() {
            self.shielded_writer
                .push_with_supplement(&compact, native_bytes)?;
        } else {
            self.shielded_writer.push(&compact, native_bytes)?;
        }
        if let Some(sup_writer) = &mut self.supplement_writer {
            let sup_record = record.supplement.as_ref().ok_or_else(|| {
                SettledSegmentsError::ChainLinkage(
                    "supplement disappeared from staging spool while sealing".to_owned(),
                )
            })?;
            sup_writer.push_record(
                record.block_id,
                record.previous_block_hash,
                record.block_time,
                sup_record,
            )?;
        }
        if let Some(raw_writer) = &mut self.raw_block_writer {
            let raw = raw_block.ok_or_else(|| {
                SettledSegmentsError::ChainLinkage(
                    "raw_block disappeared from staging spool while sealing".to_owned(),
                )
            })?;
            let slices = raw.raw_slices();
            raw_writer.push(
                record.block_id,
                record.previous_block_hash,
                &raw.raw_block_bytes,
                &slices,
            )?;
        }
        Ok(())
    }

    fn finish_all(
        self,
        staging_dir: &VerifiedDir,
        config: &SettledSegmentSealerConfig,
    ) -> Result<StagedIdentities, SettledSegmentsError> {
        let (shielded_dev_ino, shielded_identity) = self.shielded_writer.finish_staged_file()?;
        let shielded_ro =
            staging_dir.open_regular_matching_inode(&self.shielded_name, shielded_dev_ino)?;
        enforce_staged_segment_size(&shielded_ro, config.max_shielded_segment_bytes, "shielded")?;
        segment_verity::protect_new_segment(
            &shielded_ro,
            shielded_identity.fs_verity_sha256,
            "shielded segment",
        )?;
        drop(shielded_ro);

        let supplement_identity = match (self.supplement_name.as_deref(), self.supplement_writer) {
            (Some(name), Some(writer)) => {
                let (dev_ino, sup_id) = writer.finish_staged_file()?;
                let sup_ro = staging_dir.open_regular_matching_inode(name, dev_ino)?;
                enforce_staged_segment_size(
                    &sup_ro,
                    config.max_supplement_segment_bytes,
                    "supplement",
                )?;
                segment_verity::protect_new_segment(
                    &sup_ro,
                    sup_id.fs_verity_sha256,
                    "supplement segment",
                )?;
                drop(sup_ro);
                Some(sup_id)
            }
            _ => None,
        };

        let raw_block_identity = match (self.raw_block_name.as_deref(), self.raw_block_writer) {
            (Some(name), Some(writer)) => {
                let (dev_ino, raw_id) = writer.finish_staged_file()?;
                let raw_ro = staging_dir.open_regular_matching_inode(name, dev_ino)?;
                enforce_staged_segment_size(&raw_ro, config.max_raw_segment_bytes, "raw block")?;
                segment_verity::protect_new_segment(
                    &raw_ro,
                    raw_id.fs_verity_sha256,
                    "raw block segment",
                )?;
                drop(raw_ro);
                Some(raw_id)
            }
            _ => None,
        };

        Ok(StagedIdentities {
            shielded_name: self.shielded_name,
            shielded_identity,
            supplement_name: self.supplement_name,
            supplement_identity,
            raw_block_name: self.raw_block_name,
            raw_block_identity,
        })
    }
}

fn enforce_staged_segment_size(
    file: &File,
    max_bytes: u64,
    label: &str,
) -> Result<(), SettledSegmentsError> {
    let observed = file.metadata()?.len();
    if observed > max_bytes {
        return Err(SettledSegmentsError::InvalidFormat(format!(
            "staged {label} segment ({observed} bytes) exceeds cap ({max_bytes} bytes)"
        )));
    }
    Ok(())
}

struct StagedIdentities {
    shielded_name: String,
    shielded_identity: ShieldedSegmentIdentity,
    supplement_name: Option<String>,
    supplement_identity: Option<SupplementSegmentIdentity>,
    raw_block_name: Option<String>,
    raw_block_identity: Option<RawBlockSegmentIdentity>,
}

fn prepare_staged_file(
    staging_dir: &VerifiedDir,
    staged_name: &str,
    cleanup: &mut StagingCleanupGuard,
) -> Result<File, SettledSegmentsError> {
    cleanup.track(staging_dir, staged_name.to_owned())?;
    match staging_dir.stat_entry_nofollow(staged_name)? {
        Some(EntryKind::RegularFile) => staging_dir.unlink_file(staged_name)?,
        Some(_) => {
            return Err(SettledSegmentsError::InvalidLayout(format!(
                "refusing non-regular entry {staged_name:?} inside segments/.staging"
            )));
        }
        None => {}
    }
    staging_dir.create_named_exclusive_rw(staged_name)
}

fn open_staged_shielded_writer(
    staging_dir: &VerifiedDir,
    config: &SettledSegmentSealerConfig,
    start_height: BlockHeight,
    end_block: BlockId,
    cleanup: &mut StagingCleanupGuard,
) -> Result<(String, SettledShieldedSegmentWriter), SettledSegmentsError> {
    let filename =
        format_segment_filename(SettledSegmentKind::Shielded, start_height, end_block.height)?;
    let staged_name = format!("staged-{filename}");
    let staged_file = prepare_staged_file(staging_dir, &staged_name, cleanup)?;
    let span = ShieldedSegmentIdentity::span(
        config.network,
        config.artifact_schema_version,
        start_height,
        end_block,
    );
    let writer = SettledShieldedSegmentWriter::create_in_file(staged_file, span)?;
    Ok((staged_name, writer))
}

fn open_staged_supplement_writer(
    staging_dir: &VerifiedDir,
    config: &SettledSegmentSealerConfig,
    start_height: BlockHeight,
    end_block: BlockId,
    cleanup: &mut StagingCleanupGuard,
) -> Result<(String, SettledSupplementSegmentWriter), SettledSegmentsError> {
    let filename = format_segment_filename(
        SettledSegmentKind::Supplement,
        start_height,
        end_block.height,
    )?;
    let staged_name = format!("staged-{filename}");
    let staged_file = prepare_staged_file(staging_dir, &staged_name, cleanup)?;
    let span = SupplementSegmentIdentity::span(
        config.network,
        config.artifact_schema_version,
        start_height,
        end_block,
    );
    let writer = SettledSupplementSegmentWriter::create_in_file(
        staged_file,
        span,
        config.supplement_options,
    )?;
    Ok((staged_name, writer))
}

fn open_staged_raw_block_writer(
    staging_dir: &VerifiedDir,
    config: &SettledSegmentSealerConfig,
    start_height: BlockHeight,
    end_block: BlockId,
    cleanup: &mut StagingCleanupGuard,
) -> Result<(String, SettledRawBlockSegmentWriter), SettledSegmentsError> {
    let filename =
        format_segment_filename(SettledSegmentKind::RawBlock, start_height, end_block.height)?;
    let staged_name = format!("staged-{filename}");
    let staged_file = prepare_staged_file(staging_dir, &staged_name, cleanup)?;
    let span = RawBlockSegmentIdentity::span(
        config.network,
        config.raw_block_codec,
        start_height,
        end_block,
    );
    let writer = SettledRawBlockSegmentWriter::create_in_file(staged_file, span)?;
    Ok((staged_name, writer))
}

fn stage_stride_files(
    staging_dir: &VerifiedDir,
    config: &SettledSegmentSealerConfig,
    active: ActiveStrideState,
) -> Result<StagedStrideArtifacts, SettledSegmentsError> {
    let mut cleanup = StagingCleanupGuard::new();
    let mut spool_reader = active.spool.into_reader()?;
    let mut writers = StagedWriters::open(
        staging_dir,
        config,
        active.start_height,
        active.last_block_id,
        &mut cleanup,
    )?;

    for _ in 0..active.block_count {
        let (native_bytes, record, raw_block) = spool_reader.read_next_frame()?;
        writers.push_spooled_block(native_bytes, &record, raw_block.as_ref())?;
    }
    spool_reader.finish_eof()?;

    let identities = writers.finish_all(staging_dir, config)?;
    let end_checkpoint = CanonicalSequenceCheckpoint::from_admitted_parts(
        active.last_block_id,
        active.retained_count,
        active.digest_builder.finish(),
        active.logical_replay_bytes,
    );
    staging_dir.sync()?;
    cleanup.disarm();
    Ok(StagedStrideArtifacts {
        start_height: active.start_height,
        end_block: active.last_block_id,
        modules: config.modules,
        shielded_staged_name: identities.shielded_name,
        shielded_identity: identities.shielded_identity,
        supplement_staged_name: identities.supplement_name,
        supplement_identity: identities.supplement_identity,
        raw_block_staged_name: identities.raw_block_name,
        raw_block_identity: identities.raw_block_identity,
        end_checkpoint,
    })
}

fn promote_staged_stride(
    dirs: &PreparedDirs,
    start_height: BlockHeight,
    end_block: BlockId,
    staged: &StagedStrideArtifacts,
    end_sequence_checkpoint: CanonicalSequenceCheckpoint,
) -> Result<SettledSegmentIndexEntry, SettledSegmentsError> {
    let mut cleanup = StagingCleanupGuard::new();
    cleanup.track(&dirs.staging, staged.shielded_staged_name.clone())?;
    if let Some(sup_name) = &staged.supplement_staged_name {
        cleanup.track(&dirs.staging, sup_name.clone())?;
    }
    if let Some(raw_name) = &staged.raw_block_staged_name {
        cleanup.track(&dirs.staging, raw_name.clone())?;
    }

    let range = (start_height, end_block.height);
    let shielded_final = format_segment_filename(SettledSegmentKind::Shielded, range.0, range.1)?;
    promote_single_file(
        &dirs.staging,
        &dirs.shielded,
        &staged.shielded_staged_name,
        &shielded_final,
        staged.shielded_identity.sha256,
    )?;

    if let (Some(sup_dir), Some(sup_staged), Some(sup_id)) = (
        dirs.supplement.as_ref(),
        staged.supplement_staged_name.as_deref(),
        staged.supplement_identity.as_ref(),
    ) {
        let sup_final = format_segment_filename(SettledSegmentKind::Supplement, range.0, range.1)?;
        promote_single_file(
            &dirs.staging,
            sup_dir,
            sup_staged,
            &sup_final,
            sup_id.sha256,
        )?;
    }

    if let (Some(raw_dir), Some(raw_staged), Some(raw_id)) = (
        dirs.raw_block.as_ref(),
        staged.raw_block_staged_name.as_deref(),
        staged.raw_block_identity.as_ref(),
    ) {
        let raw_final = format_segment_filename(SettledSegmentKind::RawBlock, range.0, range.1)?;
        promote_single_file(
            &dirs.staging,
            raw_dir,
            raw_staged,
            &raw_final,
            raw_id.sha256,
        )?;
    }

    cleanup.disarm();
    dirs.staging.sync()?;

    let entry = SettledSegmentIndexEntry {
        start_height,
        end_block,
        modules: staged.modules,
        shielded_identity: staged.shielded_identity,
        supplement_identity: staged.supplement_identity,
        raw_block_identity: staged.raw_block_identity,
        end_sequence_checkpoint,
    };
    entry.validate()?;
    Ok(entry)
}

const SPOOL_HASH_CHUNK_BYTES: usize = 64 * 1024;
const TRAILER_DIGEST_LEN: usize = 32;

fn promote_single_file(
    staging_dir: &VerifiedDir,
    dest_dir: &VerifiedDir,
    staged_name: &str,
    final_name: &str,
    expected_body_sha256: [u8; 32],
) -> Result<(), SettledSegmentsError> {
    match dest_dir.stat_entry_nofollow(final_name)? {
        Some(EntryKind::RegularFile) => {
            verify_existing_regular_destination(
                staging_dir,
                dest_dir,
                staged_name,
                final_name,
                expected_body_sha256,
            )?;
        }
        Some(_) => {
            return Err(SettledSegmentsError::InvalidLayout(format!(
                "refusing non-regular settled segment destination {final_name}"
            )));
        }
        None => match staging_dir.rename_noclobber_to_dir(staged_name, dest_dir, final_name) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                match dest_dir.stat_entry_nofollow(final_name)? {
                    Some(EntryKind::RegularFile) => {
                        verify_existing_regular_destination(
                            staging_dir,
                            dest_dir,
                            staged_name,
                            final_name,
                            expected_body_sha256,
                        )?;
                    }
                    _ => {
                        return Err(SettledSegmentsError::InvalidLayout(format!(
                            "refusing non-regular settled segment destination {final_name}"
                        )));
                    }
                }
            }
            Err(err) => return Err(SettledSegmentsError::Io(err)),
        },
    }
    dest_dir.sync()
}

fn verify_existing_regular_destination(
    staging_dir: &VerifiedDir,
    dest_dir: &VerifiedDir,
    staged_name: &str,
    final_name: &str,
    expected_body_sha256: [u8; 32],
) -> Result<(), SettledSegmentsError> {
    let mut staged_file = staging_dir.open_regular_file_nofollow(staged_name)?;
    let (staged_len, staged_full_sha) =
        measure_segment_file(&mut staged_file, staged_name, expected_body_sha256)?;
    let mut final_file = dest_dir.open_regular_file_nofollow(final_name)?;
    let (final_len, final_full_sha) =
        measure_segment_file(&mut final_file, final_name, expected_body_sha256)?;
    if final_len != staged_len || final_full_sha != staged_full_sha {
        return Err(SettledSegmentsError::Integrity(format!(
            "existing settled segment destination {final_name} differs from staged segment"
        )));
    }
    staging_dir.unlink_file(staged_name)?;
    final_file.sync_all()?;
    Ok(())
}

fn measure_segment_file(
    file: &mut File,
    label: &str,
    expected_body_sha256: [u8; 32],
) -> Result<(u64, [u8; 32]), SettledSegmentsError> {
    use sha2::Digest as _;
    use sha2::Sha256;

    let total_len = file.metadata()?.len();
    let trailer_len_u64 = u64::try_from(TRAILER_DIGEST_LEN).unwrap_or(32);
    if total_len < trailer_len_u64 {
        return Err(SettledSegmentsError::Integrity(format!(
            "settled segment file {label} truncated before SHA-256 trailer"
        )));
    }
    let body_len = total_len - trailer_len_u64;
    let mut body_hasher = Sha256::new();
    let mut full_hasher = Sha256::new();
    let mut chunk = vec![0_u8; SPOOL_HASH_CHUNK_BYTES];
    let chunk_cap = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
    let mut remaining = body_len;
    while remaining > 0 {
        let chunk_len = remaining.min(chunk_cap);
        let to_read = usize::try_from(chunk_len).map_err(|_| {
            SettledSegmentsError::InvalidFormat(
                "segment hash chunk exceeds address space".to_owned(),
            )
        })?;
        file.read_exact(&mut chunk[..to_read])?;
        body_hasher.update(&chunk[..to_read]);
        full_hasher.update(&chunk[..to_read]);
        remaining -= chunk_len;
    }
    let mut trailer = [0_u8; TRAILER_DIGEST_LEN];
    file.read_exact(&mut trailer)?;
    let mut trailing = [0_u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(SettledSegmentsError::Integrity(format!(
            "settled segment file {label} grew during digest verification"
        )));
    }
    let computed_body: [u8; 32] = body_hasher.finalize().into();
    if computed_body != expected_body_sha256 || trailer != expected_body_sha256 {
        return Err(SettledSegmentsError::Integrity(format!(
            "settled segment file {label} failed SHA-256 trailer verification"
        )));
    }
    full_hasher.update(trailer);
    Ok((total_len, full_hasher.finalize().into()))
}
