//! Fresh canonical construction from one ordered node-source pass.

pub(crate) mod progress;
pub(crate) mod source_fetch;
mod tree_hashing;
pub(crate) mod watermark;

use std::{
    collections::{BTreeMap, VecDeque},
    num::{NonZeroU32, NonZeroU64},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures_util::{
    Stream, StreamExt,
    future::BoxFuture,
    stream::{self, BoxStream, FuturesUnordered},
};
use metrics::Histogram;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::{
    OwnedSemaphorePermit, Semaphore,
    mpsc::{self, error::TryRecvError},
};
use tokio_util::sync::CancellationToken;
use zinder_core::{
    BatchAppendedBlock, BatchPoolAppendDurations, BlockCommitmentBatchInput, BlockHeight, BlockId,
    ChainTipMetadata, CommitmentTreeAccumulator, CommitmentTreeAccumulatorError,
    CommitmentTreeCheckpoint, CommitmentTreeSnapshot, Network, NetworkUpgradeActivations,
    NetworkUpgradeActivationsFingerprint, NetworkUpgradeActivationsFingerprintVersion,
    ShieldedProtocol, SubtreeRootIndex, SubtreeRootRange,
};
use zinder_source::{NodeSource, SourceBlock, SourceKnownTip};
use zinder_store::{
    CanonicalBlockLoadEvidence, CanonicalBuildBlock, CanonicalBuildSubtreeRoot,
    CanonicalStoreBuildError, CanonicalStoreError, CanonicalSubtreeRootLoadEvidence,
    RawBlobRetention, RocksDbCanonicalBuilder, TREE_STATE_CHECKPOINT_STRIDE,
    TreeStateCheckpointSelection,
};

use crate::{
    artifact_builder::{
        CommitmentTreeSizes, PositionedCanonicalBlock, PreparedCanonicalBlock, RetainedRawBlobs,
        position_canonical_block, prepare_canonical_block_for_retention,
    },
    chain_ingest::IngestError,
    subtree_verification::SubtreeVerificationObservation,
};
use progress::ConstructionProgress;
use source_fetch::{
    CanonicalSourceFetchConfig, SourceBlockChunk, SourceSegmentSizer, build_source_block_stream,
};
use tree_hashing::{GuardedBuildBlock, PipelinedBuildBlocks, TreeHashers};

const CANONICAL_PREPARE_MEMORY_UNIT_BYTES: u64 = 1_024;
const CANONICAL_PREPARE_RAW_BLOCK_MULTIPLIER: u64 = 16;
const CANONICAL_PREPARE_FIXED_BYTES: u64 = 64 * 1_024;
const MIB: u64 = 1_024 * 1_024;
const DEFAULT_SOURCE_SEGMENT_TARGET_RESPONSE_BYTES: u64 = 32 * MIB;
const DEFAULT_SOURCE_SEGMENT_MAX_BLOCKS: u32 = 64;
/// Each segment block is one JSON-RPC batch entry, and Zakura rejects a batch
/// with more than 1,000 entries.
const SOURCE_SEGMENT_MAX_BLOCKS_CEILING: u32 = 1_000;
const DEFAULT_SOURCE_FETCH_MAX_IN_FLIGHT_REQUESTS: u32 = 12;
const FALLBACK_SOURCE_FETCH_MAX_IN_FLIGHT_BYTES: u64 = 384 * MIB;
const FALLBACK_BLOCK_PREPARE_MEMORY_WATERMARK_BYTES: u64 = 512 * MIB;
const MIN_PIPELINE_MEMORY_WATERMARK_BYTES: u64 = 128 * MIB;
const PIPELINE_MEMORY_BUDGET_DIVISOR: u64 = 64;
const BLOCK_PREPARE_CONCURRENCY_CEILING: u32 = 16;
const AUTOMATIC_TREE_HASHING_WORKERS_CEILING: u32 = 8;
const CONSTRUCTION_PHASE_DURATION_METRIC: &str =
    "zinder_ingest_canonical_construction_phase_duration_seconds";
const BLOCK_LOAD_STAGE_DURATION_METRIC: &str =
    "zinder_ingest_canonical_block_load_stage_duration_seconds";
const TREE_APPEND_POOL_DURATION_METRIC: &str =
    "zinder_ingest_canonical_tree_append_duration_seconds";
/// Most prepared blocks combined into one cross-block commitment-tree append.
const MAX_COMMITMENT_BATCH_BLOCKS: usize = 64;
/// Soft ceiling on total note commitments combined into one loader tree batch.
const MAX_COMMITMENT_BATCH_COMMITMENTS: usize = 16_384;

/// Records how long one completed construction phase took.
pub(crate) fn record_construction_phase(phase: &'static str, started_at: Instant) {
    metrics::histogram!(CONSTRUCTION_PHASE_DURATION_METRIC, "phase" => phase)
        .record(started_at.elapsed());
}

/// Shared source-fetch and block-preparation limits for canonical ingestion.
///
/// Fresh store construction and the construction benchmarks consume this
/// value so runtime resource resolution cannot silently produce two pipeline
/// shapes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalPipelineLimits {
    /// Maximum bytes accepted from one source response.
    pub max_response_bytes: NonZeroU64,
    /// Adaptive target for one source response payload.
    pub source_segment_target_response_bytes: NonZeroU64,
    /// Maximum blocks requested in one source segment.
    pub source_segment_max_blocks: NonZeroU32,
    /// Maximum concurrent source-segment requests.
    pub source_fetch_max_in_flight_requests: NonZeroU32,
    /// Aggregate byte watermark for in-flight source responses.
    pub source_fetch_max_in_flight_bytes: NonZeroU64,
    /// Maximum canonical block preparations in flight.
    pub block_prepare_concurrency: NonZeroU32,
    /// Aggregate byte watermark for block preparation and queued canonical blocks.
    pub block_prepare_memory_watermark_bytes: NonZeroU64,
    /// Threads that hash the ordered loader's commitment trees.
    ///
    /// One hashes on the loader thread itself. More run each block batch's
    /// commitment-tree appends and distinct pool frontier cross-checks in
    /// parallel across all worker threads, while a bounded handoff queue lets
    /// the next batch's tree hashing overlap with `RocksDB` SST staging.
    /// Blocks still reach the store in height order with the same trees and
    /// checkpoints.
    pub tree_hashing_workers: NonZeroU32,
}

impl CanonicalPipelineLimits {
    /// Resolves canonical pipeline limits from the runtime resource envelope.
    #[must_use]
    pub fn resolve(
        memory_budget_bytes: Option<NonZeroU64>,
        logical_core_count: NonZeroU32,
        max_response_bytes: NonZeroU64,
    ) -> Self {
        let source_fetch_max_in_flight_bytes = pipeline_memory_watermark(
            memory_budget_bytes,
            FALLBACK_SOURCE_FETCH_MAX_IN_FLIGHT_BYTES,
        )
        .max(max_response_bytes);
        Self {
            max_response_bytes,
            source_segment_target_response_bytes: NonZeroU64::new(
                max_response_bytes
                    .get()
                    .min(DEFAULT_SOURCE_SEGMENT_TARGET_RESPONSE_BYTES),
            )
            .unwrap_or(NonZeroU64::MIN),
            source_segment_max_blocks: NonZeroU32::new(DEFAULT_SOURCE_SEGMENT_MAX_BLOCKS)
                .unwrap_or(NonZeroU32::MIN),
            source_fetch_max_in_flight_requests: NonZeroU32::new(
                DEFAULT_SOURCE_FETCH_MAX_IN_FLIGHT_REQUESTS,
            )
            .unwrap_or(NonZeroU32::MIN),
            source_fetch_max_in_flight_bytes,
            block_prepare_concurrency: core_bounded_concurrency(logical_core_count),
            block_prepare_memory_watermark_bytes: pipeline_memory_watermark(
                memory_budget_bytes,
                FALLBACK_BLOCK_PREPARE_MEMORY_WATERMARK_BYTES,
            ),
            tree_hashing_workers: NonZeroU32::MIN,
        }
    }

    /// Returns these limits with one tree-hashing thread per detected logical
    /// CPU, at most 8, or with one thread when the CPU count is unknown.
    ///
    /// Construction that stores checkpoints beyond the cadence and build tip
    /// spends most of its loader time hashing, so it defaults to this. Sparse
    /// construction keeps hashing on the loader thread, where extra threads
    /// slowed hosts with two cores. Eight is the most workers a full Mainnet
    /// build has measured, with in-flight block memory bounded by
    /// `block_prepare_memory_watermark_bytes` across tree hashing and SST
    /// staging.
    #[must_use]
    pub fn with_checkpoint_hashing(self, detected_logical_cpus: Option<NonZeroU32>) -> Self {
        let ceiling =
            NonZeroU32::new(AUTOMATIC_TREE_HASHING_WORKERS_CEILING).unwrap_or(NonZeroU32::MIN);
        Self {
            tree_hashing_workers: detected_logical_cpus
                .map_or(NonZeroU32::MIN, |cpus| cpus.min(ceiling)),
            ..self
        }
    }

    /// Returns these explicit limits after validating their cross-field invariants.
    pub fn validate(self) -> Result<Self, CanonicalPipelineLimitsError> {
        if self.source_segment_max_blocks.get() > SOURCE_SEGMENT_MAX_BLOCKS_CEILING {
            return Err(
                CanonicalPipelineLimitsError::SourceSegmentExceedsBatchLimit {
                    source_segment_max_blocks: self.source_segment_max_blocks,
                    max_batch_entries: SOURCE_SEGMENT_MAX_BLOCKS_CEILING,
                },
            );
        }
        if self.source_segment_target_response_bytes > self.max_response_bytes {
            return Err(
                CanonicalPipelineLimitsError::SourceTargetExceedsResponseLimit {
                    source_segment_target_response_bytes: self.source_segment_target_response_bytes,
                    max_response_bytes: self.max_response_bytes,
                },
            );
        }
        if self.source_fetch_max_in_flight_bytes < self.max_response_bytes {
            return Err(
                CanonicalPipelineLimitsError::SourceWatermarkBelowResponseLimit {
                    source_fetch_max_in_flight_bytes: self.source_fetch_max_in_flight_bytes,
                    max_response_bytes: self.max_response_bytes,
                },
            );
        }
        Ok(self)
    }
}

/// Invalid explicitly configured canonical pipeline limits.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum CanonicalPipelineLimitsError {
    /// One source segment would need more JSON-RPC batch entries than a node accepts.
    #[error(
        "source segment max blocks {source_segment_max_blocks} exceeds the node batch limit of {max_batch_entries} requests"
    )]
    SourceSegmentExceedsBatchLimit {
        /// Configured maximum blocks in one source segment.
        source_segment_max_blocks: NonZeroU32,
        /// Largest JSON-RPC batch every supported node accepts.
        max_batch_entries: u32,
    },
    /// The adaptive response target is larger than the node response cap.
    #[error(
        "source segment target {source_segment_target_response_bytes} exceeds maximum response {max_response_bytes}"
    )]
    SourceTargetExceedsResponseLimit {
        /// Configured adaptive response target.
        source_segment_target_response_bytes: NonZeroU64,
        /// Maximum response accepted from the node.
        max_response_bytes: NonZeroU64,
    },
    /// One legal node response would exceed the entire source-fetch watermark.
    #[error(
        "source fetch watermark {source_fetch_max_in_flight_bytes} is below maximum response {max_response_bytes}"
    )]
    SourceWatermarkBelowResponseLimit {
        /// Aggregate source-fetch byte watermark.
        source_fetch_max_in_flight_bytes: NonZeroU64,
        /// Maximum response accepted from the node.
        max_response_bytes: NonZeroU64,
    },
}

fn core_bounded_concurrency(logical_core_count: NonZeroU32) -> NonZeroU32 {
    logical_core_count
        .min(NonZeroU32::new(BLOCK_PREPARE_CONCURRENCY_CEILING).unwrap_or(NonZeroU32::MIN))
}

fn pipeline_memory_watermark(
    memory_budget_bytes: Option<NonZeroU64>,
    fallback_bytes: u64,
) -> NonZeroU64 {
    let bytes = memory_budget_bytes.map_or(fallback_bytes, |memory_budget_bytes| {
        (memory_budget_bytes.get() / PIPELINE_MEMORY_BUDGET_DIVISOR)
            .clamp(MIN_PIPELINE_MEMORY_WATERMARK_BYTES, fallback_bytes)
    });
    NonZeroU64::new(bytes).unwrap_or(NonZeroU64::MIN)
}

/// Bounded source-fetch and preparation settings for fresh canonical construction.
#[derive(Clone, Debug)]
pub struct CanonicalConstructionConfig {
    /// Timeout applied to one upstream source request.
    pub request_timeout: Duration,
    /// Shared source-fetch and block-preparation limits.
    pub pipeline_limits: CanonicalPipelineLimits,
    /// Node-discovered consensus upgrade activations used to parse transaction facts.
    pub network_upgrade_activations: Arc<NetworkUpgradeActivations>,
}

impl CanonicalConstructionConfig {
    /// Returns bounded settings suitable for deterministic local integration tests.
    #[must_use]
    pub fn for_local_tests(
        request_timeout: Duration,
        network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    ) -> Self {
        Self {
            request_timeout,
            pipeline_limits: CanonicalPipelineLimits {
                max_response_bytes: NonZeroU64::new(16 * MIB).unwrap_or(NonZeroU64::MIN),
                source_segment_target_response_bytes: NonZeroU64::new(8 * MIB)
                    .unwrap_or(NonZeroU64::MIN),
                source_segment_max_blocks: NonZeroU32::new(8).unwrap_or(NonZeroU32::MIN),
                source_fetch_max_in_flight_requests: NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
                source_fetch_max_in_flight_bytes: NonZeroU64::new(32 * MIB)
                    .unwrap_or(NonZeroU64::MIN),
                block_prepare_concurrency: NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
                block_prepare_memory_watermark_bytes: NonZeroU64::new(32 * MIB)
                    .unwrap_or(NonZeroU64::MIN),
                tree_hashing_workers: NonZeroU32::MIN,
            },
            network_upgrade_activations,
        }
    }
}

/// Failure while constructing a fresh canonical store.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CanonicalConstructionError {
    /// Source fetch or canonical block preparation failed.
    #[error("canonical construction source pipeline failed")]
    Source {
        /// Concrete ingest failure preserved for diagnosis.
        #[source]
        source: IngestError,
    },
    /// Canonical storage construction failed.
    #[error(transparent)]
    Store(#[from] CanonicalStoreError),
    /// Configured upgrade activations describe another network.
    #[error(
        "canonical construction activations use {configured_network:?}, expected {store_network:?}"
    )]
    NetworkMismatch {
        /// Network persisted by the fresh canonical builder.
        store_network: Network,
        /// Network advertised by the activation table.
        configured_network: Network,
    },
    /// Configured upgrade activations differ from the immutable store identity.
    #[error(
        "canonical construction activation fingerprint {configured_fingerprint:?} does not match store identity {store_fingerprint:?}"
    )]
    NetworkUpgradeActivationsMismatch {
        /// Fingerprint persisted before canonical source work begins.
        store_fingerprint: NetworkUpgradeActivationsFingerprint,
        /// Fingerprint derived from the construction activation table.
        configured_fingerprint: NetworkUpgradeActivationsFingerprint,
    },
    /// A source block is labeled for another network than the fresh store.
    #[error(
        "source block {height:?} uses {source_network:?}, expected canonical store network {store_network:?}"
    )]
    SourceBlockNetworkMismatch {
        /// Height reported by the mismatched source block.
        height: BlockHeight,
        /// Network persisted by the fresh canonical builder.
        store_network: Network,
        /// Network declared by the source block.
        source_network: Network,
    },
    /// The ordered commitment-tree accumulator rejected a predecessor or block update.
    #[error("canonical commitment-tree state failed at height {height:?}")]
    CommitmentTreeState {
        /// Height being seeded or applied.
        height: BlockHeight,
        /// Exact accumulator failure.
        #[source]
        source: CommitmentTreeAccumulatorError,
    },
    /// A commitment copied into the compact-block representation has the wrong width.
    #[error(
        "block {height:?} contains a {protocol:?} compact commitment with {byte_count} bytes; expected 32"
    )]
    CompactCommitmentWidth {
        /// Block containing the malformed compact commitment.
        height: BlockHeight,
        /// Shielded pool whose commitment was malformed.
        protocol: ShieldedProtocol,
        /// Observed byte length.
        byte_count: usize,
    },
    /// The independently calculated compact-block position and typed frontier diverged.
    #[error(
        "block {height:?} commitment-tree positions diverged: size-only={positioned:?}, accumulated={accumulated:?}"
    )]
    CommitmentTreePositionMismatch {
        /// Block where the two independent calculations disagreed.
        height: BlockHeight,
        /// Position stamped into the compact block.
        positioned: ChainTipMetadata,
        /// Position derived from typed commitment-tree frontiers.
        accumulated: ChainTipMetadata,
    },
    /// The node holds the fixed build tip, but on two reads its root for a
    /// subtree that a loaded block completed differs from the root derived
    /// from the loaded blocks' note commitments.
    #[error(
        "source {protocol:?} subtree {} root differs from the root derived from the loaded blocks",
        .subtree_index.value()
    )]
    SourceSubtreeRootMismatch {
        /// Shielded pool of the subtree.
        protocol: ShieldedProtocol,
        /// First subtree whose root, index, or completing height differed.
        subtree_index: SubtreeRootIndex,
    },
    /// The blocking SST loader task could not complete.
    #[error("canonical loader task failed: {reason}")]
    LoaderTaskFailed {
        /// Tokio blocking-task failure.
        reason: String,
    },
    /// One exact source-family request exceeded the construction deadline.
    #[error("canonical source request {operation} exceeded timeout {timeout:?}")]
    SourceRequestTimedOut {
        /// Exact source operation that did not finish.
        operation: &'static str,
        /// Per-request deadline from construction config.
        timeout: Duration,
    },
    /// The commitment-tree hashing threads could not be started.
    #[error("could not start {workers} commitment-tree hashing threads: {reason}")]
    TreeHashingWorkersUnavailable {
        /// Configured `tree_hashing_workers`.
        workers: u32,
        /// Thread-pool construction failure.
        reason: String,
    },
    /// Shutdown stopped construction before the store was published.
    ///
    /// Only the canonical writer's cancellable construction returns this; the
    /// unpublished staging is discarded on the next start.
    #[error("canonical construction was cancelled before publication")]
    Cancelled,
}

/// Result of staging and ingesting block-local families into a fresh store.
pub struct CanonicalBlockLoadOutcome {
    /// Exclusive builder retained for subsequent source and publication families.
    pub builder: RocksDbCanonicalBuilder,
    /// Prepared row counts and SST measurements accepted by `RocksDB`.
    ///
    /// Cache-bypassing family readback remains a separate prerequisite for
    /// publishing this build as `READY`.
    pub evidence: CanonicalBlockLoadEvidence,
    /// Roots of the subtrees the loaded blocks completed, derived from their
    /// note commitments, in the order the node's ranges list them.
    pub derived_subtree_roots: Vec<CanonicalBuildSubtreeRoot>,
}

/// Complete source-family outcome ready for fresh-reopen publication validation.
pub struct CanonicalSourceLoadOutcome {
    /// Exclusive builder retaining authenticated source evidence.
    pub builder: RocksDbCanonicalBuilder,
    /// Block-local family measurements.
    pub block_evidence: CanonicalBlockLoadEvidence,
    /// Contiguous completed-subtree measurements.
    pub subtree_root_evidence: CanonicalSubtreeRootLoadEvidence,
}

pub(crate) struct FreshCanonicalLoadOptions<'a> {
    pub(crate) cancel: &'a CancellationToken,
    pub(crate) progress: Option<ConstructionProgress>,
    pub(crate) known_source_tip_height: Option<BlockHeight>,
}

/// Loads every block-local canonical family from one ordered source pass.
///
/// Parallel preparation parses each source block exactly once. The blocking
/// store consumer performs ordered commitment-tree positioning immediately
/// before it fans the owned build block into the canonical column families.
/// The store remains `BUILDING` until the remaining source-observed families
/// and baseline publication records are loaded and validated.
pub async fn load_fresh_canonical_blocks<Source>(
    builder: RocksDbCanonicalBuilder,
    source: &Source,
    config: &CanonicalConstructionConfig,
) -> Result<CanonicalBlockLoadOutcome, CanonicalConstructionError>
where
    Source: NodeSource + Clone,
{
    let cancel = CancellationToken::new();
    Box::pin(load_fresh_canonical_blocks_until_cancelled(
        builder,
        source,
        config,
        FreshCanonicalLoadOptions {
            cancel: &cancel,
            progress: None,
            known_source_tip_height: None,
        },
    ))
    .await
}

async fn load_fresh_canonical_blocks_until_cancelled<Source>(
    builder: RocksDbCanonicalBuilder,
    source: &Source,
    config: &CanonicalConstructionConfig,
    options: FreshCanonicalLoadOptions<'_>,
) -> Result<CanonicalBlockLoadOutcome, CanonicalConstructionError>
where
    Source: NodeSource + Clone,
{
    let started_at = Instant::now();
    register_prohibited_read_metrics();
    let build_plan = builder.build_plan().clone();
    validate_construction_identity(&build_plan, config)?;
    let progress = options
        .progress
        .unwrap_or_else(|| ConstructionProgress::start(&build_plan, None));
    let prepare_concurrency =
        usize::try_from(config.pipeline_limits.block_prepare_concurrency.get())
            .unwrap_or(usize::MAX)
            .max(1);
    let block_queue_capacity = if config.pipeline_limits.tree_hashing_workers.get() > 1 {
        prepare_concurrency.max(MAX_COMMITMENT_BATCH_BLOCKS)
    } else {
        prepare_concurrency
    };
    let prepared_blocks = build_prepared_block_stream(
        source,
        &build_plan,
        config,
        builder.raw_blob_retention(),
        options.known_source_tip_height,
    );
    let checkpoint_selection = builder.tree_state_checkpoint_selection();
    let outcome = drive_block_loader(
        builder,
        prepared_blocks,
        BlockLoaderSettings {
            block_queue_capacity,
            network_upgrade_activations: Arc::clone(&config.network_upgrade_activations),
            checkpoint_selection,
            tree_hashing_workers: config.pipeline_limits.tree_hashing_workers,
            cancel: options.cancel.clone(),
            progress,
        },
    )
    .await?;
    record_construction_phase("block_load", started_at);
    Ok(outcome)
}

/// Authenticates the final fixed tip and loads the subtree roots derived from
/// the loaded blocks once the node reports the same roots.
pub async fn load_fresh_canonical_source_families<Source>(
    block_outcome: CanonicalBlockLoadOutcome,
    source: &Source,
    config: &CanonicalConstructionConfig,
) -> Result<CanonicalSourceLoadOutcome, CanonicalConstructionError>
where
    Source: NodeSource,
{
    let started_at = Instant::now();
    let CanonicalBlockLoadOutcome {
        mut builder,
        evidence: block_evidence,
        derived_subtree_roots,
    } = block_outcome;
    validate_network_upgrade_activations(
        builder.build_plan(),
        &config.network_upgrade_activations,
    )?;
    let required_ranges = builder.required_subtree_root_ranges()?;
    let mut observation =
        SubtreeVerificationObservation::new(required_ranges.iter().map(|range| range.protocol));
    let source_subtree_roots = fetch_source_subtree_roots(source, config, &required_ranges).await?;
    // The subtree-root reads name no block. The fixed tip is read by hash
    // after them, so a node that no longer holds it refuses that read.
    let fixed_tip = builder.build_plan().build_tip();
    let source_tip_checkpoint = tokio::time::timeout(
        config.request_timeout,
        source.fetch_chain_checkpoint_for_block(fixed_tip, &config.network_upgrade_activations),
    )
    .await
    .map_err(|_| CanonicalConstructionError::SourceRequestTimedOut {
        operation: "fetch fixed-tip checkpoint",
        timeout: config.request_timeout,
    })?
    .map_err(|source| CanonicalConstructionError::Source {
        source: IngestError::from(source),
    })?;
    builder.confirm_source_tip_checkpoint(&source_tip_checkpoint)?;
    if first_differing_subtree_root(&derived_subtree_roots, &source_subtree_roots).is_some() {
        observation.begin_recheck();
        // A node that switched away and back between the two reads answered
        // the first one for another chain, so it is asked once more while it
        // holds the fixed tip.
        let source_subtree_roots =
            fetch_source_subtree_roots(source, config, &required_ranges).await?;
        if let Some(differing_root) =
            first_differing_subtree_root(&derived_subtree_roots, &source_subtree_roots)
        {
            observation.mismatch();
            return Err(CanonicalConstructionError::SourceSubtreeRootMismatch {
                protocol: differing_root.protocol,
                subtree_index: differing_root.subtree_index,
            });
        }
        observation.recheck_match();
    } else {
        observation.first_match();
    }
    drop(observation);
    let subtree_root_evidence = builder.load_subtree_roots(derived_subtree_roots)?;
    builder.record_resource_gauges();
    record_construction_phase("source_families", started_at);
    Ok(CanonicalSourceLoadOutcome {
        builder,
        block_evidence,
        subtree_root_evidence,
    })
}

/// Reads the node's roots for `ranges`, in range order.
async fn fetch_source_subtree_roots<Source>(
    source: &Source,
    config: &CanonicalConstructionConfig,
    ranges: &[SubtreeRootRange],
) -> Result<Vec<CanonicalBuildSubtreeRoot>, CanonicalConstructionError>
where
    Source: NodeSource,
{
    let mut subtree_roots = Vec::new();
    for range in ranges {
        let source_roots = tokio::time::timeout(
            config.request_timeout,
            source.fetch_subtree_root_range(*range),
        )
        .await
        .map_err(|_| CanonicalConstructionError::SourceRequestTimedOut {
            operation: "fetch exact subtree-root range",
            timeout: config.request_timeout,
        })?
        .map_err(|source| CanonicalConstructionError::Source {
            source: IngestError::from(source),
        })?;
        subtree_roots.extend(source_roots.subtree_roots.into_iter().map(|root| {
            CanonicalBuildSubtreeRoot {
                protocol: source_roots.protocol,
                subtree_index: root.subtree_index,
                root_hash: root.root_hash,
                completing_block_height: root.completing_block_height,
            }
        }));
    }
    Ok(subtree_roots)
}

/// Loads every clean-v1 source family needed before publication validation.
pub async fn load_fresh_canonical<Source>(
    builder: RocksDbCanonicalBuilder,
    source: &Source,
    config: &CanonicalConstructionConfig,
) -> Result<CanonicalSourceLoadOutcome, CanonicalConstructionError>
where
    Source: NodeSource + Clone,
{
    let cancel = CancellationToken::new();
    Box::pin(load_fresh_canonical_until_cancelled(
        builder,
        source,
        config,
        FreshCanonicalLoadOptions {
            cancel: &cancel,
            progress: None,
            known_source_tip_height: None,
        },
    ))
    .await
}

/// Loads every source family until `options.cancel` fires.
///
/// Cancellation during the block stream stops the blocking loader at its next
/// block boundary. Once every block is queued, the loader finalizes SST files,
/// which cannot be interrupted. Either way the builder is closed on return.
pub(crate) async fn load_fresh_canonical_until_cancelled<Source>(
    builder: RocksDbCanonicalBuilder,
    source: &Source,
    config: &CanonicalConstructionConfig,
    options: FreshCanonicalLoadOptions<'_>,
) -> Result<CanonicalSourceLoadOutcome, CanonicalConstructionError>
where
    Source: NodeSource + Clone,
{
    let cancel = options.cancel;
    let block_outcome =
        load_fresh_canonical_blocks_until_cancelled(builder, source, config, options).await?;
    cancel
        .run_until_cancelled(load_fresh_canonical_source_families(
            block_outcome,
            source,
            config,
        ))
        .await
        .unwrap_or_else(|| {
            record_construction_cancelled("source_families");
            Err(CanonicalConstructionError::Cancelled)
        })
}

/// Logs the construction stage that shutdown interrupted.
pub(crate) fn record_construction_cancelled(stage: &'static str) {
    tracing::info!(
        target: "zinder::ingest",
        event = "canonical_construction_cancelled",
        stage,
        "stopped canonical construction for shutdown; the next start rebuilds unpublished staging or installs a staged READY store"
    );
}

pub(crate) fn register_prohibited_read_metrics() {
    metrics::counter!("zinder_ingest_canonical_historical_prevout_reads_total").absolute(0);
    metrics::counter!("zinder_ingest_canonical_cross_block_wallet_reads_total").absolute(0);
}

fn validate_construction_identity(
    build_plan: &zinder_store::CanonicalStoreBuildPlan,
    config: &CanonicalConstructionConfig,
) -> Result<(), CanonicalConstructionError> {
    validate_network_upgrade_activations(build_plan, &config.network_upgrade_activations)
}

fn validate_network_upgrade_activations(
    build_plan: &zinder_store::CanonicalStoreBuildPlan,
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<(), CanonicalConstructionError> {
    let store_network = build_plan.network();
    let configured_network = network_upgrade_activations.network();
    if configured_network != store_network {
        return Err(CanonicalConstructionError::NetworkMismatch {
            store_network,
            configured_network,
        });
    }
    let store_fingerprint = build_plan.network_upgrade_activations_fingerprint();
    let configured_fingerprint =
        network_upgrade_activations.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1);
    if configured_fingerprint != store_fingerprint {
        return Err(
            CanonicalConstructionError::NetworkUpgradeActivationsMismatch {
                store_fingerprint,
                configured_fingerprint,
            },
        );
    }
    Ok(())
}

struct AdmittedSourceBlock {
    source_block: SourceBlock,
    memory_permit: OwnedSemaphorePermit,
}

struct SourceBlockAdmissionState<'source> {
    source_chunks: BoxStream<'source, Result<SourceBlockChunk, IngestError>>,
    pending_chunk: Option<SourceBlockChunk>,
    prepare_memory: Arc<Semaphore>,
    prepare_memory_permits: u32,
    expected_network: Network,
}

fn admit_source_blocks(
    source_chunks: BoxStream<'_, Result<SourceBlockChunk, IngestError>>,
    prepare_memory: Arc<Semaphore>,
    prepare_memory_permits: u32,
    expected_network: Network,
) -> impl Stream<Item = Result<AdmittedSourceBlock, CanonicalConstructionError>> + Send + '_ {
    let state = SourceBlockAdmissionState {
        source_chunks,
        pending_chunk: None,
        prepare_memory,
        prepare_memory_permits,
        expected_network,
    };
    stream::unfold(state, |mut state| async move {
        loop {
            if let Some(source_block) = state
                .pending_chunk
                .as_ref()
                .and_then(SourceBlockChunk::front)
            {
                if source_block.network != state.expected_network {
                    let error = CanonicalConstructionError::SourceBlockNetworkMismatch {
                        height: source_block.height,
                        store_network: state.expected_network,
                        source_network: source_block.network,
                    };
                    state.pending_chunk = None;
                    return Some((Err(error), state));
                }
                let permits = memory_permits_for_block(
                    source_block.raw_block_bytes.capacity(),
                    state.prepare_memory_permits,
                );
                let Ok(memory_permit) = Arc::clone(&state.prepare_memory)
                    .acquire_many_owned(permits)
                    .await
                else {
                    let error = CanonicalConstructionError::Source {
                        source: IngestError::BlockingTaskFailed {
                            reason: "canonical preparation memory gate closed".to_owned(),
                        },
                    };
                    return Some((Err(error), state));
                };
                let Some(source_block) = state
                    .pending_chunk
                    .as_mut()
                    .and_then(SourceBlockChunk::pop_front)
                else {
                    continue;
                };
                if state
                    .pending_chunk
                    .as_ref()
                    .is_some_and(SourceBlockChunk::is_empty)
                {
                    state.pending_chunk = None;
                }
                return Some((
                    Ok(AdmittedSourceBlock {
                        source_block,
                        memory_permit,
                    }),
                    state,
                ));
            }

            match state.source_chunks.next().await {
                Some(Ok(source_chunk)) => state.pending_chunk = Some(source_chunk),
                Some(Err(source)) => {
                    return Some((Err(CanonicalConstructionError::Source { source }), state));
                }
                None => return None,
            }
        }
    })
}

fn canonical_source_fetch_config(
    build_plan: &zinder_store::CanonicalStoreBuildPlan,
    config: &CanonicalConstructionConfig,
    known_source_tip_height: Option<BlockHeight>,
) -> CanonicalSourceFetchConfig {
    let first_height = build_plan.history_bounds().first_available_height();
    let source_segment_sizer = Arc::new(Mutex::new(SourceSegmentSizer::new(
        config.pipeline_limits.source_segment_max_blocks,
        config.pipeline_limits.source_segment_target_response_bytes,
        Arc::clone(&config.network_upgrade_activations),
        first_height,
    )));
    let known_source_tip = known_source_tip_height.map(|tip_height| {
        SourceKnownTip::new(tip_height, build_plan.reorg_policy().reorg_window_blocks())
    });
    CanonicalSourceFetchConfig {
        request_timeout: config.request_timeout,
        history_predecessor: Some(build_plan.history_predecessor().block_id),
        from_height: first_height,
        to_height: build_plan.build_tip().height,
        max_response_bytes: config.pipeline_limits.max_response_bytes,
        target_response_payload_bytes: config.pipeline_limits.source_segment_target_response_bytes,
        source_fetch_max_in_flight_requests: config
            .pipeline_limits
            .source_fetch_max_in_flight_requests,
        source_fetch_max_in_flight_bytes: config.pipeline_limits.source_fetch_max_in_flight_bytes,
        source_segment_sizer,
        known_source_tip,
    }
}

fn build_prepared_block_stream<'source, Source>(
    source: &'source Source,
    build_plan: &zinder_store::CanonicalStoreBuildPlan,
    config: &CanonicalConstructionConfig,
    raw_blob_retention: RawBlobRetention,
    known_source_tip_height: Option<BlockHeight>,
) -> impl Stream<Item = Result<QueuedPreparedBlock, CanonicalConstructionError>>
+ Send
+ use<'source, Source>
where
    Source: NodeSource + Clone,
{
    let source_chunks = build_source_block_stream(
        source,
        canonical_source_fetch_config(build_plan, config, known_source_tip_height),
    )
    .boxed();

    let prepare_memory_permits = memory_permit_count(
        config
            .pipeline_limits
            .block_prepare_memory_watermark_bytes
            .get(),
    );
    let prepare_memory = Arc::new(Semaphore::new(
        usize::try_from(prepare_memory_permits).unwrap_or(usize::MAX),
    ));
    let prepare_concurrency =
        usize::try_from(config.pipeline_limits.block_prepare_concurrency.get())
            .unwrap_or(usize::MAX)
            .max(1);
    let activations = Arc::clone(&config.network_upgrade_activations);
    let admitted_blocks = admit_source_blocks(
        source_chunks,
        prepare_memory,
        prepare_memory_permits,
        build_plan.network(),
    );
    UnorderedPreparedBlockStream::new(admitted_blocks, prepare_concurrency, move |admitted| {
        prepare_admitted_source_block(admitted, Arc::clone(&activations), raw_blob_retention)
    })
}

const BLOCK_PREPARE_STAGE_LABEL: &str = "block_prepare";

async fn prepare_admitted_source_block(
    admitted: AdmittedSourceBlock,
    activations: Arc<NetworkUpgradeActivations>,
    raw_blob_retention: RawBlobRetention,
) -> Result<QueuedPreparedBlock, CanonicalConstructionError> {
    let AdmittedSourceBlock {
        source_block,
        memory_permit,
    } = admitted;
    let prepared = tokio::task::spawn_blocking(move || {
        prepare_canonical_block_for_retention(&source_block, &activations, raw_blob_retention)
            .map_err(IngestError::from)
    })
    .await
    .map_err(|join_error| CanonicalConstructionError::Source {
        source: IngestError::BlockingTaskFailed {
            reason: join_error.to_string(),
        },
    })?
    .map_err(|source| CanonicalConstructionError::Source { source })?;
    Ok(QueuedPreparedBlock {
        prepared,
        memory_permit,
    })
}

type PreparedBlockFuture = BoxFuture<
    'static,
    (
        u64,
        u64,
        Result<QueuedPreparedBlock, CanonicalConstructionError>,
    ),
>;

struct BufferedPreparedBlock {
    outcome: Result<QueuedPreparedBlock, CanonicalConstructionError>,
    retained_bytes: u64,
}

struct UnorderedPreparedBlockStream<Admitted, Preparer> {
    admitted_blocks: Pin<Box<Admitted>>,
    prepare_concurrency: usize,
    preparer: Preparer,
    in_flight: FuturesUnordered<PreparedBlockFuture>,
    reorder_buffer: BTreeMap<u64, BufferedPreparedBlock>,
    reorder_buffer_bytes: u64,
    next_admit_seq: u64,
    next_emit_seq: u64,
    upstream_done: bool,
    earliest_stop_seq: Option<u64>,
    head_of_line_wait_started_at: Option<Instant>,
    terminated: bool,
}

impl<Admitted, Preparer, Fut> UnorderedPreparedBlockStream<Admitted, Preparer>
where
    Admitted: Stream<Item = Result<AdmittedSourceBlock, CanonicalConstructionError>>,
    Preparer: FnMut(AdmittedSourceBlock) -> Fut,
    Fut: std::future::Future<Output = Result<QueuedPreparedBlock, CanonicalConstructionError>>
        + Send
        + 'static,
{
    fn new(admitted_blocks: Admitted, prepare_concurrency: usize, preparer: Preparer) -> Self {
        Self {
            admitted_blocks: Box::pin(admitted_blocks),
            prepare_concurrency: prepare_concurrency.max(1),
            preparer,
            in_flight: FuturesUnordered::new(),
            reorder_buffer: BTreeMap::new(),
            reorder_buffer_bytes: 0,
            next_admit_seq: 0,
            next_emit_seq: 0,
            upstream_done: false,
            earliest_stop_seq: None,
            head_of_line_wait_started_at: None,
            terminated: false,
        }
    }

    fn drain_ready_in_flight(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while let Poll::Ready(Some((seq, retained_bytes, outcome))) =
            self.in_flight.poll_next_unpin(cx)
        {
            progressed = true;
            if self.earliest_stop_seq.is_some_and(|stop| seq > stop) {
                continue;
            }
            if outcome.is_err() {
                self.record_stop_seq(seq);
            }
            let retained_bytes = if outcome.is_ok() { retained_bytes } else { 0 };
            self.insert_buffered_outcome(seq, retained_bytes, outcome);
        }
        progressed
    }

    fn fill_in_flight(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while !self.upstream_done && self.in_flight.len() < self.prepare_concurrency {
            match self.admitted_blocks.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(admitted))) => {
                    progressed = true;
                    let seq = self.next_admit_seq;
                    self.next_admit_seq = self.next_admit_seq.saturating_add(1);
                    let retained_bytes =
                        u64::try_from(admitted.source_block.raw_block_bytes.capacity())
                            .unwrap_or(u64::MAX);
                    let prepare_future = (self.preparer)(admitted);
                    self.in_flight.push(Box::pin(async move {
                        let outcome = prepare_future.await;
                        (seq, retained_bytes, outcome)
                    }));
                }
                Poll::Ready(Some(Err(error))) => {
                    progressed = true;
                    let seq = self.next_admit_seq;
                    self.next_admit_seq = self.next_admit_seq.saturating_add(1);
                    self.record_stop_seq(seq);
                    self.insert_buffered_outcome(seq, 0, Err(error));
                    break;
                }
                Poll::Ready(None) => {
                    progressed = true;
                    self.upstream_done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }
        progressed
    }

    fn record_stop_seq(&mut self, seq: u64) {
        self.upstream_done = true;
        let next_stop = self
            .earliest_stop_seq
            .map_or(seq, |current| current.min(seq));
        self.earliest_stop_seq = Some(next_stop);
        let discarded = self.reorder_buffer.split_off(&next_stop.saturating_add(1));
        for entry in discarded.into_values() {
            self.reorder_buffer_bytes = self
                .reorder_buffer_bytes
                .saturating_sub(entry.retained_bytes);
        }
    }

    fn insert_buffered_outcome(
        &mut self,
        seq: u64,
        retained_bytes: u64,
        outcome: Result<QueuedPreparedBlock, CanonicalConstructionError>,
    ) {
        self.reorder_buffer_bytes = self.reorder_buffer_bytes.saturating_add(retained_bytes);
        self.reorder_buffer.insert(
            seq,
            BufferedPreparedBlock {
                outcome,
                retained_bytes,
            },
        );
    }

    fn take_next_ordered_outcome(
        &mut self,
    ) -> Option<Result<QueuedPreparedBlock, CanonicalConstructionError>> {
        let buffered = self.reorder_buffer.remove(&self.next_emit_seq)?;
        self.next_emit_seq = self.next_emit_seq.saturating_add(1);
        self.reorder_buffer_bytes = self
            .reorder_buffer_bytes
            .saturating_sub(buffered.retained_bytes);
        if let Some(started_at) = self.head_of_line_wait_started_at.take() {
            metrics::histogram!(
                "zinder_ingest_bulk_pipeline_head_of_line_wait_seconds",
                "stage" => BLOCK_PREPARE_STAGE_LABEL
            )
            .record(started_at.elapsed());
        }
        if buffered.outcome.is_err() {
            self.terminate_and_clear();
        }
        Some(buffered.outcome)
    }

    fn terminate_and_clear(&mut self) {
        self.terminated = true;
        self.upstream_done = true;
        self.reorder_buffer.clear();
        self.reorder_buffer_bytes = 0;
        self.in_flight = FuturesUnordered::new();
        self.head_of_line_wait_started_at = None;
    }

    fn record_metrics(&mut self) {
        if !self.reorder_buffer.is_empty() && !self.reorder_buffer.contains_key(&self.next_emit_seq)
        {
            self.head_of_line_wait_started_at
                .get_or_insert_with(Instant::now);
        }
        watermark::record_queue_depth(
            BLOCK_PREPARE_STAGE_LABEL,
            self.in_flight
                .len()
                .saturating_add(self.reorder_buffer.len()),
        );
        watermark::record_reorder_buffer(
            BLOCK_PREPARE_STAGE_LABEL,
            self.reorder_buffer.len(),
            self.reorder_buffer_bytes,
        );
    }
}

impl<Admitted, Preparer> Drop for UnorderedPreparedBlockStream<Admitted, Preparer> {
    fn drop(&mut self) {
        watermark::record_queue_depth(BLOCK_PREPARE_STAGE_LABEL, 0);
        watermark::record_reorder_buffer(BLOCK_PREPARE_STAGE_LABEL, 0, 0);
    }
}

impl<Admitted, Preparer, Fut> Stream for UnorderedPreparedBlockStream<Admitted, Preparer>
where
    Admitted: Stream<Item = Result<AdmittedSourceBlock, CanonicalConstructionError>>,
    Preparer: FnMut(AdmittedSourceBlock) -> Fut + Unpin,
    Fut: std::future::Future<Output = Result<QueuedPreparedBlock, CanonicalConstructionError>>
        + Send
        + 'static,
{
    type Item = Result<QueuedPreparedBlock, CanonicalConstructionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }
        loop {
            let mut progressed = self.drain_ready_in_flight(cx);
            if let Some(outcome) = self.take_next_ordered_outcome() {
                if !self.terminated && self.fill_in_flight(cx) {
                    let _ = self.drain_ready_in_flight(cx);
                }
                self.record_metrics();
                return Poll::Ready(Some(outcome));
            }
            if self.fill_in_flight(cx) {
                progressed = true;
            }
            if self.upstream_done && self.in_flight.is_empty() && self.reorder_buffer.is_empty() {
                self.terminated = true;
                self.record_metrics();
                return Poll::Ready(None);
            }
            if !progressed {
                self.record_metrics();
                return Poll::Pending;
            }
        }
    }
}

struct QueuedPreparedBlock {
    prepared: PreparedCanonicalBlock,
    memory_permit: OwnedSemaphorePermit,
}

struct BlockLoaderSettings {
    block_queue_capacity: usize,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    checkpoint_selection: Option<TreeStateCheckpointSelection>,
    tree_hashing_workers: NonZeroU32,
    cancel: CancellationToken,
    progress: ConstructionProgress,
}

struct BuildBlockReceiverConfig {
    runtime: tokio::runtime::Handle,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    checkpoint_selection: Option<TreeStateCheckpointSelection>,
    cancel: CancellationToken,
    loader_stop: CancellationToken,
    progress: ConstructionProgress,
}

async fn drive_block_loader<PreparedBlocks>(
    builder: RocksDbCanonicalBuilder,
    prepared_blocks: PreparedBlocks,
    settings: BlockLoaderSettings,
) -> Result<CanonicalBlockLoadOutcome, CanonicalConstructionError>
where
    PreparedBlocks: Stream<Item = Result<QueuedPreparedBlock, CanonicalConstructionError>> + Send,
{
    let BlockLoaderSettings {
        block_queue_capacity,
        network_upgrade_activations,
        checkpoint_selection,
        tree_hashing_workers,
        cancel,
        progress,
    } = settings;
    let tree_hashers = TreeHashers::start(tree_hashing_workers)?;
    let loader_stop = cancel.child_token();
    let (block_sender, block_receiver) = mpsc::channel(block_queue_capacity);
    let receiver_config = BuildBlockReceiverConfig {
        runtime: tokio::runtime::Handle::current(),
        network_upgrade_activations,
        checkpoint_selection,
        cancel: cancel.clone(),
        loader_stop: loader_stop.clone(),
        progress,
    };
    let loader_task = tokio::task::spawn_blocking(move || {
        run_block_loader(builder, block_receiver, tree_hashers, receiver_config)
    });

    let forwarded = loader_stop
        .run_until_cancelled(forward_prepared_blocks(prepared_blocks, block_sender))
        .await
        .is_some();
    // A cancelled loader stops at its next block boundary, but SST
    // finalization after the last block runs to completion. Waiting for the
    // task either way keeps `RocksDB` work from outliving the writer.
    let loaded = loader_task.await;
    if cancel.is_cancelled() {
        record_construction_cancelled(if forwarded {
            "block_finalize"
        } else {
            "block_stream"
        });
        return Err(CanonicalConstructionError::Cancelled);
    }
    loaded.map_err(|source| CanonicalConstructionError::LoaderTaskFailed {
        reason: source.to_string(),
    })?
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "build_blocks and pipelined move by value into load_blocks_into_builder and finish_and_join"
)]
fn run_block_loader(
    mut builder: RocksDbCanonicalBuilder,
    block_receiver: mpsc::Receiver<Result<QueuedPreparedBlock, CanonicalConstructionError>>,
    tree_hashers: Option<TreeHashers>,
    receiver_config: BuildBlockReceiverConfig,
) -> Result<CanonicalBlockLoadOutcome, CanonicalConstructionError> {
    let predecessor = builder.build_plan().history_predecessor();
    let predecessor_tip_metadata = predecessor.tip_metadata();
    let commitment_tree_accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        predecessor.block_id.height,
        &predecessor.frontiers,
        &receiver_config.network_upgrade_activations,
    )
    .map_err(|source| CanonicalConstructionError::CommitmentTreeState {
        height: predecessor.block_id.height,
        source,
    })?;
    let build_tip_height = builder.build_plan().build_tip().height;
    let build_blocks = CanonicalBuildBlockReceiver {
        receiver: block_receiver,
        runtime: receiver_config.runtime,
        ready: VecDeque::new(),
        terminated_after_error: false,
        active_memory_permit: None,
        running_tree_sizes: CommitmentTreeSizes::from_tip_metadata(predecessor_tip_metadata),
        commitment_tree_accumulator,
        derived_subtree_roots: Vec::new(),
        build_tip_height,
        checkpoint_selection: receiver_config.checkpoint_selection,
        checkpoint_tree_sizes: predecessor_tip_metadata,
        cancel: receiver_config.cancel,
        loader_stop: receiver_config.loader_stop,
        stage_timers: BlockLoaderStageTimers::register(),
        tree_hashers,
        in_flight_store_height: None,
        progress: None,
        #[cfg(test)]
        parallel_batch_sizes: Vec::new(),
    };
    let (evidence, mut derived_subtree_roots) = load_blocks_into_builder(
        &mut builder,
        &receiver_config.network_upgrade_activations,
        build_blocks,
        receiver_config.progress,
    )?;
    builder.record_resource_gauges();
    // Blocks complete different pools' subtrees in any order, while the
    // node's ranges and the store's keys list one pool after another.
    derived_subtree_roots.sort_by_key(|root| (root.protocol.id(), root.subtree_index.value()));
    Ok(CanonicalBlockLoadOutcome {
        builder,
        evidence,
        derived_subtree_roots,
    })
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "finish_and_join consumes pipelined and drops its active memory permit before returning"
)]
fn load_blocks_into_builder(
    builder: &mut RocksDbCanonicalBuilder,
    activations: &NetworkUpgradeActivations,
    mut build_blocks: CanonicalBuildBlockReceiver,
    progress: ConstructionProgress,
) -> Result<(CanonicalBlockLoadEvidence, Vec<CanonicalBuildSubtreeRoot>), CanonicalConstructionError>
{
    if build_blocks.tree_hashers.is_none() {
        build_blocks.progress = Some(progress);
        let load_outcome = builder.bulk_load_blocks(activations, &mut build_blocks);
        build_blocks.loader_stop.cancel();
        let evidence = load_outcome.map_err(canonical_build_error)?;
        if let Some(tracked_progress) = &mut build_blocks.progress {
            tracked_progress.finish();
        }
        let derived_subtree_roots = std::mem::take(&mut build_blocks.derived_subtree_roots);
        return Ok((evidence, derived_subtree_roots));
    }
    let cancel = build_blocks.cancel.clone();
    let loader_stop = build_blocks.loader_stop.clone();
    let mut pipelined = PipelinedBuildBlocks::spawn(
        build_blocks,
        progress,
        cancel,
        loader_stop,
        MAX_COMMITMENT_BATCH_BLOCKS,
    )?;
    let load_outcome = builder.bulk_load_blocks(activations, &mut pipelined);
    let (mut tracked_progress, derived_subtree_roots) =
        pipelined.finish_and_join(load_outcome.is_ok())?;
    let evidence = load_outcome.map_err(canonical_build_error)?;
    tracked_progress.finish();
    Ok((evidence, derived_subtree_roots))
}

async fn forward_prepared_blocks<PreparedBlocks>(
    prepared_blocks: PreparedBlocks,
    block_sender: mpsc::Sender<Result<QueuedPreparedBlock, CanonicalConstructionError>>,
) where
    PreparedBlocks: Stream<Item = Result<QueuedPreparedBlock, CanonicalConstructionError>> + Send,
{
    let mut prepared_blocks = std::pin::pin!(prepared_blocks);
    while let Some(prepared_block) = prepared_blocks.next().await {
        let source_failed = prepared_block.is_err();
        if block_sender.send(prepared_block).await.is_err() || source_failed {
            break;
        }
    }
}

struct CanonicalBuildBlockReceiver {
    receiver: mpsc::Receiver<Result<QueuedPreparedBlock, CanonicalConstructionError>>,
    runtime: tokio::runtime::Handle,
    ready: VecDeque<Result<GuardedBuildBlock, CanonicalConstructionError>>,
    terminated_after_error: bool,
    active_memory_permit: Option<OwnedSemaphorePermit>,
    running_tree_sizes: CommitmentTreeSizes,
    commitment_tree_accumulator: CommitmentTreeAccumulator,
    derived_subtree_roots: Vec<CanonicalBuildSubtreeRoot>,
    build_tip_height: BlockHeight,
    checkpoint_selection: Option<TreeStateCheckpointSelection>,
    checkpoint_tree_sizes: ChainTipMetadata,
    cancel: CancellationToken,
    loader_stop: CancellationToken,
    stage_timers: BlockLoaderStageTimers,
    /// Hashing threads, or `None` when the loader hashes on its own thread.
    tree_hashers: Option<TreeHashers>,
    in_flight_store_height: Option<BlockHeight>,
    progress: Option<ConstructionProgress>,
    #[cfg(test)]
    parallel_batch_sizes: Vec<usize>,
}

/// Stage durations of the canonical block loader, resolved once per construction.
///
/// `receive_wait` is time spent idle waiting for the next prepared block, so a
/// high share means source fetch or block preparation bounds construction.
/// Every stage named `tree_hashing_*` records commitment-tree hashing wall
/// time on the tree-staging thread (or on the loader thread when
/// `tree_hashing_workers == 1`): `tree_hashing_append` measures batch or
/// per-block tree appends and `tree_hashing_checkpoint` measures pool frontier
/// cross-checks.
struct BlockLoaderStageTimers {
    receive_wait: Histogram,
    commitment_extract: Histogram,
    tree_hashing_append: Histogram,
    tree_append_sapling: Histogram,
    tree_append_orchard: Histogram,
    tree_append_ironwood: Histogram,
    position: Histogram,
    tree_hashing_checkpoint: Histogram,
}

impl BlockLoaderStageTimers {
    fn register() -> Self {
        let stage = |stage: &'static str| metrics::histogram!(BLOCK_LOAD_STAGE_DURATION_METRIC, "stage" => stage);
        let pool_timer = |pool: &'static str| metrics::histogram!(TREE_APPEND_POOL_DURATION_METRIC, "pool" => pool);
        Self {
            receive_wait: stage("receive_wait"),
            commitment_extract: stage("commitment_extract"),
            tree_hashing_append: stage("tree_hashing_append"),
            tree_append_sapling: pool_timer("sapling"),
            tree_append_orchard: pool_timer("orchard"),
            tree_append_ironwood: pool_timer("ironwood"),
            position: stage("position"),
            tree_hashing_checkpoint: stage("tree_hashing_checkpoint"),
        }
    }

    fn record_pool_append_durations(&self, durations: BatchPoolAppendDurations) {
        if let Some(sapling) = durations.sapling {
            self.tree_append_sapling.record(sapling);
        }
        if let Some(orchard) = durations.orchard {
            self.tree_append_orchard.record(orchard);
        }
        if let Some(ironwood) = durations.ironwood {
            self.tree_append_ironwood.record(ironwood);
        }
    }
}

struct PreparedBatchEntry {
    queued: QueuedPreparedBlock,
    commitments: CompactBlockCommitments,
}

struct PositionedBatchEntry {
    positioned: PositionedCanonicalBlock,
    memory_permit: OwnedSemaphorePermit,
    snapshot: CommitmentTreeSnapshot,
    checkpoint_required: bool,
}

struct BatchCheckpointOutcome {
    valid_prefix_len: usize,
    checkpoints: Vec<Option<CommitmentTreeCheckpoint>>,
    checkpoint_error: Option<CanonicalConstructionError>,
}

impl Iterator for CanonicalBuildBlockReceiver {
    type Item = Result<CanonicalBuildBlock, CanonicalConstructionError>;

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the block's memory permit moves into active_memory_permit and stays held through the store write"
    )]
    fn next(&mut self) -> Option<Self::Item> {
        self.active_memory_permit = None;
        self.record_staged_block();
        let next_guarded = self.next_guarded()?;
        match next_guarded {
            Ok(guarded) => {
                self.active_memory_permit = Some(guarded.memory_permit);
                self.in_flight_store_height = Some(guarded.block.facts.block_header.height);
                Some(Ok(guarded.block))
            }
            Err(error) => Some(Err(error)),
        }
    }
}

/// One attempt to take a prepared block from the source pipeline.
enum Received {
    Block(Box<QueuedPreparedBlock>),
    Failed(CanonicalConstructionError),
    /// No prepared block is waiting yet.
    Empty,
    /// The pipeline finished, failed earlier, or was cancelled.
    Closed,
}

impl CanonicalBuildBlockReceiver {
    /// Advances construction progress for the block just staged into the store.
    fn record_staged_block(&mut self) {
        if let Some(height) = self.in_flight_store_height.take()
            && !self.cancel.is_cancelled()
            && let Some(progress) = &mut self.progress
        {
            progress.record_loaded(height);
        }
    }

    /// Takes the next prepared block, waiting for one only when `wait` is
    /// set.
    fn receive(&mut self, wait: bool) -> Received {
        let received = if wait {
            let wait_started_at = Instant::now();
            let received = match self.receiver.try_recv() {
                Ok(received) => Some(received),
                Err(TryRecvError::Disconnected) => None,
                Err(TryRecvError::Empty) => {
                    if !self.cancel.is_cancelled()
                        && let Some(progress) = &mut self.progress
                    {
                        progress.flush_pending();
                    }
                    let loader_stop = &self.loader_stop;
                    let receiver = &mut self.receiver;
                    self.runtime.block_on(async {
                        tokio::select! {
                            biased;
                            () = loader_stop.cancelled() => None,
                            next_msg = receiver.recv() => next_msg,
                        }
                    })
                }
            };
            if matches!(received, Some(Ok(_))) {
                self.stage_timers
                    .receive_wait
                    .record(wait_started_at.elapsed());
            }
            received
        } else {
            match self.receiver.try_recv() {
                Ok(received) => Some(received),
                Err(TryRecvError::Empty) => return Received::Empty,
                Err(TryRecvError::Disconnected) => None,
            }
        };
        // A cancelled producer drops its sender, so a closed channel must not
        // read as a complete stream and start SST finalization.
        match received.filter(|_| !self.cancel.is_cancelled() && !self.loader_stop.is_cancelled()) {
            Some(Ok(queued_block)) => Received::Block(Box::new(queued_block)),
            Some(Err(source)) => Received::Failed(source),
            None => Received::Closed,
        }
    }

    fn pop_ready(&mut self) -> Option<Result<GuardedBuildBlock, CanonicalConstructionError>> {
        let ready_entry = self.ready.pop_front()?;
        if ready_entry.is_err() {
            self.terminated_after_error = true;
            self.ready.clear();
        }
        Some(ready_entry)
    }

    /// Produces the next ordered canonical build block while retaining its
    /// preparation memory permit.
    fn next_guarded(&mut self) -> Option<Result<GuardedBuildBlock, CanonicalConstructionError>> {
        if self.cancel.is_cancelled() {
            return Some(Err(CanonicalConstructionError::Cancelled));
        }
        if self.loader_stop.is_cancelled() {
            return None;
        }
        if self.tree_hashers.is_none() {
            return match self.receive(true) {
                Received::Block(queued_block) => {
                    let QueuedPreparedBlock {
                        prepared,
                        memory_permit,
                    } = *queued_block;
                    Some(self.build_block(prepared).map(|block| GuardedBuildBlock {
                        block,
                        memory_permit,
                    }))
                }
                Received::Failed(source) => Some(Err(source)),
                Received::Empty | Received::Closed => self
                    .cancel
                    .is_cancelled()
                    .then_some(Err(CanonicalConstructionError::Cancelled)),
            };
        }
        if let Some(ready_entry) = self.pop_ready() {
            return Some(ready_entry);
        }
        if self.terminated_after_error {
            return None;
        }
        self.fill_ready_batch();
        if let Some(ready_entry) = self.pop_ready() {
            return Some(ready_entry);
        }
        self.cancel
            .is_cancelled()
            .then_some(Err(CanonicalConstructionError::Cancelled))
    }

    fn fill_ready_batch(&mut self) {
        let (batch, trailing_error) = self.drain_prepared_batch();
        let batch_succeeded = if batch.is_empty() {
            true
        } else {
            self.stage_prepared_batch(batch)
        };
        if batch_succeeded && let Some(error) = trailing_error {
            self.ready.push_back(Err(error));
        }
    }

    fn drain_prepared_batch(
        &mut self,
    ) -> (Vec<PreparedBatchEntry>, Option<CanonicalConstructionError>) {
        let mut batch = Vec::new();
        let mut total_commitments = 0_usize;
        let mut trailing_error = None;

        while batch.len() < MAX_COMMITMENT_BATCH_BLOCKS {
            // Block only when `batch` is empty so a tight memory watermark
            // never waits for permits still held by earlier blocks in `batch`.
            match self.receive(batch.is_empty()) {
                Received::Block(queued_block) => {
                    let started_at = Instant::now();
                    let commitments = match compact_block_commitments(&queued_block.prepared) {
                        Ok(commitments) => commitments,
                        Err(error) => {
                            trailing_error = Some(error);
                            break;
                        }
                    };
                    self.stage_timers
                        .commitment_extract
                        .record(started_at.elapsed());
                    total_commitments = total_commitments.saturating_add(commitments.total_len());
                    batch.push(PreparedBatchEntry {
                        queued: *queued_block,
                        commitments,
                    });
                    if total_commitments >= MAX_COMMITMENT_BATCH_COMMITMENTS {
                        break;
                    }
                }
                Received::Failed(error) => {
                    trailing_error = Some(error);
                    break;
                }
                Received::Empty | Received::Closed => break,
            }
        }
        (batch, trailing_error)
    }

    fn stage_prepared_batch(&mut self, batch: Vec<PreparedBatchEntry>) -> bool {
        if batch.len() < 2 || self.tree_hashers.is_none() {
            return self.stage_batch_sequentially(batch);
        }
        let batch_len = batch.len();
        let total_commitments: usize = batch
            .iter()
            .map(|entry| entry.commitments.total_len())
            .sum();
        let append_started_at = Instant::now();
        let batch_outcome = {
            let inputs: Vec<BlockCommitmentBatchInput<'_>> = batch
                .iter()
                .map(|entry| BlockCommitmentBatchInput {
                    block_height: entry.queued.prepared.facts.block_header.height,
                    sapling_cmus: &entry.commitments.sapling,
                    orchard_cmxs: &entry.commitments.orchard,
                    ironwood_cmxs: &entry.commitments.ironwood,
                })
                .collect();
            let accumulator = &mut self.commitment_tree_accumulator;
            match &self.tree_hashers {
                Some(tree_hashers) if total_commitments > 0 => {
                    tree_hashers.install(|| accumulator.append_block_batch_in_parallel(&inputs))
                }
                _ => accumulator.append_block_batch_in_parallel(&inputs),
            }
        };
        let Ok(outcome) = batch_outcome else {
            // Fall back block by block so valid prefix blocks still reach the
            // store before the first failing block ends construction.
            return self.stage_batch_sequentially(batch);
        };
        #[cfg(test)]
        self.parallel_batch_sizes.push(batch_len);
        let _ = batch_len;
        self.stage_timers
            .tree_hashing_append
            .record(append_started_at.elapsed());
        self.stage_timers
            .record_pool_append_durations(outcome.pool_durations);
        self.commit_staged_batch(batch, outcome.blocks)
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "built moves directly into self.ready while retaining its preparation memory permit"
    )]
    fn stage_batch_sequentially(&mut self, batch: Vec<PreparedBatchEntry>) -> bool {
        for entry in batch {
            let PreparedBatchEntry {
                queued,
                commitments,
            } = entry;
            let built = self.stage_single_entry(queued, &commitments);
            let failed = built.is_err();
            self.ready.push_back(built);
            if failed {
                return false;
            }
        }
        true
    }

    fn stage_single_entry(
        &mut self,
        queued: QueuedPreparedBlock,
        commitments: &CompactBlockCommitments,
    ) -> Result<GuardedBuildBlock, CanonicalConstructionError> {
        let QueuedPreparedBlock {
            prepared,
            memory_permit,
        } = queued;
        let height = prepared.facts.block_header.height;
        let block_hash = prepared.facts.block_header.block_hash;
        let block_time_seconds = prepared.partial_compact_block.time();
        self.append_commitments_to_accumulator(height, commitments)?;
        let positioned = self.position_block(prepared)?;
        let positioned_at = Instant::now();
        let checkpoint_required = self.select_checkpoint(height, positioned.tip_metadata);
        let tree_state_checkpoint = checkpoint_required
            .then(|| {
                self.commitment_tree_accumulator
                    .cross_checked_frontiers()
                    .map(|frontiers| {
                        CommitmentTreeCheckpoint::new(
                            BlockId::new(height, block_hash),
                            block_time_seconds,
                            frontiers,
                        )
                    })
            })
            .transpose()
            .map_err(|source| CanonicalConstructionError::CommitmentTreeState { height, source })?;
        if checkpoint_required {
            self.stage_timers
                .tree_hashing_checkpoint
                .record(positioned_at.elapsed());
        }
        Ok(GuardedBuildBlock {
            block: canonical_build_block(positioned, tree_state_checkpoint),
            memory_permit,
        })
    }

    fn commit_staged_batch(
        &mut self,
        batch: Vec<PreparedBatchEntry>,
        appended_blocks: Vec<BatchAppendedBlock>,
    ) -> bool {
        let (positioned_entries, position_error) =
            self.position_staged_batch(batch, appended_blocks);
        let BatchCheckpointOutcome {
            valid_prefix_len,
            mut checkpoints,
            checkpoint_error,
        } = self.derive_batch_checkpoints(&positioned_entries);
        for (entry, checkpoint) in positioned_entries
            .into_iter()
            .take(valid_prefix_len)
            .zip(checkpoints.drain(..valid_prefix_len))
        {
            self.ready.push_back(Ok(GuardedBuildBlock {
                block: canonical_build_block(entry.positioned, checkpoint),
                memory_permit: entry.memory_permit,
            }));
        }
        if let Some(error) = checkpoint_error {
            self.ready.push_back(Err(error));
            return false;
        }
        if let Some(error) = position_error {
            self.ready.push_back(Err(error));
            return false;
        }
        true
    }

    fn position_staged_batch(
        &mut self,
        batch: Vec<PreparedBatchEntry>,
        appended_blocks: Vec<BatchAppendedBlock>,
    ) -> (
        Vec<PositionedBatchEntry>,
        Option<CanonicalConstructionError>,
    ) {
        let mut positioned_entries = Vec::with_capacity(batch.len());
        let mut position_error = None;
        for (entry, appended) in batch.into_iter().zip(appended_blocks) {
            self.derived_subtree_roots.extend(
                appended
                    .completed_subtree_roots
                    .into_iter()
                    .map(CanonicalBuildSubtreeRoot::from),
            );
            let accumulated = appended.snapshot.tip_metadata();
            match self.position_block_against(entry.queued.prepared, accumulated) {
                Ok(positioned) => {
                    let height = positioned.facts.block_header.height;
                    let checkpoint_required =
                        self.select_checkpoint(height, positioned.tip_metadata);
                    positioned_entries.push(PositionedBatchEntry {
                        positioned,
                        memory_permit: entry.queued.memory_permit,
                        snapshot: appended.snapshot,
                        checkpoint_required,
                    });
                }
                Err(error) => {
                    position_error = Some(error);
                    break;
                }
            }
        }
        (positioned_entries, position_error)
    }

    fn derive_batch_checkpoints(&self, entries: &[PositionedBatchEntry]) -> BatchCheckpointOutcome {
        let mut checkpoint_indices = Vec::new();
        let mut snapshot_refs = Vec::new();
        for (idx, entry) in entries.iter().enumerate() {
            if entry.checkpoint_required {
                checkpoint_indices.push(idx);
                snapshot_refs.push(&entry.snapshot);
            }
        }
        if snapshot_refs.is_empty() {
            return BatchCheckpointOutcome {
                valid_prefix_len: entries.len(),
                checkpoints: vec![None; entries.len()],
                checkpoint_error: None,
            };
        }
        let started_at = Instant::now();
        let batch_frontiers = self.tree_hashers.as_ref().map_or_else(
            || CommitmentTreeSnapshot::cross_checked_frontiers_batch(&snapshot_refs),
            |tree_hashers| {
                tree_hashers.install(|| {
                    CommitmentTreeSnapshot::cross_checked_frontiers_batch(&snapshot_refs)
                })
            },
        );
        self.stage_timers
            .tree_hashing_checkpoint
            .record(started_at.elapsed());
        match batch_frontiers {
            Ok(frontiers_list) => {
                let mut checkpoints = vec![None; entries.len()];
                for (entry_idx, frontiers) in checkpoint_indices.into_iter().zip(frontiers_list) {
                    if let Some(entry) = entries.get(entry_idx) {
                        let header = &entry.positioned.facts.block_header;
                        let block_id = BlockId::new(header.height, header.block_hash);
                        checkpoints[entry_idx] = Some(CommitmentTreeCheckpoint::new(
                            block_id,
                            entry.positioned.compact_block.time(),
                            frontiers,
                        ));
                    }
                }
                BatchCheckpointOutcome {
                    valid_prefix_len: entries.len(),
                    checkpoints,
                    checkpoint_error: None,
                }
            }
            Err((failed_slot, source)) => {
                Self::recover_prefix_checkpoints(entries, &checkpoint_indices, failed_slot, source)
            }
        }
    }

    fn recover_prefix_checkpoints(
        entries: &[PositionedBatchEntry],
        checkpoint_indices: &[usize],
        failed_slot: usize,
        source: CommitmentTreeAccumulatorError,
    ) -> BatchCheckpointOutcome {
        let failed_entry_idx = checkpoint_indices
            .get(failed_slot)
            .copied()
            .unwrap_or(entries.len());
        let mut checkpoints = vec![None; failed_entry_idx];
        for &entry_idx in checkpoint_indices.iter().take(failed_slot) {
            let Some(entry) = entries.get(entry_idx) else {
                break;
            };
            let header = &entry.positioned.facts.block_header;
            match entry.snapshot.cross_checked_frontiers() {
                Ok(frontiers) => {
                    checkpoints[entry_idx] = Some(CommitmentTreeCheckpoint::new(
                        BlockId::new(header.height, header.block_hash),
                        entry.positioned.compact_block.time(),
                        frontiers,
                    ));
                }
                Err(earlier_source) => {
                    return BatchCheckpointOutcome {
                        valid_prefix_len: entry_idx,
                        checkpoints,
                        checkpoint_error: Some(CanonicalConstructionError::CommitmentTreeState {
                            height: header.height,
                            source: earlier_source,
                        }),
                    };
                }
            }
        }
        let failed_height = entries
            .get(failed_entry_idx)
            .map_or(BlockHeight::new(0), |entry| {
                entry.positioned.facts.block_header.height
            });
        BatchCheckpointOutcome {
            valid_prefix_len: failed_entry_idx,
            checkpoints,
            checkpoint_error: Some(CanonicalConstructionError::CommitmentTreeState {
                height: failed_height,
                source,
            }),
        }
    }

    /// Positions an appended block against the accumulator's current tip sizes.
    fn position_block(
        &mut self,
        prepared: PreparedCanonicalBlock,
    ) -> Result<PositionedCanonicalBlock, CanonicalConstructionError> {
        let accumulated = self.commitment_tree_accumulator.tip_metadata();
        self.position_block_against(prepared, accumulated)
    }

    /// Positions an appended block and requires its tree sizes to match `accumulated`.
    fn position_block_against(
        &mut self,
        prepared: PreparedCanonicalBlock,
        accumulated: ChainTipMetadata,
    ) -> Result<PositionedCanonicalBlock, CanonicalConstructionError> {
        let height = prepared.facts.block_header.height;
        let tree_appended_at = Instant::now();
        let positioned =
            position_canonical_block(prepared, &mut self.running_tree_sizes).map_err(|source| {
                CanonicalConstructionError::Source {
                    source: IngestError::from(source),
                }
            })?;
        if positioned.tip_metadata != accumulated {
            return Err(CanonicalConstructionError::CommitmentTreePositionMismatch {
                height,
                positioned: positioned.tip_metadata,
                accumulated,
            });
        }
        self.stage_timers
            .position
            .record(tree_appended_at.elapsed());
        Ok(positioned)
    }

    /// Decides, in height order, whether the block at `height` with
    /// `tree_sizes` after it stores a checkpoint, and resets the
    /// commitment-budget baseline at every stored checkpoint.
    ///
    /// The store stager rechecks this rule, so it must stay identical.
    fn select_checkpoint(&mut self, height: BlockHeight, tree_sizes: ChainTipMetadata) -> bool {
        let selected = height == self.build_tip_height
            || height.value().is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE)
            || self.checkpoint_selection.is_some_and(|selection| {
                selection.requires_checkpoint_since(self.checkpoint_tree_sizes, tree_sizes)
            });
        if selected {
            self.checkpoint_tree_sizes = tree_sizes;
        }
        selected
    }

    fn build_block(
        &mut self,
        prepared: PreparedCanonicalBlock,
    ) -> Result<CanonicalBuildBlock, CanonicalConstructionError> {
        let height = prepared.facts.block_header.height;
        let block_hash = prepared.facts.block_header.block_hash;
        let block_time_seconds = prepared.partial_compact_block.time();
        self.append_to_commitment_trees(&prepared)?;
        let positioned = self.position_block(prepared)?;
        let positioned_at = Instant::now();
        let checkpoint_required = self.select_checkpoint(height, positioned.tip_metadata);
        // Only the build-tip checkpoint is later compared with the node's tree
        // state, so each checkpoint cross-checks its encoding here.
        let tree_state_checkpoint = checkpoint_required
            .then(|| {
                self.commitment_tree_accumulator
                    .cross_checked_frontiers()
                    .map(|frontiers| {
                        CommitmentTreeCheckpoint::new(
                            BlockId::new(height, block_hash),
                            block_time_seconds,
                            frontiers,
                        )
                    })
            })
            .transpose()
            .map_err(|source| CanonicalConstructionError::CommitmentTreeState { height, source })?;
        if checkpoint_required {
            self.stage_timers
                .tree_hashing_checkpoint
                .record(positioned_at.elapsed());
        }
        Ok(canonical_build_block(positioned, tree_state_checkpoint))
    }

    /// Appends the block's note commitments to the trees and collects the
    /// subtree roots the block completes.
    fn append_to_commitment_trees(
        &mut self,
        prepared: &PreparedCanonicalBlock,
    ) -> Result<(), CanonicalConstructionError> {
        let height = prepared.facts.block_header.height;
        let started_at = Instant::now();
        let commitments = compact_block_commitments(prepared)?;
        self.stage_timers
            .commitment_extract
            .record(started_at.elapsed());
        self.append_commitments_to_accumulator(height, &commitments)
    }

    fn append_commitments_to_accumulator(
        &mut self,
        height: BlockHeight,
        commitments: &CompactBlockCommitments,
    ) -> Result<(), CanonicalConstructionError> {
        let started_at = Instant::now();
        let accumulator = &mut self.commitment_tree_accumulator;
        let parallel_append = should_parallelize_append(commitments);
        match &self.tree_hashers {
            Some(tree_hashers) if parallel_append => tree_hashers.install(|| {
                accumulator.append_block_commitments_in_parallel(
                    height,
                    &commitments.sapling,
                    &commitments.orchard,
                    &commitments.ironwood,
                )
            }),
            Some(_) | None => accumulator.append_block_commitments(
                height,
                &commitments.sapling,
                &commitments.orchard,
                &commitments.ironwood,
            ),
        }
        .map_err(|source| CanonicalConstructionError::CommitmentTreeState { height, source })?;
        self.derived_subtree_roots.extend(
            self.commitment_tree_accumulator
                .completed_subtree_roots()
                .iter()
                .copied()
                .map(CanonicalBuildSubtreeRoot::from),
        );
        self.stage_timers
            .tree_hashing_append
            .record(started_at.elapsed());
        Ok(())
    }
}

/// Minimum note commitments in one pool before block appends enter the parallel
/// append worker pool.
const PARALLEL_COMMITMENT_THRESHOLD: usize = 32;

fn should_parallelize_append(commitments: &CompactBlockCommitments) -> bool {
    let active_pools = usize::from(!commitments.sapling.is_empty())
        + usize::from(!commitments.orchard.is_empty())
        + usize::from(!commitments.ironwood.is_empty());
    active_pools >= 2
        || commitments.sapling.len() >= PARALLEL_COMMITMENT_THRESHOLD
        || commitments.orchard.len() >= PARALLEL_COMMITMENT_THRESHOLD
        || commitments.ironwood.len() >= PARALLEL_COMMITMENT_THRESHOLD
}

#[derive(Default)]
pub(crate) struct CompactBlockCommitments {
    pub(crate) sapling: Vec<[u8; 32]>,
    pub(crate) orchard: Vec<[u8; 32]>,
    pub(crate) ironwood: Vec<[u8; 32]>,
}

impl CompactBlockCommitments {
    fn total_len(&self) -> usize {
        self.sapling.len() + self.orchard.len() + self.ironwood.len()
    }
}

pub(crate) fn compact_block_commitments(
    prepared: &PreparedCanonicalBlock,
) -> Result<CompactBlockCommitments, CanonicalConstructionError> {
    let height = prepared.facts.block_header.height;
    let mut commitments = CompactBlockCommitments::default();
    for transaction in prepared.partial_compact_block.transactions() {
        for output in &transaction.data.sapling_outputs {
            commitments.sapling.push(compact_commitment_bytes(
                height,
                ShieldedProtocol::Sapling,
                &output.commitment,
            )?);
        }
        for action in &transaction.data.orchard_actions {
            commitments.orchard.push(compact_commitment_bytes(
                height,
                ShieldedProtocol::Orchard,
                &action.commitment,
            )?);
        }
        for action in &transaction.data.ironwood_actions {
            commitments.ironwood.push(compact_commitment_bytes(
                height,
                ShieldedProtocol::Ironwood,
                &action.commitment,
            )?);
        }
    }
    Ok(commitments)
}

fn compact_commitment_bytes(
    height: BlockHeight,
    protocol: ShieldedProtocol,
    bytes: &[u8],
) -> Result<[u8; 32], CanonicalConstructionError> {
    bytes
        .try_into()
        .map_err(|_| CanonicalConstructionError::CompactCommitmentWidth {
            height,
            protocol,
            byte_count: bytes.len(),
        })
}

pub(crate) fn canonical_build_block(
    positioned: PositionedCanonicalBlock,
    tree_state_checkpoint: Option<CommitmentTreeCheckpoint>,
) -> CanonicalBuildBlock {
    let PositionedCanonicalBlock {
        facts,
        replay_envelope,
        retained_raw_blobs,
        compact_block,
        tip_metadata,
    } = positioned;
    let RetainedRawBlobs {
        block_blob,
        transaction_blobs,
    } = retained_raw_blobs;
    CanonicalBuildBlock {
        facts,
        replay_envelope,
        compact_block,
        tip_metadata,
        tree_state_checkpoint,
        transaction_blobs,
        block_blob,
    }
}

/// Returns the first root at which the derived and node-reported subtree-root
/// lists differ, or `None` when they are equal.
pub(crate) fn first_differing_subtree_root(
    derived: &[CanonicalBuildSubtreeRoot],
    source: &[CanonicalBuildSubtreeRoot],
) -> Option<CanonicalBuildSubtreeRoot> {
    let common_len = derived
        .iter()
        .zip(source)
        .take_while(|(derived, source)| derived == source)
        .count();
    derived
        .get(common_len)
        .or_else(|| source.get(common_len))
        .copied()
}

fn canonical_build_error(
    source: CanonicalStoreBuildError<CanonicalConstructionError>,
) -> CanonicalConstructionError {
    match source {
        CanonicalStoreBuildError::Source { source } => source,
        CanonicalStoreBuildError::Store(source) => CanonicalConstructionError::Store(source),
    }
}

fn memory_permit_count(limit_bytes: u64) -> u32 {
    let units = limit_bytes
        .saturating_add(CANONICAL_PREPARE_MEMORY_UNIT_BYTES.saturating_sub(1))
        .saturating_div(CANONICAL_PREPARE_MEMORY_UNIT_BYTES)
        .max(1);
    u32::try_from(units).unwrap_or(u32::MAX)
}

fn memory_permits_for_block(raw_block_capacity: usize, available_permits: u32) -> u32 {
    let estimated_bytes = u64::try_from(raw_block_capacity)
        .unwrap_or(u64::MAX)
        .max(1)
        .saturating_mul(CANONICAL_PREPARE_RAW_BLOCK_MULTIPLIER)
        .saturating_add(CANONICAL_PREPARE_FIXED_BYTES);
    memory_permit_count(estimated_bytes).min(available_permits)
}

#[cfg(test)]
mod tests {
    use super::{CanonicalPipelineLimits, CanonicalPipelineLimitsError};
    use std::num::{NonZeroU32, NonZeroU64};

    const MIB: u64 = 1_024 * 1_024;

    #[test]
    fn checkpoint_hashing_uses_detected_cpus_up_to_eight_and_one_when_unknown() {
        let limits = CanonicalPipelineLimits::resolve(
            None,
            NonZeroU32::MIN,
            NonZeroU64::new(64 * MIB).unwrap_or(NonZeroU64::MIN),
        );
        assert_eq!(limits.tree_hashing_workers, NonZeroU32::MIN);
        for (detected, expected) in [(Some(2), 2), (Some(8), 8), (Some(64), 8), (None, 1)] {
            let detected = detected.and_then(NonZeroU32::new);
            assert_eq!(
                limits
                    .with_checkpoint_hashing(detected)
                    .tree_hashing_workers
                    .get(),
                expected,
                "detected {detected:?}"
            );
        }
    }

    #[test]
    fn pipeline_limits_resolve_from_runtime_resources() {
        let limits = CanonicalPipelineLimits::resolve(
            NonZeroU64::new(10 * 1_024 * MIB),
            NonZeroU32::new(10).unwrap_or(NonZeroU32::MIN),
            NonZeroU64::new(64 * MIB).unwrap_or(NonZeroU64::MIN),
        );

        assert_eq!(limits.max_response_bytes.get(), 64 * MIB);
        assert_eq!(limits.source_segment_target_response_bytes.get(), 32 * MIB);
        assert_eq!(limits.source_segment_max_blocks.get(), 64);
        assert_eq!(limits.source_fetch_max_in_flight_requests.get(), 12);
        assert_eq!(limits.source_fetch_max_in_flight_bytes.get(), 160 * MIB);
        assert_eq!(limits.block_prepare_concurrency.get(), 10);
        assert_eq!(limits.block_prepare_memory_watermark_bytes.get(), 160 * MIB);
    }

    #[test]
    fn pipeline_limits_reject_segments_above_the_node_batch_limit() {
        let mut limits = CanonicalPipelineLimits::resolve(
            None,
            NonZeroU32::MIN,
            NonZeroU64::new(64 * MIB).unwrap_or(NonZeroU64::MIN),
        );
        limits.source_segment_max_blocks = NonZeroU32::new(1_000).unwrap_or(NonZeroU32::MIN);
        assert_eq!(limits.validate(), Ok(limits));

        limits.source_segment_max_blocks = NonZeroU32::new(1_001).unwrap_or(NonZeroU32::MIN);
        assert_eq!(
            limits.validate(),
            Err(
                CanonicalPipelineLimitsError::SourceSegmentExceedsBatchLimit {
                    source_segment_max_blocks: limits.source_segment_max_blocks,
                    max_batch_entries: 1_000,
                }
            )
        );
    }

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn test_source_block_with_capacity(height: u32, capacity: usize) -> zinder_source::SourceBlock {
        let mut raw_bytes = Vec::with_capacity(capacity);
        raw_bytes.push(1);
        zinder_source::SourceBlock::new(
            zinder_source::SourceBlockHeader {
                network: zinder_core::Network::ZcashRegtest,
                height: zinder_core::BlockHeight::new(height),
                hash: zinder_core::BlockHash::from_bytes(
                    [u8::try_from(height).unwrap_or(u8::MAX); 32],
                ),
                parent_hash: zinder_core::BlockHash::from_bytes([0_u8; 32]),
                block_time_seconds: 1_700_000_000_u32.saturating_add(height),
            },
            raw_bytes,
        )
    }

    fn test_prepared_block_at_height(
        height: zinder_core::BlockHeight,
    ) -> Result<super::PreparedCanonicalBlock, Box<dyn std::error::Error>> {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/z3-regtest-block-1.json"))?;
        let raw_hex = fixture
            .get("raw_block_hex")
            .and_then(serde_json::Value::as_str)
            .ok_or("fixture missing raw_block_hex")?;
        let raw_bytes = hex::decode(raw_hex)?;
        let source_block = zinder_source::SourceBlock::from_raw_block_bytes(
            zinder_core::Network::ZcashRegtest,
            zinder_core::BlockHeight::new(1),
            raw_bytes,
        )?;
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let mut prepared = super::prepare_canonical_block_for_retention(
            &source_block,
            &activations,
            super::RawBlobRetention::None,
        )?;
        prepared.facts.block_header.height = height;
        Ok(prepared)
    }

    fn test_chunk_stream(
        heights: &[u32],
        raw_capacity: usize,
    ) -> futures_util::stream::BoxStream<
        'static,
        Result<super::source_fetch::SourceBlockChunk, super::IngestError>,
    > {
        use futures_util::StreamExt;

        let watermark = super::watermark::ByteWatermark::new(
            "test_source_fetch",
            NonZeroU64::new(1_024 * 1_024).unwrap_or(NonZeroU64::MIN),
        );
        let blocks = heights
            .iter()
            .map(|height| test_source_block_with_capacity(*height, raw_capacity))
            .collect::<Vec<_>>();
        let reservation = watermark.try_reserve(1);
        let chunk = reservation.map(|reservation| {
            super::source_fetch::SourceBlockChunk::from_test_blocks(blocks, reservation)
        });
        futures_util::stream::iter(chunk.into_iter().map(Ok)).boxed()
    }

    fn map_prepare_test_error(error: &dyn std::error::Error) -> super::CanonicalConstructionError {
        super::CanonicalConstructionError::Source {
            source: super::IngestError::BlockingTaskFailed {
                reason: error.to_string(),
            },
        }
    }

    async fn poll_stream_while_pending<S>(
        stream: &mut S,
        recorder: &metrics_exporter_prometheus::PrometheusRecorder,
        stop_when: impl Fn() -> bool + Send,
    ) where
        S: futures_util::Stream<
                Item = Result<super::QueuedPreparedBlock, super::CanonicalConstructionError>,
            > + Unpin
            + Send,
    {
        for _ in 0..32 {
            let is_pending = {
                let waker = futures_util::task::noop_waker();
                let mut cx = std::task::Context::from_waker(&waker);
                metrics::with_local_recorder(recorder, || {
                    std::pin::Pin::new(&mut *stream).poll_next(&mut cx)
                })
                .is_pending()
            };
            assert!(is_pending, "stream must not emit out of order");
            if stop_when() {
                break;
            }
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn unordered_prepared_block_stream_buffers_faster_blocks_behind_slow_head_and_emits_in_order()
    -> TestResult {
        use futures_util::StreamExt;
        use parking_lot::Mutex;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::sync::{Notify, Semaphore};

        let prepare_memory_permits = super::memory_permit_count(16 * MIB);
        let prepare_memory = Arc::new(Semaphore::new(usize::try_from(prepare_memory_permits)?));
        let admitted = super::admit_source_blocks(
            test_chunk_stream(&[1, 2, 3, 4], 64),
            Arc::clone(&prepare_memory),
            prepare_memory_permits,
            zinder_core::Network::ZcashRegtest,
        );
        let head_gate = Arc::new(Notify::new());
        let in_flight_now = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let finished_heights = Arc::new(Mutex::new(Vec::new()));

        let mut prepared_stream =
            super::UnorderedPreparedBlockStream::new(admitted, 2, |admitted_block| {
                let head_gate = Arc::clone(&head_gate);
                let in_flight_now = Arc::clone(&in_flight_now);
                let max_in_flight = Arc::clone(&max_in_flight);
                let finished_heights = Arc::clone(&finished_heights);
                async move {
                    let active = in_flight_now.fetch_add(1, Ordering::SeqCst) + 1;
                    max_in_flight.fetch_max(active, Ordering::SeqCst);
                    let height = admitted_block.source_block.height;
                    if height.value() == 1 {
                        head_gate.notified().await;
                    }
                    let prepared = test_prepared_block_at_height(height)
                        .map_err(|error| map_prepare_test_error(error.as_ref()))?;
                    finished_heights.lock().push(height.value());
                    in_flight_now.fetch_sub(1, Ordering::SeqCst);
                    Ok(super::QueuedPreparedBlock {
                        prepared,
                        memory_permit: admitted_block.memory_permit,
                    })
                }
            });

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        poll_stream_while_pending(&mut prepared_stream, &recorder, || {
            finished_heights.lock().len() == 3
        })
        .await;

        assert_eq!(*finished_heights.lock(), vec![2, 3, 4]);
        assert!(max_in_flight.load(Ordering::SeqCst) <= 2);
        let rendered = handle.render();
        assert!(
            rendered.contains(
                "zinder_ingest_bulk_pipeline_reorder_buffer_blocks{stage=\"block_prepare\"} 3"
            ),
            "expected 3 buffered blocks behind slow head, got:\n{rendered}"
        );

        head_gate.notify_one();
        let mut emitted_heights = Vec::new();
        loop {
            let Some(outcome) =
                tokio::time::timeout(std::time::Duration::from_secs(2), prepared_stream.next())
                    .await?
            else {
                break;
            };
            emitted_heights.push(outcome?.prepared.facts.block_header.height.value());
        }
        assert_eq!(emitted_heights, vec![1, 2, 3, 4]);
        assert_eq!(
            prepare_memory.available_permits(),
            usize::try_from(prepare_memory_permits)?
        );
        Ok(())
    }

    #[tokio::test]
    async fn unordered_prepared_block_stream_enforces_byte_watermark_across_reorder_buffer()
    -> TestResult {
        use futures_util::StreamExt;
        use parking_lot::Mutex;
        use std::sync::Arc;
        use tokio::sync::{Notify, Semaphore};

        let one_block_permits =
            super::memory_permit_count(16 * MIB).min(super::memory_permits_for_block(64, u32::MAX));
        let prepare_memory_permits = one_block_permits.saturating_mul(2);
        let prepare_memory = Arc::new(Semaphore::new(usize::try_from(prepare_memory_permits)?));
        let admitted = super::admit_source_blocks(
            test_chunk_stream(&[1, 2, 3], 64),
            Arc::clone(&prepare_memory),
            prepare_memory_permits,
            zinder_core::Network::ZcashRegtest,
        );
        let head_gate = Arc::new(Notify::new());
        let started_heights = Arc::new(Mutex::new(Vec::new()));
        let mut prepared_stream =
            super::UnorderedPreparedBlockStream::new(admitted, 4, |admitted_block| {
                let head_gate = Arc::clone(&head_gate);
                let started_heights = Arc::clone(&started_heights);
                async move {
                    let height = admitted_block.source_block.height;
                    started_heights.lock().push(height.value());
                    if height.value() == 1 {
                        head_gate.notified().await;
                    }
                    let prepared = test_prepared_block_at_height(height)
                        .map_err(|error| map_prepare_test_error(error.as_ref()))?;
                    Ok(super::QueuedPreparedBlock {
                        prepared,
                        memory_permit: admitted_block.memory_permit,
                    })
                }
            });

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        poll_stream_while_pending(&mut prepared_stream, &recorder, || false).await;
        assert_eq!(
            *started_heights.lock(),
            vec![1, 2],
            "block 3 must wait for byte watermark even with prepare_concurrency = 4"
        );

        head_gate.notify_one();
        let first_block =
            tokio::time::timeout(std::time::Duration::from_secs(2), prepared_stream.next())
                .await?
                .ok_or("expected first prepared block")??;
        assert_eq!(first_block.prepared.facts.block_header.height.value(), 1);
        drop(first_block);

        let mut remaining = Vec::new();
        loop {
            let Some(outcome) =
                tokio::time::timeout(std::time::Duration::from_secs(2), prepared_stream.next())
                    .await?
            else {
                break;
            };
            remaining.push(outcome?.prepared.facts.block_header.height.value());
        }
        assert_eq!(remaining, vec![2, 3]);
        assert_eq!(
            prepare_memory.available_permits(),
            usize::try_from(prepare_memory_permits)?
        );
        Ok(())
    }

    fn unordered_source_error_reason(
        outcome: Result<super::QueuedPreparedBlock, super::CanonicalConstructionError>,
    ) -> String {
        match outcome {
            Err(super::CanonicalConstructionError::Source { source }) => source.to_string(),
            Err(other) => other.to_string(),
            Ok(_) => String::new(),
        }
    }

    async fn run_unordered_error_precedence_case(head_fails: bool) -> TestResult {
        use futures_util::StreamExt;
        use std::sync::Arc;
        use tokio::sync::{Notify, Semaphore};

        let prepare_memory_permits = super::memory_permit_count(16 * MIB);
        let prepare_memory = Arc::new(Semaphore::new(usize::try_from(prepare_memory_permits)?));
        let admitted = super::admit_source_blocks(
            test_chunk_stream(&[1, 2, 3], 64),
            Arc::clone(&prepare_memory),
            prepare_memory_permits,
            zinder_core::Network::ZcashRegtest,
        );
        let head_gate = Arc::new(Notify::new());
        let mut prepared_stream =
            super::UnorderedPreparedBlockStream::new(admitted, 4, |admitted_block| {
                let head_gate = Arc::clone(&head_gate);
                async move {
                    let height = admitted_block.source_block.height;
                    if height.value() == 1 {
                        head_gate.notified().await;
                        if head_fails {
                            return Err(super::CanonicalConstructionError::Source {
                                source: super::IngestError::BlockingTaskFailed {
                                    reason: "head failure".to_owned(),
                                },
                            });
                        }
                    } else if height.value() == 2 {
                        return Err(super::CanonicalConstructionError::Source {
                            source: super::IngestError::BlockingTaskFailed {
                                reason: "second failure".to_owned(),
                            },
                        });
                    }
                    let prepared = test_prepared_block_at_height(height)
                        .map_err(|error| map_prepare_test_error(error.as_ref()))?;
                    Ok(super::QueuedPreparedBlock {
                        prepared,
                        memory_permit: admitted_block.memory_permit,
                    })
                }
            });

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        poll_stream_while_pending(&mut prepared_stream, &recorder, || false).await;
        head_gate.notify_one();

        if head_fails {
            let reason = unordered_source_error_reason(
                prepared_stream.next().await.ok_or("expected first item")?,
            );
            assert!(reason.contains("head failure"), "{reason}");
        } else {
            let height = prepared_stream
                .next()
                .await
                .ok_or("expected first item")??
                .prepared
                .facts
                .block_header
                .height
                .value();
            assert_eq!(height, 1);
            let reason = unordered_source_error_reason(
                prepared_stream
                    .next()
                    .await
                    .ok_or("expected second failure")?,
            );
            assert!(reason.contains("second failure"), "{reason}");
        }
        assert!(prepared_stream.next().await.is_none());
        assert_eq!(
            prepare_memory.available_permits(),
            usize::try_from(prepare_memory_permits)?
        );
        Ok(())
    }

    #[tokio::test]
    async fn unordered_prepared_block_stream_emits_prefix_and_earliest_error_on_failure()
    -> TestResult {
        run_unordered_error_precedence_case(false).await?;
        run_unordered_error_precedence_case(true).await?;
        Ok(())
    }

    fn test_queued_prepared_block(
        height_value: u32,
        semaphore: &std::sync::Arc<tokio::sync::Semaphore>,
    ) -> Result<super::QueuedPreparedBlock, Box<dyn std::error::Error>> {
        test_queued_prepared_block_with_sapling_commitments(height_value, &[], semaphore)
    }

    fn test_queued_prepared_block_with_sapling_commitments(
        height_value: u32,
        sapling_commitments: &[[u8; 32]],
        semaphore: &std::sync::Arc<tokio::sync::Semaphore>,
    ) -> Result<super::QueuedPreparedBlock, Box<dyn std::error::Error>> {
        let height = zinder_core::BlockHeight::new(height_value);
        let hash = zinder_core::BlockHash::from_bytes([u8::try_from(height_value)?; 32]);
        let parent_hash =
            zinder_core::BlockHash::from_bytes([u8::try_from(height_value.saturating_sub(1))?; 32]);
        let block_header = zinder_core::BlockHeaderArtifact::new(
            height,
            hash,
            parent_hash,
            [0; 32],
            [0; 32],
            1_700_000_000_i64.saturating_add(i64::from(height_value)),
            0,
            [0; 32],
            4,
            64,
        );
        let facts = zinder_core::CanonicalBlockFacts {
            block_header,
            serialized_bytes_digest: zinder_core::SerializedBytesDigest::from_serialized_bytes(
                &height_value.to_be_bytes(),
            ),
            transactions: Vec::new(),
        };
        let replay_envelope = zinder_core::encode_canonical_block_replay(
            &facts,
            zinder_core::CanonicalBlockReplayFormatVersion::CURRENT,
            zinder_core::CanonicalBlockFactsDigestVersion::CURRENT,
        );
        let compact_transactions = if sapling_commitments.is_empty() {
            Vec::new()
        } else {
            let sapling_outputs = sapling_commitments
                .iter()
                .map(|commitment| zinder_core::CompactSaplingOutput {
                    commitment: *commitment,
                    ephemeral_key: [0x11; 32],
                    ciphertext: [0x22; 52],
                })
                .collect();
            vec![zinder_core::CompactTransaction {
                index: 0,
                transaction_id: zinder_core::TransactionId::from_bytes(
                    [u8::try_from(height_value)?; 32],
                ),
                data: zinder_core::CompactTransactionData {
                    sapling_outputs,
                    ..Default::default()
                },
            }]
        };
        let partial_compact_block = zinder_core::CompactBlockArtifact::new(
            zinder_core::BlockId::new(height, hash),
            parent_hash,
            1_700_000_000_u32.saturating_add(height_value),
            compact_transactions,
            zinder_core::CompactChainMetadata {
                sapling_commitment_tree_size: u32::try_from(sapling_commitments.len())?,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )?;
        let prepared = crate::artifact_builder::PreparedCanonicalBlock {
            facts,
            replay_envelope,
            retained_raw_blobs: crate::artifact_builder::RetainedRawBlobs {
                block_blob: None,
                transaction_blobs: Vec::new(),
            },
            partial_compact_block,
            tree_size_additions: crate::artifact_builder::CommitmentTreeSizes {
                sapling: u32::try_from(sapling_commitments.len())?,
                orchard: 0,
                ironwood: 0,
            },
        };
        Ok(super::QueuedPreparedBlock {
            prepared,
            memory_permit: semaphore.clone().try_acquire_owned()?,
        })
    }

    fn test_build_receiver(
        block_receiver: tokio::sync::mpsc::Receiver<
            Result<super::QueuedPreparedBlock, super::CanonicalConstructionError>,
        >,
        tip_height: u32,
        workers: u32,
    ) -> Result<super::CanonicalBuildBlockReceiver, Box<dyn std::error::Error>> {
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let build_plan = zinder_store::CanonicalStoreBuildPlan::complete(
            &activations,
            1_700_000_000,
            zinder_core::BlockId::new(
                zinder_core::BlockHeight::new(tip_height),
                zinder_core::BlockHash::from_bytes([u8::try_from(tip_height)?; 32]),
            ),
            zinder_store::RawBlobRetention::None,
            zinder_store::CanonicalReorgPolicy::new(100)?,
        )?;
        let commitment_tree_accumulator =
            zinder_core::CommitmentTreeAccumulator::from_validated_frontiers(
                zinder_core::BlockHeight::new(0),
                &zinder_core::CommitmentTreeFrontiers::default(),
                &activations,
            )?;
        let tree_hashers =
            super::TreeHashers::start(NonZeroU32::new(workers).ok_or("workers must be non-zero")?)?;
        let cancel = tokio_util::sync::CancellationToken::new();
        let loader_stop = cancel.child_token();
        Ok(super::CanonicalBuildBlockReceiver {
            receiver: block_receiver,
            runtime: tokio::runtime::Handle::current(),
            ready: std::collections::VecDeque::new(),
            terminated_after_error: false,
            active_memory_permit: None,
            running_tree_sizes: crate::artifact_builder::CommitmentTreeSizes::from_tip_metadata(
                zinder_core::ChainTipMetadata::default(),
            ),
            commitment_tree_accumulator,
            derived_subtree_roots: Vec::new(),
            build_tip_height: zinder_core::BlockHeight::new(tip_height),
            checkpoint_selection: None,
            checkpoint_tree_sizes: zinder_core::ChainTipMetadata::default(),
            cancel,
            loader_stop,
            stage_timers: super::BlockLoaderStageTimers::register(),
            tree_hashers,
            in_flight_store_height: None,
            progress: Some(super::ConstructionProgress::start(&build_plan, None)),
            parallel_batch_sizes: Vec::new(),
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backlogged_receiver_drains_full_batches_up_to_batch_ceiling()
    -> Result<(), Box<dyn std::error::Error>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(160);
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(160));
        for height in 1_u32..=140 {
            sender
                .try_send(Ok(test_queued_prepared_block(height, &semaphore)?))
                .map_err(|_| "channel capacity exceeded")?;
        }
        drop(sender);
        let mut build_blocks = test_build_receiver(receiver, 140, 4)?;
        let (emitted_heights, batch_sizes) = tokio::task::spawn_blocking(move || {
            let mut heights = Vec::new();
            for next_entry in &mut build_blocks {
                let block = next_entry?;
                heights.push(block.facts.block_header.height.value());
            }
            Ok::<_, super::CanonicalConstructionError>((heights, build_blocks.parallel_batch_sizes))
        })
        .await??;

        assert_eq!(emitted_heights, (1_u32..=140).collect::<Vec<_>>());
        assert_eq!(batch_sizes, vec![64, 64, 12]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backlogged_receiver_preserves_trailing_channel_error_across_batches()
    -> Result<(), Box<dyn std::error::Error>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(80);
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(80));
        for height in 1_u32..=70 {
            sender
                .try_send(Ok(test_queued_prepared_block(height, &semaphore)?))
                .map_err(|_| "channel capacity exceeded")?;
        }
        sender
            .try_send(Err(super::CanonicalConstructionError::LoaderTaskFailed {
                reason: "injected source error".to_owned(),
            }))
            .map_err(|_| "channel capacity exceeded")?;
        drop(sender);
        let mut build_blocks = test_build_receiver(receiver, 70, 4)?;
        let (emitted_heights, terminal_error, batch_sizes) =
            tokio::task::spawn_blocking(move || {
                let mut heights = Vec::new();
                let mut terminal_error = None;
                for next_entry in &mut build_blocks {
                    match next_entry {
                        Ok(block) => heights.push(block.facts.block_header.height.value()),
                        Err(error) => {
                            terminal_error = Some(error);
                            break;
                        }
                    }
                }
                (heights, terminal_error, build_blocks.parallel_batch_sizes)
            })
            .await?;

        assert_eq!(emitted_heights, (1_u32..=70).collect::<Vec<_>>());
        assert_eq!(batch_sizes, vec![64, 6]);
        assert!(matches!(
            terminal_error,
            Some(super::CanonicalConstructionError::LoaderTaskFailed { ref reason })
                if reason == "injected source error"
        ));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the test transfers single-block permits across async and blocking tasks"
    )]
    async fn pipelined_build_blocks_makes_progress_under_single_block_permit()
    -> Result<(), Box<dyn std::error::Error>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let mut build_blocks = test_build_receiver(receiver, 12, 4)?;
        let progress = build_blocks
            .progress
            .take()
            .ok_or("expected test progress")?;
        let cancel = build_blocks.cancel.clone();
        let loader_stop = build_blocks.loader_stop.clone();

        let producer_sem = std::sync::Arc::clone(&semaphore);
        let sender_task = tokio::spawn(async move {
            for height in 1_u32..=12 {
                let permit = std::sync::Arc::clone(&producer_sem)
                    .acquire_owned()
                    .await
                    .map_err(|error| error.to_string())?;
                let step_sem = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
                let mut queued = test_queued_prepared_block(height, &step_sem)
                    .map_err(|error| error.to_string())?;
                queued.memory_permit = permit;
                sender
                    .send(Ok(queued))
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok::<(), String>(())
        });

        let emitted_heights = tokio::task::spawn_blocking(move || {
            let mut pipelined = super::PipelinedBuildBlocks::spawn(
                build_blocks,
                progress,
                cancel,
                loader_stop,
                super::MAX_COMMITMENT_BATCH_BLOCKS,
            )?;
            let mut heights = Vec::new();
            for next_entry in &mut pipelined {
                heights.push(next_entry?.facts.block_header.height.value());
            }
            let _ = pipelined.finish_and_join(true)?;
            Ok::<Vec<u32>, super::CanonicalConstructionError>(heights)
        });

        let (sent_outcome, consumed) =
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(sender_task, emitted_heights)
            })
            .await?;
        sent_outcome?.map_err(std::io::Error::other)?;
        assert_eq!(consumed??, (1_u32..=12).collect::<Vec<_>>());
        assert_eq!(semaphore.available_permits(), 1);
        Ok(())
    }

    async fn collect_prefix_and_failure_for_workers(
        workers: u32,
    ) -> Result<(Vec<u32>, String, Vec<u32>, Vec<usize>), Box<dyn std::error::Error>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(16));
        sender
            .try_send(Ok(test_queued_prepared_block(1, &semaphore)?))
            .map_err(|_| "channel capacity exceeded")?;
        sender
            .try_send(Ok(test_queued_prepared_block(2, &semaphore)?))
            .map_err(|_| "channel capacity exceeded")?;
        sender
            .try_send(Ok(test_queued_prepared_block_with_sapling_commitments(
                3,
                &[[0xff; 32]],
                &semaphore,
            )?))
            .map_err(|_| "channel capacity exceeded")?;
        sender
            .try_send(Ok(test_queued_prepared_block(4, &semaphore)?))
            .map_err(|_| "channel capacity exceeded")?;
        sender
            .try_send(Ok(test_queued_prepared_block(5, &semaphore)?))
            .map_err(|_| "channel capacity exceeded")?;
        drop(sender);
        let mut build_blocks = test_build_receiver(receiver, 5, workers)?;
        let outcome = tokio::task::spawn_blocking(move || {
            let mut prefix_heights = Vec::new();
            let mut terminal_error = None;
            let mut trailing_heights = Vec::new();
            for next_entry in &mut build_blocks {
                match next_entry {
                    Ok(block) if terminal_error.is_none() => {
                        prefix_heights.push(block.facts.block_header.height.value());
                    }
                    Ok(block) => {
                        trailing_heights.push(block.facts.block_header.height.value());
                    }
                    Err(error) if terminal_error.is_none() => {
                        terminal_error = Some(error.to_string());
                    }
                    Err(_) => {}
                }
            }
            (
                prefix_heights,
                terminal_error.unwrap_or_default(),
                trailing_heights,
                build_blocks.parallel_batch_sizes,
            )
        })
        .await?;
        Ok(outcome)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backlogged_receiver_recovers_valid_prefix_before_batch_append_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let (serial_prefix, serial_error, serial_trailing, serial_batches) =
            collect_prefix_and_failure_for_workers(1).await?;
        let (batched_prefix, batched_error, batched_trailing, batched_batches) =
            collect_prefix_and_failure_for_workers(4).await?;

        assert_eq!(serial_prefix, vec![1, 2]);
        assert_eq!(batched_prefix, vec![1, 2]);
        assert_ne!(batched_error, "");
        assert_eq!(batched_error, serial_error);
        assert_eq!(serial_trailing, Vec::<u32>::new());
        assert_eq!(batched_trailing, Vec::<u32>::new());
        assert_eq!(serial_batches, Vec::<usize>::new());
        assert_eq!(batched_batches, Vec::<usize>::new());
        Ok(())
    }
}
