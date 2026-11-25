//! Fresh canonical construction from one ordered node-source pass.

pub(crate) mod progress;
pub(crate) mod source_fetch;
mod tree_hashing;
pub(crate) mod watermark;

use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::{
    Stream, StreamExt,
    stream::{self, BoxStream},
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
    BlockHeight, BlockId, ChainTipMetadata, CommitmentTreeAccumulator,
    CommitmentTreeAccumulatorError, CommitmentTreeCheckpoint, Network, NetworkUpgradeActivations,
    NetworkUpgradeActivationsFingerprint, NetworkUpgradeActivationsFingerprintVersion,
    ShieldedProtocol, SubtreeRootIndex, SubtreeRootRange,
};
use zinder_source::{NodeSource, SourceBlock};
use zinder_store::{
    CanonicalBlockLoadEvidence, CanonicalBuildBlock, CanonicalBuildSubtreeRoot,
    CanonicalStoreBuildError, CanonicalStoreError, CanonicalStoreWorkload,
    CanonicalSubtreeRootLoadEvidence, RawBlobRetention, RocksDbCanonicalBuilder,
    TREE_STATE_CHECKPOINT_STRIDE, TreeStateCheckpointSelection,
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
use tree_hashing::{PendingBuildBlock, TreeHashers, TreeWork};

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
    /// One hashes on the loader thread itself. More run each block's pool
    /// appends and large commitment batches in parallel, and derive the
    /// checkpoints of up to twice this many blocks while the loader writes
    /// earlier ones. Blocks still reach the store in height order with the
    /// same trees and checkpoints.
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
    /// build has measured, including its peak memory: each worker lets two
    /// more prepared blocks wait with their memory permits.
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
    Box::pin(load_fresh_canonical_blocks_until_cancelled(
        builder,
        source,
        config,
        &CancellationToken::new(),
        None,
    ))
    .await
}

async fn load_fresh_canonical_blocks_until_cancelled<Source>(
    builder: RocksDbCanonicalBuilder,
    source: &Source,
    config: &CanonicalConstructionConfig,
    cancel: &CancellationToken,
    progress: Option<ConstructionProgress>,
) -> Result<CanonicalBlockLoadOutcome, CanonicalConstructionError>
where
    Source: NodeSource + Clone,
{
    let started_at = Instant::now();
    register_prohibited_read_metrics();
    let build_plan = builder.build_plan().clone();
    validate_construction_identity(&build_plan, config)?;
    let progress = progress.unwrap_or_else(|| ConstructionProgress::start(&build_plan, None));
    let block_queue_capacity =
        usize::try_from(config.pipeline_limits.block_prepare_concurrency.get())
            .unwrap_or(usize::MAX)
            .max(1);
    let prepared_blocks =
        build_prepared_block_stream(source, &build_plan, config, builder.raw_blob_retention());
    let checkpoint_selection = builder.tree_state_checkpoint_selection();
    let outcome = drive_block_loader(
        builder,
        prepared_blocks,
        BlockLoaderSettings {
            block_queue_capacity,
            network_upgrade_activations: Arc::clone(&config.network_upgrade_activations),
            checkpoint_selection,
            tree_hashing_workers: config.pipeline_limits.tree_hashing_workers,
            cancel: cancel.clone(),
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
    Box::pin(load_fresh_canonical_until_cancelled(
        builder,
        source,
        config,
        &CancellationToken::new(),
        None,
    ))
    .await
}

/// Loads every source family until `cancel` fires.
///
/// Cancellation during the block stream stops the blocking loader at its next
/// block boundary. Once every block is queued, the loader finalizes SST files,
/// which cannot be interrupted. Either way the builder is closed on return.
pub(crate) async fn load_fresh_canonical_until_cancelled<Source>(
    builder: RocksDbCanonicalBuilder,
    source: &Source,
    config: &CanonicalConstructionConfig,
    cancel: &CancellationToken,
    progress: Option<ConstructionProgress>,
) -> Result<CanonicalSourceLoadOutcome, CanonicalConstructionError>
where
    Source: NodeSource + Clone,
{
    let block_outcome =
        load_fresh_canonical_blocks_until_cancelled(builder, source, config, cancel, progress)
            .await?;
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

fn build_prepared_block_stream<'source, Source>(
    source: &'source Source,
    build_plan: &zinder_store::CanonicalStoreBuildPlan,
    config: &CanonicalConstructionConfig,
    raw_blob_retention: RawBlobRetention,
) -> impl Stream<Item = Result<QueuedPreparedBlock, CanonicalConstructionError>>
+ Send
+ use<'source, Source>
where
    Source: NodeSource + Clone,
{
    let first_height = build_plan.history_bounds().first_available_height();
    let source_segment_sizer = Arc::new(Mutex::new(SourceSegmentSizer::new(
        config.pipeline_limits.source_segment_max_blocks,
        config.pipeline_limits.source_segment_target_response_bytes,
        Arc::clone(&config.network_upgrade_activations),
        first_height,
    )));
    let source_chunks = build_source_block_stream(
        source,
        CanonicalSourceFetchConfig {
            request_timeout: config.request_timeout,
            history_predecessor: Some(build_plan.history_predecessor().block_id),
            from_height: first_height,
            to_height: build_plan.build_tip().height,
            max_response_bytes: config.pipeline_limits.max_response_bytes,
            target_response_payload_bytes: config
                .pipeline_limits
                .source_segment_target_response_bytes,
            source_fetch_max_in_flight_requests: config
                .pipeline_limits
                .source_fetch_max_in_flight_requests,
            source_fetch_max_in_flight_bytes: config
                .pipeline_limits
                .source_fetch_max_in_flight_bytes,
            source_segment_sizer,
        },
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
    admit_source_blocks(
        source_chunks,
        prepare_memory,
        prepare_memory_permits,
        build_plan.network(),
    )
    .map(move |admitted_source_block| {
        let activations = Arc::clone(&activations);
        async move {
            let AdmittedSourceBlock {
                source_block,
                memory_permit,
            } = admitted_source_block?;
            let prepared = tokio::task::spawn_blocking(move || {
                prepare_canonical_block_for_retention(
                    &source_block,
                    &activations,
                    raw_blob_retention,
                )
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
    })
    .buffered(prepare_concurrency)
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
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    checkpoint_selection: Option<TreeStateCheckpointSelection>,
    cancel: CancellationToken,
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
    let tree_hashers = TreeHashers::start(tree_hashing_workers, tokio::runtime::Handle::current())?;
    let (block_sender, block_receiver) = mpsc::channel(block_queue_capacity);
    let receiver_config = BuildBlockReceiverConfig {
        network_upgrade_activations,
        checkpoint_selection,
        cancel: cancel.clone(),
        progress,
    };
    let loader_task = tokio::task::spawn_blocking(move || {
        run_block_loader(builder, block_receiver, tree_hashers, receiver_config)
    });

    let forwarded = cancel
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
    let workload = builder.workload();
    let build_tip_height = builder.build_plan().build_tip().height;
    let mut build_blocks = CanonicalBuildBlockReceiver {
        receiver: block_receiver,
        active_memory_permit: None,
        running_tree_sizes: CommitmentTreeSizes::from_tip_metadata(predecessor_tip_metadata),
        commitment_tree_accumulator,
        derived_subtree_roots: Vec::new(),
        workload,
        build_tip_height,
        checkpoint_selection: receiver_config.checkpoint_selection,
        checkpoint_tree_sizes: predecessor_tip_metadata,
        cancel: receiver_config.cancel,
        stage_timers: BlockLoaderStageTimers::register(workload),
        tree_hashers,
        in_flight_store_height: None,
        progress: receiver_config.progress,
    };
    let evidence = builder
        .bulk_load_blocks(
            &receiver_config.network_upgrade_activations,
            &mut build_blocks,
        )
        .map_err(canonical_build_error)?;
    build_blocks.progress.finish();
    builder.record_resource_gauges();
    let mut derived_subtree_roots = std::mem::take(&mut build_blocks.derived_subtree_roots);
    drop(build_blocks);
    // Blocks complete different pools' subtrees in any order, while the
    // node's ranges and the store's keys list one pool after another.
    derived_subtree_roots.sort_by_key(|root| (root.protocol.id(), root.subtree_index.value()));
    Ok(CanonicalBlockLoadOutcome {
        builder,
        evidence,
        derived_subtree_roots,
    })
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
    active_memory_permit: Option<OwnedSemaphorePermit>,
    running_tree_sizes: CommitmentTreeSizes,
    commitment_tree_accumulator: CommitmentTreeAccumulator,
    derived_subtree_roots: Vec<CanonicalBuildSubtreeRoot>,
    workload: CanonicalStoreWorkload,
    build_tip_height: BlockHeight,
    checkpoint_selection: Option<TreeStateCheckpointSelection>,
    checkpoint_tree_sizes: ChainTipMetadata,
    cancel: CancellationToken,
    stage_timers: BlockLoaderStageTimers,
    /// Hashing threads, or `None` when the loader hashes on its own thread.
    tree_hashers: Option<TreeHashers>,
    in_flight_store_height: Option<BlockHeight>,
    progress: ConstructionProgress,
}

/// Per-block durations of the serial loader, resolved once per construction.
///
/// The stages do not overlap. `receive_wait` is time the loader spent idle
/// waiting for a prepared block, so a high share means the source or
/// preparation, not the loader, bounds construction. Every stage named
/// `tree_hashing_*` is time the loader spent on commitment-tree hashing, so
/// with one tree-hashing worker their sum is the serial hashing cost. With
/// more workers, `tree_hashing_append` is the loader's wait for a block's
/// parallel append, and `tree_hashing_checkpoint` and
/// `tree_hashing_final_roots` are its wait for work the workers finished
/// while it handled other blocks.
struct BlockLoaderStageTimers {
    receive_wait: Histogram,
    commitment_extract: Histogram,
    tree_hashing_append: Histogram,
    position: Histogram,
    tree_hashing_checkpoint: Histogram,
    /// Present only for the explorer workload, the one that derives final roots.
    tree_hashing_final_roots: Option<Histogram>,
}

impl BlockLoaderStageTimers {
    fn register(workload: CanonicalStoreWorkload) -> Self {
        let stage = |stage: &'static str| metrics::histogram!(BLOCK_LOAD_STAGE_DURATION_METRIC, "stage" => stage);
        Self {
            receive_wait: stage("receive_wait"),
            commitment_extract: stage("commitment_extract"),
            tree_hashing_append: stage("tree_hashing_append"),
            position: stage("position"),
            tree_hashing_checkpoint: stage("tree_hashing_checkpoint"),
            tree_hashing_final_roots: workload
                .derives_final_roots()
                .then(|| stage("tree_hashing_final_roots")),
        }
    }
}

impl Iterator for CanonicalBuildBlockReceiver {
    type Item = Result<CanonicalBuildBlock, CanonicalConstructionError>;

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the received block's memory permit moves into queued_block and stays active through the store write"
    )]
    fn next(&mut self) -> Option<Self::Item> {
        self.active_memory_permit = None;
        self.record_staged_block();
        if self.tree_hashers.is_some() {
            return self.next_hashed_ahead();
        }
        match self.receive(true) {
            Received::Block(queued_block) => {
                self.active_memory_permit = Some(queued_block.memory_permit);
                let built = self.build_block(queued_block.prepared);
                if let Ok(block) = &built {
                    self.in_flight_store_height = Some(block.facts.block_header.height);
                }
                Some(built)
            }
            Received::Failed(source) => Some(Err(source)),
            Received::Empty | Received::Closed => self
                .cancel
                .is_cancelled()
                .then_some(Err(CanonicalConstructionError::Cancelled)),
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
        {
            self.progress.record_loaded(height);
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
                    if !self.cancel.is_cancelled() {
                        self.progress.flush_pending();
                    }
                    self.receiver.blocking_recv()
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
        match received.filter(|_| !self.cancel.is_cancelled()) {
            Some(Ok(queued_block)) => Received::Block(Box::new(queued_block)),
            Some(Err(source)) => Received::Failed(source),
            None => Received::Closed,
        }
    }

    /// Hands the store the oldest appended block once its tree work is done,
    /// after appending further blocks that are already prepared.
    fn next_hashed_ahead(
        &mut self,
    ) -> Option<Result<CanonicalBuildBlock, CanonicalConstructionError>> {
        if self.cancel.is_cancelled() {
            return Some(Err(CanonicalConstructionError::Cancelled));
        }
        while self
            .tree_hashers
            .as_ref()
            .is_some_and(TreeHashers::wants_more)
        {
            // Waiting for a new block while earlier blocks hold memory permits
            // could stall a source pipeline that needs those permits back.
            let idle = self.tree_hashers.as_ref().is_some_and(TreeHashers::is_idle);
            let staged = match self.receive(idle) {
                Received::Block(queued_block) => self.stage_block(*queued_block),
                Received::Failed(source) => Err(source),
                Received::Empty | Received::Closed => break,
            };
            if let Some(tree_hashers) = self.tree_hashers.as_mut() {
                tree_hashers.push(staged);
            }
        }
        let Some(pending) = self.tree_hashers.as_mut().and_then(TreeHashers::pop) else {
            return self
                .cancel
                .is_cancelled()
                .then_some(Err(CanonicalConstructionError::Cancelled));
        };
        let built = pending.and_then(|pending| self.finish_block(pending));
        if let Ok(block) = &built {
            self.in_flight_store_height = Some(block.facts.block_header.height);
        }
        Some(built)
    }

    /// Appends and positions one block, and starts deriving its checkpoint
    /// and final roots on the hashing workers.
    fn stage_block(
        &mut self,
        queued_block: QueuedPreparedBlock,
    ) -> Result<PendingBuildBlock, CanonicalConstructionError> {
        let QueuedPreparedBlock {
            prepared,
            memory_permit,
        } = queued_block;
        let height = prepared.facts.block_header.height;
        let block = BlockId::new(height, prepared.facts.block_header.block_hash);
        let block_time_seconds = prepared.partial_compact_block.time();
        self.append_to_commitment_trees(&prepared)?;
        let positioned = self.position_block(prepared)?;
        let work = TreeWork {
            checkpoint_required: self.select_checkpoint(height, positioned.tip_metadata),
            final_roots_required: self.workload.derives_final_roots(),
        };
        let tree_work = (work.checkpoint_required || work.final_roots_required)
            .then(|| {
                self.tree_hashers.as_ref().map(|tree_hashers| {
                    tree_hashers.spawn_tree_work(
                        self.commitment_tree_accumulator.snapshot(),
                        block,
                        block_time_seconds,
                        work,
                    )
                })
            })
            .flatten();
        Ok(PendingBuildBlock {
            height,
            positioned,
            memory_permit,
            work,
            tree_work,
        })
    }

    /// Waits for a staged block's tree work and builds the block the store
    /// writes, keeping its memory permit until the store is done with it.
    fn finish_block(
        &mut self,
        pending: PendingBuildBlock,
    ) -> Result<CanonicalBuildBlock, CanonicalConstructionError> {
        let PendingBuildBlock {
            height,
            positioned,
            memory_permit,
            work,
            tree_work,
        } = pending;
        self.active_memory_permit = Some(memory_permit);
        let Some(tree_work) = tree_work else {
            return Ok(canonical_build_block(positioned, None, None));
        };
        let waited_at = Instant::now();
        let output = tree_work.wait(&self.cancel)?;
        if work.checkpoint_required {
            self.stage_timers
                .tree_hashing_checkpoint
                .record(waited_at.elapsed());
        } else if let Some(timer) = &self.stage_timers.tree_hashing_final_roots {
            timer.record(waited_at.elapsed());
        }
        let tree_state_checkpoint = output
            .checkpoint
            .transpose()
            .map_err(|source| CanonicalConstructionError::CommitmentTreeState { height, source })?;
        Ok(canonical_build_block(
            positioned,
            tree_state_checkpoint,
            output.final_roots,
        ))
    }

    /// Positions an appended block and requires its tree sizes to match the
    /// accumulated frontiers.
    fn position_block(
        &mut self,
        prepared: PreparedCanonicalBlock,
    ) -> Result<PositionedCanonicalBlock, CanonicalConstructionError> {
        let height = prepared.facts.block_header.height;
        let tree_appended_at = Instant::now();
        let positioned =
            position_canonical_block(prepared, &mut self.running_tree_sizes).map_err(|source| {
                CanonicalConstructionError::Source {
                    source: IngestError::from(source),
                }
            })?;
        let accumulated = self.commitment_tree_accumulator.tip_metadata();
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
        let block_final_note_commitment_roots = self.block_final_note_commitment_roots(block_hash);
        Ok(canonical_build_block(
            positioned,
            tree_state_checkpoint,
            block_final_note_commitment_roots,
        ))
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
        let extracted_at = Instant::now();
        self.stage_timers
            .commitment_extract
            .record(extracted_at.duration_since(started_at));
        let accumulator = &mut self.commitment_tree_accumulator;
        let parallel_append = should_parallelize_append(&commitments);
        match &self.tree_hashers {
            Some(tree_hashers) if parallel_append && tree_hashers.has_append_workers() => {
                tree_hashers.install(|| {
                    accumulator.append_block_commitments_in_parallel(
                        height,
                        &commitments.sapling,
                        &commitments.orchard,
                        &commitments.ironwood,
                    )
                })
            }
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
            .record(extracted_at.elapsed());
        Ok(())
    }

    /// Derives the explorer workload's per-block final roots, omitting a
    /// block before any pool activates.
    fn block_final_note_commitment_roots(
        &mut self,
        block_hash: zinder_core::BlockHash,
    ) -> Option<zinder_core::BlockFinalNoteCommitmentRoots> {
        if !self.workload.derives_final_roots() {
            return None;
        }
        let started_at = Instant::now();
        let roots = self
            .commitment_tree_accumulator
            .final_note_commitment_roots(block_hash);
        if let Some(timer) = &self.stage_timers.tree_hashing_final_roots {
            timer.record(started_at.elapsed());
        }
        (roots.sapling.is_some() || roots.orchard.is_some() || roots.ironwood.is_some())
            .then_some(roots)
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
    block_final_note_commitment_roots: Option<zinder_core::BlockFinalNoteCommitmentRoots>,
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
        block_final_note_commitment_roots,
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
    use super::{
        BlockLoaderStageTimers, CanonicalPipelineLimits, CanonicalPipelineLimitsError,
        CanonicalStoreWorkload,
    };
    use std::num::{NonZeroU32, NonZeroU64};

    #[test]
    fn only_an_explorer_loader_registers_a_final_roots_timer() {
        let registered_stages = |workload| {
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            let timers = metrics::with_local_recorder(&recorder, || {
                BlockLoaderStageTimers::register(workload)
            });
            (timers.tree_hashing_final_roots.is_some(), handle.render())
        };

        let (wallet_timer, wallet_series) = registered_stages(CanonicalStoreWorkload::Wallet);
        let (explorer_timer, explorer_series) = registered_stages(CanonicalStoreWorkload::Explorer);

        assert!(!wallet_timer);
        assert!(!wallet_series.contains("tree_hashing_final_roots"));
        assert!(explorer_timer);
        assert!(explorer_series.contains("tree_hashing_final_roots"));
    }

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
}
