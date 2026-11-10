//! Fresh canonical construction from one ordered node-source pass.

pub(crate) mod source_fetch;
pub(crate) mod watermark;

use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Duration,
};

use futures_util::{
    Stream, StreamExt,
    stream::{self, BoxStream},
};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use zinder_core::{
    BlockHeight, BlockId, ChainTipMetadata, CommitmentTreeAccumulator,
    CommitmentTreeAccumulatorError, CommitmentTreeCheckpoint, Network, NetworkUpgradeActivations,
    NetworkUpgradeActivationsFingerprint, NetworkUpgradeActivationsFingerprintVersion,
    ShieldedProtocol,
};
use zinder_source::{NodeSource, SourceBlock};
use zinder_store::{
    CanonicalBlockLoadEvidence, CanonicalBuildBlock, CanonicalBuildSubtreeRoot,
    CanonicalStoreBuildError, CanonicalStoreError, CanonicalStoreWorkload,
    CanonicalSubtreeRootLoadEvidence, RocksDbCanonicalBuilder, TREE_STATE_CHECKPOINT_STRIDE,
};

use crate::{
    RawBlobPolicy,
    artifact_builder::{
        CommitmentTreeSizes, PositionedCanonicalBlock, PreparedCanonicalBlock, RetainedRawBlobs,
        position_canonical_block, prepare_canonical_block,
    },
    chain_ingest::IngestError,
    writer::raw_blob_policy_for_retention,
};
use source_fetch::{
    CanonicalSourceFetchConfig, SourceBlockChunk, SourceSegmentSizer, build_source_block_stream,
};

const CANONICAL_PREPARE_MEMORY_UNIT_BYTES: u64 = 1_024;
const CANONICAL_PREPARE_RAW_BLOCK_MULTIPLIER: u64 = 16;
const CANONICAL_PREPARE_FIXED_BYTES: u64 = 64 * 1_024;
const MIB: u64 = 1_024 * 1_024;
const DEFAULT_SOURCE_SEGMENT_TARGET_RESPONSE_BYTES: u64 = 32 * MIB;
const DEFAULT_SOURCE_SEGMENT_MAX_BLOCKS: u32 = 64;
const DEFAULT_SOURCE_FETCH_MAX_IN_FLIGHT_REQUESTS: u32 = 12;
const FALLBACK_SOURCE_FETCH_MAX_IN_FLIGHT_BYTES: u64 = 384 * MIB;
const FALLBACK_BLOCK_PREPARE_MEMORY_WATERMARK_BYTES: u64 = 512 * MIB;
const MIN_PIPELINE_MEMORY_WATERMARK_BYTES: u64 = 128 * MIB;
const PIPELINE_MEMORY_BUDGET_DIVISOR: u64 = 64;
const BLOCK_PREPARE_CONCURRENCY_CEILING: u32 = 16;

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
            block_prepare_concurrency: NonZeroU32::new(
                logical_core_count
                    .get()
                    .min(BLOCK_PREPARE_CONCURRENCY_CEILING),
            )
            .unwrap_or(NonZeroU32::MIN),
            block_prepare_memory_watermark_bytes: pipeline_memory_watermark(
                memory_budget_bytes,
                FALLBACK_BLOCK_PREPARE_MEMORY_WATERMARK_BYTES,
            ),
        }
    }

    /// Returns these explicit limits after validating their cross-field invariants.
    pub fn validate(self) -> Result<Self, CanonicalPipelineLimitsError> {
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
    register_prohibited_read_metrics();
    let build_plan = builder.build_plan().clone();
    validate_construction_identity(&build_plan, config)?;
    let block_queue_capacity =
        usize::try_from(config.pipeline_limits.block_prepare_concurrency.get())
            .unwrap_or(usize::MAX)
            .max(1);
    let prepared_blocks = build_prepared_block_stream(
        source,
        &build_plan,
        config,
        raw_blob_policy_for_retention(builder.raw_blob_retention()),
    );
    drive_block_loader(
        builder,
        prepared_blocks,
        block_queue_capacity,
        Arc::clone(&config.network_upgrade_activations),
    )
    .await
}

/// Loads the exact completed-subtree ranges and authenticates the final fixed tip.
pub async fn load_fresh_canonical_source_families<Source>(
    block_outcome: CanonicalBlockLoadOutcome,
    source: &Source,
    config: &CanonicalConstructionConfig,
) -> Result<CanonicalSourceLoadOutcome, CanonicalConstructionError>
where
    Source: NodeSource,
{
    let CanonicalBlockLoadOutcome {
        mut builder,
        evidence: block_evidence,
    } = block_outcome;
    validate_network_upgrade_activations(
        builder.build_plan(),
        &config.network_upgrade_activations,
    )?;
    let required_ranges = builder.required_subtree_root_ranges()?;
    let mut subtree_roots = Vec::new();
    for range in required_ranges {
        let source_roots = tokio::time::timeout(
            config.request_timeout,
            source.fetch_subtree_root_range(range),
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
    let subtree_root_evidence = builder.load_subtree_roots(subtree_roots)?;
    let fixed_tip = builder.build_plan().build_tip();
    let source_tip_checkpoint = tokio::time::timeout(
        config.request_timeout,
        source.fetch_chain_checkpoint(fixed_tip.height, &config.network_upgrade_activations),
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
    Ok(CanonicalSourceLoadOutcome {
        builder,
        block_evidence,
        subtree_root_evidence,
    })
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
    let block_outcome = load_fresh_canonical_blocks(builder, source, config).await?;
    load_fresh_canonical_source_families(block_outcome, source, config).await
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
    raw_blob_policy: RawBlobPolicy,
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
                prepare_canonical_block(&source_block, &activations, raw_blob_policy)
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

async fn drive_block_loader<PreparedBlocks>(
    builder: RocksDbCanonicalBuilder,
    prepared_blocks: PreparedBlocks,
    block_queue_capacity: usize,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
) -> Result<CanonicalBlockLoadOutcome, CanonicalConstructionError>
where
    PreparedBlocks: Stream<Item = Result<QueuedPreparedBlock, CanonicalConstructionError>> + Send,
{
    let predecessor = builder.build_plan().history_predecessor();
    let predecessor_tip_metadata = predecessor.tip_metadata();
    let commitment_tree_accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        predecessor.block_id.height,
        &predecessor.frontiers,
        &network_upgrade_activations,
    )
    .map_err(|source| CanonicalConstructionError::CommitmentTreeState {
        height: predecessor.block_id.height,
        source,
    })?;
    let workload = builder.workload();
    let build_tip_height = builder.build_plan().build_tip().height;
    let (block_sender, block_receiver) = mpsc::channel(block_queue_capacity);
    let loader_task = tokio::task::spawn_blocking(move || {
        let mut builder = builder;
        let mut build_blocks = CanonicalBuildBlockReceiver::new(
            block_receiver,
            CommitmentTreeSizes::from_tip_metadata(predecessor_tip_metadata),
            commitment_tree_accumulator,
            workload,
            build_tip_height,
        );
        let evidence = builder
            .bulk_load_blocks(&mut build_blocks)
            .map_err(canonical_build_error)?;
        drop(build_blocks);
        Ok(CanonicalBlockLoadOutcome { builder, evidence })
    });

    let mut prepared_blocks = Box::pin(prepared_blocks);
    while let Some(prepared_block) = prepared_blocks.next().await {
        match prepared_block {
            Ok(queued_block) => {
                if block_sender.send(Ok(queued_block)).await.is_err() {
                    break;
                }
            }
            Err(source) => {
                let _ = block_sender.send(Err(source)).await;
                break;
            }
        }
    }
    drop(prepared_blocks);
    drop(block_sender);

    loader_task
        .await
        .map_err(|source| CanonicalConstructionError::LoaderTaskFailed {
            reason: source.to_string(),
        })?
}

struct CanonicalBuildBlockReceiver {
    receiver: mpsc::Receiver<Result<QueuedPreparedBlock, CanonicalConstructionError>>,
    active_memory_permit: Option<OwnedSemaphorePermit>,
    running_tree_sizes: CommitmentTreeSizes,
    commitment_tree_accumulator: CommitmentTreeAccumulator,
    workload: CanonicalStoreWorkload,
    build_tip_height: BlockHeight,
}

impl CanonicalBuildBlockReceiver {
    fn new(
        receiver: mpsc::Receiver<Result<QueuedPreparedBlock, CanonicalConstructionError>>,
        running_tree_sizes: CommitmentTreeSizes,
        commitment_tree_accumulator: CommitmentTreeAccumulator,
        workload: CanonicalStoreWorkload,
        build_tip_height: BlockHeight,
    ) -> Self {
        Self {
            receiver,
            active_memory_permit: None,
            running_tree_sizes,
            commitment_tree_accumulator,
            workload,
            build_tip_height,
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
        let received = self.receiver.blocking_recv()?;
        let queued_block = match received {
            Ok(queued_block) => queued_block,
            Err(source) => return Some(Err(source)),
        };
        self.active_memory_permit = Some(queued_block.memory_permit);
        Some(self.build_block(queued_block.prepared))
    }
}

impl CanonicalBuildBlockReceiver {
    fn build_block(
        &mut self,
        prepared: PreparedCanonicalBlock,
    ) -> Result<CanonicalBuildBlock, CanonicalConstructionError> {
        let height = prepared.facts.block_header.height;
        let block_hash = prepared.facts.block_header.block_hash;
        let block_time_seconds = prepared.partial_compact_block.time();
        let commitments = compact_block_commitments(&prepared)?;
        self.commitment_tree_accumulator
            .append_block_commitments(
                height,
                &commitments.sapling,
                &commitments.orchard,
                &commitments.ironwood,
            )
            .map_err(|source| CanonicalConstructionError::CommitmentTreeState { height, source })?;
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
        let checkpoint_required = height == self.build_tip_height
            || height.value().is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE);
        let tree_state_checkpoint = checkpoint_required
            .then(|| {
                self.commitment_tree_accumulator
                    .validated_frontiers()
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
        let block_final_note_commitment_roots = (self.workload == CanonicalStoreWorkload::Explorer)
            .then(|| {
                self.commitment_tree_accumulator
                    .final_note_commitment_roots(block_hash)
            })
            .filter(|roots| {
                roots.sapling.is_some() || roots.orchard.is_some() || roots.ironwood.is_some()
            });
        Ok(canonical_build_block(
            positioned,
            tree_state_checkpoint,
            block_final_note_commitment_roots,
        ))
    }
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
    use super::CanonicalPipelineLimits;
    use std::num::{NonZeroU32, NonZeroU64};

    const MIB: u64 = 1_024 * 1_024;

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
}
