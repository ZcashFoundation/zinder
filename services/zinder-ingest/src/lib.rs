//! Canonical writer, artifact builders, and ingest-side workers for Zinder.
//!
//! This crate owns deterministic conversion from upstream node source values into
//! canonical artifacts and the writer that constructs and follows the canonical
//! store. Node I/O belongs to `zinder-source`; durable writes belong to
//! `zinder-store`.

mod artifact_builder;
mod background_pass;
mod canonical_block_context;
mod chain_ingest;
mod conventional_fee_distribution_backfill;
mod materialized_view_replay;
mod memory_pressure;
mod mempool;
mod node_failover;
mod phase;
mod runtime_config;
mod source_recovery;
mod transaction_component_backfill;
mod tree_state_densification;
mod tree_state_verification;
mod upstream_health_probe;
mod writer;

pub use artifact_builder::{
    BlockMismatchField, CanonicalBlockConstructionError, CommitmentTreeSizes,
    PositionedCanonicalBlock, PreparedCanonicalBlock, RawBlobPolicy, RetainedRawBlobs,
    position_canonical_block, prepare_canonical_block,
};
pub use canonical_block_context::{
    CanonicalBlockContextReader, PrevoutCache, require_genesis_complete_history,
};
pub use chain_ingest::{IngestError, NodeSourceKind};
pub use conventional_fee_distribution_backfill::{
    ConventionalFeeDistributionBackfillConfig, ConventionalFeeDistributionBackfillContext,
    spawn_conventional_fee_distribution_backfill_task,
};
pub use materialized_view_replay::{
    DEFAULT_EXPLORER_VIEWS_MIN_FREE_BYTES, DEFAULT_MATERIALIZED_VIEW_TAILER_POLL_INTERVAL,
    MaterializedViewTailer, ReplayPrevoutCaches, TransparentAddressRankingBuildStep,
    VolumeSpaceGuard, open_primary_materialized_view_store, seed_backfill_owned_consumer_cursors,
    spawn_materialized_view_replay_budget_metrics_task, spawn_materialized_view_tailer_task,
    spawn_undo_index_pruning_task,
};
pub use memory_pressure::{
    DEFAULT_RUNTIME_MEMORY_METRICS_INTERVAL, spawn_runtime_memory_metrics_task,
};
pub use mempool::{
    DEFAULT_RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES, LiveMempoolOwner,
    MempoolApplyOutcome, MempoolEntryBuildError, MempoolIndex, MempoolReadyGate,
    MempoolReadySignal, MempoolRetentionSettings, MempoolSnapshotPage, ObservedNodeEndpoint,
    build_mempool_entry, mempool_ready_channel, run_live_mempool_owner, run_mempool_retention,
};
pub use node_failover::{
    AdmittedEndpoint, EndpointAdmission, EndpointExclusion, FailoverReadiness, IneligibleReason,
    NoNodeEndpointAdmitted, NodeEndpointAdmissions, NodeEndpointMonitor, NodeEndpointState,
    NodeEndpointSwitch, NodeFailover, NodeFailoverPolicy, REQUIRED_INGEST_NODE_CAPABILITIES,
    check_single_endpoint_genesis, missing_required_capability, spawn_node_endpoint_monitor,
};
pub use phase::classify_phase;
pub use runtime_config::{
    CanonicalConstructionSettings, CanonicalFollowSettings, CanonicalRunOverrides,
    DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS, HistoricalWorkGate, IngestRuntimeConfig,
    MaterializedViewReplayConfig, MaterializedViewReplayPolicy, MempoolIngestSettings,
    PhaseClassificationConfig,
};
pub use transaction_component_backfill::{
    TransactionComponentBackfillConfig, TransactionComponentBackfillContext,
    spawn_transaction_component_backfill_task,
};
pub use tree_state_densification::{
    DEFAULT_TREE_STATE_COMMITMENT_BUDGET, DEFAULT_TREE_STATE_DENSIFICATION_MAX_BLOCKS_PER_SECOND,
    DEFAULT_TREE_STATE_DENSIFICATION_WORKERS, TREE_STATE_DENSIFICATION_WATERMARK_FILE,
    TreeStateDensificationConfig, TreeStateDensificationContext, TreeStateStrideWriteError,
    TreeStateStrideWriter, spawn_tree_state_densification_task,
};
pub use tree_state_verification::{
    CanonicalStoreDamage, CanonicalStoreDamageSignal, CanonicalStoreDamageSite,
    DEFAULT_TREE_STATE_VERIFICATION_MAX_ROWS_PER_SECOND, TREE_STATE_VERIFICATION_WATERMARK_FILE,
    TreeStateVerificationConfig, TreeStateVerificationContext, spawn_tree_state_verification_task,
};
pub use upstream_health_probe::spawn_upstream_health_probe_task;
pub use writer::construction::{
    CanonicalBlockLoadOutcome, CanonicalConstructionConfig, CanonicalConstructionError,
    CanonicalPipelineLimits, CanonicalPipelineLimitsError, CanonicalSourceLoadOutcome,
    load_fresh_canonical, load_fresh_canonical_blocks, load_fresh_canonical_source_families,
};
pub use writer::control::{
    CANONICAL_CONTROL_COMMAND_CAPACITY, CanonicalCheckpointStagingRoot, CanonicalControlCommand,
    CanonicalControlGrpcAdapter, CanonicalControlHandle, canonical_control_channel,
};
pub use writer::follow::{
    CanonicalFollowConfig, CanonicalFollowError, CanonicalFollower, CanonicalReorgWindowExceeded,
    CheckpointComponent, DEFAULT_MAX_REPLACEMENT_BYTES, follow_canonical_tip,
    follow_canonical_tip_with_control,
};
pub use writer::ingest_control::{
    CanonicalIngestControlGrpcAdapter,
    MAX_MEMPOOL_SNAPSHOT_PAGE_SIZE as CANONICAL_WRITER_MAX_MEMPOOL_SNAPSHOT_PAGE_SIZE,
};
pub use writer::{
    CanonicalWriterConfig, CanonicalWriterError, admit_canonical_store,
    follow_admitted_canonical_store, read_ready_store_summary, run_canonical_writer,
    run_canonical_writer_with_control,
};
pub use zinder_runtime::container_memory_budget_bytes;
