//! Ingestion artifact builders and bulk catchup operations for Zinder.
//!
//! This crate owns deterministic conversion from upstream node source values into
//! canonical artifacts. Node I/O belongs to `zinder-source`; durable writes
//! belong to `zinder-store`.

mod artifact_builder;
pub mod bench_support;
mod bulk_catchup;
mod canonical_block_context;
mod chain_ingest;
mod conventional_fee_distribution_backfill;
mod materialized_view_replay;
mod memory_pressure;
mod mempool;
mod phase;
mod runtime_config;
mod source_recovery;
mod tip_follow;
mod transaction_component_backfill;
mod upstream_health_probe;
mod writer;

pub use artifact_builder::{
    BlockMismatchField, CanonicalBlockConstructionError, CommitmentTreeSizes,
    PositionedCanonicalBlock, PreparedCanonicalBlock, RawBlobPolicy, RetainedRawBlobs,
    position_canonical_block, prepare_canonical_block,
};
pub use bulk_catchup::{
    BulkCatchupRunConfig, run_bulk_catchup, run_bulk_catchup_until_complete,
    run_bulk_catchup_with_store,
};
pub use canonical_block_context::{CanonicalBlockContextReader, require_genesis_complete_history};
pub use chain_ingest::{
    DEFAULT_CANONICAL_BATCH_MAX_ESTIMATED_WRITE_BYTES,
    DEFAULT_CANONICAL_BATCH_MIN_BLOCKS_BEFORE_ESTIMATED_WRITE_CLOSE, IngestError, NodeSourceKind,
};
pub use conventional_fee_distribution_backfill::{
    ConventionalFeeDistributionBackfillConfig, ConventionalFeeDistributionBackfillContext,
    spawn_conventional_fee_distribution_backfill_task,
};
pub use materialized_view_replay::{
    DEFAULT_MATERIALIZED_VIEW_TAILER_POLL_INTERVAL, MaterializedViewTailer,
    open_primary_materialized_view_store, seed_backfill_owned_consumer_cursors,
    spawn_materialized_view_replay_budget_metrics_task, spawn_materialized_view_tailer_task,
};
pub use memory_pressure::{
    DEFAULT_RUNTIME_MEMORY_METRICS_INTERVAL, spawn_runtime_memory_metrics_task,
};
pub use mempool::{
    DEFAULT_RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES, LiveMempoolOwner,
    MempoolApplyOutcome, MempoolEntryBuildError, MempoolIndex, MempoolReadyGate,
    MempoolReadySignal, MempoolRetentionSettings, MempoolSnapshotPage, build_mempool_entry,
    mempool_ready_channel, run_live_mempool_owner, run_mempool_retention,
};
pub use phase::{classify_phase, current_chain_height};
pub use runtime_config::{
    CanonicalConstructionSettings, CanonicalFollowSettings, CanonicalRunOverrides,
    HistoricalWorkGate, IngestRuntimeConfig, MaterializedViewReplayConfig,
    MaterializedViewReplayPolicy, MempoolIngestSettings, PhaseClassificationConfig,
};
pub use tip_follow::{
    DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS, TipFollowConfig, open_tip_follow_store, tip_follow,
    tip_follow_with_primary_store,
};
pub use transaction_component_backfill::{
    TransactionComponentBackfillConfig, TransactionComponentBackfillContext,
    spawn_transaction_component_backfill_task,
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
    DEFAULT_MAX_REPLACEMENT_BYTES, follow_canonical_tip, follow_canonical_tip_with_control,
};
pub use writer::ingest_control::{
    CanonicalIngestControlGrpcAdapter,
    MAX_MEMPOOL_SNAPSHOT_PAGE_SIZE as CANONICAL_WRITER_MAX_MEMPOOL_SNAPSHOT_PAGE_SIZE,
};
pub use writer::{
    CanonicalWriterConfig, CanonicalWriterError, run_canonical_writer,
    run_canonical_writer_with_control,
};
pub use zinder_runtime::container_memory_budget_bytes;
