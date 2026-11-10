//! Canonical chain storage contracts for Zinder.
//!
//! `zinder-store` exposes domain-shaped commit and read APIs while keeping
//! `RocksDB` handles, column families, and write batches private to the adapter.

mod block_hash_index;
mod canonical_store;
mod chain_event;
mod event_stream;
mod format;
mod grpc_status;
mod kv;
mod mempool_event;
mod mempool_event_store;
mod proto_codec;
mod raw_blob_retention;
mod rocksdb_resource_budget;
mod store_error;

pub use block_hash_index::BlockHashLookup;
pub use canonical_store::{
    CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION, CANONICAL_STORE_IDENTITY,
    CANONICAL_STORE_SCHEMA_VERSION, CURRENT_ARTIFACT_SCHEMA_VERSION, CanonicalActivationSchedule,
    CanonicalAppendAnchor, CanonicalBaselinePublication, CanonicalBlockLoadEvidence,
    CanonicalBuildBlock, CanonicalBuildSubtreeRoot, CanonicalConstructionManifestBinding,
    CanonicalEventCursor, CanonicalEventFence, CanonicalEventHistoryRequest, CanonicalEventKind,
    CanonicalEventRetentionReport, CanonicalLiveAppend, CanonicalLiveReplacement,
    CanonicalMempoolSnapshotStart, CanonicalOwnerCheckpointAdmission,
    CanonicalOwnerCheckpointEvidence, CanonicalReorgPolicy, CanonicalReplacementBlock,
    CanonicalReplayRangeScan, CanonicalReplayScan, CanonicalRetainedEvent,
    CanonicalSecondaryCatchupOutcome, CanonicalSequenceCheckpoint, CanonicalStoreBuildError,
    CanonicalStoreBuildPlan, CanonicalStoreBuildPlanError, CanonicalStoreBuildState,
    CanonicalStoreError, CanonicalStoreReadyEvidence, CanonicalStoreWorkload,
    CanonicalSubtreeRootLoadCoverage, CanonicalSubtreeRootLoadEvidence,
    DEFAULT_REORG_WINDOW_BLOCKS, DisplacedBlockCursor, DisplacedBlockPage,
    MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS, PreparedCanonicalBaselinePublication,
    ProjectionBuildAnchor, ProjectionBuildLease, ProjectionBuildLeaseId, RocksDbCanonicalBuilder,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore, TREE_STATE_CHECKPOINT_STRIDE,
    ValidatedRocksDbCanonicalBuild,
};
pub use chain_event::{
    ChainEpochCommitted, ChainEvent, ChainEventEnvelope, ChainEventHistoryRequest,
    ChainRangeReverted, DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS,
};
pub use event_stream::{
    ChainEventStreamResume, EventEnvelope, EventStreamStartPosition, run_event_stream,
};
pub use format::{
    AddressOutputCursorPayload, ChainEventStreamFamily, MempoolEventCursorPayload,
    STREAM_CURSOR_TOKEN_V1_LEN, SnapshotPageCursorAnchor, SnapshotPageCursorPayload,
    StreamCursorError, StreamCursorTokenV1,
};
pub use grpc_status::{
    chain_event_stream_family_from_request, event_stream_start_from_request,
    status_from_store_error,
};
pub use kv::{
    BoundedRocksDbOpen, ResourceGaugeThrottle, RocksDbIoMode, RocksDbOpenRole,
    RocksDbResourceGaugeInputs, StoreRole, build_block_based_table_factory, open_bounded_rocksdb,
    record_rocksdb_resource_gauges,
};
pub use mempool_event::{MempoolEvent, MempoolEventEnvelope, MempoolEventPosition};
pub use mempool_event_store::{
    DEFAULT_MAX_MEMPOOL_EVENT_HISTORY_EVENTS, MempoolEventHistoryRequest,
    MempoolEventRetentionConfig, MempoolEventRetentionReport, MempoolEventRetentionStepBudget,
    MempoolEventRetentionStepOutcome, MempoolEventRetentionStepStop,
};
pub use proto_codec::{
    ChainEventEncodeError, MempoolDecodeError, block_tip_message, chain_epoch_from_message,
    chain_epoch_message, chain_event_envelope_message, chain_event_stream_family_from_message,
    chain_view_message, compact_block_from_message, compact_block_message,
    decode_compact_block_artifact, encode_compact_block_artifact, event_stream_start_from_message,
    event_stream_start_message, mempool_entry_from_message, mempool_entry_message,
    mempool_event_envelope_from_message, mempool_event_envelope_message, outpoint_from_message,
    outpoint_message, stream_cursor_from_message_bytes, transparent_mempool_output_from_message,
    transparent_mempool_output_message, transparent_mempool_spend_from_message,
    transparent_mempool_spend_message, transparent_output_entry_message,
    transparent_output_message, transparent_spend_message,
};
pub use raw_blob_retention::RawBlobRetention;
pub use rocksdb_resource_budget::{RocksDbResourceBudget, RocksDbStatisticsLevel};
pub use store_error::{ArtifactFamily, StorageErrorKind, StorageKey, StoreError};
