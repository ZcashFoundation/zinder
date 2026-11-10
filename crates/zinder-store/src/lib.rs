//! Canonical chain storage contracts for Zinder.
//!
//! `zinder-store` exposes domain-shaped commit and read APIs while keeping
//! `RocksDB` handles, column families, and write batches private to the adapter.

mod address_output_index;
mod artifact_visibility;
mod block_artifact;
mod block_hash_index;
mod block_replay;
mod block_value_pool_balances;
mod canonical_store;
mod chain_epoch;
mod chain_epoch_reader;
mod chain_event;
mod chain_store;
mod displaced_block;
mod event_stream;
mod final_note_commitment_roots;
mod format;
mod grpc_status;
mod kv;
mod mempool_event;
mod mempool_event_store;
mod proto_codec;
mod raw_blob_retention;
mod rocksdb_resource_budget;
mod store_error;
mod subtree_root;
mod transaction_artifact;
mod transparent_output;
mod transparent_spend_fact;
mod tree_state;

pub use address_output_index::{
    AddressOutputIndexStore, TransparentAddressBalanceSnapshot, TransparentAddressBalanceSummary,
};
pub use block_artifact::{
    BlockBlobStore, BlockHeaderStore, BlockTransactionIndexStore, CompactBlockStore,
};
pub use block_hash_index::BlockHashLookup;
pub use block_replay::{BlockReplayBatchRequest, BlockReplayStore, MAX_BLOCK_REPLAY_BATCH_BLOCKS};
pub use block_value_pool_balances::BlockValuePoolBalancesStore;
pub use canonical_store::{
    CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION, CANONICAL_STORE_IDENTITY,
    CANONICAL_STORE_SCHEMA_VERSION, CanonicalActivationSchedule, CanonicalAppendAnchor,
    CanonicalBaselinePublication, CanonicalBlockLoadEvidence, CanonicalBuildBlock,
    CanonicalBuildSubtreeRoot, CanonicalConstructionManifestBinding, CanonicalEventCursor,
    CanonicalEventFence, CanonicalEventHistoryRequest, CanonicalEventKind,
    CanonicalEventRetentionReport, CanonicalLiveAppend, CanonicalLiveReplacement,
    CanonicalMempoolSnapshotStart, CanonicalOwnerCheckpointAdmission,
    CanonicalOwnerCheckpointEvidence, CanonicalReorgPolicy, CanonicalReplacementBlock,
    CanonicalReplayRangeScan, CanonicalReplayScan, CanonicalRetainedEvent,
    CanonicalSecondaryCatchupOutcome, CanonicalSequenceCheckpoint, CanonicalStoreBuildError,
    CanonicalStoreBuildPlan, CanonicalStoreBuildPlanError, CanonicalStoreBuildState,
    CanonicalStoreError, CanonicalStoreReadyEvidence, CanonicalStoreWorkload,
    CanonicalSubtreeRootLoadCoverage, CanonicalSubtreeRootLoadEvidence,
    MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS, PreparedCanonicalBaselinePublication,
    ProjectionBuildAnchor, ProjectionBuildLease, ProjectionBuildLeaseId, RocksDbCanonicalBuilder,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore, TREE_STATE_CHECKPOINT_STRIDE,
    ValidatedRocksDbCanonicalBuild,
};
pub use chain_epoch::{ChainEpochArtifacts, ReorgWindowChange};
pub use chain_epoch_reader::ChainEpochReader;
pub use chain_event::{
    ChainEpochCommitOutcome, ChainEpochCommitted, ChainEvent, ChainEventEnvelope,
    ChainRangeReverted,
};
pub use chain_store::{
    AddressOutputIndexPage, AddressOutputIndexPageRequest, BlockValuePoolBalanceEnrichmentOutcome,
    CURRENT_ARTIFACT_SCHEMA_VERSION, CURRENT_STORE_SCHEMA_VERSION, ChainEpochReadApi,
    ChainEventHistoryRequest, ChainEventRetentionReport, ChainStoreOptions,
    DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS, FinalNoteCommitmentRootEnrichmentOutcome,
    MAX_BLOCK_VALUE_POOL_BALANCE_ENRICHMENT_BATCH, MAX_FINAL_NOTE_COMMITMENT_ROOT_ENRICHMENT_BATCH,
    MAX_SUPPORTED_ARTIFACT_SCHEMA_VERSION,
    MAX_TRANSACTION_INTRINSIC_VALUE_BALANCE_ENRICHMENT_BATCH,
    MIN_SUPPORTED_ARTIFACT_SCHEMA_VERSION, PrimaryChainStore, SecondaryCatchupOutcome,
    SecondaryChainStore, TransactionIntrinsicValueBalanceEnrichmentOutcome,
    TransparentRetentionSweepOutcome,
};
pub use displaced_block::{DisplacedBlockCursor, DisplacedBlockPage, DisplacedBlockStore};
pub use event_stream::{
    ChainEventStreamResume, EventEnvelope, EventStreamStartPosition, run_event_stream,
};
pub use final_note_commitment_roots::FinalNoteCommitmentRootsStore;
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
    RocksDbResourceGaugeInputs, StoreReadCaller, StoreRole, build_block_based_table_factory,
    open_bounded_rocksdb, record_rocksdb_resource_gauges,
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
pub use subtree_root::SubtreeRootStore;
pub use transaction_artifact::{
    TransactionBlobStore, TransactionFactsStore, TransactionIntrinsicValueBalancesStore,
    TransactionLocationStore,
};
pub use transparent_spend_fact::TransparentSpendReplayBlock;
pub use tree_state::TreeStateStore;
