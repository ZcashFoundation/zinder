//! Canonical chain storage contracts for Zinder.
//!
//! `zinder-store` exposes domain-shaped commit and read APIs while keeping
//! `RocksDB` handles, column families, and write batches private to the adapter.

mod block_hash_index;
mod canonical_store;
pub use canonical_store::edge_snapshot;
mod chain_event;
mod event_stream;
mod format;
mod grpc_status;
mod kv;
mod mempool_event;
mod mempool_event_store;
mod positional_source;
mod proto_codec;
mod raw_blob_retention;
mod rocksdb_resource_budget;
mod segment_verity;
mod settled_raw_block_segment;
mod settled_shielded_segment;
mod settled_supplement_segment;
mod store_error;

pub use block_hash_index::BlockHashLookup;
pub use canonical_store::settled_segments::layout::{
    RAW_BLOCK_SEGMENT_DIR, RAW_BLOCK_SEGMENT_EXT, SEGMENTS_DIR_NAME, SEGMENTS_MANIFEST_FILE_NAME,
    SEGMENTS_STAGING_DIR_NAME, SHIELDED_SEGMENT_DIR, SHIELDED_SEGMENT_EXT, SUPPLEMENT_SEGMENT_DIR,
    SUPPLEMENT_SEGMENT_EXT, validate_settled_segment_filename,
};
pub use canonical_store::{
    ArchivedDisplacedBlockSummary, CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION,
    CANONICAL_STORE_IDENTITY, CANONICAL_STORE_SCHEMA_VERSION, CURRENT_ARTIFACT_SCHEMA_VERSION,
    CanonicalActivationSchedule, CanonicalAppendAnchor, CanonicalBaselinePublication,
    CanonicalBlockLoadEvidence, CanonicalBuildBlock, CanonicalBuildSubtreeRoot,
    CanonicalConstructionManifestBinding, CanonicalEventCursor, CanonicalEventFence,
    CanonicalEventHistoryRequest, CanonicalEventKind, CanonicalEventRetentionReport,
    CanonicalLiveAppend, CanonicalLiveReplacement, CanonicalMempoolSnapshotStart,
    CanonicalOwnerCheckpointAdmission, CanonicalOwnerCheckpointEvidence,
    CanonicalReadyStoreSummary, CanonicalReorgPolicy, CanonicalReplacementBlock,
    CanonicalReplayRangeScan, CanonicalReplayScan, CanonicalReplaySliceScan,
    CanonicalReplaySliceSource, CanonicalRetainedEvent, CanonicalSecondaryCatchupOutcome,
    CanonicalSequenceCheckpoint, CanonicalStoreBuildError, CanonicalStoreBuildPlan,
    CanonicalStoreBuildPlanError, CanonicalStoreBuildState, CanonicalStoreError,
    CanonicalStoreReadyEvidence, CanonicalStoreRequirements, CanonicalStoredData,
    CanonicalStoredSet, CanonicalSubtreeRootLoadCoverage, CanonicalSubtreeRootLoadEvidence,
    DEFAULT_REORG_WINDOW_BLOCKS, DensifiedTreeStateStride, DisplacedBlockCursor,
    DisplacedBlockPage, DisplacedBlockSummaryPage, DisplacedFinalRootPage,
    DisplacedFinalRootSearch, DisplacedRootCursor, HeightOrder, InadmissibleActivationSchedule,
    LocatedTransactionBlob, MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS,
    MAX_TREE_STATE_STRIDE_SCAN_READS, ObservedCanonicalReplaySequence,
    PreparedCanonicalBaselinePublication, ProjectionBuildAnchor, ProjectionBuildLease,
    ProjectionBuildLeaseId, RocksDbCanonicalBuilder, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore, RocksDbTreeStateVerifier, ServedTreeState, TREE_STATE_CHECKPOINT_STRIDE,
    TreeStateCheckpointMismatch, TreeStateCheckpointMismatchKind,
    TreeStateCheckpointRowVerification, TreeStateCheckpointSelection,
    TreeStateCheckpointVerification, TreeStateLookup, TreeStateReplay, TreeStateReplayOutcome,
    TreeStateStride, TreeStateStrideCommit, TreeStateStrideScan, TreeStateVerificationError,
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
    SNAPSHOT_PAGE_CURSOR_LEN, STREAM_CURSOR_TOKEN_V1_LEN, SnapshotPageCursorAnchor,
    SnapshotPageCursorPayload, StreamCursorError, StreamCursorTokenV1,
};
pub use grpc_status::{
    chain_event_stream_family_from_request, event_stream_start_from_request,
    status_from_store_error,
};
pub use kv::{
    BoundedRocksDbOpen, ResourceGaugeThrottle, RocksDbInstanceMemory, RocksDbIoMode,
    RocksDbOpenResources, RocksDbOpenRole, RocksDbResourceGaugeInputs, SharedBlockCache,
    SharedSecondaryResources, StoreRole, build_block_based_table_factory,
    build_point_lookup_block_based_table_factory, is_missing_table_file_error,
    keep_blocks_of_dropped_table_files, open_bounded_rocksdb, open_bounded_rocksdb_read_only_probe,
    record_instance_memory_gauges, record_rocksdb_resource_gauges,
    record_shared_block_cache_gauges,
};
pub use mempool_event::{MempoolEvent, MempoolEventEnvelope, MempoolEventPosition};
pub use mempool_event_store::{
    DEFAULT_MAX_MEMPOOL_EVENT_HISTORY_EVENTS, MempoolEventHistoryRequest,
    MempoolEventRetentionConfig, MempoolEventRetentionReport, MempoolEventRetentionStepBudget,
    MempoolEventRetentionStepOutcome, MempoolEventRetentionStepStop,
};
pub use positional_source::{ByteRange, PositionalByteSource};
pub use proto_codec::{
    ChainEventEncodeError, MempoolDecodeError, block_tip_message, chain_epoch_from_message,
    chain_epoch_message, chain_event_envelope_message, chain_event_stream_family_from_message,
    chain_view_message, compact_block_from_message, compact_block_message,
    decode_compact_block_artifact, encode_compact_block_artifact, event_stream_start_from_message,
    event_stream_start_message, mempool_entry_from_message, mempool_entry_message,
    mempool_event_envelope_from_message, mempool_event_envelope_message, outpoint_message,
    stream_cursor_from_message_bytes, transparent_spend_message,
};
pub use raw_blob_retention::RawBlobRetention;
pub use rocksdb_resource_budget::{RocksDbResourceBudget, RocksDbStatisticsLevel};
pub use settled_raw_block_segment::{
    DEFAULT_SETTLED_RAW_BLOCK_SEGMENT_MAX_SLAB_BYTES, RawBlockCodec, RawBlockReadPlan,
    RawBlockRowIndexSlice, RawBlockSegmentHeader, RawBlockSegmentIdentity,
    RawBlockTransactionSlice, RawBlockTxDigestsPlan, RawBlockTxTableSlice, RawTransactionReadPlan,
    SETTLED_RAW_BLOCK_SEGMENT_MAGIC, SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES,
    SettledRawBlockSegmentError, SettledRawBlockSegmentIndex, SettledRawBlockSegmentOpenOptions,
    SettledRawBlockSegmentReader, SettledRawBlockSegmentWriter, VerifiedRawBlockRowIndexSlice,
    VerifiedRawBlockTxTableSlice,
};
pub use settled_shielded_segment::{
    BoundedShieldedRangePlan, DEFAULT_SETTLED_SHIELDED_SEGMENT_MAX_SLAB_BYTES, PoolPresenceMask,
    SEGMENT_FLAG_RESERVED_SHIFTED_EPK, SETTLED_SHIELDED_SEGMENT_MAGIC,
    SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES, SettledShieldedSegmentError,
    SettledShieldedSegmentIndex, SettledShieldedSegmentOpenOptions, SettledShieldedSegmentReader,
    SettledShieldedSegmentWriter, ShieldedBatchReadRequest, ShieldedBoundedBatchReadRequest,
    ShieldedFetchedSlab, ShieldedIndexSlice, ShieldedProjectionSelection, ShieldedRangeByteBudget,
    ShieldedRangePlan, ShieldedReadScratch, ShieldedReadStep, ShieldedSegmentHeader,
    ShieldedSegmentIdentity, ShieldedSlabDecodeRequest, VerifiedShieldedIndexSlice,
    is_passthrough_shielded_block_wire, project_shielded_block_wire,
    project_shielded_block_wire_with_scratch,
};
pub use settled_supplement_segment::{
    BlockSupplementRecord, CanonicalBlockSupplement, CanonicalTransactionSupplement,
    CompactBlockSpliceSelection, CompactTransparentSupplementTx,
    DEFAULT_SETTLED_SUPPLEMENT_SEGMENT_MAX_BYTES, ExplicitTransparentFacts,
    SETTLED_SUPPLEMENT_DEFAULT_CHUNK_BLOCKS, SETTLED_SUPPLEMENT_MAX_CHUNK_BLOCKS,
    SETTLED_SUPPLEMENT_SEGMENT_MAGIC, SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES,
    SettledSupplementSegmentError, SettledSupplementSegmentIndex,
    SettledSupplementSegmentOpenOptions, SettledSupplementSegmentReader,
    SettledSupplementSegmentWriteOptions, SettledSupplementSegmentWriter, SupplementChunkReadStep,
    SupplementCompressionCodec, SupplementRangePlan, SupplementRowIndexSlice,
    SupplementSegmentHeader, SupplementSegmentIdentity, SupplementShieldedAnchors,
    SupplementSpliceChunkStep, SupplementSplicePlan, SupplementTxFeeAndBurn,
    VerifiedSupplementRowIndexSlice, reconstruct_canonical_block_facts,
    reconstruct_compact_block_artifact, splice_compact_block_wire,
};
pub use store_error::{ArtifactFamily, StorageKey, StoreError};
