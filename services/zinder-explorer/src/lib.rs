//! Explorer-plane runtime serving block-explorer, dashboard, and analytics reads.
//!
//! `zinder-explorer` is the fourth Zinder deployable. It runs in a separate
//! process from `zinder-ingest` and `zinder-query`, reads the ingest-owned
//! materialized-view store in secondary mode, and serves the `ExplorerQuery` gRPC surface.
//!
//! See `docs/architecture/explorer-plane.md` for the product surface and
//! `docs/architecture/materialized-view-plane.md` for the materialized-view store this crate reads.

mod grpc;
pub mod runtime;

pub use grpc::{
    EXPLORER_QUERY_FIELD_LIMITS, ExplorerQueryGrpcAdapter, ExplorerServerInfoSettings,
    describe_request_metrics,
};
pub use zinder_materialized_views::{
    BLOCK_SUMMARY_CAPABILITIES, BLOCK_SUMMARY_COLUMN_FAMILY, BLOCK_SUMMARY_CONSUMER_NAME,
    BlockCommitContext, BlockKeyedConsumer, BlockSummaryConsumer, BlockSummaryConsumerError,
    ChainCommittedEvent, ChainReorgedEvent, CommittedRange, ConsumerEntry,
    MATERIALIZED_VIEW_STORE_FORMAT_VERSION, MEMPOOL_EVENT_COUNTS_COLUMN_FAMILY,
    MEMPOOL_EVENT_COUNTS_CONSUMER_NAME, MEMPOOL_EVENT_COUNTS_RETENTION_SECONDS,
    MaterializedViewConsumer, MaterializedViewConsumerCtx, MaterializedViewConsumerError,
    MaterializedViewConsumerName, MaterializedViewCursorEntry, MaterializedViewError,
    MaterializedViewMempoolConsumer, MaterializedViewStore, MaterializedViewStoreColumnFamily,
    MaterializedViewStoreError, MaterializedViewStoreOptions, MaterializedViewStoreTable,
    MempoolConsumerEvent, MempoolConsumerEventVariant, MempoolEventCountsConsumer, RevertedRange,
    TRANSACTION_FEES_COLUMN_FAMILIES, TRANSACTION_FEES_COLUMN_FAMILY,
    TRANSACTION_FEES_CONSUMER_NAME, TRANSACTION_FEES_INDEX_COLUMN_FAMILY,
    TRANSACTION_HISTORY_COLUMN_FAMILY, TRANSACTION_HISTORY_CONSUMER_NAME,
    TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILIES, TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_ACTIVITY_CONSUMER_NAME, TRANSPARENT_ADDRESS_ACTIVITY_INDEX_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_ACTIVITY_KEY_LEN, TransactionFeesConsumer, TransactionFeesConsumerError,
    TransactionHistoryConsumer, TransactionHistoryConsumerError,
    TransparentAddressActivityConsumer, TransparentAddressActivityConsumerError,
    TransparentSpendFacts, decode_stored_record,
};
