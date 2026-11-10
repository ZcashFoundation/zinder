//! Materialized-view plane SDK.
//!
//! Reusable abstractions for building consumers that receive canonical chain
//! or mempool events from `zinder-ingest`, project query state from each event,
//! and persist the result through [`MaterializedViewStore`] atomically with
//! their cursor.
//!
//! - [`MaterializedViewConsumer`] is the seam between consumer-agnostic infrastructure
//!   (the writer scheduler, the store, and parsed block contexts) and the
//!   consumer-specific aggregation logic each impl owns.
//! - [`BlockKeyedConsumer`] is the per-block convention every production
//!   chain-events consumer follows; the in-process dispatch helpers walk the
//!   height range from each event and dispatch per-height work.
//! - [`bundled_materialized_view_consumer_definitions`] is the closed registry
//!   of bundled consumers. Store presets, chain-event cursor lists, and the
//!   replay host's dispatch sets derive from it.
//! - [`MaterializedViewStore`] is the `RocksDB` wrapper consumers write through. It
//!   shares its option layering with `zinder_store`'s canonical store so the
//!   bulk-catchup-OOM bound from [ADR-0020] applies to both planes.
//!
//! See `docs/architecture/materialized-view-plane.md` for the SDK pattern and
//! `docs/adrs/0017-materialized-view-consumer-and-key-codec.md` for the trait
//! and codec convention.

pub mod consumer;
mod consumer_registry;
pub mod error;
pub mod store;
pub mod value_pool_change;

pub use consumer::block_production_time::{
    BLOCK_PRODUCTION_TIME_COLUMN_FAMILIES, BLOCK_PRODUCTION_TIME_COLUMN_FAMILY,
    BLOCK_PRODUCTION_TIME_CONSUMER_NAME, BLOCK_PRODUCTION_TIME_COVERAGE_COLUMN_FAMILY,
    BLOCK_PRODUCTION_TIME_INDEX_COLUMN_FAMILY, BLOCK_PRODUCTION_TIME_MAX_PAGE_SIZE,
    BLOCK_PRODUCTION_TIME_SCHEMA, BlockProductionTimeBackfillCoverage, BlockProductionTimeConsumer,
    BlockProductionTimeConsumerError, BlockProductionTimeCursor, BlockProductionTimePage,
    BlockProductionTimePageRequest, BlockProductionTimeRow, BlockProductionTimeTailCoverage,
};
pub use consumer::block_summary::{
    BLOCK_SUMMARY_CAPABILITIES, BLOCK_SUMMARY_COLUMN_FAMILY, BLOCK_SUMMARY_CONSUMER_NAME,
    BLOCK_SUMMARY_SCHEMA, BlockSummaryConsumer, BlockSummaryConsumerError, decode_stored_record,
    project_block_summary_record,
};
pub use consumer::commitment_root_search::{
    COMMITMENT_ROOT_SEARCH_COLUMN_FAMILIES, COMMITMENT_ROOT_SEARCH_COLUMN_FAMILY,
    COMMITMENT_ROOT_SEARCH_CONSUMER_NAME, COMMITMENT_ROOT_SEARCH_COVERAGE_COLUMN_FAMILY,
    COMMITMENT_ROOT_SEARCH_INDEX_COLUMN_FAMILY, COMMITMENT_ROOT_SEARCH_SCHEMA,
    CommitmentRootBackfillCoverage, CommitmentRootIndexEntry, CommitmentRootSearchConsumer,
    CommitmentRootSearchConsumerError,
};
pub use consumer::conventional_fee_distribution::{
    CONVENTIONAL_FEE_DISTRIBUTION_CAPABILITIES, CONVENTIONAL_FEE_DISTRIBUTION_COLUMN_FAMILIES,
    CONVENTIONAL_FEE_DISTRIBUTION_COLUMN_FAMILY, CONVENTIONAL_FEE_DISTRIBUTION_CONSUMER_NAME,
    CONVENTIONAL_FEE_DISTRIBUTION_COVERAGE_COLUMN_FAMILY,
    CONVENTIONAL_FEE_DISTRIBUTION_DAY_COLUMN_FAMILY,
    CONVENTIONAL_FEE_DISTRIBUTION_INDEX_COLUMN_FAMILY, CONVENTIONAL_FEE_DISTRIBUTION_SCHEMA,
    ConventionalFeeDistribution, ConventionalFeeDistributionBackfillCoverage,
    ConventionalFeeDistributionConsumer, ConventionalFeeDistributionConsumerError,
    ConventionalFeeDistributionDay, ConventionalFeeDistributionTailCoverage,
    ConventionalFeeFrequency,
};
pub use consumer::ironwood_migration::{
    IRONWOOD_MIGRATION_COLUMN_FAMILIES, IRONWOOD_MIGRATION_CONSUMER_NAME,
    IRONWOOD_MIGRATION_POOL_TOTALS_COLUMN_FAMILY, IRONWOOD_MIGRATION_SCHEMA,
    IRONWOOD_MIGRATIONS_COLUMN_FAMILY, IronwoodMigrationConsumer, IronwoodMigrationConsumerError,
    Migration, MigrationPoolTotals,
};
pub use consumer::mempool_event_counts::{
    MEMPOOL_EVENT_COUNTS_COLUMN_FAMILY, MEMPOOL_EVENT_COUNTS_CONSUMER_NAME,
    MEMPOOL_EVENT_COUNTS_RETENTION_SECONDS, MEMPOOL_EVENT_COUNTS_SCHEMA,
    MempoolEventCountsConsumer,
};
pub use consumer::paid_fee_distribution::{
    PAID_FEE_DISTRIBUTION_CAPABILITIES, PAID_FEE_DISTRIBUTION_COLUMN_FAMILIES,
    PAID_FEE_DISTRIBUTION_COLUMN_FAMILY, PAID_FEE_DISTRIBUTION_CONSUMER_NAME,
    PAID_FEE_DISTRIBUTION_COVERAGE_COLUMN_FAMILY, PAID_FEE_DISTRIBUTION_DAY_COLUMN_FAMILY,
    PAID_FEE_DISTRIBUTION_INDEX_COLUMN_FAMILY, PAID_FEE_DISTRIBUTION_SCHEMA, PaidFeeBlockTotal,
    PaidFeeDistribution, PaidFeeDistributionBackfillCoverage, PaidFeeDistributionConsumer,
    PaidFeeDistributionConsumerError, PaidFeeDistributionDay, PaidFeeDistributionTailCoverage,
    PaidFeeFrequency,
};
pub use consumer::recent_transactions::{
    RECENT_TRANSACTIONS_COLUMN_FAMILY, RECENT_TRANSACTIONS_CONSUMER_NAME,
    RECENT_TRANSACTIONS_SCHEMA, RecentTransactionsConsumer, RecentTransactionsConsumerError,
};
pub use consumer::reorg_incidents::{
    REORG_INCIDENTS_COLUMN_FAMILY, REORG_INCIDENTS_CONSUMER_NAME, REORG_INCIDENTS_KEY_LEN,
    REORG_INCIDENTS_SCHEMA, ReorgIncidentsConsumer, ReorgIncidentsConsumerError,
};
pub use consumer::transaction_component_summary::{
    TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILIES, TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILY,
    TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME,
    TRANSACTION_COMPONENT_SUMMARY_COVERAGE_COLUMN_FAMILY,
    TRANSACTION_COMPONENT_SUMMARY_DAY_COLUMN_FAMILY,
    TRANSACTION_COMPONENT_SUMMARY_INDEX_COLUMN_FAMILY, TRANSACTION_COMPONENT_SUMMARY_SCHEMA,
    TransactionComponentBackfillCoverage, TransactionComponentDay, TransactionComponentSummary,
    TransactionComponentSummaryConsumer, TransactionComponentSummaryConsumerError,
    TransactionComponentTailCoverage, TransactionComponentTotals,
};
pub use consumer::transaction_fees::{
    TRANSACTION_FEES_COLUMN_FAMILIES, TRANSACTION_FEES_COLUMN_FAMILY,
    TRANSACTION_FEES_CONSUMER_NAME, TRANSACTION_FEES_INDEX_COLUMN_FAMILY, TRANSACTION_FEES_SCHEMA,
    TransactionFeesConsumer, TransactionFeesConsumerError,
};
pub use consumer::transaction_history::{
    TRANSACTION_HISTORY_COLUMN_FAMILY, TRANSACTION_HISTORY_CONSUMER_NAME,
    TRANSACTION_HISTORY_KEY_LEN, TRANSACTION_HISTORY_SCHEMA, TransactionHistoryConsumer,
    TransactionHistoryConsumerError,
};
pub use consumer::transparent_address_activity::{
    TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILIES, TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_ACTIVITY_CONSUMER_NAME, TRANSPARENT_ADDRESS_ACTIVITY_INDEX_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_ACTIVITY_KEY_LEN, TRANSPARENT_ADDRESS_ACTIVITY_SCHEMA,
    TransparentAddressActivityConsumer, TransparentAddressActivityConsumerError,
};
pub use consumer::transparent_address_deltas::{
    TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILIES, TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_DELTAS_CONSUMER_NAME, TRANSPARENT_ADDRESS_DELTAS_INDEX_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_DELTAS_KEY_LEN, TRANSPARENT_ADDRESS_DELTAS_SCHEMA,
    TransparentAddressDeltasConsumer, TransparentAddressDeltasConsumerError,
    TransparentAddressDeltasLifetimeBootstrap, TransparentAddressDeltasSourceCoverage,
    TransparentAddressLifetimeSummary,
};
pub use consumer::transparent_address_ranking::{
    TRANSPARENT_ADDRESS_RANKING_COLUMN_FAMILIES, TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME,
    TRANSPARENT_ADDRESS_RANKING_INDEX_COLUMN_FAMILY, TRANSPARENT_ADDRESS_RANKING_MAX_PAGE_SIZE,
    TRANSPARENT_ADDRESS_RANKING_METADATA_COLUMN_FAMILY, TRANSPARENT_ADDRESS_RANKING_SCHEMA,
    TRANSPARENT_ADDRESS_RANKING_SUMMARY_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_RANKING_UNDO_COLUMN_FAMILY, TransparentAddressRankingConsumer,
    TransparentAddressRankingConsumerError, TransparentAddressRankingCoverage,
    TransparentAddressRankingEntry, TransparentAddressRankingMetadata,
    TransparentAddressRankingPage, TransparentAddressRankingSnapshotPlan,
    TransparentAddressRankingSnapshotRow, TransparentAddressScriptTypeTotals,
    TransparentAddressSummary,
};
pub use consumer::transparent_address_transaction_history::{
    TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_COLUMN_FAMILIES,
    TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_CONSUMER_NAME,
    TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_DESCENDING_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_INDEX_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_SCHEMA, TransparentAddressTransactionHistoryConsumer,
    TransparentAddressTransactionHistoryConsumerError, TransparentAddressTransactionHistoryPage,
    TransparentAddressTransactionHistoryPageRequest,
};
pub use consumer::transparent_outpoint_spend::{
    TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILIES, TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILY,
    TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME, TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY,
    TRANSPARENT_OUTPOINT_SPEND_SCHEMA, TransparentOutpointSpendConsumer,
    TransparentOutpointSpendConsumerError, encode_transparent_spend_row_value,
};
pub use consumer::value_pool_balance_history::{
    VALUE_POOL_BALANCE_HISTORY_COLUMN_FAMILIES, VALUE_POOL_BALANCE_HISTORY_COLUMN_FAMILY,
    VALUE_POOL_BALANCE_HISTORY_CONSUMER_NAME, VALUE_POOL_BALANCE_HISTORY_METADATA_COLUMN_FAMILY,
    VALUE_POOL_BALANCE_HISTORY_SCHEMA, ValuePoolBalanceBackfillCoverage, ValuePoolBalanceDay,
    ValuePoolBalanceHistoryConsumer, ValuePoolBalanceHistoryConsumerError, ValuePoolBalancePoint,
    ValuePoolBalanceTailCoverage,
};
pub use consumer::value_pool_flow_history::{
    VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILIES, VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY,
    VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME, VALUE_POOL_FLOW_HISTORY_COVERAGE_COLUMN_FAMILY,
    VALUE_POOL_FLOW_HISTORY_INDEX_COLUMN_FAMILY, VALUE_POOL_FLOW_HISTORY_KEY_LEN,
    VALUE_POOL_FLOW_HISTORY_SCHEMA, ValuePoolFlowBackfillCoverage, ValuePoolFlowDirection,
    ValuePoolFlowEvent, ValuePoolFlowHistoryConsumer, ValuePoolFlowHistoryConsumerError,
    ValuePoolFlowHistoryRow, ValuePoolFlowPool, ValuePoolFlowTailCoverage,
};
pub use consumer::{
    BlockCommitContext, BlockCommitInput, BlockKeyedConsumer, BlockValuePoolBalanceFacts,
    ChainCommittedEvent, ChainReorgedEvent, CommittedRange, MaterializedViewBlockCheckpoint,
    MaterializedViewConsumer, MaterializedViewConsumerCtx, MaterializedViewConsumerError,
    MaterializedViewConsumerName, MaterializedViewConsumerSchema, MaterializedViewMempoolConsumer,
    MempoolConsumerEvent, MempoolConsumerEventVariant, RevertedRange,
    TransactionIntrinsicValueBalanceFacts, TransparentSpendFacts, apply_chain_committed_in_memory,
    apply_chain_reorged_in_memory,
};
pub use consumer_registry::{
    ChainEventBlockJoin, MaterializedViewActivationProbe, MaterializedViewConsumerDefinition,
    MaterializedViewConsumerRole, MaterializedViewDispatch, MaterializedViewPresetMembership,
    MaterializedViewRecoverySource, bundled_materialized_view_consumer_definitions,
};
pub use error::{
    MaterializedViewError, MaterializedViewStoreColumnFamily, MaterializedViewStoreError,
};
pub use store::{
    ChainEventDispatchConsumers, ChainEventDispatchInputs, ConsumerEntry,
    MATERIALIZED_VIEW_STORE_FORMAT_VERSION, MATERIALIZED_VIEW_STORE_SUBDIR,
    MaterializedViewCoverage, MaterializedViewCursorEntry, MaterializedViewPreset,
    MaterializedViewState, MaterializedViewStore, MaterializedViewStoreOptions,
    MaterializedViewStoreReadSnapshot, MaterializedViewStoreTable,
    MaterializedViewWriteMeasurement,
};
