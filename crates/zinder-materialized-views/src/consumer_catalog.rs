//! Product roles and recovery contracts for bundled materialized-view consumers.

use crate::{
    BLOCK_PRODUCTION_TIME_SCHEMA, BLOCK_SUMMARY_SCHEMA, COMMITMENT_ROOT_SEARCH_SCHEMA,
    CONVENTIONAL_FEE_DISTRIBUTION_SCHEMA, IRONWOOD_MIGRATION_SCHEMA, MEMPOOL_EVENT_COUNTS_SCHEMA,
    MaterializedViewConsumerSchema, MaterializedViewPreset, PAID_FEE_DISTRIBUTION_SCHEMA,
    RECENT_TRANSACTIONS_SCHEMA, REORG_INCIDENTS_SCHEMA, TRANSACTION_COMPONENT_SUMMARY_SCHEMA,
    TRANSACTION_FEES_SCHEMA, TRANSACTION_HISTORY_SCHEMA, TRANSPARENT_ADDRESS_ACTIVITY_SCHEMA,
    TRANSPARENT_ADDRESS_DELTAS_SCHEMA, TRANSPARENT_ADDRESS_RANKING_SCHEMA,
    TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_SCHEMA, TRANSPARENT_OUTPOINT_SPEND_SCHEMA,
    VALUE_POOL_BALANCE_HISTORY_SCHEMA, VALUE_POOL_FLOW_HISTORY_SCHEMA,
};

/// Product responsibility assigned to one consumer identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializedViewConsumerRole {
    /// Required for wallet spend-resolution correctness.
    WalletCorrectness,
    /// Required by the supported wallet-serving read contract.
    WalletServing,
    /// Explorer, analytics, dashboard, or other optional product view.
    OptionalProductView,
}

/// Durable source used to reconstruct one materialized-view consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializedViewRecoverySource {
    /// Replays retained canonical chain events.
    CanonicalChainEvents,
    /// Replays the live tail and owns a separate historical canonical backfill.
    CanonicalBackfillAndChainEvents,
    /// Builds an initial canonical snapshot before following chain events.
    CanonicalSnapshotAndChainEvents,
    /// Reconstructs entirely from a historical canonical backfill worker.
    CanonicalBackfill,
    /// Replays writer-owned mempool events.
    MempoolEvents,
}

/// Product presets that include one consumer identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializedViewPresetMembership {
    /// Consumer is required by both wallet and explorer deployments.
    WalletAndExplorer,
    /// Consumer is included only in the explorer workload.
    ExplorerOnly,
}

/// Complete product and recovery declaration for one durable consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaterializedViewConsumerDefinition {
    /// Durable schema identity and owned row contract.
    pub schema: MaterializedViewConsumerSchema,
    /// Product role served by the consumer.
    pub role: MaterializedViewConsumerRole,
    /// Source used to recover consumer state.
    pub recovery_source: MaterializedViewRecoverySource,
    /// Presets that select the consumer.
    pub preset_membership: MaterializedViewPresetMembership,
}

impl MaterializedViewConsumerDefinition {
    /// Returns whether this consumer belongs to `preset`.
    #[must_use]
    pub const fn included_in(self, preset: MaterializedViewPreset) -> bool {
        match (self.preset_membership, preset) {
            (MaterializedViewPresetMembership::WalletAndExplorer, _)
            | (MaterializedViewPresetMembership::ExplorerOnly, MaterializedViewPreset::Explorer) => {
                true
            }
            (MaterializedViewPresetMembership::ExplorerOnly, MaterializedViewPreset::Wallet) => {
                false
            }
        }
    }
}

const fn optional(
    schema: MaterializedViewConsumerSchema,
    recovery_source: MaterializedViewRecoverySource,
) -> MaterializedViewConsumerDefinition {
    MaterializedViewConsumerDefinition {
        schema,
        role: MaterializedViewConsumerRole::OptionalProductView,
        recovery_source,
        preset_membership: MaterializedViewPresetMembership::ExplorerOnly,
    }
}

const BUNDLED_MATERIALIZED_VIEW_CONSUMER_DEFINITIONS: &[MaterializedViewConsumerDefinition] = &[
    optional(
        BLOCK_PRODUCTION_TIME_SCHEMA,
        MaterializedViewRecoverySource::CanonicalBackfillAndChainEvents,
    ),
    optional(
        BLOCK_SUMMARY_SCHEMA,
        MaterializedViewRecoverySource::CanonicalChainEvents,
    ),
    optional(
        IRONWOOD_MIGRATION_SCHEMA,
        MaterializedViewRecoverySource::CanonicalChainEvents,
    ),
    optional(
        COMMITMENT_ROOT_SEARCH_SCHEMA,
        MaterializedViewRecoverySource::CanonicalBackfillAndChainEvents,
    ),
    optional(
        CONVENTIONAL_FEE_DISTRIBUTION_SCHEMA,
        MaterializedViewRecoverySource::CanonicalBackfillAndChainEvents,
    ),
    optional(
        MEMPOOL_EVENT_COUNTS_SCHEMA,
        MaterializedViewRecoverySource::MempoolEvents,
    ),
    optional(
        PAID_FEE_DISTRIBUTION_SCHEMA,
        MaterializedViewRecoverySource::CanonicalBackfillAndChainEvents,
    ),
    optional(
        RECENT_TRANSACTIONS_SCHEMA,
        MaterializedViewRecoverySource::CanonicalChainEvents,
    ),
    optional(
        TRANSACTION_HISTORY_SCHEMA,
        MaterializedViewRecoverySource::CanonicalChainEvents,
    ),
    optional(
        REORG_INCIDENTS_SCHEMA,
        MaterializedViewRecoverySource::CanonicalChainEvents,
    ),
    optional(
        TRANSACTION_FEES_SCHEMA,
        MaterializedViewRecoverySource::CanonicalChainEvents,
    ),
    optional(
        TRANSACTION_COMPONENT_SUMMARY_SCHEMA,
        MaterializedViewRecoverySource::CanonicalBackfillAndChainEvents,
    ),
    optional(
        TRANSPARENT_ADDRESS_ACTIVITY_SCHEMA,
        MaterializedViewRecoverySource::CanonicalChainEvents,
    ),
    optional(
        TRANSPARENT_ADDRESS_DELTAS_SCHEMA,
        MaterializedViewRecoverySource::CanonicalChainEvents,
    ),
    optional(
        TRANSPARENT_ADDRESS_RANKING_SCHEMA,
        MaterializedViewRecoverySource::CanonicalSnapshotAndChainEvents,
    ),
    MaterializedViewConsumerDefinition {
        schema: TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_SCHEMA,
        role: MaterializedViewConsumerRole::WalletServing,
        recovery_source: MaterializedViewRecoverySource::CanonicalChainEvents,
        preset_membership: MaterializedViewPresetMembership::WalletAndExplorer,
    },
    MaterializedViewConsumerDefinition {
        schema: TRANSPARENT_OUTPOINT_SPEND_SCHEMA,
        role: MaterializedViewConsumerRole::WalletCorrectness,
        recovery_source: MaterializedViewRecoverySource::CanonicalChainEvents,
        preset_membership: MaterializedViewPresetMembership::WalletAndExplorer,
    },
    optional(
        VALUE_POOL_BALANCE_HISTORY_SCHEMA,
        MaterializedViewRecoverySource::CanonicalBackfill,
    ),
    optional(
        VALUE_POOL_FLOW_HISTORY_SCHEMA,
        MaterializedViewRecoverySource::CanonicalBackfillAndChainEvents,
    ),
];

/// Returns every bundled consumer's product and recovery declaration.
#[must_use]
pub const fn bundled_materialized_view_consumer_definitions()
-> &'static [MaterializedViewConsumerDefinition] {
    BUNDLED_MATERIALIZED_VIEW_CONSUMER_DEFINITIONS
}
