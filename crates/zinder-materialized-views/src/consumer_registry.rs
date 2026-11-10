//! Closed registry of the materialized-view consumers bundled with this release.
//!
//! Each entry declares one consumer's durable schema, product role, preset
//! membership, and how the replay host constructs and drives it. Store preset lists, chain-event cursor lists, and
//! the replay host's dispatch sets all derive from this one table.

use crate::{
    BLOCK_PRODUCTION_TIME_SCHEMA, BLOCK_SUMMARY_SCHEMA, BlockKeyedConsumer,
    BlockProductionTimeConsumer, BlockSummaryConsumer, COMMITMENT_ROOT_SEARCH_SCHEMA,
    CONVENTIONAL_FEE_DISTRIBUTION_SCHEMA, CommitmentRootSearchConsumer,
    ConventionalFeeDistributionConsumer, IRONWOOD_MIGRATION_SCHEMA, IronwoodMigrationConsumer,
    MEMPOOL_EVENT_COUNTS_SCHEMA, MaterializedViewConsumer, MaterializedViewConsumerName,
    MaterializedViewConsumerSchema, MaterializedViewMempoolConsumer, MaterializedViewPreset,
    MaterializedViewStore, MaterializedViewStoreError, MempoolEventCountsConsumer,
    PAID_FEE_DISTRIBUTION_SCHEMA, PaidFeeDistributionConsumer, RECENT_TRANSACTIONS_SCHEMA,
    REORG_INCIDENTS_SCHEMA, RecentTransactionsConsumer, ReorgIncidentsConsumer,
    TRANSACTION_COMPONENT_SUMMARY_SCHEMA, TRANSACTION_FEES_SCHEMA, TRANSACTION_HISTORY_SCHEMA,
    TRANSPARENT_ADDRESS_ACTIVITY_SCHEMA, TRANSPARENT_ADDRESS_DELTAS_SCHEMA,
    TRANSPARENT_ADDRESS_RANKING_SCHEMA, TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_SCHEMA,
    TRANSPARENT_OUTPOINT_SPEND_SCHEMA, TransactionComponentSummaryConsumer,
    TransactionFeesConsumer, TransactionHistoryConsumer, TransparentAddressActivityConsumer,
    TransparentAddressDeltasConsumer, TransparentAddressRankingConsumer,
    TransparentAddressTransactionHistoryConsumer, TransparentOutpointSpendConsumer,
    VALUE_POOL_BALANCE_HISTORY_SCHEMA, VALUE_POOL_FLOW_HISTORY_SCHEMA,
    ValuePoolFlowHistoryConsumer,
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
///
/// Derived from [`MaterializedViewDispatch`] by
/// [`MaterializedViewConsumerDefinition::recovery_source`].
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

/// Reports whether an activation-gated consumer may receive chain events.
pub type MaterializedViewActivationProbe =
    fn(&MaterializedViewStore) -> Result<bool, MaterializedViewStoreError>;

/// How a block-keyed chain-event consumer joins the shared replay cursor.
#[derive(Clone, Copy, Debug)]
pub enum ChainEventBlockJoin {
    /// Replays every retained chain event from the shared replay cursor.
    Replay,
    /// Follows the live tail from the shared replay cursor; a canonical
    /// backfill owns the settled history before the point where it joined.
    CanonicalBackfill,
    /// Receives chain events only after `is_active` reports that its canonical
    /// snapshot has been published.
    CanonicalSnapshot {
        /// Activation check evaluated before each dispatch.
        is_active: MaterializedViewActivationProbe,
    },
}

/// How the replay host constructs and drives one bundled consumer.
///
/// The enum is closed: a new dispatch shape must be handled by every
/// exhaustive match in the replay host before it compiles.
#[derive(Clone, Copy, Debug)]
pub enum MaterializedViewDispatch {
    /// Block-keyed consumer that receives chain events with their committed
    /// block contexts.
    ChainEventBlocks {
        /// How the consumer joins the shared replay cursor.
        join: ChainEventBlockJoin,
        /// Builds a fresh consumer for one dispatch.
        construct: fn() -> Box<dyn BlockKeyedConsumer>,
    },
    /// Chain-event consumer that reads only the event envelope.
    ChainEventEnvelope {
        /// Builds a fresh consumer for one dispatch.
        construct: fn() -> Box<dyn MaterializedViewConsumer>,
    },
    /// Consumer of writer-owned mempool events.
    MempoolEvents {
        /// Builds a fresh consumer for one dispatch.
        construct: fn() -> Box<dyn MaterializedViewMempoolConsumer>,
    },
    /// Consumer written only by a canonical backfill worker, never by events.
    CanonicalBackfillOnly,
}

/// Complete product, recovery, and dispatch declaration for one durable consumer.
#[derive(Clone, Copy, Debug)]
pub struct MaterializedViewConsumerDefinition {
    /// Durable schema identity and owned row contract.
    pub schema: MaterializedViewConsumerSchema,
    /// Product role served by the consumer.
    pub role: MaterializedViewConsumerRole,
    /// Presets that select the consumer.
    pub preset_membership: MaterializedViewPresetMembership,
    /// How the replay host constructs and drives the consumer.
    pub dispatch: MaterializedViewDispatch,
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

    /// Returns the durable source used to reconstruct this consumer.
    #[must_use]
    pub const fn recovery_source(self) -> MaterializedViewRecoverySource {
        match self.dispatch {
            MaterializedViewDispatch::ChainEventBlocks {
                join: ChainEventBlockJoin::Replay,
                ..
            }
            | MaterializedViewDispatch::ChainEventEnvelope { .. } => {
                MaterializedViewRecoverySource::CanonicalChainEvents
            }
            MaterializedViewDispatch::ChainEventBlocks {
                join: ChainEventBlockJoin::CanonicalBackfill,
                ..
            } => MaterializedViewRecoverySource::CanonicalBackfillAndChainEvents,
            MaterializedViewDispatch::ChainEventBlocks {
                join: ChainEventBlockJoin::CanonicalSnapshot { .. },
                ..
            } => MaterializedViewRecoverySource::CanonicalSnapshotAndChainEvents,
            MaterializedViewDispatch::MempoolEvents { .. } => {
                MaterializedViewRecoverySource::MempoolEvents
            }
            MaterializedViewDispatch::CanonicalBackfillOnly => {
                MaterializedViewRecoverySource::CanonicalBackfill
            }
        }
    }
}

const fn optional(
    schema: MaterializedViewConsumerSchema,
    dispatch: MaterializedViewDispatch,
) -> MaterializedViewConsumerDefinition {
    MaterializedViewConsumerDefinition {
        schema,
        role: MaterializedViewConsumerRole::OptionalProductView,
        preset_membership: MaterializedViewPresetMembership::ExplorerOnly,
        dispatch,
    }
}

const fn chain_event_blocks(
    join: ChainEventBlockJoin,
    construct: fn() -> Box<dyn BlockKeyedConsumer>,
) -> MaterializedViewDispatch {
    MaterializedViewDispatch::ChainEventBlocks { join, construct }
}

fn transparent_address_ranking_is_active(
    store: &MaterializedViewStore,
) -> Result<bool, MaterializedViewStoreError> {
    Ok(TransparentAddressRankingConsumer::active_metadata(store)?.is_some())
}

const BUNDLED_MATERIALIZED_VIEW_CONSUMER_DEFINITIONS: &[MaterializedViewConsumerDefinition] = &[
    optional(
        BLOCK_PRODUCTION_TIME_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::CanonicalBackfill, || {
            Box::new(BlockProductionTimeConsumer::new())
        }),
    ),
    optional(
        BLOCK_SUMMARY_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::Replay, || {
            Box::new(BlockSummaryConsumer::new())
        }),
    ),
    optional(
        IRONWOOD_MIGRATION_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::Replay, || {
            Box::new(IronwoodMigrationConsumer::new())
        }),
    ),
    optional(
        COMMITMENT_ROOT_SEARCH_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::CanonicalBackfill, || {
            Box::new(CommitmentRootSearchConsumer::new())
        }),
    ),
    optional(
        CONVENTIONAL_FEE_DISTRIBUTION_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::CanonicalBackfill, || {
            Box::new(ConventionalFeeDistributionConsumer::new())
        }),
    ),
    optional(
        MEMPOOL_EVENT_COUNTS_SCHEMA,
        MaterializedViewDispatch::MempoolEvents {
            construct: || Box::new(MempoolEventCountsConsumer::new()),
        },
    ),
    optional(
        PAID_FEE_DISTRIBUTION_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::CanonicalBackfill, || {
            Box::new(PaidFeeDistributionConsumer::new())
        }),
    ),
    optional(
        RECENT_TRANSACTIONS_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::Replay, || {
            Box::new(RecentTransactionsConsumer::new())
        }),
    ),
    optional(
        TRANSACTION_HISTORY_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::Replay, || {
            Box::new(TransactionHistoryConsumer::new())
        }),
    ),
    optional(
        REORG_INCIDENTS_SCHEMA,
        MaterializedViewDispatch::ChainEventEnvelope {
            construct: || Box::new(ReorgIncidentsConsumer::new()),
        },
    ),
    optional(
        TRANSACTION_FEES_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::Replay, || {
            Box::new(TransactionFeesConsumer::new())
        }),
    ),
    optional(
        TRANSACTION_COMPONENT_SUMMARY_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::CanonicalBackfill, || {
            Box::new(TransactionComponentSummaryConsumer::new())
        }),
    ),
    optional(
        TRANSPARENT_ADDRESS_ACTIVITY_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::Replay, || {
            Box::new(TransparentAddressActivityConsumer::new())
        }),
    ),
    optional(
        TRANSPARENT_ADDRESS_DELTAS_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::Replay, || {
            Box::new(TransparentAddressDeltasConsumer::new())
        }),
    ),
    optional(
        TRANSPARENT_ADDRESS_RANKING_SCHEMA,
        chain_event_blocks(
            ChainEventBlockJoin::CanonicalSnapshot {
                is_active: transparent_address_ranking_is_active,
            },
            || Box::new(TransparentAddressRankingConsumer::new()),
        ),
    ),
    MaterializedViewConsumerDefinition {
        schema: TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_SCHEMA,
        role: MaterializedViewConsumerRole::WalletServing,
        preset_membership: MaterializedViewPresetMembership::WalletAndExplorer,
        dispatch: chain_event_blocks(ChainEventBlockJoin::Replay, || {
            Box::new(TransparentAddressTransactionHistoryConsumer::new())
        }),
    },
    MaterializedViewConsumerDefinition {
        schema: TRANSPARENT_OUTPOINT_SPEND_SCHEMA,
        role: MaterializedViewConsumerRole::WalletCorrectness,
        preset_membership: MaterializedViewPresetMembership::WalletAndExplorer,
        dispatch: chain_event_blocks(ChainEventBlockJoin::Replay, || {
            Box::new(TransparentOutpointSpendConsumer::new())
        }),
    },
    optional(
        VALUE_POOL_BALANCE_HISTORY_SCHEMA,
        MaterializedViewDispatch::CanonicalBackfillOnly,
    ),
    optional(
        VALUE_POOL_FLOW_HISTORY_SCHEMA,
        chain_event_blocks(ChainEventBlockJoin::CanonicalBackfill, || {
            Box::new(ValuePoolFlowHistoryConsumer::new())
        }),
    ),
];

/// Returns every bundled consumer's product, recovery, and dispatch declaration.
#[must_use]
pub const fn bundled_materialized_view_consumer_definitions()
-> &'static [MaterializedViewConsumerDefinition] {
    BUNDLED_MATERIALIZED_VIEW_CONSUMER_DEFINITIONS
}

/// Registry subsets the store derives its fixed lists from.
#[derive(Clone, Copy)]
pub(crate) enum RegistrySelection {
    /// Every bundled consumer.
    Explorer,
    /// Consumers the wallet preset selects.
    Wallet,
    /// Block-keyed chain-event consumers.
    ChainEventBlocks,
    /// Envelope-only chain-event consumers.
    ChainEventEnvelope,
}

impl RegistrySelection {
    const fn selects(self, definition: &MaterializedViewConsumerDefinition) -> bool {
        match self {
            Self::Explorer => definition.included_in(MaterializedViewPreset::Explorer),
            Self::Wallet => definition.included_in(MaterializedViewPreset::Wallet),
            Self::ChainEventBlocks => matches!(
                definition.dispatch,
                MaterializedViewDispatch::ChainEventBlocks { .. }
            ),
            Self::ChainEventEnvelope => matches!(
                definition.dispatch,
                MaterializedViewDispatch::ChainEventEnvelope { .. }
            ),
        }
    }

    pub(crate) const fn count(self) -> usize {
        let definitions = BUNDLED_MATERIALIZED_VIEW_CONSUMER_DEFINITIONS;
        let mut count = 0;
        let mut index = 0;
        while index < definitions.len() {
            if self.selects(&definitions[index]) {
                count += 1;
            }
            index += 1;
        }
        count
    }

    pub(crate) const fn schemas<const N: usize>(self) -> [MaterializedViewConsumerSchema; N] {
        let definitions = BUNDLED_MATERIALIZED_VIEW_CONSUMER_DEFINITIONS;
        let mut schemas = [MaterializedViewConsumerSchema::new(
            MaterializedViewConsumerName::from_static(""),
            0,
            &[],
        ); N];
        let mut selected = 0;
        let mut index = 0;
        while index < definitions.len() {
            if self.selects(&definitions[index]) {
                schemas[selected] = definitions[index].schema;
                selected += 1;
            }
            index += 1;
        }
        assert!(selected == N, "registry selection length mismatch");
        schemas
    }

    pub(crate) const fn names<const N: usize>(self) -> [MaterializedViewConsumerName; N] {
        let schemas = self.schemas::<N>();
        let mut names = [MaterializedViewConsumerName::from_static(""); N];
        let mut index = 0;
        while index < N {
            names[index] = schemas[index].name;
            index += 1;
        }
        names
    }
}
