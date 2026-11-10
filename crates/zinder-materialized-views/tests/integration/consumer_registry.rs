//! Bundled consumer registry contracts.

use std::collections::BTreeMap;

use zinder_materialized_views::{
    BLOCK_PRODUCTION_TIME_CONSUMER_NAME, BLOCK_SUMMARY_CONSUMER_NAME,
    COMMITMENT_ROOT_SEARCH_CONSUMER_NAME, CONVENTIONAL_FEE_DISTRIBUTION_CONSUMER_NAME,
    ChainEventBlockJoin, IRONWOOD_MIGRATION_CONSUMER_NAME, MEMPOOL_EVENT_COUNTS_CONSUMER_NAME,
    MaterializedViewConsumerName, MaterializedViewDispatch, MaterializedViewPreset,
    MaterializedViewRecoverySource, MaterializedViewStore, PAID_FEE_DISTRIBUTION_CONSUMER_NAME,
    RECENT_TRANSACTIONS_CONSUMER_NAME, REORG_INCIDENTS_CONSUMER_NAME,
    TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME, TRANSACTION_FEES_CONSUMER_NAME,
    TRANSACTION_HISTORY_CONSUMER_NAME, TRANSPARENT_ADDRESS_ACTIVITY_CONSUMER_NAME,
    TRANSPARENT_ADDRESS_DELTAS_CONSUMER_NAME, TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME,
    TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_CONSUMER_NAME,
    TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME, VALUE_POOL_BALANCE_HISTORY_CONSUMER_NAME,
    VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME, bundled_materialized_view_consumer_definitions,
};

fn names(selected: impl IntoIterator<Item = MaterializedViewConsumerName>) -> Vec<&'static str> {
    let mut names = selected
        .into_iter()
        .map(MaterializedViewConsumerName::as_str)
        .collect::<Vec<_>>();
    names.sort_unstable();
    names
}

fn registry_names(selects: impl Fn(&MaterializedViewDispatch) -> bool) -> Vec<&'static str> {
    names(
        bundled_materialized_view_consumer_definitions()
            .iter()
            .filter(|definition| selects(&definition.dispatch))
            .map(|definition| definition.schema.name),
    )
}

#[test]
fn every_dispatched_entry_constructs_the_consumer_its_schema_names() {
    for definition in bundled_materialized_view_consumer_definitions() {
        let constructed = match definition.dispatch {
            MaterializedViewDispatch::ChainEventBlocks { construct, .. } => {
                Some(construct().name())
            }
            MaterializedViewDispatch::ChainEventEnvelope { construct } => Some(construct().name()),
            MaterializedViewDispatch::MempoolEvents { construct } => Some(construct().name()),
            MaterializedViewDispatch::CanonicalBackfillOnly => None,
        };
        if let Some(constructed) = constructed {
            assert_eq!(
                constructed,
                definition.schema.name,
                "registry entry {} constructs a different consumer",
                definition.schema.name.as_str()
            );
        }
    }
}

#[test]
fn store_dispatch_lists_derive_from_the_registry() {
    assert_eq!(
        names(
            MaterializedViewStore::bundled_chain_event_consumer_names()
                .iter()
                .copied()
        ),
        registry_names(|dispatch| matches!(
            dispatch,
            MaterializedViewDispatch::ChainEventBlocks { .. }
        )),
    );
    assert_eq!(
        names(
            MaterializedViewStore::bundled_event_only_chain_event_consumer_names()
                .iter()
                .copied()
        ),
        registry_names(|dispatch| matches!(
            dispatch,
            MaterializedViewDispatch::ChainEventEnvelope { .. }
        )),
    );
    assert_eq!(
        names(
            MaterializedViewPreset::Explorer
                .consumer_schemas()
                .iter()
                .map(|schema| schema.name)
        ),
        registry_names(|_| true),
    );
    assert_eq!(
        names(
            MaterializedViewStore::bundled_consumers()
                .iter()
                .map(|schema| schema.name)
        ),
        registry_names(|_| true),
    );
    assert_eq!(
        names(
            MaterializedViewPreset::Wallet
                .consumer_schemas()
                .iter()
                .map(|schema| schema.name)
        ),
        names(
            bundled_materialized_view_consumer_definitions()
                .iter()
                .filter(|definition| definition.included_in(MaterializedViewPreset::Wallet))
                .map(|definition| definition.schema.name)
        ),
    );
}

/// Recovery sources each bundled consumer declared before dispatch was registered.
fn expected_recovery_sources() -> BTreeMap<&'static str, MaterializedViewRecoverySource> {
    use MaterializedViewRecoverySource::{
        CanonicalBackfill as Backfill, CanonicalBackfillAndChainEvents as BackfillAndEvents,
        CanonicalChainEvents as Events, CanonicalSnapshotAndChainEvents as SnapshotAndEvents,
        MempoolEvents as Mempool,
    };
    [
        (BLOCK_PRODUCTION_TIME_CONSUMER_NAME, BackfillAndEvents),
        (BLOCK_SUMMARY_CONSUMER_NAME, Events),
        (IRONWOOD_MIGRATION_CONSUMER_NAME, Events),
        (COMMITMENT_ROOT_SEARCH_CONSUMER_NAME, BackfillAndEvents),
        (
            CONVENTIONAL_FEE_DISTRIBUTION_CONSUMER_NAME,
            BackfillAndEvents,
        ),
        (MEMPOOL_EVENT_COUNTS_CONSUMER_NAME, Mempool),
        (PAID_FEE_DISTRIBUTION_CONSUMER_NAME, BackfillAndEvents),
        (RECENT_TRANSACTIONS_CONSUMER_NAME, Events),
        (TRANSACTION_HISTORY_CONSUMER_NAME, Events),
        (REORG_INCIDENTS_CONSUMER_NAME, Events),
        (TRANSACTION_FEES_CONSUMER_NAME, Events),
        (
            TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME,
            BackfillAndEvents,
        ),
        (TRANSPARENT_ADDRESS_ACTIVITY_CONSUMER_NAME, Events),
        (TRANSPARENT_ADDRESS_DELTAS_CONSUMER_NAME, Events),
        (TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME, SnapshotAndEvents),
        (
            TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_CONSUMER_NAME,
            Events,
        ),
        (TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME, Events),
        (VALUE_POOL_BALANCE_HISTORY_CONSUMER_NAME, Backfill),
        (VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME, BackfillAndEvents),
    ]
    .into_iter()
    .map(|(name, source)| (name.as_str(), source))
    .collect()
}

#[test]
fn dispatch_declares_each_consumer_recovery_source() {
    let declared = bundled_materialized_view_consumer_definitions()
        .iter()
        .map(|definition| {
            (
                definition.schema.name.as_str(),
                definition.recovery_source(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(declared, expected_recovery_sources());
}

#[test]
fn backfill_owned_block_consumers_are_the_canonical_backfill_joins() {
    assert_eq!(
        registry_names(|dispatch| matches!(
            dispatch,
            MaterializedViewDispatch::ChainEventBlocks {
                join: ChainEventBlockJoin::CanonicalBackfill,
                ..
            }
        )),
        names([
            BLOCK_PRODUCTION_TIME_CONSUMER_NAME,
            COMMITMENT_ROOT_SEARCH_CONSUMER_NAME,
            CONVENTIONAL_FEE_DISTRIBUTION_CONSUMER_NAME,
            PAID_FEE_DISTRIBUTION_CONSUMER_NAME,
            TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME,
            VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME,
        ]),
    );
}
