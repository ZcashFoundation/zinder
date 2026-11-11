//! Bundled consumer registry contracts.

use zinder_materialized_views::{
    MaterializedViewDispatch, bundled_materialized_view_consumer_definitions,
};

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
