//! Explorer view manifest storage contracts.

use eyre::Result;
use tempfile::TempDir;
use zinder_materialized_views::{
    BLOCK_SUMMARY_COLUMN_FAMILY, MaterializedViewConsumerName, MaterializedViewConsumerSchema,
    MaterializedViewStore, MaterializedViewStoreError, MaterializedViewStoreOptions,
    TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILY, TRANSPARENT_OUTPOINT_SPEND_SCHEMA,
    bundled_materialized_view_consumer_definitions,
};
use zinder_store::RocksDbResourceBudget;

const FOREIGN_CONSUMER_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("foreign_materialized_view");
const FOREIGN_COLUMN_FAMILY: &str = "foreign_consumer_rows";
const FOREIGN_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(FOREIGN_CONSUMER_NAME, 1, &[FOREIGN_COLUMN_FAMILY]);

/// Identity of the address-history view that earlier releases bundled.
const RETIRED_ADDRESS_HISTORY_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("transparent_address_transaction_history");
const RETIRED_ADDRESS_HISTORY_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        RETIRED_ADDRESS_HISTORY_NAME,
        1,
        &["transparent_address_transaction_history"],
    );

fn options() -> MaterializedViewStoreOptions {
    MaterializedViewStoreOptions {
        rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
        ..MaterializedViewStoreOptions::default()
    }
}

#[test]
fn explorer_view_store_persists_every_bundled_consumer() -> Result<()> {
    let primary = TempDir::new()?;
    let secondary = TempDir::new()?;

    {
        let store = MaterializedViewStore::open_explorer_views(primary.path(), options())?;
        store.consumer_column_family(BLOCK_SUMMARY_COLUMN_FAMILY)?;
        store.consumer_column_family(TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILY)?;
        assert_eq!(
            store.consumer_identities().count(),
            MaterializedViewStore::bundled_consumers().len()
        );
        assert!(store.has_explorer_view_manifest());
    }

    let reader = MaterializedViewStore::open_explorer_views_secondary(
        primary.path(),
        secondary.path(),
        options(),
    )?;
    assert!(reader.has_consumer(TRANSPARENT_OUTPOINT_SPEND_SCHEMA.name));
    assert!(reader.has_explorer_view_manifest());
    Ok(())
}

#[test]
fn store_detection_distinguishes_initialized_and_missing_paths() -> Result<()> {
    let explorer = TempDir::new()?;
    let missing = TempDir::new()?;
    MaterializedViewStore::open_explorer_views(explorer.path(), options())?;

    assert!(MaterializedViewStore::exists_at_path(explorer.path())?);
    assert!(!MaterializedViewStore::exists_at_path(missing.path())?);
    Ok(())
}

#[test]
fn a_partial_manifest_fails_explorer_admission_before_mutation() -> Result<()> {
    let primary = TempDir::new()?;
    let secondary = TempDir::new()?;
    {
        let partial = MaterializedViewStore::open(
            primary.path(),
            MaterializedViewStoreOptions {
                consumers: &[TRANSPARENT_OUTPOINT_SPEND_SCHEMA],
                ..options()
            },
        )?;
        assert!(!partial.has_explorer_view_manifest());
    }
    let column_families_before =
        rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), primary.path())?;

    let primary_outcome = MaterializedViewStore::open_explorer_views(primary.path(), options());
    assert!(matches!(
        primary_outcome,
        Err(MaterializedViewStoreError::ExplorerViewManifestRequiresFreshStore)
    ));
    let secondary_outcome = MaterializedViewStore::open_explorer_views_secondary(
        primary.path(),
        secondary.path(),
        options(),
    );
    assert!(matches!(
        secondary_outcome,
        Err(MaterializedViewStoreError::ExplorerViewManifestRequiresFreshStore)
    ));
    let column_families_after =
        rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), primary.path())?;
    assert_eq!(column_families_after, column_families_before);
    Ok(())
}

#[test]
fn a_store_with_the_retired_address_history_view_requires_a_rebuild() -> Result<()> {
    let primary = TempDir::new()?;
    {
        MaterializedViewStore::open(
            primary.path(),
            MaterializedViewStoreOptions {
                consumers: &[
                    RETIRED_ADDRESS_HISTORY_SCHEMA,
                    TRANSPARENT_OUTPOINT_SPEND_SCHEMA,
                ],
                ..options()
            },
        )?;
    }
    let column_families_before =
        rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), primary.path())?;

    assert!(matches!(
        MaterializedViewStore::exists_at_path(primary.path()),
        Err(MaterializedViewStoreError::ConsumerNotDeclared {
            consumer,
            persisted_schema_version: 1,
        }) if consumer == RETIRED_ADDRESS_HISTORY_NAME.as_str()
    ));
    assert!(matches!(
        MaterializedViewStore::open_explorer_views(primary.path(), options()),
        Err(MaterializedViewStoreError::ConsumerNotDeclared { consumer, .. })
            if consumer == RETIRED_ADDRESS_HISTORY_NAME.as_str()
    ));
    let column_families_after =
        rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), primary.path())?;
    assert_eq!(column_families_after, column_families_before);
    Ok(())
}

#[test]
fn store_detection_rejects_a_foreign_consumer_without_mutation() -> Result<()> {
    let primary = TempDir::new()?;
    {
        let store = MaterializedViewStore::open(
            primary.path(),
            MaterializedViewStoreOptions {
                consumers: &[FOREIGN_SCHEMA],
                ..options()
            },
        )?;
        store.put_consumer(FOREIGN_COLUMN_FAMILY, b"key", b"value")?;
        store.put_chain_event_cursor(FOREIGN_CONSUMER_NAME, b"cursor")?;
    }
    let column_families_before =
        rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), primary.path())?;

    let outcome = MaterializedViewStore::exists_at_path(primary.path());
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ConsumerNotDeclared {
            consumer,
            persisted_schema_version: 1,
        }) if consumer == FOREIGN_CONSUMER_NAME.as_str()
    ));
    let column_families_after =
        rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), primary.path())?;
    assert_eq!(column_families_after, column_families_before);

    let store = MaterializedViewStore::open(
        primary.path(),
        MaterializedViewStoreOptions {
            consumers: &[FOREIGN_SCHEMA],
            ..options()
        },
    )?;
    assert_eq!(
        store.get_consumer(FOREIGN_COLUMN_FAMILY, b"key")?,
        Some(b"value".to_vec())
    );
    assert_eq!(
        store.get_chain_event_cursor(FOREIGN_CONSUMER_NAME)?,
        Some(b"cursor".to_vec())
    );
    Ok(())
}

#[test]
fn consumer_catalog_declares_every_bundled_identity_once() {
    let definitions = bundled_materialized_view_consumer_definitions();
    let bundled = MaterializedViewStore::bundled_consumers();
    assert_eq!(definitions.len(), bundled.len());
    for schema in bundled {
        assert_eq!(
            definitions
                .iter()
                .filter(|definition| definition.schema.name == schema.name)
                .count(),
            1,
            "consumer {} must have exactly one registry declaration",
            schema.name.as_str()
        );
    }
    assert!(
        bundled
            .iter()
            .all(|schema| schema.name != RETIRED_ADDRESS_HISTORY_NAME),
        "the address-history view has no reader and is not bundled"
    );
}
