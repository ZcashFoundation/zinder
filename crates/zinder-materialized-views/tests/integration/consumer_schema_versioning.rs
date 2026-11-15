//! Per-consumer materialized-view schema versioning rejects every mismatch.

use std::{collections::BTreeSet, path::Path};

use eyre::Result;
use tempfile::TempDir;
use zinder_materialized_views::{
    MaterializedViewConsumerName, MaterializedViewConsumerSchema, MaterializedViewStore,
    MaterializedViewStoreError, MaterializedViewStoreOptions, MaterializedViewStoreTable,
};
use zinder_store::RocksDbResourceBudget;

const CONSUMER_A: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("consumer_a");
const CONSUMER_B: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("consumer_b");
const CONSUMER_C: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("consumer_c");
const CONSUMER_A_CF: &str = "consumer_a_cf";
const CONSUMER_B_CF: &str = "consumer_b_cf";

const CONSUMER_A_V1: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(CONSUMER_A, 1, &[CONSUMER_A_CF]);
const CONSUMER_A_V2: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(CONSUMER_A, 2, &[CONSUMER_A_CF]);
const CONSUMER_A_TWO_CFS_V1: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(CONSUMER_A, 1, &[CONSUMER_A_CF, CONSUMER_B_CF]);
const CONSUMER_A_TWO_CFS_REORDERED_V1: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(CONSUMER_A, 1, &[CONSUMER_B_CF, CONSUMER_A_CF]);
const CONSUMER_B_V1: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(CONSUMER_B, 1, &[CONSUMER_B_CF]);
const CONSUMER_B_EMPTY_V1: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(CONSUMER_B, 1, &[]);
const CONSUMER_B_ON_A_CF: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(CONSUMER_B, 1, &[CONSUMER_A_CF]);
const CONSUMER_C_ON_A_CF: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(CONSUMER_C, 1, &[CONSUMER_A_CF]);
const CONSUMER_A_ON_RESERVED_CF: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        CONSUMER_A,
        1,
        &[MaterializedViewStoreTable::ChainEventCursor.column_family_name()],
    );

const CONSUMER_A_FOREIGN_UNDO_INDEX: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(CONSUMER_A, 1, &[CONSUMER_A_CF])
        .with_height_undo_index(CONSUMER_B_CF);
const CONSUMER_A_OWN_UNDO_INDEX: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(CONSUMER_A, 1, &[CONSUMER_A_CF])
        .with_height_undo_index(CONSUMER_A_CF);

const BOTH_V1: &[MaterializedViewConsumerSchema] = &[CONSUMER_A_V1, CONSUMER_B_V1];
const A_AND_EMPTY_B_V1: &[MaterializedViewConsumerSchema] = &[CONSUMER_A_V1, CONSUMER_B_EMPTY_V1];
const A_V2_B_V1: &[MaterializedViewConsumerSchema] = &[CONSUMER_A_V2, CONSUMER_B_V1];
const ONLY_A_V1: &[MaterializedViewConsumerSchema] = &[CONSUMER_A_V1];
const ONLY_A_V2: &[MaterializedViewConsumerSchema] = &[CONSUMER_A_V2];
const ONLY_A_TWO_CFS_V1: &[MaterializedViewConsumerSchema] = &[CONSUMER_A_TWO_CFS_V1];
const ONLY_A_TWO_CFS_REORDERED_V1: &[MaterializedViewConsumerSchema] =
    &[CONSUMER_A_TWO_CFS_REORDERED_V1];
const DUPLICATE_CF: &[MaterializedViewConsumerSchema] = &[CONSUMER_A_V1, CONSUMER_B_ON_A_CF];
const RESERVED_CF: &[MaterializedViewConsumerSchema] = &[CONSUMER_A_ON_RESERVED_CF];
const ONLY_C_ON_A_CF: &[MaterializedViewConsumerSchema] = &[CONSUMER_C_ON_A_CF];

fn options(consumers: &'static [MaterializedViewConsumerSchema]) -> MaterializedViewStoreOptions {
    MaterializedViewStoreOptions {
        sync_writes: false,
        consumers,
        rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
        reorg_policy: None,
    }
}

fn open(
    path: &Path,
    consumers: &'static [MaterializedViewConsumerSchema],
) -> Result<MaterializedViewStore> {
    Ok(MaterializedViewStore::open(path, options(consumers))?)
}

fn column_family_set(path: &Path) -> BTreeSet<String> {
    rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), path)
        .unwrap_or_default()
        .into_iter()
        .collect()
}

#[test]
fn higher_declared_consumer_schema_rejects_lower_manifest_without_mutation() -> Result<()> {
    let tempdir = TempDir::new()?;
    {
        let store = open(tempdir.path(), BOTH_V1)?;
        store.put_consumer(CONSUMER_A_CF, b"key-a", b"value-a")?;
        store.put_consumer(CONSUMER_B_CF, b"key-b", b"value-b")?;
        store.put_chain_event_cursor(CONSUMER_A, b"cursor-a")?;
        store.put_chain_event_cursor(CONSUMER_B, b"cursor-b")?;
    }

    let column_families_before = column_family_set(tempdir.path());
    let outcome = MaterializedViewStore::open(tempdir.path(), options(A_V2_B_V1));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ConsumerSchemaMismatch {
            consumer,
            persisted: Some(1),
            running: 2,
        }) if consumer == CONSUMER_A.as_str()
    ));
    assert_eq!(column_family_set(tempdir.path()), column_families_before);

    let store = open(tempdir.path(), BOTH_V1)?;
    assert_eq!(
        store.get_consumer(CONSUMER_A_CF, b"key-a")?,
        Some(b"value-a".to_vec())
    );
    assert_eq!(
        store.get_chain_event_cursor(CONSUMER_A)?,
        Some(b"cursor-a".to_vec())
    );
    assert_eq!(
        store.get_consumer(CONSUMER_B_CF, b"key-b")?,
        Some(b"value-b".to_vec())
    );
    assert_eq!(
        store.get_chain_event_cursor(CONSUMER_B)?,
        Some(b"cursor-b".to_vec())
    );
    Ok(())
}

#[test]
fn reordering_the_same_column_family_set_preserves_rows_and_cursor() -> Result<()> {
    let tempdir = TempDir::new()?;
    {
        let store = open(tempdir.path(), ONLY_A_TWO_CFS_V1)?;
        store.put_consumer(CONSUMER_A_CF, b"key-a", b"value-a")?;
        store.put_consumer(CONSUMER_B_CF, b"key-b", b"value-b")?;
        store.put_chain_event_cursor(CONSUMER_A, b"cursor-a")?;
    }

    let store = open(tempdir.path(), ONLY_A_TWO_CFS_REORDERED_V1)?;
    assert_eq!(
        store.get_consumer(CONSUMER_A_CF, b"key-a")?,
        Some(b"value-a".to_vec())
    );
    assert_eq!(
        store.get_consumer(CONSUMER_B_CF, b"key-b")?,
        Some(b"value-b".to_vec())
    );
    assert_eq!(
        store.get_chain_event_cursor(CONSUMER_A)?,
        Some(b"cursor-a".to_vec())
    );
    Ok(())
}

#[test]
fn lower_declared_consumer_schema_rejects_higher_manifest_without_mutation() -> Result<()> {
    let tempdir = TempDir::new()?;
    {
        let store = open(tempdir.path(), ONLY_A_V2)?;
        store.put_consumer(CONSUMER_A_CF, b"key-a", b"value-a")?;
        store.put_chain_event_cursor(CONSUMER_A, b"cursor-a")?;
    }

    let outcome = MaterializedViewStore::open(tempdir.path(), options(ONLY_A_V1));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ConsumerSchemaMismatch {
            consumer,
            persisted,
            running,
        }) if consumer == CONSUMER_A.as_str() && persisted == Some(2) && running == 1
    ));

    let store = open(tempdir.path(), ONLY_A_V2)?;
    assert_eq!(
        store.get_consumer(CONSUMER_A_CF, b"key-a")?,
        Some(b"value-a".to_vec())
    );
    assert_eq!(
        store.get_chain_event_cursor(CONSUMER_A)?,
        Some(b"cursor-a".to_vec())
    );
    Ok(())
}

#[test]
fn changed_consumer_column_family_set_rejects_without_mutation() -> Result<()> {
    let tempdir = TempDir::new()?;
    {
        let store = open(tempdir.path(), ONLY_A_TWO_CFS_V1)?;
        store.put_consumer(CONSUMER_A_CF, b"key-a", b"value-a")?;
        store.put_consumer(CONSUMER_B_CF, b"key-b", b"value-b")?;
        store.put_chain_event_cursor(CONSUMER_A, b"cursor-a")?;
    }

    let column_families_before = column_family_set(tempdir.path());
    let outcome = MaterializedViewStore::open(tempdir.path(), options(ONLY_A_V1));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ConsumerSchemaMismatch {
            consumer,
            persisted: Some(1),
            running: 1,
        }) if consumer == CONSUMER_A.as_str()
    ));
    assert_eq!(column_family_set(tempdir.path()), column_families_before);

    let store = open(tempdir.path(), ONLY_A_TWO_CFS_V1)?;
    assert_eq!(
        store.get_consumer(CONSUMER_A_CF, b"key-a")?,
        Some(b"value-a".to_vec())
    );
    assert_eq!(
        store.get_consumer(CONSUMER_B_CF, b"key-b")?,
        Some(b"value-b".to_vec())
    );
    assert_eq!(
        store.get_chain_event_cursor(CONSUMER_A)?,
        Some(b"cursor-a".to_vec())
    );
    Ok(())
}

#[test]
fn adding_a_consumer_to_an_existing_manifest_rejects_without_mutation() -> Result<()> {
    let tempdir = TempDir::new()?;
    {
        let store = open(tempdir.path(), ONLY_A_V1)?;
        store.put_consumer(CONSUMER_A_CF, b"key-a", b"value-a")?;
        store.put_chain_event_cursor(CONSUMER_A, b"cursor-a")?;
    }

    let column_families_before = column_family_set(tempdir.path());
    let outcome = MaterializedViewStore::open(tempdir.path(), options(A_AND_EMPTY_B_V1));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ConsumerSchemaMismatch {
            consumer,
            persisted: None,
            running: 1,
        }) if consumer == CONSUMER_B.as_str()
    ));
    assert_eq!(column_family_set(tempdir.path()), column_families_before);

    let store = open(tempdir.path(), ONLY_A_V1)?;
    assert_eq!(
        store.get_consumer(CONSUMER_A_CF, b"key-a")?,
        Some(b"value-a".to_vec())
    );
    assert_eq!(
        store.get_chain_event_cursor(CONSUMER_A)?,
        Some(b"cursor-a".to_vec())
    );
    Ok(())
}

#[test]
fn physical_column_family_drift_rejects_before_primary_open_mutates_the_store() -> Result<()> {
    let tempdir = TempDir::new()?;
    open(tempdir.path(), ONLY_A_V1)?;
    {
        let names = rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), tempdir.path())?;
        let db =
            rust_rocksdb::DB::open_cf(&rust_rocksdb::Options::default(), tempdir.path(), names)?;
        db.drop_cf(CONSUMER_A_CF)?;
    }

    let missing_before = column_family_set(tempdir.path());
    let outcome = MaterializedViewStore::open(tempdir.path(), options(ONLY_A_V1));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ColumnFamilyIdentityMismatch { .. })
    ));
    assert_eq!(column_family_set(tempdir.path()), missing_before);

    let orphan = TempDir::new()?;
    open(orphan.path(), ONLY_A_V1)?;
    {
        let names = rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), orphan.path())?;
        let db =
            rust_rocksdb::DB::open_cf(&rust_rocksdb::Options::default(), orphan.path(), names)?;
        db.create_cf("orphan_consumer_rows", &rust_rocksdb::Options::default())?;
    }

    let orphan_before = column_family_set(orphan.path());
    let outcome = MaterializedViewStore::open(orphan.path(), options(ONLY_A_V1));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ColumnFamilyIdentityMismatch { .. })
    ));
    assert_eq!(column_family_set(orphan.path()), orphan_before);
    Ok(())
}

#[test]
fn unknown_manifest_consumer_fails_closed_without_clearing_rows() -> Result<()> {
    let tempdir = TempDir::new()?;
    {
        let store = open(tempdir.path(), BOTH_V1)?;
        store.put_consumer(CONSUMER_A_CF, b"key-a", b"value-a")?;
        store.put_consumer(CONSUMER_B_CF, b"key-b", b"value-b")?;
        store.put_chain_event_cursor(CONSUMER_B, b"cursor-b")?;
    }

    let outcome = MaterializedViewStore::open(tempdir.path(), options(ONLY_A_V1));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ConsumerNotDeclared {
            consumer,
            persisted_schema_version: 1,
        }) if consumer == CONSUMER_B.as_str()
    ));

    let store = open(tempdir.path(), BOTH_V1)?;
    assert_eq!(
        store.get_consumer(CONSUMER_B_CF, b"key-b")?,
        Some(b"value-b".to_vec())
    );
    assert_eq!(
        store.get_chain_event_cursor(CONSUMER_B)?,
        Some(b"cursor-b".to_vec())
    );
    assert_eq!(
        store.get_consumer(CONSUMER_A_CF, b"key-a")?,
        Some(b"value-a".to_vec())
    );
    Ok(())
}

#[test]
fn fresh_store_records_every_declared_consumer_version() -> Result<()> {
    let tempdir = TempDir::new()?;
    {
        let store = open(tempdir.path(), BOTH_V1)?;
        store.put_consumer(CONSUMER_A_CF, b"key-a", b"value-a")?;
        store.put_consumer(CONSUMER_B_CF, b"key-b", b"value-b")?;
    }

    // Reopening at the same versions preserves every row only when the fresh
    // open recorded the complete declared manifest.
    let store = open(tempdir.path(), BOTH_V1)?;
    assert_eq!(
        store.get_consumer(CONSUMER_A_CF, b"key-a")?,
        Some(b"value-a".to_vec())
    );
    assert_eq!(
        store.get_consumer(CONSUMER_B_CF, b"key-b")?,
        Some(b"value-b".to_vec())
    );
    Ok(())
}

#[test]
fn declaring_one_column_family_from_two_consumers_is_rejected() -> Result<()> {
    let tempdir = TempDir::new()?;
    let outcome = MaterializedViewStore::open(tempdir.path(), options(DUPLICATE_CF));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ConsumerColumnFamilyConflict { name }) if name == CONSUMER_A_CF
    ));
    Ok(())
}

#[test]
fn declaring_a_reserved_store_table_column_family_is_rejected() -> Result<()> {
    let tempdir = TempDir::new()?;
    let outcome = MaterializedViewStore::open(tempdir.path(), options(RESERVED_CF));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ConsumerColumnFamilyConflict { name })
            if name == MaterializedViewStoreTable::ChainEventCursor.column_family_name()
    ));
    Ok(())
}

#[test]
fn declaring_an_undo_index_the_consumer_does_not_own_is_rejected() -> Result<()> {
    let tempdir = TempDir::new()?;
    let outcome = MaterializedViewStore::open(
        tempdir.path(),
        options(&[CONSUMER_A_FOREIGN_UNDO_INDEX, CONSUMER_B_V1]),
    );
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::UndeclaredHeightUndoIndex { consumer, name })
            if consumer == CONSUMER_A.as_str() && name == CONSUMER_B_CF
    ));
    Ok(())
}

#[test]
fn declaring_an_undo_index_keeps_the_persisted_manifest() -> Result<()> {
    let tempdir = TempDir::new()?;
    drop(open(tempdir.path(), ONLY_A_V1)?);
    let store = open(tempdir.path(), &[CONSUMER_A_OWN_UNDO_INDEX])?;
    store.put_consumer(CONSUMER_A_CF, b"key-a", b"value-a")?;
    drop(store);
    open(tempdir.path(), ONLY_A_V1)?;
    Ok(())
}

#[test]
fn transferring_a_column_family_to_a_new_consumer_fails_closed() -> Result<()> {
    let tempdir = TempDir::new()?;
    {
        let store = open(tempdir.path(), ONLY_A_V1)?;
        store.put_consumer(CONSUMER_A_CF, b"key-a", b"value-a")?;
    }

    let outcome = MaterializedViewStore::open(tempdir.path(), options(ONLY_C_ON_A_CF));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ConsumerNotDeclared {
            consumer,
            persisted_schema_version: 1,
        }) if consumer == CONSUMER_A.as_str()
    ));

    let store = open(tempdir.path(), ONLY_A_V1)?;
    assert_eq!(
        store.get_consumer(CONSUMER_A_CF, b"key-a")?,
        Some(b"value-a".to_vec())
    );
    Ok(())
}

#[test]
fn open_secondary_rejects_a_consumer_declared_at_a_newer_version() -> Result<()> {
    let primary = TempDir::new()?;
    let secondary = TempDir::new()?;
    {
        let store = open(primary.path(), ONLY_A_V1)?;
        store.put_consumer(CONSUMER_A_CF, b"key-a", b"value-a")?;
    }

    let outcome =
        MaterializedViewStore::open_secondary(primary.path(), secondary.path(), options(ONLY_A_V2));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ConsumerSchemaMismatch {
            consumer,
            persisted,
            running,
        }) if consumer == CONSUMER_A.as_str() && persisted == Some(1) && running == 2
    ));
    Ok(())
}

#[test]
fn open_secondary_accepts_matching_consumer_versions() -> Result<()> {
    let primary = TempDir::new()?;
    let secondary = TempDir::new()?;
    {
        let store = open(primary.path(), ONLY_A_V1)?;
        store.put_consumer(CONSUMER_A_CF, b"key-a", b"value-a")?;
    }

    let outcome =
        MaterializedViewStore::open_secondary(primary.path(), secondary.path(), options(ONLY_A_V1));
    assert!(outcome.is_ok());
    Ok(())
}

#[test]
fn secondary_open_rejects_unknown_manifest_consumer() -> Result<()> {
    let tempdir = TempDir::new()?;
    {
        let store = open(tempdir.path(), BOTH_V1)?;
        store.put_consumer(CONSUMER_B_CF, b"key-b", b"value-b")?;
    }

    let secondary = TempDir::new()?;
    let outcome =
        MaterializedViewStore::open_secondary(tempdir.path(), secondary.path(), options(ONLY_A_V1));
    assert!(matches!(
        outcome,
        Err(MaterializedViewStoreError::ConsumerNotDeclared {
            consumer,
            persisted_schema_version: 1,
        }) if consumer == CONSUMER_B.as_str()
    ));

    let store = open(tempdir.path(), BOTH_V1)?;
    assert_eq!(
        store.get_consumer(CONSUMER_B_CF, b"key-b")?,
        Some(b"value-b".to_vec())
    );
    Ok(())
}
