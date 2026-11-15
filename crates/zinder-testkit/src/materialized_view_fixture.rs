//! Materialized-view store fixtures for tests that exercise materialized projections.

use std::{collections::BTreeMap, path::Path};

use thiserror::Error;
use zinder_core::{
    BlockHeight, TransparentSpendFact,
    wire::{encode_height_key_ascending, encode_outpoint_key},
};
use zinder_materialized_views::{
    MaterializedViewStore, MaterializedViewStoreOptions, TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILY,
    TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY, encode_transparent_spend_row_value,
};

/// Failure returned while preparing a materialized-view fixture.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MaterializedViewFixtureError {
    /// Materialized-view store could not be opened or written.
    #[error(transparent)]
    Store(#[from] zinder_materialized_views::MaterializedViewStoreError),
}

/// Opens the materialized-view primary paired with `canonical_path` for tests.
///
/// Production processes use `zinder-ingest` as the materialized-view writer. Tests that
/// construct canonical rows directly use this helper to keep the paired materialized-view
/// projection explicit.
///
/// # Errors
///
/// Returns [`MaterializedViewFixtureError`] when the materialized-view store cannot be opened.
pub fn open_test_materialized_view_store_for_canonical(
    canonical_path: &Path,
) -> Result<MaterializedViewStore, MaterializedViewFixtureError> {
    Ok(MaterializedViewStore::open(
        MaterializedViewStore::path_for_canonical(canonical_path),
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?)
}

/// Seeds durable transparent-outpoint-spend rows in a materialized-view store.
///
/// Writes the primary spend rows keyed by spent outpoint plus the per-height
/// index rows the projection uses for rewind and freshness. Intended for query
/// and client tests that populate the projection without running the ingest
/// materialized-view tailer. Row values reuse the consumer's
/// [`encode_transparent_spend_row_value`] so the seeded bytes never diverge
/// from what the consumer writes.
///
/// # Errors
///
/// Returns [`MaterializedViewFixtureError`] when any materialized-view write fails.
pub fn seed_transparent_outpoint_spends(
    materialized_view_store: &MaterializedViewStore,
    spends: &[TransparentSpendFact],
) -> Result<(), MaterializedViewFixtureError> {
    let mut index_payloads_by_height = BTreeMap::<BlockHeight, Vec<u8>>::new();
    for spend in spends {
        let key = encode_outpoint_key(spend.spent_outpoint);
        materialized_view_store.put_consumer(
            TRANSPARENT_OUTPOINT_SPEND_COLUMN_FAMILY,
            &key,
            &encode_transparent_spend_row_value(spend),
        )?;
        index_payloads_by_height
            .entry(spend.block_height)
            .or_default()
            .extend_from_slice(&key);
    }
    for (height, index_payload) in index_payloads_by_height {
        materialized_view_store.put_consumer(
            TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY,
            &encode_height_key_ascending(height),
            &index_payload,
        )?;
    }
    Ok(())
}
