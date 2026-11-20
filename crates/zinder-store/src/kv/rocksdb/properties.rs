use std::{collections::HashSet, sync::LazyLock};

use parking_lot::Mutex;
use rust_rocksdb::{AsColumnFamilyRef, DB, properties};

use super::{RocksDbResourceGaugeInputs, StoreRole, u64_to_f64};

pub(super) fn record_cache_entries(db: &DB, role: StoreRole) {
    let Ok(Some(property)) = db.property_value("rocksdb.block-cache-entry-stats") else {
        return;
    };
    let Some((_, entries)) = property.split_once("Block cache entry stats(count,size,portion):")
    else {
        return;
    };
    // RocksDB omits empty roles and rounds each charge to two binary-unit decimals.
    for entry_role in [
        "DataBlock",
        "FilterBlock",
        "FilterMetaBlock",
        "DeprecatedFilterBlock",
        "IndexBlock",
        "OtherBlock",
        "WriteBuffer",
        "CompressionDictionaryBuildingBuffer",
        "FilterConstruction",
        "BlockBasedTableReader",
        "FileMetadata",
        "BlobValue",
        "BlobCache",
        "Misc",
    ] {
        let entry = entries
            .split(')')
            .filter_map(|entry| entry.trim().split_once('('))
            .find(|(name, _)| *name == entry_role);
        let (count, bytes) = match entry {
            Some((_, fields)) => {
                let mut fields = fields.split(',');
                let Some(count) = fields.next().and_then(|n| n.parse::<u64>().ok()) else {
                    continue;
                };
                let Some(bytes) = fields.next().and_then(cache_charge_bytes) else {
                    continue;
                };
                (count, bytes)
            }
            None => (0, 0.0),
        };
        metrics::gauge!(
            "zinder_store_block_cache_entries",
            "store_role" => role.as_str(), "entry_role" => entry_role
        )
        .set(u64_to_f64(count));
        metrics::gauge!(
            "zinder_store_block_cache_entry_bytes",
            "store_role" => role.as_str(), "entry_role" => entry_role
        )
        .set(bytes);
    }
}

fn cache_charge_bytes(charge: &str) -> Option<f64> {
    let (amount, unit) = charge.split_once(' ')?;
    let scale = match unit {
        "KB" => 1024.0,
        "MB" => 1024.0 * 1024.0,
        "GB" => 1024.0 * 1024.0 * 1024.0,
        "TB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    Some(amount.parse::<f64>().ok()? * scale)
}

// Gauge registrations outlive database handles. Remember occupied levels so a
// compaction or store reopen clears their last samples without registering empty levels.
type OccupiedLevel = (&'static str, &'static str, usize);
static OCCUPIED_LEVELS: LazyLock<Mutex<HashSet<OccupiedLevel>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

pub(super) fn record_level_gauges(
    inputs: &RocksDbResourceGaugeInputs<'_>,
    cf: &'static str,
    family: &impl AsColumnFamilyRef,
) {
    let role = inputs.store_role.as_str();
    for level in 0..=6 {
        let files = properties::num_files_at_level(level);
        let Ok(Some(count)) = inputs.db.property_int_value_cf(family, &files) else {
            continue;
        };
        let key = (role, cf, level);
        let drained = if count == 0 {
            OCCUPIED_LEVELS.lock().remove(&key)
        } else {
            OCCUPIED_LEVELS.lock().insert(key);
            false
        };
        if level == 0 || count > 0 || drained {
            metrics::gauge!(
                "zinder_store_rocksdb_property",
                "property" => files.as_str().to_owned(), "cf" => cf, "store_role" => role
            )
            .set(u64_to_f64(count));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::cache_charge_bytes;

    #[test]
    fn cache_charges_use_rocksdb_binary_units() {
        assert_eq!(cache_charge_bytes("1.50 KB"), Some(1536.0));
        assert_eq!(cache_charge_bytes("0.25 MB"), Some(262_144.0));
        assert_eq!(cache_charge_bytes("1.00 GB"), Some(1_073_741_824.0));
        assert_eq!(cache_charge_bytes("0.50 TB"), Some(549_755_813_888.0));
    }
}
