//! Spike-only hooks for in-process wallet serving and index attach.
//!
//! Throwaway code for the in-process serving spike: shared primary handle,
//! a physical column-family alias so a rebuilt family can replace a live one,
//! and an entry point that writes the real projection SSTs from any
//! authenticated canonical replay source without building a whole store.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use parking_lot::RwLock;
use rust_rocksdb::{DB, Options};
use zinder_store::{RocksDbResourceBudget, SpikeSnapshot, spike_read_options};
use zinder_wallet_projection::{
    WALLET_STORE_CONTROL_KEY, WalletProjectionBuildState, WalletProjectionFamilyRowCounts,
    WalletProjectionReadyEvidence, WalletStoreControlRecord, WalletUtxoSetSummary,
};

use crate::{
    RocksDbWalletBuildOptions, RocksDbWalletError, RocksDbWalletFollowingStore,
    WalletProjectionReplaySource,
    projection_load::{WalletProjectionLoadConfig, write_projection_ssts},
    store::wallet_data_options,
};

/// Logical wallet family names, in stored order.
pub const SPIKE_WALLET_FAMILIES: [&str; 6] = [
    crate::store::TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
    crate::store::TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
    crate::store::TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
    crate::store::TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
    crate::store::TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
    crate::store::REORG_UNDO_COLUMN_FAMILY,
];

static SPIKE_FAMILY_ALIASES: RwLock<Option<HashMap<&'static str, String>>> = RwLock::new(None);

/// Points one logical wallet family at a different physical column family.
///
/// `None` restores the logical name. The writer must call this between
/// transitions; readers resolve through their own snapshot instead.
pub fn spike_set_family_alias(logical: &'static str, physical: Option<String>) {
    let mut aliases = SPIKE_FAMILY_ALIASES.write();
    let map = aliases.get_or_insert_with(HashMap::new);
    match physical {
        Some(physical) => {
            map.insert(logical, physical);
        }
        None => {
            map.remove(logical);
        }
    }
}

/// Returns the physical column family currently used for a logical family.
#[must_use]
pub fn spike_resolve_family(logical: &'static str) -> String {
    SPIKE_FAMILY_ALIASES
        .read()
        .as_ref()
        .and_then(|map| map.get(logical).cloned())
        .unwrap_or_else(|| logical.to_owned())
}

impl RocksDbWalletFollowingStore {
    /// Spike: returns the shared primary handle for in-process snapshot readers.
    #[must_use]
    pub fn spike_shared_db(&self) -> Arc<DB> {
        Arc::clone(&self.store.bounded_open.db)
    }

    /// Spike: returns the retained open options that carry the statistics tickers.
    #[must_use]
    pub fn spike_statistics(&self) -> Arc<Options> {
        Arc::clone(&self.store.bounded_open.statistics)
    }

    /// Spike: production wallet data-family options on this store's block cache.
    #[must_use]
    pub fn spike_data_cf_options(&self, budget: RocksDbResourceBudget) -> Options {
        wallet_data_options(&self.store.bounded_open.block_cache, budget)
    }
}

/// Decodes READY evidence from the control record visible in one view.
pub fn spike_wallet_ready_evidence_at(
    db: &DB,
    snapshot: Option<&SpikeSnapshot<'_>>,
) -> Result<WalletProjectionReadyEvidence, RocksDbWalletError> {
    let encoded = db
        .get_opt(
            WALLET_STORE_CONTROL_KEY,
            &spike_read_options(snapshot, true),
        )
        .map_err(|source| RocksDbWalletError::rocksdb("spike control read", source))?
        .ok_or(RocksDbWalletError::StoreControlMissing)?;
    let control = WalletStoreControlRecord::decode(&encoded)?;
    if let WalletProjectionBuildState::Ready(evidence) = control.build_state {
        Ok(evidence)
    } else {
        Err(RocksDbWalletError::AdmissionChanged {
            reason: "spike: wallet control is not READY",
        })
    }
}

/// Projection SST files for every family, written from one canonical source.
pub struct SpikeProjectionSsts {
    /// Tip the files reproduce.
    pub tip: zinder_core::BlockId,
    /// Per-family external SST paths, by logical family name.
    pub families: Vec<(&'static str, Vec<PathBuf>)>,
    /// Row counts the files carry.
    pub row_counts: WalletProjectionFamilyRowCounts,
    /// UTXO summary the files carry.
    pub utxo_summary: WalletUtxoSetSummary,
    /// Total SST bytes written.
    pub sst_file_bytes: u64,
    /// Total SST files written.
    pub sst_file_count: u64,
    /// Logical row bytes written.
    pub logical_row_bytes: u64,
    /// Canonical blocks scanned.
    pub scanned_block_count: u64,
    /// Canonical transactions scanned.
    pub scanned_transaction_count: u64,
    /// Historical prevout reads (must stay zero).
    pub historical_prevout_read_count: u64,
    /// Canonical scan duration.
    pub canonical_scan: Duration,
    /// Outpoint sort duration.
    pub outpoint_sort: Duration,
    /// Outpoint merge duration.
    pub outpoint_merge: Duration,
    /// Secondary row derivation duration.
    pub secondary_row_derivation: Duration,
    /// Logical evidence duration.
    pub logical_evidence: Duration,
}

/// Writes the real wallet projection SSTs from one canonical replay source.
pub fn spike_write_projection_ssts<Source>(
    canonical: &Source,
    staging_path: &Path,
    options: RocksDbWalletBuildOptions,
    data_options: &Options,
) -> Result<SpikeProjectionSsts, RocksDbWalletError>
where
    Source: WalletProjectionReplaySource + ?Sized,
{
    let ready = canonical.wallet_projection_ready_evidence();
    let mut prepared = write_projection_ssts(
        WalletProjectionLoadConfig {
            staging_path,
            options: data_options,
            network: canonical.wallet_projection_network(),
            first_retained_block: ready.first_retained_block,
            settled_tip: ready.sequence_checkpoint.through(),
            supported_reorg_depth: options.supported_reorg_depth,
            max_outpoint_sort_memory_bytes: options.max_outpoint_sort_memory_bytes,
            max_secondary_sort_memory_bytes_per_sorter: options
                .max_secondary_sort_memory_bytes_per_sorter,
            max_temporary_file_bytes_per_sorter: options.max_temporary_file_bytes_per_sorter,
            sst_target_logical_bytes: options.sst_target_logical_bytes,
            max_accounted_reorg_undo_bytes: options.max_accounted_reorg_undo_bytes,
        },
        canonical
            .wallet_projection_scan()
            .map_err(|source| RocksDbWalletError::CanonicalReplay { source })?,
    )?;
    let families = std::mem::take(&mut prepared.families)
        .into_iter()
        .map(|family| (family.name, family.paths))
        .collect();
    Ok(SpikeProjectionSsts {
        tip: prepared.tip,
        families,
        row_counts: prepared.row_counts,
        utxo_summary: prepared.utxo_summary.clone(),
        sst_file_bytes: prepared.sst_file_bytes,
        sst_file_count: prepared.sst_file_count,
        logical_row_bytes: prepared.logical_row_bytes,
        scanned_block_count: prepared.counters.scanned_block_count,
        scanned_transaction_count: prepared.counters.scanned_transaction_count,
        historical_prevout_read_count: prepared.counters.historical_prevout_read_count,
        canonical_scan: prepared.phase_durations.canonical_scan,
        outpoint_sort: prepared.phase_durations.outpoint_sort,
        outpoint_merge: prepared.phase_durations.outpoint_merge,
        secondary_row_derivation: prepared.phase_durations.secondary_row_derivation,
        logical_evidence: prepared.phase_durations.logical_evidence,
    })
}
