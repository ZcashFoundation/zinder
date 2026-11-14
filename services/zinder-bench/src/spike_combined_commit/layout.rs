//! Column-family layout, store copies, and writer budgets.

use std::{
    fs,
    path::{Path, PathBuf},
};

use clap::ValueEnum;
use rust_rocksdb::{Cache, ColumnFamilyDescriptor, DB, DBCompressionType, Options};
use serde::Serialize;
use zinder_bench::BenchError;
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore, RocksDbResourceBudget, RocksDbStatisticsLevel,
    build_block_based_table_factory,
};
use zinder_core::NetworkUpgradeActivations;
use zinder_wallet_rocksdb::{
    spike_wallet_column_family_descriptors, spike_wallet_column_family_names,
    spike_wallet_data_column_family_names, spike_wallet_data_options,
};

use super::spike_error;

/// The fifteen canonical data families (mirrors `CANONICAL_DATA_COLUMN_FAMILIES`).
pub(crate) const CANONICAL_DATA_FAMILIES: [&str; 15] = [
    "block_header",
    "block_hash_index",
    "block_replay",
    "daily_value_pool_balance",
    "transaction_location",
    "compact_block",
    "tree_state_checkpoint",
    "block_final_note_commitment_roots",
    "subtree_root",
    "chain_epoch",
    "chain_event",
    "mempool_event",
    "displaced_block_facts",
    "block_blob",
    "transaction_blob",
];

const MIRROR_PREFIX: &str = "mirror_";
const MIB: u64 = 1024 * 1024;

/// Writer budget choice for the combined database.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum BudgetProfile {
    /// Canonical writer defaults plus the wallet writer's cache, memtable,
    /// WAL, open-file, and background-job budgets (two databases' worth).
    Summed,
    /// Canonical writer defaults only.
    Canonical,
}

/// `RocksDB` statistics level.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum StatisticsLevelArg {
    /// Tickers only (production default).
    Tickers,
    /// Tickers and histograms.
    Full,
}

/// Which extra families a database carries.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct Layout {
    /// Wallet row families and the wallet control family.
    pub(crate) wallet: bool,
    /// `mirror_*` families for density and pressure lanes.
    pub(crate) mirrors: bool,
}

impl Layout {
    pub(crate) fn extra_names(self) -> Vec<String> {
        let mut names = Vec::new();
        if self.wallet {
            names.extend(spike_wallet_column_family_names());
        }
        if self.mirrors {
            names.extend(CANONICAL_DATA_FAMILIES.iter().map(|name| mirror_family(name)));
            if self.wallet {
                names.extend(
                    spike_wallet_data_column_family_names()
                        .iter()
                        .map(|name| mirror_family(name)),
                );
            }
        }
        names
    }

    /// Registers this layout with the canonical store's open and admission.
    pub(crate) fn register(self, wallet_budget: RocksDbResourceBudget) {
        let names = self.extra_names();
        if names.is_empty() {
            zinder_store::spike_clear_extra_column_families();
            return;
        }
        let layout = self;
        zinder_store::spike_set_extra_column_families(names, move |cache, canonical_budget| {
            let mut descriptors = Vec::new();
            if layout.wallet {
                descriptors.extend(spike_wallet_column_family_descriptors(cache, wallet_budget));
            }
            if layout.mirrors {
                for name in CANONICAL_DATA_FAMILIES {
                    descriptors.push(ColumnFamilyDescriptor::new(
                        mirror_family(name),
                        canonical_like_options(cache, canonical_budget),
                    ));
                }
                if layout.wallet {
                    for name in spike_wallet_data_column_family_names() {
                        descriptors.push(ColumnFamilyDescriptor::new(
                            mirror_family(name),
                            spike_wallet_data_options(cache, wallet_budget),
                        ));
                    }
                }
            }
            descriptors
        });
    }
}

/// Same options as `canonical_data_options`.
fn canonical_like_options(cache: &Cache, budget: RocksDbResourceBudget) -> Options {
    let mut options = Options::default();
    options.set_compression_type(DBCompressionType::Snappy);
    options.set_block_based_table_factory(&build_block_based_table_factory(cache));
    options.set_write_buffer_size(usize::try_from(budget.write_buffer_bytes).unwrap_or(usize::MAX));
    options.set_max_write_buffer_number(budget.max_write_buffer_count);
    options
}

pub(crate) fn mirror_family(name: &str) -> String {
    format!("{MIRROR_PREFIX}{name}")
}

/// Creates any missing column family with a plain open (rows untouched).
pub(crate) fn ensure_column_families(
    path: &Path,
    names: &[String],
) -> Result<Vec<String>, BenchError> {
    let existing = DB::list_cf(&Options::default(), path)
        .map_err(|source| spike_error("list column families", source))?;
    let missing = names
        .iter()
        .filter(|name| !existing.contains(name))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(missing);
    }
    let mut options = Options::default();
    options.create_if_missing(false);
    options.create_missing_column_families(true);
    let all = existing.iter().chain(missing.iter()).cloned().collect::<Vec<_>>();
    let db = DB::open_cf(&options, path, &all)
        .map_err(|source| spike_error("create column families", source))?;
    drop(db);
    Ok(missing)
}

/// Bytes and files reproduced by one store copy.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct CopyStats {
    pub(crate) hard_linked_files: u64,
    pub(crate) hard_linked_bytes: u64,
    pub(crate) copied_files: u64,
    pub(crate) copied_bytes: u64,
    pub(crate) seconds: f64,
}

/// Copies a closed `RocksDB` directory, hard-linking immutable SST files.
///
/// This is what a `RocksDB` checkpoint does on one filesystem. The source is
/// never opened or written.
pub(crate) fn copy_closed_store(source: &Path, target: &Path) -> Result<CopyStats, BenchError> {
    let started = std::time::Instant::now();
    if target.symlink_metadata().is_ok() {
        return Err(BenchError::invalid_argument(format!(
            "copy target {} already exists",
            target.display()
        )));
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|source| BenchError::io(parent, source))?;
    }
    fs::create_dir(target).map_err(|source| BenchError::io(target, source))?;
    let mut stats = CopyStats::default();
    let entries = fs::read_dir(source).map_err(|error| BenchError::io(source, error))?;
    for entry in entries {
        let entry = entry.map_err(|error| BenchError::io(source, error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| BenchError::io(entry.path(), error))?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy().to_string();
        if name == "LOCK" || name == "LOG" || name.starts_with("LOG.old") {
            continue;
        }
        let from = entry.path();
        let to = target.join(&name);
        let bytes = entry
            .metadata()
            .map_err(|error| BenchError::io(&from, error))?
            .len();
        if name.ends_with(".sst") && fs::hard_link(&from, &to).is_ok() {
            stats.hard_linked_files += 1;
            stats.hard_linked_bytes += bytes;
            continue;
        }
        fs::copy(&from, &to).map_err(|error| BenchError::io(&to, error))?;
        stats.copied_files += 1;
        stats.copied_bytes += bytes;
    }
    stats.seconds = started.elapsed().as_secs_f64();
    Ok(stats)
}

/// Apparent size of a directory's regular files.
pub(crate) fn directory_bytes(path: &Path) -> u64 {
    fs::read_dir(path).map_or(0, |entries| {
        entries
            .flatten()
            .filter_map(|entry| entry.metadata().ok())
            .filter(fs::Metadata::is_file)
            .map(|metadata| metadata.len())
            .sum()
    })
}

/// Writer budget for a database in `mode`.
pub(crate) fn writer_budget(
    combined: bool,
    profile: BudgetProfile,
    max_background_jobs: Option<i32>,
    statistics_level: StatisticsLevelArg,
) -> RocksDbResourceBudget {
    let canonical = RocksDbResourceBudget::canonical_writer_defaults();
    let wallet = RocksDbResourceBudget::wallet_projection_writer_defaults();
    let mut budget = if combined && profile == BudgetProfile::Summed {
        RocksDbResourceBudget {
            block_cache_bytes: canonical.block_cache_bytes + wallet.block_cache_bytes,
            max_wal_bytes: canonical.max_wal_bytes + wallet.max_wal_bytes,
            max_open_files: canonical.max_open_files + wallet.max_open_files,
            max_background_jobs: canonical.max_background_jobs + wallet.max_background_jobs,
            memtable_budget_bytes: canonical.memtable_budget_bytes + wallet.memtable_budget_bytes,
            ..canonical
        }
    } else {
        canonical
    };
    if let Some(jobs) = max_background_jobs {
        budget.max_background_jobs = jobs;
    }
    budget.statistics_level = match statistics_level {
        StatisticsLevelArg::Tickers => RocksDbStatisticsLevel::Tickers,
        StatisticsLevelArg::Full => RocksDbStatisticsLevel::Full,
    };
    budget
}

/// Serializable view of a resource budget.
#[derive(Clone, Copy, Debug, Serialize)]
pub(crate) struct BudgetView {
    pub(crate) block_cache_mib: u64,
    pub(crate) max_wal_mib: u64,
    pub(crate) max_open_files: i32,
    pub(crate) write_buffer_mib: u64,
    pub(crate) max_write_buffer_count: i32,
    pub(crate) max_background_jobs: i32,
    pub(crate) memtable_budget_mib: u64,
    pub(crate) statistics_level: &'static str,
}

impl From<RocksDbResourceBudget> for BudgetView {
    fn from(budget: RocksDbResourceBudget) -> Self {
        Self {
            block_cache_mib: budget.block_cache_bytes / MIB,
            max_wal_mib: budget.max_wal_bytes / MIB,
            max_open_files: budget.max_open_files,
            write_buffer_mib: budget.write_buffer_bytes / MIB,
            max_write_buffer_count: budget.max_write_buffer_count,
            max_background_jobs: budget.max_background_jobs,
            memtable_budget_mib: budget.memtable_budget_bytes / MIB,
            statistics_level: match budget.statistics_level {
                RocksDbStatisticsLevel::Off => "off",
                RocksDbStatisticsLevel::Tickers => "tickers",
                RocksDbStatisticsLevel::Full => "full",
            },
        }
    }
}

pub(crate) fn wallet_family_budget() -> RocksDbResourceBudget {
    RocksDbResourceBudget::wallet_projection_writer_defaults()
}

/// Opens a READY wallet-serving canonical store with the registered layout.
pub(crate) fn open_canonical(
    path: &Path,
    activations: &NetworkUpgradeActivations,
    reorg_window_blocks: u32,
    budget: RocksDbResourceBudget,
) -> Result<RocksDbCanonicalStore, BenchError> {
    Ok(RocksDbCanonicalStore::open_ready(
        path,
        activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(reorg_window_blocks)?,
        budget,
    )?)
}

/// Opens a canonical secondary over a primary with the registered layout.
pub(crate) fn open_secondary(
    primary: &Path,
    secondary: &Path,
    activations: &NetworkUpgradeActivations,
    reorg_window_blocks: u32,
) -> Result<RocksDbCanonicalSecondary, BenchError> {
    fs::create_dir_all(secondary).map_err(|source| BenchError::io(secondary, source))?;
    Ok(RocksDbCanonicalSecondary::open_ready(
        primary,
        secondary,
        activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(reorg_window_blocks)?,
        RocksDbResourceBudget::canonical_reader_defaults(),
    )?)
}

pub(crate) fn absolute(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}
