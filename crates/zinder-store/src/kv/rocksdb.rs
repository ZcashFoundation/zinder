use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use rust_rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompressionType,
    DEFAULT_COLUMN_FAMILY_NAME, Options, WriteBufferManager,
    statistics::{StatsLevel, Ticker},
};

use crate::{RocksDbResourceBudget, RocksDbStatisticsLevel};

const DIRECT_IO_COMPACTION_READAHEAD_BYTES: usize = 2 * 1024 * 1024;
const SECONDARY_MAX_FILE_OPENING_THREADS: i32 = 1;

/// Filesystem I/O mode of a `RocksDB` instance: the mode a resource budget
/// requests, or the mode an open resolved to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RocksDbIoMode {
    /// Direct reads, and for primary stores direct flush and compaction I/O.
    /// An open that requests it falls back to [`Self::Buffered`] when the
    /// filesystem or platform rejects direct I/O.
    Direct,
    /// Buffered filesystem I/O through the page cache. An open resolves to it
    /// when the budget requests it or when direct I/O was rejected.
    Buffered,
}

impl RocksDbIoMode {
    /// Stable label used in logs and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Buffered => "buffered",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RocksDbOpenPosture {
    Primary,
    Secondary,
}

impl RocksDbOpenPosture {
    const fn effective_max_background_jobs(
        self,
        resource_budget: RocksDbResourceBudget,
    ) -> Option<i32> {
        match self {
            Self::Primary => Some(resource_budget.max_background_jobs),
            Self::Secondary => None,
        }
    }

    const fn effective_max_file_opening_threads(self) -> Option<i32> {
        match self {
            Self::Primary => None,
            Self::Secondary => Some(SECONDARY_MAX_FILE_OPENING_THREADS),
        }
    }
}

/// Store domain and open posture used to label `RocksDB` resource gauges.
///
/// The canonical chain store and the materialized-view store share one process, so the
/// cache, memtable, WAL, and per-CF gauges carry a `store_role` label to keep
/// their resident footprints distinguishable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreRole {
    /// Canonical chain store opened as the primary writer.
    CanonicalPrimary,
    /// Canonical chain store opened as a secondary reader.
    CanonicalSecondary,
    /// Materialized-view store opened as the primary writer.
    MaterializedViewPrimary,
    /// Materialized-view store opened as a secondary reader.
    MaterializedViewSecondary,
    /// Wallet-projection store opened as a secondary reader.
    WalletSecondary,
}

impl StoreRole {
    /// Stable label used in metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CanonicalPrimary => "canonical_primary",
            Self::CanonicalSecondary => "canonical_secondary",
            Self::MaterializedViewPrimary => "materialized_view_primary",
            Self::MaterializedViewSecondary => "materialized_view_secondary",
            Self::WalletSecondary => "wallet_secondary",
        }
    }

    const fn open_posture(self) -> RocksDbOpenPosture {
        match self {
            Self::CanonicalPrimary | Self::MaterializedViewPrimary => RocksDbOpenPosture::Primary,
            Self::CanonicalSecondary | Self::MaterializedViewSecondary | Self::WalletSecondary => {
                RocksDbOpenPosture::Secondary
            }
        }
    }
}

/// A bounded LRU block cache that several `RocksDB` instances of one store
/// read through.
///
/// `RocksDB` keys a cached block by the identity of the table file that holds
/// it, so instances that follow one primary share the cached blocks of every
/// file they both reference. Cloning the handle shares the cache, and the
/// cache lives until the last handle and the last instance opened on it drop.
///
/// The capacity is a target, not a hard limit. Index and filter blocks that
/// open table readers pin can take usage past it; a strict limit would fail
/// reads and table opens instead.
#[derive(Clone)]
pub struct SharedBlockCache {
    cache: Cache,
}

impl SharedBlockCache {
    /// Builds a cache of `capacity_bytes`.
    #[must_use]
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            cache: build_block_cache(capacity_bytes),
        }
    }

    /// Returns the capacity the cache evicts toward, in bytes.
    #[must_use]
    pub fn capacity_bytes(&self) -> u64 {
        usize_to_u64(self.cache.get_capacity())
    }

    /// Returns the bytes the cache holds, pinned entries included.
    #[must_use]
    pub fn usage_bytes(&self) -> u64 {
        usize_to_u64(self.cache.get_usage())
    }

    /// Returns the bytes of entries that an open instance currently holds.
    #[must_use]
    pub fn pinned_usage_bytes(&self) -> u64 {
        usize_to_u64(self.cache.get_pinned_usage())
    }

    fn has_capacity(&self, capacity_bytes: u64) -> bool {
        self.cache.get_capacity() == block_cache_capacity(capacity_bytes)
    }
}

impl std::fmt::Debug for SharedBlockCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedBlockCache")
            .field("capacity_bytes", &self.capacity_bytes())
            .field("usage_bytes", &self.usage_bytes())
            .finish()
    }
}

/// The bounded resources one `RocksDB` open applies.
///
/// A [`RocksDbResourceBudget`] converts into resources whose open builds a
/// block cache of its own. [`Self::with_shared_block_cache`] instead opens the
/// instance on a cache that other instances of the same store read through.
#[derive(Clone, Debug)]
pub struct RocksDbOpenResources {
    budget: RocksDbResourceBudget,
    shared_block_cache: Option<SharedBlockCache>,
}

impl RocksDbOpenResources {
    /// Resources that open an instance on `block_cache` instead of a cache of
    /// its own.
    ///
    /// The budget's [`RocksDbResourceBudget::block_cache_bytes`] must equal
    /// the cache's capacity; [`Self::validate`] rejects the resources
    /// otherwise.
    #[must_use]
    pub fn with_shared_block_cache(
        budget: RocksDbResourceBudget,
        block_cache: &SharedBlockCache,
    ) -> Self {
        Self {
            budget,
            shared_block_cache: Some(block_cache.clone()),
        }
    }

    /// Returns the bounded resource budget.
    #[must_use]
    pub const fn budget(&self) -> RocksDbResourceBudget {
        self.budget
    }

    /// Validates the budget and, for a shared block cache, that the budget
    /// names the cache's capacity.
    pub fn validate(&self) -> Result<(), &'static str> {
        self.budget.validate()?;
        if let Some(block_cache) = &self.shared_block_cache
            && !block_cache.has_capacity(self.budget.block_cache_bytes)
        {
            return Err(
                "rocksdb_resource_budget.block_cache_bytes must equal the capacity of the shared block cache",
            );
        }
        Ok(())
    }
}

impl From<RocksDbResourceBudget> for RocksDbOpenResources {
    fn from(budget: RocksDbResourceBudget) -> Self {
        Self {
            budget,
            shared_block_cache: None,
        }
    }
}

/// Memory one `RocksDB` instance holds outside its block cache.
///
/// Several instances that share one block cache each keep their own
/// memtables and table readers, so a process sums these over its instances to
/// separate them from the cache.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RocksDbInstanceMemory {
    /// Memtable budget of the instance's write-buffer manager.
    pub memtable_budget_bytes: u64,
    /// Memtable memory the write-buffer manager records.
    pub memtable_usage_bytes: u64,
    /// Table-reader memory not charged to the block cache, summed over the
    /// instance's column families (`rocksdb.estimate-table-readers-mem`).
    pub table_reader_bytes: u64,
}

impl std::iter::Sum for RocksDbInstanceMemory {
    fn sum<I: Iterator<Item = Self>>(instances: I) -> Self {
        instances.fold(Self::default(), |total, instance| Self {
            memtable_budget_bytes: total
                .memtable_budget_bytes
                .saturating_add(instance.memtable_budget_bytes),
            memtable_usage_bytes: total
                .memtable_usage_bytes
                .saturating_add(instance.memtable_usage_bytes),
            table_reader_bytes: total
                .table_reader_bytes
                .saturating_add(instance.table_reader_bytes),
        })
    }
}

/// Open role and filesystem paths for a bounded `RocksDB` instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RocksDbOpenRole<'path> {
    /// Primary writer opening its owned store path.
    Primary {
        /// Primary store path.
        path: &'path Path,
    },
    /// Primary writer opening a path whose identity and exact column-family
    /// contract were already admitted.
    ExistingPrimary {
        /// Previously admitted primary store path.
        path: &'path Path,
    },
    /// Secondary reader opening a writer-owned primary path plus its own
    /// reader-local metadata path.
    Secondary {
        /// Writer-owned primary store path.
        primary_path: &'path Path,
        /// Reader-local secondary metadata path.
        secondary_path: &'path Path,
    },
}

impl<'path> RocksDbOpenRole<'path> {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Primary { .. } => "primary",
            Self::ExistingPrimary { .. } => "existing_primary",
            Self::Secondary { .. } => "secondary",
        }
    }

    const fn store_path(self) -> &'path Path {
        match self {
            Self::Primary { path } | Self::ExistingPrimary { path } => path,
            Self::Secondary { primary_path, .. } => primary_path,
        }
    }

    const fn is_primary(self) -> bool {
        matches!(self, Self::Primary { .. } | Self::ExistingPrimary { .. })
    }

    const fn open_posture(self) -> RocksDbOpenPosture {
        match self {
            Self::Primary { .. } | Self::ExistingPrimary { .. } => RocksDbOpenPosture::Primary,
            Self::Secondary { .. } => RocksDbOpenPosture::Secondary,
        }
    }
}

/// Opened `RocksDB` handle plus the resource handles that must outlive it.
pub struct BoundedRocksDbOpen {
    /// Opened database handle.
    pub db: DB,
    /// Block cache every column family reads through, retained for the
    /// database lifetime: the instance's own, or one it shares with other
    /// instances of the same store.
    pub block_cache: Cache,
    /// Shared write-buffer manager retained for the database lifetime.
    pub write_buffer_manager: WriteBufferManager,
    /// Resolved filesystem I/O mode.
    pub io_mode: RocksDbIoMode,
    /// Open options retained so the DB-wide statistics tickers stay readable
    /// for metric export.
    pub statistics: Arc<Options>,
}

impl BoundedRocksDbOpen {
    /// Samples the memory this instance holds outside its block cache.
    #[must_use]
    pub fn instance_memory(&self) -> RocksDbInstanceMemory {
        let table_reader_bytes = self
            .db
            .cf_names()
            .iter()
            .filter_map(|name| self.db.cf_handle(name))
            .filter_map(|column_family| {
                self.db
                    .property_int_value_cf(&column_family, TABLE_READERS_MEMORY_PROPERTY)
                    .ok()
                    .flatten()
            })
            .fold(0, u64::saturating_add);
        RocksDbInstanceMemory {
            memtable_budget_bytes: usize_to_u64(self.write_buffer_manager.get_buffer_size()),
            memtable_usage_bytes: usize_to_u64(self.write_buffer_manager.get_usage()),
            table_reader_bytes,
        }
    }
}

/// Opens a bounded `RocksDB` instance with direct I/O when the platform supports it.
///
/// The open builds a block cache of its own unless `resources` name a
/// [`SharedBlockCache`]. The column-family descriptors are rebuilt for each
/// open attempt because their options hold references to that cache. On
/// direct I/O failure, the function logs the unsupported mode and retries with
/// the same bounded resource budget under buffered I/O. A budget whose
/// [`RocksDbResourceBudget::preferred_io_mode`] is [`RocksDbIoMode::Buffered`]
/// skips the direct attempt.
pub fn open_bounded_rocksdb(
    role: RocksDbOpenRole<'_>,
    resources: impl Into<RocksDbOpenResources>,
    build_column_families: impl Fn(&Cache, RocksDbResourceBudget) -> Vec<ColumnFamilyDescriptor>,
) -> Result<BoundedRocksDbOpen, rust_rocksdb::Error> {
    let RocksDbOpenResources {
        budget: rocksdb_resource_budget,
        shared_block_cache,
    } = resources.into();
    if let RocksDbOpenRole::ExistingPrimary { path } = role {
        DB::list_cf(&Options::default(), path)?;
    }
    let block_cache = shared_block_cache.map_or_else(
        || build_block_cache(rocksdb_resource_budget.block_cache_bytes),
        |shared| shared.cache,
    );
    let write_buffer_manager =
        build_write_buffer_manager(rocksdb_resource_budget.memtable_budget_bytes);
    let open_attempt = BoundedRocksDbOpenAttempt {
        role,
        rocksdb_resource_budget,
        block_cache: &block_cache,
        write_buffer_manager: &write_buffer_manager,
        build_column_families: &build_column_families,
    };
    let (db, statistics, io_mode) = open_with_preferred_io_mode(&open_attempt)?;
    record_rocksdb_io_mode(role, io_mode);
    Ok(BoundedRocksDbOpen {
        db,
        block_cache,
        write_buffer_manager,
        io_mode,
        statistics: Arc::new(statistics),
    })
}

fn open_with_preferred_io_mode<BuildColumnFamilies>(
    open_attempt: &BoundedRocksDbOpenAttempt<'_, '_, BuildColumnFamilies>,
) -> Result<(DB, Options, RocksDbIoMode), rust_rocksdb::Error>
where
    BuildColumnFamilies: Fn(&Cache, RocksDbResourceBudget) -> Vec<ColumnFamilyDescriptor>,
{
    if open_attempt.rocksdb_resource_budget.preferred_io_mode == RocksDbIoMode::Direct {
        match open_bounded_rocksdb_once(open_attempt, RocksDbIoMode::Direct) {
            Ok((db, statistics)) => return Ok((db, statistics, RocksDbIoMode::Direct)),
            Err(direct_error) => {
                tracing::warn!(
                    target: "zinder::store",
                    event = "rocksdb_direct_io_unsupported",
                    store_path = %open_attempt.role.store_path().display(),
                    role = open_attempt.role.as_str(),
                    error = %direct_error,
                    "RocksDB direct I/O open failed; retrying with buffered I/O"
                );
            }
        }
    }
    let (db, statistics) = open_bounded_rocksdb_once(open_attempt, RocksDbIoMode::Buffered)?;
    Ok((db, statistics, RocksDbIoMode::Buffered))
}

struct BoundedRocksDbOpenAttempt<'attempt, 'path, BuildColumnFamilies>
where
    BuildColumnFamilies: Fn(&Cache, RocksDbResourceBudget) -> Vec<ColumnFamilyDescriptor>,
{
    role: RocksDbOpenRole<'path>,
    rocksdb_resource_budget: RocksDbResourceBudget,
    block_cache: &'attempt Cache,
    write_buffer_manager: &'attempt WriteBufferManager,
    build_column_families: &'attempt BuildColumnFamilies,
}

fn open_bounded_rocksdb_once<BuildColumnFamilies>(
    open_attempt: &BoundedRocksDbOpenAttempt<'_, '_, BuildColumnFamilies>,
    io_mode: RocksDbIoMode,
) -> Result<(DB, Options), rust_rocksdb::Error>
where
    BuildColumnFamilies: Fn(&Cache, RocksDbResourceBudget) -> Vec<ColumnFamilyDescriptor>,
{
    let mut db_options = match open_attempt.role {
        RocksDbOpenRole::Primary { .. } => build_primary_db_options(
            open_attempt.rocksdb_resource_budget,
            open_attempt.block_cache,
            true,
        ),
        RocksDbOpenRole::ExistingPrimary { .. } => build_primary_db_options(
            open_attempt.rocksdb_resource_budget,
            open_attempt.block_cache,
            false,
        ),
        RocksDbOpenRole::Secondary { .. } => build_secondary_db_options(
            open_attempt.rocksdb_resource_budget,
            open_attempt.block_cache,
        ),
    };
    if let Some(max_background_jobs) = open_attempt
        .role
        .open_posture()
        .effective_max_background_jobs(open_attempt.rocksdb_resource_budget)
    {
        db_options.set_max_background_jobs(max_background_jobs);
    }
    if let Some(max_file_opening_threads) = open_attempt
        .role
        .open_posture()
        .effective_max_file_opening_threads()
    {
        db_options.set_max_file_opening_threads(max_file_opening_threads);
    }
    db_options.set_write_buffer_manager(open_attempt.write_buffer_manager);
    if io_mode == RocksDbIoMode::Direct {
        db_options.set_use_direct_reads(true);
        if open_attempt.role.is_primary() {
            db_options.set_use_direct_io_for_flush_and_compaction(true);
            db_options.set_compaction_readahead_size(DIRECT_IO_COMPACTION_READAHEAD_BYTES);
        }
    }
    let mut column_families = (open_attempt.build_column_families)(
        open_attempt.block_cache,
        open_attempt.rocksdb_resource_budget,
    );
    if !column_families
        .iter()
        .any(|family| family.name() == DEFAULT_COLUMN_FAMILY_NAME)
    {
        column_families.push(ColumnFamilyDescriptor::new(
            DEFAULT_COLUMN_FAMILY_NAME,
            default_column_family_options(),
        ));
    }

    let db = match open_attempt.role {
        RocksDbOpenRole::Primary { path } | RocksDbOpenRole::ExistingPrimary { path } => {
            DB::open_cf_descriptors(&db_options, path, column_families)
        }
        RocksDbOpenRole::Secondary {
            primary_path,
            secondary_path,
        } => DB::open_cf_descriptors_as_secondary(
            &db_options,
            primary_path,
            secondary_path,
            column_families,
        ),
    }?;
    Ok((db, db_options))
}

/// Options for the `default` family, which holds the canonical and wallet
/// control rows.
///
/// Without a descriptor of its own, `rust-rocksdb` opens this family with
/// `Options::default()`, whose compression `RocksDB` 11.5 changed from Snappy
/// to LZ4. Pinning Snappy keeps new control-row files in the codec that
/// existing stores already hold; every other option keeps `RocksDB`'s default.
fn default_column_family_options() -> Options {
    let mut options = Options::default();
    options.set_compression_type(DBCompressionType::Snappy);
    options
}

fn record_rocksdb_io_mode(role: RocksDbOpenRole<'_>, io_mode: RocksDbIoMode) {
    tracing::info!(
        target: "zinder::store",
        event = "rocksdb_io_mode",
        store_path = %role.store_path().display(),
        role = role.as_str(),
        mode = io_mode.as_str(),
        "RocksDB I/O mode resolved"
    );
}

/// Inputs for [`record_rocksdb_resource_gauges`].
pub struct RocksDbResourceGaugeInputs<'sample> {
    /// Opened database whose properties are sampled.
    pub db: &'sample DB,
    /// Store domain and open posture, emitted as the `store_role` label.
    pub store_role: StoreRole,
    /// Column families to probe for the per-CF integer properties.
    pub column_family_names: &'sample [&'static str],
    /// Shared block cache retained for the database lifetime.
    pub block_cache: &'sample Cache,
    /// Shared write-buffer manager retained for the database lifetime.
    pub write_buffer_manager: &'sample WriteBufferManager,
    /// Statistics-bearing open options whose DB-wide tickers are exported.
    pub statistics: &'sample Options,
    /// Resolved filesystem I/O mode.
    pub io_mode: RocksDbIoMode,
    /// Bounded resource budget applied at open.
    pub resource_budget: RocksDbResourceBudget,
}

/// Walks the on-disk WAL files and returns their combined size in bytes.
///
/// Returns `None` when the store path is unreadable; the metric scrape treats
/// that as "no sample" rather than failing closed.
fn wal_size_bytes(db: &DB) -> Option<u64> {
    let entries = fs::read_dir(db.path()).ok()?;
    let mut total_bytes = 0_u64;
    for entry in entries.flatten() {
        let entry_path = entry.path();
        let Some(extension) = entry_path.extension().and_then(|ext| ext.to_str()) else {
            continue;
        };
        if !extension.eq_ignore_ascii_case("log") {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        total_bytes = total_bytes.saturating_add(metadata.len());
    }
    Some(total_bytes)
}

/// Publishes the bounded-`RocksDB` resource gauges under a `store_role` label.
///
/// Shared by the canonical chain store and the materialized-view store so both instances
/// contribute distinguishable cache, memtable, WAL, and per-CF footprints to
/// the same series. `store_role` keeps the aggregate resident set attributable
/// to each instance. Primary-only controls are omitted for secondary roles.
pub fn record_rocksdb_resource_gauges(inputs: &RocksDbResourceGaugeInputs<'_>) {
    let store_role = inputs.store_role.as_str();
    for cf_name in inputs.column_family_names {
        let Some(column_family) = inputs.db.cf_handle(cf_name) else {
            continue;
        };
        for property in PER_CF_INT_PROPERTIES {
            let Ok(Some(property_sample)) =
                inputs.db.property_int_value_cf(&column_family, property)
            else {
                continue;
            };
            metrics::gauge!(
                "zinder_store_rocksdb_property",
                "property" => property,
                "cf" => *cf_name,
                "store_role" => store_role
            )
            .set(u64_to_f64(property_sample));
        }
    }
    for property in DB_INT_PROPERTIES {
        let Ok(Some(property_sample)) = inputs.db.property_int_value(property) else {
            continue;
        };
        metrics::gauge!(
            "zinder_store_rocksdb_property",
            "property" => property,
            "cf" => DB_LEVEL_PROPERTY_CF,
            "store_role" => store_role
        )
        .set(u64_to_f64(property_sample));
    }
    if let Some(wal_bytes) = wal_size_bytes(inputs.db) {
        metrics::gauge!("zinder_store_wal_bytes", "store_role" => store_role)
            .set(u64_to_f64(wal_bytes));
    }
    metrics::gauge!("zinder_store_wal_bytes_limit", "store_role" => store_role)
        .set(u64_to_f64(inputs.resource_budget.max_wal_bytes));
    if let Some(max_background_jobs) = inputs
        .store_role
        .open_posture()
        .effective_max_background_jobs(inputs.resource_budget)
    {
        metrics::gauge!("zinder_store_max_background_jobs", "store_role" => store_role)
            .set(f64::from(max_background_jobs));
    }
    record_block_cache_gauges(store_role, inputs.block_cache);
    if inputs.resource_budget.statistics_level != RocksDbStatisticsLevel::Off {
        record_rocksdb_ticker_gauges(inputs.statistics, store_role);
    }
    metrics::gauge!("zinder_store_memtable_budget_bytes", "store_role" => store_role)
        .set(usize_to_f64(inputs.write_buffer_manager.get_buffer_size()));
    metrics::gauge!("zinder_store_memtable_budget_usage_bytes", "store_role" => store_role)
        .set(usize_to_f64(inputs.write_buffer_manager.get_usage()));
    metrics::gauge!(
        "zinder_store_rocksdb_io_mode",
        "mode" => inputs.io_mode.as_str(),
        "store_role" => store_role
    )
    .set(1.0);
}

/// Publishes one shared block cache and the summed memory of the instances
/// that read through it under `store_role`.
///
/// The cache gauges describe the one cache, however many instances share it.
/// `instances` is the sum of every open instance's
/// [`RocksDbInstanceMemory`], so the memtable and table-reader gauges count
/// each instance once. Per-instance properties and tickers are not exported,
/// because the instances would overwrite one another's series.
pub fn record_shared_block_cache_gauges(
    store_role: StoreRole,
    block_cache: &SharedBlockCache,
    instances: RocksDbInstanceMemory,
) {
    let store_role = store_role.as_str();
    record_block_cache_gauges(store_role, &block_cache.cache);
    metrics::gauge!("zinder_store_memtable_budget_bytes", "store_role" => store_role)
        .set(u64_to_f64(instances.memtable_budget_bytes));
    metrics::gauge!("zinder_store_memtable_budget_usage_bytes", "store_role" => store_role)
        .set(u64_to_f64(instances.memtable_usage_bytes));
    metrics::gauge!("zinder_store_table_reader_memory_bytes", "store_role" => store_role)
        .set(u64_to_f64(instances.table_reader_bytes));
}

fn record_block_cache_gauges(store_role: &'static str, block_cache: &Cache) {
    metrics::gauge!("zinder_store_block_cache_capacity_bytes", "store_role" => store_role)
        .set(usize_to_f64(block_cache.get_capacity()));
    metrics::gauge!("zinder_store_block_cache_usage_bytes", "store_role" => store_role)
        .set(usize_to_f64(block_cache.get_usage()));
    metrics::gauge!("zinder_store_block_cache_pinned_usage_bytes", "store_role" => store_role)
        .set(usize_to_f64(block_cache.get_pinned_usage()));
}

/// DB-wide `RocksDB` statistics tickers exported as monotonic gauges.
///
/// Every ticker is aggregated across all column families of one store. The
/// canonical and materialized-view stores that export these series configure
/// no filter, so their `BloomFilter*` entries read zero. The wallet store,
/// whose hash-keyed families carry filters, does not export its tickers here.
const EXPORTED_TICKERS: &[Ticker] = &[
    Ticker::BloomFilterUseful,
    Ticker::BloomFilterFullPositive,
    Ticker::BloomFilterFullTruePositive,
    Ticker::BlockCacheDataHit,
    Ticker::BlockCacheDataMiss,
    Ticker::BlockCacheIndexHit,
    Ticker::BlockCacheIndexMiss,
    Ticker::BlockCacheFilterHit,
    Ticker::BlockCacheFilterMiss,
    Ticker::NumberMultigetCalls,
    Ticker::NumberMultigetKeysRead,
    Ticker::NumberMultigetBytesRead,
    Ticker::BytesRead,
    Ticker::BytesWritten,
    Ticker::WalFileBytes,
    Ticker::FlushWriteBytes,
    Ticker::StallMicros,
    Ticker::MemtablePayloadBytesAtFlush,
    Ticker::MemtableGarbageBytesAtFlush,
    Ticker::CompactReadBytes,
    Ticker::CompactWriteBytes,
];

/// Publishes [`EXPORTED_TICKERS`] under `zinder_store_rocksdb_ticker`, keyed by
/// the upstream `RocksDB` ticker name and the store role.
fn record_rocksdb_ticker_gauges(statistics: &Options, store_role: &'static str) {
    for ticker in EXPORTED_TICKERS {
        metrics::gauge!(
            "zinder_store_rocksdb_ticker",
            "ticker" => ticker.name(),
            "store_role" => store_role
        )
        .set(u64_to_f64(statistics.get_ticker_count(*ticker)));
    }
}

/// Sampling cadence for the resource-gauge sweep on the write path.
///
/// Sits below the 15s Prometheus scrape interval so scrapes always read a
/// fresh sample while a commit burst probes the sweep at most once per second.
const RESOURCE_GAUGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Rate-limits the `RocksDB` resource-gauge sweep so a burst of small commits
/// probes every property at most once per interval instead of once per write.
///
/// The sweep in [`record_rocksdb_resource_gauges`] acquires the `RocksDB` DB
/// mutex for per-column-family properties and reads the WAL directory, so
/// running it per commit couples that cost to write rate on a store whose
/// smallest write unit is a single mempool event.
pub struct ResourceGaugeThrottle {
    interval: Duration,
    last_sampled_at: Mutex<Option<Instant>>,
}

impl ResourceGaugeThrottle {
    /// Builds a throttle that admits one sample per `interval`.
    #[must_use]
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_sampled_at: Mutex::new(None),
        }
    }

    /// Returns `true` when a sweep is due, recording the sample instant.
    ///
    /// The first call always admits; a later call admits only once `interval`
    /// has elapsed since the last admitted sample.
    pub fn should_sample(&self) -> bool {
        let now = Instant::now();
        let mut last_sampled_at = self.last_sampled_at.lock();
        if last_sampled_at.is_some_and(|previous| now.duration_since(previous) < self.interval) {
            return false;
        }
        *last_sampled_at = Some(now);
        true
    }
}

impl Default for ResourceGaugeThrottle {
    fn default() -> Self {
        Self::new(RESOURCE_GAUGE_SAMPLE_INTERVAL)
    }
}

/// Applies [`RocksDbResourceBudget::statistics_level`] to `db_options`.
///
/// `RocksDbStatisticsLevel::Off` skips `enable_statistics()` entirely, so
/// `db_options.get_ticker_count` reads a null statistics pointer and
/// returns `0` rather than erroring; [`record_rocksdb_resource_gauges`]
/// still skips the ticker export at this level so the exported series
/// stays absent rather than reporting fabricated zeros.
fn apply_statistics_level(db_options: &mut Options, level: RocksDbStatisticsLevel) {
    match level {
        RocksDbStatisticsLevel::Off => {}
        RocksDbStatisticsLevel::Tickers => {
            db_options.enable_statistics();
            db_options.set_statistics_level(StatsLevel::ExceptHistogramOrTimers);
        }
        RocksDbStatisticsLevel::Full => db_options.enable_statistics(),
    }
}

/// Builds `RocksDB` options for any writer-posture instance in the workspace.
///
/// The canonical chain store and the materialized-view store both route through this
/// factory; ADR-0020 describes the bounded resource budget applied here.
///
/// Locked invariants applied here are non-tunable: write-ahead logging on
/// (`disable_wal=false`), point-in-time recovery on (`RocksDB` default),
/// atomic cross-CF flush on (`set_atomic_flush(true)`), and ordered
/// writes on (`unordered_write=false`). See
/// [ADR-0020](../../../docs/adrs/0020-bounded-rocksdb-resource-budget.md)
/// for the full design.
#[must_use]
fn build_primary_db_options(
    rocksdb_resource_budget: RocksDbResourceBudget,
    cache: &Cache,
    create_missing: bool,
) -> Options {
    let mut db_options = Options::default();
    db_options.create_if_missing(create_missing);
    db_options.create_missing_column_families(create_missing);
    apply_statistics_level(&mut db_options, rocksdb_resource_budget.statistics_level);
    db_options.set_max_total_wal_size(rocksdb_resource_budget.max_wal_bytes);
    db_options.set_max_open_files(rocksdb_resource_budget.max_open_files);
    db_options.set_atomic_flush(true);
    db_options.set_block_based_table_factory(&build_block_based_table_factory(cache));
    db_options
}

/// Builds `RocksDB` options for a secondary replica from the uniform bounded
/// resource-budget type.
///
/// Secondaries replay the writer's WAL but do not generate one, so the WAL
/// ceiling and atomic-flush flag are not applied here. `OpenAsSecondary` also
/// disables automatic flushes and compactions, so the primary-only background
/// job limit is not applied. Secondary catch-up serializes table-handler loads
/// because `RocksDB` otherwise creates and joins a fresh thread group for every
/// column family on every catch-up. The block-cache and open-file caps still
/// bound the secondary's open-time RAM peak as the store grows.
#[must_use]
fn build_secondary_db_options(
    rocksdb_resource_budget: RocksDbResourceBudget,
    cache: &Cache,
) -> Options {
    let mut db_options = Options::default();
    db_options.create_if_missing(false);
    db_options.create_missing_column_families(false);
    apply_statistics_level(&mut db_options, rocksdb_resource_budget.statistics_level);
    db_options.set_max_open_files(rocksdb_resource_budget.max_open_files);
    db_options.set_block_based_table_factory(&build_block_based_table_factory(cache));
    db_options
}

/// Allocates the bounded LRU block cache shared by every column family.
///
/// The cache holds data and index blocks, plus filter blocks for any family
/// that configures a filter, so total resident metadata stays bounded by
/// `capacity_bytes`. Cloning the returned handle shares the underlying
/// allocation (`RocksDB`'s `Cache` is reference counted); [`SharedBlockCache`]
/// carries one cache across several opens.
#[must_use]
fn build_block_cache(capacity_bytes: u64) -> Cache {
    Cache::new_lru_cache(block_cache_capacity(capacity_bytes))
}

fn block_cache_capacity(capacity_bytes: u64) -> usize {
    usize::try_from(capacity_bytes).unwrap_or(usize::MAX)
}

fn build_write_buffer_manager(memtable_budget_bytes: u64) -> WriteBufferManager {
    let capacity = usize::try_from(memtable_budget_bytes).unwrap_or(usize::MAX);
    WriteBufferManager::new_write_buffer_manager(capacity, false)
}

/// Builds the shared `BlockBasedOptions` every column family routes through.
///
/// Index and filter blocks are accounted to `cache` so the at-rest
/// metadata budget stays bounded by [`RocksDbResourceBudget::block_cache_bytes`].
/// The factory sets no filter policy; a store that filters a family adds one
/// to the returned options, as the wallet store does for its hash-keyed
/// families. Public because the materialized-view and wallet stores re-use the
/// same factory; see
/// [ADR-0020](../../../docs/adrs/0020-bounded-rocksdb-resource-budget.md).
#[must_use]
pub fn build_block_based_table_factory(cache: &Cache) -> BlockBasedOptions {
    let mut bbt = BlockBasedOptions::default();
    bbt.set_block_cache(cache);
    bbt.set_cache_index_and_filter_blocks(true);
    bbt.set_pin_l0_filter_and_index_blocks_in_cache(true);
    bbt
}

/// Keeps the cached blocks of a table file the instance stops referencing.
///
/// Serving processes open several secondaries of one store on one
/// [`SharedBlockCache`], and a file one of them drops at catch-up is still
/// read through the others. A non-zero `uncache_aggressiveness` erases that
/// file's blocks, pinned ones included, and `RocksDB` plans to raise its
/// default from 0 to about 300. Pinning 0 leaves unused blocks to age out of
/// the LRU instead. Apply it to every column family that reads through a
/// block cache that may be shared.
pub fn keep_blocks_of_dropped_table_files(column_family_options: &mut Options) {
    column_family_options.set_uncache_aggressiveness(0);
}

/// Reports whether `error` says a table file the failing handle still names is
/// absent from disk.
///
/// A secondary instance does not keep the primary's table files alive. Once
/// the primary's compaction deletes a file, a secondary read that has to
/// reopen it fails this way until the secondary catches up past the deletion
/// or is reopened. Corruption, a missing MANIFEST or WAL, and every other
/// failure return `false`. `rust-rocksdb` exposes only the status text, so
/// the check matches the I/O status `RocksDB` builds for an absent file and
/// requires the path it reports to end in a numbered table file name, such as
/// `000199.sst`. A directory name that merely contains `.sst` does not count.
#[must_use]
pub fn is_missing_table_file_error(error: &rust_rocksdb::Error) -> bool {
    is_missing_table_file_status(&error.kind(), error.as_ref())
}

fn is_missing_table_file_status(kind: &rust_rocksdb::ErrorKind, message: &str) -> bool {
    matches!(
        kind,
        rust_rocksdb::ErrorKind::IOError | rust_rocksdb::ErrorKind::NotFound
    ) && message.contains("No such file or directory")
        && message.split(": ").any(names_table_file)
}

/// Reports whether `status_part`, one `": "`-separated part of a `RocksDB`
/// status, is a path whose file name is a table file `RocksDB` numbered.
fn names_table_file(status_part: &str) -> bool {
    Path::new(status_part)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .and_then(|file_name| file_name.strip_suffix(".sst"))
        .is_some_and(|file_number| {
            !file_number.is_empty() && file_number.bytes().all(|byte| byte.is_ascii_digit())
        })
}

const PER_CF_INT_PROPERTIES: [&str; 13] = [
    "rocksdb.estimate-live-data-size",
    "rocksdb.total-sst-files-size",
    "rocksdb.size-all-mem-tables",
    "rocksdb.cur-size-active-mem-table",
    TABLE_READERS_MEMORY_PROPERTY,
    "rocksdb.estimate-pending-compaction-bytes",
    "rocksdb.num-running-compactions",
    "rocksdb.num-files-at-level0",
    "rocksdb.num-immutable-mem-table",
    "rocksdb.mem-table-flush-pending",
    "rocksdb.num-running-flushes",
    "rocksdb.compaction-pending",
    "rocksdb.base-level",
];

/// Write-controller properties reported by the whole database instance rather
/// than per column family.
const DB_INT_PROPERTIES: [&str; 2] = [
    "rocksdb.actual-delayed-write-rate",
    "rocksdb.is-write-stopped",
];

/// `cf` label value for DB-level properties, which have no owning column family.
const DB_LEVEL_PROPERTY_CF: &str = "__db__";

/// Per-family table-reader memory that the block cache does not account for.
const TABLE_READERS_MEMORY_PROPERTY: &str = "rocksdb.estimate-table-readers-mem";

#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus gauges use f64 samples; RocksDB property values are diagnostic magnitudes"
)]
fn u64_to_f64(sample: u64) -> f64 {
    sample as f64
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus gauges use f64 samples; block cache sizes are diagnostic magnitudes"
)]
fn usize_to_f64(sample: usize) -> f64 {
    sample as f64
}

fn usize_to_u64(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use rust_rocksdb::statistics::Histogram;

    use super::*;

    #[test]
    fn resource_gauge_throttle_admits_first_sample_then_holds_within_interval() {
        let throttle = ResourceGaugeThrottle::new(Duration::from_hours(1));
        assert!(throttle.should_sample());
        assert!(!throttle.should_sample());
        assert!(!throttle.should_sample());
    }

    #[test]
    fn only_an_absent_table_file_is_a_missing_table_file() {
        assert!(is_missing_table_file_status(
            &rust_rocksdb::ErrorKind::IOError,
            "IO error: No such file or directory: While open a file for random read: \
             /canonical/000199.sst: No such file or directory"
        ));
        assert!(is_missing_table_file_status(
            &rust_rocksdb::ErrorKind::IOError,
            "IO error: No such file or directory: While open a file for random read: \
             /data/wallet.sst/1234567.sst: No such file or directory"
        ));
        assert!(!is_missing_table_file_status(
            &rust_rocksdb::ErrorKind::IOError,
            "IO error: No such file or directory: /canonical/MANIFEST-000123"
        ));
        // A directory whose name contains `.sst` does not make its MANIFEST or
        // WAL a table file.
        assert!(!is_missing_table_file_status(
            &rust_rocksdb::ErrorKind::IOError,
            "IO error: No such file or directory: While opening a file for sequentially \
             reading: /data/wallet.sst/MANIFEST-000123: No such file or directory"
        ));
        assert!(!is_missing_table_file_status(
            &rust_rocksdb::ErrorKind::IOError,
            "IO error: No such file or directory: While opening a file for sequentially \
             reading: /data/000123.sst/000124.log: No such file or directory"
        ));
        // Only the status kind rejects this one.
        assert!(!is_missing_table_file_status(
            &rust_rocksdb::ErrorKind::Corruption,
            "IO error: No such file or directory: While open a file for random read: \
             /canonical/000199.sst: No such file or directory"
        ));
        assert!(!is_missing_table_file_status(
            &rust_rocksdb::ErrorKind::Corruption,
            "Corruption: block checksum mismatch: stored = 1, computed = 2 \
             in /canonical/000199.sst offset 0 size 64"
        ));
        assert!(!is_missing_table_file_status(
            &rust_rocksdb::ErrorKind::IOError,
            "IO error: Input/output error: While pread offset 0 len 64: /canonical/000199.sst"
        ));
    }

    #[test]
    fn resource_gauge_throttle_readmits_after_interval() {
        let throttle = ResourceGaugeThrottle::new(Duration::ZERO);
        assert!(throttle.should_sample());
        assert!(throttle.should_sample());
    }

    #[test]
    fn ticker_export_covers_flush_efficiency() {
        for ticker in [
            Ticker::FlushWriteBytes,
            Ticker::MemtablePayloadBytesAtFlush,
            Ticker::MemtableGarbageBytesAtFlush,
        ] {
            assert!(EXPORTED_TICKERS.contains(&ticker));
        }
    }

    #[test]
    fn ticker_export_names_each_ticker_once() {
        let mut names = EXPORTED_TICKERS
            .iter()
            .map(Ticker::name)
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), EXPORTED_TICKERS.len());
    }

    #[test]
    fn property_export_covers_flush_and_compaction_pressure() {
        for property in [
            "rocksdb.num-files-at-level0",
            "rocksdb.num-immutable-mem-table",
            "rocksdb.mem-table-flush-pending",
            "rocksdb.num-running-flushes",
            "rocksdb.compaction-pending",
            "rocksdb.estimate-pending-compaction-bytes",
            "rocksdb.num-running-compactions",
            "rocksdb.base-level",
        ] {
            assert!(PER_CF_INT_PROPERTIES.contains(&property));
        }
    }

    #[test]
    fn background_job_limit_is_effective_only_for_primary_posture() {
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.max_background_jobs = 6;

        let primary_open = RocksDbOpenRole::Primary {
            path: Path::new("primary"),
        };
        let existing_primary_open = RocksDbOpenRole::ExistingPrimary {
            path: Path::new("primary"),
        };
        let secondary_open = RocksDbOpenRole::Secondary {
            primary_path: Path::new("primary"),
            secondary_path: Path::new("secondary"),
        };
        assert_eq!(
            primary_open
                .open_posture()
                .effective_max_background_jobs(budget),
            Some(6)
        );
        assert_eq!(
            existing_primary_open
                .open_posture()
                .effective_max_background_jobs(budget),
            Some(6)
        );
        assert_eq!(
            secondary_open
                .open_posture()
                .effective_max_background_jobs(budget),
            None
        );

        for role in [
            StoreRole::CanonicalPrimary,
            StoreRole::MaterializedViewPrimary,
        ] {
            assert_eq!(
                role.open_posture().effective_max_background_jobs(budget),
                Some(6)
            );
        }
        for role in [
            StoreRole::CanonicalSecondary,
            StoreRole::MaterializedViewSecondary,
            StoreRole::WalletSecondary,
        ] {
            assert_eq!(
                role.open_posture().effective_max_background_jobs(budget),
                None
            );
        }
    }

    #[test]
    fn secondary_open_serializes_table_handler_loading() {
        let primary_open = RocksDbOpenRole::Primary {
            path: Path::new("primary"),
        };
        let existing_primary_open = RocksDbOpenRole::ExistingPrimary {
            path: Path::new("primary"),
        };
        let secondary_open = RocksDbOpenRole::Secondary {
            primary_path: Path::new("primary"),
            secondary_path: Path::new("secondary"),
        };

        assert_eq!(
            primary_open
                .open_posture()
                .effective_max_file_opening_threads(),
            None
        );
        assert_eq!(
            existing_primary_open
                .open_posture()
                .effective_max_file_opening_threads(),
            None
        );
        assert_eq!(
            secondary_open
                .open_posture()
                .effective_max_file_opening_threads(),
            Some(1)
        );
    }

    #[test]
    fn shared_cache_resources_require_the_budget_to_name_the_cache_capacity() {
        let budget = RocksDbResourceBudget::for_local_tests();
        let matching = SharedBlockCache::new(budget.block_cache_bytes);
        assert_eq!(matching.capacity_bytes(), budget.block_cache_bytes);
        assert_eq!(
            RocksDbOpenResources::with_shared_block_cache(budget, &matching).validate(),
            Ok(())
        );
        let larger = SharedBlockCache::new(budget.block_cache_bytes * 2);
        assert!(matches!(
            RocksDbOpenResources::with_shared_block_cache(budget, &larger).validate(),
            Err(reason) if reason.contains("shared block cache")
        ));
        assert_eq!(RocksDbOpenResources::from(budget).validate(), Ok(()));
    }

    #[test]
    fn dropped_table_file_blocks_stay_cached_whatever_the_default() {
        let mut options = Options::default();
        options.set_uncache_aggressiveness(300);
        keep_blocks_of_dropped_table_files(&mut options);
        assert_eq!(options.get_uncache_aggressiveness(), 0);
    }

    #[test]
    fn an_open_on_a_shared_cache_reads_through_that_cache() -> Result<(), Box<dyn std::error::Error>>
    {
        let store_dir = tempfile::tempdir()?;
        let budget = RocksDbResourceBudget::for_local_tests();
        let shared = SharedBlockCache::new(budget.block_cache_bytes);
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary {
                path: store_dir.path(),
            },
            RocksDbOpenResources::with_shared_block_cache(budget, &shared),
            |cache, _| {
                let mut options = Options::default();
                options.set_block_based_table_factory(&build_block_based_table_factory(cache));
                vec![ColumnFamilyDescriptor::new("rows", options)]
            },
        )?;
        let rows = bounded_open.db.cf_handle("rows").ok_or("rows must open")?;
        bounded_open.db.put_cf(&rows, b"key", vec![7_u8; 4096])?;
        bounded_open.db.flush_cf(&rows)?;
        let cached_before_read = shared.usage_bytes();
        assert_eq!(
            bounded_open.db.get_cf(&rows, b"key")?.map(|row| row.len()),
            Some(4096)
        );
        assert!(shared.usage_bytes() >= cached_before_read + 4096);
        assert_eq!(
            bounded_open.block_cache.get_usage(),
            usize::try_from(shared.usage_bytes())?
        );
        Ok(())
    }

    #[test]
    fn existing_primary_never_creates_a_missing_store() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let path = temporary.path().join("missing-store");
        let result = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary { path: &path },
            RocksDbResourceBudget::for_local_tests(),
            |_, _| Vec::new(),
        );
        assert!(result.is_err());
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn buffered_preference_skips_the_direct_attempt() -> Result<(), Box<dyn std::error::Error>> {
        let store_dir = tempfile::tempdir()?;
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.preferred_io_mode = RocksDbIoMode::Buffered;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary {
                path: store_dir.path(),
            },
            budget,
            |_, _| Vec::new(),
        )?;
        assert_eq!(bounded_open.io_mode, RocksDbIoMode::Buffered);
        Ok(())
    }

    #[test]
    fn ticker_export_runs_on_an_open_store() -> Result<(), Box<dyn std::error::Error>> {
        record_gauges_on_fresh_store(RocksDbResourceBudget::for_local_tests())
    }

    #[test]
    fn ticker_export_runs_on_an_open_store_with_statistics_off()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.statistics_level = RocksDbStatisticsLevel::Off;
        record_gauges_on_fresh_store(budget)
    }

    #[test]
    fn the_capacity_gauge_reports_the_cache_the_instance_reads_through()
    -> Result<(), Box<dyn std::error::Error>> {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let rendered_metrics = recorder.handle();
        let store_dir = tempfile::tempdir()?;
        let budget = RocksDbResourceBudget::for_local_tests();
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary {
                path: store_dir.path(),
            },
            budget,
            |_, _| Vec::new(),
        )?;
        let mut stated_budget = budget;
        stated_budget.block_cache_bytes = budget.block_cache_bytes * 2;
        metrics::with_local_recorder(&recorder, || {
            record_rocksdb_resource_gauges(&RocksDbResourceGaugeInputs {
                db: &bounded_open.db,
                store_role: StoreRole::CanonicalPrimary,
                column_family_names: &["default"],
                block_cache: &bounded_open.block_cache,
                write_buffer_manager: &bounded_open.write_buffer_manager,
                statistics: &bounded_open.statistics,
                io_mode: bounded_open.io_mode,
                resource_budget: stated_budget,
            });
        });
        let capacity = rendered_metrics.render().lines().find_map(|line| {
            line.strip_prefix(
                "zinder_store_block_cache_capacity_bytes{store_role=\"canonical_primary\"} ",
            )?
            .parse::<f64>()
            .ok()
        });
        assert_eq!(
            capacity,
            Some(f64::from(u32::try_from(budget.block_cache_bytes)?))
        );
        Ok(())
    }

    #[test]
    fn tickers_statistics_level_collects_no_histograms() -> Result<(), Box<dyn std::error::Error>> {
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.statistics_level = RocksDbStatisticsLevel::Tickers;
        assert_eq!(write_one_key_and_count_statistics(budget)?, (1, 0));
        Ok(())
    }

    #[test]
    fn full_statistics_level_collects_histograms() -> Result<(), Box<dyn std::error::Error>> {
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.statistics_level = RocksDbStatisticsLevel::Full;
        assert_eq!(write_one_key_and_count_statistics(budget)?, (1, 1));
        Ok(())
    }

    /// Writes one key and returns the keys-written ticker and the number of
    /// samples in the bytes-per-write histogram.
    fn write_one_key_and_count_statistics(
        budget: RocksDbResourceBudget,
    ) -> Result<(u64, u64), Box<dyn std::error::Error>> {
        let store_dir = tempfile::tempdir()?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary {
                path: store_dir.path(),
            },
            budget,
            |_, _| Vec::new(),
        )?;
        bounded_open.db.put(b"key", b"row")?;
        let statistics = &bounded_open.statistics;
        Ok((
            statistics.get_ticker_count(Ticker::NumberKeysWritten),
            statistics
                .get_histogram_data(Histogram::BytesPerWrite)
                .count(),
        ))
    }

    #[test]
    fn default_family_compresses_with_snappy() -> Result<(), Box<dyn std::error::Error>> {
        let store_dir = tempfile::tempdir()?;
        let _bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary {
                path: store_dir.path(),
            },
            RocksDbResourceBudget::for_local_tests(),
            |_, _| vec![ColumnFamilyDescriptor::new("rows", Options::default())],
        )?;
        assert_eq!(
            persisted_compression(store_dir.path(), DEFAULT_COLUMN_FAMILY_NAME)?,
            Some("kSnappyCompression".to_owned())
        );
        Ok(())
    }

    /// Reads `compression` for `family` from the newest `OPTIONS-*` file, which
    /// `RocksDB` writes on every open from the options it applied.
    fn persisted_compression(
        store: &Path,
        family: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let mut newest: Option<(u64, String)> = None;
        for entry in fs::read_dir(store)? {
            let file_name = entry?
                .file_name()
                .into_string()
                .map_err(|_| "store file name is not UTF-8")?;
            let Some(number) = file_name.strip_prefix("OPTIONS-") else {
                continue;
            };
            let number = number.parse::<u64>()?;
            if newest
                .as_ref()
                .is_none_or(|(newest_number, _)| number > *newest_number)
            {
                newest = Some((number, file_name));
            }
        }
        let (_, file_name) = newest.ok_or("store has no OPTIONS file")?;
        let options_text = fs::read_to_string(store.join(file_name))?;
        let section = format!("[CFOptions \"{family}\"]");
        Ok(options_text
            .lines()
            .skip_while(|line| *line != section)
            .skip(1)
            .take_while(|line| !line.starts_with('['))
            .find_map(|line| line.trim().strip_prefix("compression=").map(str::to_owned)))
    }

    fn record_gauges_on_fresh_store(
        budget: RocksDbResourceBudget,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store_dir = tempfile::tempdir()?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary {
                path: store_dir.path(),
            },
            budget,
            |_, _| Vec::new(),
        )?;
        record_rocksdb_resource_gauges(&RocksDbResourceGaugeInputs {
            db: &bounded_open.db,
            store_role: StoreRole::CanonicalPrimary,
            column_family_names: &["default"],
            block_cache: &bounded_open.block_cache,
            write_buffer_manager: &bounded_open.write_buffer_manager,
            statistics: &bounded_open.statistics,
            io_mode: bounded_open.io_mode,
            resource_budget: budget,
        });
        Ok(())
    }
}
