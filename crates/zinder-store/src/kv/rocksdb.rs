mod properties;

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

use self::properties::{record_cache_entries, record_level_gauges};
use crate::{RocksDbResourceBudget, RocksDbStatisticsLevel};

const DIRECT_IO_COMPACTION_READAHEAD_BYTES: usize = 2 * 1024 * 1024;
const NON_PRIMARY_MAX_FILE_OPENING_THREADS: i32 = 1;

/// Filesystem I/O mode of a `RocksDB` instance: the mode a resource budget
/// requests, or the mode an open resolved to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RocksDbIoMode {
    /// Direct reads, and for primary stores direct flush and compaction I/O.
    /// If this open fails, the next attempt uses [`Self::Buffered`].
    Direct,
    /// Buffered filesystem I/O through the page cache. An open resolves to it
    /// when the budget requests it or a buffered retry succeeds.
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
    ReadOnlyProbe,
}

impl RocksDbOpenPosture {
    const fn effective_max_background_jobs(
        self,
        resource_budget: RocksDbResourceBudget,
    ) -> Option<i32> {
        match self {
            Self::Primary => Some(resource_budget.max_background_jobs),
            Self::Secondary | Self::ReadOnlyProbe => None,
        }
    }

    const fn effective_max_file_opening_threads(self) -> Option<i32> {
        match self {
            Self::Primary => None,
            Self::Secondary | Self::ReadOnlyProbe => Some(NON_PRIMARY_MAX_FILE_OPENING_THREADS),
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
    /// Canonical secondary dedicated to verifying settled tree-state checkpoints.
    TreeStateVerificationSecondary,
    /// Wallet-projection store opened as the primary writer.
    WalletPrimary,
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
            Self::TreeStateVerificationSecondary => "tree_state_verification_secondary",
            Self::WalletPrimary => "wallet_primary",
            Self::WalletSecondary => "wallet_secondary",
        }
    }

    const fn open_posture(self) -> RocksDbOpenPosture {
        match self {
            Self::CanonicalPrimary | Self::MaterializedViewPrimary | Self::WalletPrimary => {
                RocksDbOpenPosture::Primary
            }
            Self::CanonicalSecondary
            | Self::TreeStateVerificationSecondary
            | Self::MaterializedViewSecondary
            | Self::WalletSecondary => RocksDbOpenPosture::Secondary,
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

/// The cache and cumulative statistics shared by secondary generations of one store.
#[derive(Clone)]
pub struct SharedSecondaryResources {
    budget: RocksDbResourceBudget,
    block_cache: SharedBlockCache,
    options: Arc<Options>,
}

impl SharedSecondaryResources {
    /// Allocates a cache and applies the store's statistics level once for all generations.
    #[must_use]
    pub fn new(budget: RocksDbResourceBudget) -> Self {
        let block_cache = SharedBlockCache::new(budget.block_cache_bytes);
        let options = build_secondary_db_options(None, budget, &block_cache.cache);
        Self {
            budget,
            block_cache,
            options: Arc::new(options),
        }
    }

    /// Returns the block cache used by every generation.
    #[must_use]
    pub const fn block_cache(&self) -> &SharedBlockCache {
        &self.block_cache
    }

    /// Publishes the cache footprint and cumulative tickers across generations.
    pub fn record_resource_gauges(&self, store_role: StoreRole) {
        record_shared_block_cache_gauges(store_role, &self.block_cache);
        if self.budget.statistics_level != RocksDbStatisticsLevel::Off {
            record_rocksdb_ticker_gauges(&self.options, store_role.as_str());
        }
    }
}

/// The bounded resources one `RocksDB` open applies.
///
/// A [`RocksDbResourceBudget`] converts into resources whose open builds a
/// block cache of its own. [`Self::with_shared_block_cache`] instead opens the
/// instance on a cache that other instances of the same store read through.
/// A serving publisher passes a reference to its [`SharedSecondaryResources`]
/// to share both the cache and statistics across secondary opens.
#[derive(Clone)]
pub struct RocksDbOpenResources {
    budget: RocksDbResourceBudget,
    shared_block_cache: Option<SharedBlockCache>,
    secondary_options: Option<Arc<Options>>,
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
            secondary_options: None,
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

impl std::fmt::Debug for RocksDbOpenResources {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RocksDbOpenResources")
            .field("budget", &self.budget)
            .field("shared_block_cache", &self.shared_block_cache)
            .finish_non_exhaustive()
    }
}

impl From<&SharedSecondaryResources> for RocksDbOpenResources {
    fn from(resources: &SharedSecondaryResources) -> Self {
        Self {
            budget: resources.budget,
            shared_block_cache: Some(resources.block_cache.clone()),
            secondary_options: Some(Arc::clone(&resources.options)),
        }
    }
}

impl From<RocksDbResourceBudget> for RocksDbOpenResources {
    fn from(budget: RocksDbResourceBudget) -> Self {
        Self {
            budget,
            shared_block_cache: None,
            secondary_options: None,
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
    /// Short-lived read-only admission probe opening an existing store path
    /// without creating files or acquiring a writer lock.
    ReadOnlyProbe {
        /// Existing store path inspected read-only.
        path: &'path Path,
    },
}

impl<'path> RocksDbOpenRole<'path> {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Primary { .. } => "primary",
            Self::ExistingPrimary { .. } => "existing_primary",
            Self::Secondary { .. } => "secondary",
            Self::ReadOnlyProbe { .. } => "read_only_probe",
        }
    }

    const fn store_path(self) -> &'path Path {
        match self {
            Self::Primary { path }
            | Self::ExistingPrimary { path }
            | Self::ReadOnlyProbe { path } => path,
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
            Self::ReadOnlyProbe { .. } => RocksDbOpenPosture::ReadOnlyProbe,
        }
    }

    const fn effective_block_cache_bytes(self, budget: RocksDbResourceBudget) -> u64 {
        match self {
            Self::Primary { .. } | Self::ExistingPrimary { .. } | Self::Secondary { .. } => {
                budget.block_cache_bytes
            }
            Self::ReadOnlyProbe { .. } => RocksDbResourceBudget::MIN_BLOCK_CACHE_BYTES,
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
    resource_budget: RocksDbResourceBudget,
    resource_gauge_throttle: ResourceGaugeThrottle,
    cache_entry_throttle: ResourceGaugeThrottle,
}

impl BoundedRocksDbOpen {
    /// Publishes resource gauges for a single-instance role at most once per second.
    pub fn record_resource_gauges(&self, role: StoreRole, families: &[&'static str]) {
        if !self.resource_gauge_throttle.should_sample() {
            return;
        }
        let inputs = RocksDbResourceGaugeInputs {
            db: &self.db,
            store_role: role,
            column_family_names: families,
            block_cache: &self.block_cache,
            write_buffer_manager: &self.write_buffer_manager,
            statistics: &self.statistics,
            io_mode: self.io_mode,
            resource_budget: self.resource_budget,
        };
        let sample_cache = self.cache_entry_throttle.should_sample();
        record_single_instance_resource_gauges(&inputs, sample_cache);
        if sample_cache {
            inputs.record_block_cache_entry_gauges();
        }
    }

    /// Samples cache entry counts and charges through this database. A publisher
    /// calls this on one generation per shared cache per instance-memory interval.
    pub fn record_block_cache_entry_gauges(&self, role: StoreRole) {
        record_cache_entries(&self.db, role);
    }

    /// Samples the memory this instance holds outside its block cache.
    ///
    /// The table-reader estimate looks up the reader of every table file in
    /// the instance's table cache, which costs time in proportion to the file
    /// count and marks every resident reader as recently used. A caller that
    /// samples periodically gates it with
    /// [`ResourceGaugeThrottle::for_instance_memory`] and runs it on a
    /// blocking thread.
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
/// a failed direct open, the function logs the error and retries with
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
        secondary_options,
    } = resources.into();
    if matches!(
        role,
        RocksDbOpenRole::ExistingPrimary { .. } | RocksDbOpenRole::ReadOnlyProbe { .. }
    ) {
        DB::list_cf(&Options::default(), role.store_path())?;
    }
    let block_cache = shared_block_cache.map_or_else(
        || build_block_cache(role.effective_block_cache_bytes(rocksdb_resource_budget)),
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
        secondary_options: secondary_options.as_deref(),
    };
    let (db, statistics, io_mode) = open_with_preferred_io_mode(&open_attempt)?;
    record_rocksdb_io_mode(role, io_mode);
    Ok(BoundedRocksDbOpen {
        db,
        block_cache,
        write_buffer_manager,
        io_mode,
        statistics: Arc::new(statistics),
        resource_budget: rocksdb_resource_budget,
        resource_gauge_throttle: ResourceGaugeThrottle::default(),
        cache_entry_throttle: ResourceGaugeThrottle::for_instance_memory(),
    })
}

/// Opens an existing `RocksDB` store read-only for a short-lived admission
/// probe, bounding open files, block cache, and direct reads.
///
/// Every column family in `column_family_names` opens with a
/// [`build_block_based_table_factory`] descriptor bound to the probe's block
/// cache so index and filter blocks charge against that cache instead of
/// accumulating in uncached table-reader memory. When `resources` do not
/// supply a [`SharedBlockCache`], the probe allocates a block cache of
/// [`RocksDbResourceBudget::MIN_BLOCK_CACHE_BYTES`] (4 MiB) rather than the
/// caller's full writer or reader cache. Table-file opens are serialized to
/// one thread and capped by `max_open_files`, and direct reads fall back to
/// buffered I/O when the filesystem rejects direct I/O.
pub fn open_bounded_rocksdb_read_only_probe<S>(
    path: &Path,
    column_family_names: &[S],
    resources: impl Into<RocksDbOpenResources>,
) -> Result<BoundedRocksDbOpen, rust_rocksdb::Error>
where
    S: AsRef<str>,
{
    open_bounded_rocksdb(
        RocksDbOpenRole::ReadOnlyProbe { path },
        resources,
        |cache, _| {
            column_family_names
                .iter()
                .map(AsRef::as_ref)
                .filter(|name| *name != DEFAULT_COLUMN_FAMILY_NAME)
                .map(|name| {
                    let mut options = Options::default();
                    options.set_block_based_table_factory(&build_block_based_table_factory(cache));
                    keep_blocks_of_dropped_table_files(&mut options);
                    ColumnFamilyDescriptor::new(name, options)
                })
                .collect()
        },
    )
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
                    event = "rocksdb_direct_io_open_failed",
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
    secondary_options: Option<&'attempt Options>,
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
            open_attempt.secondary_options,
            open_attempt.rocksdb_resource_budget,
            open_attempt.block_cache,
        ),
        RocksDbOpenRole::ReadOnlyProbe { .. } => build_secondary_db_options(
            None,
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
            default_column_family_options(
                open_attempt.block_cache,
                open_attempt.rocksdb_resource_budget,
            ),
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
        RocksDbOpenRole::ReadOnlyProbe { path } => {
            DB::open_cf_descriptors_read_only(&db_options, path, column_families, false)
        }
    }?;
    Ok((db, db_options))
}

/// Options for the `default` family, which holds the canonical and wallet
/// control rows.
///
/// Without a descriptor of its own, `rust-rocksdb` opens this family with
/// `Options::default()`, whose compression `RocksDB` 11.5 changed from Snappy
/// to LZ4 and whose table factory builds a 32 MiB block cache for each
/// instance. Pinning Snappy keeps new control-row files in the codec that
/// existing stores already hold, and the shared table factory makes the
/// family read through `cache`, so the configured cache bounds every family.
fn default_column_family_options(cache: &Cache, budget: RocksDbResourceBudget) -> Options {
    let mut options = Options::default();
    options.set_compression_type(DBCompressionType::Snappy);
    options.set_block_based_table_factory(&build_block_based_table_factory(cache));
    options.set_write_buffer_size(usize::try_from(budget.write_buffer_bytes).unwrap_or(usize::MAX));
    options.set_max_write_buffer_number(budget.max_write_buffer_count);
    keep_blocks_of_dropped_table_files(&mut options);
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
    /// Column families to probe for table and level properties.
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
/// Each single-instance role reports its own cache, memtables, WAL, column
/// families, and DB-wide statistics. Primary roles also report writer controls.
pub fn record_rocksdb_resource_gauges(inputs: &RocksDbResourceGaugeInputs<'_>) {
    record_single_instance_resource_gauges(inputs, true);
}

fn record_single_instance_resource_gauges(
    inputs: &RocksDbResourceGaugeInputs<'_>,
    sample_wal: bool,
) {
    let store_role = inputs.store_role.as_str();
    // Replay and materialized views already expose these file-by-file estimates.
    // Adding them to a writer commit would make sampling scale with its SST count.
    let per_file_properties: &[&str] = match inputs.store_role {
        StoreRole::CanonicalSecondary
        | StoreRole::MaterializedViewPrimary
        | StoreRole::MaterializedViewSecondary => &PER_FILE_PROPERTIES,
        StoreRole::CanonicalPrimary
        | StoreRole::WalletPrimary
        | StoreRole::WalletSecondary
        | StoreRole::TreeStateVerificationSecondary => &[],
    };
    for cf_name in inputs.column_family_names {
        let Some(column_family) = inputs.db.cf_handle(cf_name) else {
            continue;
        };
        record_level_gauges(inputs, cf_name, &column_family);
        for &property in PER_CF_INT_PROPERTIES.iter().chain(per_file_properties) {
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
    if sample_wal && let Some(wal_bytes) = wal_size_bytes(inputs.db) {
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

/// Publishes one shared block cache under `store_role`.
///
/// The gauges describe the one cache, however many instances share it.
/// [`SharedSecondaryResources::record_resource_gauges`] also publishes the
/// tickers collected across those instances.
pub fn record_shared_block_cache_gauges(store_role: StoreRole, block_cache: &SharedBlockCache) {
    record_block_cache_gauges(store_role.as_str(), &block_cache.cache);
}

/// Publishes the memory that the instances sharing one block cache hold
/// outside it, under `store_role`.
///
/// `instances` is the sum of every open instance's [`RocksDbInstanceMemory`],
/// so the memtable and table-reader gauges count each instance once.
pub fn record_instance_memory_gauges(store_role: StoreRole, instances: RocksDbInstanceMemory) {
    let store_role = store_role.as_str();
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
/// wallet's filtered families contribute Bloom tickers. Canonical and wallet
/// serving secondaries retain their counters across recycled generations.
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
    Ticker::NoFileOpens,
    Ticker::NoFileErrors,
    Ticker::BlockCacheBytesRead,
    Ticker::MemtableHit,
    Ticker::GetHitL0,
    Ticker::GetHitL1,
    Ticker::GetHitL2AndUp,
    Ticker::NumberDbSeek,
    Ticker::NumberDbNext,
    Ticker::IterBytesRead,
    Ticker::PrefetchBytes,
    Ticker::PrefetchBytesUseful,
    Ticker::ReadaheadTrimmed,
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

/// Sampling cadence for [`BoundedRocksDbOpen::instance_memory`] over a serving
/// process's open instances.
///
/// Matches the 15s Prometheus scrape interval: a scrape reads a sample at most
/// one interval old, while the table-cache lookups the sample makes run once
/// per scrape instead of on every catch-up.
const INSTANCE_MEMORY_SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

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

    /// Builds a throttle for periodic [`BoundedRocksDbOpen::instance_memory`]
    /// samples, which admits one sample per Prometheus scrape interval.
    #[must_use]
    pub fn for_instance_memory() -> Self {
        Self::new(INSTANCE_MEMORY_SAMPLE_INTERVAL)
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

/// Builds the base options for a store's [`RocksDbResourceBudget::statistics_level`].
///
/// `RocksDbStatisticsLevel::Off` skips `enable_statistics()` entirely, so
/// `db_options.get_ticker_count` reads a null statistics pointer and
/// returns `0` rather than erroring; [`record_rocksdb_resource_gauges`]
/// still skips the ticker export at this level so the exported series
/// stays absent rather than reporting fabricated zeros.
fn statistics_options(level: RocksDbStatisticsLevel) -> Options {
    let mut db_options = Options::default();
    match level {
        RocksDbStatisticsLevel::Off => {}
        RocksDbStatisticsLevel::Tickers => {
            db_options.enable_statistics();
            db_options.set_statistics_level(StatsLevel::ExceptHistogramOrTimers);
        }
        RocksDbStatisticsLevel::Full => db_options.enable_statistics(),
    }
    db_options
}

/// Builds `RocksDB` options for any writer-posture instance in the workspace.
///
/// The canonical chain store and the materialized-view store both route through this
/// factory; ADR-0005 describes the bounded resource budget applied here.
///
/// Locked invariants applied here are non-tunable: write-ahead logging on
/// (`disable_wal=false`), point-in-time recovery on (`RocksDB` default),
/// atomic cross-CF flush on (`set_atomic_flush(true)`), and ordered
/// writes on (`unordered_write=false`). See
/// [ADR-0005](../../../docs/adrs/0005-bounded-process-memory.md)
/// for the full design.
#[must_use]
fn build_primary_db_options(
    rocksdb_resource_budget: RocksDbResourceBudget,
    cache: &Cache,
    create_missing: bool,
) -> Options {
    let mut db_options = statistics_options(rocksdb_resource_budget.statistics_level);
    db_options.create_if_missing(create_missing);
    db_options.create_missing_column_families(create_missing);
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
    base: Option<&Options>,
    rocksdb_resource_budget: RocksDbResourceBudget,
    cache: &Cache,
) -> Options {
    if let Some(base) = base {
        // Copying Options preserves RocksDB's shared Statistics pointer.
        return base.clone();
    }
    let mut db_options = statistics_options(rocksdb_resource_budget.statistics_level);
    db_options.create_if_missing(false);
    db_options.create_missing_column_families(false);
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
/// [ADR-0005](../../../docs/adrs/0005-bounded-process-memory.md).
#[must_use]
pub fn build_block_based_table_factory(cache: &Cache) -> BlockBasedOptions {
    let mut bbt = BlockBasedOptions::default();
    // Keep the bundled engine's current SST format when upstream changes its default.
    bbt.set_format_version(7);
    bbt.set_block_cache(cache);
    bbt.set_cache_index_and_filter_blocks(true);
    bbt.set_pin_l0_filter_and_index_blocks_in_cache(true);
    bbt
}

/// Builds `BlockBasedOptions` that read through no block cache.
///
/// A read through a block cache takes a block another read cached without
/// reading the file again, so it cannot see damage done to the file since,
/// whatever its `ReadOptions` set. A family opened with these options reads
/// every data block from the file and checks its checksum. Index and filter
/// blocks stay in table-reader memory instead of a cache.
#[must_use]
pub(crate) fn build_uncached_block_based_table_factory() -> BlockBasedOptions {
    let mut bbt = BlockBasedOptions::default();
    bbt.disable_cache();
    bbt
}

/// Keeps the cached blocks of a table file the instance stops referencing.
///
/// Serving processes open several secondaries of one store on one
/// [`SharedBlockCache`], and a file one of them drops at catch-up is still
/// read through the others. A non-zero `uncache_aggressiveness` erases that
/// file's blocks, pinned ones included, and `RocksDB` documents that its
/// default of 0 will likely change to about 300. Pinning 0 leaves unused
/// blocks to age out of the LRU instead. Apply it to every column family that
/// reads through a block cache that may be shared.
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

const PER_FILE_PROPERTIES: [&str; 3] = [
    "rocksdb.estimate-live-data-size",
    "rocksdb.total-sst-files-size",
    TABLE_READERS_MEMORY_PROPERTY,
];

const PER_CF_INT_PROPERTIES: [&str; 9] = [
    "rocksdb.size-all-mem-tables",
    "rocksdb.cur-size-active-mem-table",
    "rocksdb.estimate-pending-compaction-bytes",
    "rocksdb.num-running-compactions",
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
        let probe_open = RocksDbOpenRole::ReadOnlyProbe {
            path: Path::new("primary"),
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
        assert_eq!(
            probe_open
                .open_posture()
                .effective_max_background_jobs(budget),
            None
        );

        for role in [
            StoreRole::CanonicalPrimary,
            StoreRole::WalletPrimary,
            StoreRole::MaterializedViewPrimary,
        ] {
            assert_eq!(
                role.open_posture().effective_max_background_jobs(budget),
                Some(6)
            );
        }
        for role in [
            StoreRole::CanonicalSecondary,
            StoreRole::TreeStateVerificationSecondary,
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
        let probe_open = RocksDbOpenRole::ReadOnlyProbe {
            path: Path::new("primary"),
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
        assert_eq!(
            probe_open
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

    /// The `default` family, which holds the control rows, reads through the
    /// configured cache instead of a cache `RocksDB` builds for the instance.
    #[test]
    fn the_default_family_reads_through_the_configured_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let store_dir = tempfile::tempdir()?;
        let budget = RocksDbResourceBudget::for_local_tests();
        let shared = SharedBlockCache::new(budget.block_cache_bytes);
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary {
                path: store_dir.path(),
            },
            RocksDbOpenResources::with_shared_block_cache(budget, &shared),
            |_, _| Vec::new(),
        )?;
        bounded_open.db.put(b"control", vec![7_u8; 4096])?;
        bounded_open.db.flush()?;
        let cached_before_read = shared.usage_bytes();
        assert_eq!(
            bounded_open.db.get(b"control")?.map(|row| row.len()),
            Some(4096)
        );
        assert!(shared.usage_bytes() >= cached_before_read + 4096);
        Ok(())
    }

    #[test]
    fn instance_memory_samples_once_per_scrape_interval() {
        let throttle = ResourceGaugeThrottle::for_instance_memory();
        assert_eq!(throttle.interval, Duration::from_secs(15));
        assert!(throttle.should_sample());
        assert!(!throttle.should_sample());
    }

    /// Bytes of each row the shared-cache isolation tests write, large enough
    /// that a read loads a data block of its own.
    const ISOLATION_ROW_BYTES: usize = 64 * 1024;

    fn rows_family(cache: &Cache, _: RocksDbResourceBudget) -> Vec<ColumnFamilyDescriptor> {
        let mut options = Options::default();
        options.set_block_based_table_factory(&build_block_based_table_factory(cache));
        keep_blocks_of_dropped_table_files(&mut options);
        vec![ColumnFamilyDescriptor::new("rows", options)]
    }

    fn open_rows_primary(path: &Path) -> Result<BoundedRocksDbOpen, rust_rocksdb::Error> {
        open_bounded_rocksdb(
            RocksDbOpenRole::Primary { path },
            RocksDbResourceBudget::for_local_tests(),
            rows_family,
        )
    }

    fn open_rows_secondary(
        primary_path: &Path,
        secondary_path: &Path,
        cache: &SharedBlockCache,
    ) -> Result<BoundedRocksDbOpen, rust_rocksdb::Error> {
        open_bounded_rocksdb(
            RocksDbOpenRole::Secondary {
                primary_path,
                secondary_path,
            },
            RocksDbOpenResources::with_shared_block_cache(
                RocksDbResourceBudget::for_local_tests(),
                cache,
            ),
            rows_family,
        )
    }

    /// Writes `fill` repeated as the row under `key` and flushes it to a
    /// table file.
    fn write_row(open: &BoundedRocksDbOpen, fill: u8) -> Result<(), Box<dyn std::error::Error>> {
        let rows = open.db.cf_handle("rows").ok_or("rows must open")?;
        open.db
            .put_cf(&rows, b"key", vec![fill; ISOLATION_ROW_BYTES])?;
        open.db.flush_cf(&rows)?;
        Ok(())
    }

    /// The outcome of one row read, apart from a missing family.
    type RowRead = Result<Option<Vec<u8>>, rust_rocksdb::Error>;

    /// Reads the row under `key`, keeping the read's own error apart from a
    /// missing family.
    fn read_row(open: &BoundedRocksDbOpen) -> Result<RowRead, Box<dyn std::error::Error>> {
        let rows = open.db.cf_handle("rows").ok_or("rows must open")?;
        Ok(open.db.get_cf(&rows, b"key"))
    }

    #[test]
    fn shared_secondary_tickers_include_retired_generations()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let primary_path = root.path().join("primary");
        let primary = open_rows_primary(&primary_path)?;
        write_row(&primary, 1)?;
        let shared = SharedSecondaryResources::new(RocksDbResourceBudget::for_local_tests());
        let generation = |path: &Path| {
            open_bounded_rocksdb(
                RocksDbOpenRole::Secondary {
                    primary_path: &primary_path,
                    secondary_path: path,
                },
                &shared,
                rows_family,
            )
        };
        let first = generation(&root.path().join("first"))?;
        let second = generation(&root.path().join("second"))?;
        let ticker = Ticker::NumberKeysRead;
        let before = second.statistics.get_ticker_count(ticker);
        assert!(read_row(&first)??.is_some());
        assert!(read_row(&second)??.is_some());
        assert_eq!(first.statistics.get_ticker_count(ticker), before + 2);
        drop(first);
        assert_eq!(second.statistics.get_ticker_count(ticker), before + 2);
        assert!(read_row(&second)??.is_some());
        assert_eq!(second.statistics.get_ticker_count(ticker), before + 3);
        drop(second);
        assert_eq!(shared.options.get_ticker_count(ticker), before + 3);
        let reopened = generation(&root.path().join("second"))?;
        assert!(read_row(&reopened)??.is_some());
        assert_eq!(shared.options.get_ticker_count(ticker), before + 4);
        Ok(())
    }

    #[test]
    fn statistics_off_omits_shared_tickers() -> Result<(), Box<dyn std::error::Error>> {
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.statistics_level = RocksDbStatisticsLevel::Off;
        let root = tempfile::tempdir()?;
        let primary_path = root.path().join("primary");
        let primary = open_rows_primary(&primary_path)?;
        write_row(&primary, 1)?;
        let shared = SharedSecondaryResources::new(budget);
        let secondary = open_bounded_rocksdb(
            RocksDbOpenRole::Secondary {
                primary_path: &primary_path,
                secondary_path: &root.path().join("secondary"),
            },
            &shared,
            rows_family,
        )?;
        assert!(read_row(&secondary)??.is_some());
        assert_eq!(
            secondary
                .statistics
                .get_ticker_count(Ticker::NumberKeysRead),
            0
        );
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        metrics::with_local_recorder(&recorder, || {
            shared.record_resource_gauges(StoreRole::WalletSecondary);
        });
        let exposition = recorder.handle().render();
        assert!(exposition.contains("zinder_store_block_cache_capacity_bytes"));
        assert!(!exposition.contains("zinder_store_rocksdb_ticker"));
        Ok(())
    }

    #[test]
    fn primary_reports_level_counts_and_cache_entries_without_per_file_properties()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let mut primary = open_rows_primary(root.path())?;
        primary.resource_gauge_throttle = ResourceGaugeThrottle::new(Duration::ZERO);
        write_row(&primary, 7)?;
        assert!(read_row(&primary)??.is_some());
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let record = || primary.record_resource_gauges(StoreRole::CanonicalPrimary, &["rows"]);
        record();
        let first = recorder.handle().render();
        let property = |exposition: &str, label: &str| {
            primary_gauge(exposition, "zinder_store_rocksdb_property{", label)
        };
        assert_eq!(property(&first, "rocksdb.num-files-at-level0"), Some(1.0));
        assert_eq!(property(&first, "rocksdb.num-files-at-level1"), None);
        for per_file_property in super::PER_FILE_PROPERTIES {
            assert_eq!(property(&first, per_file_property), None);
        }
        assert!(!first.contains("compression-ratio-at-level"));
        assert!(!first.contains("zinder_store_table_metadata_bytes"));
        // RocksDB can return its cached startup snapshot on the first collection.
        assert!(
            primary_gauge(
                &first,
                "zinder_store_block_cache_entries{",
                "entry_role=\"Misc\""
            )
            .is_some_and(|count| count > 0.0)
        );
        assert!(
            primary_gauge(
                &first,
                "zinder_store_block_cache_entry_bytes{",
                "entry_role=\"DataBlock\""
            )
            .is_some()
        );
        let rows = primary.db.cf_handle("rows").ok_or("rows must open")?;
        primary
            .db
            .compact_range_cf(&rows, None::<&[u8]>, None::<&[u8]>);
        record();
        let compacted = recorder.handle().render();
        assert_eq!(
            property(&compacted, "rocksdb.num-files-at-level0"),
            Some(0.0)
        );
        let occupied_property = (1..=6)
            .map(|level| format!("rocksdb.num-files-at-level{level}"))
            .find(|name| property(&compacted, name) == Some(1.0))
            .ok_or("compaction must move the SST above level 0")?;
        drop(rows);
        drop(primary);

        let empty = tempfile::tempdir()?;
        let reopened = open_rows_primary(empty.path())?;
        reopened.record_resource_gauges(StoreRole::CanonicalPrimary, &["rows"]);
        let after_reopen = recorder.handle().render();
        assert_eq!(property(&after_reopen, &occupied_property), Some(0.0));
        Ok(())
    }

    fn primary_gauge(exposition: &str, metric: &str, label: &str) -> Option<f64> {
        let line = exposition.lines().find(|line| {
            line.starts_with(metric)
                && line.contains(label)
                && line.contains("store_role=\"canonical_primary\"")
        })?;
        line.rsplit_once(' ')?.1.parse().ok()
    }

    fn table_file_names(store: &Path) -> Result<Vec<String>, std::io::Error> {
        let mut names = fs::read_dir(store)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "sst"))
            .filter_map(|path| Some(path.file_name()?.to_string_lossy().into_owned()))
            .collect::<Vec<_>>();
        names.sort_unstable();
        Ok(names)
    }

    /// A secondary frozen at an older epoch never reads a newer epoch's row
    /// through the shared cache.
    ///
    /// It keeps reading its own row after a sibling on the same cache loads
    /// the newer version of that row. After compaction deletes its file, it
    /// reads its own row or fails with a missing table file.
    #[test]
    fn a_frozen_secondary_never_reads_a_newer_epoch_through_the_shared_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let primary_path = root.path().join("primary");
        let primary = open_rows_primary(&primary_path)?;
        write_row(&primary, 1)?;
        let cache =
            SharedBlockCache::new(RocksDbResourceBudget::for_local_tests().block_cache_bytes);
        let frozen = open_rows_secondary(&primary_path, &root.path().join("frozen"), &cache)?;
        let current = open_rows_secondary(&primary_path, &root.path().join("current"), &cache)?;
        assert_eq!(read_row(&frozen)??, Some(vec![1; ISOLATION_ROW_BYTES]));
        assert_eq!(read_row(&current)??, Some(vec![1; ISOLATION_ROW_BYTES]));

        write_row(&primary, 2)?;
        current.db.try_catch_up_with_primary()?;
        assert_eq!(read_row(&current)??, Some(vec![2; ISOLATION_ROW_BYTES]));
        assert_eq!(read_row(&frozen)??, Some(vec![1; ISOLATION_ROW_BYTES]));
        assert_eq!(read_row(&current)??, Some(vec![2; ISOLATION_ROW_BYTES]));

        {
            let rows = primary.db.cf_handle("rows").ok_or("rows must open")?;
            primary
                .db
                .compact_range_cf(&rows, None::<&[u8]>, None::<&[u8]>);
        }
        current.db.try_catch_up_with_primary()?;
        assert_eq!(read_row(&current)??, Some(vec![2; ISOLATION_ROW_BYTES]));
        match read_row(&frozen)? {
            Ok(found) => assert_eq!(found, Some(vec![1; ISOLATION_ROW_BYTES])),
            Err(error) => assert!(is_missing_table_file_error(&error), "{error}"),
        }
        Ok(())
    }

    /// A secondary of a recreated store never reads the old store's blocks.
    ///
    /// The store recreated at the same path reuses the old table-file
    /// numbers, and a secondary of the new store on the warm cache still
    /// reads the new store's row.
    #[test]
    fn a_recreated_store_never_reads_the_old_stores_blocks()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let primary_path = root.path().join("primary");
        let secondary_path = root.path().join("secondary");
        let cache =
            SharedBlockCache::new(RocksDbResourceBudget::for_local_tests().block_cache_bytes);
        let old_files = {
            let primary = open_rows_primary(&primary_path)?;
            write_row(&primary, 1)?;
            let secondary = open_rows_secondary(&primary_path, &secondary_path, &cache)?;
            assert_eq!(read_row(&secondary)??, Some(vec![1; ISOLATION_ROW_BYTES]));
            table_file_names(&primary_path)?
        };
        let cached_old = cache.usage_bytes();
        fs::remove_dir_all(&primary_path)?;
        fs::remove_dir_all(&secondary_path)?;

        let primary = open_rows_primary(&primary_path)?;
        write_row(&primary, 2)?;
        assert_eq!(
            table_file_names(&primary_path)?,
            old_files,
            "the recreated store must reuse the old table-file numbers"
        );
        let secondary = open_rows_secondary(&primary_path, &secondary_path, &cache)?;
        assert_eq!(read_row(&secondary)??, Some(vec![2; ISOLATION_ROW_BYTES]));
        assert!(cache.usage_bytes() > cached_old);
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
    fn read_only_probe_never_creates_a_missing_store() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let path = temporary.path().join("missing-probe-store");
        let outcome = open_bounded_rocksdb_read_only_probe(
            &path,
            &[DEFAULT_COLUMN_FAMILY_NAME],
            RocksDbResourceBudget::read_only_probe_defaults(),
        );
        assert!(outcome.is_err());
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn read_only_probe_uses_minimum_cache_unless_shared_cache_is_supplied()
    -> Result<(), Box<dyn std::error::Error>> {
        let store_dir = tempfile::tempdir()?;
        {
            let primary = open_rows_primary(store_dir.path())?;
            write_row(&primary, 9)?;
        }
        let column_families = DB::list_cf(&Options::default(), store_dir.path())?;
        let writer_budget = RocksDbResourceBudget::canonical_writer_defaults();
        let private_probe = open_bounded_rocksdb_read_only_probe(
            store_dir.path(),
            &column_families,
            writer_budget,
        )?;
        assert_eq!(
            usize_to_u64(private_probe.block_cache.get_capacity()),
            RocksDbResourceBudget::MIN_BLOCK_CACHE_BYTES
        );
        drop(private_probe);

        let local_budget = RocksDbResourceBudget::for_local_tests();
        let resources = SharedSecondaryResources::new(local_budget);
        let shared = resources.block_cache();
        let shared_probe =
            open_bounded_rocksdb_read_only_probe(store_dir.path(), &column_families, &resources)?;
        assert_eq!(
            usize_to_u64(shared_probe.block_cache.get_capacity()),
            shared.capacity_bytes()
        );
        let cached_before = shared.usage_bytes();
        assert_eq!(
            read_row(&shared_probe)??,
            Some(vec![9; ISOLATION_ROW_BYTES])
        );
        assert!(shared.usage_bytes() >= cached_before + u64::try_from(ISOLATION_ROW_BYTES)?);
        assert_eq!(
            resources.options.get_ticker_count(Ticker::NumberKeysRead),
            0
        );
        Ok(())
    }

    #[test]
    fn read_only_probe_buffered_preference_skips_direct_io()
    -> Result<(), Box<dyn std::error::Error>> {
        let store_dir = tempfile::tempdir()?;
        {
            let primary = open_rows_primary(store_dir.path())?;
            write_row(&primary, 3)?;
        }
        let column_families = DB::list_cf(&Options::default(), store_dir.path())?;
        let mut budget = RocksDbResourceBudget::read_only_probe_defaults();
        budget.preferred_io_mode = RocksDbIoMode::Buffered;
        let probe =
            open_bounded_rocksdb_read_only_probe(store_dir.path(), &column_families, budget)?;
        assert_eq!(probe.io_mode, RocksDbIoMode::Buffered);
        Ok(())
    }

    fn build_store_with_many_ssts(
        path: &Path,
        payload_table_count: u32,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let primary = open_bounded_rocksdb(
            RocksDbOpenRole::Primary { path },
            RocksDbResourceBudget::for_local_tests(),
            |cache, _| {
                let mut options = Options::default();
                options.set_block_based_table_factory(&build_block_based_table_factory(cache));
                options.set_disable_auto_compactions(true);
                vec![ColumnFamilyDescriptor::new("payload", options)]
            },
        )?;
        primary.db.put(b"control", b"ready")?;
        primary.db.flush()?;
        let payload_cf = primary.db.cf_handle("payload").ok_or("payload must open")?;
        let mut flush = rust_rocksdb::FlushOptions::default();
        flush.set_allow_write_stall(true);
        for batch_index in 0..payload_table_count {
            for row_offset in 0_u32..8 {
                let key = (batch_index * 8 + row_offset).to_be_bytes();
                primary.db.put_cf(&payload_cf, key, vec![0x5A_u8; 2048])?;
            }
            primary.db.flush_cf_opt(&payload_cf, &flush)?;
        }
        drop(payload_cf);
        drop(primary);
        Ok(table_file_names(path)?.len())
    }

    #[test]
    fn read_only_probe_bounds_table_opens_and_table_reader_memory_when_ssts_exceed_max_open_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let store_dir = tempfile::tempdir()?;
        let probe_budget = RocksDbResourceBudget::read_only_probe_defaults();
        let payload_table_count = u32::try_from(probe_budget.max_open_files)? + 16;
        let sst_count = build_store_with_many_ssts(store_dir.path(), payload_table_count)?;
        assert!(sst_count > usize::try_from(probe_budget.max_open_files)?);

        let column_families = DB::list_cf(&Options::default(), store_dir.path())?;
        let bounded_probe =
            open_bounded_rocksdb_read_only_probe(store_dir.path(), &column_families, probe_budget)?;
        assert_eq!(bounded_probe.db.get(b"control")?, Some(b"ready".to_vec()));
        let bounded_file_opens = bounded_probe
            .statistics
            .get_ticker_count(Ticker::NoFileOpens);
        let bounded_table_reader_bytes = bounded_probe.instance_memory().table_reader_bytes;
        assert!(bounded_file_opens > 0);
        assert!(
            bounded_file_opens < u64::try_from(probe_budget.max_open_files)?,
            "bounded probe opened {bounded_file_opens} tables with max_open_files={} across {sst_count} SSTs",
            probe_budget.max_open_files
        );
        drop(bounded_probe);

        let mut unbounded_options = Options::default();
        unbounded_options.enable_statistics();
        let unbounded_db = DB::open_cf_for_read_only(
            &unbounded_options,
            store_dir.path(),
            &column_families,
            false,
        )?;
        let unbounded_file_opens = unbounded_options.get_ticker_count(Ticker::NoFileOpens);
        let unbounded_table_reader_bytes = column_families
            .iter()
            .filter_map(|name| unbounded_db.cf_handle(name))
            .filter_map(|handle| {
                unbounded_db
                    .property_int_value_cf(&handle, TABLE_READERS_MEMORY_PROPERTY)
                    .ok()
                    .flatten()
            })
            .fold(0_u64, u64::saturating_add);
        assert!(unbounded_file_opens >= u64::from(payload_table_count));
        assert!(bounded_file_opens < unbounded_file_opens);
        assert!(
            bounded_table_reader_bytes < unbounded_table_reader_bytes,
            "bounded table_reader_bytes={bounded_table_reader_bytes} must stay below unbounded={unbounded_table_reader_bytes}"
        );
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
    fn bounded_writer_reads_an_older_table_and_flushes_format_seven()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::io::{Read, Seek, SeekFrom};

        let store_dir = tempfile::tempdir()?;
        let mut older_table = BlockBasedOptions::default();
        older_table.set_format_version(6);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_block_based_table_factory(&older_table);
        {
            let db = DB::open(&options, store_dir.path())?;
            db.put(b"existing", b"old row")?;
            db.flush()?;
        }
        let opened = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary {
                path: store_dir.path(),
            },
            RocksDbResourceBudget::for_local_tests(),
            |_, _| Vec::new(),
        )?;
        assert_eq!(
            opened.db.get(b"existing")?.as_deref(),
            Some(b"old row".as_slice())
        );
        opened.db.put(b"new", b"new row")?;
        opened.db.flush()?;

        let mut versions = Vec::new();
        for table in opened.db.live_files()? {
            let mut sst =
                fs::File::open(store_dir.path().join(table.name.trim_start_matches('/')))?;
            // The block-based footer ends with a little-endian u32 version and u64 magic.
            sst.seek(SeekFrom::End(-12))?;
            let mut version = [0; 4];
            let mut magic = [0; 8];
            sst.read_exact(&mut version)?;
            sst.read_exact(&mut magic)?;
            assert_eq!(u64::from_le_bytes(magic), 0x88e2_41b7_85f4_cff7);
            versions.push(u32::from_le_bytes(version));
        }
        versions.sort_unstable();
        assert_eq!(versions, [6, 7]);
        Ok(())
    }

    #[test]
    fn a_failed_direct_open_logs_the_error_and_retries_buffered()
    -> Result<(), Box<dyn std::error::Error>> {
        let file = tempfile::NamedTempFile::new()?;
        let capture = zinder_testkit::LogCapture::install_for_target("zinder::store");
        let attempts = std::cell::Cell::new(0);
        let opened = open_bounded_rocksdb(
            RocksDbOpenRole::Primary { path: file.path() },
            RocksDbResourceBudget::for_local_tests(),
            |_, _| {
                attempts.set(attempts.get() + 1);
                Vec::new()
            },
        );
        assert!(opened.is_err());
        assert_eq!(attempts.get(), 2);
        assert!(capture.any(|event| {
            event.field("event") == Some("rocksdb_direct_io_open_failed")
                && event.field("error").is_some()
        }));
        Ok(())
    }

    #[test]
    fn default_family_uses_snappy_and_the_configured_write_buffers()
    -> Result<(), Box<dyn std::error::Error>> {
        let store_dir = tempfile::tempdir()?;
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.max_write_buffer_count = 3;
        let _bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary {
                path: store_dir.path(),
            },
            budget,
            |_, _| vec![ColumnFamilyDescriptor::new("rows", Options::default())],
        )?;
        let options = persisted_family_options(store_dir.path(), DEFAULT_COLUMN_FAMILY_NAME)?;
        for (name, expected) in [
            ("compression", "kSnappyCompression".to_owned()),
            ("write_buffer_size", budget.write_buffer_bytes.to_string()),
            (
                "max_write_buffer_number",
                budget.max_write_buffer_count.to_string(),
            ),
        ] {
            assert_eq!(options.get(name), Some(&expected), "{name}");
        }
        Ok(())
    }

    /// Reads the settings for `family` from the newest `OPTIONS-*` file, which
    /// `RocksDB` writes on every open from the options it applied.
    fn persisted_family_options(
        store: &Path,
        family: &str,
    ) -> Result<std::collections::BTreeMap<String, String>, Box<dyn std::error::Error>> {
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
            .filter_map(|line| line.trim().split_once('='))
            .map(|(name, setting)| (name.to_owned(), setting.to_owned()))
            .collect())
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
