use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use rust_rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, Options, WriteBufferManager,
    statistics::{StatsLevel, Ticker},
};

use crate::{RocksDbResourceBudget, RocksDbStatisticsLevel};

const DIRECT_IO_COMPACTION_READAHEAD_BYTES: usize = 2 * 1024 * 1024;
const SECONDARY_MAX_FILE_OPENING_THREADS: i32 = 1;

/// Filesystem I/O mode resolved while opening a `RocksDB` instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RocksDbIoMode {
    /// `RocksDB` opened with direct reads, and primary stores also opened
    /// flush and compaction work with direct I/O.
    Direct,
    /// `RocksDB` opened with buffered filesystem I/O after direct I/O was
    /// rejected by the filesystem or platform.
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
        }
    }

    const fn open_posture(self) -> RocksDbOpenPosture {
        match self {
            Self::CanonicalPrimary | Self::MaterializedViewPrimary => RocksDbOpenPosture::Primary,
            Self::CanonicalSecondary | Self::MaterializedViewSecondary => {
                RocksDbOpenPosture::Secondary
            }
        }
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
    /// Shared block cache retained for the database lifetime.
    pub block_cache: Cache,
    /// Shared write-buffer manager retained for the database lifetime.
    pub write_buffer_manager: WriteBufferManager,
    /// Resolved filesystem I/O mode.
    pub io_mode: RocksDbIoMode,
    /// Open options retained so the DB-wide statistics tickers stay readable
    /// for metric export.
    pub statistics: Arc<Options>,
}

/// Opens a bounded `RocksDB` instance with direct I/O when the platform supports it.
///
/// The column-family descriptors are rebuilt for each open attempt because
/// their options hold references to the helper-owned block cache. On direct
/// I/O failure, the function logs the unsupported mode and retries with the
/// same bounded resource budget under buffered I/O.
pub fn open_bounded_rocksdb(
    role: RocksDbOpenRole<'_>,
    rocksdb_resource_budget: RocksDbResourceBudget,
    build_column_families: impl Fn(&Cache, RocksDbResourceBudget) -> Vec<ColumnFamilyDescriptor>,
) -> Result<BoundedRocksDbOpen, rust_rocksdb::Error> {
    if let RocksDbOpenRole::ExistingPrimary { path } = role {
        DB::list_cf(&Options::default(), path)?;
    }
    let block_cache = build_block_cache(rocksdb_resource_budget.block_cache_bytes);
    let write_buffer_manager =
        build_write_buffer_manager(rocksdb_resource_budget.memtable_budget_bytes);
    let open_attempt = BoundedRocksDbOpenAttempt {
        role,
        rocksdb_resource_budget,
        block_cache: &block_cache,
        write_buffer_manager: &write_buffer_manager,
        build_column_families: &build_column_families,
    };

    match open_bounded_rocksdb_once(&open_attempt, RocksDbIoMode::Direct) {
        Ok((db, statistics)) => {
            record_rocksdb_io_mode(role, RocksDbIoMode::Direct);
            Ok(BoundedRocksDbOpen {
                db,
                block_cache,
                write_buffer_manager,
                io_mode: RocksDbIoMode::Direct,
                statistics: Arc::new(statistics),
            })
        }
        Err(direct_error) => {
            tracing::warn!(
                target: "zinder::store",
                event = "rocksdb_direct_io_unsupported",
                store_path = %role.store_path().display(),
                role = role.as_str(),
                error = %direct_error,
                "RocksDB direct I/O open failed; retrying with buffered I/O"
            );
            let (db, statistics) =
                open_bounded_rocksdb_once(&open_attempt, RocksDbIoMode::Buffered)?;
            record_rocksdb_io_mode(role, RocksDbIoMode::Buffered);
            Ok(BoundedRocksDbOpen {
                db,
                block_cache,
                write_buffer_manager,
                io_mode: RocksDbIoMode::Buffered,
                statistics: Arc::new(statistics),
            })
        }
    }
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
    let column_families = (open_attempt.build_column_families)(
        open_attempt.block_cache,
        open_attempt.rocksdb_resource_budget,
    );

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
    metrics::gauge!("zinder_store_block_cache_capacity_bytes", "store_role" => store_role)
        .set(u64_to_f64(inputs.resource_budget.block_cache_bytes));
    metrics::gauge!("zinder_store_block_cache_usage_bytes", "store_role" => store_role)
        .set(usize_to_f64(inputs.block_cache.get_usage()));
    metrics::gauge!("zinder_store_block_cache_pinned_usage_bytes", "store_role" => store_role)
        .set(usize_to_f64(inputs.block_cache.get_pinned_usage()));
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

/// DB-wide `RocksDB` statistics tickers exported as monotonic gauges.
///
/// Every ticker is aggregated across all column families of one store; the
/// bloom entries reflect the transparent point-lookup families, which share
/// the filter policy used by batched outpoint reads.
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
    Ticker::FlushWriteBytes,
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
/// The cache holds data, index, and bloom-filter blocks together so total
/// resident metadata stays bounded by `capacity_bytes`. Cloning the
/// returned handle shares the underlying allocation (`RocksDB`'s `Cache`
/// is reference counted), so callers needing the same cache across
/// multiple open paths pass `&` references rather than reconstructing.
#[must_use]
fn build_block_cache(capacity_bytes: u64) -> Cache {
    let capacity = usize::try_from(capacity_bytes).unwrap_or(usize::MAX);
    Cache::new_lru_cache(capacity)
}

fn build_write_buffer_manager(memtable_budget_bytes: u64) -> WriteBufferManager {
    let capacity = usize::try_from(memtable_budget_bytes).unwrap_or(usize::MAX);
    WriteBufferManager::new_write_buffer_manager(capacity, false)
}

/// Builds the shared `BlockBasedOptions` every column family routes through.
///
/// Index and filter blocks are accounted to `cache` so the at-rest
/// metadata budget stays bounded by [`RocksDbResourceBudget::block_cache_bytes`].
/// Public because the materialized-view store re-uses the same factory; see
/// [ADR-0020](../../../docs/adrs/0020-bounded-rocksdb-resource-budget.md).
#[must_use]
pub fn build_block_based_table_factory(cache: &Cache) -> BlockBasedOptions {
    let mut bbt = BlockBasedOptions::default();
    bbt.set_block_cache(cache);
    bbt.set_cache_index_and_filter_blocks(true);
    bbt.set_pin_l0_filter_and_index_blocks_in_cache(true);
    bbt
}

const PER_CF_INT_PROPERTIES: [&str; 13] = [
    "rocksdb.estimate-live-data-size",
    "rocksdb.total-sst-files-size",
    "rocksdb.size-all-mem-tables",
    "rocksdb.cur-size-active-mem-table",
    "rocksdb.estimate-table-readers-mem",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_gauge_throttle_admits_first_sample_then_holds_within_interval() {
        let throttle = ResourceGaugeThrottle::new(Duration::from_hours(1));
        assert!(throttle.should_sample());
        assert!(!throttle.should_sample());
        assert!(!throttle.should_sample());
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
