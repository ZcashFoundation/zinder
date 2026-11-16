//! Role-scoped `RocksDB` resource budget.
//!
//! `RocksDbResourceBudget` separates the two halves of `RocksDB` option setting that
//! [ADR-0020](../../../docs/adrs/0020-bounded-rocksdb-resource-budget.md)
//! identifies:
//!
//! - **Architectural invariants** (WAL on, point-in-time recovery, atomic
//!   flush, ordered writes, and direct-I/O fallback) live in the bounded
//!   `RocksDB` open path. They are not tunable; touching them breaks the
//!   per-epoch commit contract.
//! - **Bounded resource budget** lives here. The numbers below cap the
//!   block-cache, table-cache, WAL, primary-writer background jobs,
//!   per-column-family write buffers, and total memtable memory surfaces so a
//!   crash-replay open or bulk write does not OOM the host. The defaults target
//!   a mainnet-sized store and are documented in
//!   [the resource-tuning runbook](../../../docs/runbooks/bulk-catchup-resource-tuning.md).
//!
//! Construct one with writer defaults for primary stores and reader defaults
//! for `RocksDB` secondaries. Operators may override individual fields through
//! `[storage.canonical.rocksdb]`, `[storage.materialized_views.rocksdb]`, and
//! `[wallet.rocksdb]` in their TOML.

use crate::RocksDbIoMode;

/// Bounded `RocksDB` resource budget applied to one DB instance at open.
///
/// The fields together cap the resident-memory peak at roughly
/// `block_cache_bytes + max_wal_bytes + memtable_budget_bytes` regardless of
/// store size. Each field has a single concrete effect:
///
/// - `block_cache_bytes` is the size of the bounded LRU cache shared by
///   data blocks, index blocks, and the filter blocks of any column family
///   that configures a filter. Without a bounded cache, `RocksDB` pins index
///   and filter blocks per-SST in resident memory, which scales with store
///   size.
/// - `max_wal_bytes` is the ceiling for the live WAL across all column
///   families. Crossing it triggers a memtable flush so the WAL truncates.
///   The default of 0 (`RocksDB`'s own) means "never trigger from WAL size",
///   which is the bug the OOM-recovery runbook documents.
/// - `max_open_files` caps the number of `SST` file handles `RocksDB` keeps
///   open. The default of -1 (`RocksDB`'s own) means "open every `SST` and
///   pin its metadata", which is what makes a mainnet-sized store's
///   open-time RSS scale with store size.
/// - `write_buffer_bytes` caps each column family's mutable memtable before it
///   rotates to an immutable memtable and flushes to an `SST`.
/// - `max_write_buffer_count` caps how many mutable plus immutable memtables a
///   column family may hold before writes stall.
/// - `max_background_jobs` caps the primary writer's shared `RocksDB`
///   background job pool used for flushes and compactions. `OpenAsSecondary`
///   disables automatic flushes and compactions, so secondary opens retain the
///   field in this uniform budget type but do not apply it.
/// - `memtable_budget_bytes` caps total memtable memory across column
///   families via `RocksDB`'s write-buffer manager.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RocksDbResourceBudget {
    /// Bounded LRU cache size for data, index, and filter blocks.
    pub block_cache_bytes: u64,
    /// Total live WAL size ceiling across all column families.
    pub max_wal_bytes: u64,
    /// Open `SST` file handle limit. `RocksDB` takes `i32` natively;
    /// negative values mean "unbounded".
    pub max_open_files: i32,
    /// Per-column-family mutable memtable size.
    pub write_buffer_bytes: u64,
    /// Per-column-family mutable plus immutable memtable count.
    pub max_write_buffer_count: i32,
    /// Primary-writer background flush and compaction job limit.
    pub max_background_jobs: i32,
    /// Total mutable and immutable memtable memory budget across column families.
    pub memtable_budget_bytes: u64,
    /// `RocksDB` statistics collection gate.
    pub statistics_level: RocksDbStatisticsLevel,
    /// Filesystem I/O mode the open attempts first.
    ///
    /// Every profile defaults to [`RocksDbIoMode::Direct`], which falls back
    /// to buffered I/O when the filesystem rejects direct I/O.
    /// [`RocksDbIoMode::Buffered`] skips the direct attempt. Only the
    /// benchmark harness overrides it; no configuration section exposes it.
    pub preferred_io_mode: RocksDbIoMode,
}

impl RocksDbResourceBudget {
    /// Smallest accepted [`Self::block_cache_bytes`]. Anything smaller
    /// degenerates into per-`SST` pinning, which is the regression
    /// [ADR-0020](../../../docs/adrs/0020-bounded-rocksdb-resource-budget.md)
    /// closes.
    pub const MIN_BLOCK_CACHE_BYTES: u64 = 4 * MIB;

    /// Smallest accepted [`Self::max_wal_bytes`]. Zero specifically would
    /// disable `RocksDB`'s WAL-size flush trigger and reopen the
    /// bulk-catchup OOM trap; smaller-but-nonzero values would force the
    /// writer to flush on every batch.
    pub const MIN_MAX_WAL_BYTES: u64 = 4 * MIB;

    /// Smallest accepted [`Self::max_open_files`]. Negative values
    /// (`RocksDB`'s "unbounded") pin every `SST`'s metadata in resident
    /// memory.
    pub const MIN_MAX_OPEN_FILES: i32 = 32;

    /// Smallest accepted [`Self::write_buffer_bytes`]. Smaller values create
    /// flush churn and make high-throughput catch-up compaction-bound.
    pub const MIN_WRITE_BUFFER_BYTES: u64 = 4 * MIB;

    /// Smallest accepted [`Self::max_write_buffer_count`]. One buffer cannot
    /// absorb writes while another buffer flushes.
    pub const MIN_MAX_WRITE_BUFFER_COUNT: i32 = 2;

    /// Smallest accepted [`Self::max_background_jobs`] for primary writers.
    /// One background job cannot flush and compact concurrently when both
    /// kinds of maintenance are pending. The uniform budget keeps this valid
    /// value for secondary profiles even though secondary opens do not apply it.
    pub const MIN_MAX_BACKGROUND_JOBS: i32 = 2;

    /// Smallest accepted [`Self::memtable_budget_bytes`]. Smaller values
    /// force constant flush churn and do not leave enough room for one
    /// bounded column-family memtable to rotate cleanly.
    pub const MIN_MEMTABLE_BUDGET_BYTES: u64 = 4 * MIB;

    /// Canonical-store writer defaults sized for a mainnet-shaped deployment.
    ///
    /// `512 MiB` block cache, `256 MiB` WAL ceiling, `512` open file handles,
    /// `16 MiB x 2` write buffers per column family, `2` background jobs, and
    /// `256 MiB` total memtable budget. See ADR-0020 for the budget derivation.
    #[must_use]
    pub const fn canonical_writer_defaults() -> Self {
        Self {
            block_cache_bytes: 512 * MIB,
            max_wal_bytes: 256 * MIB,
            max_open_files: 512,
            write_buffer_bytes: 16 * MIB,
            max_write_buffer_count: 2,
            max_background_jobs: 2,
            memtable_budget_bytes: 256 * MIB,
            statistics_level: RocksDbStatisticsLevel::Tickers,
            preferred_io_mode: RocksDbIoMode::Direct,
        }
    }

    /// Materialized-view store writer defaults sized for sustained multi-CF replay.
    ///
    /// `256 MiB` block cache, `256 MiB` WAL ceiling, `512` open file handles,
    /// `16 MiB x 4` write buffers per column family, `2` background jobs, and
    /// `512 MiB` total memtable budget. Replay writes many consumer families in
    /// every ordered dispatch; the larger shared memtable envelope lets hot
    /// families rotate while compaction catches up instead of turning flush
    /// churn into write stalls. The write-buffer manager remains the hard
    /// aggregate bound.
    #[must_use]
    pub const fn materialized_view_writer_defaults() -> Self {
        Self {
            block_cache_bytes: 256 * MIB,
            max_wal_bytes: 256 * MIB,
            max_open_files: 512,
            write_buffer_bytes: 16 * MIB,
            max_write_buffer_count: 4,
            max_background_jobs: 2,
            memtable_budget_bytes: 512 * MIB,
            statistics_level: RocksDbStatisticsLevel::Tickers,
            preferred_io_mode: RocksDbIoMode::Direct,
        }
    }

    /// Wallet-projection writer defaults.
    ///
    /// The wallet projection currently has the same multi-column-family write
    /// posture as a materialized-view consumer, but it is a distinct durable
    /// ownership domain and therefore has its own named profile.
    #[must_use]
    pub const fn wallet_projection_writer_defaults() -> Self {
        Self::materialized_view_writer_defaults()
    }

    /// Canonical-store reader defaults for secondary processes.
    ///
    /// `128 MiB` block cache, `32 MiB` WAL ceiling, `128` open file handles,
    /// `8 MiB x 2` write buffers per column family, and `16 MiB` total memtable
    /// budget. The uniform budget retains the primary-only background-job value
    /// at `2`, but secondary opens do not apply it. Secondary readers replay the
    /// writer's manifest and serve public traffic; their memory posture must
    /// stay below the writer so readers cannot starve clean sync.
    #[must_use]
    pub const fn canonical_reader_defaults() -> Self {
        Self {
            block_cache_bytes: 128 * MIB,
            max_wal_bytes: 32 * MIB,
            max_open_files: 128,
            write_buffer_bytes: 8 * MIB,
            max_write_buffer_count: 2,
            max_background_jobs: 2,
            memtable_budget_bytes: 16 * MIB,
            statistics_level: RocksDbStatisticsLevel::Tickers,
            preferred_io_mode: RocksDbIoMode::Direct,
        }
    }

    /// Materialized-view store reader defaults for secondary processes.
    ///
    /// `64 MiB` block cache, `16 MiB` WAL ceiling, `64` open file handles,
    /// `4 MiB x 2` write buffers per column family, and `16 MiB` total memtable
    /// budget. The uniform budget retains the primary-only background-job value
    /// at `2`, but secondary opens do not apply it. Materialized-view reader stores are
    /// rebuildable and subordinate to the canonical reader path.
    #[must_use]
    pub const fn materialized_view_reader_defaults() -> Self {
        Self {
            block_cache_bytes: 64 * MIB,
            max_wal_bytes: 16 * MIB,
            max_open_files: 64,
            write_buffer_bytes: 4 * MIB,
            max_write_buffer_count: 2,
            max_background_jobs: 2,
            memtable_budget_bytes: 16 * MIB,
            statistics_level: RocksDbStatisticsLevel::Tickers,
            preferred_io_mode: RocksDbIoMode::Direct,
        }
    }

    /// Wallet-projection reader defaults for immutable serving secondaries.
    ///
    /// The serving secondary currently uses the same resource profile as a
    /// materialized-view reader, while retaining a distinct domain name at the
    /// configuration boundary.
    #[must_use]
    pub const fn wallet_projection_reader_defaults() -> Self {
        Self::materialized_view_reader_defaults()
    }

    /// Defaults for throwaway local test stores.
    ///
    /// `32 MiB` block cache, `16 MiB` WAL ceiling, `64` open file handles,
    /// `4 MiB x 2` write buffers per column family, a `2`-job primary limit,
    /// and an `8 MiB` total memtable budget. Keeps unit-test memory footprint
    /// negligible while still exercising the bounded code path.
    #[must_use]
    pub const fn for_local_tests() -> Self {
        Self {
            block_cache_bytes: 32 * MIB,
            max_wal_bytes: 16 * MIB,
            max_open_files: 64,
            write_buffer_bytes: 4 * MIB,
            max_write_buffer_count: 2,
            max_background_jobs: 2,
            memtable_budget_bytes: 8 * MIB,
            statistics_level: RocksDbStatisticsLevel::Tickers,
            preferred_io_mode: RocksDbIoMode::Direct,
        }
    }

    /// Validates that the budget keeps the bounded-resource invariants
    /// closed before the caller builds `RocksDB` options from it.
    pub const fn validate(self) -> Result<(), &'static str> {
        if self.block_cache_bytes < Self::MIN_BLOCK_CACHE_BYTES {
            return Err("rocksdb_resource_budget.block_cache_bytes must be at least 4 MiB");
        }
        if self.max_wal_bytes < Self::MIN_MAX_WAL_BYTES {
            return Err(
                "rocksdb_resource_budget.max_wal_bytes must be at least 4 MiB; zero disables RocksDB's WAL-size flush trigger",
            );
        }
        if self.max_open_files < Self::MIN_MAX_OPEN_FILES {
            return Err(
                "rocksdb_resource_budget.max_open_files must be at least 32; negative values pin every SST's metadata in resident memory",
            );
        }
        if self.write_buffer_bytes < Self::MIN_WRITE_BUFFER_BYTES {
            return Err("rocksdb_resource_budget.write_buffer_bytes must be at least 4 MiB");
        }
        if self.max_write_buffer_count < Self::MIN_MAX_WRITE_BUFFER_COUNT {
            return Err("rocksdb_resource_budget.max_write_buffer_count must be at least 2");
        }
        if self.max_background_jobs < Self::MIN_MAX_BACKGROUND_JOBS {
            return Err("rocksdb_resource_budget.max_background_jobs must be at least 2");
        }
        if self.memtable_budget_bytes < Self::MIN_MEMTABLE_BUDGET_BYTES {
            return Err("rocksdb_resource_budget.memtable_budget_bytes must be at least 4 MiB");
        }
        Ok(())
    }
}

/// `RocksDB` statistics collection gate applied at open.
///
/// Statistics collection has measurable CPU cost on write-heavy paths;
/// this gate lets an operator trade the `zinder_store_rocksdb_ticker`
/// series away for that headroom without a code change.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RocksDbStatisticsLevel {
    /// No `enable_statistics()` call at all. The bounded LRU cache and any
    /// configured filters still run; only their counters stop being collected.
    /// `zinder_store_rocksdb_ticker` exports nothing at this level.
    Off,
    /// Ticker counters only (`StatsLevel::ExceptHistogramOrTimers`), the
    /// production default.
    #[default]
    Tickers,
    /// `RocksDB`'s own default level: tickers plus per-operation timer
    /// histograms. Use when histogram detail is required.
    Full,
}

impl RocksDbStatisticsLevel {
    /// Stable lowercase label used in TOML, environment variables, and
    /// `--print-config` output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Tickers => "tickers",
            Self::Full => "full",
        }
    }

    /// Resolves the inverse of [`Self::as_str`], returning `None` for any
    /// input other than `off`, `tickers`, or `full`.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "off" => Some(Self::Off),
            "tickers" => Some(Self::Tickers),
            "full" => Some(Self::Full),
            _ => None,
        }
    }
}

const MIB: u64 = 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_writer_defaults_match_mainnet_envelope() {
        let budget = RocksDbResourceBudget::canonical_writer_defaults();
        assert_eq!(budget.block_cache_bytes, 512 * MIB);
        assert_eq!(budget.max_wal_bytes, 256 * MIB);
        assert_eq!(budget.max_open_files, 512);
        assert_eq!(budget.write_buffer_bytes, 16 * MIB);
        assert_eq!(budget.max_write_buffer_count, 2);
        assert_eq!(budget.max_background_jobs, 2);
        assert_eq!(budget.memtable_budget_bytes, 256 * MIB);
    }

    #[test]
    fn materialized_view_writer_defaults_reserve_multi_family_compaction_headroom() {
        let canonical = RocksDbResourceBudget::canonical_writer_defaults();
        let materialized_view = RocksDbResourceBudget::materialized_view_writer_defaults();
        assert_eq!(
            materialized_view.block_cache_bytes * 2,
            canonical.block_cache_bytes
        );
        assert_eq!(materialized_view.max_wal_bytes, canonical.max_wal_bytes);
        assert_eq!(materialized_view.max_open_files, canonical.max_open_files);
        assert_eq!(
            materialized_view.write_buffer_bytes,
            canonical.write_buffer_bytes
        );
        assert_eq!(materialized_view.max_write_buffer_count, 4);
        assert_eq!(
            materialized_view.max_background_jobs,
            canonical.max_background_jobs
        );
        assert_eq!(
            materialized_view.memtable_budget_bytes,
            canonical.memtable_budget_bytes * 2
        );
    }

    #[test]
    fn reader_defaults_are_smaller_than_writer_defaults() {
        assert!(
            RocksDbResourceBudget::canonical_reader_defaults().block_cache_bytes
                < RocksDbResourceBudget::canonical_writer_defaults().block_cache_bytes
        );
        assert!(
            RocksDbResourceBudget::materialized_view_reader_defaults().block_cache_bytes
                < RocksDbResourceBudget::materialized_view_writer_defaults().block_cache_bytes
        );
    }

    #[test]
    fn wallet_projection_profiles_match_the_current_multi_family_profiles() {
        assert_eq!(
            RocksDbResourceBudget::wallet_projection_writer_defaults(),
            RocksDbResourceBudget::materialized_view_writer_defaults()
        );
        assert_eq!(
            RocksDbResourceBudget::wallet_projection_reader_defaults(),
            RocksDbResourceBudget::materialized_view_reader_defaults()
        );
    }

    #[test]
    fn local_test_defaults_fit_in_megabytes() {
        let budget = RocksDbResourceBudget::for_local_tests();
        assert!(budget.block_cache_bytes <= 64 * MIB);
        assert!(budget.max_wal_bytes <= 32 * MIB);
        assert!(budget.max_open_files <= 128);
        assert!(budget.write_buffer_bytes <= 8 * MIB);
        assert!(budget.memtable_budget_bytes <= 8 * MIB);
    }

    #[test]
    fn zero_wal_budget_is_rejected() {
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.max_wal_bytes = 0;
        assert!(matches!(budget.validate(), Err(reason) if reason.contains("max_wal_bytes")));
    }

    #[test]
    fn negative_open_file_budget_is_rejected() {
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.max_open_files = -1;
        assert!(matches!(budget.validate(), Err(reason) if reason.contains("max_open_files")));
    }

    #[test]
    fn undersized_write_buffer_budget_is_rejected() {
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.write_buffer_bytes = MIB;
        assert!(matches!(budget.validate(), Err(reason) if reason.contains("write_buffer_bytes")));
    }

    #[test]
    fn single_write_buffer_is_rejected() {
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.max_write_buffer_count = 1;
        assert!(
            matches!(budget.validate(), Err(reason) if reason.contains("max_write_buffer_count"))
        );
    }

    #[test]
    fn single_background_job_is_rejected() {
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.max_background_jobs = 1;
        assert!(matches!(
            budget.validate(),
            Err(reason) if reason.contains("max_background_jobs")
        ));
    }

    #[test]
    fn every_default_budget_preserves_two_background_jobs() {
        for budget in [
            RocksDbResourceBudget::canonical_writer_defaults(),
            RocksDbResourceBudget::materialized_view_writer_defaults(),
            RocksDbResourceBudget::wallet_projection_writer_defaults(),
            RocksDbResourceBudget::canonical_reader_defaults(),
            RocksDbResourceBudget::materialized_view_reader_defaults(),
            RocksDbResourceBudget::wallet_projection_reader_defaults(),
            RocksDbResourceBudget::for_local_tests(),
        ] {
            assert_eq!(budget.max_background_jobs, 2);
        }
    }

    #[test]
    fn undersized_memtable_budget_is_rejected() {
        let mut budget = RocksDbResourceBudget::for_local_tests();
        budget.memtable_budget_bytes = MIB;
        assert!(
            matches!(budget.validate(), Err(reason) if reason.contains("memtable_budget_bytes"))
        );
    }

    #[test]
    fn every_default_budget_starts_at_tickers() {
        for budget in [
            RocksDbResourceBudget::canonical_writer_defaults(),
            RocksDbResourceBudget::materialized_view_writer_defaults(),
            RocksDbResourceBudget::wallet_projection_writer_defaults(),
            RocksDbResourceBudget::canonical_reader_defaults(),
            RocksDbResourceBudget::materialized_view_reader_defaults(),
            RocksDbResourceBudget::wallet_projection_reader_defaults(),
            RocksDbResourceBudget::for_local_tests(),
        ] {
            assert_eq!(budget.statistics_level, RocksDbStatisticsLevel::Tickers);
        }
    }

    #[test]
    fn statistics_level_default_is_tickers() {
        assert_eq!(
            RocksDbStatisticsLevel::default(),
            RocksDbStatisticsLevel::Tickers
        );
    }

    #[test]
    fn statistics_level_round_trips_through_as_str() {
        for level in [
            RocksDbStatisticsLevel::Off,
            RocksDbStatisticsLevel::Tickers,
            RocksDbStatisticsLevel::Full,
        ] {
            assert_eq!(RocksDbStatisticsLevel::parse(level.as_str()), Some(level));
        }
    }

    #[test]
    fn statistics_level_rejects_unknown_text() {
        assert_eq!(RocksDbStatisticsLevel::parse("verbose"), None);
    }
}
