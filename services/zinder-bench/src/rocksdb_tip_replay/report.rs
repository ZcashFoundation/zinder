//! The tip replay report: provenance, inputs, configuration in effect, and
//! per-block measurements with their summaries.

use serde::{Deserialize, Serialize};
use zinder_bench::fixture::WorkloadDensity;
use zinder_store::RocksDbResourceBudget;
use zinder_wallet_projection::WalletProjectionReadyEvidence;

use super::{
    measurement::{BlockSample, StratumRules, TimerReport},
    session::{CanonicalPosition, ResolvedIoModes},
};

/// Contract identity of a tip replay report.
pub(super) const REPORT_CONTRACT_IDENTITY: &str = "rocksdb-tip-replay";
/// Version of the tip replay report shape.
pub(super) const REPORT_FORMAT_VERSION: u32 = 1;

/// Complete evidence from one tip replay run.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct TipReplayReport {
    pub(super) contract_identity: String,
    pub(super) report_format_version: u32,
    pub(super) provenance: Provenance,
    pub(super) inputs: Inputs,
    pub(super) configuration: Configuration,
    pub(super) method: Method,
    /// Whole-process wall time, CPU time, and peak resident memory.
    pub(super) process: ProcessResources,
    /// Timer summaries pooled over every block of every repeat.
    pub(super) aggregate: TimerReport,
    pub(super) unavailable_measurements: Vec<UnavailableMeasurement>,
    pub(super) repeats: Vec<RepeatReport>,
}

/// Who built and ran the measured binary.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Provenance {
    pub(super) benchmark_version: String,
    /// Source revision supplied by the operator with `--software-revision`.
    pub(super) software_revision: String,
    pub(super) label: Option<String>,
    pub(super) build: BuildProvenance,
    pub(super) host: HostProvenance,
    pub(super) started_at_unix_millis: u64,
    pub(super) completed_at_unix_millis: u64,
}

/// Compiler settings recorded by the build script.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct BuildProvenance {
    /// Cargo profile directory, such as `release`, or `debug` for the dev profile.
    pub(super) cargo_profile: String,
    /// `release` or `debug`, the profile family Cargo reports to build scripts.
    pub(super) cargo_profile_kind: String,
    pub(super) opt_level: String,
    pub(super) debug_info: String,
    pub(super) debug_assertions: bool,
    /// Every rustflag Cargo passed, from the environment or Cargo configuration.
    pub(super) rustflags: String,
    pub(super) rustc_version: String,
    pub(super) target: String,
}

const BUILD_PROFILE: &str = env!("ZINDER_BENCH_BUILD_PROFILE");
const BUILD_PROFILE_KIND: &str = env!("ZINDER_BENCH_BUILD_PROFILE_KIND");
const BUILD_OPT_LEVEL: &str = env!("ZINDER_BENCH_BUILD_OPT_LEVEL");
const BUILD_DEBUG_INFO: &str = env!("ZINDER_BENCH_BUILD_DEBUG_INFO");
const BUILD_RUSTFLAGS: &str = env!("ZINDER_BENCH_BUILD_RUSTFLAGS");
const BUILD_RUSTC_VERSION: &str = env!("ZINDER_BENCH_BUILD_RUSTC_VERSION");
const BUILD_TARGET: &str = env!("ZINDER_BENCH_BUILD_TARGET");

impl BuildProvenance {
    pub(super) fn of_this_binary() -> Self {
        Self {
            cargo_profile: BUILD_PROFILE.to_owned(),
            cargo_profile_kind: BUILD_PROFILE_KIND.to_owned(),
            opt_level: BUILD_OPT_LEVEL.to_owned(),
            debug_info: BUILD_DEBUG_INFO.to_owned(),
            debug_assertions: cfg!(debug_assertions),
            rustflags: BUILD_RUSTFLAGS.to_owned(),
            rustc_version: BUILD_RUSTC_VERSION.to_owned(),
            target: BUILD_TARGET.to_owned(),
        }
    }
}

/// Host the replay ran on.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct HostProvenance {
    pub(super) os: String,
    pub(super) arch: String,
    pub(super) available_parallelism: Option<u32>,
}

impl HostProvenance {
    pub(super) fn current() -> Self {
        Self {
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            available_parallelism: std::thread::available_parallelism()
                .ok()
                .and_then(|parallelism| u32::try_from(parallelism.get()).ok()),
        }
    }
}

/// The immutable inputs every repeat copied.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Inputs {
    pub(super) network: String,
    pub(super) fixture: FixtureInput,
    pub(super) canonical_store: StoreInput<FenceRecord>,
    pub(super) wallet_store: StoreInput<WalletRecord>,
}

/// The fixture and its tip replay checkpoints.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct FixtureInput {
    pub(super) path: String,
    pub(super) manifest_sha256: String,
    pub(super) tip_replay_checkpoints_sha256: String,
    pub(super) from_height: u32,
    pub(super) to_height: u32,
    pub(super) block_count: u32,
    pub(super) tip_hash_hex: String,
    pub(super) workload_density: WorkloadDensity,
}

/// One input store and the state it started at.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct StoreInput<Start> {
    pub(super) path: String,
    pub(super) bytes: u64,
    pub(super) start: Start,
}

/// A canonical fence and settled tip.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct FenceRecord {
    pub(super) chain_epoch: u64,
    pub(super) event_sequence: u64,
    pub(super) tip_height: u32,
    pub(super) tip_hash_hex: String,
    pub(super) settled_tip_height: u32,
    pub(super) settled_tip_hash_hex: String,
    pub(super) sequence_block_count: u64,
    pub(super) sequence_digest_hex: String,
}

impl From<CanonicalPosition> for FenceRecord {
    fn from(position: CanonicalPosition) -> Self {
        let fence = position.fence;
        Self {
            chain_epoch: fence.chain_epoch_id().value(),
            event_sequence: fence.chain_event_sequence(),
            tip_height: fence.visible_tip().height.value(),
            tip_hash_hex: hex::encode(fence.visible_tip().hash.as_bytes()),
            settled_tip_height: position.settled_tip.height.value(),
            settled_tip_hash_hex: hex::encode(position.settled_tip.hash.as_bytes()),
            sequence_block_count: fence.sequence_digest().block_count(),
            sequence_digest_hex: hex::encode(fence.sequence_digest().as_bytes()),
        }
    }
}

/// A wallet projection's digest, canonical source, and row counts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct WalletRecord {
    pub(super) projection_digest_hex: String,
    pub(super) source_chain_epoch: u64,
    pub(super) source_event_sequence: u64,
    pub(super) source_tip_height: u32,
    pub(super) source_tip_hash_hex: String,
    pub(super) settled_tip_height: u32,
    pub(super) transparent_unspent_output_count: u64,
    pub(super) transparent_spent_output_count: u64,
    pub(super) transparent_address_transaction_count: u64,
    pub(super) transparent_address_balance_count: u64,
    pub(super) reorg_undo_count: u64,
    pub(super) utxo_count: u64,
    pub(super) utxo_total_value_zat: u64,
}

impl From<&WalletProjectionReadyEvidence> for WalletRecord {
    fn from(evidence: &WalletProjectionReadyEvidence) -> Self {
        let source = evidence.source_position;
        let rows = evidence.row_counts;
        Self {
            projection_digest_hex: hex::encode(evidence.projection_digest.as_bytes()),
            source_chain_epoch: source.chain_epoch_id.value(),
            source_event_sequence: source.event_sequence,
            source_tip_height: source.tip.height.value(),
            source_tip_hash_hex: hex::encode(source.tip.hash.as_bytes()),
            settled_tip_height: evidence.settled_tip.height.value(),
            transparent_unspent_output_count: rows.transparent_unspent_output_count,
            transparent_spent_output_count: rows.transparent_spent_output_count,
            transparent_address_transaction_count: rows.transparent_address_transaction_count,
            transparent_address_balance_count: rows.transparent_address_balance_count,
            reorg_undo_count: rows.reorg_undo_count,
            utxo_count: evidence.utxo_summary.utxo_count,
            utxo_total_value_zat: evidence.utxo_summary.total_value_zat,
        }
    }
}

/// Settings in effect for every repeat.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Configuration {
    pub(super) repeat_count: u32,
    pub(super) scratch_directory: String,
    pub(super) keep_scratch: bool,
    pub(super) raw_blob_retention: String,
    pub(super) requested_reorg_window_blocks: u32,
    pub(super) effective_reorg_window_blocks: u32,
    pub(super) max_transition_logical_bytes: u64,
    pub(super) canonical_writer_budget: BudgetRecord,
    pub(super) canonical_secondary_budget: BudgetRecord,
    pub(super) wallet_budget: BudgetRecord,
    pub(super) rocksdb_io_mode: IoModeConfiguration,
    /// The writer's age-based event pruning, which never runs in a replay.
    pub(super) event_retention_pruning: String,
}

/// One `RocksDB` resource budget.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct BudgetRecord {
    pub(super) block_cache_bytes: u64,
    pub(super) max_wal_bytes: u64,
    pub(super) max_open_files: i32,
    pub(super) write_buffer_bytes: u64,
    pub(super) max_write_buffer_count: i32,
    pub(super) max_background_jobs: i32,
    pub(super) memtable_budget_bytes: u64,
    pub(super) statistics_level: String,
}

impl From<RocksDbResourceBudget> for BudgetRecord {
    fn from(budget: RocksDbResourceBudget) -> Self {
        Self {
            block_cache_bytes: budget.block_cache_bytes,
            max_wal_bytes: budget.max_wal_bytes,
            max_open_files: budget.max_open_files,
            write_buffer_bytes: budget.write_buffer_bytes,
            max_write_buffer_count: budget.max_write_buffer_count,
            max_background_jobs: budget.max_background_jobs,
            memtable_budget_bytes: budget.memtable_budget_bytes,
            statistics_level: budget.statistics_level.as_str().to_owned(),
        }
    }
}

/// How the stores chose their I/O mode, and what they resolved to.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct IoModeConfiguration {
    /// Whether the replay could select the mode; the stores fix the policy.
    pub(super) selectable: bool,
    pub(super) policy: String,
    pub(super) resolved: IoModes,
}

/// The I/O mode each store open resolved to.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct IoModes {
    pub(super) canonical_primary: String,
    pub(super) canonical_secondary: String,
    pub(super) wallet: String,
}

impl From<&ResolvedIoModes> for IoModes {
    fn from(modes: &ResolvedIoModes) -> Self {
        Self {
            canonical_primary: modes.canonical_primary.to_owned(),
            canonical_secondary: modes.canonical_secondary.to_owned(),
            wallet: modes.wallet.to_owned(),
        }
    }
}

/// How the summaries were computed.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Method {
    pub(super) percentile_method: String,
    pub(super) stratum_rules: StratumRules,
}

/// Whole-process resources.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ProcessResources {
    pub(super) wall_seconds: f64,
    pub(super) cpu_seconds: f64,
    pub(super) peak_rss_bytes: Option<u64>,
    pub(super) peak_rss_source: String,
}

/// A measurement the report cannot provide on this revision.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct UnavailableMeasurement {
    pub(super) measurement: String,
    pub(super) reason: String,
}

/// Scratch stores kept after the final repeat.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct KeptScratch {
    pub(super) canonical_store: String,
    pub(super) wallet_store: String,
}

/// Evidence from one replay over fresh scratch copies.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RepeatReport {
    pub(super) repeat_index: u32,
    pub(super) scratch_copy_seconds: f64,
    pub(super) store_open_seconds: f64,
    /// Wall time of the block loop only.
    pub(super) replay_wall_seconds: f64,
    /// Process CPU time, every thread included, during the block loop.
    pub(super) replay_process_cpu_seconds: f64,
    pub(super) blocks_per_second: f64,
    /// Process peak resident memory when the repeat finished.
    pub(super) peak_rss_bytes_at_end: Option<u64>,
    pub(super) io_modes: IoModes,
    pub(super) final_canonical: FenceRecord,
    pub(super) final_wallet: WalletRecord,
    pub(super) kept_scratch: Option<KeptScratch>,
    pub(super) timers: TimerReport,
    pub(super) blocks: Vec<BlockSample>,
}

/// Measurements the stores do not expose on this revision.
pub(super) fn unavailable_measurements() -> Vec<UnavailableMeasurement> {
    vec![
        UnavailableMeasurement {
            measurement: "point_read_counts".to_owned(),
            reason: "RocksDbCanonicalStore, RocksDbCanonicalSecondary, and RocksDbWalletFollowingStore expose no per-operation read counts".to_owned(),
        },
        UnavailableMeasurement {
            measurement: "rocksdb_statistics_deltas".to_owned(),
            reason: "the canonical and wallet stores keep their RocksDB statistics private and export no ticker values".to_owned(),
        },
    ]
}
