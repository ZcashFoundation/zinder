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
    pub(super) build: crate::build_provenance::BuildProvenance,
    pub(super) host: HostProvenance,
    pub(super) started_at_unix_millis: u64,
    pub(super) completed_at_unix_millis: u64,
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
    pub(super) block_hash_by_height_count: u64,
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
            block_hash_by_height_count: rows.block_hash_by_height_count,
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
    pub(super) wallet_statistics: WalletStatisticsConfiguration,
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

/// How the stores chose their I/O mode, what the run requested, and what they resolved to.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct IoModeConfiguration {
    pub(super) policy: String,
    pub(super) requested: RequestedIoModes,
    pub(super) resolved: IoModes,
}

/// The I/O mode requested for each store group.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct RequestedIoModes {
    /// The canonical writer and its secondary.
    pub(super) canonical: String,
    pub(super) wallet: String,
}

/// Whether the wallet store's statistics tickers were collected.
///
/// Collection changes the wallet store's `RocksDB` options and can change
/// timings, so a report of a collecting run is not comparable with one that
/// did not collect.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct WalletStatisticsConfiguration {
    pub(super) collected: bool,
    /// `RocksDB` ticker names reported per block; empty when not collected.
    pub(super) tickers: Vec<String>,
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

/// How the stores were prepared and the summaries computed.
#[derive(Debug, Serialize, Deserialize)]
#[allow(
    clippy::struct_field_names,
    reason = "`percentile_method` is a stable report field name"
)]
pub(super) struct Method {
    pub(super) percentile_method: String,
    pub(super) stratum_rules: StratumRules,
    /// Page-cache state of each repeat's scratch copies when its stores open:
    /// `evicted-after-copy`, or `not-evicted` on a platform without
    /// `posix_fadvise`, where buffered stores start with their files cached.
    pub(super) scratch_page_cache: String,
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

/// Measurements the report cannot provide on this run.
///
/// Wallet-store point-read counts and statistics deltas are available only
/// when the replay collected them; the canonical stores expose neither.
pub(super) fn unavailable_measurements(
    wallet_statistics_collected: bool,
) -> Vec<UnavailableMeasurement> {
    if wallet_statistics_collected {
        return vec![UnavailableMeasurement {
            measurement: "canonical_store_read_statistics".to_owned(),
            reason: "RocksDbCanonicalStore and RocksDbCanonicalSecondary expose no per-operation read counts or statistics tickers".to_owned(),
        }];
    }
    vec![
        UnavailableMeasurement {
            measurement: "point_read_counts".to_owned(),
            reason: "not collected; pass --wallet-rocksdb-statistics for the wallet store, and the canonical stores expose none".to_owned(),
        },
        UnavailableMeasurement {
            measurement: "rocksdb_statistics_deltas".to_owned(),
            reason: "not collected; pass --wallet-rocksdb-statistics for the wallet store, and the canonical stores expose none".to_owned(),
        },
    ]
}
