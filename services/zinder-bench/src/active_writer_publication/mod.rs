//! Concurrent active-writer, projector, and publisher publication benchmark.
//!
//! Exercises identical advancing Zcash Regtest workloads across four matched
//! writer/publication states (`quiescent_control`, `ingest_active`,
//! `ingest_and_projector`, `ingest_projector_and_publisher`) plus matched
//! `canonical_quiescent_control` (for canonical-only `ingest_active`) and
//! `published_quiescent_control` baselines, crossing a configurable shielded segment boundary while
//! concurrent lightwalletd compatibility readers measure publication
//! visibility, range first-frame and completion latency, uncovered-tail cold
//! refills and `64 MiB` byte-cap evictions, slow readers, transparent wallet
//! RPCs, mempool churn, durability fsyncs, `RocksDB` I/O activity, and exact
//! reply byte parity.

mod chain_fixture;
mod mempool;
mod parity;
mod pipelines;
mod readers;
pub mod report;
mod runner;
mod serving;
mod trace;

use std::{num::NonZeroU32, path::PathBuf, time::Duration};

pub use report::{
    ACTIVE_WRITER_PUBLICATION_CONTRACT, ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
    ActiveWriterPublicationReport, ByteParityReport, DifferentialBaselines, IngestStageMetrics,
    IoModeReport, LatencySummaryMs, MempoolTierMetrics, ModeDifferential, ModeParityDigests,
    ModeReport, NodeRpcCounts, ParityMismatch, ProjectorMetrics, ProtocolErrorCounts,
    PublisherMetrics, RocksDbActivityReport, StoreTickerDelta, StreamLatencyReport,
    TailRefillMetrics, WalletRpcMetrics, WorkloadGeometryReport, WriterFsyncCounts, WriterMode,
};
pub use runner::run_active_writer_publication;
use zinder_store::RocksDbIoMode;

use crate::BenchError;

/// Configuration parameters for one [`run_active_writer_publication`] run.
#[derive(Clone, Debug)]
pub struct ActiveWriterPublicationConfig {
    /// Pre-built base chain length before live commits start (default `4,092`).
    pub base_blocks: u32,
    /// Number of live blocks committed while the reader fleet is active (default `16`).
    pub live_blocks: u32,
    /// Canonical settlement reorg window depth in blocks (default `4`).
    pub reorg_window_blocks: u32,
    /// Settled shielded catalog segment width in blocks (default `4,096`).
    pub segment_blocks: NonZeroU32,
    /// Cadence (in blocks) for commitment-dense shielded blocks (default `4`).
    pub dense_block_cadence: u32,
    /// Number of synthetic shielded V5 transactions attached to each dense block (default `1`).
    pub dense_txs_per_block: u32,
    /// Number of Orchard actions included in each dense shielded V5 transaction (default `2`).
    pub dense_orchard_actions_per_tx: u32,
    /// Width (in blocks) of descending `GetBlockRange(tip, floor)` uncovered-tail probes.
    pub tail_probe_blocks: u32,
    /// Pacing interval between consecutive live commits while readers run.
    pub commit_interval: Duration,
    /// Artificial per-frame delay applied by the slow boundary-crossing reader.
    pub slow_reader_frame_delay: Duration,
    /// Pending-transaction churn batch sizes tested sequentially (e.g. `[50, 500]`).
    pub mempool_churn_sizes: Vec<u32>,
    /// Minimum request rounds for the `quiescent_control` baseline reader fleet.
    pub quiescent_rounds: u32,
    /// Optional parent scratch directory for stores (defaults to system temp directory).
    pub scratch_dir: Option<PathBuf>,
    /// Requested `RocksDB` I/O mode (`Direct` tries `O_DIRECT` with buffered fallback).
    pub io_mode: RocksDbIoMode,
    /// Optional worker count for signed-catalog publication (defaults to production clamping `1..=4`).
    pub publish_workers: Option<NonZeroU32>,
    /// When `true`, report validation requires `EncodedTail` `64 MiB` byte-cap evictions.
    pub require_tail_eviction: bool,
}

impl Default for ActiveWriterPublicationConfig {
    fn default() -> Self {
        Self {
            base_blocks: 4_092,
            live_blocks: 16,
            reorg_window_blocks: 4,
            segment_blocks: NonZeroU32::new(4_096).unwrap_or(NonZeroU32::MIN),
            dense_block_cadence: 4,
            dense_txs_per_block: 1,
            dense_orchard_actions_per_tx: 2,
            tail_probe_blocks: 24,
            commit_interval: Duration::from_millis(8),
            slow_reader_frame_delay: Duration::from_millis(3),
            mempool_churn_sizes: vec![50, 500],
            quiescent_rounds: 6,
            scratch_dir: None,
            io_mode: RocksDbIoMode::Direct,
            publish_workers: None,
            require_tail_eviction: false,
        }
    }
}

/// Validates geometry and runtime options before generating chain fixtures or stores.
///
/// # Errors
///
/// Returns [`BenchError::InvalidArgument`] when heights, reorg depth, tail probe
/// configuration, or mempool tier counts violate benchmark requirements.
pub fn validate_config(config: &ActiveWriterPublicationConfig) -> Result<(), BenchError> {
    if config.base_blocks <= config.reorg_window_blocks {
        return Err(BenchError::invalid_argument(
            "base_blocks must exceed reorg_window_blocks",
        ));
    }
    if config.reorg_window_blocks == 0 {
        return Err(BenchError::invalid_argument(
            "reorg_window_blocks must be positive",
        ));
    }
    if config.live_blocks == 0 {
        return Err(BenchError::invalid_argument("live_blocks must be positive"));
    }
    if config.dense_orchard_actions_per_tx == 0 {
        return Err(BenchError::invalid_argument(
            "dense_orchard_actions_per_tx must be positive",
        ));
    }
    if config.tail_probe_blocks < 2 {
        return Err(BenchError::invalid_argument(
            "tail_probe_blocks must be at least 2 so descending uncovered-tail probes are strictly descending",
        ));
    }
    if config.quiescent_rounds == 0 {
        return Err(BenchError::invalid_argument(
            "quiescent_rounds must be positive",
        ));
    }
    if config.mempool_churn_sizes.is_empty() {
        return Err(BenchError::invalid_argument(
            "mempool_churn_sizes must name at least one batch size",
        ));
    }
    if u64::from(config.live_blocks)
        < u64::try_from(config.mempool_churn_sizes.len()).unwrap_or(u64::MAX)
    {
        return Err(BenchError::invalid_argument(
            "live_blocks must be at least mempool_churn_sizes.len() so every mempool tier overlaps live commits",
        ));
    }
    if config.require_tail_eviction
        && (config.dense_block_cadence == 0 || config.dense_txs_per_block == 0)
    {
        return Err(BenchError::invalid_argument(
            "require_tail_eviction needs positive dense_block_cadence and dense_txs_per_block",
        ));
    }
    let base_settled = config
        .base_blocks
        .saturating_sub(config.reorg_window_blocks);
    let width = config.segment_blocks.get();
    let next_boundary = ((base_settled / width).saturating_add(1)).saturating_mul(width);
    let final_tip = config.base_blocks.saturating_add(config.live_blocks);
    let final_settled = final_tip.saturating_sub(config.reorg_window_blocks);
    if final_settled < next_boundary {
        return Err(BenchError::invalid_argument(format!(
            "final settled height ({final_settled}) must reach next segment boundary ({next_boundary})"
        )));
    }
    Ok(())
}
