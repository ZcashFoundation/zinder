//! Concurrent active-writer, projector, and publisher publication benchmark.
//!
//! Exercises identical advancing Zcash Regtest workloads across four matched
//! writer/publication states (`quiescent_control`, `ingest_active`,
//! `ingest_and_projector`, `ingest_projector_and_publisher`) crossing a
//! `4,096`-block shielded segment boundary while concurrent lightwalletd
//! compatibility readers measure publication visibility, range first-frame and
//! completion latency, uncovered-tail cold refills, slow readers, mempool
//! churn, durability fsyncs, `RocksDB` I/O activity, and exact reply byte parity.

mod chain_fixture;
mod mempool;
mod readers;
pub mod report;
mod runner;

use std::{num::NonZeroU32, time::Duration};

pub use report::{
    ACTIVE_WRITER_PUBLICATION_CONTRACT, ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
    ActiveWriterPublicationReport, ByteParityReport, DifferentialBaselines, IngestStageMetrics,
    IoModeReport, LatencySummaryMs, MempoolTierMetrics, ModeDifferential, ModeParityDigests,
    ModeReport, NodeRpcCounts, ParityMismatch, ProjectorMetrics, ProtocolErrorCounts,
    PublisherMetrics, RocksDbActivityReport, StoreTickerDelta, StreamLatencyReport,
    TailRefillMetrics, WorkloadGeometryReport, WriterFsyncCounts, WriterMode,
};
pub use runner::run_active_writer_publication;

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
    /// Number of synthetic shielded V5 transactions attached to each dense block.
    pub dense_txs_per_block: u32,
    /// Pacing interval between consecutive live commits while readers run.
    pub commit_interval: Duration,
    /// Artificial per-frame delay applied by the slow boundary-crossing reader.
    pub slow_reader_frame_delay: Duration,
    /// Pending-transaction churn batch sizes tested sequentially (e.g. `[50, 500]`).
    pub mempool_churn_sizes: Vec<u32>,
    /// Minimum request rounds for the `quiescent_control` baseline reader fleet.
    pub quiescent_rounds: u32,
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
            commit_interval: Duration::from_millis(8),
            slow_reader_frame_delay: Duration::from_millis(3),
            mempool_churn_sizes: vec![50, 500],
            quiescent_rounds: 6,
        }
    }
}
