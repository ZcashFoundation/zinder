//! Typed report structures, percentile summaries, invariant validation, and
//! byte-parity verification for `active-writer-publication`.

use std::time::Duration;

use rust_rocksdb::statistics::Ticker;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::ActiveWriterPublicationConfig;
use crate::BenchError;

/// Stable contract identifier for this benchmark report format.
pub const ACTIVE_WRITER_PUBLICATION_CONTRACT: &str = "active-writer-publication";
/// Current schema version for [`ActiveWriterPublicationReport`].
pub const ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION: u32 = 2;

/// Tickers collected around each writer, reader, projector, and publisher lifecycle.
pub(crate) const MONITORED_TICKERS: [Ticker; 10] = [
    Ticker::WalFileSynced,
    Ticker::WalFileBytes,
    Ticker::FlushWriteBytes,
    Ticker::CompactReadBytes,
    Ticker::CompactWriteBytes,
    Ticker::BytesRead,
    Ticker::BytesWritten,
    Ticker::BlockCacheDataHit,
    Ticker::BlockCacheDataMiss,
    Ticker::StallMicros,
];

/// Snapshot of [`MONITORED_TICKERS`] for one store handle.
#[derive(Clone, Debug, Default)]
pub(crate) struct TickerCounterSnapshot {
    counts: [u64; MONITORED_TICKERS.len()],
}

impl TickerCounterSnapshot {
    /// Captures all monitored tickers from a single `read_ticker` closure.
    pub(crate) fn capture<F>(mut read_ticker: F) -> Self
    where
        F: FnMut(Ticker) -> u64,
    {
        Self {
            counts: MONITORED_TICKERS.map(&mut read_ticker),
        }
    }

    /// Computes saturated deltas against an earlier snapshot.
    pub(crate) fn delta_since(&self, earlier: &Self) -> StoreTickerDelta {
        StoreTickerDelta {
            wal_synced: self.counts[0].saturating_sub(earlier.counts[0]),
            wal_bytes: self.counts[1].saturating_sub(earlier.counts[1]),
            flush_write_bytes: self.counts[2].saturating_sub(earlier.counts[2]),
            compact_read_bytes: self.counts[3].saturating_sub(earlier.counts[3]),
            compact_write_bytes: self.counts[4].saturating_sub(earlier.counts[4]),
            bytes_read: self.counts[5].saturating_sub(earlier.counts[5]),
            bytes_written: self.counts[6].saturating_sub(earlier.counts[6]),
            block_cache_data_hit: self.counts[7].saturating_sub(earlier.counts[7]),
            block_cache_data_miss: self.counts[8].saturating_sub(earlier.counts[8]),
            stall_micros: self.counts[9].saturating_sub(earlier.counts[9]),
        }
    }
}

/// Ticker delta summary for one `RocksDB` store instance.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreTickerDelta {
    /// Count of `rocksdb.wal.synced` operations.
    pub wal_synced: u64,
    /// Bytes appended to WAL (`rocksdb.wal.bytes`).
    pub wal_bytes: u64,
    /// Bytes flushed from memtables (`rocksdb.flush.write.bytes`).
    pub flush_write_bytes: u64,
    /// Bytes read by compaction (`rocksdb.compact.read.bytes`).
    pub compact_read_bytes: u64,
    /// Bytes written by compaction (`rocksdb.compact.write.bytes`).
    pub compact_write_bytes: u64,
    /// Total read bytes (`rocksdb.bytes.read`).
    pub bytes_read: u64,
    /// Total written bytes (`rocksdb.bytes.written`).
    pub bytes_written: u64,
    /// Block cache data hits (`rocksdb.block.cache.data.hit`).
    pub block_cache_data_hit: u64,
    /// Block cache data misses (`rocksdb.block.cache.data.miss`).
    pub block_cache_data_miss: u64,
    /// Accumulated write stall duration (`rocksdb.stall.micros`).
    pub stall_micros: u64,
}

impl StoreTickerDelta {
    /// Sums two deltas across e.g. alternating secondary generations.
    #[must_use]
    pub const fn saturating_add(self, other: Self) -> Self {
        Self {
            wal_synced: self.wal_synced.saturating_add(other.wal_synced),
            wal_bytes: self.wal_bytes.saturating_add(other.wal_bytes),
            flush_write_bytes: self
                .flush_write_bytes
                .saturating_add(other.flush_write_bytes),
            compact_read_bytes: self
                .compact_read_bytes
                .saturating_add(other.compact_read_bytes),
            compact_write_bytes: self
                .compact_write_bytes
                .saturating_add(other.compact_write_bytes),
            bytes_read: self.bytes_read.saturating_add(other.bytes_read),
            bytes_written: self.bytes_written.saturating_add(other.bytes_written),
            block_cache_data_hit: self
                .block_cache_data_hit
                .saturating_add(other.block_cache_data_hit),
            block_cache_data_miss: self
                .block_cache_data_miss
                .saturating_add(other.block_cache_data_miss),
            stall_micros: self.stall_micros.saturating_add(other.stall_micros),
        }
    }
}

/// Four matched writer/publication states exercised against an identical chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriterMode {
    /// Closed stores at the final tip with no active writers or publishers.
    QuiescentControl,
    /// Canonical writer following tip commits concurrently with readers.
    IngestActive,
    /// Canonical writer plus concurrent wallet projection transitions.
    IngestAndProjector,
    /// Canonical writer, wallet projector, and concurrent shielded catalog publisher.
    IngestProjectorAndPublisher,
}

impl WriterMode {
    /// Stable lowercase identifier of this mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QuiescentControl => "quiescent_control",
            Self::IngestActive => "ingest_active",
            Self::IngestAndProjector => "ingest_and_projector",
            Self::IngestProjectorAndPublisher => "ingest_projector_and_publisher",
        }
    }

    /// Whether this mode serves a wallet projection alongside canonical state.
    #[must_use]
    pub const fn has_wallet_serving(self) -> bool {
        matches!(
            self,
            Self::QuiescentControl | Self::IngestAndProjector | Self::IngestProjectorAndPublisher
        )
    }
}

/// Complete report emitted by `zinder-bench active-writer-publication`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActiveWriterPublicationReport {
    /// Contract identity string (`"active-writer-publication"`).
    pub contract_identity: String,
    /// Report schema format version.
    pub report_format_version: u32,
    /// Chain and workload geometry used across all matched arms.
    pub workload: WorkloadGeometryReport,
    /// Baseline quiescent arm with signed catalog published through `crossed_segment_boundary_height`
    /// (matched baseline for `ingest_projector_and_publisher`).
    pub published_quiescent_control: ModeReport,
    /// Per-mode measurement records in canonical execution order (`quiescent_control`,
    /// `ingest_active`, `ingest_and_projector`, `ingest_projector_and_publisher`).
    pub modes: Vec<ModeReport>,
    /// Stage differentials comparing each active mode against its matched quiescent control.
    pub differentials_vs_quiescent: Vec<ModeDifferential>,
    /// Byte-level reply parity comparison across all arms at the final tip.
    pub byte_parity: ByteParityReport,
}

impl ActiveWriterPublicationReport {
    /// Validates internal invariants across all arms, parity checks, and
    /// configured acceptance constraints.
    ///
    /// # Errors
    ///
    /// Returns [`BenchError`] if any arm recorded unexpected gRPC errors, missed
    /// required overlap or publication stages, failed byte parity, or failed a
    /// requested tail-eviction requirement.
    pub fn validate(&self, config: &ActiveWriterPublicationConfig) -> Result<(), BenchError> {
        if !self.byte_parity.all_matched {
            return Err(BenchError::acceptance_completion_mismatch(format!(
                "byte parity mismatch across modes: {:?}",
                self.byte_parity.mismatches
            )));
        }
        let expected_modes = [
            WriterMode::QuiescentControl,
            WriterMode::IngestActive,
            WriterMode::IngestAndProjector,
            WriterMode::IngestProjectorAndPublisher,
        ];
        if self.modes.len() != expected_modes.len()
            || self
                .modes
                .iter()
                .zip(expected_modes)
                .any(|(report, expected)| report.mode != expected)
        {
            return Err(BenchError::acceptance_completion_mismatch(
                "report modes must match [quiescent_control, ingest_active, ingest_and_projector, ingest_projector_and_publisher]",
            ));
        }

        validate_single_mode(&self.published_quiescent_control, true, config)?;
        for mode_report in &self.modes {
            validate_single_mode(
                mode_report,
                mode_report.mode == WriterMode::IngestProjectorAndPublisher,
                config,
            )?;
        }

        if config.require_tail_eviction {
            let evicted = std::iter::once(&self.published_quiescent_control)
                .chain(self.modes.iter())
                .any(|report| {
                    report.tail_refill.encoded_tail_byte_cap_evictions > 0
                        && report.tail_refill.byte_cap_exceeded
                });
            if !evicted {
                return Err(BenchError::acceptance_hard_limit_missed(
                    "encoded_tail_64mib_eviction_not_observed",
                ));
            }
        }
        Ok(())
    }
}

fn validate_single_mode(
    report: &ModeReport,
    expect_published_catalog: bool,
    config: &ActiveWriterPublicationConfig,
) -> Result<(), BenchError> {
    if report.errors.unexpected_grpc_errors > 0 {
        return Err(BenchError::acceptance_completion_mismatch(format!(
            "{} recorded {} unexpected gRPC errors: {:?}",
            report.mode.as_str(),
            report.errors.unexpected_grpc_errors,
            report.errors.unexpected_error_samples
        )));
    }
    if report.latest_block_rpc_ms.samples == 0
        || report.catalog_range.completion_ms.samples == 0
        || report.slow_boundary_range.completion_ms.samples == 0
        || report.tree_state_ms.samples == 0
        || report.tail_refill.cold_after_advance.completion_ms.samples == 0
        || report.tail_refill.warm_repeat.completion_ms.samples == 0
        || report.tail_refill.encoded_tail_cache_fills == 0
        || report.tail_refill.encoded_tail_cache_hits == 0
    {
        return Err(BenchError::acceptance_telemetry_missing(format!(
            "{} missing core reader or EncodedTail Prometheus samples",
            report.mode.as_str()
        )));
    }
    if expect_published_catalog && report.catalog_range_published.completion_ms.samples == 0 {
        return Err(BenchError::acceptance_telemetry_missing(format!(
            "{} missing published catalog samples",
            report.mode.as_str()
        )));
    }
    if report.mode.has_wallet_serving()
        && (report.wallet_rpc_ms.taddress_balance_ms.samples == 0
            || report.wallet_rpc_ms.address_utxos_ms.samples == 0
            || report.wallet_rpc_ms.get_transaction_ms.samples == 0)
    {
        return Err(BenchError::acceptance_telemetry_missing(format!(
            "{} missing wallet projection RPC samples",
            report.mode.as_str()
        )));
    }
    validate_mode_pipeline_stages(report, config)
}

fn validate_mode_pipeline_stages(
    report: &ModeReport,
    config: &ActiveWriterPublicationConfig,
) -> Result<(), BenchError> {
    if report.mode == WriterMode::QuiescentControl {
        if report.ingest.blocks_committed != 0 || report.commit_to_served_tip_ms.samples != 0 {
            return Err(BenchError::acceptance_completion_mismatch(
                "quiescent_control unexpectedly recorded live writer activity",
            ));
        }
        return Ok(());
    }
    let expected_live = usize::try_from(config.live_blocks).unwrap_or(usize::MAX);
    if report.ingest.blocks_committed != config.live_blocks
        || report.commit_to_served_tip_ms.samples != expected_live
    {
        return Err(BenchError::acceptance_completion_mismatch(format!(
            "{} committed {}/{} blocks with {} tip visibility samples",
            report.mode.as_str(),
            report.ingest.blocks_committed,
            config.live_blocks,
            report.commit_to_served_tip_ms.samples
        )));
    }
    if report
        .mempool_churn
        .iter()
        .any(|tier| !tier.writer_overlap_verified || tier.commits_observed_during_tier == 0)
    {
        return Err(BenchError::acceptance_completion_mismatch(format!(
            "{} failed to overlap active writer commits across all mempool tiers",
            report.mode.as_str()
        )));
    }
    if matches!(
        report.mode,
        WriterMode::IngestAndProjector | WriterMode::IngestProjectorAndPublisher
    ) && report.projector.transitions_applied < config.live_blocks
    {
        return Err(BenchError::acceptance_completion_mismatch(format!(
            "{} projected only {} transitions (< {})",
            report.mode.as_str(),
            report.projector.transitions_applied,
            config.live_blocks
        )));
    }
    if report.mode == WriterMode::IngestProjectorAndPublisher
        && (report.publisher.segments_published == 0 || report.publisher.catalog_refreshes == 0)
    {
        return Err(BenchError::acceptance_completion_mismatch(
            "ingest_projector_and_publisher did not seal and refresh signed catalog",
        ));
    }
    Ok(())
}

/// Chain and trace geometry shared identically by all matched modes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkloadGeometryReport {
    /// Network identifier (`"zcash-regtest"`).
    pub network: String,
    /// Number of pre-built base blocks before live tip progression begins.
    pub base_blocks: u32,
    /// Number of live blocks committed while the reader fleet is active.
    pub live_blocks: u32,
    /// Final chain tip height across all matched arms.
    pub final_tip_height: u32,
    /// Canonical reorg window (settlement lag in blocks).
    pub reorg_window_blocks: u32,
    /// Settled catalog segment size in blocks (`4,096` by default).
    pub segment_blocks: u32,
    /// Shielded catalog segment boundary crossed during live commits.
    pub crossed_segment_boundary_height: u32,
    /// Sparse blocks present in the generated chain.
    pub sparse_block_count: u32,
    /// Commitment-dense shielded blocks present in the generated chain.
    pub dense_block_count: u32,
    /// Orchard actions per dense shielded transaction.
    pub dense_orchard_actions_per_tx: u32,
    /// Transparent coinbase-spend transactions included across the generated chain.
    pub transparent_spend_txs: u32,
    /// Descending uncovered-tail probe width in blocks.
    pub tail_probe_blocks: u32,
    /// Requested `RocksDB` I/O mode (`"direct"` or `"buffered"`).
    pub requested_io_mode: String,
    /// Worker count used for signed-catalog publication.
    pub publish_workers: u32,
    /// Distinct mempool churn transaction counts exercised (e.g. `[50, 500]`).
    pub mempool_churn_sizes: Vec<u32>,
    /// Deterministic SHA-256 hex digest of the shared `WorkloadTrace`.
    pub request_trace_digest: String,
}

/// Nearest-rank latency distribution in milliseconds.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct LatencySummaryMs {
    /// Number of observed samples.
    pub samples: usize,
    /// 50th percentile in milliseconds.
    pub p50: f64,
    /// 95th percentile in milliseconds.
    pub p95: f64,
    /// Maximum observed sample in milliseconds.
    pub max: f64,
    /// Arithmetic mean in milliseconds.
    pub mean: f64,
}

impl LatencySummaryMs {
    /// Summarizes duration samples in milliseconds using nearest-rank percentiles.
    #[must_use]
    pub fn from_durations(durations: &[Duration]) -> Self {
        let millis: Vec<f64> = durations
            .iter()
            .map(|duration| duration.as_secs_f64() * 1_000.0)
            .collect();
        Self::from_f64(&millis)
    }

    /// Summarizes floating-point samples using nearest-rank percentiles.
    #[must_use]
    pub fn from_f64(raw_samples: &[f64]) -> Self {
        if raw_samples.is_empty() {
            return Self::default();
        }
        let mut sorted = raw_samples.to_vec();
        sorted.sort_by(f64::total_cmp);
        let sum: f64 = sorted.iter().sum();
        let mean = sum / usize_to_f64(sorted.len());
        Self {
            samples: sorted.len(),
            p50: nearest_rank(&sorted, 50),
            p95: nearest_rank(&sorted, 95),
            max: sorted.last().copied().unwrap_or(0.0),
            mean,
        }
    }
}

/// Dual latency record for streaming RPCs (`first_frame` and `completion`).
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct StreamLatencyReport {
    /// Time from logical RPC start (including any transient retries) to the first delivered frame.
    pub first_frame_ms: LatencySummaryMs,
    /// Time from logical RPC start (including any transient retries) to stream EOF.
    pub completion_ms: LatencySummaryMs,
    /// Total decoded frames delivered across completed calls.
    pub frames_delivered: u64,
    /// Total protobuf-encoded payload bytes delivered across completed calls.
    pub payload_bytes_delivered: u64,
}

/// Separate ingest preparation and commit histograms emitted by `zinder-ingest`.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct IngestStageMetrics {
    /// Samples of `zinder_ingest_canonical_live_prepare_seconds` (converted to ms).
    pub live_prepare_ms: LatencySummaryMs,
    /// Samples of `zinder_ingest_canonical_live_commit_seconds` (converted to ms).
    pub live_commit_ms: LatencySummaryMs,
    /// Total live blocks committed in this mode.
    pub blocks_committed: u32,
}

/// Wallet-projection gRPC endpoint latency distributions.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct WalletRpcMetrics {
    /// `GetTaddressBalance` unary RPC timings.
    pub taddress_balance_ms: LatencySummaryMs,
    /// `GetAddressUtxos` unary RPC timings.
    pub address_utxos_ms: LatencySummaryMs,
    /// `GetTransaction` unary RPC timings for confirmed chain transactions.
    pub get_transaction_ms: LatencySummaryMs,
}

/// Concurrent wallet-projector apply timings.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ProjectorMetrics {
    /// End-to-end duration per retained event application.
    pub apply_ms: LatencySummaryMs,
    /// Number of retained events applied to the wallet store.
    pub transitions_applied: u32,
}

/// Shielded-history catalog publication timings.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct PublisherMetrics {
    /// Wall duration across all `publish_available` passes while running.
    pub total_ms: f64,
    /// Per-call duration of `publish_available` invocations that sealed a segment.
    pub segment_publish_ms: LatencySummaryMs,
    /// Total segments newly published during this mode run.
    pub segments_published: usize,
    /// Total catalog refresh swaps completed on the serving listener.
    pub catalog_refreshes: u64,
}

/// Encoded tail fill/refill timing and `64 MiB` byte-cap observation from
/// standard `zinder_compat_encoded_tail_*` Prometheus counters.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct TailRefillMetrics {
    /// First tail `GetBlockRange(tip, floor)` after tip advance or generation rotation.
    pub cold_after_advance: StreamLatencyReport,
    /// Immediate repeat tail `GetBlockRange(tip, floor)` on the same generation.
    pub warm_repeat: StreamLatencyReport,
    /// Peak payload bytes observed across a single uncovered-tail range response.
    pub peak_tail_response_bytes: u64,
    /// Peak retained bytes observed on `zinder_compat_encoded_tail_bytes`.
    pub peak_encoded_tail_bytes: u64,
    /// Delta of `zinder_compat_encoded_tail_byte_cap_evictions_total` during this mode run.
    pub encoded_tail_byte_cap_evictions: u64,
    /// Delta of `zinder_compat_encoded_tail_byte_capped_fills_total` during this mode run.
    pub encoded_tail_byte_capped_fills: u64,
    /// Delta of `zinder_compat_encoded_tail_fills_total` (`rebuild` + `prepend`).
    pub encoded_tail_cache_fills: u64,
    /// Delta of `zinder_compat_encoded_tail_lookups_total{outcome="hit"}`.
    pub encoded_tail_cache_hits: u64,
    /// Delta of `zinder_compat_encoded_tail_lookups_total{outcome="fallback_native"}`.
    pub encoded_tail_fallback_native: u64,
    /// Delta of `zinder_compat_encoded_tail_lookups_total{outcome="bypass"}`.
    pub encoded_tail_bypass: u64,
    /// Cold/warm pairs where the warm probe ran against a different served generation.
    pub unpaired_tail_probes: u64,
    /// Whether `EncodedTail` reported any byte-capped fill or byte-cap eviction.
    pub byte_cap_exceeded: bool,
}

/// Mempool churn read timings for a specific pending-transaction batch size.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct MempoolTierMetrics {
    /// Target churn size (`50` or `500` pending transactions).
    pub batch_size: u32,
    /// `GetMempoolTx` completion timing and delivered compact tx count.
    pub mempool_tx_stream: StreamLatencyReport,
    /// `GetMempoolStream` tip-change closure observations.
    pub mempool_stream_closures: u64,
    /// Whether this tier overlapped active canonical writer commits.
    pub writer_overlap_verified: bool,
    /// Live commits completed while this mempool tier's probes ran.
    pub commits_observed_during_tier: u32,
}

/// Resolved filesystem I/O modes reported by `RocksDB` (`"direct"` or `"buffered"`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IoModeReport {
    /// Canonical primary store resolved I/O mode (`None` in `quiescent_control`).
    pub canonical_primary: Option<String>,
    /// Canonical serving secondaries resolved I/O modes.
    pub canonical_secondaries: Vec<String>,
    /// Wallet writer store resolved I/O mode (`None` when projector inactive).
    pub wallet_primary: Option<String>,
    /// Wallet serving secondaries resolved I/O modes (`Vec::new()` in `ingest_active`).
    pub wallet_secondaries: Vec<String>,
    /// Projector canonical secondary resolved I/O mode (`None` when projector inactive).
    pub projector_secondary: Option<String>,
    /// Publisher secondary store resolved I/O mode (`None` when publisher inactive).
    pub publisher_secondary: Option<String>,
}

/// Writer durability fsync counters from `RocksDB` `wal.synced`.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct WriterFsyncCounts {
    /// `rocksdb.wal.synced` on the canonical writer during live commits.
    pub canonical_wal_synced: u64,
    /// `rocksdb.wal.bytes` on the canonical writer during live commits.
    pub canonical_wal_bytes: u64,
    /// `rocksdb.wal.synced` on the wallet writer during projector transitions.
    pub wallet_wal_synced: u64,
    /// `rocksdb.wal.bytes` on the wallet writer during projector transitions.
    pub wallet_wal_bytes: u64,
}

/// Per-method upstream `NodeSource` RPC call counts observed during a mode run.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct NodeRpcCounts {
    /// Calls to `tip_id`.
    pub tip_id: u64,
    /// Calls to `fetch_block_at`.
    pub fetch_block_at: u64,
    /// Calls to `fetch_chain_segment`.
    pub fetch_chain_segment: u64,
    /// Calls to `fetch_chain_checkpoint`.
    pub fetch_chain_checkpoint: u64,
    /// Calls to `fetch_subtree_roots`.
    pub fetch_subtree_roots: u64,
    /// Sum of all upstream `NodeSource` calls.
    pub total: u64,
}

/// `RocksDB` SST flush, compaction, and read activity across active roles.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct RocksDbActivityReport {
    /// Canonical writer store activity during live commits.
    pub canonical_writer: StoreTickerDelta,
    /// Serving canonical secondaries activity during the reader run (across both generations).
    pub reader_canonical_secondaries: StoreTickerDelta,
    /// Serving wallet secondaries activity during the reader run (across both generations).
    pub reader_wallet_secondaries: StoreTickerDelta,
    /// Wallet writer store activity during projector transitions.
    pub wallet_writer: StoreTickerDelta,
    /// Projector canonical secondary activity while reading canonical events.
    pub projector_secondary: StoreTickerDelta,
    /// Publisher canonical secondary activity during catalog sealing.
    pub publisher_secondary: StoreTickerDelta,
}

/// Classified reader protocol errors separated from expected tip-change behavior.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProtocolErrorCounts {
    /// Unexpected non-OK gRPC statuses returned across all reader tasks.
    pub unexpected_grpc_errors: u64,
    /// Sample diagnostic messages for any unexpected errors (bounded).
    pub unexpected_error_samples: Vec<String>,
    /// Transient `Unavailable` statuses cleanly retried while tip advanced.
    pub transient_retry_count: u64,
}

/// Complete telemetry output for a single matched [`WriterMode`] execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModeReport {
    /// Mode name (`quiescent_control`, `ingest_active`, `ingest_and_projector`,
    /// `ingest_projector_and_publisher`).
    pub mode: WriterMode,
    /// Total wall-clock duration of the measured reader workload window.
    pub workload_duration_ms: f64,
    /// Resolved `RocksDB` I/O modes for every open handle in this arm.
    pub io_modes: IoModeReport,
    /// Separate prepare/commit timings recorded by `zinder-ingest`.
    pub ingest: IngestStageMetrics,
    /// `GetLatestBlock` unary RPC latency measured in every arm.
    pub latest_block_rpc_ms: LatencySummaryMs,
    /// Commit-to-served-tip visibility delay across live commits (`0` samples in quiescent controls).
    pub commit_to_served_tip_ms: LatencySummaryMs,
    /// Ordinary settled catalog `GetBlockRange` timing across all catalog phases.
    pub catalog_range: StreamLatencyReport,
    /// Settled catalog samples served strictly before the signed catalog covered the range.
    pub catalog_range_unpublished: StreamLatencyReport,
    /// Settled catalog samples served strictly once the signed catalog covered the range.
    pub catalog_range_published: StreamLatencyReport,
    /// Settled catalog samples whose call spanned catalog seal/adoption mid-flight.
    pub catalog_range_during_seal: StreamLatencyReport,
    /// Uncovered live-tail `GetBlockRange(tip, floor)` timing and `EncodedTail` Prometheus deltas.
    pub tail_refill: TailRefillMetrics,
    /// Slow reader `GetBlockRange` crossing the segment boundary into live tail.
    pub slow_boundary_range: StreamLatencyReport,
    /// `GetTreeState` latency across catalog and tail heights.
    pub tree_state_ms: LatencySummaryMs,
    /// Real wallet-projection unary RPC timings (`0` samples in `ingest_active`).
    pub wallet_rpc_ms: WalletRpcMetrics,
    /// Mempool churn results for each configured batch size.
    pub mempool_churn: Vec<MempoolTierMetrics>,
    /// Concurrent wallet projector metrics.
    pub projector: ProjectorMetrics,
    /// Concurrent shielded history publisher metrics.
    pub publisher: PublisherMetrics,
    /// Canonical and wallet WAL fsync counts and bytes.
    pub writer_fsyncs: WriterFsyncCounts,
    /// Upstream `NodeSource` RPC call counts.
    pub node_rpcs: NodeRpcCounts,
    /// `RocksDB` SST flush, compaction, and read counters per role.
    pub rocksdb_activity: RocksDbActivityReport,
    /// Protocol errors observed by readers.
    pub errors: ProtocolErrorCounts,
}

/// Matched quiescent baseline pair passed to [`ModeDifferential::between`].
#[derive(Clone, Copy, Debug)]
pub struct DifferentialBaselines<'a> {
    /// Unpublished `quiescent_control` report (canonical storage baseline).
    pub unpublished: &'a ModeReport,
    /// Published `published_quiescent_control` report (signed catalog baseline).
    pub published: &'a ModeReport,
    /// Matched baseline label (`"quiescent_control"` or `"published_quiescent_control"`).
    pub baseline_label: &'a str,
    /// Whether the candidate's primary comparison uses the published catalog baseline.
    pub baseline_catalog_published: bool,
}

/// Latency and activity deltas for one active mode compared to its matched quiescent baseline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModeDifferential {
    /// Active writer mode being compared.
    pub mode: WriterMode,
    /// Matched quiescent baseline identifier (`"quiescent_control"` or `"published_quiescent_control"`).
    pub baseline_label: String,
    /// Whether the matched quiescent baseline held a published signed catalog.
    pub baseline_catalog_published: bool,
    /// Change in phase-matched settled catalog `GetBlockRange` completion p50 (ms), if available.
    pub catalog_completion_p50_delta_ms: Option<f64>,
    /// Change in phase-matched settled catalog `GetBlockRange` completion p95 (ms), if available.
    pub catalog_completion_p95_delta_ms: Option<f64>,
    /// Change in unpublished canonical settled catalog completion p50 vs `quiescent_control` (ms).
    pub unpublished_catalog_completion_p50_delta_ms: Option<f64>,
    /// Change in unpublished canonical settled catalog completion p95 vs `quiescent_control` (ms).
    pub unpublished_catalog_completion_p95_delta_ms: Option<f64>,
    /// Change in signed-catalog settled completion p50 vs `published_quiescent_control` (ms).
    pub published_catalog_completion_p50_delta_ms: Option<f64>,
    /// Change in signed-catalog settled completion p95 vs `published_quiescent_control` (ms).
    pub published_catalog_completion_p95_delta_ms: Option<f64>,
    /// Change in cold tail refill completion p50 vs matched baseline (ms), if available.
    pub cold_tail_completion_p50_delta_ms: Option<f64>,
    /// Change in cold tail refill completion p95 vs matched baseline (ms), if available.
    pub cold_tail_completion_p95_delta_ms: Option<f64>,
    /// Change in warm tail repeat completion p50 vs matched baseline (ms), if available.
    pub warm_tail_completion_p50_delta_ms: Option<f64>,
    /// Change in warm tail repeat completion p95 vs matched baseline (ms), if available.
    pub warm_tail_completion_p95_delta_ms: Option<f64>,
    /// Change in slow boundary-crossing completion p50 (ms), if both arms recorded samples.
    pub slow_boundary_completion_p50_delta_ms: Option<f64>,
    /// Change in `GetTreeState` p95 (ms), if both arms recorded samples.
    pub tree_state_p95_delta_ms: Option<f64>,
    /// Change in `GetLatestBlock` unary p50 (ms), if both arms recorded samples.
    pub latest_block_rpc_p50_delta_ms: Option<f64>,
    /// Change in `GetTaddressBalance` p50 (ms), if both arms served wallet RPCs.
    pub wallet_taddress_balance_p50_delta_ms: Option<f64>,
    /// Change in `GetAddressUtxos` p50 (ms), if both arms served wallet RPCs.
    pub wallet_address_utxos_p50_delta_ms: Option<f64>,
    /// Change in `GetTransaction` p50 (ms), if both arms served wallet RPCs.
    pub wallet_get_transaction_p50_delta_ms: Option<f64>,
    /// Change in serving secondary read bytes compared to matched quiescent baseline.
    pub reader_bytes_read_delta: i64,
}

fn optional_delta(
    baseline: LatencySummaryMs,
    candidate: LatencySummaryMs,
    pick: fn(LatencySummaryMs) -> f64,
) -> Option<f64> {
    (baseline.samples > 0 && candidate.samples > 0).then(|| pick(candidate) - pick(baseline))
}

fn optional_p50_p95(
    baseline: &LatencySummaryMs,
    candidate: &LatencySummaryMs,
) -> (Option<f64>, Option<f64>) {
    (
        optional_delta(*baseline, *candidate, |summary| summary.p50),
        optional_delta(*baseline, *candidate, |summary| summary.p95),
    )
}

fn total_reader_bytes_read(report: &ModeReport) -> u64 {
    report
        .rocksdb_activity
        .reader_canonical_secondaries
        .bytes_read
        .saturating_add(report.rocksdb_activity.reader_wallet_secondaries.bytes_read)
}

impl ModeDifferential {
    /// Computes stage latency and secondary-read deltas relative to matched `baselines`.
    #[must_use]
    pub fn between(baselines: DifferentialBaselines<'_>, candidate: &ModeReport) -> Self {
        let primary = if baselines.baseline_catalog_published {
            baselines.published
        } else {
            baselines.unpublished
        };
        let (unpublished_p50, unpublished_p95) = optional_p50_p95(
            &baselines
                .unpublished
                .catalog_range_unpublished
                .completion_ms,
            &candidate.catalog_range_unpublished.completion_ms,
        );
        let (published_p50, published_p95) = optional_p50_p95(
            &baselines.published.catalog_range_published.completion_ms,
            &candidate.catalog_range_published.completion_ms,
        );
        let (catalog_p50, catalog_p95) = if baselines.baseline_catalog_published {
            (published_p50, published_p95)
        } else {
            (unpublished_p50, unpublished_p95)
        };
        let (cold_tail_p50, cold_tail_p95) = optional_p50_p95(
            &primary.tail_refill.cold_after_advance.completion_ms,
            &candidate.tail_refill.cold_after_advance.completion_ms,
        );
        let (warm_tail_p50, warm_tail_p95) = optional_p50_p95(
            &primary.tail_refill.warm_repeat.completion_ms,
            &candidate.tail_refill.warm_repeat.completion_ms,
        );
        Self {
            mode: candidate.mode,
            baseline_label: baselines.baseline_label.to_owned(),
            baseline_catalog_published: baselines.baseline_catalog_published,
            catalog_completion_p50_delta_ms: catalog_p50,
            catalog_completion_p95_delta_ms: catalog_p95,
            unpublished_catalog_completion_p50_delta_ms: unpublished_p50,
            unpublished_catalog_completion_p95_delta_ms: unpublished_p95,
            published_catalog_completion_p50_delta_ms: published_p50,
            published_catalog_completion_p95_delta_ms: published_p95,
            cold_tail_completion_p50_delta_ms: cold_tail_p50,
            cold_tail_completion_p95_delta_ms: cold_tail_p95,
            warm_tail_completion_p50_delta_ms: warm_tail_p50,
            warm_tail_completion_p95_delta_ms: warm_tail_p95,
            slow_boundary_completion_p50_delta_ms: optional_delta(
                primary.slow_boundary_range.completion_ms,
                candidate.slow_boundary_range.completion_ms,
                |summary| summary.p50,
            ),
            tree_state_p95_delta_ms: optional_delta(
                primary.tree_state_ms,
                candidate.tree_state_ms,
                |summary| summary.p95,
            ),
            latest_block_rpc_p50_delta_ms: optional_delta(
                primary.latest_block_rpc_ms,
                candidate.latest_block_rpc_ms,
                |summary| summary.p50,
            ),
            wallet_taddress_balance_p50_delta_ms: optional_delta(
                primary.wallet_rpc_ms.taddress_balance_ms,
                candidate.wallet_rpc_ms.taddress_balance_ms,
                |summary| summary.p50,
            ),
            wallet_address_utxos_p50_delta_ms: optional_delta(
                primary.wallet_rpc_ms.address_utxos_ms,
                candidate.wallet_rpc_ms.address_utxos_ms,
                |summary| summary.p50,
            ),
            wallet_get_transaction_p50_delta_ms: optional_delta(
                primary.wallet_rpc_ms.get_transaction_ms,
                candidate.wallet_rpc_ms.get_transaction_ms,
                |summary| summary.p50,
            ),
            reader_bytes_read_delta: saturating_i64_delta(
                total_reader_bytes_read(candidate),
                total_reader_bytes_read(primary),
            ),
        }
    }
}

/// Reply-byte digest probe across settled catalog, boundary-crossing, tail,
/// tree states, and wallet-projection endpoints at `final_tip_height`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeParityDigests {
    /// Mode that produced these finalized-tip digests.
    pub mode: WriterMode,
    /// Matched arm identifier (`"quiescent_control"`, `"published_quiescent_control"`, etc.).
    pub label: String,
    /// SHA-256 hex over concatenated protobuf frames of settled catalog range.
    pub settled_catalog_range_sha256: String,
    /// SHA-256 hex over concatenated protobuf frames of boundary-crossing range.
    pub boundary_crossing_range_sha256: String,
    /// SHA-256 hex over concatenated protobuf frames of uncovered live-tail range.
    pub uncovered_tail_range_sha256: String,
    /// SHA-256 hex over serialized `TreeState` replies at settled + tail heights.
    pub tree_states_sha256: String,
    /// SHA-256 hex over `GetTaddressBalance` raw protobuf replies (`None` in `ingest_active`).
    pub wallet_taddress_balance_sha256: Option<String>,
    /// SHA-256 hex over `GetAddressUtxos` raw protobuf replies (`None` in `ingest_active`).
    pub wallet_address_utxos_sha256: Option<String>,
    /// SHA-256 hex over `GetTransaction` raw protobuf replies (`None` in `ingest_active`).
    pub wallet_transactions_sha256: Option<String>,
}

/// Comparison of one arm against the `quiescent_control` byte oracle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParityMismatch {
    /// Mode whose bytes diverged.
    pub mode: WriterMode,
    /// Matched arm identifier that diverged.
    pub label: String,
    /// Logical probe surface that diverged.
    pub surface: String,
    /// Hex digest observed in `quiescent_control`.
    pub expected_sha256: String,
    /// Hex digest observed in `mode`.
    pub observed_sha256: String,
}

/// Cross-mode exact byte-level reply parity report.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ByteParityReport {
    /// True iff every mode returned identical protobuf bytes to `quiescent_control`.
    pub all_matched: bool,
    /// Per-mode digests collected after convergence at `final_tip_height`.
    pub digests: Vec<ModeParityDigests>,
    /// Any mismatched surfaces detected (empty when `all_matched == true`).
    pub mismatches: Vec<ParityMismatch>,
}

impl ByteParityReport {
    /// Verifies that every mode's finalized-tip digests match `quiescent_control`.
    #[must_use]
    pub fn evaluate(digests: Vec<ModeParityDigests>) -> Self {
        let Some(oracle) = digests
            .iter()
            .find(|digest| digest.label == "quiescent_control")
            .or_else(|| {
                digests
                    .iter()
                    .find(|digest| digest.mode == WriterMode::QuiescentControl)
            })
        else {
            return Self {
                all_matched: false,
                digests,
                mismatches: Vec::new(),
            };
        };
        let mut mismatches = Vec::new();
        for candidate in &digests {
            if candidate.label == oracle.label && candidate.mode == oracle.mode {
                continue;
            }
            check_surface(
                candidate,
                "settled_catalog_range",
                &oracle.settled_catalog_range_sha256,
                &candidate.settled_catalog_range_sha256,
                &mut mismatches,
            );
            check_surface(
                candidate,
                "boundary_crossing_range",
                &oracle.boundary_crossing_range_sha256,
                &candidate.boundary_crossing_range_sha256,
                &mut mismatches,
            );
            check_surface(
                candidate,
                "uncovered_tail_range",
                &oracle.uncovered_tail_range_sha256,
                &candidate.uncovered_tail_range_sha256,
                &mut mismatches,
            );
            check_surface(
                candidate,
                "tree_states",
                &oracle.tree_states_sha256,
                &candidate.tree_states_sha256,
                &mut mismatches,
            );
            if candidate.mode.has_wallet_serving() {
                check_optional_surface(
                    candidate,
                    "wallet_taddress_balance",
                    oracle.wallet_taddress_balance_sha256.as_deref(),
                    candidate.wallet_taddress_balance_sha256.as_deref(),
                    &mut mismatches,
                );
                check_optional_surface(
                    candidate,
                    "wallet_address_utxos",
                    oracle.wallet_address_utxos_sha256.as_deref(),
                    candidate.wallet_address_utxos_sha256.as_deref(),
                    &mut mismatches,
                );
                check_optional_surface(
                    candidate,
                    "wallet_transactions",
                    oracle.wallet_transactions_sha256.as_deref(),
                    candidate.wallet_transactions_sha256.as_deref(),
                    &mut mismatches,
                );
            }
        }
        Self {
            all_matched: mismatches.is_empty(),
            digests,
            mismatches,
        }
    }
}

/// Feeds frame bytes into a streaming SHA-256 hex accumulator.
pub(crate) fn update_length_prefixed_digest(hasher: &mut Sha256, bytes: &[u8]) {
    let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
}

fn check_surface(
    candidate: &ModeParityDigests,
    surface: &str,
    expected: &str,
    observed: &str,
    mismatches: &mut Vec<ParityMismatch>,
) {
    if expected != observed {
        mismatches.push(ParityMismatch {
            mode: candidate.mode,
            label: candidate.label.clone(),
            surface: surface.to_owned(),
            expected_sha256: expected.to_owned(),
            observed_sha256: observed.to_owned(),
        });
    }
}

fn check_optional_surface(
    candidate: &ModeParityDigests,
    surface: &str,
    expected: Option<&str>,
    observed: Option<&str>,
    mismatches: &mut Vec<ParityMismatch>,
) {
    match (expected, observed) {
        (Some(exp), Some(obs)) if exp == obs => {}
        _ => mismatches.push(ParityMismatch {
            mode: candidate.mode,
            label: candidate.label.clone(),
            surface: surface.to_owned(),
            expected_sha256: expected.unwrap_or("<missing>").to_owned(),
            observed_sha256: observed.unwrap_or("<missing>").to_owned(),
        }),
    }
}

fn nearest_rank(sorted: &[f64], percentile: usize) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (percentile.saturating_mul(sorted.len())).div_ceil(100);
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

#[allow(
    clippy::cast_precision_loss,
    reason = "sample counts remain far below f64 exact integer precision"
)]
const fn usize_to_f64(count: usize) -> f64 {
    count as f64
}

fn saturating_i64_delta(candidate: u64, baseline: u64) -> i64 {
    if candidate >= baseline {
        i64::try_from(candidate - baseline).unwrap_or(i64::MAX)
    } else {
        let diff = i64::try_from(baseline - candidate).unwrap_or(i64::MAX);
        diff.saturating_neg()
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, num::NonZeroU32};

    use super::{
        ACTIVE_WRITER_PUBLICATION_CONTRACT, ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
        ActiveWriterPublicationConfig, ActiveWriterPublicationReport, ByteParityReport,
        LatencySummaryMs, ModeDifferential, ModeParityDigests, ModeReport, WriterMode,
    };

    fn stream_json(first_p50: f64, completion_p50: f64) -> serde_json::Value {
        serde_json::json!({
            "first_frame_ms": { "samples": 1, "p50": first_p50, "p95": first_p50, "max": first_p50, "mean": first_p50 },
            "completion_ms": { "samples": 1, "p50": completion_p50, "p95": completion_p50 * 2.0, "max": completion_p50 * 2.0, "mean": completion_p50 },
            "frames_delivered": 10,
            "payload_bytes_delivered": 100
        })
    }

    fn blank_tail_refill_json() -> serde_json::Value {
        serde_json::json!({
            "cold_after_advance": { "first_frame_ms": { "samples": 1, "p50": 2.0, "p95": 2.0, "max": 2.0, "mean": 2.0 }, "completion_ms": { "samples": 1, "p50": 4.0, "p95": 6.0, "max": 6.0, "mean": 4.0 }, "frames_delivered": 10, "payload_bytes_delivered": 100 },
            "warm_repeat": stream_json(1.0, 1.0),
            "peak_tail_response_bytes": 100,
            "peak_encoded_tail_bytes": 200,
            "encoded_tail_byte_cap_evictions": 0,
            "encoded_tail_byte_capped_fills": 0,
            "encoded_tail_cache_fills": 1,
            "encoded_tail_cache_hits": 1,
            "encoded_tail_fallback_native": 0,
            "encoded_tail_bypass": 0,
            "unpaired_tail_probes": 0,
            "byte_cap_exceeded": false
        })
    }

    fn blank_mode_report(
        mode: WriterMode,
        unpub_cat_p50: f64,
        pub_cat_p50: f64,
        bytes_read: u64,
    ) -> Result<ModeReport, Box<dyn Error>> {
        let zero_lat =
            serde_json::json!({ "samples": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "mean": 0.0 });
        let sample_lat =
            serde_json::json!({ "samples": 4, "p50": 1.0, "p95": 1.5, "max": 2.0, "mean": 1.1 });
        let zero_ticker = serde_json::json!({ "wal_synced": 0, "wal_bytes": 0, "flush_write_bytes": 0, "compact_read_bytes": 0, "compact_write_bytes": 0, "bytes_read": 0, "bytes_written": 0, "block_cache_data_hit": 0, "block_cache_data_miss": 0, "stall_micros": 0 });
        let wallet_lat = if mode.has_wallet_serving() {
            &sample_lat
        } else {
            &zero_lat
        };
        let is_active = mode != WriterMode::QuiescentControl;
        let step_lat = if is_active { &sample_lat } else { &zero_lat };
        let has_projector = matches!(
            mode,
            WriterMode::IngestAndProjector | WriterMode::IngestProjectorAndPublisher
        );
        let base = serde_json::json!({
            "mode": mode,
            "workload_duration_ms": 100.0,
            "io_modes": {
                "canonical_primary": if is_active { Some("buffered") } else { None::<&str> },
                "canonical_secondaries": ["buffered"],
                "wallet_primary": if has_projector { Some("buffered") } else { None::<&str> },
                "wallet_secondaries": if mode.has_wallet_serving() { vec!["buffered"] } else { vec![] },
                "projector_secondary": if has_projector { Some("buffered") } else { None::<&str> },
                "publisher_secondary": if mode == WriterMode::IngestProjectorAndPublisher { Some("buffered") } else { None::<&str> }
            },
            "ingest": {
                "live_prepare_ms": step_lat,
                "live_commit_ms": step_lat,
                "blocks_committed": if is_active { 4 } else { 0 }
            },
            "latest_block_rpc_ms": sample_lat,
            "commit_to_served_tip_ms": step_lat,
            "catalog_range": stream_json(1.0, unpub_cat_p50),
            "catalog_range_unpublished": stream_json(1.0, unpub_cat_p50),
            "catalog_range_published": stream_json(0.5, pub_cat_p50),
            "catalog_range_during_seal": stream_json(0.0, 0.0),
            "tail_refill": blank_tail_refill_json(),
            "slow_boundary_range": { "first_frame_ms": { "samples": 1, "p50": 1.0, "p95": 1.0, "max": 1.0, "mean": 1.0 }, "completion_ms": { "samples": 1, "p50": 15.0, "p95": 20.0, "max": 20.0, "mean": 15.0 }, "frames_delivered": 10, "payload_bytes_delivered": 100 },
            "tree_state_ms": { "samples": 1, "p50": 0.5, "p95": 1.0, "max": 1.0, "mean": 0.5 },
            "wallet_rpc_ms": { "taddress_balance_ms": wallet_lat, "address_utxos_ms": wallet_lat, "get_transaction_ms": wallet_lat },
            "mempool_churn": [{ "batch_size": 50, "mempool_tx_stream": stream_json(0.5, 1.0), "mempool_stream_closures": 1, "writer_overlap_verified": is_active, "commits_observed_during_tier": if is_active { 2 } else { 0 } }],
            "projector": { "apply_ms": zero_lat, "transitions_applied": if has_projector { 4 } else { 0 } },
            "publisher": { "total_ms": 1.0, "segment_publish_ms": zero_lat, "segments_published": usize::from(mode == WriterMode::IngestProjectorAndPublisher), "catalog_refreshes": u64::from(mode == WriterMode::IngestProjectorAndPublisher) },
            "writer_fsyncs": { "canonical_wal_synced": 0, "canonical_wal_bytes": 0, "wallet_wal_synced": 0, "wallet_wal_bytes": 0 },
            "node_rpcs": { "tip_id": 0, "fetch_block_at": 0, "fetch_chain_segment": 0, "fetch_chain_checkpoint": 0, "fetch_subtree_roots": 0, "total": 0 },
            "rocksdb_activity": {
                "canonical_writer": zero_ticker,
                "reader_canonical_secondaries": { "wal_synced": 0, "wal_bytes": 0, "flush_write_bytes": 0, "compact_read_bytes": 0, "compact_write_bytes": 0, "bytes_read": bytes_read, "bytes_written": 0, "block_cache_data_hit": 0, "block_cache_data_miss": 0, "stall_micros": 0 },
                "reader_wallet_secondaries": zero_ticker,
                "wallet_writer": zero_ticker,
                "projector_secondary": zero_ticker,
                "publisher_secondary": zero_ticker
            },
            "errors": { "unexpected_grpc_errors": 0, "unexpected_error_samples": [], "transient_retry_count": 0 }
        });
        Ok(serde_json::from_value(base)?)
    }

    #[test]
    fn latency_summary_uses_nearest_rank_percentiles() {
        let numbers: Vec<f64> = (1..=20).map(f64::from).collect();
        let summary = LatencySummaryMs::from_f64(&numbers);
        assert_eq!(summary.samples, 20);
        assert!((summary.p50 - 10.0).abs() < f64::EPSILON);
        assert!((summary.p95 - 19.0).abs() < f64::EPSILON);
        assert!((summary.max - 20.0).abs() < f64::EPSILON);
        assert!((summary.mean - 10.5).abs() < f64::EPSILON);
    }

    #[test]
    fn differential_marks_all_empty_stage_deltas_unavailable() -> Result<(), Box<dyn Error>> {
        let mut unpublished = blank_mode_report(WriterMode::QuiescentControl, 5.0, 0.0, 2_000)?;
        unpublished.catalog_range_published.completion_ms.samples = 0;
        unpublished.slow_boundary_range.completion_ms.samples = 0;
        let mut published = blank_mode_report(WriterMode::QuiescentControl, 0.0, 2.0, 1_800)?;
        published.catalog_range_unpublished.completion_ms.samples = 0;
        let mut candidate =
            blank_mode_report(WriterMode::IngestProjectorAndPublisher, 9.0, 3.5, 1_900)?;
        candidate
            .tail_refill
            .cold_after_advance
            .completion_ms
            .samples = 0;
        candidate.tree_state_ms.samples = 0;

        let diff = ModeDifferential::between(
            super::DifferentialBaselines {
                unpublished: &unpublished,
                published: &published,
                baseline_label: "published_quiescent_control",
                baseline_catalog_published: true,
            },
            &candidate,
        );
        assert!(
            diff.unpublished_catalog_completion_p50_delta_ms
                .is_some_and(|delta| (delta - 4.0).abs() < f64::EPSILON)
        );
        assert!(
            diff.published_catalog_completion_p50_delta_ms
                .is_some_and(|delta| (delta - 1.5).abs() < f64::EPSILON)
        );
        assert!(diff.cold_tail_completion_p50_delta_ms.is_none());
        assert!(diff.tree_state_p95_delta_ms.is_none());
        assert_eq!(diff.reader_bytes_read_delta, 100);
        Ok(())
    }

    #[test]
    fn parity_report_verifies_wallet_surfaces_only_on_wallet_enabled_modes() {
        let oracle = ModeParityDigests {
            mode: WriterMode::QuiescentControl,
            label: "quiescent_control".to_owned(),
            settled_catalog_range_sha256: "aa".to_owned(),
            boundary_crossing_range_sha256: "bb".to_owned(),
            uncovered_tail_range_sha256: "cc".to_owned(),
            tree_states_sha256: "dd".to_owned(),
            wallet_taddress_balance_sha256: Some("ee".to_owned()),
            wallet_address_utxos_sha256: Some("ff".to_owned()),
            wallet_transactions_sha256: Some("11".to_owned()),
        };
        let ingest_only = ModeParityDigests {
            mode: WriterMode::IngestActive,
            label: "ingest_active".to_owned(),
            wallet_taddress_balance_sha256: None,
            wallet_address_utxos_sha256: None,
            wallet_transactions_sha256: None,
            ..oracle.clone()
        };
        assert!(ByteParityReport::evaluate(vec![oracle.clone(), ingest_only]).all_matched);

        let broken_wallet = ModeParityDigests {
            mode: WriterMode::IngestAndProjector,
            label: "ingest_and_projector".to_owned(),
            wallet_taddress_balance_sha256: Some("99".to_owned()),
            ..oracle.clone()
        };
        let failed = ByteParityReport::evaluate(vec![oracle, broken_wallet]);
        assert!(!failed.all_matched);
        assert_eq!(failed.mismatches.len(), 1);
        assert_eq!(failed.mismatches[0].surface, "wallet_taddress_balance");
    }

    #[test]
    fn report_validate_checks_tail_eviction_requirement() -> Result<(), Box<dyn Error>> {
        let oracle = ModeParityDigests {
            mode: WriterMode::QuiescentControl,
            label: "quiescent_control".to_owned(),
            settled_catalog_range_sha256: "a".to_owned(),
            boundary_crossing_range_sha256: "b".to_owned(),
            uncovered_tail_range_sha256: "c".to_owned(),
            tree_states_sha256: "d".to_owned(),
            wallet_taddress_balance_sha256: Some("e".to_owned()),
            wallet_address_utxos_sha256: Some("f".to_owned()),
            wallet_transactions_sha256: Some("g".to_owned()),
        };
        let report = ActiveWriterPublicationReport {
            contract_identity: ACTIVE_WRITER_PUBLICATION_CONTRACT.to_owned(),
            report_format_version: ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
            workload: super::WorkloadGeometryReport {
                network: "zcash-regtest".to_owned(),
                base_blocks: 60,
                live_blocks: 4,
                final_tip_height: 64,
                reorg_window_blocks: 4,
                segment_blocks: 64,
                crossed_segment_boundary_height: 64,
                sparse_block_count: 50,
                dense_block_count: 14,
                dense_orchard_actions_per_tx: 2,
                transparent_spend_txs: 4,
                tail_probe_blocks: 24,
                requested_io_mode: "buffered".to_owned(),
                publish_workers: 1,
                mempool_churn_sizes: vec![50],
                request_trace_digest: "deadbeef".to_owned(),
            },
            published_quiescent_control: blank_mode_report(
                WriterMode::QuiescentControl,
                1.0,
                1.0,
                10,
            )?,
            modes: vec![
                blank_mode_report(WriterMode::QuiescentControl, 1.0, 1.0, 10)?,
                blank_mode_report(WriterMode::IngestActive, 1.0, 1.0, 10)?,
                blank_mode_report(WriterMode::IngestAndProjector, 1.0, 1.0, 10)?,
                blank_mode_report(WriterMode::IngestProjectorAndPublisher, 1.0, 1.0, 10)?,
            ],
            differentials_vs_quiescent: Vec::new(),
            byte_parity: ByteParityReport::evaluate(vec![oracle]),
        };
        let config = ActiveWriterPublicationConfig {
            base_blocks: 60,
            live_blocks: 4,
            reorg_window_blocks: 4,
            segment_blocks: NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
            mempool_churn_sizes: vec![50],
            ..ActiveWriterPublicationConfig::default()
        };
        assert!(report.validate(&config).is_ok());

        let eviction_required = ActiveWriterPublicationConfig {
            require_tail_eviction: true,
            ..config
        };
        assert!(report.validate(&eviction_required).is_err());
        Ok(())
    }
}
