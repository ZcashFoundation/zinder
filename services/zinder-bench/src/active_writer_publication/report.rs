//! Typed report structures, percentile summaries, and byte-parity verification
//! for `active-writer-publication`.

use std::time::Duration;

use rust_rocksdb::statistics::Ticker;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Stable contract identifier for this benchmark report format.
pub const ACTIVE_WRITER_PUBLICATION_CONTRACT: &str = "active-writer-publication";
/// Current schema version for [`ActiveWriterPublicationReport`].
pub const ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION: u32 = 1;

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
    /// Captures all monitored tickers from a single `read_fn`.
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
    pub fn saturating_add(self, other: Self) -> Self {
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

/// Chain geometry shared identically by all four matched modes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkloadGeometryReport {
    /// Network identifier (`"zcash-regtest"`).
    pub network: String,
    /// Number of pre-built base blocks before live tip progression begins.
    pub base_blocks: u32,
    /// Number of live blocks committed while the reader fleet is active.
    pub live_blocks: u32,
    /// Final chain tip height across all four arms.
    pub final_tip_height: u32,
    /// Canonical reorg window (settlement lag in blocks).
    pub reorg_window_blocks: u32,
    /// Settled catalog segment size in blocks (4,096 by default).
    pub segment_blocks: u32,
    /// Shielded catalog segment boundary crossed during live commits.
    pub crossed_segment_boundary_height: u32,
    /// Sparse blocks present in the generated chain.
    pub sparse_block_count: u32,
    /// Commitment-dense shielded blocks present in the generated chain.
    pub dense_block_count: u32,
    /// Distinct mempool churn transaction counts exercised (e.g. `[50, 500]`).
    pub mempool_churn_sizes: Vec<u32>,
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
    /// Time from RPC dispatch to the first delivered frame.
    pub first_frame_ms: LatencySummaryMs,
    /// Time from RPC dispatch to stream EOF.
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

/// Encoded tail fill/refill timing and `64 MiB` byte-cap observation.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct TailRefillMetrics {
    /// First tail `GetBlockRange` after a tip publication that triggered an `EncodedTail` fill.
    pub cold_after_advance: StreamLatencyReport,
    /// Immediate repeat tail `GetBlockRange` verified as a same-generation cache hit.
    pub warm_repeat: StreamLatencyReport,
    /// Peak payload bytes observed across a single uncovered-tail range response.
    pub peak_tail_response_bytes: u64,
    /// Peak retained bytes reported by `EncodedTail` telemetry across tail builds.
    pub peak_encoded_tail_bytes: u64,
    /// Cumulative rows evicted by `EncodedTail`'s 64 MiB byte cap during this mode run.
    pub encoded_tail_byte_cap_evictions: u64,
    /// Count of `EncodedTail` builds/prepends observed during this mode run.
    pub encoded_tail_cache_fills: u64,
    /// Count of `EncodedTail` cache hits observed during this mode run.
    pub encoded_tail_cache_hits: u64,
    /// Pairs excluded because a catalog publication or generation advance crossed the pair.
    pub excluded_cross_epoch_pairs: u64,
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
}

/// Resolved filesystem I/O modes reported by `RocksDB` (`"direct"` or `"buffered"`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IoModeReport {
    /// Canonical primary store resolved I/O mode (`None` in `quiescent_control`).
    pub canonical_primary: Option<String>,
    /// Canonical serving secondaries resolved I/O mode(s).
    pub canonical_secondaries: Vec<String>,
    /// Wallet writer store resolved I/O mode (`None` when projector inactive).
    pub wallet_primary: Option<String>,
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
    /// Pooled reader canonical secondaries activity during the reader run.
    pub reader_secondaries: StoreTickerDelta,
    /// Wallet writer store activity during projector transitions.
    pub wallet_writer: StoreTickerDelta,
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
    /// Commit-to-served-tip visibility delay across live commits (`GetLatestBlock`).
    pub commit_to_served_tip_ms: LatencySummaryMs,
    /// Ordinary settled catalog `GetBlockRange` timing (`first_frame` and `completion`).
    pub catalog_range: StreamLatencyReport,
    /// Uncovered live-tail `GetBlockRange` timing, separating cold refill vs warm repeat.
    pub tail_refill: TailRefillMetrics,
    /// Slow reader `GetBlockRange` crossing the `4,096` segment boundary into live tail.
    pub slow_boundary_range: StreamLatencyReport,
    /// `GetTreeState` latency across catalog and tail heights.
    pub tree_state_ms: LatencySummaryMs,
    /// Mempool churn results for each configured batch size (`50` and `500`).
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

/// Latency and activity deltas for one active mode compared to its matched quiescent baseline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModeDifferential {
    /// Active writer mode being compared.
    pub mode: WriterMode,
    /// Matched quiescent baseline identifier (`"quiescent_control"` or `"published_quiescent_control"`).
    pub baseline_label: String,
    /// Whether the matched quiescent baseline held a published signed catalog.
    pub baseline_catalog_published: bool,
    /// Change in settled catalog `GetBlockRange` completion p50 (ms).
    pub catalog_completion_p50_delta_ms: f64,
    /// Change in settled catalog `GetBlockRange` completion p95 (ms).
    pub catalog_completion_p95_delta_ms: f64,
    /// Change in cold tail refill completion p50 (ms).
    pub cold_tail_completion_p50_delta_ms: f64,
    /// Change in cold tail refill completion p95 (ms).
    pub cold_tail_completion_p95_delta_ms: f64,
    /// Change in slow boundary-crossing completion p50 (ms).
    pub slow_boundary_completion_p50_delta_ms: f64,
    /// Change in `GetTreeState` p95 (ms).
    pub tree_state_p95_delta_ms: f64,
    /// Change in reader secondary read bytes compared to matched quiescent baseline.
    pub reader_bytes_read_delta: i64,
}

impl ModeDifferential {
    /// Computes stage latency and secondary-read deltas relative to `baseline`.
    #[must_use]
    pub fn between(
        baseline: &ModeReport,
        baseline_label: &str,
        baseline_catalog_published: bool,
        candidate: &ModeReport,
    ) -> Self {
        Self {
            mode: candidate.mode,
            baseline_label: baseline_label.to_owned(),
            baseline_catalog_published,
            catalog_completion_p50_delta_ms: candidate.catalog_range.completion_ms.p50
                - baseline.catalog_range.completion_ms.p50,
            catalog_completion_p95_delta_ms: candidate.catalog_range.completion_ms.p95
                - baseline.catalog_range.completion_ms.p95,
            cold_tail_completion_p50_delta_ms: candidate
                .tail_refill
                .cold_after_advance
                .completion_ms
                .p50
                - baseline.tail_refill.cold_after_advance.completion_ms.p50,
            cold_tail_completion_p95_delta_ms: candidate
                .tail_refill
                .cold_after_advance
                .completion_ms
                .p95
                - baseline.tail_refill.cold_after_advance.completion_ms.p95,
            slow_boundary_completion_p50_delta_ms: candidate.slow_boundary_range.completion_ms.p50
                - baseline.slow_boundary_range.completion_ms.p50,
            tree_state_p95_delta_ms: candidate.tree_state_ms.p95 - baseline.tree_state_ms.p95,
            reader_bytes_read_delta: saturating_i64_delta(
                candidate.rocksdb_activity.reader_secondaries.bytes_read,
                baseline.rocksdb_activity.reader_secondaries.bytes_read,
            ),
        }
    }
}

/// Reply-byte digest probe across settled catalog, boundary-crossing, tail, and tree states.
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
    use std::error::Error;

    use super::{
        ByteParityReport, LatencySummaryMs, ModeDifferential, ModeParityDigests, ModeReport,
        WriterMode,
    };

    fn blank_mode_report(
        mode: WriterMode,
        cat_p50: f64,
        bytes_read: u64,
    ) -> Result<ModeReport, Box<dyn Error>> {
        let mut base = serde_json::json!({
            "mode": mode,
            "workload_duration_ms": 100.0,
            "io_modes": {
                "canonical_primary": null,
                "canonical_secondaries": ["buffered"],
                "wallet_primary": null,
                "publisher_secondary": null
            },
            "ingest": {
                "live_prepare_ms": { "samples": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "mean": 0.0 },
                "live_commit_ms": { "samples": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "mean": 0.0 },
                "blocks_committed": 0
            },
            "commit_to_served_tip_ms": { "samples": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "mean": 0.0 },
            "catalog_range": {
                "first_frame_ms": { "samples": 1, "p50": 1.0, "p95": 1.0, "max": 1.0, "mean": 1.0 },
                "completion_ms": { "samples": 1, "p50": cat_p50, "p95": cat_p50 * 2.0, "max": cat_p50 * 2.0, "mean": cat_p50 },
                "frames_delivered": 10,
                "payload_bytes_delivered": 100
            },
            "tail_refill": {
                "cold_after_advance": {
                    "first_frame_ms": { "samples": 1, "p50": 2.0, "p95": 2.0, "max": 2.0, "mean": 2.0 },
                    "completion_ms": { "samples": 1, "p50": 4.0, "p95": 6.0, "max": 6.0, "mean": 4.0 },
                    "frames_delivered": 10,
                    "payload_bytes_delivered": 100
                },
                "warm_repeat": {
                    "first_frame_ms": { "samples": 1, "p50": 1.0, "p95": 1.0, "max": 1.0, "mean": 1.0 },
                    "completion_ms": { "samples": 1, "p50": 2.0, "p95": 2.0, "max": 2.0, "mean": 2.0 },
                    "frames_delivered": 10,
                    "payload_bytes_delivered": 100
                },
                "peak_tail_response_bytes": 100,
                "peak_encoded_tail_bytes": 200,
                "encoded_tail_byte_cap_evictions": 0,
                "encoded_tail_cache_fills": 1,
                "encoded_tail_cache_hits": 1,
                "excluded_cross_epoch_pairs": 0,
                "byte_cap_exceeded": false
            },
            "slow_boundary_range": {
                "first_frame_ms": { "samples": 1, "p50": 1.0, "p95": 1.0, "max": 1.0, "mean": 1.0 },
                "completion_ms": { "samples": 1, "p50": 15.0, "p95": 20.0, "max": 20.0, "mean": 15.0 },
                "frames_delivered": 10,
                "payload_bytes_delivered": 100
            },
            "tree_state_ms": { "samples": 1, "p50": 0.5, "p95": 1.0, "max": 1.0, "mean": 0.5 },
            "mempool_churn": [],
            "projector": {
                "apply_ms": { "samples": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "mean": 0.0 },
                "transitions_applied": 0
            },
            "publisher": {
                "total_ms": 0.0,
                "segment_publish_ms": { "samples": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "mean": 0.0 },
                "segments_published": 0,
                "catalog_refreshes": 0
            },
            "writer_fsyncs": { "canonical_wal_synced": 0, "canonical_wal_bytes": 0, "wallet_wal_synced": 0, "wallet_wal_bytes": 0 },
            "node_rpcs": { "tip_id": 0, "fetch_block_at": 0, "fetch_chain_segment": 0, "fetch_chain_checkpoint": 0, "fetch_subtree_roots": 0, "total": 0 },
            "rocksdb_activity": {
                "canonical_writer": { "wal_synced": 0, "wal_bytes": 0, "flush_write_bytes": 0, "compact_read_bytes": 0, "compact_write_bytes": 0, "bytes_read": 0, "bytes_written": 0, "block_cache_data_hit": 0, "block_cache_data_miss": 0, "stall_micros": 0 },
                "reader_secondaries": { "wal_synced": 0, "wal_bytes": 0, "flush_write_bytes": 0, "compact_read_bytes": 0, "compact_write_bytes": 0, "bytes_read": bytes_read, "bytes_written": 0, "block_cache_data_hit": 0, "block_cache_data_miss": 0, "stall_micros": 0 },
                "wallet_writer": { "wal_synced": 0, "wal_bytes": 0, "flush_write_bytes": 0, "compact_read_bytes": 0, "compact_write_bytes": 0, "bytes_read": 0, "bytes_written": 0, "block_cache_data_hit": 0, "block_cache_data_miss": 0, "stall_micros": 0 },
                "publisher_secondary": { "wal_synced": 0, "wal_bytes": 0, "flush_write_bytes": 0, "compact_read_bytes": 0, "compact_write_bytes": 0, "bytes_read": 0, "bytes_written": 0, "block_cache_data_hit": 0, "block_cache_data_miss": 0, "stall_micros": 0 }
            },
            "errors": { "unexpected_grpc_errors": 0, "unexpected_error_samples": [], "transient_retry_count": 0 }
        });
        base["mode"] = serde_json::json!(mode);
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
    fn differential_reports_signed_stage_and_read_byte_changes() -> Result<(), Box<dyn Error>> {
        let baseline = blank_mode_report(WriterMode::QuiescentControl, 5.0, 2_000)?;
        let candidate = blank_mode_report(WriterMode::IngestActive, 8.5, 1_500)?;
        let diff = ModeDifferential::between(&baseline, "quiescent_control", false, &candidate);
        assert_eq!(diff.baseline_label, "quiescent_control");
        assert!(!diff.baseline_catalog_published);
        assert!((diff.catalog_completion_p50_delta_ms - 3.5).abs() < f64::EPSILON);
        assert!((diff.catalog_completion_p95_delta_ms - 7.0).abs() < f64::EPSILON);
        assert_eq!(diff.reader_bytes_read_delta, -500);
        Ok(())
    }

    #[test]
    fn parity_report_flags_divergent_surface_and_accepts_identical_digests() {
        let oracle = ModeParityDigests {
            mode: WriterMode::QuiescentControl,
            label: "quiescent_control".to_owned(),
            settled_catalog_range_sha256: "aa".to_owned(),
            boundary_crossing_range_sha256: "bb".to_owned(),
            uncovered_tail_range_sha256: "cc".to_owned(),
            tree_states_sha256: "dd".to_owned(),
        };
        let matching = ModeParityDigests {
            mode: WriterMode::IngestActive,
            label: "ingest_active".to_owned(),
            ..oracle.clone()
        };
        assert!(ByteParityReport::evaluate(vec![oracle.clone(), matching]).all_matched);

        let divergent = ModeParityDigests {
            mode: WriterMode::IngestProjectorAndPublisher,
            label: "ingest_projector_and_publisher".to_owned(),
            uncovered_tail_range_sha256: "ff".to_owned(),
            ..oracle.clone()
        };
        let failed = ByteParityReport::evaluate(vec![oracle, divergent]);
        assert!(!failed.all_matched);
        assert_eq!(failed.mismatches.len(), 1);
        assert_eq!(failed.mismatches[0].surface, "uncovered_tail_range");
    }
}
