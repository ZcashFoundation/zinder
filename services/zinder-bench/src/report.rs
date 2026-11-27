//! Machine-readable benchmark report and its assembly from scraped metrics.

use std::{collections::BTreeMap, fmt};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use zinder_core::CanonicalBlockReplayFormatVersion;
use zinder_store::{RocksDbResourceBudget, TREE_STATE_CHECKPOINT_STRIDE};

use crate::{
    canonical_replay_storage::postgres::PostgresServerSettings,
    error::BenchError,
    fixture::{
        CanonicalBlockFactsDigestEvidence, FIXTURE_CONTRACT_IDENTITY, FIXTURE_FORMAT_VERSION,
        FixtureManifest, WorkloadDensity,
    },
    metrics_scrape::{MetricSample, parse_prometheus_samples, sum_by_name},
    postgres_segmented::{
        PostgresSegmentedRunReport, schema::MAX_SETTLED_SEGMENT_BYTES,
        tip_follow::LatencyPercentilesMillis,
    },
    rss::PeakRss,
};
use zinder_core::wire::encode_zinder_native_chain_name;

/// Machine-readable report schema version for `canonical-replay-storage` reports.
pub const REPORT_FORMAT_VERSION: u32 = 1;
/// Machine-readable report schema version for `rocksdb-canonical-fixture-replay` reports.
pub const ROCKSDB_CANONICAL_FIXTURE_REPLAY_REPORT_FORMAT_VERSION: u32 = 1;
/// Machine-readable report schema version for `rocksdb-storage-lifecycle` reports.
pub const ROCKSDB_STORAGE_LIFECYCLE_REPORT_FORMAT_VERSION: u32 = 1;
/// Machine-readable report schema version for `postgres-segmented-storage` reports.
pub const POSTGRES_SEGMENTED_REPORT_FORMAT_VERSION: u32 = 1;
/// Stable identity stamped into every benchmark report.
pub const REPORT_CONTRACT_IDENTITY: &str = "benchmark-report";
/// CPU envelope used to derive canonical fixture replay defaults.
pub const CANONICAL_FIXTURE_REPLAY_PROFILE_CPU_CORES: u32 = 10;
/// Memory envelope used to derive canonical fixture replay defaults.
pub const CANONICAL_FIXTURE_REPLAY_PROFILE_MEMORY_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Returns whether a container image identity is content addressed.
#[must_use]
pub fn is_immutable_image_reference(reference: &str) -> bool {
    let reference = reference.trim();
    let digest = reference.strip_prefix("sha256:").or_else(|| {
        reference
            .rsplit_once("@sha256:")
            .and_then(|(image_name, digest)| (!image_name.is_empty()).then_some(digest))
    });
    digest.is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

/// Returns whether an operator-supplied trial identity is safe and unambiguous.
#[must_use]
pub fn is_valid_benchmark_trial_id(trial_id: &str) -> bool {
    let mut bytes = trial_id.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

const SOURCE_REQUEST_TOTAL: &str = "zinder_ingest_source_request_total";
const SOURCE_REQUEST_DURATION_SUM: &str = "zinder_ingest_source_request_duration_seconds_sum";
const SOURCE_SEGMENT_CONNECTED_BLOCKS_TOTAL: &str =
    "zinder_ingest_source_segment_connected_blocks_total";
const SOURCE_SEGMENT_RESPONSE_PAYLOAD_BYTES_SUM: &str =
    "zinder_ingest_source_segment_response_payload_bytes_sum";
const SOURCE_SEGMENT_SIZING_ADJUSTMENT_TOTAL: &str =
    "zinder_ingest_source_segment_sizing_adjustment_total";
const SOURCE_SEGMENT_DISJOINT_PREFETCH_DISCARDED_COMPLETED_SEGMENTS_TOTAL: &str =
    "zinder_ingest_source_segment_disjoint_prefetch_discarded_completed_segments_total";
const SOURCE_SEGMENT_DISJOINT_PREFETCH_DISCARDED_IN_FLIGHT_SEGMENTS_TOTAL: &str =
    "zinder_ingest_source_segment_disjoint_prefetch_discarded_in_flight_segments_total";
const SOURCE_SEGMENT_DISJOINT_PREFETCH_DISCARDED_COMPLETED_RESPONSE_BYTES_TOTAL: &str =
    "zinder_ingest_source_segment_disjoint_prefetch_discarded_completed_response_bytes_total";
const SOURCE_SEGMENT_PREFETCH_RETAINED_COMPLETED_SEGMENTS_TOTAL: &str =
    "zinder_ingest_source_segment_prefetch_retained_completed_segments_total";
const SOURCE_SEGMENT_PREFETCH_RETAINED_IN_FLIGHT_SEGMENTS_TOTAL: &str =
    "zinder_ingest_source_segment_prefetch_retained_in_flight_segments_total";
const SOURCE_SEGMENT_PREFETCH_RETAINED_COMPLETED_RESPONSE_BYTES_TOTAL: &str =
    "zinder_ingest_source_segment_prefetch_retained_completed_response_bytes_total";
const BULK_PIPELINE_WATERMARK_BLOCKED_TOTAL: &str =
    "zinder_ingest_bulk_pipeline_watermark_blocked_total";
const HEAD_OF_LINE_WAIT_SUM: &str = "zinder_ingest_bulk_pipeline_head_of_line_wait_seconds_sum";
const HEAD_OF_LINE_WAIT_COUNT: &str = "zinder_ingest_bulk_pipeline_head_of_line_wait_seconds_count";
const CANONICAL_BLOCK_CONSTRUCTION_STAGE_DURATION_COUNT: &str =
    "zinder_ingest_canonical_block_construction_stage_duration_seconds_count";
const CANONICAL_BLOCK_CONSTRUCTION_STAGE_DURATION_SUM: &str =
    "zinder_ingest_canonical_block_construction_stage_duration_seconds_sum";
const CANONICAL_BLOCK_LOAD_STAGE_DURATION_COUNT: &str =
    "zinder_ingest_canonical_block_load_stage_duration_seconds_count";
const CANONICAL_BLOCK_LOAD_STAGE_DURATION_SUM: &str =
    "zinder_ingest_canonical_block_load_stage_duration_seconds_sum";
const CANONICAL_BLOCK_STORE_LOAD_STAGE_DURATION_COUNT: &str =
    "zinder_store_canonical_block_load_stage_duration_seconds_count";
const CANONICAL_BLOCK_STORE_LOAD_STAGE_DURATION_SUM: &str =
    "zinder_store_canonical_block_load_stage_duration_seconds_sum";
const CANONICAL_HISTORICAL_PREVOUT_READS_TOTAL: &str =
    "zinder_ingest_canonical_historical_prevout_reads_total";
const CANONICAL_CROSS_BLOCK_WALLET_READS_TOTAL: &str =
    "zinder_ingest_canonical_cross_block_wallet_reads_total";
const CANONICAL_PUBLICATION_FAMILY_SCAN_DURATION_COUNT: &str =
    "zinder_store_canonical_publication_family_scan_duration_seconds_count";
const CANONICAL_PUBLICATION_FAMILY_SCAN_DURATION_SUM: &str =
    "zinder_store_canonical_publication_family_scan_duration_seconds_sum";
const CANONICAL_PUBLICATION_FAMILY_SCAN_ROWS_TOTAL: &str =
    "zinder_store_canonical_publication_family_scan_rows_total";
const CANONICAL_PUBLICATION_FAMILY_SCAN_LOGICAL_BYTES_TOTAL: &str =
    "zinder_store_canonical_publication_family_scan_logical_bytes_total";

/// Fixture identity echoed into the report.
#[derive(Clone, Debug, Serialize)]
pub struct FixtureSummary {
    /// Stable fixture contract identity.
    pub contract_identity: String,
    /// Fixture manifest format version.
    pub fixture_format_version: u32,
    /// Canonical artifact schema version used when capturing the fixture.
    pub canonical_artifact_schema_version: u16,
    /// Backend-neutral canonical-replay digest oracle captured with the fixture.
    pub canonical_block_facts_digest_evidence: CanonicalBlockFactsDigestEvidence,
    /// Captured range tip hash, hex-encoded in internal byte order.
    pub tip_hash_hex: String,
    /// SHA-256 identity of the normalized fixture manifest and segment digests.
    pub digest_sha256: String,
    /// Network name in Zinder-native encoding.
    pub network: String,
    /// First replayed block height.
    pub from_height: u32,
    /// Last replayed block height.
    pub to_height: u32,
    /// Captured block count.
    pub block_count: u32,
    /// Consensus-byte workload density for the replayed fixture.
    pub workload_density: WorkloadDensity,
    /// Number of segment files.
    pub segment_count: usize,
}

impl TryFrom<&FixtureManifest> for FixtureSummary {
    type Error = BenchError;

    fn try_from(manifest: &FixtureManifest) -> Result<Self, Self::Error> {
        Ok(Self {
            contract_identity: manifest.contract_identity.clone(),
            fixture_format_version: manifest.fixture_format_version,
            canonical_artifact_schema_version: manifest.canonical_artifact_schema_version,
            canonical_block_facts_digest_evidence: manifest
                .canonical_block_facts_digest_evidence
                .clone(),
            tip_hash_hex: manifest.tip_hash_hex.clone(),
            digest_sha256: manifest.digest_sha256()?,
            network: manifest.network.clone(),
            from_height: manifest.from_height,
            to_height: manifest.to_height,
            block_count: manifest.block_count,
            workload_density: manifest.workload_density,
            segment_count: manifest.segments.len(),
        })
    }
}

/// Storage implementation identity recorded with every measured run.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct StorageCandidateIdentity {
    /// Stable comparison identifier.
    pub id: &'static str,
    /// Canonical storage engine.
    pub canonical_engine: &'static str,
    /// Canonical logical model exercised by this candidate.
    pub canonical_model: &'static str,
    /// Deployment/storage topology used by the candidate.
    pub topology: &'static str,
}

impl StorageCandidateIdentity {
    /// Identifies the diagnostic canonical-replay `RocksDB` round-trip arm.
    #[must_use]
    pub const fn rocksdb_canonical_replay_storage() -> Self {
        Self {
            id: "rocksdb-canonical-replay-storage",
            canonical_engine: "rocksdb",
            canonical_model: "block-granular-canonical-replays",
            topology: "rocksdb",
        }
    }

    /// Identifies the diagnostic canonical-replay Postgres round-trip arm.
    #[must_use]
    pub const fn postgres_canonical_replay_storage() -> Self {
        Self {
            id: "postgres-canonical-replay-storage",
            canonical_engine: "postgres",
            canonical_model: "block-granular-canonical-replays",
            topology: "postgres-diagnostic",
        }
    }

    /// Identifies the segmented canonical + wallet `PostgreSQL` benchmark arm.
    #[must_use]
    pub const fn postgres_segmented_storage() -> Self {
        Self {
            id: "postgres-segmented-storage",
            canonical_engine: "postgres",
            canonical_model: "settled-segments-with-bounded-live-tail",
            topology: "postgres-segmented",
        }
    }

    /// Identifies the single-host canonical and wallet storage lifecycle.
    #[must_use]
    pub const fn rocksdb_storage_lifecycle() -> Self {
        Self {
            id: "rocksdb-storage-lifecycle",
            canonical_engine: "rocksdb",
            canonical_model: "canonical-replay-storage",
            topology: "rocksdb",
        }
    }

    /// Identifies authenticated checkpointed fixture replay into canonical `RocksDB`.
    #[must_use]
    pub const fn rocksdb_canonical_fixture_replay() -> Self {
        Self {
            id: "rocksdb-canonical-fixture-replay",
            canonical_engine: "rocksdb",
            canonical_model: "canonical-replay-storage",
            topology: "rocksdb",
        }
    }
}

/// Serializable form of the effective `RocksDB` resource budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RocksDbResourceBudgetSummary {
    /// Shared block-cache allocation.
    pub block_cache_bytes: u64,
    /// Live WAL size ceiling.
    pub max_wal_bytes: u64,
    /// Open `SST` file-handle limit.
    pub max_open_files: i32,
    /// Per-column-family write-buffer size.
    pub write_buffer_bytes: u64,
    /// Mutable plus immutable write-buffer count per column family.
    pub max_write_buffer_count: i32,
    /// Background flush and compaction job limit.
    pub max_background_jobs: i32,
    /// Aggregate memtable budget.
    pub memtable_budget_bytes: u64,
    /// Enabled `RocksDB` statistics level.
    pub statistics_level: &'static str,
}

impl From<RocksDbResourceBudget> for RocksDbResourceBudgetSummary {
    fn from(budget: RocksDbResourceBudget) -> Self {
        Self {
            block_cache_bytes: budget.block_cache_bytes,
            max_wal_bytes: budget.max_wal_bytes,
            max_open_files: budget.max_open_files,
            write_buffer_bytes: budget.write_buffer_bytes,
            max_write_buffer_count: budget.max_write_buffer_count,
            max_background_jobs: budget.max_background_jobs,
            memtable_budget_bytes: budget.memtable_budget_bytes,
            statistics_level: budget.statistics_level.as_str(),
        }
    }
}

/// Target and hard-limit durations for one measured acceptance boundary.
#[derive(Clone, Copy, Debug)]
pub struct AcceptanceThresholds {
    target_seconds: f64,
    hard_limit_seconds: f64,
}

impl AcceptanceThresholds {
    /// Validates and constructs acceptance-boundary thresholds in seconds.
    pub fn try_from_seconds(
        target_seconds: f64,
        hard_limit_seconds: f64,
    ) -> Result<Self, BenchError> {
        if !target_seconds.is_finite() || target_seconds <= 0.0 {
            return Err(BenchError::invalid_argument(
                "acceptance target seconds must be finite and greater than zero",
            ));
        }
        if !hard_limit_seconds.is_finite() || hard_limit_seconds <= 0.0 {
            return Err(BenchError::invalid_argument(
                "acceptance hard-limit seconds must be finite and greater than zero",
            ));
        }
        if target_seconds > hard_limit_seconds {
            return Err(BenchError::invalid_argument(
                "acceptance target seconds must not exceed hard-limit seconds",
            ));
        }
        Ok(Self {
            target_seconds,
            hard_limit_seconds,
        })
    }
}

/// Build and runtime provenance for one report.
#[derive(Clone, Debug, Serialize)]
pub struct ReportProvenance {
    /// `zinder-bench` package version.
    pub benchmark_version: &'static str,
    /// Source revision supplied by the operator, when available.
    pub software_revision: Option<String>,
    /// Per-invocation trial, cache-policy, and timing provenance.
    pub run: BenchmarkRunProvenance,
    /// Structured runner and complete-arm resource provenance.
    pub runner: RunnerProvenance,
    /// Immutable container image reference supplied by the operator.
    pub image_reference: Option<String>,
    /// Operating system for which the benchmark binary was built.
    pub target_os: &'static str,
    /// CPU architecture for which the benchmark binary was built.
    pub target_arch: &'static str,
}

/// Per-invocation provenance used to bind reports into a benchmark campaign.
#[derive(Clone, Debug, Serialize)]
pub struct BenchmarkRunProvenance {
    /// Operator-assigned trial identity shared by the paired candidate arms.
    pub trial_id: Option<String>,
    /// Declared treatment of the fixture's filesystem cache.
    pub fixture_cache_policy: Option<FixtureCachePolicy>,
    /// Wall-clock Unix timestamp captured before benchmark setup begins.
    pub started_at_unix_millis: u64,
    /// Wall-clock Unix timestamp captured after measured work completes.
    pub completed_at_unix_millis: u64,
}

/// Controlled fixture-cache treatment for a benchmark run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum FixtureCachePolicy {
    /// The operator evicted the fixture from the filesystem cache before the run.
    Cold,
    /// The operator deliberately retained or preloaded the fixture cache.
    Warm,
}

/// Tree-state checkpoint coverage persisted during fresh canonical construction.
///
/// [`Self::CadenceAndTip`] matches default ingest construction. Select
/// [`Self::EveryBlock`] to measure dense checkpoints before READY.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum TreeStateCheckpointCoverage {
    /// Persist and cross-check a tree-state checkpoint for every loaded block.
    EveryBlock,
    /// Persist sparse checkpoints only at the predecessor, every
    /// `TREE_STATE_CHECKPOINT_STRIDE` blocks, and at the build tip.
    #[default]
    CadenceAndTip,
}

impl TreeStateCheckpointCoverage {
    /// Returns the stable kebab-case identifier used in CLI flags and reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EveryBlock => "every-block",
            Self::CadenceAndTip => "cadence-and-tip",
        }
    }

    /// Returns whether construction stores a tree-state checkpoint for every loaded block.
    #[must_use]
    pub const fn stores_every_block(self) -> bool {
        matches!(self, Self::EveryBlock)
    }

    /// Maps a builder's effective per-block checkpoint setting back to its coverage enum.
    #[must_use]
    pub const fn from_every_block(every_block: bool) -> Self {
        if every_block {
            Self::EveryBlock
        } else {
            Self::CadenceAndTip
        }
    }

    /// Returns the exact persisted tree-state checkpoint count, including the
    /// predecessor checkpoint at `first_height - 1`, for contiguous loaded
    /// heights `first_height..=tip_height`.
    ///
    /// Returns `None` when `first_height == 0` or `tip_height < first_height`.
    #[must_use]
    pub fn expected_checkpoint_count(self, first_height: u32, tip_height: u32) -> Option<u64> {
        if first_height == 0 || tip_height < first_height {
            return None;
        }
        let predecessor_height = first_height - 1;
        match self {
            Self::EveryBlock => {
                let loaded_blocks = u64::from(tip_height) - u64::from(first_height) + 1;
                Some(loaded_blocks + 1)
            }
            Self::CadenceAndTip => {
                let stride = TREE_STATE_CHECKPOINT_STRIDE;
                let cadence_checkpoints =
                    u64::from(tip_height / stride) - u64::from(predecessor_height / stride);
                let tip_checkpoint = u64::from(!tip_height.is_multiple_of(stride));
                Some(1 + cadence_checkpoints + tip_checkpoint)
            }
        }
    }
}

impl fmt::Display for TreeStateCheckpointCoverage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Operator-supplied runner and complete-arm resource provenance.
#[derive(Clone, Debug, Serialize)]
pub struct RunnerProvenance {
    /// Stable runner identity; this label is not a substitute for the fields below.
    pub id: Option<String>,
    /// Aggregate CPU allocation for the measured benchmark arm, in logical cores.
    pub cpu_limit_cores: Option<f64>,
    /// Aggregate memory allocation for the measured benchmark arm.
    pub memory_limit_bytes: Option<u64>,
    /// Stable operator-defined storage performance class.
    pub storage_class: Option<String>,
}

/// One directly measured acceptance boundary.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct AcceptanceMeasurementSummary {
    /// Exact history scope covered by this measurement.
    pub scope: &'static str,
    /// Direct wall-clock duration of the stage.
    pub wall_clock_seconds: f64,
    /// Supplied target and hard-limit evaluation, when configured.
    pub thresholds: Option<AcceptanceThresholdSummary>,
}

/// Target and hard-limit evaluation for one measured duration.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct AcceptanceThresholdSummary {
    /// Desired completion time.
    pub target_seconds: f64,
    /// Maximum accepted completion time.
    pub hard_limit_seconds: f64,
    /// Whether the measured stage completed within the target.
    pub target_met: bool,
    /// Whether the measured stage completed within the hard limit.
    pub hard_limit_met: bool,
}

/// Aggregated head-of-line wait for one bulk-pipeline stage.
///
/// A large source-fetch wait relative to the replay wall clock signals the run
/// was source-bound rather than limited by the knob under test.
#[derive(Clone, Debug, Serialize)]
pub struct StageWaitStat {
    /// Bulk-pipeline stage that stalled waiting on its input.
    pub stage: String,
    /// Number of recorded head-of-line waits.
    pub wait_count: u64,
    /// Cumulative seconds the stage spent waiting on its input.
    pub wait_seconds: f64,
}

/// Aggregated work time for one bulk-catchup or canonical-construction substage.
#[derive(Clone, Debug, Serialize)]
pub struct StageDurationStat {
    /// Metric family that owns the stage.
    pub family: String,
    /// Stable stage label.
    pub stage: String,
    /// Outcome label.
    pub status: String,
    /// Number of completed stage invocations.
    pub call_count: u64,
    /// Cumulative histogram seconds across the invocations.
    pub task_seconds: f64,
}

/// Required proof that canonical construction did not cross a prohibited read boundary.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct CanonicalProhibitedReadSummary {
    /// Historical canonical prevout reads; canonical construction requires zero.
    pub historical_prevout_read_count: u64,
    /// Cross-block wallet-state reads; canonical construction requires zero.
    pub cross_block_wallet_read_count: u64,
}

/// Aggregated cache-bypassing publication scan evidence for one canonical family.
#[derive(Clone, Debug, Serialize)]
pub struct CanonicalPublicationFamilyScanStat {
    /// Canonical column family scanned.
    pub family: String,
    /// Number of complete successful scans.
    pub scan_count: u64,
    /// Cumulative wall-clock seconds spent scanning the family.
    pub scan_seconds: f64,
    /// Cumulative rows observed across the scans.
    pub row_count: u64,
    /// Cumulative logical key-and-value bytes observed across the scans.
    pub logical_bytes: u64,
}

/// Source-lane evidence aggregated for one successful fixture replay.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct SourceFetchAttributionSummary {
    /// Successfully completed source-segment requests.
    pub completed_segment_request_count: u64,
    /// Total connected blocks returned, including responses later discarded
    /// and fetched again.
    pub total_connected_blocks_returned: u64,
    /// Total adapter-observed response payload bytes, including responses
    /// later discarded and fetched again.
    pub total_response_payload_bytes: u64,
    /// Completed source-segment requests per replay wall-clock second.
    pub completed_segment_requests_per_second: f64,
    /// Response payload bytes per replay wall-clock second.
    pub response_payload_bytes_per_second: f64,
    /// Cumulative concurrent-task seconds spent awaiting completed segment
    /// requests. This can exceed replay wall-clock time.
    pub cumulative_fetch_chain_segment_task_seconds: f64,
    /// Always zero. Density adjustments retain disjoint prefetched ranges.
    pub density_restart_count: u64,
    /// Segment-size reductions caused by dense response sizing, including
    /// adjustments that retain already-valid speculative work.
    pub density_sizing_adjustment_count: u64,
    /// Always zero. The adapter splits oversized requests and retains disjoint prefetch.
    pub response_too_large_restart_count: u64,
    /// Already-completed speculative segments discarded across restarts.
    pub discarded_completed_segment_count: u64,
    /// Still-in-flight speculative segments discarded across restarts.
    pub discarded_in_flight_segment_count: u64,
    /// Exact payload bytes held by completed speculative segments at discard.
    pub discarded_completed_response_bytes: u64,
    /// Completed speculative segments retained after density adjustments.
    pub retained_completed_segment_count: u64,
    /// In-flight speculative segments retained after density adjustments.
    pub retained_in_flight_segment_count: u64,
    /// Completed response bytes retained after density adjustments.
    pub retained_completed_response_bytes: u64,
    /// Source-fetch reservations rejected by the configured byte watermark.
    pub source_watermark_blocked_count: u64,
    /// Source-fetch watermark blocks per replay wall-clock second.
    pub source_watermark_blocks_per_second: f64,
}

/// Digest evidence recomputed from persisted canonical replay encodings.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CanonicalBlockFactsSequenceDigestSummary {
    /// Per-block reference-encoding digest version.
    pub block_digest_version: u16,
    /// Ordered sequence-digest version.
    pub sequence_digest_version: u16,
    /// Number of ordered block digests committed by the sequence.
    pub block_count: u64,
    /// Sequence SHA-256, hex encoded.
    pub sha256: String,
}

impl CanonicalBlockFactsSequenceDigestSummary {
    /// Converts the typed core digest into report-safe scalar evidence.
    #[must_use]
    pub fn from_digest(
        block_digest_version: zinder_core::CanonicalBlockFactsDigestVersion,
        sequence_digest: zinder_core::CanonicalBlockFactsSequenceDigest,
    ) -> Self {
        Self {
            block_digest_version: block_digest_version.value(),
            sequence_digest_version: sequence_digest.version().value(),
            block_count: sequence_digest.block_count(),
            sha256: hex::encode(sequence_digest.as_bytes()),
        }
    }
}

/// `PostgreSQL` client and database runtime evidence for one measured arm.
#[derive(Clone, Debug, Serialize)]
pub struct PostgresBenchmarkRuntimeEvidence {
    /// Immutable database container image reference supplied by the operator.
    pub database_image_reference: Option<String>,
    /// CPU limit applied to the benchmark client container.
    pub client_cpu_limit_cores: Option<f64>,
    /// Memory limit applied to the benchmark client container.
    pub client_memory_limit_bytes: Option<u64>,
    /// CPU limit applied to the database container.
    pub database_cpu_limit_cores: Option<f64>,
    /// Memory limit applied to the database container.
    pub database_memory_limit_bytes: Option<u64>,
}

/// Engine-specific settings and physical write evidence for one canonical-replay round-trip arm.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "engine", rename_all = "kebab-case")]
pub enum CanonicalReplayStorageEvidence {
    /// `RocksDB` external-SST construction settings.
    #[serde(rename = "rocksdb")]
    RocksDb {
        /// Candidate-owned physical schema version.
        storage_schema_version: u16,
        /// Stable construction mechanism label.
        ingestion_mode: &'static str,
        /// Stable durability description for ingestion and publication.
        durability_mode: &'static str,
        /// Resolved direct or buffered filesystem I/O mode for database access.
        database_io_mode: String,
        /// Filesystem I/O mode used while constructing the external SST.
        external_sst_io_mode: &'static str,
        /// Explicit block-compression algorithm used by both SST construction and the column family.
        compression: &'static str,
        /// Bytes written into the external SST before ingestion.
        external_sst_bytes: u64,
        /// Effective bounded `RocksDB` resources.
        rocksdb_resource_budget: RocksDbResourceBudgetSummary,
    },
    /// Postgres binary-COPY construction settings.
    Postgres {
        /// Candidate-owned physical schema version.
        storage_schema_version: u16,
        /// Stable construction mechanism label.
        ingestion_mode: &'static str,
        /// Whether the candidate replay table is durable and WAL logged.
        tables_logged: bool,
        /// Explicit TOAST compression used for canonical replay encodings.
        replay_envelope_compression: &'static str,
        /// Queried performance and durability settings for the measured database arm.
        server_settings: Box<PostgresServerSettings>,
        /// Bytes owned by the canonical-replay heap and its auxiliary forks.
        replay_table_bytes: u64,
        /// Bytes owned by indexes created after replay loading.
        index_bytes: u64,
        /// WAL bytes advanced during construction and publication.
        wal_bytes: u64,
        /// Client/database images and component resource limits.
        benchmark_runtime: PostgresBenchmarkRuntimeEvidence,
    },
}

/// Direct measurements from a persisted canonical-replay-storage round trip.
#[derive(Clone, Debug)]
pub struct CanonicalReplayStorageMeasurements {
    /// Prepare concurrency used while parsing fixture blocks.
    pub block_prepare_concurrency: u32,
    /// Total measured round-trip seconds, including validation and publication.
    pub wall_clock_seconds: f64,
    /// Fixture metadata validation plus fresh backend and physical-schema initialization.
    pub storage_initialization_wall_clock_seconds: f64,
    /// Fixture read, parse, and semantic replay encoding seconds.
    pub replay_preparation_wall_clock_seconds: f64,
    /// Primary table or external-SST construction and ingestion seconds.
    pub replay_persistence_wall_clock_seconds: f64,
    /// Index construction performed after primary replay loading.
    pub index_construction_wall_clock_seconds: f64,
    /// Post-load storage optimization, such as `PostgreSQL` `ANALYZE`.
    pub storage_optimization_wall_clock_seconds: f64,
    /// Persisted read-back and digest-validation seconds.
    pub validation_wall_clock_seconds: f64,
    /// Completion-fence publication seconds.
    pub publication_wall_clock_seconds: f64,
    /// Validation through a new reader: a database reopen or server reconnection.
    pub fresh_reader_validation_wall_clock_seconds: f64,
    /// Final physical storage and write-amplification measurement seconds.
    pub storage_measurement_wall_clock_seconds: f64,
    /// First persisted block height.
    pub first_height: u32,
    /// First persisted block hash in internal byte order.
    pub first_hash_hex: String,
    /// Last persisted block height.
    pub tip_height: u32,
    /// Last persisted block hash in internal byte order.
    pub tip_hash_hex: String,
    /// Logical semantic replay encoding bytes submitted to storage.
    pub logical_replay_bytes: u64,
    /// Final physical bytes owned by the candidate tables/store.
    pub physical_storage_bytes: u64,
    /// Ordered digest recomputed from persisted rows.
    pub persisted_sequence_digest: CanonicalBlockFactsSequenceDigestSummary,
    /// Semantic replay format version decoded from every persisted row.
    pub replay_format_version: u32,
    /// Whether every persisted replay envelope decoded into complete canonical replay records.
    pub semantic_replay_validated: bool,
    /// Engine-specific settings and physical write evidence.
    pub storage: CanonicalReplayStorageEvidence,
    /// Peak resident-set-size reading for the benchmark client process.
    ///
    /// This includes the embedded database in the `RocksDB` arm and excludes
    /// the separate database server in the `PostgreSQL` arm.
    pub benchmark_client_peak_rss: PeakRss,
    /// Storage implementation measured by this run.
    pub storage_candidate: StorageCandidateIdentity,
    /// Source revision of the measured binary, when supplied.
    pub software_revision: Option<String>,
    /// Campaign trial identity supplied by the operator, when available.
    pub trial_id: Option<String>,
    /// Declared fixture-cache treatment for this run, when available.
    pub fixture_cache_policy: Option<FixtureCachePolicy>,
    /// Wall-clock Unix timestamp captured before benchmark setup begins.
    pub run_started_at_unix_millis: u64,
    /// Wall-clock Unix timestamp captured after measured work completes.
    pub run_completed_at_unix_millis: u64,
    /// Stable operator label for the complete benchmark arm.
    pub runner_id: Option<String>,
    /// Aggregate CPU limit for the complete benchmark arm.
    pub cpu_limit_cores: Option<f64>,
    /// Aggregate memory limit for the complete benchmark arm.
    pub memory_limit_bytes: Option<u64>,
    /// Stable operator-defined storage performance class.
    pub storage_class: Option<String>,
    /// Immutable container image reference, when supplied.
    pub image_reference: Option<String>,
}

/// Persisted canonical-replay-storage scalars.
///
/// This is storage evidence for block-local replay records, not a chain epoch,
/// query-readiness, reorg, or materialized-view lifecycle.
#[derive(Clone, Debug, Serialize)]
pub struct CanonicalReplayStorageSummary {
    /// Exact diagnostic boundary this report drove.
    pub scope: &'static str,
    /// Prepare concurrency used while parsing fixture blocks.
    pub block_prepare_concurrency: u32,
    /// Total measured round-trip seconds.
    pub wall_clock_seconds: f64,
    /// Fixture metadata validation plus fresh backend and physical-schema initialization.
    pub storage_initialization_wall_clock_seconds: f64,
    /// Fixture read, parse, and semantic replay encoding seconds.
    pub replay_preparation_wall_clock_seconds: f64,
    /// Primary table or external-SST construction and ingestion seconds.
    pub replay_persistence_wall_clock_seconds: f64,
    /// Index construction performed after primary replay loading.
    pub index_construction_wall_clock_seconds: f64,
    /// Post-load storage optimization seconds.
    pub storage_optimization_wall_clock_seconds: f64,
    /// Persisted read-back and digest-validation seconds.
    pub validation_wall_clock_seconds: f64,
    /// Completion-fence publication seconds.
    pub publication_wall_clock_seconds: f64,
    /// Validation through a new reader: a database reopen or server reconnection.
    pub fresh_reader_validation_wall_clock_seconds: f64,
    /// Final physical storage and write-amplification measurement seconds.
    pub storage_measurement_wall_clock_seconds: f64,
    /// Wall-clock time not attributed to an explicitly measured stage.
    pub unattributed_wall_clock_seconds: f64,
    /// First persisted block height.
    pub first_height: u32,
    /// First persisted block hash in internal byte order.
    pub first_hash_hex: String,
    /// Last persisted block height.
    pub tip_height: u32,
    /// Last persisted block hash in internal byte order.
    pub tip_hash_hex: String,
    /// Persisted block count.
    pub block_count: u64,
    /// End-to-end persisted blocks per second.
    pub blocks_per_second: f64,
    /// Logical semantic replay encoding bytes submitted to storage.
    pub logical_replay_bytes: u64,
    /// Final physical bytes owned by the candidate tables/store.
    pub physical_storage_bytes: u64,
    /// Ordered digest recomputed from persisted rows.
    pub persisted_sequence_digest: CanonicalBlockFactsSequenceDigestSummary,
    /// Whether the persisted sequence equals the fixture capture oracle.
    pub fixture_sequence_digest_match: bool,
    /// Semantic replay format version decoded from every persisted row.
    pub replay_format_version: u32,
    /// Whether every persisted replay envelope decoded into complete canonical replay records.
    pub semantic_replay_validated: bool,
    /// Engine-specific settings and physical write evidence.
    pub storage: CanonicalReplayStorageEvidence,
    /// Peak resident-set-size reading for the benchmark client process.
    ///
    /// This includes the embedded database in the `RocksDB` arm and excludes
    /// the separate database server in the `PostgreSQL` arm.
    pub benchmark_client_peak_rss: PeakRss,
}

/// Full report for one canonical-replay-storage persisted round trip.
#[derive(Clone, Debug, Serialize)]
pub struct CanonicalReplayStorageReport {
    /// Stable report contract identity.
    pub contract_identity: String,
    /// Machine-readable report schema version.
    pub report_format_version: u32,
    /// Build and source provenance.
    pub provenance: ReportProvenance,
    /// Fixture identity.
    pub fixture: FixtureSummary,
    /// Storage candidate measured by this invocation.
    pub storage_candidate: StorageCandidateIdentity,
    /// Canonical replay storage evidence.
    pub round_trip: CanonicalReplayStorageSummary,
}

/// Exact resource profile and effective limits used by one canonical fixture replay.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct RocksDbCanonicalFixtureReplayResourceLimits {
    /// Concrete block transport admitted for this replay.
    pub block_source: &'static str,
    /// Server-side delay recorded as transport experiment provenance.
    pub injected_response_delay_millis: u64,
    /// Global actual unary `GetBlock` limit, when gRPC is selected.
    pub indexer_get_block_max_in_flight_requests: Option<u32>,
    /// CPU envelope from which default preparation concurrency was derived.
    pub derived_for_cpu_limit_cores: u32,
    /// Memory envelope from which default admission watermarks were derived.
    pub derived_for_memory_limit_bytes: u64,
    /// Per-operation source deadline.
    pub request_timeout_seconds: u64,
    /// Maximum accepted source response body.
    pub max_response_bytes: u64,
    /// Adaptive target for one source response.
    pub source_segment_target_response_bytes: u64,
    /// Maximum blocks requested in one source segment.
    pub source_segment_max_blocks: u32,
    /// Maximum concurrent source segment requests.
    pub source_fetch_max_in_flight_requests: u32,
    /// Aggregate in-flight source response watermark.
    pub source_fetch_max_in_flight_bytes: u64,
    /// Maximum canonical block preparations in flight.
    pub block_prepare_concurrency: u32,
    /// Aggregate canonical preparation memory watermark.
    pub block_prepare_memory_watermark_bytes: u64,
    /// Threads hashing the ordered loader's commitment trees.
    pub tree_hashing_workers: u32,
    /// Retained shallow-reorg depth used for baseline settlement.
    pub supported_reorg_depth: u32,
    /// Fixed delay applied to each outer fixture segment response.
    pub source_segment_delay_millis: u64,
    /// Tree-state checkpoint coverage persisted during canonical construction.
    pub tree_state_checkpoint_coverage: TreeStateCheckpointCoverage,
    /// Canonical embedded-store writer and cold-reopen budget.
    pub canonical_rocksdb: RocksDbResourceBudgetSummary,
}

/// Report-safe block and SST evidence from the production canonical loader.
#[derive(Clone, Debug, Serialize)]
pub struct RocksDbCanonicalFixtureSourceLoadSummary {
    /// First retained block.
    pub first_block: StorageLifecycleBlockId,
    /// Parent hash of the first retained block, in canonical internal byte order.
    pub first_parent_hash_hex: String,
    /// Fixed source tip loaded into the store.
    pub tip: StorageLifecycleBlockId,
    /// Contiguous source blocks loaded.
    pub block_count: u64,
    /// Source transactions loaded.
    pub transaction_count: u64,
    /// Height-addressed header rows.
    pub block_header_count: u64,
    /// Hash-addressed block index rows.
    pub block_hash_index_count: u64,
    /// Settled segment index rows persisted in `settled_segment_index`.
    pub settled_segment_index_count: u64,
    /// Unsealed live-tail block rows persisted in `live_tail_block`.
    pub live_tail_block_count: u64,
    /// Transaction location rows.
    pub transaction_location_count: u64,
    /// Retained raw transaction rows; none retention requires zero.
    pub transaction_blob_count: u64,
    /// Raw block rows; none retention requires zero.
    pub block_blob_count: u64,
    /// Typed commitment-tree checkpoints, including the predecessor.
    pub tree_state_checkpoint_count: u64,
    /// Source-authenticated completed subtree roots.
    pub subtree_root_count: u64,
    /// Logical semantic replay bytes across settled segments and live-tail blocks.
    pub logical_replay_bytes: u64,
    /// Settled segment index key-and-value bytes submitted to SST writers.
    pub settled_segment_index_logical_bytes: u64,
    /// Live-tail block key-and-value bytes submitted to SST writers.
    pub live_tail_block_logical_bytes: u64,
    /// Live-tail raw block key-and-value bytes submitted to SST writers.
    pub live_tail_raw_block_logical_bytes: u64,
    /// Logical key-and-value bytes submitted to block SST writers.
    pub logical_bytes: u64,
    /// Physical bytes occupied by staged block SSTs.
    pub sst_file_bytes: u64,
    /// Staged block SST files ingested.
    pub sst_file_count: u64,
    /// Canonical replay-envelope contract admitted for every block.
    pub replay_format_version: u32,
    /// Ordered canonical replay digest loaded into the store.
    pub sequence_digest: CanonicalBlockFactsSequenceDigestSummary,
}

/// Event fence authenticated by READY publication and independent cold reopen.
#[derive(Clone, Debug, Serialize)]
pub struct RocksDbCanonicalFixtureEventFenceSummary {
    /// Visible chain epoch identifier.
    pub chain_epoch_id: u64,
    /// Durable chain-event sequence.
    pub chain_event_sequence: u64,
    /// Visible canonical tip.
    pub visible_tip: StorageLifecycleBlockId,
    /// Ordered canonical replay digest at the fence.
    pub sequence_digest: CanonicalBlockFactsSequenceDigestSummary,
}

/// Canonical-store READY, cold-reopen, and full-scan evidence for a checkpointed fixture.
#[derive(Clone, Debug, Serialize)]
pub struct RocksDbCanonicalFixtureReadySummary {
    /// Exact boundary certified by this report section.
    pub scope: &'static str,
    /// First retained canonical block.
    pub first_retained_block: StorageLifecycleBlockId,
    /// Visible fixed canonical tip.
    pub visible_tip: StorageLifecycleBlockId,
    /// Visible baseline epoch.
    pub visible_epoch_id: u64,
    /// Visible baseline event sequence.
    pub visible_event_sequence: u64,
    /// Contiguous READY block count.
    pub visible_block_count: u64,
    /// Canonical replay-envelope version.
    pub replay_format_version: u32,
    /// Ordered READY sequence digest.
    pub sequence_digest: CanonicalBlockFactsSequenceDigestSummary,
    /// Logical replay bytes authenticated by READY.
    pub logical_replay_bytes: u64,
    /// Settled baseline tip selected from the retained range.
    pub settled_tip: StorageLifecycleBlockId,
    /// Event fence read after the independent cold reopen.
    pub event_fence: RocksDbCanonicalFixtureEventFenceSummary,
    /// Whether the fixed-tip checkpoint was authenticated before publication.
    pub source_tip_checkpoint_authenticated: bool,
    /// Whether publication and cold reopen returned identical READY evidence.
    pub published_and_reopened_ready_match: bool,
    /// Whether cold-reopen READY and event-fence fields match exactly.
    pub reopened_ready_and_event_fence_match: bool,
    /// Cache-bypassing semantic replay rows authenticated after cold reopen.
    pub full_scan_block_count: u64,
}

/// Direct measurements for one canonical fixture replay report.
#[derive(Clone, Debug)]
pub struct RocksDbCanonicalFixtureReplayMeasurements {
    /// Fixture manifest digest bound into the admitted replay plan.
    pub replay_plan_fixture_manifest_sha256: String,
    /// Stable digest of the admitted canonical replay-plan sidecar.
    pub replay_plan_digest_sha256: String,
    /// Exact effective resource limits.
    pub resource_limits: RocksDbCanonicalFixtureReplayResourceLimits,
    /// Publication-proof producer used by the fresh canonical build.
    pub publication_proof_provenance: &'static str,
    /// Complete measured lifecycle seconds.
    pub total_seconds: f64,
    /// Block/SST/count evidence from the real canonical loader.
    pub source_load: RocksDbCanonicalFixtureSourceLoadSummary,
    /// READY and independent cold-reopen evidence.
    pub canonical_ready: RocksDbCanonicalFixtureReadySummary,
    /// Final bytes occupied by the cold-reopened canonical store directory.
    pub physical_store_bytes: u64,
    /// Peak resident set size for the embedded process.
    pub benchmark_client_peak_rss: PeakRss,
    /// Wall-clock timestamp captured before the command begins.
    pub run_started_at_unix_millis: u64,
    /// Wall-clock timestamp captured after the measured lifecycle.
    pub run_completed_at_unix_millis: u64,
}

/// Authenticated checkpointed fixture replay into the real canonical store.
#[derive(Clone, Debug, Serialize)]
pub struct RocksDbCanonicalFixtureReplayReport {
    /// Stable report contract identity.
    pub contract_identity: String,
    /// Machine-readable report schema version.
    pub report_format_version: u32,
    /// Build and run provenance.
    pub provenance: ReportProvenance,
    /// Fixture identity.
    pub fixture: FixtureSummary,
    /// Fixture manifest digest bound into the admitted replay plan.
    pub replay_plan_fixture_manifest_sha256: String,
    /// Stable digest of the admitted canonical replay-plan sidecar.
    pub replay_plan_digest_sha256: String,
    /// Concrete embedded storage candidate.
    pub storage_candidate: StorageCandidateIdentity,
    /// Exact effective resource limits.
    pub resource_limits: RocksDbCanonicalFixtureReplayResourceLimits,
    /// Publication-proof producer used by the fresh canonical build.
    pub publication_proof_provenance: String,
    /// Complete measured lifecycle seconds.
    pub total_seconds: f64,
    /// End-to-end loaded blocks per second.
    pub blocks_per_second: f64,
    /// Block/SST/count evidence from the real canonical loader.
    pub source_load: RocksDbCanonicalFixtureSourceLoadSummary,
    /// READY and independent cold-reopen evidence.
    pub canonical_ready: RocksDbCanonicalFixtureReadySummary,
    /// Final bytes occupied by the cold-reopened canonical store directory.
    pub physical_store_bytes: u64,
    /// Source request, response, and adaptive-prefetch attribution.
    pub source_fetch_attribution: Option<SourceFetchAttributionSummary>,
    /// Required zero-read evidence for the canonical construction boundary.
    pub prohibited_reads: Option<CanonicalProhibitedReadSummary>,
    /// Full publication scans grouped by canonical family; empty for trusted fresh-writer proof.
    pub publication_family_scans: Vec<CanonicalPublicationFamilyScanStat>,
    /// Per-stage bulk-pipeline head-of-line wait totals.
    pub head_of_line_wait: Vec<StageWaitStat>,
    /// Per-substage block preparation and canonical construction timing.
    pub stage_durations: Vec<StageDurationStat>,
    /// Peak resident set size for the embedded process.
    pub benchmark_client_peak_rss: PeakRss,
}

/// Node source identity frozen before one storage lifecycle begins.
#[derive(Clone, Debug, Serialize)]
pub struct StorageLifecycleSourceSummary {
    /// Concrete source adapter family used by the measurement.
    pub family: &'static str,
    /// Network name in Zinder-native encoding.
    pub network: String,
    /// Number of node-advertised consensus upgrade activations.
    pub network_upgrade_activation_count: usize,
    /// Activation-table fingerprint algorithm version.
    pub network_upgrade_activations_fingerprint_version: u16,
    /// Domain-separated activation-table fingerprint.
    pub network_upgrade_activations_fingerprint_hex: String,
    /// Node tip observed before selecting the fixed build tip.
    pub source_tip_at_freeze: StorageLifecycleBlockId,
    /// Exact immutable tip authenticated by canonical construction.
    pub fixed_build_tip: StorageLifecycleBlockId,
    /// Node tip observed after canonical source-family loading completed.
    pub source_tip_after_canonical_load: StorageLifecycleBlockId,
}

/// Report-safe block identity using Zinder's canonical internal hash order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StorageLifecycleBlockId {
    /// Block height.
    pub height: u32,
    /// Block hash in canonical internal byte order.
    pub hash_hex: String,
}

/// Fixed storage identities exercised by the lifecycle command.
#[derive(Clone, Debug, Serialize)]
pub struct StorageLifecycleContractSummary {
    /// Canonical store contract identity.
    pub canonical_store_identity: &'static str,
    /// Canonical physical schema version.
    pub canonical_store_schema_version: u16,
    /// Wallet store contract identity.
    pub wallet_store_identity: &'static str,
    /// Wallet physical schema version.
    pub wallet_store_schema_version: u16,
    /// Wallet materialized-view schema version.
    pub wallet_projection_schema_version: u16,
    /// Wallet row-value encoding version.
    pub wallet_value_encoding_version: u16,
}

/// Canonical publication checks included in the lifecycle measurement.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum CanonicalPublicationMode {
    /// Use the fresh writer's authenticated construction evidence, as ingest does.
    TrustedFresh,
    /// Independently certify the stored rows before publishing.
    ColdCertified,
}

/// Exact source-pipeline and embedded-store ceilings used by one run.
#[derive(Clone, Debug, Serialize)]
pub struct StorageLifecycleResourceLimits {
    /// Per-request source deadline.
    pub request_timeout_seconds: u64,
    /// Maximum accepted source response body.
    pub max_response_bytes: u64,
    /// Adaptive target for one source segment response.
    pub source_segment_target_response_bytes: u64,
    /// Maximum blocks requested in one source segment.
    pub source_segment_max_blocks: u32,
    /// Maximum concurrent source segment requests.
    pub source_fetch_max_in_flight_requests: u32,
    /// Aggregate in-flight source response watermark.
    pub source_fetch_max_in_flight_bytes: u64,
    /// Maximum canonical block preparations in flight.
    pub block_prepare_concurrency: u32,
    /// Aggregate canonical preparation memory watermark.
    pub block_prepare_memory_watermark_bytes: u64,
    /// Canonical embedded-store resource budget.
    pub canonical_rocksdb: RocksDbResourceBudgetSummary,
    /// Tree-state checkpoint coverage persisted during canonical construction.
    pub tree_state_checkpoint_coverage: TreeStateCheckpointCoverage,
    /// Publication path measured before canonical READY.
    pub canonical_publication_mode: CanonicalPublicationMode,
    /// Wallet embedded-store resource budget.
    pub wallet_rocksdb: RocksDbResourceBudgetSummary,
    /// Retained wallet reorg depth.
    pub supported_reorg_depth: u32,
    /// Wallet outpoint event sort memory ceiling.
    pub wallet_max_outpoint_sort_memory_bytes: u64,
    /// Per-sorter wallet secondary-index sort memory ceiling.
    pub wallet_max_secondary_sort_memory_bytes_per_sorter: u64,
    /// Per-sorter wallet external-sort temporary-file ceiling.
    pub wallet_max_temporary_file_bytes_per_sorter: u64,
    /// Target logical bytes for one wallet SST file.
    pub wallet_sst_target_logical_bytes: u64,
    /// Wallet build's accounted reorg-undo memory ceiling.
    pub wallet_max_accounted_reorg_undo_bytes: u64,
}

/// Acceptance results for the two storage readiness boundaries this command owns.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct RocksDbStorageLifecycleAcceptance {
    /// Canonical version-1 READY plus cold admission.
    pub canonical_storage_ready: AcceptanceMeasurementSummary,
    /// Wallet version-1 READY plus final cold canonical/wallet fence admission.
    pub wallet_storage_ready: AcceptanceMeasurementSummary,
}

/// Direct wall-clock durations for the complete storage lifecycle.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct StorageLifecyclePhaseDurations {
    /// Source creation, activation discovery, tip freeze, and genesis identity fetch.
    pub source_discovery_seconds: f64,
    /// Fresh canonical store identity and BUILDING publication.
    pub canonical_store_initialization_seconds: f64,
    /// Canonical source fetch, preparation, SST ingestion, source-family load, and tip authentication.
    pub canonical_source_load_seconds: f64,
    /// Canonical cold certification, including flush and reopen. Zero for trusted-fresh runs.
    pub canonical_cold_validation_seconds: f64,
    /// Atomic epoch-1, event-1, and READY publication, including any trusted-fresh preparation.
    pub canonical_ready_publication_seconds: f64,
    /// Independent cold admission before wallet construction.
    pub canonical_cold_reopen_seconds: f64,
    /// Complete public wallet build call through READY publication.
    pub wallet_build_seconds: f64,
    /// Final independent cold admission of both stores and fence comparison.
    pub final_cold_reopen_seconds: f64,
    /// Entire command through the final admitted storage fences.
    pub total_seconds: f64,
}

/// Canonical version-1 durable evidence after publication and cold admission.
#[derive(Clone, Debug, Serialize)]
pub struct CanonicalStorageReadySummary {
    /// Exact boundary certified by this section.
    pub scope: &'static str,
    /// First retained canonical block.
    pub first_retained_block: StorageLifecycleBlockId,
    /// Visible fixed canonical tip.
    pub visible_tip: StorageLifecycleBlockId,
    /// Fixed baseline epoch identifier.
    pub visible_epoch_id: u64,
    /// Fixed baseline event sequence.
    pub visible_event_sequence: u64,
    /// Contiguous source block count.
    pub block_count: u64,
    /// Parsed source transaction count.
    pub transaction_count: u64,
    /// Persisted tree-state checkpoints, including the predecessor checkpoint.
    pub tree_state_checkpoint_count: u64,
    /// Source-authenticated completed subtree-root count.
    pub subtree_root_count: u64,
    /// Canonical semantic replay format version.
    pub replay_format_version: u32,
    /// Ordered canonical replay digest evidence.
    pub sequence_digest: CanonicalBlockFactsSequenceDigestSummary,
    /// Logical semantic replay bytes authenticated by READY.
    pub logical_replay_bytes: u64,
    /// Total logical key-and-value bytes submitted to canonical SST writers.
    pub logical_storage_bytes: u64,
    /// Physical staged SST bytes ingested by `RocksDB`.
    pub sst_file_bytes: u64,
    /// Number of staged SST files ingested by `RocksDB`.
    pub sst_file_count: u64,
    /// Final bytes owned below the canonical store path.
    pub physical_store_bytes: u64,
    /// Database filesystem I/O mode selected by `RocksDB`.
    pub database_io_mode: String,
    /// Whether exact final checkpoint authentication completed before publication.
    pub source_tip_checkpoint_authenticated: bool,
    /// Whether a new process-equivalent open admitted the exact READY evidence.
    pub cold_reopen_evidence_match: bool,
}

/// Wallet row counts rendered without coupling report consumers to Rust types.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct WalletStorageRowCounts {
    /// Current transparent output rows.
    pub transparent_unspent_output_count: u64,
    /// Address-to-current-output index rows.
    pub transparent_unspent_output_by_address_count: u64,
    /// Historical spent output rows.
    pub transparent_spent_output_count: u64,
    /// Address transaction-history rows.
    pub transparent_address_transaction_count: u64,
    /// Non-zero address-balance rows.
    pub transparent_address_balance_count: u64,
    /// Retained reorg undo rows.
    pub reorg_undo_count: u64,
}

/// Current transparent UTXO aggregate admitted with wallet READY.
#[derive(Clone, Debug, Serialize)]
pub struct WalletStorageUtxoSummary {
    /// Current unspent output count.
    pub utxo_count: u64,
    /// Current transparent unspent value in zatoshis.
    pub total_value_zat: u64,
    /// Commitment scheme name.
    pub commitment_scheme: &'static str,
    /// Exact full accumulator bytes.
    pub commitment_accumulator_hex: String,
    /// Display digest of the full accumulator.
    pub commitment_display_digest_hex: String,
}

/// Evidence from one bounded fixed-key, variable-value external sorter.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct WalletVariableValueSortEvidence {
    /// Records admitted to this sorter.
    pub record_count: u64,
    /// Initial sorted run count emitted before merge passes.
    pub initial_run_count: u64,
    /// Bounded-fan-in merge passes completed.
    pub merge_pass_count: u64,
    /// Peak explicitly accounted sort memory.
    pub peak_accounted_sort_memory_bytes: u64,
    /// Caller-supplied accounted sort memory ceiling.
    pub max_accounted_sort_memory_bytes: u64,
    /// Peak temporary-file bytes present during construction.
    pub peak_temporary_file_bytes: u64,
    /// Caller-supplied temporary-file ceiling.
    pub max_temporary_file_bytes: u64,
    /// Bytes in the runs the consumer reads, which it removes as it finishes.
    pub final_run_file_bytes: u64,
}

/// External sorting and SST evidence from the wallet build.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct WalletStorageConstructionEvidence {
    /// Output and spend events sorted by transparent outpoint.
    pub outpoint_sort: WalletVariableValueSortEvidence,
    /// Current output rows sorted by address and outpoint.
    pub address_index_sort: WalletVariableValueSortEvidence,
    /// Address history rows sorted by address and canonical position.
    pub address_transaction_sort: WalletVariableValueSortEvidence,
    /// Peak accounted memory retained for bounded reorg undo rows.
    pub peak_accounted_reorg_undo_bytes: u64,
    /// Caller-supplied reorg undo memory ceiling.
    pub max_accounted_reorg_undo_bytes: u64,
    /// Peak accounted reorg-undo suffix memory during cold validation.
    pub cold_validation_peak_accounted_reorg_undo_bytes: u64,
    /// Caller-supplied cold-validation reorg-undo memory ceiling.
    pub cold_validation_max_accounted_reorg_undo_bytes: u64,
    /// Random `RocksDB` point reads during cold validation; version 1 requires zero.
    pub cold_validation_random_read_count: u64,
    /// Logical wallet row bytes submitted to SST writers.
    pub logical_row_bytes: u64,
    /// Physical wallet SST bytes ingested by `RocksDB`.
    pub sst_file_bytes: u64,
    /// Wallet SST files ingested by `RocksDB`.
    pub sst_file_count: u64,
}

/// Public wallet builder phase durations copied into report-safe seconds.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct WalletStoragePhaseDurations {
    /// Fresh wallet store initialization.
    pub store_initialization_seconds: f64,
    /// Authenticated canonical replay scan.
    pub canonical_scan_seconds: f64,
    /// External outpoint run finalization and merge.
    pub outpoint_sort_seconds: f64,
    /// Ordered output/spend merge and primary-family SST writes.
    pub outpoint_merge_seconds: f64,
    /// Secondary sorting, balances, history, and retained undo derivation.
    pub secondary_row_derivation_seconds: f64,
    /// Row-count and version-1 wallet-projection-digest finalization.
    pub logical_evidence_seconds: f64,
    /// External SST ingestion into the unpublished wallet store.
    pub row_load_seconds: f64,
    /// Flush, close, and BUILDING cold reopen.
    pub flush_and_cold_reopen_seconds: f64,
    /// Cold digest and row-count readback of every wallet family.
    pub cold_validation_seconds: f64,
    /// Merged scan of the unspent and spent primary output families.
    pub cold_validation_primary_output_scan_seconds: f64,
    /// Scan of the address-unspent index and address-balance families.
    pub cold_validation_address_index_and_balance_readback_seconds: f64,
    /// Scan of the address-transaction family.
    pub cold_validation_address_transaction_readback_seconds: f64,
    /// Scan of the retained reorg-undo suffix.
    pub cold_validation_reorg_undo_readback_seconds: f64,
    /// Synchronous READY publication.
    pub ready_publication_seconds: f64,
    /// Complete wallet build duration.
    pub total_seconds: f64,
}

/// One non-overlapping wallet build stage in report-safe seconds.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct WalletBuildStageSeconds {
    /// Stable stage name shared with the projector's build-stage histogram.
    pub stage: &'static str,
    /// Wall time spent in the stage.
    pub seconds: f64,
}

impl WalletBuildStageSeconds {
    /// Converts a builder report's stages, in execution order.
    #[must_use]
    pub fn from_report(report: &zinder_wallet_rocksdb::RocksDbWalletBuildReport) -> Vec<Self> {
        report
            .stage_durations()
            .into_iter()
            .map(|stage| Self {
                stage: stage.stage,
                seconds: stage.duration.as_secs_f64(),
            })
            .collect()
    }
}

/// Wallet version-1 durable evidence after publication and cold admission.
#[derive(Clone, Debug, Serialize)]
pub struct WalletStorageReadySummary {
    /// Exact boundary certified by this section.
    pub scope: &'static str,
    /// Canonical source epoch represented by the wallet projection.
    pub source_epoch_id: u64,
    /// Canonical source tip represented by the wallet projection.
    pub source_tip: StorageLifecycleBlockId,
    /// Canonical source event represented by the wallet projection.
    pub source_event_sequence: u64,
    /// Ordered canonical sequence digest represented by the wallet projection.
    pub source_sequence_digest: CanonicalBlockFactsSequenceDigestSummary,
    /// Digest of every durable wallet row.
    pub wallet_projection_digest_hex: String,
    /// Exact durable row counts by wallet family.
    pub row_counts: WalletStorageRowCounts,
    /// Current transparent UTXO aggregate.
    pub utxo_summary: WalletStorageUtxoSummary,
    /// Canonical blocks consumed by the wallet builder.
    pub scanned_block_count: u64,
    /// Canonical transactions consumed by the wallet builder.
    pub scanned_transaction_count: u64,
    /// Historical canonical prevout reads; version 1 requires zero.
    pub historical_prevout_read_count: u64,
    /// External sorting and SST construction evidence.
    pub construction: WalletStorageConstructionEvidence,
    /// Detailed public builder phase durations.
    pub phase_durations: WalletStoragePhaseDurations,
    /// Non-overlapping builder stages, in execution order.
    pub build_stages: Vec<WalletBuildStageSeconds>,
    /// Final bytes owned below the wallet store path.
    pub physical_store_bytes: u64,
    /// Whether a new process-equivalent open admitted the exact READY evidence.
    pub cold_reopen_evidence_match: bool,
    /// Whether the admitted wallet source fence equals the admitted canonical READY fence.
    pub canonical_fence_match: bool,
}

/// Measurements assembled by the `RocksDB` fixed-tip lifecycle command.
#[derive(Clone, Debug)]
pub struct RocksDbStorageLifecycleMeasurements {
    /// Build and runner provenance.
    pub provenance: ReportProvenance,
    /// Frozen node source identity.
    pub source: StorageLifecycleSourceSummary,
    /// Fixed storage contracts.
    pub contracts: StorageLifecycleContractSummary,
    /// Exact resource ceilings.
    pub resource_limits: StorageLifecycleResourceLimits,
    /// Direct acceptance measurements.
    pub acceptance: RocksDbStorageLifecycleAcceptance,
    /// Phase-level duration evidence.
    pub phase_durations: StorageLifecyclePhaseDurations,
    /// Canonical READY evidence.
    pub canonical_storage_ready: CanonicalStorageReadySummary,
    /// Wallet READY evidence.
    pub wallet_storage_ready: WalletStorageReadySummary,
    /// Peak resident-set-size reading for the embedded lifecycle process.
    pub benchmark_client_peak_rss: PeakRss,
}

/// Complete `RocksDB` fixed-tip canonical and wallet storage certification.
#[derive(Clone, Debug, Serialize)]
pub struct RocksDbStorageLifecycleReport {
    /// Stable report contract identity.
    pub contract_identity: String,
    /// Machine-readable report schema version.
    pub report_format_version: u32,
    /// Build and runner provenance.
    pub provenance: ReportProvenance,
    /// Concrete embedded storage candidate.
    pub storage_candidate: StorageCandidateIdentity,
    /// Frozen node source identity.
    pub source: StorageLifecycleSourceSummary,
    /// Fixed storage contracts.
    pub contracts: StorageLifecycleContractSummary,
    /// Exact resource ceilings.
    pub resource_limits: StorageLifecycleResourceLimits,
    /// Acceptance for only the two storage readiness boundaries.
    pub acceptance: RocksDbStorageLifecycleAcceptance,
    /// Phase-level duration evidence.
    pub phase_durations: StorageLifecyclePhaseDurations,
    /// Canonical READY evidence.
    pub canonical_storage_ready: CanonicalStorageReadySummary,
    /// Wallet READY evidence.
    pub wallet_storage_ready: WalletStorageReadySummary,
    /// Peak resident-set-size reading for the embedded lifecycle process.
    pub benchmark_client_peak_rss: PeakRss,
}

/// Nearest-rank latency percentiles in milliseconds serialized into segmented reports.
#[derive(Clone, Debug, Serialize)]
pub struct PostgresSegmentedLatencySummary {
    /// Sample count represented by these percentiles.
    pub sample_count: u64,
    /// Median latency (`p50`) in milliseconds.
    pub p50_ms: f64,
    /// 95th-percentile latency (`p95`) in milliseconds.
    pub p95_ms: f64,
    /// 99th-percentile latency (`p99`) in milliseconds.
    pub p99_ms: f64,
    /// Maximum observed latency (`max`) in milliseconds.
    pub max_ms: f64,
}

impl From<&LatencyPercentilesMillis> for PostgresSegmentedLatencySummary {
    fn from(stats: &LatencyPercentilesMillis) -> Self {
        Self {
            sample_count: stats.sample_count,
            p50_ms: stats.p50_ms,
            p95_ms: stats.p95_ms,
            p99_ms: stats.p99_ms,
            max_ms: stats.max_ms,
        }
    }
}

/// Configuration parameters recorded in a segmented PostgreSQL benchmark report.
#[derive(Clone, Debug, Serialize)]
pub struct PostgresSegmentedConfigSummary {
    /// Isolated schema written and validated during the run.
    pub schema_name: String,
    /// Persisted segmented schema revision (`2`).
    pub schema_version: u16,
    /// Zinder-native network name (`zcash-mainnet`, `zcash-testnet`, `zcash-regtest`).
    pub network: String,
    /// Whether `ZSBLKB01` raw consensus block segments were retained.
    pub full_blocks: bool,
    /// Maximum allowed settled segment size in bytes (`<= 268435456`).
    pub max_segment_bytes: u64,
    /// Target block span per sealed settled segment batch.
    pub settled_segment_blocks: u32,
    /// Maximum rows retained in `canonical_live_tail`.
    pub live_tail_max_blocks: u32,
    /// Declared reader connection pooling mode (`direct` or `pgbouncer-transaction`).
    pub reader_mode: String,
    /// Number of concurrent readers active during contended tip-following.
    pub concurrent_readers: u32,
    /// Mempool churn entries admitted and evicted per tip block.
    pub mempool_churn_txs_per_block: u32,
    /// Count of blocks loaded via bulk `COPY`.
    pub bulk_block_count: u64,
    /// Count of blocks appended incrementally during tip-following phases.
    pub tip_follow_block_count: u64,
    /// Total committed blocks (`bulk_block_count + tip_follow_block_count`).
    pub total_block_count: u64,
}

/// Validation evidence recorded from the fresh read-only connection check.
#[derive(Clone, Debug, Serialize)]
pub struct PostgresSegmentedValidationSummary {
    /// First committed height observed in `schema_meta`.
    pub first_height: u32,
    /// Highest committed height observed at completion.
    pub tip_height: u32,
    /// Hex-encoded internal byte-order hash at `tip_height`.
    pub tip_hash_hex: String,
    /// Count of settled blocks reconstructed via `ZSHRNG02` + `ZSTSUP01`.
    pub settled_blocks_verified: u64,
    /// Count of live-tail blocks verified from `canonical_live_tail`.
    pub live_tail_blocks_verified: u64,
    /// Count of settled raw consensus blocks verified via `ZSBLKB01`.
    pub raw_blocks_verified: u64,
    /// Reconstructed ordered canonical sequence digest hex.
    pub persisted_sequence_digest_hex: String,
    /// Exact agreement with the fixture manifest digest oracle.
    pub fixture_sequence_digest_match: bool,
    /// Confirmation that validation ran over an independent fresh connection.
    pub fresh_reader_validated: bool,
}

/// Build and streaming timing/throughput summary for `postgres-segmented-storage`.
#[derive(Clone, Debug, Serialize)]
pub struct PostgresSegmentedTimingSummary {
    /// End-to-end wall-clock duration across all phases.
    pub wall_clock_seconds: f64,
    /// Fixture parse, canonical synthesis, and replay encoding duration.
    pub prepare_seconds: f64,
    /// Settled segment sealing duration.
    pub seal_seconds: f64,
    /// Binary `COPY` ingest duration.
    pub bulk_copy_seconds: f64,
    /// Primary key/index build and `ANALYZE` duration.
    pub finalize_seconds: f64,
    /// Uncontended tip-follow phase duration (`0` concurrent readers).
    pub uncontended_tip_follow_seconds: f64,
    /// Contended tip-follow phase duration (`N` concurrent readers).
    pub contended_tip_follow_seconds: f64,
    /// Fresh-connection validation duration.
    pub fresh_validation_seconds: f64,
    /// Relation/TOAST/index/WAL footprint query duration.
    pub footprint_measurement_seconds: f64,
    /// Bulk build rate in blocks per second over initial prefix.
    pub bulk_build_blocks_per_second: f64,
    /// Per-reader streaming throughput mean in blocks per second.
    pub streaming_blocks_per_second: f64,
}

/// Tip-follow commit latency, live-tail sealing, and mempool churn summary.
#[derive(Clone, Debug, Serialize)]
pub struct PostgresSegmentedTipFollowSummary {
    /// Per-block commit latency percentiles with `0` concurrent readers.
    pub uncontended_commit_latencies_ms: PostgresSegmentedLatencySummary,
    /// Per-block commit latency percentiles with concurrent readers running.
    pub contended_commit_latencies_ms: PostgresSegmentedLatencySummary,
    /// Number of runtime tail-to-settled segment seal transitions completed.
    pub tail_seal_transitions: u64,
    /// Cumulative `mempool_active_entry` admit operations.
    pub mempool_admits: u64,
    /// Cumulative `mempool_active_entry` eviction operations.
    pub mempool_evictions: u64,
}

/// Concurrent reader streaming throughput and transaction latency summary.
#[derive(Clone, Debug, Serialize)]
pub struct PostgresSegmentedReaderWorkloadSummary {
    /// Total completed `REPEATABLE READ READ ONLY` transactions across workers.
    pub completed_transactions: u64,
    /// Total settled compact-block ranges verified via `substring()`.
    pub settled_range_reads: u64,
    /// Total blocks decoded across settled ranges and live tail.
    pub blocks_decoded: u64,
    /// Total payload bytes transferred over reader queries.
    pub bytes_fetched: u64,
    /// Mean streaming rate per reader in blocks per second.
    pub per_reader_blocks_per_second_mean: f64,
    /// Mean streaming rate per reader in bytes per second.
    pub per_reader_bytes_per_second_mean: f64,
    /// Aggregate streaming rate across all concurrent readers in blocks per second.
    pub aggregate_blocks_per_second: f64,
    /// Aggregate streaming rate across all concurrent readers in bytes per second.
    pub aggregate_bytes_per_second: f64,
    /// Reader transaction latency percentiles in milliseconds.
    pub transaction_latencies_ms: PostgresSegmentedLatencySummary,
}

/// Non-double-counted per-table PostgreSQL relation footprint in report JSON.
#[derive(Clone, Debug, Serialize)]
pub struct PostgresSegmentedRelationFootprint {
    /// Table name inside `{schema}`.
    pub table_name: String,
    /// Exact table row count.
    pub row_count: u64,
    /// Main fork heap size in bytes.
    pub heap_bytes: u64,
    /// Dedicated TOAST table size in bytes.
    pub toast_bytes: u64,
    /// Secondary + PK index size in bytes.
    pub index_bytes: u64,
    /// Non-double-counted `pg_total_relation_size` in bytes.
    pub total_relation_bytes: u64,
    /// Whether this relation stores provisional wallet projection data.
    pub is_wallet_table: bool,
}

/// Schema-wide storage, TOAST, index, segment, and WAL footprint summary.
#[derive(Clone, Debug, Serialize)]
pub struct PostgresSegmentedFootprintSummary {
    /// Sum of `pg_total_relation_size` across canonical and operational tables.
    pub canonical_storage_bytes: u64,
    /// Sum of `pg_total_relation_size` across wallet projection tables.
    pub wallet_storage_bytes: u64,
    /// Total PostgreSQL storage bytes (`canonical_storage_bytes + wallet_storage_bytes`).
    pub total_storage_bytes: u64,
    /// Main heap fork bytes across all relations.
    pub total_heap_bytes: u64,
    /// Dedicated TOAST bytes across all relations.
    pub total_toast_bytes: u64,
    /// Index bytes across all relations.
    pub total_index_bytes: u64,
    /// WAL bytes emitted during bulk ingest (`pg_wal_lsn_diff`).
    pub bulk_wal_bytes: u64,
    /// WAL bytes emitted during tip-following (`pg_wal_lsn_diff`).
    pub tip_follow_wal_bytes: u64,
    /// Count of `shielded` settled segment rows.
    pub settled_shielded_segments: u64,
    /// Count of `supplement` settled segment rows.
    pub settled_supplement_segments: u64,
    /// Count of `raw_block` settled segment rows.
    pub settled_raw_block_segments: u64,
    /// Largest individual `segment_byte_len` persisted across all settled segments.
    pub max_segment_byte_len: u64,
    /// Final remaining row count in `canonical_live_tail`.
    pub live_tail_rows: u64,
    /// Individual relation footprints ordered by table name.
    pub relations: Vec<PostgresSegmentedRelationFootprint>,
}

/// Optional `RocksDB` comparison baseline parsed from `--rocksdb-baseline-report`.
#[derive(Clone, Debug, Default)]
pub struct PostgresSegmentedBaselineInput {
    /// Measured `RocksDB` build duration in seconds, when available.
    pub build_seconds: Option<f64>,
    /// Measured `RocksDB` physical store footprint in bytes, when available.
    pub storage_bytes: Option<u64>,
    /// Measured `RocksDB` per-reader streaming rate in blocks/s, when available.
    pub streaming_blocks_per_second: Option<f64>,
}

/// ADR-0004 Decision 7 diagnostic gate evaluation recorded in segmented reports.
#[allow(
    clippy::struct_excessive_bools,
    reason = "serialized ADR-0004 Decision 7 gate summary exposes explicit independent boolean flags"
)]
#[derive(Clone, Debug, Serialize)]
pub struct PostgresSegmentedGateEvaluation {
    /// Tip commit max latency remained under 2,000 ms (both uncontended and contended).
    pub tip_commit_latency_under_2s: bool,
    /// All settled segments stayed within `<= max_segment_bytes <= 256 MiB`.
    pub max_segment_bytes_respected: bool,
    /// Final `canonical_live_tail` row count stayed within `<= live_tail_max_blocks`.
    pub live_tail_bounded: bool,
    /// Concurrent reader pool completed without worker failures.
    pub concurrent_readers_zero_errors: bool,
    /// True when `--rocksdb-baseline-report` supplied comparison measurements.
    pub baseline_provided: bool,
    /// Baseline `RocksDB` build duration in seconds, if supplied.
    pub rocksdb_build_seconds: Option<f64>,
    /// Baseline `RocksDB` physical store bytes, if supplied.
    pub rocksdb_storage_bytes: Option<u64>,
    /// Baseline `RocksDB` streaming blocks/s, if supplied.
    pub rocksdb_streaming_blocks_per_second: Option<f64>,
    /// Build duration ratio (`postgres_build / rocksdb_build`, target `<= 4.0`).
    pub build_time_ratio_vs_rocksdb: Option<f64>,
    /// Storage footprint ratio (`postgres_bytes / rocksdb_bytes`, target `< 2.0`).
    pub storage_size_ratio_vs_rocksdb: Option<f64>,
    /// Per-reader streaming throughput ratio (`postgres_rate / rocksdb_rate`, target `>= 0.5`).
    pub streaming_throughput_ratio_vs_rocksdb: Option<f64>,
    /// True when every standalone segmented invariant/gate passed.
    pub all_standalone_gates_passed: bool,
    /// True when every supplied baseline comparison ratio satisfied ADR-0004 Decision 7.
    pub all_baseline_gates_passed: Option<bool>,
}

/// Segmented `PostgreSQL` build, tip-following, mempool churn, and pooled-reader report.
#[derive(Clone, Debug, Serialize)]
pub struct PostgresSegmentedStorageReport {
    /// Stable report contract identity.
    pub contract_identity: String,
    /// Machine-readable report schema version.
    pub report_format_version: u32,
    /// Build and runner provenance.
    pub provenance: ReportProvenance,
    /// Storage engine identity (`postgres-segmented-storage`).
    pub storage_candidate: StorageCandidateIdentity,
    /// Container image and resource-partition evidence.
    pub runtime_evidence: PostgresBenchmarkRuntimeEvidence,
    /// Durable PostgreSQL server settings inspected at connection start.
    pub server_settings: PostgresServerSettings,
    /// Authenticated fixture summary and oracle digest.
    pub fixture: FixtureSummary,
    /// Benchmark run parameters.
    pub configuration: PostgresSegmentedConfigSummary,
    /// End-to-end validation outcome.
    pub validation: PostgresSegmentedValidationSummary,
    /// Phased wall-clock durations and build/streaming throughput.
    pub timings: PostgresSegmentedTimingSummary,
    /// Tip-follow latencies, tail sealing transitions, and mempool churn counts.
    pub tip_follow: PostgresSegmentedTipFollowSummary,
    /// Pooled concurrent reader workload throughput and latencies.
    pub reader_workload: PostgresSegmentedReaderWorkloadSummary,
    /// Non-double-counted relation/TOAST/index/WAL footprint.
    pub footprint: PostgresSegmentedFootprintSummary,
    /// ADR-0004 Decision 7 diagnostic evaluation.
    pub adr0004_gate_evaluation: PostgresSegmentedGateEvaluation,
    /// Benchmark client process peak RSS.
    pub benchmark_client_peak_rss: PeakRss,
}

/// Assembly inputs passed to [`build_postgres_segmented_storage_report`].
#[derive(Clone, Debug)]
pub struct PostgresSegmentedReportInputs {
    /// Completed benchmark domain output.
    pub run: PostgresSegmentedRunReport,
    /// Optional `RocksDB` baseline report comparison inputs.
    pub baseline: Option<PostgresSegmentedBaselineInput>,
    /// Runtime container resource partition and DB image evidence.
    pub runtime_evidence: PostgresBenchmarkRuntimeEvidence,
    /// Benchmark client process peak RSS.
    pub benchmark_client_peak_rss: PeakRss,
    /// Source revision supplied via CLI.
    pub software_revision: Option<String>,
    /// Trial identity shared across paired arms.
    pub trial_id: Option<String>,
    /// Controlled fixture-cache treatment.
    pub fixture_cache_policy: Option<FixtureCachePolicy>,
    /// Run start timestamp in Unix milliseconds.
    pub run_started_at_unix_millis: u64,
    /// Run completion timestamp in Unix milliseconds.
    pub run_completed_at_unix_millis: u64,
    /// Operator label for the complete benchmark arm.
    pub runner_id: Option<String>,
    /// Aggregate CPU limit for the complete arm.
    pub cpu_limit_cores: Option<f64>,
    /// Aggregate memory limit for the complete arm.
    pub memory_limit_bytes: Option<u64>,
    /// Operator storage performance class label.
    pub storage_class: Option<String>,
    /// Immutable container image reference for the client binary.
    pub image_reference: Option<String>,
}

/// One versioned benchmark report with a closed, candidate-honest measurement
/// shape.
///
/// The tagged variants deliberately prevent one measurement from serializing
/// another measurement's acceptance or telemetry fields.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "measurement_kind", rename_all = "kebab-case")]
pub enum BenchmarkReport {
    /// Persisted round trip of block-local canonical replay records only.
    CanonicalReplayStorage(Box<CanonicalReplayStorageReport>),
    /// Authenticated checkpointed fixture replay into canonical `RocksDB`.
    #[serde(rename = "rocksdb-canonical-fixture-replay")]
    RocksDbCanonicalFixtureReplay(Box<RocksDbCanonicalFixtureReplayReport>),
    /// Fixed-tip canonical and wallet `RocksDB` storage lifecycle.
    #[serde(rename = "rocksdb-storage-lifecycle")]
    RocksDbStorageLifecycle(Box<RocksDbStorageLifecycleReport>),
    /// Segmented `PostgreSQL` build, tip-follow, mempool churn, and pooled-reader run.
    #[serde(rename = "postgres-segmented-storage")]
    PostgresSegmentedStorage(Box<PostgresSegmentedStorageReport>),
}

impl From<CanonicalReplayStorageReport> for BenchmarkReport {
    fn from(report: CanonicalReplayStorageReport) -> Self {
        Self::CanonicalReplayStorage(Box::new(report))
    }
}

impl From<RocksDbStorageLifecycleReport> for BenchmarkReport {
    fn from(report: RocksDbStorageLifecycleReport) -> Self {
        Self::RocksDbStorageLifecycle(Box::new(report))
    }
}

impl From<RocksDbCanonicalFixtureReplayReport> for BenchmarkReport {
    fn from(report: RocksDbCanonicalFixtureReplayReport) -> Self {
        Self::RocksDbCanonicalFixtureReplay(Box::new(report))
    }
}

impl From<PostgresSegmentedStorageReport> for BenchmarkReport {
    fn from(report: PostgresSegmentedStorageReport) -> Self {
        Self::PostgresSegmentedStorage(Box::new(report))
    }
}

impl BenchmarkReport {
    /// Validates the evidence boundary owned by the selected measurement.
    pub fn validate(&self) -> Result<(), BenchError> {
        self.validate_contract_identity()?;
        match self {
            Self::CanonicalReplayStorage(report) => {
                if report.round_trip.replay_format_version
                    != CanonicalBlockReplayFormatVersion::CURRENT.value()
                    || !report.round_trip.semantic_replay_validated
                {
                    return Err(BenchError::canonical_replay_storage_sequence_mismatch(
                        "persisted canonical replay records lack complete semantic replay validation",
                    ));
                }
                if !report.round_trip.fixture_sequence_digest_match {
                    return Err(BenchError::canonical_replay_storage_sequence_mismatch(
                        "persisted sequence digest does not match the fixture capture oracle",
                    ));
                }
                if report.round_trip.block_count != u64::from(report.fixture.block_count)
                    || report.round_trip.first_height != report.fixture.from_height
                    || report.round_trip.tip_height != report.fixture.to_height
                    || report.round_trip.tip_hash_hex != report.fixture.tip_hash_hex
                {
                    return Err(BenchError::canonical_replay_storage_sequence_mismatch(
                        format!(
                            "expected {} blocks over heights {}..={} ending at hash {}, observed {} blocks over heights {}..={} ending at hash {}",
                            report.fixture.block_count,
                            report.fixture.from_height,
                            report.fixture.to_height,
                            report.fixture.tip_hash_hex,
                            report.round_trip.block_count,
                            report.round_trip.first_height,
                            report.round_trip.tip_height,
                            report.round_trip.tip_hash_hex
                        ),
                    ));
                }
                Ok(())
            }
            Self::RocksDbCanonicalFixtureReplay(report) => report.validate_evidence(),
            Self::RocksDbStorageLifecycle(report) => report.validate_acceptance(),
            Self::PostgresSegmentedStorage(report) => report.validate_segmented(),
        }
    }

    fn validate_contract_identity(&self) -> Result<(), BenchError> {
        let (contract_identity, report_format_version, expected_report_format_version) = match self
        {
            Self::CanonicalReplayStorage(report) => (
                report.contract_identity.as_str(),
                report.report_format_version,
                REPORT_FORMAT_VERSION,
            ),
            Self::RocksDbCanonicalFixtureReplay(report) => (
                report.contract_identity.as_str(),
                report.report_format_version,
                ROCKSDB_CANONICAL_FIXTURE_REPLAY_REPORT_FORMAT_VERSION,
            ),
            Self::RocksDbStorageLifecycle(report) => (
                report.contract_identity.as_str(),
                report.report_format_version,
                ROCKSDB_STORAGE_LIFECYCLE_REPORT_FORMAT_VERSION,
            ),
            Self::PostgresSegmentedStorage(report) => (
                report.contract_identity.as_str(),
                report.report_format_version,
                POSTGRES_SEGMENTED_REPORT_FORMAT_VERSION,
            ),
        };
        if contract_identity != REPORT_CONTRACT_IDENTITY {
            return Err(BenchError::report_format(format!(
                "report contract identity {contract_identity:?} does not match {REPORT_CONTRACT_IDENTITY:?}"
            )));
        }
        if report_format_version != expected_report_format_version {
            return Err(BenchError::report_format(format!(
                "report format version {report_format_version} does not match {expected_report_format_version}"
            )));
        }
        let fixture = match self {
            Self::CanonicalReplayStorage(report) => Some(&report.fixture),
            Self::RocksDbCanonicalFixtureReplay(report) => Some(&report.fixture),
            Self::PostgresSegmentedStorage(report) => Some(&report.fixture),
            Self::RocksDbStorageLifecycle(_) => None,
        };
        if let Some(fixture) = fixture {
            if fixture.contract_identity != FIXTURE_CONTRACT_IDENTITY {
                return Err(BenchError::report_format(format!(
                    "fixture contract identity {:?} does not match {FIXTURE_CONTRACT_IDENTITY:?}",
                    fixture.contract_identity
                )));
            }
            if fixture.fixture_format_version != FIXTURE_FORMAT_VERSION {
                return Err(BenchError::report_format(format!(
                    "fixture format version {} does not match {FIXTURE_FORMAT_VERSION}",
                    fixture.fixture_format_version
                )));
            }
        }
        Ok(())
    }

    /// Returns the canonical replay storage measurement when present.
    #[must_use]
    pub const fn canonical_replay_storage(&self) -> Option<&CanonicalReplayStorageReport> {
        match self {
            Self::CanonicalReplayStorage(report) => Some(report),
            Self::RocksDbCanonicalFixtureReplay(_)
            | Self::RocksDbStorageLifecycle(_)
            | Self::PostgresSegmentedStorage(_) => None,
        }
    }

    /// Returns the `RocksDB` storage lifecycle measurement when present.
    #[must_use]
    pub const fn rocksdb_storage_lifecycle(&self) -> Option<&RocksDbStorageLifecycleReport> {
        match self {
            Self::RocksDbStorageLifecycle(report) => Some(report),
            Self::CanonicalReplayStorage(_)
            | Self::RocksDbCanonicalFixtureReplay(_)
            | Self::PostgresSegmentedStorage(_) => None,
        }
    }

    /// Returns the segmented `PostgreSQL` measurement when present.
    #[must_use]
    pub const fn postgres_segmented_storage(&self) -> Option<&PostgresSegmentedStorageReport> {
        match self {
            Self::PostgresSegmentedStorage(report) => Some(report),
            Self::CanonicalReplayStorage(_)
            | Self::RocksDbCanonicalFixtureReplay(_)
            | Self::RocksDbStorageLifecycle(_) => None,
        }
    }
}

impl PostgresSegmentedStorageReport {
    /// Validates every segmented PostgreSQL invariant, durable server posture, and
    /// non-double-counted storage metric.
    pub fn validate_segmented(&self) -> Result<(), BenchError> {
        self.validate_server_and_config()?;
        self.validate_validation_and_footprint()?;
        self.validate_timings_and_rates()
    }

    fn validate_server_and_config(&self) -> Result<(), BenchError> {
        if !self.server_settings.fsync
            || !self.server_settings.full_page_writes
            || self.server_settings.synchronous_commit != "on"
            || (self.server_settings.wal_level != "replica"
                && self.server_settings.wal_level != "logical")
        {
            return Err(BenchError::report_format(
                "postgres-segmented requires durable server posture (fsync, full_page_writes, synchronous_commit=on, replica/logical wal_level)",
            ));
        }
        if self.configuration.schema_version != 2 {
            return Err(BenchError::report_format(format!(
                "postgres-segmented schema_version {} must equal 2",
                self.configuration.schema_version
            )));
        }
        if self.configuration.max_segment_bytes == 0
            || self.configuration.max_segment_bytes > MAX_SETTLED_SEGMENT_BYTES
        {
            return Err(BenchError::report_format(
                "postgres-segmented max_segment_bytes must be within 1..=256 MiB",
            ));
        }
        if self.configuration.total_block_count != u64::from(self.fixture.block_count)
            || self
                .configuration
                .bulk_block_count
                .saturating_add(self.configuration.tip_follow_block_count)
                != self.configuration.total_block_count
        {
            return Err(BenchError::report_format(
                "postgres-segmented block count breakdown disagrees with fixture.block_count",
            ));
        }
        Ok(())
    }

    fn validate_validation_and_footprint(&self) -> Result<(), BenchError> {
        if !self.validation.fixture_sequence_digest_match || !self.validation.fresh_reader_validated
        {
            return Err(BenchError::canonical_replay_storage_sequence_mismatch(
                "postgres-segmented report lacks validated fresh-reader digest parity",
            ));
        }
        if self.validation.first_height != self.fixture.from_height
            || self.validation.tip_height != self.fixture.to_height
            || self.validation.tip_hash_hex != self.fixture.tip_hash_hex
        {
            return Err(BenchError::canonical_replay_storage_sequence_mismatch(
                "postgres-segmented validated heights/hash disagree with fixture",
            ));
        }
        if self.footprint.max_segment_byte_len > self.configuration.max_segment_bytes {
            return Err(BenchError::acceptance_hard_limit_missed(
                "max_segment_byte_len exceeded configured max_segment_bytes",
            ));
        }
        if self.footprint.live_tail_rows > u64::from(self.configuration.live_tail_max_blocks) {
            return Err(BenchError::acceptance_hard_limit_missed(
                "canonical_live_tail exceeded configured live_tail_max_blocks",
            ));
        }
        if self
            .footprint
            .canonical_storage_bytes
            .checked_add(self.footprint.wallet_storage_bytes)
            != Some(self.footprint.total_storage_bytes)
        {
            return Err(BenchError::report_format(
                "total_storage_bytes does not equal canonical_storage_bytes + wallet_storage_bytes",
            ));
        }
        Ok(())
    }

    fn validate_timings_and_rates(&self) -> Result<(), BenchError> {
        let numeric_metrics = [
            ("wall_clock_seconds", self.timings.wall_clock_seconds),
            ("prepare_seconds", self.timings.prepare_seconds),
            ("seal_seconds", self.timings.seal_seconds),
            ("bulk_copy_seconds", self.timings.bulk_copy_seconds),
            ("finalize_seconds", self.timings.finalize_seconds),
            (
                "uncontended_tip_follow_seconds",
                self.timings.uncontended_tip_follow_seconds,
            ),
            (
                "contended_tip_follow_seconds",
                self.timings.contended_tip_follow_seconds,
            ),
            (
                "fresh_validation_seconds",
                self.timings.fresh_validation_seconds,
            ),
            (
                "footprint_measurement_seconds",
                self.timings.footprint_measurement_seconds,
            ),
            (
                "bulk_build_blocks_per_second",
                self.timings.bulk_build_blocks_per_second,
            ),
            (
                "streaming_blocks_per_second",
                self.timings.streaming_blocks_per_second,
            ),
            (
                "per_reader_blocks_per_second_mean",
                self.reader_workload.per_reader_blocks_per_second_mean,
            ),
            (
                "per_reader_bytes_per_second_mean",
                self.reader_workload.per_reader_bytes_per_second_mean,
            ),
            (
                "aggregate_blocks_per_second",
                self.reader_workload.aggregate_blocks_per_second,
            ),
            (
                "aggregate_bytes_per_second",
                self.reader_workload.aggregate_bytes_per_second,
            ),
        ];
        for (name, val) in numeric_metrics {
            if !val.is_finite() || val < 0.0 {
                return Err(BenchError::report_format(format!(
                    "postgres-segmented metric `{name}` must be finite and non-negative"
                )));
            }
        }
        for (label, lat) in [
            (
                "uncontended_commit_latencies_ms",
                &self.tip_follow.uncontended_commit_latencies_ms,
            ),
            (
                "contended_commit_latencies_ms",
                &self.tip_follow.contended_commit_latencies_ms,
            ),
            (
                "reader_transaction_latencies_ms",
                &self.reader_workload.transaction_latencies_ms,
            ),
        ] {
            for stat in [lat.p50_ms, lat.p95_ms, lat.p99_ms, lat.max_ms] {
                if !stat.is_finite() || stat < 0.0 {
                    return Err(BenchError::report_format(format!(
                        "postgres-segmented latency `{label}` must be finite and non-negative"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Builds a closed [`BenchmarkReport::PostgresSegmentedStorage`] report from a
/// completed segmented benchmark run and optional `RocksDB` comparison baseline.
#[must_use]
pub fn build_postgres_segmented_storage_report(
    fixture: FixtureSummary,
    inputs: PostgresSegmentedReportInputs,
) -> BenchmarkReport {
    let provenance = build_segmented_provenance(&inputs);
    let run = inputs.run;
    let tip_hash_hex = hex::encode(zinder_core::wire::encode_internal_block_hash(run.tip_hash));
    let fixture_sequence_digest_match = run.persisted_sequence_digest_hex
        == fixture
            .canonical_block_facts_digest_evidence
            .sequence_digest_sha256;
    let build_seconds = run.timings.prepare_seconds
        + run.timings.seal_seconds
        + run.timings.bulk_copy_seconds
        + run.timings.finalize_seconds;
    let bulk_build_blocks_per_second = if build_seconds > 0.0 {
        u64_to_f64(run.bulk_block_count) / build_seconds
    } else {
        0.0
    };
    let streaming_blocks_per_second = run.reader_workload.per_reader_blocks_per_second_mean;
    let adr0004_gate_evaluation = evaluate_adr0004_gates(
        &run,
        build_seconds,
        streaming_blocks_per_second,
        inputs.baseline.as_ref(),
    );

    let configuration = build_segmented_config_summary(&run);
    let validation =
        build_segmented_validation_summary(&run, tip_hash_hex, fixture_sequence_digest_match);
    let timings = build_segmented_timing_summary(
        &run,
        bulk_build_blocks_per_second,
        streaming_blocks_per_second,
    );
    let tip_follow = build_segmented_tip_follow_summary(&run);
    let reader_workload = build_segmented_reader_summary(&run);
    let footprint = build_segmented_footprint_summary(run.footprint);

    BenchmarkReport::PostgresSegmentedStorage(Box::new(PostgresSegmentedStorageReport {
        contract_identity: REPORT_CONTRACT_IDENTITY.to_owned(),
        report_format_version: POSTGRES_SEGMENTED_REPORT_FORMAT_VERSION,
        provenance,
        storage_candidate: StorageCandidateIdentity::postgres_segmented_storage(),
        runtime_evidence: inputs.runtime_evidence,
        server_settings: run.server_settings,
        fixture,
        configuration,
        validation,
        timings,
        tip_follow,
        reader_workload,
        footprint,
        adr0004_gate_evaluation,
        benchmark_client_peak_rss: inputs.benchmark_client_peak_rss,
    }))
}

fn build_segmented_provenance(inputs: &PostgresSegmentedReportInputs) -> ReportProvenance {
    ReportProvenance {
        benchmark_version: env!("CARGO_PKG_VERSION"),
        software_revision: inputs.software_revision.clone(),
        run: BenchmarkRunProvenance {
            trial_id: inputs.trial_id.clone(),
            fixture_cache_policy: inputs.fixture_cache_policy,
            started_at_unix_millis: inputs.run_started_at_unix_millis,
            completed_at_unix_millis: inputs.run_completed_at_unix_millis,
        },
        runner: RunnerProvenance {
            id: inputs.runner_id.clone(),
            cpu_limit_cores: inputs.cpu_limit_cores,
            memory_limit_bytes: inputs.memory_limit_bytes,
            storage_class: inputs.storage_class.clone(),
        },
        image_reference: inputs.image_reference.clone(),
        target_os: std::env::consts::OS,
        target_arch: std::env::consts::ARCH,
    }
}

fn build_segmented_config_summary(
    run: &PostgresSegmentedRunReport,
) -> PostgresSegmentedConfigSummary {
    PostgresSegmentedConfigSummary {
        schema_name: run.schema_name.clone(),
        schema_version: run.schema_version,
        network: encode_zinder_native_chain_name(run.network).to_owned(),
        full_blocks: run.full_blocks,
        max_segment_bytes: run.max_segment_bytes,
        settled_segment_blocks: run.settled_segment_blocks,
        live_tail_max_blocks: run.live_tail_max_blocks,
        reader_mode: run.reader_mode.as_str().to_owned(),
        concurrent_readers: run.concurrent_readers,
        mempool_churn_txs_per_block: run.mempool_churn_txs_per_block,
        bulk_block_count: run.bulk_block_count,
        tip_follow_block_count: run.tip_follow_block_count,
        total_block_count: run.total_block_count,
    }
}

fn build_segmented_validation_summary(
    run: &PostgresSegmentedRunReport,
    tip_hash_hex: String,
    fixture_sequence_digest_match: bool,
) -> PostgresSegmentedValidationSummary {
    PostgresSegmentedValidationSummary {
        first_height: run.first_height.value(),
        tip_height: run.tip_height.value(),
        tip_hash_hex,
        settled_blocks_verified: run.settled_blocks_verified,
        live_tail_blocks_verified: run.live_tail_blocks_verified,
        raw_blocks_verified: run.raw_blocks_verified,
        persisted_sequence_digest_hex: run.persisted_sequence_digest_hex.clone(),
        fixture_sequence_digest_match,
        fresh_reader_validated: true,
    }
}

fn build_segmented_timing_summary(
    run: &PostgresSegmentedRunReport,
    bulk_build_blocks_per_second: f64,
    streaming_blocks_per_second: f64,
) -> PostgresSegmentedTimingSummary {
    PostgresSegmentedTimingSummary {
        wall_clock_seconds: run.timings.wall_clock_seconds,
        prepare_seconds: run.timings.prepare_seconds,
        seal_seconds: run.timings.seal_seconds,
        bulk_copy_seconds: run.timings.bulk_copy_seconds,
        finalize_seconds: run.timings.finalize_seconds,
        uncontended_tip_follow_seconds: run.timings.uncontended_tip_follow_seconds,
        contended_tip_follow_seconds: run.timings.contended_tip_follow_seconds,
        fresh_validation_seconds: run.timings.fresh_validation_seconds,
        footprint_measurement_seconds: run.timings.footprint_measurement_seconds,
        bulk_build_blocks_per_second,
        streaming_blocks_per_second,
    }
}

fn build_segmented_tip_follow_summary(
    run: &PostgresSegmentedRunReport,
) -> PostgresSegmentedTipFollowSummary {
    PostgresSegmentedTipFollowSummary {
        uncontended_commit_latencies_ms: PostgresSegmentedLatencySummary::from(
            &run.tip_follow.uncontended_commit_latencies_ms,
        ),
        contended_commit_latencies_ms: PostgresSegmentedLatencySummary::from(
            &run.tip_follow.contended_commit_latencies_ms,
        ),
        tail_seal_transitions: run.tip_follow.tail_seal_transitions,
        mempool_admits: run.tip_follow.mempool_admits,
        mempool_evictions: run.tip_follow.mempool_evictions,
    }
}

fn build_segmented_reader_summary(
    run: &PostgresSegmentedRunReport,
) -> PostgresSegmentedReaderWorkloadSummary {
    PostgresSegmentedReaderWorkloadSummary {
        completed_transactions: run.reader_workload.completed_transactions,
        settled_range_reads: run.reader_workload.settled_range_reads,
        blocks_decoded: run.reader_workload.blocks_decoded,
        bytes_fetched: run.reader_workload.bytes_fetched,
        per_reader_blocks_per_second_mean: run.reader_workload.per_reader_blocks_per_second_mean,
        per_reader_bytes_per_second_mean: run.reader_workload.per_reader_bytes_per_second_mean,
        aggregate_blocks_per_second: run.reader_workload.aggregate_blocks_per_second,
        aggregate_bytes_per_second: run.reader_workload.aggregate_bytes_per_second,
        transaction_latencies_ms: PostgresSegmentedLatencySummary::from(&run.reader_latencies_ms),
    }
}

fn build_segmented_footprint_summary(
    footprint: crate::postgres_segmented::storage_footprint::StorageFootprintSummary,
) -> PostgresSegmentedFootprintSummary {
    PostgresSegmentedFootprintSummary {
        canonical_storage_bytes: footprint.canonical_storage_bytes,
        wallet_storage_bytes: footprint.wallet_storage_bytes,
        total_storage_bytes: footprint.total_storage_bytes,
        total_heap_bytes: footprint.total_heap_bytes,
        total_toast_bytes: footprint.total_toast_bytes,
        total_index_bytes: footprint.total_index_bytes,
        bulk_wal_bytes: footprint.bulk_wal_bytes,
        tip_follow_wal_bytes: footprint.tip_follow_wal_bytes,
        settled_shielded_segments: footprint.settled_shielded_segments,
        settled_supplement_segments: footprint.settled_supplement_segments,
        settled_raw_block_segments: footprint.settled_raw_block_segments,
        max_segment_byte_len: footprint.max_segment_byte_len,
        live_tail_rows: footprint.live_tail_rows,
        relations: footprint
            .relations
            .into_iter()
            .map(|rel| PostgresSegmentedRelationFootprint {
                table_name: rel.table_name,
                row_count: rel.row_count,
                heap_bytes: rel.heap_bytes,
                toast_bytes: rel.toast_bytes,
                index_bytes: rel.index_bytes,
                total_relation_bytes: rel.total_relation_bytes,
                is_wallet_table: rel.is_wallet_table,
            })
            .collect(),
    }
}

fn evaluate_adr0004_gates(
    run: &PostgresSegmentedRunReport,
    build_seconds: f64,
    streaming_blocks_per_second: f64,
    baseline: Option<&PostgresSegmentedBaselineInput>,
) -> PostgresSegmentedGateEvaluation {
    let tip_commit_latency_under_2s = run.tip_follow.uncontended_commit_latencies_ms.max_ms
        < 2_000.0
        && run.tip_follow.contended_commit_latencies_ms.max_ms < 2_000.0;
    let max_segment_bytes_respected = run.footprint.max_segment_byte_len <= run.max_segment_bytes
        && run.max_segment_bytes <= MAX_SETTLED_SEGMENT_BYTES;
    let live_tail_bounded = run.footprint.live_tail_rows <= u64::from(run.live_tail_max_blocks);
    let concurrent_readers_zero_errors =
        run.concurrent_readers == 0 || run.reader_workload.completed_transactions > 0;
    let all_standalone_gates_passed = tip_commit_latency_under_2s
        && max_segment_bytes_respected
        && live_tail_bounded
        && concurrent_readers_zero_errors;

    let (rocksdb_build_seconds, rocksdb_storage_bytes, rocksdb_streaming_blocks_per_second) =
        baseline.map_or((None, None, None), |base| {
            (
                base.build_seconds,
                base.storage_bytes,
                base.streaming_blocks_per_second,
            )
        });

    let build_time_ratio_vs_rocksdb = rocksdb_build_seconds
        .filter(|&b| b > 0.0)
        .map(|b| build_seconds / b);
    let storage_size_ratio_vs_rocksdb = rocksdb_storage_bytes
        .filter(|&b| b > 0)
        .map(|b| u64_to_f64(run.footprint.total_storage_bytes) / u64_to_f64(b));
    let streaming_throughput_ratio_vs_rocksdb = rocksdb_streaming_blocks_per_second
        .filter(|&b| b > 0.0)
        .map(|b| streaming_blocks_per_second / b);

    let baseline_provided = baseline.is_some();
    let all_baseline_gates_passed = baseline_provided.then(|| {
        all_standalone_gates_passed
            && build_time_ratio_vs_rocksdb.is_none_or(|r| r <= 4.0)
            && storage_size_ratio_vs_rocksdb.is_none_or(|r| r < 2.0)
            && streaming_throughput_ratio_vs_rocksdb.is_none_or(|r| r >= 0.5)
    });

    PostgresSegmentedGateEvaluation {
        tip_commit_latency_under_2s,
        max_segment_bytes_respected,
        live_tail_bounded,
        concurrent_readers_zero_errors,
        baseline_provided,
        rocksdb_build_seconds,
        rocksdb_storage_bytes,
        rocksdb_streaming_blocks_per_second,
        build_time_ratio_vs_rocksdb,
        storage_size_ratio_vs_rocksdb,
        streaming_throughput_ratio_vs_rocksdb,
        all_standalone_gates_passed,
        all_baseline_gates_passed,
    }
}

impl RocksDbCanonicalFixtureReplayReport {
    /// Validates authenticated checkpointed fixture replay without claiming live-source certification.
    pub fn validate_evidence(&self) -> Result<(), BenchError> {
        validate_canonical_fixture_report_identity_and_resources(self)?;
        validate_canonical_fixture_source_load(
            &self.fixture,
            &self.source_load,
            self.resource_limits.tree_state_checkpoint_coverage,
        )?;
        validate_canonical_fixture_ready(&self.source_load, &self.canonical_ready)?;
        validate_canonical_fixture_source_attribution(
            self.source_load.block_count,
            self.source_fetch_attribution,
        )?;
        validate_canonical_fixture_prohibited_reads(self.prohibited_reads)?;
        validate_canonical_fixture_publication_family_scans(
            &self.publication_proof_provenance,
            &self.publication_family_scans,
        )
    }
}

fn validate_canonical_fixture_prohibited_reads(
    prohibited_reads: Option<CanonicalProhibitedReadSummary>,
) -> Result<(), BenchError> {
    let prohibited_reads = prohibited_reads
        .ok_or_else(|| BenchError::acceptance_telemetry_missing("prohibited_reads"))?;
    if prohibited_reads.historical_prevout_read_count != 0
        || prohibited_reads.cross_block_wallet_read_count != 0
    {
        return Err(BenchError::acceptance_completion_mismatch(
            "canonical fixture replay crossed a prohibited read boundary",
        ));
    }
    Ok(())
}

fn validate_canonical_fixture_publication_family_scans(
    proof_provenance: &str,
    scans: &[CanonicalPublicationFamilyScanStat],
) -> Result<(), BenchError> {
    if proof_provenance == "trusted-fresh-writer" {
        if scans.is_empty() {
            return Ok(());
        }
        return Err(BenchError::acceptance_completion_mismatch(
            "trusted fresh-writer publication unexpectedly performed full family scans",
        ));
    }
    if proof_provenance != "cold-certification" || scans.is_empty() {
        return Err(BenchError::acceptance_completion_mismatch(
            "canonical fixture replay publication proof provenance is invalid",
        ));
    }
    if scans.iter().any(|scan| {
        scan.family.is_empty()
            || scan.scan_count == 0
            || !scan.scan_seconds.is_finite()
            || scan.scan_seconds < 0.0
    }) {
        return Err(BenchError::acceptance_completion_mismatch(
            "canonical fixture replay publication-family scan attribution is invalid",
        ));
    }
    Ok(())
}

fn validate_canonical_fixture_report_identity_and_resources(
    report: &RocksDbCanonicalFixtureReplayReport,
) -> Result<(), BenchError> {
    if report.storage_candidate.id != "rocksdb-canonical-fixture-replay"
        || report.storage_candidate.canonical_engine != "rocksdb"
        || report.storage_candidate.canonical_model != "canonical-replay-storage"
        || report.storage_candidate.topology != "rocksdb"
        || !is_lowercase_sha256(&report.replay_plan_digest_sha256)
        || report.replay_plan_fixture_manifest_sha256 != report.fixture.digest_sha256
        || !is_lowercase_sha256(&report.fixture.digest_sha256)
    {
        return Err(BenchError::report_format(
            "canonical fixture replay identity or replay-plan binding is invalid",
        ));
    }
    let limits = report.resource_limits;
    if limits.derived_for_cpu_limit_cores != CANONICAL_FIXTURE_REPLAY_PROFILE_CPU_CORES
        || limits.derived_for_memory_limit_bytes != CANONICAL_FIXTURE_REPLAY_PROFILE_MEMORY_BYTES
        || limits.request_timeout_seconds == 0
        || limits.max_response_bytes == 0
        || limits.source_segment_target_response_bytes == 0
        || limits.source_segment_target_response_bytes > limits.max_response_bytes
        || limits.source_segment_max_blocks == 0
        || limits.source_fetch_max_in_flight_requests == 0
        || limits.source_fetch_max_in_flight_bytes < limits.max_response_bytes
        || limits.block_prepare_concurrency == 0
        || limits.block_prepare_memory_watermark_bytes == 0
        || limits.tree_hashing_workers == 0
        || limits.supported_reorg_depth == 0
        || limits.canonical_rocksdb
            != RocksDbResourceBudgetSummary::from(RocksDbResourceBudget::canonical_writer_defaults())
        || !report.total_seconds.is_finite()
        || report.total_seconds <= 0.0
        || !report.blocks_per_second.is_finite()
        || report.blocks_per_second <= 0.0
        || report.physical_store_bytes == 0
    {
        return Err(BenchError::report_format(
            "canonical fixture replay resources or timing are invalid",
        ));
    }
    Ok(())
}

fn validate_canonical_fixture_source_load(
    fixture: &FixtureSummary,
    load: &RocksDbCanonicalFixtureSourceLoadSummary,
    tree_state_checkpoint_coverage: TreeStateCheckpointCoverage,
) -> Result<(), BenchError> {
    let expected_digest = &fixture.canonical_block_facts_digest_evidence;
    if load.first_block.height != fixture.from_height
        || load.tip.height != fixture.to_height
        || load.tip.hash_hex != fixture.tip_hash_hex
        || load.block_count != u64::from(fixture.block_count)
        || load.block_count != expected_digest.block_count
        || load.transaction_count != fixture.workload_density.transaction_count
        || load.sequence_digest.block_digest_version != expected_digest.block_digest_version
        || load.sequence_digest.sequence_digest_version != expected_digest.sequence_digest_version
        || load.sequence_digest.block_count != expected_digest.block_count
        || load.sequence_digest.sha256 != expected_digest.sequence_digest_sha256
        || load.replay_format_version != CanonicalBlockReplayFormatVersion::CURRENT.value()
    {
        return Err(BenchError::acceptance_completion_mismatch(
            "canonical fixture replay load range, tip, count, or digest differs from the fixture",
        ));
    }
    let expected_checkpoint_count = tree_state_checkpoint_coverage
        .expected_checkpoint_count(load.first_block.height, load.tip.height);
    if load.block_count == 0
        || load.block_header_count != load.block_count
        || load.block_hash_index_count != load.block_count
        || load.logical_replay_bytes == 0
        || (load.live_tail_block_count == 0 && load.settled_segment_index_count == 0)
        || load.live_tail_block_count > load.block_count
        || load.settled_segment_index_count
            > load.block_count.saturating_sub(load.live_tail_block_count)
        || (load.live_tail_block_count > 0 && load.live_tail_block_logical_bytes == 0)
        || (load.settled_segment_index_count > 0 && load.settled_segment_index_logical_bytes == 0)
        || (load.settled_segment_index_count == 0
            && (load.settled_segment_index_logical_bytes != 0
                || load.live_tail_block_count != load.block_count))
        || load.transaction_location_count != load.transaction_count
        || load.transaction_blob_count != 0
        || load.block_blob_count != 0
        || load.live_tail_raw_block_logical_bytes != 0
        || Some(load.tree_state_checkpoint_count) != expected_checkpoint_count
        || load.logical_bytes == 0
        || load.sst_file_bytes == 0
        || load.sst_file_count == 0
    {
        return Err(BenchError::acceptance_completion_mismatch(
            "canonical fixture replay block, SST, or family counts are incomplete",
        ));
    }
    Ok(())
}

fn validate_canonical_fixture_ready(
    load: &RocksDbCanonicalFixtureSourceLoadSummary,
    ready: &RocksDbCanonicalFixtureReadySummary,
) -> Result<(), BenchError> {
    let fence = &ready.event_fence;
    if ready.scope != "canonical-fixture-ready"
        || !ready.source_tip_checkpoint_authenticated
        || !ready.published_and_reopened_ready_match
        || !ready.reopened_ready_and_event_fence_match
        || ready.first_retained_block != load.first_block
        || ready.visible_tip != load.tip
        || ready.visible_block_count != load.block_count
        || ready.full_scan_block_count != load.block_count
        || ready.visible_epoch_id != 1
        || ready.visible_event_sequence != 1
        || ready.replay_format_version != load.replay_format_version
        || ready.sequence_digest != load.sequence_digest
        || ready.logical_replay_bytes == 0
        || fence.chain_epoch_id != ready.visible_epoch_id
        || fence.chain_event_sequence != ready.visible_event_sequence
        || fence.visible_tip != ready.visible_tip
        || fence.sequence_digest != ready.sequence_digest
        || ready.settled_tip.height < ready.first_retained_block.height
        || ready.settled_tip.height > ready.visible_tip.height
    {
        return Err(BenchError::acceptance_completion_mismatch(
            "canonical fixture replay READY, event fence, source-tip authentication, cold reopen, or full scan evidence does not match",
        ));
    }
    Ok(())
}

fn validate_canonical_fixture_source_attribution(
    block_count: u64,
    source_fetch: Option<SourceFetchAttributionSummary>,
) -> Result<(), BenchError> {
    let source_fetch = source_fetch
        .ok_or_else(|| BenchError::acceptance_telemetry_missing("source_fetch_attribution"))?;
    if source_fetch.completed_segment_request_count == 0
        || source_fetch.total_connected_blocks_returned < block_count
        || source_fetch.total_response_payload_bytes == 0
        || source_fetch.discarded_completed_response_bytes
            > source_fetch.total_response_payload_bytes
    {
        return Err(BenchError::acceptance_completion_mismatch(
            "canonical fixture replay source attribution does not cover the loaded fixture",
        ));
    }
    Ok(())
}

fn is_lowercase_sha256(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

impl RocksDbStorageLifecycleReport {
    /// Validates the exact storage fences and configured hard limits.
    #[allow(
        clippy::too_many_lines,
        reason = "the closed report contract validates every storage fence and resource ceiling together"
    )]
    pub fn validate_acceptance(&self) -> Result<(), BenchError> {
        let canonical = &self.canonical_storage_ready;
        let wallet = &self.wallet_storage_ready;
        if self.storage_candidate.id != "rocksdb-storage-lifecycle"
            || self.storage_candidate.canonical_engine != "rocksdb"
            || self.storage_candidate.topology != "rocksdb"
        {
            return Err(BenchError::report_format(
                "storage lifecycle candidate identity is not the RocksDB contract",
            ));
        }
        if self.contracts.canonical_store_identity != zinder_store::CANONICAL_STORE_IDENTITY
            || self.contracts.canonical_store_schema_version
                != zinder_store::CANONICAL_STORE_SCHEMA_VERSION
            || self.contracts.wallet_store_identity != "wallet"
            || self.contracts.wallet_store_schema_version
                != zinder_wallet_rocksdb::WALLET_ROCKSDB_SCHEMA_VERSION
            || self.contracts.wallet_projection_schema_version
                != zinder_wallet_projection::WALLET_PROJECTION_SCHEMA_VERSION
            || self.contracts.wallet_value_encoding_version
                != zinder_wallet_projection::WALLET_PROJECTION_VALUE_ENCODING_VERSION
        {
            return Err(BenchError::report_format(
                "storage lifecycle report does not carry the current fixed storage contracts",
            ));
        }
        if canonical.scope != "canonical-storage-ready"
            || wallet.scope != "wallet-storage-ready"
            || self.acceptance.canonical_storage_ready.scope != "canonical-storage-ready"
            || self.acceptance.wallet_storage_ready.scope != "wallet-storage-ready"
        {
            return Err(BenchError::report_format(
                "storage lifecycle report contains an unowned acceptance boundary",
            ));
        }
        if self.resource_limits.canonical_publication_mode == CanonicalPublicationMode::TrustedFresh
            && self.phase_durations.canonical_cold_validation_seconds != 0.0
        {
            return Err(BenchError::report_format(
                "trusted-fresh publication must report zero cold certification time",
            ));
        }
        let expected_checkpoint_count = self
            .resource_limits
            .tree_state_checkpoint_coverage
            .expected_checkpoint_count(
                canonical.first_retained_block.height,
                canonical.visible_tip.height,
            );
        if canonical.first_retained_block.height != 1
            || canonical.visible_epoch_id != 1
            || canonical.visible_event_sequence != 1
            || canonical.replay_format_version != 1
            || canonical.sequence_digest.block_digest_version != 1
            || canonical.sequence_digest.sequence_digest_version != 1
            || canonical.block_count == 0
            || canonical.block_count != canonical.sequence_digest.block_count
            || Some(canonical.tree_state_checkpoint_count) != expected_checkpoint_count
            || !canonical.source_tip_checkpoint_authenticated
            || !canonical.cold_reopen_evidence_match
        {
            return Err(BenchError::acceptance_completion_mismatch(
                "canonical storage did not prove a complete version-1 READY baseline",
            ));
        }
        if self.source.fixed_build_tip != canonical.visible_tip
            || self.source.fixed_build_tip.height == 0
            || self.source.network_upgrade_activations_fingerprint_version != 1
            || wallet.source_tip != canonical.visible_tip
            || wallet.source_epoch_id != canonical.visible_epoch_id
            || wallet.source_event_sequence != canonical.visible_event_sequence
            || wallet.source_sequence_digest.block_digest_version
                != canonical.sequence_digest.block_digest_version
            || wallet.source_sequence_digest.sequence_digest_version
                != canonical.sequence_digest.sequence_digest_version
            || wallet.source_sequence_digest.block_count != canonical.sequence_digest.block_count
            || wallet.source_sequence_digest.sha256 != canonical.sequence_digest.sha256
            || wallet.scanned_block_count != canonical.block_count
            || wallet.historical_prevout_read_count != 0
            || wallet.construction.cold_validation_random_read_count != 0
            || wallet.utxo_summary.utxo_count != wallet.row_counts.transparent_unspent_output_count
            || wallet.utxo_summary.commitment_scheme != "lthash16-chacha12"
            || !wallet.cold_reopen_evidence_match
            || !wallet.canonical_fence_match
        {
            return Err(BenchError::acceptance_completion_mismatch(
                "wallet storage READY evidence does not match the admitted canonical fence",
            ));
        }
        for (role, sort, expected_memory) in [
            (
                "outpoint build",
                wallet.construction.outpoint_sort,
                self.resource_limits.wallet_max_outpoint_sort_memory_bytes,
            ),
            (
                "address-index build",
                wallet.construction.address_index_sort,
                self.resource_limits
                    .wallet_max_secondary_sort_memory_bytes_per_sorter,
            ),
            (
                "address-transaction build",
                wallet.construction.address_transaction_sort,
                self.resource_limits
                    .wallet_max_secondary_sort_memory_bytes_per_sorter,
            ),
        ] {
            validate_wallet_sort_evidence(
                role,
                sort,
                expected_memory,
                self.resource_limits
                    .wallet_max_temporary_file_bytes_per_sorter,
            )?;
        }
        if wallet.construction.max_accounted_reorg_undo_bytes
            != self.resource_limits.wallet_max_accounted_reorg_undo_bytes
            || wallet.construction.peak_accounted_reorg_undo_bytes
                > wallet.construction.max_accounted_reorg_undo_bytes
            || wallet
                .construction
                .cold_validation_max_accounted_reorg_undo_bytes
                != self.resource_limits.wallet_max_accounted_reorg_undo_bytes
            || wallet
                .construction
                .cold_validation_peak_accounted_reorg_undo_bytes
                > wallet
                    .construction
                    .cold_validation_max_accounted_reorg_undo_bytes
        {
            return Err(BenchError::acceptance_completion_mismatch(
                "wallet reorg-undo construction exceeded or misstated its configured memory ceiling",
            ));
        }
        for (boundary, measurement) in [
            (
                "canonical_storage_ready",
                self.acceptance.canonical_storage_ready,
            ),
            ("wallet_storage_ready", self.acceptance.wallet_storage_ready),
        ] {
            if !measurement.wall_clock_seconds.is_finite() || measurement.wall_clock_seconds < 0.0 {
                return Err(BenchError::report_format(format!(
                    "{boundary} duration must be finite and non-negative"
                )));
            }
            if measurement
                .thresholds
                .is_some_and(|thresholds| !thresholds.hard_limit_met)
            {
                return Err(BenchError::acceptance_hard_limit_missed(boundary));
            }
        }
        Ok(())
    }
}

fn validate_wallet_sort_evidence(
    role: &str,
    evidence: WalletVariableValueSortEvidence,
    expected_memory_bytes: u64,
    expected_temporary_file_bytes: u64,
) -> Result<(), BenchError> {
    if evidence.max_accounted_sort_memory_bytes != expected_memory_bytes
        || evidence.peak_accounted_sort_memory_bytes > evidence.max_accounted_sort_memory_bytes
        || evidence.max_temporary_file_bytes != expected_temporary_file_bytes
        || evidence.peak_temporary_file_bytes > evidence.max_temporary_file_bytes
        || evidence.final_run_file_bytes > evidence.max_temporary_file_bytes
    {
        return Err(BenchError::acceptance_completion_mismatch(format!(
            "wallet {role} evidence exceeded or misstated its configured ceiling"
        )));
    }
    Ok(())
}

/// Builds the closed canonical checkpointed fixture replay report.
#[must_use]
pub fn build_rocksdb_canonical_fixture_replay_report(
    fixture: FixtureSummary,
    measurements: RocksDbCanonicalFixtureReplayMeasurements,
    exposition: Option<&str>,
) -> BenchmarkReport {
    let samples = exposition.map_or_default(parse_prometheus_samples);
    let block_count = measurements.source_load.block_count;
    let blocks_per_second = if measurements.total_seconds > 0.0 {
        u64_to_f64(block_count) / measurements.total_seconds
    } else {
        0.0
    };
    RocksDbCanonicalFixtureReplayReport {
        contract_identity: REPORT_CONTRACT_IDENTITY.to_owned(),
        report_format_version: ROCKSDB_CANONICAL_FIXTURE_REPLAY_REPORT_FORMAT_VERSION,
        provenance: ReportProvenance {
            benchmark_version: env!("CARGO_PKG_VERSION"),
            software_revision: None,
            run: BenchmarkRunProvenance {
                trial_id: None,
                fixture_cache_policy: None,
                started_at_unix_millis: measurements.run_started_at_unix_millis,
                completed_at_unix_millis: measurements.run_completed_at_unix_millis,
            },
            runner: RunnerProvenance {
                id: None,
                cpu_limit_cores: None,
                memory_limit_bytes: None,
                storage_class: None,
            },
            image_reference: None,
            target_os: std::env::consts::OS,
            target_arch: std::env::consts::ARCH,
        },
        fixture,
        replay_plan_fixture_manifest_sha256: measurements.replay_plan_fixture_manifest_sha256,
        replay_plan_digest_sha256: measurements.replay_plan_digest_sha256,
        storage_candidate: StorageCandidateIdentity::rocksdb_canonical_fixture_replay(),
        resource_limits: measurements.resource_limits,
        publication_proof_provenance: measurements.publication_proof_provenance.to_owned(),
        total_seconds: measurements.total_seconds,
        blocks_per_second,
        source_load: measurements.source_load,
        canonical_ready: measurements.canonical_ready,
        physical_store_bytes: measurements.physical_store_bytes,
        source_fetch_attribution: aggregate_source_fetch_attribution(
            &samples,
            measurements.total_seconds,
        ),
        prohibited_reads: aggregate_canonical_prohibited_reads(&samples),
        publication_family_scans: aggregate_canonical_publication_family_scans(&samples),
        head_of_line_wait: aggregate_head_of_line_wait(&samples),
        stage_durations: aggregate_stage_durations(&samples),
        benchmark_client_peak_rss: measurements.benchmark_client_peak_rss,
    }
    .into()
}

/// Builds the exact fixed-tip `RocksDB` storage lifecycle report.
#[must_use]
pub fn build_rocksdb_storage_lifecycle_report(
    measurements: RocksDbStorageLifecycleMeasurements,
) -> BenchmarkReport {
    RocksDbStorageLifecycleReport {
        contract_identity: REPORT_CONTRACT_IDENTITY.to_owned(),
        report_format_version: ROCKSDB_STORAGE_LIFECYCLE_REPORT_FORMAT_VERSION,
        provenance: measurements.provenance,
        storage_candidate: StorageCandidateIdentity::rocksdb_storage_lifecycle(),
        source: measurements.source,
        contracts: measurements.contracts,
        resource_limits: measurements.resource_limits,
        acceptance: measurements.acceptance,
        phase_durations: measurements.phase_durations,
        canonical_storage_ready: measurements.canonical_storage_ready,
        wallet_storage_ready: measurements.wallet_storage_ready,
        benchmark_client_peak_rss: measurements.benchmark_client_peak_rss,
    }
    .into()
}

/// Builds a block-local replay persisted round-trip report.
#[must_use]
#[allow(
    clippy::too_many_lines,
    reason = "the report builder keeps the versioned measurement-to-contract mapping explicit"
)]
pub fn build_canonical_replay_storage_report(
    fixture: FixtureSummary,
    measurements: CanonicalReplayStorageMeasurements,
) -> BenchmarkReport {
    let block_count = measurements.persisted_sequence_digest.block_count;
    let blocks_per_second = if measurements.wall_clock_seconds > 0.0 {
        u64_to_f64(block_count) / measurements.wall_clock_seconds
    } else {
        0.0
    };
    let fixture_sequence_digest_match = block_count == u64::from(fixture.block_count)
        && measurements.persisted_sequence_digest.block_digest_version
            == fixture
                .canonical_block_facts_digest_evidence
                .block_digest_version
        && measurements
            .persisted_sequence_digest
            .sequence_digest_version
            == fixture
                .canonical_block_facts_digest_evidence
                .sequence_digest_version
        && measurements.persisted_sequence_digest.sha256.as_str()
            == fixture
                .canonical_block_facts_digest_evidence
                .sequence_digest_sha256
                .as_str();
    let provenance = ReportProvenance {
        benchmark_version: env!("CARGO_PKG_VERSION"),
        software_revision: measurements.software_revision,
        run: BenchmarkRunProvenance {
            trial_id: measurements.trial_id,
            fixture_cache_policy: measurements.fixture_cache_policy,
            started_at_unix_millis: measurements.run_started_at_unix_millis,
            completed_at_unix_millis: measurements.run_completed_at_unix_millis,
        },
        runner: RunnerProvenance {
            id: measurements.runner_id,
            cpu_limit_cores: measurements.cpu_limit_cores,
            memory_limit_bytes: measurements.memory_limit_bytes,
            storage_class: measurements.storage_class,
        },
        image_reference: measurements.image_reference,
        target_os: std::env::consts::OS,
        target_arch: std::env::consts::ARCH,
    };
    let attributed_wall_clock_seconds = measurements.storage_initialization_wall_clock_seconds
        + measurements.replay_preparation_wall_clock_seconds
        + measurements.replay_persistence_wall_clock_seconds
        + measurements.index_construction_wall_clock_seconds
        + measurements.storage_optimization_wall_clock_seconds
        + measurements.validation_wall_clock_seconds
        + measurements.publication_wall_clock_seconds
        + measurements.fresh_reader_validation_wall_clock_seconds
        + measurements.storage_measurement_wall_clock_seconds;
    let round_trip = CanonicalReplayStorageSummary {
        scope: "block-local-canonical-replay",
        block_prepare_concurrency: measurements.block_prepare_concurrency,
        wall_clock_seconds: measurements.wall_clock_seconds,
        storage_initialization_wall_clock_seconds: measurements
            .storage_initialization_wall_clock_seconds,
        replay_preparation_wall_clock_seconds: measurements.replay_preparation_wall_clock_seconds,
        replay_persistence_wall_clock_seconds: measurements.replay_persistence_wall_clock_seconds,
        index_construction_wall_clock_seconds: measurements.index_construction_wall_clock_seconds,
        storage_optimization_wall_clock_seconds: measurements
            .storage_optimization_wall_clock_seconds,
        validation_wall_clock_seconds: measurements.validation_wall_clock_seconds,
        publication_wall_clock_seconds: measurements.publication_wall_clock_seconds,
        fresh_reader_validation_wall_clock_seconds: measurements
            .fresh_reader_validation_wall_clock_seconds,
        storage_measurement_wall_clock_seconds: measurements.storage_measurement_wall_clock_seconds,
        unattributed_wall_clock_seconds: (measurements.wall_clock_seconds
            - attributed_wall_clock_seconds)
            .max(0.0),
        first_height: measurements.first_height,
        first_hash_hex: measurements.first_hash_hex,
        tip_height: measurements.tip_height,
        tip_hash_hex: measurements.tip_hash_hex,
        block_count,
        blocks_per_second,
        logical_replay_bytes: measurements.logical_replay_bytes,
        physical_storage_bytes: measurements.physical_storage_bytes,
        persisted_sequence_digest: measurements.persisted_sequence_digest,
        fixture_sequence_digest_match,
        replay_format_version: measurements.replay_format_version,
        semantic_replay_validated: measurements.semantic_replay_validated,
        storage: measurements.storage,
        benchmark_client_peak_rss: measurements.benchmark_client_peak_rss,
    };
    CanonicalReplayStorageReport {
        contract_identity: REPORT_CONTRACT_IDENTITY.to_owned(),
        report_format_version: REPORT_FORMAT_VERSION,
        provenance,
        fixture,
        storage_candidate: measurements.storage_candidate,
        round_trip,
    }
    .into()
}

fn aggregate_source_fetch_attribution(
    samples: &[MetricSample],
    replay_wall_clock_seconds: f64,
) -> Option<SourceFetchAttributionSummary> {
    let mut completed_requests = samples
        .iter()
        .filter(|sample| {
            sample.name == SOURCE_REQUEST_TOTAL
                && sample.label("operation") == Some("fetch_chain_segment")
                && sample.label("status") == Some("ok")
        })
        .peekable();
    completed_requests.peek()?;
    let completed_segment_request_count = completed_requests
        .map(|sample| round_to_u64(sample.reading))
        .sum::<u64>();
    let cumulative_fetch_chain_segment_task_seconds = samples
        .iter()
        .filter(|sample| {
            sample.name == SOURCE_REQUEST_DURATION_SUM
                && sample.label("operation") == Some("fetch_chain_segment")
                && sample.label("status") == Some("ok")
        })
        .map(|sample| sample.reading)
        .sum();

    let total_connected_blocks_returned =
        round_to_u64(sum_by_name(samples, SOURCE_SEGMENT_CONNECTED_BLOCKS_TOTAL));
    let total_response_payload_bytes = round_to_u64(sum_by_name(
        samples,
        SOURCE_SEGMENT_RESPONSE_PAYLOAD_BYTES_SUM,
    ));
    let (source_watermark_blocked_count, source_watermark_blocks_per_second) =
        source_watermark_attribution(samples, replay_wall_clock_seconds);
    let (
        retained_completed_segment_count,
        retained_in_flight_segment_count,
        retained_completed_response_bytes,
    ) = source_retention_attribution(samples);
    let (completed_segment_requests_per_second, response_payload_bytes_per_second) =
        if replay_wall_clock_seconds > 0.0 {
            (
                u64_to_f64(completed_segment_request_count) / replay_wall_clock_seconds,
                u64_to_f64(total_response_payload_bytes) / replay_wall_clock_seconds,
            )
        } else {
            (0.0, 0.0)
        };

    Some(SourceFetchAttributionSummary {
        completed_segment_request_count,
        total_connected_blocks_returned,
        total_response_payload_bytes,
        completed_segment_requests_per_second,
        response_payload_bytes_per_second,
        cumulative_fetch_chain_segment_task_seconds,
        density_restart_count: 0,
        density_sizing_adjustment_count: sum_metric_by_label(
            samples,
            SOURCE_SEGMENT_SIZING_ADJUSTMENT_TOTAL,
            "reason",
            "density",
        ),
        response_too_large_restart_count: 0,
        discarded_completed_segment_count: round_to_u64(sum_by_name(
            samples,
            SOURCE_SEGMENT_DISJOINT_PREFETCH_DISCARDED_COMPLETED_SEGMENTS_TOTAL,
        )),
        discarded_in_flight_segment_count: round_to_u64(sum_by_name(
            samples,
            SOURCE_SEGMENT_DISJOINT_PREFETCH_DISCARDED_IN_FLIGHT_SEGMENTS_TOTAL,
        )),
        discarded_completed_response_bytes: round_to_u64(sum_by_name(
            samples,
            SOURCE_SEGMENT_DISJOINT_PREFETCH_DISCARDED_COMPLETED_RESPONSE_BYTES_TOTAL,
        )),
        retained_completed_segment_count,
        retained_in_flight_segment_count,
        retained_completed_response_bytes,
        source_watermark_blocked_count,
        source_watermark_blocks_per_second,
    })
}

fn source_retention_attribution(samples: &[MetricSample]) -> (u64, u64, u64) {
    (
        round_to_u64(sum_by_name(
            samples,
            SOURCE_SEGMENT_PREFETCH_RETAINED_COMPLETED_SEGMENTS_TOTAL,
        )),
        round_to_u64(sum_by_name(
            samples,
            SOURCE_SEGMENT_PREFETCH_RETAINED_IN_FLIGHT_SEGMENTS_TOTAL,
        )),
        round_to_u64(sum_by_name(
            samples,
            SOURCE_SEGMENT_PREFETCH_RETAINED_COMPLETED_RESPONSE_BYTES_TOTAL,
        )),
    )
}

fn source_watermark_attribution(
    samples: &[MetricSample],
    replay_wall_clock_seconds: f64,
) -> (u64, f64) {
    let blocked_count = sum_metric_by_label(
        samples,
        BULK_PIPELINE_WATERMARK_BLOCKED_TOTAL,
        "stage",
        "source_fetch",
    );
    let blocked_per_second = if replay_wall_clock_seconds > 0.0 {
        u64_to_f64(blocked_count) / replay_wall_clock_seconds
    } else {
        0.0
    };
    (blocked_count, blocked_per_second)
}

fn sum_metric_by_label(
    samples: &[MetricSample],
    metric_name: &str,
    label_name: &str,
    label_value: &str,
) -> u64 {
    samples
        .iter()
        .filter(|sample| {
            sample.name == metric_name && sample.label(label_name) == Some(label_value)
        })
        .map(|sample| round_to_u64(sample.reading))
        .sum()
}

fn summarize_acceptance_measurement(
    scope: &'static str,
    wall_clock_seconds: f64,
    thresholds: Option<AcceptanceThresholds>,
) -> AcceptanceMeasurementSummary {
    AcceptanceMeasurementSummary {
        scope,
        wall_clock_seconds,
        thresholds: thresholds.map(|thresholds| AcceptanceThresholdSummary {
            target_seconds: thresholds.target_seconds,
            hard_limit_seconds: thresholds.hard_limit_seconds,
            target_met: wall_clock_seconds <= thresholds.target_seconds,
            hard_limit_met: wall_clock_seconds <= thresholds.hard_limit_seconds,
        }),
    }
}

/// Evaluates the two acceptance boundaries owned by the storage lifecycle command.
#[must_use]
pub fn summarize_rocksdb_storage_lifecycle_acceptance(
    canonical_storage_ready_seconds: f64,
    canonical_thresholds: Option<AcceptanceThresholds>,
    wallet_storage_ready_seconds: f64,
    wallet_thresholds: Option<AcceptanceThresholds>,
) -> RocksDbStorageLifecycleAcceptance {
    RocksDbStorageLifecycleAcceptance {
        canonical_storage_ready: summarize_acceptance_measurement(
            "canonical-storage-ready",
            canonical_storage_ready_seconds,
            canonical_thresholds,
        ),
        wallet_storage_ready: summarize_acceptance_measurement(
            "wallet-storage-ready",
            wallet_storage_ready_seconds,
            wallet_thresholds,
        ),
    }
}

fn aggregate_head_of_line_wait(samples: &[MetricSample]) -> Vec<StageWaitStat> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut seconds: BTreeMap<String, f64> = BTreeMap::new();
    for sample in samples {
        let is_count = sample.name == HEAD_OF_LINE_WAIT_COUNT;
        let is_sum = sample.name == HEAD_OF_LINE_WAIT_SUM;
        if !is_count && !is_sum {
            continue;
        }
        let Some(stage) = sample.label("stage") else {
            continue;
        };
        if is_count {
            *counts.entry(stage.to_owned()).or_insert(0) += round_to_u64(sample.reading);
        } else {
            *seconds.entry(stage.to_owned()).or_insert(0.0) += sample.reading;
        }
    }
    counts
        .into_iter()
        .map(|(stage, wait_count)| {
            let wait_seconds = seconds.get(&stage).copied().unwrap_or(0.0);
            StageWaitStat {
                stage,
                wait_count,
                wait_seconds,
            }
        })
        .collect()
}

fn aggregate_stage_durations(samples: &[MetricSample]) -> Vec<StageDurationStat> {
    let metric_families = [
        (
            "canonical_block_construction",
            CANONICAL_BLOCK_CONSTRUCTION_STAGE_DURATION_COUNT,
            CANONICAL_BLOCK_CONSTRUCTION_STAGE_DURATION_SUM,
        ),
        (
            "canonical_block_load",
            CANONICAL_BLOCK_LOAD_STAGE_DURATION_COUNT,
            CANONICAL_BLOCK_LOAD_STAGE_DURATION_SUM,
        ),
        (
            "canonical_block_store_load",
            CANONICAL_BLOCK_STORE_LOAD_STAGE_DURATION_COUNT,
            CANONICAL_BLOCK_STORE_LOAD_STAGE_DURATION_SUM,
        ),
    ];
    let mut counts: BTreeMap<(String, String, String), u64> = BTreeMap::new();
    let mut seconds: BTreeMap<(String, String, String), f64> = BTreeMap::new();
    for sample in samples {
        let Some((family, is_count)) =
            metric_families
                .iter()
                .find_map(|(family, count_name, sum_name)| {
                    if sample.name == *count_name {
                        Some((*family, true))
                    } else if sample.name == *sum_name {
                        Some((*family, false))
                    } else {
                        None
                    }
                })
        else {
            continue;
        };
        // The ordered loader's stages record completed blocks only, so they
        // carry no status label.
        let Some(stage) = sample.label("stage") else {
            continue;
        };
        let status = sample.label("status").unwrap_or("ok");
        let key = (family.to_owned(), stage.to_owned(), status.to_owned());
        if is_count {
            *counts.entry(key).or_insert(0) += round_to_u64(sample.reading);
        } else {
            *seconds.entry(key).or_insert(0.0) += sample.reading;
        }
    }
    counts
        .into_iter()
        .map(|((family, stage, status), call_count)| {
            let task_seconds = seconds
                .get(&(family.clone(), stage.clone(), status.clone()))
                .copied()
                .unwrap_or(0.0);
            StageDurationStat {
                family,
                stage,
                status,
                call_count,
                task_seconds,
            }
        })
        .collect()
}

fn aggregate_canonical_prohibited_reads(
    samples: &[MetricSample],
) -> Option<CanonicalProhibitedReadSummary> {
    let historical_present = samples
        .iter()
        .any(|sample| sample.name == CANONICAL_HISTORICAL_PREVOUT_READS_TOTAL);
    let cross_block_present = samples
        .iter()
        .any(|sample| sample.name == CANONICAL_CROSS_BLOCK_WALLET_READS_TOTAL);
    if !historical_present || !cross_block_present {
        return None;
    }
    Some(CanonicalProhibitedReadSummary {
        historical_prevout_read_count: round_to_u64(sum_by_name(
            samples,
            CANONICAL_HISTORICAL_PREVOUT_READS_TOTAL,
        )),
        cross_block_wallet_read_count: round_to_u64(sum_by_name(
            samples,
            CANONICAL_CROSS_BLOCK_WALLET_READS_TOTAL,
        )),
    })
}

fn aggregate_canonical_publication_family_scans(
    samples: &[MetricSample],
) -> Vec<CanonicalPublicationFamilyScanStat> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut seconds: BTreeMap<String, f64> = BTreeMap::new();
    let mut rows: BTreeMap<String, u64> = BTreeMap::new();
    let mut logical_bytes: BTreeMap<String, u64> = BTreeMap::new();
    for sample in samples {
        let target = if sample.name == CANONICAL_PUBLICATION_FAMILY_SCAN_DURATION_COUNT {
            Some(0_u8)
        } else if sample.name == CANONICAL_PUBLICATION_FAMILY_SCAN_DURATION_SUM {
            Some(1)
        } else if sample.name == CANONICAL_PUBLICATION_FAMILY_SCAN_ROWS_TOTAL {
            Some(2)
        } else if sample.name == CANONICAL_PUBLICATION_FAMILY_SCAN_LOGICAL_BYTES_TOTAL {
            Some(3)
        } else {
            None
        };
        let (Some(target), Some(family)) = (target, sample.label("family")) else {
            continue;
        };
        match target {
            0 => *counts.entry(family.to_owned()).or_insert(0) += round_to_u64(sample.reading),
            1 => *seconds.entry(family.to_owned()).or_insert(0.0) += sample.reading,
            2 => *rows.entry(family.to_owned()).or_insert(0) += round_to_u64(sample.reading),
            _ => {
                *logical_bytes.entry(family.to_owned()).or_insert(0) +=
                    round_to_u64(sample.reading);
            }
        }
    }
    counts
        .into_iter()
        .map(|(family, scan_count)| CanonicalPublicationFamilyScanStat {
            scan_seconds: seconds.get(&family).copied().unwrap_or(0.0),
            row_count: rows.get(&family).copied().unwrap_or(0),
            logical_bytes: logical_bytes.get(&family).copied().unwrap_or(0),
            family,
            scan_count,
        })
        .collect()
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Prometheus counters are rendered as non-negative integers within u64 range"
)]
fn round_to_u64(reading: f64) -> u64 {
    if reading.is_finite() && reading >= 0.0 {
        reading.round() as u64
    } else {
        0
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "block counts fit well within f64 exact-integer range for a bounded benchmark range"
)]
fn u64_to_f64(amount: u64) -> f64 {
    amount as f64
}

#[cfg(test)]
mod tests {
    use crate::{fixture::WorkloadDensity, rss::PEAK_RSS_SOURCE_UNAVAILABLE};

    use super::{
        AcceptanceThresholds, CanonicalBlockFactsSequenceDigestSummary,
        CanonicalPublicationFamilyScanStat, CanonicalReplayStorageEvidence,
        CanonicalReplayStorageMeasurements, FixtureCachePolicy, FixtureSummary,
        RocksDbCanonicalFixtureEventFenceSummary, RocksDbCanonicalFixtureReadySummary,
        RocksDbCanonicalFixtureReplayMeasurements, RocksDbCanonicalFixtureReplayResourceLimits,
        RocksDbCanonicalFixtureSourceLoadSummary, RocksDbResourceBudgetSummary,
        StorageCandidateIdentity, StorageLifecycleBlockId, aggregate_source_fetch_attribution,
        aggregate_stage_durations, build_canonical_replay_storage_report,
        build_rocksdb_canonical_fixture_replay_report, is_valid_benchmark_trial_id,
        parse_prometheus_samples, summarize_rocksdb_storage_lifecycle_acceptance,
    };

    #[test]
    fn benchmark_trial_ids_are_filename_safe() {
        for trial_id in ["trial-01", "A.2_warm", "9"] {
            assert!(is_valid_benchmark_trial_id(trial_id), "{trial_id}");
        }
        for trial_id in ["", "-trial", "trial 01", "trial/01", "trialé"] {
            assert!(!is_valid_benchmark_trial_id(trial_id), "{trial_id}");
        }
    }

    #[test]
    fn source_fetch_attribution_aggregates_requests_sizing_and_discards()
    -> Result<(), crate::BenchError> {
        let exposition = "\
zinder_ingest_source_request_total{operation=\"fetch_chain_segment\",status=\"ok\",error_class=\"none\"} 4
zinder_ingest_source_request_duration_seconds_sum{operation=\"fetch_chain_segment\",status=\"ok\",error_class=\"none\"} 15.5
zinder_ingest_source_segment_connected_blocks_total 52
zinder_ingest_source_segment_response_payload_bytes_sum 120000000
zinder_ingest_source_segment_sizing_adjustment_total{reason=\"density\"} 7
zinder_ingest_source_segment_sizing_adjustment_total{reason=\"response_too_large\"} 2
zinder_ingest_source_segment_disjoint_prefetch_discarded_completed_segments_total{reason=\"response_too_large\"} 1
zinder_ingest_source_segment_disjoint_prefetch_discarded_in_flight_segments_total{reason=\"response_too_large\"} 4
zinder_ingest_source_segment_disjoint_prefetch_discarded_completed_response_bytes_total{reason=\"response_too_large\"} 10000000
";

        let source_fetch =
            aggregate_source_fetch_attribution(&parse_prometheus_samples(exposition), 12.5)
                .ok_or_else(|| {
                    crate::BenchError::invalid_argument(
                        "source request telemetry should produce attribution",
                    )
                })?;
        assert_eq!(source_fetch.completed_segment_request_count, 4);
        assert_eq!(source_fetch.total_connected_blocks_returned, 52);
        assert_eq!(source_fetch.total_response_payload_bytes, 120_000_000);
        assert!((source_fetch.completed_segment_requests_per_second - 0.32).abs() < f64::EPSILON);
        assert!(
            (source_fetch.response_payload_bytes_per_second - 9_600_000.0).abs() < f64::EPSILON
        );
        assert!(
            (source_fetch.cumulative_fetch_chain_segment_task_seconds - 15.5).abs() < f64::EPSILON
        );
        assert_eq!(source_fetch.density_sizing_adjustment_count, 7);
        assert_eq!(source_fetch.density_restart_count, 0);
        assert_eq!(source_fetch.response_too_large_restart_count, 0);
        assert_eq!(source_fetch.discarded_completed_segment_count, 1);
        assert_eq!(source_fetch.discarded_in_flight_segment_count, 4);
        assert_eq!(source_fetch.discarded_completed_response_bytes, 10_000_000);
        Ok(())
    }

    #[test]
    fn rocksdb_canonical_fixture_report_serializes_and_validates_its_own_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let report = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            Some(canonical_fixture_source_exposition()),
        );

        report.validate()?;
        let encoded = serde_json::to_value(&report)?;
        assert_eq!(encoded["report_format_version"], 1);
        assert_eq!(
            encoded["measurement_kind"],
            "rocksdb-canonical-fixture-replay"
        );
        assert_eq!(
            encoded["storage_candidate"]["id"],
            "rocksdb-canonical-fixture-replay"
        );
        assert_eq!(
            encoded["resource_limits"]["tree_state_checkpoint_coverage"],
            "every-block"
        );
        assert_eq!(encoded["source_load"]["tree_state_checkpoint_count"], 11);
        assert!(encoded.get("wallet_storage_ready").is_none());
        assert!(encoded.get("acceptance").is_none());
        assert!(encoded.get("replay").is_none());
        Ok(())
    }

    #[test]
    fn tree_state_checkpoint_coverage_expected_counts_are_exact() {
        use super::TreeStateCheckpointCoverage::{CadenceAndTip, EveryBlock};

        assert_eq!(EveryBlock.expected_checkpoint_count(0, 10), None);
        assert_eq!(CadenceAndTip.expected_checkpoint_count(0, 10), None);
        assert_eq!(EveryBlock.expected_checkpoint_count(10, 9), None);
        assert_eq!(CadenceAndTip.expected_checkpoint_count(10, 9), None);

        assert_eq!(EveryBlock.expected_checkpoint_count(603, 603), Some(2));
        assert_eq!(CadenceAndTip.expected_checkpoint_count(603, 603), Some(2));

        assert_eq!(EveryBlock.expected_checkpoint_count(1, 5), Some(6));
        assert_eq!(CadenceAndTip.expected_checkpoint_count(1, 5), Some(2));

        assert_eq!(EveryBlock.expected_checkpoint_count(1, 100), Some(101));
        assert_eq!(CadenceAndTip.expected_checkpoint_count(1, 100), Some(2));

        assert_eq!(EveryBlock.expected_checkpoint_count(1, 105), Some(106));
        assert_eq!(CadenceAndTip.expected_checkpoint_count(1, 105), Some(3));

        assert_eq!(EveryBlock.expected_checkpoint_count(100, 205), Some(107));
        assert_eq!(CadenceAndTip.expected_checkpoint_count(100, 205), Some(4));

        assert_eq!(EveryBlock.expected_checkpoint_count(101, 200), Some(101));
        assert_eq!(CadenceAndTip.expected_checkpoint_count(101, 200), Some(2));
    }

    #[test]
    fn rocksdb_canonical_fixture_report_validates_declared_coverage_and_rejects_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut sparse_measurements = rocksdb_canonical_fixture_measurements();
        sparse_measurements
            .resource_limits
            .tree_state_checkpoint_coverage = super::TreeStateCheckpointCoverage::CadenceAndTip;
        sparse_measurements.source_load.tree_state_checkpoint_count = 2;
        let sparse_report = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            sparse_measurements,
            Some(canonical_fixture_source_exposition()),
        );
        sparse_report.validate()?;
        let encoded = serde_json::to_value(&sparse_report)?;
        assert_eq!(
            encoded["resource_limits"]["tree_state_checkpoint_coverage"],
            "cadence-and-tip"
        );
        assert_eq!(encoded["source_load"]["tree_state_checkpoint_count"], 2);

        let mut mismatched_dense = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            Some(canonical_fixture_source_exposition()),
        );
        if let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(inner) = &mut mismatched_dense
        {
            inner.source_load.tree_state_checkpoint_count = 2;
        }
        assert!(mismatched_dense.validate().is_err());

        let mut mismatched_sparse = sparse_report;
        if let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(inner) = &mut mismatched_sparse
        {
            inner.source_load.tree_state_checkpoint_count = 11;
        }
        assert!(mismatched_sparse.validate().is_err());

        let mut wrong_version = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            Some(canonical_fixture_source_exposition()),
        );
        if let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(inner) = &mut wrong_version {
            inner.report_format_version = 4;
        }
        assert!(wrong_version.validate().is_err());
        Ok(())
    }

    #[test]
    fn trusted_fresh_lifecycle_rejects_cold_certification_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut measurements = rocksdb_storage_lifecycle_measurements(
            super::TreeStateCheckpointCoverage::EveryBlock,
            206,
        );
        measurements.resource_limits.canonical_publication_mode =
            super::CanonicalPublicationMode::TrustedFresh;
        assert!(
            super::build_rocksdb_storage_lifecycle_report(measurements.clone())
                .validate()
                .is_err()
        );
        measurements
            .phase_durations
            .canonical_cold_validation_seconds = 0.0;
        let report = super::build_rocksdb_storage_lifecycle_report(measurements);
        report.validate()?;
        assert_eq!(
            serde_json::to_value(report)?["resource_limits"]["canonical_publication_mode"],
            "trusted-fresh"
        );
        Ok(())
    }

    #[test]
    fn rocksdb_storage_lifecycle_report_validates_declared_coverage_and_rejects_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let dense =
            super::build_rocksdb_storage_lifecycle_report(rocksdb_storage_lifecycle_measurements(
                super::TreeStateCheckpointCoverage::EveryBlock,
                206,
            ));
        dense.validate()?;
        let encoded = serde_json::to_value(&dense)?;
        assert_eq!(encoded["report_format_version"], 1);
        assert_eq!(
            encoded["resource_limits"]["tree_state_checkpoint_coverage"],
            "every-block"
        );
        assert_eq!(
            encoded["canonical_storage_ready"]["tree_state_checkpoint_count"],
            206
        );

        let sparse =
            super::build_rocksdb_storage_lifecycle_report(rocksdb_storage_lifecycle_measurements(
                super::TreeStateCheckpointCoverage::CadenceAndTip,
                4,
            ));
        sparse.validate()?;
        let encoded = serde_json::to_value(&sparse)?;
        assert_eq!(encoded["report_format_version"], 1);
        assert_eq!(
            encoded["resource_limits"]["tree_state_checkpoint_coverage"],
            "cadence-and-tip"
        );
        assert_eq!(
            encoded["canonical_storage_ready"]["tree_state_checkpoint_count"],
            4
        );

        let mismatched =
            super::build_rocksdb_storage_lifecycle_report(rocksdb_storage_lifecycle_measurements(
                super::TreeStateCheckpointCoverage::EveryBlock,
                4,
            ));
        assert!(mismatched.validate().is_err());

        // Previous report formats are rejected.
        for archived_version in [4, 5, 6] {
            let mut wrong_version = dense.clone();
            if let super::BenchmarkReport::RocksDbStorageLifecycle(inner) = &mut wrong_version {
                inner.report_format_version = archived_version;
            }
            assert!(wrong_version.validate().is_err());
        }
        Ok(())
    }

    #[test]
    fn rocksdb_canonical_fixture_report_rejects_mismatched_ready_and_tip_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut report = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            Some(canonical_fixture_source_exposition()),
        );
        let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(inner) = &mut report else {
            return Err("builder must return canonical fixture replay".into());
        };
        inner.canonical_ready.published_and_reopened_ready_match = false;
        assert!(report.validate().is_err());

        let mut report = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            Some(canonical_fixture_source_exposition()),
        );
        let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(inner) = &mut report else {
            return Err("builder must return canonical fixture replay".into());
        };
        inner.source_load.tip.hash_hex = "wrong-tip".to_owned();
        assert!(report.validate().is_err());
        Ok(())
    }

    #[test]
    fn rocksdb_canonical_fixture_report_aggregates_source_attribution()
    -> Result<(), Box<dyn std::error::Error>> {
        let report = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            Some(canonical_fixture_source_exposition()),
        );
        let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(inner) = report else {
            return Err("expected canonical fixture replay report".into());
        };
        let source = inner
            .source_fetch_attribution
            .ok_or("source attribution must be present")?;

        assert_eq!(source.completed_segment_request_count, 4);
        assert_eq!(source.total_connected_blocks_returned, 12);
        assert_eq!(source.total_response_payload_bytes, 120_000_000);
        assert_eq!(source.density_restart_count, 0);
        assert_eq!(source.density_sizing_adjustment_count, 7);
        assert_eq!(source.response_too_large_restart_count, 0);
        assert_eq!(source.discarded_completed_segment_count, 0);
        assert_eq!(source.discarded_in_flight_segment_count, 0);
        assert_eq!(source.discarded_completed_response_bytes, 0);
        assert_eq!(source.retained_completed_segment_count, 9);
        assert_eq!(source.retained_in_flight_segment_count, 13);
        assert_eq!(source.retained_completed_response_bytes, 110_000_000);
        assert_eq!(source.source_watermark_blocked_count, 8);
        assert!((source.source_watermark_blocks_per_second - 0.8).abs() < f64::EPSILON);
        let prohibited = inner
            .prohibited_reads
            .ok_or("prohibited read telemetry must be present")?;
        assert_eq!(prohibited.historical_prevout_read_count, 0);
        assert_eq!(prohibited.cross_block_wallet_read_count, 0);
        assert_eq!(inner.publication_proof_provenance, "trusted-fresh-writer");
        assert!(inner.publication_family_scans.is_empty());
        assert_eq!(inner.head_of_line_wait.len(), 1);
        assert_eq!(inner.stage_durations.len(), 1);
        Ok(())
    }

    #[test]
    fn rocksdb_canonical_fixture_report_requires_zero_prohibited_reads_and_exact_provenance()
    -> Result<(), Box<dyn std::error::Error>> {
        let missing = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            Some(
                "zinder_ingest_source_request_total{operation=\"fetch_chain_segment\",status=\"ok\",error_class=\"none\"} 1\n\
zinder_ingest_source_segment_connected_blocks_total 10\n\
zinder_ingest_source_segment_response_payload_bytes_sum 1000\n",
            ),
        );
        assert!(missing.validate().is_err());

        let mut nonzero = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            Some(canonical_fixture_source_exposition()),
        );
        let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(inner) = &mut nonzero else {
            return Err("expected canonical fixture replay report".into());
        };
        inner
            .prohibited_reads
            .as_mut()
            .ok_or("prohibited read telemetry must be present")?
            .historical_prevout_read_count = 1;
        assert!(nonzero.validate().is_err());

        let mut unexpected_scan = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            Some(canonical_fixture_source_exposition()),
        );
        let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(inner) = &mut unexpected_scan
        else {
            return Err("expected canonical fixture replay report".into());
        };
        inner
            .publication_family_scans
            .push(CanonicalPublicationFamilyScanStat {
                family: "block_replay".to_owned(),
                scan_count: 1,
                scan_seconds: 0.1,
                row_count: 10,
                logical_bytes: 5_000,
            });
        assert!(unexpected_scan.validate().is_err());

        let mut unknown_provenance = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            Some(canonical_fixture_source_exposition()),
        );
        let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(inner) = &mut unknown_provenance
        else {
            return Err("expected canonical fixture replay report".into());
        };
        inner.publication_proof_provenance = "unknown".to_owned();
        assert!(unknown_provenance.validate().is_err());
        Ok(())
    }

    #[test]
    fn rocksdb_canonical_fixture_report_rejects_missing_or_impossible_source_attribution()
    -> Result<(), Box<dyn std::error::Error>> {
        let report = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            None,
        );
        assert!(report.validate().is_err());

        let mut report = build_rocksdb_canonical_fixture_replay_report(
            fixture_summary(),
            rocksdb_canonical_fixture_measurements(),
            Some(canonical_fixture_source_exposition()),
        );
        let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(inner) = &mut report else {
            return Err("builder must return canonical fixture replay".into());
        };
        let source = inner
            .source_fetch_attribution
            .as_mut()
            .ok_or("source attribution must be present")?;
        source.discarded_completed_response_bytes =
            source.total_response_payload_bytes.saturating_add(1);
        assert!(report.validate().is_err());
        Ok(())
    }

    #[test]
    fn measured_acceptance_reports_threshold_results() -> Result<(), crate::BenchError> {
        let acceptance = summarize_rocksdb_storage_lifecycle_acceptance(
            12.5,
            Some(AcceptanceThresholds::try_from_seconds(10.0, 15.0)?),
            1.0,
            None,
        );

        let Some(thresholds) = acceptance.canonical_storage_ready.thresholds else {
            return Err(crate::BenchError::invalid_argument(
                "expected supplied acceptance thresholds in the report",
            ));
        };
        assert_eq!(
            acceptance.canonical_storage_ready.scope,
            "canonical-storage-ready"
        );
        assert!((thresholds.target_seconds - 10.0).abs() < f64::EPSILON);
        assert!((thresholds.hard_limit_seconds - 15.0).abs() < f64::EPSILON);
        assert!(!thresholds.target_met);
        assert!(thresholds.hard_limit_met);
        assert!(acceptance.wallet_storage_ready.thresholds.is_none());
        Ok(())
    }

    #[test]
    fn acceptance_thresholds_reject_invalid_duration_pairs() {
        assert!(AcceptanceThresholds::try_from_seconds(0.0, 1.0).is_err());
        assert!(AcceptanceThresholds::try_from_seconds(2.0, 1.0).is_err());
        assert!(AcceptanceThresholds::try_from_seconds(f64::NAN, 1.0).is_err());
        assert!(AcceptanceThresholds::try_from_seconds(1.0, f64::INFINITY).is_err());
    }

    fn sample_canonical_replay_storage_report() -> super::BenchmarkReport {
        build_canonical_replay_storage_report(
            fixture_summary(),
            CanonicalReplayStorageMeasurements {
                block_prepare_concurrency: 8,
                wall_clock_seconds: 2.3,
                storage_initialization_wall_clock_seconds: 0.1,
                replay_preparation_wall_clock_seconds: 0.8,
                replay_persistence_wall_clock_seconds: 0.7,
                index_construction_wall_clock_seconds: 0.0,
                storage_optimization_wall_clock_seconds: 0.0,
                validation_wall_clock_seconds: 0.4,
                publication_wall_clock_seconds: 0.1,
                fresh_reader_validation_wall_clock_seconds: 0.1,
                storage_measurement_wall_clock_seconds: 0.1,
                first_height: 11,
                first_hash_hex: "first-hash".to_owned(),
                tip_height: 20,
                tip_hash_hex: "abcd".to_owned(),
                logical_replay_bytes: 4_096,
                physical_storage_bytes: 8_192,
                persisted_sequence_digest: CanonicalBlockFactsSequenceDigestSummary {
                    block_digest_version: 1,
                    sequence_digest_version: 1,
                    block_count: 10,
                    sha256: "persisted-digest".to_owned(),
                },
                replay_format_version: 1,
                semantic_replay_validated: true,
                storage: CanonicalReplayStorageEvidence::RocksDb {
                    storage_schema_version: 1,
                    ingestion_mode: "external-sst",
                    durability_mode: "external-sst-ingest-with-synchronous-completion-marker",
                    database_io_mode: "buffered".to_owned(),
                    external_sst_io_mode: "buffered",
                    compression: "snappy",
                    external_sst_bytes: 7_000,
                    rocksdb_resource_budget: RocksDbResourceBudgetSummary::from(
                        zinder_store::RocksDbResourceBudget::for_local_tests(),
                    ),
                },
                benchmark_client_peak_rss: crate::rss::PeakRss {
                    bytes: None,
                    source: PEAK_RSS_SOURCE_UNAVAILABLE,
                },
                storage_candidate: StorageCandidateIdentity::rocksdb_canonical_replay_storage(),
                software_revision: None,
                trial_id: Some("trial-01".to_owned()),
                fixture_cache_policy: Some(FixtureCachePolicy::Warm),
                run_started_at_unix_millis: 1_000,
                run_completed_at_unix_millis: 4_000,
                runner_id: None,
                cpu_limit_cores: Some(8.0),
                memory_limit_bytes: Some(16 * 1024 * 1024 * 1024),
                storage_class: None,
                image_reference: None,
            },
        )
    }

    #[test]
    fn canonical_replay_storage_reports_semantic_replay_and_omits_unmeasured_contracts()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut report = sample_canonical_replay_storage_report();
        report.validate()?;
        let json = serde_json::to_value(&report)?;

        assert_eq!(json["measurement_kind"], "canonical-replay-storage");
        assert_eq!(json["contract_identity"], "benchmark-report");
        assert_eq!(json["report_format_version"], 1);
        assert_eq!(json["fixture"]["contract_identity"], "canonical-fixture");
        assert_eq!(
            json["storage_candidate"]["id"],
            "rocksdb-canonical-replay-storage"
        );
        assert_eq!(json["round_trip"]["replay_format_version"], 1);
        assert_eq!(json["round_trip"]["semantic_replay_validated"], true);
        assert_eq!(json["provenance"]["run"]["trial_id"], "trial-01");
        assert_eq!(json["provenance"]["run"]["fixture_cache_policy"], "warm");
        for omitted_field in ["acceptance", "replay", "lifecycle", "store_reads"] {
            assert!(json.get(omitted_field).is_none(), "{omitted_field}");
        }
        let mut wrong_version = report.clone();
        if let super::BenchmarkReport::CanonicalReplayStorage(inner) = &mut wrong_version {
            inner.report_format_version = 5;
        }
        assert!(wrong_version.validate().is_err());
        if let super::BenchmarkReport::CanonicalReplayStorage(inner) = &mut report {
            inner.round_trip.replay_format_version = 99;
        }
        assert!(report.validate().is_err());
        Ok(())
    }

    #[test]
    fn report_v3_rejects_unrecognized_contract_identities() -> Result<(), crate::BenchError> {
        let fixture_replay_report = || {
            build_rocksdb_canonical_fixture_replay_report(
                fixture_summary(),
                rocksdb_canonical_fixture_measurements(),
                Some(canonical_fixture_source_exposition()),
            )
        };
        fixture_replay_report().validate()?;

        let mut report = fixture_replay_report();
        if let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(report) = &mut report {
            report.contract_identity = "zinder-benchmark-report".to_owned();
        }
        assert!(report.validate().is_err());

        let mut report = fixture_replay_report();
        if let super::BenchmarkReport::RocksDbCanonicalFixtureReplay(report) = &mut report {
            report.fixture.contract_identity = "zinder-bench-fixture-manifest".to_owned();
        }
        assert!(report.validate().is_err());
        Ok(())
    }

    #[test]
    fn canonical_fixture_report_rejects_impossible_settled_and_live_tail_partition()
    -> Result<(), crate::BenchError> {
        let make_report = |live_tail_block_count: u64,
                           settled_segment_index_count: u64,
                           settled_segment_index_logical_bytes: u64| {
            let mut measurements = rocksdb_canonical_fixture_measurements();
            measurements.source_load.live_tail_block_count = live_tail_block_count;
            measurements.source_load.settled_segment_index_count = settled_segment_index_count;
            measurements.source_load.settled_segment_index_logical_bytes =
                settled_segment_index_logical_bytes;
            build_rocksdb_canonical_fixture_replay_report(
                fixture_summary(),
                measurements,
                Some(canonical_fixture_source_exposition()),
            )
        };

        assert!(make_report(10, 1, 128).validate().is_err());
        assert!(make_report(11, 0, 0).validate().is_err());
        assert!(make_report(0, 0, 0).validate().is_err());
        assert!(make_report(5, 1, 0).validate().is_err());
        assert!(make_report(5, 0, 0).validate().is_err());
        assert!(make_report(5, 6, 128).validate().is_err());
        make_report(5, 1, 128).validate()?;
        Ok(())
    }

    #[test]
    fn stage_duration_report_preserves_family_stage_and_status() {
        let samples = parse_prometheus_samples(
            "zinder_ingest_canonical_block_construction_stage_duration_seconds_count{stage=\"block_parse\",status=\"ok\"} 4\n\
             zinder_ingest_canonical_block_construction_stage_duration_seconds_sum{stage=\"block_parse\",status=\"ok\"} 1.5\n",
        );

        let stats = aggregate_stage_durations(&samples);

        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].family, "canonical_block_construction");
        assert_eq!(stats[0].stage, "block_parse");
        assert_eq!(stats[0].status, "ok");
        assert_eq!(stats[0].call_count, 4);
        assert!((stats[0].task_seconds - 1.5).abs() < f64::EPSILON);
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one report fixture keeps all cross-field evidence visibly consistent"
    )]
    fn rocksdb_canonical_fixture_measurements() -> RocksDbCanonicalFixtureReplayMeasurements {
        let sequence_digest = CanonicalBlockFactsSequenceDigestSummary {
            block_digest_version: 1,
            sequence_digest_version: 1,
            block_count: 10,
            sha256: "persisted-digest".to_owned(),
        };
        let first_block = StorageLifecycleBlockId {
            height: 11,
            hash_hex: "first-hash".to_owned(),
        };
        let visible_tip = StorageLifecycleBlockId {
            height: 20,
            hash_hex: "abcd".to_owned(),
        };
        RocksDbCanonicalFixtureReplayMeasurements {
            replay_plan_fixture_manifest_sha256: "b".repeat(64),
            replay_plan_digest_sha256: "a".repeat(64),
            resource_limits: RocksDbCanonicalFixtureReplayResourceLimits {
                block_source: "fixture",
                injected_response_delay_millis: 0,
                indexer_get_block_max_in_flight_requests: None,
                derived_for_cpu_limit_cores: 10,
                derived_for_memory_limit_bytes: 10 * 1024 * 1024 * 1024,
                request_timeout_seconds: 30,
                max_response_bytes: 64 * 1024 * 1024,
                source_segment_target_response_bytes: 32 * 1024 * 1024,
                source_segment_max_blocks: 64,
                source_fetch_max_in_flight_requests: 12,
                source_fetch_max_in_flight_bytes: 160 * 1024 * 1024,
                block_prepare_concurrency: 10,
                block_prepare_memory_watermark_bytes: 160 * 1024 * 1024,
                tree_hashing_workers: 1,
                supported_reorg_depth: 5,
                source_segment_delay_millis: 0,
                tree_state_checkpoint_coverage: super::TreeStateCheckpointCoverage::EveryBlock,
                canonical_rocksdb: RocksDbResourceBudgetSummary::from(
                    zinder_store::RocksDbResourceBudget::canonical_writer_defaults(),
                ),
            },
            publication_proof_provenance: "trusted-fresh-writer",
            total_seconds: 10.0,
            source_load: RocksDbCanonicalFixtureSourceLoadSummary {
                first_block: first_block.clone(),
                first_parent_hash_hex: "parent-hash".to_owned(),
                tip: visible_tip.clone(),
                block_count: 10,
                transaction_count: 20,
                block_header_count: 10,
                block_hash_index_count: 10,
                settled_segment_index_count: 0,
                live_tail_block_count: 10,
                transaction_location_count: 20,
                transaction_blob_count: 0,
                block_blob_count: 0,
                tree_state_checkpoint_count: 11,
                subtree_root_count: 0,
                logical_replay_bytes: 5_000,
                settled_segment_index_logical_bytes: 0,
                live_tail_block_logical_bytes: 10_000,
                live_tail_raw_block_logical_bytes: 0,
                logical_bytes: 20_000,
                sst_file_bytes: 10_000,
                sst_file_count: 8,
                replay_format_version: 1,
                sequence_digest: sequence_digest.clone(),
            },
            canonical_ready: RocksDbCanonicalFixtureReadySummary {
                scope: "canonical-fixture-ready",
                first_retained_block: first_block,
                visible_tip: visible_tip.clone(),
                visible_epoch_id: 1,
                visible_event_sequence: 1,
                visible_block_count: 10,
                replay_format_version: 1,
                sequence_digest: sequence_digest.clone(),
                logical_replay_bytes: 5_000,
                settled_tip: StorageLifecycleBlockId {
                    height: 15,
                    hash_hex: "settled-hash".to_owned(),
                },
                event_fence: RocksDbCanonicalFixtureEventFenceSummary {
                    chain_epoch_id: 1,
                    chain_event_sequence: 1,
                    visible_tip,
                    sequence_digest,
                },
                source_tip_checkpoint_authenticated: true,
                published_and_reopened_ready_match: true,
                reopened_ready_and_event_fence_match: true,
                full_scan_block_count: 10,
            },
            physical_store_bytes: 25_000,
            benchmark_client_peak_rss: crate::rss::PeakRss {
                bytes: None,
                source: PEAK_RSS_SOURCE_UNAVAILABLE,
            },
            run_started_at_unix_millis: 1_000,
            run_completed_at_unix_millis: 11_000,
        }
    }

    fn canonical_fixture_source_exposition() -> &'static str {
        "zinder_ingest_source_request_total{operation=\"fetch_chain_segment\",status=\"ok\",error_class=\"none\"} 4\n\
zinder_ingest_source_request_duration_seconds_sum{operation=\"fetch_chain_segment\",status=\"ok\",error_class=\"none\"} 15.5\n\
zinder_ingest_source_segment_connected_blocks_total 12\n\
zinder_ingest_source_segment_response_payload_bytes_sum 120000000\n\
zinder_ingest_source_segment_sizing_adjustment_total{reason=\"density\"} 7\n\
zinder_ingest_source_segment_disjoint_prefetch_discarded_completed_segments_total{reason=\"response_too_large\"} 0\n\
zinder_ingest_source_segment_disjoint_prefetch_discarded_in_flight_segments_total{reason=\"response_too_large\"} 0\n\
zinder_ingest_source_segment_disjoint_prefetch_discarded_completed_response_bytes_total{reason=\"response_too_large\"} 0\n\
zinder_ingest_source_segment_prefetch_retained_completed_segments_total{reason=\"density\"} 9\n\
zinder_ingest_source_segment_prefetch_retained_in_flight_segments_total{reason=\"density\"} 13\n\
zinder_ingest_source_segment_prefetch_retained_completed_response_bytes_total{reason=\"density\"} 110000000\n\
zinder_ingest_bulk_pipeline_watermark_blocked_total{stage=\"source_fetch\"} 8\n\
zinder_ingest_bulk_pipeline_head_of_line_wait_seconds_count{stage=\"source_fetch\"} 2\n\
zinder_ingest_bulk_pipeline_head_of_line_wait_seconds_sum{stage=\"source_fetch\"} 4.5\n\
zinder_ingest_canonical_block_construction_stage_duration_seconds_count{stage=\"block_parse\",status=\"ok\"} 10\n\
zinder_ingest_canonical_block_construction_stage_duration_seconds_sum{stage=\"block_parse\",status=\"ok\"} 1.5\n\
zinder_ingest_canonical_historical_prevout_reads_total 0\n\
zinder_ingest_canonical_cross_block_wallet_reads_total 0\n"
    }

    fn fixture_summary() -> FixtureSummary {
        FixtureSummary {
            contract_identity: crate::fixture::FIXTURE_CONTRACT_IDENTITY.to_owned(),
            fixture_format_version: crate::fixture::FIXTURE_FORMAT_VERSION,
            canonical_artifact_schema_version: 18,
            canonical_block_facts_digest_evidence:
                crate::fixture::CanonicalBlockFactsDigestEvidence {
                    block_digest_version: 1,
                    sequence_digest_version: 1,
                    block_count: 10,
                    sequence_digest_sha256: "persisted-digest".to_owned(),
                },
            tip_hash_hex: "abcd".to_owned(),
            digest_sha256: "b".repeat(64),
            network: "zcash-regtest".to_owned(),
            from_height: 11,
            to_height: 20,
            block_count: 10,
            workload_density: WorkloadDensity {
                block_count: 10,
                transaction_count: 20,
                ..WorkloadDensity::default()
            },
            segment_count: 1,
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one lifecycle report fixture keeps all cross-field evidence visibly consistent"
    )]
    fn rocksdb_storage_lifecycle_measurements(
        tree_state_checkpoint_coverage: super::TreeStateCheckpointCoverage,
        tree_state_checkpoint_count: u64,
    ) -> super::RocksDbStorageLifecycleMeasurements {
        let first_block = StorageLifecycleBlockId {
            height: 1,
            hash_hex: "11".repeat(32),
        };
        let fixed_tip = StorageLifecycleBlockId {
            height: 205,
            hash_hex: "22".repeat(32),
        };
        let sequence_digest = CanonicalBlockFactsSequenceDigestSummary {
            block_digest_version: 1,
            sequence_digest_version: 1,
            block_count: 205,
            sha256: "33".repeat(32),
        };
        let empty_sort = |max_memory_bytes: u64| super::WalletVariableValueSortEvidence {
            record_count: 0,
            initial_run_count: 0,
            merge_pass_count: 0,
            peak_accounted_sort_memory_bytes: 0,
            max_accounted_sort_memory_bytes: max_memory_bytes,
            peak_temporary_file_bytes: 0,
            max_temporary_file_bytes: 1_024,
            final_run_file_bytes: 0,
        };
        let budget = RocksDbResourceBudgetSummary::from(
            zinder_store::RocksDbResourceBudget::canonical_writer_defaults(),
        );
        super::RocksDbStorageLifecycleMeasurements {
            provenance: super::ReportProvenance {
                benchmark_version: env!("CARGO_PKG_VERSION"),
                software_revision: Some("0".repeat(40)),
                run: super::BenchmarkRunProvenance {
                    trial_id: Some("trial-01".to_owned()),
                    fixture_cache_policy: None,
                    started_at_unix_millis: 1_000,
                    completed_at_unix_millis: 2_000,
                },
                runner: super::RunnerProvenance {
                    id: Some("runner-01".to_owned()),
                    cpu_limit_cores: Some(10.0),
                    memory_limit_bytes: Some(10 * 1024 * 1024 * 1024),
                    storage_class: Some("local-nvme".to_owned()),
                },
                image_reference: Some(format!("sha256:{}", "a".repeat(64))),
                target_os: "linux",
                target_arch: "x86_64",
            },
            source: super::StorageLifecycleSourceSummary {
                family: "zebra-json-rpc",
                network: "zcash-testnet".to_owned(),
                network_upgrade_activation_count: 11,
                network_upgrade_activations_fingerprint_version: 1,
                network_upgrade_activations_fingerprint_hex: "44".repeat(32),
                source_tip_at_freeze: fixed_tip.clone(),
                fixed_build_tip: fixed_tip.clone(),
                source_tip_after_canonical_load: fixed_tip.clone(),
            },
            contracts: super::StorageLifecycleContractSummary {
                canonical_store_identity: zinder_store::CANONICAL_STORE_IDENTITY,
                canonical_store_schema_version: zinder_store::CANONICAL_STORE_SCHEMA_VERSION,
                wallet_store_identity: "wallet",
                wallet_store_schema_version: zinder_wallet_rocksdb::WALLET_ROCKSDB_SCHEMA_VERSION,
                wallet_projection_schema_version:
                    zinder_wallet_projection::WALLET_PROJECTION_SCHEMA_VERSION,
                wallet_value_encoding_version:
                    zinder_wallet_projection::WALLET_PROJECTION_VALUE_ENCODING_VERSION,
            },
            resource_limits: super::StorageLifecycleResourceLimits {
                request_timeout_seconds: 30,
                max_response_bytes: 64 * 1024 * 1024,
                source_segment_target_response_bytes: 32 * 1024 * 1024,
                source_segment_max_blocks: 64,
                source_fetch_max_in_flight_requests: 12,
                source_fetch_max_in_flight_bytes: 160 * 1024 * 1024,
                block_prepare_concurrency: 10,
                block_prepare_memory_watermark_bytes: 160 * 1024 * 1024,
                canonical_rocksdb: budget,
                tree_state_checkpoint_coverage,
                canonical_publication_mode: super::CanonicalPublicationMode::ColdCertified,
                wallet_rocksdb: budget,
                supported_reorg_depth: 100,
                wallet_max_outpoint_sort_memory_bytes: 4_096,
                wallet_max_secondary_sort_memory_bytes_per_sorter: 2_048,
                wallet_max_temporary_file_bytes_per_sorter: 1_024,
                wallet_sst_target_logical_bytes: 4_096,
                wallet_max_accounted_reorg_undo_bytes: 4_096,
            },
            acceptance: summarize_rocksdb_storage_lifecycle_acceptance(5.0, None, 2.0, None),
            phase_durations: super::StorageLifecyclePhaseDurations {
                source_discovery_seconds: 0.5,
                canonical_store_initialization_seconds: 0.5,
                canonical_source_load_seconds: 2.0,
                canonical_cold_validation_seconds: 1.0,
                canonical_ready_publication_seconds: 0.5,
                canonical_cold_reopen_seconds: 0.5,
                wallet_build_seconds: 1.5,
                final_cold_reopen_seconds: 0.5,
                total_seconds: 7.0,
            },
            canonical_storage_ready: super::CanonicalStorageReadySummary {
                scope: "canonical-storage-ready",
                first_retained_block: first_block,
                visible_tip: fixed_tip.clone(),
                visible_epoch_id: 1,
                visible_event_sequence: 1,
                block_count: 205,
                transaction_count: 205,
                tree_state_checkpoint_count,
                subtree_root_count: 0,
                replay_format_version: 1,
                sequence_digest: sequence_digest.clone(),
                logical_replay_bytes: 10_000,
                logical_storage_bytes: 20_000,
                sst_file_bytes: 12_000,
                sst_file_count: 6,
                physical_store_bytes: 15_000,
                database_io_mode: "buffered".to_owned(),
                source_tip_checkpoint_authenticated: true,
                cold_reopen_evidence_match: true,
            },
            wallet_storage_ready: super::WalletStorageReadySummary {
                scope: "wallet-storage-ready",
                source_epoch_id: 1,
                source_tip: fixed_tip,
                source_event_sequence: 1,
                source_sequence_digest: sequence_digest,
                wallet_projection_digest_hex: "55".repeat(32),
                row_counts: super::WalletStorageRowCounts {
                    transparent_unspent_output_count: 0,
                    transparent_unspent_output_by_address_count: 0,
                    transparent_spent_output_count: 0,
                    transparent_address_transaction_count: 0,
                    transparent_address_balance_count: 0,
                    reorg_undo_count: 100,
                },
                utxo_summary: super::WalletStorageUtxoSummary {
                    utxo_count: 0,
                    total_value_zat: 0,
                    commitment_scheme: "lthash16-chacha12",
                    commitment_accumulator_hex: "00".repeat(2048),
                    commitment_display_digest_hex: "66".repeat(32),
                },
                scanned_block_count: 205,
                scanned_transaction_count: 205,
                historical_prevout_read_count: 0,
                construction: super::WalletStorageConstructionEvidence {
                    outpoint_sort: empty_sort(4_096),
                    address_index_sort: empty_sort(2_048),
                    address_transaction_sort: empty_sort(2_048),
                    peak_accounted_reorg_undo_bytes: 1_024,
                    max_accounted_reorg_undo_bytes: 4_096,
                    cold_validation_peak_accounted_reorg_undo_bytes: 1_024,
                    cold_validation_max_accounted_reorg_undo_bytes: 4_096,
                    cold_validation_random_read_count: 0,
                    logical_row_bytes: 2_048,
                    sst_file_bytes: 1_024,
                    sst_file_count: 2,
                },
                phase_durations: super::WalletStoragePhaseDurations {
                    store_initialization_seconds: 0.1,
                    canonical_scan_seconds: 0.2,
                    outpoint_sort_seconds: 0.1,
                    outpoint_merge_seconds: 0.1,
                    secondary_row_derivation_seconds: 0.1,
                    logical_evidence_seconds: 0.1,
                    row_load_seconds: 0.1,
                    flush_and_cold_reopen_seconds: 0.1,
                    cold_validation_seconds: 0.2,
                    cold_validation_primary_output_scan_seconds: 0.05,
                    cold_validation_address_index_and_balance_readback_seconds: 0.05,
                    cold_validation_address_transaction_readback_seconds: 0.05,
                    cold_validation_reorg_undo_readback_seconds: 0.05,
                    ready_publication_seconds: 0.1,
                    total_seconds: 1.5,
                },
                build_stages: Vec::new(),
                physical_store_bytes: 4_096,
                cold_reopen_evidence_match: true,
                canonical_fence_match: true,
            },
            benchmark_client_peak_rss: crate::rss::PeakRss {
                bytes: Some(1_048_576),
                source: "proc_status_vmhwm",
            },
        }
    }

    #[test]
    fn postgres_segmented_report_validates_and_rejects_boundary_tampering()
    -> Result<(), crate::BenchError> {
        let mut report = super::build_postgres_segmented_storage_report(
            sample_fixture_summary(),
            sample_postgres_segmented_inputs(),
        );
        report.validate()?;

        let json_text = serde_json::to_string(&report).map_err(|err| {
            crate::BenchError::report_format(format!("serialize segmented report: {err}"))
        })?;
        assert!(json_text.contains("\"measurement_kind\":\"postgres-segmented-storage\""));

        let Some(seg_mut) = (match &mut report {
            super::BenchmarkReport::PostgresSegmentedStorage(inner) => Some(inner),
            super::BenchmarkReport::CanonicalReplayStorage(_)
            | super::BenchmarkReport::RocksDbCanonicalFixtureReplay(_)
            | super::BenchmarkReport::RocksDbStorageLifecycle(_) => None,
        }) else {
            return Err(crate::BenchError::report_format(
                "expected postgres-segmented-storage variant",
            ));
        };

        seg_mut.server_settings.fsync = false;
        assert!(seg_mut.validate_segmented().is_err());
        seg_mut.server_settings.fsync = true;

        seg_mut.footprint.live_tail_rows =
            u64::from(seg_mut.configuration.live_tail_max_blocks) + 1;
        assert!(seg_mut.validate_segmented().is_err());
        seg_mut.footprint.live_tail_rows = 2;

        seg_mut.footprint.max_segment_byte_len = seg_mut.configuration.max_segment_bytes + 1;
        assert!(seg_mut.validate_segmented().is_err());
        Ok(())
    }

    fn sample_fixture_summary() -> super::FixtureSummary {
        super::FixtureSummary {
            contract_identity: super::FIXTURE_CONTRACT_IDENTITY.to_owned(),
            fixture_format_version: super::FIXTURE_FORMAT_VERSION,
            canonical_artifact_schema_version: 1,
            canonical_block_facts_digest_evidence: super::CanonicalBlockFactsDigestEvidence {
                block_digest_version: 1,
                sequence_digest_version: 1,
                block_count: 10,
                sequence_digest_sha256: "22".repeat(32),
            },
            tip_hash_hex: "11".repeat(32),
            digest_sha256: "33".repeat(32),
            network: "zcash-regtest".to_owned(),
            from_height: 1,
            to_height: 10,
            block_count: 10,
            workload_density: super::WorkloadDensity {
                block_count: 10,
                raw_block_bytes: 1_400,
                transaction_count: 10,
                transparent_input_count: 0,
                transparent_output_count: 10,
                blocks_with_transparent_inputs: 0,
                blocks_with_transparent_outputs: 10,
                max_raw_block_bytes_per_block: 140,
                max_transactions_per_block: 1,
                max_transparent_inputs_per_block: 0,
                max_transparent_outputs_per_block: 1,
            },
            segment_count: 1,
        }
    }

    fn sample_postgres_server_settings() -> super::PostgresServerSettings {
        super::PostgresServerSettings {
            server_version: "18.7".to_owned(),
            server_version_number: 180_007,
            max_connections: 100,
            shared_buffers_bytes: 128 * 1024 * 1024,
            effective_cache_size_bytes: 512 * 1024 * 1024,
            maintenance_work_mem_bytes: 64 * 1024 * 1024,
            work_mem_bytes: 4 * 1024 * 1024,
            max_wal_size_bytes: 1024 * 1024 * 1024,
            min_wal_size_bytes: 80 * 1024 * 1024,
            checkpoint_timeout_seconds: 300,
            checkpoint_completion_target: 0.9,
            wal_compression: "off".to_owned(),
            password_encryption_default: "scram-sha-256".to_owned(),
            max_worker_processes: 8,
            max_parallel_workers: 8,
            max_parallel_maintenance_workers: 2,
            track_io_timing: false,
            huge_pages: "try".to_owned(),
            fsync: true,
            full_page_writes: true,
            synchronous_commit: "on".to_owned(),
            wal_level: "replica".to_owned(),
            data_checksums: true,
        }
    }

    fn sample_postgres_segmented_run() -> crate::postgres_segmented::PostgresSegmentedRunReport {
        use crate::postgres_segmented::{
            PostgresSegmentedBuildTimings, PostgresSegmentedRunReport,
            reader::{ConcurrentReaderSummary, ReaderConnectionMode},
            storage_footprint::StorageFootprintSummary,
            tip_follow::{LatencyPercentilesMillis, TipFollowRunMetrics},
        };
        PostgresSegmentedRunReport {
            schema_name: "zinder_bench_segmented".to_owned(),
            schema_version: 2,
            network: zinder_core::Network::ZcashRegtest,
            full_blocks: false,
            max_segment_bytes: 256 * 1024 * 1024,
            settled_segment_blocks: 4,
            live_tail_max_blocks: 3,
            reader_mode: ReaderConnectionMode::Direct,
            concurrent_readers: 2,
            mempool_churn_txs_per_block: 4,
            bulk_block_count: 6,
            tip_follow_block_count: 4,
            total_block_count: 10,
            first_height: zinder_core::BlockHeight::new(1),
            tip_height: zinder_core::BlockHeight::new(10),
            tip_hash: zinder_core::BlockHash::from_bytes([0x11; 32]),
            persisted_sequence_digest_hex: "22".repeat(32),
            settled_blocks_verified: 8,
            live_tail_blocks_verified: 2,
            raw_blocks_verified: 0,
            server_settings: sample_postgres_server_settings(),
            timings: PostgresSegmentedBuildTimings {
                wall_clock_seconds: 2.0,
                prepare_seconds: 0.2,
                seal_seconds: 0.2,
                bulk_copy_seconds: 0.3,
                finalize_seconds: 0.3,
                uncontended_tip_follow_seconds: 0.4,
                contended_tip_follow_seconds: 0.4,
                fresh_validation_seconds: 0.1,
                footprint_measurement_seconds: 0.1,
            },
            tip_follow: TipFollowRunMetrics {
                uncontended_commit_latencies_ms: LatencyPercentilesMillis::from_samples(&[
                    5.0, 6.0,
                ]),
                contended_commit_latencies_ms: LatencyPercentilesMillis::from_samples(&[7.0, 9.0]),
                tail_seal_transitions: 1,
                mempool_admits: 16,
                mempool_evictions: 8,
                max_segment_bytes_observed: 4096,
            },
            reader_workload: ConcurrentReaderSummary {
                completed_transactions: 12,
                settled_range_reads: 12,
                blocks_decoded: 48,
                bytes_fetched: 24_000,
                per_reader_blocks_per_second_mean: 60.0,
                per_reader_bytes_per_second_mean: 30_000.0,
                aggregate_blocks_per_second: 120.0,
                aggregate_bytes_per_second: 60_000.0,
                tx_latencies_ms: vec![2.0, 3.0],
                per_reader_stats: Vec::new(),
            },
            reader_latencies_ms: LatencyPercentilesMillis::from_samples(&[2.0, 3.0]),
            footprint: StorageFootprintSummary {
                canonical_storage_bytes: 80_000,
                wallet_storage_bytes: 16_000,
                total_storage_bytes: 96_000,
                total_heap_bytes: 40_000,
                total_toast_bytes: 24_000,
                total_index_bytes: 32_000,
                bulk_wal_bytes: 50_000,
                tip_follow_wal_bytes: 20_000,
                settled_shielded_segments: 2,
                settled_supplement_segments: 2,
                settled_raw_block_segments: 0,
                max_segment_byte_len: 4_096,
                live_tail_rows: 2,
                relations: Vec::new(),
            },
        }
    }

    fn sample_postgres_segmented_inputs() -> super::PostgresSegmentedReportInputs {
        super::PostgresSegmentedReportInputs {
            run: sample_postgres_segmented_run(),
            baseline: Some(super::PostgresSegmentedBaselineInput {
                build_seconds: Some(1.0),
                storage_bytes: Some(100_000),
                streaming_blocks_per_second: Some(80.0),
            }),
            runtime_evidence: super::PostgresBenchmarkRuntimeEvidence {
                database_image_reference: None,
                client_cpu_limit_cores: None,
                client_memory_limit_bytes: None,
                database_cpu_limit_cores: None,
                database_memory_limit_bytes: None,
            },
            benchmark_client_peak_rss: crate::rss::PeakRss {
                bytes: Some(1_048_576),
                source: "proc_status_vmhwm",
            },
            software_revision: None,
            trial_id: None,
            fixture_cache_policy: None,
            run_started_at_unix_millis: 1_700_000_000_000,
            run_completed_at_unix_millis: 1_700_000_002_000,
            runner_id: None,
            cpu_limit_cores: None,
            memory_limit_bytes: None,
            storage_class: None,
            image_reference: None,
        }
    }
}
