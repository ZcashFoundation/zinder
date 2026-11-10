//! Error vocabulary for the benchmark harness.

use std::path::PathBuf;

use thiserror::Error;

/// A failure raised while capturing or replaying a fixed-range fixture.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BenchError {
    /// A filesystem operation failed.
    #[error("filesystem error at {path}: {source}")]
    Io {
        /// Path the operation targeted.
        path: PathBuf,
        /// Underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// A manifest or report could not be encoded or decoded as JSON.
    #[error("json error: {source}")]
    Json {
        /// Underlying serialization failure.
        #[source]
        source: serde_json::Error,
    },
    /// A fixture file did not match the expected on-disk format.
    #[error("fixture format error: {reason}")]
    FixtureFormat {
        /// Human-readable description of the mismatch.
        reason: String,
    },
    /// A benchmark report did not match the exact serialized contract.
    #[error("report format error: {reason}")]
    ReportFormat {
        /// Human-readable description of the mismatch.
        reason: String,
    },
    /// An upstream source call failed during capture.
    #[error("source error: {source}")]
    Source {
        /// Underlying source failure.
        #[source]
        source: zinder_source::SourceError,
    },
    /// Canonical block construction failed while measuring fixture density.
    #[error("canonical block construction error: {source}")]
    CanonicalBlockConstruction {
        /// Underlying deterministic block-construction failure.
        #[source]
        source: Box<zinder_ingest::CanonicalBlockConstructionError>,
    },
    /// A blocking fixture-read or replay-preparation task did not complete.
    #[error("canonical replay preparation task failed: {reason}")]
    CanonicalReplayPreparationTask {
        /// Human-readable join failure.
        reason: String,
    },
    /// An ordered canonical replay sequence violated its range or digest contract.
    #[error("canonical replay sequence mismatch: {reason}")]
    CanonicalReplaySequenceMismatch {
        /// Human-readable mismatch description.
        reason: String,
    },
    /// A concrete canonical-replay-storage storage candidate failed its build or validation.
    #[error("{candidate} storage candidate error: {reason}")]
    CanonicalReplayCandidate {
        /// Stable benchmark candidate identifier.
        candidate: &'static str,
        /// Engine-specific failure description without credentials.
        reason: String,
    },
    /// A canonical-store operation failed.
    #[error("store error: {source}")]
    Store {
        /// Underlying store failure.
        #[source]
        source: zinder_store::StoreError,
    },
    /// Fresh canonical construction failed.
    #[error("canonical storage lifecycle construction error: {source}")]
    CanonicalConstruction {
        /// Underlying construction failure.
        #[source]
        source: zinder_ingest::CanonicalConstructionError,
    },
    /// A canonical store could not be created, published, or admitted.
    #[error("canonical storage lifecycle error: {source}")]
    CanonicalStorage {
        /// Underlying canonical store failure.
        #[source]
        source: zinder_store::CanonicalStoreError,
    },
    /// A fixed canonical construction plan was invalid.
    #[error("canonical storage lifecycle build plan error: {source}")]
    CanonicalBuildPlan {
        /// Underlying plan validation failure.
        #[source]
        source: zinder_store::CanonicalStoreBuildPlanError,
    },
    /// Wallet construction or admission failed.
    #[error("wallet storage lifecycle error: {source}")]
    WalletStorage {
        /// Underlying wallet storage failure.
        #[source]
        source: zinder_wallet_rocksdb::RocksDbWalletError,
    },
    /// A materialized-view store operation failed while constructing projections.
    #[error("materialized-view store error: {source}")]
    MaterializedView {
        /// Underlying materialized-view store failure.
        #[source]
        source: zinder_materialized_views::MaterializedViewStoreError,
    },
    /// Materialized-view construction returned without reaching the canonical event tip.
    #[error("materialized-view build incomplete: {reason}")]
    MaterializedViewBuildIncomplete {
        /// Consumer cursor mismatch that prevents a completed build claim.
        reason: String,
    },
    /// The Prometheus recorder could not be installed.
    #[error("metrics recorder error: {reason}")]
    Recorder {
        /// Human-readable description of the failure.
        reason: String,
    },
    /// A caller-supplied argument was rejected before any work started.
    #[error("invalid argument: {reason}")]
    InvalidArgument {
        /// Human-readable description of the rejection.
        reason: String,
    },
    /// One or more configured acceptance hard limits were missed.
    #[error("acceptance hard limit missed for measured boundary: {boundary}")]
    AcceptanceHardLimitMissed {
        /// Stable report field name for the boundary that missed its limit.
        boundary: String,
    },
    /// A thresholded acceptance report lacks required telemetry coverage.
    #[error("acceptance telemetry missing for: {families}")]
    AcceptanceTelemetryMissing {
        /// Stable names for the absent report evidence.
        families: String,
    },
    /// A thresholded replay did not prove the requested fixture range completed.
    #[error("acceptance completion evidence mismatch: {reason}")]
    AcceptanceCompletionMismatch {
        /// Human-readable mismatch between fixture and final canonical state.
        reason: String,
    },
}

impl From<serde_json::Error> for BenchError {
    fn from(source: serde_json::Error) -> Self {
        Self::Json { source }
    }
}

impl From<zinder_source::SourceError> for BenchError {
    fn from(source: zinder_source::SourceError) -> Self {
        Self::Source { source }
    }
}

impl From<zinder_ingest::CanonicalBlockConstructionError> for BenchError {
    fn from(source: zinder_ingest::CanonicalBlockConstructionError) -> Self {
        Self::CanonicalBlockConstruction {
            source: Box::new(source),
        }
    }
}

impl From<zinder_store::StoreError> for BenchError {
    fn from(source: zinder_store::StoreError) -> Self {
        Self::Store { source }
    }
}

impl From<zinder_ingest::CanonicalConstructionError> for BenchError {
    fn from(source: zinder_ingest::CanonicalConstructionError) -> Self {
        Self::CanonicalConstruction { source }
    }
}

impl From<zinder_store::CanonicalStoreError> for BenchError {
    fn from(source: zinder_store::CanonicalStoreError) -> Self {
        Self::CanonicalStorage { source }
    }
}

impl From<zinder_store::CanonicalStoreBuildPlanError> for BenchError {
    fn from(source: zinder_store::CanonicalStoreBuildPlanError) -> Self {
        Self::CanonicalBuildPlan { source }
    }
}

impl From<zinder_wallet_rocksdb::RocksDbWalletError> for BenchError {
    fn from(source: zinder_wallet_rocksdb::RocksDbWalletError) -> Self {
        Self::WalletStorage { source }
    }
}

impl From<zinder_materialized_views::MaterializedViewStoreError> for BenchError {
    fn from(source: zinder_materialized_views::MaterializedViewStoreError) -> Self {
        Self::MaterializedView { source }
    }
}

impl BenchError {
    /// Wraps a filesystem failure with the path it targeted.
    #[must_use]
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    /// Builds a fixture-format error from a reason string.
    #[must_use]
    pub fn fixture_format(reason: impl Into<String>) -> Self {
        Self::FixtureFormat {
            reason: reason.into(),
        }
    }

    /// Builds a report-format error from a reason string.
    #[must_use]
    pub fn report_format(reason: impl Into<String>) -> Self {
        Self::ReportFormat {
            reason: reason.into(),
        }
    }

    /// Builds an invalid-argument error from a reason string.
    #[must_use]
    pub fn invalid_argument(reason: impl Into<String>) -> Self {
        Self::InvalidArgument {
            reason: reason.into(),
        }
    }

    /// Builds a replay-preparation task error from a join failure.
    #[must_use]
    pub fn canonical_replay_storage_preparation_task(reason: impl Into<String>) -> Self {
        Self::CanonicalReplayPreparationTask {
            reason: reason.into(),
        }
    }

    /// Builds an ordered replay-sequence validation error.
    #[must_use]
    pub fn canonical_replay_storage_sequence_mismatch(reason: impl Into<String>) -> Self {
        Self::CanonicalReplaySequenceMismatch {
            reason: reason.into(),
        }
    }

    /// Builds an engine-specific replay storage error without exposing its
    /// connection string or other secret-bearing configuration.
    #[must_use]
    pub fn canonical_replay_storage_candidate(
        candidate: &'static str,
        reason: impl Into<String>,
    ) -> Self {
        Self::CanonicalReplayCandidate {
            candidate,
            reason: reason.into(),
        }
    }

    /// Builds a hard-limit error for one measured acceptance boundary.
    #[must_use]
    pub fn acceptance_hard_limit_missed(boundary: impl Into<String>) -> Self {
        Self::AcceptanceHardLimitMissed {
            boundary: boundary.into(),
        }
    }

    /// Builds an error for missing thresholded-acceptance telemetry.
    #[must_use]
    pub fn acceptance_telemetry_missing(families: impl Into<String>) -> Self {
        Self::AcceptanceTelemetryMissing {
            families: families.into(),
        }
    }

    /// Builds an error for incomplete or mismatched fixture replay evidence.
    #[must_use]
    pub fn acceptance_completion_mismatch(reason: impl Into<String>) -> Self {
        Self::AcceptanceCompletionMismatch {
            reason: reason.into(),
        }
    }

    /// Builds an incomplete-materialized-view error from a cursor mismatch.
    #[must_use]
    pub fn materialized_view_build_incomplete(reason: impl Into<String>) -> Self {
        Self::MaterializedViewBuildIncomplete {
            reason: reason.into(),
        }
    }
}
