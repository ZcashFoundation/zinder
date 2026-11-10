//! Ingest error vocabulary and retrying node-source fetches.
//!
//! The canonical writer's construction pipeline fetches source chain segments
//! through [`fetch_chain_segment_with_retry`], which retries transient
//! transport failures within a per-call deadline and a per-run failure budget.
//! [`IngestError`] is the error type for ingest-side workers that are not part
//! of the canonical writer's own error vocabulary.

use std::time::{Duration, Instant};

use thiserror::Error;
use zinder_core::{BlockHeight, CommitmentTreeAccumulatorError, TransactionId};
use zinder_source::{
    NodeSource, SourceChainSegment, SourceChainSegmentLimits, SourceError, SourceFailureClass,
};
use zinder_store::{CanonicalStoreError, StoreError};

use crate::CanonicalBlockConstructionError;

const FETCH_RETRY_MAX_ATTEMPTS: u32 = 5;
#[cfg(not(test))]
const FETCH_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
#[cfg(test)]
const FETCH_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(1);
const FETCH_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(5);
const FETCH_RETRY_FAILURE_BUDGET: u32 = 100;

/// Supported node source kinds for ingestion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeSourceKind {
    /// Zebra JSON-RPC source.
    ZebraJsonRpc,
}

/// Error returned by ingestion operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IngestError {
    /// Requested node source name is not supported.
    #[error("unknown node source: {node_source}")]
    UnknownNodeSource {
        /// User-provided node source name.
        node_source: String,
    },

    /// Background source segment fetch task stopped before returning a result.
    #[error("source segment fetch task stopped unexpectedly: {reason}")]
    SourceSegmentFetchTaskStopped {
        /// Task join failure description.
        reason: String,
    },

    /// Materialized-view plane dispatch (consumer apply or store write) failed.
    #[error("materialized-view dispatch failed: {0}")]
    MaterializedViewDispatch(String),

    /// Materialized-view store open or operation failed.
    #[error(transparent)]
    MaterializedViewStore(#[from] zinder_materialized_views::MaterializedViewStoreError),

    /// A persisted materialized-view cursor is not a canonical event cursor.
    #[error(
        "materialized-view consumer cursors in {path} are not canonical event cursors; delete that directory and let the tailer rebuild it"
    )]
    MaterializedViewCursorUnreadable {
        /// Materialized-view store directory holding the undecodable cursor.
        path: std::path::PathBuf,
        /// Exact decode failure.
        #[source]
        source: CanonicalStoreError,
    },

    /// Materialized views were requested on a store built from a checkpoint.
    #[error(
        "materialized views require canonical history from height 1; this store starts at {first_available_height:?}"
    )]
    MaterializedViewHistoryIncomplete {
        /// First height the canonical store retains.
        first_available_height: BlockHeight,
    },

    /// A non-coinbase transparent input references an unresolvable previous output.
    #[error(
        "transparent prevout {transaction_id:?}:{output_index} cannot be resolved from canonical storage: {reason}"
    )]
    TransparentPrevoutUnresolved {
        /// Transaction that created the referenced output.
        transaction_id: TransactionId,
        /// Output index referenced by the spending transaction input.
        output_index: u32,
        /// Exact resolution failure.
        reason: &'static str,
    },

    /// The canonical commitment-tree checkpoint required to seed derivation is absent.
    #[error("canonical commitment-tree checkpoint is absent at or before height {height:?}")]
    CommitmentTreeCheckpointMissing {
        /// Height whose predecessor state was required.
        height: BlockHeight,
    },

    /// The ordered commitment-tree accumulator rejected a seed or block update.
    #[error("canonical commitment-tree state failed at height {height:?}")]
    CommitmentTreeState {
        /// Height being seeded or applied.
        height: BlockHeight,
        /// Exact accumulator failure.
        #[source]
        source: CommitmentTreeAccumulatorError,
    },

    /// Retryable node failures exceeded the per-run ingest budget.
    #[error(
        "ingest source retry budget exceeded during {operation}: {retryable_failures} failures, budget {failure_budget}"
    )]
    SourceRetryBudgetExceeded {
        /// Source operation being retried.
        operation: String,
        /// Retryable failures observed in this run.
        retryable_failures: u32,
        /// Configured retryable failure budget.
        failure_budget: u32,
    },

    /// Retryable node failures outlasted the per-block fetch deadline.
    #[error("ingest source retry deadline exceeded during {operation}: {reason}")]
    SourceRetryDeadlineExceeded {
        /// Source operation being retried.
        operation: String,
        /// Last retryable failure reason.
        reason: String,
    },

    /// Node source failed.
    #[error(transparent)]
    Source(#[from] SourceError),

    /// Canonical block construction failed.
    #[error(transparent)]
    CanonicalBlockConstruction(#[from] CanonicalBlockConstructionError),

    /// Canonical store failed.
    #[error(transparent)]
    Store(#[from] StoreError),

    /// Canonical storage failed.
    #[error(transparent)]
    CanonicalStore(#[from] CanonicalStoreError),

    /// A `spawn_blocking` task hosting a `RocksDB` call failed to join
    /// (panic or runtime shutdown).
    #[error("blocking storage task failed to join: {reason}")]
    BlockingTaskFailed {
        /// Reason from `tokio::task::JoinError::to_string`.
        reason: String,
    },
}

/// Counter of retryable source failures observed across a single ingest run.
#[derive(Default)]
pub(crate) struct IngestRetryState {
    retryable_failures: u32,
}

/// Fetches a bounded source-chain segment with the same retry policy as
/// single-block fetches.
pub(crate) async fn fetch_chain_segment_with_retry<Source>(
    request_timeout: Duration,
    source: &Source,
    limits: SourceChainSegmentLimits,
    retry_state: &mut IngestRetryState,
) -> Result<SourceChainSegment, IngestError>
where
    Source: NodeSource,
{
    let started_at = Instant::now();
    let operation = limits.cursor.next_connected_height().map_or_else(
        || "fetch source chain segment after cursor".to_owned(),
        |height| format!("fetch source chain segment from height {}", height.value()),
    );
    let _active = SourceFetchActiveGauge::enter("fetch_chain_segment");
    let source_outcome = retry_source_request(
        "fetch_chain_segment",
        operation,
        request_timeout,
        retry_state,
        || async { source.fetch_chain_segment(limits).await },
    )
    .await;
    record_ingest_source_outcome("fetch_chain_segment", started_at, &source_outcome);
    if let Ok(segment) = &source_outcome {
        let stats = segment.stats();
        metrics::counter!("zinder_ingest_source_segment_connected_blocks_total")
            .increment(u64::from(stats.connected_blocks()));
        metrics::histogram!("zinder_ingest_source_segment_max_blocks")
            .record(usize_to_u32_saturating(segment.len()));
        metrics::histogram!("zinder_ingest_source_segment_response_payload_bytes")
            .record(u64_to_f64(stats.response_payload_bytes()));
    }
    source_outcome
}

async fn retry_source_request<RequestResult, Fut, MakeRequest>(
    operation_label: &'static str,
    operation: String,
    request_timeout: Duration,
    retry_state: &mut IngestRetryState,
    mut request: MakeRequest,
) -> Result<RequestResult, IngestError>
where
    MakeRequest: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<RequestResult, SourceError>>,
{
    let started_at = Instant::now();
    let deadline = fetch_retry_deadline(request_timeout);
    let mut attempt = 1;
    let mut next_backoff = FETCH_RETRY_INITIAL_BACKOFF;

    loop {
        match request().await {
            Ok(response) => return Ok(response),
            Err(error) if per_call_retry_permitted(&error) => {
                retry_state.retryable_failures = retry_state.retryable_failures.saturating_add(1);
                metrics::counter!(
                    "zinder_ingest_source_retry_total",
                    "operation" => operation_label
                )
                .increment(1);
                if retry_state.retryable_failures > FETCH_RETRY_FAILURE_BUDGET {
                    return Err(IngestError::SourceRetryBudgetExceeded {
                        operation,
                        retryable_failures: retry_state.retryable_failures,
                        failure_budget: FETCH_RETRY_FAILURE_BUDGET,
                    });
                }

                if attempt >= FETCH_RETRY_MAX_ATTEMPTS
                    || started_at.elapsed().saturating_add(next_backoff) > deadline
                {
                    return Err(IngestError::SourceRetryDeadlineExceeded {
                        operation,
                        reason: error.to_string(),
                    });
                }

                tokio::time::sleep(next_backoff).await;
                next_backoff = next_fetch_retry_backoff(next_backoff);
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(IngestError::Source(error)),
        }
    }
}

fn fetch_retry_deadline(request_timeout: Duration) -> Duration {
    request_timeout
        .saturating_mul(FETCH_RETRY_MAX_ATTEMPTS)
        .max(Duration::from_secs(5))
}

fn next_fetch_retry_backoff(current_backoff: Duration) -> Duration {
    current_backoff
        .saturating_mul(2)
        .min(FETCH_RETRY_MAX_BACKOFF)
}

/// Returns whether `retry_source_request` should sleep and re-issue the
/// same call.
///
/// Per-call retry handles the failure classes where retrying the *same
/// request* can plausibly succeed: transient transport
/// ([`SourceFailureClass::NodeUnreachable`]) and short-lived subscription
/// failures ([`SourceFailureClass::StreamDisconnected`]). View-stale
/// failures ([`SourceFailureClass::UpstreamViewChanged`]) and structural
/// failures are bubbled up to the loop, which re-observes the upstream
/// before issuing dependent requests.
fn per_call_retry_permitted(error: &SourceError) -> bool {
    matches!(
        error.upstream_classification(),
        SourceFailureClass::NodeUnreachable | SourceFailureClass::StreamDisconnected,
    )
}

fn record_ingest_source_outcome<Response>(
    operation: &'static str,
    started_at: Instant,
    source_outcome: &Result<Response, IngestError>,
) {
    metrics::histogram!(
        "zinder_ingest_source_request_duration_seconds",
        "operation" => operation,
        "status" => outcome_status(source_outcome),
        "error_class" => ingest_error_class(source_outcome.as_ref().err())
    )
    .record(started_at.elapsed());
    metrics::counter!(
        "zinder_ingest_source_request_total",
        "operation" => operation,
        "status" => outcome_status(source_outcome),
        "error_class" => ingest_error_class(source_outcome.as_ref().err())
    )
    .increment(1);
}

/// In-flight gauge for source-side fetches, partitioned by `operation`.
///
/// The gauge tracks how many source requests are currently awaiting an
/// upstream response. Increment fires when the guard enters scope and the
/// decrement fires from `Drop`, so cancellation, timeout, and panic all
/// release the gauge without an explicit early-return path.
struct SourceFetchActiveGauge {
    operation: &'static str,
}

impl SourceFetchActiveGauge {
    fn enter(operation: &'static str) -> Self {
        metrics::gauge!(
            "zinder_ingest_source_fetch_active",
            "operation" => operation,
        )
        .increment(1.0);
        Self { operation }
    }
}

impl Drop for SourceFetchActiveGauge {
    fn drop(&mut self) {
        metrics::gauge!(
            "zinder_ingest_source_fetch_active",
            "operation" => self.operation,
        )
        .decrement(1.0);
    }
}

pub(crate) const fn outcome_status<T, E>(outcome: &Result<T, E>) -> &'static str {
    if outcome.is_ok() { "ok" } else { "error" }
}

pub(crate) fn ingest_error_class(error: Option<&IngestError>) -> &'static str {
    match error {
        None => "none",
        Some(IngestError::UnknownNodeSource { .. }) => "unknown_node_source",
        Some(IngestError::SourceSegmentFetchTaskStopped { .. }) => {
            "source_segment_fetch_task_stopped"
        }
        Some(IngestError::SourceRetryBudgetExceeded { .. }) => "source_retry_budget_exceeded",
        Some(IngestError::SourceRetryDeadlineExceeded { .. }) => "source_retry_deadline_exceeded",
        Some(IngestError::Source(_)) => "source",
        Some(IngestError::CanonicalBlockConstruction(_)) => "canonical_block_construction",
        Some(IngestError::Store(_)) => "store",
        Some(IngestError::CanonicalStore(_)) => "canonical_store",
        Some(IngestError::BlockingTaskFailed { .. }) => "blocking_task_failed",
        Some(IngestError::MaterializedViewDispatch(_)) => "materialized_view_dispatch",
        Some(IngestError::MaterializedViewStore(_)) => "materialized_view_store",
        Some(IngestError::MaterializedViewCursorUnreadable { .. }) => {
            "materialized_view_cursor_unreadable"
        }
        Some(IngestError::MaterializedViewHistoryIncomplete { .. }) => {
            "materialized_view_history_incomplete"
        }
        Some(IngestError::TransparentPrevoutUnresolved { .. }) => "transparent_prevout_unresolved",
        Some(IngestError::CommitmentTreeCheckpointMissing { .. }) => {
            "commitment_tree_checkpoint_missing"
        }
        Some(IngestError::CommitmentTreeState { .. }) => "commitment_tree_state",
    }
}

fn usize_to_u32_saturating(amount: usize) -> u32 {
    u32::try_from(amount).map_or(u32::MAX, |converted| converted)
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus gauges use f64 samples; chain progress values are diagnostic"
)]
fn u64_to_f64(sample: u64) -> f64 {
    sample as f64
}
