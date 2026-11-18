//! Upstream sync-health probe output.
//!
//! [`UpstreamHealthSnapshot`] is the source-boundary value produced by
//! [`crate::NodeSource::poll_upstream_health`]. Writers translate the
//! snapshot to a `cause=upstream_not_ready` readiness state per
//! [ADR-0009 §Upstream sync detection].
//!
//! [ADR-0009 §Upstream sync detection]:
//!     ../../../docs/adrs/0009-ingest-lifecycle-and-construction.md#upstream-sync-detection

use std::borrow::Cow;

/// Source label written when the snapshot comes from Zebra's HTTP
/// `/ready` endpoint.
pub const UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT: &str = "zebra_ready_endpoint";

/// Source label written when the snapshot comes from the JSON-RPC
/// `getblockchaininfo` fallback (`verificationprogress` +
/// `estimatedheight`).
pub const UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK: &str =
    "verification_progress_fallback";

/// Sentinel returned by Zebra's `/ready` endpoint when upstream is
/// answering-ready (HTTP 200 body `ok`).
pub const UPSTREAM_HEALTH_REASON_OK: &str = "ok";

/// Sentinel returned by Zebra's `/ready` endpoint when upstream lacks
/// enough peers.
pub const UPSTREAM_HEALTH_REASON_INSUFFICIENT_PEERS: &str = "insufficient peers";

/// Sentinel returned by Zebra's `/ready` endpoint while the node is
/// initial-syncing.
pub const UPSTREAM_HEALTH_REASON_SYNCING: &str = "syncing";

/// Sentinel returned by Zebra's `/ready` endpoint when no chain tip is
/// known yet.
pub const UPSTREAM_HEALTH_REASON_NO_TIP: &str = "no tip";

/// Reason emitted by the JSON-RPC fallback when
/// `verificationprogress < verification_progress_floor`.
pub const UPSTREAM_HEALTH_REASON_VERIFICATION_PROGRESS_BELOW_FLOOR: &str =
    "verification_progress_below_floor";

/// Reason emitted by the JSON-RPC fallback when
/// `estimated_height - blocks > estimated_gap_floor_blocks`.
pub const UPSTREAM_HEALTH_REASON_ESTIMATED_GAP_ABOVE_FLOOR: &str = "estimated_gap_above_floor";

/// Snapshot of the upstream-sync probe at one observation moment.
///
/// `ready_for_queries` is the single bit operators care about. The
/// surrounding fields carry the structured detail surfaced on
/// `/readyz` when the writer transitions to the
/// `cause=upstream_not_ready` payload owned by `zinder-runtime`. A
/// successful probe still carries `source` and `reason` so logs and
/// metrics see the signal source on every iteration.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub struct UpstreamHealthSnapshot {
    /// True when upstream is at network tip with enough peers and the
    /// writer should not gate readiness.
    pub ready_for_queries: bool,
    /// Stable kebab-case label naming the signal source. Use the
    /// `UPSTREAM_HEALTH_SOURCE_*` constants when constructing.
    pub source: &'static str,
    /// Sentinel string carried into the readiness payload. For
    /// `zebra_ready_endpoint` this is the body of Zebra's `/ready`
    /// response; for `verification_progress_fallback` it is the
    /// predicate name that triggered.
    pub reason: Cow<'static, str>,
    /// Upstream's last committed tip height when known.
    pub upstream_committed_height: Option<u32>,
    /// Upstream's wall-clock-extrapolated estimate of network tip height
    /// when known.
    pub upstream_estimated_height: Option<u32>,
    /// Upstream's reported verification progress in `[0.0, 1.0]` when
    /// known.
    pub upstream_verification_progress: Option<f64>,
}

impl UpstreamHealthSnapshot {
    /// Returns a `ready_for_queries = true` snapshot tagged with the
    /// supplied signal source.
    #[must_use]
    pub fn ready(
        source: &'static str,
        upstream_committed_height: Option<u32>,
        upstream_estimated_height: Option<u32>,
        upstream_verification_progress: Option<f64>,
    ) -> Self {
        Self {
            ready_for_queries: true,
            source,
            reason: Cow::Borrowed(UPSTREAM_HEALTH_REASON_OK),
            upstream_committed_height,
            upstream_estimated_height,
            upstream_verification_progress,
        }
    }

    /// Returns a `ready_for_queries = false` snapshot tagged with the
    /// supplied signal source and sentinel reason.
    #[must_use]
    pub fn not_ready(
        source: &'static str,
        reason: impl Into<Cow<'static, str>>,
        upstream_committed_height: Option<u32>,
        upstream_estimated_height: Option<u32>,
        upstream_verification_progress: Option<f64>,
    ) -> Self {
        Self {
            ready_for_queries: false,
            source,
            reason: reason.into(),
            upstream_committed_height,
            upstream_estimated_height,
            upstream_verification_progress,
        }
    }
}
