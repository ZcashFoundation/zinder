//! Typed readiness state shared by every Zinder service.
//!
//! The hand-written [`ReadinessCause`] enum below mirrors the proto-defined
//! `zinder.v1.ops.ReadinessCause` enum 1:1. The proto enum and
//! `docs/architecture/service-operations.md` are the documented source of
//! truth for the cause vocabulary.
//! The Rust enum carries the struct-variant payloads so the existing
//! `/readyz` JSON wire shape stays byte-identical; the [`ReadinessReport`]
//! type below converts to the proto message via [`Into`] for any gRPC
//! consumer.

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use serde::Serialize;
use tonic::{Request, Status, service::Interceptor};
use tonic_types::ErrorDetails;
use zinder_core::UnixTimestampMillis;
use zinder_proto::{
    status_with_reason_and_metadata,
    v1::{
        ingest as ingest_proto,
        ops::{self as ops_proto, ErrorReason},
    },
    wire::encode_reorg_window_exceeded_metadata,
};

use crate::composed_readiness::ReadinessInput;

/// Current phase of the canonical writer ([ADR-0015]).
///
/// `phase` is orthogonal to [`ReadinessCause`]: an ingest writer in
/// [`IngestPhase::BulkCatchup`] may report `cause=syncing` (normal) or
/// `cause=upstream_not_ready` (Zebra itself is behind). Non-ingest binaries
/// have no phase and serialize `phase = None`.
///
/// Wire shape: snake-case strings (`awaiting_upstream`, `bulk_catchup`,
/// `following_tip`) on both `/readyz` JSON and the proto
/// `zinder.v1.ingest.WriterPhase` enum. The wire shape is part of the
/// public contract; the Rust enum spelling is an implementation detail.
///
/// [ADR-0015]: ../../../docs/adrs/0015-phase-driven-ingest.md
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum IngestPhase {
    /// Upstream tip is at height zero (regtest near genesis or a freshly
    /// initialized node), so there is no committable chain yet. Only the
    /// `zinder-ingest probe` classifier reports this phase.
    AwaitingUpstream,
    /// The writer is constructing a fresh canonical store: pipelined source
    /// fetches and parallel block preparation load an inactive builder that
    /// publishes one authenticated baseline. `zinder-ingest probe` reports
    /// this phase when the gap exceeds
    /// `ingest.phase_classification.catchup_threshold_blocks`.
    BulkCatchup,
    /// The writer follows a published canonical store, committing one append
    /// or replacement per coherent source observation. `zinder-ingest probe`
    /// reports this phase when the gap is within the catch-up threshold.
    FollowingTip,
}

impl IngestPhase {
    /// Stable snake-case label used in metric label sets and structured
    /// log fields. Matches the JSON and proto wire shape.
    #[must_use]
    pub const fn wire_label(self) -> &'static str {
        match self {
            Self::AwaitingUpstream => "awaiting_upstream",
            Self::BulkCatchup => "bulk_catchup",
            Self::FollowingTip => "following_tip",
        }
    }
}

impl From<IngestPhase> for ingest_proto::WriterPhase {
    fn from(phase: IngestPhase) -> Self {
        match phase {
            IngestPhase::AwaitingUpstream => Self::AwaitingUpstream,
            IngestPhase::BulkCatchup => Self::BulkCatchup,
            IngestPhase::FollowingTip => Self::FollowingTip,
        }
    }
}

/// Stable readiness cause matching `docs/architecture/service-operations.md`.
///
/// Causes that carry operator-actionable detail use struct variants so the
/// data is reachable by `serde_json` consumers without an out-of-band lookup.
///
/// `Eq` is not derived because [`UpstreamNotReadyDetail::upstream_verification_progress`]
/// holds an `f64`; use [`PartialEq`] to compare causes.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ReadinessCause {
    /// Service is starting up but has not yet completed initialization.
    Starting,
    /// Service is catching up to the upstream node tip or replaying state.
    Syncing {
        /// Distance between the visible chain tip and the target tip, when
        /// known. `None` for read-only services with no observable tip.
        lag_blocks: Option<u64>,
    },
    /// Service is healthy and accepting production traffic.
    Ready,
    /// Upstream node source is unavailable.
    ///
    /// Long-running writer loops (canonical following, construction, mempool
    /// orchestrator) drain readiness here when an upstream source error
    /// classifies as recoverable. The payload carries the operator
    /// narrative the runbook expects: which class of upstream failure,
    /// the last reason, and how the outage is progressing.
    NodeUnavailable(NodeUnavailableDetail),
    /// A required node capability is missing.
    NodeCapabilityMissing {
        /// Stable name of the missing capability (matches
        /// `NodeCapability::name()` in `zinder-source`).
        capability: &'static str,
    },
    /// Canonical storage is unavailable.
    StorageUnavailable,
    /// Persisted store schema is incompatible with this binary.
    SchemaMismatch,
    /// Reorg replacement crossed the configured reorg window depth.
    ReorgWindowExceeded {
        /// Number of replaced visible heights.
        depth: u64,
        /// Configured reorg window in blocks.
        configured: u64,
        /// Replaceable depth the window covered when the replacement was
        /// refused. Below `configured` while a raised window is still growing
        /// toward it; `None` when the reporter did not supply it.
        #[serde(skip_serializing_if = "Option::is_none")]
        covered: Option<u64>,
    },
    /// The wallet projector refused its next following transition because it
    /// needs more logical bytes than `projector.follow.max_transition_logical_bytes`
    /// allows. The projector stays parked with the wallet unchanged until it
    /// restarts with a larger limit or a later canonical event changes the
    /// transition.
    WalletTransitionLimitExceeded {
        /// Logical bytes the transition had accounted when it was refused, a
        /// lower bound on what the complete transition needs.
        required_bytes: u64,
        /// Configured logical-byte limit for one following transition.
        configured_bytes: u64,
    },
    /// A `RocksDB` secondary reader is behind the primary beyond the configured threshold.
    ReplicaLagging {
        /// Chain-epoch distance between the writer and this secondary reader.
        lag_chain_epochs: u64,
    },
    /// A wallet-serving reader cannot refresh or re-match its published pair,
    /// but that pair is still exactly what the writer last attested and is
    /// within its staleness ceiling.
    ///
    /// Traffic stays admitted: the served chain view is a real admitted fence,
    /// only an older one. Crossing the ceiling replaces this warning with the
    /// fail-closed cause the underlying failure raises on its own.
    ServingPairStale {
        /// Chain-epoch distance between the writer and the published pair at
        /// the last successful writer-status fetch.
        lag_chain_epochs: u64,
        /// Seconds since the writer last attested the published pair.
        staleness_seconds: u64,
    },
    /// The private ingest writer-status RPC cannot be reached.
    WriterStatusUnavailable,
    /// The authenticated ingest-control channel or one of its required live
    /// health RPCs is temporarily unavailable.
    IngestControlUnavailable,
    /// The canonical writer has not confirmed its store's visible tip against
    /// its node since it opened the store, so its fence may sit on a branch
    /// the node abandoned. Clears once the writer observes that tip on the
    /// node's branch or commits past it.
    CanonicalTipUnconfirmed,
    /// Retained event history is approaching the configured cursor-expiry window.
    CursorAtRisk {
        /// Age of the oldest retained event, rounded down to whole hours.
        oldest_retained_age_hours: u64,
        /// Configured retention window, rounded down to whole hours.
        retention_hours: u64,
    },
    /// Service is shutting down and no longer accepting new traffic.
    ShuttingDown,
    /// Upstream node is reachable but reports it is itself behind the
    /// network tip. ADR-0015 §Upstream sync detection. Payload carries the
    /// signal source (`zebra_ready_endpoint` or
    /// `verification_progress_fallback`) and the sentinel string that
    /// triggered, so operators can triage from `/readyz` alone.
    UpstreamNotReady(UpstreamNotReadyDetail),
}

/// Operator-actionable narrative for [`ReadinessCause::NodeUnavailable`].
///
/// Surfaces in `/readyz` JSON, the proto report, and the
/// `zinder_readiness_state{cause="node_unavailable",class="..."}` Prometheus
/// label set. Dashboards and alert rules pivot on
/// [`Self::failure_class`]; humans reading the JSON payload also get
/// [`Self::last_reason`], a running count of [`Self::consecutive_failures`],
/// and an [`Self::outage_seconds`] elapsed counter.
///
/// The fields are populated by `zinder-ingest`'s recovery primitive
/// (`services/zinder-ingest/src/source_recovery.rs`); other services that
/// drain readiness for upstream errors should construct this through
/// [`ReadinessState::node_unavailable_with_detail`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct NodeUnavailableDetail {
    /// Stable kebab-case label naming the upstream failure class. Matches
    /// `SourceFailureClass::label()` in `zinder-source`.
    pub failure_class: &'static str,
    /// Sanitized one-line reason from the most recent failure. Bounded
    /// in length by the producing recovery primitive so the payload
    /// stays metric- and log-safe.
    pub last_reason: Cow<'static, str>,
    /// Consecutive failed iterations since the outage began. Resets to
    /// zero after a successful upstream response.
    pub consecutive_failures: u32,
    /// Seconds since the first failure of the current outage window.
    pub outage_seconds: u32,
}

/// Operator-actionable narrative for [`ReadinessCause::UpstreamNotReady`].
///
/// Surfaces in `/readyz` JSON, the proto report, and the
/// `zinder_readiness_state{cause="upstream_not_ready"}` Prometheus label
/// set. Carries the dual-path probe output: the upstream's reported
/// heights, the signal source label, and the sentinel string from
/// Zebra's `/ready` response or the fallback predicate name. See
/// [ADR-0015 §Upstream sync detection].
///
/// [ADR-0015 §Upstream sync detection]:
///     ../../../docs/adrs/0015-phase-driven-ingest.md#upstream-sync-detection
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UpstreamNotReadyDetail {
    /// Upstream's last committed tip height when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_committed_height: Option<u32>,
    /// Upstream's wall-clock-extrapolated estimate of network tip height
    /// when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_estimated_height: Option<u32>,
    /// Upstream's reported verification progress in `[0.0, 1.0]` when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_verification_progress: Option<f64>,
    /// Nested upstream-health diagnostic. The JSON shape `{source, reason}`
    /// is documented in the [Initial sync runbook]
    /// (`docs/runbooks/initial-sync.md`).
    pub upstream_health: UpstreamHealth,
}

/// Source + reason pair carried inside [`UpstreamNotReadyDetail`].
///
/// The JSON shape is `{ "source": "...", "reason": "..." }`; agents
/// parsing `/readyz` can build a closed dispatch table from the
/// enumerated source labels and the sentinel-string set documented in
/// ADR-0015.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct UpstreamHealth {
    /// Stable kebab-case label naming the signal source:
    /// `zebra_ready_endpoint` (HTTP `/ready` probe) or
    /// `verification_progress_fallback` (JSON-RPC derivation). A reader
    /// that relays the canonical writer's observation carries the writer's
    /// label.
    pub source: Cow<'static, str>,
    /// Sentinel string surfaced by the signal source. For
    /// `zebra_ready_endpoint` this is the body of Zebra's 503 response
    /// (`syncing`, `no tip`, `tip_age=<N>s`, `lag=<N> blocks`,
    /// `insufficient peers`); for `verification_progress_fallback` it is
    /// the predicate name that triggered.
    pub reason: Cow<'static, str>,
}

impl From<&UpstreamNotReadyDetail> for ops_proto::UpstreamNotReadyDetail {
    fn from(detail: &UpstreamNotReadyDetail) -> Self {
        Self {
            upstream_committed_height: detail.upstream_committed_height,
            upstream_estimated_height: detail.upstream_estimated_height,
            upstream_verification_progress: detail.upstream_verification_progress,
            upstream_health_source: detail.upstream_health.source.clone().into_owned(),
            upstream_health_reason: detail.upstream_health.reason.clone().into_owned(),
        }
    }
}

impl From<&ops_proto::UpstreamNotReadyDetail> for UpstreamNotReadyDetail {
    /// Relays the upstream observation another Zinder runtime reported, such
    /// as the `upstream_not_ready` the canonical writer carries in
    /// `IngestControl.WriterStatus`.
    fn from(detail: &ops_proto::UpstreamNotReadyDetail) -> Self {
        Self {
            upstream_committed_height: detail.upstream_committed_height,
            upstream_estimated_height: detail.upstream_estimated_height,
            upstream_verification_progress: detail.upstream_verification_progress,
            upstream_health: UpstreamHealth {
                source: Cow::Owned(detail.upstream_health_source.clone()),
                reason: Cow::Owned(detail.upstream_health_reason.clone()),
            },
        }
    }
}

impl NodeUnavailableDetail {
    /// Returns a detail snapshot for the first iteration of a new outage.
    ///
    /// Use this when a writer loop transitions out of `Ready`/`Syncing`
    /// because of an upstream failure. Subsequent iterations during the
    /// same outage should bump `consecutive_failures` and
    /// `outage_seconds` via [`Self::extend_with`].
    #[must_use]
    pub fn first_iteration(
        failure_class: &'static str,
        last_reason: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            failure_class,
            last_reason: last_reason.into(),
            consecutive_failures: 1,
            outage_seconds: 0,
        }
    }

    /// Returns a detail snapshot extending an ongoing outage.
    ///
    /// `consecutive_failures` advances by one (saturating) and
    /// `outage_seconds` is updated from the supplied elapsed duration.
    #[must_use]
    pub fn extend_with(
        previous: &Self,
        failure_class: &'static str,
        last_reason: impl Into<Cow<'static, str>>,
        outage_seconds: u32,
    ) -> Self {
        Self {
            failure_class,
            last_reason: last_reason.into(),
            consecutive_failures: previous.consecutive_failures.saturating_add(1),
            outage_seconds,
        }
    }
}

impl ReadinessCause {
    /// Every Prometheus label `metric_label` may return, in declaration order.
    ///
    /// `ops_endpoint::record_readiness_metrics` iterates this slice every
    /// scrape to zero out the gauges for inactive causes. The unit test
    /// `metric_label_is_listed_in_all_metric_labels` enforces that every
    /// `ReadinessCause` variant is represented; combined with the exhaustive
    /// `match` in [`Self::metric_label`], adding a new variant without
    /// extending this table fails CI.
    pub const ALL_METRIC_LABELS: &'static [&'static str] = &[
        "starting",
        "syncing",
        "ready",
        "node_unavailable",
        "node_capability_missing",
        "storage_unavailable",
        "schema_mismatch",
        "reorg_window_exceeded",
        "wallet_transition_limit_exceeded",
        "replica_lagging",
        "serving_pair_stale",
        "writer_status_unavailable",
        "ingest_control_unavailable",
        "canonical_tip_unconfirmed",
        "cursor_at_risk",
        "shutting_down",
        "upstream_not_ready",
    ];

    /// Stable Prometheus label for this readiness cause.
    #[must_use]
    pub const fn metric_label(&self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Syncing { .. } => "syncing",
            Self::Ready => "ready",
            Self::NodeUnavailable(_) => "node_unavailable",
            Self::NodeCapabilityMissing { .. } => "node_capability_missing",
            Self::StorageUnavailable => "storage_unavailable",
            Self::SchemaMismatch => "schema_mismatch",
            Self::ReorgWindowExceeded { .. } => "reorg_window_exceeded",
            Self::WalletTransitionLimitExceeded { .. } => "wallet_transition_limit_exceeded",
            Self::ReplicaLagging { .. } => "replica_lagging",
            Self::ServingPairStale { .. } => "serving_pair_stale",
            Self::WriterStatusUnavailable => "writer_status_unavailable",
            Self::IngestControlUnavailable => "ingest_control_unavailable",
            Self::CanonicalTipUnconfirmed => "canonical_tip_unconfirmed",
            Self::CursorAtRisk { .. } => "cursor_at_risk",
            Self::ShuttingDown => "shutting_down",
            Self::UpstreamNotReady(_) => "upstream_not_ready",
        }
    }

    /// Returns the source failure class label when the cause is
    /// [`Self::NodeUnavailable`].
    ///
    /// `None` for every other cause. Operators read this through the
    /// `class` Prometheus label on `zinder_readiness_state`; the empty
    /// string is rendered for inactive causes so the label set stays
    /// stable across scrapes.
    #[must_use]
    pub const fn node_failure_class_label(&self) -> Option<&'static str> {
        match self {
            Self::NodeUnavailable(detail) => Some(detail.failure_class),
            Self::Starting
            | Self::Syncing { .. }
            | Self::Ready
            | Self::NodeCapabilityMissing { .. }
            | Self::StorageUnavailable
            | Self::SchemaMismatch
            | Self::ReorgWindowExceeded { .. }
            | Self::WalletTransitionLimitExceeded { .. }
            | Self::ReplicaLagging { .. }
            | Self::ServingPairStale { .. }
            | Self::WriterStatusUnavailable
            | Self::IngestControlUnavailable
            | Self::CanonicalTipUnconfirmed
            | Self::CursorAtRisk { .. }
            | Self::ShuttingDown
            | Self::UpstreamNotReady(_) => None,
        }
    }

    /// Returns whether this cause still permits production traffic.
    ///
    /// Warning causes remain operator-actionable through `/readyz` and
    /// `zinder_readiness_state`, but they must not make load balancer probes
    /// fail while the service can still safely serve requests.
    #[must_use]
    pub const fn permits_traffic(&self) -> bool {
        matches!(
            self,
            Self::Ready | Self::CursorAtRisk { .. } | Self::ServingPairStale { .. }
        )
    }

    const fn preserves_observed_target(&self) -> bool {
        matches!(self, Self::CursorAtRisk { .. })
    }
}

/// Latest cause one dependency input of a [`crate::ComposedReadiness`]
/// reports.
///
/// A dependency is something the runtime keeps serving without, such as the
/// node behind broadcast. Its refusing cause leaves the traffic gate open and
/// appears in [`ReadinessReport::degraded`].
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ReadinessDependency {
    /// Dependency the cause describes.
    pub input: ReadinessInput,
    /// Latest cause the dependency's owner published.
    ///
    /// The JSON key is `dependency_cause`, so a `/readyz` parser that takes
    /// the first `"cause"` key reads the runtime's own cause.
    #[serde(rename = "dependency_cause")]
    pub cause: ReadinessCause,
}

impl ReadinessDependency {
    /// Returns whether the dependency is degraded: its cause does not admit
    /// traffic and is not a canonical writer's halt, which refuses traffic as
    /// the runtime's own cause instead.
    #[must_use]
    pub const fn is_degraded(&self) -> bool {
        !self.cause.permits_traffic() && self.cause.reorg_window_exceeded_detail().is_none()
    }
}

/// Snapshot of the current readiness state surfaced to operators.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ReadinessReport {
    /// `true` when the service is healthy enough to receive production traffic.
    pub is_ready: bool,
    /// Stable readiness cause.
    pub cause: ReadinessCause,
    /// Current visible chain height when known.
    pub current_height: Option<u32>,
    /// Node-observed target height when known.
    pub target_height: Option<u32>,
    /// Ingest loop phase ([ADR-0015]). `Some` only for `zinder-ingest`;
    /// reader binaries serialize `phase: null` (omitted from JSON via
    /// `skip_serializing_if`). Orthogonal to [`Self::cause`].
    ///
    /// [ADR-0015]: ../../../docs/adrs/0015-phase-driven-ingest.md
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<IngestPhase>,
    /// Consumer identities of the Explorer view store this service admitted.
    /// Empty when the service has not admitted one.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub materialized_view_identities: Vec<String>,
    /// Seconds since the canonical writer committed the chain epoch this
    /// reader serves, when the runtime serves one.
    ///
    /// A reader keeps serving its last chain view while its node or the
    /// writer stops advancing, so this age, not [`Self::is_ready`], shows how
    /// stale that view has become.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub served_epoch_age_seconds: Option<u64>,
    /// Dependencies the runtime keeps serving without whose latest cause does
    /// not admit traffic, in [`ReadinessInput`] declaration order. Each one
    /// fails only the requests that call it.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub degraded: Vec<ReadinessDependency>,
}

impl ReadinessReport {
    /// Returns a starting-state report carrying no chain heights yet.
    #[must_use]
    pub const fn starting() -> Self {
        Self {
            is_ready: false,
            cause: ReadinessCause::Starting,
            current_height: None,
            target_height: None,
            phase: None,
            materialized_view_identities: Vec::new(),
            served_epoch_age_seconds: None,
            degraded: Vec::new(),
        }
    }
}

/// Internal readiness state guarded by an `Arc<Mutex<_>>` so HTTP handlers
/// and runtime tasks can update and observe the same value.
#[derive(Clone, Debug)]
pub struct Readiness {
    inner: Arc<Mutex<RuntimeReadiness>>,
    materialized_view_identities: Arc<Mutex<Vec<String>>>,
}

/// One lock covers the state and the dependency causes a composition
/// projected with it, so a report never pairs a cause with dependency causes
/// from another publication.
#[derive(Clone, Debug)]
struct RuntimeReadiness {
    state: ReadinessState,
    dependencies: Vec<ReadinessDependency>,
    served_epoch_committed_at: Option<UnixTimestampMillis>,
}

impl Default for Readiness {
    fn default() -> Self {
        Self::new(ReadinessState::starting())
    }
}

impl Readiness {
    /// Creates a readiness handle seeded with `state`.
    #[must_use]
    pub fn new(state: ReadinessState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RuntimeReadiness {
                state,
                dependencies: Vec::new(),
                served_epoch_committed_at: None,
            })),
            materialized_view_identities: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Records the writer's commit time of the chain epoch this reader now
    /// serves, from which reports derive
    /// [`ReadinessReport::served_epoch_age_seconds`].
    pub fn set_served_epoch_committed_at(&self, committed_at: UnixTimestampMillis) {
        self.inner.lock().served_epoch_committed_at = Some(committed_at);
    }

    /// Replaces the state and dependency causes with one composition's
    /// projection.
    pub(crate) fn publish_composition(
        &self,
        state: ReadinessState,
        dependencies: Vec<ReadinessDependency>,
    ) {
        let mut guard = self.inner.lock();
        guard.state = state;
        guard.dependencies = dependencies;
    }

    /// Returns every dependency input's latest cause, degraded or not.
    pub(crate) fn dependencies(&self) -> Vec<ReadinessDependency> {
        self.inner.lock().dependencies.clone()
    }

    /// Records the consumer identities of the Explorer view store this
    /// service admitted, exposed by readiness and metrics.
    pub fn set_materialized_view_identities(&self, identities: Vec<String>) {
        *self.materialized_view_identities.lock() = identities;
    }

    /// Replaces the readiness cause and heights, preserving the writer phase
    /// and the last observed target across orthogonal warning states.
    ///
    /// `phase` is orthogonal to [`ReadinessCause`] and owned by the canonical
    /// writer via [`Self::set_phase`]; cause writers (upstream-outage backoff,
    /// retention warnings) build a fresh [`ReadinessState`] with no phase, so
    /// retaining the last-stamped phase keeps `/readyz` and the
    /// materialized-view replay phase gate stable between writer phase stamps. Retention and mempool warnings do not observe an
    /// upstream target, so they retain the preceding target through the
    /// warning and its transition back to ready. An explicit
    /// [`ReadinessState::with_phase`] on `state` overrides the phase.
    pub fn set(&self, state: ReadinessState) {
        let mut guard = self.inner.lock();
        guard.state = state.replacing(&guard.state);
    }

    /// Atomically mutates the current readiness state.
    ///
    /// Use when a write depends on the field values it does not change
    /// (for example: setting a new `cause` while preserving the
    /// concurrently-updated `current_height` or `phase`). The lock is
    /// held for the duration of `update`, so concurrent readers and
    /// writers see the pre- and post-state but never a torn read.
    pub fn update<F>(&self, update: F)
    where
        F: FnOnce(&mut ReadinessState),
    {
        update(&mut self.inner.lock().state);
    }

    /// Sets the [`IngestPhase`] on the current state without disturbing
    /// any other field. Used by the phase-driven ingest loop's per-iteration
    /// classifier stamp.
    pub fn set_phase(&self, phase: IngestPhase) {
        self.inner.lock().state.phase = Some(phase);
    }

    /// Returns the current [`IngestPhase`] without cloning the readiness cause.
    ///
    /// Cheaper than [`Self::report`] for hot readers that only need the phase,
    /// such as the materialized-view replay phase gate that samples it per replayed event.
    #[must_use]
    pub fn phase(&self) -> Option<IngestPhase> {
        self.inner.lock().state.phase
    }

    /// Returns the halt of a writer parked by a reorg deeper than its window,
    /// or `None` for any other cause.
    ///
    /// Cheaper than [`Self::report`] for hot readers that only check for the
    /// halt, such as open streams that poll it per page.
    #[must_use]
    pub fn reorg_window_exceeded(&self) -> Option<ops_proto::ReorgWindowExceededDetail> {
        self.inner.lock().state.cause.reorg_window_exceeded_detail()
    }

    /// Reports the current readiness as a serializable snapshot.
    #[must_use]
    pub fn report(&self) -> ReadinessReport {
        let RuntimeReadiness {
            state,
            dependencies,
            served_epoch_committed_at,
        } = self.inner.lock().clone();
        let materialized_view_identities = self.materialized_view_identities.lock().clone();
        ReadinessReport {
            is_ready: state.cause.permits_traffic(),
            cause: state.cause,
            current_height: state.current_height,
            target_height: state.target_height,
            phase: state.phase,
            materialized_view_identities,
            served_epoch_age_seconds: served_epoch_committed_at.map(|committed_at| {
                UnixTimestampMillis::now()
                    .value()
                    .saturating_sub(committed_at.value())
                    / 1_000
            }),
            degraded: dependencies
                .into_iter()
                .filter(ReadinessDependency::is_degraded)
                .collect(),
        }
    }
}

/// Rejects new gRPC requests while the shared runtime readiness state blocks traffic.
///
/// The interceptor samples readiness once when a request begins. Existing streaming
/// requests keep their established immutable view and may drain after readiness changes.
/// Warning states that explicitly permit traffic remain admitted.
#[derive(Clone, Debug)]
pub struct TrafficReadinessInterceptor {
    readiness: Readiness,
}

impl TrafficReadinessInterceptor {
    /// Creates a request gate over the service's shared readiness handle.
    #[must_use]
    pub const fn new(readiness: Readiness) -> Self {
        Self { readiness }
    }
}

impl Interceptor for TrafficReadinessInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let report = self.readiness.report();
        if report.is_ready {
            Ok(request)
        } else {
            Err(not_ready_status(&report.cause))
        }
    }
}

/// Builds the status a gRPC surface returns while `cause` keeps its
/// readiness gate closed.
///
/// `ErrorInfo.metadata["readiness_cause"]` always carries the stable
/// readiness label. A canonical writer parked by `reorg_window_exceeded`
/// maps to the typed `REORG_WINDOW_EXCEEDED` reason, with
/// `replacement_depth_blocks`, `reorg_window_blocks`, and, when known,
/// `covered_depth_blocks` metadata, because
/// retrying cannot succeed until an operator reconciles the store. Every
/// other cause maps to `SERVICE_NOT_READY`.
#[must_use]
pub fn not_ready_status(cause: &ReadinessCause) -> Status {
    let readiness_cause = cause.metric_label();
    let mut metadata = HashMap::from([("readiness_cause".to_owned(), readiness_cause.to_owned())]);
    if let Some(
        halt @ ops_proto::ReorgWindowExceededDetail {
            depth,
            configured,
            covered,
        },
    ) = cause.reorg_window_exceeded_detail()
    {
        metadata.extend(encode_reorg_window_exceeded_metadata(halt));
        let coverage = covered.map_or_else(
            || format!("its {configured}-block reorg window"),
            |covered| format!("the {covered} blocks its {configured}-block reorg window covered"),
        );
        return status_with_reason_and_metadata(
            ErrorReason::ReorgWindowExceeded,
            format!(
                "the canonical writer is parked: a {depth}-block replacement exceeds {coverage}, and an operator must reconcile the canonical store"
            ),
            ErrorDetails::new(),
            metadata,
        );
    }
    status_with_reason_and_metadata(
        ErrorReason::ServiceNotReady,
        format!("service is not ready to accept new traffic: {readiness_cause}"),
        ErrorDetails::new(),
        metadata,
    )
}

/// Mutable readiness state owned by the service's runtime task.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadinessState {
    /// Stable readiness cause.
    pub cause: ReadinessCause,
    /// Current visible chain height when known.
    pub current_height: Option<u32>,
    /// Node-observed target height when known.
    pub target_height: Option<u32>,
    /// Ingest loop phase ([ADR-0015]). Reader binaries leave this `None`
    /// for the life of the process. The ingest binary updates it on every
    /// classifier iteration via [`Self::with_phase`].
    ///
    /// [ADR-0015]: ../../../docs/adrs/0015-phase-driven-ingest.md
    pub phase: Option<IngestPhase>,
}

impl ReadinessState {
    /// Returns a starting state with no chain heights.
    #[must_use]
    pub const fn starting() -> Self {
        Self {
            cause: ReadinessCause::Starting,
            current_height: None,
            target_height: None,
            phase: None,
        }
    }

    /// Returns a ready state.
    ///
    /// `current_height` carries the visible chain tip when known. Read-only
    /// services that have not yet observed a chain epoch (e.g. an empty store)
    /// pass `None`; the `/readyz` and `/metrics` outputs then omit the height
    /// rather than report a fabricated `0`.
    #[must_use]
    pub const fn ready(current_height: Option<u32>) -> Self {
        Self {
            cause: ReadinessCause::Ready,
            current_height,
            target_height: current_height,
            phase: None,
        }
    }

    /// Returns a ready ingest state with the independently observed upstream
    /// target retained even when the allowed ready lag is nonzero.
    #[must_use]
    pub const fn ready_with_target(
        current_height: Option<u32>,
        target_height: Option<u32>,
    ) -> Self {
        Self {
            cause: ReadinessCause::Ready,
            current_height,
            target_height,
            phase: None,
        }
    }

    /// Returns a syncing state.
    ///
    /// `lag_blocks` is the distance between the visible chain tip and the
    /// target tip; pass `None` when the service has no observable target
    /// (for example, a read-only query node with no upstream node handle).
    #[must_use]
    pub const fn syncing(
        lag_blocks: Option<u64>,
        current_height: Option<u32>,
        target_height: Option<u32>,
    ) -> Self {
        Self {
            cause: ReadinessCause::Syncing { lag_blocks },
            current_height,
            target_height,
            phase: None,
        }
    }

    /// Returns a not-ready state for a non-paramatric failure cause.
    ///
    /// For parametric causes ([`ReadinessCause::Syncing`],
    /// [`ReadinessCause::ReorgWindowExceeded`],
    /// [`ReadinessCause::ReplicaLagging`],
    /// [`ReadinessCause::NodeUnavailable`]) use the dedicated constructors.
    #[must_use]
    pub fn not_ready(cause: ReadinessCause) -> Self {
        Self {
            cause,
            current_height: None,
            target_height: None,
            phase: None,
        }
    }

    /// Returns an upstream-node-unavailable state with an operator-actionable
    /// narrative.
    ///
    /// `current_height` carries the last visible tip when the writer can
    /// still read local storage while waiting for the upstream node to
    /// recover. `detail` carries the failure narrative the runbook expects
    /// in `/readyz`.
    #[must_use]
    pub fn node_unavailable_with_detail(
        detail: NodeUnavailableDetail,
        current_height: Option<u32>,
    ) -> Self {
        Self {
            cause: ReadinessCause::NodeUnavailable(detail),
            current_height,
            target_height: None,
            phase: None,
        }
    }

    /// Returns a reorg-window-exceeded state.
    ///
    /// `current_height` carries the visible tip at the time of failure so the
    /// `/readyz` response includes the chain height the operator should
    /// reconcile against.
    #[must_use]
    ///
    /// `covered` is the replaceable depth the window covered when the
    /// replacement was refused, which is below `configured` while a raised
    /// window is still growing toward it.
    pub const fn reorg_window_exceeded(
        depth: u64,
        configured: u64,
        covered: Option<u64>,
        current_height: Option<u32>,
    ) -> Self {
        Self {
            cause: ReadinessCause::ReorgWindowExceeded {
                depth,
                configured,
                covered,
            },
            current_height,
            target_height: None,
            phase: None,
        }
    }

    /// Returns the state of a wallet projector parked on a following
    /// transition over its logical-byte limit.
    ///
    /// `required_bytes` is the lower bound the refused transition had
    /// accounted, `configured_bytes` the limit it exceeded. `current_height`
    /// carries the unchanged wallet tip and `target_height` the writer tip
    /// the wallet cannot reach.
    #[must_use]
    pub const fn wallet_transition_limit_exceeded(
        required_bytes: u64,
        configured_bytes: u64,
        current_height: Option<u32>,
        target_height: Option<u32>,
    ) -> Self {
        Self {
            cause: ReadinessCause::WalletTransitionLimitExceeded {
                required_bytes,
                configured_bytes,
            },
            current_height,
            target_height,
            phase: None,
        }
    }

    /// Returns a node-capability-missing state.
    ///
    /// `capability` is the stable diagnostic name of the missing capability,
    /// matching the names returned by `NodeCapability::name()`.
    #[must_use]
    pub const fn node_capability_missing(capability: &'static str) -> Self {
        Self {
            cause: ReadinessCause::NodeCapabilityMissing { capability },
            current_height: None,
            target_height: None,
            phase: None,
        }
    }

    /// Returns a replica-lagging state for secondary readers.
    #[must_use]
    pub const fn replica_lagging(lag_chain_epochs: u64, current_height: Option<u32>) -> Self {
        Self {
            cause: ReadinessCause::ReplicaLagging { lag_chain_epochs },
            current_height,
            target_height: None,
            phase: None,
        }
    }

    /// Returns a stale-serving-pair warning for wallet-serving readers.
    ///
    /// `current_height` carries the visible tip of the published pair, which
    /// is the chain view the reader keeps answering from while the warning is
    /// active.
    #[must_use]
    pub const fn serving_pair_stale(
        lag_chain_epochs: u64,
        staleness_seconds: u64,
        current_height: Option<u32>,
    ) -> Self {
        Self {
            cause: ReadinessCause::ServingPairStale {
                lag_chain_epochs,
                staleness_seconds,
            },
            current_height,
            target_height: None,
            phase: None,
        }
    }

    /// Returns a cursor-at-risk state for event retention.
    #[must_use]
    pub const fn cursor_at_risk(
        oldest_retained_age_hours: u64,
        retention_hours: u64,
        current_height: Option<u32>,
    ) -> Self {
        Self {
            cause: ReadinessCause::CursorAtRisk {
                oldest_retained_age_hours,
                retention_hours,
            },
            current_height,
            target_height: current_height,
            phase: None,
        }
    }

    /// Returns an upstream-not-ready state with the dual-path probe
    /// payload.
    ///
    /// `current_height` carries the writer's last visible tip so
    /// operators see how far Zinder has committed while waiting for
    /// Zebra to catch up. `detail` carries the signal-source label and
    /// sentinel string per [ADR-0015 §Upstream sync detection].
    ///
    /// [ADR-0015 §Upstream sync detection]:
    ///     ../../../docs/adrs/0015-phase-driven-ingest.md#upstream-sync-detection
    #[must_use]
    pub fn upstream_not_ready_with_detail(
        detail: UpstreamNotReadyDetail,
        current_height: Option<u32>,
    ) -> Self {
        Self {
            cause: ReadinessCause::UpstreamNotReady(detail),
            current_height,
            target_height: None,
            phase: None,
        }
    }

    /// Returns `self` as the replacement of `previous`, keeping the
    /// last-stamped phase when `self` carries none and the observed target
    /// across a warning that does not observe one. [`Readiness::set`] applies
    /// this rule.
    pub(crate) fn replacing(mut self, previous: &Self) -> Self {
        let warning_replaces_chain_state = self.cause.preserves_observed_target();
        let warning_cleared_to_ready = matches!(&self.cause, ReadinessCause::Ready)
            && previous.cause.preserves_observed_target();
        if warning_replaces_chain_state || warning_cleared_to_ready {
            self.target_height = previous.target_height;
        }
        self.phase = self.phase.or(previous.phase);
        self
    }

    /// Returns a copy of `self` tagged with the supplied ingest loop
    /// phase. Ingest binaries chain this onto every state transition;
    /// reader binaries never call it.
    #[must_use]
    pub fn with_phase(mut self, phase: IngestPhase) -> Self {
        self.phase = Some(phase);
        self
    }
}

impl From<&ReadinessCause> for ops_proto::ReadinessCause {
    fn from(cause: &ReadinessCause) -> Self {
        match cause {
            ReadinessCause::Starting => Self::Starting,
            ReadinessCause::Syncing { .. } => Self::Syncing,
            ReadinessCause::Ready => Self::Ready,
            ReadinessCause::NodeUnavailable(_) => Self::NodeUnavailable,
            ReadinessCause::NodeCapabilityMissing { .. } => Self::NodeCapabilityMissing,
            ReadinessCause::StorageUnavailable => Self::StorageUnavailable,
            ReadinessCause::SchemaMismatch => Self::SchemaMismatch,
            ReadinessCause::ReorgWindowExceeded { .. } => Self::ReorgWindowExceeded,
            ReadinessCause::WalletTransitionLimitExceeded { .. } => {
                Self::WalletTransitionLimitExceeded
            }
            ReadinessCause::ReplicaLagging { .. } => Self::ReplicaLagging,
            ReadinessCause::ServingPairStale { .. } => Self::ServingPairStale,
            ReadinessCause::WriterStatusUnavailable => Self::WriterStatusUnavailable,
            ReadinessCause::IngestControlUnavailable => Self::IngestControlUnavailable,
            ReadinessCause::CanonicalTipUnconfirmed => Self::CanonicalTipUnconfirmed,
            ReadinessCause::CursorAtRisk { .. } => Self::CursorAtRisk,
            ReadinessCause::ShuttingDown => Self::ShuttingDown,
            ReadinessCause::UpstreamNotReady(_) => Self::UpstreamNotReady,
        }
    }
}

impl From<ReadinessCause> for ops_proto::ReadinessCause {
    fn from(cause: ReadinessCause) -> Self {
        Self::from(&cause)
    }
}

impl From<ops_proto::ReorgWindowExceededDetail> for ReadinessCause {
    /// Maps a parked writer's reported halt to the cause a reader projects.
    fn from(detail: ops_proto::ReorgWindowExceededDetail) -> Self {
        Self::ReorgWindowExceeded {
            depth: detail.depth,
            configured: detail.configured,
            covered: detail.covered,
        }
    }
}

impl ReadinessCause {
    /// Returns the halt a parked writer reports for this cause, or `None`
    /// for every cause other than [`ReadinessCause::ReorgWindowExceeded`].
    #[must_use]
    pub const fn reorg_window_exceeded_detail(
        &self,
    ) -> Option<ops_proto::ReorgWindowExceededDetail> {
        if let Self::ReorgWindowExceeded {
            depth,
            configured,
            covered,
        } = *self
        {
            Some(ops_proto::ReorgWindowExceededDetail {
                depth,
                configured,
                covered,
            })
        } else {
            None
        }
    }
}

impl From<&ReadinessCause> for Option<ops_proto::ReadinessCauseDetail> {
    #[allow(
        clippy::too_many_lines,
        reason = "one-payload-per-cause dispatch reads as a single auditable contract; splitting it would scatter the cause-to-detail mapping across helpers without simplifying any single arm"
    )]
    fn from(cause: &ReadinessCause) -> Self {
        let payload = match cause {
            ReadinessCause::Syncing { lag_blocks } => {
                ops_proto::readiness_cause_detail::Payload::Syncing(ops_proto::SyncingDetail {
                    lag_blocks: *lag_blocks,
                })
            }
            ReadinessCause::NodeCapabilityMissing { capability } => {
                ops_proto::readiness_cause_detail::Payload::NodeCapabilityMissing(
                    ops_proto::NodeCapabilityMissingDetail {
                        capability: (*capability).to_owned(),
                    },
                )
            }
            ReadinessCause::ReorgWindowExceeded {
                depth,
                configured,
                covered,
            } => ops_proto::readiness_cause_detail::Payload::ReorgWindowExceeded(
                ops_proto::ReorgWindowExceededDetail {
                    depth: *depth,
                    configured: *configured,
                    covered: *covered,
                },
            ),
            ReadinessCause::WalletTransitionLimitExceeded {
                required_bytes,
                configured_bytes,
            } => ops_proto::readiness_cause_detail::Payload::WalletTransitionLimitExceeded(
                ops_proto::WalletTransitionLimitExceededDetail {
                    required_bytes: *required_bytes,
                    configured_bytes: *configured_bytes,
                },
            ),
            ReadinessCause::ReplicaLagging { lag_chain_epochs } => {
                ops_proto::readiness_cause_detail::Payload::ReplicaLagging(
                    ops_proto::ReplicaLaggingDetail {
                        lag_chain_epochs: *lag_chain_epochs,
                    },
                )
            }
            ReadinessCause::ServingPairStale {
                lag_chain_epochs,
                staleness_seconds,
            } => ops_proto::readiness_cause_detail::Payload::ServingPairStale(
                ops_proto::ServingPairStaleDetail {
                    lag_chain_epochs: *lag_chain_epochs,
                    staleness_seconds: *staleness_seconds,
                },
            ),
            ReadinessCause::CursorAtRisk {
                oldest_retained_age_hours,
                retention_hours,
            } => ops_proto::readiness_cause_detail::Payload::CursorAtRisk(
                ops_proto::CursorAtRiskDetail {
                    oldest_retained_age_hours: *oldest_retained_age_hours,
                    retention_hours: *retention_hours,
                },
            ),
            ReadinessCause::NodeUnavailable(detail) => {
                ops_proto::readiness_cause_detail::Payload::NodeUnavailable(
                    ops_proto::NodeUnavailableDetail {
                        failure_class: detail.failure_class.to_owned(),
                        last_reason: detail.last_reason.clone().into_owned(),
                        consecutive_failures: detail.consecutive_failures,
                        outage_seconds: detail.outage_seconds,
                    },
                )
            }
            ReadinessCause::UpstreamNotReady(detail) => {
                ops_proto::readiness_cause_detail::Payload::UpstreamNotReady(
                    ops_proto::UpstreamNotReadyDetail::from(detail),
                )
            }
            ReadinessCause::Starting
            | ReadinessCause::Ready
            | ReadinessCause::StorageUnavailable
            | ReadinessCause::SchemaMismatch
            | ReadinessCause::WriterStatusUnavailable
            | ReadinessCause::IngestControlUnavailable
            | ReadinessCause::CanonicalTipUnconfirmed
            | ReadinessCause::ShuttingDown => return None,
        };
        Some(ops_proto::ReadinessCauseDetail {
            payload: Some(payload),
        })
    }
}

impl From<&ReadinessDependency> for ops_proto::ReadinessDependency {
    fn from(dependency: &ReadinessDependency) -> Self {
        Self {
            input: dependency.input.wire_label().to_owned(),
            cause: ops_proto::ReadinessCause::from(&dependency.cause) as i32,
            detail: Option::<ops_proto::ReadinessCauseDetail>::from(&dependency.cause),
        }
    }
}

impl From<&ReadinessReport> for ops_proto::ReadinessReport {
    fn from(report: &ReadinessReport) -> Self {
        Self {
            cause: ops_proto::ReadinessCause::from(&report.cause) as i32,
            current_height: report.current_height,
            target_height: report.target_height,
            detail: Option::<ops_proto::ReadinessCauseDetail>::from(&report.cause),
            materialized_view_identities: report.materialized_view_identities.clone(),
            degraded: report
                .degraded
                .iter()
                .map(ops_proto::ReadinessDependency::from)
                .collect(),
            served_epoch_age_seconds: report.served_epoch_age_seconds,
        }
    }
}

impl From<ReadinessReport> for ops_proto::ReadinessReport {
    fn from(report: ReadinessReport) -> Self {
        Self::from(&report)
    }
}

#[cfg(test)]
mod tests {
    use tonic_types::StatusExt as _;

    use super::*;

    #[test]
    fn report_marks_ready_for_ready_cause() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let report = readiness.report();
        assert!(report.is_ready);
        assert!(matches!(report.cause, ReadinessCause::Ready));
        assert_eq!(report.current_height, Some(10));
    }

    #[test]
    fn traffic_interceptor_tracks_blocking_and_warning_readiness()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = Readiness::default();
        let mut interceptor = TrafficReadinessInterceptor::new(readiness.clone());
        let blocked = interceptor
            .call(Request::new(()))
            .err()
            .ok_or("starting readiness must reject new traffic")?;
        assert_eq!(blocked.code(), tonic::Code::Unavailable);
        let starting_error_details = blocked.get_error_details();
        let starting_error_info = starting_error_details
            .error_info()
            .ok_or("readiness rejection must carry ErrorInfo")?;
        assert_eq!(
            starting_error_info.reason,
            ErrorReason::ServiceNotReady.as_str_name()
        );
        assert_eq!(
            starting_error_info.metadata.get("readiness_cause"),
            Some(&"starting".to_owned())
        );

        readiness.set(ReadinessState::ready(Some(100)));
        interceptor.call(Request::new(()))?;

        readiness.set(ReadinessState::cursor_at_risk(145, 168, Some(100)));
        interceptor.call(Request::new(()))?;

        readiness.set(ReadinessState::serving_pair_stale(2, 42, Some(100)));
        interceptor.call(Request::new(()))?;

        readiness.set(ReadinessState::replica_lagging(3, Some(100)));
        let blocked = interceptor
            .call(Request::new(()))
            .err()
            .ok_or("replica lag readiness must reject new traffic")?;
        assert_eq!(blocked.code(), tonic::Code::Unavailable);
        let replica_error_details = blocked.get_error_details();
        let replica_error_info = replica_error_details
            .error_info()
            .ok_or("readiness rejection must carry ErrorInfo")?;
        assert_eq!(
            replica_error_info.metadata.get("readiness_cause"),
            Some(&"replica_lagging".to_owned())
        );

        readiness.set(ReadinessState::not_ready(ReadinessCause::SchemaMismatch));
        let blocked = interceptor
            .call(Request::new(()))
            .err()
            .ok_or("schema mismatch readiness must reject new traffic")?;
        assert_eq!(blocked.code(), tonic::Code::Unavailable);

        readiness.set(ReadinessState::reorg_window_exceeded(
            1_001,
            1_000,
            Some(100),
            Some(100),
        ));
        let halted = interceptor
            .call(Request::new(()))
            .err()
            .ok_or("a parked writer must reject new traffic")?;
        assert_eq!(halted.code(), tonic::Code::FailedPrecondition);
        let halted_error_details = halted.get_error_details();
        let halted_error_info = halted_error_details
            .error_info()
            .ok_or("a halt rejection must carry ErrorInfo")?;
        assert_eq!(
            halted_error_info.reason,
            ErrorReason::ReorgWindowExceeded.as_str_name()
        );
        for (key, expected) in [
            ("readiness_cause", "reorg_window_exceeded"),
            ("replacement_depth_blocks", "1001"),
            ("reorg_window_blocks", "1000"),
            ("covered_depth_blocks", "100"),
        ] {
            assert_eq!(
                halted_error_info.metadata.get(key).map(String::as_str),
                Some(expected)
            );
        }
        Ok(())
    }

    #[test]
    fn serving_pair_stale_admits_traffic_and_carries_lag_and_staleness()
    -> Result<(), serde_json::Error> {
        let readiness = Readiness::new(ReadinessState::serving_pair_stale(7, 42, Some(100)));
        let report = readiness.report();
        assert!(report.is_ready);
        assert_eq!(report.current_height, Some(100));
        assert_eq!(report.cause.metric_label(), "serving_pair_stale");

        let rendered = serde_json::to_value(&report)?;
        let cause = &rendered["cause"]["serving_pair_stale"];
        assert_eq!(cause["lag_chain_epochs"], 7);
        assert_eq!(cause["staleness_seconds"], 42);

        let proto = ops_proto::ReadinessReport::from(&report);
        assert_eq!(
            proto.cause,
            ops_proto::ReadinessCause::ServingPairStale as i32
        );
        let Some(detail) = proto.detail else {
            unreachable!("ServingPairStale must carry detail")
        };
        let Some(payload) = detail.payload else {
            unreachable!("detail must carry a payload")
        };
        let ops_proto::readiness_cause_detail::Payload::ServingPairStale(payload) = payload else {
            unreachable!("expected ServingPairStale payload variant")
        };
        assert_eq!(payload.lag_chain_epochs, 7);
        assert_eq!(payload.staleness_seconds, 42);
        Ok(())
    }

    #[test]
    fn ingest_phase_serializes_snake_case() -> Result<(), serde_json::Error> {
        for (phase, expected) in [
            (IngestPhase::AwaitingUpstream, "\"awaiting_upstream\""),
            (IngestPhase::BulkCatchup, "\"bulk_catchup\""),
            (IngestPhase::FollowingTip, "\"following_tip\""),
        ] {
            let rendered = serde_json::to_string(&phase)?;
            assert_eq!(
                rendered, expected,
                "wire shape for {phase:?} must be snake_case"
            );
            assert_eq!(phase.wire_label(), expected.trim_matches('"'));
        }
        Ok(())
    }

    #[test]
    fn report_carries_phase_when_set() {
        let state = ReadinessState::syncing(Some(3), Some(10), Some(13))
            .with_phase(IngestPhase::BulkCatchup);
        let readiness = Readiness::new(state);
        let report = readiness.report();
        assert_eq!(report.phase, Some(IngestPhase::BulkCatchup));
    }

    #[test]
    fn report_omits_phase_for_reader_binaries() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let report = readiness.report();
        assert_eq!(report.phase, None);
    }

    #[test]
    fn report_json_includes_phase_when_set() -> Result<(), serde_json::Error> {
        let state = ReadinessState::syncing(Some(3), Some(10), Some(13))
            .with_phase(IngestPhase::FollowingTip);
        let readiness = Readiness::new(state);
        let rendered = serde_json::to_value(readiness.report())?;
        assert_eq!(rendered["phase"], "following_tip");
        Ok(())
    }

    #[test]
    fn report_json_omits_phase_when_unset() -> Result<(), serde_json::Error> {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let rendered = serde_json::to_value(readiness.report())?;
        assert!(
            rendered.get("phase").is_none(),
            "reader binaries must not surface a `phase` field; got {rendered}"
        );
        Ok(())
    }

    #[test]
    fn materialized_view_identities_survive_readiness_state_changes()
    -> Result<(), serde_json::Error> {
        let readiness = Readiness::default();
        readiness.set_materialized_view_identities(vec![
            "block_summary".to_owned(),
            "transparent_outpoint_spend".to_owned(),
        ]);
        readiness.set(ReadinessState::ready(Some(10)));

        let report = readiness.report();
        assert_eq!(report.materialized_view_identities.len(), 2);
        let rendered = serde_json::to_value(report)?;
        assert!(rendered.get("materialized_view_preset").is_none());
        assert_eq!(
            rendered["materialized_view_identities"]
                .as_array()
                .map(Vec::len),
            Some(2)
        );
        Ok(())
    }

    #[test]
    fn upstream_not_ready_serializes_full_substructure() -> Result<(), serde_json::Error> {
        let detail = UpstreamNotReadyDetail {
            upstream_committed_height: Some(600_000),
            upstream_estimated_height: Some(4_016_431),
            upstream_verification_progress: Some(0.149),
            upstream_health: UpstreamHealth {
                source: Cow::Borrowed("zebra_ready_endpoint"),
                reason: Cow::Borrowed("syncing"),
            },
        };
        let state = ReadinessState::upstream_not_ready_with_detail(detail, Some(600_000))
            .with_phase(IngestPhase::FollowingTip);
        let report = Readiness::new(state).report();
        let rendered = serde_json::to_value(&report)?;
        assert_eq!(rendered["phase"], "following_tip");
        let cause = &rendered["cause"]["upstream_not_ready"];
        assert_eq!(cause["upstream_committed_height"], 600_000);
        assert_eq!(cause["upstream_estimated_height"], 4_016_431);
        assert_eq!(cause["upstream_verification_progress"], 0.149);
        assert_eq!(cause["upstream_health"]["source"], "zebra_ready_endpoint");
        assert_eq!(cause["upstream_health"]["reason"], "syncing");
        Ok(())
    }

    #[test]
    fn upstream_not_ready_does_not_permit_traffic() {
        let detail = UpstreamNotReadyDetail {
            upstream_committed_height: None,
            upstream_estimated_height: None,
            upstream_verification_progress: None,
            upstream_health: UpstreamHealth {
                source: Cow::Borrowed("verification_progress_fallback"),
                reason: Cow::Borrowed("verification_progress_below_floor"),
            },
        };
        let state = ReadinessState::upstream_not_ready_with_detail(detail, None);
        let report = Readiness::new(state).report();
        assert!(!report.is_ready);
        assert_eq!(report.cause.metric_label(), "upstream_not_ready");
    }

    #[test]
    fn ingest_phase_maps_to_writer_phase_proto() {
        assert_eq!(
            ingest_proto::WriterPhase::from(IngestPhase::AwaitingUpstream),
            ingest_proto::WriterPhase::AwaitingUpstream
        );
        assert_eq!(
            ingest_proto::WriterPhase::from(IngestPhase::BulkCatchup),
            ingest_proto::WriterPhase::BulkCatchup
        );
        assert_eq!(
            ingest_proto::WriterPhase::from(IngestPhase::FollowingTip),
            ingest_proto::WriterPhase::FollowingTip
        );
    }

    #[test]
    fn warning_causes_remain_ready_for_traffic() {
        let report = Readiness::new(ReadinessState::cursor_at_risk(145, 168, Some(100))).report();
        assert!(report.is_ready);
        assert_eq!(report.current_height, Some(100));
    }

    #[test]
    fn ready_without_height_omits_chain_heights() {
        let readiness = Readiness::new(ReadinessState::ready(None));
        let report = readiness.report();
        assert!(report.is_ready);
        assert_eq!(report.current_height, None);
        assert_eq!(report.target_height, None);
    }

    #[test]
    fn report_marks_not_ready_for_syncing_cause() {
        let readiness = Readiness::new(ReadinessState::syncing(Some(5), Some(5), Some(10)));
        let report = readiness.report();
        assert!(!report.is_ready);
        assert!(matches!(
            report.cause,
            ReadinessCause::Syncing {
                lag_blocks: Some(5)
            }
        ));
        assert_eq!(report.current_height, Some(5));
        assert_eq!(report.target_height, Some(10));
    }

    #[test]
    fn reorg_window_exceeded_carries_depth_and_configured() {
        let readiness = Readiness::new(ReadinessState::reorg_window_exceeded(
            12,
            10,
            Some(11),
            Some(100),
        ));
        let report = readiness.report();
        assert!(!report.is_ready);
        assert!(matches!(
            report.cause,
            ReadinessCause::ReorgWindowExceeded {
                depth: 12,
                configured: 10,
                covered: Some(11),
            }
        ));
        assert_eq!(report.current_height, Some(100));
        assert_eq!(
            readiness.reorg_window_exceeded(),
            Some(ops_proto::ReorgWindowExceededDetail {
                depth: 12,
                configured: 10,
                covered: Some(11),
            })
        );
        readiness.set(ReadinessState::ready(Some(100)));
        assert_eq!(readiness.reorg_window_exceeded(), None);
    }

    #[test]
    fn wallet_transition_limit_exceeded_refuses_traffic_and_carries_both_byte_counts()
    -> Result<(), serde_json::Error> {
        let readiness = Readiness::new(ReadinessState::wallet_transition_limit_exceeded(
            541_122_672,
            536_870_912,
            Some(4_602_673),
            Some(4_602_680),
        ));
        let report = readiness.report();
        assert!(!report.is_ready);
        assert_eq!(report.current_height, Some(4_602_673));
        assert_eq!(report.target_height, Some(4_602_680));
        assert_eq!(
            report.cause.metric_label(),
            "wallet_transition_limit_exceeded"
        );

        let rendered = serde_json::to_value(&report)?;
        assert_eq!(
            rendered["cause"],
            serde_json::json!({
                "wallet_transition_limit_exceeded": {
                    "required_bytes": 541_122_672_u64,
                    "configured_bytes": 536_870_912_u64,
                }
            })
        );

        let proto = ops_proto::ReadinessReport::from(&report);
        assert_eq!(
            proto.cause,
            ops_proto::ReadinessCause::WalletTransitionLimitExceeded as i32
        );
        assert_eq!(
            proto.detail.and_then(|detail| detail.payload),
            Some(
                ops_proto::readiness_cause_detail::Payload::WalletTransitionLimitExceeded(
                    ops_proto::WalletTransitionLimitExceededDetail {
                        required_bytes: 541_122_672,
                        configured_bytes: 536_870_912,
                    }
                )
            )
        );

        let refusal = not_ready_status(&report.cause);
        assert_eq!(refusal.code(), tonic::Code::Unavailable);
        let refusal_details = refusal.get_error_details();
        assert_eq!(
            refusal_details
                .error_info()
                .and_then(|error_info| error_info.metadata.get("readiness_cause"))
                .map(String::as_str),
            Some("wallet_transition_limit_exceeded")
        );
        Ok(())
    }

    #[test]
    fn replica_lagging_carries_lag_chain_epochs() {
        let readiness = Readiness::new(ReadinessState::replica_lagging(4, Some(100)));
        let report = readiness.report();
        assert!(!report.is_ready);
        assert!(matches!(
            report.cause,
            ReadinessCause::ReplicaLagging {
                lag_chain_epochs: 4
            }
        ));
        assert_eq!(report.current_height, Some(100));
    }

    #[test]
    fn cursor_at_risk_carries_retention_window() {
        let readiness = Readiness::new(ReadinessState::cursor_at_risk(145, 168, Some(100)));
        let report = readiness.report();
        assert!(report.is_ready);
        assert!(matches!(
            report.cause,
            ReadinessCause::CursorAtRisk {
                oldest_retained_age_hours: 145,
                retention_hours: 168
            }
        ));
        assert_eq!(report.current_height, Some(100));
        assert_eq!(report.target_height, Some(100));
    }

    #[test]
    fn metric_label_is_listed_in_all_metric_labels() {
        // Every constructed variant's metric_label must appear in
        // ALL_METRIC_LABELS, and the table cardinality must equal the
        // variant count so a new variant cannot silently drop a gauge.
        let every_cause: &[ReadinessCause] = &[
            ReadinessCause::Starting,
            ReadinessCause::Syncing { lag_blocks: None },
            ReadinessCause::Ready,
            ReadinessCause::NodeUnavailable(NodeUnavailableDetail::first_iteration(
                "node_unreachable",
                "test reason",
            )),
            ReadinessCause::NodeCapabilityMissing { capability: "test" },
            ReadinessCause::StorageUnavailable,
            ReadinessCause::SchemaMismatch,
            ReadinessCause::ReorgWindowExceeded {
                depth: 0,
                configured: 0,
                covered: None,
            },
            ReadinessCause::WalletTransitionLimitExceeded {
                required_bytes: 0,
                configured_bytes: 0,
            },
            ReadinessCause::ReplicaLagging {
                lag_chain_epochs: 0,
            },
            ReadinessCause::ServingPairStale {
                lag_chain_epochs: 0,
                staleness_seconds: 0,
            },
            ReadinessCause::WriterStatusUnavailable,
            ReadinessCause::IngestControlUnavailable,
            ReadinessCause::CanonicalTipUnconfirmed,
            ReadinessCause::CursorAtRisk {
                oldest_retained_age_hours: 0,
                retention_hours: 0,
            },
            ReadinessCause::ShuttingDown,
            ReadinessCause::UpstreamNotReady(UpstreamNotReadyDetail {
                upstream_committed_height: None,
                upstream_estimated_height: None,
                upstream_verification_progress: None,
                upstream_health: UpstreamHealth {
                    source: Cow::Borrowed("zebra_ready_endpoint"),
                    reason: Cow::Borrowed("syncing"),
                },
            }),
        ];
        for cause in every_cause {
            let label = cause.metric_label();
            assert!(
                ReadinessCause::ALL_METRIC_LABELS.contains(&label),
                "metric_label {label} for {cause:?} missing from ALL_METRIC_LABELS",
            );
        }
        assert_eq!(
            every_cause.len(),
            ReadinessCause::ALL_METRIC_LABELS.len(),
            "ALL_METRIC_LABELS cardinality must equal ReadinessCause variant count",
        );
    }

    fn proto_cause_for(cause: &ReadinessCause) -> ops_proto::ReadinessCause {
        match cause {
            ReadinessCause::Starting => ops_proto::ReadinessCause::Starting,
            ReadinessCause::Syncing { .. } => ops_proto::ReadinessCause::Syncing,
            ReadinessCause::Ready => ops_proto::ReadinessCause::Ready,
            ReadinessCause::NodeUnavailable(_) => ops_proto::ReadinessCause::NodeUnavailable,
            ReadinessCause::NodeCapabilityMissing { .. } => {
                ops_proto::ReadinessCause::NodeCapabilityMissing
            }
            ReadinessCause::StorageUnavailable => ops_proto::ReadinessCause::StorageUnavailable,
            ReadinessCause::SchemaMismatch => ops_proto::ReadinessCause::SchemaMismatch,
            ReadinessCause::ReorgWindowExceeded { .. } => {
                ops_proto::ReadinessCause::ReorgWindowExceeded
            }
            ReadinessCause::WalletTransitionLimitExceeded { .. } => {
                ops_proto::ReadinessCause::WalletTransitionLimitExceeded
            }
            ReadinessCause::ReplicaLagging { .. } => ops_proto::ReadinessCause::ReplicaLagging,
            ReadinessCause::ServingPairStale { .. } => ops_proto::ReadinessCause::ServingPairStale,
            ReadinessCause::WriterStatusUnavailable => {
                ops_proto::ReadinessCause::WriterStatusUnavailable
            }
            ReadinessCause::IngestControlUnavailable => {
                ops_proto::ReadinessCause::IngestControlUnavailable
            }
            ReadinessCause::CanonicalTipUnconfirmed => {
                ops_proto::ReadinessCause::CanonicalTipUnconfirmed
            }
            ReadinessCause::CursorAtRisk { .. } => ops_proto::ReadinessCause::CursorAtRisk,
            ReadinessCause::ShuttingDown => ops_proto::ReadinessCause::ShuttingDown,
            ReadinessCause::UpstreamNotReady(_) => ops_proto::ReadinessCause::UpstreamNotReady,
        }
    }

    #[test]
    fn proto_cause_maps_every_variant() {
        for cause in every_rust_cause() {
            assert_eq!(
                ops_proto::ReadinessCause::from(&cause),
                proto_cause_for(&cause),
                "Rust cause {cause:?} mapped to the wrong proto code"
            );
        }
    }

    fn every_rust_cause() -> Vec<ReadinessCause> {
        vec![
            ReadinessCause::Starting,
            ReadinessCause::Syncing { lag_blocks: None },
            ReadinessCause::Ready,
            ReadinessCause::NodeUnavailable(NodeUnavailableDetail::first_iteration(
                "node_unreachable",
                "test reason",
            )),
            ReadinessCause::NodeCapabilityMissing {
                capability: "tx_broadcast",
            },
            ReadinessCause::StorageUnavailable,
            ReadinessCause::SchemaMismatch,
            ReadinessCause::ReorgWindowExceeded {
                depth: 0,
                configured: 0,
                covered: None,
            },
            ReadinessCause::WalletTransitionLimitExceeded {
                required_bytes: 0,
                configured_bytes: 0,
            },
            ReadinessCause::ReplicaLagging {
                lag_chain_epochs: 0,
            },
            ReadinessCause::ServingPairStale {
                lag_chain_epochs: 0,
                staleness_seconds: 0,
            },
            ReadinessCause::WriterStatusUnavailable,
            ReadinessCause::IngestControlUnavailable,
            ReadinessCause::CanonicalTipUnconfirmed,
            ReadinessCause::CursorAtRisk {
                oldest_retained_age_hours: 0,
                retention_hours: 0,
            },
            ReadinessCause::ShuttingDown,
            ReadinessCause::UpstreamNotReady(UpstreamNotReadyDetail {
                upstream_committed_height: None,
                upstream_estimated_height: None,
                upstream_verification_progress: None,
                upstream_health: UpstreamHealth {
                    source: Cow::Borrowed("zebra_ready_endpoint"),
                    reason: Cow::Borrowed("syncing"),
                },
            }),
        ]
    }

    #[test]
    fn proto_report_preserves_payload_for_parametric_causes() {
        let report = ReadinessReport {
            is_ready: false,
            cause: ReadinessCause::ReorgWindowExceeded {
                depth: 12,
                configured: 10,
                covered: Some(10),
            },
            current_height: Some(100),
            target_height: None,
            phase: None,
            materialized_view_identities: vec!["transparent_outpoint_spend".to_owned()],
            served_epoch_age_seconds: None,
            degraded: Vec::new(),
        };

        let proto = ops_proto::ReadinessReport::from(&report);
        assert_eq!(
            proto.cause,
            ops_proto::ReadinessCause::ReorgWindowExceeded as i32
        );
        assert_eq!(proto.current_height, Some(100));
        assert_eq!(proto.target_height, None);
        assert_eq!(
            proto.materialized_view_identities,
            ["transparent_outpoint_spend"]
        );

        let Some(detail) = proto.detail else {
            unreachable!("parametric cause must carry detail")
        };
        let Some(payload) = detail.payload else {
            unreachable!("detail must carry a payload")
        };
        let ops_proto::readiness_cause_detail::Payload::ReorgWindowExceeded(payload) = payload
        else {
            unreachable!("expected ReorgWindowExceeded payload variant")
        };
        assert_eq!(payload.depth, 12);
        assert_eq!(payload.configured, 10);
        assert_eq!(payload.covered, Some(10));
    }

    #[test]
    fn proto_report_carries_no_detail_for_scalar_causes() {
        let report = ReadinessReport {
            is_ready: false,
            cause: ReadinessCause::StorageUnavailable,
            current_height: None,
            target_height: None,
            phase: None,
            materialized_view_identities: Vec::new(),
            served_epoch_age_seconds: None,
            degraded: Vec::new(),
        };

        let proto = ops_proto::ReadinessReport::from(&report);
        assert_eq!(
            proto.cause,
            ops_proto::ReadinessCause::StorageUnavailable as i32
        );
        assert!(proto.detail.is_none());
    }

    #[test]
    fn proto_report_preserves_node_unavailable_payload() {
        let report = ReadinessReport {
            is_ready: false,
            cause: ReadinessCause::NodeUnavailable(NodeUnavailableDetail {
                failure_class: "upstream_view_changed",
                last_reason: Cow::Borrowed("block height not in best chain"),
                consecutive_failures: 3,
                outage_seconds: 12,
            }),
            current_height: Some(4_013_801),
            target_height: None,
            phase: None,
            materialized_view_identities: Vec::new(),
            served_epoch_age_seconds: None,
            degraded: Vec::new(),
        };

        let proto = ops_proto::ReadinessReport::from(&report);
        assert_eq!(
            proto.cause,
            ops_proto::ReadinessCause::NodeUnavailable as i32
        );
        let Some(detail) = proto.detail else {
            unreachable!("NodeUnavailable must carry detail")
        };
        let Some(payload) = detail.payload else {
            unreachable!("detail must carry a payload")
        };
        let ops_proto::readiness_cause_detail::Payload::NodeUnavailable(payload) = payload else {
            unreachable!("expected NodeUnavailable payload variant")
        };
        assert_eq!(payload.failure_class, "upstream_view_changed");
        assert_eq!(payload.last_reason, "block height not in best chain");
        assert_eq!(payload.consecutive_failures, 3);
        assert_eq!(payload.outage_seconds, 12);
    }

    #[test]
    fn node_failure_class_label_exposes_active_class() {
        let cause = ReadinessCause::NodeUnavailable(NodeUnavailableDetail::first_iteration(
            "node_unreachable",
            "connection refused",
        ));
        assert_eq!(cause.node_failure_class_label(), Some("node_unreachable"));
        assert_eq!(ReadinessCause::Ready.node_failure_class_label(), None);
    }

    #[test]
    fn report_lists_degraded_dependencies_and_relays_them_on_the_wire()
    -> Result<(), serde_json::Error> {
        let runtime = Readiness::default();
        let composed = crate::ComposedReadiness::with_dependency_inputs(
            runtime.clone(),
            &[ReadinessInput::NodeSource, ReadinessInput::IngestControl],
        );
        composed.publish_admission(ReadinessState::ready(Some(42)));
        composed.publish_input(ReadinessInput::NodeSource, ReadinessCause::Ready);
        composed.publish_input(ReadinessInput::IngestControl, ReadinessCause::Ready);
        let healthy = serde_json::to_value(runtime.report())?;
        assert!(healthy.get("degraded").is_none(), "{healthy}");

        composed.publish_input(
            ReadinessInput::NodeSource,
            ReadinessCause::NodeUnavailable(NodeUnavailableDetail::first_iteration(
                "node_unreachable",
                "connection refused",
            )),
        );
        let report = runtime.report();
        assert!(report.is_ready);
        let rendered = serde_json::to_value(&report)?;
        assert_eq!(rendered["cause"], "ready");
        assert_eq!(
            rendered["degraded"],
            serde_json::json!([{
                "input": "node_source",
                "dependency_cause": {"node_unavailable": {
                    "failure_class": "node_unreachable",
                    "last_reason": "connection refused",
                    "consecutive_failures": 1,
                    "outage_seconds": 0,
                }},
            }])
        );

        let proto = ops_proto::ReadinessReport::from(&report);
        assert_eq!(proto.cause, ops_proto::ReadinessCause::Ready as i32);
        assert_eq!(
            proto.degraded,
            vec![ops_proto::ReadinessDependency {
                input: "node_source".to_owned(),
                cause: ops_proto::ReadinessCause::NodeUnavailable as i32,
                detail: Some(ops_proto::ReadinessCauseDetail {
                    payload: Some(ops_proto::readiness_cause_detail::Payload::NodeUnavailable(
                        ops_proto::NodeUnavailableDetail {
                            failure_class: "node_unreachable".to_owned(),
                            last_reason: "connection refused".to_owned(),
                            consecutive_failures: 1,
                            outage_seconds: 0,
                        }
                    )),
                }),
            }]
        );
        Ok(())
    }

    #[test]
    fn report_derives_served_epoch_age_from_the_writer_commit_time() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        assert_eq!(readiness.report().served_epoch_age_seconds, None);
        let committed_at =
            UnixTimestampMillis::new(UnixTimestampMillis::now().value().saturating_sub(90_500));
        readiness.set_served_epoch_committed_at(committed_at);
        readiness.set(ReadinessState::ready(Some(11)));
        let report = readiness.report();
        let age = report.served_epoch_age_seconds.unwrap_or_default();
        assert!((90..=92).contains(&age), "served epoch age {age}");
        assert_eq!(
            ops_proto::ReadinessReport::from(&report).served_epoch_age_seconds,
            Some(age)
        );
    }

    #[test]
    fn writer_upstream_observation_converts_back_to_its_readiness_detail() {
        let detail = UpstreamNotReadyDetail {
            upstream_committed_height: Some(600_000),
            upstream_estimated_height: Some(4_016_431),
            upstream_verification_progress: Some(0.149),
            upstream_health: UpstreamHealth {
                source: Cow::Borrowed("zebra_ready_endpoint"),
                reason: Cow::Borrowed("syncing"),
            },
        };
        let relayed =
            UpstreamNotReadyDetail::from(&ops_proto::UpstreamNotReadyDetail::from(&detail));
        assert_eq!(relayed, detail);
    }

    #[test]
    fn set_replaces_current_state() {
        let readiness = Readiness::default();
        assert!(matches!(readiness.report().cause, ReadinessCause::Starting));
        readiness.set(ReadinessState::ready(Some(7)));
        assert!(matches!(readiness.report().cause, ReadinessCause::Ready));
    }

    #[test]
    fn set_preserves_stamped_phase() {
        let readiness = Readiness::default();
        readiness.set_phase(IngestPhase::BulkCatchup);

        readiness.set(ReadinessState::syncing(Some(5), Some(10), Some(15)));
        assert_eq!(readiness.report().phase, Some(IngestPhase::BulkCatchup));

        readiness.set(ReadinessState::node_unavailable_with_detail(
            NodeUnavailableDetail::first_iteration("node_unreachable", "connection refused"),
            Some(10),
        ));
        assert_eq!(readiness.report().phase, Some(IngestPhase::BulkCatchup));
    }

    #[test]
    fn set_with_explicit_phase_overrides_stamped_phase() {
        let readiness = Readiness::default();
        readiness.set_phase(IngestPhase::BulkCatchup);

        readiness.set(ReadinessState::ready(Some(20)).with_phase(IngestPhase::FollowingTip));
        assert_eq!(readiness.report().phase, Some(IngestPhase::FollowingTip));
    }

    #[test]
    fn ready_and_warning_transitions_preserve_observed_upstream_target() {
        let readiness = Readiness::new(
            ReadinessState::ready_with_target(Some(100), Some(105))
                .with_phase(IngestPhase::FollowingTip),
        );

        readiness.set(ReadinessState::cursor_at_risk(145, 168, Some(100)));
        assert_eq!(readiness.report().target_height, Some(105));

        readiness.set(ReadinessState::ready(Some(100)));
        assert_eq!(readiness.report().target_height, Some(105));
    }

    #[test]
    fn set_leaves_phase_unset_for_reader_binaries() {
        let readiness = Readiness::default();
        readiness.set(ReadinessState::ready(Some(3)));
        assert_eq!(readiness.report().phase, None);
    }
}
