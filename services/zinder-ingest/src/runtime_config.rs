//! Resolved ingest configuration types shared by the canonical writer and
//! its historical-work gate.
//!
//! [`IngestRuntimeConfig`] is the seam between `services/zinder-ingest/src/config.rs`'s
//! TOML schema and the runtime; [`HistoricalWorkGate`] is the process-local
//! admission gate that decides when rebuildable historical work may run.

use std::{
    num::{NonZeroU32, NonZeroU64},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;
use zinder_core::BlockHeight;
use zinder_runtime::{IngestPhase, Readiness};
use zinder_source::{MempoolSourceAdmissionLimits, NodeTarget};

use crate::{CanonicalPipelineLimits, NodeSourceKind, RawBlobPolicy};

const BACKGROUND_WORK_PHASE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Default lag threshold (in blocks) below which the canonical follower
/// reports `Ready`.
pub const DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS: u64 = 1;

/// Process-local admission gate for rebuildable historical work.
///
/// Canonical bulk catch-up owns the storage budget first. Once canonical is
/// following tip, materialized-view replay owns it until every canonical block is
/// materialized. Historical backfills and verifiers may run only after both
/// conditions hold. Keeping this state out of [`Readiness`] avoids exposing an
/// internal scheduler concern as part of the service's public health contract.
#[derive(Clone, Debug)]
pub struct HistoricalWorkGate {
    readiness: Readiness,
    materialized_views_caught_up: Arc<AtomicBool>,
}

impl HistoricalWorkGate {
    /// Creates a closed historical-work gate tied to canonical phase state.
    #[must_use]
    pub fn new(readiness: Readiness) -> Self {
        metrics::gauge!("zinder_ingest_historical_work_gate_open").set(0.0);
        metrics::gauge!("zinder_materialized_view_replay_caught_up").set(0.0);
        Self {
            readiness,
            materialized_views_caught_up: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the shared canonical readiness handle used by materialized-view replay's
    /// bulk-catchup phase gate.
    #[must_use]
    pub fn readiness(&self) -> Readiness {
        self.readiness.clone()
    }

    /// Publishes whether materialized-view replay has materialized the canonical tip.
    pub fn set_materialized_views_caught_up(&self, caught_up: bool) {
        let was_caught_up = self
            .materialized_views_caught_up
            .swap(caught_up, Ordering::AcqRel);
        metrics::gauge!("zinder_materialized_view_replay_caught_up").set(if caught_up {
            1.0
        } else {
            0.0
        });
        self.record_open_metric();
        if was_caught_up == caught_up {
            return;
        }

        if caught_up {
            tracing::info!(
                target: "zinder::ingest",
                event = "materialized_view_replay_caught_up",
                "materialized-view replay reached the canonical tip; historical work may run during tip follow"
            );
        } else {
            tracing::info!(
                target: "zinder::ingest",
                event = "materialized_view_replay_fell_behind",
                "materialized-view replay no longer covers the canonical tip; historical work is deferred"
            );
        }
    }

    /// Returns whether historical work currently owns any storage budget.
    #[must_use]
    pub fn is_open(&self) -> bool {
        matches!(self.readiness.phase(), Some(IngestPhase::FollowingTip))
            && self.materialized_views_caught_up.load(Ordering::Acquire)
    }

    fn record_open_metric(&self) {
        metrics::gauge!("zinder_ingest_historical_work_gate_open").set(if self.is_open() {
            1.0
        } else {
            0.0
        });
    }
}

/// Resolved configuration parsed from `services/zinder-ingest/src/config.rs`'s
/// TOML schema.
#[derive(Clone, Debug)]
pub struct IngestRuntimeConfig {
    /// Resolved upstream node endpoint.
    pub node: NodeTarget,
    /// Upstream node source adapter selector.
    pub node_source: NodeSourceKind,
    /// Local canonical store path.
    pub storage_path: PathBuf,
    /// Bounded `RocksDB` resource budget for the canonical store.
    pub canonical_rocksdb_budget: zinder_store::RocksDbResourceBudget,
    /// Bounded `RocksDB` resource budget for the materialized-view store the
    /// in-process tailer writes.
    pub materialized_view_rocksdb_budget: zinder_store::RocksDbResourceBudget,
    /// Optional raw-byte blob write policy for canonical ingest.
    pub raw_blob_policy: RawBlobPolicy,
    /// Number of near-tip blocks the canonical store keeps replaceable by a
    /// reorg.
    pub reorg_window_blocks: u32,
    /// Phase-classifier knobs (`[ingest.phase_classification]`).
    pub phase_classification: PhaseClassificationConfig,
    /// Source-fetch and block-preparation knobs (`[ingest.construction]`).
    pub construction: CanonicalConstructionSettings,
    /// Live mempool admission and durable reconciliation limits (`[ingest.mempool]`).
    pub mempool: MempoolIngestSettings,
    /// Serial-loop knobs (`[ingest.follow]`).
    pub follow: CanonicalFollowSettings,
    /// One-shot or disposable-store `run_overrides` (`[ingest.run_overrides]`).
    pub run_overrides: CanonicalRunOverrides,
}

/// Resolved `[ingest.mempool]` resource limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MempoolIngestSettings {
    /// Bounds the number and cumulative raw bytes of transactions admitted from
    /// either upstream mempool source.
    pub source_admission_limits: MempoolSourceAdmissionLimits,
    /// Target raw transaction bytes for one durable reconciliation write; a
    /// single protocol-valid transaction may exceed it as a singleton.
    pub reconciliation_batch_target_raw_transaction_bytes: NonZeroU64,
}

/// Resolved `[ingest.phase_classification]` configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhaseClassificationConfig {
    /// Gap at which `zinder-ingest probe` reports `BulkCatchup` instead of
    /// `FollowingTip`. Defaults to `reorg_window_blocks`.
    pub catchup_threshold_blocks: u32,
}

/// Programmatic limits for reusable materialized-view replay workers.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MaterializedViewReplayConfig {
    /// Maximum block contexts hydrated and dispatched in one materialized-view replay
    /// write.
    ///
    /// Replay may resume from retained chain events after the writer has
    /// already committed them. Keeping replay memory bounded here prevents a
    /// large retained event from becoming one unbounded in-memory hydration.
    pub replay_batch_blocks: NonZeroU32,
    /// Replay pressure policy for the async materialized-view tailer.
    ///
    /// `CanonicalFirst` keeps canonical ingest ahead of rebuildable
    /// materialized views under memory pressure. `Continuous` always lets the tailer
    /// replay retained chain events as soon as it observes them.
    pub replay_policy: MaterializedViewReplayPolicy,
    /// Optional explicit memory budget for materialized-view replay pressure decisions.
    ///
    /// When unset, the replay budget uses the runtime cgroup `memory.high` or
    /// `memory.max` value reported by the runtime memory-pressure sampler.
    pub memory_budget_bytes: Option<NonZeroU64>,
    /// Memory ratio at which materialized-view replay starts shrinking the effective
    /// replay batch size.
    pub memory_degrade_ratio: f64,
    /// Memory ratio at which materialized-view replay stops until pressure drops below
    /// [`Self::memory_resume_ratio`].
    pub memory_pause_ratio: f64,
    /// Memory ratio below which materialized-view replay resumes normal batch sizing.
    pub memory_resume_ratio: f64,
    /// Smallest effective replay batch while memory pressure is degraded.
    pub min_replay_batch_blocks: NonZeroU32,
}

impl MaterializedViewReplayConfig {
    /// Replay limits the always-on materialized-view tailer runs with.
    pub const DEFAULT: Self = Self {
        replay_batch_blocks: nonzero_u32(100),
        replay_policy: MaterializedViewReplayPolicy::DEFAULT,
        memory_budget_bytes: None,
        memory_degrade_ratio: 0.90,
        memory_pause_ratio: 0.99,
        memory_resume_ratio: 0.80,
        min_replay_batch_blocks: nonzero_u32(10),
    };
}

pub(crate) const fn nonzero_u32(candidate: u32) -> NonZeroU32 {
    match NonZeroU32::new(candidate) {
        Some(nonzero) => nonzero,
        None => NonZeroU32::MIN,
    }
}

/// Runtime policy for replaying retained chain events into materialized-view consumers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializedViewReplayPolicy {
    /// Pause rebuildable materialized-view replay when process or cgroup memory pressure
    /// would compete with canonical ingest.
    CanonicalFirst,
    /// Continuously replay retained chain events regardless of memory pressure.
    Continuous,
}

impl MaterializedViewReplayPolicy {
    /// Default materialized-view replay policy for performance-first indexing.
    pub const DEFAULT: Self = Self::CanonicalFirst;

    /// Stable TOML/env rendering.
    #[must_use]
    pub const fn as_kebab_case(self) -> &'static str {
        match self {
            Self::CanonicalFirst => "canonical-first",
            Self::Continuous => "continuous",
        }
    }
}

/// Resolved `[ingest.construction]` configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalConstructionSettings {
    /// Shared source-fetch and block-preparation limits.
    pub pipeline_limits: CanonicalPipelineLimits,
}

/// Resolved `[ingest.follow]` configuration.
#[derive(Clone, Copy, Debug)]
pub struct CanonicalFollowSettings {
    /// Maximum delay between tip observations when no chain-tip
    /// notification stream is available.
    pub poll_interval: Duration,
    /// Threshold below which the loop reports `cause=ready` instead of
    /// `cause=syncing` while in [`IngestPhase::FollowingTip`].
    pub lag_threshold_blocks: u64,
    /// Maximum bytes of prepared blocks one replacement may hold before its
    /// atomic commit.
    pub max_replacement_bytes: std::num::NonZeroU64,
}

/// Resolved `[ingest.run_overrides]` configuration.
///
/// Optional knobs that scope or bound a single ingest run. Operators
/// rarely set these; they exist for bounded runs and the historical
/// wallet-serving bootstrap.
#[derive(Clone, Debug, Default)]
pub struct CanonicalRunOverrides {
    /// Stop committing after reaching this height. `None` means run
    /// indefinitely.
    pub target_height: Option<BlockHeight>,
    /// Construct an empty store from the upstream node's checkpoint at this
    /// height when set.
    pub checkpoint_height: Option<BlockHeight>,
}

/// Waits until canonical ingest and materialized-view replay release the storage budget
/// to rebuildable historical work.
///
/// Returns `true` when cancellation wins and the caller should exit.
pub(crate) async fn wait_until_historical_work_or_cancelled(
    gate: &HistoricalWorkGate,
    cancel: &CancellationToken,
) -> bool {
    loop {
        gate.record_open_metric();
        if gate.is_open() {
            return false;
        }
        if sleep_or_cancel(BACKGROUND_WORK_PHASE_POLL_INTERVAL, cancel).await {
            return true;
        }
    }
}

/// Sleeps for `duration` unless cancellation arrives first.
///
/// Returns `true` when the caller was cancelled and should stop.
pub(crate) async fn sleep_or_cancel(duration: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        () = cancel.cancelled() => true,
        () = tokio::time::sleep(duration) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn background_work_gate_requires_materialized_views_to_cover_tip() {
        let readiness = Readiness::default();
        readiness.set_phase(IngestPhase::FollowingTip);
        let gate = HistoricalWorkGate::new(readiness);
        let cancel = CancellationToken::new();
        let gate_for_waiter = gate.clone();
        let cancel_for_waiter = cancel.clone();
        let waiter = tokio::spawn(async move {
            wait_until_historical_work_or_cancelled(&gate_for_waiter, &cancel_for_waiter).await
        });

        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        gate.set_materialized_views_caught_up(true);
        let outcome = tokio::time::timeout(Duration::from_secs(2), waiter).await;
        assert!(matches!(outcome, Ok(Ok(false))));
    }

    #[tokio::test]
    async fn background_work_gate_resumes_after_bulk_catchup_and_materialized_view_catchup() {
        let readiness = Readiness::default();
        readiness.set_phase(IngestPhase::BulkCatchup);
        let gate = HistoricalWorkGate::new(readiness.clone());
        gate.set_materialized_views_caught_up(true);
        let cancel = CancellationToken::new();
        let waiter_gate = gate.clone();
        let gate_cancel = cancel.clone();
        let waiter = tokio::spawn(async move {
            wait_until_historical_work_or_cancelled(&waiter_gate, &gate_cancel).await
        });

        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        readiness.set_phase(IngestPhase::FollowingTip);
        let outcome = tokio::time::timeout(Duration::from_secs(2), waiter).await;
        assert!(matches!(outcome, Ok(Ok(false))));
    }

    #[tokio::test]
    async fn background_work_gate_exits_when_cancelled_before_tip_follow() {
        let readiness = Readiness::default();
        readiness.set_phase(IngestPhase::BulkCatchup);
        let gate = HistoricalWorkGate::new(readiness);
        let cancel = CancellationToken::new();
        cancel.cancel();

        assert!(wait_until_historical_work_or_cancelled(&gate, &cancel).await);
    }
}
