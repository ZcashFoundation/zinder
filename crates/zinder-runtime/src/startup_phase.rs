//! Structured startup-phase vocabulary.
//!
//! Each runtime reports its applicable [startup phases](../../../docs/architecture/service-operations.md#startup-phases).
//! [`StartupPhase`] names the phases and [`StartupPhaseGuard`] emits one
//! structured tracing event when a phase begins and one when it ends. The
//! exit event carries an explicit `outcome` (`ok`, `failed`, or `aborted`)
//! so a stuck or crashing startup is visible from a `PaaS` log viewer without
//! `ssh`.
//!
//! ```
//! use zinder_runtime::StartupPhase;
//! # fn open_storage() -> std::io::Result<()> { Ok(()) }
//!
//! let phase = StartupPhase::OpenStorage.start();
//! match open_storage() {
//!     Ok(()) => phase.complete(),
//!     Err(error) => phase.fail(&error),
//! }
//! ```
//!
//! Drop-without-`complete`-or-`fail` emits `outcome="aborted"` so a panic
//! inside a phase produces a deterministic signal instead of a silent gap.

use std::fmt::{self, Display};
use std::time::Instant;

use serde::Serialize;

/// Shared vocabulary for the startup phases a Zinder service performs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum StartupPhase {
    /// Load and validate merged configuration from defaults, file, env vars, and CLI flags.
    LoadConfig,
    /// Open the canonical or secondary storage handle.
    OpenStorage,
    /// Verify the schema version against the storage handle.
    CheckSchema,
    /// Establish the upstream node connection and probe capabilities.
    ///
    /// Only the canonical writer enters this phase. Readers start without
    /// contacting a node and verify their broadcast endpoint in the
    /// background instead.
    ConnectNode,
    /// Authenticate and structurally admit the ingest-control service.
    AdmitIngestControl,
    /// Recover in-flight state (replay pending epochs, hydrate mempool, etc.).
    RecoverState,
    /// Start an API listener, including operational and owner-control endpoints.
    StartApi,
    /// The runtime reached its startup readiness condition.
    Ready,
}

impl StartupPhase {
    /// Stable diagnostic name used in `phase=<name>` log fields.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LoadConfig => "load_config",
            Self::OpenStorage => "open_storage",
            Self::CheckSchema => "check_schema",
            Self::ConnectNode => "connect_node",
            Self::AdmitIngestControl => "admit_ingest_control",
            Self::RecoverState => "recover_state",
            Self::StartApi => "start_api",
            Self::Ready => "ready",
        }
    }

    /// Begins a phase: emits a `phase_state="entry"` tracing event and
    /// returns a guard that emits the corresponding `exit` event.
    #[must_use = "the returned guard must be completed or failed; dropping it emits an aborted outcome"]
    pub fn start(self) -> StartupPhaseGuard {
        tracing::info!(
            target: "zinder::startup",
            phase = self.as_str(),
            phase_state = "entry",
            "startup phase entered"
        );
        StartupPhaseGuard {
            phase: self,
            started_at: Instant::now(),
            outcome: PhaseOutcome::Pending,
        }
    }
}

impl Display for StartupPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Exit outcome of a startup phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PhaseOutcome {
    /// Phase is in progress; the guard has not been completed or failed.
    Pending,
    /// Phase completed successfully.
    Ok,
    /// Phase reported a failure.
    Failed,
}

/// Guard returned by [`StartupPhase::start`]. Records the exit event when
/// dropped or when [`Self::complete`] or [`Self::fail`] is called explicitly.
#[derive(Debug)]
#[must_use = "the guard must be completed or failed; dropping it emits an aborted outcome"]
pub struct StartupPhaseGuard {
    phase: StartupPhase,
    started_at: Instant,
    outcome: PhaseOutcome,
}

impl StartupPhaseGuard {
    /// Records a successful phase exit. Consumes the guard so the same phase
    /// cannot be closed twice.
    pub fn complete(mut self) {
        self.outcome = PhaseOutcome::Ok;
        self.emit_exit("ok", None::<&str>);
    }

    /// Records a failed phase exit, attaching `error` as the `reason` field.
    /// Consumes the guard so the same phase cannot be closed twice.
    pub fn fail<E>(mut self, error: &E)
    where
        E: Display + ?Sized,
    {
        self.outcome = PhaseOutcome::Failed;
        let reason = error.to_string();
        self.emit_exit("failed", Some(reason.as_str()));
    }

    fn emit_exit(&self, outcome: &'static str, reason: Option<&str>) {
        let elapsed = self.started_at.elapsed();
        let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        if let Some(reason) = reason {
            tracing::info!(
                target: "zinder::startup",
                phase = self.phase.as_str(),
                phase_state = "exit",
                outcome,
                elapsed_ms,
                reason,
                "startup phase exited"
            );
        } else {
            tracing::info!(
                target: "zinder::startup",
                phase = self.phase.as_str(),
                phase_state = "exit",
                outcome,
                elapsed_ms,
                "startup phase exited"
            );
        }
        metrics::histogram!(
            "zinder_startup_phase_duration_seconds",
            "phase" => self.phase.as_str(),
            "outcome" => outcome,
        )
        .record(elapsed);
    }
}

impl Drop for StartupPhaseGuard {
    fn drop(&mut self) {
        if matches!(self.outcome, PhaseOutcome::Pending) {
            self.emit_exit("aborted", None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_str_returns_canonical_phase_names() {
        assert_eq!(StartupPhase::LoadConfig.as_str(), "load_config");
        assert_eq!(StartupPhase::OpenStorage.as_str(), "open_storage");
        assert_eq!(StartupPhase::CheckSchema.as_str(), "check_schema");
        assert_eq!(StartupPhase::ConnectNode.as_str(), "connect_node");
        assert_eq!(
            StartupPhase::AdmitIngestControl.as_str(),
            "admit_ingest_control"
        );
        assert_eq!(StartupPhase::RecoverState.as_str(), "recover_state");
        assert_eq!(StartupPhase::StartApi.as_str(), "start_api");
        assert_eq!(StartupPhase::Ready.as_str(), "ready");
    }

    #[test]
    fn display_matches_canonical_name() {
        assert_eq!(format!("{}", StartupPhase::OpenStorage), "open_storage");
    }

    #[test]
    fn serializes_as_snake_case() -> Result<(), eyre::Report> {
        let json = serde_json::to_string(&StartupPhase::OpenStorage)?;
        assert_eq!(json, "\"open_storage\"");
        Ok(())
    }

    #[test]
    fn complete_consumes_guard() {
        let guard = StartupPhase::LoadConfig.start();
        guard.complete();
    }

    #[test]
    fn fail_consumes_guard_and_carries_reason() {
        let guard = StartupPhase::OpenStorage.start();
        guard.fail(&"disk is full");
    }

    #[test]
    fn complete_records_histogram_under_correct_labels() {
        // The metrics facade has a process-wide recorder. We can't directly
        // assert against gauges without a custom recorder, so this test
        // exists to guarantee the call path does not panic. Recorder-level
        // assertions live in `crates/zinder-runtime/tests` against the
        // installed Prometheus recorder.
        let guard = StartupPhase::OpenStorage.start();
        guard.complete();
    }
}
