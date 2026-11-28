//! Bounded-cardinality metrics for source-confirmed subtree roots.

use std::time::Instant;

use zinder_core::ShieldedProtocol;

#[derive(Clone, Copy)]
enum SubtreeVerificationOutcome {
    FirstMatch,
    RecheckMatch,
    Mismatch,
    Interrupted,
}

impl SubtreeVerificationOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::FirstMatch => "first_match",
            Self::RecheckMatch => "recheck_match",
            Self::Mismatch => "mismatch",
            Self::Interrupted => "interrupted",
        }
    }
}

pub(crate) struct SubtreeVerificationObservation {
    batch: Option<(&'static str, Instant)>,
    recheck_started_at: Option<Instant>,
    outcome: SubtreeVerificationOutcome,
}

impl SubtreeVerificationObservation {
    pub(crate) fn new(protocols: impl IntoIterator<Item = ShieldedProtocol>) -> Self {
        let mut protocols = protocols.into_iter();
        let batch = protocols.next().map(|first| {
            let pool = if protocols.next().is_some() {
                "multiple"
            } else {
                first.rpc_pool_name()
            };
            (pool, Instant::now())
        });
        Self {
            batch,
            recheck_started_at: None,
            outcome: SubtreeVerificationOutcome::Interrupted,
        }
    }

    pub(crate) fn begin_recheck(&mut self) {
        self.recheck_started_at = Some(Instant::now());
    }

    pub(crate) fn first_match(&mut self) {
        self.outcome = SubtreeVerificationOutcome::FirstMatch;
    }

    pub(crate) fn recheck_match(&mut self) {
        self.outcome = SubtreeVerificationOutcome::RecheckMatch;
    }

    pub(crate) fn mismatch(&mut self) {
        self.outcome = SubtreeVerificationOutcome::Mismatch;
    }
}

impl Drop for SubtreeVerificationObservation {
    fn drop(&mut self) {
        let Some((pool, started_at)) = self.batch else {
            return;
        };
        let finished_at = Instant::now();
        let outcome = self.outcome.as_str();
        metrics::counter!(
            "zinder_ingest_subtree_root_verifications_total",
            "pool" => pool,
            "outcome" => outcome,
        )
        .increment(1);
        metrics::histogram!(
            "zinder_ingest_subtree_root_verification_duration_seconds",
            "pool" => pool,
            "outcome" => outcome,
        )
        .record(finished_at.duration_since(started_at));
        if let Some(recheck) = self.recheck_started_at {
            metrics::histogram!(
                "zinder_ingest_subtree_root_recheck_duration_seconds",
                "pool" => pool,
                "outcome" => outcome,
            )
            .record(finished_at.duration_since(recheck));
        }
    }
}
