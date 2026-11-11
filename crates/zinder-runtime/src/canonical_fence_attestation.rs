//! Canonical secondary attestation against `CanonicalControl.WriterStatus`.
//!
//! Every runtime that projects or serves from a canonical `RocksDB` secondary
//! proves that the secondary exposes exactly the fence the canonical writer
//! publishes before it uses that view. This module owns the comparison, the
//! bounded catch-up loop, and the [`WriterAttestation`] rules that refuse
//! traffic while a parked or unconfirmed writer keeps attesting a frozen
//! fence. It holds no control-plane client or credential: callers read writer
//! status through their own [`CanonicalFenceProbe`].

use std::{future::Future, num::NonZeroU32, time::Duration};

use thiserror::Error;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use zinder_core::{Network, wire::encode_zinder_native_chain_name};
use zinder_proto::{
    v1::ingest::{CanonicalWriterFence, CanonicalWriterStatusResponse},
    wire::{canonical_writer_fence_message, encode_canonical_network_upgrade_schedule},
};
use zinder_store::{CanonicalActivationSchedule, CanonicalEventFence};

use crate::{ReadinessCause, ReadinessState};

/// Relation between the writer's attested fence and a local secondary fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalFenceRelation {
    /// The writer attests every field of the local fence.
    Matches,
    /// The writer has published events the secondary has not caught up to.
    SecondaryBehind(CanonicalFenceLag),
    /// Catching the secondary up cannot reach the writer's fence.
    Diverged(CanonicalFenceDivergence),
}

/// Ordinary replication lag between a secondary and the canonical writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalFenceLag {
    /// Chain epochs the writer has published past the secondary.
    pub lag_chain_epochs: u64,
    /// Visible tip height of the local secondary.
    pub secondary_visible_tip_height: u32,
    /// Visible tip height the writer attests.
    pub writer_visible_tip_height: u32,
}

/// Reason a writer fence cannot be reached by catching the secondary up.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CanonicalFenceDivergence {
    /// The writer-status response carries no fence.
    #[error("canonical writer status carries no fence")]
    MissingFence,
    /// The writer attests a different network than the secondary serves.
    #[error("canonical writer status attests a different network")]
    NetworkMismatch,
    /// The fences differ and the writer is not strictly ahead in both event
    /// sequence and chain epoch.
    #[error(
        "canonical secondary at event {secondary_event_sequence} diverged from writer fence at event {writer_event_sequence}"
    )]
    FenceMismatch {
        /// Event sequence of the local secondary.
        secondary_event_sequence: u64,
        /// Event sequence the writer attests.
        writer_event_sequence: u64,
    },
}

/// Returns the writer fence a secondary exposing `fence` must be attested by.
#[must_use]
pub fn expected_writer_fence(fence: CanonicalEventFence) -> CanonicalWriterFence {
    canonical_writer_fence_message(
        fence.chain_epoch_id(),
        fence.chain_event_sequence(),
        fence.visible_tip(),
        fence.sequence_digest(),
    )
}

/// Classifies writer status against the fence a local secondary exposes.
///
/// `local` is the fence in wire form, usually from [`expected_writer_fence`].
/// The writer serializes status reads with its commits, so a secondary can
/// trail the writer but never lead it: a writer strictly ahead in both event
/// sequence and chain epoch is lag, and every other difference in any fence
/// field is divergence.
#[must_use]
pub fn classify_canonical_writer_fence(
    status: &CanonicalWriterStatusResponse,
    network: Network,
    local: &CanonicalWriterFence,
) -> CanonicalFenceRelation {
    let Some(writer) = status.fence.as_ref() else {
        return CanonicalFenceRelation::Diverged(CanonicalFenceDivergence::MissingFence);
    };
    if status.network_name != encode_zinder_native_chain_name(network) {
        return CanonicalFenceRelation::Diverged(CanonicalFenceDivergence::NetworkMismatch);
    }
    if writer == local {
        return CanonicalFenceRelation::Matches;
    }
    if writer.event_sequence > local.event_sequence && writer.chain_epoch_id > local.chain_epoch_id
    {
        return CanonicalFenceRelation::SecondaryBehind(CanonicalFenceLag {
            lag_chain_epochs: writer.chain_epoch_id - local.chain_epoch_id,
            secondary_visible_tip_height: local.visible_tip_height,
            writer_visible_tip_height: writer.visible_tip_height,
        });
    }
    CanonicalFenceRelation::Diverged(CanonicalFenceDivergence::FenceMismatch {
        secondary_event_sequence: local.event_sequence,
        writer_event_sequence: writer.event_sequence,
    })
}

/// Returns whether `status` reports exactly the network-upgrade schedule
/// revision `schedule` carries, including when neither side has admitted one.
#[must_use]
pub fn writer_attests_network_upgrade_schedule(
    status: &CanonicalWriterStatusResponse,
    schedule: Option<&CanonicalActivationSchedule>,
) -> bool {
    status.network_upgrade_schedule
        == schedule.map(|schedule| {
            encode_canonical_network_upgrade_schedule(schedule.activations(), schedule.revision())
        })
}

/// Bounds for [`converge_on_canonical_writer_fence`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalFenceConvergencePolicy {
    /// Wall-clock deadline measured from the first catch-up.
    pub timeout: Duration,
    /// Maximum catch-up rounds, or `None` to bound convergence by `timeout` alone.
    pub max_attempts: Option<NonZeroU32>,
    /// Delay after the first unconverged round.
    pub initial_delay: Duration,
    /// Ceiling for the delay, which doubles after each unconverged round.
    pub max_delay: Duration,
}

/// Local secondary and writer-status access driven by the convergence loop.
pub trait CanonicalFenceProbe {
    /// Failure that stops convergence immediately.
    type Error;

    /// Catches the local secondary up and returns the fence it now exposes in
    /// wire form, or `None` when the local view is not yet coherent and the
    /// round must be retried.
    fn catch_up(
        &mut self,
    ) -> impl Future<Output = Result<Option<CanonicalWriterFence>, Self::Error>> + Send;

    /// Reads `CanonicalControl.WriterStatus` from the canonical writer.
    ///
    /// The returned future is dropped unfinished when the convergence
    /// deadline or cancellation fires, so it must be cancel-safe.
    fn writer_status(
        &mut self,
    ) -> impl Future<Output = Result<CanonicalWriterStatusResponse, Self::Error>> + Send;

    /// Returns whether the caught-up secondary carries the network-upgrade
    /// schedule revision `status` reports.
    ///
    /// A writer can admit a schedule revision without moving its fence, so a
    /// probe that requires the revision returns `false` until its secondary
    /// catches up to it, and the round is retried like fence lag. The default
    /// accepts any revision.
    fn attests_writer_schedule(&self, _status: &CanonicalWriterStatusResponse) -> bool {
        true
    }

    /// Observes one round in which the secondary trailed the writer, `elapsed`
    /// after convergence started.
    fn secondary_behind(&mut self, _lag: CanonicalFenceLag, _elapsed: Duration) {}
}

/// Successful end of [`converge_on_canonical_writer_fence`].
#[derive(Clone, Debug, PartialEq)]
pub enum CanonicalFenceConvergence {
    /// The writer attested the fence the probe last caught up to, with the
    /// status that attested it.
    ///
    /// Convergence does not interpret a halt or an unconfirmed tip in that
    /// status; a serving reader passes it to [`WriterAttestation::observe`].
    Converged(Box<CanonicalWriterStatusResponse>),
    /// Cancellation fired while the secondary was still behind.
    Cancelled,
}

/// Failure of [`converge_on_canonical_writer_fence`].
#[derive(Debug, Error)]
pub enum CanonicalFenceConvergenceError<ProbeError> {
    /// The probe failed to catch up or to read writer status.
    #[error("{0}")]
    Probe(ProbeError),
    /// The writer fence cannot be reached by catching up.
    #[error(transparent)]
    Diverged(CanonicalFenceDivergence),
    /// The secondary was still behind when the deadline or attempt bound expired.
    #[error("canonical secondary did not reach the writer fence within the convergence bounds")]
    DidNotConverge,
}

/// Catches a secondary up until the canonical writer attests its exact fence.
///
/// The writer keeps committing while a secondary converges, so lag is retried
/// with capped exponential backoff until the policy's deadline or attempt
/// bound. A matching fence whose network-upgrade schedule revision the probe
/// has not caught up to is retried the same way. Divergence fails on the
/// round that observes it.
///
/// Each writer-status read races the deadline and `cancel`: a read still
/// pending when either fires is dropped, so a stalled writer cannot hold the
/// loop past its bounds. A local catch-up always runs to completion, because
/// a probe may move its secondary into that work.
pub async fn converge_on_canonical_writer_fence<Probe>(
    probe: &mut Probe,
    network: Network,
    policy: CanonicalFenceConvergencePolicy,
    cancel: &CancellationToken,
) -> Result<CanonicalFenceConvergence, CanonicalFenceConvergenceError<Probe::Error>>
where
    Probe: CanonicalFenceProbe,
{
    let started = Instant::now();
    let deadline = started + policy.timeout;
    let mut delay = policy.initial_delay;
    let mut attempts: u32 = 0;
    loop {
        attempts = attempts.saturating_add(1);
        let local = probe
            .catch_up()
            .await
            .map_err(CanonicalFenceConvergenceError::Probe)?;
        if let Some(local) = local {
            let status = tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(CanonicalFenceConvergence::Cancelled),
                () = tokio::time::sleep_until(deadline) => {
                    return Err(CanonicalFenceConvergenceError::DidNotConverge);
                }
                status = probe.writer_status() => {
                    status.map_err(CanonicalFenceConvergenceError::Probe)?
                }
            };
            match classify_canonical_writer_fence(&status, network, &local) {
                CanonicalFenceRelation::Matches if probe.attests_writer_schedule(&status) => {
                    return Ok(CanonicalFenceConvergence::Converged(Box::new(status)));
                }
                CanonicalFenceRelation::Matches => {
                    probe.secondary_behind(
                        CanonicalFenceLag {
                            lag_chain_epochs: 0,
                            secondary_visible_tip_height: local.visible_tip_height,
                            writer_visible_tip_height: local.visible_tip_height,
                        },
                        started.elapsed(),
                    );
                }
                CanonicalFenceRelation::Diverged(divergence) => {
                    return Err(CanonicalFenceConvergenceError::Diverged(divergence));
                }
                CanonicalFenceRelation::SecondaryBehind(lag) => {
                    probe.secondary_behind(lag, started.elapsed());
                }
            }
        }
        let now = Instant::now();
        if now >= deadline
            || policy
                .max_attempts
                .is_some_and(|max_attempts| attempts >= max_attempts.get())
        {
            return Err(CanonicalFenceConvergenceError::DidNotConverge);
        }
        tokio::select! {
            () = cancel.cancelled() => return Ok(CanonicalFenceConvergence::Cancelled),
            () = tokio::time::sleep(delay.min(deadline.saturating_duration_since(now))) => {}
        }
        delay = delay.saturating_mul(2).min(policy.max_delay);
    }
}

/// The canonical writer-status rules a reader applies before it admits
/// traffic on a reader generation the writer attested.
///
/// A writer parked on a reorg deeper than its window, and a writer that has
/// not confirmed its tip since it reopened, both keep attesting their frozen
/// fence, so their status refuses traffic instead. A halt is remembered until
/// a valid status carries neither the halt nor an unconfirmed tip: a writer
/// that restarts after a park reports the halt again only once it rediscovers
/// the reorg, and a writer-status outage must not reopen traffic. Only a
/// following writer with a confirmed tip earns the staleness allowance that
/// keeps a lagging reader admitting traffic.
#[derive(Debug)]
pub struct WriterAttestation {
    network: Network,
    staleness_ceiling: Duration,
    attested_at: Option<Instant>,
    remembered_halt: Option<ReadinessState>,
}

impl WriterAttestation {
    /// Creates the rules for a reader of `network` that may serve a lagging
    /// reader for `staleness_ceiling` after the writer last attested it.
    #[must_use]
    pub const fn new(network: Network, staleness_ceiling: Duration) -> Self {
        Self {
            network,
            staleness_ceiling,
            attested_at: None,
            remembered_halt: None,
        }
    }

    /// Returns the not-ready state `status` imposes on a reader whose visible
    /// tip is at `visible_height`, or `None` when the writer follows with a
    /// confirmed tip.
    ///
    /// A refusal withdraws the staleness allowance. A halt or unconfirmed tip
    /// reported for another network is not honoured; such a status clears the
    /// remembered halt and falls through to the caller's fence validation.
    pub fn observe(
        &mut self,
        status: &CanonicalWriterStatusResponse,
        visible_height: Option<u32>,
    ) -> Option<ReadinessState> {
        let refusal = self.refusal(status, visible_height);
        if refusal.is_some() {
            self.attested_at = None;
        }
        refusal
    }

    fn refusal(
        &mut self,
        status: &CanonicalWriterStatusResponse,
        visible_height: Option<u32>,
    ) -> Option<ReadinessState> {
        let refusing = |cause| ReadinessState {
            current_height: visible_height,
            ..ReadinessState::not_ready(cause)
        };
        if status.network_name != encode_zinder_native_chain_name(self.network) {
            self.remembered_halt = None;
            return None;
        }
        if let Some(halt) = status.reorg_window_exceeded {
            let halt = refusing(ReadinessCause::from(halt));
            self.remembered_halt = Some(halt.clone());
            return Some(halt);
        }
        if status.tip_unconfirmed {
            return Some(
                self.remembered_halt
                    .clone()
                    .unwrap_or_else(|| refusing(ReadinessCause::CanonicalTipUnconfirmed)),
            );
        }
        self.remembered_halt = None;
        None
    }

    /// Records that a following writer with a confirmed tip attested the
    /// reader now.
    pub fn attest(&mut self) {
        self.attested_at = Some(Instant::now());
    }

    /// Returns the halt the writer last reported, which a reader keeps
    /// publishing through writer-status failures.
    #[must_use]
    pub fn remembered_halt(&self) -> Option<ReadinessState> {
        self.remembered_halt.clone()
    }

    /// Returns the time since the last attestation while it is below the
    /// staleness ceiling; `None` once the ceiling passes or while no following
    /// writer with a confirmed tip has attested the reader.
    #[must_use]
    pub fn stale_allowance(&self) -> Option<Duration> {
        self.attested_at
            .map(|attested_at| attested_at.elapsed())
            .filter(|staleness| *staleness < self.staleness_ceiling)
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, time::Duration};

    use tokio_util::sync::CancellationToken;
    use zinder_core::{
        BlockHash, BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
        CanonicalBlockFactsSequenceDigestVersion, ChainEpochId, Network,
        wire::encode_zinder_native_chain_name,
    };
    use zinder_proto::{
        v1::{
            ingest::{
                CanonicalNetworkUpgradeSchedule, CanonicalWriterFence,
                CanonicalWriterStatusResponse,
            },
            ops::ReorgWindowExceededDetail,
        },
        wire::canonical_writer_fence_message,
    };

    use super::{
        CanonicalFenceConvergence, CanonicalFenceConvergenceError, CanonicalFenceConvergencePolicy,
        CanonicalFenceDivergence, CanonicalFenceLag, CanonicalFenceProbe, CanonicalFenceRelation,
        WriterAttestation, classify_canonical_writer_fence, converge_on_canonical_writer_fence,
        writer_attests_network_upgrade_schedule,
    };
    use crate::{ReadinessCause, ReadinessState};

    const NETWORK: Network = Network::ZcashRegtest;

    #[derive(Debug, Eq, PartialEq)]
    struct ProbeFailure;

    fn fence(event_sequence: u64, digest_byte: u8) -> CanonicalWriterFence {
        let height = u32::try_from(event_sequence).unwrap_or(u32::MAX);
        canonical_writer_fence_message(
            ChainEpochId::new(event_sequence),
            event_sequence,
            BlockId::new(
                BlockHeight::new(height),
                BlockHash::from_bytes([digest_byte; 32]),
            ),
            CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                CanonicalBlockFactsSequenceDigestVersion::V1,
                event_sequence,
                [digest_byte; 32],
            ),
        )
    }

    fn status(writer: CanonicalWriterFence) -> CanonicalWriterStatusResponse {
        CanonicalWriterStatusResponse {
            network_name: encode_zinder_native_chain_name(NETWORK).to_owned(),
            fence: Some(writer),
            oldest_retained_event_sequence: 1,
            ..CanonicalWriterStatusResponse::default()
        }
    }

    fn fast_policy(timeout: Duration) -> CanonicalFenceConvergencePolicy {
        CanonicalFenceConvergencePolicy {
            timeout,
            max_attempts: None,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(4),
        }
    }

    /// Writer that commits once per status read for `commits_remaining`
    /// reads, or forever when it is `None`.
    struct ScriptedProbe {
        secondary_sequence: u64,
        writer_sequence: u64,
        commits_remaining: Option<u32>,
        catch_ups: u32,
        schedule_lag_catch_ups: u32,
        behind_rounds: Vec<CanonicalFenceLag>,
    }

    impl ScriptedProbe {
        fn new(commits_remaining: Option<u32>) -> Self {
            Self {
                secondary_sequence: 1,
                writer_sequence: 1,
                commits_remaining,
                catch_ups: 0,
                schedule_lag_catch_ups: 0,
                behind_rounds: Vec::new(),
            }
        }
    }

    impl CanonicalFenceProbe for ScriptedProbe {
        type Error = ProbeFailure;

        async fn catch_up(&mut self) -> Result<Option<CanonicalWriterFence>, ProbeFailure> {
            self.catch_ups += 1;
            self.secondary_sequence = self.writer_sequence;
            Ok(Some(fence(self.secondary_sequence, 7)))
        }

        async fn writer_status(&mut self) -> Result<CanonicalWriterStatusResponse, ProbeFailure> {
            match &mut self.commits_remaining {
                Some(0) => {}
                Some(remaining) => {
                    *remaining -= 1;
                    self.writer_sequence += 1;
                }
                None => self.writer_sequence += 1,
            }
            Ok(status(fence(self.writer_sequence, 7)))
        }

        fn attests_writer_schedule(&self, _status: &CanonicalWriterStatusResponse) -> bool {
            self.catch_ups > self.schedule_lag_catch_ups
        }

        fn secondary_behind(&mut self, lag: CanonicalFenceLag, _elapsed: Duration) {
            self.behind_rounds.push(lag);
        }
    }

    /// Probe whose local fence and writer status never change.
    struct FixedProbe {
        local: Option<CanonicalWriterFence>,
        writer: CanonicalWriterStatusResponse,
        catch_ups: u32,
    }

    impl CanonicalFenceProbe for FixedProbe {
        type Error = ProbeFailure;

        async fn catch_up(&mut self) -> Result<Option<CanonicalWriterFence>, ProbeFailure> {
            self.catch_ups += 1;
            Ok(self.local.clone())
        }

        async fn writer_status(&mut self) -> Result<CanonicalWriterStatusResponse, ProbeFailure> {
            Ok(self.writer.clone())
        }
    }

    /// Probe whose writer-status read never resolves.
    struct StalledStatusProbe;

    impl CanonicalFenceProbe for StalledStatusProbe {
        type Error = ProbeFailure;

        async fn catch_up(&mut self) -> Result<Option<CanonicalWriterFence>, ProbeFailure> {
            Ok(Some(fence(5, 1)))
        }

        async fn writer_status(&mut self) -> Result<CanonicalWriterStatusResponse, ProbeFailure> {
            std::future::pending().await
        }
    }

    #[test]
    fn exact_fence_matches() {
        assert_eq!(
            classify_canonical_writer_fence(&status(fence(5, 1)), NETWORK, &fence(5, 1)),
            CanonicalFenceRelation::Matches
        );
    }

    #[test]
    fn writer_ahead_in_sequence_and_epoch_is_lag() {
        assert_eq!(
            classify_canonical_writer_fence(&status(fence(8, 2)), NETWORK, &fence(5, 1)),
            CanonicalFenceRelation::SecondaryBehind(CanonicalFenceLag {
                lag_chain_epochs: 3,
                secondary_visible_tip_height: 5,
                writer_visible_tip_height: 8,
            })
        );
    }

    #[test]
    fn secondary_ahead_of_the_writer_is_divergence() {
        assert_eq!(
            classify_canonical_writer_fence(&status(fence(4, 1)), NETWORK, &fence(5, 1)),
            CanonicalFenceRelation::Diverged(CanonicalFenceDivergence::FenceMismatch {
                secondary_event_sequence: 5,
                writer_event_sequence: 4,
            })
        );
    }

    #[test]
    fn every_same_sequence_field_difference_is_divergence() {
        let local = fence(5, 1);
        let mutations: [fn(&mut CanonicalWriterFence); 5] = [
            |writer| writer.chain_epoch_id += 1,
            |writer| writer.visible_tip_height += 1,
            |writer| writer.visible_tip_hash = vec![0xee; 32],
            |writer| writer.canonical_sequence_digest = vec![0xee; 32],
            |writer| writer.visible_block_count += 1,
        ];
        for mutate in mutations {
            let mut writer = local.clone();
            mutate(&mut writer);
            assert_eq!(
                classify_canonical_writer_fence(&status(writer), NETWORK, &local),
                CanonicalFenceRelation::Diverged(CanonicalFenceDivergence::FenceMismatch {
                    secondary_event_sequence: 5,
                    writer_event_sequence: 5,
                })
            );
        }
    }

    #[test]
    fn writer_ahead_in_only_one_ordering_field_is_divergence() {
        let local = fence(5, 1);
        let mut epoch_behind = fence(8, 2);
        epoch_behind.chain_epoch_id = 5;
        let mut sequence_behind = fence(8, 2);
        sequence_behind.event_sequence = 5;
        for writer in [epoch_behind, sequence_behind] {
            assert!(matches!(
                classify_canonical_writer_fence(&status(writer), NETWORK, &local),
                CanonicalFenceRelation::Diverged(CanonicalFenceDivergence::FenceMismatch { .. })
            ));
        }
    }

    #[test]
    fn missing_fence_and_wrong_network_are_divergence() {
        let local = fence(5, 1);
        let missing = CanonicalWriterStatusResponse {
            fence: None,
            ..status(local.clone())
        };
        let wrong_network = CanonicalWriterStatusResponse {
            network_name: encode_zinder_native_chain_name(Network::ZcashMainnet).to_owned(),
            ..status(local.clone())
        };

        assert_eq!(
            classify_canonical_writer_fence(&missing, NETWORK, &local),
            CanonicalFenceRelation::Diverged(CanonicalFenceDivergence::MissingFence)
        );
        assert_eq!(
            classify_canonical_writer_fence(&wrong_network, NETWORK, &local),
            CanonicalFenceRelation::Diverged(CanonicalFenceDivergence::NetworkMismatch)
        );
    }

    #[tokio::test]
    async fn convergence_survives_writer_commits_between_catch_up_and_status() {
        let mut probe = ScriptedProbe::new(Some(6));

        let outcome = converge_on_canonical_writer_fence(
            &mut probe,
            NETWORK,
            fast_policy(Duration::from_secs(5)),
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(
            outcome,
            Ok(CanonicalFenceConvergence::Converged(_))
        ));
        assert_eq!(probe.catch_ups, 7);
        assert_eq!(probe.behind_rounds.len(), 6);
        assert!(
            probe
                .behind_rounds
                .iter()
                .all(|lag| lag.lag_chain_epochs == 1)
        );
    }

    #[tokio::test]
    async fn convergence_returns_promptly_when_cancelled_during_lag() {
        let mut probe = ScriptedProbe::new(None);
        let cancel = CancellationToken::new();
        let policy = CanonicalFenceConvergencePolicy {
            initial_delay: Duration::from_mins(10),
            max_delay: Duration::from_mins(10),
            ..fast_policy(Duration::from_hours(1))
        };
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            trigger.cancel();
        });

        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            converge_on_canonical_writer_fence(&mut probe, NETWORK, policy, &cancel),
        )
        .await;

        assert!(matches!(
            outcome,
            Ok(Ok(CanonicalFenceConvergence::Cancelled))
        ));
    }

    #[tokio::test]
    async fn stalled_writer_status_fails_at_the_deadline() {
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            converge_on_canonical_writer_fence(
                &mut StalledStatusProbe,
                NETWORK,
                fast_policy(Duration::from_millis(30)),
                &CancellationToken::new(),
            ),
        )
        .await;

        assert!(matches!(
            outcome,
            Ok(Err(CanonicalFenceConvergenceError::DidNotConverge))
        ));
    }

    #[tokio::test]
    async fn stalled_writer_status_returns_promptly_when_cancelled() {
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            trigger.cancel();
        });

        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            converge_on_canonical_writer_fence(
                &mut StalledStatusProbe,
                NETWORK,
                fast_policy(Duration::from_hours(1)),
                &cancel,
            ),
        )
        .await;

        assert!(matches!(
            outcome,
            Ok(Ok(CanonicalFenceConvergence::Cancelled))
        ));
    }

    #[tokio::test]
    async fn convergence_fails_after_the_deadline_under_sustained_lag() {
        let mut probe = ScriptedProbe::new(None);

        let outcome = converge_on_canonical_writer_fence(
            &mut probe,
            NETWORK,
            fast_policy(Duration::from_millis(30)),
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(
            outcome,
            Err(CanonicalFenceConvergenceError::DidNotConverge)
        ));
        assert!(probe.catch_ups > 1);
    }

    #[tokio::test]
    async fn convergence_stops_at_the_attempt_bound() {
        let mut probe = ScriptedProbe::new(None);
        let policy = CanonicalFenceConvergencePolicy {
            max_attempts: NonZeroU32::new(3),
            ..fast_policy(Duration::from_mins(1))
        };

        let outcome = converge_on_canonical_writer_fence(
            &mut probe,
            NETWORK,
            policy,
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(
            outcome,
            Err(CanonicalFenceConvergenceError::DidNotConverge)
        ));
        assert_eq!(probe.catch_ups, 3);
    }

    #[tokio::test]
    async fn convergence_fails_immediately_on_same_sequence_mismatch() {
        let mut probe = FixedProbe {
            local: Some(fence(5, 1)),
            writer: status(fence(5, 2)),
            catch_ups: 0,
        };

        let outcome = converge_on_canonical_writer_fence(
            &mut probe,
            NETWORK,
            fast_policy(Duration::from_mins(1)),
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(
            outcome,
            Err(CanonicalFenceConvergenceError::Diverged(
                CanonicalFenceDivergence::FenceMismatch {
                    secondary_event_sequence: 5,
                    writer_event_sequence: 5,
                }
            ))
        ));
        assert_eq!(probe.catch_ups, 1);
    }

    #[tokio::test]
    async fn probe_failure_stops_convergence() {
        struct FailingProbe;
        impl CanonicalFenceProbe for FailingProbe {
            type Error = ProbeFailure;

            async fn catch_up(&mut self) -> Result<Option<CanonicalWriterFence>, ProbeFailure> {
                Err(ProbeFailure)
            }

            async fn writer_status(
                &mut self,
            ) -> Result<CanonicalWriterStatusResponse, ProbeFailure> {
                Err(ProbeFailure)
            }
        }

        let outcome = converge_on_canonical_writer_fence(
            &mut FailingProbe,
            NETWORK,
            fast_policy(Duration::from_mins(1)),
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(
            outcome,
            Err(CanonicalFenceConvergenceError::Probe(ProbeFailure))
        ));
    }

    #[tokio::test]
    async fn convergence_waits_for_the_writer_admitted_schedule_revision() {
        let mut probe = ScriptedProbe::new(Some(0));
        probe.schedule_lag_catch_ups = 3;

        let outcome = converge_on_canonical_writer_fence(
            &mut probe,
            NETWORK,
            fast_policy(Duration::from_secs(5)),
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(
            outcome,
            Ok(CanonicalFenceConvergence::Converged(_))
        ));
        assert_eq!(probe.catch_ups, 4);
        assert_eq!(probe.behind_rounds.len(), 3);
        assert!(
            probe
                .behind_rounds
                .iter()
                .all(|lag| lag.lag_chain_epochs == 0)
        );

        let mut never_admitted = ScriptedProbe::new(Some(0));
        never_admitted.schedule_lag_catch_ups = u32::MAX;
        let outcome = converge_on_canonical_writer_fence(
            &mut never_admitted,
            NETWORK,
            fast_policy(Duration::from_millis(30)),
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(
            outcome,
            Err(CanonicalFenceConvergenceError::DidNotConverge)
        ));
    }

    /// Convergence succeeds against a parked or unconfirmed writer.
    ///
    /// Both keep attesting their fence, so the loop hands the status to the
    /// caller; only serving readers refuse it through `WriterAttestation`.
    #[tokio::test]
    async fn convergence_returns_a_halted_or_unconfirmed_attesting_status() {
        let halted = CanonicalWriterStatusResponse {
            reorg_window_exceeded: Some(ReorgWindowExceededDetail {
                depth: 1_001,
                configured: 1_000,
                covered: Some(1_000),
            }),
            tip_unconfirmed: true,
            ..status(fence(5, 1))
        };
        let mut probe = FixedProbe {
            local: Some(fence(5, 1)),
            writer: halted.clone(),
            catch_ups: 0,
        };

        let outcome = converge_on_canonical_writer_fence(
            &mut probe,
            NETWORK,
            fast_policy(Duration::from_secs(5)),
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(
            outcome,
            Ok(CanonicalFenceConvergence::Converged(attesting)) if *attesting == halted
        ));
        assert_eq!(probe.catch_ups, 1);
    }

    #[test]
    fn schedule_attestation_requires_the_same_revision_on_both_sides() {
        let unadmitted = status(fence(5, 1));
        let admitted = CanonicalWriterStatusResponse {
            network_upgrade_schedule: Some(CanonicalNetworkUpgradeSchedule::default()),
            ..status(fence(5, 1))
        };

        assert!(writer_attests_network_upgrade_schedule(&unadmitted, None));
        assert!(!writer_attests_network_upgrade_schedule(&admitted, None));
    }

    #[tokio::test]
    async fn incoherent_local_view_is_retried_without_reading_writer_status() {
        let mut probe = FixedProbe {
            local: None,
            writer: CanonicalWriterStatusResponse::default(),
            catch_ups: 0,
        };
        let policy = CanonicalFenceConvergencePolicy {
            max_attempts: NonZeroU32::new(2),
            ..fast_policy(Duration::from_mins(1))
        };

        let outcome = converge_on_canonical_writer_fence(
            &mut probe,
            NETWORK,
            policy,
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(
            outcome,
            Err(CanonicalFenceConvergenceError::DidNotConverge)
        ));
        assert_eq!(probe.catch_ups, 2);
    }

    fn following_writer_status() -> CanonicalWriterStatusResponse {
        CanonicalWriterStatusResponse {
            network_name: encode_zinder_native_chain_name(NETWORK).to_owned(),
            ..CanonicalWriterStatusResponse::default()
        }
    }

    fn unconfirmed_writer_status() -> CanonicalWriterStatusResponse {
        CanonicalWriterStatusResponse {
            tip_unconfirmed: true,
            ..following_writer_status()
        }
    }

    fn parked_writer_status() -> CanonicalWriterStatusResponse {
        CanonicalWriterStatusResponse {
            reorg_window_exceeded: Some(ReorgWindowExceededDetail {
                depth: 1_001,
                configured: 1_000,
                covered: Some(1_000),
            }),
            tip_unconfirmed: true,
            ..following_writer_status()
        }
    }

    #[test]
    fn writer_with_an_unconfirmed_tip_refuses_without_a_staleness_allowance() {
        let mut attestation = WriterAttestation::new(NETWORK, Duration::from_mins(1));
        assert_eq!(
            attestation.observe(&following_writer_status(), Some(7)),
            None
        );
        attestation.attest();
        assert!(attestation.stale_allowance().is_some());

        let refusal = attestation.observe(&unconfirmed_writer_status(), Some(7));
        assert_eq!(
            refusal,
            Some(ReadinessState {
                current_height: Some(7),
                ..ReadinessState::not_ready(ReadinessCause::CanonicalTipUnconfirmed)
            })
        );
        assert_eq!(attestation.stale_allowance(), None);
        assert_eq!(attestation.remembered_halt(), None);

        assert_eq!(
            attestation.observe(&following_writer_status(), Some(7)),
            None
        );
        attestation.attest();
        assert!(attestation.stale_allowance().is_some());
    }

    #[test]
    fn writer_halt_is_remembered_through_a_restart_and_status_failures() {
        let mut attestation = WriterAttestation::new(NETWORK, Duration::from_mins(1));
        attestation.attest();
        let halt = ReadinessState::reorg_window_exceeded(1_001, 1_000, Some(1_000), Some(7));
        assert_eq!(
            attestation.observe(&parked_writer_status(), Some(7)),
            Some(halt.clone())
        );
        assert_eq!(attestation.stale_allowance(), None);
        assert_eq!(attestation.remembered_halt(), Some(halt.clone()));

        // A restarted writer that has not yet rediscovered the reorg keeps
        // the remembered halt instead of the unconfirmed cause.
        assert_eq!(
            attestation.observe(&unconfirmed_writer_status(), Some(8)),
            Some(halt.clone())
        );
        assert_eq!(attestation.remembered_halt(), Some(halt));

        assert_eq!(
            attestation.observe(&following_writer_status(), Some(8)),
            None
        );
        assert_eq!(attestation.remembered_halt(), None);
        assert_eq!(attestation.stale_allowance(), None);
    }

    #[test]
    fn writer_status_for_another_network_is_not_a_refusal() {
        let mut attestation = WriterAttestation::new(NETWORK, Duration::from_mins(1));
        assert!(
            attestation
                .observe(&parked_writer_status(), Some(7))
                .is_some()
        );
        let other_network = CanonicalWriterStatusResponse {
            network_name: encode_zinder_native_chain_name(Network::ZcashTestnet).to_owned(),
            ..parked_writer_status()
        };
        assert_eq!(attestation.observe(&other_network, Some(7)), None);
        assert_eq!(attestation.remembered_halt(), None);
    }

    #[test]
    fn staleness_allowance_ends_at_the_ceiling() {
        let mut attestation = WriterAttestation::new(NETWORK, Duration::ZERO);
        assert_eq!(attestation.stale_allowance(), None);
        attestation.attest();
        assert_eq!(attestation.stale_allowance(), None);
    }
}
