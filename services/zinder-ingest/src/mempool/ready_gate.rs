//! Coordination point between the live mempool owner and the tip-follow
//! readiness state machine.
//!
//! The writer must finish rebuilding the live `MempoolIndex` before
//! signalling `ready`. After a writer restart the index is empty until the
//! owner receives the source's
//! [`MempoolSourceEvent::InitialSnapshotComplete`] control marker. The
//! streaming backend emits it after its upstream snapshot; the polling
//! backend emits it after a complete first poll. Opening a stream alone is
//! not evidence that an empty index is safe to expose.
//!
//! [`MempoolReadyGate`] watches the source tip certified by the serving
//! generation. The tip-follow readiness state machine publishes
//! [`zinder_runtime::ReadinessCause::Ready`] when that source tip matches its
//! canonical fence. A normal tip change keeps the previous generation
//! certified while the owner hydrates its replacement, so the gate also
//! admits a different certified tip for a bounded grace after the two first
//! disagree. The certification is withdrawn only when no
//! generation can serve: before the first snapshot completes, after its replacement
//! exceeds the staging grace, and during a rebuild after a source failure.
//!
//! With fallback node endpoints, each certification also records the active
//! endpoint the owner observed before it last found its source stream open.
//! The gate admits a certification only while that endpoint is still active,
//! so a switch stops admitting the previous endpoint's mempool at once, even
//! when the owner certifies a snapshot from it after the switch.
//!
//! The same pair carries the opposite signal when the canonical writer parks
//! on a reorg deeper than its window: the follower halts the owner, which
//! stops consuming the node mempool for the rest of the process.
//!
//! [`MempoolSourceEvent::InitialSnapshotComplete`]:
//! zinder_source::MempoolSourceEvent::InitialSnapshotComplete

use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio::sync::watch;
use zinder_core::BlockId;

use crate::writer::control::ParkedWriterHalt;

/// Longest the canonical tip may disagree with the certified mempool source
/// tip before ingest readiness is withdrawn.
///
/// Each block ends the serving source generation, and the owner certifies the
/// replacement only once canonical ingest has committed the same tip. The
/// streaming source does this within milliseconds; the polling source can
/// take one poll interval plus a full mempool hydration.
pub(crate) const MEMPOOL_TIP_TRANSITION_GRACE: Duration = Duration::from_secs(30);

/// The serving generation's certification.
#[derive(Clone, Debug)]
struct SourceCertification {
    source_tip: BlockId,
    endpoint: ObservedNodeEndpoint,
    replacement_deadline: OnceLock<tokio::time::Instant>,
}

/// The active node endpoint as the mempool owner observed it.
///
/// Take it with [`MempoolReadySignal::observe_node_endpoint`] before checking
/// that the source stream is still open, and certify with it. A stream ends
/// when the endpoint it reads from stops being active, so a certification
/// whose observed endpoint is still active describes that endpoint's mempool.
#[derive(Clone, Debug)]
pub struct ObservedNodeEndpoint {
    /// The active-endpoint channel, marked seen at the observation. `None`
    /// without fallback endpoints, where the endpoint never changes.
    switches: Option<watch::Receiver<usize>>,
}

impl ObservedNodeEndpoint {
    /// Returns whether no switch happened since the observation. A closed
    /// switch channel fails closed.
    pub(super) fn is_active(&self) -> bool {
        self.switches
            .as_ref()
            .is_none_or(|switches| matches!(switches.has_changed(), Ok(false)))
    }

    pub(super) async fn deactivated(mut self) {
        match self.switches.as_mut() {
            Some(switches) => {
                let _changed_or_closed = switches.changed().await;
            }
            None => std::future::pending().await,
        }
    }
}

/// Read-side handle observed by the tip-follow state machine.
#[derive(Clone, Debug)]
pub struct MempoolReadyGate {
    certified_source_tip: watch::Receiver<Option<SourceCertification>>,
    writer_halt: watch::Sender<Option<ParkedWriterHalt>>,
    /// When the certified source tip and the canonical tip began to disagree.
    tip_disagreement_since: Arc<Mutex<Option<Instant>>>,
}

impl MempoolReadyGate {
    /// Returns the current source generation's certified tip while its owner
    /// is still running and the node endpoint it certified from is active.
    ///
    /// A closed channel fails closed even though `tokio::sync::watch` retains
    /// its last value. Otherwise an owner that exits after publishing `true`
    /// could leave ingest readiness permanently admitted.
    #[must_use]
    pub fn certified_source_tip(&self) -> Option<BlockId> {
        if self.certified_source_tip.has_changed().is_err() {
            return None;
        }
        self.certified_source_tip
            .borrow()
            .as_ref()
            .filter(|certification| {
                certification.endpoint.is_active()
                    && certification
                        .replacement_deadline
                        .get()
                        .is_none_or(|deadline| tokio::time::Instant::now() < *deadline)
            })
            .map(|certification| certification.source_tip)
    }

    /// Returns whether the serving source generation admits the supplied
    /// canonical fence.
    ///
    /// A generation certified at exactly `canonical_tip` always admits it. A
    /// generation certified at another tip admits it only while the two have
    /// disagreed for less than 30 seconds, which covers the replacement
    /// generation a normal tip change starts.
    #[must_use]
    pub fn admits_canonical_tip(&self, canonical_tip: BlockId) -> bool {
        self.admits_canonical_tip_at(canonical_tip, Instant::now())
    }

    fn admits_canonical_tip_at(&self, canonical_tip: BlockId, now: Instant) -> bool {
        let certified_source_tip = self.certified_source_tip();
        let disagreeing_since = {
            let mut tip_disagreement_since = self.tip_disagreement_since.lock();
            match certified_source_tip {
                Some(certified_source_tip) if certified_source_tip != canonical_tip => {
                    Some(*tip_disagreement_since.get_or_insert(now))
                }
                Some(_) | None => {
                    *tip_disagreement_since = None;
                    None
                }
            }
        };
        match (certified_source_tip, disagreeing_since) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(_), Some(disagreeing_since)) => {
                now.saturating_duration_since(disagreeing_since) < MEMPOOL_TIP_TRANSITION_GRACE
            }
        }
    }

    /// Waits until the source generation changes its hydration state.
    ///
    /// The canonical follower uses this to withdraw or restamp readiness
    /// immediately instead of waiting for its next tip poll.
    pub(crate) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.certified_source_tip.changed().await
    }

    /// Halts the live mempool owner because the canonical writer parked.
    pub(crate) fn halt_owner(&self, halt: ParkedWriterHalt) {
        let _ = self.writer_halt.send_replace(Some(halt));
    }
}

/// Write-side handle owned by the live mempool owner's source loop. Cloning is
/// cheap; every clone shares the same hydration state.
#[derive(Clone, Debug)]
pub struct MempoolReadySignal {
    certified_source_tip: watch::Sender<Option<SourceCertification>>,
    writer_halt: watch::Receiver<Option<ParkedWriterHalt>>,
    endpoint_switches: Option<watch::Receiver<usize>>,
}

impl MempoolReadySignal {
    /// Binds certifications to the active node endpoint, which
    /// `endpoint_switches` reports, so the gate stops admitting one once the
    /// endpoint it was made from is no longer active.
    #[must_use]
    pub fn following_node_endpoints(mut self, endpoint_switches: watch::Receiver<usize>) -> Self {
        self.endpoint_switches = Some(endpoint_switches);
        self
    }

    /// Observes the active node endpoint for a later certification.
    #[must_use]
    pub fn observe_node_endpoint(&self) -> ObservedNodeEndpoint {
        ObservedNodeEndpoint {
            switches: self.endpoint_switches.clone().map(|mut switches| {
                switches.mark_unchanged();
                switches
            }),
        }
    }

    /// Marks the serving source generation as hydrated and certified against
    /// `source_tip`, from the mempool of the `observed` endpoint.
    pub fn certify_source_tip(&self, source_tip: BlockId, observed: ObservedNodeEndpoint) {
        // `send_replace` ignores the "no receivers" condition that `send`
        // returns, so optional readiness wiring does not make source
        // ownership fail.
        let _ = self
            .certified_source_tip
            .send_replace(Some(SourceCertification {
                source_tip,
                endpoint: observed,
                replacement_deadline: OnceLock::new(),
            }));
    }

    /// Shares the owner's deadline without waking the follower on ordinary tip changes.
    pub(super) fn stage_replacement_until(&self, deadline: tokio::time::Instant) {
        if let Some(certification) = self.certified_source_tip.borrow().as_ref() {
            let _first_deadline = certification.replacement_deadline.set(deadline);
        }
    }

    /// Withdraws the gate while no source generation can serve.
    ///
    /// Withdrawing a certification that is already withdrawn does not wake
    /// the gate: a source that keeps failing to reconnect must not keep
    /// cancelling the canonical follower's in-flight node requests.
    pub fn withdraw_certification(&self) {
        self.certified_source_tip
            .send_if_modified(|certified_source_tip| certified_source_tip.take().is_some());
    }

    /// Resolves once the canonical writer parks and halts the owner.
    ///
    /// Never resolves when the follower side is gone without a halt.
    pub(crate) async fn writer_halted(&self) -> ParkedWriterHalt {
        let mut writer_halt = self.writer_halt.clone();
        let halt = writer_halt
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|halt| *halt);
        match halt {
            Some(halt) => halt,
            None => std::future::pending().await,
        }
    }
}

/// Constructs a fresh, unhydrated gate plus its sender.
#[must_use]
pub fn mempool_ready_channel() -> (MempoolReadySignal, MempoolReadyGate) {
    let (tx, rx) = watch::channel(None);
    let (halt_tx, halt_rx) = watch::channel(None);
    (
        MempoolReadySignal {
            certified_source_tip: tx,
            writer_halt: halt_rx,
            endpoint_switches: None,
        },
        MempoolReadyGate {
            certified_source_tip: rx,
            writer_halt: halt_tx,
            tip_disagreement_since: Arc::new(Mutex::new(None)),
        },
    )
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use std::time::{Duration, Instant};

    use tokio::sync::watch;
    use zinder_core::{BlockHash, BlockHeight, BlockId};

    use super::{MEMPOOL_TIP_TRANSITION_GRACE, mempool_ready_channel};
    use crate::writer::control::ParkedWriterHalt;

    fn source_tip(tag: u8) -> BlockId {
        BlockId::new(
            BlockHeight::new(u32::from(tag)),
            BlockHash::from_bytes([tag; 32]),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn readiness_expires_even_before_the_owner_publishes_its_withdrawal() {
        let (signal, gate) = mempool_ready_channel();
        let tip = source_tip(1);
        signal.certify_source_tip(tip, signal.observe_node_endpoint());
        let deadline = tokio::time::Instant::now() + MEMPOOL_TIP_TRANSITION_GRACE;
        signal.stage_replacement_until(deadline);
        signal.stage_replacement_until(deadline + MEMPOOL_TIP_TRANSITION_GRACE);
        assert!(gate.admits_canonical_tip(tip));
        tokio::time::advance(MEMPOOL_TIP_TRANSITION_GRACE).await;
        assert!(!gate.admits_canonical_tip(tip));
        signal.certify_source_tip(tip, signal.observe_node_endpoint());
        assert!(gate.admits_canonical_tip(tip));
    }

    #[test]
    fn gate_is_unhydrated_at_construction() {
        let (_signal, gate) = mempool_ready_channel();
        assert_eq!(gate.certified_source_tip(), None);
    }

    #[test]
    fn certifying_a_source_tip_admits_that_exact_tip() {
        let (signal, gate) = mempool_ready_channel();
        let certified_tip = source_tip(1);
        signal.certify_source_tip(certified_tip, signal.observe_node_endpoint());
        assert!(gate.admits_canonical_tip(certified_tip));
    }

    #[test]
    fn certifying_the_same_source_tip_is_idempotent() {
        let (signal, gate) = mempool_ready_channel();
        let certified_tip = source_tip(1);
        signal.certify_source_tip(certified_tip, signal.observe_node_endpoint());
        signal.certify_source_tip(certified_tip, signal.observe_node_endpoint());
        assert!(gate.admits_canonical_tip(certified_tip));
    }

    #[test]
    fn withdrawing_certification_revokes_a_previously_admitted_tip() {
        let (signal, gate) = mempool_ready_channel();
        let certified_tip = source_tip(1);
        signal.certify_source_tip(certified_tip, signal.observe_node_endpoint());
        signal.withdraw_certification();
        assert!(!gate.admits_canonical_tip(certified_tip));
    }

    /// A snapshot the owner reconciled from the previous endpoint and
    /// certified after a switch never admits a tip; one observed after the
    /// switch does.
    #[test]
    fn a_certification_observed_before_a_switch_admits_no_tip_after_it() {
        let (active_endpoint, endpoint_switches) = watch::channel(0_usize);
        let (signal, gate) = mempool_ready_channel();
        let signal = signal.following_node_endpoints(endpoint_switches);
        let certified_tip = source_tip(1);
        signal.certify_source_tip(certified_tip, signal.observe_node_endpoint());
        assert!(gate.admits_canonical_tip(certified_tip));
        let before_switch = signal.observe_node_endpoint();

        active_endpoint.send_replace(1);

        assert_eq!(gate.certified_source_tip(), None);
        signal.certify_source_tip(certified_tip, before_switch);
        assert!(!gate.admits_canonical_tip(certified_tip));
        signal.certify_source_tip(certified_tip, signal.observe_node_endpoint());
        assert!(gate.admits_canonical_tip(certified_tip));
    }

    #[test]
    fn a_previous_tip_stays_admitted_only_during_the_transition_grace() {
        let (signal, gate) = mempool_ready_channel();
        signal.certify_source_tip(source_tip(1), signal.observe_node_endpoint());
        let started_at = Instant::now();

        assert!(gate.admits_canonical_tip_at(source_tip(2), started_at));
        assert!(gate.admits_canonical_tip_at(
            source_tip(3),
            started_at + MEMPOOL_TIP_TRANSITION_GRACE.saturating_sub(Duration::from_millis(1))
        ));
        assert!(
            !gate.admits_canonical_tip_at(source_tip(3), started_at + MEMPOOL_TIP_TRANSITION_GRACE),
            "a later block must not restart the grace while the tips still disagree"
        );

        signal.certify_source_tip(source_tip(3), signal.observe_node_endpoint());
        let recertified_at = started_at + MEMPOOL_TIP_TRANSITION_GRACE;
        assert!(gate.admits_canonical_tip_at(source_tip(3), recertified_at));
        assert!(
            gate.admits_canonical_tip_at(source_tip(4), recertified_at + Duration::from_secs(1)),
            "agreement restarts the grace for the next block"
        );
    }

    #[test]
    fn a_withdrawn_generation_admits_no_tip_during_the_grace() {
        let (signal, gate) = mempool_ready_channel();
        signal.certify_source_tip(source_tip(1), signal.observe_node_endpoint());
        let started_at = Instant::now();
        assert!(gate.admits_canonical_tip_at(source_tip(2), started_at));

        signal.withdraw_certification();

        assert!(!gate.admits_canonical_tip_at(source_tip(2), started_at));
        assert!(!gate.admits_canonical_tip_at(source_tip(1), started_at));
    }

    #[tokio::test]
    async fn halting_the_owner_resolves_its_writer_halt() {
        let (signal, gate) = mempool_ready_channel();
        let halt = ParkedWriterHalt::new(zinder_proto::v1::ops::ReorgWindowExceededDetail {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        });
        gate.halt_owner(halt);
        let observed =
            tokio::time::timeout(std::time::Duration::from_secs(1), signal.writer_halted()).await;
        assert_eq!(observed.ok(), Some(halt));
    }

    #[tokio::test]
    async fn a_follower_gone_without_a_halt_never_halts_the_owner() {
        let (signal, gate) = mempool_ready_channel();
        drop(gate);
        let observed =
            tokio::time::timeout(std::time::Duration::from_millis(50), signal.writer_halted())
                .await;
        assert!(observed.is_err());
    }

    #[test]
    fn dropping_the_signal_withdraws_a_previously_ready_gate() {
        let (signal, gate) = mempool_ready_channel();
        let certified_tip = source_tip(1);
        signal.certify_source_tip(certified_tip, signal.observe_node_endpoint());
        assert!(gate.admits_canonical_tip(certified_tip));

        drop(signal);

        assert!(!gate.admits_canonical_tip(certified_tip));
    }
}
