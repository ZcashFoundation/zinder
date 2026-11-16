//! Wallet following parked on a transition over its logical-byte limit.
//!
//! The refusal is deterministic. The persisted wallet cursor plans the same
//! transition again, and the same limit refuses it at the same point, so
//! exiting only starts a supervisor restart loop. Each attempt would repeat
//! the transition's canonical replay and wallet reads up to the refusal and
//! acquire another canonical retention lease. A parked projector stays up,
//! reports `wallet_transition_limit_exceeded`, and keeps renewing the one
//! lease it holds. It plans again only after the writer commits, and it
//! resumes following only when that plan differs from the refused one.

use tokio_util::sync::CancellationToken;
use zinder_runtime::{Readiness, ReadinessState};
use zinder_store::{CanonicalEventFence, RocksDbCanonicalSecondary};
use zinder_wallet_projection::WalletCanonicalSourceIdentity;

use crate::{
    ConvergenceLease, NextWalletTransition, ProjectorControlTasks, RetainedEventPageValidation,
    canonical_writer_control::{CanonicalRetentionLease, CanonicalWriterControlClient},
    config::{ProjectorConfig, ProjectorError},
    converge_on_writer_fence, fetch_retained_event_page_to_target,
    following_readiness::FollowingReadiness,
    plan_next_wallet_transition,
    projector_control::ProjectorControlCommand,
    rebuild_required_for_expired_cursor, renew_following_retention_lease_if_due,
};

/// A following transition the wallet store refused for its logical-byte limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RefusedTransition {
    /// Canonical fence the refused transition would have published.
    pub(crate) resulting_fence: CanonicalEventFence,
    /// Logical bytes accounted when planning stopped, a lower bound on what
    /// the complete transition needs.
    pub(crate) required_bytes: u64,
    /// Configured limit the transition exceeded.
    pub(crate) limit_bytes: u64,
}

impl RefusedTransition {
    /// Whether `next` is the refused transition again.
    ///
    /// The wallet cursor does not move while following is parked and retained
    /// events never change, so a plan that ends at the same fence replays the
    /// same blocks and would be refused at the same point.
    pub(crate) fn is_planned_again(&self, next: &NextWalletTransition) -> bool {
        next.resulting_fence() == self.resulting_fence
    }

    /// Logs the park once and publishes its readiness.
    fn announce(self, readiness: &Readiness, wallet_height: u32, target_height: u32) {
        let error = ProjectorError::WalletTransitionLogicalByteLimit {
            limit_bytes: self.limit_bytes,
            required_bytes: self.required_bytes,
        };
        tracing::error!(
            target: "zinder::projector",
            event = "wallet_projection_following_parked",
            %error,
            required_bytes = self.required_bytes,
            limit_bytes = self.limit_bytes,
            wallet_height,
            target_height,
            "wallet following parked on a refused transition; the projector stays not ready and keeps its canonical retention lease"
        );
        self.publish(readiness, wallet_height, target_height);
    }

    fn publish(self, readiness: &Readiness, wallet_height: u32, target_height: u32) {
        readiness.set(ReadinessState::wallet_transition_limit_exceeded(
            self.required_bytes,
            self.limit_bytes,
            Some(wallet_height),
            Some(target_height),
        ));
    }
}

/// How a parked follower stopped waiting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParkedFollowing {
    /// Shutdown was requested; the retention lease stays held, as it does
    /// for any cancelled follower.
    Cancelled,
    /// A later writer event changed the next transition, so following plans
    /// and applies it.
    TransitionChanged,
}

/// The retention lease a parked follower keeps renewing, with the source
/// it is anchored at.
///
/// A bootstrapping follower anchors its lease at the first retained event
/// after the wallet cursor, not at the cursor itself.
pub(crate) struct ParkedLease<'a> {
    pub(crate) lease: &'a mut CanonicalRetentionLease,
    pub(crate) anchor: WalletCanonicalSourceIdentity,
}

/// Holds wallet following on `refused` until shutdown or a writer event that
/// changes the next transition.
///
/// The wallet is unchanged and no retention lease is acquired or released.
/// Readiness reports `wallet_transition_limit_exceeded` with the wallet tip
/// and the writer tip it cannot reach. Owner-control commands are refused,
/// because the wallet cannot reach the canonical fence a state bundle would
/// capture. Any other failure ends following as it would outside the park.
#[allow(
    clippy::too_many_arguments,
    reason = "the park keeps the storage owners, the held lease, the refusal, and cancellation explicit, as the follow loop does"
)]
pub(crate) async fn park_on_refused_transition(
    config: &ProjectorConfig,
    readiness: &Readiness,
    cancel: &CancellationToken,
    canonical: &mut RocksDbCanonicalSecondary,
    canonical_control: &mut CanonicalWriterControlClient,
    control: &mut ProjectorControlTasks,
    lease: ParkedLease<'_>,
    wallet_source: WalletCanonicalSourceIdentity,
    refused: RefusedTransition,
    target_height: u32,
) -> Result<ParkedFollowing, ProjectorError> {
    let ParkedLease {
        lease,
        anchor: lease_anchor,
    } = lease;
    let wallet_position = wallet_source.source_position();
    let wallet_height = wallet_position.tip.height.value();
    refused.announce(readiness, wallet_height, target_height);
    // Convergence reports lag into a readiness nobody serves, so the park
    // cause stays published until following resumes.
    let unserved = Readiness::default();
    let mut convergence_readiness = FollowingReadiness::for_wallet(&unserved, wallet_height);
    let mut planned_at_writer_sequence = Some(canonical.event_fence().chain_event_sequence());
    loop {
        if let Some(command) = control.next_command_or_poll(cancel).await? {
            refuse_parked_command(command, refused);
            continue;
        }
        if cancel.is_cancelled() {
            return Ok(ParkedFollowing::Cancelled);
        }
        let _renewed = renew_following_retention_lease_if_due(
            canonical_control,
            lease,
            lease_anchor,
            config.lease_duration,
        )
        .await?;
        let writer_sequence = canonical_control
            .writer_status()
            .await?
            .fence
            .map(|fence| fence.event_sequence);
        if writer_sequence == planned_at_writer_sequence {
            continue;
        }
        let Some(writer_fence) = converge_on_writer_fence(
            canonical,
            canonical_control,
            &mut convergence_readiness,
            Some(wallet_position.event_sequence),
            Some(ConvergenceLease {
                lease: &mut *lease,
                duration: config.lease_duration,
            }),
            cancel,
            config.fence_convergence_timeout,
        )
        .await?
        else {
            return Ok(ParkedFollowing::Cancelled);
        };
        let retained_events = match fetch_retained_event_page_to_target(
            canonical_control,
            canonical,
            wallet_source,
            canonical.event_fence(),
        )
        .await
        {
            Ok(RetainedEventPageValidation::Events(events)) => events,
            Ok(RetainedEventPageValidation::Reconverge) => {
                planned_at_writer_sequence = None;
                continue;
            }
            Err(error) => return Err(rebuild_required_for_expired_cursor(error)),
        };
        let next = plan_next_wallet_transition(canonical, &retained_events)?;
        planned_at_writer_sequence = Some(canonical.event_fence().chain_event_sequence());
        if !refused.is_planned_again(&next) {
            tracing::info!(
                target: "zinder::projector",
                event = "wallet_projection_following_unparked",
                wallet_height,
                target_height = writer_fence.writer_tip_height(),
                "a later canonical event changed the refused wallet transition; following plans it again"
            );
            return Ok(ParkedFollowing::TransitionChanged);
        }
        refused.publish(readiness, wallet_height, writer_fence.writer_tip_height());
    }
}

fn refuse_parked_command(command: ProjectorControlCommand, refused: RefusedTransition) {
    match command {
        ProjectorControlCommand::CreateStateBundleCapture { reply, .. } => {
            let _reply_sent = reply.send(Err(tonic::Status::failed_precondition(format!(
                "wallet following is parked: its next transition needs at least {} logical bytes, more than projector.follow.max_transition_logical_bytes allows ({}), so the wallet cannot reach the canonical fence a state bundle captures; raise the limit and restart the projector",
                refused.required_bytes, refused.limit_bytes
            ))));
        }
    }
}

#[cfg(test)]
mod tests;
