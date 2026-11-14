//! Readiness of a projector that follows the canonical writer.
//!
//! Every block puts the wallet one block behind the canonical fence until the
//! projector applies it. Reporting `syncing` for that moment would make a
//! normal block look like an outage to anything that polls `/readyz`, so a
//! ready projector that trails by at most one block stays `ready` for a
//! bounded grace and reports the trailing target height instead.

use std::time::{Duration, Instant};

use tokio_util::sync::{CancellationToken, DropGuard};
use zinder_runtime::{Readiness, ReadinessCause, ReadinessState};

/// Longest a ready projector may trail the canonical fence by one block
/// without applying a block before it reports `syncing`.
///
/// Applying one block to the wallet store normally takes well under a second;
/// the grace leaves room for a large block on a loaded host.
pub(crate) const FOLLOWING_TIP_TRANSITION_GRACE: Duration = Duration::from_secs(30);

/// Largest lag, in blocks, that the tip-transition grace admits.
const FOLLOWING_TIP_TRANSITION_MAX_LAG_BLOCKS: u32 = 1;

/// Publishes the projector's following readiness, holding `ready` through a
/// one-block tip transition.
#[derive(Debug)]
pub(crate) struct FollowingReadiness<'a> {
    readiness: &'a Readiness,
    grace_period: Duration,
    /// Authenticated wallet tip height, when a wallet store is open.
    wallet_height: Option<u32>,
    grace: Option<TransitionGrace>,
}

/// A running tip-transition grace. Dropping it ends the grace and disarms its
/// expiry task.
#[derive(Debug)]
struct TransitionGrace {
    /// Wallet tip height when this grace started; wallet progress past it
    /// starts a new grace.
    wallet_height: u32,
    started_at: Instant,
    _expiry: DropGuard,
}

impl<'a> FollowingReadiness<'a> {
    /// Returns the readiness of a projector that has no wallet store open.
    pub(crate) const fn new(readiness: &'a Readiness) -> Self {
        Self::with_grace_period(readiness, None, FOLLOWING_TIP_TRANSITION_GRACE)
    }

    /// Returns the readiness of a projector whose wallet tip is at
    /// `wallet_height`.
    pub(crate) const fn for_wallet(readiness: &'a Readiness, wallet_height: u32) -> Self {
        Self::with_grace_period(
            readiness,
            Some(wallet_height),
            FOLLOWING_TIP_TRANSITION_GRACE,
        )
    }

    const fn with_grace_period(
        readiness: &'a Readiness,
        wallet_height: Option<u32>,
        grace_period: Duration,
    ) -> Self {
        Self {
            readiness,
            grace_period,
            wallet_height,
            grace: None,
        }
    }

    /// Reports that the wallet matches the canonical fence at `height`.
    pub(crate) fn caught_up(&mut self, height: u32) {
        self.wallet_height = Some(height);
        self.grace = None;
        self.readiness.set(ReadinessState::ready(Some(height)));
    }

    /// Records that the wallet applied a transition up to `height`, which
    /// starts a new grace if one is running.
    pub(crate) fn wallet_advanced(&mut self, height: u32) {
        self.wallet_height = Some(height);
        if self
            .grace
            .as_ref()
            .is_some_and(|grace| grace.wallet_height != height)
        {
            self.start_grace(height, Instant::now());
        }
    }

    /// Reports a wallet at `wallet_height` trailing a canonical fence at
    /// `target_height`.
    pub(crate) fn trailing(&mut self, wallet_height: u32, target_height: u32) {
        self.trailing_at(wallet_height, target_height, Instant::now());
    }

    /// Reports a canonical secondary at `secondary_height` that has not yet
    /// caught up to a writer at `writer_height`.
    ///
    /// The wallet, not the secondary, is what the projector serves, so the
    /// report carries the wallet height whenever a wallet store is open.
    pub(crate) fn writer_ahead(&mut self, secondary_height: u32, writer_height: u32) {
        let Some(wallet_height) = self.wallet_height else {
            self.grace = None;
            self.readiness.set(ReadinessState::syncing(
                writer_height.checked_sub(secondary_height).map(u64::from),
                Some(secondary_height),
                Some(writer_height),
            ));
            return;
        };
        self.trailing(wallet_height, writer_height);
    }

    fn trailing_at(&mut self, wallet_height: u32, target_height: u32, now: Instant) {
        self.wallet_height = Some(wallet_height);
        let lag_blocks = target_height.checked_sub(wallet_height);
        let within_transition =
            lag_blocks.is_some_and(|lag| lag <= FOLLOWING_TIP_TRANSITION_MAX_LAG_BLOCKS);
        if within_transition && matches!(self.readiness.report().cause, ReadinessCause::Ready) {
            if self
                .grace
                .as_ref()
                .is_none_or(|grace| grace.wallet_height != wallet_height)
            {
                self.start_grace(wallet_height, now);
            }
            if self.grace.as_ref().is_some_and(|grace| {
                now.saturating_duration_since(grace.started_at) < self.grace_period
            }) {
                self.readiness.set(ReadinessState::ready_with_target(
                    Some(wallet_height),
                    Some(target_height),
                ));
                return;
            }
        }
        self.grace = None;
        self.readiness.set(ReadinessState::syncing(
            lag_blocks.map(u64::from),
            Some(wallet_height),
            Some(target_height),
        ));
    }

    /// Starts a grace and a task that reports `syncing` when it expires, so
    /// the bound holds while the follow loop is blocked on a slow request or
    /// a large transition.
    fn start_grace(&mut self, wallet_height: u32, now: Instant) {
        // Replacing the previous grace drops its guard, which disarms its
        // expiry before this grace publishes anything.
        self.grace = None;
        let expiry = CancellationToken::new();
        let disarmed = expiry.clone();
        let readiness = self.readiness.clone();
        let grace_period = self.grace_period;
        tokio::spawn(async move {
            tokio::select! {
                () = disarmed.cancelled() => {}
                () = tokio::time::sleep(grace_period) => readiness.update(|state| {
                    // Checked under the readiness lock: a grace that ended
                    // before this point has already published its own state.
                    if !disarmed.is_cancelled() && matches!(state.cause, ReadinessCause::Ready) {
                        state.cause = ReadinessCause::Syncing {
                            lag_blocks: state
                                .target_height
                                .zip(state.current_height)
                                .and_then(|(target, current)| target.checked_sub(current))
                                .map(u64::from),
                        };
                    }
                }),
            }
        });
        self.grace = Some(TransitionGrace {
            wallet_height,
            started_at: now,
            _expiry: expiry.drop_guard(),
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use zinder_runtime::{Readiness, ReadinessCause, ReadinessState};

    use super::{FOLLOWING_TIP_TRANSITION_GRACE, FollowingReadiness};

    fn assert_syncing(readiness: &Readiness, lag_blocks: Option<u64>) {
        let report = readiness.report();
        assert!(!report.is_ready);
        assert_eq!(report.cause, ReadinessCause::Syncing { lag_blocks });
    }

    #[tokio::test]
    async fn a_ready_projector_one_block_behind_stays_ready_with_the_target() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();

        following.trailing_at(10, 11, started);
        following.trailing_at(10, 11, started + Duration::from_secs(5));

        let report = readiness.report();
        assert!(report.is_ready);
        assert_eq!(report.cause, ReadinessCause::Ready);
        assert_eq!(report.current_height, Some(10));
        assert_eq!(report.target_height, Some(11));

        following.caught_up(11);
        let report = readiness.report();
        assert!(report.is_ready);
        assert_eq!(report.current_height, Some(11));
        assert_eq!(report.target_height, Some(11));
    }

    #[tokio::test]
    async fn a_same_height_replacement_stays_ready() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);

        following.trailing_at(10, 10, Instant::now());

        assert!(readiness.report().is_ready);
    }

    #[tokio::test]
    async fn a_trail_that_outlasts_the_grace_reports_syncing() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();

        following.trailing_at(10, 11, started);
        following.trailing_at(10, 11, started + FOLLOWING_TIP_TRANSITION_GRACE);

        assert_syncing(&readiness, Some(1));
    }

    #[tokio::test]
    async fn each_caught_up_block_restarts_the_grace() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();

        following.trailing_at(10, 11, started);
        following.caught_up(11);
        let next_block = started + FOLLOWING_TIP_TRANSITION_GRACE;
        following.trailing_at(11, 12, next_block);

        assert!(readiness.report().is_ready);
    }

    #[tokio::test]
    async fn each_applied_block_restarts_the_grace_without_catching_up() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();
        let block_interval = FOLLOWING_TIP_TRANSITION_GRACE.saturating_sub(Duration::from_secs(1));

        following.trailing_at(10, 11, started);
        following.trailing_at(11, 12, started + block_interval);
        following.trailing_at(12, 13, started + block_interval * 2);

        let report = readiness.report();
        assert!(report.is_ready);
        assert_eq!(report.current_height, Some(12));
        assert_eq!(report.target_height, Some(13));
    }

    #[tokio::test]
    async fn lag_beyond_one_block_reports_syncing() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);

        following.trailing_at(10, 12, Instant::now());

        assert_syncing(&readiness, Some(2));
    }

    #[tokio::test]
    async fn a_rollback_below_the_projector_reports_syncing() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);

        following.trailing_at(10, 9, Instant::now());

        assert_syncing(&readiness, None);
    }

    #[tokio::test]
    async fn a_projector_that_was_not_ready_reports_syncing() {
        let readiness = Readiness::new(ReadinessState::syncing(Some(5), Some(5), Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);

        following.trailing_at(10, 11, Instant::now());

        assert_syncing(&readiness, Some(1));
    }

    #[tokio::test]
    async fn a_syncing_report_ends_the_grace() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();

        following.trailing_at(10, 11, started);
        following.trailing_at(10, 13, started + Duration::from_secs(1));
        assert_syncing(&readiness, Some(3));

        readiness.set(ReadinessState::ready(Some(13)));
        following.trailing_at(13, 14, started + FOLLOWING_TIP_TRANSITION_GRACE);
        assert!(readiness.report().is_ready);
    }

    #[tokio::test]
    async fn the_grace_expires_while_no_further_report_arrives() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let grace_period = Duration::from_millis(50);
        let mut following =
            FollowingReadiness::with_grace_period(&readiness, Some(10), grace_period);

        following.trailing(10, 11);
        assert!(readiness.report().is_ready);
        tokio::time::sleep(grace_period * 4).await;

        assert_syncing(&readiness, Some(1));
        let report = readiness.report();
        assert_eq!(report.current_height, Some(10));
        assert_eq!(report.target_height, Some(11));
    }

    #[tokio::test]
    async fn an_ended_grace_does_not_expire_later() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let grace_period = Duration::from_millis(50);
        let mut following =
            FollowingReadiness::with_grace_period(&readiness, Some(10), grace_period);

        following.trailing(10, 11);
        following.caught_up(11);
        tokio::time::sleep(grace_period * 4).await;

        assert!(readiness.report().is_ready);
    }

    #[tokio::test]
    async fn an_applied_block_rearms_the_grace_expiry() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let grace_period = Duration::from_secs(1);
        let mut following =
            FollowingReadiness::with_grace_period(&readiness, Some(10), grace_period);

        following.trailing(10, 11);
        tokio::time::sleep(grace_period * 3 / 5).await;
        following.wallet_advanced(11);
        tokio::time::sleep(grace_period * 3 / 5).await;

        assert!(readiness.report().is_ready);
    }

    #[tokio::test]
    async fn writer_lag_reports_the_wallet_height() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);

        following.writer_ahead(11, 12);

        assert_syncing(&readiness, Some(2));
        assert_eq!(readiness.report().current_height, Some(10));
    }

    #[tokio::test]
    async fn writer_lag_without_a_wallet_reports_the_secondary_height() {
        let readiness = Readiness::new(ReadinessState::ready(Some(1)));
        let mut following = FollowingReadiness::new(&readiness);

        following.writer_ahead(1, 2);

        assert_syncing(&readiness, Some(1));
        assert_eq!(readiness.report().current_height, Some(1));
    }
}
