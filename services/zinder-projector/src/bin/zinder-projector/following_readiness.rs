//! Readiness of a projector that follows the canonical writer.
//!
//! Every block puts the wallet behind the canonical fence until the projector
//! applies it, and a burst of blocks keeps it behind until the projector has
//! applied the last of them. Reporting `syncing` for that would make normal
//! block arrival look like an outage to anything that polls `/readyz`, so a
//! ready projector that keeps applying blocks stays `ready` through a bounded
//! trail and reports the trailing target height instead.

use std::time::{Duration, Instant};

use tokio_util::sync::{CancellationToken, DropGuard};
use zinder_runtime::{CanonicalFenceLag, Readiness, ReadinessCause, ReadinessState};

/// Longest a ready projector may trail the canonical fence without its wallet
/// applying a block before it reports `syncing`.
///
/// Applying one block to the wallet store normally takes well under a second;
/// the grace leaves room for a large block on a loaded host.
pub(crate) const FOLLOWING_STALL_GRACE: Duration = Duration::from_secs(30);

/// Longest a ready projector may keep trailing the canonical fence, however
/// many blocks it applies meanwhile, before it reports `syncing`.
///
/// A burst of blocks keeps the wallet behind until the projector applies the
/// last of them. The bound covers a burst of several blocks on a loaded host
/// and still surfaces a projector that cannot keep up with the writer.
pub(crate) const FOLLOWING_TRAIL_TOLERANCE: Duration = Duration::from_mins(2);

/// Publishes the projector's following readiness, holding `ready` while a
/// ready projector trails the canonical fence within its bounds.
#[derive(Debug)]
pub(crate) struct FollowingReadiness<'a> {
    readiness: &'a Readiness,
    bounds: TrailBounds,
    /// Authenticated wallet tip height, when a wallet store is open.
    wallet_height: Option<u32>,
    trail: Option<Trail>,
    /// Whether the last convergence deadline passed without the secondary
    /// reaching the writer, and nothing has advanced the wallet since.
    secondary_stale: bool,
}

#[derive(Clone, Copy, Debug)]
struct TrailBounds {
    stall_grace: Duration,
    tolerance: Duration,
}

impl TrailBounds {
    const FOLLOWING: Self = Self {
        stall_grace: FOLLOWING_STALL_GRACE,
        tolerance: FOLLOWING_TRAIL_TOLERANCE,
    };
}

/// A trail that keeps a ready projector `ready`. Dropping it ends the trail
/// and disarms its expiry task.
#[derive(Debug)]
struct Trail {
    started_at: Instant,
    /// Wallet tip height when the wallet last made progress during this
    /// trail; progress past it restarts the stall grace.
    wallet_height: u32,
    advanced_at: Instant,
    _expiry: DropGuard,
}

impl Trail {
    /// Returns how long this trail may still hold `ready` at `now`, which is
    /// zero once either bound has passed.
    fn remaining(&self, bounds: TrailBounds, now: Instant) -> Duration {
        remaining_trail(bounds, self.started_at, self.advanced_at, now)
    }
}

fn remaining_trail(
    bounds: TrailBounds,
    started_at: Instant,
    advanced_at: Instant,
    now: Instant,
) -> Duration {
    let stall = bounds
        .stall_grace
        .saturating_sub(now.saturating_duration_since(advanced_at));
    let tolerance = bounds
        .tolerance
        .saturating_sub(now.saturating_duration_since(started_at));
    stall.min(tolerance)
}

impl<'a> FollowingReadiness<'a> {
    /// Returns the readiness of a projector that has no wallet store open.
    pub(crate) const fn new(readiness: &'a Readiness) -> Self {
        Self::with_bounds(readiness, None, TrailBounds::FOLLOWING)
    }

    /// Returns the readiness of a projector whose wallet tip is at
    /// `wallet_height`.
    pub(crate) const fn for_wallet(readiness: &'a Readiness, wallet_height: u32) -> Self {
        Self::with_bounds(readiness, Some(wallet_height), TrailBounds::FOLLOWING)
    }

    const fn with_bounds(
        readiness: &'a Readiness,
        wallet_height: Option<u32>,
        bounds: TrailBounds,
    ) -> Self {
        Self {
            readiness,
            bounds,
            wallet_height,
            trail: None,
            secondary_stale: false,
        }
    }

    /// Reports that the wallet matches the canonical fence at `height`.
    pub(crate) fn caught_up(&mut self, height: u32) {
        self.wallet_height = Some(height);
        self.trail = None;
        self.secondary_stale = false;
        self.readiness.set(ReadinessState::ready(Some(height)));
    }

    /// Records that the wallet applied a transition up to `height`, which
    /// restarts the stall grace of a running trail.
    pub(crate) fn wallet_advanced(&mut self, height: u32) {
        self.wallet_height = Some(height);
        if self
            .trail
            .as_ref()
            .is_some_and(|trail| trail.wallet_height != height)
        {
            self.advance_trail(height, Instant::now());
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
    /// report carries the wallet height whenever a wallet store is open. A
    /// stale secondary keeps reporting its staleness instead.
    pub(crate) fn writer_ahead(&mut self, secondary_height: u32, writer_height: u32) {
        if self.secondary_stale {
            return;
        }
        let Some(wallet_height) = self.wallet_height else {
            self.trail = None;
            self.readiness.set(ReadinessState::syncing(
                writer_height.checked_sub(secondary_height).map(u64::from),
                Some(secondary_height),
                Some(writer_height),
            ));
            return;
        };
        self.trailing(wallet_height, writer_height);
    }

    /// Reports that the canonical secondary has not reached the writer within
    /// a whole convergence deadline, with the last lag observed, if any.
    ///
    /// This is staleness rather than block arrival, so it ends any trail. The
    /// projector reports `replica_lagging`, or `writer_status_unavailable`
    /// when no writer status classified the secondary, until the secondary
    /// gives the wallet a block to apply or the wallet catches up.
    pub(crate) fn writer_unreached(&mut self, lag: Option<CanonicalFenceLag>) {
        self.trail = None;
        self.secondary_stale = true;
        let current_height = self
            .wallet_height
            .or_else(|| lag.map(|lag| lag.secondary_visible_tip_height));
        self.readiness.set(lag.map_or_else(
            || ReadinessState {
                current_height,
                ..ReadinessState::not_ready(ReadinessCause::WriterStatusUnavailable)
            },
            |lag| ReadinessState {
                target_height: Some(lag.writer_visible_tip_height),
                ..ReadinessState::replica_lagging(lag.lag_chain_epochs, current_height)
            },
        ));
    }

    fn trailing_at(&mut self, wallet_height: u32, target_height: u32, now: Instant) {
        self.wallet_height = Some(wallet_height);
        self.secondary_stale = false;
        let lag_blocks = target_height.checked_sub(wallet_height);
        if lag_blocks.is_some() && matches!(self.readiness.report().cause, ReadinessCause::Ready) {
            match self.trail.as_ref().map(|trail| trail.wallet_height) {
                None => self.start_trail(wallet_height, now),
                Some(trail_height) if trail_height != wallet_height => {
                    self.advance_trail(wallet_height, now);
                }
                Some(_) => {}
            }
            if self
                .trail
                .as_ref()
                .is_some_and(|trail| !trail.remaining(self.bounds, now).is_zero())
            {
                self.readiness.set(ReadinessState::ready_with_target(
                    Some(wallet_height),
                    Some(target_height),
                ));
                return;
            }
        }
        self.trail = None;
        self.readiness.set(ReadinessState::syncing(
            lag_blocks.map(u64::from),
            Some(wallet_height),
            Some(target_height),
        ));
    }

    fn start_trail(&mut self, wallet_height: u32, now: Instant) {
        self.arm_trail(now, wallet_height, now);
    }

    fn advance_trail(&mut self, wallet_height: u32, now: Instant) {
        let started_at = self.trail.as_ref().map_or(now, |trail| trail.started_at);
        self.arm_trail(started_at, wallet_height, now);
    }

    /// Replaces the running trail and arms a task that reports `syncing` when
    /// the new trail expires, so the bounds hold while the follow loop is
    /// blocked on a slow request or a large transition.
    fn arm_trail(&mut self, started_at: Instant, wallet_height: u32, advanced_at: Instant) {
        // Replacing the previous trail drops its guard, which disarms its
        // expiry before this trail publishes anything.
        self.trail = None;
        let expiry = CancellationToken::new();
        let disarmed = expiry.clone();
        let readiness = self.readiness.clone();
        let remaining = remaining_trail(self.bounds, started_at, advanced_at, advanced_at);
        tokio::spawn(async move {
            tokio::select! {
                () = disarmed.cancelled() => {}
                () = tokio::time::sleep(remaining) => readiness.update(|state| {
                    // Checked under the readiness lock: a trail that ended
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
        self.trail = Some(Trail {
            started_at,
            wallet_height,
            advanced_at,
            _expiry: expiry.drop_guard(),
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use zinder_runtime::{CanonicalFenceLag, Readiness, ReadinessCause, ReadinessState};

    use super::{
        FOLLOWING_STALL_GRACE, FOLLOWING_TRAIL_TOLERANCE, FollowingReadiness, TrailBounds,
    };

    fn assert_syncing(readiness: &Readiness, lag_blocks: Option<u64>) {
        let report = readiness.report();
        assert!(!report.is_ready);
        assert_eq!(report.cause, ReadinessCause::Syncing { lag_blocks });
    }

    fn assert_ready_at(readiness: &Readiness, current_height: u32, target_height: u32) {
        let report = readiness.report();
        assert!(report.is_ready, "expected ready, got {:?}", report.cause);
        assert_eq!(report.cause, ReadinessCause::Ready);
        assert_eq!(report.current_height, Some(current_height));
        assert_eq!(report.target_height, Some(target_height));
    }

    #[tokio::test]
    async fn a_ready_projector_one_block_behind_stays_ready_with_the_target() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();

        following.trailing_at(10, 11, started);
        following.trailing_at(10, 11, started + Duration::from_secs(5));
        assert_ready_at(&readiness, 10, 11);

        following.caught_up(11);
        assert_ready_at(&readiness, 11, 11);
    }

    #[tokio::test]
    async fn a_same_height_replacement_stays_ready() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);

        following.trailing_at(10, 10, Instant::now());

        assert!(readiness.report().is_ready);
    }

    #[tokio::test]
    async fn a_multi_block_burst_stays_ready_while_the_projector_applies_it() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();

        // Five blocks land at once, and two more arrive while the projector
        // applies the first ones.
        for (offset_seconds, wallet_height, target_height) in [
            (0, 10, 15),
            (6, 11, 15),
            (12, 12, 16),
            (18, 13, 16),
            (24, 14, 17),
            (30, 15, 17),
            (36, 16, 17),
        ] {
            following.trailing_at(
                wallet_height,
                target_height,
                started + Duration::from_secs(offset_seconds),
            );
            assert_ready_at(&readiness, wallet_height, target_height);
        }

        following.caught_up(17);
        assert_ready_at(&readiness, 17, 17);
    }

    #[tokio::test]
    async fn a_trail_without_an_applied_block_reports_syncing_after_the_stall_grace() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();

        following.trailing_at(10, 13, started);
        following.trailing_at(10, 13, started + FOLLOWING_STALL_GRACE);

        assert_syncing(&readiness, Some(3));
    }

    #[tokio::test]
    async fn a_trail_that_outlasts_the_tolerance_reports_syncing_while_it_advances() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();
        let block_interval = FOLLOWING_STALL_GRACE.saturating_sub(Duration::from_secs(1));

        let mut now = started;
        let mut wallet_height = 10;
        while now.saturating_duration_since(started) < FOLLOWING_TRAIL_TOLERANCE {
            following.trailing_at(wallet_height, wallet_height + 3, now);
            assert!(readiness.report().is_ready);
            wallet_height += 1;
            now += block_interval;
        }
        following.trailing_at(
            wallet_height,
            wallet_height + 3,
            started + FOLLOWING_TRAIL_TOLERANCE,
        );

        assert_syncing(&readiness, Some(3));
    }

    #[tokio::test]
    async fn catching_up_ends_the_trail_and_its_tolerance() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();
        let next_burst =
            started + FOLLOWING_TRAIL_TOLERANCE.saturating_sub(Duration::from_secs(10));

        following.trailing_at(10, 12, started);
        following.trailing_at(11, 12, started + Duration::from_secs(10));
        following.caught_up(12);
        following.trailing_at(12, 14, next_burst);
        following.trailing_at(13, 14, next_burst + Duration::from_secs(20));

        assert_ready_at(&readiness, 13, 14);
    }

    #[tokio::test]
    async fn each_applied_block_restarts_the_stall_grace_without_catching_up() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();
        let block_interval = FOLLOWING_STALL_GRACE.saturating_sub(Duration::from_secs(1));

        following.trailing_at(10, 11, started);
        following.trailing_at(11, 12, started + block_interval);
        following.trailing_at(12, 13, started + block_interval * 2);

        assert_ready_at(&readiness, 12, 13);
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
    async fn a_syncing_report_ends_the_trail() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);
        let started = Instant::now();

        following.trailing_at(10, 11, started);
        following.trailing_at(10, 9, started + Duration::from_secs(1));
        assert_syncing(&readiness, None);

        readiness.set(ReadinessState::ready(Some(10)));
        following.trailing_at(10, 11, started + FOLLOWING_TRAIL_TOLERANCE);
        assert_ready_at(&readiness, 10, 11);
    }

    #[tokio::test]
    async fn the_stall_grace_expires_while_no_further_report_arrives() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let bounds = TrailBounds {
            stall_grace: Duration::from_millis(50),
            tolerance: Duration::from_mins(1),
        };
        let mut following = FollowingReadiness::with_bounds(&readiness, Some(10), bounds);

        following.trailing(10, 11);
        assert!(readiness.report().is_ready);
        tokio::time::sleep(bounds.stall_grace * 4).await;

        assert_syncing(&readiness, Some(1));
        let report = readiness.report();
        assert_eq!(report.current_height, Some(10));
        assert_eq!(report.target_height, Some(11));
    }

    #[tokio::test]
    async fn the_trail_tolerance_expires_while_the_wallet_keeps_advancing() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let bounds = TrailBounds {
            stall_grace: Duration::from_mins(1),
            tolerance: Duration::from_millis(200),
        };
        let mut following = FollowingReadiness::with_bounds(&readiness, Some(10), bounds);

        following.trailing(10, 14);
        tokio::time::sleep(Duration::from_millis(60)).await;
        following.wallet_advanced(11);
        tokio::time::sleep(Duration::from_millis(60)).await;
        following.wallet_advanced(12);
        assert!(readiness.report().is_ready);
        tokio::time::sleep(bounds.tolerance * 3).await;

        assert_syncing(&readiness, Some(4));
    }

    #[tokio::test]
    async fn an_ended_trail_does_not_expire_later() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let bounds = TrailBounds {
            stall_grace: Duration::from_millis(50),
            tolerance: Duration::from_millis(50),
        };
        let mut following = FollowingReadiness::with_bounds(&readiness, Some(10), bounds);

        following.trailing(10, 11);
        following.caught_up(11);
        tokio::time::sleep(bounds.stall_grace * 4).await;

        assert!(readiness.report().is_ready);
    }

    #[tokio::test]
    async fn an_applied_block_rearms_the_stall_expiry() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let bounds = TrailBounds {
            stall_grace: Duration::from_secs(1),
            tolerance: Duration::from_mins(1),
        };
        let mut following = FollowingReadiness::with_bounds(&readiness, Some(10), bounds);

        following.trailing(10, 12);
        tokio::time::sleep(bounds.stall_grace * 3 / 5).await;
        following.wallet_advanced(11);
        tokio::time::sleep(bounds.stall_grace * 3 / 5).await;

        assert!(readiness.report().is_ready);
    }

    #[tokio::test]
    async fn writer_lag_keeps_a_ready_projector_ready_at_the_wallet_height() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);

        following.writer_ahead(11, 13);

        assert_ready_at(&readiness, 10, 13);
    }

    #[tokio::test]
    async fn writer_lag_reports_a_projector_that_was_not_ready_as_syncing() {
        let readiness = Readiness::new(ReadinessState::syncing(Some(4), Some(8), Some(12)));
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

    #[tokio::test]
    async fn an_unreached_writer_reports_replica_lagging_until_the_wallet_catches_up() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);

        following.trailing(10, 11);
        following.writer_unreached(Some(CanonicalFenceLag {
            lag_chain_epochs: 3,
            secondary_visible_tip_height: 10,
            writer_visible_tip_height: 13,
        }));
        let report = readiness.report();
        assert!(!report.is_ready);
        assert_eq!(
            report.cause,
            ReadinessCause::ReplicaLagging {
                lag_chain_epochs: 3
            }
        );
        assert_eq!(report.current_height, Some(10));
        assert_eq!(report.target_height, Some(13));

        following.writer_ahead(10, 14);
        assert_eq!(
            readiness.report().cause,
            ReadinessCause::ReplicaLagging {
                lag_chain_epochs: 3
            }
        );

        following.trailing(11, 13);
        assert_syncing(&readiness, Some(2));
        following.caught_up(13);
        assert_ready_at(&readiness, 13, 13);
    }

    #[tokio::test]
    async fn an_unanswered_writer_status_reports_writer_status_unavailable() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::for_wallet(&readiness, 10);

        following.writer_unreached(None);

        let report = readiness.report();
        assert!(!report.is_ready);
        assert_eq!(report.cause, ReadinessCause::WriterStatusUnavailable);
        assert_eq!(report.current_height, Some(10));
    }
}
