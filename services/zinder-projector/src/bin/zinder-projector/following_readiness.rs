//! Readiness of a projector that follows the canonical writer.
//!
//! Every block puts the wallet one block behind the canonical fence until the
//! projector applies it. Reporting `syncing` for that moment would make a
//! normal block look like an outage to anything that polls `/readyz`, so a
//! ready projector that trails by at most one block stays `ready` for a
//! bounded grace and reports the trailing target height instead.

use std::time::{Duration, Instant};

use zinder_runtime::{Readiness, ReadinessCause, ReadinessState};

/// Longest a ready projector may trail the canonical fence by one block
/// before it reports `syncing`.
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
    /// When a ready projector first trailed its target in the current
    /// transition.
    trailing_since: Option<Instant>,
}

impl<'a> FollowingReadiness<'a> {
    pub(crate) const fn new(readiness: &'a Readiness) -> Self {
        Self {
            readiness,
            trailing_since: None,
        }
    }

    /// Reports that the projector state matches the canonical fence at
    /// `height`.
    pub(crate) fn caught_up(&mut self, height: u32) {
        self.trailing_since = None;
        self.readiness.set(ReadinessState::ready(Some(height)));
    }

    /// Reports projector state at `current_height` trailing a canonical fence
    /// at `target_height`.
    pub(crate) fn trailing(&mut self, current_height: u32, target_height: u32) {
        self.trailing_at(current_height, target_height, Instant::now());
    }

    fn trailing_at(&mut self, current_height: u32, target_height: u32, now: Instant) {
        let lag_blocks = target_height.checked_sub(current_height);
        let within_transition =
            lag_blocks.is_some_and(|lag| lag <= FOLLOWING_TIP_TRANSITION_MAX_LAG_BLOCKS);
        if within_transition && matches!(self.readiness.report().cause, ReadinessCause::Ready) {
            let trailing_since = *self.trailing_since.get_or_insert(now);
            if now.saturating_duration_since(trailing_since) < FOLLOWING_TIP_TRANSITION_GRACE {
                self.readiness.set(ReadinessState::ready_with_target(
                    Some(current_height),
                    Some(target_height),
                ));
                return;
            }
        }
        self.trailing_since = None;
        self.readiness.set(ReadinessState::syncing(
            lag_blocks.map(u64::from),
            Some(current_height),
            Some(target_height),
        ));
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

    #[test]
    fn a_ready_projector_one_block_behind_stays_ready_with_the_target() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::new(&readiness);
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

    #[test]
    fn a_same_height_replacement_stays_ready() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::new(&readiness);

        following.trailing_at(10, 10, Instant::now());

        assert!(readiness.report().is_ready);
    }

    #[test]
    fn a_trail_that_outlasts_the_grace_reports_syncing() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::new(&readiness);
        let started = Instant::now();

        following.trailing_at(10, 11, started);
        following.trailing_at(10, 11, started + FOLLOWING_TIP_TRANSITION_GRACE);

        assert_syncing(&readiness, Some(1));
    }

    #[test]
    fn each_caught_up_block_restarts_the_grace() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::new(&readiness);
        let started = Instant::now();

        following.trailing_at(10, 11, started);
        following.caught_up(11);
        let next_block = started + FOLLOWING_TIP_TRANSITION_GRACE;
        following.trailing_at(11, 12, next_block);

        assert!(readiness.report().is_ready);
    }

    #[test]
    fn lag_beyond_one_block_reports_syncing() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::new(&readiness);

        following.trailing_at(10, 12, Instant::now());

        assert_syncing(&readiness, Some(2));
    }

    #[test]
    fn a_rollback_below_the_projector_reports_syncing() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::new(&readiness);

        following.trailing_at(10, 9, Instant::now());

        assert_syncing(&readiness, None);
    }

    #[test]
    fn a_projector_that_was_not_ready_reports_syncing() {
        let readiness = Readiness::new(ReadinessState::syncing(Some(5), Some(5), Some(10)));
        let mut following = FollowingReadiness::new(&readiness);

        following.trailing_at(10, 11, Instant::now());

        assert_syncing(&readiness, Some(1));
    }

    #[test]
    fn a_syncing_report_ends_the_grace() {
        let readiness = Readiness::new(ReadinessState::ready(Some(10)));
        let mut following = FollowingReadiness::new(&readiness);
        let started = Instant::now();

        following.trailing_at(10, 11, started);
        following.trailing_at(10, 13, started + Duration::from_secs(1));
        assert_syncing(&readiness, Some(3));

        readiness.set(ReadinessState::ready(Some(13)));
        following.trailing_at(13, 14, started + FOLLOWING_TIP_TRANSITION_GRACE);
        assert!(readiness.report().is_ready);
    }
}
