//! How old a served tip may be before lightwalletd tip calls refuse it.
//!
//! Wallets build a transaction to expire a fixed number of blocks after the
//! tip `GetLatestBlock` reported, 50 minutes of target block time with the
//! librustzcash defaults. A wallet shown a tip that stopped advancing, as it
//! does while the node is unreachable, would build transactions that are dead
//! on arrival. Go lightwalletd fails its tip calls while its node is down, and
//! wallets treat that `UNAVAILABLE` as "this server is not usable now", so the
//! compatibility server fails `GetLatestBlock` and `GetLightdInfo` the same
//! way once its served tip is older than the limit and the writer's node does
//! not vouch for it. A tip the node still reports is the chain tip of a quiet
//! network and keeps answering. Every other call keeps serving the stored
//! chain.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arc_swap::ArcSwapOption;
use tonic::Status;
use zinder_core::{BlockHeight, ChainEpochId};

/// Default stale-tip limit on every network: 25 minutes.
///
/// That is half the 50-minute window of the default transaction expiry, so a
/// wallet is told well before a transaction built on the served tip would
/// expire.
pub const DEFAULT_STALE_TIP_LIMIT: Duration = Duration::from_mins(25);

/// Counter of tip calls refused because the served tip was too old.
const STALE_TIP_REFUSALS_COUNTER: &str = "zinder_compat_stale_tip_refusals_total";

/// The longest a served tip may age before `GetLatestBlock` and
/// `GetLightdInfo` fail with `UNAVAILABLE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StaleTipLimit {
    limit: Option<Duration>,
}

impl StaleTipLimit {
    /// Never refuses, so tip calls answer through an outage of any length.
    pub const DISABLED: Self = Self { limit: None };

    /// Refuses tip calls once the served tip is older than `limit`.
    #[must_use]
    pub const fn after(limit: Duration) -> Self {
        Self { limit: Some(limit) }
    }

    /// Returns the limit, or `None` when disabled.
    #[must_use]
    pub const fn limit(self) -> Option<Duration> {
        self.limit
    }

    /// Checks a tip whose age counts from `tip_aged_from_unix_seconds`, as
    /// [`TipBlockTime::aged_from_unix_seconds`] gives it, at
    /// `now_unix_seconds` for the tip call `method`.
    ///
    /// A tip the writer's node vouches for, `node_vouches`, is the chain tip
    /// however old it is, because the network has only been quiet, so only a
    /// tip the node cannot vouch for is refused.
    ///
    /// # Errors
    ///
    /// Returns `UNAVAILABLE` when the node does not vouch for the tip and the
    /// tip is older than the limit, and counts the refusal in
    /// `zinder_compat_stale_tip_refusals_total{method}`.
    pub fn check(
        self,
        method: &'static str,
        tip_aged_from_unix_seconds: i64,
        now_unix_seconds: i64,
        node_vouches: bool,
    ) -> Result<(), Status> {
        let Some(limit) = self.limit else {
            return Ok(());
        };
        if node_vouches {
            return Ok(());
        }
        let age_seconds =
            u64::try_from(now_unix_seconds.saturating_sub(tip_aged_from_unix_seconds)).unwrap_or(0);
        if age_seconds <= limit.as_secs() {
            return Ok(());
        }
        metrics::counter!(STALE_TIP_REFUSALS_COUNTER, "method" => method).increment(1);
        Err(Status::unavailable(format!(
            "the newest block this server serves is {} minutes old, past its {}-minute stale-tip \
             limit, and its node has not confirmed that block as the chain tip; retry later or use \
             another server",
            age_seconds / 60,
            limit.as_secs() / 60,
        )))
    }
}

/// The times of a served tip that tip calls use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TipBlockTime {
    /// The Unix time in the tip block's header, which `estimatedHeight`
    /// counts from.
    pub block_time_unix_seconds: i64,
    /// The Unix time the tip's age counts from: its block time, or when this
    /// process first read the tip if that is earlier.
    ///
    /// Consensus accepts a block time well ahead of the local clock, and such
    /// a tip would otherwise look fresh long after it stopped advancing.
    pub aged_from_unix_seconds: i64,
}

/// The times of the last served tip a tip call read, so the frequent tip
/// calls read a tip's header once per chain epoch instead of once per call.
///
/// Clones share one cache.
#[derive(Clone, Debug, Default)]
pub struct TipBlockTimeCache {
    last: Arc<ArcSwapOption<(ChainEpochId, BlockHeight, TipBlockTime)>>,
}

impl TipBlockTimeCache {
    /// Returns the times of the tip at `height` in `chain_epoch_id`, reading
    /// its block time with `read_block_time` only when the cache holds
    /// another tip.
    ///
    /// # Errors
    ///
    /// Returns the error of `read_block_time`.
    pub async fn block_time<Read, ReadFuture>(
        &self,
        chain_epoch_id: ChainEpochId,
        height: BlockHeight,
        read_block_time: Read,
    ) -> Result<TipBlockTime, Status>
    where
        Read: FnOnce() -> ReadFuture,
        ReadFuture: Future<Output = Result<i64, Status>>,
    {
        if let Some(cached) = self.cached(chain_epoch_id, height) {
            return Ok(cached);
        }
        let block_time = read_block_time().await?;
        Ok(self.store(chain_epoch_id, height, block_time, unix_now_seconds()))
    }

    /// Returns the cached times of the tip at `height` in `chain_epoch_id`,
    /// if the cache holds that tip.
    #[must_use]
    pub fn cached(
        &self,
        chain_epoch_id: ChainEpochId,
        height: BlockHeight,
    ) -> Option<TipBlockTime> {
        self.last
            .load()
            .as_deref()
            .and_then(|&(cached_epoch, cached_height, tip)| {
                (cached_epoch == chain_epoch_id && cached_height == height).then_some(tip)
            })
    }

    /// Records the tip at `height` in `chain_epoch_id`, whose block time is
    /// `block_time_unix_seconds`, read at `read_at_unix_seconds`, and returns
    /// its times.
    ///
    /// A newer chain epoch over the same tip block keeps the time the cache
    /// first read that tip.
    #[must_use]
    pub fn store(
        &self,
        chain_epoch_id: ChainEpochId,
        height: BlockHeight,
        block_time_unix_seconds: i64,
        read_at_unix_seconds: i64,
    ) -> TipBlockTime {
        let first_read_unix_seconds = self
            .last
            .load()
            .as_deref()
            .filter(|&&(_, cached_height, cached)| {
                cached_height == height && cached.block_time_unix_seconds == block_time_unix_seconds
            })
            .map_or(read_at_unix_seconds, |&(_, _, cached)| {
                cached.aged_from_unix_seconds
            });
        let tip = TipBlockTime {
            block_time_unix_seconds,
            aged_from_unix_seconds: block_time_unix_seconds.min(first_read_unix_seconds),
        };
        self.last
            .store(Some(Arc::new((chain_epoch_id, height, tip))));
        tip
    }
}

/// Returns the current Unix time in whole seconds.
#[must_use]
pub fn unix_now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since_epoch| {
            i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::Code;
    use zinder_core::{BlockHeight, ChainEpochId};

    use super::{StaleTipLimit, TipBlockTimeCache};

    const NOW: i64 = 1_800_000_000;

    #[test]
    fn a_tip_within_the_limit_answers_and_an_older_one_is_unavailable() {
        let limit = StaleTipLimit::after(Duration::from_mins(25));
        assert!(
            limit
                .check("get_latest_block", NOW - 25 * 60, NOW, false)
                .is_ok()
        );
        // A tip mined ahead of the local clock is fresh.
        assert!(
            limit
                .check("get_latest_block", NOW + 600, NOW, false)
                .is_ok()
        );
        assert_eq!(
            limit
                .check("get_latest_block", NOW - 25 * 60 - 1, NOW, false)
                .err()
                .map(|status| status.code()),
            Some(Code::Unavailable)
        );
        // The node vouches for an old tip: the chain is only quiet.
        assert!(
            limit
                .check("get_latest_block", NOW - 24 * 60 * 60, NOW, true)
                .is_ok()
        );
    }

    #[test]
    fn a_future_dated_tip_ages_from_when_it_was_first_read() {
        let limit = StaleTipLimit::after(Duration::from_mins(25));
        let cache = TipBlockTimeCache::default();
        let (epoch, height) = (ChainEpochId::new(7), BlockHeight::new(120));
        // Consensus accepts a block time well ahead of the local clock.
        let tip = cache.store(epoch, height, NOW + 90 * 60, NOW);
        assert_eq!(tip.block_time_unix_seconds, NOW + 90 * 60);
        assert!(
            limit
                .check("get_latest_block", tip.aged_from_unix_seconds, NOW, false)
                .is_ok()
        );
        // The node is down 26 minutes after the tip was first served.
        assert_eq!(
            limit
                .check(
                    "get_latest_block",
                    tip.aged_from_unix_seconds,
                    NOW + 26 * 60,
                    false
                )
                .err()
                .map(|status| status.code()),
            Some(Code::Unavailable)
        );
        // A later epoch over the same tip keeps the first read.
        let later = cache.store(ChainEpochId::new(8), height, NOW + 90 * 60, NOW + 20 * 60);
        assert_eq!(later.aged_from_unix_seconds, NOW);
        // A past block time counts from the block.
        let past = cache.store(epoch, BlockHeight::new(121), NOW - 60, NOW);
        assert_eq!(past.aged_from_unix_seconds, NOW - 60);
    }

    #[test]
    fn a_disabled_limit_answers_any_tip() {
        assert!(
            StaleTipLimit::DISABLED
                .check("get_lightd_info", 0, NOW, false)
                .is_ok()
        );
    }
}
