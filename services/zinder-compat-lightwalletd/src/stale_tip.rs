//! How old a served tip may be before lightwalletd tip calls refuse it.
//!
//! Wallets build a transaction to expire a fixed number of blocks after the
//! tip `GetLatestBlock` reported, 50 minutes of target block time with the
//! librustzcash defaults. A wallet shown a tip that stopped advancing, as it
//! does while the node is unreachable, would build transactions that are dead
//! on arrival. Go lightwalletd fails its tip calls while its node is down, and
//! wallets treat that `UNAVAILABLE` as "this server is not usable now", so the
//! compatibility server fails `GetLatestBlock` and `GetLightdInfo` the same
//! way once its served tip is older than the limit. Every other call keeps
//! serving the stored chain.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arc_swap::ArcSwapOption;
use tonic::Status;
use zinder_core::{BlockHeight, ChainEpochId};

/// Default stale-tip limit on Mainnet and Testnet: 25 minutes.
///
/// That is half the 50-minute window of the default transaction expiry, so a
/// wallet is told well before a transaction built on the served tip would
/// expire.
pub const DEFAULT_STALE_TIP_LIMIT: Duration = Duration::from_mins(25);

/// Counter of tip calls refused because the served tip was too old.
const STALE_TIP_REFUSALS_COUNTER: &str = "zinder_compat_stale_tip_refusals_total";

/// The longest a served tip's block may predate the clock before
/// `GetLatestBlock` and `GetLightdInfo` fail with `UNAVAILABLE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StaleTipLimit {
    limit: Option<Duration>,
}

impl StaleTipLimit {
    /// Never refuses, so tip calls answer through an outage of any length.
    pub const DISABLED: Self = Self { limit: None };

    /// Refuses tip calls once the served tip's block is older than `limit`.
    #[must_use]
    pub const fn after(limit: Duration) -> Self {
        Self { limit: Some(limit) }
    }

    /// Returns the limit, or `None` when disabled.
    #[must_use]
    pub const fn limit(self) -> Option<Duration> {
        self.limit
    }

    /// Checks a tip whose block time is `tip_block_time_unix_seconds` at
    /// `now_unix_seconds` for the tip call `method`.
    ///
    /// # Errors
    ///
    /// Returns `UNAVAILABLE` when the tip's block is older than the limit,
    /// and counts the refusal in
    /// `zinder_compat_stale_tip_refusals_total{method}`.
    pub fn check(
        self,
        method: &'static str,
        tip_block_time_unix_seconds: i64,
        now_unix_seconds: i64,
    ) -> Result<(), Status> {
        let Some(limit) = self.limit else {
            return Ok(());
        };
        let age_seconds =
            u64::try_from(now_unix_seconds.saturating_sub(tip_block_time_unix_seconds))
                .unwrap_or(0);
        if age_seconds <= limit.as_secs() {
            return Ok(());
        }
        metrics::counter!(STALE_TIP_REFUSALS_COUNTER, "method" => method).increment(1);
        Err(Status::unavailable(format!(
            "the newest block this server serves is {} minutes old, past its {}-minute stale-tip \
             limit; its node may be unreachable, so retry later or use another server",
            age_seconds / 60,
            limit.as_secs() / 60,
        )))
    }
}

/// The block time of the last served tip a tip call read, so the frequent
/// tip calls read a tip's header once per chain epoch instead of once per
/// call.
///
/// Clones share one cache.
#[derive(Clone, Debug, Default)]
pub struct TipBlockTimeCache {
    last: Arc<ArcSwapOption<(ChainEpochId, BlockHeight, i64)>>,
}

impl TipBlockTimeCache {
    /// Returns the block time of the tip at `height` in `chain_epoch_id`,
    /// reading it with `read_block_time` only when the cache holds another
    /// tip.
    ///
    /// # Errors
    ///
    /// Returns the error of `read_block_time`.
    pub async fn block_time<Read, ReadFuture>(
        &self,
        chain_epoch_id: ChainEpochId,
        height: BlockHeight,
        read_block_time: Read,
    ) -> Result<i64, Status>
    where
        Read: FnOnce() -> ReadFuture,
        ReadFuture: Future<Output = Result<i64, Status>>,
    {
        if let Some(cached) = self.cached(chain_epoch_id, height) {
            return Ok(cached);
        }
        let block_time = read_block_time().await?;
        self.store(chain_epoch_id, height, block_time);
        Ok(block_time)
    }

    /// Returns the cached block time of the tip at `height` in
    /// `chain_epoch_id`, if the cache holds that tip.
    #[must_use]
    pub fn cached(&self, chain_epoch_id: ChainEpochId, height: BlockHeight) -> Option<i64> {
        self.last
            .load()
            .as_deref()
            .and_then(|&(cached_epoch, cached_height, block_time)| {
                (cached_epoch == chain_epoch_id && cached_height == height).then_some(block_time)
            })
    }

    /// Records the block time of the tip at `height` in `chain_epoch_id`.
    pub fn store(&self, chain_epoch_id: ChainEpochId, height: BlockHeight, block_time: i64) {
        self.last
            .store(Some(Arc::new((chain_epoch_id, height, block_time))));
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

    use super::StaleTipLimit;

    const NOW: i64 = 1_800_000_000;

    #[test]
    fn a_tip_within_the_limit_answers_and_an_older_one_is_unavailable() {
        let limit = StaleTipLimit::after(Duration::from_mins(25));
        assert!(limit.check("get_latest_block", NOW - 25 * 60, NOW).is_ok());
        // A tip mined ahead of the local clock is fresh.
        assert!(limit.check("get_latest_block", NOW + 600, NOW).is_ok());
        assert_eq!(
            limit
                .check("get_latest_block", NOW - 25 * 60 - 1, NOW)
                .err()
                .map(|status| status.code()),
            Some(Code::Unavailable)
        );
    }

    #[test]
    fn a_disabled_limit_answers_any_tip() {
        assert!(
            StaleTipLimit::DISABLED
                .check("get_lightd_info", 0, NOW)
                .is_ok()
        );
    }
}
