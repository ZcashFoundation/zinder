//! The bounded wait a height-selected block read makes when it names a height
//! just past the visible tip.
//!
//! Readers behind one load balancer publish each new block at slightly
//! different times, and the balancer routes every request on its own, so a
//! wallet can learn a tip from one reader and ask a reader still one block
//! behind for it. [`TipWait`] lets that request wait briefly for the block
//! instead of failing at once.
//!
//! The wait is a pre-step, not a read path. It decides only when the request
//! runs: it compares the requested height with the visible tip, waits for a
//! publication while the height is a few blocks ahead, and returns. The
//! method then runs exactly as it would have without a wait, against the
//! view current when the wait ends, so an expired wait answers with the
//! method's own status. A wait holds no view, serving generation, stream
//! slot, or read permit, only a place among a fixed number of waiters.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, watch},
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use zinder_core::BlockHeight;

use crate::block_methods::CompactBlockReads;

/// How long a request past the visible tip waits for its height by default.
pub const DEFAULT_TIP_WAIT: Duration = Duration::from_secs(3);

/// How far past the visible tip a request may name a height and still wait,
/// by default.
pub const DEFAULT_TIP_WAIT_MAX_BLOCKS: NonZeroU32 = match NonZeroU32::new(2) {
    Some(max_blocks_ahead) => max_blocks_ahead,
    None => NonZeroU32::MIN,
};

/// How many requests may wait for the tip at once, by default.
pub const DEFAULT_TIP_WAIT_MAX_WAITERS: NonZeroU32 = match NonZeroU32::new(256) {
    Some(max_waiters) => max_waiters,
    None => NonZeroU32::MIN,
};

#[cfg(test)]
mod tests;

const WAITS_COUNTER: &str = "zinder_compat_tip_wait_total";
const WAIT_DURATION_HISTOGRAM: &str = "zinder_compat_tip_wait_duration_seconds";
const WAITERS_GAUGE: &str = "zinder_compat_tip_waiters";

/// How long, and how far past the visible tip, a height-selected block read
/// waits for its height before it answers.
///
/// A read waits only when the height it names is above the visible tip by at
/// most the block window, and only while it has a place among the waiters.
/// It ends its wait when a publication brings the tip to its height, when a
/// publication moves the tip down out of the window, when the wait expires,
/// at shutdown, or when its client leaves. Then the method answers as it
/// would have without the wait, against the tip current at that moment.
///
/// Clones share one set of waiter places.
#[derive(Clone, Debug)]
pub struct TipWait {
    max_wait: Duration,
    max_blocks_ahead: u32,
    waiters: Arc<Semaphore>,
    shutdown: CancellationToken,
}

impl Default for TipWait {
    /// A disabled wait, so a library adapter keeps answering at once unless
    /// its owner opts in.
    fn default() -> Self {
        Self::disabled()
    }
}

impl TipWait {
    /// A wait that never waits: every read answers at once.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            max_wait: Duration::ZERO,
            max_blocks_ahead: DEFAULT_TIP_WAIT_MAX_BLOCKS.get(),
            waiters: Arc::new(Semaphore::new(0)),
            shutdown: CancellationToken::new(),
        }
    }

    /// A wait of up to `max_wait` for a height at most `max_blocks_ahead`
    /// past the visible tip, with at most `max_waiters` reads waiting at
    /// once. A zero `max_wait` disables the wait.
    #[must_use]
    pub fn new(max_wait: Duration, max_blocks_ahead: NonZeroU32, max_waiters: NonZeroU32) -> Self {
        Self {
            max_wait,
            max_blocks_ahead: max_blocks_ahead.get(),
            waiters: Arc::new(Semaphore::new(
                usize::try_from(max_waiters.get()).unwrap_or(Semaphore::MAX_PERMITS),
            )),
            shutdown: CancellationToken::new(),
        }
    }

    /// Ends every wait, at once and for good, when `shutdown` is cancelled,
    /// so waiting reads never hold up a graceful shutdown.
    #[must_use]
    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Returns the longest a read waits; zero when the wait is disabled.
    #[must_use]
    pub const fn max_wait(&self) -> Duration {
        self.max_wait
    }

    /// Returns how far past the visible tip a read may name a height and
    /// still wait.
    #[must_use]
    pub const fn max_blocks_ahead(&self) -> u32 {
        self.max_blocks_ahead
    }

    /// Waits until `reads` publishes a visible tip at or above `requested`,
    /// when `requested` is just past the tip, for the method `method`.
    ///
    /// Returns `Ok` whenever the caller should run its method: the height
    /// arrived, the wait expired or was cut short, or the request never
    /// qualified. Returns `Err` only when capturing the visible tip failed,
    /// with that capture's status, which the method would have returned.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the waiter keeps its place for the whole wait"
    )]
    pub(crate) async fn await_height<Reads: CompactBlockReads>(
        &self,
        reads: &Reads,
        method: &'static str,
        requested: BlockHeight,
    ) -> Result<(), Status> {
        if self.max_wait.is_zero() {
            return Ok(());
        }
        // Subscribe before the tip is read, so a publication between the two
        // still wakes the wait.
        let Some(mut publications) = reads.subscribe_publications() else {
            return Ok(());
        };
        let tip = visible_tip_height(reads).await?;
        let Some(blocks_ahead) = self.blocks_ahead(requested, tip) else {
            return Ok(());
        };
        let Ok(place) = Arc::clone(&self.waiters).try_acquire_owned() else {
            record_waiters_full(method, blocks_ahead);
            return Ok(());
        };
        let waiter = Waiter::start(place, method, blocks_ahead);
        let outcome = self
            .wait_for_height(reads, &mut publications, requested)
            .await;
        waiter.end(*outcome.as_ref().unwrap_or(&WaitOutcome::Refused));
        outcome.map(|_| ())
    }

    async fn wait_for_height<Reads: CompactBlockReads>(
        &self,
        reads: &Reads,
        publications: &mut watch::Receiver<()>,
        requested: BlockHeight,
    ) -> Result<WaitOutcome, Status> {
        let deadline = Instant::now() + self.max_wait;
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Ok(WaitOutcome::Cancelled),
                changed = timeout_at(deadline, publications.changed()) => match changed {
                    Err(_elapsed) => return Ok(WaitOutcome::Expired),
                    // The publisher is gone, so no newer tip can arrive.
                    Ok(Err(_closed)) => return Ok(WaitOutcome::Cancelled),
                    Ok(Ok(())) => {}
                },
            }
            let tip = visible_tip_height(reads).await?;
            if requested <= tip {
                return Ok(WaitOutcome::Served);
            }
            if self.blocks_ahead(requested, tip).is_none() {
                return Ok(WaitOutcome::LeftWindow);
            }
        }
    }

    /// Returns how far `requested` is past `tip` when it is past it by at
    /// most the block window.
    fn blocks_ahead(&self, requested: BlockHeight, tip: BlockHeight) -> Option<u32> {
        let ahead = requested.value().checked_sub(tip.value())?;
        (1..=self.max_blocks_ahead)
            .contains(&ahead)
            .then_some(ahead)
    }
}

/// Reads the visible tip height without holding or retaining a view, so a
/// wait never keeps a serving generation.
async fn visible_tip_height<Reads: CompactBlockReads>(
    reads: &Reads,
) -> Result<BlockHeight, Status> {
    reads.peek_visible_tip_height().await
}

/// How one wait ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WaitOutcome {
    /// The tip reached the requested height.
    Served,
    /// The wait ran its full length without the height arriving.
    Expired,
    /// The client left, the server shut down, or the publisher stopped.
    Cancelled,
    /// A publication moved the tip down until the height left the window.
    LeftWindow,
    /// Capturing the tip failed, and the read fails with that status.
    Refused,
}

impl WaitOutcome {
    const fn label(self) -> &'static str {
        match self {
            Self::Served => "served",
            Self::Expired => "expired",
            Self::Cancelled => "cancelled",
            Self::LeftWindow => "left_window",
            Self::Refused => "refused",
        }
    }
}

fn blocks_ahead_label(blocks_ahead: u32) -> &'static str {
    match blocks_ahead {
        1 => "1",
        2 => "2",
        3 => "3",
        4 => "4",
        _ => "over_4",
    }
}

fn record_waiters_full(method: &'static str, blocks_ahead: u32) {
    metrics::counter!(
        WAITS_COUNTER,
        "method" => method,
        "outcome" => "waiters_full",
        "blocks_ahead" => blocks_ahead_label(blocks_ahead),
    )
    .increment(1);
    metrics::histogram!(WAIT_DURATION_HISTOGRAM, "outcome" => "waiters_full").record(0.0);
}

/// One read's place among the waiters, which records the wait when it ends,
/// including when the client drops the request mid-wait.
struct Waiter {
    _place: OwnedSemaphorePermit,
    method: &'static str,
    blocks_ahead: u32,
    started: Instant,
    outcome: Option<WaitOutcome>,
}

impl Waiter {
    fn start(place: OwnedSemaphorePermit, method: &'static str, blocks_ahead: u32) -> Self {
        metrics::gauge!(WAITERS_GAUGE).increment(1.0);
        Self {
            _place: place,
            method,
            blocks_ahead,
            started: Instant::now(),
            outcome: None,
        }
    }

    /// Ends the wait with `outcome`; a wait dropped without it ended because
    /// its client left.
    fn end(mut self, outcome: WaitOutcome) {
        self.outcome = Some(outcome);
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let outcome = self.outcome.unwrap_or(WaitOutcome::Cancelled).label();
        metrics::gauge!(WAITERS_GAUGE).decrement(1.0);
        metrics::counter!(
            WAITS_COUNTER,
            "method" => self.method,
            "outcome" => outcome,
            "blocks_ahead" => blocks_ahead_label(self.blocks_ahead),
        )
        .increment(1);
        metrics::histogram!(WAIT_DURATION_HISTOGRAM, "outcome" => outcome)
            .record(self.started.elapsed().as_secs_f64());
    }
}
