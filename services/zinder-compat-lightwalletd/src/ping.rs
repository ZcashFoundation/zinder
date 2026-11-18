//! The lightwalletd `Ping` load-testing RPC.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use tonic::Status;
use zinder_proto::compat::lightwalletd;

/// How a compatibility server answers `Ping`.
///
/// Reference lightwalletd refuses `Ping` with `FAILED_PRECONDITION` unless it
/// is started with `--ping-very-insecure`, because each call holds a server
/// task for as long as the client asks. Clones share one in-flight counter.
#[derive(Clone, Debug, Default)]
pub struct LightwalletdPing {
    in_flight: Option<Arc<AtomicI64>>,
}

impl LightwalletdPing {
    /// Answers `Ping` the way `lightwalletd --ping-very-insecure` does.
    #[must_use]
    pub fn enabled() -> Self {
        Self {
            in_flight: Some(Arc::new(AtomicI64::new(0))),
        }
    }

    /// Returns whether this server answers `Ping`.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.in_flight.is_some()
    }

    /// Answers one `Ping` request.
    ///
    /// When enabled, `entry` is the number of `Ping` calls in flight once this
    /// one starts, the call then sleeps `interval_us` microseconds, and `exit`
    /// is the number still in flight once it ends. A negative interval sleeps
    /// for no time.
    ///
    /// # Errors
    ///
    /// Returns `FAILED_PRECONDITION` when `Ping` is not enabled.
    pub async fn respond(
        &self,
        request: lightwalletd::Duration,
    ) -> Result<lightwalletd::PingResponse, Status> {
        let Some(in_flight) = self.in_flight.as_ref() else {
            return Err(Status::failed_precondition(
                "Ping is not enabled; set compat.ping_very_insecure = true to enable it",
            ));
        };
        let call = InFlightPing::start(in_flight);
        let sleep_micros = u64::try_from(request.interval_us).unwrap_or(0);
        tokio::time::sleep(Duration::from_micros(sleep_micros)).await;
        Ok(lightwalletd::PingResponse {
            entry: call.entry,
            exit: call.finish(),
        })
    }
}

/// One counted `Ping` call. Dropping it before [`Self::finish`], as a
/// cancelled request does, still releases its count.
struct InFlightPing<'counter> {
    in_flight: &'counter AtomicI64,
    entry: i64,
    finished: bool,
}

impl<'counter> InFlightPing<'counter> {
    fn start(in_flight: &'counter AtomicI64) -> Self {
        let entry = in_flight.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
        Self {
            in_flight,
            entry,
            finished: false,
        }
    }

    fn finish(mut self) -> i64 {
        self.finished = true;
        self.in_flight
            .fetch_sub(1, Ordering::SeqCst)
            .wrapping_sub(1)
    }
}

impl Drop for InFlightPing<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_ping_is_refused_with_failed_precondition() {
        let outcome = LightwalletdPing::default()
            .respond(lightwalletd::Duration { interval_us: 0 })
            .await;

        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(tonic::Code::FailedPrecondition)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn enabled_ping_counts_concurrent_calls_and_sleeps_the_interval() -> Result<(), Status> {
        let ping = LightwalletdPing::enabled();
        let request = lightwalletd::Duration {
            interval_us: 1_000_000,
        };

        let started = tokio::time::Instant::now();
        let (first, second) = tokio::join!(ping.respond(request), ping.respond(request));
        let (first, second) = (first?, second?);

        assert_eq!((first.entry, second.entry), (1, 2));
        // Both calls wake at the same instant, so either may finish first.
        let mut exits = [first.exit, second.exit];
        exits.sort_unstable();
        assert_eq!(exits, [0, 1]);
        assert!(started.elapsed() >= Duration::from_secs(1));
        Ok(())
    }

    #[tokio::test]
    async fn a_cancelled_ping_releases_its_count() -> Result<(), Status> {
        let ping = LightwalletdPing::enabled();
        let cancelled = ping.respond(lightwalletd::Duration {
            interval_us: i64::MAX,
        });
        let _ = tokio::time::timeout(Duration::from_millis(10), cancelled).await;

        let response = ping
            .respond(lightwalletd::Duration { interval_us: -5 })
            .await?;

        assert_eq!((response.entry, response.exit), (1, 0));
        Ok(())
    }
}
