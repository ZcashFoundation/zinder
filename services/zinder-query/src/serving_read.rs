//! Execution of release wallet reads against one captured serving pair.
//!
//! Every `WalletServingQuery` read runs through this module, so storage work
//! never occupies the async runtime, a join failure surfaces as
//! [`QueryError::BlockingTaskFailed`], and each request records one
//! `zinder_query_request_*` sample through [`record`].
//!
//! The protocol adapter records the operations it owns, such as the
//! `chain_events` stream. Reads made on behalf of such a request use
//! [`off_reactor`] alone so the request is never counted twice.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use crate::{QueryError, WalletServingReadPair, join_blocking, record_wallet_query_outcome};

/// Runs one blocking read of `pair` off the async runtime and records the
/// request under `operation`.
pub(crate) async fn run<Output, Read>(
    operation: &'static str,
    pair: Arc<WalletServingReadPair>,
    read: Read,
) -> Result<Output, QueryError>
where
    Output: Send + 'static,
    Read: FnOnce(Arc<WalletServingReadPair>) -> Result<Output, QueryError> + Send + 'static,
{
    record(operation, None, off_reactor(pair, read)).await
}

/// Runs one blocking read of `pair` that stops early once its request is
/// dropped, and records the request under `operation`.
///
/// Dropping a request cannot abort its blocking task, so `read` receives a
/// [`RequestDropped`] signal to check between bounded steps. An abandoned
/// scan then releases its pair instead of pinning that secondary generation
/// until the scan ends.
pub(crate) async fn run_until_dropped<Output, Read>(
    operation: &'static str,
    pair: Arc<WalletServingReadPair>,
    read: Read,
) -> Result<Output, QueryError>
where
    Output: Send + 'static,
    Read: FnOnce(Arc<WalletServingReadPair>, &RequestDropped) -> Result<Output, QueryError>
        + Send
        + 'static,
{
    record(operation, None, off_reactor_until_dropped(pair, read)).await
}

/// Set once the request that started a blocking read has been dropped.
#[derive(Clone, Debug, Default)]
pub(crate) struct RequestDropped(Arc<AtomicBool>);

impl RequestDropped {
    /// Fails once the request is gone, so a scan can stop between steps.
    pub(crate) fn check(&self) -> Result<(), QueryError> {
        if self.0.load(Ordering::Acquire) {
            return Err(QueryError::BlockingTaskFailed {
                reason: "the request was dropped before its read finished".to_owned(),
            });
        }
        Ok(())
    }
}

/// Raises [`RequestDropped`] when the awaiting request future goes away.
struct RaiseOnDrop(Arc<AtomicBool>);

impl Drop for RaiseOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

async fn off_reactor_until_dropped<Input, Output, Read>(
    input: Input,
    read: Read,
) -> Result<Output, QueryError>
where
    Input: Send + 'static,
    Output: Send + 'static,
    Read: FnOnce(Input, &RequestDropped) -> Result<Output, QueryError> + Send + 'static,
{
    let request_dropped = RequestDropped::default();
    let _raise_on_drop = RaiseOnDrop(Arc::clone(&request_dropped.0));
    let span = tracing::Span::current();
    join_blocking(tokio::task::spawn_blocking(move || {
        span.in_scope(|| read(input, &request_dropped))
    }))
    .await
}

/// Times `request`, including validation and any work after its blocking
/// read, and records its typed outcome under `operation`.
///
/// `requested_blocks` is `Some` only for `compact_blocks_in_range`, which
/// also samples how many blocks the request asked for.
pub(crate) async fn record<Output>(
    operation: &'static str,
    requested_blocks: Option<usize>,
    request: impl Future<Output = Result<Output, QueryError>>,
) -> Result<Output, QueryError> {
    let started_at = Instant::now();
    let request_outcome = request.await;
    record_wallet_query_outcome(operation, started_at, &request_outcome, requested_blocks);
    request_outcome
}

/// Runs one blocking read of `pair` on the blocking pool inside the caller's
/// tracing span, without recording a request sample.
///
/// Only a read inside a [`record`]ed request, or a read made on behalf of an
/// adapter-recorded request, may call this alone. Any other caller would
/// leave its request out of the `zinder_query_request_*` series.
pub(crate) async fn off_reactor<Output, Read>(
    pair: Arc<WalletServingReadPair>,
    read: Read,
) -> Result<Output, QueryError>
where
    Output: Send + 'static,
    Read: FnOnce(Arc<WalletServingReadPair>) -> Result<Output, QueryError> + Send + 'static,
{
    let span = tracing::Span::current();
    join_blocking(tokio::task::spawn_blocking(move || {
        span.in_scope(|| read(pair))
    }))
    .await
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::off_reactor_until_dropped;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_request_stops_its_blocking_scan_and_releases_its_input()
    -> Result<(), Box<dyn std::error::Error>> {
        let pinned_input = Arc::new(());
        let (stopped_sender, stopped_receiver) = std::sync::mpsc::channel();
        let scan = off_reactor_until_dropped(Arc::clone(&pinned_input), move |_input, dropped| {
            while dropped.check().is_ok() {
                std::thread::sleep(Duration::from_millis(1));
            }
            let _ = stopped_sender.send(());
            dropped.check()
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(50), scan)
                .await
                .is_err(),
            "the scan runs until its request is dropped"
        );

        stopped_receiver.recv_timeout(Duration::from_secs(5))?;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while Arc::strong_count(&pinned_input) > 1 {
            if std::time::Instant::now() > deadline {
                return Err("the stopped scan kept its input alive".into());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    }
}
