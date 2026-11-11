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

use std::{future::Future, sync::Arc, time::Instant};

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
