//! Execution of release wallet reads against one captured serving pair.
//!
//! Every `WalletServingQuery` read runs through this module, so storage work
//! never occupies the async runtime, a join failure surfaces as
//! [`QueryError::BlockingTaskFailed`], a read that finds a table file of its
//! pair deleted runs once more on the pair that replaces it, and each request
//! records one `zinder_query_request_*` sample through [`record`].
//!
//! The protocol adapter records the operations it owns, such as the
//! `chain_events` stream. Reads made on behalf of such a request use
//! [`read_at`] or [`read_pair`] alone so the request is never counted twice.

use std::{future::Future, sync::Arc, time::Instant};

use zinder_core::ChainEpochId;

use crate::{
    QueryError, WalletServingPairSlot, WalletServingReadPair, join_blocking,
    record_wallet_query_outcome,
};

/// The pair one read captures: the one `slot` serves at a pinned epoch, or
/// its current pair when the read is unpinned.
#[derive(Clone, Copy)]
pub(crate) struct SlotPin<'slot> {
    slot: &'slot WalletServingPairSlot,
    chain_epoch_id: Option<ChainEpochId>,
}

impl<'slot> SlotPin<'slot> {
    pub(crate) const fn new(
        slot: &'slot WalletServingPairSlot,
        chain_epoch_id: Option<ChainEpochId>,
    ) -> Self {
        Self {
            slot,
            chain_epoch_id,
        }
    }
}

/// Reads the pair `pin` selects as [`read_at`] does and records the request
/// under `operation`.
pub(crate) async fn run<Output, Read>(
    operation: &'static str,
    pin: SlotPin<'_>,
    read: Read,
) -> Result<Output, QueryError>
where
    Output: Send + 'static,
    Read: FnOnce(Arc<WalletServingReadPair>) -> Result<Output, QueryError> + Clone + Send + 'static,
{
    record(operation, None, read_at(pin, read)).await
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

/// Captures the pair `pin` selects and reads it as [`read_pair`] does.
///
/// Only a read inside a [`record`]ed request, or a read made on behalf of an
/// adapter-recorded request, may call this alone. Any other caller would
/// leave its request out of the `zinder_query_request_*` series.
pub(crate) async fn read_at<Output, Read>(
    pin: SlotPin<'_>,
    read: Read,
) -> Result<Output, QueryError>
where
    Output: Send + 'static,
    Read: FnOnce(Arc<WalletServingReadPair>) -> Result<Output, QueryError> + Clone + Send + 'static,
{
    let pair = pin.slot.capture_at(pin.chain_epoch_id);
    read_pair(pin.slot, pair, pin.chain_epoch_id, read).await
}

/// Runs one blocking read of `pair` on the blocking pool inside the caller's
/// tracing span, recovering once from a missing table file.
///
/// A secondary does not keep the writer's table files alive. When `read`
/// fails because a table file of `pair` was deleted, the pair is retired and
/// `read` runs once more on the pair the publisher publishes in its place.
/// That retry happens only when the replacement answers `chain_epoch_id`:
/// otherwise the read fails with [`QueryError::ChainEpochPinUnavailable`],
/// and with its original storage error when no replacement is published in
/// time. A multi-step read passes its own epoch so it never continues at
/// another one.
pub(crate) async fn read_pair<Output, Read>(
    slot: &WalletServingPairSlot,
    pair: Arc<WalletServingReadPair>,
    chain_epoch_id: Option<ChainEpochId>,
    read: Read,
) -> Result<Output, QueryError>
where
    Output: Send + 'static,
    Read: FnOnce(Arc<WalletServingReadPair>) -> Result<Output, QueryError> + Clone + Send + 'static,
{
    let retry = read.clone();
    let error = match off_reactor(Arc::clone(&pair), read).await {
        Err(error) if error.is_missing_table_file() => error,
        outcome => return outcome,
    };
    let Some(replacement) = slot
        .replace_missing_table_file_pair(pair, &error, chain_epoch_id)
        .await
    else {
        return Err(error);
    };
    if let Some(pinned) =
        chain_epoch_id.filter(|pinned| replacement.canonical_fence().chain_epoch_id() != *pinned)
    {
        return Err(QueryError::ChainEpochPinUnavailable {
            chain_epoch_id: pinned,
        });
    }
    let outcome = off_reactor(Arc::clone(&replacement), retry).await;
    if let Err(error) = &outcome
        && error.is_missing_table_file()
    {
        slot.report_missing_table_file(&replacement, error);
    }
    outcome
}

async fn off_reactor<Output, Read>(
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
