//! Execution of release wallet reads against one captured serving pair.
//!
//! Every `WalletServingQuery` read runs through this module, so a read that
//! finds the reader of its pair invalidated runs once more on the pair that
//! replaces it, and each request records one `zinder_query_request_*` sample
//! through [`record`]. The readers themselves keep storage work off the async
//! runtime.
//!
//! The protocol adapter records the operations it owns, such as the
//! `chain_events` stream. Reads made on behalf of such a request use
//! [`read_at`] or [`read_pair`] alone so the request is never counted twice.

use std::{future::Future, sync::Arc, time::Instant};

use zinder_core::ChainEpochId;

use crate::{
    QueryError, WalletServingPairSlot, WalletServingReadPair, record_wallet_query_outcome,
};

/// The pair one read captures: the one `slot` serves at a pinned epoch, or
/// its current pair when the read is unpinned.
#[derive(Clone, Copy)]
pub(crate) struct SlotPin<'slot> {
    slot: &'slot WalletServingPairSlot,
    mode: Replacement,
}

impl<'slot> SlotPin<'slot> {
    pub(crate) const fn new(
        slot: &'slot WalletServingPairSlot,
        chain_epoch_id: Option<ChainEpochId>,
    ) -> Self {
        Self {
            slot,
            mode: Replacement::Pinned(chain_epoch_id),
        }
    }

    /// Captures ephemerally when unpinned (`None`), or pins to `Some(id)` when
    /// a multi-step caller bound the read to a specific epoch.
    pub(crate) const fn ephemeral_unless_pinned(
        slot: &'slot WalletServingPairSlot,
        chain_epoch_id: Option<ChainEpochId>,
    ) -> Self {
        let mode = match chain_epoch_id {
            Some(id) => Replacement::Pinned(Some(id)),
            None => Replacement::Ephemeral,
        };
        Self { slot, mode }
    }
}

/// Reads the pair `pin` selects as [`read_at`] does and records the request
/// under `operation`.
pub(crate) async fn run<Output, Read, Reading>(
    operation: &'static str,
    pin: SlotPin<'_>,
    read: Read,
) -> Result<Output, QueryError>
where
    Read: FnOnce(Arc<WalletServingReadPair>) -> Reading + Clone,
    Reading: Future<Output = Result<Output, QueryError>>,
{
    record(operation, None, read_at(pin, read)).await
}

/// Times `request`, including validation and any work after its storage
/// read, and records its typed outcome under `operation`.
///
/// Compact-block and nullifier range reads also sample the requested block count
/// by passing it as `requested_blocks`.
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
pub(crate) async fn read_at<Output, Read, Reading>(
    pin: SlotPin<'_>,
    read: Read,
) -> Result<Output, QueryError>
where
    Read: FnOnce(Arc<WalletServingReadPair>) -> Reading + Clone,
    Reading: Future<Output = Result<Output, QueryError>>,
{
    let pair = pin.mode.capture(pin.slot);
    read_replaceable_pair(pin.slot, pair, pin.mode, read).await
}

/// How a read selects the pair that replaces one whose reader was
/// invalidated.
///
/// It matches how the read selected its own pair, so recovery never changes
/// whether the slot retains the replacement for pins.
#[derive(Clone, Copy)]
enum Replacement {
    /// The pair that answers the read's pin, observing the current pair.
    Pinned(Option<ChainEpochId>),
    /// The current pair, without observing it.
    Ephemeral,
}

impl Replacement {
    const fn chain_epoch_id(self) -> Option<ChainEpochId> {
        match self {
            Self::Pinned(chain_epoch_id) => chain_epoch_id,
            Self::Ephemeral => None,
        }
    }

    fn capture(self, slot: &WalletServingPairSlot) -> Arc<WalletServingReadPair> {
        match self {
            Self::Pinned(chain_epoch_id) => slot.capture_at(chain_epoch_id),
            Self::Ephemeral => slot.capture_ephemeral(),
        }
    }
}

/// Runs one read of `pair`, recovering once from an invalidated reader.
///
/// A `RocksDB` secondary does not keep the writer's table files alive, so a
/// read can find a table file of `pair` deleted, which its reader reports as
/// invalidated. The pair is then retired and `read` runs once more on the
/// pair the publisher publishes in its place. That retry happens only when
/// the replacement answers `chain_epoch_id`: otherwise the read fails with
/// [`QueryError::ChainEpochPinUnavailable`], and with its original storage
/// error when no replacement is published in time. A multi-step read passes
/// its own epoch so it never continues at another one.
pub(crate) async fn read_pair<Output, Read, Reading>(
    slot: &WalletServingPairSlot,
    pair: Arc<WalletServingReadPair>,
    chain_epoch_id: Option<ChainEpochId>,
    read: Read,
) -> Result<Output, QueryError>
where
    Read: FnOnce(Arc<WalletServingReadPair>) -> Reading + Clone,
    Reading: Future<Output = Result<Output, QueryError>>,
{
    read_replaceable_pair(slot, pair, Replacement::Pinned(chain_epoch_id), read).await
}

/// Runs one read of `pair`, which the caller captured with
/// [`WalletServingPairSlot::capture_ephemeral`], as [`read_pair`] does.
///
/// A replacement after an invalidated reader is captured ephemerally too, and
/// `read` must answer from whichever pair it receives.
pub(crate) async fn read_ephemeral_pair<Output, Read, Reading>(
    slot: &WalletServingPairSlot,
    pair: Arc<WalletServingReadPair>,
    read: Read,
) -> Result<Output, QueryError>
where
    Read: FnOnce(Arc<WalletServingReadPair>) -> Reading + Clone,
    Reading: Future<Output = Result<Output, QueryError>>,
{
    read_replaceable_pair(slot, pair, Replacement::Ephemeral, read).await
}

async fn read_replaceable_pair<Output, Read, Reading>(
    slot: &WalletServingPairSlot,
    pair: Arc<WalletServingReadPair>,
    mode: Replacement,
    read: Read,
) -> Result<Output, QueryError>
where
    Read: FnOnce(Arc<WalletServingReadPair>) -> Reading + Clone,
    Reading: Future<Output = Result<Output, QueryError>>,
{
    let chain_epoch_id = mode.chain_epoch_id();
    let retry = read.clone();
    let error = match read(Arc::clone(&pair)).await {
        Err(error) if error.invalidates_reader() => error,
        outcome => return outcome,
    };
    let Some(replacement) = slot
        .replace_missing_table_file_pair(pair, &error, || mode.capture(slot))
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
    let outcome = retry(Arc::clone(&replacement)).await;
    if let Err(error) = &outcome
        && error.invalidates_reader()
    {
        slot.report_missing_table_file(&replacement, error);
    }
    outcome
}
