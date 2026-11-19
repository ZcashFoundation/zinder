//! Bound on concurrent native `CompactBlocksInRange` streams.

use std::{future::Future, num::NonZeroU32, sync::Arc};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

/// Counter of `CompactBlocksInRange` streams refused because every slot was
/// taken.
const STREAM_REFUSALS_COUNTER: &str = "zinder_query_compact_block_range_stream_refusals_total";

/// Default number of `CompactBlocksInRange` streams one adapter serves at
/// once, the same as the lightwalletd block-range default.
pub const DEFAULT_MAX_CONCURRENT_COMPACT_BLOCK_STREAMS: NonZeroU32 =
    NonZeroU32::MIN.saturating_add(99);

/// Process-wide limit on open `CompactBlocksInRange` streams.
///
/// A stream holds one slot from before its first read until it ends or the
/// client drops it and its outstanding read finishes, so each open stream's
/// buffered blocks and blocking reads count against the limit. Clones share
/// the slots.
#[derive(Clone, Debug)]
pub(super) struct CompactBlockStreamLimit {
    slots: Arc<Semaphore>,
    max_streams: NonZeroU32,
}

impl CompactBlockStreamLimit {
    /// Serves at most `max_streams` streams at once.
    pub(super) fn new(max_streams: NonZeroU32) -> Self {
        let permits = usize::try_from(max_streams.get())
            .unwrap_or(Semaphore::MAX_PERMITS)
            .min(Semaphore::MAX_PERMITS);
        Self {
            slots: Arc::new(Semaphore::new(permits)),
            max_streams,
        }
    }

    /// Takes a slot for one stream, held until the returned slot and every
    /// clone of it drop.
    ///
    /// # Errors
    ///
    /// Returns `RESOURCE_EXHAUSTED` when every slot is taken, and counts the
    /// refusal.
    pub(super) fn try_open(&self) -> Result<CompactBlockStreamSlot, Status> {
        let permit = Arc::clone(&self.slots).try_acquire_owned().map_err(|_| {
            metrics::counter!(STREAM_REFUSALS_COUNTER).increment(1);
            Status::resource_exhausted(format!(
                "this server already serves {} CompactBlocksInRange streams; retry after a short delay",
                self.max_streams
            ))
        })?;
        Ok(CompactBlockStreamSlot {
            _permit: Arc::new(permit),
        })
    }
}

/// One open stream's slot, returned to the limit when the last clone drops.
///
/// A cancelled request or a dropped stream does not stop a read already on
/// the blocking pool. The query opening the stream takes a clone with
/// [`CompactBlockStreamSlot::current`] for each such read and for the task
/// that issues the later ones, so the slot stays taken until they end.
#[derive(Clone, Debug)]
pub(crate) struct CompactBlockStreamSlot {
    _permit: Arc<OwnedSemaphorePermit>,
}

tokio::task_local! {
    static CURRENT_SLOT: CompactBlockStreamSlot;
}

impl CompactBlockStreamSlot {
    /// Runs `open` while this slot is its [`current`](Self::current) one,
    /// holding it until `open` ends or is dropped.
    pub(super) async fn hold_during<F: Future>(self, open: F) -> F::Output {
        CURRENT_SLOT.scope(self, open).await
    }

    /// The slot of the stream being opened, if any.
    pub(crate) fn current() -> Option<Self> {
        CURRENT_SLOT.try_with(Self::clone).ok()
    }
}

/// Registers `# HELP` and `# TYPE` text for the stream-limit metrics.
pub(crate) fn describe_metrics() {
    metrics::describe_counter!(
        STREAM_REFUSALS_COUNTER,
        metrics::Unit::Count,
        "Native CompactBlocksInRange streams refused because the adapter already \
         serves its limit of open streams."
    );
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use tonic::Code;

    use super::CompactBlockStreamLimit;

    #[test]
    fn the_stream_limit_refuses_past_its_slots_until_one_is_returned() -> Result<(), tonic::Status>
    {
        let limit = CompactBlockStreamLimit::new(NonZeroU32::MIN);
        let open = limit.try_open()?;
        let shared = open.clone();
        assert_eq!(
            limit.try_open().err().map(|status| status.code()),
            Some(Code::ResourceExhausted)
        );
        drop(open);
        assert!(
            limit.try_open().is_err(),
            "a clone of the slot keeps it taken"
        );
        drop(shared);
        limit.try_open().map(drop)
    }
}
