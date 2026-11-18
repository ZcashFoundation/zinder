//! Bound on concurrent native `CompactBlocksInRange` streams.

use std::{num::NonZeroU32, sync::Arc};

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
/// client drops it, so each open stream's buffered blocks count against the
/// limit. Clones share the slots.
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

    /// Takes a slot for one stream, held until the returned permit drops.
    ///
    /// # Errors
    ///
    /// Returns `RESOURCE_EXHAUSTED` when every slot is taken, and counts the
    /// refusal.
    pub(super) fn try_open(&self) -> Result<OwnedSemaphorePermit, Status> {
        Arc::clone(&self.slots).try_acquire_owned().map_err(|_| {
            metrics::counter!(STREAM_REFUSALS_COUNTER).increment(1);
            Status::resource_exhausted(format!(
                "this server already serves {} CompactBlocksInRange streams; retry after a short delay",
                self.max_streams
            ))
        })
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
        assert_eq!(
            limit.try_open().err().map(|status| status.code()),
            Some(Code::ResourceExhausted)
        );
        drop(open);
        limit.try_open().map(drop)
    }
}
