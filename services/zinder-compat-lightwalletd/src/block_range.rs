//! Chunk planning, chain continuity, and the concurrency limit of lightwalletd
//! block-range streams.
//!
//! A `GetBlockRange` is served as a sequence of chunk reads. A chunk holds at
//! most 1,000 blocks and at most [`BLOCK_RANGE_CHUNK_MAX_BYTES`] of stored
//! compact blocks, or one block when that block alone is larger, so a range
//! of large blocks is read and sent in smaller chunks. The first chunk holds
//! at most [`BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES`], so a response opens after a
//! small read. [`BlockRangeChunks`] offers each read up to 1,000 heights from
//! the next height to send, so every read can fill its byte budget. When a
//! read finds a height outside retained history while heights an earlier
//! read checked are unsent, the stream reads those heights alone, so it sends
//! every height a read checked before that failure ends it.
//!
//! Each chunk after the first is read at the chain view current when the
//! server reads it, so a stream never holds a serving pair or a
//! compact-blocks generation while it waits for its client. The stream's
//! reader reads the next chunk while the response sends the one before it,
//! and the stream holds at most those two chunks. A later chunk continues
//! the stream only when it links by hash to the last block the stream sent.
//! A block hash commits to its parent, so that one link proves the blocks
//! sent and the blocks to come form one chain. Otherwise a reorg replaced a
//! block the stream needs, and the stream ends after the blocks it already
//! sent with the code lightwalletd returns for the same case: `ABORTED` for a
//! broken link, and `OUT_OF_RANGE` when the visible tip fell below a height
//! still to send. ADR-0015, "Compatibility block-range streams", records the
//! contract.
//!
//! A stream holds one slot of a process-wide [`BlockRangeStreamLimit`] from
//! its admission until the stream ends, fails, or its response is dropped, and
//! every chunk read of the stream has finished, so the number of slots bounds
//! how many streams hold chunks at once. A request that finds every slot taken
//! waits in a bounded queue for a bounded time, then fails with `UNAVAILABLE`,
//! as lightwalletd does.

use std::{
    num::NonZeroU32,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::Instrument as _;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, ChainEpoch, ChainEpochId, CompactBlockArtifact,
};
use zinder_query::DEFAULT_MAX_COMPACT_BLOCK_RANGE;
use zinder_store::HeightOrder;

/// Stored compact-block bytes one block-range chunk keeps at most.
///
/// A chunk keeps one block even when that block alone is larger. The stored
/// encoding is the whole compact block before pool filtering, which is what a
/// chunk read materializes, so `GetBlockRangeNullifiers` reads the same chunks
/// as `GetBlockRange`.
pub const BLOCK_RANGE_CHUNK_MAX_BYTES: usize = 2 * 1024 * 1024;

/// Stored compact-block bytes the first chunk of a block-range stream keeps
/// at most, counted as [`BLOCK_RANGE_CHUNK_MAX_BYTES`] counts them.
///
/// The response opens once this chunk is read and converted, so the read
/// before a stream's first block keeps at most an eighth of what a later
/// chunk keeps, or one block when that block alone is larger. A thousand
/// blocks of up to 262 stored bytes each fit, so a request for that many
/// small blocks is served by one read. A denser range opens after its first
/// few blocks, and the server reads on while it sends them.
pub const BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES: usize = 256 * 1024;

/// Default for the process-wide limit on concurrent `GetBlockRange` and
/// `GetBlockRangeNullifiers` streams.
pub const DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS: NonZeroU32 =
    NonZeroU32::MIN.saturating_add(99);

/// Default for how long a block-range request waits for a stream slot before
/// it fails with `UNAVAILABLE`, as in lightwalletd.
pub const DEFAULT_BLOCK_RANGE_ADMISSION_WAIT: Duration = Duration::from_secs(10);

/// Number of requests that may wait for a block-range stream slot for each
/// configured stream slot.
const BLOCK_RANGE_WAITERS_PER_SLOT: usize = 3;

/// Plans the chunk reads of one block-range stream in wire order.
///
/// [`Self::next_chunk`] offers up to 1,000 heights from the end of the
/// remaining range that the stream sends first. A byte-bounded read keeps a
/// run of them from that end, and [`Self::advance`] moves past the blocks it
/// kept, so the next chunk starts at the next height to send and every read
/// can fill its byte budget.
///
/// A read checks every height it is offered against retained history before
/// it keeps any, so a read offered heights past those the last read checked
/// and did not keep can fail before it sends them. [`Self::checked_heights`]
/// then offers the checked heights alone until they are sent, so the stream
/// sends every height a read has checked before a read past them fails, such
/// as at height 0 at the end of a descending range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlockRangeChunks {
    /// Heights no chunk has covered yet, or `None` once every height has.
    remaining: Option<BlockHeightRange>,
    /// Heights at the sending end of `remaining` that the last read checked
    /// against retained history and did not keep.
    checked_unkept: u32,
    /// Whether the next read is offered the `checked_unkept` heights alone,
    /// which holds only while some remain.
    only_checked: bool,
    is_descending: bool,
}

impl BlockRangeChunks {
    /// Plans the chunks of `range` in the order the stream sends them.
    #[must_use]
    pub(crate) const fn new(range: BlockHeightRange, is_descending: bool) -> Self {
        let remaining = if range.start.value() <= range.end.value() {
            Some(range)
        } else {
            None
        };
        Self {
            remaining,
            checked_unkept: 0,
            only_checked: false,
            is_descending,
        }
    }

    /// Returns whether a chunk remains to be read.
    #[must_use]
    pub(crate) const fn has_remaining(&self) -> bool {
        self.remaining.is_some()
    }

    /// Returns the height order of the stream, which every chunk read walks.
    #[must_use]
    pub(crate) const fn order(&self) -> HeightOrder {
        if self.is_descending {
            HeightOrder::Descending
        } else {
            HeightOrder::Ascending
        }
    }

    /// Returns the heights the next chunk read may cover, at the end of the
    /// remaining range that the stream sends first: up to 1,000, or the
    /// heights [`Self::checked_heights`] offers alone.
    #[must_use]
    pub(crate) fn next_chunk(&self) -> Option<BlockHeightRange> {
        let remaining = self.remaining?;
        let heights = if self.only_checked {
            self.checked_unkept
        } else {
            DEFAULT_MAX_COMPACT_BLOCK_RANGE.get()
        };
        let span = heights.saturating_sub(1);
        Some(if self.is_descending {
            let start = remaining
                .end
                .value()
                .saturating_sub(span)
                .max(remaining.start.value());
            BlockHeightRange::inclusive(BlockHeight::new(start), remaining.end)
        } else {
            let end = remaining
                .start
                .value()
                .saturating_add(span)
                .min(remaining.end.value());
            BlockHeightRange::inclusive(remaining.start, BlockHeight::new(end))
        })
    }

    /// Returns the heights the last read checked and did not keep, and the
    /// plan that offers them alone until the stream has sent them, when
    /// [`Self::next_chunk`] offers heights past them too.
    ///
    /// A stream reads these after the read of [`Self::next_chunk`] finds a
    /// height outside retained history, so it sends the heights an earlier
    /// read checked before that failure ends it. Returns `None` when no such
    /// heights remain or the next chunk is already them alone, and the
    /// failure stands.
    #[must_use]
    pub(crate) fn checked_heights(&self) -> Option<(BlockHeightRange, Self)> {
        let offered = self.next_chunk()?;
        if self.checked_unkept == 0 || offered_heights(offered) <= self.checked_unkept {
            return None;
        }
        let only_checked = Self {
            only_checked: true,
            ..*self
        };
        Some((only_checked.next_chunk()?, only_checked))
    }

    /// Moves past `read_blocks` heights at the end of the remaining range
    /// that the stream sends first, the blocks a read of
    /// [`Self::next_chunk`] kept.
    ///
    /// A read that kept no block leaves the plan unchanged. Its caller must
    /// end the stream, as the continuity check of an empty chunk does.
    pub(crate) fn advance(&mut self, read_blocks: usize) {
        let (Some(remaining), Some(offered)) = (self.remaining, self.next_chunk()) else {
            return;
        };
        let span = remaining.end.value() - remaining.start.value();
        match u32::try_from(read_blocks) {
            Ok(0) => {}
            Ok(read_blocks) if read_blocks <= span => {
                self.checked_unkept = offered_heights(offered).saturating_sub(read_blocks);
                self.only_checked &= self.checked_unkept > 0;
                self.remaining = Some(if self.is_descending {
                    BlockHeightRange::inclusive(
                        remaining.start,
                        BlockHeight::new(remaining.end.value() - read_blocks),
                    )
                } else {
                    BlockHeightRange::inclusive(
                        BlockHeight::new(remaining.start.value() + read_blocks),
                        remaining.end,
                    )
                });
            }
            _ => self.remaining = None,
        }
    }
}

/// Returns how many heights `chunk`, a chunk the plan offered, covers.
const fn offered_heights(chunk: BlockHeightRange) -> u32 {
    chunk
        .end
        .value()
        .saturating_sub(chunk.start.value())
        .saturating_add(1)
}

/// Gauge of the block-range streams that hold a slot.
const OPEN_STREAMS_GAUGE: &str = "zinder_compat_block_range_streams_open";
/// Counter of the block-range streams that took a slot.
const ADMITTED_STREAMS_COUNTER: &str = "zinder_compat_block_range_streams_admitted_total";
/// Counter of the block-range requests refused a slot, labelled by
/// `reason`.
const REFUSED_STREAMS_COUNTER: &str = "zinder_compat_block_range_stream_refusals_total";
/// Histogram of the time a block-range request waited for a slot, labelled
/// by `outcome`.
const SLOT_WAIT_HISTOGRAM: &str = "zinder_compat_block_range_slot_wait_duration_seconds";
/// Histogram of the time one chunk of a block-range stream spent in one
/// stage, labelled by `stage`.
const STAGE_DURATION_HISTOGRAM: &str = "zinder_compat_block_range_stage_duration_seconds";

/// A stage of serving one chunk of a block-range stream, timed in
/// `zinder_compat_block_range_stage_duration_seconds{stage}`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum BlockRangeStage {
    /// Reading the chunk's compact blocks from storage.
    Read,
    /// Converting the chunk's blocks to lightwalletd messages and pruning
    /// them to the requested pools.
    Convert,
    /// Waiting for room for the chunk in the response before it is read. A
    /// stream holds at most two chunks, so a chunk waits until the gRPC
    /// server has taken every block of the chunk two before it to send. Only
    /// the chunks after the first have this stage, because the first chunk
    /// is part of the response itself.
    Send,
}

impl BlockRangeStage {
    const fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Convert => "convert",
            Self::Send => "send",
        }
    }

    /// Records that one chunk spent `elapsed` in this stage.
    pub(crate) fn record(self, elapsed: Duration) {
        metrics::histogram!(STAGE_DURATION_HISTOGRAM, "stage" => self.label()).record(elapsed);
    }

    /// Awaits `stage_work` and records how long it took.
    pub(crate) async fn time_async<Output>(
        self,
        stage_work: impl Future<Output = Output>,
    ) -> Output {
        let started_at = Instant::now();
        let output = stage_work.await;
        self.record(started_at.elapsed());
        output
    }
}

/// Process-wide limit on concurrent `GetBlockRange` and
/// `GetBlockRangeNullifiers` streams.
///
/// Clones share one limit. A stream takes a slot after its request passes
/// validation. It returns the slot when the stream ends, fails, or the client
/// goes away, and no chunk read of the stream is still running.
///
/// A request that finds every slot taken waits for one, first come first
/// served, for at most [`Self::max_wait`]. At most three times as many
/// requests wait as the limit has slots, so a request that finds the queue
/// full fails at once.
#[derive(Clone, Debug)]
pub(crate) struct BlockRangeStreamLimit {
    encoded_range_reads: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    waiters: Arc<AtomicUsize>,
    max_streams: NonZeroU32,
    max_wait: Duration,
}

const ENCODED_RANGE_READERS: usize = 16;

impl BlockRangeStreamLimit {
    pub(crate) fn encoded_range_chunk_budget(&self, offered: usize) -> usize {
        let capacity = (self.max_streams.get() as usize).min(Semaphore::MAX_PERMITS);
        let active = capacity.saturating_sub(self.slots.available_permits());
        offered.min(
            (ENCODED_RANGE_READERS * BLOCK_RANGE_CHUNK_MAX_BYTES)
                / active.max(ENCODED_RANGE_READERS),
        )
    }

    pub(crate) fn try_encoded_range_read_permit(&self) -> Option<OwnedSemaphorePermit> {
        self.encoded_range_reads.clone().try_acquire_owned().ok()
    }

    pub(crate) async fn encoded_range_read_permit(&self) -> Result<OwnedSemaphorePermit, Status> {
        self.encoded_range_reads
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("encoded range reader closed"))
    }

    /// Creates a limit that admits at most `max_streams` streams at once and
    /// lets a request wait [`DEFAULT_BLOCK_RANGE_ADMISSION_WAIT`] for a slot.
    #[must_use]
    pub(crate) fn new(max_streams: NonZeroU32) -> Self {
        let permits = usize::try_from(max_streams.get())
            .unwrap_or(Semaphore::MAX_PERMITS)
            .min(Semaphore::MAX_PERMITS);
        // Publishes the gauge before the first stream opens. Setting it to
        // zero would discard the slots that other limits in the process hold.
        metrics::gauge!(OPEN_STREAMS_GAUGE).increment(0.0);
        register_refusal_counter();
        Self {
            encoded_range_reads: Arc::new(Semaphore::new(ENCODED_RANGE_READERS)),
            slots: Arc::new(Semaphore::new(permits)),
            waiters: Arc::new(AtomicUsize::new(0)),
            max_streams,
            max_wait: DEFAULT_BLOCK_RANGE_ADMISSION_WAIT,
        }
    }

    /// Sets how long a request waits for a slot before it fails.
    #[must_use]
    pub(crate) const fn with_max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self
    }

    /// Returns the most streams this limit admits at once.
    #[must_use]
    pub(crate) const fn max_streams(&self) -> NonZeroU32 {
        self.max_streams
    }

    /// Returns how long a request waits for a slot before it fails.
    #[must_use]
    pub(crate) const fn max_wait(&self) -> Duration {
        self.max_wait
    }

    /// Takes a slot for one new stream, waiting for one when every slot is
    /// taken, and counts it in
    /// `zinder_compat_block_range_streams_admitted_total`.
    ///
    /// Dropping the returned future while it waits, as tonic does when the
    /// client cancels or disconnects, gives up the request's place in the
    /// queue.
    ///
    /// # Errors
    ///
    /// Returns `UNAVAILABLE` when no slot returns within
    /// [`Self::max_wait`], or at once when three times as many requests
    /// already wait as the limit has slots. Each refusal is counted in
    /// `zinder_compat_block_range_stream_refusals_total{reason}`.
    pub(crate) async fn open(&self) -> Result<BlockRangeStreamSlot, Status> {
        let started_at = Instant::now();
        // The semaphore hands a returned slot to the oldest waiter, so a new
        // request takes a free slot here only when nobody waits.
        if let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() {
            record_admission(AdmissionOutcome::Admitted, started_at.elapsed());
            return Ok(BlockRangeStreamSlot::new(permit));
        }
        let Some(mut waiter) = QueuedRequest::enter(&self.waiters, self.max_waiters(), started_at)
        else {
            record_admission(
                AdmissionOutcome::Refused(RefusalReason::WaitersFull),
                started_at.elapsed(),
            );
            return Err(self.refusal());
        };
        let acquired =
            tokio::time::timeout(self.max_wait, Arc::clone(&self.slots).acquire_owned()).await;
        waiter.resolved = true;
        // The semaphore is never closed, so only the wait can end it.
        if let Ok(Ok(permit)) = acquired {
            record_admission(AdmissionOutcome::Admitted, started_at.elapsed());
            return Ok(BlockRangeStreamSlot::new(permit));
        }
        record_admission(
            AdmissionOutcome::Refused(RefusalReason::WaitTimeout),
            started_at.elapsed(),
        );
        Err(self.refusal())
    }

    /// Requests that may wait for a slot at once: three for each slot.
    fn max_waiters(&self) -> usize {
        usize::try_from(self.max_streams.get())
            .unwrap_or(usize::MAX)
            .saturating_mul(BLOCK_RANGE_WAITERS_PER_SLOT)
    }

    /// `UNAVAILABLE`, which wallets retry, rather than `RESOURCE_EXHAUSTED`,
    /// as lightwalletd returns for its own block-range limit.
    fn refusal(&self) -> Status {
        Status::unavailable(format!(
            "this server already serves its limit of {} concurrent block-range streams; retry \
             shortly",
            self.max_streams
        ))
    }
}

/// A request's place in the queue for a slot, given back when it is dropped.
///
/// A request dropped before it was resolved, because its client went away,
/// records its wait with `outcome="cancelled"`.
struct QueuedRequest {
    waiters: Arc<AtomicUsize>,
    started_at: Instant,
    resolved: bool,
}

impl QueuedRequest {
    /// Takes a place in the queue, or returns `None` when `max_waiters`
    /// requests already wait.
    fn enter(waiters: &Arc<AtomicUsize>, max_waiters: usize, started_at: Instant) -> Option<Self> {
        waiters
            .try_update(Ordering::AcqRel, Ordering::Acquire, |waiting| {
                (waiting < max_waiters).then(|| waiting + 1)
            })
            .ok()?;
        Some(Self {
            waiters: Arc::clone(waiters),
            started_at,
            resolved: false,
        })
    }
}

impl Drop for QueuedRequest {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::AcqRel);
        if !self.resolved {
            record_admission(AdmissionOutcome::Cancelled, self.started_at.elapsed());
        }
    }
}

/// How a block-range request left admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionOutcome {
    Admitted,
    Refused(RefusalReason),
    /// The client went away while the request waited.
    Cancelled,
}

/// Why a block-range request was refused a slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RefusalReason {
    /// Three times as many requests already waited as the limit has slots.
    WaitersFull,
    /// No slot returned within the limit's wait.
    WaitTimeout,
}

impl RefusalReason {
    const ALL: [Self; 2] = [Self::WaitTimeout, Self::WaitersFull];

    const fn label(self) -> &'static str {
        match self {
            Self::WaitersFull => "waiters_full",
            Self::WaitTimeout => "wait_timeout",
        }
    }
}

/// Describes and pre-registers `zinder_compat_block_range_stream_refusals_total`
/// at zero for every finite refusal reason.
fn register_refusal_counter() {
    metrics::describe_counter!(
        REFUSED_STREAMS_COUNTER,
        metrics::Unit::Count,
        "Block-range stream requests refused with UNAVAILABLE, by reason: `wait_timeout` \
         when no slot returned within the admission wait, and `waiters_full` when every \
         waiting place was taken."
    );
    for reason in RefusalReason::ALL {
        metrics::counter!(REFUSED_STREAMS_COUNTER, "reason" => reason.label()).increment(0);
    }
}

/// Counts one admission decision and records how long the request waited
/// for it.
fn record_admission(outcome: AdmissionOutcome, waited: Duration) {
    let outcome_label = match outcome {
        AdmissionOutcome::Admitted => {
            metrics::counter!(ADMITTED_STREAMS_COUNTER).increment(1);
            "admitted"
        }
        AdmissionOutcome::Refused(reason) => {
            metrics::counter!(REFUSED_STREAMS_COUNTER, "reason" => reason.label()).increment(1);
            if reason == RefusalReason::WaitTimeout {
                maybe_warn_cgroup_memory_pressure();
            }
            "refused"
        }
        AdmissionOutcome::Cancelled => "cancelled",
    };
    metrics::histogram!(SLOT_WAIT_HISTOGRAM, "outcome" => outcome_label).record(waited);
}

const CGROUP_PRESSURE_WARN_INTERVAL_SECS: u64 = 30;

fn maybe_warn_cgroup_memory_pressure() {
    use std::sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    };

    static START: OnceLock<Instant> = OnceLock::new();
    static LAST_SAMPLE_SECS: AtomicU64 = AtomicU64::new(0);

    // Reserve the sampling interval atomically before touching cgroup files so
    // bursts of concurrent timeouts cannot hammer synchronous reads on Tokio
    // workers.
    let elapsed_secs = START.get_or_init(Instant::now).elapsed().as_secs().max(1);
    let previous = LAST_SAMPLE_SECS.load(AtomicOrdering::Relaxed);
    if previous != 0 && elapsed_secs.saturating_sub(previous) < CGROUP_PRESSURE_WARN_INTERVAL_SECS {
        return;
    }
    if LAST_SAMPLE_SECS
        .compare_exchange(
            previous,
            elapsed_secs,
            AtomicOrdering::AcqRel,
            AtomicOrdering::Relaxed,
        )
        .is_err()
    {
        return;
    }

    let Some(snapshot) = zinder_runtime::container_memory_snapshot() else {
        return;
    };
    if snapshot.limit_bytes == 0
        || u128::from(snapshot.current_bytes) * 10 < u128::from(snapshot.limit_bytes) * 8
    {
        return;
    }
    tracing::warn!(
        target: "zinder::compat::lightwalletd",
        event = "block_range_wait_timeout_under_cgroup_memory_pressure",
        memory_current_bytes = snapshot.current_bytes,
        memory_limit_bytes = snapshot.limit_bytes,
        memory_file_cache_bytes = snapshot.file_cache_bytes,
        "block-range admission timed out while cgroup v2 memory usage is near its limit; \
         canonical settled-segment reads charge OS page cache to MemoryMax/mem_limit outside RocksDB"
    );
}

/// A taken slot, counted in `zinder_compat_block_range_streams_open` until it
/// is dropped.
///
/// The gauge moves by one on each open and close rather than taking a count,
/// so concurrent opens and closes cannot leave it at a stale value.
#[derive(Debug)]
struct OpenStream {
    _permit: OwnedSemaphorePermit,
}

impl OpenStream {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        metrics::gauge!(OPEN_STREAMS_GAUGE).increment(1.0);
        Self { _permit: permit }
    }
}

impl Drop for OpenStream {
    fn drop(&mut self) {
        metrics::gauge!(OPEN_STREAMS_GAUGE).decrement(1.0);
    }
}

/// A block-range response: the blocks a stream sends, in wire order.
pub(crate) type BlockRangeResponse<Message> = Pin<Box<dyn Stream<Item = Message> + Send + 'static>>;

/// One stream's slot of a [`BlockRangeStreamLimit`].
///
/// The slot returns to the limit once every holder has dropped it: the
/// response, the [`BlockRangeSink`] of the stream's reader for a stream of
/// more than one chunk, and a read run with [`Self::hold_during`] that has
/// not finished. A reader drops its sink once it has no chunk left to read
/// or its response is gone and its read in flight has finished.
#[derive(Clone, Debug)]
pub(crate) struct BlockRangeStreamSlot {
    _open_stream: Arc<OpenStream>,
}

impl BlockRangeStreamSlot {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            _open_stream: Arc::new(OpenStream::new(permit)),
        }
    }

    /// Runs `read` on a task of its own that holds this slot until `read`
    /// has finished and its output is dropped, and returns that output.
    ///
    /// A store read runs on the blocking pool, which finishes it even after
    /// its caller is cancelled. A client that cancels its request during
    /// `read` therefore cannot return the slot while `read` still holds the
    /// chunk it reads.
    ///
    /// # Errors
    ///
    /// Returns the error of `read`, or `UNAVAILABLE` when the task that runs
    /// `read` panics.
    pub(crate) async fn hold_during<Output, Read>(&self, read: Read) -> Result<Output, Status>
    where
        Output: Send + 'static,
        Read: Future<Output = Result<Output, Status>> + Send + 'static,
    {
        let stream_slot = self.clone();
        let (outcome, _stream_slot) = tokio::spawn(
            async move { (read.await, stream_slot) }.instrument(tracing::Span::current()),
        )
        .await
        .map_err(|error| Status::unavailable(format!("block-range read failed: {error}")))?;
        outcome
    }

    /// Returns a response that sends `messages` and holds this slot until the
    /// stream ends, yields a terminal error, or is dropped.
    pub(crate) fn hold_for<Message>(self, messages: Vec<Message>) -> BlockRangeResponse<Message>
    where
        Message: StreamItemEndsStream + Send + 'static,
    {
        self.hold_for_stream(Box::pin(ChunkIter::new(messages)))
    }

    /// Returns a response that sends `first_messages` and then each chunk
    /// `reader` hands it, and spawns `reader` to read the later chunks.
    ///
    /// The response sends a chunk's messages from the chunk itself and takes
    /// the next chunk only once it has sent them all, and the sink gives
    /// `reader` room for one chunk at a time. The stream therefore holds at
    /// most two chunks however slowly its client reads: the one the response
    /// sends and the one `reader` reads or has read next.
    ///
    /// The response and the reader's sink each hold this slot, so the reader
    /// keeps the slot while a read runs, even after the client has gone away.
    /// The response releases its clone as soon as the stream ends, yields a
    /// terminal error, or is dropped.
    pub(crate) fn hold_with_reader<Message, Reader>(
        self,
        first_messages: Vec<Message>,
        reader: impl FnOnce(BlockRangeSink<Message>) -> Reader,
    ) -> BlockRangeResponse<Message>
    where
        Message: StreamItemEndsStream + Send + 'static,
        Reader: Future<Output = ()> + Send + 'static,
    {
        let (next_chunk, next_chunks) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        tokio::spawn(
            reader(BlockRangeSink {
                next_chunk,
                cancel: cancel.clone(),
                _stream_slot: self.clone(),
            })
            .instrument(tracing::Span::current()),
        );
        self.hold_for_stream(Box::pin(ChunkedMessages {
            current: Some(ChunkIter::new(first_messages)),
            next_chunks,
            cancel,
        }))
    }

    fn hold_for_stream<Message>(
        self,
        stream: BlockRangeResponse<Message>,
    ) -> BlockRangeResponse<Message>
    where
        Message: StreamItemEndsStream + 'static,
    {
        Box::pin(SlotHoldingStream {
            stream,
            slot: Some(self),
        })
    }
}

/// Where the reader of a stream's later chunks hands them to the response.
///
/// It holds the stream's slot until the reader drops it.
#[derive(Debug)]
pub(crate) struct BlockRangeSink<Message> {
    next_chunk: mpsc::Sender<Vec<Message>>,
    cancel: CancellationToken,
    _stream_slot: BlockRangeStreamSlot,
}

impl<Message> BlockRangeSink<Message> {
    /// Returns a cooperative cancellation token signaled when the client drops
    /// the response stream.
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
    /// Waits until the response has room for one more chunk and returns
    /// that room.
    ///
    /// The response has room once it has taken every chunk handed to it
    /// before, which it does when it has sent every message of the chunk
    /// before that. A reader that takes this room before each read therefore
    /// holds one chunk beside the one the response sends.
    ///
    /// Returns `None` once the client has dropped the response, so a reader
    /// that asks before each read starts no read after its client went away.
    pub(crate) async fn room_for_chunk(&self) -> Option<ChunkRoom<'_, Message>> {
        let permit = self.next_chunk.reserve().await.ok()?;
        Some(ChunkRoom { permit })
    }

    pub(crate) async fn closed(&self) {
        self.next_chunk.closed().await;
    }

    /// Returns whether the client has dropped the response, so a reader
    /// that already holds room can start no further read after its client
    /// went away.
    pub(crate) fn is_closed(&self) -> bool {
        self.next_chunk.is_closed()
    }
}

/// Room for one chunk in a block-range response, which the read that fills
/// it holds.
pub(crate) struct ChunkRoom<'sink, Message> {
    permit: mpsc::Permit<'sink, Vec<Message>>,
}

impl<Message> ChunkRoom<'_, Message> {
    /// Hands `messages` to the response, which sends them after the chunk
    /// before them. A response the client has dropped drops them.
    pub(crate) fn fill(self, messages: Vec<Message>) {
        self.permit.send(messages);
    }
}

/// Items yielded from one chunk before returning `Poll::Pending` once to let
/// sibling tasks run.
///
/// `tokio_stream::iter` yields every 32 items (~2.5 KiB of empty blocks), which
/// breaks 32 KiB gRPC batches into tiny HTTP/2 DATA frames and forces a task
/// reschedule every 32 blocks. 768 items covers two full 32 KiB frames of
/// minimum-size blocks (current + `ReleaseGate` lookahead) while still yielding
/// inside a 1,000-block typed chunk.
const CHUNK_YIELD_EVERY: usize = 768;

struct ChunkIter<Message> {
    messages: std::vec::IntoIter<Message>,
    polled_since_yield: usize,
}

impl<Message> ChunkIter<Message> {
    fn new(messages: Vec<Message>) -> Self {
        Self {
            messages: messages.into_iter(),
            polled_since_yield: 0,
        }
    }
}

impl<Message> Unpin for ChunkIter<Message> {}

impl<Message> Stream for ChunkIter<Message> {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Message>> {
        if self.messages.len() == 0 {
            return Poll::Ready(None);
        }
        if self.polled_since_yield >= CHUNK_YIELD_EVERY {
            self.polled_since_yield = 0;
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.polled_since_yield += 1;
        Poll::Ready(self.messages.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.messages.size_hint()
    }
}

/// The messages of a stream with later chunks, sent from one chunk at a
/// time.
struct ChunkedMessages<Message> {
    /// The messages of the chunk being sent that the client has not taken,
    /// which yield to the runtime as a one-chunk response does, or `None`
    /// once they have all been taken.
    current: Option<ChunkIter<Message>>,
    /// The chunks the stream's reader hands on, at most one ahead.
    next_chunks: mpsc::Receiver<Vec<Message>>,
    /// Cooperative cancellation token signaled when this stream drops.
    cancel: CancellationToken,
}

impl<Message> Drop for ChunkedMessages<Message> {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// No field is pinned: the stream moves its messages out by value.
impl<Message> Unpin for ChunkedMessages<Message> {}

impl<Message> Stream for ChunkedMessages<Message> {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Message>> {
        loop {
            if let Some(current) = self.current.as_mut() {
                match Pin::new(current).poll_next(context) {
                    Poll::Ready(Some(message)) => return Poll::Ready(Some(message)),
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => self.current = None,
                }
            }
            let Some(next_chunk) = std::task::ready!(self.next_chunks.poll_recv(context)) else {
                return Poll::Ready(None);
            };
            self.current = Some(ChunkIter::new(next_chunk));
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let current = self
            .current
            .as_ref()
            .map_or(0, |current| current.size_hint().0);
        (current, None)
    }
}

/// Reports whether yielding a message terminates a block-range response stream.
///
/// Both tonic's server encoder and `EncodedBlockBody` stop polling a response
/// stream as soon as it yields `Some(Err(_))` rather than polling again for
/// `None`. Releasing the slot on a terminal error item matches that behavior.
pub(crate) trait StreamItemEndsStream {
    /// Returns `true` when yielding `self` ends the response stream.
    fn ends_stream(&self) -> bool;
}

impl<Message, Error> StreamItemEndsStream for Result<Message, Error> {
    fn ends_stream(&self) -> bool {
        self.is_err()
    }
}

#[cfg(test)]
impl StreamItemEndsStream for u32 {
    fn ends_stream(&self) -> bool {
        false
    }
}

struct SlotHoldingStream<S> {
    stream: S,
    slot: Option<BlockRangeStreamSlot>,
}

impl<S> Stream for SlotHoldingStream<S>
where
    S: Stream + Unpin,
    S::Item: StreamItemEndsStream,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<S::Item>> {
        if self.slot.is_none() {
            return Poll::Ready(None);
        }
        let next_message = Pin::new(&mut self.stream).poll_next(context);
        match &next_message {
            Poll::Ready(None) => {
                drop(self.slot.take());
            }
            Poll::Ready(Some(message)) if message.ends_stream() => {
                drop(self.slot.take());
            }
            Poll::Ready(Some(_)) | Poll::Pending => {}
        }
        next_message
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.slot.is_none() {
            (0, Some(0))
        } else {
            self.stream.size_hint()
        }
    }
}

/// The last block a lightwalletd block-range stream sent, and the chain view
/// it was read at.
///
/// Both compatibility serving modes check every chunk after the first
/// against this value, so they end a stream for the same reasons and record
/// the same metric and log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlockRangeContinuity {
    is_descending: bool,
    last_sent_height: BlockHeight,
    /// An ascending chunk must name this hash as its first block's parent.
    last_sent_hash: BlockHash,
    /// A descending chunk must end with the block that has this hash.
    last_sent_parent_hash: BlockHash,
    /// Epoch of the view the last sent chunk was read at.
    sent_chain_epoch_id: ChainEpochId,
}

/// A later chunk that [`BlockRangeContinuity::link`] checked against the
/// chain a stream has sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChunkLink {
    next_chunk: BlockHeightRange,
    read_at: ChainEpochId,
    /// The continuity past the chunk, or `None` when the chunk does not link.
    continued: Option<BlockRangeContinuity>,
}

impl ChunkLink {
    /// Returns whether the chunk continues the chain the stream has sent.
    pub(crate) const fn continues(&self) -> bool {
        self.continued.is_some()
    }
}

/// Why a block-range stream ended before its last requested height.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReorgEndingCause {
    /// The next chunk does not link by hash to the last block sent.
    ChainReplaced,
    /// The current view's visible tip is below a height still to send.
    TipLowered,
}

impl ReorgEndingCause {
    const fn label(self) -> &'static str {
        match self {
            Self::ChainReplaced => "chain_replaced",
            Self::TipLowered => "tip_lowered",
        }
    }
}

pub(crate) trait RangeBlock {
    fn height(&self) -> BlockHeight;
    fn block_hash(&self) -> BlockHash;
    fn previous_block_hash(&self) -> BlockHash;
}

impl RangeBlock for CompactBlockArtifact {
    fn height(&self) -> BlockHeight {
        self.height()
    }
    fn block_hash(&self) -> BlockHash {
        self.block_hash()
    }
    fn previous_block_hash(&self) -> BlockHash {
        self.previous_block_hash()
    }
}

impl RangeBlock for zinder_store::EncodedCompactBlockLink {
    fn height(&self) -> BlockHeight {
        self.height
    }
    fn block_hash(&self) -> BlockHash {
        self.block_hash
    }
    fn previous_block_hash(&self) -> BlockHash {
        self.previous_block_hash
    }
}

impl BlockRangeContinuity {
    /// Returns the continuity after the stream's first chunk.
    ///
    /// `blocks` are the chunk's compact blocks in ascending height order, read
    /// at `chain_epoch_id`. The stream sends them in its own direction, so the
    /// last block sent is the highest when ascending and the lowest when
    /// descending.
    ///
    /// # Errors
    ///
    /// Returns `UNAVAILABLE` when `blocks` is empty. A chunk read keeps at
    /// least one block or fails, so an empty chunk means the store broke that
    /// contract.
    pub(crate) fn after_first_chunk(
        blocks: &[impl RangeBlock],
        chain_epoch_id: ChainEpochId,
        is_descending: bool,
    ) -> Result<Self, Status> {
        let last_sent = if is_descending {
            blocks.first()
        } else {
            blocks.last()
        };
        let last_sent = last_sent.ok_or_else(|| {
            Status::unavailable("compact-block range read returned no blocks for its chunk")
        })?;
        Ok(Self {
            is_descending,
            last_sent_height: last_sent.height(),
            last_sent_hash: last_sent.block_hash(),
            last_sent_parent_hash: last_sent.previous_block_hash(),
            sent_chain_epoch_id: chain_epoch_id,
        })
    }

    /// Ends the stream at `view`, whose visible tip a reorg lowered below
    /// `next_chunk`, records the ending, and returns lightwalletd's
    /// `OUT_OF_RANGE`.
    ///
    /// The chunk read finds the lowered tip before it reads, so the stream
    /// ends with this code rather than the `NOT_FOUND` of a read above the
    /// tip.
    pub(crate) fn end_below_tip(&self, next_chunk: BlockHeightRange, view: &ChainEpoch) -> Status {
        self.record_reorg_ending(ReorgEndingCause::TipLowered, next_chunk, view.id);
        Status::out_of_range(format!(
            "block {} is newer than latest block {} after a reorg",
            next_chunk.end.value(),
            view.visible_tip_height.value(),
        ))
    }

    /// Checks whether `blocks`, the compact blocks a read of `next_chunk` at
    /// `read_at` kept from the end the stream sends first, in ascending height
    /// order, continue the chain the stream has sent.
    ///
    /// Ascending, the chunk's lowest block must sit one height above the last
    /// block sent and name it as its parent. Descending, the chunk's highest
    /// block must sit one height below the last block sent and be its parent.
    ///
    /// The check records nothing, so it runs where the chunk was read, and
    /// the stream's task passes its outcome to [`Self::accept`].
    pub(crate) fn link(
        &self,
        next_chunk: BlockHeightRange,
        read_at: ChainEpochId,
        blocks: &[impl RangeBlock],
    ) -> ChunkLink {
        let continued = if self.is_continued_by(blocks) {
            Self::after_first_chunk(blocks, read_at, self.is_descending).ok()
        } else {
            None
        };
        ChunkLink {
            next_chunk,
            read_at,
            continued,
        }
    }

    /// Moves past the chunk `link` checked, and records a stream it ends.
    ///
    /// # Errors
    ///
    /// Returns `ABORTED` when the chunk does not link.
    pub(crate) fn accept(&mut self, link: ChunkLink) -> Result<(), Status> {
        let Some(continued) = link.continued else {
            self.record_reorg_ending(
                ReorgEndingCause::ChainReplaced,
                link.next_chunk,
                link.read_at,
            );
            return Err(Status::aborted(
                "block-range stream ended at a chain discontinuity: a reorg replaced the chain it \
                 was sending",
            ));
        };
        if link.read_at != self.sent_chain_epoch_id {
            tracing::debug!(
                target: "zinder::compat_lightwalletd",
                event = "compat_block_range_continued_at_newer_epoch",
                last_sent_height = self.last_sent_height.value(),
                sent_chain_epoch = self.sent_chain_epoch_id.value(),
                current_chain_epoch = link.read_at.value(),
                next_chunk_start = link.next_chunk.start.value(),
                next_chunk_end = link.next_chunk.end.value(),
                "block-range stream continued at a newer chain view"
            );
        }
        *self = continued;
        Ok(())
    }

    fn is_continued_by(&self, blocks: &[impl RangeBlock]) -> bool {
        if self.is_descending {
            blocks.last().is_some_and(|next| {
                next.height().next() == Some(self.last_sent_height)
                    && next.block_hash() == self.last_sent_parent_hash
            })
        } else {
            blocks.first().is_some_and(|next| {
                self.last_sent_height.next() == Some(next.height())
                    && next.previous_block_hash() == self.last_sent_hash
            })
        }
    }

    fn record_reorg_ending(
        &self,
        cause: ReorgEndingCause,
        next_chunk: BlockHeightRange,
        current_chain_epoch_id: ChainEpochId,
    ) {
        metrics::counter!(
            "zinder_compat_block_range_reorg_endings_total",
            "cause" => cause.label()
        )
        .increment(1);
        tracing::info!(
            target: "zinder::compat_lightwalletd",
            event = "compat_block_range_ended_by_reorg",
            cause = cause.label(),
            last_sent_height = self.last_sent_height.value(),
            sent_chain_epoch = self.sent_chain_epoch_id.value(),
            current_chain_epoch = current_chain_epoch_id.value(),
            next_chunk_start = next_chunk.start.value(),
            next_chunk_end = next_chunk.end.value(),
            "block-range stream ended because a reorg replaced the chain it was sending"
        );
    }
}

#[cfg(test)]
#[allow(
    clippy::significant_drop_tightening,
    reason = "Admission tests hold slots and join handles until the step that releases or awaits them."
)]
mod tests {
    use std::{
        num::NonZeroU32,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
        },
        task::Poll,
        time::Duration,
    };

    use tokio::{
        sync::{Semaphore, mpsc},
        time::Instant,
    };
    use tokio_stream::{Stream, StreamExt as _};
    use tonic::{Code, Status};
    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpoch,
        ChainEpochId, ChainTipMetadata, CompactBlockArtifact, CompactChainMetadata, Network,
        UnixTimestampMillis,
    };

    use zinder_store::HeightOrder;

    use super::{
        BLOCK_RANGE_CHUNK_MAX_BYTES, BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES,
        BLOCK_RANGE_WAITERS_PER_SLOT, BlockRangeChunks, BlockRangeContinuity,
        BlockRangeStreamLimit, ChunkedMessages, DEFAULT_BLOCK_RANGE_ADMISSION_WAIT,
        DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS,
    };

    const SENT_EPOCH: ChainEpochId = ChainEpochId::new(4);
    const CURRENT_EPOCH: ChainEpochId = ChainEpochId::new(5);

    /// Hash of the block at `height` on `branch`; branch 0 is the entry chain.
    fn block_hash(height: u32, branch: u8) -> BlockHash {
        let mut bytes = [0_u8; 32];
        bytes[..4].copy_from_slice(&height.to_le_bytes());
        bytes[4] = branch;
        BlockHash::from_bytes(bytes)
    }

    fn block_at(height: u32, hash: BlockHash, parent_hash: BlockHash) -> CompactBlockArtifact {
        CompactBlockArtifact::empty(
            BlockId::new(BlockHeight::new(height), hash),
            parent_hash,
            0,
            CompactChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )
    }

    /// Blocks `start..=end` in ascending order of the chain that follows
    /// branch 0 below `fork_height` and `branch` from it.
    fn chain(start: u32, end: u32, branch: u8, fork_height: u32) -> Vec<CompactBlockArtifact> {
        let branch_at = |height: u32| if height >= fork_height { branch } else { 0 };
        (start..=end)
            .map(|height| {
                let parent_height = height.saturating_sub(1);
                block_at(
                    height,
                    block_hash(height, branch_at(height)),
                    block_hash(parent_height, branch_at(parent_height)),
                )
            })
            .collect()
    }

    fn view(visible_tip_height: u32) -> ChainEpoch {
        ChainEpoch {
            id: CURRENT_EPOCH,
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(visible_tip_height),
            visible_tip_hash: block_hash(visible_tip_height, 0),
            settled_tip_height: BlockHeight::new(1),
            settled_tip_hash: block_hash(1, 0),
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            created_at: UnixTimestampMillis::new(0),
        }
    }

    fn range(start: u32, end: u32) -> BlockHeightRange {
        BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end))
    }

    /// Checks `blocks` against `continuity` and applies the outcome, as a
    /// stream's reader does across the blocking pool and its own task.
    fn advance(
        continuity: &mut BlockRangeContinuity,
        next_chunk: BlockHeightRange,
        read_at: &ChainEpoch,
        blocks: &[CompactBlockArtifact],
    ) -> Result<(), tonic::Status> {
        let link = continuity.link(next_chunk, read_at.id, blocks);
        continuity.accept(link)
    }

    fn assert_chain_discontinuity(outcome: Result<(), tonic::Status>) {
        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::Aborted)
        );
    }

    #[test]
    fn ascending_continues_only_from_the_last_sent_block() -> Result<(), tonic::Status> {
        let sent = chain(1, 10, 0, u32::MAX);
        let mut continuity = BlockRangeContinuity::after_first_chunk(&sent, SENT_EPOCH, false)?;

        // A fork just above the last sent block keeps the sent chain.
        advance(
            &mut continuity,
            range(11, 20),
            &view(30),
            &chain(11, 20, 1, 11),
        )?;
        // The next chunk must follow the new last block, not the first chunk's.
        advance(
            &mut continuity,
            range(21, 30),
            &view(30),
            &chain(21, 30, 1, 11),
        )?;

        let mut fork_at_last_sent =
            BlockRangeContinuity::after_first_chunk(&sent, SENT_EPOCH, false)?;
        assert_chain_discontinuity(advance(
            &mut fork_at_last_sent,
            range(11, 20),
            &view(20),
            &chain(11, 20, 2, 10),
        ));
        let mut fork_below = BlockRangeContinuity::after_first_chunk(&sent, SENT_EPOCH, false)?;
        assert_chain_discontinuity(advance(
            &mut fork_below,
            range(11, 20),
            &view(20),
            &chain(11, 20, 2, 9),
        ));
        Ok(())
    }

    #[test]
    fn descending_continues_only_into_the_parent_of_the_last_sent_block()
    -> Result<(), tonic::Status> {
        // Sent 30 down to 21: the last block sent is 21.
        let sent = chain(21, 30, 0, u32::MAX);
        let mut continuity = BlockRangeContinuity::after_first_chunk(&sent, SENT_EPOCH, true)?;

        // A fork at the last sent height keeps its parent, so the stream
        // continues on the chain it has sent.
        advance(
            &mut continuity,
            range(11, 20),
            &view(30),
            &chain(11, 20, 1, 21),
        )?;
        advance(
            &mut continuity,
            range(1, 10),
            &view(30),
            &chain(1, 10, 1, 21),
        )?;

        let mut fork_below = BlockRangeContinuity::after_first_chunk(&sent, SENT_EPOCH, true)?;
        assert_chain_discontinuity(advance(
            &mut fork_below,
            range(11, 20),
            &view(30),
            &chain(11, 20, 2, 20),
        ));
        Ok(())
    }

    #[test]
    fn a_chunk_at_the_wrong_height_does_not_link() -> Result<(), tonic::Status> {
        let ascending = chain(1, 10, 0, u32::MAX);
        let mut skipped = BlockRangeContinuity::after_first_chunk(&ascending, SENT_EPOCH, false)?;
        assert_chain_discontinuity(advance(
            &mut skipped,
            range(12, 20),
            &view(20),
            &chain(12, 20, 0, u32::MAX),
        ));

        let descending = chain(21, 30, 0, u32::MAX);
        let mut overlapping =
            BlockRangeContinuity::after_first_chunk(&descending, SENT_EPOCH, true)?;
        assert_chain_discontinuity(advance(
            &mut overlapping,
            range(12, 21),
            &view(30),
            &chain(12, 21, 0, u32::MAX),
        ));

        // A block that names the last sent block as its parent but claims
        // another height does not link either.
        let mut misnumbered =
            BlockRangeContinuity::after_first_chunk(&ascending, SENT_EPOCH, false)?;
        assert_chain_discontinuity(advance(
            &mut misnumbered,
            range(11, 11),
            &view(20),
            &[block_at(12, block_hash(12, 0), block_hash(10, 0))],
        ));
        let mut misnumbered_parent =
            BlockRangeContinuity::after_first_chunk(&descending, SENT_EPOCH, true)?;
        assert_chain_discontinuity(advance(
            &mut misnumbered_parent,
            range(20, 20),
            &view(30),
            &[block_at(19, block_hash(20, 0), block_hash(18, 0))],
        ));

        let mut empty = BlockRangeContinuity::after_first_chunk(&ascending, SENT_EPOCH, false)?;
        assert_chain_discontinuity(advance(&mut empty, range(11, 20), &view(20), &[]));
        Ok(())
    }

    #[test]
    fn a_view_below_the_next_chunk_ends_the_stream_out_of_range() -> Result<(), tonic::Status> {
        let continuity =
            BlockRangeContinuity::after_first_chunk(&chain(1, 10, 0, u32::MAX), SENT_EPOCH, false)?;
        let status = continuity.end_below_tip(range(11, 20), &view(19));
        assert_eq!(status.code(), Code::OutOfRange);
        Ok(())
    }

    #[test]
    fn an_empty_first_chunk_is_unavailable() {
        let outcome = BlockRangeContinuity::after_first_chunk(
            &[] as &[CompactBlockArtifact],
            SENT_EPOCH,
            false,
        );
        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::Unavailable)
        );
    }

    /// Plans every chunk of `chunks`, assuming each read keeps the whole
    /// chunk it was offered.
    fn drain(chunks: &mut BlockRangeChunks) -> Vec<BlockHeightRange> {
        std::iter::from_fn(|| {
            let chunk = chunks.next_chunk()?;
            chunks.advance(chunk.into_iter().len());
            Some(chunk)
        })
        .collect()
    }

    #[test]
    fn chunks_hold_at_most_one_store_read_and_preserve_direction() {
        let mut ascending = BlockRangeChunks::new(range(1, 2_500), false);
        assert_eq!(ascending.order(), HeightOrder::Ascending);
        assert_eq!(
            drain(&mut ascending),
            vec![range(1, 1_000), range(1_001, 2_000), range(2_001, 2_500)]
        );
        assert!(!ascending.has_remaining());

        let mut descending = BlockRangeChunks::new(range(1, 2_500), true);
        assert_eq!(descending.order(), HeightOrder::Descending);
        assert_eq!(
            drain(&mut descending),
            vec![range(1_501, 2_500), range(501, 1_500), range(1, 500)]
        );

        let mut single = BlockRangeChunks::new(range(7, 7), true);
        assert_eq!(drain(&mut single), vec![range(7, 7)]);
        let mut top = BlockRangeChunks::new(range(u32::MAX - 1, u32::MAX), false);
        assert_eq!(drain(&mut top), vec![range(u32::MAX - 1, u32::MAX)]);
        let mut whole = BlockRangeChunks::new(range(0, u32::MAX), true);
        assert_eq!(whole.next_chunk(), Some(range(u32::MAX - 999, u32::MAX)));
        whole.advance(usize::MAX);
        assert!(!whole.has_remaining());
    }

    #[test]
    fn each_chunk_is_offered_up_to_1000_heights_from_the_next_height_to_send() {
        let mut ascending = BlockRangeChunks::new(range(1, 3_000), false);
        assert_eq!(ascending.next_chunk(), Some(range(1, 1_000)));
        // A byte-bounded read kept 120 of the 1,000 heights it was offered.
        ascending.advance(120);
        assert_eq!(ascending.next_chunk(), Some(range(121, 1_120)));
        // A read that kept nothing leaves the plan where it was.
        ascending.advance(0);
        assert_eq!(ascending.next_chunk(), Some(range(121, 1_120)));
        ascending.advance(1_000);
        assert_eq!(ascending.next_chunk(), Some(range(1_121, 2_120)));
        ascending.advance(880);
        assert_eq!(ascending.next_chunk(), Some(range(2_001, 3_000)));
        ascending.advance(999);
        assert_eq!(ascending.next_chunk(), Some(range(3_000, 3_000)));
        ascending.advance(1);
        assert_eq!(ascending.next_chunk(), None);

        let mut descending = BlockRangeChunks::new(range(1, 3_000), true);
        assert_eq!(descending.next_chunk(), Some(range(2_001, 3_000)));
        descending.advance(120);
        assert_eq!(descending.next_chunk(), Some(range(1_881, 2_880)));
        descending.advance(2_880);
        assert!(!descending.has_remaining());
    }

    /// Returns how many reads a stream over `range` makes when every block
    /// holds `block_bytes` stored bytes and each read keeps as many blocks
    /// of the heights it is offered as its byte budget holds.
    fn reads_of_equal_blocks(
        range: BlockHeightRange,
        is_descending: bool,
        block_bytes: usize,
    ) -> usize {
        let mut chunks = BlockRangeChunks::new(range, is_descending);
        let mut max_bytes = BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES;
        let mut reads = 0;
        while let Some(chunk) = chunks.next_chunk() {
            let kept = (max_bytes / block_bytes).clamp(1, chunk.into_iter().len());
            chunks.advance(kept);
            max_bytes = BLOCK_RANGE_CHUNK_MAX_BYTES;
            reads += 1;
        }
        reads
    }

    /// A dense range takes one read for each chunk its byte budget holds.
    ///
    /// A read fills its budget whatever heights the read before it was
    /// offered, so blocks that just overflow a 1,000-block chunk take one
    /// read per 998 blocks rather than two per 1,000 heights.
    #[test]
    fn a_dense_range_takes_one_read_for_each_byte_capped_chunk() {
        const BLOCK_BYTES: usize = 2_100;
        let whole = range(1, 100_000);
        let first_chunk_blocks = BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES / BLOCK_BYTES;
        let later_chunk_blocks = BLOCK_RANGE_CHUNK_MAX_BYTES / BLOCK_BYTES;
        assert_eq!(
            (first_chunk_blocks, later_chunk_blocks),
            (124, 998),
            "a later read must keep just under 1,000 blocks"
        );
        let heights = whole.into_iter().len();
        let expected_reads = 1 + (heights - first_chunk_blocks).div_ceil(later_chunk_blocks);
        assert_eq!(expected_reads, 102);
        for is_descending in [false, true] {
            assert_eq!(
                reads_of_equal_blocks(whole, is_descending, BLOCK_BYTES),
                expected_reads,
                "descending: {is_descending}"
            );
        }
    }

    /// A descending range whose last height is outside retained history
    /// sends every height an earlier read checked before a read reaches it.
    ///
    /// A read checks every height it is offered, so the read offered height
    /// 0 with the 936 heights the first read checked and did not keep fails
    /// them all. The stream then reads those 936 alone, until it has sent
    /// them.
    #[test]
    fn a_read_past_the_checked_heights_gives_way_to_them_alone() -> Result<(), &'static str> {
        let mut descending = BlockRangeChunks::new(range(0, 1_000), true);
        assert_eq!(descending.next_chunk(), Some(range(1, 1_000)));
        assert_eq!(
            descending.checked_heights(),
            None,
            "no read has checked a height"
        );
        descending.advance(64);
        assert_eq!(descending.next_chunk(), Some(range(0, 936)));

        let (checked, mut only_checked) = descending
            .checked_heights()
            .ok_or("the first read checked 936 heights it did not keep")?;
        assert_eq!(checked, range(1, 936));
        assert_eq!(only_checked.next_chunk(), Some(checked));
        assert_eq!(
            only_checked.checked_heights(),
            None,
            "a read of the checked heights alone has no smaller retry"
        );
        only_checked.advance(900);
        assert_eq!(only_checked.next_chunk(), Some(range(1, 36)));
        only_checked.advance(36);
        assert_eq!(only_checked.next_chunk(), Some(range(0, 0)));
        assert_eq!(
            only_checked.checked_heights(),
            None,
            "every checked height was sent"
        );

        // Once the checked heights are sent, reads are offered 1,000 new
        // heights again.
        let mut ascending = BlockRangeChunks::new(range(1, 3_000), false);
        ascending.advance(100);
        let (checked, mut only_checked) = ascending
            .checked_heights()
            .ok_or("the first read checked 900 heights it did not keep")?;
        assert_eq!(checked, range(101, 1_000));
        only_checked.advance(900);
        assert_eq!(only_checked.next_chunk(), Some(range(1_001, 2_000)));

        // A read offered only heights the last read checked has no retry.
        let mut short = BlockRangeChunks::new(range(1, 500), false);
        short.advance(100);
        assert_eq!(short.next_chunk(), Some(range(101, 500)));
        assert_eq!(short.checked_heights(), None);
        Ok(())
    }

    /// Lets every spawned task run until it waits.
    async fn let_tasks_run() {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    /// A stream of one-message chunks whose reader counts the reads it
    /// starts, on a limit of one slot.
    struct CountedStream {
        limit: BlockRangeStreamLimit,
        response: super::BlockRangeResponse<u32>,
        reads_started: Arc<AtomicUsize>,
    }

    impl CountedStream {
        /// Opens a stream whose response holds `first_messages` and whose
        /// reader reads one-message chunks from 100 on, each once `gate`
        /// gives it a permit.
        async fn open(
            first_messages: Vec<u32>,
            gate: Arc<tokio::sync::Semaphore>,
        ) -> Result<Self, Status> {
            let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN).with_max_wait(Duration::ZERO);
            let reads_started = Arc::new(AtomicUsize::new(0));
            let reader_reads = Arc::clone(&reads_started);
            let response =
                limit
                    .open()
                    .await?
                    .hold_with_reader(first_messages, |sink| async move {
                        for message in 100_u32.. {
                            let Some(room) = sink.room_for_chunk().await else {
                                return;
                            };
                            reader_reads.fetch_add(1, AtomicOrdering::SeqCst);
                            let Ok(_permit) = gate.acquire().await else {
                                return;
                            };
                            room.fill(vec![message]);
                        }
                    });
            Ok(Self {
                limit,
                response,
                reads_started,
            })
        }

        fn reads_started(&self) -> usize {
            self.reads_started.load(AtomicOrdering::SeqCst)
        }

        async fn next(&mut self) -> Option<u32> {
            self.response.next().await
        }
    }

    #[tokio::test]
    async fn a_stream_holds_the_chunk_it_sends_and_at_most_one_more() -> Result<(), Status> {
        let open_gate = Arc::new(tokio::sync::Semaphore::new(Semaphore::MAX_PERMITS));
        let mut stream = CountedStream::open(vec![1, 2], open_gate).await?;
        let_tasks_run().await;
        assert_eq!(
            stream.reads_started(),
            1,
            "a stream whose client takes nothing reads one chunk past its first"
        );

        assert_eq!(stream.next().await, Some(1));
        assert_eq!(stream.next().await, Some(2));
        let_tasks_run().await;
        assert_eq!(
            stream.reads_started(),
            1,
            "a client that has not asked past the first chunk makes no room"
        );

        // Asking past the first chunk takes the second, which makes room for
        // the third, and no more, however long the client then stalls.
        assert_eq!(stream.next().await, Some(100));
        let_tasks_run().await;
        assert_eq!(stream.reads_started(), 2);
        assert_eq!(stream.next().await, Some(101));
        let_tasks_run().await;
        assert_eq!(stream.reads_started(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn a_dropped_stream_starts_no_read_and_returns_its_slot_after_the_one_in_flight()
    -> Result<(), Status> {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let mut stream = CountedStream::open(vec![1], Arc::clone(&gate)).await?;
        assert_eq!(stream.next().await, Some(1));
        let_tasks_run().await;
        assert_eq!(stream.reads_started(), 1, "the second chunk's read waits");

        let CountedStream {
            limit,
            response,
            reads_started,
        } = stream;
        drop(response);
        let_tasks_run().await;
        assert_unavailable(limit.open().await);

        gate.add_permits(1);
        let_tasks_run().await;
        assert_eq!(
            reads_started.load(AtomicOrdering::SeqCst),
            1,
            "a reader whose client went away starts no read after the one in flight"
        );
        drop(limit.open().await?);
        Ok(())
    }

    fn join_failed(error: &tokio::task::JoinError) -> Status {
        Status::internal(format!("admission task failed: {error}"))
    }

    fn assert_unavailable<T>(outcome: Result<T, Status>) {
        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::Unavailable)
        );
    }

    /// Lets a spawned admission run until it waits for a slot.
    async fn let_waiters_queue() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn the_stream_limit_admits_up_to_its_slots_until_one_is_returned() -> Result<(), Status> {
        let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN.saturating_add(1))
            .with_max_wait(Duration::ZERO);
        let response_side = limit.open().await?;
        let other_stream = limit.open().await?;
        assert_unavailable(limit.open().await);

        // A slot shared by a response and its reader returns only when both
        // are dropped.
        let reader_side = response_side.clone();
        drop(response_side);
        assert!(limit.open().await.is_err());
        drop(reader_side);
        let reopened = limit.open().await?;
        drop((reopened, other_stream));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_past_the_limit_waits_and_takes_the_next_returned_slot() -> Result<(), Status>
    {
        let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN);
        let open_stream = limit.open().await?;
        let waiting = tokio::spawn({
            let limit = limit.clone();
            async move { limit.open().await }
        });
        tokio::time::sleep(DEFAULT_BLOCK_RANGE_ADMISSION_WAIT / 2).await;
        assert!(!waiting.is_finished(), "the request must still wait");

        drop(open_stream);
        let admitted = waiting.await.map_err(|error| join_failed(&error))??;
        drop(admitted);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_that_waits_out_the_limit_is_unavailable() -> Result<(), Status> {
        let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN);
        let open_stream = limit.open().await?;
        let started_at = Instant::now();
        assert_unavailable(limit.open().await);
        assert_eq!(started_at.elapsed(), DEFAULT_BLOCK_RANGE_ADMISSION_WAIT);
        drop(open_stream);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_past_the_waiter_cap_is_unavailable_at_once() -> Result<(), Status> {
        let default_limit = BlockRangeStreamLimit::new(DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS);
        assert_eq!(default_limit.max_waiters(), 300);

        // One slot admits 3x waiters.
        let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN);
        assert_eq!(limit.max_waiters(), BLOCK_RANGE_WAITERS_PER_SLOT);
        let open_stream = limit.open().await?;
        let mut waiting = Vec::with_capacity(BLOCK_RANGE_WAITERS_PER_SLOT);
        for _ in 0..BLOCK_RANGE_WAITERS_PER_SLOT {
            waiting.push(tokio::spawn({
                let limit = limit.clone();
                async move { limit.open().await }
            }));
        }
        let_waiters_queue().await;

        let started_at = Instant::now();
        assert_unavailable(limit.open().await);
        assert_eq!(started_at.elapsed(), Duration::ZERO);

        drop(open_stream);
        for queued in waiting {
            drop(queued.await.map_err(|error| join_failed(&error))??);
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancelled_waiter_gives_back_its_place_in_the_queue() -> Result<(), Status> {
        let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN);
        let open_stream = limit.open().await?;
        let mut staying = Vec::with_capacity(BLOCK_RANGE_WAITERS_PER_SLOT - 1);
        for _ in 0..(BLOCK_RANGE_WAITERS_PER_SLOT - 1) {
            staying.push(tokio::spawn({
                let limit = limit.clone();
                async move { limit.open().await }
            }));
        }
        let cancelled = tokio::spawn({
            let limit = limit.clone();
            async move { limit.open().await }
        });
        let_waiters_queue().await;
        cancelled.abort();
        assert!(cancelled.await.is_err_and(|error| error.is_cancelled()));

        // The place the cancelled request held is free, so this request waits
        // instead of being refused, and takes the slot when it returns.
        let waiting = tokio::spawn({
            let limit = limit.clone();
            async move { limit.open().await }
        });
        let_waiters_queue().await;
        assert!(
            !waiting.is_finished(),
            "the request must wait, not be refused"
        );
        // The queue is now full again, so one more request fails immediately.
        assert_unavailable(limit.open().await);
        drop(open_stream);
        for queued in staying {
            drop(queued.await.map_err(|error| join_failed(&error))??);
        }
        drop(waiting.await.map_err(|error| join_failed(&error))??);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn the_open_streams_gauge_counts_each_slot_until_its_last_holder_drops_it()
    -> Result<(), Status> {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let open_streams =
            || rendered_sample(&handle.render(), "zinder_compat_block_range_streams_open");
        let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN.saturating_add(1));
        assert_eq!(open_streams(), Some(0.0));
        let response_side = limit.open().await?;
        let other_stream = limit.open().await?;
        let reader_side = response_side.clone();
        // A limit created while streams are open leaves their count alone.
        let _other_limit = BlockRangeStreamLimit::new(NonZeroU32::MIN);
        assert_eq!(open_streams(), Some(2.0));

        drop((response_side, other_stream));
        assert_eq!(open_streams(), Some(1.0));
        drop(reader_side);
        assert_eq!(open_streams(), Some(0.0));
        Ok(())
    }

    /// Returns the value of the first sample of `rendered` whose series is
    /// exactly `series`, name and labels included.
    fn rendered_sample(rendered: &str, series: &str) -> Option<f64> {
        rendered.lines().find_map(|line| {
            line.strip_prefix(series)?
                .strip_prefix(' ')?
                .parse::<f64>()
                .ok()
        })
    }

    #[test]
    fn stream_limit_preregisters_refusal_counters_at_zero_with_metadata() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);

        let _limit = BlockRangeStreamLimit::new(NonZeroU32::MIN);

        let rendered = handle.render();
        assert!(
            rendered.contains("# HELP zinder_compat_block_range_stream_refusals_total "),
            "missing # HELP for block-range refusals in:\n{rendered}"
        );
        assert!(
            rendered.contains("# TYPE zinder_compat_block_range_stream_refusals_total counter"),
            "missing # TYPE for block-range refusals in:\n{rendered}"
        );
        for reason in ["wait_timeout", "waiters_full"] {
            assert_eq!(
                rendered_sample(
                    &rendered,
                    &format!(
                        "zinder_compat_block_range_stream_refusals_total{{reason=\"{reason}\"}}"
                    ),
                ),
                Some(0.0),
                "expected 0 sample for reason={reason} in:\n{rendered}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn admission_counts_each_outcome_and_samples_its_slot_wait() -> Result<(), Status> {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN);
        let open_stream = limit.open().await?;

        // Fill all waiter places so one more request finds the queue full,
        // while the queued requests wait out the admission timeout.
        let mut timed_out = Vec::with_capacity(BLOCK_RANGE_WAITERS_PER_SLOT);
        for _ in 0..BLOCK_RANGE_WAITERS_PER_SLOT {
            timed_out.push(tokio::spawn({
                let limit = limit.clone();
                async move { limit.open().await }
            }));
        }
        let_waiters_queue().await;
        assert_unavailable(limit.open().await);
        for waiter in timed_out {
            assert_unavailable(waiter.await.map_err(|error| join_failed(&error))?);
        }

        // One request gives up while it waits.
        let cancelled = tokio::spawn({
            let limit = limit.clone();
            async move { limit.open().await }
        });
        let_waiters_queue().await;
        cancelled.abort();
        assert!(cancelled.await.is_err_and(|error| error.is_cancelled()));
        drop(open_stream);

        let rendered = handle.render();
        let sample = |series: &str| rendered_sample(&rendered, series);
        assert_eq!(
            sample("zinder_compat_block_range_streams_admitted_total"),
            Some(1.0)
        );
        for (reason, expected) in [("waiters_full", 1.0), ("wait_timeout", 3.0)] {
            assert_eq!(
                sample(&format!(
                    "zinder_compat_block_range_stream_refusals_total{{reason=\"{reason}\"}}"
                )),
                Some(expected),
                "unexpected refusal count for {reason}"
            );
        }
        for (outcome, count) in [("admitted", 1.0), ("refused", 4.0), ("cancelled", 1.0)] {
            assert_eq!(
                sample(&format!(
                    "zinder_compat_block_range_slot_wait_duration_seconds_count{{outcome=\"{outcome}\"}}"
                )),
                Some(count),
                "{outcome} requests must each record one slot-wait sample"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_single_chunk_stream_releases_its_slot_when_it_ends_before_drop() -> Result<(), Status>
    {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let open_streams =
            || rendered_sample(&handle.render(), "zinder_compat_block_range_streams_open");

        let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN).with_max_wait(Duration::ZERO);
        let mut response = limit
            .open()
            .await?
            .hold_for(vec![Ok::<u32, Status>(1), Ok(2)]);
        assert_eq!(open_streams(), Some(1.0));
        assert!(matches!(response.next().await, Some(Ok(1))));
        assert!(matches!(response.next().await, Some(Ok(2))));
        assert_eq!(open_streams(), Some(1.0));
        assert_unavailable(limit.open().await);

        assert!(response.next().await.is_none());
        assert_eq!(
            open_streams(),
            Some(0.0),
            "slot must release as soon as the stream yields None"
        );
        let reopened = limit.open().await?;
        assert_eq!(open_streams(), Some(1.0));
        // Polling again and dropping the finished wrapper must not touch the
        // new slot or double-decrement the gauge.
        assert!(response.next().await.is_none());
        drop(response);
        assert_eq!(open_streams(), Some(1.0));
        drop(reopened);
        assert_eq!(open_streams(), Some(0.0));
        Ok(())
    }

    #[tokio::test]
    async fn a_multi_chunk_stream_releases_its_slot_when_it_ends_before_drop() -> Result<(), Status>
    {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let open_streams =
            || rendered_sample(&handle.render(), "zinder_compat_block_range_streams_open");

        let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN).with_max_wait(Duration::ZERO);
        let mut response =
            limit
                .open()
                .await?
                .hold_with_reader(vec![Ok::<u32, Status>(1)], |sink| async move {
                    if let Some(room) = sink.room_for_chunk().await {
                        room.fill(vec![Ok(2)]);
                    }
                });
        assert_eq!(open_streams(), Some(1.0));
        assert!(matches!(response.next().await, Some(Ok(1))));
        assert!(matches!(response.next().await, Some(Ok(2))));
        assert!(response.next().await.is_none());

        assert_eq!(
            open_streams(),
            Some(0.0),
            "slot must release once the multi-chunk stream ends before drop"
        );
        let reopened = limit.open().await?;
        assert_eq!(open_streams(), Some(1.0));
        drop(response);
        assert_eq!(open_streams(), Some(1.0));
        drop(reopened);
        assert_eq!(open_streams(), Some(0.0));
        Ok(())
    }

    #[tokio::test]
    async fn a_stream_releases_its_slot_on_a_terminal_error_before_drop() -> Result<(), Status> {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let open_streams =
            || rendered_sample(&handle.render(), "zinder_compat_block_range_streams_open");
        let limit = BlockRangeStreamLimit::new(NonZeroU32::MIN).with_max_wait(Duration::ZERO);

        // Single-chunk stream ending in an error item (never polled to None).
        let mut single = limit.open().await?.hold_for(vec![
            Ok::<u32, Status>(1),
            Err(Status::aborted("reorg")),
            Ok(2),
        ]);
        assert!(matches!(single.next().await, Some(Ok(1))));
        assert_eq!(open_streams(), Some(1.0));
        assert_eq!(
            single.next().await.and_then(Result::err).map(|s| s.code()),
            Some(Code::Aborted)
        );
        assert_eq!(
            open_streams(),
            Some(0.0),
            "slot must release on Err without polling to None or dropping"
        );
        let reopened = limit.open().await?;
        assert!(single.next().await.is_none());
        drop(single);
        assert_eq!(open_streams(), Some(1.0));
        drop(reopened);
        assert_eq!(open_streams(), Some(0.0));

        // Multi-chunk stream whose reader sends a terminal error and exits.
        let mut chunked =
            limit
                .open()
                .await?
                .hold_with_reader(vec![Ok::<u32, Status>(1)], |sink| async move {
                    if let Some(room) = sink.room_for_chunk().await {
                        room.fill(vec![Err(Status::aborted("reorg"))]);
                    }
                });
        assert!(matches!(chunked.next().await, Some(Ok(1))));
        assert_eq!(
            chunked.next().await.and_then(Result::err).map(|s| s.code()),
            Some(Code::Aborted)
        );
        let_tasks_run().await;
        assert_eq!(open_streams(), Some(0.0));
        let reopened = limit.open().await?;
        assert!(chunked.next().await.is_none());
        drop(chunked);
        assert_eq!(open_streams(), Some(1.0));
        drop(reopened);
        assert_eq!(open_streams(), Some(0.0));
        Ok(())
    }

    /// Polls `stream` once with the task's context, so the poll spends the
    /// task's cooperative budget as tonic's poll does.
    async fn poll_once<S: Stream + Unpin>(stream: &mut S) -> Poll<Option<S::Item>> {
        std::future::poll_fn(|context| Poll::Ready(Pin::new(&mut *stream).poll_next(context))).await
    }

    /// Polls `stream` until it yields, letting the runtime run each time it
    /// returns `Pending`, and returns how many times it did.
    async fn next_counting_yields<S: Stream + Unpin>(stream: &mut S) -> (Option<S::Item>, usize) {
        let mut yields = 0;
        loop {
            match poll_once(stream).await {
                Poll::Ready(message) => return (message, yields),
                Poll::Pending => {
                    yields += 1;
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    /// A chunk of a stream with later chunks yields to the runtime as a
    /// one-chunk response does. Once every message of it has been taken, the
    /// response drops it before the reader hands on the next chunk.
    #[tokio::test]
    async fn a_chunk_yields_to_the_runtime_and_is_released_once_taken() {
        const FIRST_CHUNK_MESSAGES: usize = 1_000;
        let first_messages = (0..FIRST_CHUNK_MESSAGES).collect::<Vec<_>>();
        let (next_chunk, next_chunks) = mpsc::channel(1);
        let mut chunked_messages = ChunkedMessages {
            current: Some(super::ChunkIter::new(first_messages.clone())),
            next_chunks,
            cancel: super::CancellationToken::new(),
        };

        let mut sent = Vec::new();
        let mut first_chunk_yields = 0;
        while sent.len() < FIRST_CHUNK_MESSAGES {
            let (message, yields) = next_counting_yields(&mut chunked_messages).await;
            first_chunk_yields += yields;
            let Some(message) = message else {
                break;
            };
            sent.push(message);
        }
        assert_eq!(sent, first_messages);
        assert!(
            first_chunk_yields > 0,
            "the first chunk must yield to the runtime while it is sent"
        );

        // A poll past the chunk may first spend its turn yielding, so two
        // polls reach the empty hand-off.
        for _ in 0..2 {
            assert!(
                poll_once(&mut chunked_messages).await.is_pending(),
                "no later chunk has been handed on"
            );
        }
        assert!(
            chunked_messages.current.is_none(),
            "the response must drop a chunk once every message of it is taken"
        );

        assert!(next_chunk.try_send(vec![FIRST_CHUNK_MESSAGES]).is_ok());
        let (handed_on, _) = next_counting_yields(&mut chunked_messages).await;
        assert_eq!(handed_on, Some(FIRST_CHUNK_MESSAGES));
    }
}
