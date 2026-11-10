//! Ordered, connected block-segment fetching from the node source with
//! adaptive concurrency, segment sizing, and byte-budget backpressure.

use std::{
    collections::{BTreeMap, VecDeque},
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::{
    FutureExt,
    future::BoxFuture,
    stream::{self, FuturesUnordered, Stream, StreamExt},
};
use parking_lot::Mutex;
use tokio_util::task::AbortOnDropHandle;
use zinder_core::{BlockHeight, BlockId, ConsensusBranchId, NetworkUpgradeActivations};
use zinder_source::{
    NodeSource, SourceBlock, SourceChainCursor, SourceChainSegment, SourceChainSegmentLimits,
    SourceChainSegmentStats, SourceChainUpdate, SourceError,
};

use super::watermark::{ByteReservation, ByteWatermark, record_queue_depth, record_reorder_buffer};
use crate::chain_ingest::{IngestError, IngestRetryState, fetch_chain_segment_with_retry};

const CANONICAL_STAGE_SOURCE_FETCH: &str = "source_fetch";
const SOURCE_SEGMENT_DENSITY_SAMPLE_LIMIT: usize = 64;
const SOURCE_SEGMENT_GROW_AFTER_SUCCESS_COUNT: u32 = 8;
const SOURCE_SEGMENT_GROW_NUMERATOR: u32 = 5;
const SOURCE_SEGMENT_GROW_DENOMINATOR: u32 = 4;
const SOURCE_RESPONSE_RESERVATION_HEADROOM_NUMERATOR: u64 = 3;
const SOURCE_RESPONSE_RESERVATION_HEADROOM_DENOMINATOR: u64 = 2;

pub(crate) struct CanonicalSourceFetchConfig {
    pub(crate) request_timeout: Duration,
    pub(crate) history_predecessor: Option<BlockId>,
    pub(crate) from_height: BlockHeight,
    pub(crate) to_height: BlockHeight,
    pub(crate) max_response_bytes: NonZeroU64,
    pub(crate) target_response_payload_bytes: NonZeroU64,
    pub(crate) source_fetch_max_in_flight_requests: NonZeroU32,
    pub(crate) source_fetch_max_in_flight_bytes: NonZeroU64,
    pub(crate) source_segment_sizer: Arc<Mutex<SourceSegmentSizer>>,
}

struct SourceBlockStreamState<'a, Source> {
    source: &'a Source,
    request_timeout: Duration,
    history_predecessor: Option<BlockId>,
    from_height: BlockHeight,
    to_height: BlockHeight,
    source_segment_sizer: Arc<Mutex<SourceSegmentSizer>>,
    max_response_bytes: NonZeroU64,
    target_response_payload_bytes: NonZeroU64,
    source_fetch_max_in_flight_requests: NonZeroU32,
    source_fetch_watermark: ByteWatermark,
    completed_segment_bytes: u64,
    source_head_of_line_started_at: Option<Instant>,
    next_fetch_height: Option<BlockHeight>,
    next_emit_height: Option<BlockHeight>,
    in_flight_segments:
        FuturesUnordered<BoxFuture<'static, Result<PrefetchedSourceSegment, IngestError>>>,
    completed_segments: BTreeMap<BlockHeight, PrefetchedSourceSegment>,
    last_connected_block_id: Option<BlockId>,
}

pub(crate) fn build_source_block_stream<'a, Source>(
    source: &'a Source,
    config: CanonicalSourceFetchConfig,
) -> impl Stream<Item = Result<SourceBlockChunk, IngestError>> + Send + 'a
where
    Source: NodeSource + Clone + 'a,
{
    config.source_segment_sizer.lock().reset_probe_in_flight();
    let state = SourceBlockStreamState {
        source,
        request_timeout: config.request_timeout,
        history_predecessor: config.history_predecessor,
        from_height: config.from_height,
        to_height: config.to_height,
        source_segment_sizer: config.source_segment_sizer,
        max_response_bytes: config.max_response_bytes,
        target_response_payload_bytes: config.target_response_payload_bytes,
        source_fetch_max_in_flight_requests: config.source_fetch_max_in_flight_requests,
        source_fetch_watermark: ByteWatermark::new(
            CANONICAL_STAGE_SOURCE_FETCH,
            config.source_fetch_max_in_flight_bytes,
        ),
        completed_segment_bytes: 0,
        source_head_of_line_started_at: None,
        next_fetch_height: Some(config.from_height),
        next_emit_height: Some(config.from_height),
        in_flight_segments: FuturesUnordered::new(),
        completed_segments: BTreeMap::new(),
        last_connected_block_id: config.history_predecessor,
    };

    stream::unfold(state, |mut state| async move {
        let next_chunk = next_source_block_chunk(&mut state).await;
        next_chunk.map(|chunk_result| (chunk_result, state))
    })
}

async fn next_source_block_chunk<Source>(
    state: &mut SourceBlockStreamState<'_, Source>,
) -> Option<Result<SourceBlockChunk, IngestError>>
where
    Source: NodeSource + Clone,
{
    loop {
        fill_source_segment_prefetch_queue(state);
        if let Some(prefetched_segment) = pop_next_completed_source_segment(state) {
            let PrefetchedSourceSegment {
                segment,
                feedback_action,
                reservation,
                ..
            } = prefetched_segment;
            if segment.is_empty() {
                return None;
            }

            let source_block_chunk = match validated_source_block_chunk(state, segment, reservation)
            {
                Ok(source_block_chunk) => source_block_chunk,
                Err(error) => return Some(Err(error)),
            };
            if let Some(action) = feedback_action {
                apply_source_segment_feedback_action(state, action);
            }
            return Some(Ok(source_block_chunk));
        }
        record_source_head_of_line_wait_started(state);

        let mut prefetched_segment = match state.in_flight_segments.next().await {
            Some(Ok(prefetched_segment)) => prefetched_segment,
            Some(Err(error)) => {
                return Some(Err(error));
            }
            None => return None,
        };
        prefetched_segment.feedback_action = state.source_segment_sizer.lock().record_segment(
            prefetched_segment.start_height,
            prefetched_segment.segment.stats(),
        );
        if let Err(error) = insert_completed_source_segment(state, prefetched_segment) {
            return Some(Err(error));
        }
        record_source_fetch_queue_state(state);
    }
}

fn fill_source_segment_prefetch_queue<'a, Source>(state: &mut SourceBlockStreamState<'a, Source>)
where
    Source: NodeSource + Clone + 'a,
{
    while state.in_flight_segments.len()
        < nonzero_u32_to_usize(state.source_fetch_max_in_flight_requests)
    {
        let Some(next_height) = state.next_fetch_height else {
            break;
        };
        let segment_plan = state.source_segment_sizer.lock().plan_segment(
            next_height,
            state.to_height,
            state.max_response_bytes,
        );
        let (source_segment_max_blocks, reserved_response_bytes) = match segment_plan {
            SourceSegmentPlan::Ready {
                max_blocks,
                reserved_response_bytes,
            } => (max_blocks, reserved_response_bytes),
            SourceSegmentPlan::AwaitingFeedback => break,
            SourceSegmentPlan::Complete => {
                state.next_fetch_height = None;
                break;
            }
        };

        let cursor = if next_height == state.from_height
            && let Some(history_predecessor) = state.history_predecessor
        {
            SourceChainCursor::at_block(history_predecessor)
        } else {
            SourceChainCursor::before_height(next_height)
        };
        let Some(reservation) = state
            .source_fetch_watermark
            .try_reserve(reserved_response_bytes.get())
        else {
            break;
        };
        metrics::gauge!("zinder_ingest_source_fetch_request_reservation_bytes")
            .set(u64_to_f64(reserved_response_bytes.get()));
        metrics::histogram!("zinder_ingest_source_segment_reserved_response_bytes")
            .record(u64_to_f64(reserved_response_bytes.get()));
        state
            .source_segment_sizer
            .lock()
            .record_segment_scheduled(next_height);
        state
            .in_flight_segments
            .push(fetch_prefetched_chain_segment(SourceFetchRequest {
                request_timeout: state.request_timeout,
                source: state.source.clone(),
                start_height: next_height,
                cursor,
                max_connected_blocks: source_segment_max_blocks,
                target_response_bytes: state.target_response_payload_bytes,
                max_response_bytes: state.max_response_bytes,
                reserved_response_bytes: reserved_response_bytes.get(),
                reservation,
            }));
        record_source_fetch_queue_state(state);
        state.next_fetch_height = next_height_after_segment(next_height, source_segment_max_blocks)
            .filter(|height| *height <= state.to_height);
    }
}

struct SourceFetchRequest<Source> {
    request_timeout: Duration,
    source: Source,
    start_height: BlockHeight,
    cursor: SourceChainCursor,
    max_connected_blocks: NonZeroU32,
    target_response_bytes: NonZeroU64,
    max_response_bytes: NonZeroU64,
    reserved_response_bytes: u64,
    reservation: ByteReservation,
}

struct PrefetchedSourceSegment {
    start_height: BlockHeight,
    max_connected_blocks: NonZeroU32,
    segment: SourceChainSegment,
    queued_response_bytes: u64,
    feedback_action: Option<SourceSegmentFeedbackAction>,
    reservation: ByteReservation,
}

fn fetch_prefetched_chain_segment<Source>(
    request: SourceFetchRequest<Source>,
) -> BoxFuture<'static, Result<PrefetchedSourceSegment, IngestError>>
where
    Source: NodeSource + Clone,
{
    let request_timeout = request.request_timeout;
    let source = request.source;
    let start_height = request.start_height;
    let cursor = request.cursor;
    let max_connected_blocks = request.max_connected_blocks;
    let target_response_bytes = request.target_response_bytes;
    let max_response_bytes = request.max_response_bytes;
    let reserved_response_bytes = request.reserved_response_bytes;
    let reservation = request.reservation;

    // Spawned so segment fetches and block decoding progress on runtime
    // workers instead of only while the segment stream itself is polled.
    let fetch_task = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut retry_state = IngestRetryState::default();
        let limits = SourceChainSegmentLimits::new(
            cursor,
            max_connected_blocks,
            target_response_bytes.get(),
            max_response_bytes.get(),
        );
        let segment =
            fetch_chain_segment_with_retry(request_timeout, &source, limits, &mut retry_state)
                .await?;
        let queued_response_bytes = queued_source_segment_bytes(&segment, reserved_response_bytes);
        if queued_response_bytes > reserved_response_bytes {
            metrics::counter!("zinder_ingest_source_segment_reservation_undersized_total")
                .increment(1);
        }
        let mut reservation = reservation;
        reservation.resize(queued_response_bytes);
        Ok(PrefetchedSourceSegment {
            start_height,
            max_connected_blocks,
            segment,
            queued_response_bytes,
            feedback_action: None,
            reservation,
        })
    }));

    async move {
        match fetch_task.await {
            Ok(segment_result) => segment_result,
            Err(join_error) => Err(IngestError::SourceSegmentFetchTaskStopped {
                reason: join_error.to_string(),
            }),
        }
    }
    .boxed()
}

fn queued_source_segment_bytes(segment: &SourceChainSegment, reserved_response_bytes: u64) -> u64 {
    let measured_response_bytes = segment.stats().response_payload_bytes();
    if measured_response_bytes == 0 && !segment.is_empty() {
        reserved_response_bytes
    } else {
        measured_response_bytes
    }
}

fn pop_next_completed_source_segment<Source>(
    state: &mut SourceBlockStreamState<'_, Source>,
) -> Option<PrefetchedSourceSegment> {
    let next_emit_height = state.next_emit_height?;
    let prefetched_segment = state.completed_segments.remove(&next_emit_height)?;
    state.completed_segment_bytes = state
        .completed_segment_bytes
        .saturating_sub(prefetched_segment.queued_response_bytes);
    state.next_emit_height =
        next_height_after_segment(next_emit_height, prefetched_segment.max_connected_blocks)
            .filter(|height| *height <= state.to_height);
    record_source_head_of_line_wait_completed(state);
    record_source_fetch_queue_state(state);
    Some(prefetched_segment)
}

fn insert_completed_source_segment<Source>(
    state: &mut SourceBlockStreamState<'_, Source>,
    prefetched_segment: PrefetchedSourceSegment,
) -> Result<(), IngestError> {
    if prefetched_segment.start_height < state.from_height
        || prefetched_segment.start_height > state.to_height
    {
        return Err(IngestError::from(SourceError::SourceProtocolMismatch {
            reason: "source chain segment completed outside the requested construction range",
        }));
    }
    let queued_response_bytes = prefetched_segment.queued_response_bytes;
    if state
        .completed_segments
        .insert(prefetched_segment.start_height, prefetched_segment)
        .is_some()
    {
        return Err(IngestError::from(SourceError::SourceProtocolMismatch {
            reason: "source chain segment completed twice during canonical construction",
        }));
    }
    state.completed_segment_bytes = state
        .completed_segment_bytes
        .saturating_add(queued_response_bytes);
    Ok(())
}

/// Applies completed segment feedback after the head range is emitted in order.
///
/// Feedback changes only the unscheduled frontier.
///
/// The source adapter splits and retries an oversized request before returning
/// its completed segment. Future ranges were requested from disjoint cursors,
/// so they remain valid after the sizer shrinks. Retaining them also retains
/// their byte reservations until ordered emission.
fn apply_source_segment_feedback_action<Source>(
    state: &SourceBlockStreamState<'_, Source>,
    action: SourceSegmentFeedbackAction,
) {
    retain_future_source_prefetch(state, action.metric_label());
    if action == SourceSegmentFeedbackAction::ResponseTooLarge {
        record_zero_disjoint_prefetch_work();
    }
}

fn retain_future_source_prefetch<Source>(
    state: &SourceBlockStreamState<'_, Source>,
    reason: &'static str,
) {
    // Retained ranges stay bounded by their live reservations and are still
    // parent-link validated only when emitted in height order.
    let retained = source_prefetch_queue_accounting(state);
    metrics::counter!(
        "zinder_ingest_source_segment_prefetch_retained_completed_segments_total",
        "reason" => reason
    )
    .increment(retained.completed_segment_count);
    metrics::counter!(
        "zinder_ingest_source_segment_prefetch_retained_in_flight_segments_total",
        "reason" => reason
    )
    .increment(retained.in_flight_segment_count);
    metrics::counter!(
        "zinder_ingest_source_segment_prefetch_retained_completed_response_bytes_total",
        "reason" => reason
    )
    .increment(retained.completed_response_bytes);
    tracing::info!(
        event = "source_segment_prefetch_retained",
        reason,
        next_fetch_height = ?state.next_fetch_height,
        retained_in_flight_segments = retained.in_flight_segment_count,
        retained_completed_segments = retained.completed_segment_count,
        retained_completed_response_bytes = retained.completed_response_bytes,
        "retained bounded source prefetch after source segment size adjustment"
    );
}

fn record_zero_disjoint_prefetch_work() {
    metrics::counter!(
        "zinder_ingest_source_segment_disjoint_prefetch_discarded_completed_segments_total",
        "reason" => "response_too_large"
    )
    .increment(0);
    metrics::counter!(
        "zinder_ingest_source_segment_disjoint_prefetch_discarded_in_flight_segments_total",
        "reason" => "response_too_large"
    )
    .increment(0);
    metrics::counter!(
        "zinder_ingest_source_segment_disjoint_prefetch_discarded_completed_response_bytes_total",
        "reason" => "response_too_large"
    )
    .increment(0);
    metrics::counter!(
        "zinder_ingest_source_segment_disjoint_prefetch_refetched_blocks_total",
        "reason" => "response_too_large"
    )
    .increment(0);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SourcePrefetchQueueAccounting {
    completed_segment_count: u64,
    in_flight_segment_count: u64,
    completed_response_bytes: u64,
}

fn source_prefetch_queue_accounting<Source>(
    state: &SourceBlockStreamState<'_, Source>,
) -> SourcePrefetchQueueAccounting {
    SourcePrefetchQueueAccounting {
        completed_segment_count: usize_to_u64_saturating(state.completed_segments.len()),
        in_flight_segment_count: usize_to_u64_saturating(state.in_flight_segments.len()),
        completed_response_bytes: state.completed_segment_bytes,
    }
}

fn record_source_fetch_queue_state<Source>(state: &SourceBlockStreamState<'_, Source>) {
    let source_fetch_snapshot = state.source_fetch_watermark.snapshot();
    metrics::gauge!("zinder_ingest_source_fetch_queue_requests").set(f64::from(
        usize_to_u32_saturating(state.in_flight_segments.len()),
    ));
    metrics::gauge!("zinder_ingest_source_fetch_queue_bytes")
        .set(u64_to_f64(source_fetch_snapshot.reserved_bytes));
    metrics::gauge!("zinder_ingest_source_segment_reassembly_segments").set(f64::from(
        usize_to_u32_saturating(state.completed_segments.len()),
    ));
    metrics::gauge!("zinder_ingest_source_segment_reassembly_bytes")
        .set(u64_to_f64(state.completed_segment_bytes));
    record_queue_depth(CANONICAL_STAGE_SOURCE_FETCH, state.in_flight_segments.len());
    record_reorder_buffer(
        CANONICAL_STAGE_SOURCE_FETCH,
        state.completed_segments.len(),
        state.completed_segment_bytes,
    );
}

fn record_source_head_of_line_wait_started<Source>(state: &mut SourceBlockStreamState<'_, Source>) {
    if state.completed_segments.is_empty() {
        state.source_head_of_line_started_at = None;
        return;
    }
    if state.source_head_of_line_started_at.is_none() {
        state.source_head_of_line_started_at = Some(Instant::now());
    }
}

fn record_source_head_of_line_wait_completed<Source>(
    state: &mut SourceBlockStreamState<'_, Source>,
) {
    let Some(started_at) = state.source_head_of_line_started_at.take() else {
        return;
    };
    metrics::histogram!(
        "zinder_ingest_bulk_pipeline_head_of_line_wait_seconds",
        "stage" => CANONICAL_STAGE_SOURCE_FETCH
    )
    .record(started_at.elapsed());
}

fn validated_source_block_chunk<Source>(
    state: &mut SourceBlockStreamState<'_, Source>,
    segment: SourceChainSegment,
    reservation: ByteReservation,
) -> Result<SourceBlockChunk, IngestError>
where
    Source: NodeSource,
{
    let mut connected_blocks = VecDeque::new();
    for update in segment.into_updates() {
        match update {
            SourceChainUpdate::ConnectedBlock { block, .. } => {
                validate_prefetched_block_link(state.last_connected_block_id, &block)?;
                state.last_connected_block_id = Some(BlockId::new(block.height, block.hash));
                if block.height <= state.to_height {
                    connected_blocks.push_back(block);
                }
            }
            SourceChainUpdate::RevertedBlock { block_id, .. } => {
                return Err(IngestError::from(SourceError::BlockReorgDuringFetch {
                    height: block_id.height,
                    reason: "source chain segment reverted during canonical construction",
                }));
            }
            SourceChainUpdate::SettledTip { .. } => {}
        }
    }

    if connected_blocks.is_empty() {
        return Err(IngestError::from(SourceError::SourceProtocolMismatch {
            reason: "source chain segment did not contain connected blocks during canonical construction",
        }));
    }
    Ok(SourceBlockChunk {
        blocks: connected_blocks,
        _reservation: reservation,
    })
}

pub(crate) struct SourceBlockChunk {
    blocks: VecDeque<SourceBlock>,
    _reservation: ByteReservation,
}

impl SourceBlockChunk {
    pub(crate) fn front(&self) -> Option<&SourceBlock> {
        self.blocks.front()
    }

    pub(crate) fn pop_front(&mut self) -> Option<SourceBlock> {
        self.blocks.pop_front()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

fn validate_prefetched_block_link(
    previous_block_id: Option<BlockId>,
    block: &SourceBlock,
) -> Result<(), IngestError> {
    let Some(previous_block_id) = previous_block_id else {
        return Ok(());
    };
    let Some(expected_height) = previous_block_id.height.next() else {
        return Err(IngestError::from(SourceError::SourceProtocolMismatch {
            reason: "source chain segment continued after maximum block height",
        }));
    };
    if block.height != expected_height {
        return Err(IngestError::from(SourceError::SourceProtocolMismatch {
            reason: "source chain segment skipped a height during canonical construction",
        }));
    }
    if block.parent_hash != previous_block_id.hash {
        return Err(IngestError::from(SourceError::BlockReorgDuringFetch {
            height: block.height,
            reason: "prefetched source chain segment did not connect to the previous segment",
        }));
    }
    Ok(())
}

fn next_height_after_segment(
    start_height: BlockHeight,
    source_segment_max_blocks: NonZeroU32,
) -> Option<BlockHeight> {
    start_height
        .value()
        .checked_add(source_segment_max_blocks.get())
        .map(BlockHeight::new)
}

#[derive(Clone, Copy)]
struct SourceSegmentDensitySample {
    response_payload_bytes: u64,
    connected_blocks: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceSegmentPlan {
    Ready {
        max_blocks: NonZeroU32,
        reserved_response_bytes: NonZeroU64,
    },
    AwaitingFeedback,
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceSegmentFeedbackAction {
    DensityShrink,
    ResponseTooLarge,
}

impl SourceSegmentFeedbackAction {
    const fn metric_label(self) -> &'static str {
        match self {
            Self::ResponseTooLarge => "response_too_large",
            Self::DensityShrink => "density",
        }
    }
}

pub(crate) struct SourceSegmentSizer {
    max_blocks: NonZeroU32,
    target_response_payload_bytes: NonZeroU64,
    current_blocks: NonZeroU32,
    success_count: u32,
    overshoot_clear_success_count: u32,
    overshoot_bytes_per_block: Option<u64>,
    active_branch_id: ConsensusBranchId,
    initial_probe_in_flight: bool,
    activations: Arc<NetworkUpgradeActivations>,
    density_samples: VecDeque<SourceSegmentDensitySample>,
}

impl SourceSegmentSizer {
    pub(crate) fn new(
        max_blocks: NonZeroU32,
        target_response_payload_bytes: NonZeroU64,
        activations: Arc<NetworkUpgradeActivations>,
        from_height: BlockHeight,
    ) -> Self {
        let current_blocks = max_blocks;
        let active_branch_id = activations.consensus_branch_id_at(from_height);
        record_source_segment_sizer_state(
            current_blocks,
            max_blocks,
            target_response_payload_bytes,
        );
        Self {
            max_blocks,
            target_response_payload_bytes,
            current_blocks,
            success_count: 0,
            overshoot_clear_success_count: 0,
            overshoot_bytes_per_block: None,
            active_branch_id,
            initial_probe_in_flight: false,
            activations,
            density_samples: VecDeque::new(),
        }
    }

    pub(crate) fn blocks_for_remaining_range(
        &mut self,
        next_height: BlockHeight,
        to_height: BlockHeight,
    ) -> Option<NonZeroU32> {
        if next_height > to_height {
            return None;
        }
        self.reset_after_network_upgrade_if_needed(next_height);
        let remaining_blocks = to_height
            .value()
            .saturating_sub(next_height.value())
            .saturating_add(1);
        NonZeroU32::new(remaining_blocks.min(self.current_blocks.get()))
    }

    pub(crate) fn plan_segment(
        &mut self,
        next_height: BlockHeight,
        to_height: BlockHeight,
        max_response_bytes: NonZeroU64,
    ) -> SourceSegmentPlan {
        let Some(max_blocks) = self.blocks_for_remaining_range(next_height, to_height) else {
            return SourceSegmentPlan::Complete;
        };
        if self.density_samples.is_empty() && self.initial_probe_in_flight {
            return SourceSegmentPlan::AwaitingFeedback;
        }
        SourceSegmentPlan::Ready {
            max_blocks,
            reserved_response_bytes: self.reserved_response_bytes(max_blocks, max_response_bytes),
        }
    }

    fn reserved_response_bytes(
        &self,
        max_blocks: NonZeroU32,
        max_response_bytes: NonZeroU64,
    ) -> NonZeroU64 {
        let Some(bytes_per_block) = self.estimated_response_payload_bytes_per_block() else {
            return max_response_bytes;
        };
        let estimated_response_bytes = bytes_per_block.saturating_mul(u64::from(max_blocks.get()));
        let response_bytes_with_headroom = estimated_response_bytes
            .saturating_mul(SOURCE_RESPONSE_RESERVATION_HEADROOM_NUMERATOR)
            .saturating_add(SOURCE_RESPONSE_RESERVATION_HEADROOM_DENOMINATOR - 1)
            .saturating_div(SOURCE_RESPONSE_RESERVATION_HEADROOM_DENOMINATOR);
        let reserved_response_bytes = response_bytes_with_headroom
            .max(self.target_response_payload_bytes.get())
            .min(max_response_bytes.get());
        NonZeroU64::new(reserved_response_bytes).unwrap_or(max_response_bytes)
    }

    pub(crate) fn record_segment_scheduled(&mut self, start_height: BlockHeight) {
        if self.activations.consensus_branch_id_at(start_height) != self.active_branch_id
            || !self.density_samples.is_empty()
        {
            return;
        }
        self.initial_probe_in_flight = true;
        metrics::gauge!("zinder_ingest_source_segment_initial_probe_in_flight").set(1.0);
    }

    fn reset_probe_in_flight(&mut self) {
        self.initial_probe_in_flight = false;
        metrics::gauge!("zinder_ingest_source_segment_initial_probe_in_flight").set(0.0);
    }

    pub(crate) fn record_segment(
        &mut self,
        start_height: BlockHeight,
        stats: SourceChainSegmentStats,
    ) -> Option<SourceSegmentFeedbackAction> {
        if self.activations.consensus_branch_id_at(start_height) != self.active_branch_id {
            return None;
        }
        self.initial_probe_in_flight = false;
        metrics::gauge!("zinder_ingest_source_segment_initial_probe_in_flight").set(0.0);
        self.record_density_sample(stats);
        self.record_overshoot_memory(stats);
        let previous_blocks = self.current_blocks;
        let next_blocks = if stats.split_count() > 0 {
            self.shrunk_blocks_after_overshoot()
        } else {
            self.blocks_after_success()
        };
        self.current_blocks = next_blocks;
        record_source_segment_sizer_state(
            self.current_blocks,
            self.max_blocks,
            self.target_response_payload_bytes,
        );
        let feedback_action = if self.current_blocks < previous_blocks {
            if stats.split_count() > 0 {
                Some(SourceSegmentFeedbackAction::ResponseTooLarge)
            } else {
                Some(SourceSegmentFeedbackAction::DensityShrink)
            }
        } else {
            None
        };
        if previous_blocks != self.current_blocks {
            let reason =
                feedback_action.map_or("success", |feedback_action| feedback_action.metric_label());
            record_source_segment_sizer_adjustment(reason, previous_blocks, self.current_blocks);
        }
        feedback_action
    }

    fn record_density_sample(&mut self, stats: SourceChainSegmentStats) {
        if stats.connected_blocks() == 0 || stats.response_payload_bytes() == 0 {
            return;
        }
        self.density_samples.push_back(SourceSegmentDensitySample {
            response_payload_bytes: stats.response_payload_bytes(),
            connected_blocks: stats.connected_blocks(),
        });
        while self.density_samples.len() > SOURCE_SEGMENT_DENSITY_SAMPLE_LIMIT {
            self.density_samples.pop_front();
        }
    }

    fn shrunk_blocks_after_overshoot(&mut self) -> NonZeroU32 {
        self.success_count = 0;
        self.overshoot_clear_success_count = 0;
        let halved = self.current_blocks.get().saturating_div(2).max(1);
        nonzero_u32(
            self.blocks_allowed_by_density()
                .map_or(halved, |blocks| halved.min(blocks.get()))
                .max(1),
        )
    }

    fn blocks_after_success(&mut self) -> NonZeroU32 {
        let density_blocks = self.blocks_allowed_by_density();
        if let Some(density_blocks) = density_blocks
            && density_blocks < self.current_blocks
        {
            self.success_count = 0;
            return density_blocks;
        }

        self.success_count = self.success_count.saturating_add(1);
        if self.success_count < SOURCE_SEGMENT_GROW_AFTER_SUCCESS_COUNT {
            return self.current_blocks;
        }

        self.success_count = 0;
        let grown = self
            .current_blocks
            .get()
            .saturating_mul(SOURCE_SEGMENT_GROW_NUMERATOR)
            .saturating_div(SOURCE_SEGMENT_GROW_DENOMINATOR)
            .max(self.current_blocks.get().saturating_add(1));
        let capped = grown.min(self.max_blocks.get());
        nonzero_u32(density_blocks.map_or(capped, |blocks| capped.min(blocks.get())))
    }

    fn blocks_allowed_by_density(&self) -> Option<NonZeroU32> {
        let bytes_per_block = self.estimated_response_payload_bytes_per_block()?;
        let target_blocks = self
            .target_response_payload_bytes
            .get()
            .saturating_div(bytes_per_block)
            .max(1)
            .min(u64::from(self.max_blocks.get()));
        Some(nonzero_u32(
            u32::try_from(target_blocks).unwrap_or(u32::MAX),
        ))
    }

    fn estimated_response_payload_bytes_per_block(&self) -> Option<u64> {
        let p95 = self.p95_response_payload_bytes_per_block();
        match (p95, self.overshoot_bytes_per_block) {
            (Some(p95), Some(overshoot)) => Some(p95.max(overshoot)),
            (Some(p95), None) => Some(p95),
            (None, Some(overshoot)) => Some(overshoot),
            (None, None) => None,
        }
    }

    fn p95_response_payload_bytes_per_block(&self) -> Option<u64> {
        let mut samples = self
            .density_samples
            .iter()
            .map(|sample| {
                let blocks = u64::from(sample.connected_blocks);
                sample
                    .response_payload_bytes
                    .saturating_add(blocks.saturating_sub(1))
                    / blocks
            })
            .collect::<Vec<_>>();
        if samples.is_empty() {
            return None;
        }
        samples.sort_unstable();
        let percentile_index = samples
            .len()
            .saturating_mul(95)
            .saturating_add(99)
            .saturating_div(100)
            .saturating_sub(1);
        samples.get(percentile_index).copied()
    }

    fn record_overshoot_memory(&mut self, stats: SourceChainSegmentStats) {
        if stats.split_count() > 0 {
            let Some(bytes_per_block) = response_payload_bytes_per_block(stats) else {
                return;
            };
            self.overshoot_bytes_per_block = Some(
                self.overshoot_bytes_per_block
                    .map_or(bytes_per_block, |current| current.max(bytes_per_block)),
            );
            self.overshoot_clear_success_count = 0;
            return;
        }

        if stats.response_payload_bytes() > self.target_response_payload_bytes.get() {
            self.overshoot_clear_success_count = 0;
            return;
        }
        self.overshoot_clear_success_count = self.overshoot_clear_success_count.saturating_add(1);
        if self.overshoot_clear_success_count >= SOURCE_SEGMENT_GROW_AFTER_SUCCESS_COUNT {
            self.overshoot_bytes_per_block = None;
            self.overshoot_clear_success_count = 0;
        }
    }

    fn reset_after_network_upgrade_if_needed(&mut self, next_height: BlockHeight) {
        let active_branch_id = self.activations.consensus_branch_id_at(next_height);
        if active_branch_id == self.active_branch_id {
            return;
        }
        let previous_blocks = self.current_blocks;
        self.active_branch_id = active_branch_id;
        self.current_blocks = self.max_blocks;
        self.success_count = 0;
        self.overshoot_clear_success_count = 0;
        self.overshoot_bytes_per_block = None;
        self.initial_probe_in_flight = false;
        self.density_samples.clear();
        metrics::gauge!("zinder_ingest_source_segment_initial_probe_in_flight").set(0.0);
        record_source_segment_sizer_state(
            self.current_blocks,
            self.max_blocks,
            self.target_response_payload_bytes,
        );
        record_source_segment_sizer_adjustment(
            "network_upgrade",
            previous_blocks,
            self.current_blocks,
        );
    }
}

fn response_payload_bytes_per_block(stats: SourceChainSegmentStats) -> Option<u64> {
    if stats.connected_blocks() == 0 || stats.response_payload_bytes() == 0 {
        return None;
    }
    let blocks = u64::from(stats.connected_blocks());
    Some(
        stats
            .response_payload_bytes()
            .saturating_add(blocks.saturating_sub(1))
            / blocks,
    )
}

fn nonzero_u32(amount: u32) -> NonZeroU32 {
    NonZeroU32::new(amount.max(1)).unwrap_or(NonZeroU32::MIN)
}

fn nonzero_u32_to_usize(amount: NonZeroU32) -> usize {
    usize::try_from(amount.get()).unwrap_or(usize::MAX)
}

fn usize_to_u32_saturating(amount: usize) -> u32 {
    u32::try_from(amount).unwrap_or(u32::MAX)
}

fn usize_to_u64_saturating(amount: usize) -> u64 {
    u64::try_from(amount).unwrap_or(u64::MAX)
}

fn record_source_segment_sizer_state(
    current_blocks: NonZeroU32,
    max_blocks: NonZeroU32,
    target_response_payload_bytes: NonZeroU64,
) {
    metrics::gauge!("zinder_ingest_source_segment_next_blocks")
        .set(f64::from(current_blocks.get()));
    metrics::gauge!("zinder_ingest_source_segment_max_blocks").set(f64::from(max_blocks.get()));
    metrics::gauge!("zinder_ingest_source_segment_target_response_payload_bytes")
        .set(u64_to_f64(target_response_payload_bytes.get()));
}

fn record_source_segment_sizer_adjustment(
    reason: &'static str,
    previous_blocks: NonZeroU32,
    current_blocks: NonZeroU32,
) {
    if previous_blocks == current_blocks && reason != "network_upgrade" {
        return;
    }
    metrics::counter!(
        "zinder_ingest_source_segment_sizing_adjustment_total",
        "reason" => reason
    )
    .increment(1);
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus gauges and histograms use f64 samples; byte counts are diagnostic magnitudes"
)]
fn u64_to_f64(sample: u64) -> f64 {
    sample as f64
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        num::{NonZeroU32, NonZeroU64},
        sync::Arc,
    };

    use futures_util::{FutureExt, future, stream::FuturesUnordered};
    use parking_lot::Mutex;
    use zinder_core::{BlockHeight, Network, NetworkUpgradeActivations};
    use zinder_source::{SourceChainSegment, SourceChainSegmentStats};

    use super::{
        CANONICAL_STAGE_SOURCE_FETCH, PrefetchedSourceSegment, SourceBlockStreamState,
        SourceSegmentFeedbackAction, SourceSegmentSizer, apply_source_segment_feedback_action,
        source_prefetch_queue_accounting,
    };
    use crate::writer::construction::watermark::ByteWatermark;

    #[test]
    fn response_too_large_feedback_retains_disjoint_future_source_prefetch()
    -> Result<(), Box<dyn std::error::Error>> {
        let state = test_source_prefetch_state()?;
        let accounting = source_prefetch_queue_accounting(&state);

        assert_eq!(accounting.completed_segment_count, 2);
        assert_eq!(accounting.in_flight_segment_count, 1);
        assert_eq!(accounting.completed_response_bytes, 30);
        let watermark_before_feedback = state.source_fetch_watermark.snapshot();
        assert_eq!(watermark_before_feedback.reserved_bytes, 70);
        assert_eq!(watermark_before_feedback.reservations, 3);
        apply_source_segment_feedback_action(&state, SourceSegmentFeedbackAction::ResponseTooLarge);
        let retained = source_prefetch_queue_accounting(&state);
        assert_eq!(retained.completed_segment_count, 2);
        assert_eq!(retained.in_flight_segment_count, 1);
        assert_eq!(retained.completed_response_bytes, 30);
        assert_eq!(state.completed_segment_bytes, 30);
        assert_eq!(state.next_emit_height, Some(BlockHeight::new(3)));
        assert_eq!(state.next_fetch_height, Some(BlockHeight::new(5)));
        assert_eq!(
            state.source_fetch_watermark.snapshot(),
            watermark_before_feedback
        );
        Ok(())
    }

    #[test]
    fn density_feedback_retains_future_source_prefetch() -> Result<(), Box<dyn std::error::Error>> {
        let state = test_source_prefetch_state()?;

        apply_source_segment_feedback_action(&state, SourceSegmentFeedbackAction::DensityShrink);

        let accounting = source_prefetch_queue_accounting(&state);
        assert_eq!(accounting.completed_segment_count, 2);
        assert_eq!(accounting.in_flight_segment_count, 1);
        assert_eq!(accounting.completed_response_bytes, 30);
        assert_eq!(state.completed_segment_bytes, 30);
        assert_eq!(state.next_emit_height, Some(BlockHeight::new(3)));
        assert_eq!(state.next_fetch_height, Some(BlockHeight::new(5)));
        Ok(())
    }

    #[test]
    fn segment_sizer_distinguishes_density_retention_from_oversized_restart() {
        let mut density_sizer = SourceSegmentSizer::new(
            NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
            NonZeroU64::new(32).unwrap_or(NonZeroU64::MIN),
            Arc::new(NetworkUpgradeActivations::empty(Network::ZcashRegtest)),
            BlockHeight::new(1),
        );
        let dense_stats =
            SourceChainSegmentStats::from_response_payload_bytes(128).with_connected_blocks(2);
        assert_eq!(
            density_sizer.record_segment(BlockHeight::new(1), dense_stats),
            Some(SourceSegmentFeedbackAction::DensityShrink)
        );

        let mut oversized_sizer = SourceSegmentSizer::new(
            NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
            NonZeroU64::new(32).unwrap_or(NonZeroU64::MIN),
            Arc::new(NetworkUpgradeActivations::empty(Network::ZcashRegtest)),
            BlockHeight::new(1),
        );
        let oversized_stats = dense_stats.with_added_splits(1);
        assert_eq!(
            oversized_sizer.record_segment(BlockHeight::new(1), oversized_stats),
            Some(SourceSegmentFeedbackAction::ResponseTooLarge)
        );
    }

    fn test_source_prefetch_state()
    -> Result<SourceBlockStreamState<'static, ()>, Box<dyn std::error::Error>> {
        static SOURCE: () = ();
        let source_fetch_watermark = ByteWatermark::new(
            CANONICAL_STAGE_SOURCE_FETCH,
            NonZeroU64::new(100).unwrap_or(NonZeroU64::MIN),
        );
        let first_reservation = source_fetch_watermark
            .try_reserve(10)
            .ok_or("first test reservation should fit")?;
        let second_reservation = source_fetch_watermark
            .try_reserve(20)
            .ok_or("second test reservation should fit")?;
        let in_flight_reservation = source_fetch_watermark
            .try_reserve(40)
            .ok_or("in-flight test reservation should fit")?;
        let mut completed_segments = BTreeMap::new();
        completed_segments.insert(
            BlockHeight::new(3),
            PrefetchedSourceSegment {
                start_height: BlockHeight::new(3),
                max_connected_blocks: NonZeroU32::MIN,
                segment: SourceChainSegment::default(),
                queued_response_bytes: 10,
                feedback_action: None,
                reservation: first_reservation,
            },
        );
        completed_segments.insert(
            BlockHeight::new(4),
            PrefetchedSourceSegment {
                start_height: BlockHeight::new(4),
                max_connected_blocks: NonZeroU32::MIN,
                segment: SourceChainSegment::default(),
                queued_response_bytes: 20,
                feedback_action: None,
                reservation: second_reservation,
            },
        );
        let in_flight_segments = FuturesUnordered::new();
        in_flight_segments.push(
            async move {
                let _reservation = in_flight_reservation;
                future::pending::<Result<PrefetchedSourceSegment, crate::IngestError>>().await
            }
            .boxed(),
        );
        Ok(SourceBlockStreamState {
            source: &SOURCE,
            request_timeout: std::time::Duration::from_secs(1),
            history_predecessor: None,
            from_height: BlockHeight::new(1),
            to_height: BlockHeight::new(8),
            source_segment_sizer: Arc::new(Mutex::new(SourceSegmentSizer::new(
                NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
                NonZeroU64::new(32).unwrap_or(NonZeroU64::MIN),
                Arc::new(NetworkUpgradeActivations::empty(Network::ZcashRegtest)),
                BlockHeight::new(1),
            ))),
            max_response_bytes: NonZeroU64::new(64).unwrap_or(NonZeroU64::MIN),
            target_response_payload_bytes: NonZeroU64::new(32).unwrap_or(NonZeroU64::MIN),
            source_fetch_max_in_flight_requests: NonZeroU32::new(4).unwrap_or(NonZeroU32::MIN),
            source_fetch_watermark,
            completed_segment_bytes: 30,
            source_head_of_line_started_at: None,
            next_fetch_height: Some(BlockHeight::new(5)),
            next_emit_height: Some(BlockHeight::new(3)),
            in_flight_segments,
            completed_segments,
            last_connected_block_id: None,
        })
    }
}
