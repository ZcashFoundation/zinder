//! Direct canonical compact-block wire-frame streaming across `ZSHRNG02`
//! settled shielded segments and `live_tail_block` (`ZSLVTL01`).

use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use zinder_core::{BlockHash, BlockHeight, BlockHeightRange, wire::encode_internal_block_hash};
use zinder_proto::compat::lightwalletd::{EncodedBlock, EncodedBlockBatcher, EncodedBlockFrame};

use crate::{
    ArtifactFamily, HeightOrder,
    settled_shielded_segment::{
        ShieldedPlanExecution, ShieldedProjectionSelection, ShieldedRangeByteBudget,
        ShieldedReadScratch, project_shielded_block_wire_with_scratch,
        synthesize_empty_nullifier_payload,
    },
    settled_supplement_segment::{
        CompactBlockSpliceSelection, SupplementSpliceRequest, splice_compact_block_wire,
    },
};

use super::{
    CanonicalStoreError,
    live_tail::{LiveTailBlockRecord, LiveTailShieldedWire, LiveTailSpliceWire},
    reader::{
        CanonicalServingRead, bounded_batch_len, missing_compact_block_row,
        require_retained_compact_blocks, retained_compact_block_span,
    },
    settled_segments::{
        layout::SettledSegmentIndexEntry,
        store_access::{
            SegmentStoreContext, fetch_live_tail_batch_pinned, map_settled_segments_read_error,
            map_shielded_read_error, map_supplement_read_error, open_shielded_segment_reader,
            open_supplement_segment_reader, read_sealed_tip, resolve_settled_segment_for_height,
            verified_read_options,
        },
    },
};

/// Authenticated height and hash links returned from a canonical wire-frame range read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodedCompactBlockLink {
    /// Canonical block height.
    pub height: BlockHeight,
    /// Canonical block hash.
    pub block_hash: BlockHash,
    /// Canonical parent block hash.
    pub previous_block_hash: BlockHash,
}

/// Output container requested for canonical compact-block wire streaming.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EncodedCompactBlockOutput {
    /// Emit individual length-prefixed gRPC `CompactBlock` messages.
    Blocks,
    /// Emit pre-batched length-delimited `CompactBlock` wire frames.
    BatchedFrames {
        /// Whether this call emits the opening frames of a response stream.
        opening_frame: bool,
    },
}

/// Encoded compact-block messages or pre-batched wire frames produced by a canonical read.
#[derive(Clone, Debug)]
pub enum EncodedCompactBlockPayload {
    /// Individual protobuf-serialized `CompactBlock` gRPC messages in requested order.
    Blocks(Vec<EncodedBlock>),
    /// Pre-batched length-delimited `CompactBlock` frames in requested order.
    BatchedFrames(Vec<EncodedBlockFrame>),
}

/// Canonical compact-block wire-frame slice bounded by byte budget and cancellation.
#[derive(Clone, Debug)]
pub struct EncodedCompactBlockRange {
    /// Encoded blocks or pre-batched frames in requested height order.
    pub payload: EncodedCompactBlockPayload,
    /// Inclusive canonical height span covered by `payload` (at least one block).
    pub covered: BlockHeightRange,
    /// Authenticated link of the first yielded block in requested `order`.
    pub first_link: EncodedCompactBlockLink,
    /// Authenticated link of the last yielded block in requested `order`.
    pub last_link: EncodedCompactBlockLink,
    /// Number of canonical blocks represented in `payload`.
    pub block_count: usize,
}

/// Parameters for reading bounded canonical compact-block wire frames directly from
/// `ZSHRNG02` settled segments and `live_tail_block` (with optional `ZSTSUP01` splicing).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodedCompactBlockRangeRequest {
    /// Inclusive canonical height range to read.
    pub range: BlockHeightRange,
    /// Emission order across `range`.
    pub order: HeightOrder,
    /// Shielded pool selection and redaction mode.
    pub selection: ShieldedProjectionSelection,
    /// Merge Tag-1 transparent inputs, outputs, and transparent-only transactions via `ZSTSUP01`.
    pub include_transparent: bool,
    /// Output representation (individual messages or batched wire frames).
    pub output: EncodedCompactBlockOutput,
    /// Soft stored-byte budget per call (always keeps at least one block).
    pub max_bytes: usize,
    /// Reject blocks carrying an omitted transaction fee exceeding `u32::MAX`.
    pub enforce_wire_fee_limit: bool,
}

pub(super) fn encoded_compact_block_at(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
    selection: CompactBlockSpliceSelection,
) -> Result<Option<EncodedBlock>, CanonicalStoreError> {
    let retained = retained_compact_block_span(store);
    if height < retained.start || height > retained.end {
        return Ok(None);
    }
    encoded_compact_block_at_for_context(store.serving_context(), height, selection)
}

fn encoded_compact_block_at_for_context(
    ctx: SegmentStoreContext<'_>,
    height: BlockHeight,
    selection: CompactBlockSpliceSelection,
) -> Result<Option<EncodedBlock>, CanonicalStoreError> {
    if let Some(entry) = resolve_settled_segment_for_height(ctx.db, ctx.sealed_segments, height)? {
        return read_settled_unary_compact_block(ctx, &entry, height, selection).map(Some);
    }
    let Some(raw_row) =
        crate::canonical_store::settled_segments::store_access::read_live_tail_raw_bytes_with_options(
            ctx.db,
            height,
            &verified_read_options(),
        )?
    else {
        return Ok(None);
    };
    let wire = LiveTailBlockRecord::decode_splice_wire_v1(&raw_row, height)
        .map_err(map_settled_segments_read_error)?;
    if wire.has_unrepresentable_wire_fee {
        return Err(CanonicalStoreError::ArtifactCorrupt {
            family: ArtifactFamily::CompactBlock,
            reason: "compact transaction fee exceeds u32".to_owned(),
        });
    }
    if wire.transparent_transactions.is_empty() && !selection.nullifiers_only {
        return encoded_block_from_payload(wire.shielded_payload).map(Some);
    }
    splice_compact_block_wire(
        wire.shielded_payload,
        &wire.transparent_transactions,
        selection,
    )
    .map(Some)
    .map_err(map_supplement_read_error)
}

fn read_settled_unary_compact_block(
    ctx: SegmentStoreContext<'_>,
    entry: &SettledSegmentIndexEntry,
    height: BlockHeight,
    selection: CompactBlockSpliceSelection,
) -> Result<EncodedBlock, CanonicalStoreError> {
    let shielded = open_shielded_segment_reader(ctx, entry)?;
    let may_need_supplement = selection.include_transparent || selection.retain_all_transactions;
    let sup_reader = if may_need_supplement {
        open_supplement_segment_reader(ctx, entry)?
    } else {
        None
    };
    let has_tag1 = if let Some(sup) = &sup_reader {
        if sup
            .has_wide_fee_at(height)
            .map_err(map_supplement_read_error)?
        {
            return Err(CanonicalStoreError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                reason: "supplement compact transaction fee exceeds u32".to_owned(),
            });
        }
        sup.has_transparent_at(height)
            .map_err(map_supplement_read_error)?
    } else {
        false
    };
    let single_range = BlockHeightRange::inclusive(height, height);
    if !(has_tag1 && may_need_supplement) {
        let proj = if selection.nullifiers_only {
            ShieldedProjectionSelection::DEFAULT_NULLIFIERS
        } else {
            ShieldedProjectionSelection::DEFAULT_SHIELDED
        };
        let mut blocks = shielded
            .read_projected_range(single_range, HeightOrder::Ascending, proj)
            .map_err(map_shielded_read_error)?;
        return blocks
            .pop()
            .ok_or_else(|| missing_compact_block_row(height));
    }
    let sup = sup_reader.ok_or_else(|| CanonicalStoreError::SettledSegment {
        reason: format!(
            "settled supplement reader missing for block {}",
            height.value()
        ),
    })?;
    let mut blocks = shielded
        .read_default_range(single_range, HeightOrder::Ascending)
        .map_err(map_shielded_read_error)?;
    let shielded_block = blocks
        .pop()
        .ok_or_else(|| missing_compact_block_row(height))?;
    sup.splice_block_wire(height, &shielded_block.payload(), selection)
        .map_err(map_supplement_read_error)
}

pub(super) fn encoded_compact_block_range_up_to(
    store: &impl CanonicalServingRead,
    request: EncodedCompactBlockRangeRequest,
    cancel: Option<&AtomicBool>,
) -> Result<Option<EncodedCompactBlockRange>, CanonicalStoreError> {
    check_cancelled(cancel)?;
    if request.range.start > request.range.end {
        return Ok(None);
    }
    require_retained_compact_blocks(store, request.range)?;
    encoded_compact_block_range_for_context(store.serving_context(), request, cancel)
}

fn encoded_compact_block_range_for_context(
    ctx: SegmentStoreContext<'_>,
    request: EncodedCompactBlockRangeRequest,
    cancel: Option<&AtomicBool>,
) -> Result<Option<EncodedCompactBlockRange>, CanonicalStoreError> {
    check_cancelled(cancel)?;
    if request.range.start > request.range.end {
        return Ok(None);
    }

    let mut acc = RangeAccumulator::new(request);
    match request.order {
        HeightOrder::Ascending => stream_ascending(ctx, request, cancel, &mut acc)?,
        HeightOrder::Descending => stream_descending(ctx, request, cancel, &mut acc)?,
    }
    check_cancelled(cancel)?;
    Ok(acc.finish())
}

fn stream_ascending(
    ctx: SegmentStoreContext<'_>,
    request: EncodedCompactBlockRangeRequest,
    cancel: Option<&AtomicBool>,
    acc: &mut RangeAccumulator,
) -> Result<(), CanonicalStoreError> {
    let mut cursor = request.range.start;
    while cursor <= request.range.end && !acc.is_budget_spent() {
        check_cancelled(cancel)?;
        if let Some(entry) =
            resolve_settled_segment_for_height(ctx.db, ctx.sealed_segments, cursor)?
        {
            let seg_end = request.range.end.min(entry.end_block.height);
            let sub_range = BlockHeightRange::inclusive(cursor, seg_end);
            let complete = read_settled_segment_step(
                ctx,
                &entry,
                SegmentStepParams {
                    sub_range,
                    request,
                    cancel,
                },
                acc,
            )?;
            if !complete {
                break;
            }
            let Some(next) = seg_end.next() else {
                break;
            };
            cursor = next;
        } else {
            let live_span = BlockHeightRange::inclusive(cursor, request.range.end);
            let _ = read_live_tail_span(ctx, live_span, request, cancel, acc)?;
            break;
        }
    }
    Ok(())
}

fn stream_descending(
    ctx: SegmentStoreContext<'_>,
    request: EncodedCompactBlockRangeRequest,
    cancel: Option<&AtomicBool>,
    acc: &mut RangeAccumulator,
) -> Result<(), CanonicalStoreError> {
    let sealed_tip = match ctx.sealed_segments {
        Some(entries) => entries.last().map(|entry| entry.end_block),
        None => read_sealed_tip(ctx.db)?,
    };
    let mut cursor = Some(request.range.end);
    if let Some(live_start) = descending_live_tail_start(request.range, sealed_tip)
        && let Some(high) = cursor
    {
        let live_span = BlockHeightRange::inclusive(live_start, high);
        let complete = read_live_tail_span(ctx, live_span, request, cancel, acc)?;
        if !complete || acc.is_budget_spent() {
            return Ok(());
        }
        cursor = prev_height_in_range(live_start, request.range.start);
    }

    while let Some(high) = cursor
        && !acc.is_budget_spent()
    {
        check_cancelled(cancel)?;
        let Some(entry) = resolve_settled_segment_for_height(ctx.db, ctx.sealed_segments, high)?
        else {
            return Err(missing_compact_block_row(high));
        };
        let sub_start = request.range.start.max(entry.start_height);
        let sub_range = BlockHeightRange::inclusive(sub_start, high);
        let complete = read_settled_segment_step(
            ctx,
            &entry,
            SegmentStepParams {
                sub_range,
                request,
                cancel,
            },
            acc,
        )?;
        if !complete {
            break;
        }
        cursor = prev_height_in_range(sub_start, request.range.start);
    }
    Ok(())
}

fn descending_live_tail_start(
    range: BlockHeightRange,
    sealed_tip: Option<zinder_core::BlockId>,
) -> Option<BlockHeight> {
    let live_floor = match sealed_tip {
        None => range.start,
        Some(tip) => {
            let above_sealed = tip.height.next()?;
            range.start.max(above_sealed)
        }
    };
    (live_floor <= range.end).then_some(live_floor)
}

fn prev_height_in_range(height: BlockHeight, floor: BlockHeight) -> Option<BlockHeight> {
    if height <= floor {
        None
    } else {
        height.value().checked_sub(1).map(BlockHeight::new)
    }
}

#[derive(Clone, Copy)]
struct SegmentStepParams<'a> {
    sub_range: BlockHeightRange,
    request: EncodedCompactBlockRangeRequest,
    cancel: Option<&'a AtomicBool>,
}

fn read_settled_segment_step(
    ctx: SegmentStoreContext<'_>,
    entry: &SettledSegmentIndexEntry,
    params: SegmentStepParams<'_>,
    acc: &mut RangeAccumulator,
) -> Result<bool, CanonicalStoreError> {
    let reader = open_shielded_segment_reader(ctx, entry)?;
    let budget = ShieldedRangeByteBudget {
        max_bytes: acc.remaining_output_bytes(),
        allow_oversized_first: acc.block_count == 0,
        enforce_wire_fee_limit: params.request.enforce_wire_fee_limit,
    };
    let Some(bounded) = reader
        .index()
        .plan_bounded_block_range(
            params.sub_range,
            params.request.order,
            budget,
            params.request.selection,
        )
        .map_err(map_shielded_read_error)?
    else {
        return Ok(false);
    };
    let (step_first, step_last) =
        read_segment_boundary_links(&reader, &bounded, params.request.order)?;
    if let Some(prev) = acc.last_link {
        verify_step_linkage(params.request.order, prev, step_first)?;
    }
    let sup_reader =
        if params.request.include_transparent && !params.request.selection.nullifiers_only {
            open_needed_supplement_for_span(ctx, entry, &bounded)?
        } else {
            None
        };
    if let Some(sup) = sup_reader {
        emit_settled_spliced_step(&reader, &sup, &bounded, params, acc)?;
    } else {
        emit_settled_shielded_step(&reader, &bounded, params, acc)?;
    }
    acc.record_settled_span(
        step_first,
        step_last,
        bounded.kept_blocks,
        bounded.kept_native_bytes,
    );
    let completed_sub_range = match params.request.order {
        HeightOrder::Ascending => bounded.highest_height == params.sub_range.end,
        HeightOrder::Descending => bounded.lowest_height == params.sub_range.start,
    };
    Ok(completed_sub_range)
}

fn open_needed_supplement_for_span(
    ctx: SegmentStoreContext<'_>,
    entry: &SettledSegmentIndexEntry,
    bounded: &crate::BoundedShieldedRangePlan,
) -> Result<Option<std::sync::Arc<crate::SettledSupplementSegmentReader>>, CanonicalStoreError> {
    let Some(sup) = open_supplement_segment_reader(ctx, entry)? else {
        return Ok(None);
    };
    let splice_plan = sup
        .index()
        .plan_splice_range(
            bounded.lowest_height,
            bounded.highest_height,
            CompactBlockSpliceSelection::RANGE_WITH_TRANSPARENT,
        )
        .map_err(map_supplement_read_error)?;
    Ok(splice_plan.needs_any_chunk().then_some(sup))
}

fn emit_settled_shielded_step(
    reader: &crate::SettledShieldedSegmentReader,
    bounded: &crate::BoundedShieldedRangePlan,
    params: SegmentStepParams<'_>,
    acc: &mut RangeAccumulator,
) -> Result<(), CanonicalStoreError> {
    let execution = ShieldedPlanExecution {
        order: params.request.order,
        selection: params.request.selection,
        cancel: params.cancel,
    };
    match &mut acc.output {
        AccumulatorOutput::Blocks(out) => {
            let blocks = reader
                .read_bounded_projected_blocks_for_plan(bounded, execution, &mut acc.scratch)
                .map_err(map_shielded_read_error)?;
            out.extend(blocks);
        }
        AccumulatorOutput::Batcher(batcher) => {
            reader
                .append_bounded_projected_frames_for_plan(
                    bounded,
                    execution,
                    &mut acc.scratch,
                    batcher,
                )
                .map_err(map_shielded_read_error)?;
        }
    }
    Ok(())
}

fn emit_settled_spliced_step(
    reader: &crate::SettledShieldedSegmentReader,
    sup: &crate::SettledSupplementSegmentReader,
    bounded: &crate::BoundedShieldedRangePlan,
    params: SegmentStepParams<'_>,
    acc: &mut RangeAccumulator,
) -> Result<(), CanonicalStoreError> {
    let asc_execution = ShieldedPlanExecution {
        order: HeightOrder::Ascending,
        selection: params.request.selection,
        cancel: params.cancel,
    };
    let shielded_blocks = reader
        .read_bounded_projected_blocks_for_plan(bounded, asc_execution, &mut acc.scratch)
        .map_err(map_shielded_read_error)?;
    let completed = sup
        .splice_encoded_range_into(
            SupplementSpliceRequest {
                start_height: bounded.lowest_height,
                selection: CompactBlockSpliceSelection::RANGE_WITH_TRANSPARENT,
                order: params.request.order,
                cancel: params.cancel,
            },
            &shielded_blocks,
            |spliced| {
                acc.push_encoded_block(spliced);
                Ok(())
            },
        )
        .map_err(map_supplement_read_error)?;
    if !completed {
        return Err(CanonicalStoreError::ReplayCancelled);
    }
    Ok(())
}

fn read_segment_boundary_links(
    reader: &crate::SettledShieldedSegmentReader,
    bounded: &crate::BoundedShieldedRangePlan,
    order: HeightOrder,
) -> Result<(EncodedCompactBlockLink, EncodedCompactBlockLink), CanonicalStoreError> {
    let low_link = link_from_pair(
        reader
            .block_link_at(bounded.lowest_height)
            .map_err(map_shielded_read_error)?,
    );
    let high_link = if bounded.highest_height == bounded.lowest_height {
        low_link
    } else {
        link_from_pair(
            reader
                .block_link_at(bounded.highest_height)
                .map_err(map_shielded_read_error)?,
        )
    };
    Ok(match order {
        HeightOrder::Ascending => (low_link, high_link),
        HeightOrder::Descending => (high_link, low_link),
    })
}

const fn link_from_pair(pair: (zinder_core::BlockId, BlockHash)) -> EncodedCompactBlockLink {
    EncodedCompactBlockLink {
        height: pair.0.height,
        block_hash: pair.0.hash,
        previous_block_hash: pair.1,
    }
}

fn read_live_tail_span(
    ctx: SegmentStoreContext<'_>,
    span: BlockHeightRange,
    request: EncodedCompactBlockRangeRequest,
    cancel: Option<&AtomicBool>,
    acc: &mut RangeAccumulator,
) -> Result<bool, CanonicalStoreError> {
    let mut unread = span.start.value()..=span.end.value();
    let mut next_height = || {
        match request.order {
            HeightOrder::Ascending => unread.next(),
            HeightOrder::Descending => unread.next_back(),
        }
        .map(BlockHeight::new)
    };
    let splice_transparent = request.include_transparent && !request.selection.nullifiers_only;
    while !acc.is_budget_spent() {
        check_cancelled(cancel)?;
        let batch_len = acc.next_live_tail_batch_len();
        let heights = std::iter::from_fn(&mut next_height)
            .take(batch_len)
            .collect::<Vec<_>>();
        if heights.is_empty() {
            return Ok(true);
        }
        let rows = fetch_live_tail_batch_pinned(ctx.db, &heights, &verified_read_options())?;
        for (height, maybe_row) in heights.iter().zip(rows) {
            check_cancelled(cancel)?;
            let raw_row = maybe_row?.ok_or_else(|| missing_compact_block_row(*height))?;
            let stored_row_bytes = raw_row.as_ref().len();
            if acc.exceeds_live_read_budget(stored_row_bytes) {
                return Ok(false);
            }
            let raw_exceeds_output_budget = acc.exceeds_output_budget(stored_row_bytes);
            if splice_transparent {
                let wire =
                    match LiveTailBlockRecord::decode_splice_wire_v1(raw_row.as_ref(), *height) {
                        Ok(decoded) => decoded,
                        Err(_) if raw_exceeds_output_budget => return Ok(false),
                        Err(error) => return Err(map_settled_segments_read_error(error)),
                    };
                if !append_live_tail_spliced_row(&wire, stored_row_bytes, request, acc)? {
                    return Ok(false);
                }
            } else {
                let wire =
                    match LiveTailBlockRecord::decode_shielded_wire_v1(raw_row.as_ref(), *height) {
                        Ok(decoded) => decoded,
                        Err(_) if raw_exceeds_output_budget => return Ok(false),
                        Err(error) => return Err(map_settled_segments_read_error(error)),
                    };
                if !append_live_tail_row(&wire, stored_row_bytes, request, acc)? {
                    return Ok(false);
                }
            }
        }
    }
    Ok(false)
}

fn append_live_tail_spliced_row(
    wire: &LiveTailSpliceWire<'_>,
    stored_row_bytes: usize,
    request: EncodedCompactBlockRangeRequest,
    acc: &mut RangeAccumulator,
) -> Result<bool, CanonicalStoreError> {
    if wire.transparent_transactions.is_empty() {
        let shielded = LiveTailShieldedWire {
            block_id: wire.block_id,
            previous_block_hash: wire.previous_block_hash,
            time: wire.time,
            shielded_payload: wire.shielded_payload,
            pool_presence: wire.pool_presence,
            has_unrepresentable_wire_fee: wire.has_unrepresentable_wire_fee,
        };
        return append_live_tail_row(&shielded, stored_row_bytes, request, acc);
    }
    if acc.exceeds_output_budget(wire.shielded_payload.len())
        || acc.exceeds_live_read_budget(stored_row_bytes)
    {
        return Ok(false);
    }
    if request.enforce_wire_fee_limit && wire.has_unrepresentable_wire_fee {
        return Err(CanonicalStoreError::ArtifactCorrupt {
            family: ArtifactFamily::CompactBlock,
            reason: "compact transaction fee exceeds u32".to_owned(),
        });
    }
    let link = EncodedCompactBlockLink {
        height: wire.block_id.height,
        block_hash: wire.block_id.hash,
        previous_block_hash: wire.previous_block_hash,
    };
    if let Some(prev) = acc.last_link {
        verify_step_linkage(request.order, prev, link)?;
    }
    let projected = project_live_wire_bytes(wire.shielded_payload, wire, request.selection, acc)?;
    let shielded_slice =
        match projected {
            ProjectedWire::Borrowed(bytes) => bytes,
            ProjectedWire::Scratch { start } => acc.wire_scratch.get(start..).ok_or_else(|| {
                CanonicalStoreError::ArtifactCorrupt {
                    family: ArtifactFamily::CompactBlock,
                    reason: "projected scratch payload too short".to_owned(),
                }
            })?,
        };
    let spliced = splice_compact_block_wire(
        shielded_slice,
        &wire.transparent_transactions,
        CompactBlockSpliceSelection::RANGE_WITH_TRANSPARENT,
    )
    .map_err(map_supplement_read_error)?;
    let output_bytes = spliced.payload_len();
    if acc.exceeds_output_budget(output_bytes) {
        return Ok(false);
    }
    acc.push_encoded_block(spliced);
    acc.record_live_tail_row(link, output_bytes, stored_row_bytes);
    Ok(!acc.is_budget_spent())
}

fn project_live_wire_bytes<'a>(
    shielded_payload: &'a [u8],
    wire: &LiveTailSpliceWire<'_>,
    selection: ShieldedProjectionSelection,
    acc: &mut RangeAccumulator,
) -> Result<ProjectedWire<'a>, CanonicalStoreError> {
    if selection == ShieldedProjectionSelection::DEFAULT_SHIELDED {
        return Ok(ProjectedWire::Borrowed(shielded_payload));
    }
    let as_shielded = LiveTailShieldedWire {
        block_id: wire.block_id,
        previous_block_hash: wire.previous_block_hash,
        time: wire.time,
        shielded_payload,
        pool_presence: wire.pool_presence,
        has_unrepresentable_wire_fee: wire.has_unrepresentable_wire_fee,
    };
    project_live_wire(&as_shielded, selection, acc)
}

fn append_live_tail_row(
    wire: &LiveTailShieldedWire<'_>,
    stored_row_bytes: usize,
    request: EncodedCompactBlockRangeRequest,
    acc: &mut RangeAccumulator,
) -> Result<bool, CanonicalStoreError> {
    let output_bytes = wire.shielded_payload.len();
    if acc.exceeds_output_budget(output_bytes) || acc.exceeds_live_read_budget(stored_row_bytes) {
        return Ok(false);
    }
    if request.enforce_wire_fee_limit && wire.has_unrepresentable_wire_fee {
        return Err(CanonicalStoreError::ArtifactCorrupt {
            family: ArtifactFamily::CompactBlock,
            reason: "compact transaction fee exceeds u32".to_owned(),
        });
    }
    let link = EncodedCompactBlockLink {
        height: wire.block_id.height,
        block_hash: wire.block_id.hash,
        previous_block_hash: wire.previous_block_hash,
    };
    if let Some(prev) = acc.last_link {
        verify_step_linkage(request.order, prev, link)?;
    }
    let encoded_payload = project_live_wire(wire, request.selection, acc)?;
    acc.push_encoded_payload(encoded_payload)?;
    acc.record_live_tail_row(link, output_bytes, stored_row_bytes);
    Ok(!acc.is_budget_spent())
}

#[derive(Clone, Copy)]
enum ProjectedWire<'a> {
    Borrowed(&'a [u8]),
    Scratch { start: usize },
}

fn project_live_wire<'a>(
    wire: &LiveTailShieldedWire<'a>,
    selection: ShieldedProjectionSelection,
    acc: &mut RangeAccumulator,
) -> Result<ProjectedWire<'a>, CanonicalStoreError> {
    if selection.nullifiers_only && !selection.matches_pool_presence(wire.pool_presence) {
        acc.wire_scratch.clear();
        let hash_wire = encode_internal_block_hash(wire.block_id.hash);
        let prev_wire = encode_internal_block_hash(wire.previous_block_hash);
        synthesize_empty_nullifier_payload(
            wire.block_id.height,
            &hash_wire,
            &prev_wire,
            wire.time,
            &mut acc.wire_scratch,
        );
        return Ok(ProjectedWire::Scratch { start: 0 });
    }
    if selection == ShieldedProjectionSelection::DEFAULT_SHIELDED {
        return Ok(ProjectedWire::Borrowed(wire.shielded_payload));
    }
    project_shielded_block_wire_with_scratch(
        wire.shielded_payload,
        selection,
        &mut acc.wire_scratch,
        &mut acc.tx_scratch,
    )
    .map_err(map_shielded_read_error)?;
    Ok(ProjectedWire::Scratch {
        start: EncodedBlock::PREFIX_LEN,
    })
}

fn verify_step_linkage(
    order: HeightOrder,
    prev: EncodedCompactBlockLink,
    next: EncodedCompactBlockLink,
) -> Result<(), CanonicalStoreError> {
    let valid = match order {
        HeightOrder::Ascending => {
            next.height.value() == prev.height.value().saturating_add(1)
                && next.previous_block_hash == prev.block_hash
        }
        HeightOrder::Descending => {
            prev.height.value() == next.height.value().saturating_add(1)
                && prev.previous_block_hash == next.block_hash
        }
    };
    if !valid {
        return Err(CanonicalStoreError::ArtifactCorrupt {
            family: ArtifactFamily::CompactBlock,
            reason: format!(
                "compact block linkage mismatch between heights {} and {}",
                prev.height.value(),
                next.height.value()
            ),
        });
    }
    Ok(())
}

fn check_cancelled(cancel: Option<&AtomicBool>) -> Result<(), CanonicalStoreError> {
    if cancel.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
        Err(CanonicalStoreError::ReplayCancelled)
    } else {
        Ok(())
    }
}

/// Full stored `live_tail_block` row-read cap as a multiple of `request.max_bytes`.
///
/// Empty/coinbase `ZSLVTL01` rows include envelope framing and canonical facts (~500–950 B)
/// even when their shielded `CompactBlock` payload is ~74 B, so a 4× multiplier keeps 1,000
/// small live-tail blocks inside a 256 KiB opening chunk (1 MiB live-read cap) while capping
/// transparent-heavy supplements separately from shielded output bytes.
const LIVE_TAIL_READ_BUDGET_MULTIPLIER: usize = 4;

enum AccumulatorOutput {
    Blocks(Vec<EncodedBlock>),
    Batcher(EncodedBlockBatcher),
}

struct RangeAccumulator {
    order: HeightOrder,
    max_bytes: usize,
    max_live_read_bytes: usize,
    kept_bytes: usize,
    live_tail_read_bytes: usize,
    largest_live_row_bytes: usize,
    largest_live_output_bytes: usize,
    first_link: Option<EncodedCompactBlockLink>,
    last_link: Option<EncodedCompactBlockLink>,
    block_count: usize,
    output: AccumulatorOutput,
    scratch: ShieldedReadScratch,
    wire_scratch: Vec<u8>,
    tx_scratch: Vec<u8>,
}

impl RangeAccumulator {
    fn new(request: EncodedCompactBlockRangeRequest) -> Self {
        let output = match request.output {
            EncodedCompactBlockOutput::Blocks => AccumulatorOutput::Blocks(Vec::new()),
            EncodedCompactBlockOutput::BatchedFrames { opening_frame } => {
                AccumulatorOutput::Batcher(EncodedBlockBatcher::new(opening_frame))
            }
        };
        Self {
            order: request.order,
            max_bytes: request.max_bytes,
            max_live_read_bytes: request
                .max_bytes
                .saturating_mul(LIVE_TAIL_READ_BUDGET_MULTIPLIER),
            kept_bytes: 0,
            live_tail_read_bytes: 0,
            largest_live_row_bytes: 0,
            largest_live_output_bytes: 0,
            first_link: None,
            last_link: None,
            block_count: 0,
            output,
            scratch: ShieldedReadScratch::default(),
            wire_scratch: Vec::new(),
            tx_scratch: Vec::new(),
        }
    }

    const fn remaining_output_bytes(&self) -> usize {
        self.max_bytes.saturating_sub(self.kept_bytes)
    }

    const fn remaining_live_read_bytes(&self) -> usize {
        self.max_live_read_bytes
            .saturating_sub(self.live_tail_read_bytes)
    }

    const fn is_budget_spent(&self) -> bool {
        self.block_count > 0
            && (self.kept_bytes >= self.max_bytes
                || self.live_tail_read_bytes >= self.max_live_read_bytes)
    }

    const fn exceeds_live_read_budget(&self, stored_row_bytes: usize) -> bool {
        self.block_count > 0
            && (self.live_tail_read_bytes >= self.max_live_read_bytes
                || self.live_tail_read_bytes.saturating_add(stored_row_bytes)
                    > self.max_live_read_bytes)
    }

    const fn exceeds_output_budget(&self, output_bytes: usize) -> bool {
        self.block_count > 0
            && (self.kept_bytes >= self.max_bytes
                || self.kept_bytes.saturating_add(output_bytes) > self.max_bytes)
    }

    fn next_live_tail_batch_len(&self) -> usize {
        bounded_batch_len(
            self.remaining_live_read_bytes(),
            self.largest_live_row_bytes,
        )
        .min(bounded_batch_len(
            self.remaining_output_bytes(),
            self.largest_live_output_bytes,
        ))
    }

    fn record_settled_span(
        &mut self,
        first: EncodedCompactBlockLink,
        last: EncodedCompactBlockLink,
        kept_blocks: usize,
        kept_native_bytes: usize,
    ) {
        if self.first_link.is_none() {
            self.first_link = Some(first);
        }
        self.last_link = Some(last);
        self.block_count = self.block_count.saturating_add(kept_blocks);
        self.kept_bytes = self.kept_bytes.saturating_add(kept_native_bytes);
    }

    fn record_live_tail_row(
        &mut self,
        link: EncodedCompactBlockLink,
        output_bytes: usize,
        stored_row_bytes: usize,
    ) {
        if self.first_link.is_none() {
            self.first_link = Some(link);
        }
        self.last_link = Some(link);
        self.block_count = self.block_count.saturating_add(1);
        self.kept_bytes = self.kept_bytes.saturating_add(output_bytes);
        self.live_tail_read_bytes = self.live_tail_read_bytes.saturating_add(stored_row_bytes);
        self.largest_live_row_bytes = self.largest_live_row_bytes.max(stored_row_bytes);
        self.largest_live_output_bytes = self.largest_live_output_bytes.max(output_bytes);
    }

    fn push_encoded_block(&mut self, block: EncodedBlock) {
        match &mut self.output {
            AccumulatorOutput::Blocks(out) => out.push(block),
            AccumulatorOutput::Batcher(batcher) => batcher.push_block(block),
        }
    }

    fn push_encoded_payload(&mut self, wire: ProjectedWire<'_>) -> Result<(), CanonicalStoreError> {
        let slice = match wire {
            ProjectedWire::Borrowed(bytes) => bytes,
            ProjectedWire::Scratch { start } => {
                self.wire_scratch.get(start..).ok_or_else(|| {
                    CanonicalStoreError::ArtifactCorrupt {
                        family: ArtifactFamily::CompactBlock,
                        reason: format!(
                            "projected scratch payload too short: {} < {start}",
                            self.wire_scratch.len()
                        ),
                    }
                })?
            }
        };
        match &mut self.output {
            AccumulatorOutput::Blocks(out) => {
                out.push(encoded_block_from_payload(slice)?);
            }
            AccumulatorOutput::Batcher(batcher) => {
                batcher.push_payload(slice).map_err(|err| {
                    CanonicalStoreError::ArtifactCorrupt {
                        family: ArtifactFamily::CompactBlock,
                        reason: err.to_string(),
                    }
                })?;
            }
        }
        Ok(())
    }

    fn finish(self) -> Option<EncodedCompactBlockRange> {
        let first_link = self.first_link?;
        let last_link = self.last_link?;
        let covered = match self.order {
            HeightOrder::Ascending => {
                BlockHeightRange::inclusive(first_link.height, last_link.height)
            }
            HeightOrder::Descending => {
                BlockHeightRange::inclusive(last_link.height, first_link.height)
            }
        };
        let payload = match self.output {
            AccumulatorOutput::Blocks(blocks) => EncodedCompactBlockPayload::Blocks(blocks),
            AccumulatorOutput::Batcher(batcher) => {
                EncodedCompactBlockPayload::BatchedFrames(batcher.finish())
            }
        };
        Some(EncodedCompactBlockRange {
            payload,
            covered,
            first_link,
            last_link,
            block_count: self.block_count,
        })
    }
}

fn encoded_block_from_payload(payload: &[u8]) -> Result<EncodedBlock, CanonicalStoreError> {
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| CanonicalStoreError::ArtifactCorrupt {
            family: ArtifactFamily::CompactBlock,
            reason: "encoded compact block payload exceeds u32".to_owned(),
        })?;
    let mut framed = Vec::with_capacity(5 + payload.len());
    framed.push(0);
    framed.extend_from_slice(&payload_len.to_be_bytes());
    framed.extend_from_slice(payload);
    EncodedBlock::from_length_prefixed(Bytes::from(framed)).map_err(|err| {
        CanonicalStoreError::ArtifactCorrupt {
            family: ArtifactFamily::CompactBlock,
            reason: err.to_string(),
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::{Seek, SeekFrom, Write},
        sync::Arc,
        sync::atomic::AtomicBool,
    };

    use prost::Message as _;
    use rust_rocksdb::DB;
    use tempfile::TempDir;
    use zinder_core::{
        BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId,
        CanonicalBlockFacts, CanonicalTransactionFacts, CompactBlockArtifact, CompactChainMetadata,
        CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransaction,
        CompactTransactionData, CompactTransparentOutput, LockTime, Network, PrivacyShape,
        SerializedBytesDigest, TransactionComponentCounts, TransactionId,
        TransactionIntrinsicValueBalances, TransactionPublicFacts, TransactionVersion,
        UnsupportedSection,
        wire::{encode_height_key_ascending, encode_internal_block_hash},
    };
    use zinder_proto::compat::lightwalletd;

    use super::*;
    use crate::{
        CanonicalReorgPolicy, CanonicalStoreRequirements, RocksDbCanonicalSecondary,
        RocksDbResourceBudget,
        canonical_store::{
            live_replacement_tests::published_store,
            live_tail::LiveTailEncodeOptions,
            rocksdb::{LIVE_TAIL_BLOCK_COLUMN_FAMILY, SETTLED_SEGMENT_INDEX_COLUMN_FAMILY},
            settled_segments::{
                cache::SettledSegmentReaderCache,
                layout::{
                    SettledSegmentKind, SettledSegmentModules, format_segment_filename,
                    segment_dir_path,
                },
                sealer::{
                    SettledSegmentBlockInput, SettledSegmentSealer, SettledSegmentSealerConfig,
                },
                store_access::{open_replay_segment_readers, reconstruct_segment_compact_range},
            },
            stored_set::CANONICAL_DATA_COLUMN_FAMILIES,
        },
    };

    type TestOutcome<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn hash_with_tag(tag: u32) -> BlockHash {
        let mut raw = [0_u8; 32];
        raw[..4].copy_from_slice(&tag.to_le_bytes());
        raw[31] = 0x11;
        BlockHash::from_bytes(raw)
    }

    #[expect(
        clippy::struct_excessive_bools,
        reason = "test fixture flags configure independent transaction pools and fees"
    )]
    #[derive(Clone, Copy)]
    struct FixtureShape {
        sapling: bool,
        orchard: bool,
        transparent: bool,
        wide_fee: bool,
    }

    fn build_fixture_transactions(height_num: u32, shape: FixtureShape) -> Vec<CompactTransaction> {
        let fee_zat = Some(if shape.wide_fee {
            u64::from(u32::MAX) + 1
        } else {
            1_000
        });
        let seed = u8::try_from(height_num % 200).unwrap_or(1);
        let mut txs = Vec::new();
        if shape.sapling {
            txs.push(CompactTransaction {
                index: u64::try_from(txs.len()).unwrap_or(0),
                transaction_id: TransactionId::from_bytes([seed; 32]),
                data: CompactTransactionData {
                    fee_zat: Some(500),
                    sapling_spends: vec![CompactSaplingSpend {
                        nullifier: [seed; 32],
                    }],
                    sapling_outputs: vec![CompactSaplingOutput {
                        commitment: [seed; 32],
                        ephemeral_key: [seed; 32],
                        ciphertext: [seed; 52],
                    }],
                    ..CompactTransactionData::default()
                },
            });
        }
        if shape.orchard {
            txs.push(CompactTransaction {
                index: u64::try_from(txs.len()).unwrap_or(0),
                transaction_id: TransactionId::from_bytes([seed.wrapping_add(1); 32]),
                data: CompactTransactionData {
                    fee_zat: if shape.wide_fee && !shape.transparent {
                        fee_zat
                    } else {
                        Some(700)
                    },
                    orchard_actions: vec![CompactShieldedAction {
                        nullifier: [seed.wrapping_add(2); 32],
                        commitment: [seed.wrapping_add(3); 32],
                        ephemeral_key: [seed.wrapping_add(4); 32],
                        ciphertext: [seed.wrapping_add(5); 52],
                    }],
                    ..CompactTransactionData::default()
                },
            });
        }
        if shape.transparent {
            txs.push(CompactTransaction {
                index: u64::try_from(txs.len()).unwrap_or(0),
                transaction_id: TransactionId::from_bytes([seed.wrapping_add(6); 32]),
                data: CompactTransactionData {
                    fee_zat,
                    transparent_outputs: vec![CompactTransparentOutput {
                        value_zat: 42_000,
                        script_pub_key: vec![0x76, 0xa9],
                    }],
                    ..CompactTransactionData::default()
                },
            });
        }
        txs
    }

    fn shielded_block_fixture(
        height_num: u32,
        shape: FixtureShape,
    ) -> TestOutcome<(CompactBlockArtifact, CanonicalBlockFacts)> {
        let height = BlockHeight::new(height_num);
        let block_hash = hash_with_tag(height_num);
        let parent_hash = if height_num == 1 {
            Network::ZcashRegtest.genesis_hash()
        } else {
            hash_with_tag(height_num - 1)
        };
        let txs = build_fixture_transactions(height_num, shape);
        let compact = CompactBlockArtifact::new(
            BlockId::new(height, block_hash),
            parent_hash,
            1_700_000_000 + height_num,
            txs,
            CompactChainMetadata {
                sapling_commitment_tree_size: height_num * 2,
                orchard_commitment_tree_size: height_num * 3,
                ironwood_commitment_tree_size: 0,
            },
        )?;
        let facts = build_facts_for_compact(&compact);
        Ok((compact, facts))
    }

    fn build_facts_for_compact(compact: &CompactBlockArtifact) -> CanonicalBlockFacts {
        let tx_facts = compact
            .transactions()
            .iter()
            .map(|tx| CanonicalTransactionFacts {
                public_facts: TransactionPublicFacts {
                    transaction_id: tx.transaction_id,
                    auth_digest: None,
                    wtxid: None,
                    version: TransactionVersion::Unsupported {
                        effective_version: 0,
                        version_group_id: None,
                    },
                    consensus_branch_id: None,
                    lock_time: LockTime::Unlocked,
                    expiry_height: None,
                    size_bytes: 1,
                    counts: TransactionComponentCounts::EMPTY,
                    orchard_value_balance_zat: None,
                    orchard_anchor: None,
                    ironwood_value_balance_zat: None,
                    privacy_shape: PrivacyShape::Unclassified,
                    is_coinbase: false,
                    unsupported_sections: vec![UnsupportedSection::FutureVersionHeader],
                },
                serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[1, 2]),
                intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
                transparent_inputs: Vec::new(),
                transparent_outputs: Vec::new(),
            })
            .collect();
        let header = BlockHeaderArtifact::new(
            compact.height(),
            compact.block_hash(),
            compact.previous_block_hash(),
            [0x11; 32],
            [0x22; 32],
            i64::from(compact.time()),
            0x1f07_ffff,
            [0x33; 32],
            4,
            128,
        );
        CanonicalBlockFacts {
            block_header: header,
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[9, 8]),
            transactions: tx_facts,
        }
    }

    type SeededMultiStore = (
        DB,
        Vec<SettledSegmentIndexEntry>,
        Arc<SettledSegmentReaderCache>,
        Vec<CompactBlockArtifact>,
    );

    fn seed_multi_segment_store(
        dir: &std::path::Path,
        total_blocks: u32,
        segment_stride: u32,
        wide_fee_heights: &[u32],
    ) -> TestOutcome<SeededMultiStore> {
        let mut opts = rust_rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let descriptors = CANONICAL_DATA_COLUMN_FAMILIES
            .into_iter()
            .map(|name| rust_rocksdb::ColumnFamilyDescriptor::new(name, opts.clone()))
            .collect::<Vec<_>>();
        let db = DB::open_cf_descriptors(&opts, dir, descriptors)?;
        let index_cf = db
            .cf_handle(SETTLED_SEGMENT_INDEX_COLUMN_FAMILY)
            .ok_or("missing settled_segment_index cf")?;
        let live_cf = db
            .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
            .ok_or("missing live_tail_block cf")?;

        let modules = SettledSegmentModules {
            supplement: true,
            raw_bodies: false,
        };
        let config = SettledSegmentSealerConfig::new(Network::ZcashRegtest, 2, modules)
            .with_segment_blocks(segment_stride);
        let mut segment_writer = SettledSegmentSealer::new(dir, config, None)?;
        let mut entries = Vec::new();
        let mut blocks = Vec::new();
        let settled_count = (total_blocks / segment_stride) * segment_stride;

        for h in 1..=total_blocks {
            let shape = FixtureShape {
                sapling: h % 3 == 1,
                orchard: h % 3 == 2,
                transparent: h == 2 || h == 5 || h == total_blocks,
                wide_fee: wide_fee_heights.contains(&h),
            };
            let (compact, facts) = shielded_block_fixture(h, shape)?;
            blocks.push(compact.clone());
            if h <= settled_count {
                let completed = segment_writer.push_block(SettledSegmentBlockInput {
                    compact_block: compact,
                    native_bytes: 256,
                    canonical_facts: facts,
                    raw_block: None,
                })?;
                for entry in completed {
                    db.put_cf(
                        &index_cf,
                        encode_height_key_ascending(entry.start_height),
                        entry.encode_v1()?,
                    )?;
                    entries.push(entry);
                }
            } else {
                let record = LiveTailBlockRecord::from_compact_block(&compact, true, Some(facts))?;
                let encoded = record.encode_v1()?;
                db.put_cf(
                    &live_cf,
                    encode_height_key_ascending(BlockHeight::new(h)),
                    encoded,
                )?;
            }
        }
        drop((index_cf, live_cf));
        let cache = SettledSegmentReaderCache::with_default_capacity(dir, false)?;
        Ok((db, entries, cache, blocks))
    }

    fn decode_blocks(blocks: &[EncodedBlock]) -> TestOutcome<Vec<lightwalletd::CompactBlock>> {
        blocks
            .iter()
            .map(|blk| lightwalletd::CompactBlock::decode(blk.payload()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    #[test]
    fn canonical_reader_streams_primary_and_secondary_and_handles_empty_or_bounds() -> TestOutcome {
        let dir = TempDir::new()?;
        let store_path = dir.path().join("primary");
        let sec_path = dir.path().join("secondary");
        let primary = published_store(&store_path, 2, BlockHeight::new(6), BlockHeight::new(4))?;
        let activations = super::super::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let secondary = RocksDbCanonicalSecondary::open_ready(
            &store_path,
            &sec_path,
            &activations,
            CanonicalStoreRequirements::default(),
            CanonicalReorgPolicy::new(2)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;

        let full = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(6));
        let asc_req = EncodedCompactBlockRangeRequest {
            range: full,
            order: HeightOrder::Ascending,
            selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
            include_transparent: false,
            output: EncodedCompactBlockOutput::Blocks,
            max_bytes: usize::MAX,
            enforce_wire_fee_limit: true,
        };
        let p_range = primary
            .encoded_compact_block_range_up_to(asc_req, None)?
            .ok_or("expected blocks from primary")?;
        let s_range = secondary
            .encoded_compact_block_range_up_to(asc_req, None)?
            .ok_or("expected blocks from secondary")?;
        assert_eq!(p_range.block_count, 6);
        assert_eq!(s_range.block_count, 6);
        assert_eq!(p_range.covered, full);
        assert_eq!(p_range.first_link, s_range.first_link);
        assert_eq!(p_range.last_link, s_range.last_link);

        let empty_req = EncodedCompactBlockRangeRequest {
            range: BlockHeightRange::inclusive(BlockHeight::new(5), BlockHeight::new(3)),
            ..asc_req
        };
        assert!(
            primary
                .encoded_compact_block_range_up_to(empty_req, None)?
                .is_none()
        );

        let oob_req = EncodedCompactBlockRangeRequest {
            range: BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(7)),
            ..asc_req
        };
        assert!(matches!(
            primary.encoded_compact_block_range_up_to(oob_req, None),
            Err(CanonicalStoreError::ArtifactUnavailable {
                family: ArtifactFamily::CompactBlock,
                height,
            }) if height == BlockHeight::new(7)
        ));
        Ok(())
    }

    #[test]
    fn multi_segment_and_live_stitching_ascending_and_descending_skips_supplement() -> TestOutcome {
        let dir = TempDir::new()?;
        let (db, entries, cache, blocks) = seed_multi_segment_store(dir.path(), 8, 2, &[])?;
        let ctx = SegmentStoreContext {
            db: &db,
            segment_cache: Some(&cache),
            sealed_segments: Some(&entries),
        };
        let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(8));

        for order in [HeightOrder::Ascending, HeightOrder::Descending] {
            let block_req = EncodedCompactBlockRangeRequest {
                range,
                order,
                selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                include_transparent: false,
                output: EncodedCompactBlockOutput::Blocks,
                max_bytes: usize::MAX,
                enforce_wire_fee_limit: true,
            };
            let block_slice = encoded_compact_block_range_for_context(ctx, block_req, None)?
                .ok_or("expected blocks output")?;
            assert_eq!(block_slice.block_count, 8);
            assert_eq!(block_slice.covered, range);
            let EncodedCompactBlockPayload::Blocks(encoded_blocks) = block_slice.payload else {
                return Err("expected Blocks variant".into());
            };
            let decoded = decode_blocks(&encoded_blocks)?;
            let expected_heights: Vec<u64> = match order {
                HeightOrder::Ascending => (1..=8).collect(),
                HeightOrder::Descending => (1..=8).rev().collect(),
            };
            let actual_heights: Vec<u64> = decoded.iter().map(|blk| blk.height).collect();
            assert_eq!(actual_heights, expected_heights);
            assert_eq!(
                block_slice.first_link.height.value(),
                u32::try_from(expected_heights[0])?
            );
            assert_eq!(
                block_slice.last_link.height.value(),
                u32::try_from(expected_heights[7])?
            );
            for proto in &decoded {
                let src = &blocks[usize::try_from(proto.height - 1)?];
                assert_eq!(proto.hash, encode_internal_block_hash(src.block_hash()));
                assert_eq!(
                    proto.prev_hash,
                    encode_internal_block_hash(src.previous_block_hash())
                );
            }

            let frame_req = EncodedCompactBlockRangeRequest {
                output: EncodedCompactBlockOutput::BatchedFrames {
                    opening_frame: true,
                },
                ..block_req
            };
            let frame_slice = encoded_compact_block_range_for_context(ctx, frame_req, None)?
                .ok_or("expected frames output")?;
            let EncodedCompactBlockPayload::BatchedFrames(frames) = frame_slice.payload else {
                return Err("expected BatchedFrames variant".into());
            };
            let mut ref_batcher = EncodedBlockBatcher::new(true);
            for blk in encoded_blocks {
                ref_batcher.push_block(blk);
            }
            assert_eq!(frames, ref_batcher.finish());
        }
        assert!(cache.open_count(SettledSegmentKind::Shielded) >= 3);
        assert_eq!(cache.open_count(SettledSegmentKind::Supplement), 0);
        assert_eq!(cache.open_count(SettledSegmentKind::RawBlock), 0);
        Ok(())
    }

    #[test]
    fn byte_budget_pool_subsets_and_cancellation_behave_accurately() -> TestOutcome {
        let dir = TempDir::new()?;
        let (db, entries, cache, _) = seed_multi_segment_store(dir.path(), 8, 2, &[])?;
        let ctx = SegmentStoreContext {
            db: &db,
            segment_cache: Some(&cache),
            sealed_segments: Some(&entries),
        };
        let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(8));

        let min_budget_req = EncodedCompactBlockRangeRequest {
            range,
            order: HeightOrder::Ascending,
            selection: ShieldedProjectionSelection::new(false, true, false, false),
            include_transparent: false,
            output: EncodedCompactBlockOutput::Blocks,
            max_bytes: 0,
            enforce_wire_fee_limit: true,
        };
        let single = encoded_compact_block_range_for_context(ctx, min_budget_req, None)?
            .ok_or("oversized first block must always be kept")?;
        assert_eq!(single.block_count, 1);
        assert_eq!(
            single.covered,
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1))
        );
        let EncodedCompactBlockPayload::Blocks(single_blocks) = single.payload else {
            return Err("expected Blocks variant".into());
        };
        let orchard_only = decode_blocks(&single_blocks)?;
        assert!(
            orchard_only[0]
                .vtx
                .iter()
                .all(|tx| tx.spends.is_empty() && tx.outputs.is_empty())
        );

        let partial_req = EncodedCompactBlockRangeRequest {
            range,
            order: HeightOrder::Descending,
            selection: ShieldedProjectionSelection::DEFAULT_NULLIFIERS,
            include_transparent: false,
            output: EncodedCompactBlockOutput::Blocks,
            max_bytes: 400,
            enforce_wire_fee_limit: true,
        };
        let bounded = encoded_compact_block_range_for_context(ctx, partial_req, None)?
            .ok_or("expected bounded descending slice")?;
        assert!((1..8).contains(&bounded.block_count));
        assert_eq!(bounded.first_link.height, BlockHeight::new(8));
        let EncodedCompactBlockPayload::Blocks(bounded_blocks) = bounded.payload else {
            return Err("expected Blocks variant".into());
        };
        let nullifiers = decode_blocks(&bounded_blocks)?;
        assert!(nullifiers.iter().all(|blk| {
            blk.vtx
                .iter()
                .all(|tx| tx.outputs.is_empty() && tx.actions.iter().all(|act| act.cmx.is_empty()))
        }));

        let cancel = AtomicBool::new(true);
        let cancelled = encoded_compact_block_range_for_context(ctx, partial_req, Some(&cancel));
        assert!(matches!(
            cancelled,
            Err(CanonicalStoreError::ReplayCancelled)
        ));
        Ok(())
    }

    #[test]
    fn wide_fee_and_tamper_surface_as_artifact_corrupt_across_settled_and_live() -> TestOutcome {
        let settled_wide_dir = TempDir::new()?;
        let (s_db, s_entries, s_cache, _) =
            seed_multi_segment_store(settled_wide_dir.path(), 4, 2, &[2])?;
        let s_ctx = SegmentStoreContext {
            db: &s_db,
            segment_cache: Some(&s_cache),
            sealed_segments: Some(&s_entries),
        };
        let settled_req = EncodedCompactBlockRangeRequest {
            range: BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(4)),
            order: HeightOrder::Ascending,
            selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
            include_transparent: false,
            output: EncodedCompactBlockOutput::Blocks,
            max_bytes: usize::MAX,
            enforce_wire_fee_limit: true,
        };
        assert!(matches!(
            encoded_compact_block_range_for_context(s_ctx, settled_req, None),
            Err(CanonicalStoreError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                ..
            })
        ));
        let relaxed_req = EncodedCompactBlockRangeRequest {
            enforce_wire_fee_limit: false,
            ..settled_req
        };
        assert_eq!(
            encoded_compact_block_range_for_context(s_ctx, relaxed_req, None)?
                .ok_or("relaxed read")?
                .block_count,
            4
        );

        let live_wide_dir = TempDir::new()?;
        let (l_db, l_entries, l_cache, _) =
            seed_multi_segment_store(live_wide_dir.path(), 4, 2, &[4])?;
        let l_ctx = SegmentStoreContext {
            db: &l_db,
            segment_cache: Some(&l_cache),
            sealed_segments: Some(&l_entries),
        };
        assert!(matches!(
            encoded_compact_block_range_for_context(l_ctx, settled_req, None),
            Err(CanonicalStoreError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                ..
            })
        ));

        l_cache.clear();
        let seg_name = format_segment_filename(
            SettledSegmentKind::Shielded,
            l_entries[0].start_height,
            l_entries[0].end_block.height,
        )?;
        let seg_file =
            segment_dir_path(live_wide_dir.path(), SettledSegmentKind::Shielded).join(seg_name);
        let mut file = OpenOptions::new().read(true).write(true).open(&seg_file)?;
        file.seek(SeekFrom::Start(200))?;
        file.write_all(&[0xff, 0xaa, 0x55])?;
        file.sync_all()?;
        assert!(matches!(
            encoded_compact_block_range_for_context(l_ctx, settled_req, None),
            Err(CanonicalStoreError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn reconstruction_skips_supplement_when_all_shielded_and_preserves_wide_fees() -> TestOutcome {
        let dir = TempDir::new()?;
        let (db, entries, cache, blocks) = seed_multi_segment_store(dir.path(), 4, 2, &[2])?;
        let shielded_only_readers = open_replay_segment_readers(&db, Some(&cache), &entries[1])?;
        let reconstructed_pure = reconstruct_segment_compact_range(
            &shielded_only_readers,
            BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(4)),
        )?;
        assert_eq!(reconstructed_pure, blocks[2..4]);

        let mixed_readers = open_replay_segment_readers(&db, Some(&cache), &entries[0])?;
        let reconstructed_mixed = reconstruct_segment_compact_range(
            &mixed_readers,
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2)),
        )?;
        assert_eq!(reconstructed_mixed, blocks[0..2]);
        Ok(())
    }

    #[test]
    fn default_range_serving_rejects_live_tail_mismatched_shielded_payload_with_valid_checksum()
    -> TestOutcome {
        let dir = TempDir::new()?;
        let (db, entries, cache, _) = seed_multi_segment_store(dir.path(), 5, 2, &[])?;
        let (compact_live, facts_live) = shielded_block_fixture(
            5,
            FixtureShape {
                sapling: true,
                orchard: false,
                transparent: false,
                wide_fee: false,
            },
        )?;
        let record =
            LiveTailBlockRecord::from_compact_block(&compact_live, true, Some(facts_live))?;
        let mut altered_proto =
            lightwalletd::CompactBlock::decode(record.shielded_payload.as_slice())?;
        altered_proto.hash = vec![0xab; 32];
        let tampered_row = super::super::live_tail::build_raw_v1_record_with_outer_checksum(
            &record,
            record.pool_presence,
            &altered_proto.encode_to_vec(),
        )?;
        let live_cf = db
            .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
            .ok_or("missing live_tail_block cf")?;
        db.put_cf(
            &live_cf,
            encode_height_key_ascending(BlockHeight::new(5)),
            tampered_row,
        )?;
        drop(live_cf);

        let ctx = SegmentStoreContext {
            db: &db,
            segment_cache: Some(&cache),
            sealed_segments: Some(&entries),
        };
        let req = EncodedCompactBlockRangeRequest {
            range: BlockHeightRange::inclusive(BlockHeight::new(4), BlockHeight::new(5)),
            order: HeightOrder::Ascending,
            selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
            include_transparent: false,
            output: EncodedCompactBlockOutput::Blocks,
            max_bytes: usize::MAX,
            enforce_wire_fee_limit: true,
        };
        assert!(matches!(
            encoded_compact_block_range_for_context(ctx, req, None),
            Err(CanonicalStoreError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                ..
            })
        ));
        Ok(())
    }

    fn seed_transparent_heavy_live_tail_store(
        dir: &std::path::Path,
        block_count: u32,
        script_len: usize,
    ) -> TestOutcome<(DB, Vec<usize>, Vec<usize>)> {
        let mut opts = rust_rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let descriptors = CANONICAL_DATA_COLUMN_FAMILIES
            .into_iter()
            .map(|name| rust_rocksdb::ColumnFamilyDescriptor::new(name, opts.clone()))
            .collect::<Vec<_>>();
        let db = DB::open_cf_descriptors(&opts, dir, descriptors)?;
        let live_cf = db
            .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
            .ok_or("missing live_tail_block cf")?;

        let mut stored_row_lens = Vec::new();
        let mut shielded_lens = Vec::new();
        let padding_script = vec![0x6a_u8; script_len];

        for h in 1_u32..=block_count {
            let height = BlockHeight::new(h);
            let block_hash = hash_with_tag(h);
            let prev_hash = if h == 1 {
                Network::ZcashRegtest.genesis_hash()
            } else {
                hash_with_tag(h - 1)
            };
            let seed = u8::try_from(h)?;
            let compact = CompactBlockArtifact::new(
                BlockId::new(height, block_hash),
                prev_hash,
                1_700_000_000 + h,
                vec![
                    CompactTransaction {
                        index: 0,
                        transaction_id: TransactionId::from_bytes([seed; 32]),
                        data: CompactTransactionData {
                            fee_zat: Some(500),
                            sapling_spends: vec![CompactSaplingSpend {
                                nullifier: [seed; 32],
                            }],
                            ..Default::default()
                        },
                    },
                    CompactTransaction {
                        index: 1,
                        transaction_id: TransactionId::from_bytes([seed.wrapping_add(10); 32]),
                        data: CompactTransactionData {
                            fee_zat: Some(1_000),
                            transparent_outputs: vec![CompactTransparentOutput {
                                value_zat: 10_000,
                                script_pub_key: padding_script.clone(),
                            }],
                            ..Default::default()
                        },
                    },
                ],
                CompactChainMetadata {
                    sapling_commitment_tree_size: h,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            )?;
            let facts = build_facts_for_compact(&compact);
            let (encoded, shielded_len) = LiveTailBlockRecord::encode_from_compact_block_v1(
                &compact,
                Some(facts),
                LiveTailEncodeOptions {
                    include_supplement: true,
                    retain_canonical_facts: true,
                },
            )?;
            stored_row_lens.push(encoded.len());
            shielded_lens.push(shielded_len);
            db.put_cf(&live_cf, encode_height_key_ascending(height), encoded)?;
        }
        drop(live_cf);
        Ok((db, stored_row_lens, shielded_lens))
    }

    #[test]
    fn transparent_heavy_live_tail_bounds_full_row_reads() -> TestOutcome {
        let dir = TempDir::new()?;
        let (db, stored_row_lens, shielded_lens) =
            seed_transparent_heavy_live_tail_store(dir.path(), 6, 16 * 1024)?;

        let min_stored = stored_row_lens.iter().copied().min().unwrap_or(0);
        let max_shielded = shielded_lens.iter().copied().max().unwrap_or(0);
        assert!(min_stored > max_shielded * 50);

        let ctx = SegmentStoreContext {
            db: &db,
            segment_cache: None,
            sealed_segments: Some(&[]),
        };
        let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(6));

        for order in [HeightOrder::Ascending, HeightOrder::Descending] {
            let one_row_budget = min_stored / 3;
            assert!(one_row_budget * LIVE_TAIL_READ_BUDGET_MULTIPLIER < min_stored * 2);
            assert!(one_row_budget > max_shielded * 10);
            let single = encoded_compact_block_range_for_context(
                ctx,
                EncodedCompactBlockRangeRequest {
                    range,
                    order,
                    selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                    include_transparent: false,
                    output: EncodedCompactBlockOutput::Blocks,
                    max_bytes: one_row_budget,
                    enforce_wire_fee_limit: true,
                },
                None,
            )?
            .ok_or("expected 1 block")?;
            assert_eq!(single.block_count, 1);

            let two_rows = match order {
                HeightOrder::Ascending => stored_row_lens[0] + stored_row_lens[1],
                HeightOrder::Descending => stored_row_lens[5] + stored_row_lens[4],
            };
            let two_row_budget = two_rows.div_ceil(LIVE_TAIL_READ_BUDGET_MULTIPLIER);
            assert!(two_row_budget * LIVE_TAIL_READ_BUDGET_MULTIPLIER < two_rows + min_stored);
            assert!(two_row_budget > max_shielded * 20);
            let two_blocks = encoded_compact_block_range_for_context(
                ctx,
                EncodedCompactBlockRangeRequest {
                    range,
                    order,
                    selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                    include_transparent: false,
                    output: EncodedCompactBlockOutput::Blocks,
                    max_bytes: two_row_budget,
                    enforce_wire_fee_limit: true,
                },
                None,
            )?
            .ok_or("expected 2 blocks")?;
            assert_eq!(two_blocks.block_count, 2);
        }
        Ok(())
    }

    #[test]
    fn range_accumulator_sizes_live_tail_batches_by_stored_row_size() {
        let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(6));
        let link1 = EncodedCompactBlockLink {
            height: BlockHeight::new(1),
            block_hash: hash_with_tag(1),
            previous_block_hash: Network::ZcashRegtest.genesis_hash(),
        };

        let transparent_req = EncodedCompactBlockRangeRequest {
            range,
            order: HeightOrder::Ascending,
            selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
            include_transparent: false,
            output: EncodedCompactBlockOutput::Blocks,
            max_bytes: 16 * 1024,
            enforce_wire_fee_limit: true,
        };
        let mut live_acc = RangeAccumulator::new(transparent_req);
        live_acc.record_live_tail_row(link1, 80, 16 * 1024);
        assert_eq!(live_acc.remaining_live_read_bytes(), 48 * 1024);
        assert_eq!(live_acc.largest_live_row_bytes, 16 * 1024);
        assert_eq!(live_acc.next_live_tail_batch_len(), 3);

        let shielded_req = EncodedCompactBlockRangeRequest {
            max_bytes: 64 * 1024,
            ..transparent_req
        };
        let mut shielded_acc = RangeAccumulator::new(shielded_req);
        shielded_acc.record_live_tail_row(link1, 40 * 1024, 41 * 1024);
        assert_eq!(shielded_acc.next_live_tail_batch_len(), 1);

        let mut settled_acc = RangeAccumulator::new(shielded_req);
        settled_acc.record_settled_span(link1, link1, 4, 1_024);
        assert_eq!(settled_acc.largest_live_row_bytes, 0);
        assert_eq!(settled_acc.next_live_tail_batch_len(), 1);
    }

    fn seed_wide_fee_live_tail_store(dir: &std::path::Path, wide_height: u32) -> TestOutcome<DB> {
        let mut opts = rust_rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let descriptors = CANONICAL_DATA_COLUMN_FAMILIES
            .into_iter()
            .map(|name| rust_rocksdb::ColumnFamilyDescriptor::new(name, opts.clone()))
            .collect::<Vec<_>>();
        let db = DB::open_cf_descriptors(&opts, dir, descriptors)?;
        let live_cf = db
            .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
            .ok_or("missing live_tail_block cf")?;

        for h in 1_u32..=3 {
            let mut tx = build_fixture_transactions(
                h,
                FixtureShape {
                    sapling: false,
                    orchard: true,
                    transparent: false,
                    wide_fee: h == wide_height,
                },
            );
            let seed = u8::try_from(h % 200).unwrap_or(1);
            for offset in 1_u8..8 {
                tx[0].data.orchard_actions.push(CompactShieldedAction {
                    nullifier: [seed.wrapping_add(offset); 32],
                    commitment: [seed.wrapping_add(offset + 1); 32],
                    ephemeral_key: [seed.wrapping_add(offset + 2); 32],
                    ciphertext: [seed.wrapping_add(offset + 3); 52],
                });
            }
            let parent_hash = if h == 1 {
                Network::ZcashRegtest.genesis_hash()
            } else {
                hash_with_tag(h - 1)
            };
            let compact = CompactBlockArtifact::new(
                BlockId::new(BlockHeight::new(h), hash_with_tag(h)),
                parent_hash,
                1_700_000_000 + h,
                tx,
                CompactChainMetadata {
                    sapling_commitment_tree_size: h * 2,
                    orchard_commitment_tree_size: h * 8,
                    ironwood_commitment_tree_size: 0,
                },
            )?;
            let facts = build_facts_for_compact(&compact);
            let record = LiveTailBlockRecord::from_compact_block(&compact, true, Some(facts))?;
            db.put_cf(
                &live_cf,
                encode_height_key_ascending(BlockHeight::new(h)),
                record.encode_v1()?,
            )?;
        }
        drop(live_cf);
        Ok(db)
    }

    #[test]
    fn out_of_budget_wide_fee_live_tail_block_returns_valid_prefix_before_failing() -> TestOutcome {
        let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(3));
        for (wide_height, prefix_range, failing_height, order) in [
            (
                3_u32,
                BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2)),
                BlockHeight::new(3),
                HeightOrder::Ascending,
            ),
            (
                1_u32,
                BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(3)),
                BlockHeight::new(1),
                HeightOrder::Descending,
            ),
        ] {
            let dir = TempDir::new()?;
            let db = seed_wide_fee_live_tail_store(dir.path(), wide_height)?;
            let ctx = SegmentStoreContext {
                db: &db,
                segment_cache: None,
                sealed_segments: Some(&[]),
            };
            let prefix = encoded_compact_block_range_for_context(
                ctx,
                EncodedCompactBlockRangeRequest {
                    range: prefix_range,
                    order,
                    selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                    include_transparent: false,
                    output: EncodedCompactBlockOutput::Blocks,
                    max_bytes: usize::MAX,
                    enforce_wire_fee_limit: true,
                },
                None,
            )?
            .ok_or("expected valid prefix before wide-fee block")?;
            let EncodedCompactBlockPayload::Blocks(prefix_blocks) = prefix.payload else {
                return Err("expected Blocks variant".into());
            };
            let prefix_bytes: usize = prefix_blocks.iter().map(|blk| blk.payload().len()).sum();

            // Leave 1 byte of headroom so the loop reads the wide-fee live-tail
            // block and hits `exceeds_output_budget` rather than `is_budget_spent`.
            let bounded = encoded_compact_block_range_for_context(
                ctx,
                EncodedCompactBlockRangeRequest {
                    range,
                    order,
                    selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                    include_transparent: false,
                    output: EncodedCompactBlockOutput::Blocks,
                    max_bytes: prefix_bytes + 1,
                    enforce_wire_fee_limit: true,
                },
                None,
            )?
            .ok_or("expected bounded prefix stopping before wide-fee block")?;
            assert_eq!(bounded.block_count, 2);
            assert_eq!(bounded.covered, prefix_range);

            let wide_req = EncodedCompactBlockRangeRequest {
                range: BlockHeightRange::inclusive(failing_height, failing_height),
                order,
                selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                include_transparent: false,
                output: EncodedCompactBlockOutput::Blocks,
                max_bytes: prefix_bytes + 1,
                enforce_wire_fee_limit: true,
            };
            assert!(matches!(
                encoded_compact_block_range_for_context(ctx, wide_req, None),
                Err(CanonicalStoreError::ArtifactCorrupt {
                    family: ArtifactFamily::CompactBlock,
                    ..
                })
            ));
        }
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum FixtureBlockKind {
        ShieldedOnly,
        TransparentOnly,
        MixedAllPools,
        ShieldedWithFee,
        ComponentlessTx,
        EmptyBlock,
    }

    fn splice_fixture_action(tag: u8) -> CompactShieldedAction {
        CompactShieldedAction {
            nullifier: [tag; 32],
            commitment: [tag.wrapping_add(1); 32],
            ephemeral_key: [tag.wrapping_add(2); 32],
            ciphertext: [tag.wrapping_add(3); 52],
        }
    }

    fn fixture_txs_for_kind(
        height_num: u32,
        kind: FixtureBlockKind,
        seed: u8,
    ) -> Vec<CompactTransaction> {
        let tx_a = TransactionId::from_bytes([seed; 32]);
        let tx_b = TransactionId::from_bytes([seed.wrapping_add(31); 32]);
        let mixed_second = CompactTransaction {
            index: 1,
            transaction_id: tx_b,
            data: CompactTransactionData {
                fee_zat: Some(777),
                sapling_spends: vec![CompactSaplingSpend {
                    nullifier: [seed.wrapping_add(2); 32],
                }],
                orchard_actions: vec![splice_fixture_action(seed.wrapping_add(6))],
                ironwood_actions: vec![splice_fixture_action(seed.wrapping_add(10))],
                transparent_inputs: vec![zinder_core::CompactTransparentInput {
                    previous_transaction_id: TransactionId::from_bytes([seed.wrapping_add(14); 32]),
                    previous_output_index: 2,
                }],
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 90_000,
                    script_pub_key: vec![0x00, 0x14, seed],
                }],
                ..Default::default()
            },
        };
        let first_data = match kind {
            FixtureBlockKind::ShieldedOnly => CompactTransactionData {
                fee_zat: None,
                sapling_spends: vec![CompactSaplingSpend {
                    nullifier: [seed; 32],
                }],
                sapling_outputs: vec![CompactSaplingOutput {
                    commitment: [seed.wrapping_add(1); 32],
                    ephemeral_key: [seed.wrapping_add(2); 32],
                    ciphertext: [seed.wrapping_add(3); 52],
                }],
                orchard_actions: vec![splice_fixture_action(seed.wrapping_add(4))],
                ironwood_actions: vec![splice_fixture_action(seed.wrapping_add(8))],
                ..Default::default()
            },
            FixtureBlockKind::TransparentOnly => CompactTransactionData {
                fee_zat: Some(250 + u64::from(height_num)),
                transparent_inputs: vec![zinder_core::CompactTransparentInput {
                    previous_transaction_id: TransactionId::from_bytes([seed.wrapping_add(12); 32]),
                    previous_output_index: height_num % 5,
                }],
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 50_000 + u64::from(height_num),
                    script_pub_key: vec![0x76, 0xa9, seed, 0x88, 0xac],
                }],
                ..Default::default()
            },
            FixtureBlockKind::ShieldedWithFee => CompactTransactionData {
                fee_zat: Some(333),
                orchard_actions: vec![splice_fixture_action(seed.wrapping_add(15))],
                ..Default::default()
            },
            FixtureBlockKind::MixedAllPools | FixtureBlockKind::ComponentlessTx => {
                CompactTransactionData::default()
            }
            FixtureBlockKind::EmptyBlock => return Vec::new(),
        };
        let mut out = vec![CompactTransaction {
            index: 0,
            transaction_id: tx_a,
            data: first_data,
        }];
        if matches!(kind, FixtureBlockKind::MixedAllPools) {
            out.push(mixed_second);
        }
        out
    }

    fn build_splice_fixture_block(
        height_num: u32,
        kind: FixtureBlockKind,
        parent_hash: BlockHash,
    ) -> TestOutcome<(CompactBlockArtifact, CanonicalBlockFacts)> {
        let height = BlockHeight::new(height_num);
        let block_hash = hash_with_tag(height_num);
        let seed = u8::try_from(height_num % 200)
            .unwrap_or(1)
            .saturating_add(10);
        let transactions = fixture_txs_for_kind(height_num, kind, seed);
        let compact = CompactBlockArtifact::new(
            BlockId::new(height, block_hash),
            parent_hash,
            1_700_000_000 + height_num,
            transactions,
            CompactChainMetadata {
                sapling_commitment_tree_size: height_num * 2,
                orchard_commitment_tree_size: height_num * 3,
                ironwood_commitment_tree_size: height_num,
            },
        )?;
        let facts = build_facts_for_compact(&compact);
        Ok((compact, facts))
    }

    fn append_live_tail_splice_blocks(
        db: &DB,
        mut parent: BlockHash,
        artifacts: &mut Vec<CompactBlockArtifact>,
    ) -> TestOutcome {
        let live_cf = db
            .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
            .ok_or("missing live_tail_block cf")?;
        for h in 21_u32..=24 {
            let kind = match h {
                21 => FixtureBlockKind::ShieldedOnly,
                22 => FixtureBlockKind::TransparentOnly,
                23 => FixtureBlockKind::MixedAllPools,
                _ => FixtureBlockKind::ComponentlessTx,
            };
            let (compact, facts) = build_splice_fixture_block(h, kind, parent)?;
            parent = compact.block_hash();
            artifacts.push(compact.clone());
            let record = LiveTailBlockRecord::from_compact_block(&compact, true, Some(facts))?;
            db.put_cf(
                &live_cf,
                encode_height_key_ascending(BlockHeight::new(h)),
                record.encode_v1()?,
            )?;
        }
        Ok(())
    }

    fn seed_spliced_settled_and_live_store(
        dir: &std::path::Path,
        settled_wide_fee: Option<u32>,
    ) -> TestOutcome<(DB, Vec<CompactBlockArtifact>, SettledSegmentIndexEntry)> {
        use crate::canonical_store::rocksdb::SETTLED_SEGMENT_INDEX_COLUMN_FAMILY;
        use crate::canonical_store::settled_segments::{
            layout::SettledSegmentModules,
            sealer::{SettledSegmentBlockInput, SettledSegmentSealer, SettledSegmentSealerConfig},
        };
        use crate::settled_supplement_segment::SettledSupplementSegmentWriteOptions;

        let mut opts = rust_rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let descriptors = CANONICAL_DATA_COLUMN_FAMILIES
            .into_iter()
            .map(|name| rust_rocksdb::ColumnFamilyDescriptor::new(name, opts.clone()))
            .collect::<Vec<_>>();
        let db = DB::open_cf_descriptors(&opts, dir, descriptors)?;
        let sealer_config = SettledSegmentSealerConfig::new(
            Network::ZcashRegtest,
            crate::CURRENT_ARTIFACT_SCHEMA_VERSION.value(),
            SettledSegmentModules {
                supplement: true,
                raw_bodies: false,
            },
        )
        .with_segment_blocks(20)
        .with_supplement_options(SettledSupplementSegmentWriteOptions::default());
        let mut sealer = SettledSegmentSealer::new(dir, sealer_config, None)?;

        let mut artifacts = Vec::with_capacity(24);
        let mut parent = Network::ZcashRegtest.genesis_hash();
        let mut entry = None;
        for h in 1_u32..=20 {
            let kind = match h {
                1..=16 => FixtureBlockKind::ShieldedOnly,
                17 => FixtureBlockKind::TransparentOnly,
                18 => FixtureBlockKind::MixedAllPools,
                19 => FixtureBlockKind::ShieldedWithFee,
                _ => FixtureBlockKind::EmptyBlock,
            };
            let (mut compact, mut facts) = build_splice_fixture_block(h, kind, parent)?;
            if settled_wide_fee == Some(h) {
                let mut txs = compact.transactions().to_vec();
                if let Some(first_tx) = txs.first_mut() {
                    first_tx.data.fee_zat = Some(u64::from(u32::MAX) + 1);
                }
                compact = CompactBlockArtifact::new(
                    BlockId::new(compact.height(), compact.block_hash()),
                    compact.previous_block_hash(),
                    compact.time(),
                    txs,
                    compact.chain_metadata(),
                )?;
                facts = build_facts_for_compact(&compact);
            }
            parent = compact.block_hash();
            artifacts.push(compact.clone());
            if let Some(sealed) = sealer
                .push_block(SettledSegmentBlockInput {
                    compact_block: compact,
                    native_bytes: 512,
                    canonical_facts: facts,
                    raw_block: None,
                })?
                .into_iter()
                .last()
            {
                entry = Some(sealed);
            }
        }
        let settled_entry = entry.ok_or("expected 20-block settled segment")?;
        let index_cf = db
            .cf_handle(SETTLED_SEGMENT_INDEX_COLUMN_FAMILY)
            .ok_or("missing settled_segment_index cf")?;
        db.put_cf(
            &index_cf,
            encode_height_key_ascending(settled_entry.start_height),
            settled_entry.encode_v1()?,
        )?;
        drop(index_cf);
        append_live_tail_splice_blocks(&db, parent, &mut artifacts)?;
        Ok((db, artifacts, settled_entry))
    }

    fn reference_action_proto(
        action: &CompactShieldedAction,
        nullifiers_only: bool,
    ) -> zinder_proto::compat::lightwalletd::CompactOrchardAction {
        if nullifiers_only {
            zinder_proto::compat::lightwalletd::CompactOrchardAction {
                nullifier: action.nullifier.to_vec(),
                ..Default::default()
            }
        } else {
            zinder_proto::compat::lightwalletd::CompactOrchardAction {
                nullifier: action.nullifier.to_vec(),
                cmx: action.commitment.to_vec(),
                ephemeral_key: action.ephemeral_key.to_vec(),
                ciphertext: action.ciphertext.to_vec(),
            }
        }
    }

    fn reference_transparent_bundles(
        scan: &CompactTransactionData,
        enabled: bool,
    ) -> (
        Vec<zinder_proto::compat::lightwalletd::CompactTxIn>,
        Vec<zinder_proto::compat::lightwalletd::TxOut>,
    ) {
        use zinder_core::wire::encode_internal_transaction_id;
        use zinder_proto::compat::lightwalletd::{CompactTxIn, TxOut};
        if !enabled {
            return (Vec::new(), Vec::new());
        }
        let vin = scan
            .transparent_inputs
            .iter()
            .map(|inp| CompactTxIn {
                prevout_txid: encode_internal_transaction_id(inp.previous_transaction_id).to_vec(),
                prevout_index: inp.previous_output_index,
            })
            .collect();
        let vout = scan
            .transparent_outputs
            .iter()
            .map(|out| TxOut {
                value: out.value_zat,
                script_pub_key: out.script_pub_key.clone(),
            })
            .collect();
        (vin, vout)
    }

    fn reference_shielded_bundles(
        scan: &CompactTransactionData,
        projection: ShieldedProjectionSelection,
    ) -> (
        Vec<zinder_proto::compat::lightwalletd::CompactSaplingSpend>,
        Vec<zinder_proto::compat::lightwalletd::CompactSaplingOutput>,
        Vec<zinder_proto::compat::lightwalletd::CompactOrchardAction>,
        Vec<zinder_proto::compat::lightwalletd::CompactOrchardAction>,
    ) {
        use zinder_proto::compat::lightwalletd::{
            CompactSaplingOutput as LwdSaplingOut, CompactSaplingSpend as LwdSaplingSpend,
        };
        let spends = if projection.sapling {
            scan.sapling_spends
                .iter()
                .map(|spend| LwdSaplingSpend {
                    nf: spend.nullifier.to_vec(),
                })
                .collect()
        } else {
            Vec::new()
        };
        let outputs = if projection.sapling && !projection.nullifiers_only {
            scan.sapling_outputs
                .iter()
                .map(|out| LwdSaplingOut {
                    cmu: out.commitment.to_vec(),
                    ephemeral_key: out.ephemeral_key.to_vec(),
                    ciphertext: out.ciphertext.to_vec(),
                })
                .collect()
        } else {
            Vec::new()
        };
        let map_actions = |list: &[CompactShieldedAction]| {
            list.iter()
                .map(|act| reference_action_proto(act, projection.nullifiers_only))
                .collect()
        };
        let actions = if projection.orchard {
            map_actions(&scan.orchard_actions)
        } else {
            Vec::new()
        };
        let ironwood = if projection.ironwood {
            map_actions(&scan.ironwood_actions)
        } else {
            Vec::new()
        };
        (spends, outputs, actions, ironwood)
    }

    fn reference_compact_tx(
        tx: &CompactTransaction,
        projection: ShieldedProjectionSelection,
        include_transparent: bool,
        retain_all_transactions: bool,
    ) -> TestOutcome<Option<zinder_proto::compat::lightwalletd::CompactTx>> {
        use zinder_core::wire::encode_internal_transaction_id;
        use zinder_proto::compat::lightwalletd::CompactTx;

        let scan = &tx.data;
        let show_transparent = include_transparent && !projection.nullifiers_only;
        let keep = retain_all_transactions
            || (projection.sapling
                && (!scan.sapling_spends.is_empty() || !scan.sapling_outputs.is_empty()))
            || (projection.orchard && !scan.orchard_actions.is_empty())
            || (projection.ironwood && !scan.ironwood_actions.is_empty())
            || (show_transparent
                && (!scan.transparent_outputs.is_empty() || !scan.transparent_inputs.is_empty()));
        if !keep {
            return Ok(None);
        }
        let fee = scan
            .fee_zat
            .map(u32::try_from)
            .transpose()?
            .unwrap_or_default();
        let (spends, outputs, actions, ironwood_actions) =
            reference_shielded_bundles(scan, projection);
        let (vin, vout) = reference_transparent_bundles(scan, show_transparent);
        Ok(Some(CompactTx {
            index: tx.index,
            txid: encode_internal_transaction_id(tx.transaction_id).to_vec(),
            fee,
            spends,
            outputs,
            actions,
            vin,
            vout,
            ironwood_actions,
        }))
    }

    fn reference_wire_bytes(
        artifact: &CompactBlockArtifact,
        projection: ShieldedProjectionSelection,
        include_transparent: bool,
        retain_all_transactions: bool,
    ) -> TestOutcome<Vec<u8>> {
        use prost::Message as _;
        use zinder_proto::compat::lightwalletd::{ChainMetadata, CompactBlock as LwdCompactBlock};

        let mut vtx = Vec::new();
        for tx in artifact.transactions() {
            if let Some(proto_tx) =
                reference_compact_tx(tx, projection, include_transparent, retain_all_transactions)?
            {
                vtx.push(proto_tx);
            }
        }
        let meta = artifact.chain_metadata();
        let chain_metadata = Some(if projection.nullifiers_only {
            ChainMetadata::default()
        } else {
            ChainMetadata {
                sapling_commitment_tree_size: meta.sapling_commitment_tree_size,
                orchard_commitment_tree_size: meta.orchard_commitment_tree_size,
                ironwood_commitment_tree_size: meta.ironwood_commitment_tree_size,
            }
        });
        let msg = LwdCompactBlock {
            height: u64::from(artifact.height().value()),
            hash: encode_internal_block_hash(artifact.block_hash()).to_vec(),
            prev_hash: encode_internal_block_hash(artifact.previous_block_hash()).to_vec(),
            time: artifact.time(),
            header: Vec::new(),
            vtx,
            chain_metadata,
        };
        Ok(msg.encode_to_vec())
    }

    fn expected_range_payloads(
        artifacts: &[CompactBlockArtifact],
        range: BlockHeightRange,
        order: HeightOrder,
        projection: ShieldedProjectionSelection,
        include_transparent: bool,
    ) -> TestOutcome<Vec<Vec<u8>>> {
        let low = usize::try_from(range.start.value() - 1)?;
        let high = usize::try_from(range.end.value() - 1)?;
        let mut slice = artifacts[low..=high]
            .iter()
            .map(|blk| reference_wire_bytes(blk, projection, include_transparent, false))
            .collect::<TestOutcome<Vec<_>>>()?;
        if order == HeightOrder::Descending {
            slice.reverse();
        }
        Ok(slice)
    }

    #[test]
    fn range_with_transparent_matches_reference_across_pool_subsets_orders_and_outputs()
    -> TestOutcome {
        let dir = TempDir::new()?;
        let (db, artifacts, _) = seed_spliced_settled_and_live_store(dir.path(), None)?;
        let ctx = SegmentStoreContext {
            db: &db,
            segment_cache: None,
            sealed_segments: None,
        };
        let selections = [
            ShieldedProjectionSelection::new(false, false, false, false),
            ShieldedProjectionSelection::new(true, false, false, false),
            ShieldedProjectionSelection::new(false, true, false, false),
            ShieldedProjectionSelection::new(true, true, false, false),
            ShieldedProjectionSelection::DEFAULT_SHIELDED,
        ];
        for span in [
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(24)),
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(20)),
            BlockHeightRange::inclusive(BlockHeight::new(17), BlockHeight::new(24)),
        ] {
            for selection in selections {
                for order in [HeightOrder::Ascending, HeightOrder::Descending] {
                    let expected =
                        expected_range_payloads(&artifacts, span, order, selection, true)?;
                    let blocks_resp = encoded_compact_block_range_for_context(
                        ctx,
                        EncodedCompactBlockRangeRequest {
                            range: span,
                            order,
                            selection,
                            include_transparent: true,
                            output: EncodedCompactBlockOutput::Blocks,
                            max_bytes: usize::MAX,
                            enforce_wire_fee_limit: true,
                        },
                        None,
                    )?
                    .ok_or("expected Blocks response")?;
                    let EncodedCompactBlockPayload::Blocks(actual_blocks) = blocks_resp.payload
                    else {
                        return Err("expected Blocks variant".into());
                    };
                    let actual_wire: Vec<Vec<u8>> = actual_blocks
                        .iter()
                        .map(|blk| blk.payload().to_vec())
                        .collect();
                    assert_eq!(actual_wire, expected);

                    let frames_resp = encoded_compact_block_range_for_context(
                        ctx,
                        EncodedCompactBlockRangeRequest {
                            range: span,
                            order,
                            selection,
                            include_transparent: true,
                            output: EncodedCompactBlockOutput::BatchedFrames {
                                opening_frame: false,
                            },
                            max_bytes: usize::MAX,
                            enforce_wire_fee_limit: true,
                        },
                        None,
                    )?
                    .ok_or("expected BatchedFrames response")?;
                    let EncodedCompactBlockPayload::BatchedFrames(frames) = frames_resp.payload
                    else {
                        return Err("expected BatchedFrames variant".into());
                    };
                    let mut ref_batcher = EncodedBlockBatcher::new(false);
                    for blk in actual_blocks {
                        ref_batcher.push_block(blk);
                    }
                    assert_eq!(frames, ref_batcher.finish());
                }
            }
        }
        assert_spliced_range_budget_cancel_and_wide_fee(ctx, &artifacts)
    }

    fn assert_spliced_range_budget_cancel_and_wide_fee(
        ctx: SegmentStoreContext<'_>,
        artifacts: &[CompactBlockArtifact],
    ) -> TestOutcome {
        let bounded = encoded_compact_block_range_for_context(
            ctx,
            EncodedCompactBlockRangeRequest {
                range: BlockHeightRange::inclusive(BlockHeight::new(16), BlockHeight::new(24)),
                order: HeightOrder::Ascending,
                selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                include_transparent: true,
                output: EncodedCompactBlockOutput::Blocks,
                max_bytes: 1_100,
                enforce_wire_fee_limit: true,
            },
            None,
        )?
        .ok_or("expected bounded spliced prefix")?;
        assert!(bounded.block_count >= 1 && bounded.covered.end < BlockHeight::new(24));
        let expected = expected_range_payloads(
            artifacts,
            bounded.covered,
            HeightOrder::Ascending,
            ShieldedProjectionSelection::DEFAULT_SHIELDED,
            true,
        )?;
        let EncodedCompactBlockPayload::Blocks(actual) = bounded.payload else {
            return Err("expected Blocks variant".into());
        };
        let wire: Vec<Vec<u8>> = actual.iter().map(|blk| blk.payload().to_vec()).collect();
        assert_eq!(wire, expected);

        let cancel = AtomicBool::new(true);
        assert!(matches!(
            encoded_compact_block_range_for_context(
                ctx,
                EncodedCompactBlockRangeRequest {
                    range: BlockHeightRange::inclusive(BlockHeight::new(17), BlockHeight::new(23)),
                    order: HeightOrder::Descending,
                    selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                    include_transparent: true,
                    output: EncodedCompactBlockOutput::Blocks,
                    max_bytes: usize::MAX,
                    enforce_wire_fee_limit: true,
                },
                Some(&cancel),
            ),
            Err(CanonicalStoreError::ReplayCancelled)
        ));

        let wide_dir = TempDir::new()?;
        let (wide_db, _, _) = seed_spliced_settled_and_live_store(wide_dir.path(), Some(18))?;
        let wide_ctx = SegmentStoreContext {
            db: &wide_db,
            segment_cache: None,
            sealed_segments: None,
        };
        let err = encoded_compact_block_range_for_context(
            wide_ctx,
            EncodedCompactBlockRangeRequest {
                range: BlockHeightRange::inclusive(BlockHeight::new(17), BlockHeight::new(20)),
                order: HeightOrder::Ascending,
                selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                include_transparent: true,
                output: EncodedCompactBlockOutput::Blocks,
                max_bytes: usize::MAX,
                enforce_wire_fee_limit: true,
            },
            None,
        );
        assert!(matches!(
            err,
            Err(CanonicalStoreError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                ..
            })
        ));
        Ok(())
    }

    fn assert_unary_parity_at(
        ctx: SegmentStoreContext<'_>,
        height: BlockHeight,
        artifact: &CompactBlockArtifact,
    ) -> TestOutcome {
        for selection in [
            CompactBlockSpliceSelection::FULL_BLOCK,
            CompactBlockSpliceSelection::BLOCK_NULLIFIERS,
        ] {
            let encoded = encoded_compact_block_at_for_context(ctx, height, selection)?
                .ok_or("expected unary compact block")?;
            let expected = reference_wire_bytes(
                artifact,
                ShieldedProjectionSelection::new(true, true, true, selection.nullifiers_only),
                selection.include_transparent,
                selection.retain_all_transactions,
            )?;
            assert_eq!(encoded.payload().as_ref(), expected.as_slice());
        }
        Ok(())
    }

    #[test]
    fn unary_encoded_compact_block_matches_reference_and_skips_shielded_only_supplement_chunks()
    -> TestOutcome {
        use crate::canonical_store::settled_segments::{
            cache::SettledSegmentReaderCache, layout::SettledSegmentKind,
        };

        let dir = TempDir::new()?;
        let (db, artifacts, entry) = seed_spliced_settled_and_live_store(dir.path(), None)?;
        let cache = SettledSegmentReaderCache::with_default_capacity(dir.path(), false)?;
        let ctx = SegmentStoreContext {
            db: &db,
            segment_cache: Some(&cache),
            sealed_segments: None,
        };

        let _ = encoded_compact_block_at_for_context(
            ctx,
            BlockHeight::new(1),
            CompactBlockSpliceSelection::DEFAULT_SHIELDED,
        )?
        .ok_or("expected height 1 DEFAULT_SHIELDED")?;
        assert_eq!(cache.open_count(SettledSegmentKind::Supplement), 0);

        for height_val in [1_u32, 16_u32] {
            assert_unary_parity_at(
                ctx,
                BlockHeight::new(height_val),
                &artifacts[usize::try_from(height_val - 1)?],
            )?;
        }
        let sup_reader = cache
            .get_or_open_supplement(&entry)?
            .ok_or("expected cached supplement reader")?;
        assert!(sup_reader.chunk_is_unvalidated_at(BlockHeight::new(1))?);
        assert!(sup_reader.chunk_is_unvalidated_at(BlockHeight::new(16))?);

        for (idx, artifact) in artifacts.iter().enumerate() {
            assert_unary_parity_at(ctx, BlockHeight::new(u32::try_from(idx + 1)?), artifact)?;
        }
        assert!(
            encoded_compact_block_at_for_context(
                ctx,
                BlockHeight::new(25),
                CompactBlockSpliceSelection::FULL_BLOCK,
            )?
            .is_none()
        );

        let wide_dir = TempDir::new()?;
        let wide_db = seed_wide_fee_live_tail_store(wide_dir.path(), 2)?;
        let wide_ctx = SegmentStoreContext {
            db: &wide_db,
            segment_cache: None,
            sealed_segments: Some(&[]),
        };
        for selection in [
            CompactBlockSpliceSelection::FULL_BLOCK,
            CompactBlockSpliceSelection::BLOCK_NULLIFIERS,
        ] {
            assert!(matches!(
                encoded_compact_block_at_for_context(wide_ctx, BlockHeight::new(2), selection),
                Err(CanonicalStoreError::ArtifactCorrupt {
                    family: ArtifactFamily::CompactBlock,
                    ..
                })
            ));
        }
        Ok(())
    }
}
