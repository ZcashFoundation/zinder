//! Zero-allocation protobuf wire projection and validation for `ZSHRNG02`.

use prost::{
    Message,
    encoding::{WireType, decode_key, decode_varint, encode_key, encode_varint},
};
use zinder_core::{
    BlockHeight, CompactBlockArtifact, CompactShieldedAction,
    wire::{encode_internal_block_hash, encode_internal_transaction_id},
};
use zinder_proto::compat::lightwalletd;

use super::{
    PoolPresenceMask, SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES, SettledShieldedSegmentError,
    ShieldedProjectionSelection,
    format::{FRAME_PREFIX_LEN, encode_frame_prefix},
};

const COMPACT_BLOCK_TAG_HEIGHT: u32 = 2;
const COMPACT_BLOCK_TAG_HASH: u32 = 3;
const COMPACT_BLOCK_TAG_PREV_HASH: u32 = 4;
const COMPACT_BLOCK_TAG_TIME: u32 = 5;
const COMPACT_BLOCK_TAG_HEADER: u32 = 6;
const COMPACT_BLOCK_TAG_VTX: u32 = 7;
const COMPACT_BLOCK_TAG_CHAIN_METADATA: u32 = 8;

const COMPACT_TX_TAG_INDEX: u32 = 1;
const COMPACT_TX_TAG_TXID: u32 = 2;
const COMPACT_TX_TAG_FEE: u32 = 3;
const COMPACT_TX_TAG_SPENDS: u32 = 4;
const COMPACT_TX_TAG_OUTPUTS: u32 = 5;
const COMPACT_TX_TAG_ACTIONS: u32 = 6;
const COMPACT_TX_TAG_VIN: u32 = 7;
const COMPACT_TX_TAG_VOUT: u32 = 8;
const COMPACT_TX_TAG_IRONWOOD_ACTIONS: u32 = 9;

const ACTION_TAG_NULLIFIER: u32 = 1;
const ACTION_TAG_CMX: u32 = 2;
const ACTION_TAG_EPHEMERAL_KEY: u32 = 3;
const ACTION_TAG_CIPHERTEXT: u32 = 4;

const SAPLING_SPEND_TAG_NF: u32 = 1;

const SAPLING_OUTPUT_TAG_CMU: u32 = 1;
const SAPLING_OUTPUT_TAG_EPHEMERAL_KEY: u32 = 2;
const SAPLING_OUTPUT_TAG_CIPHERTEXT: u32 = 3;

const CHAIN_METADATA_TAG_SAPLING_TREE_SIZE: u32 = 1;
const CHAIN_METADATA_TAG_ORCHARD_TREE_SIZE: u32 = 2;
const CHAIN_METADATA_TAG_IRONWOOD_TREE_SIZE: u32 = 3;

const EMPTY_CHAIN_METADATA_WIRE: [u8; 2] = [0x42, 0x00];

/// Encoded `DEFAULT_RANGE` protobuf payload and row-flag metadata for one block.
pub(super) struct EncodedShieldedBlock {
    pub(super) payload: Vec<u8>,
    pub(super) pool_presence: PoolPresenceMask,
    pub(super) has_unrepresentable_wire_fee: bool,
}

/// Encodes one canonical [`CompactBlockArtifact`] into the `Slab 0`
/// `DEFAULT_RANGE` protobuf payload, tracking wide fees when supplement-backed.
pub(super) fn encode_default_shielded_block_with_options(
    block: &CompactBlockArtifact,
    allow_wide_fee_in_supplement: bool,
) -> Result<EncodedShieldedBlock, SettledShieldedSegmentError> {
    let mut presence = PoolPresenceMask::EMPTY;
    let mut vtx = Vec::new();
    let mut has_unrepresentable_wire_fee = false;
    for tx in block.transactions() {
        let scan = &tx.data;
        let wire_fee = match scan.fee_zat {
            Some(fee_zat) => match u32::try_from(fee_zat) {
                Ok(fee) => Some(fee),
                Err(_) if allow_wide_fee_in_supplement => None,
                Err(_) => {
                    return Err(SettledShieldedSegmentError::PayloadIntegrity(
                        "compact transaction fee exceeds u32".to_owned(),
                    ));
                }
            },
            None => Some(0),
        };
        let tx_mask = transaction_pool_mask(scan);
        presence = presence.union(tx_mask);
        if tx_mask.is_empty() {
            continue;
        }
        let fee = wire_fee.unwrap_or_else(|| {
            has_unrepresentable_wire_fee = true;
            0
        });
        vtx.push(lightwalletd::CompactTx {
            index: tx.index,
            txid: encode_internal_transaction_id(tx.transaction_id).to_vec(),
            fee,
            spends: encode_sapling_spends(&scan.sapling_spends),
            outputs: encode_sapling_outputs(&scan.sapling_outputs),
            actions: encode_shielded_actions(&scan.orchard_actions),
            ironwood_actions: encode_shielded_actions(&scan.ironwood_actions),
            vin: Vec::new(),
            vout: Vec::new(),
        });
    }
    let metadata = block.chain_metadata();
    let proto_block = lightwalletd::CompactBlock {
        height: u64::from(block.height().value()),
        hash: encode_internal_block_hash(block.block_hash()).to_vec(),
        prev_hash: encode_internal_block_hash(block.previous_block_hash()).to_vec(),
        time: block.time(),
        header: Vec::new(),
        vtx,
        chain_metadata: Some(lightwalletd::ChainMetadata {
            sapling_commitment_tree_size: metadata.sapling_commitment_tree_size,
            orchard_commitment_tree_size: metadata.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: metadata.ironwood_commitment_tree_size,
        }),
    };
    let payload = proto_block.encode_to_vec();
    if payload.len() > SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES {
        return Err(SettledShieldedSegmentError::PayloadLimitExceeded {
            block_height: block.height(),
            payload_bytes: payload.len(),
            max_bytes: SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES,
        });
    }
    Ok(EncodedShieldedBlock {
        payload,
        pool_presence: presence,
        has_unrepresentable_wire_fee,
    })
}

/// Appends one empty nullifier-only gRPC-framed `CompactBlock` straight from
/// the 48-byte in-memory row index without reading `Slab 0`.
pub(super) fn synthesize_empty_nullifier_block(
    height: BlockHeight,
    block_hash_wire: &[u8; 32],
    prev_hash_wire: &[u8; 32],
    block_time: u32,
    out: &mut Vec<u8>,
) -> Result<(), SettledShieldedSegmentError> {
    let frame_start = out.len();
    out.resize(frame_start + FRAME_PREFIX_LEN, 0);
    if height.value() != 0 {
        encode_key(COMPACT_BLOCK_TAG_HEIGHT, WireType::Varint, out);
        encode_varint(u64::from(height.value()), out);
    }
    out.extend_from_slice(&[0x1a, 0x20]);
    out.extend_from_slice(block_hash_wire);
    out.extend_from_slice(&[0x22, 0x20]);
    out.extend_from_slice(prev_hash_wire);
    if block_time != 0 {
        encode_key(COMPACT_BLOCK_TAG_TIME, WireType::Varint, out);
        encode_varint(u64::from(block_time), out);
    }
    out.extend_from_slice(&EMPTY_CHAIN_METADATA_WIRE);
    patch_frame_prefix(out, frame_start)
}

/// Projects one `CompactBlock` protobuf payload into a gRPC-framed message
/// according to `selection`, clearing `out` first.
///
/// # Errors
///
/// Returns [`SettledShieldedSegmentError::PayloadIntegrity`] on malformed wire
/// fields or unexpected transparent tags, or
/// [`SettledShieldedSegmentError::InvalidFormat`] if the framed message exceeds
/// the 4 GiB gRPC length prefix.
pub fn project_shielded_block_wire(
    raw_payload: &[u8],
    selection: ShieldedProjectionSelection,
    out: &mut Vec<u8>,
) -> Result<(), SettledShieldedSegmentError> {
    let mut tx_scratch = Vec::new();
    project_shielded_block_wire_with_scratch(raw_payload, selection, out, &mut tx_scratch)
}

/// Projects one `CompactBlock` protobuf payload into a gRPC-framed message in
/// `out` using caller-supplied `tx_scratch`, clearing `out` first.
///
/// # Errors
///
/// Returns [`SettledShieldedSegmentError::PayloadIntegrity`] on malformed wire
/// fields or unexpected transparent tags, or
/// [`SettledShieldedSegmentError::InvalidFormat`] if the framed message exceeds
/// the 4 GiB gRPC length prefix.
pub fn project_shielded_block_wire_with_scratch(
    raw_payload: &[u8],
    selection: ShieldedProjectionSelection,
    out: &mut Vec<u8>,
    tx_scratch: &mut Vec<u8>,
) -> Result<(), SettledShieldedSegmentError> {
    out.clear();
    project_shielded_block_wire_into(raw_payload, selection, out, tx_scratch)
}

/// Returns `Ok(true)` when `raw_payload` is a valid `CompactBlock` wire payload
/// with no `vtx` entries whose projected output under `selection` is
/// byte-for-byte identical to `raw_payload`.
///
/// # Errors
///
/// Returns [`SettledShieldedSegmentError::PayloadIntegrity`] if any visited
/// wire tag, varint, or length-delimited field in `raw_payload` is malformed.
pub fn is_passthrough_shielded_block_wire(
    raw_payload: &[u8],
    selection: ShieldedProjectionSelection,
) -> Result<bool, SettledShieldedSegmentError> {
    if raw_payload.is_empty()
        || raw_payload.len() > super::SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES
    {
        return Ok(false);
    }
    let mut cursor = raw_payload;
    let mut nullifier_meta_at_end = false;
    let mut chain_metadata_count = 0_usize;
    while !cursor.is_empty() {
        let field_start = cursor;
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            COMPACT_BLOCK_TAG_HEIGHT | COMPACT_BLOCK_TAG_TIME => {
                expect_wire_type(wire_type, WireType::Varint, "CompactBlock scalar")?;
                decode_varint(&mut cursor).map_err(|err| wire_error(&err))?;
                nullifier_meta_at_end = false;
            }
            COMPACT_BLOCK_TAG_HASH | COMPACT_BLOCK_TAG_PREV_HASH | COMPACT_BLOCK_TAG_HEADER => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactBlock bytes")?;
                let _bytes = take_length_delimited(&mut cursor)?;
                nullifier_meta_at_end = false;
            }
            COMPACT_BLOCK_TAG_CHAIN_METADATA => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "chain_metadata")?;
                let meta_bytes = take_length_delimited(&mut cursor)?;
                validate_chain_metadata_wire(meta_bytes)?;
                chain_metadata_count += 1;
                let consumed = field_start.len() - cursor.len();
                nullifier_meta_at_end =
                    cursor.is_empty() && field_start[..consumed] == EMPTY_CHAIN_METADATA_WIRE;
            }
            // COMPACT_BLOCK_TAG_VTX (tag 7) or any unknown field requires full projection.
            _ => return Ok(false),
        }
    }
    if selection.nullifiers_only {
        Ok(chain_metadata_count == 1 && nullifier_meta_at_end)
    } else {
        Ok(true)
    }
}

/// Appends one gRPC-framed projected `CompactBlock` into `out`, reusing `tx_scratch`.
pub(super) fn project_shielded_block_wire_into(
    raw_payload: &[u8],
    selection: ShieldedProjectionSelection,
    out: &mut Vec<u8>,
    tx_scratch: &mut Vec<u8>,
) -> Result<(), SettledShieldedSegmentError> {
    let frame_start = out.len();
    out.resize(frame_start + FRAME_PREFIX_LEN, 0);
    let mut cursor = raw_payload;
    while !cursor.is_empty() {
        let field_start = cursor;
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            COMPACT_BLOCK_TAG_HEIGHT | COMPACT_BLOCK_TAG_TIME => {
                expect_wire_type(wire_type, WireType::Varint, "CompactBlock scalar")?;
                decode_varint(&mut cursor).map_err(|err| wire_error(&err))?;
                copy_consumed(field_start, cursor, out);
            }
            COMPACT_BLOCK_TAG_HASH | COMPACT_BLOCK_TAG_PREV_HASH | COMPACT_BLOCK_TAG_HEADER => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactBlock bytes")?;
                let _bytes = take_length_delimited(&mut cursor)?;
                copy_consumed(field_start, cursor, out);
            }
            COMPACT_BLOCK_TAG_VTX => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactBlock.vtx")?;
                let tx_bytes = take_length_delimited(&mut cursor)?;
                if project_compact_tx_wire(tx_bytes, selection, tx_scratch)? {
                    encode_key(COMPACT_BLOCK_TAG_VTX, WireType::LengthDelimited, out);
                    encode_varint(tx_scratch.len() as u64, out);
                    out.extend_from_slice(tx_scratch);
                }
            }
            COMPACT_BLOCK_TAG_CHAIN_METADATA => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "chain_metadata")?;
                let meta_bytes = take_length_delimited(&mut cursor)?;
                validate_chain_metadata_wire(meta_bytes)?;
                if !selection.nullifiers_only {
                    copy_consumed(field_start, cursor, out);
                }
            }
            _ => skip_wire_field(wire_type, &mut cursor)?,
        }
    }
    if selection.nullifiers_only {
        out.extend_from_slice(&EMPTY_CHAIN_METADATA_WIRE);
    }
    patch_frame_prefix(out, frame_start)
}

#[derive(Clone, Copy)]
pub(super) struct ExpectedShieldedBlock<'a> {
    pub(super) height: BlockHeight,
    pub(super) block_hash_wire: &'a [u8; 32],
    pub(super) prev_hash_wire: &'a [u8; 32],
    pub(super) block_time: u32,
    pub(super) pool_presence: PoolPresenceMask,
}

#[derive(Default)]
struct ObservedShieldedBlock {
    height: Option<u64>,
    time: Option<u64>,
    has_hash: bool,
    has_prev_hash: bool,
    pool_presence: PoolPresenceMask,
}

/// Validates a `Slab 0` `CompactBlock` protobuf payload without heap allocation.
pub(super) fn validate_shielded_block_wire(
    raw_payload: &[u8],
    expected: ExpectedShieldedBlock<'_>,
) -> Result<(), SettledShieldedSegmentError> {
    let mut cursor = raw_payload;
    let mut observed = ObservedShieldedBlock::default();
    while !cursor.is_empty() {
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            COMPACT_BLOCK_TAG_HEIGHT => {
                check_unique_varint_field(wire_type, &mut cursor, &mut observed.height, "height")?;
            }
            COMPACT_BLOCK_TAG_HASH => {
                observed.has_hash = check_header_hash_field(
                    wire_type,
                    &mut cursor,
                    expected.block_hash_wire,
                    observed.has_hash,
                    "hash",
                )?;
            }
            COMPACT_BLOCK_TAG_PREV_HASH => {
                observed.has_prev_hash = check_header_hash_field(
                    wire_type,
                    &mut cursor,
                    expected.prev_hash_wire,
                    observed.has_prev_hash,
                    "prev_hash",
                )?;
            }
            COMPACT_BLOCK_TAG_TIME => {
                check_unique_varint_field(wire_type, &mut cursor, &mut observed.time, "time")?;
            }
            COMPACT_BLOCK_TAG_HEADER => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactBlock.header")?;
                let nested = take_length_delimited(&mut cursor)?;
                if !nested.is_empty() {
                    return Err(SettledShieldedSegmentError::PayloadIntegrity(
                        "unexpected header bytes inside shielded segment block".to_owned(),
                    ));
                }
            }
            COMPACT_BLOCK_TAG_CHAIN_METADATA => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "chain_metadata")?;
                let nested = take_length_delimited(&mut cursor)?;
                validate_chain_metadata_wire(nested)?;
            }
            COMPACT_BLOCK_TAG_VTX => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactBlock.vtx")?;
                let tx_bytes = take_length_delimited(&mut cursor)?;
                let tx_presence = validate_compact_tx_wire(tx_bytes)?;
                observed.pool_presence = observed.pool_presence.union(tx_presence);
            }
            _ => skip_wire_field(wire_type, &mut cursor)?,
        }
    }
    verify_observed_shielded_block(&observed, expected)
}

fn check_unique_varint_field(
    wire_type: WireType,
    cursor: &mut &[u8],
    seen: &mut Option<u64>,
    label: &str,
) -> Result<(), SettledShieldedSegmentError> {
    expect_wire_type(wire_type, WireType::Varint, label)?;
    let decoded = decode_varint(cursor).map_err(|err| wire_error(&err))?;
    if seen.replace(decoded).is_some() {
        return Err(SettledShieldedSegmentError::PayloadIntegrity(format!(
            "duplicate {label} field in shielded segment block"
        )));
    }
    Ok(())
}

fn verify_observed_shielded_block(
    observed: &ObservedShieldedBlock,
    expected: ExpectedShieldedBlock<'_>,
) -> Result<(), SettledShieldedSegmentError> {
    if observed.height.unwrap_or(0) != u64::from(expected.height.value())
        || !observed.has_hash
        || !observed.has_prev_hash
    {
        return Err(SettledShieldedSegmentError::PayloadIntegrity(
            "shielded segment payload height, hash, or prev_hash mismatches index".to_owned(),
        ));
    }
    if observed.time.unwrap_or(0) != u64::from(expected.block_time) {
        return Err(SettledShieldedSegmentError::PayloadIntegrity(
            "shielded segment payload time disagrees with row index".to_owned(),
        ));
    }
    if observed.pool_presence != expected.pool_presence {
        return Err(SettledShieldedSegmentError::PayloadIntegrity(
            "shielded segment payload pool_presence disagrees with row index".to_owned(),
        ));
    }
    Ok(())
}

fn check_header_hash_field(
    wire_type: WireType,
    cursor: &mut &[u8],
    expected_wire: &[u8; 32],
    already_seen: bool,
    label: &str,
) -> Result<bool, SettledShieldedSegmentError> {
    expect_wire_type(wire_type, WireType::LengthDelimited, label)?;
    let bytes = take_length_delimited(cursor)?;
    if already_seen || bytes != expected_wire {
        return Err(SettledShieldedSegmentError::PayloadIntegrity(format!(
            "shielded segment payload {label} disagrees with row index"
        )));
    }
    Ok(true)
}

fn validate_compact_tx_wire(
    tx_bytes: &[u8],
) -> Result<PoolPresenceMask, SettledShieldedSegmentError> {
    let mut cursor = tx_bytes;
    let mut presence = PoolPresenceMask::EMPTY;
    while !cursor.is_empty() {
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            COMPACT_TX_TAG_INDEX | COMPACT_TX_TAG_FEE => {
                expect_wire_type(wire_type, WireType::Varint, "CompactTx scalar")?;
                decode_varint(&mut cursor).map_err(|err| wire_error(&err))?;
            }
            COMPACT_TX_TAG_TXID => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactTx.txid")?;
                let _bytes = take_length_delimited(&mut cursor)?;
            }
            COMPACT_TX_TAG_SPENDS => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactTx.spends")?;
                let nested = take_length_delimited(&mut cursor)?;
                validate_sapling_spend_wire(nested)?;
                presence = presence.union(PoolPresenceMask::SAPLING_SPEND);
            }
            COMPACT_TX_TAG_OUTPUTS => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactTx.outputs")?;
                let nested = take_length_delimited(&mut cursor)?;
                validate_sapling_output_wire(nested)?;
                presence = presence.union(PoolPresenceMask::SAPLING_OUTPUT);
            }
            COMPACT_TX_TAG_ACTIONS | COMPACT_TX_TAG_IRONWOOD_ACTIONS => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactTx.actions")?;
                let nested = take_length_delimited(&mut cursor)?;
                validate_action_wire(nested)?;
                let pool = if tag == COMPACT_TX_TAG_ACTIONS {
                    PoolPresenceMask::ORCHARD
                } else {
                    PoolPresenceMask::IRONWOOD
                };
                presence = presence.union(pool);
            }
            COMPACT_TX_TAG_VIN | COMPACT_TX_TAG_VOUT => {
                return Err(SettledShieldedSegmentError::PayloadIntegrity(
                    "unexpected transparent field inside shielded segment".to_owned(),
                ));
            }
            _ => skip_wire_field(wire_type, &mut cursor)?,
        }
    }
    Ok(presence)
}

#[derive(Clone, Copy)]
struct WireFieldHead<'a> {
    tag: u32,
    wire_type: WireType,
    field_start: &'a [u8],
}

#[derive(Clone, Copy)]
struct ActionWireSlice<'a> {
    outer_tag: u32,
    field_start: &'a [u8],
    cursor_after: &'a [u8],
    action_bytes: &'a [u8],
}

fn project_compact_tx_wire(
    tx_bytes: &[u8],
    selection: ShieldedProjectionSelection,
    tx_scratch: &mut Vec<u8>,
) -> Result<bool, SettledShieldedSegmentError> {
    tx_scratch.clear();
    let mut flags = ProjectedTxPresence::default();
    let mut cursor = tx_bytes;
    while !cursor.is_empty() {
        let field_start = cursor;
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        project_tx_field(
            WireFieldHead {
                tag,
                wire_type,
                field_start,
            },
            &mut cursor,
            selection,
            &mut flags,
            tx_scratch,
        )?;
    }
    Ok(flags.selected(selection))
}

#[derive(Default)]
struct ProjectedTxPresence {
    has_sapling: bool,
    has_orchard: bool,
    has_ironwood: bool,
}

impl ProjectedTxPresence {
    const fn selected(&self, selection: ShieldedProjectionSelection) -> bool {
        (selection.sapling && self.has_sapling)
            || (selection.orchard && self.has_orchard)
            || (selection.ironwood && self.has_ironwood)
    }
}

fn project_tx_field(
    head: WireFieldHead<'_>,
    cursor: &mut &[u8],
    selection: ShieldedProjectionSelection,
    flags: &mut ProjectedTxPresence,
    tx_scratch: &mut Vec<u8>,
) -> Result<(), SettledShieldedSegmentError> {
    match head.tag {
        COMPACT_TX_TAG_INDEX | COMPACT_TX_TAG_FEE => {
            expect_wire_type(head.wire_type, WireType::Varint, "CompactTx scalar")?;
            decode_varint(cursor).map_err(|err| wire_error(&err))?;
            copy_consumed(head.field_start, cursor, tx_scratch);
        }
        COMPACT_TX_TAG_TXID => {
            expect_wire_type(head.wire_type, WireType::LengthDelimited, "CompactTx.txid")?;
            let _txid = take_length_delimited(cursor)?;
            copy_consumed(head.field_start, cursor, tx_scratch);
        }
        COMPACT_TX_TAG_SPENDS => {
            expect_wire_type(
                head.wire_type,
                WireType::LengthDelimited,
                "CompactTx.spends",
            )?;
            let nested = take_length_delimited(cursor)?;
            validate_sapling_spend_wire(nested)?;
            flags.has_sapling = true;
            if selection.sapling {
                copy_consumed(head.field_start, cursor, tx_scratch);
            }
        }
        COMPACT_TX_TAG_OUTPUTS => {
            expect_wire_type(
                head.wire_type,
                WireType::LengthDelimited,
                "CompactTx.outputs",
            )?;
            let nested = take_length_delimited(cursor)?;
            validate_sapling_output_wire(nested)?;
            flags.has_sapling = true;
            if selection.sapling && !selection.nullifiers_only {
                copy_consumed(head.field_start, cursor, tx_scratch);
            }
        }
        COMPACT_TX_TAG_ACTIONS | COMPACT_TX_TAG_IRONWOOD_ACTIONS => {
            project_tx_action_field(head, cursor, selection, flags, tx_scratch)?;
        }
        COMPACT_TX_TAG_VIN | COMPACT_TX_TAG_VOUT => {
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "unexpected transparent field inside shielded segment".to_owned(),
            ));
        }
        _ => skip_wire_field(head.wire_type, cursor)?,
    }
    Ok(())
}

fn project_tx_action_field(
    head: WireFieldHead<'_>,
    cursor: &mut &[u8],
    selection: ShieldedProjectionSelection,
    flags: &mut ProjectedTxPresence,
    tx_scratch: &mut Vec<u8>,
) -> Result<(), SettledShieldedSegmentError> {
    let (label, include_pool) = if head.tag == COMPACT_TX_TAG_ACTIONS {
        flags.has_orchard = true;
        ("CompactTx.actions", selection.orchard)
    } else {
        flags.has_ironwood = true;
        ("ironwood_actions", selection.ironwood)
    };
    expect_wire_type(head.wire_type, WireType::LengthDelimited, label)?;
    let action_bytes = take_length_delimited(cursor)?;
    emit_projected_action(
        ActionWireSlice {
            outer_tag: head.tag,
            field_start: head.field_start,
            cursor_after: cursor,
            action_bytes,
        },
        include_pool,
        selection.nullifiers_only,
        tx_scratch,
    )
}

fn emit_projected_action(
    action: ActionWireSlice<'_>,
    include_pool: bool,
    nullifiers_only: bool,
    tx_scratch: &mut Vec<u8>,
) -> Result<(), SettledShieldedSegmentError> {
    if !include_pool {
        validate_action_wire(action.action_bytes)?;
        return Ok(());
    }
    if !nullifiers_only {
        validate_action_wire(action.action_bytes)?;
        copy_consumed(action.field_start, action.cursor_after, tx_scratch);
        return Ok(());
    }
    let nullifier_wire = extract_action_nullifier_wire(action.action_bytes)?;
    encode_key(action.outer_tag, WireType::LengthDelimited, tx_scratch);
    encode_varint(nullifier_wire.len() as u64, tx_scratch);
    tx_scratch.extend_from_slice(nullifier_wire);
    Ok(())
}

fn extract_action_nullifier_wire<'a>(
    action_bytes: &'a [u8],
) -> Result<&'a [u8], SettledShieldedSegmentError> {
    let mut cursor = action_bytes;
    let mut nullifier_wire: &'a [u8] = &[];
    while !cursor.is_empty() {
        let field_start = cursor;
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            ACTION_TAG_NULLIFIER => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "action.nullifier")?;
                let _nf = take_length_delimited(&mut cursor)?;
                let consumed = field_start.len() - cursor.len();
                nullifier_wire = &field_start[..consumed];
            }
            ACTION_TAG_CMX | ACTION_TAG_EPHEMERAL_KEY | ACTION_TAG_CIPHERTEXT => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "action field")?;
                let _bytes = take_length_delimited(&mut cursor)?;
            }
            _ => skip_wire_field(wire_type, &mut cursor)?,
        }
    }
    Ok(nullifier_wire)
}

fn patch_frame_prefix(
    out: &mut [u8],
    frame_start: usize,
) -> Result<(), SettledShieldedSegmentError> {
    let payload_len = out.len() - frame_start - FRAME_PREFIX_LEN;
    let prefix = encode_frame_prefix(payload_len)?;
    out[frame_start..frame_start + FRAME_PREFIX_LEN].copy_from_slice(&prefix);
    Ok(())
}

fn copy_consumed(field_start: &[u8], cursor_after: &[u8], out: &mut Vec<u8>) {
    let consumed = field_start.len() - cursor_after.len();
    out.extend_from_slice(&field_start[..consumed]);
}

fn expect_wire_type(
    observed: WireType,
    expected: WireType,
    field: &str,
) -> Result<(), SettledShieldedSegmentError> {
    if observed != expected {
        return Err(SettledShieldedSegmentError::PayloadIntegrity(format!(
            "unexpected wire type for {field}"
        )));
    }
    Ok(())
}

fn take_length_delimited<'a>(
    cursor: &mut &'a [u8],
) -> Result<&'a [u8], SettledShieldedSegmentError> {
    let raw_len = decode_varint(cursor).map_err(|err| wire_error(&err))?;
    let len = usize::try_from(raw_len).map_err(|_| {
        SettledShieldedSegmentError::PayloadIntegrity(
            "protobuf length-delimited field exceeds address space".to_owned(),
        )
    })?;
    take_slice(cursor, len)
}

fn take_slice<'a>(
    cursor: &mut &'a [u8],
    len: usize,
) -> Result<&'a [u8], SettledShieldedSegmentError> {
    let slice = cursor.get(..len).ok_or_else(|| {
        SettledShieldedSegmentError::PayloadIntegrity("truncated protobuf payload".to_owned())
    })?;
    *cursor = &cursor[len..];
    Ok(slice)
}

fn validate_sapling_spend_wire(bytes: &[u8]) -> Result<(), SettledShieldedSegmentError> {
    validate_uniform_wire_fields(
        bytes,
        &[SAPLING_SPEND_TAG_NF],
        WireType::LengthDelimited,
        "CompactSaplingSpend",
    )
}

fn validate_sapling_output_wire(bytes: &[u8]) -> Result<(), SettledShieldedSegmentError> {
    validate_uniform_wire_fields(
        bytes,
        &[
            SAPLING_OUTPUT_TAG_CMU,
            SAPLING_OUTPUT_TAG_EPHEMERAL_KEY,
            SAPLING_OUTPUT_TAG_CIPHERTEXT,
        ],
        WireType::LengthDelimited,
        "CompactSaplingOutput",
    )
}

fn validate_action_wire(bytes: &[u8]) -> Result<(), SettledShieldedSegmentError> {
    validate_uniform_wire_fields(
        bytes,
        &[
            ACTION_TAG_NULLIFIER,
            ACTION_TAG_CMX,
            ACTION_TAG_EPHEMERAL_KEY,
            ACTION_TAG_CIPHERTEXT,
        ],
        WireType::LengthDelimited,
        "CompactOrchardAction",
    )
}

fn validate_chain_metadata_wire(bytes: &[u8]) -> Result<(), SettledShieldedSegmentError> {
    validate_uniform_wire_fields(
        bytes,
        &[
            CHAIN_METADATA_TAG_SAPLING_TREE_SIZE,
            CHAIN_METADATA_TAG_ORCHARD_TREE_SIZE,
            CHAIN_METADATA_TAG_IRONWOOD_TREE_SIZE,
        ],
        WireType::Varint,
        "ChainMetadata",
    )
}

#[inline]
fn validate_uniform_wire_fields(
    bytes: &[u8],
    known_tags: &[u32],
    expected_wire_type: WireType,
    field: &str,
) -> Result<(), SettledShieldedSegmentError> {
    let mut cursor = bytes;
    while !cursor.is_empty() {
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        if known_tags.contains(&tag) {
            expect_wire_type(wire_type, expected_wire_type, field)?;
        }
        skip_wire_field(wire_type, &mut cursor)?;
    }
    Ok(())
}

fn skip_wire_field(
    wire_type: WireType,
    cursor: &mut &[u8],
) -> Result<(), SettledShieldedSegmentError> {
    match wire_type {
        WireType::Varint => {
            decode_varint(cursor).map_err(|err| wire_error(&err))?;
        }
        WireType::SixtyFourBit => {
            let _bytes = take_slice(cursor, 8)?;
        }
        WireType::LengthDelimited => {
            let _bytes = take_length_delimited(cursor)?;
        }
        WireType::ThirtyTwoBit => {
            let _bytes = take_slice(cursor, 4)?;
        }
        WireType::StartGroup | WireType::EndGroup => {
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "unexpected protobuf group wire type in proto3 payload".to_owned(),
            ));
        }
    }
    Ok(())
}

fn wire_error(error: &prost::DecodeError) -> SettledShieldedSegmentError {
    SettledShieldedSegmentError::PayloadIntegrity(format!(
        "malformed CompactBlock protobuf wire encoding: {error}"
    ))
}

fn transaction_pool_mask(scan: &zinder_core::CompactTransactionData) -> PoolPresenceMask {
    let mut mask = PoolPresenceMask::EMPTY;
    if !scan.sapling_spends.is_empty() {
        mask = mask.union(PoolPresenceMask::SAPLING_SPEND);
    }
    if !scan.sapling_outputs.is_empty() {
        mask = mask.union(PoolPresenceMask::SAPLING_OUTPUT);
    }
    if !scan.orchard_actions.is_empty() {
        mask = mask.union(PoolPresenceMask::ORCHARD);
    }
    if !scan.ironwood_actions.is_empty() {
        mask = mask.union(PoolPresenceMask::IRONWOOD);
    }
    mask
}

fn encode_sapling_spends(
    spends: &[zinder_core::CompactSaplingSpend],
) -> Vec<lightwalletd::CompactSaplingSpend> {
    spends
        .iter()
        .map(|spend| lightwalletd::CompactSaplingSpend {
            nf: spend.nullifier.to_vec(),
        })
        .collect()
}

fn encode_sapling_outputs(
    outputs: &[zinder_core::CompactSaplingOutput],
) -> Vec<lightwalletd::CompactSaplingOutput> {
    outputs
        .iter()
        .map(|output| lightwalletd::CompactSaplingOutput {
            cmu: output.commitment.to_vec(),
            ephemeral_key: output.ephemeral_key.to_vec(),
            ciphertext: output.ciphertext.to_vec(),
        })
        .collect()
}

fn encode_shielded_actions(
    actions: &[CompactShieldedAction],
) -> Vec<lightwalletd::CompactOrchardAction> {
    actions
        .iter()
        .map(|action| lightwalletd::CompactOrchardAction {
            nullifier: action.nullifier.to_vec(),
            cmx: action.commitment.to_vec(),
            ephemeral_key: action.ephemeral_key.to_vec(),
            ciphertext: action.ciphertext.to_vec(),
        })
        .collect()
}
