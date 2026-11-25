//! Byte-level wire splice and lossless [`CompactBlockArtifact`] reconstruction.

use bytes::Bytes;
use prost::{
    Message,
    encoding::{WireType, decode_key, decode_varint, encode_key, encode_varint},
};
use zinder_core::{
    BlockHeight, BlockId, CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput,
    CompactSaplingSpend, CompactShieldedAction, CompactTransaction, CompactTransactionData,
    wire::{
        decode_internal_block_hash, decode_internal_transaction_id, encode_internal_transaction_id,
    },
};
use zinder_proto::compat::lightwalletd::{self, EncodedBlock};

use super::{
    SettledSupplementSegmentError,
    section::{BlockSupplementRecord, CanonicalBlockSupplement, CompactTransparentSupplementTx},
};

const FRAME_PREFIX_LEN: usize = EncodedBlock::PREFIX_LEN;

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

const SAPLING_SPEND_TAG_NF: u32 = 1;
const SAPLING_OUTPUT_TAG_CMU: u32 = 1;
const SAPLING_OUTPUT_TAG_EPK: u32 = 2;
const SAPLING_OUTPUT_TAG_CIPHERTEXT: u32 = 3;

const ACTION_TAG_NULLIFIER: u32 = 1;
const ACTION_TAG_CMX: u32 = 2;
const ACTION_TAG_EPHEMERAL_KEY: u32 = 3;
const ACTION_TAG_CIPHERTEXT: u32 = 4;

const CHAIN_METADATA_TAG_SAPLING: u32 = 1;
const CHAIN_METADATA_TAG_ORCHARD: u32 = 2;
const CHAIN_METADATA_TAG_IRONWOOD: u32 = 3;

const EMPTY_CHAIN_METADATA_WIRE: [u8; 2] = [0x42, 0x00];

/// Selection flags controlling how [`splice_compact_block_wire`] merges
/// `ZSTSUP01` Tag-1 transparent supplements onto a `ZSHRNG02` `CompactBlock`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactBlockSpliceSelection {
    /// Merge transparent inputs (`vin`), outputs (`vout`), and transparent-only transactions.
    pub include_transparent: bool,
    /// Emit nullifier-only form (strip outputs/ciphertexts/metadata and omit `vin`/`vout`).
    pub nullifiers_only: bool,
    /// When `nullifiers_only` is `true`, retain transparent-only transactions as
    /// `{index, txid, fee}` shells matching `GetBlockNullifiers`.
    pub include_nullifier_tx_shells: bool,
}

impl CompactBlockSpliceSelection {
    /// Full compact block (`GetBlock` / shielded + transparent `GetBlockRange`).
    pub const FULL_BLOCK: Self = Self {
        include_transparent: true,
        nullifiers_only: false,
        include_nullifier_tx_shells: false,
    };

    /// Default shielded range (`GetBlockRange` without transparent pool).
    pub const DEFAULT_SHIELDED: Self = Self {
        include_transparent: false,
        nullifiers_only: false,
        include_nullifier_tx_shells: false,
    };

    /// Default nullifier range (`GetBlockRangeNullifiers`).
    pub const DEFAULT_NULLIFIERS: Self = Self {
        include_transparent: false,
        nullifiers_only: true,
        include_nullifier_tx_shells: false,
    };

    /// Unary nullifier block (`GetBlockNullifiers`), retaining transparent-only
    /// transactions as `{index, txid, fee}` shells.
    pub const BLOCK_NULLIFIERS: Self = Self {
        include_transparent: false,
        nullifiers_only: true,
        include_nullifier_tx_shells: true,
    };

    /// Constructs a range splice selection from `include_transparent` and `nullifiers_only`.
    #[must_use]
    pub const fn new(include_transparent: bool, nullifiers_only: bool) -> Self {
        Self {
            include_transparent,
            nullifiers_only,
            include_nullifier_tx_shells: false,
        }
    }

    /// Enables retaining transparent-only `{index, txid, fee}` shells when
    /// `nullifiers_only` is set.
    #[must_use]
    pub const fn with_nullifier_tx_shells(mut self, include_shells: bool) -> Self {
        self.include_nullifier_tx_shells = include_shells;
        self
    }
}

#[derive(Default)]
pub(super) struct SpliceScratch {
    tx_buf: Vec<u8>,
    sub_buf: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(super) struct SpliceTarget<'a> {
    pub(super) transparent: &'a [CompactTransparentSupplementTx],
    pub(super) selection: CompactBlockSpliceSelection,
    pub(super) expected_hash_wire: Option<[u8; 32]>,
}

struct WireTxMeta<'a> {
    index: u64,
    txid_bytes: &'a [u8],
    fee: u32,
}

const HEADER_SEEN_HEIGHT: u8 = 1 << 0;
const HEADER_SEEN_HASH: u8 = 1 << 1;
const HEADER_SEEN_PREV_HASH: u8 = 1 << 2;
const HEADER_SEEN_TIME: u8 = 1 << 3;
const HEADER_SEEN_HEADER: u8 = 1 << 4;
const HEADER_SEEN_VTX: u8 = 1 << 5;
const HEADER_SEEN_CHAIN_METADATA: u8 = 1 << 6;

struct BlockHeaderPresence {
    flags: u8,
    last_tx_index: Option<u64>,
}

impl BlockHeaderPresence {
    const fn empty() -> Self {
        Self {
            flags: 0,
            last_tx_index: None,
        }
    }

    const fn has(&self, mask: u8) -> bool {
        (self.flags & mask) != 0
    }

    const fn set(&mut self, mask: u8) {
        self.flags |= mask;
    }

    const fn in_body(&self) -> bool {
        self.has(HEADER_SEEN_VTX | HEADER_SEEN_CHAIN_METADATA)
    }
}

/// Merges one `ZSHRNG02` `Slab 0` projected `CompactBlock` protobuf payload with
/// its `ZSTSUP01` Tag-1 transparent supplement into one gRPC-framed [`EncodedBlock`].
///
/// The emitted `vtx` entries are strictly ordered by ascending `CompactTx.index`
/// and are byte-identical to `prost` encoding of [`CompactBlockArtifact`].
pub fn splice_compact_block_wire(
    shielded_payload: &[u8],
    transparent_supplement: &[CompactTransparentSupplementTx],
    selection: CompactBlockSpliceSelection,
) -> Result<EncodedBlock, SettledSupplementSegmentError> {
    let mut out = Vec::with_capacity(FRAME_PREFIX_LEN + shielded_payload.len() + 64);
    let mut scratch = SpliceScratch::default();
    splice_compact_block_wire_into(
        shielded_payload,
        SpliceTarget {
            transparent: transparent_supplement,
            selection,
            expected_hash_wire: None,
        },
        &mut out,
        &mut scratch,
    )?;
    let alloc_len = out.capacity();
    EncodedBlock::from_length_prefixed_in(Bytes::from(out), alloc_len).map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity(
            "spliced CompactBlock failed gRPC framing".to_owned(),
        )
    })
}

pub(super) fn splice_compact_block_wire_into(
    shielded_payload: &[u8],
    target: SpliceTarget<'_>,
    out: &mut Vec<u8>,
    scratch: &mut SpliceScratch,
) -> Result<(), SettledSupplementSegmentError> {
    validate_transparent_supplement_order(target.transparent)?;
    let frame_start = out.len();
    out.resize(frame_start + FRAME_PREFIX_LEN, 0);
    let mut ctx = BlockSpliceContext {
        transparent: target.transparent,
        sup_cursor: 0,
        selection: target.selection,
        expected_hash_wire: target.expected_hash_wire,
        presence: BlockHeaderPresence::empty(),
        out,
        scratch,
    };
    let mut cursor = shielded_payload;

    while !cursor.is_empty() {
        let field_start = cursor;
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        process_block_field(
            BlockFieldSlice {
                tag,
                wire_type,
                field_start,
            },
            &mut cursor,
            &mut ctx,
        )?;
    }

    if !ctx.presence.has(HEADER_SEEN_HASH) || !ctx.presence.has(HEADER_SEEN_PREV_HASH) {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "shielded CompactBlock payload missing hash or prev_hash".to_owned(),
        ));
    }
    if !ctx.presence.has(HEADER_SEEN_CHAIN_METADATA) {
        emit_remaining_transparent(&mut ctx)?;
        if ctx.selection.nullifiers_only {
            ctx.out.extend_from_slice(&EMPTY_CHAIN_METADATA_WIRE);
        }
    }
    patch_frame_prefix(ctx.out, frame_start)
}

#[derive(Clone, Copy)]
struct BlockFieldSlice<'a> {
    tag: u32,
    wire_type: WireType,
    field_start: &'a [u8],
}

struct BlockSpliceContext<'a> {
    transparent: &'a [CompactTransparentSupplementTx],
    sup_cursor: usize,
    selection: CompactBlockSpliceSelection,
    expected_hash_wire: Option<[u8; 32]>,
    presence: BlockHeaderPresence,
    out: &'a mut Vec<u8>,
    scratch: &'a mut SpliceScratch,
}

fn process_block_field(
    field: BlockFieldSlice<'_>,
    cursor: &mut &[u8],
    ctx: &mut BlockSpliceContext<'_>,
) -> Result<(), SettledSupplementSegmentError> {
    match field.tag {
        COMPACT_BLOCK_TAG_HEIGHT | COMPACT_BLOCK_TAG_TIME => {
            process_scalar_block_field(field, cursor, &mut ctx.presence, ctx.out)
        }
        COMPACT_BLOCK_TAG_HASH | COMPACT_BLOCK_TAG_PREV_HASH | COMPACT_BLOCK_TAG_HEADER => {
            process_bytes_block_field(field, cursor, ctx)
        }
        COMPACT_BLOCK_TAG_VTX => process_vtx_field(field, cursor, ctx),
        COMPACT_BLOCK_TAG_CHAIN_METADATA => {
            expect_wire_type(field.wire_type, WireType::LengthDelimited, "chain_metadata")?;
            if ctx.presence.has(HEADER_SEEN_CHAIN_METADATA) {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "duplicate chain_metadata in CompactBlock".to_owned(),
                ));
            }
            let meta_bytes = take_length_delimited(cursor)?;
            validate_chain_metadata_wire(meta_bytes)?;
            emit_remaining_transparent(ctx)?;
            if ctx.selection.nullifiers_only {
                ctx.out.extend_from_slice(&EMPTY_CHAIN_METADATA_WIRE);
            } else {
                copy_consumed(field.field_start, cursor, ctx.out);
            }
            ctx.presence.set(HEADER_SEEN_CHAIN_METADATA);
            Ok(())
        }
        _ => skip_wire_field(field.wire_type, cursor),
    }
}

fn process_scalar_block_field(
    field: BlockFieldSlice<'_>,
    cursor: &mut &[u8],
    presence: &mut BlockHeaderPresence,
    out: &mut Vec<u8>,
) -> Result<(), SettledSupplementSegmentError> {
    expect_wire_type(field.wire_type, WireType::Varint, "CompactBlock scalar")?;
    if presence.in_body() {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "CompactBlock header scalar appeared after vtx or chain_metadata".to_owned(),
        ));
    }
    decode_varint(cursor).map_err(|err| wire_error(&err))?;
    let mask = if field.tag == COMPACT_BLOCK_TAG_HEIGHT {
        HEADER_SEEN_HEIGHT
    } else {
        HEADER_SEEN_TIME
    };
    if presence.has(mask) {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "duplicate CompactBlock scalar field".to_owned(),
        ));
    }
    presence.set(mask);
    copy_consumed(field.field_start, cursor, out);
    Ok(())
}

fn process_bytes_block_field(
    field: BlockFieldSlice<'_>,
    cursor: &mut &[u8],
    ctx: &mut BlockSpliceContext<'_>,
) -> Result<(), SettledSupplementSegmentError> {
    expect_wire_type(
        field.wire_type,
        WireType::LengthDelimited,
        "CompactBlock bytes",
    )?;
    if ctx.presence.in_body() {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "CompactBlock header bytes appeared after vtx or chain_metadata".to_owned(),
        ));
    }
    let bytes = take_length_delimited(cursor)?;
    match field.tag {
        COMPACT_BLOCK_TAG_HASH => {
            check_unique_32(&mut ctx.presence, HEADER_SEEN_HASH, bytes, "hash")?;
            if let Some(expected_hash) = ctx.expected_hash_wire
                && bytes != expected_hash
            {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "shielded CompactBlock hash disagrees with ZSTSUP01 row index".to_owned(),
                ));
            }
        }
        COMPACT_BLOCK_TAG_PREV_HASH => {
            check_unique_32(&mut ctx.presence, HEADER_SEEN_PREV_HASH, bytes, "prev_hash")?;
        }
        _ => {
            if ctx.presence.has(HEADER_SEEN_HEADER) {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "duplicate CompactBlock header bytes".to_owned(),
                ));
            }
            ctx.presence.set(HEADER_SEEN_HEADER);
        }
    }
    copy_consumed(field.field_start, cursor, ctx.out);
    Ok(())
}

fn check_unique_32(
    presence: &mut BlockHeaderPresence,
    mask: u8,
    bytes: &[u8],
    label: &str,
) -> Result<(), SettledSupplementSegmentError> {
    if presence.has(mask) {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "duplicate CompactBlock {label}"
        )));
    }
    decode_internal_block_hash(bytes).map_err(|err| {
        SettledSupplementSegmentError::PayloadIntegrity(format!(
            "invalid CompactBlock {label}: {err}"
        ))
    })?;
    presence.set(mask);
    Ok(())
}

fn process_vtx_field(
    field: BlockFieldSlice<'_>,
    cursor: &mut &[u8],
    ctx: &mut BlockSpliceContext<'_>,
) -> Result<(), SettledSupplementSegmentError> {
    expect_wire_type(
        field.wire_type,
        WireType::LengthDelimited,
        "CompactBlock.vtx",
    )?;
    if ctx.presence.has(HEADER_SEEN_CHAIN_METADATA) {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "CompactBlock.vtx appeared after chain_metadata".to_owned(),
        ));
    }
    ctx.presence.set(HEADER_SEEN_VTX);
    let tx_bytes = take_length_delimited(cursor)?;
    let tx_meta = inspect_and_validate_shielded_tx(tx_bytes)?;
    if ctx
        .presence
        .last_tx_index
        .is_some_and(|prev| tx_meta.index <= prev)
    {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "shielded CompactBlock.vtx is not strictly ascending by index".to_owned(),
        ));
    }
    ctx.presence.last_tx_index = Some(tx_meta.index);

    while ctx.sup_cursor < ctx.transparent.len()
        && ctx.transparent[ctx.sup_cursor].index < tx_meta.index
    {
        emit_transparent_only_if_selected(
            &ctx.transparent[ctx.sup_cursor],
            ctx.selection,
            ctx.out,
        )?;
        ctx.sup_cursor += 1;
    }

    if ctx.sup_cursor < ctx.transparent.len()
        && ctx.transparent[ctx.sup_cursor].index == tx_meta.index
    {
        let sup_tx = &ctx.transparent[ctx.sup_cursor];
        verify_mixed_tx_match(&tx_meta, sup_tx)?;
        emit_mixed_tx(tx_bytes, sup_tx, ctx)?;
        ctx.sup_cursor += 1;
    } else {
        emit_shielded_only_tx(field.field_start, cursor, tx_bytes, ctx)?;
    }
    Ok(())
}

fn emit_remaining_transparent(
    ctx: &mut BlockSpliceContext<'_>,
) -> Result<(), SettledSupplementSegmentError> {
    while ctx.sup_cursor < ctx.transparent.len() {
        emit_transparent_only_if_selected(
            &ctx.transparent[ctx.sup_cursor],
            ctx.selection,
            ctx.out,
        )?;
        ctx.sup_cursor += 1;
    }
    Ok(())
}

fn emit_transparent_only_if_selected(
    sup_tx: &CompactTransparentSupplementTx,
    selection: CompactBlockSpliceSelection,
    out: &mut Vec<u8>,
) -> Result<(), SettledSupplementSegmentError> {
    let fee = fee_to_wire_u32(sup_tx.fee_zat)?;
    if selection.nullifiers_only {
        if selection.include_nullifier_tx_shells {
            encode_shell_compact_tx(sup_tx.index, sup_tx.transaction_id, fee, out);
        }
        return Ok(());
    }
    if selection.include_transparent {
        encode_full_transparent_compact_tx(sup_tx, fee, out);
    }
    Ok(())
}

fn emit_mixed_tx(
    tx_bytes: &[u8],
    sup_tx: &CompactTransparentSupplementTx,
    ctx: &mut BlockSpliceContext<'_>,
) -> Result<(), SettledSupplementSegmentError> {
    if ctx.selection.nullifiers_only {
        redact_tx_to_nullifiers(tx_bytes, &mut ctx.scratch.tx_buf)?;
        write_vtx_wrapper(&ctx.scratch.tx_buf, ctx.out);
        return Ok(());
    }
    if !ctx.selection.include_transparent {
        write_vtx_wrapper(tx_bytes, ctx.out);
        return Ok(());
    }
    splice_transparent_into_tx(tx_bytes, sup_tx, ctx.scratch)?;
    write_vtx_wrapper(&ctx.scratch.tx_buf, ctx.out);
    Ok(())
}

fn emit_shielded_only_tx(
    field_start: &[u8],
    cursor_after: &[u8],
    tx_bytes: &[u8],
    ctx: &mut BlockSpliceContext<'_>,
) -> Result<(), SettledSupplementSegmentError> {
    if ctx.selection.nullifiers_only {
        redact_tx_to_nullifiers(tx_bytes, &mut ctx.scratch.tx_buf)?;
        write_vtx_wrapper(&ctx.scratch.tx_buf, ctx.out);
    } else {
        copy_consumed(field_start, cursor_after, ctx.out);
    }
    Ok(())
}

fn splice_transparent_into_tx(
    tx_bytes: &[u8],
    sup_tx: &CompactTransparentSupplementTx,
    scratch: &mut SpliceScratch,
) -> Result<(), SettledSupplementSegmentError> {
    scratch.tx_buf.clear();
    let mut cursor = tx_bytes;
    let mut injected = false;
    while !cursor.is_empty() {
        let field_start = cursor;
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        if !injected && tag > COMPACT_TX_TAG_VOUT {
            encode_transparent_fields(sup_tx, &mut scratch.tx_buf, &mut scratch.sub_buf);
            injected = true;
        }
        skip_wire_field(wire_type, &mut cursor)?;
        copy_consumed(field_start, cursor, &mut scratch.tx_buf);
    }
    if !injected {
        encode_transparent_fields(sup_tx, &mut scratch.tx_buf, &mut scratch.sub_buf);
    }
    Ok(())
}

fn redact_tx_to_nullifiers(
    tx_bytes: &[u8],
    tx_buf: &mut Vec<u8>,
) -> Result<(), SettledSupplementSegmentError> {
    tx_buf.clear();
    let mut cursor = tx_bytes;
    while !cursor.is_empty() {
        let field_start = cursor;
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            COMPACT_TX_TAG_INDEX | COMPACT_TX_TAG_FEE => {
                expect_wire_type(wire_type, WireType::Varint, "CompactTx scalar")?;
                decode_varint(&mut cursor).map_err(|err| wire_error(&err))?;
                copy_consumed(field_start, cursor, tx_buf);
            }
            COMPACT_TX_TAG_TXID | COMPACT_TX_TAG_SPENDS => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactTx bytes")?;
                let _bytes = take_length_delimited(&mut cursor)?;
                copy_consumed(field_start, cursor, tx_buf);
            }
            COMPACT_TX_TAG_OUTPUTS => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactTx.outputs")?;
                let _bytes = take_length_delimited(&mut cursor)?;
            }
            COMPACT_TX_TAG_ACTIONS | COMPACT_TX_TAG_IRONWOOD_ACTIONS => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactTx action")?;
                let action_bytes = take_length_delimited(&mut cursor)?;
                let nf_wire = extract_action_nullifier_wire(action_bytes)?;
                encode_key(tag, WireType::LengthDelimited, tx_buf);
                encode_varint(nf_wire.len() as u64, tx_buf);
                tx_buf.extend_from_slice(nf_wire);
            }
            COMPACT_TX_TAG_VIN | COMPACT_TX_TAG_VOUT => {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "unexpected transparent field inside shielded payload".to_owned(),
                ));
            }
            _ => skip_wire_field(wire_type, &mut cursor)?,
        }
    }
    Ok(())
}

fn extract_action_nullifier_wire<'a>(
    action_bytes: &'a [u8],
) -> Result<&'a [u8], SettledSupplementSegmentError> {
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

fn encode_shell_compact_tx(
    index: u64,
    txid: zinder_core::TransactionId,
    fee: u32,
    out: &mut Vec<u8>,
) {
    let txid_wire = encode_internal_transaction_id(txid);
    let mut payload_len = 1 + 1 + txid_wire.len();
    if index != 0 {
        payload_len += 1 + varint_len(index);
    }
    if fee != 0 {
        payload_len += 1 + varint_len(u64::from(fee));
    }
    encode_key(COMPACT_BLOCK_TAG_VTX, WireType::LengthDelimited, out);
    encode_varint(payload_len as u64, out);
    if index != 0 {
        encode_key(COMPACT_TX_TAG_INDEX, WireType::Varint, out);
        encode_varint(index, out);
    }
    encode_key(COMPACT_TX_TAG_TXID, WireType::LengthDelimited, out);
    encode_varint(txid_wire.len() as u64, out);
    out.extend_from_slice(&txid_wire);
    if fee != 0 {
        encode_key(COMPACT_TX_TAG_FEE, WireType::Varint, out);
        encode_varint(u64::from(fee), out);
    }
}

fn encode_full_transparent_compact_tx(
    sup_tx: &CompactTransparentSupplementTx,
    fee: u32,
    out: &mut Vec<u8>,
) {
    let mut tx_buf = Vec::with_capacity(64 + sup_tx.transparent_inputs.len() * 42);
    let mut sub_buf = Vec::new();
    if sup_tx.index != 0 {
        encode_key(COMPACT_TX_TAG_INDEX, WireType::Varint, &mut tx_buf);
        encode_varint(sup_tx.index, &mut tx_buf);
    }
    let txid_wire = encode_internal_transaction_id(sup_tx.transaction_id);
    encode_key(COMPACT_TX_TAG_TXID, WireType::LengthDelimited, &mut tx_buf);
    encode_varint(txid_wire.len() as u64, &mut tx_buf);
    tx_buf.extend_from_slice(&txid_wire);
    if fee != 0 {
        encode_key(COMPACT_TX_TAG_FEE, WireType::Varint, &mut tx_buf);
        encode_varint(u64::from(fee), &mut tx_buf);
    }
    encode_transparent_fields(sup_tx, &mut tx_buf, &mut sub_buf);
    write_vtx_wrapper(&tx_buf, out);
}

fn encode_transparent_fields(
    sup_tx: &CompactTransparentSupplementTx,
    tx_buf: &mut Vec<u8>,
    sub_buf: &mut Vec<u8>,
) {
    for tx_in in &sup_tx.transparent_inputs {
        sub_buf.clear();
        let prev_wire = encode_internal_transaction_id(tx_in.previous_transaction_id);
        encode_key(1, WireType::LengthDelimited, sub_buf);
        encode_varint(prev_wire.len() as u64, sub_buf);
        sub_buf.extend_from_slice(&prev_wire);
        if tx_in.previous_output_index != 0 {
            encode_key(2, WireType::Varint, sub_buf);
            encode_varint(u64::from(tx_in.previous_output_index), sub_buf);
        }
        encode_key(COMPACT_TX_TAG_VIN, WireType::LengthDelimited, tx_buf);
        encode_varint(sub_buf.len() as u64, tx_buf);
        tx_buf.extend_from_slice(sub_buf);
    }
    for tx_out in &sup_tx.transparent_outputs {
        sub_buf.clear();
        if tx_out.value_zat != 0 {
            encode_key(1, WireType::Varint, sub_buf);
            encode_varint(tx_out.value_zat, sub_buf);
        }
        if !tx_out.script_pub_key.is_empty() {
            encode_key(2, WireType::LengthDelimited, sub_buf);
            encode_varint(tx_out.script_pub_key.len() as u64, sub_buf);
            sub_buf.extend_from_slice(&tx_out.script_pub_key);
        }
        encode_key(COMPACT_TX_TAG_VOUT, WireType::LengthDelimited, tx_buf);
        encode_varint(sub_buf.len() as u64, tx_buf);
        tx_buf.extend_from_slice(sub_buf);
    }
}

fn write_vtx_wrapper(tx_buf: &[u8], out: &mut Vec<u8>) {
    encode_key(COMPACT_BLOCK_TAG_VTX, WireType::LengthDelimited, out);
    encode_varint(tx_buf.len() as u64, out);
    out.extend_from_slice(tx_buf);
}

const fn varint_len(mut num: u64) -> usize {
    let mut len = 1;
    while num >= 0x80 {
        num >>= 7;
        len += 1;
    }
    len
}

fn verify_mixed_tx_match(
    wire_tx: &WireTxMeta<'_>,
    sup_tx: &CompactTransparentSupplementTx,
) -> Result<(), SettledSupplementSegmentError> {
    let expected_txid = encode_internal_transaction_id(sup_tx.transaction_id);
    if wire_tx.txid_bytes != expected_txid {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "mixed transaction txid disagrees between shielded and supplement segments".to_owned(),
        ));
    }
    let expected_fee = fee_to_wire_u32(sup_tx.fee_zat)?;
    if wire_tx.fee != expected_fee {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "mixed transaction fee disagrees between shielded and supplement segments".to_owned(),
        ));
    }
    Ok(())
}

fn fee_to_wire_u32(fee_zat: Option<u64>) -> Result<u32, SettledSupplementSegmentError> {
    fee_zat
        .map(u32::try_from)
        .transpose()
        .map_err(|_| {
            SettledSupplementSegmentError::PayloadIntegrity(
                "supplement compact transaction fee exceeds u32".to_owned(),
            )
        })
        .map(Option::unwrap_or_default)
}

fn validate_transparent_supplement_order(
    transparent_supplement: &[CompactTransparentSupplementTx],
) -> Result<(), SettledSupplementSegmentError> {
    if transparent_supplement
        .windows(2)
        .any(|pair| pair[0].index >= pair[1].index)
    {
        return Err(SettledSupplementSegmentError::InvalidFormat(
            "transparent supplement transactions are not strictly ascending by index".to_owned(),
        ));
    }
    for sup_tx in transparent_supplement {
        fee_to_wire_u32(sup_tx.fee_zat)?;
    }
    Ok(())
}

/// Losslessly reconstructs a [`CompactBlockArtifact`] from a `ZSHRNG02` `Slab 0`
/// `CompactBlock` protobuf payload and its decoded `ZSTSUP01` [`BlockSupplementRecord`].
pub fn reconstruct_compact_block_artifact(
    shielded_payload: &[u8],
    supplement: &BlockSupplementRecord,
) -> Result<CompactBlockArtifact, SettledSupplementSegmentError> {
    validate_shielded_block_wire(shielded_payload)?;
    validate_transparent_supplement_order(&supplement.transparent_transactions)?;
    let proto_block = lightwalletd::CompactBlock::decode(shielded_payload).map_err(|err| {
        SettledSupplementSegmentError::PayloadIntegrity(format!(
            "failed to decode CompactBlock payload: {err}"
        ))
    })?;
    let height = u32::try_from(proto_block.height).map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity(
            "CompactBlock height exceeds u32".to_owned(),
        )
    })?;
    let block_hash = decode_internal_block_hash(&proto_block.hash).map_err(|err| {
        SettledSupplementSegmentError::PayloadIntegrity(format!("invalid block hash: {err}"))
    })?;
    let prev_hash = decode_internal_block_hash(&proto_block.prev_hash).map_err(|err| {
        SettledSupplementSegmentError::PayloadIntegrity(format!("invalid prev_hash: {err}"))
    })?;
    let meta = proto_block.chain_metadata.ok_or_else(|| {
        SettledSupplementSegmentError::PayloadIntegrity(
            "CompactBlock payload missing chain_metadata".to_owned(),
        )
    })?;
    let transactions = merge_reconstructed_transactions(
        &proto_block.vtx,
        &supplement.transparent_transactions,
        supplement.canonical_metadata.as_ref(),
    )?;
    CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(height), block_hash),
        prev_hash,
        proto_block.time,
        transactions,
        CompactChainMetadata {
            sapling_commitment_tree_size: meta.sapling_commitment_tree_size,
            orchard_commitment_tree_size: meta.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: meta.ironwood_commitment_tree_size,
        },
    )
    .map_err(|err| {
        SettledSupplementSegmentError::PayloadIntegrity(format!(
            "reconstructed CompactBlockArtifact transaction order invalid: {err}"
        ))
    })
}

fn merge_reconstructed_transactions(
    shielded_vtx: &[lightwalletd::CompactTx],
    transparent: &[CompactTransparentSupplementTx],
    canonical: Option<&CanonicalBlockSupplement>,
) -> Result<Vec<CompactTransaction>, SettledSupplementSegmentError> {
    let mut merged = Vec::with_capacity(shielded_vtx.len() + transparent.len());
    let mut sup_idx = 0_usize;
    let mut prev_shielded: Option<u64> = None;
    for proto_tx in shielded_vtx {
        if prev_shielded.is_some_and(|prev| proto_tx.index <= prev) {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "CompactBlock vtx indexes are not strictly ascending".to_owned(),
            ));
        }
        prev_shielded = Some(proto_tx.index);
        while sup_idx < transparent.len() && transparent[sup_idx].index < proto_tx.index {
            merged.push(transparent_only_compact_tx(&transparent[sup_idx]));
            sup_idx += 1;
        }
        let mixed_sup =
            if sup_idx < transparent.len() && transparent[sup_idx].index == proto_tx.index {
                let sup = &transparent[sup_idx];
                sup_idx += 1;
                Some(sup)
            } else {
                None
            };
        merged.push(reconstruct_shielded_or_mixed_tx(
            proto_tx, mixed_sup, canonical,
        )?);
    }
    while sup_idx < transparent.len() {
        merged.push(transparent_only_compact_tx(&transparent[sup_idx]));
        sup_idx += 1;
    }
    Ok(merged)
}

fn transparent_only_compact_tx(sup_tx: &CompactTransparentSupplementTx) -> CompactTransaction {
    CompactTransaction {
        index: sup_tx.index,
        transaction_id: sup_tx.transaction_id,
        data: CompactTransactionData {
            fee_zat: sup_tx.fee_zat,
            transparent_inputs: sup_tx.transparent_inputs.clone(),
            transparent_outputs: sup_tx.transparent_outputs.clone(),
            ..CompactTransactionData::default()
        },
    }
}

fn reconstruct_shielded_or_mixed_tx(
    proto_tx: &lightwalletd::CompactTx,
    mixed_sup: Option<&CompactTransparentSupplementTx>,
    canonical: Option<&CanonicalBlockSupplement>,
) -> Result<CompactTransaction, SettledSupplementSegmentError> {
    let transaction_id = decode_internal_transaction_id(&proto_tx.txid).map_err(|err| {
        SettledSupplementSegmentError::PayloadIntegrity(format!("invalid CompactTx.txid: {err}"))
    })?;
    let fee_zat = resolve_reconstructed_fee(proto_tx, transaction_id, mixed_sup, canonical)?;
    let (transparent_inputs, transparent_outputs) = match mixed_sup {
        Some(sup) => {
            if sup.transaction_id != transaction_id {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "mixed transaction txid disagrees on reconstruction".to_owned(),
                ));
            }
            (
                sup.transparent_inputs.clone(),
                sup.transparent_outputs.clone(),
            )
        }
        None => (Vec::new(), Vec::new()),
    };
    Ok(CompactTransaction {
        index: proto_tx.index,
        transaction_id,
        data: CompactTransactionData {
            fee_zat,
            sapling_spends: decode_sapling_spends(&proto_tx.spends)?,
            sapling_outputs: decode_sapling_outputs(&proto_tx.outputs)?,
            orchard_actions: decode_shielded_actions(&proto_tx.actions, "orchard")?,
            ironwood_actions: decode_shielded_actions(&proto_tx.ironwood_actions, "ironwood")?,
            transparent_inputs,
            transparent_outputs,
        },
    })
}

fn resolve_reconstructed_fee(
    proto_tx: &lightwalletd::CompactTx,
    transaction_id: zinder_core::TransactionId,
    mixed_sup: Option<&CompactTransparentSupplementTx>,
    canonical: Option<&CanonicalBlockSupplement>,
) -> Result<Option<u64>, SettledSupplementSegmentError> {
    if let Some(sup) = mixed_sup {
        if fee_to_wire_u32(sup.fee_zat)? != proto_tx.fee {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "mixed transaction fee disagrees on reconstruction".to_owned(),
            ));
        }
        return Ok(sup.fee_zat);
    }
    if let Some(meta) = canonical
        && let Some(meta_tx) = meta
            .transactions
            .iter()
            .find(|entry| entry.tx_index == proto_tx.index)
    {
        if meta_tx.public_facts.transaction_id != transaction_id {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "Tag 2 transaction_id disagrees with shielded CompactTx".to_owned(),
            ));
        }
        if let Some(tag2_fee) = meta_tx.fee_and_burn.fee_zat {
            if fee_to_wire_u32(Some(tag2_fee))? != proto_tx.fee {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "Tag 2 fee disagrees with shielded CompactTx".to_owned(),
                ));
            }
            return Ok(Some(tag2_fee));
        }
    }
    Ok((proto_tx.fee != 0).then(|| u64::from(proto_tx.fee)))
}

fn decode_sapling_spends(
    spends: &[lightwalletd::CompactSaplingSpend],
) -> Result<Vec<CompactSaplingSpend>, SettledSupplementSegmentError> {
    spends
        .iter()
        .map(|spend| {
            let nullifier = fixed_32(&spend.nf, "CompactSaplingSpend.nf")?;
            Ok(CompactSaplingSpend { nullifier })
        })
        .collect()
}

fn decode_sapling_outputs(
    outputs: &[lightwalletd::CompactSaplingOutput],
) -> Result<Vec<CompactSaplingOutput>, SettledSupplementSegmentError> {
    outputs
        .iter()
        .map(|output| {
            Ok(CompactSaplingOutput {
                commitment: fixed_32(&output.cmu, "CompactSaplingOutput.cmu")?,
                ephemeral_key: fixed_32(
                    &output.ephemeral_key,
                    "CompactSaplingOutput.ephemeralKey",
                )?,
                ciphertext: fixed_52(&output.ciphertext, "CompactSaplingOutput.ciphertext")?,
            })
        })
        .collect()
}

fn decode_shielded_actions(
    actions: &[lightwalletd::CompactOrchardAction],
    pool: &str,
) -> Result<Vec<CompactShieldedAction>, SettledSupplementSegmentError> {
    actions
        .iter()
        .map(|action| {
            Ok(CompactShieldedAction {
                nullifier: fixed_32(&action.nullifier, pool)?,
                commitment: fixed_32(&action.cmx, pool)?,
                ephemeral_key: fixed_32(&action.ephemeral_key, pool)?,
                ciphertext: fixed_52(&action.ciphertext, pool)?,
            })
        })
        .collect()
}

fn validate_shielded_block_wire(raw_payload: &[u8]) -> Result<(), SettledSupplementSegmentError> {
    let mut cursor = raw_payload;
    while !cursor.is_empty() {
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            COMPACT_BLOCK_TAG_HEIGHT | COMPACT_BLOCK_TAG_TIME => {
                expect_wire_type(wire_type, WireType::Varint, "CompactBlock scalar")?;
                decode_varint(&mut cursor).map_err(|err| wire_error(&err))?;
            }
            COMPACT_BLOCK_TAG_HASH | COMPACT_BLOCK_TAG_PREV_HASH | COMPACT_BLOCK_TAG_HEADER => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactBlock bytes")?;
                let _bytes = take_length_delimited(&mut cursor)?;
            }
            COMPACT_BLOCK_TAG_VTX => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactBlock.vtx")?;
                let tx_bytes = take_length_delimited(&mut cursor)?;
                inspect_and_validate_shielded_tx(tx_bytes)?;
            }
            COMPACT_BLOCK_TAG_CHAIN_METADATA => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "chain_metadata")?;
                let meta_bytes = take_length_delimited(&mut cursor)?;
                validate_chain_metadata_wire(meta_bytes)?;
            }
            _ => skip_wire_field(wire_type, &mut cursor)?,
        }
    }
    Ok(())
}

fn inspect_and_validate_shielded_tx(
    tx_bytes: &[u8],
) -> Result<WireTxMeta<'_>, SettledSupplementSegmentError> {
    let mut cursor = tx_bytes;
    let mut index = 0_u64;
    let mut fee = 0_u32;
    let mut txid_bytes: &[u8] = &[];
    let mut seen_txid = false;
    while !cursor.is_empty() {
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            COMPACT_TX_TAG_INDEX => {
                expect_wire_type(wire_type, WireType::Varint, "CompactTx.index")?;
                index = decode_varint(&mut cursor).map_err(|err| wire_error(&err))?;
            }
            COMPACT_TX_TAG_FEE => {
                expect_wire_type(wire_type, WireType::Varint, "CompactTx.fee")?;
                let raw_fee = decode_varint(&mut cursor).map_err(|err| wire_error(&err))?;
                fee = u32::try_from(raw_fee).map_err(|_| {
                    SettledSupplementSegmentError::PayloadIntegrity(
                        "CompactTx.fee exceeds u32".to_owned(),
                    )
                })?;
            }
            COMPACT_TX_TAG_TXID => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "CompactTx.txid")?;
                txid_bytes = take_length_delimited(&mut cursor)?;
                decode_internal_transaction_id(txid_bytes).map_err(|err| {
                    SettledSupplementSegmentError::PayloadIntegrity(format!(
                        "invalid CompactTx.txid: {err}"
                    ))
                })?;
                seen_txid = true;
            }
            COMPACT_TX_TAG_SPENDS => {
                validate_nested_field(wire_type, &mut cursor, validate_sapling_spend_wire)?;
            }
            COMPACT_TX_TAG_OUTPUTS => {
                validate_nested_field(wire_type, &mut cursor, validate_sapling_output_wire)?;
            }
            COMPACT_TX_TAG_ACTIONS | COMPACT_TX_TAG_IRONWOOD_ACTIONS => {
                validate_nested_field(wire_type, &mut cursor, validate_action_wire)?;
            }
            COMPACT_TX_TAG_VIN | COMPACT_TX_TAG_VOUT => {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "unexpected transparent field inside shielded payload".to_owned(),
                ));
            }
            _ => skip_wire_field(wire_type, &mut cursor)?,
        }
    }
    if !seen_txid {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "CompactTx missing txid".to_owned(),
        ));
    }
    Ok(WireTxMeta {
        index,
        txid_bytes,
        fee,
    })
}

fn validate_nested_field(
    wire_type: WireType,
    cursor: &mut &[u8],
    validator: fn(&[u8]) -> Result<(), SettledSupplementSegmentError>,
) -> Result<(), SettledSupplementSegmentError> {
    expect_wire_type(wire_type, WireType::LengthDelimited, "CompactTx nested")?;
    let nested = take_length_delimited(cursor)?;
    validator(nested)
}

fn validate_sapling_spend_wire(bytes: &[u8]) -> Result<(), SettledSupplementSegmentError> {
    let mut cursor = bytes;
    while !cursor.is_empty() {
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            SAPLING_SPEND_TAG_NF => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "spend.nf")?;
                let _nf = take_length_delimited(&mut cursor)?;
            }
            _ => skip_wire_field(wire_type, &mut cursor)?,
        }
    }
    Ok(())
}

fn validate_sapling_output_wire(bytes: &[u8]) -> Result<(), SettledSupplementSegmentError> {
    let mut cursor = bytes;
    while !cursor.is_empty() {
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            SAPLING_OUTPUT_TAG_CMU | SAPLING_OUTPUT_TAG_EPK | SAPLING_OUTPUT_TAG_CIPHERTEXT => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "sapling output field")?;
                let _bytes = take_length_delimited(&mut cursor)?;
            }
            _ => skip_wire_field(wire_type, &mut cursor)?,
        }
    }
    Ok(())
}

fn validate_action_wire(bytes: &[u8]) -> Result<(), SettledSupplementSegmentError> {
    let mut cursor = bytes;
    while !cursor.is_empty() {
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            ACTION_TAG_NULLIFIER
            | ACTION_TAG_CMX
            | ACTION_TAG_EPHEMERAL_KEY
            | ACTION_TAG_CIPHERTEXT => {
                expect_wire_type(wire_type, WireType::LengthDelimited, "action field")?;
                let _bytes = take_length_delimited(&mut cursor)?;
            }
            _ => skip_wire_field(wire_type, &mut cursor)?,
        }
    }
    Ok(())
}

fn validate_chain_metadata_wire(bytes: &[u8]) -> Result<(), SettledSupplementSegmentError> {
    let mut cursor = bytes;
    while !cursor.is_empty() {
        let (tag, wire_type) = decode_key(&mut cursor).map_err(|err| wire_error(&err))?;
        match tag {
            CHAIN_METADATA_TAG_SAPLING
            | CHAIN_METADATA_TAG_ORCHARD
            | CHAIN_METADATA_TAG_IRONWOOD => {
                expect_wire_type(wire_type, WireType::Varint, "ChainMetadata size")?;
                decode_varint(&mut cursor).map_err(|err| wire_error(&err))?;
            }
            _ => skip_wire_field(wire_type, &mut cursor)?,
        }
    }
    Ok(())
}

fn patch_frame_prefix(
    out: &mut [u8],
    frame_start: usize,
) -> Result<(), SettledSupplementSegmentError> {
    let payload_len = out.len() - frame_start - FRAME_PREFIX_LEN;
    let len_u32 = u32::try_from(payload_len).map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity(
            "spliced CompactBlock payload exceeds u32 frame prefix".to_owned(),
        )
    })?;
    out[frame_start] = 0;
    out[frame_start + 1..frame_start + FRAME_PREFIX_LEN].copy_from_slice(&len_u32.to_be_bytes());
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
) -> Result<(), SettledSupplementSegmentError> {
    if observed != expected {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "unexpected wire type for {field}"
        )));
    }
    Ok(())
}

fn take_length_delimited<'a>(
    cursor: &mut &'a [u8],
) -> Result<&'a [u8], SettledSupplementSegmentError> {
    let raw_len = decode_varint(cursor).map_err(|err| wire_error(&err))?;
    let len = usize::try_from(raw_len).map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity(
            "protobuf length-delimited field exceeds address space".to_owned(),
        )
    })?;
    take_slice(cursor, len)
}

fn take_slice<'a>(
    cursor: &mut &'a [u8],
    len: usize,
) -> Result<&'a [u8], SettledSupplementSegmentError> {
    let slice = cursor.get(..len).ok_or_else(|| {
        SettledSupplementSegmentError::PayloadIntegrity("truncated protobuf payload".to_owned())
    })?;
    *cursor = &cursor[len..];
    Ok(slice)
}

fn skip_wire_field(
    wire_type: WireType,
    cursor: &mut &[u8],
) -> Result<(), SettledSupplementSegmentError> {
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
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "unexpected protobuf group wire type in proto3 payload".to_owned(),
            ));
        }
    }
    Ok(())
}

fn fixed_32(bytes: &[u8], field: &str) -> Result<[u8; 32], SettledSupplementSegmentError> {
    bytes.try_into().map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity(format!(
            "expected 32 bytes for {field}, got {}",
            bytes.len()
        ))
    })
}

fn fixed_52(bytes: &[u8], field: &str) -> Result<[u8; 52], SettledSupplementSegmentError> {
    bytes.try_into().map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity(format!(
            "expected 52 bytes for {field}, got {}",
            bytes.len()
        ))
    })
}

fn wire_error(error: &prost::DecodeError) -> SettledSupplementSegmentError {
    SettledSupplementSegmentError::PayloadIntegrity(format!(
        "malformed CompactBlock protobuf wire encoding: {error}"
    ))
}
