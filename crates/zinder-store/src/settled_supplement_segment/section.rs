//! Versioned per-block tagged sections (`Tag 1` transparent supplement and
//! `Tag 2` extensible canonical metadata) inside `ZSTSUP01` decompressed records.

use zinder_core::{
    BlockHeight, CanonicalBlockFacts, CanonicalTransactionFacts, CompactBlockArtifact,
    CompactTransparentInput, CompactTransparentOutput, ConsensusBranchId, LockTime, PrivacyShape,
    SerializedBytesDigest, TransactionComponentCounts, TransactionId,
    TransactionIntrinsicValueBalances, TransactionPublicFacts, TransactionVersion,
    UnsupportedSection,
    wire::{
        decode_internal_auth_digest, decode_internal_block_hash, decode_internal_transaction_id,
        decode_internal_wtxid, encode_internal_auth_digest, encode_internal_transaction_id,
        encode_internal_wtxid,
    },
};

use super::{
    SettledSupplementSegmentError,
    format::{
        ROW_FLAG_HAS_CANONICAL_METADATA, ROW_FLAG_HAS_TRANSPARENT, ROW_FLAG_HAS_WIDE_FEE,
        ROW_KNOWN_FLAGS,
    },
};

const BLOCK_ENVELOPE_VERSION_V1: u8 = 1;
const BLOCK_ENVELOPE_HEADER_LEN: usize = 74;

const SECTION_TAG_TRANSPARENT_SUPPLEMENT: u16 = 1;
const SECTION_TAG_CANONICAL_METADATA: u16 = 2;
const SECTION_TAG_OPTIONAL_MASK: u16 = 0x8000;
const SECTION_VERSION_V1: u16 = 1;

const EXT_TAG_OPTIONAL_MASK: u16 = 0x8000;

/// Compact transparent transaction supplement stored in `ZSTSUP01` Tag 1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactTransparentSupplementTx {
    /// Block-local transaction index (`CompactTx.index`).
    pub index: u64,
    /// Consensus transaction identifier.
    pub transaction_id: TransactionId,
    /// Transaction fee in zatoshis, preserving `None` vs `Some(0)`.
    pub fee_zat: Option<u64>,
    /// Ordered transparent inputs (coinbase sentinel omitted).
    pub transparent_inputs: Vec<CompactTransparentInput>,
    /// Ordered transparent outputs.
    pub transparent_outputs: Vec<CompactTransparentOutput>,
}

/// Explicit optional shielded note-commitment-tree anchors stored per transaction.
///
/// Presence is always encoded with an explicit presence tag (`0` = absent,
/// `1` = present); an all-zero anchor is never used as an absence sentinel.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SupplementShieldedAnchors {
    /// Orchard bundle note-commitment-tree anchor, when present.
    pub orchard: Option<[u8; 32]>,
    /// Ironwood bundle note-commitment-tree anchor, when present.
    pub ironwood: Option<[u8; 32]>,
    /// Sapling note-commitment-tree anchor, when present.
    pub sapling: Option<[u8; 32]>,
}

/// Optional transaction-derived fee and burn amounts stored in Tag 2.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SupplementTxFeeAndBurn {
    /// Derived transaction fee in zatoshis, when known.
    pub fee_zat: Option<u64>,
    /// Explicit burn slot in zatoshis, when present.
    pub burn_zat: Option<u64>,
}

/// Extensible per-transaction canonical supplement metadata stored in Tag 2.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalTransactionSupplement {
    /// Block-local transaction index (`0` for coinbase).
    pub tx_index: u64,
    /// Scalar protocol facts parsed from the transaction.
    pub public_facts: TransactionPublicFacts,
    /// SHA-256 commitment to the exact serialized transaction bytes.
    pub serialized_bytes_digest: SerializedBytesDigest,
    /// Signed intrinsic value balances across shielded pools.
    pub intrinsic_value_balances: TransactionIntrinsicValueBalances,
    /// Explicit optional shielded anchors without all-zero sentinels.
    pub shielded_anchors: SupplementShieldedAnchors,
    /// Exact [ZIP-317](https://zips.z.cash/zip-0317) logical action count when
    /// all serialized transparent and shielded inputs to the formula are known.
    ///
    /// [`CanonicalBlockSupplement::from_canonical_facts`] populates `Some` when
    /// the transaction has no transparent inputs and matching transparent output
    /// facts supply exact `script_pub_key` byte lengths, and stores `None` when
    /// transparent inputs are present (since [`CanonicalBlockFacts`] omits
    /// `script_sig` byte lengths) or output facts are incomplete. Callers with
    /// full raw transaction bytes may populate `Some(exact_actions)` directly.
    pub zip317_logical_actions: Option<u32>,
    /// Transaction-derived fee and burn amounts.
    pub fee_and_burn: SupplementTxFeeAndBurn,
}

/// Extensible per-block canonical supplement metadata stored in Tag 2.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalBlockSupplement {
    /// SHA-256 commitment to the exact serialized block bytes.
    pub serialized_bytes_digest: SerializedBytesDigest,
    /// Ordered per-transaction canonical metadata entries.
    pub transactions: Vec<CanonicalTransactionSupplement>,
}

impl CanonicalBlockSupplement {
    /// Builds a [`CanonicalBlockSupplement`] from [`CanonicalBlockFacts`] and an
    /// optional matching [`CompactBlockArtifact`] for `fee_zat` slots.
    pub fn from_canonical_facts(
        facts: &CanonicalBlockFacts,
        compact_block: Option<&CompactBlockArtifact>,
    ) -> Result<Self, SettledSupplementSegmentError> {
        let compact_txs = compact_block.map_or(&[][..], CompactBlockArtifact::transactions);
        check_compact_facts_alignment(facts, compact_txs)?;
        let mut transactions = Vec::with_capacity(facts.transactions.len());
        let mut compact_cursor = 0_usize;
        for (idx, tx_facts) in facts.transactions.iter().enumerate() {
            let tx_index = u64::try_from(idx).map_err(|_| {
                SettledSupplementSegmentError::InvalidFormat(
                    "canonical transaction index exceeds u64".to_owned(),
                )
            })?;
            let fee_zat = advance_fee_lookup(compact_txs, &mut compact_cursor, tx_index);
            transactions.push(transaction_supplement_from_facts(
                tx_index, tx_facts, fee_zat,
            ));
        }
        Ok(Self {
            serialized_bytes_digest: facts.serialized_bytes_digest,
            transactions,
        })
    }
}

fn check_compact_facts_alignment(
    facts: &CanonicalBlockFacts,
    compact_txs: &[zinder_core::CompactTransaction],
) -> Result<(), SettledSupplementSegmentError> {
    for tx in compact_txs {
        let idx = usize::try_from(tx.index).map_err(|_| {
            SettledSupplementSegmentError::ChainLinkage(
                "compact transaction index exceeds host address space".to_owned(),
            )
        })?;
        let Some(fact_tx) = facts.transactions.get(idx) else {
            return Err(SettledSupplementSegmentError::ChainLinkage(format!(
                "canonical block facts missing compact transaction at index {}",
                tx.index
            )));
        };
        if fact_tx.public_facts.transaction_id != tx.transaction_id {
            return Err(SettledSupplementSegmentError::ChainLinkage(format!(
                "canonical block facts txid disagrees with compact transaction at index {}",
                tx.index
            )));
        }
    }
    Ok(())
}

/// Decoded per-block sections carried by one `ZSTSUP01` block record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockSupplementRecord {
    /// Tag-1 compact transparent supplement transactions ordered by `index`.
    pub transparent_transactions: Vec<CompactTransparentSupplementTx>,
    /// Optional Tag-2 extensible canonical block/transaction supplement metadata.
    pub canonical_metadata: Option<CanonicalBlockSupplement>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BlockEnvelopeIdentity {
    pub(super) height: BlockHeight,
    pub(super) block_hash_wire: [u8; 32],
    pub(super) prev_hash_wire: [u8; 32],
    pub(super) block_time: u32,
}

fn advance_fee_lookup(
    compact_txs: &[zinder_core::CompactTransaction],
    cursor: &mut usize,
    tx_index: u64,
) -> Option<u64> {
    while *cursor < compact_txs.len() && compact_txs[*cursor].index < tx_index {
        *cursor += 1;
    }
    if *cursor < compact_txs.len() && compact_txs[*cursor].index == tx_index {
        compact_txs[*cursor].data.fee_zat
    } else {
        None
    }
}

const ZIP317_OUTPUT_DIVISOR: u64 = 34;

const fn compact_size_len(len: u64) -> u64 {
    if len < 253 {
        1
    } else if len <= 0xFFFF {
        3
    } else if len <= 0xFFFF_FFFF {
        5
    } else {
        9
    }
}

pub(crate) fn exact_zip317_logical_actions_from_outputs(
    counts: TransactionComponentCounts,
    output_scripts: impl IntoIterator<Item = usize>,
) -> Option<u32> {
    if counts.transparent_input_count != 0 {
        return None;
    }
    let mut tx_out_total_size = 0_u64;
    let mut observed_outputs = 0_u32;
    for script_len in output_scripts {
        observed_outputs = observed_outputs.checked_add(1)?;
        let script_len_u64 = u64::try_from(script_len).ok()?;
        let output_bytes = 8_u64
            .checked_add(compact_size_len(script_len_u64))?
            .checked_add(script_len_u64)?;
        tx_out_total_size = tx_out_total_size.checked_add(output_bytes)?;
    }
    if observed_outputs != counts.transparent_output_count {
        return None;
    }
    let transparent_actions = tx_out_total_size.div_ceil(ZIP317_OUTPUT_DIVISOR);
    let sprout_actions = u64::from(counts.sprout_joinsplit_count).checked_mul(2)?;
    let sapling_actions = u64::from(counts.sapling_spend_count.max(counts.sapling_output_count));
    let total = transparent_actions
        .checked_add(sprout_actions)?
        .checked_add(sapling_actions)?
        .checked_add(u64::from(counts.orchard_action_count))?
        .checked_add(u64::from(counts.ironwood_action_count))?;
    u32::try_from(total).ok()
}

fn exact_zip317_logical_actions_from_facts(tx_facts: &CanonicalTransactionFacts) -> Option<u32> {
    if !tx_facts.transparent_inputs.is_empty() {
        return None;
    }
    exact_zip317_logical_actions_from_outputs(
        tx_facts.public_facts.counts,
        tx_facts
            .transparent_outputs
            .iter()
            .map(|out| out.script_pub_key.len()),
    )
}

fn transaction_supplement_from_facts(
    tx_index: u64,
    tx_facts: &CanonicalTransactionFacts,
    fee_zat: Option<u64>,
) -> CanonicalTransactionSupplement {
    CanonicalTransactionSupplement {
        tx_index,
        public_facts: tx_facts.public_facts.clone(),
        serialized_bytes_digest: tx_facts.serialized_bytes_digest,
        intrinsic_value_balances: tx_facts.intrinsic_value_balances,
        shielded_anchors: SupplementShieldedAnchors {
            orchard: tx_facts.public_facts.orchard_anchor,
            ironwood: None,
            sapling: None,
        },
        zip317_logical_actions: exact_zip317_logical_actions_from_facts(tx_facts),
        fee_and_burn: SupplementTxFeeAndBurn {
            fee_zat,
            burn_zat: None,
        },
    }
}

pub(crate) fn transparent_supplement_from_compact(
    block: &CompactBlockArtifact,
) -> Vec<CompactTransparentSupplementTx> {
    block
        .transactions()
        .iter()
        .filter(|tx| {
            let has_transparent =
                !tx.data.transparent_inputs.is_empty() || !tx.data.transparent_outputs.is_empty();
            let has_shielded = !tx.data.sapling_spends.is_empty()
                || !tx.data.sapling_outputs.is_empty()
                || !tx.data.orchard_actions.is_empty()
                || !tx.data.ironwood_actions.is_empty();
            has_transparent || tx.data.fee_zat.is_some() || !has_shielded
        })
        .map(|tx| CompactTransparentSupplementTx {
            index: tx.index,
            transaction_id: tx.transaction_id,
            fee_zat: tx.data.fee_zat,
            transparent_inputs: tx.data.transparent_inputs.clone(),
            transparent_outputs: tx.data.transparent_outputs.clone(),
        })
        .collect()
}

pub(super) fn encode_block_record(
    identity: BlockEnvelopeIdentity,
    record: &BlockSupplementRecord,
) -> Result<(Vec<u8>, u8), SettledSupplementSegmentError> {
    let mut encoded = Vec::with_capacity(BLOCK_ENVELOPE_HEADER_LEN + 64);
    encoded.push(BLOCK_ENVELOPE_VERSION_V1);
    encoded.push(0);
    encoded.extend_from_slice(&identity.height.value().to_be_bytes());
    encoded.extend_from_slice(&identity.block_hash_wire);
    encoded.extend_from_slice(&identity.prev_hash_wire);
    encoded.extend_from_slice(&identity.block_time.to_be_bytes());

    let tag1 = encode_tag1_section(&record.transparent_transactions)?;
    append_section(&mut encoded, SECTION_TAG_TRANSPARENT_SUPPLEMENT, &tag1)?;
    let mut row_flags = 0_u8;
    if !record.transparent_transactions.is_empty() {
        row_flags |= ROW_FLAG_HAS_TRANSPARENT;
    }
    if let Some(canonical) = &record.canonical_metadata {
        let tag2 = encode_tag2_section(canonical)?;
        append_section(&mut encoded, SECTION_TAG_CANONICAL_METADATA, &tag2)?;
        row_flags |= ROW_FLAG_HAS_CANONICAL_METADATA;
    }
    if record_has_wide_fee(
        &record.transparent_transactions,
        record.canonical_metadata.as_ref(),
    ) {
        row_flags |= ROW_FLAG_HAS_WIDE_FEE;
    }
    verify_decoded_record_consistency(
        &record.transparent_transactions,
        record.canonical_metadata.as_ref(),
        row_flags,
    )?;
    Ok((encoded, row_flags))
}

fn append_section(
    out: &mut Vec<u8>,
    tag: u16,
    payload: &[u8],
) -> Result<(), SettledSupplementSegmentError> {
    let len_u32 = u32::try_from(payload.len()).map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("supplement section exceeds u32".to_owned())
    })?;
    out.extend_from_slice(&tag.to_be_bytes());
    out.extend_from_slice(&SECTION_VERSION_V1.to_be_bytes());
    out.push(0);
    out.extend_from_slice(&len_u32.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

pub(super) fn decode_block_record(
    raw: &[u8],
    expected: BlockEnvelopeIdentity,
    expected_row_flags: u8,
) -> Result<BlockSupplementRecord, SettledSupplementSegmentError> {
    if (expected_row_flags & !ROW_KNOWN_FLAGS) != 0 {
        return Err(SettledSupplementSegmentError::InvalidFormat(format!(
            "unknown row flags {expected_row_flags:#04x}"
        )));
    }
    let mut cursor = raw;
    check_block_envelope_header(&mut cursor, expected)?;
    let (transparent_transactions, canonical_metadata) = decode_sections(&mut cursor)?;
    verify_decoded_record_consistency(
        &transparent_transactions,
        canonical_metadata.as_ref(),
        expected_row_flags,
    )?;
    Ok(BlockSupplementRecord {
        transparent_transactions,
        canonical_metadata,
    })
}

fn check_block_envelope_header(
    cursor: &mut &[u8],
    expected: BlockEnvelopeIdentity,
) -> Result<(), SettledSupplementSegmentError> {
    let version = read_u8(cursor, "envelope version")?;
    if version != BLOCK_ENVELOPE_VERSION_V1 {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "unsupported supplement block envelope version {version}"
        )));
    }
    let flags = read_u8(cursor, "envelope flags")?;
    if flags != 0 {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "unsupported supplement block envelope flags {flags:#04x}"
        )));
    }
    let height = read_u32(cursor, "envelope height")?;
    let block_hash = read_array::<32>(cursor, "envelope block hash")?;
    let prev_hash = read_array::<32>(cursor, "envelope prev hash")?;
    let block_time = read_u32(cursor, "envelope block time")?;
    if height != expected.height.value()
        || block_hash != expected.block_hash_wire
        || prev_hash != expected.prev_hash_wire
        || block_time != expected.block_time
    {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "supplement block envelope identity disagrees with row index".to_owned(),
        ));
    }
    Ok(())
}

fn decode_sections(
    cursor: &mut &[u8],
) -> Result<
    (
        Vec<CompactTransparentSupplementTx>,
        Option<CanonicalBlockSupplement>,
    ),
    SettledSupplementSegmentError,
> {
    let mut tag1: Option<Vec<CompactTransparentSupplementTx>> = None;
    let mut tag2: Option<CanonicalBlockSupplement> = None;
    while !cursor.is_empty() {
        let tag = read_u16(cursor, "section tag")?;
        let section_version = read_u16(cursor, "section version")?;
        let section_flags = read_u8(cursor, "section flags")?;
        if section_flags != 0 {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                "unsupported supplement section flags {section_flags:#04x}"
            )));
        }
        let payload_len = usize::try_from(read_u32(cursor, "section length")?).map_err(|_| {
            SettledSupplementSegmentError::PayloadIntegrity("section length overflow".to_owned())
        })?;
        let payload = take_slice(cursor, payload_len, "section payload")?;
        dispatch_section(tag, section_version, payload, &mut tag1, &mut tag2)?;
    }
    let transparent = tag1.ok_or_else(|| {
        SettledSupplementSegmentError::PayloadIntegrity(
            "supplement block record is missing mandatory Tag 1 section".to_owned(),
        )
    })?;
    Ok((transparent, tag2))
}

fn dispatch_section(
    tag: u16,
    section_version: u16,
    payload: &[u8],
    tag1: &mut Option<Vec<CompactTransparentSupplementTx>>,
    tag2: &mut Option<CanonicalBlockSupplement>,
) -> Result<(), SettledSupplementSegmentError> {
    match tag {
        SECTION_TAG_TRANSPARENT_SUPPLEMENT => {
            if tag1.is_some() {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "duplicate Tag 1 section in block record".to_owned(),
                ));
            }
            if section_version != SECTION_VERSION_V1 {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                    "unsupported Tag 1 section version {section_version}"
                )));
            }
            *tag1 = Some(decode_tag1_section(payload)?);
        }
        SECTION_TAG_CANONICAL_METADATA => {
            if tag2.is_some() {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "duplicate Tag 2 section in block record".to_owned(),
                ));
            }
            if section_version != SECTION_VERSION_V1 {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                    "unsupported Tag 2 section version {section_version}"
                )));
            }
            *tag2 = Some(decode_tag2_section(payload)?);
        }
        unknown if (unknown & SECTION_TAG_OPTIONAL_MASK) != 0 => {}
        mandatory => {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                "unknown mandatory supplement section tag {mandatory}"
            )));
        }
    }
    Ok(())
}

fn verify_decoded_record_consistency(
    transparent: &[CompactTransparentSupplementTx],
    canonical: Option<&CanonicalBlockSupplement>,
    expected_row_flags: u8,
) -> Result<(), SettledSupplementSegmentError> {
    let has_transparent = (expected_row_flags & ROW_FLAG_HAS_TRANSPARENT) != 0;
    let has_canonical = (expected_row_flags & ROW_FLAG_HAS_CANONICAL_METADATA) != 0;
    let has_wide_fee = (expected_row_flags & ROW_FLAG_HAS_WIDE_FEE) != 0;
    if transparent.is_empty() == has_transparent
        || canonical.is_some() != has_canonical
        || record_has_wide_fee(transparent, canonical) != has_wide_fee
    {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "supplement block sections disagree with row index flags".to_owned(),
        ));
    }
    let Some(canonical_block) = canonical else {
        return Ok(());
    };
    let mut meta_idx = 0_usize;
    let metas = &canonical_block.transactions;
    for sup_tx in transparent {
        while meta_idx < metas.len() && metas[meta_idx].tx_index < sup_tx.index {
            meta_idx += 1;
        }
        if meta_idx < metas.len() && metas[meta_idx].tx_index == sup_tx.index {
            if metas[meta_idx].public_facts.transaction_id != sup_tx.transaction_id {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "Tag 1 and Tag 2 disagree on transaction_id".to_owned(),
                ));
            }
            if let (Some(sup_fee), Some(tag2_fee)) =
                (sup_tx.fee_zat, metas[meta_idx].fee_and_burn.fee_zat)
                && tag2_fee != sup_fee
            {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "Tag 1 and Tag 2 disagree on fee_zat".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn record_has_wide_fee(
    transparent: &[CompactTransparentSupplementTx],
    canonical: Option<&CanonicalBlockSupplement>,
) -> bool {
    transparent
        .iter()
        .any(|tx| tx.fee_zat.is_some_and(|fee| u32::try_from(fee).is_err()))
        || canonical.is_some_and(|canonical_block| {
            canonical_block.transactions.iter().any(|tx| {
                tx.fee_and_burn
                    .fee_zat
                    .is_some_and(|fee| u32::try_from(fee).is_err())
            })
        })
}

fn encode_tag1_section(
    txs: &[CompactTransparentSupplementTx],
) -> Result<Vec<u8>, SettledSupplementSegmentError> {
    let tx_count = u32::try_from(txs.len()).map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("Tag 1 transaction count overflow".to_owned())
    })?;
    let mut out = Vec::with_capacity(4 + txs.len() * 48);
    out.extend_from_slice(&tx_count.to_be_bytes());
    let mut prev_index: Option<u64> = None;
    for tx in txs {
        if prev_index.is_some_and(|prev| tx.index <= prev) {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "Tag 1 transactions must be strictly ascending by index".to_owned(),
            ));
        }
        prev_index = Some(tx.index);
        encode_tag1_tx(&mut out, tx)?;
    }
    Ok(out)
}

fn encode_tag1_tx(
    out: &mut Vec<u8>,
    tx: &CompactTransparentSupplementTx,
) -> Result<(), SettledSupplementSegmentError> {
    out.extend_from_slice(&tx.index.to_be_bytes());
    out.extend_from_slice(&encode_internal_transaction_id(tx.transaction_id));
    encode_optional_u64(out, tx.fee_zat);
    let vin_count = u32::try_from(tx.transparent_inputs.len()).map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("Tag 1 vin count overflow".to_owned())
    })?;
    out.extend_from_slice(&vin_count.to_be_bytes());
    for tx_in in &tx.transparent_inputs {
        out.extend_from_slice(&encode_internal_transaction_id(
            tx_in.previous_transaction_id,
        ));
        out.extend_from_slice(&tx_in.previous_output_index.to_be_bytes());
    }
    let vout_count = u32::try_from(tx.transparent_outputs.len()).map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("Tag 1 vout count overflow".to_owned())
    })?;
    out.extend_from_slice(&vout_count.to_be_bytes());
    for tx_out in &tx.transparent_outputs {
        out.extend_from_slice(&tx_out.value_zat.to_be_bytes());
        let script_len = u32::try_from(tx_out.script_pub_key.len()).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("scriptPubKey length overflow".to_owned())
        })?;
        out.extend_from_slice(&script_len.to_be_bytes());
        out.extend_from_slice(&tx_out.script_pub_key);
    }
    Ok(())
}

fn decode_tag1_section(
    payload: &[u8],
) -> Result<Vec<CompactTransparentSupplementTx>, SettledSupplementSegmentError> {
    let mut cursor = payload;
    let tx_count = usize::try_from(read_u32(&mut cursor, "Tag 1 tx count")?).map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity("Tag 1 tx count overflow".to_owned())
    })?;
    let min_tx_bytes = 8 + 32 + 1 + 4 + 4;
    if tx_count > cursor.len() / min_tx_bytes {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "Tag 1 tx count exceeds remaining section bytes".to_owned(),
        ));
    }
    let mut txs = Vec::with_capacity(tx_count);
    let mut prev_index: Option<u64> = None;
    for _ in 0..tx_count {
        let tx = decode_tag1_tx(&mut cursor)?;
        if prev_index.is_some_and(|prev| tx.index <= prev) {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "Tag 1 transactions are not strictly ascending by index".to_owned(),
            ));
        }
        prev_index = Some(tx.index);
        txs.push(tx);
    }
    reject_trailing_bytes(cursor, "Tag 1")?;
    Ok(txs)
}

fn decode_tag1_tx(
    cursor: &mut &[u8],
) -> Result<CompactTransparentSupplementTx, SettledSupplementSegmentError> {
    let index = read_u64(cursor, "Tag 1 tx index")?;
    let txid_bytes = read_array::<32>(cursor, "Tag 1 txid")?;
    let transaction_id = decode_internal_transaction_id(&txid_bytes).map_err(|err| {
        SettledSupplementSegmentError::PayloadIntegrity(format!("invalid Tag 1 txid: {err}"))
    })?;
    let fee_zat = read_optional_u64(cursor, "Tag 1 fee_zat")?;
    let transparent_inputs = decode_tag1_inputs(cursor)?;
    let transparent_outputs = decode_tag1_outputs(cursor)?;
    Ok(CompactTransparentSupplementTx {
        index,
        transaction_id,
        fee_zat,
        transparent_inputs,
        transparent_outputs,
    })
}

fn decode_tag1_inputs(
    cursor: &mut &[u8],
) -> Result<Vec<CompactTransparentInput>, SettledSupplementSegmentError> {
    let vin_count = usize::try_from(read_u32(cursor, "Tag 1 vin count")?).map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity("Tag 1 vin count overflow".to_owned())
    })?;
    let entry_bytes = 36_usize;
    if vin_count > cursor.len() / entry_bytes {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "Tag 1 vin count exceeds remaining section bytes".to_owned(),
        ));
    }
    let mut inputs = Vec::with_capacity(vin_count);
    for _ in 0..vin_count {
        let prev_bytes = read_array::<32>(cursor, "Tag 1 prevout_txid")?;
        let previous_transaction_id =
            decode_internal_transaction_id(&prev_bytes).map_err(|err| {
                SettledSupplementSegmentError::PayloadIntegrity(format!(
                    "invalid Tag 1 prevout_txid: {err}"
                ))
            })?;
        let previous_output_index = read_u32(cursor, "Tag 1 prevout_index")?;
        inputs.push(CompactTransparentInput {
            previous_transaction_id,
            previous_output_index,
        });
    }
    Ok(inputs)
}

fn decode_tag1_outputs(
    cursor: &mut &[u8],
) -> Result<Vec<CompactTransparentOutput>, SettledSupplementSegmentError> {
    let vout_count = usize::try_from(read_u32(cursor, "Tag 1 vout count")?).map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity("Tag 1 vout count overflow".to_owned())
    })?;
    let min_entry_bytes = 12_usize;
    if vout_count > cursor.len() / min_entry_bytes {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "Tag 1 vout count exceeds remaining section bytes".to_owned(),
        ));
    }
    let mut outputs = Vec::with_capacity(vout_count);
    for _ in 0..vout_count {
        let amount_zat = read_u64(cursor, "Tag 1 vout value_zat")?;
        let script_len =
            usize::try_from(read_u32(cursor, "Tag 1 script length")?).map_err(|_| {
                SettledSupplementSegmentError::PayloadIntegrity(
                    "Tag 1 script length overflow".to_owned(),
                )
            })?;
        let script_slice = take_slice(cursor, script_len, "Tag 1 script_pub_key")?;
        outputs.push(CompactTransparentOutput {
            value_zat: amount_zat,
            script_pub_key: script_slice.to_vec(),
        });
    }
    Ok(outputs)
}

fn encode_tag2_section(
    canonical: &CanonicalBlockSupplement,
) -> Result<Vec<u8>, SettledSupplementSegmentError> {
    let tx_count = u32::try_from(canonical.transactions.len()).map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("Tag 2 transaction count overflow".to_owned())
    })?;
    let mut out = Vec::with_capacity(38 + canonical.transactions.len() * 128);
    out.extend_from_slice(&canonical.serialized_bytes_digest.as_bytes());
    out.extend_from_slice(&tx_count.to_be_bytes());
    let mut prev_index: Option<u64> = None;
    for tx in &canonical.transactions {
        if prev_index.is_some_and(|prev| tx.tx_index <= prev) {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "Tag 2 transactions must be strictly ascending by index".to_owned(),
            ));
        }
        if tx.public_facts.orchard_anchor != tx.shielded_anchors.orchard {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "Tag 2 public_facts.orchard_anchor disagrees with shielded_anchors".to_owned(),
            ));
        }
        prev_index = Some(tx.tx_index);
        encode_tag2_tx(&mut out, tx)?;
    }
    out.extend_from_slice(&0_u16.to_be_bytes());
    Ok(out)
}

fn encode_tag2_tx(
    out: &mut Vec<u8>,
    tx: &CanonicalTransactionSupplement,
) -> Result<(), SettledSupplementSegmentError> {
    out.extend_from_slice(&tx.tx_index.to_be_bytes());
    encode_public_facts(out, &tx.public_facts)?;
    out.extend_from_slice(&tx.serialized_bytes_digest.as_bytes());
    encode_intrinsic_balances(out, tx.intrinsic_value_balances);
    encode_shielded_anchors(out, tx.shielded_anchors);
    encode_optional_u32(out, tx.zip317_logical_actions);
    encode_optional_u64(out, tx.fee_and_burn.fee_zat);
    encode_optional_u64(out, tx.fee_and_burn.burn_zat);
    out.extend_from_slice(&0_u16.to_be_bytes());
    Ok(())
}

fn encode_public_facts(
    out: &mut Vec<u8>,
    facts: &TransactionPublicFacts,
) -> Result<(), SettledSupplementSegmentError> {
    out.extend_from_slice(&encode_internal_transaction_id(facts.transaction_id));
    encode_optional_fixed(out, facts.auth_digest.map(encode_internal_auth_digest));
    encode_optional_fixed(out, facts.wtxid.map(encode_internal_wtxid));
    encode_tx_version(out, facts.version);
    encode_optional_u32(out, facts.consensus_branch_id.map(ConsensusBranchId::value));
    encode_lock_time(out, facts.lock_time);
    encode_optional_u32(out, facts.expiry_height.map(BlockHeight::value));
    out.extend_from_slice(&facts.size_bytes.to_be_bytes());
    encode_counts(out, facts.counts);
    encode_optional_i64(out, facts.orchard_value_balance_zat);
    encode_optional_i64(out, facts.ironwood_value_balance_zat);
    out.push(privacy_shape_code(facts.privacy_shape));
    out.push(u8::from(facts.is_coinbase));
    encode_unsupported_sections(out, &facts.unsupported_sections)
}

fn encode_tx_version(out: &mut Vec<u8>, version: TransactionVersion) {
    match version {
        TransactionVersion::V1 => out.push(1),
        TransactionVersion::V2 => out.push(2),
        TransactionVersion::V3 => out.push(3),
        TransactionVersion::V4 => out.push(4),
        TransactionVersion::V5 => out.push(5),
        TransactionVersion::V6 => out.push(6),
        TransactionVersion::Unsupported {
            effective_version,
            version_group_id,
        } => {
            out.push(7);
            out.extend_from_slice(&effective_version.to_be_bytes());
            encode_optional_u32(out, version_group_id);
        }
    }
}

fn encode_lock_time(out: &mut Vec<u8>, lock_time: LockTime) {
    match lock_time {
        LockTime::Unlocked => out.push(1),
        LockTime::Height(height) => {
            out.push(2);
            out.extend_from_slice(&height.value().to_be_bytes());
        }
        LockTime::UnixSeconds(seconds) => {
            out.push(3);
            out.extend_from_slice(&seconds.to_be_bytes());
        }
    }
}

fn encode_counts(out: &mut Vec<u8>, counts: TransactionComponentCounts) {
    out.extend_from_slice(&counts.transparent_input_count.to_be_bytes());
    out.extend_from_slice(&counts.transparent_output_count.to_be_bytes());
    out.extend_from_slice(&counts.sapling_spend_count.to_be_bytes());
    out.extend_from_slice(&counts.sapling_output_count.to_be_bytes());
    out.extend_from_slice(&counts.orchard_action_count.to_be_bytes());
    out.extend_from_slice(&counts.ironwood_action_count.to_be_bytes());
    out.extend_from_slice(&counts.sprout_joinsplit_count.to_be_bytes());
}

const fn privacy_shape_code(shape: PrivacyShape) -> u8 {
    match shape {
        PrivacyShape::TransparentOnly => 1,
        PrivacyShape::Shielding => 2,
        PrivacyShape::Deshielding => 3,
        PrivacyShape::ShieldedOnly => 4,
        PrivacyShape::Mixed => 5,
        PrivacyShape::Coinbase => 6,
        PrivacyShape::ShieldedCoinbase => 7,
        PrivacyShape::Unclassified => 8,
    }
}

fn encode_unsupported_sections(
    out: &mut Vec<u8>,
    sections: &[UnsupportedSection],
) -> Result<(), SettledSupplementSegmentError> {
    let count = u32::try_from(sections.len()).map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat(
            "unsupported section count overflow".to_owned(),
        )
    })?;
    out.extend_from_slice(&count.to_be_bytes());
    for section in sections {
        #[allow(
            unreachable_patterns,
            clippy::wildcard_enum_match_arm,
            reason = "UnsupportedSection is #[non_exhaustive] in zinder-core; fail closed on future variants."
        )]
        let code = match section {
            UnsupportedSection::FutureVersionHeader => 1_u8,
            UnsupportedSection::FutureShieldedProtocol => 2_u8,
            _ => {
                return Err(SettledSupplementSegmentError::InvalidFormat(
                    "unrecognized UnsupportedSection variant".to_owned(),
                ));
            }
        };
        out.push(code);
    }
    Ok(())
}

fn encode_intrinsic_balances(out: &mut Vec<u8>, balances: TransactionIntrinsicValueBalances) {
    out.extend_from_slice(&balances.sprout_zat.to_be_bytes());
    out.extend_from_slice(&balances.sapling_zat.to_be_bytes());
    out.extend_from_slice(&balances.orchard_zat.to_be_bytes());
    out.extend_from_slice(&balances.ironwood_zat.to_be_bytes());
}

fn encode_shielded_anchors(out: &mut Vec<u8>, anchors: SupplementShieldedAnchors) {
    encode_optional_fixed(out, anchors.orchard);
    encode_optional_fixed(out, anchors.ironwood);
    encode_optional_fixed(out, anchors.sapling);
}

fn decode_tag2_section(
    payload: &[u8],
) -> Result<CanonicalBlockSupplement, SettledSupplementSegmentError> {
    let mut cursor = payload;
    let block_digest =
        SerializedBytesDigest::from_bytes(read_array::<32>(&mut cursor, "Tag 2 block digest")?);
    let tx_count = usize::try_from(read_u32(&mut cursor, "Tag 2 tx count")?).map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity("Tag 2 tx count overflow".to_owned())
    })?;
    let min_tx_bytes = 96_usize;
    if tx_count > cursor.len() / min_tx_bytes {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "Tag 2 tx count exceeds remaining section bytes".to_owned(),
        ));
    }
    let mut transactions = Vec::with_capacity(tx_count);
    let mut prev_index: Option<u64> = None;
    for _ in 0..tx_count {
        let tx = decode_tag2_tx(&mut cursor)?;
        if prev_index.is_some_and(|prev| tx.tx_index <= prev) {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "Tag 2 transactions are not strictly ascending by index".to_owned(),
            ));
        }
        prev_index = Some(tx.tx_index);
        transactions.push(tx);
    }
    skip_ext_tlvs(&mut cursor, "Tag 2 block")?;
    reject_trailing_bytes(cursor, "Tag 2")?;
    Ok(CanonicalBlockSupplement {
        serialized_bytes_digest: block_digest,
        transactions,
    })
}

fn decode_tag2_tx(
    cursor: &mut &[u8],
) -> Result<CanonicalTransactionSupplement, SettledSupplementSegmentError> {
    let tx_index = read_u64(cursor, "Tag 2 tx_index")?;
    let mut public_facts = decode_public_facts(cursor)?;
    let serialized_bytes_digest =
        SerializedBytesDigest::from_bytes(read_array::<32>(cursor, "Tag 2 tx digest")?);
    let intrinsic_value_balances = TransactionIntrinsicValueBalances::new(
        read_i64(cursor, "Tag 2 sprout_zat")?,
        read_i64(cursor, "Tag 2 sapling_zat")?,
        read_i64(cursor, "Tag 2 orchard_zat")?,
        read_i64(cursor, "Tag 2 ironwood_zat")?,
    );
    let shielded_anchors = SupplementShieldedAnchors {
        orchard: read_optional_fixed::<32>(cursor, "Tag 2 orchard_anchor")?,
        ironwood: read_optional_fixed::<32>(cursor, "Tag 2 ironwood_anchor")?,
        sapling: read_optional_fixed::<32>(cursor, "Tag 2 sapling_anchor")?,
    };
    public_facts.orchard_anchor = shielded_anchors.orchard;
    let zip317_logical_actions = read_optional_u32(cursor, "Tag 2 zip317_logical_actions")?;
    let fee_and_burn = SupplementTxFeeAndBurn {
        fee_zat: read_optional_u64(cursor, "Tag 2 fee_zat")?,
        burn_zat: read_optional_u64(cursor, "Tag 2 burn_zat")?,
    };
    skip_ext_tlvs(cursor, "Tag 2 tx")?;
    Ok(CanonicalTransactionSupplement {
        tx_index,
        public_facts,
        serialized_bytes_digest,
        intrinsic_value_balances,
        shielded_anchors,
        zip317_logical_actions,
        fee_and_burn,
    })
}

fn decode_public_facts(
    cursor: &mut &[u8],
) -> Result<TransactionPublicFacts, SettledSupplementSegmentError> {
    let txid_bytes = read_array::<32>(cursor, "Tag 2 transaction_id")?;
    let transaction_id = decode_internal_transaction_id(&txid_bytes).map_err(|err| {
        SettledSupplementSegmentError::PayloadIntegrity(format!("invalid Tag 2 txid: {err}"))
    })?;
    let auth_digest = read_optional_fixed::<32>(cursor, "Tag 2 auth_digest")?
        .map(|bytes| {
            decode_internal_auth_digest(&bytes).map_err(|err| {
                SettledSupplementSegmentError::PayloadIntegrity(format!(
                    "invalid Tag 2 auth_digest: {err}"
                ))
            })
        })
        .transpose()?;
    let wtxid = read_optional_fixed::<64>(cursor, "Tag 2 wtxid")?
        .map(|bytes| {
            decode_internal_wtxid(&bytes).map_err(|err| {
                SettledSupplementSegmentError::PayloadIntegrity(format!(
                    "invalid Tag 2 wtxid: {err}"
                ))
            })
        })
        .transpose()?;
    let version = decode_tx_version(cursor)?;
    let consensus_branch_id =
        read_optional_u32(cursor, "Tag 2 consensus_branch_id")?.map(ConsensusBranchId::new);
    let lock_time = decode_lock_time(cursor)?;
    let expiry_height = read_optional_u32(cursor, "Tag 2 expiry_height")?.map(BlockHeight::new);
    let size_bytes = read_u32(cursor, "Tag 2 size_bytes")?;
    let counts = decode_counts(cursor)?;
    let orchard_value_balance_zat = read_optional_i64(cursor, "Tag 2 orchard_value_balance_zat")?;
    let ironwood_value_balance_zat = read_optional_i64(cursor, "Tag 2 ironwood_value_balance_zat")?;
    let privacy_shape = decode_privacy_shape(read_u8(cursor, "Tag 2 privacy_shape")?)?;
    let is_coinbase = read_bool(cursor, "Tag 2 is_coinbase")?;
    let unsupported_sections = decode_unsupported_sections(cursor)?;
    Ok(TransactionPublicFacts {
        transaction_id,
        auth_digest,
        wtxid,
        version,
        consensus_branch_id,
        lock_time,
        expiry_height,
        size_bytes,
        counts,
        orchard_value_balance_zat,
        orchard_anchor: None,
        ironwood_value_balance_zat,
        privacy_shape,
        is_coinbase,
        unsupported_sections,
    })
}

fn decode_tx_version(
    cursor: &mut &[u8],
) -> Result<TransactionVersion, SettledSupplementSegmentError> {
    match read_u8(cursor, "Tag 2 tx version kind")? {
        1 => Ok(TransactionVersion::V1),
        2 => Ok(TransactionVersion::V2),
        3 => Ok(TransactionVersion::V3),
        4 => Ok(TransactionVersion::V4),
        5 => Ok(TransactionVersion::V5),
        6 => Ok(TransactionVersion::V6),
        7 => Ok(TransactionVersion::Unsupported {
            effective_version: read_u32(cursor, "Tag 2 unsupported effective_version")?,
            version_group_id: read_optional_u32(cursor, "Tag 2 unsupported version_group_id")?,
        }),
        kind => Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "unknown Tag 2 transaction version kind {kind}"
        ))),
    }
}

fn decode_lock_time(cursor: &mut &[u8]) -> Result<LockTime, SettledSupplementSegmentError> {
    match read_u8(cursor, "Tag 2 lock_time kind")? {
        1 => Ok(LockTime::Unlocked),
        2 => Ok(LockTime::Height(BlockHeight::new(read_u32(
            cursor,
            "Tag 2 lock_time height",
        )?))),
        3 => Ok(LockTime::UnixSeconds(read_u64(
            cursor,
            "Tag 2 lock_time unix_seconds",
        )?)),
        kind => Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "unknown Tag 2 lock_time kind {kind}"
        ))),
    }
}

fn decode_counts(
    cursor: &mut &[u8],
) -> Result<TransactionComponentCounts, SettledSupplementSegmentError> {
    Ok(TransactionComponentCounts {
        transparent_input_count: read_u32(cursor, "Tag 2 transparent_input_count")?,
        transparent_output_count: read_u32(cursor, "Tag 2 transparent_output_count")?,
        sapling_spend_count: read_u32(cursor, "Tag 2 sapling_spend_count")?,
        sapling_output_count: read_u32(cursor, "Tag 2 sapling_output_count")?,
        orchard_action_count: read_u32(cursor, "Tag 2 orchard_action_count")?,
        ironwood_action_count: read_u32(cursor, "Tag 2 ironwood_action_count")?,
        sprout_joinsplit_count: read_u32(cursor, "Tag 2 sprout_joinsplit_count")?,
    })
}

fn decode_privacy_shape(code: u8) -> Result<PrivacyShape, SettledSupplementSegmentError> {
    match code {
        1 => Ok(PrivacyShape::TransparentOnly),
        2 => Ok(PrivacyShape::Shielding),
        3 => Ok(PrivacyShape::Deshielding),
        4 => Ok(PrivacyShape::ShieldedOnly),
        5 => Ok(PrivacyShape::Mixed),
        6 => Ok(PrivacyShape::Coinbase),
        7 => Ok(PrivacyShape::ShieldedCoinbase),
        8 => Ok(PrivacyShape::Unclassified),
        _ => Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "unknown Tag 2 privacy_shape discriminant {code}"
        ))),
    }
}

fn decode_unsupported_sections(
    cursor: &mut &[u8],
) -> Result<Vec<UnsupportedSection>, SettledSupplementSegmentError> {
    let count =
        usize::try_from(read_u32(cursor, "Tag 2 unsupported_sections count")?).map_err(|_| {
            SettledSupplementSegmentError::PayloadIntegrity(
                "Tag 2 unsupported_sections count overflow".to_owned(),
            )
        })?;
    if count > cursor.len() {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "Tag 2 unsupported_sections count exceeds remaining bytes".to_owned(),
        ));
    }
    let mut sections = Vec::with_capacity(count);
    for _ in 0..count {
        match read_u8(cursor, "Tag 2 unsupported_section")? {
            1 => sections.push(UnsupportedSection::FutureVersionHeader),
            2 => sections.push(UnsupportedSection::FutureShieldedProtocol),
            code => {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                    "unknown Tag 2 unsupported_section discriminant {code}"
                )));
            }
        }
    }
    Ok(sections)
}

fn skip_ext_tlvs(cursor: &mut &[u8], label: &str) -> Result<(), SettledSupplementSegmentError> {
    let count = usize::from(read_u16(cursor, "ext TLV count")?);
    let min_ext_bytes = 6_usize;
    if count > cursor.len() / min_ext_bytes {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "{label} ext TLV count exceeds remaining bytes"
        )));
    }
    for _ in 0..count {
        let ext_tag = read_u16(cursor, "ext TLV tag")?;
        let ext_len = usize::try_from(read_u32(cursor, "ext TLV length")?).map_err(|_| {
            SettledSupplementSegmentError::PayloadIntegrity("ext TLV length overflow".to_owned())
        })?;
        let _ext_payload = take_slice(cursor, ext_len, "ext TLV payload")?;
        if (ext_tag & EXT_TAG_OPTIONAL_MASK) == 0 {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                "unknown mandatory {label} extension tag {ext_tag}"
            )));
        }
    }
    Ok(())
}

pub(super) fn block_hash_wire_to_domain(
    wire: &[u8; 32],
) -> Result<zinder_core::BlockHash, SettledSupplementSegmentError> {
    decode_internal_block_hash(wire).map_err(|err| {
        SettledSupplementSegmentError::PayloadIntegrity(format!("invalid wire block hash: {err}"))
    })
}

fn encode_optional_fixed<const N: usize>(out: &mut Vec<u8>, maybe: Option<[u8; N]>) {
    match maybe {
        None => out.push(0),
        Some(bytes) => {
            out.push(1);
            out.extend_from_slice(&bytes);
        }
    }
}

fn encode_optional_u32(out: &mut Vec<u8>, maybe: Option<u32>) {
    match maybe {
        None => out.push(0),
        Some(num) => {
            out.push(1);
            out.extend_from_slice(&num.to_be_bytes());
        }
    }
}

fn encode_optional_u64(out: &mut Vec<u8>, maybe: Option<u64>) {
    match maybe {
        None => out.push(0),
        Some(num) => {
            out.push(1);
            out.extend_from_slice(&num.to_be_bytes());
        }
    }
}

fn encode_optional_i64(out: &mut Vec<u8>, maybe: Option<i64>) {
    match maybe {
        None => out.push(0),
        Some(num) => {
            out.push(1);
            out.extend_from_slice(&num.to_be_bytes());
        }
    }
}

fn read_presence_tag(
    cursor: &mut &[u8],
    label: &str,
) -> Result<bool, SettledSupplementSegmentError> {
    match read_u8(cursor, label)? {
        0 => Ok(false),
        1 => Ok(true),
        flag => Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "invalid presence tag {flag} for {label}"
        ))),
    }
}

fn read_bool(cursor: &mut &[u8], label: &str) -> Result<bool, SettledSupplementSegmentError> {
    read_presence_tag(cursor, label)
}

fn read_optional_fixed<const N: usize>(
    cursor: &mut &[u8],
    label: &str,
) -> Result<Option<[u8; N]>, SettledSupplementSegmentError> {
    if read_presence_tag(cursor, label)? {
        Ok(Some(read_array::<N>(cursor, label)?))
    } else {
        Ok(None)
    }
}

fn read_optional_u32(
    cursor: &mut &[u8],
    label: &str,
) -> Result<Option<u32>, SettledSupplementSegmentError> {
    if read_presence_tag(cursor, label)? {
        Ok(Some(read_u32(cursor, label)?))
    } else {
        Ok(None)
    }
}

fn read_optional_u64(
    cursor: &mut &[u8],
    label: &str,
) -> Result<Option<u64>, SettledSupplementSegmentError> {
    if read_presence_tag(cursor, label)? {
        Ok(Some(read_u64(cursor, label)?))
    } else {
        Ok(None)
    }
}

fn read_optional_i64(
    cursor: &mut &[u8],
    label: &str,
) -> Result<Option<i64>, SettledSupplementSegmentError> {
    if read_presence_tag(cursor, label)? {
        Ok(Some(read_i64(cursor, label)?))
    } else {
        Ok(None)
    }
}

fn read_u8(cursor: &mut &[u8], label: &str) -> Result<u8, SettledSupplementSegmentError> {
    Ok(read_array::<1>(cursor, label)?[0])
}

fn read_u16(cursor: &mut &[u8], label: &str) -> Result<u16, SettledSupplementSegmentError> {
    Ok(u16::from_be_bytes(read_array::<2>(cursor, label)?))
}

fn read_u32(cursor: &mut &[u8], label: &str) -> Result<u32, SettledSupplementSegmentError> {
    Ok(u32::from_be_bytes(read_array::<4>(cursor, label)?))
}

fn read_u64(cursor: &mut &[u8], label: &str) -> Result<u64, SettledSupplementSegmentError> {
    Ok(u64::from_be_bytes(read_array::<8>(cursor, label)?))
}

fn read_i64(cursor: &mut &[u8], label: &str) -> Result<i64, SettledSupplementSegmentError> {
    Ok(i64::from_be_bytes(read_array::<8>(cursor, label)?))
}

fn read_array<const N: usize>(
    cursor: &mut &[u8],
    label: &str,
) -> Result<[u8; N], SettledSupplementSegmentError> {
    let slice = take_slice(cursor, N, label)?;
    let mut array = [0_u8; N];
    array.copy_from_slice(slice);
    Ok(array)
}

fn take_slice<'a>(
    cursor: &mut &'a [u8],
    len: usize,
    label: &str,
) -> Result<&'a [u8], SettledSupplementSegmentError> {
    let slice = cursor.get(..len).ok_or_else(|| {
        SettledSupplementSegmentError::PayloadIntegrity(format!("truncated {label} in section"))
    })?;
    *cursor = &cursor[len..];
    Ok(slice)
}

fn reject_trailing_bytes(cursor: &[u8], label: &str) -> Result<(), SettledSupplementSegmentError> {
    if !cursor.is_empty() {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "{label} section contains {} trailing bytes",
            cursor.len()
        )));
    }
    Ok(())
}
