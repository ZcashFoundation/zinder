use std::{
    error::Error,
    fs::{self, OpenOptions},
    io::{Read as _, Seek as _, SeekFrom, Write as _},
    path::Path,
    sync::atomic::Ordering,
};

use sha2::{Digest as _, Sha256};
use tempfile::{NamedTempFile, TempDir};
use zinder_core::{
    AuthDigest, BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId,
    CanonicalBlockFacts, CanonicalTransactionFacts, CompactBlockArtifact, CompactChainMetadata,
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransaction,
    CompactTransactionData, CompactTransparentInput, CompactTransparentOutput, ConsensusBranchId,
    LockTime, Network, PrivacyShape, SerializedBytesDigest, TransactionComponentCounts,
    TransactionId, TransactionIntrinsicValueBalances, TransactionPublicFacts, TransactionVersion,
    UnsupportedSection, Wtxid,
    wire::{encode_internal_block_hash, encode_internal_transaction_id},
};
use zinder_proto::compat::lightwalletd::{self, EncodedBlock};

use super::{
    SETTLED_SUPPLEMENT_SEGMENT_MAGIC, SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES,
    SettledSupplementSegmentError, SettledSupplementSegmentOpenOptions,
    SettledSupplementSegmentReader, SettledSupplementSegmentWriteOptions,
    SettledSupplementSegmentWriter, SupplementCompressionCodec, SupplementSegmentIdentity,
    format::{
        CHUNK_INDEX_ENTRY_LEN, DecodedChunkIndexEntry, DecodedRowIndexEntry, HEADER_LEN,
        IndexedChunk, IndexedSupplementBlock, ROW_INDEX_ENTRY_LEN, SegmentHeader, TRAILER_LEN,
        VALIDATION_INVALID, max_chunk_bytes,
    },
    reader::{PortablePin, SegmentTables, admit_protected, validate_indices},
    section::{
        BlockEnvelopeIdentity, BlockSupplementRecord, CanonicalBlockSupplement,
        CompactTransparentSupplementTx, SupplementShieldedAnchors, SupplementTxFeeAndBurn,
        encode_block_record, transparent_supplement_from_compact,
    },
    wire_splice::{CompactBlockSpliceSelection, splice_compact_block_wire},
};
use crate::{
    HeightOrder, SettledShieldedSegmentOpenOptions, SettledShieldedSegmentReader,
    SettledShieldedSegmentWriter, ShieldedProjectionSelection, ShieldedSegmentIdentity,
};

type TestOutcome<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const fn det_block_hash(seed: u32) -> BlockHash {
    let mut bytes = [0_u8; 32];
    let be = seed.to_be_bytes();
    let le = seed.to_le_bytes();
    bytes[0] = be[0];
    bytes[1] = be[1];
    bytes[2] = be[2];
    bytes[3] = be[3];
    bytes[15] = 0x5A;
    bytes[28] = le[0];
    bytes[29] = le[1];
    bytes[30] = le[2];
    bytes[31] = le[3];
    BlockHash::from_bytes(bytes)
}

const fn det_txid(seed: u8) -> TransactionId {
    let mut bytes = [seed; 32];
    bytes[0] = seed.wrapping_add(1);
    bytes[31] = seed.wrapping_mul(3).wrapping_add(7);
    TransactionId::from_bytes(bytes)
}

const fn sapling_spend(seed: u8) -> CompactSaplingSpend {
    CompactSaplingSpend {
        nullifier: [seed; 32],
    }
}

const fn sapling_output(seed: u8) -> CompactSaplingOutput {
    CompactSaplingOutput {
        commitment: [seed; 32],
        ephemeral_key: [seed.wrapping_add(1); 32],
        ciphertext: [seed.wrapping_add(2); 52],
    }
}

const fn shielded_action(seed: u8) -> CompactShieldedAction {
    CompactShieldedAction {
        nullifier: [seed; 32],
        commitment: [seed.wrapping_add(1); 32],
        ephemeral_key: [seed.wrapping_add(2); 32],
        ciphertext: [seed.wrapping_add(3); 52],
    }
}

const fn t_in(seed: u8, vout_index: u32) -> CompactTransparentInput {
    CompactTransparentInput {
        previous_transaction_id: det_txid(seed),
        previous_output_index: vout_index,
    }
}

fn t_out(zats: u64, script_byte: u8) -> CompactTransparentOutput {
    CompactTransparentOutput {
        value_zat: zats,
        script_pub_key: vec![0x76, 0xa9, 0x14, script_byte, 0x88, 0xac],
    }
}

const fn make_meta(height: u32) -> CompactChainMetadata {
    CompactChainMetadata {
        sapling_commitment_tree_size: 100 + height,
        orchard_commitment_tree_size: 200 + height,
        ironwood_commitment_tree_size: 300 + height,
    }
}

fn empty_block(height: u32) -> CompactBlockArtifact {
    CompactBlockArtifact::empty(
        BlockId::new(BlockHeight::new(height), det_block_hash(height)),
        det_block_hash(height.saturating_sub(1)),
        1_735_000_000 + height,
        make_meta(height),
    )
}

fn transparent_only_block(height: u32) -> TestOutcome<CompactBlockArtifact> {
    let coinbase = CompactTransaction {
        index: 0,
        transaction_id: det_txid(1),
        data: CompactTransactionData {
            fee_zat: Some(0),
            transparent_outputs: vec![t_out(312_500_000, 0x10)],
            ..CompactTransactionData::default()
        },
    };
    let spend = CompactTransaction {
        index: 1,
        transaction_id: det_txid(2),
        data: CompactTransactionData {
            fee_zat: Some(10_000),
            transparent_inputs: vec![t_in(9, 0), t_in(10, 2)],
            transparent_outputs: vec![t_out(50_000, 0x20), t_out(0, 0x21)],
            ..CompactTransactionData::default()
        },
    };
    Ok(CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(height), det_block_hash(height)),
        det_block_hash(height - 1),
        1_735_000_000 + height,
        vec![coinbase, spend],
        make_meta(height),
    )?)
}

fn shielded_only_block(height: u32) -> TestOutcome<CompactBlockArtifact> {
    let tx = CompactTransaction {
        index: 1,
        transaction_id: det_txid(3),
        data: CompactTransactionData {
            fee_zat: Some(15_000),
            sapling_spends: vec![sapling_spend(0x11)],
            sapling_outputs: vec![sapling_output(0x12)],
            orchard_actions: vec![shielded_action(0x13)],
            ironwood_actions: vec![shielded_action(0x14)],
            ..CompactTransactionData::default()
        },
    };
    Ok(CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(height), det_block_hash(height)),
        det_block_hash(height - 1),
        1_735_000_000 + height,
        vec![tx],
        make_meta(height),
    )?)
}

fn sapling_deposit_block(height: u32) -> TestOutcome<CompactBlockArtifact> {
    let tx = CompactTransaction {
        index: 0,
        transaction_id: det_txid(4),
        data: CompactTransactionData {
            fee_zat: Some(0),
            sapling_outputs: vec![sapling_output(0x25)],
            ..CompactTransactionData::default()
        },
    };
    Ok(CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(height), det_block_hash(height)),
        det_block_hash(height - 1),
        1_735_000_000 + height,
        vec![tx],
        make_meta(height),
    )?)
}

fn orchard_and_ironwood_block(height: u32) -> TestOutcome<CompactBlockArtifact> {
    let orchard_tx = CompactTransaction {
        index: 1,
        transaction_id: det_txid(5),
        data: CompactTransactionData {
            fee_zat: Some(5_000),
            orchard_actions: vec![shielded_action(0x31), shielded_action(0x32)],
            ..CompactTransactionData::default()
        },
    };
    let ironwood_tx = CompactTransaction {
        index: 2,
        transaction_id: det_txid(6),
        data: CompactTransactionData {
            fee_zat: Some(10_000),
            ironwood_actions: vec![shielded_action(0x41)],
            ..CompactTransactionData::default()
        },
    };
    Ok(CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(height), det_block_hash(height)),
        det_block_hash(height - 1),
        1_735_000_000 + height,
        vec![orchard_tx, ironwood_tx],
        make_meta(height),
    )?)
}

fn mixed_interleaved_block(height: u32) -> TestOutcome<CompactBlockArtifact> {
    let coinbase = CompactTransaction {
        index: 0,
        transaction_id: det_txid(10),
        data: CompactTransactionData {
            fee_zat: None,
            transparent_outputs: vec![t_out(100_000, 0x51)],
            ..CompactTransactionData::default()
        },
    };
    let shielded = CompactTransaction {
        index: 1,
        transaction_id: det_txid(11),
        data: CompactTransactionData {
            fee_zat: Some(0),
            orchard_actions: vec![shielded_action(0x52)],
            ..CompactTransactionData::default()
        },
    };
    let mixed_one = CompactTransaction {
        index: 2,
        transaction_id: det_txid(12),
        data: CompactTransactionData {
            fee_zat: Some(20_000),
            sapling_spends: vec![sapling_spend(0x53)],
            orchard_actions: vec![shielded_action(0x54)],
            ironwood_actions: vec![shielded_action(0x55)],
            transparent_inputs: vec![t_in(22, 1)],
            transparent_outputs: vec![t_out(75_000, 0x56)],
            ..CompactTransactionData::default()
        },
    };
    let transparent_mid = CompactTransaction {
        index: 4,
        transaction_id: det_txid(13),
        data: CompactTransactionData {
            fee_zat: Some(5_000),
            transparent_inputs: vec![t_in(23, 0)],
            transparent_outputs: vec![t_out(40_000, 0x57)],
            ..CompactTransactionData::default()
        },
    };
    let deposit_tail = CompactTransaction {
        index: 6,
        transaction_id: det_txid(14),
        data: CompactTransactionData {
            fee_zat: Some(5_000),
            sapling_outputs: vec![sapling_output(0x58)],
            ..CompactTransactionData::default()
        },
    };
    let transparent_tail = CompactTransaction {
        index: 9,
        transaction_id: det_txid(15),
        data: CompactTransactionData {
            fee_zat: None,
            transparent_outputs: vec![t_out(1_000, 0x59)],
            ..CompactTransactionData::default()
        },
    };
    Ok(CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(height), det_block_hash(height)),
        det_block_hash(height - 1),
        1_735_000_000 + height,
        vec![
            coinbase,
            shielded,
            mixed_one,
            transparent_mid,
            deposit_tail,
            transparent_tail,
        ],
        make_meta(height),
    )?)
}

fn build_rich_corpus(start: u32) -> TestOutcome<Vec<CompactBlockArtifact>> {
    Ok(vec![
        empty_block(start),
        transparent_only_block(start + 1)?,
        shielded_only_block(start + 2)?,
        sapling_deposit_block(start + 3)?,
        orchard_and_ironwood_block(start + 4)?,
        mixed_interleaved_block(start + 5)?,
        mixed_interleaved_block(start + 6)?,
    ])
}

fn make_canonical_facts(block: &CompactBlockArtifact) -> CanonicalBlockFacts {
    let header = BlockHeaderArtifact::new(
        block.height(),
        block.block_hash(),
        block.previous_block_hash(),
        [0xAA; 32],
        [0xBB; 32],
        i64::from(block.time()),
        0x1d00_ffff,
        [0xCC; 32],
        5,
        1_024,
    );
    let compact_txs = block.transactions();
    let dense_len = compact_txs
        .last()
        .and_then(|tx| usize::try_from(tx.index).ok())
        .map_or(0, |last_idx| last_idx + 1);
    let mut transactions = Vec::with_capacity(dense_len);
    let mut compact_cursor = 0_usize;
    for pos in 0..dense_len {
        let pos_u64 = u64::try_from(pos).unwrap_or(u64::MAX);
        let candidate = compact_txs.get(compact_cursor);
        let matched = candidate.filter(|tx| tx.index == pos_u64);
        if matched.is_some() {
            compact_cursor += 1;
        }
        transactions.push(make_tx_facts(block.height(), pos, matched));
    }
    CanonicalBlockFacts {
        block_header: header,
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[0x99; 8]),
        transactions,
    }
}

type TxFactsSummary = (
    TransactionId,
    TransactionComponentCounts,
    Option<i64>,
    Option<[u8; 32]>,
    Option<i64>,
    PrivacyShape,
);

fn count_entries<T>(entries: &[T]) -> u32 {
    u32::try_from(entries.len()).unwrap_or(0)
}

fn present_tx_tuple(tx: &CompactTransaction) -> TxFactsSummary {
    (
        tx.transaction_id,
        TransactionComponentCounts {
            transparent_input_count: count_entries(&tx.data.transparent_inputs),
            transparent_output_count: count_entries(&tx.data.transparent_outputs),
            sapling_spend_count: count_entries(&tx.data.sapling_spends),
            sapling_output_count: count_entries(&tx.data.sapling_outputs),
            orchard_action_count: count_entries(&tx.data.orchard_actions),
            ironwood_action_count: count_entries(&tx.data.ironwood_actions),
            sprout_joinsplit_count: 0,
        },
        (!tx.data.orchard_actions.is_empty()).then_some(-50_000),
        (!tx.data.orchard_actions.is_empty()).then_some([0_u8; 32]),
        (!tx.data.ironwood_actions.is_empty()).then_some(0),
        PrivacyShape::Mixed,
    )
}

fn placeholder_tx_tuple(pos: usize) -> TxFactsSummary {
    (
        det_txid(200_u8.wrapping_add(u8::try_from(pos).unwrap_or(0))),
        TransactionComponentCounts {
            transparent_input_count: 0,
            transparent_output_count: 0,
            sapling_spend_count: 0,
            sapling_output_count: 0,
            orchard_action_count: 0,
            ironwood_action_count: 0,
            sprout_joinsplit_count: 0,
        },
        None,
        None,
        None,
        PrivacyShape::ShieldedOnly,
    )
}

fn make_tx_facts(
    block_height: BlockHeight,
    pos: usize,
    maybe_tx: Option<&CompactTransaction>,
) -> CanonicalTransactionFacts {
    let (transaction_id, counts, orchard_bal, orchard_anchor, ironwood_bal, privacy_shape) =
        maybe_tx.map_or_else(|| placeholder_tx_tuple(pos), present_tx_tuple);
    CanonicalTransactionFacts {
        public_facts: TransactionPublicFacts {
            transaction_id,
            auth_digest: Some(AuthDigest::from_bytes([0x11; 32])),
            wtxid: Some(Wtxid::from_bytes([0x22; 64])),
            version: TransactionVersion::V5,
            consensus_branch_id: Some(ConsensusBranchId::new(0xC2D6_D0B4)),
            lock_time: LockTime::Height(block_height),
            expiry_height: Some(BlockHeight::new(block_height.value() + 40)),
            size_bytes: 420 + u32::try_from(pos).unwrap_or(0),
            counts,
            orchard_value_balance_zat: orchard_bal,
            orchard_anchor,
            ironwood_value_balance_zat: ironwood_bal,
            privacy_shape,
            is_coinbase: pos == 0,
            unsupported_sections: vec![UnsupportedSection::FutureShieldedProtocol],
        },
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[0x33; 16]),
        intrinsic_value_balances: TransactionIntrinsicValueBalances::new(0, -10, 50_000, 0),
        transparent_inputs: Vec::new(),
        transparent_outputs: Vec::new(),
    }
}

fn prost_compact_block(
    block: &CompactBlockArtifact,
    selection: CompactBlockSpliceSelection,
) -> TestOutcome<EncodedBlock> {
    let mut vtx = Vec::new();
    for tx in block.transactions() {
        if let Some(proto_tx) = prost_compact_tx(tx, selection)? {
            vtx.push(proto_tx);
        }
    }
    let meta = block.chain_metadata();
    let chain_metadata = if selection.nullifiers_only {
        Some(lightwalletd::ChainMetadata::default())
    } else {
        Some(lightwalletd::ChainMetadata {
            sapling_commitment_tree_size: meta.sapling_commitment_tree_size,
            orchard_commitment_tree_size: meta.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: meta.ironwood_commitment_tree_size,
        })
    };
    let proto_block = lightwalletd::CompactBlock {
        height: u64::from(block.height().value()),
        hash: encode_internal_block_hash(block.block_hash()).to_vec(),
        prev_hash: encode_internal_block_hash(block.previous_block_hash()).to_vec(),
        time: block.time(),
        header: Vec::new(),
        vtx,
        chain_metadata,
    };
    Ok(EncodedBlock::encode(&proto_block)?)
}

fn prost_compact_tx(
    tx: &CompactTransaction,
    selection: CompactBlockSpliceSelection,
) -> TestOutcome<Option<lightwalletd::CompactTx>> {
    let has_shielded = !tx.data.sapling_spends.is_empty()
        || !tx.data.sapling_outputs.is_empty()
        || !tx.data.orchard_actions.is_empty()
        || !tx.data.ironwood_actions.is_empty();
    let fee = tx.data.fee_zat.map(u32::try_from).transpose()?.unwrap_or(0);
    let txid = encode_internal_transaction_id(tx.transaction_id).to_vec();
    if !has_shielded {
        return Ok(prost_transparent_only_tx(tx, fee, txid, selection));
    }
    let spends = tx
        .data
        .sapling_spends
        .iter()
        .map(|spend| lightwalletd::CompactSaplingSpend {
            nf: spend.nullifier.to_vec(),
        })
        .collect();
    let outputs = if selection.nullifiers_only {
        Vec::new()
    } else {
        tx.data
            .sapling_outputs
            .iter()
            .map(|output| lightwalletd::CompactSaplingOutput {
                cmu: output.commitment.to_vec(),
                ephemeral_key: output.ephemeral_key.to_vec(),
                ciphertext: output.ciphertext.to_vec(),
            })
            .collect()
    };
    let actions = tx
        .data
        .orchard_actions
        .iter()
        .map(|act| prost_action(act, selection.nullifiers_only))
        .collect();
    let ironwood_actions = tx
        .data
        .ironwood_actions
        .iter()
        .map(|act| prost_action(act, selection.nullifiers_only))
        .collect();
    let (vin, vout) = if !selection.nullifiers_only && selection.include_transparent {
        prost_transparent_parts(tx)
    } else {
        (Vec::new(), Vec::new())
    };
    Ok(Some(lightwalletd::CompactTx {
        index: tx.index,
        txid,
        fee,
        spends,
        outputs,
        actions,
        ironwood_actions,
        vin,
        vout,
    }))
}

fn prost_transparent_only_tx(
    tx: &CompactTransaction,
    fee: u32,
    txid: Vec<u8>,
    selection: CompactBlockSpliceSelection,
) -> Option<lightwalletd::CompactTx> {
    if selection.nullifiers_only {
        return selection
            .include_nullifier_tx_shells
            .then_some(lightwalletd::CompactTx {
                index: tx.index,
                txid,
                fee,
                spends: Vec::new(),
                outputs: Vec::new(),
                actions: Vec::new(),
                ironwood_actions: Vec::new(),
                vin: Vec::new(),
                vout: Vec::new(),
            });
    }
    if !selection.include_transparent {
        return None;
    }
    let (vin, vout) = prost_transparent_parts(tx);
    Some(lightwalletd::CompactTx {
        index: tx.index,
        txid,
        fee,
        spends: Vec::new(),
        outputs: Vec::new(),
        actions: Vec::new(),
        ironwood_actions: Vec::new(),
        vin,
        vout,
    })
}

fn prost_action(
    act: &CompactShieldedAction,
    nullifiers_only: bool,
) -> lightwalletd::CompactOrchardAction {
    if nullifiers_only {
        lightwalletd::CompactOrchardAction {
            nullifier: act.nullifier.to_vec(),
            cmx: Vec::new(),
            ephemeral_key: Vec::new(),
            ciphertext: Vec::new(),
        }
    } else {
        lightwalletd::CompactOrchardAction {
            nullifier: act.nullifier.to_vec(),
            cmx: act.commitment.to_vec(),
            ephemeral_key: act.ephemeral_key.to_vec(),
            ciphertext: act.ciphertext.to_vec(),
        }
    }
}

fn prost_transparent_parts(
    tx: &CompactTransaction,
) -> (Vec<lightwalletd::CompactTxIn>, Vec<lightwalletd::TxOut>) {
    let vin = tx
        .data
        .transparent_inputs
        .iter()
        .map(|tx_in| lightwalletd::CompactTxIn {
            prevout_txid: encode_internal_transaction_id(tx_in.previous_transaction_id).to_vec(),
            prevout_index: tx_in.previous_output_index,
        })
        .collect();
    let vout = tx
        .data
        .transparent_outputs
        .iter()
        .map(|tx_out| lightwalletd::TxOut {
            value: tx_out.value_zat,
            script_pub_key: tx_out.script_pub_key.clone(),
        })
        .collect();
    (vin, vout)
}

fn write_paired_segments(
    dir: &Path,
    blocks: &[CompactBlockArtifact],
    with_canonical: bool,
    options: SettledSupplementSegmentWriteOptions,
) -> TestOutcome<(
    std::path::PathBuf,
    SettledShieldedSegmentReader,
    SupplementSegmentIdentity,
    SettledSupplementSegmentReader,
)> {
    let first = &blocks[0];
    let last = &blocks[blocks.len() - 1];
    let end_id = BlockId::new(last.height(), last.block_hash());
    let shielded_path = dir.join("seg.zshrng02");
    let sup_path = dir.join("seg.zstsup01");

    let mut shielded_writer = SettledShieldedSegmentWriter::create(
        &shielded_path,
        NamedTempFile::new_in(dir)?,
        ShieldedSegmentIdentity::span(Network::ZcashMainnet, 1, first.height(), end_id),
    )?;
    let mut sup_writer = SettledSupplementSegmentWriter::create(
        &sup_path,
        SupplementSegmentIdentity::span(Network::ZcashMainnet, 1, first.height(), end_id),
        options,
    )?;
    for block in blocks {
        shielded_writer.push(block, 512 + block.transactions().len() * 256)?;
        let facts = with_canonical.then(|| make_canonical_facts(block));
        sup_writer.push(block, facts.as_ref())?;
    }
    let shielded_id = shielded_writer.finish()?;
    let sup_id = sup_writer.finish()?;

    let shielded_reader = SettledShieldedSegmentReader::open(
        &shielded_path,
        &shielded_id,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let sup_reader = SettledSupplementSegmentReader::open(
        &sup_path,
        &sup_id,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    Ok((sup_path, shielded_reader, sup_id, sup_reader))
}

fn build_synthetic_segment_bytes(
    start_height: u32,
    records: &[(CompactBlockArtifact, BlockSupplementRecord)],
    codec: SupplementCompressionCodec,
    chunk_blocks: u16,
) -> TestOutcome<(Vec<u8>, SupplementSegmentIdentity)> {
    let mut chunks = Vec::new();
    let mut rows = Vec::new();
    let mut body_bytes = Vec::new();
    for group in records.chunks(usize::from(chunk_blocks)) {
        let mut raw_chunk = Vec::new();
        for (block, record) in group {
            let env = BlockEnvelopeIdentity {
                height: block.height(),
                block_hash_wire: encode_internal_block_hash(block.block_hash()),
                prev_hash_wire: encode_internal_block_hash(block.previous_block_hash()),
                block_time: block.time(),
            };
            let (encoded, row_flags) = encode_block_record(env, record)?;
            let chunk_rel_offset = u32::try_from(raw_chunk.len())?;
            let uncompressed_len = u32::try_from(encoded.len())?;
            let payload_sha256: [u8; 32] = Sha256::digest(&encoded).into();
            raw_chunk.extend_from_slice(&encoded);
            rows.push(DecodedRowIndexEntry {
                block_hash_wire: encode_internal_block_hash(block.block_hash()),
                payload_sha256,
                chunk_rel_offset,
                uncompressed_len,
                block_time: block.time(),
                row_flags,
            });
        }
        let chunk_uncompressed_len = u32::try_from(raw_chunk.len())?;
        let frame = match codec {
            SupplementCompressionCodec::None => raw_chunk,
            SupplementCompressionCodec::Zstd => zstd::bulk::compress(&raw_chunk, 3)?,
        };
        let chunk_sha256: [u8; 32] = Sha256::digest(&frame).into();
        let data_rel_offset = u64::try_from(body_bytes.len())?;
        body_bytes.extend_from_slice(&frame);
        chunks.push(DecodedChunkIndexEntry {
            data_rel_offset,
            compressed_len: u32::try_from(frame.len())?,
            uncompressed_len: chunk_uncompressed_len,
            chunk_sha256,
        });
    }
    assemble_segment_file(
        start_height,
        records,
        (codec, chunk_blocks),
        (&body_bytes, &chunks, &rows),
    )
}

fn assemble_segment_file(
    start_height: u32,
    records: &[(CompactBlockArtifact, BlockSupplementRecord)],
    encoding: (SupplementCompressionCodec, u16),
    parts: (&[u8], &[DecodedChunkIndexEntry], &[DecodedRowIndexEntry]),
) -> TestOutcome<(Vec<u8>, SupplementSegmentIdentity)> {
    let first = &records[0].0;
    let last = &records[records.len() - 1].0;
    let (body_bytes, chunks, rows) = parts;
    let data_offset = HEADER_LEN as u64;
    let chunk_index_offset = data_offset + u64::try_from(body_bytes.len())?;
    let chunk_bytes = u64::try_from(chunks.len() * CHUNK_INDEX_ENTRY_LEN)?;
    let row_index_offset = chunk_index_offset + chunk_bytes;
    let header = SegmentHeader {
        network_tag: Network::ZcashMainnet.id(),
        artifact_schema_version: 1,
        segment_flags: 0,
        codec_id: encoding.0.id(),
        chunk_blocks: encoding.1,
        start_height,
        block_count: u32::try_from(rows.len())?,
        first_prev_hash_wire: encode_internal_block_hash(first.previous_block_hash()),
        last_block_hash_wire: encode_internal_block_hash(last.block_hash()),
        data_offset,
        chunk_index_offset,
        row_index_offset,
    };
    let mut file_bytes = Vec::new();
    file_bytes.extend_from_slice(&header.encode());
    file_bytes.extend_from_slice(body_bytes);
    for entry in chunks {
        file_bytes.extend_from_slice(&entry.encode());
    }
    for entry in rows {
        file_bytes.extend_from_slice(&entry.encode());
    }
    let sha256: [u8; 32] = Sha256::digest(&file_bytes).into();
    file_bytes.extend_from_slice(&sha256);
    let mut identity = SupplementSegmentIdentity::span(
        Network::ZcashMainnet,
        1,
        BlockHeight::new(start_height),
        BlockId::new(last.height(), last.block_hash()),
    );
    identity.sha256 = sha256;
    Ok((file_bytes, identity))
}

#[test]
fn splice_wire_matches_prost_across_all_block_shapes_and_modes() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = build_rich_corpus(500)?;
    let (_path, shielded_reader, _sup_id, sup_reader) = write_paired_segments(
        dir.path(),
        &blocks,
        true,
        SettledSupplementSegmentWriteOptions {
            codec: SupplementCompressionCodec::Zstd,
            chunk_blocks: 3,
        },
    )?;

    let range = BlockHeightRange::inclusive(BlockHeight::new(500), BlockHeight::new(506));
    let shielded_frames = shielded_reader.read_projected_range(
        range,
        HeightOrder::Ascending,
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
    )?;

    let selections = [
        CompactBlockSpliceSelection::FULL_BLOCK,
        CompactBlockSpliceSelection::DEFAULT_SHIELDED,
        CompactBlockSpliceSelection::DEFAULT_NULLIFIERS,
        CompactBlockSpliceSelection::BLOCK_NULLIFIERS,
    ];
    for selection in selections {
        let spliced = sup_reader.splice_encoded_range_wire(
            BlockHeight::new(500),
            &shielded_frames,
            selection,
        )?;
        assert_eq!(spliced.len(), blocks.len());
        for (block, (actual, shielded)) in blocks
            .iter()
            .zip(spliced.iter().zip(shielded_frames.iter()))
        {
            let oracle = prost_compact_block(block, selection)?;
            assert_eq!(actual.as_length_prefixed(), oracle.as_length_prefixed());
            let unary =
                sup_reader.splice_block_wire(block.height(), &shielded.payload(), selection)?;
            assert_eq!(unary.as_length_prefixed(), oracle.as_length_prefixed());
        }
    }
    Ok(())
}

#[test]
fn reconstruct_roundtrip_recovers_artifacts_and_preserves_zero_fee_vs_none() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = build_rich_corpus(700)?;
    let (_path, shielded_reader, _sup_id, sup_reader) = write_paired_segments(
        dir.path(),
        &blocks,
        true,
        SettledSupplementSegmentWriteOptions::default(),
    )?;
    let range = BlockHeightRange::inclusive(BlockHeight::new(700), BlockHeight::new(706));
    let shielded_frames = shielded_reader.read_projected_range(
        range,
        HeightOrder::Ascending,
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
    )?;
    let payloads: Vec<bytes::Bytes> = shielded_frames.iter().map(EncodedBlock::payload).collect();
    let reconstructed = sup_reader.reconstruct_range(BlockHeight::new(700), &payloads)?;
    assert_eq!(reconstructed, blocks);

    for (block, payload) in blocks.iter().zip(payloads.iter()) {
        let single = sup_reader.reconstruct_block(block.height(), payload)?;
        assert_eq!(&single, block);
    }
    Ok(())
}

#[test]
fn chunked_zstd_and_none_roundtrip_across_chunk_boundaries() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = build_rich_corpus(1_000)?;
    for (label, codec, chunk_blocks) in [
        ("zstd-1", SupplementCompressionCodec::Zstd, 1),
        ("zstd-3", SupplementCompressionCodec::Zstd, 3),
        ("none-3", SupplementCompressionCodec::None, 3),
        ("zstd-16", SupplementCompressionCodec::Zstd, 16),
    ] {
        let sub = dir.path().join(label);
        fs::create_dir(&sub)?;
        let (_path, _shielded, _id, reader) = write_paired_segments(
            &sub,
            &blocks,
            true,
            SettledSupplementSegmentWriteOptions {
                codec,
                chunk_blocks,
            },
        )?;
        assert_eq!(reader.codec(), codec);
        assert_eq!(reader.chunk_blocks(), chunk_blocks);
        assert_eq!(reader.block_count(), blocks.len());
        let range = reader.read_range(BlockHeight::new(1_000), BlockHeight::new(1_006))?;
        assert_eq!(range.len(), blocks.len());
        assert!(!reader.has_transparent_at(BlockHeight::new(1_000))?);
        assert!(reader.has_transparent_at(BlockHeight::new(1_001))?);
        assert!(!reader.has_transparent_at(BlockHeight::new(1_002))?);
        assert!(reader.has_transparent_at(BlockHeight::new(1_005))?);
        assert_eq!(
            reader.read_transparent_block(BlockHeight::new(1_001))?,
            range[1].transparent_transactions
        );
        let meta = reader.read_canonical_metadata(BlockHeight::new(1_005))?;
        assert!(meta.is_some());
        assert!(matches!(
            reader.read_block(BlockHeight::new(999)),
            Err(SettledSupplementSegmentError::HeightOutOfRange { .. })
        ));
        assert!(matches!(
            reader.read_range(BlockHeight::new(1_004), BlockHeight::new(1_002)),
            Err(SettledSupplementSegmentError::HeightOutOfRange { .. })
        ));
    }
    Ok(())
}

#[test]
fn tag2_canonical_metadata_and_explicit_zero_anchors_roundtrip() -> TestOutcome {
    let dir = TempDir::new()?;
    let block = mixed_interleaved_block(200)?;
    let facts = make_canonical_facts(&block);
    let mut canonical = CanonicalBlockSupplement::from_canonical_facts(&facts, Some(&block))?;
    canonical.transactions[0].shielded_anchors = SupplementShieldedAnchors {
        orchard: Some([0_u8; 32]),
        ironwood: Some([0_u8; 32]),
        sapling: Some([0x77; 32]),
    };
    canonical.transactions[0].public_facts.orchard_anchor = Some([0_u8; 32]);
    canonical.transactions[0].public_facts.version = TransactionVersion::Unsupported {
        effective_version: 7,
        version_group_id: Some(0xABCD_1234),
    };
    canonical.transactions[0].fee_and_burn = SupplementTxFeeAndBurn {
        fee_zat: Some(0),
        burn_zat: Some(12_345),
    };

    let path = dir.path().join("anchors.zstsup01");
    let end_id = BlockId::new(block.height(), block.block_hash());
    let mut writer = SettledSupplementSegmentWriter::create(
        &path,
        SupplementSegmentIdentity::span(Network::ZcashMainnet, 1, block.height(), end_id),
        SettledSupplementSegmentWriteOptions::default(),
    )?;
    let record = BlockSupplementRecord {
        transparent_transactions: transparent_supplement_from_compact(&block),
        canonical_metadata: Some(canonical.clone()),
    };
    writer.push_record(end_id, block.previous_block_hash(), block.time(), &record)?;
    let id = writer.finish()?;
    let reader = SettledSupplementSegmentReader::open(
        &path,
        &id,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    let read_back = reader.read_canonical_metadata(block.height())?;
    assert_eq!(read_back.as_ref(), Some(&canonical));
    Ok(())
}

fn single_large_script_block(height: u32, script_len: usize) -> TestOutcome<CompactBlockArtifact> {
    let tx = CompactTransaction {
        index: 0,
        transaction_id: det_txid(90),
        data: CompactTransactionData {
            fee_zat: Some(1_000),
            transparent_outputs: vec![CompactTransparentOutput {
                value_zat: 50_000,
                script_pub_key: vec![0xAA; script_len],
            }],
            ..CompactTransactionData::default()
        },
    };
    Ok(CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(height), det_block_hash(height)),
        det_block_hash(height - 1),
        1_735_000_000 + height,
        vec![tx],
        make_meta(height),
    )?)
}

#[test]
fn supports_over_2mib_block_and_enforces_16mib_cap() -> TestOutcome {
    let dir = TempDir::new()?;
    let big_block = single_large_script_block(42, 3 * 1024 * 1024)?;
    let ok_path = dir.path().join("over_2mib.zstsup01");
    let end_id = BlockId::new(big_block.height(), big_block.block_hash());
    let mut writer = SettledSupplementSegmentWriter::create(
        &ok_path,
        SupplementSegmentIdentity::span(Network::ZcashMainnet, 1, big_block.height(), end_id),
        SettledSupplementSegmentWriteOptions::default(),
    )?;
    writer.push(&big_block, None)?;
    let id = writer.finish()?;
    let reader = SettledSupplementSegmentReader::open(
        &ok_path,
        &id,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    assert_eq!(
        reader.read_transparent_block(BlockHeight::new(42))?.len(),
        1
    );

    let oversize_block = single_large_script_block(43, SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES)?;
    let mut fail_writer = SettledSupplementSegmentWriter::create(
        dir.path().join("too_big.zstsup01"),
        SupplementSegmentIdentity::span(
            Network::ZcashMainnet,
            1,
            oversize_block.height(),
            BlockId::new(oversize_block.height(), oversize_block.block_hash()),
        ),
        SettledSupplementSegmentWriteOptions::default(),
    )?;
    let outcome = fail_writer.push(&oversize_block, None);
    assert!(matches!(
        outcome,
        Err(SettledSupplementSegmentError::PayloadLimitExceeded { .. })
    ));
    Ok(())
}

const fn synthetic_header(chunk_blocks: u16, data_len: u64) -> SegmentHeader {
    SegmentHeader {
        network_tag: Network::ZcashMainnet.id(),
        artifact_schema_version: 1,
        segment_flags: 0,
        codec_id: 1,
        chunk_blocks,
        start_height: 10,
        block_count: 1,
        first_prev_hash_wire: [0x11; 32],
        last_block_hash_wire: [0x22; 32],
        data_offset: HEADER_LEN as u64,
        chunk_index_offset: HEADER_LEN as u64 + data_len,
        row_index_offset: HEADER_LEN as u64 + data_len + CHUNK_INDEX_ENTRY_LEN as u64,
    }
}

fn synthetic_row(uncompressed_len: u32) -> IndexedSupplementBlock {
    IndexedSupplementBlock::from_entry(&DecodedRowIndexEntry {
        block_hash_wire: [0x22; 32],
        payload_sha256: [0x33; 32],
        chunk_rel_offset: 0,
        uncompressed_len,
        block_time: 100,
        row_flags: 0,
    })
}

fn synthetic_chunk(compressed_len: u32, uncompressed_len: u32) -> IndexedChunk {
    IndexedChunk::from_entry(&DecodedChunkIndexEntry {
        data_rel_offset: 0,
        compressed_len,
        uncompressed_len,
        chunk_sha256: [0x44; 32],
    })
}

#[test]
fn validate_indices_rejects_oversized_bounds_before_allocation_on_both_paths() -> TestOutcome {
    let chunk_limit = max_chunk_bytes(16)?;
    let max_block_u32 = u32::try_from(SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES)?;
    let ok_header = synthetic_header(16, 128);
    validate_indices(
        &ok_header,
        SupplementCompressionCodec::Zstd,
        &[synthetic_chunk(128, max_block_u32)],
        &[synthetic_row(max_block_u32)],
        None,
    )?;

    assert!(matches!(
        validate_indices(
            &synthetic_header(16, u64::from(u32::MAX)),
            SupplementCompressionCodec::Zstd,
            &[synthetic_chunk(u32::MAX, 1_024)],
            &[synthetic_row(1_024)],
            None,
        ),
        Err(SettledSupplementSegmentError::InvalidFormat(_))
    ));

    let digest = [0x44; 32];
    assert!(matches!(
        validate_indices(
            &synthetic_header(16, 128),
            SupplementCompressionCodec::Zstd,
            &[synthetic_chunk(128, chunk_limit + 1)],
            &[synthetic_row(128)],
            Some(&[digest]),
        ),
        Err(SettledSupplementSegmentError::InvalidFormat(_))
    ));

    assert!(matches!(
        validate_indices(
            &ok_header,
            SupplementCompressionCodec::Zstd,
            &[synthetic_chunk(128, max_block_u32 + 1)],
            &[synthetic_row(max_block_u32 + 1)],
            None,
        ),
        Err(SettledSupplementSegmentError::PayloadLimitExceeded { .. })
    ));

    assert!(matches!(
        validate_indices(
            &synthetic_header(16, 100),
            SupplementCompressionCodec::None,
            &[synthetic_chunk(100, 200)],
            &[synthetic_row(200)],
            None,
        ),
        Err(SettledSupplementSegmentError::InvalidFormat(_))
    ));
    Ok(())
}

#[test]
fn header_flags_codec_reserved_bytes_and_writer_linkage_fail_closed() -> TestOutcome {
    let base = synthetic_header(16, 64).encode();
    assert_eq!(&base[..8], SETTLED_SUPPLEMENT_SEGMENT_MAGIC);
    assert!(SegmentHeader::decode(&base).is_ok());

    let mut bad_flags = base;
    bad_flags[15] = 0x01;
    assert!(matches!(
        SegmentHeader::decode(&bad_flags),
        Err(SettledSupplementSegmentError::InvalidFormat(_))
    ));

    let mut bad_codec = base;
    bad_codec[17] = 9;
    assert!(matches!(
        SegmentHeader::decode(&bad_codec),
        Err(SettledSupplementSegmentError::InvalidCodecId(9))
    ));

    let mut non_zero_reserved = base;
    non_zero_reserved[127] = 1;
    assert!(matches!(
        SegmentHeader::decode(&non_zero_reserved),
        Err(SettledSupplementSegmentError::InvalidFormat(_))
    ));

    let dir = TempDir::new()?;
    let b0 = empty_block(10);
    let b1 = empty_block(11);
    let mut writer = SettledSupplementSegmentWriter::create(
        dir.path().join("link.zstsup01"),
        SupplementSegmentIdentity::span(
            Network::ZcashMainnet,
            1,
            b0.height(),
            BlockId::new(b1.height(), b1.block_hash()),
        ),
        SettledSupplementSegmentWriteOptions::default(),
    )?;
    writer.push(&b0, None)?;
    let wrong_prev = CompactBlockArtifact::empty(
        BlockId::new(b1.height(), b1.block_hash()),
        det_block_hash(999),
        123,
        make_meta(11),
    );
    assert!(matches!(
        writer.push(&wrong_prev, None),
        Err(SettledSupplementSegmentError::ChainLinkage(_))
    ));
    assert!(matches!(
        writer.finish(),
        Err(SettledSupplementSegmentError::ChainLinkage(_))
    ));
    Ok(())
}

#[test]
fn writer_rejects_sparse_or_mismatched_canonical_facts() -> TestOutcome {
    let dir = TempDir::new()?;
    let block = mixed_interleaved_block(450)?;
    let id = BlockId::new(block.height(), block.block_hash());
    let mut missing_facts = make_canonical_facts(&block);
    missing_facts.transactions.pop();
    let mut writer_missing = SettledSupplementSegmentWriter::create(
        dir.path().join("missing_fact.zstsup01"),
        SupplementSegmentIdentity::span(Network::ZcashMainnet, 1, block.height(), id),
        SettledSupplementSegmentWriteOptions::default(),
    )?;
    assert!(matches!(
        writer_missing.push(&block, Some(&missing_facts)),
        Err(SettledSupplementSegmentError::ChainLinkage(_))
    ));

    let mut wrong_txid = make_canonical_facts(&block);
    wrong_txid.transactions[4].public_facts.transaction_id = det_txid(254);
    let mut writer_mismatch = SettledSupplementSegmentWriter::create(
        dir.path().join("wrong_txid.zstsup01"),
        SupplementSegmentIdentity::span(Network::ZcashMainnet, 1, block.height(), id),
        SettledSupplementSegmentWriteOptions::default(),
    )?;
    assert!(matches!(
        writer_mismatch.push(&block, Some(&wrong_txid)),
        Err(SettledSupplementSegmentError::ChainLinkage(_))
    ));
    Ok(())
}

#[test]
fn envelope_and_section_tags_fail_closed_on_mandatory_and_skip_optional() -> TestOutcome {
    let dir = TempDir::new()?;
    let block = empty_block(80);
    let record = BlockSupplementRecord {
        transparent_transactions: Vec::new(),
        canonical_metadata: None,
    };
    let (mut bytes, _initial_id) = build_synthetic_segment_bytes(
        80,
        &[(block.clone(), record)],
        SupplementCompressionCodec::None,
        16,
    )?;

    let optional_payload = [0xAA, 0xBB];
    let header = SegmentHeader::decode(bytes[..HEADER_LEN].try_into()?)?;
    let base_row = &bytes[HEADER_LEN..usize::try_from(header.chunk_index_offset)?];

    let mut opt_row = base_row.to_vec();
    opt_row.extend_from_slice(&build_section_bytes(0x8003, 1, 0, &optional_payload));
    let rebuilt_ok = rebuild_single_row_segment(&block, &opt_row)?;
    let ok_path = dir.path().join("opt_section.zstsup01");
    fs::write(&ok_path, &rebuilt_ok.0)?;
    let reader = SettledSupplementSegmentReader::open(
        &ok_path,
        &rebuilt_ok.1,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    assert_eq!(
        reader.read_transparent_block(BlockHeight::new(80))?.len(),
        0
    );

    let mut bad_row = base_row.to_vec();
    bad_row.extend_from_slice(&build_section_bytes(0x0003, 1, 0, &optional_payload));
    let bad_seg = rebuild_single_row_segment(&block, &bad_row)?;
    let bad_path = dir.path().join("mand_section.zstsup01");
    fs::write(&bad_path, &bad_seg.0)?;
    let bad_reader = SettledSupplementSegmentReader::open(
        &bad_path,
        &bad_seg.1,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    assert!(matches!(
        bad_reader.read_block(BlockHeight::new(80)),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));

    bytes[HEADER_LEN] = 2;
    let bad_ver_row = &bytes[HEADER_LEN..usize::try_from(header.chunk_index_offset)?];
    let bad_ver = rebuild_single_row_segment(&block, bad_ver_row)?;
    let ver_path = dir.path().join("bad_env_ver.zstsup01");
    fs::write(&ver_path, &bad_ver.0)?;
    let ver_reader = SettledSupplementSegmentReader::open(
        &ver_path,
        &bad_ver.1,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    assert!(matches!(
        ver_reader.read_block(BlockHeight::new(80)),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn tag2_extension_tlvs_fail_closed_on_mandatory_and_skip_optional() -> TestOutcome {
    let dir = TempDir::new()?;
    let block = shielded_only_block(81)?;
    let facts = make_canonical_facts(&block);
    let record = BlockSupplementRecord {
        transparent_transactions: Vec::new(),
        canonical_metadata: Some(CanonicalBlockSupplement::from_canonical_facts(
            &facts,
            Some(&block),
        )?),
    };
    let (bytes, _id) = build_synthetic_segment_bytes(
        81,
        &[(block.clone(), record)],
        SupplementCompressionCodec::None,
        16,
    )?;
    let header = SegmentHeader::decode(bytes[..HEADER_LEN].try_into()?)?;
    let row_bytes = bytes[HEADER_LEN..usize::try_from(header.chunk_index_offset)?].to_vec();

    let opt_tlv = append_block_tlv(&row_bytes, 0x8001, &[0xDE, 0xAD])?;
    let opt_seg = rebuild_single_row_segment(&block, &opt_tlv)?;
    let opt_path = dir.path().join("tag2_opt_tlv.zstsup01");
    fs::write(&opt_path, &opt_seg.0)?;
    let opt_reader = SettledSupplementSegmentReader::open(
        &opt_path,
        &opt_seg.1,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    let opt_meta = opt_reader.read_canonical_metadata(BlockHeight::new(81))?;
    assert!(opt_meta.is_some());

    let mand_tlv = append_block_tlv(&row_bytes, 0x0001, &[0xDE, 0xAD])?;
    let mand_seg = rebuild_single_row_segment(&block, &mand_tlv)?;
    let mand_path = dir.path().join("tag2_mand_tlv.zstsup01");
    fs::write(&mand_path, &mand_seg.0)?;
    let mand_reader = SettledSupplementSegmentReader::open(
        &mand_path,
        &mand_seg.1,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    assert!(matches!(
        mand_reader.read_block(BlockHeight::new(81)),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

fn append_block_tlv(base_row: &[u8], ext_tag: u16, ext_payload: &[u8]) -> TestOutcome<Vec<u8>> {
    let env_hdr_len = 74;
    let tag1_len = u32::from_be_bytes(base_row[env_hdr_len + 5..env_hdr_len + 9].try_into()?);
    let tag2_hdr_off = env_hdr_len + 9 + usize::try_from(tag1_len)?;
    let tag2_len = u32::from_be_bytes(base_row[tag2_hdr_off + 5..tag2_hdr_off + 9].try_into()?);
    let mut out = base_row[..base_row.len() - 2].to_vec();
    out.extend_from_slice(&1_u16.to_be_bytes());
    out.extend_from_slice(&ext_tag.to_be_bytes());
    out.extend_from_slice(&u32::try_from(ext_payload.len())?.to_be_bytes());
    out.extend_from_slice(ext_payload);
    let new_tag2_len = u32::try_from(usize::try_from(tag2_len)? + 6 + ext_payload.len())?;
    out[tag2_hdr_off + 5..tag2_hdr_off + 9].copy_from_slice(&new_tag2_len.to_be_bytes());
    Ok(out)
}

fn build_section_bytes(tag: u16, version: u16, flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&tag.to_be_bytes());
    out.extend_from_slice(&version.to_be_bytes());
    out.push(flags);
    out.extend_from_slice(&u32::try_from(payload.len()).unwrap_or(0).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn rebuild_single_row_segment(
    block: &CompactBlockArtifact,
    row_bytes: &[u8],
) -> TestOutcome<(Vec<u8>, SupplementSegmentIdentity)> {
    let payload_sha256: [u8; 32] = Sha256::digest(row_bytes).into();
    let row_len = u32::try_from(row_bytes.len())?;
    let chunk = DecodedChunkIndexEntry {
        data_rel_offset: 0,
        compressed_len: row_len,
        uncompressed_len: row_len,
        chunk_sha256: payload_sha256,
    };
    let mut row = DecodedRowIndexEntry {
        block_hash_wire: encode_internal_block_hash(block.block_hash()),
        payload_sha256,
        chunk_rel_offset: 0,
        uncompressed_len: row_len,
        block_time: block.time(),
        row_flags: 0,
    };
    let tag1_count_off = 74 + 9;
    let tag1_count = u32::from_be_bytes(row_bytes[tag1_count_off..tag1_count_off + 4].try_into()?);
    let tag1_len = u32::from_be_bytes(row_bytes[74 + 5..74 + 9].try_into()?);
    let after_tag1 = 74 + 9 + usize::try_from(tag1_len)?;
    if tag1_count > 0 {
        row.row_flags |= 0x01;
    }
    if row_bytes.get(after_tag1..after_tag1 + 2) == Some(&2_u16.to_be_bytes()[..]) {
        row.row_flags |= 0x02;
    }
    assemble_segment_file(
        block.height().value(),
        &[(
            block.clone(),
            BlockSupplementRecord {
                transparent_transactions: Vec::new(),
                canonical_metadata: None,
            },
        )],
        (SupplementCompressionCodec::None, 16),
        (row_bytes, &[chunk], &[row]),
    )
}

#[test]
fn wire_splice_rejects_mismatched_txid_fee_unexpected_vin_or_wrong_hash() -> TestOutcome {
    let block = mixed_interleaved_block(300)?;
    let proto_mixed = prost_compact_block(&block, CompactBlockSpliceSelection::DEFAULT_SHIELDED)?;
    let sup = vec![CompactTransparentSupplementTx {
        index: 2,
        transaction_id: det_txid(99),
        fee_zat: Some(20_000),
        transparent_inputs: vec![t_in(22, 1)],
        transparent_outputs: vec![t_out(75_000, 0x56)],
    }];
    assert!(matches!(
        splice_compact_block_wire(
            &proto_mixed.payload(),
            &sup,
            CompactBlockSpliceSelection::FULL_BLOCK,
        ),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));

    let full_proto = prost_compact_block(&block, CompactBlockSpliceSelection::FULL_BLOCK)?;
    assert!(matches!(
        splice_compact_block_wire(
            &full_proto.payload(),
            &[],
            CompactBlockSpliceSelection::DEFAULT_SHIELDED,
        ),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));

    let dir = TempDir::new()?;
    let (_p, shielded_reader, _id, sup_reader) = write_paired_segments(
        dir.path(),
        &[empty_block(300), mixed_interleaved_block(301)?],
        false,
        SettledSupplementSegmentWriteOptions::default(),
    )?;
    let wrong_height_payload = shielded_reader.read_projected_range(
        BlockHeightRange::inclusive(BlockHeight::new(300), BlockHeight::new(300)),
        HeightOrder::Ascending,
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
    )?[0]
        .payload();
    assert!(matches!(
        sup_reader.splice_block_wire(
            BlockHeight::new(301),
            &wrong_height_payload,
            CompactBlockSpliceSelection::FULL_BLOCK,
        ),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    assert!(sup_reader.read_block(BlockHeight::new(301)).is_ok());
    Ok(())
}

#[test]
fn tamper_header_indexes_chunk_trailer_and_post_admission_sticky_invalid() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = build_rich_corpus(600)?;
    let (seg_path, shielded_reader, seg_id, reader) = write_paired_segments(
        dir.path(),
        &blocks,
        false,
        SettledSupplementSegmentWriteOptions {
            codec: SupplementCompressionCodec::Zstd,
            chunk_blocks: 3,
        },
    )?;

    for offset in [
        0_u64,
        160_u64,
        u64::try_from(fs::read(&seg_path)?.len() - 1)?,
    ] {
        let tampered_path = dir.path().join(format!("tamper_{offset}.zstsup01"));
        fs::copy(&seg_path, &tampered_path)?;
        flip_byte_at(&tampered_path, offset)?;
        assert!(
            SettledSupplementSegmentReader::open(
                &tampered_path,
                &seg_id,
                SettledSupplementSegmentOpenOptions::default(),
            )
            .is_err()
        );
    }

    if !reader.kernel_verified() {
        let post_path = dir.path().join("post_admit.zstsup01");
        fs::copy(&seg_path, &post_path)?;
        let post_reader = SettledSupplementSegmentReader::open(
            &post_path,
            &seg_id,
            SettledSupplementSegmentOpenOptions::default(),
        )?;
        flip_byte_at(&post_path, u64::try_from(HEADER_LEN + 8)?)?;
        let target_height = BlockHeight::new(600);
        assert!(matches!(
            post_reader.read_block(target_height),
            Err(SettledSupplementSegmentError::PayloadIntegrity(_))
        ));
        let shielded_frame = shielded_reader.read_projected_range(
            BlockHeightRange::inclusive(target_height, target_height),
            HeightOrder::Ascending,
            ShieldedProjectionSelection::DEFAULT_SHIELDED,
        )?[0]
            .payload();
        assert!(matches!(
            post_reader.has_transparent_at(target_height),
            Err(SettledSupplementSegmentError::PayloadIntegrity(_))
        ));
        assert!(matches!(
            post_reader.splice_block_wire(
                target_height,
                &shielded_frame,
                CompactBlockSpliceSelection::DEFAULT_SHIELDED,
            ),
            Err(SettledSupplementSegmentError::PayloadIntegrity(_))
        ));
        assert!(matches!(
            post_reader.reconstruct_block(target_height, &shielded_frame),
            Err(SettledSupplementSegmentError::PayloadIntegrity(_))
        ));
    }
    Ok(())
}

fn build_forged_interior_segment(
    b0: &CompactBlockArtifact,
    b1: &CompactBlockArtifact,
) -> TestOutcome<(Vec<u8>, SupplementSegmentIdentity)> {
    let sup_rec0 = BlockSupplementRecord {
        transparent_transactions: Vec::new(),
        canonical_metadata: None,
    };
    let sup_rec1 = BlockSupplementRecord {
        transparent_transactions: transparent_supplement_from_compact(b1),
        canonical_metadata: None,
    };
    let (mut bytes, mut id) = build_synthetic_segment_bytes(
        b0.height().value(),
        &[(b0.clone(), sup_rec0), (b1.clone(), sup_rec1)],
        SupplementCompressionCodec::None,
        16,
    )?;

    let header = SegmentHeader::decode(bytes[..HEADER_LEN].try_into()?)?;
    let row_off = usize::try_from(header.row_index_offset)?;
    let row0_raw =
        DecodedRowIndexEntry::decode(bytes[row_off..row_off + ROW_INDEX_ENTRY_LEN].try_into()?)?;
    let row1_idx_off = row_off + ROW_INDEX_ENTRY_LEN;
    let mut row1_raw = DecodedRowIndexEntry::decode(
        bytes[row1_idx_off..row1_idx_off + ROW_INDEX_ENTRY_LEN].try_into()?,
    )?;
    let row1_data_off = HEADER_LEN + usize::try_from(row0_raw.uncompressed_len)?;
    let row1_data_end = row1_data_off + usize::try_from(row1_raw.uncompressed_len)?;
    bytes[row1_data_off + 2] ^= 0x01;
    row1_raw.payload_sha256 = Sha256::digest(&bytes[row1_data_off..row1_data_end]).into();
    bytes[row1_idx_off..row1_idx_off + ROW_INDEX_ENTRY_LEN].copy_from_slice(&row1_raw.encode());

    let chunk_off = usize::try_from(header.chunk_index_offset)?;
    let new_chunk_sha: [u8; 32] = Sha256::digest(&bytes[HEADER_LEN..chunk_off]).into();
    bytes[chunk_off + 16..chunk_off + 48].copy_from_slice(&new_chunk_sha);

    let body_end = bytes.len() - TRAILER_LEN;
    let new_trailer: [u8; 32] = Sha256::digest(&bytes[..body_end]).into();
    bytes[body_end..].copy_from_slice(&new_trailer);
    id.sha256 = new_trailer;
    Ok((bytes, id))
}

#[test]
fn forged_interior_envelope_sets_sticky_invalid_on_all_read_paths() -> TestOutcome {
    let dir = TempDir::new()?;
    let b0 = empty_block(400);
    let b1 = mixed_interleaved_block(401)?;
    let (bytes, id) = build_forged_interior_segment(&b0, &b1)?;
    let path = dir.path().join("forged_interior.zstsup01");
    fs::write(&path, &bytes)?;
    let reader = SettledSupplementSegmentReader::open(
        &path,
        &id,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    assert!(reader.has_transparent_at(BlockHeight::new(401))?);
    assert!(matches!(
        reader.read_block(BlockHeight::new(401)),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.has_transparent_at(BlockHeight::new(401)),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.read_transparent_block(BlockHeight::new(401)),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.read_canonical_metadata(BlockHeight::new(401)),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.read_range(BlockHeight::new(400), BlockHeight::new(401)),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    let shielded = prost_compact_block(&b1, CompactBlockSpliceSelection::DEFAULT_SHIELDED)?;
    assert!(matches!(
        reader.splice_block_wire(
            BlockHeight::new(401),
            &shielded.payload(),
            CompactBlockSpliceSelection::DEFAULT_SHIELDED,
        ),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.splice_range_wire(
            BlockHeight::new(401),
            &[shielded.payload()],
            CompactBlockSpliceSelection::FULL_BLOCK,
        ),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.reconstruct_block(BlockHeight::new(401), &shielded.payload()),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn rejects_corrupt_zstd_stream_exceeding_declared_chunk_bound() -> TestOutcome {
    let dir = TempDir::new()?;
    let block = empty_block(250);
    let large_payload = vec![0x42_u8; 4_096];
    let oversized_zstd = zstd::bulk::compress(&large_payload, 3)?;
    let chunk_sha256: [u8; 32] = Sha256::digest(&oversized_zstd).into();
    let chunk = DecodedChunkIndexEntry {
        data_rel_offset: 0,
        compressed_len: u32::try_from(oversized_zstd.len())?,
        uncompressed_len: 128,
        chunk_sha256,
    };
    let row = DecodedRowIndexEntry {
        block_hash_wire: encode_internal_block_hash(block.block_hash()),
        payload_sha256: [0x11; 32],
        chunk_rel_offset: 0,
        uncompressed_len: 128,
        block_time: block.time(),
        row_flags: 0,
    };
    let (raw, id) = assemble_segment_file(
        250,
        &[(
            block,
            BlockSupplementRecord {
                transparent_transactions: Vec::new(),
                canonical_metadata: None,
            },
        )],
        (SupplementCompressionCodec::Zstd, 16),
        (&oversized_zstd, &[chunk], &[row]),
    )?;
    let path = dir.path().join("zstd_bomb.zstsup01");
    fs::write(&path, &raw)?;

    let reader = SettledSupplementSegmentReader::open(
        &path,
        &id,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    assert!(matches!(
        reader.read_block(BlockHeight::new(250)),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    let state = reader.chunk_validation_state(0);
    assert_eq!(state, VALIDATION_INVALID);
    assert!(matches!(
        reader.has_transparent_at(BlockHeight::new(250)),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn truncation_mid_chunk_latches_sticky_invalid_on_shortcut_paths() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = build_rich_corpus(650)?;
    let (seg_path, shielded_reader, seg_id, reader) = write_paired_segments(
        dir.path(),
        &blocks,
        false,
        SettledSupplementSegmentWriteOptions::default(),
    )?;
    if reader.kernel_verified() {
        return Ok(());
    }
    let trunc_path = dir.path().join("truncated.zstsup01");
    fs::copy(&seg_path, &trunc_path)?;
    let trunc_reader = SettledSupplementSegmentReader::open(
        &trunc_path,
        &seg_id,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    OpenOptions::new()
        .write(true)
        .open(&trunc_path)?
        .set_len(u64::try_from(HEADER_LEN + 4)?)?;

    let target = BlockHeight::new(650);
    assert!(matches!(
        trunc_reader.read_block(target),
        Err(SettledSupplementSegmentError::Io(_))
    ));
    assert!(matches!(
        trunc_reader.has_transparent_at(target),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    let shielded = shielded_reader.read_projected_range(
        BlockHeightRange::inclusive(target, target),
        HeightOrder::Ascending,
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
    )?[0]
        .payload();
    assert!(matches!(
        trunc_reader.splice_block_wire(
            target,
            &shielded,
            CompactBlockSpliceSelection::DEFAULT_SHIELDED,
        ),
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn open_respects_require_fs_verity_flag() -> TestOutcome {
    let dir = TempDir::new()?;
    let block = empty_block(900);
    let (path, _shielded, id, reader) = write_paired_segments(
        dir.path(),
        &[block],
        false,
        SettledSupplementSegmentWriteOptions::default(),
    )?;
    let strict = SettledSupplementSegmentReader::open(
        &path,
        &id,
        SettledSupplementSegmentOpenOptions {
            require_fs_verity: true,
        },
    );
    if reader.kernel_verified() {
        assert!(strict?.kernel_verified());
    } else {
        assert!(matches!(
            strict,
            Err(SettledSupplementSegmentError::FsVerityRequired)
        ));
    }
    Ok(())
}

#[test]
fn admit_protected_rejects_mismatched_trailer_pin() -> TestOutcome {
    let header = synthetic_header(16, 128);
    let chunks = [synthetic_chunk(128, 128)];
    let rows = [synthetic_row(128)];
    let tables = SegmentTables {
        chunks: &chunks,
        rows: &rows,
    };
    let mut expected = SupplementSegmentIdentity::span(
        Network::ZcashMainnet,
        1,
        BlockHeight::new(10),
        BlockId::new(BlockHeight::new(10), det_block_hash(10)),
    );
    expected.sha256 = [0xAA; 32];
    admit_protected(
        &header,
        tables,
        PortablePin {
            expected: &expected,
            trailer: &[0xAA; 32],
        },
    )?;

    let mismatch = admit_protected(
        &header,
        SegmentTables {
            chunks: &chunks,
            rows: &rows,
        },
        PortablePin {
            expected: &expected,
            trailer: &[0xBB; 32],
        },
    );
    assert!(matches!(
        mismatch,
        Err(SettledSupplementSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn indexed_state_mark_valid_preserves_sticky_invalid() {
    let chunk = synthetic_chunk(64, 64);
    chunk.mark_invalid();
    chunk.mark_valid();
    assert_eq!(
        chunk.validation_state.load(Ordering::Acquire),
        VALIDATION_INVALID
    );

    let row = synthetic_row(64);
    row.mark_invalid();
    row.mark_valid();
    assert_eq!(
        row.validation_state.load(Ordering::Acquire),
        VALIDATION_INVALID
    );
}

fn flip_byte_at(path: &Path, offset: u64) -> TestOutcome {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte)?;
    byte[0] ^= 0xFF;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&byte)?;
    file.flush()?;
    Ok(())
}
