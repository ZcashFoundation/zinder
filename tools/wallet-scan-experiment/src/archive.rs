//! Native compact-block payloads on a read-only, writer-admitted Testnet snapshot.

use std::{mem, path::Path, time::Instant};

use eyre::{OptionExt as _, Result, ensure, eyre};
use prost::{Message as _, encoding::bytes};
use serde::Serialize;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, Network, wire::encode_rpc_block_hash_hex,
};
use zinder_proto::{v1::wallet, wire::compact_block_message};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, DEFAULT_REORG_WINDOW_BLOCKS, RawBlobRetention,
    RocksDbCanonicalSecondary, RocksDbResourceBudget,
};

use crate::corpus::{Pool, ScanAction, shifted_ephemeral_key};

#[derive(Default, Serialize)]
pub(crate) struct ProjectionReport {
    start_height: u32,
    end_height: u32,
    end_hash: String,
    full_native_item_bytes: u64,
    shielded_native_item_bytes: u64,
    shielded_with_hints_native_item_bytes: u64,
    orchard_actions: usize,
    ironwood_actions: usize,
    shifted_keys: usize,
    hint_derivation_seconds: f64,
}

pub(crate) fn read_range(
    store: &Path,
    start: u32,
    end: u32,
    mut visit: impl FnMut(ScanAction) -> Result<()>,
) -> Result<ProjectionReport> {
    ensure!(
        start > 0 && start <= end,
        "expected an ascending range from height 1 or later"
    );
    let temporary = tempfile::tempdir()?;
    let reader = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        store,
        temporary.path(),
        Network::ZcashTestnet,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(DEFAULT_REORG_WINDOW_BLOCKS)?,
        RocksDbResourceBudget::canonical_reader_defaults(),
    )?;
    let fence = reader.ready_evidence();
    ensure!(
        start >= fence.first_retained_block.height.value()
            && end <= fence.visible_tip.height.value(),
        "range is outside the retained Testnet snapshot"
    );

    let mut report = ProjectionReport {
        start_height: start,
        end_height: end,
        ..ProjectionReport::default()
    };
    let mut previous_hash: Option<BlockHash> = None;
    let mut first = start;
    loop {
        let last = first.saturating_add(127).min(end);
        let range = BlockHeightRange::inclusive(BlockHeight::new(first), BlockHeight::new(last));
        let blocks = reader.compact_blocks_in_range(range)?;
        ensure!(
            usize::try_from(last - first + 1)? == blocks.len(),
            "short compact-block range"
        );
        for (height, block) in (first..=last).zip(blocks) {
            ensure!(
                height == block.height().value(),
                "unexpected block at height {height}"
            );
            ensure!(
                previous_hash.is_none_or(|hash| hash == block.previous_block_hash()),
                "chain discontinuity at {height}"
            );
            previous_hash = Some(block.block_hash());
            measure_block(compact_block_message(&block), &mut report, &mut visit)?;
        }
        if last == end {
            break;
        }
        first = last + 1;
    }
    report.end_hash = encode_rpc_block_hash_hex(previous_hash.ok_or_eyre("empty block range")?);
    Ok(report)
}

fn stream_item(block: wallet::CompactBlock) -> wallet::CompactBlocksInRangeChunk {
    wallet::CompactBlocksInRangeChunk {
        body: Some(wallet::compact_blocks_in_range_chunk::Body::Item(block)),
    }
}

fn measure_block(
    mut block: wallet::CompactBlock,
    report: &mut ProjectionReport,
    visit: &mut impl FnMut(ScanAction) -> Result<()>,
) -> Result<()> {
    report.full_native_item_bytes +=
        u64::try_from(prost::encoding::message::encoded_len(2, &block))?;
    for tx in &mut block.transactions {
        let fields = tx.data.as_mut().ok_or_eyre("missing native scan fields")?;
        fields.transparent_inputs.clear();
        fields.transparent_outputs.clear();
    }
    block.transactions.retain(|tx| {
        tx.data.as_ref().is_some_and(|fields| {
            !fields.sapling_spends.is_empty()
                || !fields.sapling_outputs.is_empty()
                || !fields.orchard_actions.is_empty()
                || !fields.ironwood_actions.is_empty()
        })
    });
    let shielded = stream_item(block.clone());
    report.shielded_native_item_bytes += u64::try_from(shielded.encoded_len())?;

    // Append the experimental length-delimited fields bottom-up so changes in
    // the enclosing messages' varint lengths are included in the measurement.
    let transactions = mem::take(&mut block.transactions);
    let mut block_bytes = block.encode_to_vec();
    for mut tx in transactions {
        let mut fields = tx.data.take().ok_or_eyre("missing native scan fields")?;
        let orchard = mem::take(&mut fields.orchard_actions);
        let ironwood = mem::take(&mut fields.ironwood_actions);
        report.orchard_actions += orchard.len();
        report.ironwood_actions += ironwood.len();
        let mut scan_fields = fields.encode_to_vec();
        for (tag, pool, selected) in [(4, Pool::Orchard, orchard), (5, Pool::Ironwood, ironwood)] {
            for action in selected {
                let (corpus_action, encoded) = encode_action(pool, &action, report)?;
                bytes::encode(tag, &encoded, &mut scan_fields);
                visit(corpus_action)?;
            }
        }
        let mut transaction = tx.encode_to_vec();
        bytes::encode(3, &scan_fields, &mut transaction);
        bytes::encode(5, &transaction, &mut block_bytes);
    }
    let mut chunk = Vec::new();
    bytes::encode(2, &block_bytes, &mut chunk);
    ensure!(
        wallet::CompactBlocksInRangeChunk::decode(chunk.as_slice())? == shielded,
        "experimental encoding changed existing native fields at height {}",
        block.height
    );
    report.shielded_with_hints_native_item_bytes += u64::try_from(chunk.len())?;
    Ok(())
}

fn encode_action(
    pool: Pool,
    action: &wallet::CompactShieldedAction,
    report: &mut ProjectionReport,
) -> Result<(ScanAction, Vec<u8>)> {
    let epk = action.ephemeral_key.as_slice().try_into()?;
    let started = Instant::now();
    let hint = shifted_ephemeral_key(&epk);
    report.hint_derivation_seconds += started.elapsed().as_secs_f64();
    let mut encoded = action.encode_to_vec();
    if let Some(point) = hint {
        bytes::encode(5, &point.to_vec(), &mut encoded);
        report.shifted_keys += 1;
    }
    let scan_action = ScanAction {
        pool,
        nullifier: action.nullifier.as_slice().try_into()?,
        cmx: action.commitment.as_slice().try_into()?,
        epk,
        ciphertext: action
            .ciphertext
            .as_slice()
            .try_into()
            .map_err(|_| eyre!("expected 52 ciphertext bytes"))?,
        hint,
    };
    Ok((scan_action, encoded))
}

#[cfg(test)]
mod tests {
    use pasta_curves::{
        group::{Group as _, GroupEncoding as _, ff::Field as _},
        pallas,
    };

    use super::*;

    fn action(epk: [u8; 32]) -> wallet::CompactShieldedAction {
        wallet::CompactShieldedAction {
            nullifier: vec![1; 32],
            commitment: vec![2; 32],
            ephemeral_key: epk.to_vec(),
            ciphertext: vec![3; 52],
        }
    }

    #[derive(prost::Message)]
    struct HintField {
        #[prost(bytes = "vec", optional, tag = "5")]
        shifted_key: Option<Vec<u8>>,
    }

    #[test]
    fn experimental_action_field_matches_its_public_point() -> Result<()> {
        let point = pallas::Point::generator();
        let expected = (point * pallas::Scalar::from(2).pow_vartime([64]))
            .to_bytes()
            .to_vec();
        for (epk, expected) in [(point.to_bytes(), Some(expected)), ([0xff; 32], None)] {
            let original = action(epk);
            let (_, encoded) =
                encode_action(Pool::Orchard, &original, &mut ProjectionReport::default())?;
            assert_eq!(HintField::decode(encoded.as_slice())?.shifted_key, expected);
            assert_eq!(
                wallet::CompactShieldedAction::decode(encoded.as_slice())?,
                original
            );
        }
        Ok(())
    }

    #[test]
    fn counts_only_requested_fields_and_emits_valid_points_in_pool_order() -> Result<()> {
        let point = pallas::Point::generator();
        let transparent = wallet::CompactTransparentInput {
            previous_transaction_id: vec![4; 32],
            previous_output_index: 1,
        };
        let transparent_tx = wallet::CompactTransaction {
            transaction_id: vec![5; 32],
            data: Some(wallet::CompactTransactionData {
                transparent_inputs: vec![transparent.clone()],
                ..Default::default()
            }),
            ..Default::default()
        };
        let shielded_fields = wallet::CompactTransactionData {
            fee_zat: Some(10_000),
            orchard_actions: vec![action(point.to_bytes()), action([0xff; 32])],
            ironwood_actions: vec![action(point.double().to_bytes())],
            ..Default::default()
        };
        let shielded_tx = wallet::CompactTransaction {
            index: 2,
            transaction_id: vec![6; 32],
            data: Some(shielded_fields.clone()),
        };
        let mixed_tx = wallet::CompactTransaction {
            data: Some(wallet::CompactTransactionData {
                transparent_inputs: vec![transparent],
                ..shielded_fields
            }),
            ..shielded_tx.clone()
        };
        let expected = wallet::CompactBlock {
            height: 100,
            transactions: vec![shielded_tx],
            chain_metadata: Some(wallet::CompactChainMetadata {
                orchard_commitment_tree_size: 123,
                ironwood_commitment_tree_size: 10,
                ..Default::default()
            }),
            ..Default::default()
        };
        let full = wallet::CompactBlock {
            transactions: vec![transparent_tx, mixed_tx],
            ..expected.clone()
        };

        let mut report = ProjectionReport::default();
        let mut actions = Vec::new();
        let full_len = stream_item(full.clone()).encoded_len() as u64;
        let expected_len = stream_item(expected).encoded_len() as u64;
        measure_block(full, &mut report, &mut |action| {
            actions.push(action);
            Ok(())
        })?;
        assert_eq!(report.full_native_item_bytes, full_len);
        assert_eq!(report.shielded_native_item_bytes, expected_len);
        assert_eq!(
            report.shielded_with_hints_native_item_bytes,
            expected_len + 68
        );
        assert_eq!(
            (
                report.orchard_actions,
                report.ironwood_actions,
                report.shifted_keys
            ),
            (2, 1, 2)
        );
        let [orchard, malformed, ironwood] = actions.as_slice() else {
            return Err(eyre!("expected three actions"));
        };
        let shift = pallas::Scalar::from(2).pow_vartime([64]);
        assert_eq!(
            (orchard.pool, malformed.pool, ironwood.pool),
            (Pool::Orchard, Pool::Orchard, Pool::Ironwood)
        );
        assert_eq!(orchard.hint, Some((point * shift).to_bytes()));
        assert_eq!(malformed.hint, None);
        assert_eq!(ironwood.hint, Some((point.double() * shift).to_bytes()));
        Ok(())
    }
}
