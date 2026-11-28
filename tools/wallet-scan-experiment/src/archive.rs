//! Native compact-block payloads on a read-only, writer-admitted Testnet snapshot.

use std::{mem, path::Path, time::Instant};

use eyre::{OptionExt as _, Result, ensure, eyre};
use prost::{Message as _, encoding::bytes};
use serde::Serialize;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, CompactBlockArtifact, Network,
    wire::encode_rpc_block_hash_hex,
};
use zinder_proto::{v1::wallet, wire::compact_block_message};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreReadyEvidence, CanonicalStoreRequirements,
    DEFAULT_REORG_WINDOW_BLOCKS, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbResourceBudget,
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
    raw_blob_retention: RawBlobRetention,
    start: u32,
    end: u32,
    visit: impl FnMut(ScanAction) -> Result<()>,
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
        CanonicalStoreRequirements::for_blob_retention(raw_blob_retention),
        CanonicalReorgPolicy::new(DEFAULT_REORG_WINDOW_BLOCKS)?,
        RocksDbResourceBudget::canonical_reader_defaults(),
    )?;
    measure_retained_range(
        &reader.ready_evidence(),
        start,
        end,
        |range| Ok(reader.compact_blocks_in_range(range)?),
        visit,
    )
}

fn measure_retained_range(
    fence: &CanonicalStoreReadyEvidence,
    start: u32,
    end: u32,
    mut read_blocks: impl FnMut(BlockHeightRange) -> Result<Vec<CompactBlockArtifact>>,
    mut visit: impl FnMut(ScanAction) -> Result<()>,
) -> Result<ProjectionReport> {
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
        let blocks = read_blocks(range)?;
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
    use zinder_core::{
        BlockId, CompactShieldedAction, CompactTransaction, CompactTransactionData,
        NetworkUpgradeActivations, TransactionId,
    };
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
        network_upgrade_fixtures::sample_regtest_upgrade_activations,
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

    #[derive(prost::Message)]
    struct HintedAction {
        #[prost(bytes = "vec", tag = "1")]
        nullifier: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        commitment: Vec<u8>,
        #[prost(bytes = "vec", tag = "3")]
        ephemeral_key: Vec<u8>,
        #[prost(bytes = "vec", tag = "4")]
        ciphertext: Vec<u8>,
        #[prost(bytes = "vec", tag = "5")]
        shifted_key: Vec<u8>,
    }

    #[derive(prost::Message)]
    struct HintedData {
        #[prost(message, repeated, tag = "4")]
        orchard_actions: Vec<HintedAction>,
    }

    #[derive(prost::Message)]
    struct HintedTransaction {
        #[prost(message, required, tag = "3")]
        scan_fields: HintedData,
    }

    #[derive(prost::Message)]
    struct HintedBlock {
        #[prost(message, repeated, tag = "5")]
        transactions: Vec<HintedTransaction>,
    }

    #[derive(prost::Message)]
    struct HintedItem {
        #[prost(message, required, tag = "2")]
        block: HintedBlock,
    }

    #[test]
    fn counts_enclosing_lengths_that_grow_past_two_varint_bytes() -> Result<()> {
        const ACTIONS: usize = 100;
        const MAX_TWO_BYTE_VARINT: usize = 16_383;
        let original = action(pallas::Point::generator().to_bytes());
        let fields = wallet::CompactTransactionData {
            orchard_actions: vec![original.clone(); ACTIONS],
            ..Default::default()
        };
        let hinted_fields = HintedData {
            orchard_actions: (0..ACTIONS)
                .map(|_| HintedAction {
                    nullifier: original.nullifier.clone(),
                    commitment: original.commitment.clone(),
                    ephemeral_key: original.ephemeral_key.clone(),
                    ciphertext: original.ciphertext.clone(),
                    shifted_key: vec![4; 32],
                })
                .collect(),
        };
        assert!(fields.encoded_len() <= MAX_TWO_BYTE_VARINT);
        assert!(hinted_fields.encoded_len() > MAX_TWO_BYTE_VARINT);
        let block = wallet::CompactBlock {
            transactions: vec![wallet::CompactTransaction {
                data: Some(fields),
                ..Default::default()
            }],
            ..Default::default()
        };
        let expected = HintedItem {
            block: HintedBlock {
                transactions: vec![HintedTransaction {
                    scan_fields: hinted_fields,
                }],
            },
        };
        assert!(block.encoded_len() <= MAX_TWO_BYTE_VARINT);
        assert_eq!(
            expected.encoded_len() - stream_item(block.clone()).encoded_len(),
            ACTIONS * 34 + 3,
        );

        let mut report = ProjectionReport::default();
        measure_block(block, &mut report, &mut |_| Ok(()))?;
        assert_eq!(report.shifted_keys, ACTIONS);
        assert_eq!(
            report.shielded_with_hints_native_item_bytes,
            u64::try_from(expected.encode_to_vec().len())?,
        );
        Ok(())
    }

    fn retained_testnet_chain() -> Result<ChainFixture> {
        let mut chain = ChainFixture::new(Network::ZcashTestnet).extend_blocks(130);
        let ephemeral_key = pallas::Point::generator().to_bytes();
        for marker in [2, 129, 130] {
            let block = chain
                .block_at(BlockHeight::new(u32::from(marker)))
                .ok_or_eyre("fixture is missing a marked block")?;
            let id = TransactionId::from_bytes([marker; 32]);
            let compact = CompactBlockArtifact::new(
                BlockId::new(block.height, block.hash),
                block.parent_hash,
                block.block_time_seconds,
                vec![CompactTransaction {
                    index: 0,
                    transaction_id: id,
                    data: CompactTransactionData {
                        orchard_actions: vec![CompactShieldedAction {
                            nullifier: [marker; 32],
                            commitment: [0; 32],
                            ephemeral_key,
                            ciphertext: [0; 52],
                        }],
                        ..Default::default()
                    },
                }],
                block.compact_block_artifact().chain_metadata(),
            )?;
            let transaction = FixtureTransactionRows::from_raw_transaction(
                id,
                block.height,
                block.hash,
                0,
                [marker],
            );
            chain = chain
                .with_transaction_rows(transaction)
                .with_compact_block_artifact(compact);
        }
        Ok(chain)
    }

    #[test]
    fn measures_both_retained_endpoints_and_rejects_a_disconnected_batch() -> Result<()> {
        let chain = retained_testnet_chain()?;
        let activations = NetworkUpgradeActivations::new(
            Network::ZcashTestnet,
            sample_regtest_upgrade_activations().activations().to_vec(),
        )?;
        let baseline = CanonicalBaseline::new(&chain).with_history_floor(BlockHeight::new(1));
        let writer = CanonicalWriterFixture::publish(&baseline, &activations)?;
        let temporary = tempfile::tempdir()?;
        let reader = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            writer.canonical_primary_path(),
            temporary.path(),
            Network::ZcashTestnet,
            CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
            writer.canonical_primary()?.reorg_policy(),
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let fence = reader.ready_evidence();
        let mut ranges = Vec::new();
        let mut nullifiers = Vec::new();
        let report = measure_retained_range(
            &fence,
            2,
            130,
            |range| {
                ranges.push((range.start.value(), range.end.value()));
                Ok(reader.compact_blocks_in_range(range)?)
            },
            |action| {
                nullifiers.push(action.nullifier);
                Ok(())
            },
        )?;
        assert_eq!(ranges, [(2, 129), (130, 130)]);
        assert_eq!((report.start_height, report.end_height), (2, 130));
        let tip = chain
            .block_at(BlockHeight::new(130))
            .ok_or_eyre("fixture is missing the range endpoint")?;
        assert_eq!(report.end_hash, encode_rpc_block_hash_hex(tip.hash));
        assert_eq!(nullifiers, [[2; 32], [129; 32], [130; 32]]);

        for (start, end) in [(1, 130), (2, 131)] {
            let error = measure_retained_range(
                &fence,
                start,
                end,
                |_| Err(eyre!("outside range reached the reader")),
                |_| Ok(()),
            )
            .err()
            .ok_or_eyre("outside range was accepted")?;
            assert_eq!(
                error.to_string(),
                "range is outside the retained Testnet snapshot"
            );
        }

        assert_disconnected_boundary_is_not_published(&reader)
    }

    fn assert_disconnected_boundary_is_not_published(
        reader: &RocksDbCanonicalSecondary,
    ) -> Result<()> {
        let output = tempfile::tempdir()?;
        let path = output.path().join("wallet.scan");
        let tip = reader
            .compact_block_at(BlockHeight::new(130))?
            .ok_or_eyre("missing retained tip")?
            .into_parts();
        let disconnected = CompactBlockArtifact::new(
            tip.block_id,
            BlockHash::from_bytes([0xff; 32]),
            tip.time,
            tip.transactions,
            tip.chain_metadata,
        )?;
        let error = crate::measure(Some(&path), |visit| {
            measure_retained_range(
                &reader.ready_evidence(),
                2,
                130,
                |range| {
                    if range.start.value() == 130 {
                        return Ok(vec![disconnected.clone()]);
                    }
                    Ok(reader.compact_blocks_in_range(range)?)
                },
                visit,
            )
        })
        .err()
        .ok_or_eyre("disconnected batch was accepted")?;
        assert_eq!(error.to_string(), "chain discontinuity at 130");
        assert!(!path.exists());
        assert_eq!(output.path().read_dir()?.count(), 0);
        Ok(())
    }
}
