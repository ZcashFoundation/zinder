use eyre::Result;
use tonic::transport::Server;
use zinder_compat_lightwalletd::{
    CanonicalReaderSlot, CompactBlocksGrpcAdapter, LightwalletdGrpcAdapter,
};
use zinder_core::{
    BlockHeight, BlockId, CommitmentTreeFrontiers, CompactBlockArtifact, CompactChainMetadata,
    CompactSaplingOutput, CompactTransaction, CompactTransactionData, ShieldedProtocol,
    TransactionId,
};
use zinder_proto::compat::lightwalletd;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
    frontier_of_size, sample_regtest_upgrade_activations,
};

use super::{Endpoint, EpochPinRecorder, NETWORK, ReaderFixture, block_range_request};

const TIP: u32 = 8;
const SEEDED_SAPLING_SIZE: u32 = 7;

/// Declares the same headers as `chain` above a checkpoint at height 1 whose
/// Sapling tree already holds `SEEDED_SAPLING_SIZE` leaves, so every compact
/// block reports that tree size instead of zero.
fn reseeded(mut chain: ChainFixture) -> Result<ChainFixture> {
    let metadata = CompactChainMetadata {
        sapling_commitment_tree_size: SEEDED_SAPLING_SIZE,
        orchard_commitment_tree_size: 0,
        ironwood_commitment_tree_size: 0,
    };
    for block in chain.blocks().to_vec() {
        let parent_hash = if block.height == BlockHeight::new(1) {
            NETWORK.genesis_hash()
        } else {
            block.parent_hash
        };
        chain = chain.with_compact_block_artifact(CompactBlockArtifact::empty(
            BlockId::new(block.height, block.hash),
            parent_hash,
            block.block_time_seconds,
            metadata,
        ));
    }
    Ok(chain.with_commitment_tree_frontiers_at(
        BlockHeight::new(1),
        CommitmentTreeFrontiers::from_validated_parts(
            Some(frontier_of_size(
                ShieldedProtocol::Sapling,
                SEEDED_SAPLING_SIZE,
            )?),
            None,
            None,
        )?,
    ))
}

fn sapling_sizes(blocks: &[lightwalletd::CompactBlock]) -> Vec<u32> {
    blocks
        .iter()
        .map(|block| {
            block
                .chain_metadata
                .as_ref()
                .map_or(u32::MAX, |metadata| metadata.sapling_commitment_tree_size)
        })
        .collect()
}

/// Serves `original` from both adapters, replaces it with `replacement`, and
/// returns what each adapter then serves for `first..=last`.
async fn served_after_replacement(
    original: &ReaderFixture,
    replacement: &ReaderFixture,
    first: u64,
    last: u64,
) -> Result<[Vec<lightwalletd::CompactBlock>; 2]> {
    let mut recorder = EpochPinRecorder::new(original.query.clone())
        .with_later_views(vec![replacement.query.clone()]);
    recorder.expose_encoded_range = true;
    let mut wallet = Endpoint::start(
        Server::builder().add_service(LightwalletdGrpcAdapter::new(recorder.clone()).into_server()),
    )
    .await?;
    let slot = CanonicalReaderSlot::new(original.reader.clone());
    let mut blocks_only = Endpoint::start(
        Server::builder().add_service(CompactBlocksGrpcAdapter::new(slot.clone()).into_server()),
    )
    .await?;
    let warmed = u64::from(TIP);
    for endpoint in [&mut wallet, &mut blocks_only] {
        let blocks = endpoint.blocks(block_range_request(first, warmed)).await?;
        assert_eq!(blocks.len(), usize::try_from(warmed - first + 1)?);
    }
    assert!(!recorder.recorded_encoded_ranges.lock().is_empty());

    recorder.advance_view();
    drop(slot.publish(replacement.reader.clone()));
    Ok([
        wallet.blocks(block_range_request(first, last)).await?,
        blocks_only.blocks(block_range_request(first, last)).await?,
    ])
}

#[tokio::test]
async fn a_replacement_store_with_another_seed_frontier_serves_its_own_chain_metadata() -> Result<()>
{
    let activations = sample_regtest_upgrade_activations();
    let chain = ChainFixture::new(NETWORK).extend_blocks(TIP + 2);
    let original = ReaderFixture::new(
        CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(&chain).through(BlockHeight::new(TIP)),
            &activations,
        )?
        .into_serving_store_fixture()?,
    )?;
    let reseeded_chain = reseeded(chain)?;
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&reseeded_chain)
            .through(BlockHeight::new(TIP))
            .with_history_floor(BlockHeight::new(1)),
        &activations,
    )?;
    let same_tip = ReaderFixture::new(writer.serving_store_fixture()?)?;
    writer.append_live_block(&reseeded_chain)?;
    let longer_tip = ReaderFixture::new(writer.into_serving_store_fixture()?)?;
    let mut before = Endpoint::start(
        Server::builder()
            .add_service(LightwalletdGrpcAdapter::new(original.query.clone()).into_server()),
    )
    .await?;
    let original_blocks = before
        .blocks(block_range_request(2, u64::from(TIP)))
        .await?;
    assert_eq!(
        sapling_sizes(&original_blocks),
        vec![0; original_blocks.len()]
    );

    for (replacement, last) in [(&same_tip, TIP), (&longer_tip, TIP + 1)] {
        let mut reference = Endpoint::start(
            Server::builder()
                .add_service(LightwalletdGrpcAdapter::new(replacement.query.clone()).into_server()),
        )
        .await?;
        let expected = reference
            .blocks(block_range_request(2, u64::from(last)))
            .await?;
        assert_eq!(
            sapling_sizes(&expected),
            vec![SEEDED_SAPLING_SIZE; expected.len()]
        );
        for served in served_after_replacement(&original, replacement, 2, u64::from(last)).await? {
            assert_eq!(served, expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn an_equal_fence_from_another_admitted_generation_is_read_again() -> Result<()> {
    let chain = ChainFixture::new(NETWORK).extend_blocks(TIP + 1);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).through(BlockHeight::new(TIP)),
        &sample_regtest_upgrade_activations(),
    )?;
    let first = ReaderFixture::new(writer.serving_store_fixture()?)?;
    let second = ReaderFixture::new(writer.into_serving_store_fixture()?)?;
    let mut recorder =
        EpochPinRecorder::new(first.query.clone()).with_later_views(vec![second.query.clone()]);
    recorder.expose_encoded_range = true;
    let mut wallet = Endpoint::start(
        Server::builder().add_service(LightwalletdGrpcAdapter::new(recorder.clone()).into_server()),
    )
    .await?;
    let span = |first: u32, last: u32| {
        zinder_core::BlockHeightRange::inclusive(BlockHeight::new(first), BlockHeight::new(last))
    };
    let expected = wallet
        .blocks(block_range_request(2, u64::from(TIP)))
        .await?;
    assert_eq!(
        wallet
            .blocks(block_range_request(2, u64::from(TIP)))
            .await?,
        expected
    );
    assert_eq!(
        *recorder.recorded_encoded_ranges.lock(),
        vec![span(2, TIP), span(2, TIP)]
    );

    recorder.advance_view();
    assert_eq!(
        wallet
            .blocks(block_range_request(2, u64::from(TIP)))
            .await?,
        expected
    );
    assert_eq!(
        *recorder.recorded_encoded_ranges.lock(),
        vec![span(2, TIP), span(2, TIP), span(2, TIP)]
    );
    Ok(())
}

const OUTPUT_HEIGHT: u32 = 4;

/// Declares one Sapling output with `commitment` at `OUTPUT_HEIGHT`, so two
/// chains that differ only in `commitment` share every header, tree size,
/// fact, and row length, and differ in one lower compact payload.
fn with_sapling_output(chain: ChainFixture, commitment: [u8; 32]) -> Result<ChainFixture> {
    let height = BlockHeight::new(OUTPUT_HEIGHT);
    let transaction_id = TransactionId::from_bytes([23; 32]);
    let output_block = super::fixture_block(&chain, height)?;
    let mut rows = FixtureTransactionRows::from_raw_transaction(
        transaction_id,
        height,
        output_block.hash,
        0,
        b"one sapling output",
    );
    rows.facts.public_facts.counts.sapling_output_count = 1;
    let mut chain = chain.with_transaction_rows(rows);
    for block in chain.blocks().to_vec() {
        if block.height < height {
            continue;
        }
        let transactions = if block.height == height {
            vec![CompactTransaction {
                index: 0,
                transaction_id,
                data: CompactTransactionData {
                    sapling_outputs: vec![CompactSaplingOutput {
                        commitment,
                        ephemeral_key: [5; 32],
                        ciphertext: [6; 52],
                    }],
                    ..Default::default()
                },
            }]
        } else {
            Vec::new()
        };
        chain = chain.with_compact_block_artifact(CompactBlockArtifact::new(
            BlockId::new(block.height, block.hash),
            block.parent_hash,
            block.block_time_seconds,
            transactions,
            CompactChainMetadata {
                sapling_commitment_tree_size: 1,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )?);
    }
    Ok(chain)
}

#[tokio::test]
async fn a_replacement_store_with_another_lower_payload_of_equal_length_serves_its_own_bytes()
-> Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let chain = ChainFixture::new(NETWORK).extend_blocks(TIP + 1);
    let store = |commitment| -> Result<ReaderFixture> {
        let chain = with_sapling_output(chain.clone(), commitment)?;
        ReaderFixture::new(
            CanonicalWriterFixture::publish(
                &CanonicalBaseline::new(&chain).through(BlockHeight::new(TIP)),
                &activations,
            )?
            .into_serving_store_fixture()?,
        )
    };
    let original = store([1; 32])?;
    let replacement = store([2; 32])?;

    let mut reference = Endpoint::start(
        Server::builder()
            .add_service(LightwalletdGrpcAdapter::new(replacement.query.clone()).into_server()),
    )
    .await?;
    let expected = reference
        .blocks(block_range_request(2, u64::from(TIP)))
        .await?;
    let lower = usize::try_from(OUTPUT_HEIGHT - 2)?;
    let outputs =
        |blocks: &[lightwalletd::CompactBlock]| blocks[lower].vtx[0].outputs[0].cmu.clone();
    assert_eq!(outputs(&expected), vec![2; 32]);
    for served in served_after_replacement(&original, &replacement, 2, u64::from(TIP)).await? {
        assert_eq!(served, expected);
    }
    Ok(())
}
