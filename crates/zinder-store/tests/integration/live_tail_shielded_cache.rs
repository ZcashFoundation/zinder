use std::error::Error;

use rust_rocksdb::statistics::Ticker;
use tempfile::TempDir;
use zinder_core::{BlockHash, BlockHeight, BlockHeightRange, Network, TransactionId};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreRequirements, EncodedCompactBlockOutput,
    EncodedCompactBlockPayload, EncodedCompactBlockRangeRequest, HeightOrder,
    RocksDbCanonicalSecondary, RocksDbResourceBudget, RocksDbStatisticsLevel,
    ShieldedProjectionSelection,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
    network_upgrade_fixtures::sample_regtest_upgrade_activations,
};

type TestOutcome<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn tip_request(tip: BlockHeight) -> EncodedCompactBlockRangeRequest {
    EncodedCompactBlockRangeRequest {
        range: BlockHeightRange::inclusive(BlockHeight::new(2), tip),
        order: HeightOrder::Ascending,
        selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
        include_transparent: false,
        output: EncodedCompactBlockOutput::Blocks,
        max_bytes: 2 * 1024 * 1024,
        enforce_wire_fee_limit: true,
    }
}

fn assert_cold_and_warm(
    reader: &RocksDbCanonicalSecondary,
    tip: BlockHeight,
    expected_hash: BlockHash,
) -> TestOutcome {
    let calls = || reader.rocksdb_ticker_count(Ticker::NumberMultigetCalls);
    let before = calls();
    let cold = reader
        .encoded_compact_block_range_up_to(tip_request(tip), None)?
        .ok_or("missing cold live-tail range")?;
    assert_eq!(cold.block_count, usize::try_from(tip.value() - 1)?);
    assert_eq!(cold.last_link.block_hash, expected_hash);
    let after = calls();
    assert_eq!(after - before, 3, "cold tip range uses three MultiGets");

    let warm = reader
        .encoded_compact_block_range_up_to(tip_request(tip), None)?
        .ok_or("missing warm live-tail range")?;
    assert_eq!(warm.last_link, cold.last_link);
    assert_eq!(warm.block_count, cold.block_count);
    let (EncodedCompactBlockPayload::Blocks(cold), EncodedCompactBlockPayload::Blocks(warm)) =
        (cold.payload, warm.payload)
    else {
        return Err("expected individual compact blocks".into());
    };
    for (cold_frame, warm_frame) in cold.iter().zip(&warm) {
        assert_eq!(cold_frame, warm_frame);
        assert_eq!(
            cold_frame.as_length_prefixed().as_ptr(),
            warm_frame.as_length_prefixed().as_ptr()
        );
    }
    assert_eq!(calls(), after, "warm tip range performs zero MultiGets");
    Ok(())
}

#[test]
fn tip_range_multigets_on_open_append_and_reorg_then_reuses_warm_frames() -> TestOutcome {
    let activations = sample_regtest_upgrade_activations();
    let initial_tip = BlockHeight::new(41);
    let appended_tip = BlockHeight::new(42);
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(appended_tip.value());
    let initial_hash = chain
        .block_at(initial_tip)
        .ok_or("absent initial tip")?
        .hash;
    let appended_hash = chain
        .block_at(appended_tip)
        .ok_or("absent appended tip")?
        .hash;
    let chain = chain.with_transaction_rows(FixtureTransactionRows::coinbase(
        TransactionId::from_bytes([42; 32]),
        appended_tip,
        appended_hash,
        vec![42],
    ));
    let baseline = CanonicalBaseline::new(&chain)
        .through(initial_tip)
        .with_settled_tip(BlockHeight::new(1));
    let mut writer = CanonicalWriterFixture::publish(&baseline, &activations)?;
    let dir = TempDir::new()?;
    let mut resources = RocksDbResourceBudget::for_local_tests();
    resources.statistics_level = RocksDbStatisticsLevel::Tickers;
    let mut reader = RocksDbCanonicalSecondary::open_ready(
        writer.canonical_primary_path(),
        dir.path().join("secondary"),
        &activations,
        CanonicalStoreRequirements::default(),
        CanonicalReorgPolicy::new(100)?,
        resources,
    )?;
    assert_cold_and_warm(&reader, initial_tip, initial_hash)?;

    let unchanged = reader.try_catch_up()?;
    assert_eq!(unchanged.before(), unchanged.after());
    let warm_calls = reader.rocksdb_ticker_count(Ticker::NumberMultigetCalls);
    let _ = reader.encoded_compact_block_range_up_to(tip_request(initial_tip), None)?;
    assert_eq!(
        reader.rocksdb_ticker_count(Ticker::NumberMultigetCalls),
        warm_calls
    );

    writer.append_live_block(&chain)?;
    reader.try_catch_up()?;
    assert_cold_and_warm(&reader, appended_tip, appended_hash)?;

    let fork = chain.fork_at(appended_tip)?.extend_blocks(1);
    let replacement_hash = fork.block_at(appended_tip).ok_or("absent fork tip")?.hash;
    assert_ne!(replacement_hash, appended_hash);
    writer.replace_live_blocks(&fork, appended_tip)?;
    reader.try_catch_up()?;
    assert_cold_and_warm(&reader, appended_tip, replacement_hash)?;
    drop(reader);
    let reopened = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        writer.canonical_primary_path(),
        dir.path().join("reopened"),
        Network::ZcashRegtest,
        CanonicalStoreRequirements::default(),
        CanonicalReorgPolicy::new(100)?,
        resources,
    )?;
    assert_cold_and_warm(&reopened, appended_tip, replacement_hash)?;
    Ok(())
}
