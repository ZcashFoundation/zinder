#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

//! Performance regression-only smoke tests for the wallet query plane.
//!
//! These tests guarantee that a representative range read completes within
//! a generous CI budget. They are not benchmarks; a dedicated benchmark suite
//! should measure P50/P99 numbers, while this file catches catastrophic
//! regressions on every CI run. Budgets are deliberately loose so this test
//! stays green under contended CI workers; live percentile measurements follow
//! the testing runbook instead of becoming static assertions here.

use std::sync::Arc;
use std::time::{Duration, Instant};

use zinder_core::{BlockHeight, BlockHeightRange, Network};
use zinder_query::{
    FULL_BLOCK_STREAM_CHANNEL_CAPACITY, RocksDbCanonicalReader, RocksDbWalletReader,
    WalletQueryApi, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_store::RawBlobRetention;
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

use crate::common::serving_query_over;

const PERF_SMOKE_BLOCK_COUNT: u32 = 1_000;
const PERF_SMOKE_RANGE_BUDGET: Duration = Duration::from_secs(2);
const PERF_SMOKE_FULL_BLOCK_RANGE_BUDGET: Duration = Duration::from_secs(5);
const PERF_SMOKE_LATEST_BUDGET: Duration = Duration::from_millis(250);

/// Upper bound the demand-driven full-block channel depth must respect. Far
/// below [`PERF_SMOKE_BLOCK_COUNT`] so the assertion proves peak buffering
/// tracks one sub-read, never the requested window.
const FULL_BLOCK_STREAM_MAX_BUFFERED_CHUNKS: usize = 16;

#[tokio::test(flavor = "multi_thread")]
async fn compact_block_range_one_thousand_blocks_stays_under_budget() -> eyre::Result<()> {
    let chain_fixture =
        ChainFixture::new(Network::ZcashRegtest).extend_blocks(PERF_SMOKE_BLOCK_COUNT);
    let (wallet_query, _store_fixture) = serving_query_over(&chain_fixture)?;

    let start = Instant::now();
    let response = wallet_query
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(
                BlockHeight::new(1),
                BlockHeight::new(PERF_SMOKE_BLOCK_COUNT),
            ),
            None,
        )
        .await?;
    let elapsed = start.elapsed();

    assert_eq!(
        response.compact_blocks.len(),
        usize::try_from(PERF_SMOKE_BLOCK_COUNT)?
    );
    assert!(
        elapsed <= PERF_SMOKE_RANGE_BUDGET,
        "compact_block_range over {PERF_SMOKE_BLOCK_COUNT} blocks took {elapsed:?}, budget is {PERF_SMOKE_RANGE_BUDGET:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn full_block_range_one_thousand_blocks_stays_under_budget() -> eyre::Result<()> {
    const {
        assert!(
            FULL_BLOCK_STREAM_CHANNEL_CAPACITY <= FULL_BLOCK_STREAM_MAX_BUFFERED_CHUNKS,
            "full-block stream must buffer far fewer chunks than the requested window so peak \
             memory tracks one sub-read, not the block count"
        );
    }

    let chain_fixture = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(PERF_SMOKE_BLOCK_COUNT);
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain_fixture, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(serving_pair),
        ingest_control,
    );

    let start = Instant::now();
    let mut stream = wallet_query
        .full_blocks_in_range(
            BlockHeightRange::inclusive(
                BlockHeight::new(1),
                BlockHeight::new(PERF_SMOKE_BLOCK_COUNT),
            ),
            None,
        )
        .await?;
    let mut delivered: u32 = 0;
    while let Some(block) = stream.blocks.recv().await {
        block?;
        delivered = delivered.saturating_add(1);
    }
    let elapsed = start.elapsed();

    assert_eq!(delivered, PERF_SMOKE_BLOCK_COUNT);
    assert!(
        elapsed <= PERF_SMOKE_FULL_BLOCK_RANGE_BUDGET,
        "full_block_range over {PERF_SMOKE_BLOCK_COUNT} blocks took {elapsed:?}, budget is {PERF_SMOKE_FULL_BLOCK_RANGE_BUDGET:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn visible_tip_block_stays_under_budget() -> eyre::Result<()> {
    let (wallet_query, _store_fixture) =
        serving_query_over(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(1))?;

    let start = Instant::now();
    let response = wallet_query.visible_tip_block(None).await?;
    let elapsed = start.elapsed();

    assert_eq!(response.height, BlockHeight::new(1));
    assert!(
        elapsed <= PERF_SMOKE_LATEST_BUDGET,
        "visible_tip_block read took {elapsed:?}, budget is {PERF_SMOKE_LATEST_BUDGET:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixteen_concurrent_cached_hex_tree_state_tasks_stay_under_budget() -> eyre::Result<()> {
    use zinder_testkit::{CanonicalBaseline, CanonicalWriterFixture, with_note_commitments};

    const TASKS: usize = 16;
    const ITERATIONS_PER_TASK: usize = 500;
    const CONCURRENT_HEX_BUDGET: Duration = Duration::from_secs(3);

    let activations = sample_regtest_upgrade_activations();
    let chain = with_note_commitments(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(130),
        &activations,
    )?;
    let baseline = CanonicalBaseline::new(&chain)
        .through(BlockHeight::new(130))
        .with_settled_tip(BlockHeight::new(105));
    let mut store_fixture =
        CanonicalWriterFixture::publish(&baseline, &activations)?.into_serving_store_fixture()?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?);
    let slot = WalletServingPairSlot::new(pair);
    let wallet_query = WalletServingQuery::from_serving_pair_slot(slot);

    let settled_checkpoint = BlockHeight::new(100);
    let settled_replayed = BlockHeight::new(57);
    let visible_tip = BlockHeight::new(130);
    let expected_latest = wallet_query.latest_tree_state_checkpoint_hex(None).await?;
    let expected_settled_checkpoint = wallet_query
        .tree_state_hex_at(settled_checkpoint, None)
        .await?;
    let expected_settled_replayed = wallet_query
        .tree_state_hex_at(settled_replayed, None)
        .await?;
    let expected_tip = wallet_query.tree_state_hex_at(visible_tip, None).await?;
    assert_eq!(expected_latest, expected_tip);

    let start = Instant::now();
    let mut handles = Vec::with_capacity(TASKS);
    for task_index in 0..TASKS {
        let query = wallet_query.clone();
        let expected_latest = expected_latest.clone();
        let expected_settled_checkpoint = expected_settled_checkpoint.clone();
        let expected_settled_replayed = expected_settled_replayed.clone();
        let expected_tip = expected_tip.clone();
        handles.push(tokio::spawn(async move {
            for step in 0..ITERATIONS_PER_TASK {
                match (task_index + step) & 3 {
                    0 => {
                        let latest = query.latest_tree_state_checkpoint_hex(None).await?;
                        assert_eq!(latest, expected_latest);
                    }
                    1 => {
                        let tip = query.tree_state_hex_at(visible_tip, None).await?;
                        assert_eq!(tip, expected_tip);
                    }
                    2 => {
                        let checkpoint = query.tree_state_hex_at(settled_checkpoint, None).await?;
                        assert_eq!(checkpoint, expected_settled_checkpoint);
                    }
                    _ => {
                        let replayed = query.tree_state_hex_at(settled_replayed, None).await?;
                        assert_eq!(replayed, expected_settled_replayed);
                    }
                }
            }
            Ok::<(), eyre::Report>(())
        }));
    }
    for handle in handles {
        handle.await??;
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed <= CONCURRENT_HEX_BUDGET,
        "16 concurrent tasks x {ITERATIONS_PER_TASK} warm hex tree-state reads took {elapsed:?}, budget is {CONCURRENT_HEX_BUDGET:?}"
    );

    Ok(())
}
