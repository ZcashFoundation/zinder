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
    FULL_BLOCK_STREAM_CHANNEL_CAPACITY, WalletQueryApi, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
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
        Arc::new(canonical),
        Arc::new(wallet),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(serving_pair),
        (),
        ingest_control,
        activations,
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
