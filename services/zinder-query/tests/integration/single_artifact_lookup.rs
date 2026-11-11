#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use eyre::eyre;
use zinder_core::{BlockHeight, Network, TransactionId, TxStatus};
use zinder_query::{QueryError, WalletQueryApi};
use zinder_store::RawBlobRetention;
use zinder_testkit::{ChainFixture, FixtureTransactionRows};

use crate::common::serving_query_over;

const RAW_TRANSACTION_BYTES: &[u8] = b"raw-transaction-bytes";

fn chain_with_transaction(
    raw_blob_retention: RawBlobRetention,
    transaction_id: TransactionId,
) -> eyre::Result<ChainFixture> {
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(raw_blob_retention)
        .extend_blocks(1);
    let block = chain
        .block_at(BlockHeight::new(1))
        .cloned()
        .ok_or_else(|| eyre!("fixture must contain height 1"))?;
    Ok(
        chain.with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            transaction_id,
            block.height,
            block.hash,
            0,
            RAW_TRANSACTION_BYTES.to_vec(),
        )),
    )
}

#[tokio::test]
async fn compact_block_at_returns_indexed_block() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let block = chain
        .block_at(BlockHeight::new(1))
        .cloned()
        .ok_or_else(|| eyre!("fixture must contain height 1"))?;
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;
    let chain_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;

    let response = wallet_query
        .compact_block_at(BlockHeight::new(1), None)
        .await?;

    assert_eq!(response.chain_epoch, chain_epoch);
    assert_eq!(response.compact_block.height(), block.height);
    assert_eq!(response.compact_block.block_hash(), block.hash);
    assert_eq!(
        response.compact_block.previous_block_hash(),
        Network::ZcashRegtest.genesis_hash()
    );
    assert_eq!(response.compact_block.time(), block.block_time_seconds);

    Ok(())
}

#[tokio::test]
async fn compact_block_at_reports_unavailable_for_missing_height() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;

    let error = match wallet_query
        .compact_block_at(BlockHeight::new(99), None)
        .await
    {
        Ok(response) => return Err(eyre!("expected unavailable, got {response:?}")),
        Err(error) => error,
    };

    assert!(
        matches!(error, QueryError::ArtifactUnavailable { .. }),
        "unexpected missing-height error: {error:?}"
    );

    Ok(())
}

#[tokio::test]
async fn transaction_returns_indexed_transaction() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([0xAB; 32]);
    let chain = chain_with_transaction(RawBlobRetention::Transactions, transaction_id)?;
    let block = chain
        .block_at(BlockHeight::new(1))
        .cloned()
        .ok_or_else(|| eyre!("fixture must contain height 1"))?;
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;
    let chain_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;

    let response = wallet_query.transaction(transaction_id, None).await?;

    assert_eq!(response.chain_epoch, chain_epoch);
    let TxStatus::Mined(mined) = response.status else {
        return Err(eyre!("expected mined transaction status, got {response:?}"));
    };
    assert_eq!(mined.location.transaction_id, transaction_id);
    assert_eq!(mined.location.block_height, block.height);
    assert_eq!(mined.location.block_hash, block.hash);
    assert_eq!(mined.location.tx_index_in_block, 0);
    assert_eq!(
        mined.raw_transaction_bytes,
        Some(RAW_TRANSACTION_BYTES.to_vec()),
        "the mined arm must carry the serialized transaction bytes alongside the location",
    );
    assert!(
        mined.chain_context.confirmations >= 1,
        "a mined transaction at the visible tip has at least one confirmation",
    );

    Ok(())
}

#[tokio::test]
async fn transaction_omits_bytes_when_blob_is_not_retained() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([0xAB; 32]);
    let chain = chain_with_transaction(RawBlobRetention::None, transaction_id)?;
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;

    let response = wallet_query.transaction(transaction_id, None).await?;

    let TxStatus::Mined(mined) = response.status else {
        return Err(eyre!("expected mined transaction status, got {response:?}"));
    };
    assert_eq!(mined.location.transaction_id, transaction_id);
    assert!(
        mined.raw_transaction_bytes.is_none(),
        "raw bytes are absent when the deployment does not retain transaction blobs",
    );

    Ok(())
}

#[tokio::test]
async fn transaction_reports_unavailable_for_unknown_id() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;
    let chain_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;

    let response = wallet_query
        .transaction(TransactionId::from_bytes([0xCD; 32]), None)
        .await?;

    assert_eq!(response.chain_epoch, chain_epoch);
    assert!(
        matches!(response.status, TxStatus::NotFound),
        "expected NotFound status, got {:?}",
        response.status
    );

    Ok(())
}
