#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use eyre::eyre;
use std::sync::Arc;
use zinder_core::{
    BlockHeight, ChainEpoch, ChainEpochId, ChainTipMetadata, Network, TransactionId, TxStatus,
    UnixTimestampMillis,
};
use zinder_query::{QueryError, WalletQuery, WalletQueryApi};
use zinder_store::{
    CURRENT_ARTIFACT_SCHEMA_VERSION, ChainEpochArtifacts, ChainStoreOptions, RawBlobRetention,
};
use zinder_testkit::{
    FixtureTransactionRows, StoreFixture, encode_fixture_block_replay,
    sample_regtest_upgrade_activations,
};

use crate::common::{block_hash_from_seed, synthetic_chain_epoch};

#[tokio::test]
async fn compact_block_at_returns_indexed_block() -> eyre::Result<()> {
    let store_fixture = StoreFixture::open()?;
    let store = store_fixture.chain_store().clone();
    let (chain_epoch, block, compact_block) = synthetic_chain_epoch(1, 1);
    let replay = encode_fixture_block_replay(&block, &[]);

    store.commit_chain_epoch(ChainEpochArtifacts::new(
        chain_epoch,
        vec![block],
        vec![replay],
        vec![compact_block.clone()],
    ))?;

    let wallet_query = WalletQuery::new(store, (), Arc::new(sample_regtest_upgrade_activations()));
    let response = wallet_query
        .compact_block_at(BlockHeight::new(1), None)
        .await?;

    assert_eq!(response.chain_epoch, chain_epoch);
    assert_eq!(response.compact_block, compact_block);

    Ok(())
}

#[tokio::test]
async fn compact_block_at_reports_unavailable_for_missing_height() -> eyre::Result<()> {
    let store_fixture = StoreFixture::open()?;
    let store = store_fixture.chain_store().clone();
    let (chain_epoch, block, compact_block) = synthetic_chain_epoch(1, 1);
    let replay = encode_fixture_block_replay(&block, &[]);

    store.commit_chain_epoch(ChainEpochArtifacts::new(
        chain_epoch,
        vec![block],
        vec![replay],
        vec![compact_block],
    ))?;

    let wallet_query = WalletQuery::new(store, (), Arc::new(sample_regtest_upgrade_activations()));
    let error = match wallet_query
        .compact_block_at(BlockHeight::new(99), None)
        .await
    {
        Ok(response) => return Err(eyre!("expected unavailable, got {response:?}")),
        Err(error) => error,
    };

    assert!(matches!(error, QueryError::ArtifactUnavailable { .. }));

    Ok(())
}

#[tokio::test]
async fn compact_block_at_reports_unavailable_below_checkpoint() -> eyre::Result<()> {
    let store_fixture = StoreFixture::open()?;
    let store = store_fixture.chain_store().clone();
    let checkpoint_height = BlockHeight::new(1_000);
    let checkpoint_hash = block_hash_from_seed(1_000);
    let checkpoint_epoch = ChainEpoch {
        id: ChainEpochId::new(1),
        network: Network::ZcashRegtest,
        visible_tip_height: checkpoint_height,
        visible_tip_hash: checkpoint_hash,
        settled_tip_height: checkpoint_height,
        settled_tip_hash: checkpoint_hash,
        artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
        tip_metadata: ChainTipMetadata::new(130_002, 39_758, 0),
        created_at: UnixTimestampMillis::new(1_774_668_000_000),
    };

    store.commit_artifactless_checkpoint(checkpoint_epoch)?;

    let wallet_query = WalletQuery::new(store, (), Arc::new(sample_regtest_upgrade_activations()));
    let error = match wallet_query.compact_block_at(checkpoint_height, None).await {
        Ok(response) => return Err(eyre!("expected unavailable, got {response:?}")),
        Err(error) => error,
    };

    assert!(matches!(error, QueryError::ArtifactUnavailable { .. }));

    Ok(())
}

#[tokio::test]
async fn transaction_returns_indexed_transaction() -> eyre::Result<()> {
    let store_fixture = StoreFixture::open_with_options(ChainStoreOptions {
        raw_blob_retention: RawBlobRetention::Transactions,
        ..ChainStoreOptions::for_local_tests()
    })?;
    let store = store_fixture.chain_store().clone();
    let (chain_epoch, block, compact_block) = synthetic_chain_epoch(1, 1);
    let transaction_id = TransactionId::from_bytes([0xAB; 32]);
    let transaction_rows = FixtureTransactionRows::from_raw_transaction(
        transaction_id,
        block.height,
        block.block_hash,
        0,
        b"raw-transaction-bytes".to_vec(),
    );
    let transaction_location = transaction_rows.location;
    let transaction_intrinsic_value_balances = transaction_rows
        .intrinsic_value_balances_artifact()
        .into_iter()
        .collect();
    let replay = encode_fixture_block_replay(&block, std::slice::from_ref(&transaction_rows));

    store.commit_chain_epoch(
        ChainEpochArtifacts::new(chain_epoch, vec![block], vec![replay], vec![compact_block])
            .with_block_transaction_index(vec![transaction_rows.block_transaction_index])
            .with_transaction_locations(vec![transaction_rows.location])
            .with_transaction_facts(vec![transaction_rows.facts])
            .with_transaction_intrinsic_value_balances(transaction_intrinsic_value_balances)
            .with_transaction_blobs(transaction_rows.blob.into_iter().collect()),
    )?;

    let wallet_query = WalletQuery::new(store, (), Arc::new(sample_regtest_upgrade_activations()));
    let response = wallet_query.transaction(transaction_id, None).await?;

    assert_eq!(response.chain_epoch, chain_epoch);
    let TxStatus::Mined(mined) = response.status else {
        return Err(eyre!("expected mined transaction status, got {response:?}"));
    };
    assert_eq!(
        mined.location.transaction_id,
        transaction_location.transaction_id
    );
    assert_eq!(
        mined.location.block_height,
        transaction_location.block_height
    );
    assert_eq!(mined.location.block_hash, transaction_location.block_hash);
    assert_eq!(mined.location.tx_index_in_block, 0);
    assert_eq!(
        mined.raw_transaction_bytes,
        Some(b"raw-transaction-bytes".to_vec()),
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
    let store_fixture = StoreFixture::open()?;
    let store = store_fixture.chain_store().clone();
    let (chain_epoch, block, compact_block) = synthetic_chain_epoch(1, 1);
    let transaction_id = TransactionId::from_bytes([0xAB; 32]);
    let transaction_rows = FixtureTransactionRows::from_raw_transaction(
        transaction_id,
        block.height,
        block.block_hash,
        0,
        b"raw-transaction-bytes".to_vec(),
    );
    let replay_transaction_rows = FixtureTransactionRows {
        blob: None,
        ..transaction_rows.clone()
    };
    let replay =
        encode_fixture_block_replay(&block, std::slice::from_ref(&replay_transaction_rows));
    let transaction_intrinsic_value_balances = transaction_rows
        .intrinsic_value_balances_artifact()
        .into_iter()
        .collect();

    store.commit_chain_epoch(
        ChainEpochArtifacts::new(chain_epoch, vec![block], vec![replay], vec![compact_block])
            .with_block_transaction_index(vec![transaction_rows.block_transaction_index])
            .with_transaction_locations(vec![transaction_rows.location])
            .with_transaction_facts(vec![transaction_rows.facts])
            .with_transaction_intrinsic_value_balances(transaction_intrinsic_value_balances),
    )?;

    let wallet_query = WalletQuery::new(store, (), Arc::new(sample_regtest_upgrade_activations()));
    let response = wallet_query.transaction(transaction_id, None).await?;

    let TxStatus::Mined(mined) = response.status else {
        return Err(eyre!("expected mined transaction status, got {response:?}"));
    };
    assert!(
        mined.raw_transaction_bytes.is_none(),
        "raw bytes are absent when the deployment does not retain transaction blobs",
    );

    Ok(())
}

#[tokio::test]
async fn transaction_reports_unavailable_for_unknown_id() -> eyre::Result<()> {
    let store_fixture = StoreFixture::open()?;
    let store = store_fixture.chain_store().clone();
    let (chain_epoch, block, compact_block) = synthetic_chain_epoch(1, 1);
    let replay = encode_fixture_block_replay(&block, &[]);

    store.commit_chain_epoch(ChainEpochArtifacts::new(
        chain_epoch,
        vec![block],
        vec![replay],
        vec![compact_block],
    ))?;

    let wallet_query = WalletQuery::new(store, (), Arc::new(sample_regtest_upgrade_activations()));
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

#[tokio::test]
async fn transaction_miss_below_checkpoint_reports_history_unavailable() -> eyre::Result<()> {
    let store_fixture = StoreFixture::open()?;
    let store = store_fixture.chain_store().clone();
    let checkpoint_height = BlockHeight::new(1_000);
    let checkpoint_hash = block_hash_from_seed(1_000);
    store.commit_artifactless_checkpoint(ChainEpoch {
        id: ChainEpochId::new(1),
        network: Network::ZcashRegtest,
        visible_tip_height: checkpoint_height,
        visible_tip_hash: checkpoint_hash,
        settled_tip_height: checkpoint_height,
        settled_tip_hash: checkpoint_hash,
        artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
        tip_metadata: ChainTipMetadata::new(130_002, 39_758, 0),
        created_at: UnixTimestampMillis::new(1_774_668_000_000),
    })?;

    let wallet_query = WalletQuery::new(store, (), Arc::new(sample_regtest_upgrade_activations()));
    for at_epoch_id in [None, Some(ChainEpochId::new(1))] {
        // The transaction may be mined at or below the checkpoint, which the
        // store does not retain, so a miss is not an absence.
        let outcome = wallet_query
            .transaction(TransactionId::from_bytes([0xCD; 32]), at_epoch_id)
            .await;
        assert!(
            matches!(
                outcome,
                Err(QueryError::CanonicalHistoryUnavailable {
                    first_available_height
                }) if first_available_height == BlockHeight::new(1_001)
            ),
            "expected the history floor, got {outcome:?}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn transaction_absence_at_a_pinned_epoch_names_that_epoch_tip() -> eyre::Result<()> {
    let store_fixture = StoreFixture::open()?;
    let store = store_fixture.chain_store().clone();
    let (first_epoch, first_block, first_compact_block) = synthetic_chain_epoch(1, 1);
    let first_replay = encode_fixture_block_replay(&first_block, &[]);
    store.commit_chain_epoch(ChainEpochArtifacts::new(
        first_epoch,
        vec![first_block],
        vec![first_replay],
        vec![first_compact_block],
    ))?;

    let (second_epoch, second_block, second_compact_block) = synthetic_chain_epoch(2, 2);
    let transaction_id = TransactionId::from_bytes([0xAB; 32]);
    let transaction_rows = FixtureTransactionRows::from_raw_transaction(
        transaction_id,
        second_block.height,
        second_block.block_hash,
        0,
        b"raw-transaction-bytes".to_vec(),
    );
    let replay_transaction_rows = FixtureTransactionRows {
        blob: None,
        ..transaction_rows.clone()
    };
    let second_replay = encode_fixture_block_replay(
        &second_block,
        std::slice::from_ref(&replay_transaction_rows),
    );
    let intrinsic_value_balances = transaction_rows
        .intrinsic_value_balances_artifact()
        .into_iter()
        .collect();
    store.commit_chain_epoch(
        ChainEpochArtifacts::new(
            second_epoch,
            vec![second_block],
            vec![second_replay],
            vec![second_compact_block],
        )
        .with_block_transaction_index(vec![transaction_rows.block_transaction_index])
        .with_transaction_locations(vec![transaction_rows.location])
        .with_transaction_facts(vec![transaction_rows.facts])
        .with_transaction_intrinsic_value_balances(intrinsic_value_balances),
    )?;

    let wallet_query = WalletQuery::new(store, (), Arc::new(sample_regtest_upgrade_activations()));
    // A replica pinned below the mining height answers an absence, but the
    // absence names an epoch whose tip is below the mining height, so the
    // caller can tell it does not cover that height.
    let lagging = wallet_query
        .transaction(transaction_id, Some(first_epoch.id))
        .await?;
    assert!(matches!(lagging.status, TxStatus::NotFound));
    assert_eq!(lagging.chain_epoch, first_epoch);
    assert!(lagging.chain_epoch.visible_tip_height < BlockHeight::new(2));

    let covering = wallet_query
        .transaction(transaction_id, Some(second_epoch.id))
        .await?;
    let TxStatus::Mined(mined) = covering.status else {
        return Err(eyre!(
            "expected mined status at the covering epoch, got {covering:?}"
        ));
    };
    assert_eq!(covering.chain_epoch, second_epoch);
    assert_eq!(mined.location.block_height, BlockHeight::new(2));

    let unknown_pin = wallet_query
        .transaction(transaction_id, Some(ChainEpochId::new(99)))
        .await;
    assert!(
        matches!(
            unknown_pin,
            Err(QueryError::ChainEpochPinUnavailable { .. })
        ),
        "an epoch the store does not hold must fail, got {unknown_pin:?}"
    );

    Ok(())
}
