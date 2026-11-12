#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use eyre::eyre;
use zinder_core::{
    BlockHeight, BlockId, BroadcastAccepted, BroadcastDuplicate, BroadcastInvalidEncoding,
    BroadcastQueued, BroadcastRejected, BroadcastRejectionReason, MAX_RAW_TRANSACTION_BYTES,
    Network, RawTransactionBytes, TransactionBroadcastOutcome, TransactionId,
};
use zinder_proto::capabilities::WALLET_BROADCAST_TRANSACTION_V1;
use zinder_query::{QueryError, WalletQueryApi, WalletServingQuery};
use zinder_source::SourceError;
use zinder_testkit::{ChainFixture, MockTransactionBroadcaster, WalletServingStoreFixture};

use crate::common::{ProbedNode, probed_serving_query_over, serving_query_over};

/// Builds the release serving query whose probed node admits broadcast and
/// forwards it to `broadcaster`.
fn broadcasting_query(
    broadcaster: MockTransactionBroadcaster,
) -> eyre::Result<(WalletServingQuery<ProbedNode>, WalletServingStoreFixture)> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let tip = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("fixture must contain height 1"))?;
    let node = ProbedNode::with_broadcaster(BlockId::new(tip.height, tip.hash), broadcaster)?;
    probed_serving_query_over(&chain, node)
}

#[tokio::test]
async fn broadcast_transaction_returns_accepted_result() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([7; 32]);
    let mock_broadcaster = MockTransactionBroadcaster::accepted(transaction_id);
    let (wallet_query, _store_fixture) = broadcasting_query(mock_broadcaster.clone())?;

    let broadcast_outcome = wallet_query
        .broadcast_transaction(RawTransactionBytes::new([0x00, 0x01]))
        .await?;

    assert_eq!(
        broadcast_outcome,
        TransactionBroadcastOutcome::Accepted(BroadcastAccepted { transaction_id })
    );
    assert_eq!(
        mock_broadcaster.captured_calls(),
        vec![RawTransactionBytes::new([0x00, 0x01])]
    );

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_preserves_rejection_classes() -> eyre::Result<()> {
    let cases = [
        TransactionBroadcastOutcome::InvalidEncoding(BroadcastInvalidEncoding {
            error_code: Some(-22),
            message: "TX decode failed".to_owned(),
        }),
        TransactionBroadcastOutcome::Duplicate(BroadcastDuplicate {
            error_code: Some(-27),
            message: "transaction already in mempool".to_owned(),
        }),
        TransactionBroadcastOutcome::Queued(BroadcastQueued {
            message: "already queued for download".to_owned(),
        }),
        TransactionBroadcastOutcome::Rejected(BroadcastRejected {
            kind: BroadcastRejectionReason::InvalidSignature,
            error_code: Some(-25),
            message: "transaction signature is invalid".to_owned(),
        }),
        TransactionBroadcastOutcome::Rejected(BroadcastRejected {
            kind: BroadcastRejectionReason::UnsupportedTransactionVersion,
            error_code: Some(-25),
            message: "transaction version 4 not supported by the network upgrade Nu7".to_owned(),
        }),
        TransactionBroadcastOutcome::Rejected(BroadcastRejected {
            kind: BroadcastRejectionReason::ExceedsBlockActionLimit,
            error_code: Some(-25),
            message: "mempool transaction has 331 Orchard actions, exceeding the next block's ZIP 218 limit of 330".to_owned(),
        }),
        TransactionBroadcastOutcome::Rejected(BroadcastRejected {
            kind: BroadcastRejectionReason::Unknown,
            error_code: Some(-25),
            message: "bad-txns-invalid".to_owned(),
        }),
    ];

    for expected_broadcast_outcome in cases {
        let (wallet_query, _store_fixture) = broadcasting_query(
            MockTransactionBroadcaster::returning(expected_broadcast_outcome.clone()),
        )?;

        let broadcast_outcome = wallet_query
            .broadcast_transaction(RawTransactionBytes::new([0x02]))
            .await?;

        assert_eq!(broadcast_outcome, expected_broadcast_outcome);
    }

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_rejects_oversized_payload_before_upstream_call() -> eyre::Result<()>
{
    let mock_broadcaster = MockTransactionBroadcaster::accepted(TransactionId::from_bytes([1; 32]));
    let (wallet_query, _store_fixture) = broadcasting_query(mock_broadcaster.clone())?;
    let oversized = RawTransactionBytes::new(vec![0u8; MAX_RAW_TRANSACTION_BYTES + 1]);

    let error = match wallet_query.broadcast_transaction(oversized).await {
        Ok(broadcast_outcome) => {
            return Err(eyre!("expected size error, got {broadcast_outcome:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        QueryError::BroadcastTransactionTooLarge { actual, maximum }
            if actual == MAX_RAW_TRANSACTION_BYTES + 1 && maximum == MAX_RAW_TRANSACTION_BYTES
    ));
    assert!(
        mock_broadcaster.captured_calls().is_empty(),
        "oversized payload must be rejected before reaching the broadcaster"
    );

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_admits_payload_at_size_bound() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([6; 32]);
    let mock_broadcaster = MockTransactionBroadcaster::accepted(transaction_id);
    let (wallet_query, _store_fixture) = broadcasting_query(mock_broadcaster.clone())?;
    let at_bound = RawTransactionBytes::new(vec![0u8; MAX_RAW_TRANSACTION_BYTES]);

    let broadcast_outcome = wallet_query.broadcast_transaction(at_bound.clone()).await?;

    assert_eq!(
        broadcast_outcome,
        TransactionBroadcastOutcome::Accepted(BroadcastAccepted { transaction_id })
    );
    assert_eq!(mock_broadcaster.captured_calls(), vec![at_bound]);

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_maps_node_unavailability_to_query_error() -> eyre::Result<()> {
    let (wallet_query, _store_fixture) = broadcasting_query(
        MockTransactionBroadcaster::node_unavailable("node is offline"),
    )?;

    let error = match wallet_query
        .broadcast_transaction(RawTransactionBytes::new([0x03]))
        .await
    {
        Ok(broadcast_outcome) => {
            return Err(eyre!("expected node error, got {broadcast_outcome:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        QueryError::Node(SourceError::NodeUnavailable { ref reason })
            if reason == "node is offline"
    ));

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_does_not_mutate_chain_epoch() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([9; 32]);
    let (wallet_query, _store_fixture) =
        broadcasting_query(MockTransactionBroadcaster::accepted(transaction_id))?;
    let chain_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;

    let broadcast_outcome = wallet_query
        .broadcast_transaction(RawTransactionBytes::new([0x04]))
        .await?;

    assert_eq!(
        broadcast_outcome,
        TransactionBroadcastOutcome::Accepted(BroadcastAccepted { transaction_id })
    );
    assert_eq!(
        wallet_query.visible_tip_block(None).await?.chain_epoch,
        chain_epoch
    );
    assert!(matches!(
        wallet_query.transaction(transaction_id, None).await?.status,
        zinder_core::TxStatus::NotFound
    ));

    Ok(())
}

#[tokio::test]
async fn read_only_wallet_query_reports_broadcast_disabled() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;

    let error = match wallet_query
        .broadcast_transaction(RawTransactionBytes::new([0x05]))
        .await
    {
        Ok(broadcast_outcome) => {
            return Err(eyre!("expected disabled error, got {broadcast_outcome:?}"));
        }
        Err(error) => error,
    };

    assert!(
        matches!(
            error,
            QueryError::EndpointCapabilityUnavailable {
                capability: WALLET_BROADCAST_TRANSACTION_V1
            }
        ),
        "unexpected read-only broadcast error: {error:?}"
    );

    Ok(())
}
