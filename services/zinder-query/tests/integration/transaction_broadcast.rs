#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::time::Duration;

use eyre::eyre;
use serde_json::json;
use zinder_core::{
    BlockHash, BlockHeight, BroadcastAccepted, BroadcastDuplicate, BroadcastInvalidEncoding,
    BroadcastQueued, BroadcastRejected, BroadcastRejectionReason, MAX_RAW_TRANSACTION_BYTES,
    Network, NetworkUpgradeActivation, NetworkUpgradeActivations, RawTransactionBytes,
    TransactionBroadcastOutcome, TransactionId,
    wire::{encode_branch_id_hex, encode_rpc_block_hash_hex},
};
use zinder_proto::capabilities::WALLET_BROADCAST_TRANSACTION_V1;
use zinder_query::{OrderedBroadcaster, QueryError, WalletQueryApi, WalletServingQuery};
use zinder_source::{
    DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES, NodeAuth, NodeTarget, SourceError, ZebraJsonRpcSource,
};
use zinder_testkit::{
    ChainFixture, JsonRpcTestServer, MockTransactionBroadcaster, RpcReply,
    WalletServingStoreFixture, method, sample_regtest_upgrade_activations,
};

use crate::common::{
    NodeBroadcastQuery, serving_pair_slot, serving_query_over, verified_node_query_over,
};

/// Builds the release serving query whose verified node forwards broadcasts
/// to `broadcaster`.
async fn broadcasting_query(
    broadcaster: MockTransactionBroadcaster,
) -> eyre::Result<(NodeBroadcastQuery, WalletServingStoreFixture)> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    verified_node_query_over(&chain, broadcaster).await
}

/// A reader starts without contacting its node, so a broadcast that arrives
/// before the node's first-contact verification fails without sending the
/// transaction anywhere.
#[tokio::test]
async fn broadcast_before_node_verification_fails_without_sending() -> eyre::Result<()> {
    let broadcaster = MockTransactionBroadcaster::accepted(TransactionId::from_bytes([7; 32]))
        .with_node_activations(sample_regtest_upgrade_activations());
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let mut store_fixture =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let slot = serving_pair_slot(&mut store_fixture)?;
    let node = OrderedBroadcaster::new(broadcaster.clone(), []);
    let wallet_query = WalletServingQuery::from_serving_pair_slot(slot.clone())
        .with_node_broadcast(Some(node.clone()));

    assert!(
        wallet_query
            .native_endpoint_capabilities()
            .contains(WALLET_BROADCAST_TRANSACTION_V1)
    );
    assert!(wallet_query.upstream_node_capabilities().is_none());
    let error = wallet_query
        .broadcast_transaction(RawTransactionBytes::new([0x00, 0x01]))
        .await
        .err()
        .ok_or_else(|| eyre!("an unverified node must not receive a broadcast"))?;
    assert!(
        matches!(error, QueryError::Node(SourceError::NodeUnavailable { .. })),
        "unexpected unverified-node error: {error:?}"
    );
    assert!(broadcaster.captured_calls().is_empty());

    node.verify(&slot).await?;
    assert!(wallet_query.upstream_node_capabilities().is_some());
    wallet_query
        .broadcast_transaction(RawTransactionBytes::new([0x00, 0x01]))
        .await?;
    assert_eq!(broadcaster.call_count(), 1);
    Ok(())
}

/// A node whose activation table differs from the writer-admitted schedule
/// through the served tip is on another network, so verification refuses it
/// and it never receives a transaction.
#[tokio::test]
async fn node_on_another_chain_fails_verification_and_receives_nothing() -> eyre::Result<()> {
    let mainnet_shaped = NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        sample_regtest_upgrade_activations()
            .activations()
            .iter()
            .map(|activation| NetworkUpgradeActivation {
                activation_height: BlockHeight::new(419_200),
                ..activation.clone()
            })
            .collect(),
    )?;
    let broadcaster = MockTransactionBroadcaster::accepted(TransactionId::from_bytes([7; 32]))
        .with_node_activations(mainnet_shaped);
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let mut store_fixture =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let slot = serving_pair_slot(&mut store_fixture)?;
    let node = OrderedBroadcaster::new(broadcaster.clone(), []);
    let wallet_query = WalletServingQuery::from_serving_pair_slot(slot.clone())
        .with_node_broadcast(Some(node.clone()));

    let refusal = node
        .verify(&slot)
        .await
        .err()
        .ok_or_else(|| eyre!("a node on another chain must fail verification"))?;
    assert!(
        matches!(refusal, SourceError::SourceProtocolMismatch { .. }),
        "unexpected verification refusal: {refusal:?}"
    );
    assert!(
        wallet_query
            .broadcast_transaction(RawTransactionBytes::new([0x00, 0x01]))
            .await
            .is_err()
    );
    assert!(broadcaster.captured_calls().is_empty());
    Ok(())
}

/// A node with the writer-admitted schedule whose best chain holds another
/// block at the served tip follows another chain, so verification refuses it
/// and it never receives a transaction.
#[tokio::test]
async fn node_with_the_same_schedule_on_another_chain_fails_verification() -> eyre::Result<()> {
    let broadcaster = MockTransactionBroadcaster::accepted(TransactionId::from_bytes([7; 32]))
        .with_node_activations(sample_regtest_upgrade_activations())
        .on_another_chain();
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let mut store_fixture =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let slot = serving_pair_slot(&mut store_fixture)?;
    let node = OrderedBroadcaster::new(broadcaster.clone(), []);
    let wallet_query = WalletServingQuery::from_serving_pair_slot(slot.clone())
        .with_node_broadcast(Some(node.clone()));

    let refusal = node
        .verify(&slot)
        .await
        .err()
        .ok_or_else(|| eyre!("a node on another chain must fail verification"))?;
    assert!(
        matches!(refusal, SourceError::SourceProtocolMismatch { .. }),
        "unexpected verification refusal: {refusal:?}"
    );
    assert!(wallet_query.upstream_node_capabilities().is_none());
    assert!(
        wallet_query
            .broadcast_transaction(RawTransactionBytes::new([0x00, 0x01]))
            .await
            .is_err()
    );
    assert!(broadcaster.captured_calls().is_empty());
    Ok(())
}

/// The sample regtest upgrades with one upgrade per height, so a node table
/// compares equal whatever order its `upgrades` object lists them in.
fn one_upgrade_per_height() -> eyre::Result<NetworkUpgradeActivations> {
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        sample_regtest_upgrade_activations()
            .activations()
            .iter()
            .zip(1..)
            .map(|(activation, height)| NetworkUpgradeActivation {
                activation_height: BlockHeight::new(height),
                ..activation.clone()
            })
            .collect(),
    )?)
}

/// Starts a node double that passes first-contact verification once for a
/// pair serving `schedule` with `tip_hash` at its tip, then accepts one
/// broadcast.
fn verifiable_node(
    schedule: &NetworkUpgradeActivations,
    tip_hash: BlockHash,
) -> eyre::Result<JsonRpcTestServer> {
    let upgrades: serde_json::Map<String, serde_json::Value> = schedule
        .activations()
        .iter()
        .map(|activation| {
            (
                encode_branch_id_hex(activation.branch_id),
                json!({
                    "name": activation.name,
                    "activationheight": activation.activation_height.value(),
                    "status": "active",
                }),
            )
        })
        .collect();
    JsonRpcTestServer::start([
        method("rpc.discover").reply(RpcReply::result(json!({
            "openrpc": "1.3.2",
            "methods": [{"name": "rpc.discover"}, {"name": "sendrawtransaction"}],
        }))),
        method("getblockchaininfo").reply(RpcReply::result(json!({ "upgrades": upgrades }))),
        method("getblockhash").reply(RpcReply::result(json!(encode_rpc_block_hash_hex(tip_hash)))),
        method("sendrawtransaction").reply(RpcReply::result(json!(
            "1111111111111111111111111111111111111111111111111111111111111111"
        ))),
    ])
}

/// A reader with cookie authentication can start before its node writes the
/// cookie file.
///
/// Verification and broadcast fail as node-unavailable without contacting
/// the node until the file appears. Verification then passes, and broadcast
/// reaches the node with the cookie credentials.
#[tokio::test]
async fn node_cookie_written_after_startup_is_verified_once_it_appears() -> eyre::Result<()> {
    const AUTHORIZATION: &str = "Basic X19jb29raWVfXzpub2RlLXN0YXJ0ZWQ=";
    let schedule = one_upgrade_per_height()?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let tip_hash = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("fixture must contain height 1"))?
        .hash;
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &schedule)?;
    let slot = serving_pair_slot(&mut store_fixture)?;
    let server = verifiable_node(&schedule, tip_hash)?;
    let cookie_dir = tempfile::tempdir()?;
    let cookie_path = cookie_dir.path().join(".cookie");
    let node = OrderedBroadcaster::new(
        ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            server.url(),
            NodeAuth::cookie_file(&cookie_path),
            Duration::from_secs(5),
        )?,
        [],
    );
    let wallet_query = WalletServingQuery::from_serving_pair_slot(slot.clone())
        .with_node_broadcast(Some(node.clone()));

    let refused_broadcast = wallet_query
        .broadcast_transaction(RawTransactionBytes::new([0x00, 0x01]))
        .await
        .err();
    let refused_verification = node.verify(&slot).await.err();
    assert!(
        matches!(
            refused_broadcast,
            Some(QueryError::Node(SourceError::NodeUnavailable { .. }))
        ),
        "unexpected broadcast outcome before the cookie exists: {refused_broadcast:?}"
    );
    assert!(
        matches!(&refused_verification, Some(SourceError::NodeUnavailable { reason }) if reason.contains("cookie")),
        "unexpected verification outcome before the cookie exists: {refused_verification:?}"
    );
    assert!(server.requests()?.is_empty());

    std::fs::write(&cookie_path, "__cookie__:node-started")?;
    node.verify(&slot).await?;
    let broadcast_outcome = wallet_query
        .broadcast_transaction(RawTransactionBytes::new([0x00, 0x01]))
        .await?;

    assert_eq!(
        broadcast_outcome,
        TransactionBroadcastOutcome::Accepted(BroadcastAccepted {
            transaction_id: TransactionId::from_bytes([0x11; 32]),
        })
    );
    let requests = server.requests()?;
    assert_eq!(requests.len(), 4, "{requests:?}");
    assert!(
        requests
            .iter()
            .all(|request| request.authorization.as_deref() == Some(AUTHORIZATION)),
        "{requests:?}"
    );
    Ok(())
}

/// A fallback endpoint's node can write its cookie file after the reader
/// starts.
///
/// Building the broadcaster from `[node]` and `[node_fallbacks.N]` does not
/// fail, `[node]` passes verification on its own, and the fallback receives
/// no request until its cookie file appears. The next verification round
/// then reaches the fallback with the cookie credentials.
#[tokio::test]
async fn fallback_cookie_written_after_startup_is_verified_once_it_appears() -> eyre::Result<()> {
    const AUTHORIZATION: &str = "Basic X19jb29raWVfXzpmYWxsYmFjay1zdGFydGVk";
    let schedule = one_upgrade_per_height()?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let tip_hash = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("fixture must contain height 1"))?
        .hash;
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &schedule)?;
    let slot = serving_pair_slot(&mut store_fixture)?;
    let primary_server = verifiable_node(&schedule, tip_hash)?;
    let fallback_server = verifiable_node(&schedule, tip_hash)?;
    let cookie_dir = tempfile::tempdir()?;
    let cookie_path = cookie_dir.path().join(".cookie");
    let target = |server: &JsonRpcTestServer, node_auth| {
        NodeTarget::new(
            Network::ZcashRegtest,
            server.url(),
            node_auth,
            Duration::from_secs(5),
            DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES,
        )
    };
    let nodes = OrderedBroadcaster::connect(
        &target(&primary_server, NodeAuth::None),
        &[target(
            &fallback_server,
            NodeAuth::cookie_file(&cookie_path),
        )],
    )?;

    nodes.verify(&slot).await?;
    assert!(nodes.verified_capabilities().is_some());
    assert!(fallback_server.requests()?.is_empty());

    std::fs::write(&cookie_path, "__cookie__:fallback-started")?;
    nodes.verify(&slot).await?;

    let fallback_requests = fallback_server.requests()?;
    let fallback_methods: Vec<&str> = fallback_requests
        .iter()
        .map(|request| request.method.as_str())
        .collect();
    assert_eq!(
        fallback_methods,
        ["rpc.discover", "getblockchaininfo", "getblockhash"]
    );
    assert!(
        fallback_requests
            .iter()
            .all(|request| request.authorization.as_deref() == Some(AUTHORIZATION)),
        "{fallback_requests:?}"
    );
    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_returns_accepted_result() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([7; 32]);
    let mock_broadcaster = MockTransactionBroadcaster::accepted(transaction_id);
    let (wallet_query, _store_fixture) = broadcasting_query(mock_broadcaster.clone()).await?;

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
        )
        .await?;

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
    let (wallet_query, _store_fixture) = broadcasting_query(mock_broadcaster.clone()).await?;
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
    let (wallet_query, _store_fixture) = broadcasting_query(mock_broadcaster.clone()).await?;
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
    )
    .await?;

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
        broadcasting_query(MockTransactionBroadcaster::accepted(transaction_id)).await?;
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
