#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::time::Duration;

use eyre::eyre;
use serde_json::{Value, json};
use zebra_chain::parameters::NetworkKind as ZebraNetworkKind;
use zebra_chain::transparent::Address as ZebraTransparentAddress;
use zinder_core::{BlockHeight, Network, TransactionId, TransparentOutPoint};
use zinder_source::{
    NodeAuth, SourceError, SourceFailureClass, SourceTransparentAddressBalance,
    TransparentAddressLookup, TransparentAddressRowLimit, ZebraJsonRpcSource,
};
use zinder_testkit::{JsonRpcTestServer, RpcReply, method};

/// Zakura's refusal from a pruned node that stopped maintaining its
/// transparent address indexes (`zakura_state::TransparentIndexesUnavailable`).
const ZAKURA_TRANSPARENT_INDEXES_REFUSAL: &str = "transparent address and spender indexes are \
     unavailable: this node's state database stopped maintaining them in pruned storage mode \
     with checkpoint sync. getaddressbalance, getaddressutxos, getaddresstxids, and transparent \
     spending transaction lookups need an archive node, or a pruned node that synced into a \
     fresh cache directory with consensus.checkpoint_sync = false";

fn regtest_source(url: impl Into<String>) -> Result<ZebraJsonRpcSource, SourceError> {
    ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        url,
        NodeAuth::None,
        Duration::from_secs(5),
    )
}

fn testnet_address(hash_byte: u8) -> ZebraTransparentAddress {
    ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Testnet, [hash_byte; 20])
}

fn regtest_lookup() -> eyre::Result<TransparentAddressLookup> {
    Ok(TransparentAddressLookup::new(
        Network::ZcashRegtest,
        [testnet_address(2), testnet_address(1)],
    )?)
}

fn sorted_addresses() -> Vec<String> {
    let mut addresses = vec![
        testnet_address(1).to_string(),
        testnet_address(2).to_string(),
    ];
    addresses.sort();
    addresses
}

fn tip_reply(height: u32) -> RpcReply {
    RpcReply::result(json!({ "height": height, "hash": vec![0_u8; 32] }))
}

fn transaction_id(byte: u8) -> TransactionId {
    TransactionId::from_bytes([byte; 32])
}

fn display_txid(byte: u8) -> String {
    zinder_core::wire::encode_rpc_transaction_id_hex(transaction_id(byte))
}

fn p2pkh_script_hex() -> String {
    format!("76a914{}88ac", "01".repeat(20))
}

fn utxo_row(height: u32, txid_byte: u8, output_index: u32) -> Value {
    json!({
        "address": testnet_address(1).to_string(),
        "txid": display_txid(txid_byte),
        "outputIndex": output_index,
        "script": p2pkh_script_hex(),
        "satoshis": 1_000 + u64::from(height),
        "height": height,
    })
}

#[tokio::test]
async fn transparent_address_balance_sends_sorted_distinct_addresses() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("getaddressbalance")
            .reply(RpcReply::result(json!({ "balance": 7, "received": 9 })))])?;
    let source = regtest_source(server.url())?;

    let balance = source
        .fetch_transparent_address_balance(&regtest_lookup()?)
        .await?;

    assert_eq!(
        balance,
        SourceTransparentAddressBalance {
            balance_zat: 7,
            received_zat: 9,
        }
    );
    assert_eq!(
        server.requests_for("getaddressbalance")?[0].params,
        json!([{ "addresses": sorted_addresses() }])
    );
    Ok(())
}

#[tokio::test]
async fn transparent_address_transaction_ids_clamp_the_end_to_the_observed_tip() -> eyre::Result<()>
{
    let server = JsonRpcTestServer::start([
        method("getbestblockheightandhash").reply(tip_reply(10)),
        method("getaddresstxids")
            .reply(RpcReply::result(json!([display_txid(3), display_txid(4)]))),
    ])?;
    let source = regtest_source(server.url())?;

    let transaction_ids = source
        .fetch_transparent_address_transaction_ids(
            &regtest_lookup()?,
            BlockHeight::new(2),
            BlockHeight::new(50),
        )
        .await?;

    assert_eq!(transaction_ids, [transaction_id(3), transaction_id(4)]);
    assert_eq!(
        server.requests_for("getaddresstxids")?[0].params,
        json!([{ "addresses": sorted_addresses(), "start": 2, "end": 10 }])
    );
    Ok(())
}

#[tokio::test]
async fn transparent_address_transaction_ids_above_the_tip_skip_the_lookup() -> eyre::Result<()> {
    // The node clamps a start height above its tip to the tip, which would
    // return the tip block's transactions for a range that holds none.
    let server = JsonRpcTestServer::start([
        method("getbestblockheightandhash").reply(tip_reply(10)),
        method("getaddresstxids").reply(RpcReply::result(json!([display_txid(3)]))),
    ])?;
    let source = regtest_source(server.url())?;

    let transaction_ids = source
        .fetch_transparent_address_transaction_ids(
            &regtest_lookup()?,
            BlockHeight::new(11),
            BlockHeight::new(20),
        )
        .await?;

    assert_eq!(transaction_ids, Vec::<TransactionId>::new());
    assert!(server.requests_for("getaddresstxids")?.is_empty());
    Ok(())
}

#[tokio::test]
async fn transparent_address_transaction_ids_for_genesis_or_empty_ranges_send_nothing()
-> eyre::Result<()> {
    // The node reads an end height of zero as its tip and never indexes the
    // genesis block, so neither range reaches the node.
    let server = JsonRpcTestServer::start([])?;
    let source = regtest_source(server.url())?;
    let lookup = regtest_lookup()?;

    for (start_height, end_height) in [(0, 0), (5, 4)] {
        let transaction_ids = source
            .fetch_transparent_address_transaction_ids(
                &lookup,
                BlockHeight::new(start_height),
                BlockHeight::new(end_height),
            )
            .await?;
        assert_eq!(transaction_ids, Vec::<TransactionId>::new());
    }
    assert!(server.requests()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn transparent_address_row_cap_refusal_is_typed() -> eyre::Result<()> {
    // Zebra 7.1.0 refuses a lookup matching more than 100,000 rows with -8.
    let server = JsonRpcTestServer::start([
        method("getbestblockheightandhash").reply(tip_reply(10)),
        method("getaddresstxids").reply(RpcReply::error(
            "more than 100000 transactions match; narrow the height range",
        )),
        method("getaddressutxos").reply(RpcReply::error(
            "more than 100000 unspent outputs match; page with startHeight and maxEntries",
        )),
    ])?;
    let source = regtest_source(server.url())?;
    let lookup = regtest_lookup()?;

    let transaction_id_error = source
        .fetch_transparent_address_transaction_ids(
            &lookup,
            BlockHeight::new(1),
            BlockHeight::new(10),
        )
        .await
        .err()
        .ok_or_else(|| eyre!("expected a row-limit refusal"))?;
    let utxo_error = source
        .fetch_transparent_address_utxos(
            &lookup,
            BlockHeight::new(0),
            TransparentAddressRowLimit::MAX,
        )
        .await
        .err()
        .ok_or_else(|| eyre!("expected a row-limit refusal"))?;

    assert!(matches!(
        transaction_id_error,
        SourceError::TransparentAddressRowLimitExceeded {
            operation: "getaddresstxids",
            max_rows: 100_000,
        }
    ));
    assert!(matches!(
        utxo_error,
        SourceError::TransparentAddressRowLimitExceeded {
            operation: "getaddressutxos",
            max_rows: 100_000,
        }
    ));
    assert_eq!(
        transaction_id_error.upstream_classification(),
        SourceFailureClass::Configuration
    );
    Ok(())
}

#[tokio::test]
async fn transparent_address_row_cap_holds_for_a_node_without_one() -> eyre::Result<()> {
    // Zakura 1.8.6 answers getaddresstxids without a row cap.
    let over_cap: Vec<String> = std::iter::repeat_n(display_txid(5), 100_001).collect();
    let server = JsonRpcTestServer::start([
        method("getbestblockheightandhash").reply(tip_reply(10)),
        method("getaddresstxids").reply(RpcReply::result(json!(over_cap))),
    ])?;
    let source = regtest_source(server.url())?;

    let error = source
        .fetch_transparent_address_transaction_ids(
            &regtest_lookup()?,
            BlockHeight::new(1),
            BlockHeight::new(10),
        )
        .await
        .err()
        .ok_or_else(|| eyre!("expected a row-limit refusal"))?;

    assert!(matches!(
        error,
        SourceError::TransparentAddressRowLimitExceeded {
            operation: "getaddresstxids",
            max_rows: 100_000,
        }
    ));
    Ok(())
}

#[tokio::test]
async fn transparent_address_utxos_send_page_bounds_and_decode_rows() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("getaddressutxos").reply(RpcReply::result(json!([
            utxo_row(4, 6, 0),
            utxo_row(7, 7, 1)
        ])))])?;
    let source = regtest_source(server.url())?;
    let max_rows = TransparentAddressRowLimit::new(2).ok_or_else(|| eyre!("row limit"))?;

    let utxos = source
        .fetch_transparent_address_utxos(&regtest_lookup()?, BlockHeight::new(4), max_rows)
        .await?;

    assert_eq!(
        server.requests_for("getaddressutxos")?[0].params,
        json!([{ "addresses": sorted_addresses(), "startHeight": 4, "maxEntries": 2 }])
    );
    assert_eq!(utxos.len(), 2);
    let first = utxos.first().ok_or_else(|| eyre!("expected an output"))?;
    assert_eq!(first.address, testnet_address(1));
    assert_eq!(
        first.outpoint,
        TransparentOutPoint::new(transaction_id(6), 0)
    );
    assert_eq!(first.value_zat, 1_004);
    assert_eq!(first.block_height, BlockHeight::new(4));
    assert_eq!(hex::encode(&first.script_pub_key), p2pkh_script_hex());
    Ok(())
}

#[tokio::test]
async fn transparent_address_utxos_bound_a_node_that_ignores_paging() -> eyre::Result<()> {
    // Zakura 1.8.6 ignores startHeight and maxEntries and returns every
    // unspent output in chain order.
    let server =
        JsonRpcTestServer::start([method("getaddressutxos").reply(RpcReply::result(json!([
            utxo_row(2, 1, 0),
            utxo_row(5, 2, 0),
            utxo_row(5, 2, 1),
            utxo_row(6, 3, 0),
        ])))])?;
    let source = regtest_source(server.url())?;
    let max_rows = TransparentAddressRowLimit::new(2).ok_or_else(|| eyre!("row limit"))?;

    let utxos = source
        .fetch_transparent_address_utxos(&regtest_lookup()?, BlockHeight::new(5), max_rows)
        .await?;

    let outpoints: Vec<TransparentOutPoint> = utxos.iter().map(|output| output.outpoint).collect();
    assert_eq!(
        outpoints,
        [
            TransparentOutPoint::new(transaction_id(2), 0),
            TransparentOutPoint::new(transaction_id(2), 1),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn transparent_address_lookups_on_a_node_without_indexes_are_capability_missing()
-> eyre::Result<()> {
    let server = JsonRpcTestServer::start([
        method("getbestblockheightandhash").reply(tip_reply(10)),
        method("getaddressbalance").reply(RpcReply::error_with_code(
            -1,
            ZAKURA_TRANSPARENT_INDEXES_REFUSAL,
        )),
        method("getaddresstxids").reply(RpcReply::error_with_code(
            -1,
            ZAKURA_TRANSPARENT_INDEXES_REFUSAL,
        )),
        method("getaddressutxos").reply(RpcReply::error_with_code(-32_601, "Method not found")),
    ])?;
    let source = regtest_source(server.url())?;
    let lookup = regtest_lookup()?;

    let errors = [
        source
            .fetch_transparent_address_balance(&lookup)
            .await
            .err(),
        source
            .fetch_transparent_address_transaction_ids(
                &lookup,
                BlockHeight::new(1),
                BlockHeight::new(10),
            )
            .await
            .err(),
        source
            .fetch_transparent_address_utxos(
                &lookup,
                BlockHeight::new(0),
                TransparentAddressRowLimit::MAX,
            )
            .await
            .err(),
    ];

    for error in errors {
        let error = error.ok_or_else(|| eyre!("expected an index refusal"))?;
        assert!(
            matches!(
                error,
                SourceError::TransparentAddressIndexesUnavailable { .. }
            ),
            "unexpected error {error:?}"
        );
        assert_eq!(
            error.upstream_classification(),
            SourceFailureClass::CapabilityMissing
        );
        assert!(error.is_node_capability_missing());
    }
    Ok(())
}

#[tokio::test]
async fn transparent_address_lookup_on_an_unreachable_node_is_node_unavailable() -> eyre::Result<()>
{
    let source = regtest_source("http://127.0.0.1:1")?;

    let error = source
        .fetch_transparent_address_balance(&regtest_lookup()?)
        .await
        .err()
        .ok_or_else(|| eyre!("expected an unreachable node"))?;

    assert!(matches!(error, SourceError::NodeUnavailable { .. }));
    assert_eq!(
        error.upstream_classification(),
        SourceFailureClass::NodeUnreachable
    );
    Ok(())
}

#[tokio::test]
async fn transparent_address_lookup_for_another_network_sends_nothing() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([])?;
    let source = regtest_source(server.url())?;
    let testnet_lookup =
        TransparentAddressLookup::new(Network::ZcashTestnet, [testnet_address(1)])?;

    let error = source
        .fetch_transparent_address_balance(&testnet_lookup)
        .await
        .err()
        .ok_or_else(|| eyre!("expected a network mismatch"))?;

    assert!(matches!(
        error,
        SourceError::TransparentAddressLookupNetworkMismatch {
            lookup_network: Network::ZcashTestnet,
            source_network: Network::ZcashRegtest,
        }
    ));
    assert!(server.requests()?.is_empty());
    Ok(())
}
