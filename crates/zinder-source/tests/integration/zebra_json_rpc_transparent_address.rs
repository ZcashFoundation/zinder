#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::time::Duration;

use eyre::eyre;
use serde_json::{Value, json};
use zebra_chain::parameters::NetworkKind as ZebraNetworkKind;
use zebra_chain::transparent::Address as ZebraTransparentAddress;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, Network, TransactionId, TransparentOutPoint,
    wire::{decode_rpc_block_hash_hex, encode_rpc_block_hash_hex},
};
use zinder_source::{
    CookieSource, NodeAuth, SourceError, SourceFailureClass, SourceTransparentAddressBalance,
    TransparentAddressLookup, TransparentAddressRowLimit, ZebraJsonRpcSource,
};
use zinder_testkit::{JsonRpcStub, JsonRpcTestServer, RpcReply, method};

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

/// The block every lookup in these tests requires the node's chain to hold.
const CHAIN_BLOCK: BlockId = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x11; 32]));

/// Starts a node double whose best chain holds [`CHAIN_BLOCK`] unless `stubs`
/// replace its `getblockhash` reply.
fn start_node(stubs: impl IntoIterator<Item = JsonRpcStub>) -> eyre::Result<JsonRpcTestServer> {
    let holds_chain_block = method("getblockhash")
        .reply_with(|_| RpcReply::result(json!(encode_rpc_block_hash_hex(CHAIN_BLOCK.hash))));
    JsonRpcTestServer::start(std::iter::once(holds_chain_block).chain(stubs))
}

fn tip_reply(height: u32) -> RpcReply {
    tip_reply_with_hash(height, 0)
}

fn tip_reply_with_hash(height: u32, hash_byte: u8) -> RpcReply {
    RpcReply::result(json!({ "height": height, "hash": vec![hash_byte; 32] }))
}

fn utxo_requests_max_entries(server: &JsonRpcTestServer) -> eyre::Result<Vec<Value>> {
    Ok(server
        .requests_for("getaddressutxos")?
        .into_iter()
        .map(|request| request.params[0]["maxEntries"].clone())
        .collect())
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

/// JSON-RPC code both nodes answer for a failed state read.
const NODE_MISC_ERROR_CODE: i64 = -1;

const NODE_RESPONSE_TOO_LARGE_CODE: i64 = -32_008;

/// Wraps `rows` in the `getaddressutxos` reply both nodes give for `chainInfo`.
fn utxos_at_tip(rows: &Value) -> Value {
    json!({"utxos": rows, "hash": "ab".repeat(32), "height": 9})
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
        start_node([method("getaddressbalance")
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
    let server = start_node([
        method("getbestblockheightandhash").reply_with(|_| tip_reply(10)),
        method("getaddresstxids")
            .reply(RpcReply::result(json!([display_txid(3), display_txid(4)]))),
    ])?;
    let source = regtest_source(server.url())?;

    let transaction_ids = source
        .fetch_transparent_address_transaction_ids(
            &regtest_lookup()?,
            BlockHeight::new(2),
            BlockHeight::new(50),
            CHAIN_BLOCK,
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
async fn transparent_address_transaction_ids_refuse_an_empty_answer_after_a_rollback()
-> eyre::Result<()> {
    // The node rolled back below the range and below the required block,
    // so it no longer holds the block at that height.
    let server = start_node([
        method("getbestblockheightandhash").reply_with(|_| tip_reply(0)),
        method("getblockhash")
            .reply_with(|_| RpcReply::error_with_code(-8, "block height not in best chain")),
    ])?;
    let source = regtest_source(server.url())?;

    let outcome = source
        .fetch_transparent_address_transaction_ids(
            &regtest_lookup()?,
            BlockHeight::new(1),
            BlockHeight::new(1),
            CHAIN_BLOCK,
        )
        .await;

    assert!(
        outcome.is_err(),
        "answered {outcome:?} from a rolled-back node"
    );
    assert!(server.requests_for("getaddresstxids")?.is_empty());
    Ok(())
}

#[tokio::test]
async fn transparent_address_transaction_ids_refuse_an_answer_from_a_chain_without_the_required_block()
-> eyre::Result<()> {
    // The node's tip reads agree, but around the request it holds another
    // block at the required height: first before the request, then after it.
    for (before, after) in [([0xee; 32], [0x11; 32]), ([0x11; 32], [0xee; 32])] {
        let server = start_node([
            method("getbestblockheightandhash").reply_with(|_| tip_reply(10)),
            method("getblockhash").reply(RpcReply::result(json!(encode_rpc_block_hash_hex(
                BlockHash::from_bytes(before)
            )))),
            method("getblockhash").reply(RpcReply::result(json!(encode_rpc_block_hash_hex(
                BlockHash::from_bytes(after)
            )))),
            method("getaddresstxids").reply_with(|_| RpcReply::result(json!([]))),
        ])?;
        let source = regtest_source(server.url())?;

        let error = source
            .fetch_transparent_address_transaction_ids(
                &regtest_lookup()?,
                BlockHeight::new(2),
                BlockHeight::new(10),
                CHAIN_BLOCK,
            )
            .await
            .err()
            .ok_or_else(|| eyre!("kept an answer from a chain without the required block"))?;

        assert!(
            matches!(
                error,
                SourceError::ChainTipChangedDuringFetch {
                    operation: "getaddresstxids",
                    attempts: 1,
                }
            ),
            "unexpected error {error:?}"
        );
        let checked: Vec<Value> = server
            .requests_for("getblockhash")?
            .into_iter()
            .map(|request| request.params)
            .collect();
        assert!(
            checked.iter().all(|params| *params == json!([1])),
            "{checked:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn transparent_address_transaction_ids_repeat_the_lookup_when_the_tip_changes_during_it()
-> eyre::Result<()> {
    // The tip falls from block 10 to a sibling at height 9 while the node
    // answers, so the first answer may hold transactions of the old tip.
    let server = start_node([
        method("getbestblockheightandhash").reply(tip_reply_with_hash(10, 1)),
        method("getbestblockheightandhash").reply(tip_reply_with_hash(9, 2)),
        method("getbestblockheightandhash").reply(tip_reply_with_hash(9, 2)),
        method("getaddresstxids")
            .reply(RpcReply::result(json!([display_txid(3), display_txid(4)]))),
        method("getaddresstxids").reply(RpcReply::result(json!([display_txid(3)]))),
    ])?;
    let source = regtest_source(server.url())?;

    let transaction_ids = source
        .fetch_transparent_address_transaction_ids(
            &regtest_lookup()?,
            BlockHeight::new(2),
            BlockHeight::new(50),
            CHAIN_BLOCK,
        )
        .await?;

    assert_eq!(transaction_ids, [transaction_id(3)]);
    let end_heights: Vec<Value> = server
        .requests_for("getaddresstxids")?
        .into_iter()
        .map(|request| request.params[0]["end"].clone())
        .collect();
    assert_eq!(end_heights, [json!(10), json!(9)]);
    Ok(())
}

#[tokio::test]
async fn transparent_address_transaction_ids_drop_an_answer_for_a_tip_that_fell_below_the_range()
-> eyre::Result<()> {
    // The tip rolls back from 10 to 8 before the node answers, so the node
    // clamps the range 10..=10 to its new tip and returns block 8's
    // transactions, which lie outside the range.
    let server = start_node([
        method("getbestblockheightandhash").reply(tip_reply_with_hash(10, 1)),
        method("getbestblockheightandhash").reply(tip_reply_with_hash(8, 2)),
        method("getaddresstxids").reply(RpcReply::result(json!([display_txid(7)]))),
    ])?;
    let source = regtest_source(server.url())?;

    let transaction_ids = source
        .fetch_transparent_address_transaction_ids(
            &regtest_lookup()?,
            BlockHeight::new(10),
            BlockHeight::new(10),
            CHAIN_BLOCK,
        )
        .await?;

    assert_eq!(transaction_ids, Vec::<TransactionId>::new());
    assert_eq!(server.requests_for("getaddresstxids")?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn transparent_address_transaction_ids_report_a_tip_that_changes_during_every_attempt()
-> eyre::Result<()> {
    let server = start_node([
        method("getbestblockheightandhash").reply(tip_reply_with_hash(10, 1)),
        method("getbestblockheightandhash").reply(tip_reply_with_hash(11, 2)),
        method("getbestblockheightandhash").reply(tip_reply_with_hash(12, 3)),
        method("getbestblockheightandhash").reply(tip_reply_with_hash(13, 4)),
        method("getaddresstxids").reply_with(|_| RpcReply::result(json!([display_txid(3)]))),
    ])?;
    let source = regtest_source(server.url())?;

    let error = source
        .fetch_transparent_address_transaction_ids(
            &regtest_lookup()?,
            BlockHeight::new(1),
            BlockHeight::new(10),
            CHAIN_BLOCK,
        )
        .await
        .err()
        .ok_or_else(|| eyre!("expected a changed-tip refusal"))?;

    assert!(
        matches!(
            error,
            SourceError::ChainTipChangedDuringFetch {
                operation: "getaddresstxids",
                attempts: 3,
            }
        ),
        "unexpected error {error:?}"
    );
    assert_eq!(
        error.upstream_classification(),
        SourceFailureClass::UpstreamViewChanged
    );
    assert_eq!(server.requests_for("getaddresstxids")?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn transparent_address_transaction_ids_above_the_tip_skip_the_lookup() -> eyre::Result<()> {
    // The node clamps a start height above its tip to the tip, which would
    // return the tip block's transactions for a range that holds none.
    let server = start_node([
        method("getbestblockheightandhash").reply(tip_reply(10)),
        method("getaddresstxids").reply(RpcReply::result(json!([display_txid(3)]))),
    ])?;
    let source = regtest_source(server.url())?;

    let transaction_ids = source
        .fetch_transparent_address_transaction_ids(
            &regtest_lookup()?,
            BlockHeight::new(11),
            BlockHeight::new(20),
            CHAIN_BLOCK,
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
    let server = start_node([])?;
    let source = regtest_source(server.url())?;
    let lookup = regtest_lookup()?;

    for (start_height, end_height) in [(0, 0), (5, 4)] {
        let transaction_ids = source
            .fetch_transparent_address_transaction_ids(
                &lookup,
                BlockHeight::new(start_height),
                BlockHeight::new(end_height),
                CHAIN_BLOCK,
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
    let server = start_node([
        method("getbestblockheightandhash").reply_with(|_| tip_reply(10)),
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
            CHAIN_BLOCK,
        )
        .await
        .err()
        .ok_or_else(|| eyre!("expected a row-limit refusal"))?;
    let utxo_error = source
        .fetch_transparent_address_utxos(&lookup, BlockHeight::new(0))
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
    let server = start_node([
        method("getbestblockheightandhash").reply_with(|_| tip_reply(10)),
        method("getaddresstxids").reply(RpcReply::result(json!(over_cap))),
    ])?;
    let source = regtest_source(server.url())?;

    let error = source
        .fetch_transparent_address_transaction_ids(
            &regtest_lookup()?,
            BlockHeight::new(1),
            BlockHeight::new(10),
            CHAIN_BLOCK,
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
async fn transparent_address_utxos_refuse_an_output_above_the_reported_tip() -> eyre::Result<()> {
    // utxos_at_tip reports height 9.
    let server = start_node([
        method("getaddressutxos").reply(RpcReply::result(utxos_at_tip(&json!([
            utxo_row(9, 1, 0),
            utxo_row(10, 2, 0)
        ])))),
    ])?;
    let source = regtest_source(server.url())?;

    let error = source
        .fetch_transparent_address_utxos(&regtest_lookup()?, BlockHeight::new(0))
        .await
        .err()
        .ok_or_else(|| eyre!("accepted an output above the reported tip"))?;

    assert!(
        matches!(
            error,
            SourceError::SourceProtocolMismatch {
                reason: "getaddressutxos returned an output above the tip it reports"
            }
        ),
        "unexpected error {error:?}"
    );
    Ok(())
}

#[tokio::test]
async fn transparent_address_utxos_ask_for_the_complete_set_and_refuse_a_capped_answer()
-> eyre::Result<()> {
    // Zebra 7.1.0 returns the first maxEntries outputs without saying whether
    // more match. Only maxEntries 0 makes it refuse a lookup that matches
    // more than 100,000 outputs.
    let server = start_node([method("getaddressutxos").reply_with(|params| {
        if params[0]["maxEntries"] == json!(0) {
            RpcReply::error(
                "more than 100000 unspent outputs match; page with startHeight and maxEntries",
            )
        } else {
            RpcReply::result(utxos_at_tip(&json!([utxo_row(4, 6, 0), utxo_row(7, 7, 1)])))
        }
    })])?;
    let source = regtest_source(server.url())?;

    let error = source
        .fetch_transparent_address_utxos(&regtest_lookup()?, BlockHeight::new(4))
        .await
        .err()
        .ok_or_else(|| eyre!("expected a row-limit refusal"))?;

    assert!(
        matches!(
            error,
            SourceError::TransparentAddressRowLimitExceeded {
                operation: "getaddressutxos",
                max_rows: 100_000,
            }
        ),
        "unexpected error {error:?}"
    );
    assert_eq!(
        server.requests_for("getaddressutxos")?[0].params,
        json!([{ "addresses": sorted_addresses(), "startHeight": 4, "maxEntries": 0, "chainInfo": true }])
    );
    Ok(())
}

#[tokio::test]
async fn transparent_address_utxos_return_every_output_at_or_above_the_start_height_and_their_tip()
-> eyre::Result<()> {
    // Zakura 1.8.6 ignores startHeight and maxEntries and returns every
    // unspent output in chain order.
    let server = start_node([
        method("getaddressutxos").reply(RpcReply::result(utxos_at_tip(&json!([
            utxo_row(2, 1, 0),
            utxo_row(5, 2, 0),
            utxo_row(5, 2, 1),
            utxo_row(6, 3, 0),
        ])))),
    ])?;
    let source = regtest_source(server.url())?;

    let utxos = source
        .fetch_transparent_address_utxos(&regtest_lookup()?, BlockHeight::new(5))
        .await?;

    let outpoints: Vec<TransparentOutPoint> =
        utxos.outputs.iter().map(|output| output.outpoint).collect();
    assert_eq!(
        outpoints,
        [
            TransparentOutPoint::new(transaction_id(2), 0),
            TransparentOutPoint::new(transaction_id(2), 1),
            TransparentOutPoint::new(transaction_id(3), 0),
        ]
    );
    assert_eq!(
        utxos.tip,
        BlockId::new(
            BlockHeight::new(9),
            decode_rpc_block_hash_hex(&"ab".repeat(32))?
        )
    );
    assert_eq!(utxo_requests_max_entries(&server)?, [json!(0)]);
    Ok(())
}

#[tokio::test]
async fn transparent_address_utxo_prefix_sends_its_bounds_and_decodes_rows() -> eyre::Result<()> {
    let server = start_node([
        method("getaddressutxos").reply(RpcReply::result(utxos_at_tip(&json!([
            utxo_row(4, 6, 0),
            utxo_row(7, 7, 1)
        ])))),
    ])?;
    let source = regtest_source(server.url())?;
    let max_rows = TransparentAddressRowLimit::new(2).ok_or_else(|| eyre!("row limit"))?;

    let utxos = source
        .fetch_transparent_address_utxo_prefix(&regtest_lookup()?, BlockHeight::new(4), max_rows)
        .await?;

    assert_eq!(
        server.requests_for("getaddressutxos")?[0].params,
        json!([{ "addresses": sorted_addresses(), "startHeight": 4, "maxEntries": 2, "chainInfo": true }])
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
async fn transparent_address_utxo_prefix_bounds_a_node_that_ignores_paging() -> eyre::Result<()> {
    // Zakura 1.8.6 ignores startHeight and maxEntries and returns every
    // unspent output in chain order.
    let server = start_node([
        method("getaddressutxos").reply(RpcReply::result(utxos_at_tip(&json!([
            utxo_row(2, 1, 0),
            utxo_row(5, 2, 0),
            utxo_row(5, 2, 1),
            utxo_row(6, 3, 0),
        ])))),
    ])?;
    let source = regtest_source(server.url())?;
    let max_rows = TransparentAddressRowLimit::new(2).ok_or_else(|| eyre!("row limit"))?;

    let utxos = source
        .fetch_transparent_address_utxo_prefix(&regtest_lookup()?, BlockHeight::new(5), max_rows)
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
async fn transparent_address_utxos_refuse_an_output_of_an_address_outside_the_lookup()
-> eyre::Result<()> {
    let mut foreign_row = utxo_row(6, 3, 0);
    foreign_row["address"] = json!(testnet_address(9).to_string());
    let server = start_node([method("getaddressutxos").reply_with(move |_| {
        RpcReply::result(utxos_at_tip(&json!([
            utxo_row(5, 2, 0),
            foreign_row.clone()
        ])))
    })])?;
    let source = regtest_source(server.url())?;
    let lookup = regtest_lookup()?;
    let max_rows = TransparentAddressRowLimit::new(2).ok_or_else(|| eyre!("row limit"))?;

    let errors = [
        source
            .fetch_transparent_address_utxos(&lookup, BlockHeight::new(0))
            .await
            .err(),
        source
            .fetch_transparent_address_utxo_prefix(&lookup, BlockHeight::new(0), max_rows)
            .await
            .err(),
    ];

    for error in errors {
        let error = error.ok_or_else(|| eyre!("expected a protocol mismatch"))?;
        assert!(
            matches!(error, SourceError::SourceProtocolMismatch { .. }),
            "unexpected error {error:?}"
        );
        assert_eq!(
            error.upstream_classification(),
            SourceFailureClass::ProtocolMismatch
        );
    }
    Ok(())
}

#[tokio::test]
async fn transparent_address_answer_over_the_node_response_limit_is_typed() -> eyre::Result<()> {
    // Zakura and Zebra answer -32008 instead of a response larger than their
    // rpc.max_response_body_size.
    let server = start_node([method("getaddressutxos").reply(RpcReply::error_with_code(
        NODE_RESPONSE_TOO_LARGE_CODE,
        "Response is too big",
    ))])?;
    let source = regtest_source(server.url())?;

    let error = source
        .fetch_transparent_address_utxos(&regtest_lookup()?, BlockHeight::new(0))
        .await
        .err()
        .ok_or_else(|| eyre!("expected a response-size refusal"))?;

    assert!(
        matches!(
            error,
            SourceError::NodeResponseTooLarge {
                operation: "getaddressutxos",
                ..
            }
        ),
        "unexpected error {error:?}"
    );
    assert_eq!(
        error.upstream_classification(),
        SourceFailureClass::Configuration
    );
    Ok(())
}

#[tokio::test]
async fn transparent_address_lookups_on_a_node_without_indexes_are_capability_missing()
-> eyre::Result<()> {
    let server = start_node([
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
                CHAIN_BLOCK,
            )
            .await
            .err(),
        source
            .fetch_transparent_address_utxos(&lookup, BlockHeight::new(0))
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
    let server = start_node([])?;
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

#[tokio::test]
async fn transparent_address_transaction_ids_drop_a_size_refusal_for_a_tip_that_fell_below_the_range()
-> eyre::Result<()> {
    // The tip rolls back from 10 to 8 before the node answers, so the node
    // clamps the range 10..=10 to block 8 and refuses that block's answer.
    let server = start_node([
        method("getbestblockheightandhash").reply(tip_reply_with_hash(10, 1)),
        method("getbestblockheightandhash").reply(tip_reply_with_hash(8, 2)),
        method("getaddresstxids").reply(RpcReply::error_with_code(
            NODE_RESPONSE_TOO_LARGE_CODE,
            "Response is too big",
        )),
    ])?;
    let source = regtest_source(server.url())?;

    let transaction_ids = source
        .fetch_transparent_address_transaction_ids(
            &regtest_lookup()?,
            BlockHeight::new(10),
            BlockHeight::new(10),
            CHAIN_BLOCK,
        )
        .await?;

    assert_eq!(transaction_ids, Vec::<TransactionId>::new());
    assert_eq!(server.requests_for("getaddresstxids")?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn transparent_address_transaction_ids_report_a_size_refusal_only_for_a_steady_tip()
-> eyre::Result<()> {
    let steady_server = start_node([
        method("getbestblockheightandhash").reply_with(|_| tip_reply(10)),
        method("getaddresstxids").reply_with(|_| {
            RpcReply::error_with_code(NODE_RESPONSE_TOO_LARGE_CODE, "Response is too big")
        }),
    ])?;
    let moving_server = start_node([
        method("getbestblockheightandhash").reply(tip_reply_with_hash(10, 1)),
        method("getbestblockheightandhash").reply(tip_reply_with_hash(11, 2)),
        method("getbestblockheightandhash").reply(tip_reply_with_hash(12, 3)),
        method("getbestblockheightandhash").reply(tip_reply_with_hash(13, 4)),
        method("getaddresstxids").reply_with(|_| {
            RpcReply::error("more than 100000 transactions match; narrow the height range")
        }),
    ])?;
    let lookup = regtest_lookup()?;

    let mut errors = Vec::new();
    for server in [&steady_server, &moving_server] {
        errors.push(
            regtest_source(server.url())?
                .fetch_transparent_address_transaction_ids(
                    &lookup,
                    BlockHeight::new(1),
                    BlockHeight::new(10),
                    CHAIN_BLOCK,
                )
                .await
                .err()
                .ok_or_else(|| eyre!("expected a refusal"))?,
        );
    }

    assert!(
        matches!(
            errors.first(),
            Some(SourceError::NodeResponseTooLarge {
                operation: "getaddresstxids",
                ..
            })
        ),
        "unexpected error {errors:?}"
    );
    assert!(
        matches!(
            errors.get(1),
            Some(SourceError::ChainTipChangedDuringFetch {
                operation: "getaddresstxids",
                attempts: 3,
            })
        ),
        "unexpected error {errors:?}"
    );
    assert_eq!(steady_server.requests_for("getaddresstxids")?.len(), 1);
    assert_eq!(moving_server.requests_for("getaddresstxids")?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn transparent_address_utxos_refuse_a_script_that_does_not_pay_the_row_address()
-> eyre::Result<()> {
    // Address 2 is in the lookup, so the row that names address 1 but pays
    // address 2 passes the address check.
    let mut op_return_row = utxo_row(5, 2, 0);
    op_return_row["script"] = json!("6a");
    let mut other_recipient_row = utxo_row(5, 2, 0);
    other_recipient_row["script"] = json!(format!("76a914{}88ac", "02".repeat(20)));

    for row in [op_return_row, other_recipient_row] {
        let server = start_node([
            method("getaddressutxos").reply(RpcReply::result(utxos_at_tip(&json!([row]))))
        ])?;
        let error = regtest_source(server.url())?
            .fetch_transparent_address_utxos(&regtest_lookup()?, BlockHeight::new(0))
            .await
            .err()
            .ok_or_else(|| eyre!("expected a protocol mismatch"))?;

        assert!(
            matches!(
                error,
                SourceError::SourceProtocolMismatch {
                    reason: "getaddressutxos returned a script that does not pay the row's address"
                }
            ),
            "unexpected error {error:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn transparent_address_utxos_refuse_a_repeated_outpoint() -> eyre::Result<()> {
    let server = start_node([
        method("getaddressutxos").reply(RpcReply::result(utxos_at_tip(&json!([
            utxo_row(5, 2, 0),
            utxo_row(5, 2, 0)
        ])))),
    ])?;
    let source = regtest_source(server.url())?;

    let error = source
        .fetch_transparent_address_utxos(&regtest_lookup()?, BlockHeight::new(0))
        .await
        .err()
        .ok_or_else(|| eyre!("expected a protocol mismatch"))?;

    assert!(
        matches!(error, SourceError::SourceProtocolMismatch { .. }),
        "unexpected error {error:?}"
    );
    Ok(())
}

#[tokio::test]
async fn transparent_address_answers_of_another_shape_are_protocol_mismatches() -> eyre::Result<()>
{
    let mut row_without_script = utxo_row(5, 2, 0);
    row_without_script
        .as_object_mut()
        .and_then(|row| row.remove("script"));
    let server = start_node([
        method("getbestblockheightandhash").reply_with(|_| tip_reply(10)),
        method("getaddressbalance").reply(RpcReply::result(json!({ "balance": 7 }))),
        method("getaddresstxids").reply(RpcReply::result(json!({ "txids": [] }))),
        method("getaddressutxos")
            .reply(RpcReply::result(utxos_at_tip(&json!([row_without_script])))),
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
                CHAIN_BLOCK,
            )
            .await
            .err(),
        source
            .fetch_transparent_address_utxos(&lookup, BlockHeight::new(0))
            .await
            .err(),
    ];

    for error in errors {
        let error = error.ok_or_else(|| eyre!("expected a protocol mismatch"))?;
        assert!(
            matches!(error, SourceError::SourceProtocolMismatch { .. }),
            "unexpected error {error:?}"
        );
        assert_eq!(
            error.upstream_classification(),
            SourceFailureClass::ProtocolMismatch
        );
    }
    Ok(())
}

#[tokio::test]
async fn transparent_address_utxos_reject_a_positional_row() -> eyre::Result<()> {
    let row = json!([
        testnet_address(1).to_string(),
        display_txid(2),
        0,
        p2pkh_script_hex(),
        1_005,
        5
    ]);
    let server = start_node([
        method("getaddressutxos").reply(RpcReply::result(utxos_at_tip(&json!([row]))))
    ])?;
    let outputs = regtest_source(server.url())?
        .fetch_transparent_address_utxos(&regtest_lookup()?, BlockHeight::new(0))
        .await;

    assert!(
        matches!(outputs, Err(SourceError::SourceProtocolMismatch { .. })),
        "unexpected outputs {outputs:?}"
    );
    Ok(())
}

#[tokio::test]
async fn transparent_address_answer_of_another_shape_keeps_the_client() -> eyre::Result<()> {
    // Consecutive transport-class failures rebuild the client, which reads
    // the cookie file again. A node that answers with another shape is
    // reachable, so the client keeps its startup credentials.
    const ATTEMPTS: usize = 8;
    const STARTUP_AUTHORIZATION: &str = "Basic X19jb29raWVfXzpiZWZvcmUtcmVzdGFydA==";

    let server =
        start_node([method("getaddressbalance").reply_with(|_| RpcReply::result(json!([5, 10])))])?;
    let cookie_dir = tempfile::tempdir()?;
    let cookie_path = cookie_dir.path().join(".cookie");
    std::fs::write(&cookie_path, "__cookie__:before-restart")?;
    let source = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        server.url(),
        NodeAuth::Cookie(CookieSource::File(cookie_path.clone())),
        Duration::from_secs(5),
    )?;
    let lookup = regtest_lookup()?;
    std::fs::write(&cookie_path, "__cookie__:after-restart")?;

    for _ in 0..ATTEMPTS {
        let balance = source.fetch_transparent_address_balance(&lookup).await;
        assert!(
            matches!(balance, Err(SourceError::SourceProtocolMismatch { .. })),
            "unexpected balance {balance:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let authorizations: Vec<Option<String>> = server
        .requests()?
        .into_iter()
        .map(|request| request.authorization)
        .collect();
    assert_eq!(authorizations.len(), ATTEMPTS);
    assert!(
        authorizations
            .iter()
            .all(|authorization| authorization.as_deref() == Some(STARTUP_AUTHORIZATION)),
        "an answer of another shape must not rebuild the client: {authorizations:?}"
    );
    Ok(())
}

#[tokio::test]
async fn transparent_address_lookups_during_a_block_commit_are_view_changes() -> eyre::Result<()> {
    // Zebra 7.1.0 and Zakura 1.8.6 answer -1 with these messages when their
    // finalized state commits a block during the address index read.
    let server = start_node([
        method("getbestblockheightandhash").reply_with(|_| tip_reply(10)),
        method("getaddressbalance").reply(RpcReply::error_with_code(
            NODE_MISC_ERROR_CODE,
            "unable to get balance: state was committing a block",
        )),
        method("getaddresstxids").reply(RpcReply::error_with_code(
            NODE_MISC_ERROR_CODE,
            "unable to get tx IDs: state was committing a block, that is missing from the \
             non-finalized chain, and the query has multiple addresses",
        )),
        method("getaddressutxos").reply(RpcReply::error_with_code(
            NODE_MISC_ERROR_CODE,
            "unable to get UTXOs: state was committing a block, and non-finalized chain is empty",
        )),
    ])?;
    let source = regtest_source(server.url())?;
    let lookup = regtest_lookup()?;

    let errors = [
        (
            "getaddressbalance",
            source
                .fetch_transparent_address_balance(&lookup)
                .await
                .err(),
        ),
        (
            "getaddresstxids",
            source
                .fetch_transparent_address_transaction_ids(
                    &lookup,
                    BlockHeight::new(1),
                    BlockHeight::new(10),
                    CHAIN_BLOCK,
                )
                .await
                .err(),
        ),
        (
            "getaddressutxos",
            source
                .fetch_transparent_address_utxos(&lookup, BlockHeight::new(0))
                .await
                .err(),
        ),
    ];

    for (expected_operation, error) in errors {
        let error = error.ok_or_else(|| eyre!("expected a block-commit refusal"))?;
        assert!(
            matches!(
                &error,
                SourceError::BlockCommittedDuringRead { operation, .. }
                    if *operation == expected_operation
            ),
            "unexpected error {error:?}"
        );
        assert_eq!(
            error.upstream_classification(),
            SourceFailureClass::UpstreamViewChanged
        );
    }
    Ok(())
}
