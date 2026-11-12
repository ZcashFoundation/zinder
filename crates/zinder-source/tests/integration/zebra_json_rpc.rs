#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{collections::HashSet, num::NonZeroU64, time::Duration};

use eyre::eyre;
use incrementalmerkletree::{
    Position,
    frontier::{CommitmentTree, Frontier},
};
use orchard::tree::MerkleHashOrchard;
use sapling::Node as SaplingNode;
use serde_json::{Value, json};
use zcash_primitives::merkle_tree::write_commitment_tree;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, BroadcastAccepted, ConsensusBranchId, Network,
    NetworkUpgradeActivation, NetworkUpgradeActivations, RawTransactionBytes, ShieldedProtocol,
    SubtreeRootIndex, SubtreeRootRange, TransactionBroadcastOutcome, TransactionId,
};
use zinder_source::{
    NodeAuth, NodeCapability, NodeHealthConfig, NodeSource, SourceChainCursor, SourceChainUpdate,
    SourceError, SourceTreeState, TransactionBroadcaster,
    UPSTREAM_HEALTH_REASON_ESTIMATED_GAP_ABOVE_FLOOR,
    UPSTREAM_HEALTH_REASON_VERIFICATION_PROGRESS_BELOW_FLOOR,
    UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK, ZebraJsonRpcSource,
    decode_rpc_block_hash,
};
use zinder_testkit::{JsonRpcTestServer, RpcReply, method};

fn regtest_source(server: &JsonRpcTestServer) -> Result<ZebraJsonRpcSource, SourceError> {
    ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        server.url(),
        NodeAuth::None,
        Duration::from_secs(5),
    )
}

fn testnet_source(server: &JsonRpcTestServer) -> Result<ZebraJsonRpcSource, SourceError> {
    ZebraJsonRpcSource::new(
        Network::ZcashTestnet,
        server.url(),
        NodeAuth::None,
        Duration::from_secs(5),
    )
}

#[tokio::test]
async fn fetch_block_at_uses_expected_json_rpc_methods_and_basic_auth() -> eyre::Result<()> {
    // `fetch_block_at` keys the required RPCs on the requested height
    // directly; no separate `getblockhash` or redundant `getblockheader`
    // round trip. Pin the method set and parameter shape since operators
    // read both through tracing.
    let fixture = fixture_block()?;
    let server = JsonRpcTestServer::start([
        method("getblock").reply(RpcReply::result(json!(fixture["raw_block_hex"])))
    ])?;
    let source = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        server.url(),
        NodeAuth::basic("zebra", "zebra"),
        Duration::from_secs(5),
    )?;

    let source_block = source.fetch_block_at(BlockHeight::new(1)).await?;
    let requests = server.requests()?;

    assert_eq!(source_block.height, BlockHeight::new(1));
    assert_eq!(
        source_block.hash,
        decode_rpc_block_hash(string_field(&fixture, "hash")?)?
    );
    let methods_called: HashSet<String> = requests.iter().map(|r| r.method.clone()).collect();
    let expected: HashSet<String> = std::iter::once("getblock").map(str::to_owned).collect();
    assert_eq!(
        methods_called, expected,
        "the block-fetch RPC keys on the height directly; tree state is fetched through its own checkpoint path",
    );
    assert!(
        server.requests_for("getblockhash")?.is_empty(),
        "`getblockhash` must not be called by the fetch path",
    );
    assert!(
        server.requests_for("getblockheader")?.is_empty(),
        "`getblockheader` must not be called by the fetch path",
    );
    assert_eq!(server.requests_for("getblock")?[0].params, json!(["1", 0]),);
    assert!(
        server.requests_for("z_gettreestate")?.is_empty(),
        "`z_gettreestate` must not be called by the raw block fetch path",
    );
    assert!(
        requests
            .iter()
            .all(|request| { request.authorization.as_deref() == Some("Basic emVicmE6emVicmE=") })
    );

    Ok(())
}

#[tokio::test]
async fn fetch_chain_update_after_start_emits_connected_block() -> eyre::Result<()> {
    let fixture = fixture_block()?;
    let server = JsonRpcTestServer::start([
        method("getbestblockheightandhash").reply(RpcReply::result(zebra_tip_response(&fixture)?)),
        method("getblock").reply(RpcReply::result(json!(fixture["raw_block_hex"]))),
    ])?;
    let source = regtest_source(&server)?;

    let update = source
        .fetch_chain_update_after(SourceChainCursor::before_first_block())
        .await?
        .ok_or_else(|| eyre!("expected connected block update"))?;

    let SourceChainUpdate::ConnectedBlock { cursor, block } = update else {
        return Err(eyre!("expected connected block update"));
    };
    let block_id = BlockId::new(
        BlockHeight::new(1),
        decode_rpc_block_hash(string_field(&fixture, "hash")?)?,
    );
    assert_eq!(cursor, SourceChainCursor::at_block(block_id));
    assert_eq!(block.height, BlockHeight::new(1));
    assert!(
        server.requests_for("getblockhash")?.is_empty(),
        "the update adapter should stay on the existing height-keyed block fetch path",
    );
    assert!(
        server.requests_for("z_gettreestate")?.is_empty(),
        "tree state is fetched separately from connected block updates",
    );
    Ok(())
}

#[tokio::test]
async fn fetch_chain_update_after_tip_cursor_returns_none() -> eyre::Result<()> {
    let fixture = fixture_block()?;
    let block_id = BlockId::new(
        BlockHeight::new(1),
        decode_rpc_block_hash(string_field(&fixture, "hash")?)?,
    );
    let server =
        JsonRpcTestServer::start([method("getbestblockheightandhash")
            .reply(RpcReply::result(zebra_tip_response(&fixture)?))])?;
    let source = regtest_source(&server)?;

    let update = source
        .fetch_chain_update_after(SourceChainCursor::at_block(block_id))
        .await?;

    assert_eq!(update, None);
    assert!(
        server.requests_for("getblock")?.is_empty(),
        "a cursor already at the observed tip should not fetch block payloads",
    );
    Ok(())
}

#[tokio::test]
async fn fetch_chain_update_after_diverged_tip_emits_reverted_block() -> eyre::Result<()> {
    let fixture = fixture_block()?;
    let old_block_id = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([7; 32]));
    let server =
        JsonRpcTestServer::start([method("getbestblockheightandhash")
            .reply(RpcReply::result(zebra_tip_response(&fixture)?))])?;
    let source = regtest_source(&server)?;

    let update = source
        .fetch_chain_update_after(SourceChainCursor::at_block(old_block_id))
        .await?
        .ok_or_else(|| eyre!("expected reverted block update"))?;

    assert_eq!(update, SourceChainUpdate::reverted_block(old_block_id));
    assert!(
        server.requests_for("getblock")?.is_empty(),
        "same-height cursor divergence should emit a revert before fetching replacement payloads",
    );
    Ok(())
}

#[tokio::test]
async fn json_rpc_error_maps_to_block_unavailable_with_view_changed_class() -> eyre::Result<()> {
    // Zebra's "height out of range" surfaces when the upstream's best
    // chain shifted between the parallel calls. The error rides on any
    // height-keyed block-fetch call. The adapter classifies it as
    // UpstreamViewChanged so the loop treats it as a recoverable signal
    // instead of a fatal exit.
    let server = JsonRpcTestServer::start([
        method("getblock").reply(RpcReply::error("height out of range"))
    ])?;
    let source = regtest_source(&server)?;

    let error = match source.fetch_block_at(BlockHeight::new(1)).await {
        Ok(source_block) => {
            return Err(eyre!("expected fetch error, got {source_block:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SourceError::BlockUnavailable { height, .. } if height == BlockHeight::new(1)
    ));
    assert_eq!(
        error.upstream_classification(),
        zinder_source::SourceFailureClass::UpstreamViewChanged,
    );

    Ok(())
}

#[tokio::test]
async fn json_rpc_warming_up_error_keeps_block_unavailable_classification() -> eyre::Result<()> {
    // Warming-up on the block payload path classifies as UpstreamViewChanged
    // so the loop recovers.
    let server = JsonRpcTestServer::start([
        method("getblock").reply(RpcReply::error_with_code(-28, "node warming up"))
    ])?;
    let source = regtest_source(&server)?;

    let error = match source.fetch_block_at(BlockHeight::new(1)).await {
        Ok(source_block) => {
            return Err(eyre!("expected fetch error, got {source_block:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SourceError::BlockUnavailable { height, .. } if height == BlockHeight::new(1)
    ));
    assert_eq!(
        error.upstream_classification(),
        zinder_source::SourceFailureClass::UpstreamViewChanged,
    );

    Ok(())
}

#[tokio::test]
async fn missing_json_rpc_result_maps_to_protocol_mismatch() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("getblock").reply(RpcReply::empty())])?;
    let source = regtest_source(&server)?;

    let error = match source.fetch_block_at(BlockHeight::new(1)).await {
        Ok(source_block) => {
            return Err(eyre!("expected fetch error, got {source_block:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(error, SourceError::NodeUnavailable { .. }));

    Ok(())
}

#[tokio::test]
async fn json_rpc_response_size_limit_is_configurable() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("getbestblockheightandhash").reply(
        RpcReply::result(json!({
            "height": 1,
            "hash": vec![0_u8; 32],
        })),
    )])?;
    let source = ZebraJsonRpcSource::with_options(
        Network::ZcashRegtest,
        server.url(),
        NodeAuth::None,
        zinder_source::ZebraJsonRpcSourceOptions {
            request_timeout: Duration::from_secs(5),
            max_response_bytes: NonZeroU64::MIN,
            broadcast_timeout: None,
        },
    )?;

    let error = match source.tip_id().await {
        Ok(tip_id) => {
            return Err(eyre!("expected response limit error, got {tip_id:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SourceError::SourceResponseTooLarge {
            operation: "getbestblockheightandhash",
            max_response_bytes: 1,
        }
    ));
    assert_eq!(
        error.upstream_classification(),
        zinder_source::SourceFailureClass::Configuration,
    );

    Ok(())
}

#[tokio::test]
async fn http_503_marks_node_unavailable_retryable() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([
        method("getbestblockheightandhash").reply(RpcReply::http_status(503))
    ])?;
    let source = regtest_source(&server)?;

    let error = match source.tip_id().await {
        Ok(tip_id) => {
            return Err(eyre!("expected HTTP status error, got {tip_id:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(error, SourceError::NodeUnavailable { .. }));
    assert_eq!(
        error.upstream_classification(),
        zinder_source::SourceFailureClass::NodeUnreachable,
    );

    Ok(())
}

#[tokio::test]
async fn json_rpc_warming_up_error_marks_tip_node_unreachable() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("getbestblockheightandhash")
        .reply(RpcReply::error_with_code(-28, "node warming up"))])?;
    let source = regtest_source(&server)?;

    let error = match source.tip_id().await {
        Ok(tip_id) => {
            return Err(eyre!("expected tip error, got {tip_id:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(error, SourceError::NodeUnavailable { .. }));
    assert_eq!(
        error.upstream_classification(),
        zinder_source::SourceFailureClass::NodeUnreachable,
    );

    Ok(())
}

#[tokio::test]
async fn tip_id_uses_atomic_height_and_hash_observation() -> eyre::Result<()> {
    let fixture = fixture_block()?;
    let expected_hash = decode_rpc_block_hash(string_field(&fixture, "hash")?)?;
    let server = JsonRpcTestServer::start([method("getbestblockheightandhash").reply(
        RpcReply::result(json!({
            "height": fixture["height"],
            "hash": expected_hash.as_bytes(),
        })),
    )])?;
    let source = regtest_source(&server)?;

    let tip_id = source.tip_id().await?;

    assert_eq!(tip_id.height, BlockHeight::new(1));
    assert_eq!(tip_id.hash, expected_hash);
    let requests = server.requests()?;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "getbestblockheightandhash");
    assert_eq!(
        server.requests_for("getbestblockheightandhash")?[0].params,
        Value::Null
    );

    Ok(())
}

#[tokio::test]
async fn bad_raw_block_hex_maps_to_invalid_raw_block_hex() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("getblock").reply(RpcReply::result(json!("not-hex")))])?;
    let source = regtest_source(&server)?;

    let error = match source.fetch_block_at(BlockHeight::new(1)).await {
        Ok(source_block) => {
            return Err(eyre!("expected fetch error, got {source_block:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(error, SourceError::InvalidRawBlockHex { .. }));

    Ok(())
}

const TREE_STATE_BLOCK_HASH_HEX: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";

async fn fetch_tree_state_response(
    tree_state_response: Value,
) -> eyre::Result<Result<SourceTreeState, SourceError>> {
    let server = JsonRpcTestServer::start([
        method("z_gettreestate").reply(RpcReply::result(tree_state_response))
    ])?;
    let source = regtest_source(&server)?;
    let block_id = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x11; 32]));

    Ok(source.fetch_tree_state_for_block(block_id).await)
}

#[tokio::test]
async fn tree_state_promotes_all_final_note_commitment_roots_without_reversing_bytes()
-> eyre::Result<()> {
    let tree_state_response = json!({
        "network": "regtest",
        "height": 1,
        "hash": TREE_STATE_BLOCK_HASH_HEX,
        "time": 1_774_668_700,
        "sapling": {"commitments": {
            "finalRoot": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            "finalState": "aabb"
        }},
        "orchard": {"commitments": {
            "finalRoot": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }},
        "ironwood": {"commitments": {
            "finalRoot": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        }}
    });

    let tree_state = fetch_tree_state_response(tree_state_response.clone()).await??;
    let roots = tree_state.final_note_commitment_roots;

    assert_eq!(roots.height, BlockHeight::new(1));
    assert_eq!(roots.block_hash, BlockHash::from_bytes([0x11; 32]));
    assert_eq!(
        roots
            .sapling
            .map(zinder_core::FinalNoteCommitmentRoot::as_bytes),
        Some([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ])
    );
    assert_eq!(
        roots
            .orchard
            .map(zinder_core::FinalNoteCommitmentRoot::as_bytes),
        Some([0xaa; 32])
    );
    assert_eq!(
        roots
            .ironwood
            .map(zinder_core::FinalNoteCommitmentRoot::as_bytes),
        Some([0xff; 32])
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&tree_state.payload_bytes)?,
        json!({
            "sapling": {"commitments": {"finalState": "aabb"}}
        }),
    );
    assert_eq!(tree_state.block_time_seconds, 1_774_668_700);

    Ok(())
}

#[tokio::test]
async fn tree_state_maps_unavailable_final_note_commitment_roots_to_none() -> eyre::Result<()> {
    let tree_state = fetch_tree_state_response(json!({
        "network": "regtest",
        "height": 1,
        "hash": TREE_STATE_BLOCK_HASH_HEX,
        "time": 1_774_668_700,
        "sapling": {"commitments": {}},
        "orchard": {"commitments": {"finalRoot": null}}
    }))
    .await??;

    assert_eq!(tree_state.final_note_commitment_roots.sapling, None);
    assert_eq!(tree_state.final_note_commitment_roots.orchard, None);
    assert_eq!(tree_state.final_note_commitment_roots.ironwood, None);
    assert_eq!(tree_state.block_time_seconds, 1_774_668_700);
    assert_eq!(
        serde_json::from_slice::<Value>(&tree_state.payload_bytes)?,
        json!({}),
    );

    Ok(())
}

#[tokio::test]
async fn malformed_final_note_commitment_root_has_typed_source_error() -> eyre::Result<()> {
    let result = fetch_tree_state_response(json!({
        "height": 1,
        "hash": TREE_STATE_BLOCK_HASH_HEX,
        "sapling": {"commitments": {"finalRoot": 7}}
    }))
    .await?;

    assert!(matches!(
        result,
        Err(SourceError::MalformedFinalNoteCommitmentRoot {
            protocol: ShieldedProtocol::Sapling,
            ..
        })
    ));

    Ok(())
}

#[tokio::test]
async fn non_hex_final_note_commitment_root_has_typed_source_error() -> eyre::Result<()> {
    let result = fetch_tree_state_response(json!({
        "height": 1,
        "hash": TREE_STATE_BLOCK_HASH_HEX,
        "orchard": {"commitments": {"finalRoot": "not-hex"}}
    }))
    .await?;

    assert!(matches!(
        result,
        Err(SourceError::InvalidFinalNoteCommitmentRootHex {
            protocol: ShieldedProtocol::Orchard,
            ..
        })
    ));

    Ok(())
}

#[tokio::test]
async fn wrong_length_final_note_commitment_root_has_typed_source_error() -> eyre::Result<()> {
    let result = fetch_tree_state_response(json!({
        "height": 1,
        "hash": TREE_STATE_BLOCK_HASH_HEX,
        "ironwood": {"commitments": {"finalRoot": "abcd"}}
    }))
    .await?;

    assert!(matches!(
        result,
        Err(SourceError::InvalidFinalNoteCommitmentRootLength {
            protocol: ShieldedProtocol::Ironwood,
            byte_count: 2,
        })
    ));

    Ok(())
}

#[tokio::test]
async fn tree_state_hash_disagreement_maps_to_block_reorg_during_fetch() -> eyre::Result<()> {
    // Same shape as the header-hash test: tree-state hash disagreeing
    // with the parsed raw block hash is the second reorg-during-fetch
    // signature. Classified as UpstreamViewChanged so the loop recovers.
    let fixture = fixture_block()?;
    let server =
        JsonRpcTestServer::start([method("z_gettreestate").reply(RpcReply::result(json!({
            "network": "regtest",
            "height": fixture["height"],
            "hash": "1111111111111111111111111111111111111111111111111111111111111111",
        })))])?;
    let source = regtest_source(&server)?;
    let requested_block = BlockId::new(
        BlockHeight::new(1),
        decode_rpc_block_hash(string_field(&fixture, "hash")?)?,
    );

    let error = match source.fetch_tree_state_for_block(requested_block).await {
        Ok(tree_state) => {
            return Err(eyre!("expected fetch error, got {tree_state:?}"));
        }
        Err(error) => error,
    };

    assert!(
        matches!(
            error,
            SourceError::BlockReorgDuringFetch { height, .. } if height == BlockHeight::new(1)
        ),
        "expected BlockReorgDuringFetch, got {error:?}",
    );
    assert_eq!(
        error.upstream_classification(),
        zinder_source::SourceFailureClass::UpstreamViewChanged,
    );

    Ok(())
}

#[tokio::test]
async fn tree_state_height_mismatch_maps_to_protocol_mismatch() -> eyre::Result<()> {
    let fixture = fixture_block()?;
    let server =
        JsonRpcTestServer::start([method("z_gettreestate").reply(RpcReply::result(json!({
            "network": "regtest",
            "height": 2,
            "hash": fixture["hash"],
        })))])?;
    let source = regtest_source(&server)?;
    let requested_block = BlockId::new(
        BlockHeight::new(1),
        decode_rpc_block_hash(string_field(&fixture, "hash")?)?,
    );

    let error = match source.fetch_tree_state_for_block(requested_block).await {
        Ok(tree_state) => {
            return Err(eyre!("expected fetch error, got {tree_state:?}"));
        }
        Err(error) => error,
    };

    // Tree-state returned the wrong height for the same request id —
    // a Zebra wire-contract violation, not a reorg.
    assert!(matches!(error, SourceError::SourceProtocolMismatch { .. }));

    Ok(())
}

#[tokio::test]
async fn zebra_json_rpc_advertises_transaction_broadcast() -> eyre::Result<()> {
    let source = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        "http://127.0.0.1:18232",
        NodeAuth::None,
        Duration::from_secs(5),
    )?;

    assert!(
        source
            .capabilities()
            .supports(NodeCapability::TransactionBroadcast)
    );

    Ok(())
}

#[tokio::test]
async fn zebra_json_rpc_advertises_subtree_roots() -> eyre::Result<()> {
    let source = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        "http://127.0.0.1:18232",
        NodeAuth::None,
        Duration::from_secs(5),
    )?;

    assert!(source.capabilities().supports(NodeCapability::SubtreeRoots));

    Ok(())
}

#[tokio::test]
async fn fetch_subtree_roots_uses_expected_json_rpc_request() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("z_getsubtreesbyindex").reply(
        RpcReply::result(json!({
            "pool": "sapling",
            "start_index": 4,
            "subtrees": [
                {
                    "root": "1111111111111111111111111111111111111111111111111111111111111111",
                    "end_height": 558_822
                },
                {
                    "root": "2222222222222222222222222222222222222222222222222222222222222222",
                    "end_height": 670_209
                }
            ]
        })),
    )])?;
    let source = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        server.url(),
        NodeAuth::basic("zebra", "zebra"),
        Duration::from_secs(5),
    )?;

    let subtree_roots = source
        .fetch_subtree_roots(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(4),
            std::num::NonZeroU32::new(2).ok_or_else(|| eyre!("invalid max entries"))?,
        )
        .await?;
    let requests = server.requests_for("z_getsubtreesbyindex")?;

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].params, json!(["sapling", 4, 2]));
    assert_eq!(
        requests[0].authorization.as_deref(),
        Some("Basic emVicmE6emVicmE=")
    );
    assert_eq!(subtree_roots.protocol, ShieldedProtocol::Sapling);
    assert_eq!(subtree_roots.start_index, SubtreeRootIndex::new(4));
    assert_eq!(subtree_roots.subtree_roots.len(), 2);
    assert_eq!(
        subtree_roots.subtree_roots[0].subtree_index,
        SubtreeRootIndex::new(4)
    );
    assert_eq!(
        subtree_roots.subtree_roots[0].root_hash.as_bytes(),
        [0x11; 32]
    );
    assert_eq!(
        subtree_roots.subtree_roots[0].completing_block_height,
        BlockHeight::new(558_822)
    );
    assert_eq!(
        subtree_roots.subtree_roots[1].subtree_index,
        SubtreeRootIndex::new(5)
    );
    assert_eq!(
        subtree_roots.subtree_roots[1].root_hash.as_bytes(),
        [0x22; 32]
    );
    assert_eq!(
        subtree_roots.subtree_roots[1].completing_block_height,
        BlockHeight::new(670_209)
    );

    Ok(())
}

#[tokio::test]
async fn fetch_subtree_root_range_requires_every_requested_root() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("z_getsubtreesbyindex").reply(
        RpcReply::result(json!({
            "pool": "orchard",
            "start_index": 9,
            "subtrees": [
                {
                    "root": "3333333333333333333333333333333333333333333333333333333333333333",
                    "end_height": 1_687_104
                },
                {
                    "root": "4444444444444444444444444444444444444444444444444444444444444444",
                    "end_height": 1_689_227
                }
            ]
        })),
    )])?;
    let source = regtest_source(&server)?;
    let requested_range = SubtreeRootRange::new(
        ShieldedProtocol::Orchard,
        SubtreeRootIndex::new(9),
        std::num::NonZeroU32::new(2).ok_or_else(|| eyre!("invalid root count"))?,
    );

    let subtree_roots = source.fetch_subtree_root_range(requested_range).await?;
    let requests = server.requests_for("z_getsubtreesbyindex")?;

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].params, json!(["orchard", 9, 2]));
    assert_eq!(subtree_roots.protocol, ShieldedProtocol::Orchard);
    assert_eq!(subtree_roots.start_index, SubtreeRootIndex::new(9));
    assert_eq!(subtree_roots.subtree_roots.len(), 2);

    Ok(())
}

#[tokio::test]
async fn fetch_subtree_root_range_rejects_incomplete_response() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("z_getsubtreesbyindex").reply(
        RpcReply::result(json!({
            "pool": "sapling",
            "start_index": 4,
            "subtrees": [{
                "root": "1111111111111111111111111111111111111111111111111111111111111111",
                "end_height": 558_822
            }]
        })),
    )])?;
    let source = regtest_source(&server)?;
    let requested_range = SubtreeRootRange::new(
        ShieldedProtocol::Sapling,
        SubtreeRootIndex::new(4),
        std::num::NonZeroU32::new(2).ok_or_else(|| eyre!("invalid root count"))?,
    );

    let outcome = source.fetch_subtree_root_range(requested_range).await;

    assert!(matches!(
        outcome,
        Err(SourceError::SubtreeRootsUnavailable {
            protocol: ShieldedProtocol::Sapling,
            start_index,
            ..
        }) if start_index == SubtreeRootIndex::new(4)
    ));
    assert_eq!(server.requests_for("z_getsubtreesbyindex")?.len(), 1);

    Ok(())
}

#[tokio::test]
async fn fetch_subtree_roots_rejects_response_above_requested_bound() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("z_getsubtreesbyindex").reply(
        RpcReply::result(json!({
            "pool": "sapling",
            "start_index": 0,
            "subtrees": [
                {
                    "root": "1111111111111111111111111111111111111111111111111111111111111111",
                    "end_height": 558_822
                },
                {
                    "root": "2222222222222222222222222222222222222222222222222222222222222222",
                    "end_height": 670_209
                }
            ]
        })),
    )])?;
    let source = regtest_source(&server)?;

    let outcome = source
        .fetch_subtree_roots(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            std::num::NonZeroU32::new(1).ok_or_else(|| eyre!("invalid max entries"))?,
        )
        .await;

    assert!(matches!(
        outcome,
        Err(SourceError::SourceProtocolMismatch { .. })
    ));

    Ok(())
}

#[tokio::test]
async fn fetch_subtree_roots_rejects_descending_completion_heights() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("z_getsubtreesbyindex").reply(
        RpcReply::result(json!({
            "pool": "sapling",
            "start_index": 0,
            "subtrees": [
                {
                    "root": "1111111111111111111111111111111111111111111111111111111111111111",
                    "end_height": 670_209
                },
                {
                    "root": "2222222222222222222222222222222222222222222222222222222222222222",
                    "end_height": 558_822
                }
            ]
        })),
    )])?;
    let source = regtest_source(&server)?;

    let outcome = source
        .fetch_subtree_roots(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            std::num::NonZeroU32::new(2).ok_or_else(|| eyre!("invalid max entries"))?,
        )
        .await;

    assert!(matches!(
        outcome,
        Err(SourceError::SourceProtocolMismatch { .. })
    ));

    Ok(())
}

#[tokio::test]
async fn json_rpc_warming_up_error_marks_subtree_roots_unavailable_retryable() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([
        method("z_getsubtreesbyindex").reply(RpcReply::error_with_code(-28, "node warming up"))
    ])?;
    let source = regtest_source(&server)?;

    let error = match source
        .fetch_subtree_roots(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            std::num::NonZeroU32::new(1).ok_or_else(|| eyre!("invalid max entries"))?,
        )
        .await
    {
        Ok(subtree_roots) => {
            return Err(eyre!("expected subtree roots error, got {subtree_roots:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SourceError::SubtreeRootsUnavailable {
            protocol: ShieldedProtocol::Sapling,
            start_index,
            ..
        } if start_index == SubtreeRootIndex::new(0)
    ));
    assert_eq!(
        error.upstream_classification(),
        zinder_source::SourceFailureClass::UpstreamViewChanged,
    );

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_maps_success_to_transaction_id() -> eyre::Result<()> {
    let display_transaction_id = "1111111111111111111111111111111111111111111111111111111111111111";
    let server = JsonRpcTestServer::start([
        method("sendrawtransaction").reply(RpcReply::result(json!(display_transaction_id)))
    ])?;
    let source = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        server.url(),
        NodeAuth::basic("zebra", "zebra"),
        Duration::from_secs(5),
    )?;

    let broadcast_outcome = source
        .broadcast_transaction(RawTransactionBytes::new([0x00, 0x01, 0x02]))
        .await?;
    let requests = server.requests_for("sendrawtransaction")?;

    assert_eq!(
        broadcast_outcome,
        TransactionBroadcastOutcome::Accepted(BroadcastAccepted {
            transaction_id: TransactionId::from_bytes([0x11; 32]),
        })
    );
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].params, json!(["000102"]));
    assert_eq!(
        requests[0].authorization.as_deref(),
        Some("Basic emVicmE6emVicmE=")
    );

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_classifies_invalid_encoding() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([
        method("sendrawtransaction").reply(RpcReply::error_with_code(-22, "TX decode failed"))
    ])?;
    let source = regtest_source(&server)?;

    let broadcast_outcome = source
        .broadcast_transaction(RawTransactionBytes::new([0x00]))
        .await?;

    assert!(matches!(
        broadcast_outcome,
        TransactionBroadcastOutcome::InvalidEncoding(invalid_encoding)
            if invalid_encoding.error_code == Some(-22)
                && invalid_encoding.message == "TX decode failed"
    ));

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_classifies_duplicate() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([
            method("sendrawtransaction").reply(RpcReply::error_with_code(
                -27,
                "transaction already in mempool",
            )),
        ])?;
    let source = regtest_source(&server)?;

    let broadcast_outcome = source
        .broadcast_transaction(RawTransactionBytes::new([0x01]))
        .await?;

    assert!(matches!(
        broadcast_outcome,
        TransactionBroadcastOutcome::Duplicate(duplicate)
            if duplicate.error_code == Some(-27)
                && duplicate.message == "transaction already in mempool"
    ));

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_classifies_mempool_duplicate_message() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([
            method("sendrawtransaction").reply(RpcReply::error_with_code(
                -1,
                "transaction already exists in mempool",
            )),
        ])?;
    let source = regtest_source(&server)?;

    let broadcast_outcome = source
        .broadcast_transaction(RawTransactionBytes::new([0x01]))
        .await?;

    assert!(matches!(
        broadcast_outcome,
        TransactionBroadcastOutcome::Duplicate(duplicate)
            if duplicate.error_code == Some(-1)
                && duplicate.message == "transaction already exists in mempool"
    ));

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_classifies_state_duplicate_message() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([
            method("sendrawtransaction").reply(RpcReply::error_with_code(
                -25,
                "failed to validate tx: WtxId(\"private\"), error: transaction is already in state",
            )),
        ])?;
    let source = regtest_source(&server)?;

    let broadcast_outcome = source
        .broadcast_transaction(RawTransactionBytes::new([0x01]))
        .await?;

    assert!(matches!(
        broadcast_outcome,
        TransactionBroadcastOutcome::Duplicate(duplicate)
            if duplicate.error_code == Some(-25)
                && duplicate.message.ends_with("transaction is already in state")
    ));

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_does_not_classify_unknown_as_duplicate() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("sendrawtransaction")
        .reply(RpcReply::error_with_code(-8, "transaction unknown to node"))])?;
    let source = regtest_source(&server)?;

    let broadcast_outcome = source
        .broadcast_transaction(RawTransactionBytes::new([0x01]))
        .await?;

    assert!(matches!(
        broadcast_outcome,
        TransactionBroadcastOutcome::Rejected(rejected)
            if rejected.error_code == Some(-8)
                && rejected.message == "transaction unknown to node"
    ));

    Ok(())
}

#[tokio::test]
async fn broadcast_transaction_without_error_code_returns_unknown() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("sendrawtransaction").reply(
        RpcReply::error_without_code("duplicate field contains a hex branch id already checked"),
    )])?;
    let source = regtest_source(&server)?;

    let broadcast_outcome = source
        .broadcast_transaction(RawTransactionBytes::new([0x01]))
        .await?;

    // jsonrpsee's ErrorObject requires a code; an error without one comes
    // through as a rejected/unclassified result via the broadcast classifier.
    assert!(matches!(
        broadcast_outcome,
        TransactionBroadcastOutcome::Rejected(_) | TransactionBroadcastOutcome::Unknown(_),
    ));

    Ok(())
}

#[tokio::test]
async fn probe_capabilities_parses_openrpc_method_list() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("rpc.discover").reply(RpcReply::result(json!({
            "openrpc": "1.3.2",
            "info": {"title": "Zebra", "version": "8.0.0"},
            "methods": [
                {"name": "getblock"},
                {"name": "getbestblockheightandhash"},
                {"name": "z_gettreestate"},
                {"name": "z_getsubtreesbyindex"},
                {"name": "sendrawtransaction"},
                {"name": "getblockchaininfo"},
                {"name": "rpc.discover"},
                {"name": "ping"},
            ],
        })))])?;
    let source = regtest_source(&server)?;

    let probed = source.probe_capabilities().await?;

    assert!(probed.supports(NodeCapability::JsonRpc));
    assert!(probed.supports(NodeCapability::OpenRpcDiscovery));
    assert!(probed.supports(NodeCapability::BestChainBlocks));
    assert!(probed.supports(NodeCapability::TipId));
    assert!(probed.supports(NodeCapability::TreeState));
    assert!(probed.supports(NodeCapability::SubtreeRoots));
    assert!(probed.supports(NodeCapability::TransactionBroadcast));
    assert!(probed.supports(NodeCapability::ChainValuePools));
    assert_eq!(source.capabilities(), probed);

    Ok(())
}

#[tokio::test]
async fn probe_capabilities_rejects_missing_openrpc_discovery() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([
        method("rpc.discover").reply(RpcReply::error_with_code(-32601, "Method not found"))
    ])?;
    let source = regtest_source(&server)?;

    let error = match source.probe_capabilities().await {
        Ok(capabilities) => {
            return Err(eyre::eyre!(
                "a Zebra source without OpenRPC discovery must fail closed, got {capabilities:?}"
            ));
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SourceError::NodeCapabilityMissing {
            capability: NodeCapability::OpenRpcDiscovery,
        }
    ));

    Ok(())
}

#[tokio::test]
async fn probe_capabilities_requires_atomic_tip_method() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("rpc.discover").reply(RpcReply::result(json!({
            "openrpc": "1.3.2",
            "methods": [
                {"name": "getbestblockhash"},
                {"name": "getblockheader"},
                {"name": "rpc.discover"},
            ],
        })))])?;
    let source = regtest_source(&server)?;

    let probed = source.probe_capabilities().await?;

    assert!(!probed.supports(NodeCapability::TipId));
    assert!(probed.supports(NodeCapability::OpenRpcDiscovery));
    assert!(!probed.supports(NodeCapability::BestChainBlocks));
    assert!(!probed.supports(NodeCapability::TreeState));
    assert!(!probed.supports(NodeCapability::SubtreeRoots));
    assert!(!probed.supports(NodeCapability::TransactionBroadcast));
    assert!(!probed.supports(NodeCapability::ChainValuePools));

    Ok(())
}

#[tokio::test]
async fn probe_capabilities_keeps_only_advertised_capabilities_on_success() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("rpc.discover").reply(RpcReply::result(json!({
            "openrpc": "1.3.2",
            "methods": [
                {"name": "getbestblockheightandhash"},
                {"name": "rpc.discover"},
            ],
        })))])?;
    let source = regtest_source(&server)?;

    let probed = source.probe_capabilities().await?;

    assert!(probed.supports(NodeCapability::TipId));
    assert!(probed.supports(NodeCapability::OpenRpcDiscovery));
    assert!(!probed.supports(NodeCapability::BestChainBlocks));
    assert!(!probed.supports(NodeCapability::TreeState));
    assert!(!probed.supports(NodeCapability::SubtreeRoots));
    assert!(!probed.supports(NodeCapability::TransactionBroadcast));
    assert!(!probed.supports(NodeCapability::ChainValuePools));

    Ok(())
}

fn discover_reply_with_getblockchaininfo() -> RpcReply {
    RpcReply::result(json!({
        "openrpc": "1.3.2",
        "methods": [
            {"name": "getblock"},
            {"name": "getbestblockheightandhash"},
            {"name": "z_gettreestate"},
            {"name": "z_getsubtreesbyindex"},
            {"name": "getblockchaininfo"},
            {"name": "rpc.discover"},
        ],
    }))
}

async fn probe_with_blockchain_info_reply(
    blockchain_info: RpcReply,
) -> eyre::Result<zinder_source::NodeCapabilities> {
    let server = JsonRpcTestServer::start([
        method("rpc.discover").reply(discover_reply_with_getblockchaininfo()),
        method("getblockchaininfo").reply(blockchain_info),
    ])?;
    let source = regtest_source(&server)?;
    Ok(source.probe_capabilities().await?)
}

#[tokio::test]
async fn probe_capabilities_grants_historical_block_bodies_for_archive_node() -> eyre::Result<()> {
    let probed =
        probe_with_blockchain_info_reply(RpcReply::result(json!({"blocks": 10, "pruned": false})))
            .await?;

    assert!(probed.supports(NodeCapability::HistoricalBlockBodies));
    assert!(probed.supports(NodeCapability::ChainValuePools));

    Ok(())
}

#[tokio::test]
async fn probe_capabilities_withholds_historical_block_bodies_for_pruned_node() -> eyre::Result<()>
{
    let probed = probe_with_blockchain_info_reply(RpcReply::result(json!({
        "blocks": 20_000,
        "pruned": true,
        "pruneheight": 10_000,
    })))
    .await?;

    assert!(!probed.supports(NodeCapability::HistoricalBlockBodies));
    assert!(probed.supports(NodeCapability::TreeState));

    Ok(())
}

#[tokio::test]
async fn probe_capabilities_withholds_historical_block_bodies_without_pruned_field()
-> eyre::Result<()> {
    let probed = probe_with_blockchain_info_reply(RpcReply::result(json!({"blocks": 10}))).await?;

    assert!(!probed.supports(NodeCapability::HistoricalBlockBodies));

    Ok(())
}

#[tokio::test]
async fn probe_capabilities_withholds_historical_block_bodies_when_storage_call_fails()
-> eyre::Result<()> {
    let probed =
        probe_with_blockchain_info_reply(RpcReply::error_with_code(-28, "node warming up")).await?;

    assert!(!probed.supports(NodeCapability::HistoricalBlockBodies));
    assert!(probed.supports(NodeCapability::TipId));

    Ok(())
}

#[tokio::test]
async fn probe_capabilities_skips_storage_call_when_getblockchaininfo_is_not_advertised()
-> eyre::Result<()> {
    let server = JsonRpcTestServer::start([
        method("rpc.discover").reply(RpcReply::result(json!({
            "openrpc": "1.3.2",
            "methods": [
                {"name": "getblock"},
                {"name": "getbestblockheightandhash"},
                {"name": "rpc.discover"},
            ],
        }))),
        method("getblockchaininfo").reply(RpcReply::result(json!({"pruned": false}))),
    ])?;
    let source = regtest_source(&server)?;

    let probed = source.probe_capabilities().await?;

    assert!(!probed.supports(NodeCapability::HistoricalBlockBodies));
    assert!(server.requests_for("getblockchaininfo")?.is_empty());

    Ok(())
}

/// `zakura_state::HistoricalTreeUnavailable` as Zakura v1.8.6 renders it.
const ZAKURA_FAST_SYNCED_TREE_STATE_MESSAGE: &str = "historical note commitment tree at Height(1000000) is unavailable: this node was fast-synced with verified commitment trees, so per-height trees below the last checkpoint Height(3418406) were never written";

/// `zakura_state::HistoricalSubtreeUnavailable` with the `NotStored` reason.
const ZAKURA_SUBTREE_NOT_STORED_MESSAGE: &str = "historical sapling note commitment subtree NoteCommitmentSubtreeIndex(0) is unavailable at last checkpoint Height(3418406): this node did not record the subtree and continuing synchronization will not restore it; use another node";

/// `zakura_state::HistoricalSubtreeUnavailable` with the `Indeterminate` reason.
const ZAKURA_SUBTREE_INDETERMINATE_MESSAGE: &str = "historical sapling note commitment subtree NoteCommitmentSubtreeIndex(0) is unavailable at last checkpoint Height(3418406): this node has not reached the last checkpoint, so it cannot yet tell whether the subtree was skipped; subtrees completed below the last checkpoint are never recorded";

#[tokio::test]
async fn fast_synced_tree_state_refusal_is_a_missing_capability() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("z_gettreestate").reply(
        RpcReply::error_with_code(-1, ZAKURA_FAST_SYNCED_TREE_STATE_MESSAGE),
    )])?;
    let source = regtest_source(&server)?;
    let block_id = BlockId::new(BlockHeight::new(1_000_000), BlockHash::from_bytes([1; 32]));

    let error = match source.fetch_tree_state_for_block(block_id).await {
        Ok(tree_state) => return Err(eyre!("expected tree-state error, got {tree_state:?}")),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SourceError::HistoricalTreeStateUnavailable { height, .. }
            if height == BlockHeight::new(1_000_000)
    ));
    assert_eq!(
        error.upstream_classification(),
        zinder_source::SourceFailureClass::CapabilityMissing,
    );
    assert!(error.is_node_capability_missing());

    Ok(())
}

#[tokio::test]
async fn fast_synced_checkpoint_refusal_is_a_missing_capability() -> eyre::Result<()> {
    let server = JsonRpcTestServer::start([method("z_gettreestate").reply(
        RpcReply::error_with_code(-1, ZAKURA_FAST_SYNCED_TREE_STATE_MESSAGE),
    )])?;
    let source = regtest_source(&server)?;

    let error = match source
        .fetch_chain_checkpoint(BlockHeight::new(100), &checkpoint_activations(1, 1)?)
        .await
    {
        Ok(checkpoint) => return Err(eyre!("expected checkpoint error, got {checkpoint:?}")),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SourceError::HistoricalTreeStateUnavailable { height, .. }
            if height == BlockHeight::new(100)
    ));
    assert!(error.is_node_capability_missing());

    Ok(())
}

async fn subtree_roots_error(message: &str) -> eyre::Result<SourceError> {
    let server = JsonRpcTestServer::start([
        method("z_getsubtreesbyindex").reply(RpcReply::error_with_code(-1, message))
    ])?;
    let source = regtest_source(&server)?;

    match source
        .fetch_subtree_roots(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            std::num::NonZeroU32::new(1).ok_or_else(|| eyre!("invalid max entries"))?,
        )
        .await
    {
        Ok(subtree_roots) => Err(eyre!("expected subtree roots error, got {subtree_roots:?}")),
        Err(error) => Ok(error),
    }
}

#[tokio::test]
async fn not_stored_subtree_refusal_is_a_missing_capability() -> eyre::Result<()> {
    let error = subtree_roots_error(ZAKURA_SUBTREE_NOT_STORED_MESSAGE).await?;

    assert!(matches!(
        error,
        SourceError::HistoricalSubtreeRootsUnavailable {
            protocol: ShieldedProtocol::Sapling,
            start_index,
            ..
        } if start_index == SubtreeRootIndex::new(0)
    ));
    assert_eq!(
        error.upstream_classification(),
        zinder_source::SourceFailureClass::CapabilityMissing,
    );
    assert!(error.is_node_capability_missing());

    Ok(())
}

#[tokio::test]
async fn indeterminate_subtree_refusal_stays_a_changed_upstream_view() -> eyre::Result<()> {
    let error = subtree_roots_error(ZAKURA_SUBTREE_INDETERMINATE_MESSAGE).await?;

    assert!(matches!(
        error,
        SourceError::SubtreeRootsUnavailable {
            protocol: ShieldedProtocol::Sapling,
            ..
        }
    ));
    assert_eq!(
        error.upstream_classification(),
        zinder_source::SourceFailureClass::UpstreamViewChanged,
    );
    assert!(!error.is_node_capability_missing());

    Ok(())
}

const CHECKPOINT_BLOCK_HASH_HEX: &str =
    "010101010101010101010101010101010101010101010101010101010101010f";
const ONE_LEAF_SAPLING_FINAL_STATE_HEX: &str = "010100000000000000000000000000000000000000000000000000000000000000001f00000000000000000000000000000000000000000000000000000000000000";

fn checkpoint_activations(
    sapling_height: u32,
    orchard_height: u32,
) -> eyre::Result<NetworkUpgradeActivations> {
    checkpoint_activations_with_ironwood(sapling_height, orchard_height, None)
}

fn checkpoint_activations_with_ironwood(
    sapling_height: u32,
    orchard_height: u32,
    ironwood_height: Option<u32>,
) -> eyre::Result<NetworkUpgradeActivations> {
    let mut activations = vec![
        NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(1),
            activation_height: BlockHeight::new(sapling_height),
            name: "Sapling".to_owned(),
        },
        NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(2),
            activation_height: BlockHeight::new(orchard_height),
            name: "NU5".to_owned(),
        },
    ];
    if let Some(ironwood_height) = ironwood_height {
        activations.push(NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(3),
            activation_height: BlockHeight::new(ironwood_height),
            name: "NU6.3".to_owned(),
        });
    }
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        activations,
    )?)
}

fn sapling_frontier_rpc_parts(
    frontier: &Frontier<SaplingNode, 32>,
) -> eyre::Result<(String, String)> {
    let tree = CommitmentTree::from_frontier(frontier);
    let mut final_state_bytes = Vec::new();
    write_commitment_tree(&tree, &mut final_state_bytes)?;
    let mut final_root_bytes = frontier.root().to_bytes();
    final_root_bytes.reverse();
    Ok((
        hex::encode(final_root_bytes),
        hex::encode(final_state_bytes),
    ))
}

fn empty_sapling_frontier_rpc_parts() -> eyre::Result<(String, String)> {
    sapling_frontier_rpc_parts(&Frontier::empty())
}

fn one_leaf_sapling_frontier_rpc_parts() -> eyre::Result<(String, String)> {
    let leaf_bytes = {
        let mut bytes = [0; 32];
        bytes[0] = 1;
        bytes
    };
    let leaf = Option::<SaplingNode>::from(SaplingNode::from_bytes(leaf_bytes))
        .ok_or_else(|| eyre!("one is a canonical Sapling field element"))?;
    let frontier: Frontier<SaplingNode, 32> =
        Frontier::from_parts(Position::from(0), leaf, Vec::new())
            .map_err(|error| eyre!("valid one-leaf frontier rejected: {error:?}"))?;
    sapling_frontier_rpc_parts(&frontier)
}

fn one_leaf_orchard_frontier_rpc_parts(leaf_byte: u8) -> eyre::Result<(String, String)> {
    let leaf_bytes = {
        let mut bytes = [0; 32];
        bytes[0] = leaf_byte;
        bytes
    };
    let leaf = Option::<MerkleHashOrchard>::from(MerkleHashOrchard::from_bytes(&leaf_bytes))
        .ok_or_else(|| eyre!("small integers are canonical Orchard field elements"))?;
    let frontier: Frontier<MerkleHashOrchard, 32> =
        Frontier::from_parts(Position::from(0), leaf, Vec::new())
            .map_err(|error| eyre!("valid one-leaf frontier rejected: {error:?}"))?;
    let tree = CommitmentTree::from_frontier(&frontier);
    let mut final_state_bytes = Vec::new();
    write_commitment_tree(&tree, &mut final_state_bytes)?;
    Ok((
        hex::encode(frontier.root().to_bytes()),
        hex::encode(final_state_bytes),
    ))
}

fn full_sapling_frontier_state_hex() -> eyre::Result<String> {
    let leaf_bytes = {
        let mut bytes = [0; 32];
        bytes[0] = 1;
        bytes
    };
    let leaf = Option::<SaplingNode>::from(SaplingNode::from_bytes(leaf_bytes))
        .ok_or_else(|| eyre!("one is a canonical Sapling field element"))?;
    let tree =
        CommitmentTree::<SaplingNode, 32>::from_parts(Some(leaf), Some(leaf), vec![Some(leaf); 31])
            .map_err(|()| eyre!("depth-32 legacy tree accepts 31 parent slots"))?;
    let mut final_state_bytes = Vec::new();
    write_commitment_tree(&tree, &mut final_state_bytes)?;
    Ok(hex::encode(final_state_bytes))
}

fn checkpoint_response(final_root: Option<&str>, final_state: Option<&str>) -> Value {
    json!({
        "height": 100,
        "hash": CHECKPOINT_BLOCK_HASH_HEX,
        "time": 1_774_668_700,
        "sapling": {"commitments": {
            "finalRoot": final_root,
            "finalState": final_state,
        }},
        "orchard": {"commitments": {}},
    })
}

fn checkpoint_response_with_all_frontiers(
    sapling: (&str, &str),
    orchard: (&str, &str),
    ironwood: (&str, &str),
) -> Value {
    json!({
        "height": 100,
        "hash": CHECKPOINT_BLOCK_HASH_HEX,
        "time": 1_774_668_700,
        "sapling": {"commitments": {
            "finalRoot": sapling.0,
            "finalState": sapling.1,
        }},
        "orchard": {"commitments": {
            "finalRoot": orchard.0,
            "finalState": orchard.1,
        }},
        "ironwood": {"commitments": {
            "finalRoot": ironwood.0,
            "finalState": ironwood.1,
        }},
    })
}

async fn fetch_checkpoint_response(
    response: Value,
    activations: &NetworkUpgradeActivations,
) -> eyre::Result<Result<zinder_core::CommitmentTreeCheckpoint, SourceError>> {
    let server =
        JsonRpcTestServer::start([method("z_gettreestate").reply(RpcReply::result(response))])?;
    let source = regtest_source(&server)?;
    Ok(source
        .fetch_chain_checkpoint(BlockHeight::new(100), activations)
        .await)
}

#[tokio::test]
async fn fetch_chain_checkpoint_uses_one_tree_state_request_and_decodes_nonempty_frontier()
-> eyre::Result<()> {
    let (final_root, final_state) = one_leaf_sapling_frontier_rpc_parts()?;
    assert_eq!(final_state, ONE_LEAF_SAPLING_FINAL_STATE_HEX);
    let server = JsonRpcTestServer::start([method("z_gettreestate").reply(RpcReply::result(
        checkpoint_response(Some(&final_root), Some(&final_state)),
    ))])?;
    let source = regtest_source(&server)?;
    let activations = checkpoint_activations(1, 200)?;

    let checkpoint = source
        .fetch_chain_checkpoint(BlockHeight::new(100), &activations)
        .await?;

    assert_eq!(checkpoint.block_id.height, BlockHeight::new(100));
    assert_eq!(checkpoint.block_time_seconds, 1_774_668_700);
    assert_eq!(
        checkpoint.block_id.hash,
        decode_rpc_block_hash(CHECKPOINT_BLOCK_HASH_HEX)?
    );
    assert_eq!(checkpoint.tip_metadata().sapling_commitment_tree_size, 1);
    assert_eq!(
        checkpoint
            .frontiers
            .sapling()
            .ok_or_else(|| eyre!("sapling frontier must be present"))?
            .final_state_bytes(),
        hex::decode(ONE_LEAF_SAPLING_FINAL_STATE_HEX)?,
    );
    assert_eq!(server.requests()?.len(), 1);
    assert_eq!(
        server.requests_for("z_gettreestate")?[0].params,
        json!(["100"])
    );
    assert!(server.requests_for("getblock")?.is_empty());
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_accepts_active_empty_canonical_frontier() -> eyre::Result<()> {
    let (final_root, final_state) = empty_sapling_frontier_rpc_parts()?;
    assert_eq!(final_state, "000000");
    let checkpoint = fetch_checkpoint_response(
        checkpoint_response(Some(&final_root), Some(&final_state)),
        &checkpoint_activations(1, 200)?,
    )
    .await??;

    assert_eq!(checkpoint.tip_metadata().sapling_commitment_tree_size, 0);
    assert!(checkpoint.frontiers.sapling().is_some());
    assert!(checkpoint.frontiers.orchard().is_none());
    assert!(checkpoint.frontiers.ironwood().is_none());
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_accepts_schedule_with_post_canopy_pools_disabled()
-> eyre::Result<()> {
    let activations = NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        vec![NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(1),
            activation_height: BlockHeight::new(1),
            name: "Sapling".to_owned(),
        }],
    )?;
    let (final_root, final_state) = empty_sapling_frontier_rpc_parts()?;

    let checkpoint = fetch_checkpoint_response(
        checkpoint_response(Some(&final_root), Some(&final_state)),
        &activations,
    )
    .await??;

    assert!(checkpoint.frontiers.sapling().is_some());
    assert!(checkpoint.frontiers.orchard().is_none());
    assert!(checkpoint.frontiers.ironwood().is_none());
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_decodes_same_height_pool_activations_independently()
-> eyre::Result<()> {
    let sapling = one_leaf_sapling_frontier_rpc_parts()?;
    let orchard = one_leaf_orchard_frontier_rpc_parts(1)?;
    let ironwood = one_leaf_orchard_frontier_rpc_parts(2)?;
    let response = checkpoint_response_with_all_frontiers(
        (&sapling.0, &sapling.1),
        (&orchard.0, &orchard.1),
        (&ironwood.0, &ironwood.1),
    );
    let checkpoint = fetch_checkpoint_response(
        response,
        &checkpoint_activations_with_ironwood(1, 1, Some(1))?,
    )
    .await??;

    assert_eq!(checkpoint.tip_metadata().sapling_commitment_tree_size, 1);
    assert_eq!(checkpoint.tip_metadata().orchard_commitment_tree_size, 1);
    assert_eq!(checkpoint.tip_metadata().ironwood_commitment_tree_size, 1);
    assert_eq!(
        checkpoint
            .frontiers
            .orchard()
            .ok_or_else(|| eyre!("orchard frontier must be present"))?
            .final_root()
            .as_bytes(),
        <[u8; 32]>::try_from(hex::decode(&orchard.0)?.as_slice())?,
        "Orchard finalRoot bytes must stay in direct RPC order",
    );
    assert_eq!(
        checkpoint
            .frontiers
            .ironwood()
            .ok_or_else(|| eyre!("ironwood frontier must be present"))?
            .final_state_bytes(),
        hex::decode(&ironwood.1)?,
    );
    assert_ne!(
        checkpoint
            .frontiers
            .orchard()
            .ok_or_else(|| eyre!("orchard frontier must be present"))?
            .final_root(),
        checkpoint
            .frontiers
            .ironwood()
            .ok_or_else(|| eyre!("ironwood frontier must be present"))?
            .final_root(),
        "Orchard and Ironwood use independent frontiers",
    );
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_rejects_height_mismatch() -> eyre::Result<()> {
    let (final_root, final_state) = empty_sapling_frontier_rpc_parts()?;
    let mut response = checkpoint_response(Some(&final_root), Some(&final_state));
    response["height"] = json!(99);
    let outcome = fetch_checkpoint_response(response, &checkpoint_activations(1, 200)?).await?;
    assert!(matches!(
        outcome,
        Err(SourceError::SourceProtocolMismatch { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_rejects_one_sided_frontier() -> eyre::Result<()> {
    let (final_root, _) = empty_sapling_frontier_rpc_parts()?;
    let outcome = fetch_checkpoint_response(
        checkpoint_response(Some(&final_root), None),
        &checkpoint_activations(1, 200)?,
    )
    .await?;
    assert!(matches!(
        outcome,
        Err(SourceError::MalformedCommitmentTreeFrontier {
            protocol: ShieldedProtocol::Sapling,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_rejects_trailing_frontier_bytes() -> eyre::Result<()> {
    let (final_root, final_state) = empty_sapling_frontier_rpc_parts()?;
    let outcome = fetch_checkpoint_response(
        checkpoint_response(Some(&final_root), Some(&format!("{final_state}00"))),
        &checkpoint_activations(1, 200)?,
    )
    .await?;
    assert!(matches!(
        outcome,
        Err(SourceError::InvalidCommitmentTreeFrontierEncoding {
            protocol: ShieldedProtocol::Sapling,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_rejects_frontier_root_mismatch() -> eyre::Result<()> {
    let (_, final_state) = empty_sapling_frontier_rpc_parts()?;
    let wrong_root = hex::encode([0; 32]);
    let outcome = fetch_checkpoint_response(
        checkpoint_response(Some(&wrong_root), Some(&final_state)),
        &checkpoint_activations(1, 200)?,
    )
    .await?;
    assert!(matches!(
        outcome,
        Err(SourceError::CommitmentTreeFrontierRootMismatch {
            protocol: ShieldedProtocol::Sapling,
        })
    ));
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_rejects_tree_size_outside_u32_contract() -> eyre::Result<()> {
    let final_state = full_sapling_frontier_state_hex()?;
    let placeholder_root = hex::encode([0; 32]);
    let outcome = fetch_checkpoint_response(
        checkpoint_response(Some(&placeholder_root), Some(&final_state)),
        &checkpoint_activations(1, 200)?,
    )
    .await?;
    assert!(matches!(
        outcome,
        Err(SourceError::CommitmentTreeSizeOutOfRange {
            protocol: ShieldedProtocol::Sapling,
            tree_size: 4_294_967_296,
        })
    ));
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_rejects_inactive_present_frontier() -> eyre::Result<()> {
    let (final_root, final_state) = empty_sapling_frontier_rpc_parts()?;
    let outcome = fetch_checkpoint_response(
        checkpoint_response(Some(&final_root), Some(&final_state)),
        &checkpoint_activations(200, 300)?,
    )
    .await?;
    assert!(matches!(
        outcome,
        Err(SourceError::CommitmentTreeFrontierActivationMismatch {
            protocol: ShieldedProtocol::Sapling,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_rejects_active_missing_frontier() -> eyre::Result<()> {
    let outcome = fetch_checkpoint_response(
        checkpoint_response(None, None),
        &checkpoint_activations(1, 200)?,
    )
    .await?;
    assert!(matches!(
        outcome,
        Err(SourceError::CommitmentTreeFrontierActivationMismatch {
            protocol: ShieldedProtocol::Sapling,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_rejects_activation_table_without_required_upgrades()
-> eyre::Result<()> {
    let activations = NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        vec![NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(2),
            activation_height: BlockHeight::new(200),
            name: "NU5".to_owned(),
        }],
    )?;
    let outcome = fetch_checkpoint_response(json!({}), &activations).await?;
    assert!(matches!(
        outcome,
        Err(SourceError::SourceProtocolMismatch { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn fetch_chain_checkpoint_rejects_activation_table_for_another_network_before_rpc()
-> eyre::Result<()> {
    let activations = NetworkUpgradeActivations::new(
        Network::ZcashTestnet,
        vec![
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(1),
                activation_height: BlockHeight::new(1),
                name: "Sapling".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(2),
                activation_height: BlockHeight::new(1),
                name: "NU5".to_owned(),
            },
        ],
    )?;
    let server =
        JsonRpcTestServer::start([method("z_gettreestate").reply(RpcReply::result(json!({})))])?;
    let source = regtest_source(&server)?;

    let outcome = source
        .fetch_chain_checkpoint(BlockHeight::new(100), &activations)
        .await;

    assert!(matches!(
        outcome,
        Err(SourceError::SourceProtocolMismatch { .. })
    ));
    assert!(server.requests()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn fetch_network_upgrade_activations_parses_getblockchaininfo_upgrades() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("getblockchaininfo").reply(RpcReply::result(json!({
            "upgrades": {
                "76b809bb": {
                    "name": "Sapling",
                    "activationheight": 280_000,
                    "status": "active"
                },
                "c2d6d0b4": {
                    "name": "NU5",
                    "activationheight": 1_842_420,
                    "status": "active"
                }
            }
        })))])?;
    let source = ZebraJsonRpcSource::new(
        Network::ZcashTestnet,
        server.url(),
        NodeAuth::None,
        Duration::from_secs(5),
    )?;

    let activations = source.fetch_network_upgrade_activations().await?;

    assert_eq!(activations.network(), Network::ZcashTestnet);
    assert_eq!(
        activations.activation_height_by_name("Sapling"),
        Some(zinder_core::BlockHeight::new(280_000))
    );
    assert_eq!(
        activations.activation_height_by_name("NU5"),
        Some(zinder_core::BlockHeight::new(1_842_420))
    );
    assert_eq!(
        activations
            .activation_height_by_branch_id(zinder_core::ConsensusBranchId::new(0x76b8_09bb)),
        Some(zinder_core::BlockHeight::new(280_000))
    );
    assert_eq!(
        activations.consensus_branch_id_at(zinder_core::BlockHeight::new(2_000_000)),
        zinder_core::ConsensusBranchId::new(0xc2d6_d0b4)
    );
    Ok(())
}

#[tokio::test]
async fn fetch_network_upgrade_activations_activates_nu6_2() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("getblockchaininfo").reply(RpcReply::result(json!({
            "upgrades": {
                "c8e71055": {
                    "name": "NU6",
                    "activationheight": 2_976_000,
                    "status": "active"
                },
                "5437f330": {
                    "name": "NU6.2",
                    "activationheight": 3_146_400,
                    "status": "active"
                }
            }
        })))])?;
    let source = ZebraJsonRpcSource::new(
        Network::ZcashTestnet,
        server.url(),
        NodeAuth::None,
        Duration::from_secs(5),
    )?;

    let activations = source.fetch_network_upgrade_activations().await?;

    assert_eq!(
        activations.activation_height_by_name("NU6.2"),
        Some(zinder_core::BlockHeight::new(3_146_400))
    );
    assert_eq!(
        activations
            .activation_height_by_branch_id(zinder_core::ConsensusBranchId::new(0x5437_f330)),
        Some(zinder_core::BlockHeight::new(3_146_400))
    );
    assert_eq!(
        activations.consensus_branch_id_at(zinder_core::BlockHeight::new(3_146_400)),
        zinder_core::ConsensusBranchId::new(0x5437_f330)
    );
    assert_eq!(
        activations.consensus_branch_id_at(zinder_core::BlockHeight::new(3_146_399)),
        zinder_core::ConsensusBranchId::new(0xc8e7_1055)
    );
    Ok(())
}

#[tokio::test]
async fn fetch_chain_value_pools_at_tip_preserves_source_tip_and_pool_entries() -> eyre::Result<()>
{
    let server =
        JsonRpcTestServer::start([method("getblockchaininfo").reply(RpcReply::result(json!({
            "blocks": 1_234_567,
            "bestblockhash": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            "valuePools": [
                {
                    "id": "transparent",
                    "monitored": true,
                    "chainValueZat": 1_000_000
                },
                {
                    "id": "sapling",
                    "monitored": true,
                    "chainValueZat": 2_000_000
                },
                {
                    "id": "lockbox",
                    "monitored": false
                }
            ]
        })))])?;
    let source = regtest_source(&server)?;

    let value_pools = source.fetch_chain_value_pools_at_tip().await?;

    assert_eq!(
        value_pools.source_tip,
        BlockId::new(
            BlockHeight::new(1_234_567),
            BlockHash::from_bytes([
                0x1f, 0x1e, 0x1d, 0x1c, 0x1b, 0x1a, 0x19, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12,
                0x11, 0x10, 0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04,
                0x03, 0x02, 0x01, 0x00,
            ])
        )
    );
    assert_eq!(value_pools.pools.len(), 3);
    assert_eq!(value_pools.pools[0].id, "transparent");
    assert!(value_pools.pools[0].monitored);
    assert_eq!(value_pools.pools[0].chain_value_zat, Some(1_000_000));
    assert_eq!(value_pools.pools[1].id, "sapling");
    assert!(value_pools.pools[1].monitored);
    assert_eq!(value_pools.pools[1].chain_value_zat, Some(2_000_000));
    assert_eq!(value_pools.pools[2].id, "lockbox");
    assert!(!value_pools.pools[2].monitored);
    assert_eq!(value_pools.pools[2].chain_value_zat, None);
    Ok(())
}

#[tokio::test]
async fn fetch_chain_value_pools_at_tip_requires_value_pools_field() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("getblockchaininfo").reply(RpcReply::result(json!({
            "blocks": 1_234_567,
            "bestblockhash": "ab".repeat(32)
        })))])?;
    let source = regtest_source(&server)?;

    let outcome = source.fetch_chain_value_pools_at_tip().await;

    assert!(matches!(
        outcome,
        Err(SourceError::NodeCapabilityMissing {
            capability: NodeCapability::ChainValuePools
        })
    ));
    Ok(())
}

#[tokio::test]
async fn fetch_block_value_pool_balances_binds_exact_block_and_preserves_pool_order()
-> eyre::Result<()> {
    let block_hash_hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let block_id = BlockId::new(BlockHeight::new(42), decode_rpc_block_hash(block_hash_hex)?);
    let server = JsonRpcTestServer::start([method("getblock").reply(RpcReply::result(json!({
        "hash": block_hash_hex,
        "height": 42,
        "time": 1_774_668_700,
        "valuePools": [
            {"id": "transparent", "monitored": true, "chainValueZat": 11},
            {"id": "future-pool", "monitored": false},
            {"id": "orchard", "monitored": true, "chainValueZat": 33}
        ]
    })))])?;
    let source = regtest_source(&server)?;
    assert!(
        source
            .capabilities()
            .supports(NodeCapability::BlockValuePoolBalances)
    );

    let balances = source.fetch_block_value_pool_balances(block_id).await?;

    assert_eq!(balances.block_id, block_id);
    assert_eq!(balances.block_time_seconds, 1_774_668_700);
    assert_eq!(
        balances
            .pools
            .iter()
            .map(|pool| pool.id.as_str())
            .collect::<Vec<_>>(),
        vec!["transparent", "future-pool", "orchard"]
    );
    assert_eq!(balances.pools[0].value_zat, Some(11));
    assert!(!balances.pools[1].monitored);
    assert_eq!(balances.pools[1].value_zat, None);
    assert_eq!(balances.pools[2].value_zat, Some(33));
    assert!(
        source
            .capabilities()
            .supports(NodeCapability::BlockValuePoolBalances)
    );
    assert_eq!(
        server.requests_for("getblock")?[0].params,
        json!([block_hash_hex, 1])
    );

    Ok(())
}

#[tokio::test]
async fn fetch_block_value_pool_balances_rejects_response_identity_mismatches() -> eyre::Result<()>
{
    let requested_hash_hex = "01".repeat(32);
    let block_id = BlockId::new(
        BlockHeight::new(42),
        decode_rpc_block_hash(&requested_hash_hex)?,
    );
    for response in [
        json!({
            "hash": requested_hash_hex,
            "height": 43,
            "time": 1,
            "valuePools": [{"id": "transparent", "monitored": true, "chainValueZat": 1}]
        }),
        json!({
            "hash": "02".repeat(32),
            "height": 42,
            "time": 1,
            "valuePools": [{"id": "transparent", "monitored": true, "chainValueZat": 1}]
        }),
    ] {
        let server =
            JsonRpcTestServer::start([method("getblock").reply(RpcReply::result(response))])?;
        let source = regtest_source(&server)?;

        assert!(matches!(
            source.fetch_block_value_pool_balances(block_id).await,
            Err(SourceError::SourceProtocolMismatch { .. })
        ));
    }

    Ok(())
}

#[tokio::test]
async fn fetch_block_value_pool_balances_rejects_invalid_pool_entries() -> eyre::Result<()> {
    let block_hash_hex = "03".repeat(32);
    let block_id = BlockId::new(
        BlockHeight::new(42),
        decode_rpc_block_hash(&block_hash_hex)?,
    );
    for value_pools in [
        json!([{"id": "", "monitored": true, "chainValueZat": 1}]),
        json!([
            {"id": "sapling", "monitored": true, "chainValueZat": 1},
            {"id": "sapling", "monitored": false}
        ]),
        json!([{"id": "orchard", "monitored": true, "chainValueZat": -1}]),
    ] {
        let server =
            JsonRpcTestServer::start([method("getblock").reply(RpcReply::result(json!({
                "hash": block_hash_hex,
                "height": 42,
                "time": 1,
                "valuePools": value_pools
            })))])?;
        let source = regtest_source(&server)?;

        assert!(matches!(
            source.fetch_block_value_pool_balances(block_id).await,
            Err(SourceError::SourceProtocolMismatch { .. })
        ));
        assert!(
            source
                .capabilities()
                .supports(NodeCapability::BlockValuePoolBalances)
        );
    }

    Ok(())
}

#[tokio::test]
async fn fetch_block_value_pool_balances_requires_verbose_value_pools() -> eyre::Result<()> {
    let block_hash_hex = "04".repeat(32);
    let block_id = BlockId::new(
        BlockHeight::new(42),
        decode_rpc_block_hash(&block_hash_hex)?,
    );
    let server = JsonRpcTestServer::start([method("getblock").reply(RpcReply::result(json!({
        "hash": block_hash_hex,
        "height": 42,
        "time": 1
    })))])?;
    let source = regtest_source(&server)?;

    assert!(matches!(
        source.fetch_block_value_pool_balances(block_id).await,
        Err(SourceError::NodeCapabilityMissing {
            capability: NodeCapability::BlockValuePoolBalances
        })
    ));

    Ok(())
}

fn fixture_block() -> eyre::Result<Value> {
    serde_json::from_str(include_str!(
        "../../../../services/zinder-ingest/tests/fixtures/z3-regtest-block-1.json"
    ))
    .map_err(|error| eyre!("failed to parse fixture block: {error}"))
}

fn zebra_tip_response(fixture: &Value) -> eyre::Result<Value> {
    let hash = decode_rpc_block_hash(string_field(fixture, "hash")?)?;
    Ok(json!({
        "height": fixture["height"],
        "hash": hash.as_bytes(),
    }))
}

fn string_field<'fixture>(
    fixture: &'fixture Value,
    field_name: &'static str,
) -> eyre::Result<&'fixture str> {
    fixture
        .get(field_name)
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("fixture field {field_name} must be a string"))
}

#[tokio::test]
async fn poll_upstream_health_falls_back_to_verification_progress() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("getblockchaininfo").reply(RpcReply::result(json!({
            "blocks": 50,
            "estimatedheight": 1_000,
            "verificationprogress": 0.5,
            "valuePools": [],
            "upgrades": {},
        })))])?;
    let source = testnet_source(&server)?;

    let snapshot = source.poll_upstream_health().await?;
    assert!(!snapshot.ready_for_queries);
    assert_eq!(
        snapshot.source,
        UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK
    );
    assert_eq!(
        snapshot.reason.as_ref(),
        UPSTREAM_HEALTH_REASON_VERIFICATION_PROGRESS_BELOW_FLOOR
    );
    assert_eq!(snapshot.upstream_committed_height, Some(50));
    assert_eq!(snapshot.upstream_estimated_height, Some(1_000));
    assert_eq!(snapshot.upstream_verification_progress, Some(0.5));
    Ok(())
}

#[tokio::test]
async fn poll_upstream_health_treats_regtest_tip_observation_as_ready() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("getblockchaininfo").reply(RpcReply::result(json!({
            "blocks": 125,
            "estimatedheight": 6_519_740,
            "verificationprogress": 1.0,
            "valuePools": [],
            "upgrades": {},
        })))])?;
    let source = regtest_source(&server)?;

    let snapshot = source.poll_upstream_health().await?;
    assert!(snapshot.ready_for_queries);
    assert_eq!(snapshot.upstream_committed_height, Some(125));
    assert_eq!(snapshot.upstream_estimated_height, Some(6_519_740));
    Ok(())
}

#[tokio::test]
async fn poll_upstream_health_flags_estimated_gap_when_progress_is_above_floor() -> eyre::Result<()>
{
    let server =
        JsonRpcTestServer::start([method("getblockchaininfo").reply(RpcReply::result(json!({
            "blocks": 100,
            "estimatedheight": 200,
            "verificationprogress": 0.9995,
            "valuePools": [],
            "upgrades": {},
        })))])?;
    let source = testnet_source(&server)?;

    let snapshot = source.poll_upstream_health().await?;
    assert!(!snapshot.ready_for_queries);
    assert_eq!(
        snapshot.reason.as_ref(),
        UPSTREAM_HEALTH_REASON_ESTIMATED_GAP_ABOVE_FLOOR
    );
    assert_eq!(snapshot.upstream_committed_height, Some(100));
    assert_eq!(snapshot.upstream_estimated_height, Some(200));
    Ok(())
}

#[tokio::test]
async fn poll_upstream_health_reports_ready_when_progress_and_gap_within_floors() -> eyre::Result<()>
{
    let server =
        JsonRpcTestServer::start([method("getblockchaininfo").reply(RpcReply::result(json!({
            "blocks": 2_500_000,
            "estimatedheight": 2_500_005,
            "verificationprogress": 0.9999,
            "valuePools": [],
            "upgrades": {},
        })))])?;
    let source = testnet_source(&server)?;

    let snapshot = source.poll_upstream_health().await?;
    assert!(snapshot.ready_for_queries);
    assert_eq!(
        snapshot.source,
        UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK
    );
    assert_eq!(snapshot.upstream_committed_height, Some(2_500_000));
    assert_eq!(snapshot.upstream_estimated_height, Some(2_500_005));
    Ok(())
}

#[tokio::test]
async fn poll_upstream_health_falls_back_when_ready_endpoint_unreachable() -> eyre::Result<()> {
    let server =
        JsonRpcTestServer::start([method("getblockchaininfo").reply(RpcReply::result(json!({
            "blocks": 50,
            "estimatedheight": 1_000,
            "verificationprogress": 0.4,
            "valuePools": [],
            "upgrades": {},
        })))])?;
    // Point the health probe at a port that nothing listens on so the
    // first call errors out and the source falls back to the JSON-RPC
    // path within the same `poll_upstream_health` invocation.
    let source = testnet_source(&server)?.with_health_config(Some(NodeHealthConfig::new(
        "http://127.0.0.1:1/ready".to_owned(),
        Duration::from_millis(500),
        0.999,
        10,
    )));

    let snapshot = source.poll_upstream_health().await?;
    assert!(!snapshot.ready_for_queries);
    assert_eq!(
        snapshot.source,
        UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK
    );
    assert_eq!(snapshot.upstream_committed_height, Some(50));
    Ok(())
}

#[tokio::test]
async fn poll_upstream_health_uses_ready_endpoint_when_configured() -> eyre::Result<()> {
    use std::net::SocketAddr;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let bound: SocketAddr = listener.local_addr()?;
    let accept_handle = tokio::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|error| eyre!("accept failed: {error}"))?;
        let mut request_buffer = [0_u8; 1024];
        let _bytes_read = stream
            .read(&mut request_buffer)
            .await
            .map_err(|error| eyre!("request read failed: {error}"))?;
        stream
            .write_all(
                b"HTTP/1.1 503 Service Unavailable\r\n\
                  Content-Length: 18\r\n\
                  Content-Type: text/plain\r\n\
                  Connection: close\r\n\
                  \r\n\
                  insufficient peers",
            )
            .await
            .map_err(|error| eyre!("response write failed: {error}"))?;
        stream
            .shutdown()
            .await
            .map_err(|error| eyre!("stream shutdown failed: {error}"))?;
        Ok::<(), eyre::Report>(())
    });

    let json_rpc_server = JsonRpcTestServer::start(Vec::<zinder_testkit::JsonRpcStub>::new())?;
    let source = regtest_source(&json_rpc_server)?.with_health_config(Some(NodeHealthConfig::new(
        format!("http://{bound}/ready"),
        Duration::from_secs(5),
        0.999,
        10,
    )));

    let snapshot = source.poll_upstream_health().await?;
    accept_handle
        .await
        .map_err(|error| eyre!("ready endpoint task panicked: {error}"))??;

    assert!(!snapshot.ready_for_queries);
    assert_eq!(snapshot.source, "zebra_ready_endpoint");
    assert_eq!(snapshot.reason.as_ref(), "insufficient peers");
    Ok(())
}
