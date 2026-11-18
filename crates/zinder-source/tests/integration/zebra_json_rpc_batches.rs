#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

//! Wire-level tests of the batched `getblock` path behind
//! `ZebraJsonRpcSource::fetch_chain_segment`.
//!
//! Every block is a copy of the regtest block-1 fixture whose previous-hash
//! header field is patched to link it to its parent. The header parser does
//! not verify Equihash, so the patched blocks parse as a parent-linked chain.

use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Duration,
};

use eyre::eyre;
use serde_json::{Value, json};
use zinder_core::{BlockHash, BlockHeight, BlockId, Network};
use zinder_source::{
    NodeAuth, NodeSource, SourceChainCursor, SourceChainSegment, SourceChainSegmentLimits,
    SourceChainUpdate, SourceError, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions,
    block_header_from_raw_block_bytes,
};
use zinder_testkit::{BatchResponseShape, JsonRpcTestServer, RpcReply, method};

const PREVIOUS_BLOCK_HASH_BYTES: std::ops::Range<usize> = 4..36;
const UNRELATED_PARENT_HASH: [u8; 32] = [0xab; 32];
/// Zebra's refusal of a known block whose stored transactions are incomplete.
const INCOMPLETE_STORED_BLOCK_REFUSAL: &str =
    "block body unavailable at height Height(3): stored transactions are incomplete";

#[derive(Clone)]
struct ChainBlock {
    id: BlockId,
    parent_hash: BlockHash,
    raw_block_hex: String,
}

fn fixture_raw_block_bytes() -> eyre::Result<Vec<u8>> {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../services/zinder-ingest/tests/fixtures/z3-regtest-block-1.json"
    ))?;
    let raw_block_hex = fixture
        .get("raw_block_hex")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("fixture field raw_block_hex must be a string"))?;
    Ok(hex::decode(raw_block_hex)?)
}

fn chain_block(
    fixture_bytes: &[u8],
    height: u32,
    parent_hash: Option<BlockHash>,
) -> eyre::Result<ChainBlock> {
    let mut raw_block_bytes = fixture_bytes.to_vec();
    if let Some(parent_hash) = parent_hash {
        raw_block_bytes
            .get_mut(PREVIOUS_BLOCK_HASH_BYTES)
            .ok_or_else(|| eyre!("fixture block is shorter than a block header"))?
            .copy_from_slice(&parent_hash.as_bytes());
    }
    let header = block_header_from_raw_block_bytes(BlockHeight::new(height), &raw_block_bytes)?;
    Ok(ChainBlock {
        id: header.block_id,
        parent_hash: header.previous_block_hash,
        raw_block_hex: hex::encode(raw_block_bytes),
    })
}

/// Builds parent-linked blocks for heights `1..=tip_height`.
fn linked_chain(tip_height: u32) -> eyre::Result<Vec<ChainBlock>> {
    let fixture_bytes = fixture_raw_block_bytes()?;
    let mut blocks: Vec<ChainBlock> = Vec::new();
    for height in 1..=tip_height {
        let parent_hash = blocks.last().map(|parent| parent.id.hash);
        blocks.push(chain_block(&fixture_bytes, height, parent_hash)?);
    }
    Ok(blocks)
}

fn block_at(blocks: &[ChainBlock], height: u32) -> eyre::Result<&ChainBlock> {
    blocks
        .iter()
        .find(|block| block.id.height.value() == height)
        .ok_or_else(|| eyre!("synthetic chain has no block at height {height}"))
}

fn tip_of(blocks: &[ChainBlock]) -> eyre::Result<BlockId> {
    blocks
        .last()
        .map(|block| block.id)
        .ok_or_else(|| eyre!("synthetic chain is empty"))
}

fn tip_stub(tip_id: BlockId) -> zinder_testkit::JsonRpcStub {
    method("getbestblockheightandhash").reply_with(move |_params| {
        RpcReply::result(json!({
            "height": tip_id.height.value(),
            "hash": tip_id.hash.as_bytes(),
        }))
    })
}

/// Answers `getblock ["<height>", 0]` from `blocks`, or with `refusals` for
/// the heights it names.
fn getblock_stub(
    blocks: &[ChainBlock],
    refusals: HashMap<u32, &'static str>,
) -> zinder_testkit::JsonRpcStub {
    let raw_blocks: Arc<HashMap<u32, String>> = Arc::new(
        blocks
            .iter()
            .map(|block| (block.id.height.value(), block.raw_block_hex.clone()))
            .collect(),
    );
    method("getblock").reply_with(move |params| {
        let Some(height) = params
            .get(0)
            .and_then(Value::as_str)
            .and_then(|height| height.parse::<u32>().ok())
        else {
            return RpcReply::error("getblock params must start with a height string");
        };
        if let Some(refusal) = refusals.get(&height) {
            return RpcReply::error(*refusal);
        }
        raw_blocks.get(&height).map_or_else(
            || RpcReply::error("block height not in best chain"),
            |raw_block_hex| RpcReply::result(json!(raw_block_hex)),
        )
    })
}

fn chain_server(blocks: &[ChainBlock]) -> eyre::Result<JsonRpcTestServer> {
    JsonRpcTestServer::start([
        tip_stub(tip_of(blocks)?),
        getblock_stub(blocks, HashMap::new()),
    ])
}

fn source_with_response_limit(
    server: &JsonRpcTestServer,
    max_response_bytes: u64,
) -> eyre::Result<ZebraJsonRpcSource> {
    let max_response_bytes = NonZeroU64::new(max_response_bytes)
        .ok_or_else(|| eyre!("max_response_bytes must be non-zero"))?;
    Ok(ZebraJsonRpcSource::with_options(
        Network::ZcashRegtest,
        server.url(),
        NodeAuth::None,
        ZebraJsonRpcSourceOptions {
            request_timeout: Duration::from_secs(5),
            max_response_bytes,
            broadcast_timeout: None,
        },
    )?)
}

fn regtest_source(server: &JsonRpcTestServer) -> eyre::Result<ZebraJsonRpcSource> {
    Ok(ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        server.url(),
        NodeAuth::None,
        Duration::from_secs(5),
    )?)
}

fn segment_limits(
    cursor: SourceChainCursor,
    max_connected_blocks: u32,
) -> eyre::Result<SourceChainSegmentLimits> {
    let max_connected_blocks = NonZeroU32::new(max_connected_blocks)
        .ok_or_else(|| eyre!("max_connected_blocks must be non-zero"))?;
    Ok(SourceChainSegmentLimits::new(
        cursor,
        max_connected_blocks,
        u64::MAX,
        u64::MAX,
    ))
}

async fn fetch_from_first_block(
    source: &ZebraJsonRpcSource,
    max_connected_blocks: u32,
) -> eyre::Result<Result<SourceChainSegment, SourceError>> {
    let limits = segment_limits(
        SourceChainCursor::before_first_block(),
        max_connected_blocks,
    )?;
    Ok(source.fetch_chain_segment(limits).await)
}

fn connected_block_ids(segment: &SourceChainSegment) -> eyre::Result<Vec<BlockId>> {
    segment
        .updates()
        .iter()
        .map(|update| match update {
            SourceChainUpdate::ConnectedBlock { block, .. } => {
                Ok(BlockId::new(block.height, block.hash))
            }
            SourceChainUpdate::RevertedBlock { .. } | SourceChainUpdate::SettledTip { .. } => {
                Err(eyre!("expected only connected blocks, got {update:?}"))
            }
        })
        .collect()
}

fn chain_ids(blocks: &[ChainBlock]) -> Vec<BlockId> {
    blocks.iter().map(|block| block.id).collect()
}

#[tokio::test]
async fn fetch_chain_segment_sends_one_getblock_batch_per_segment() -> eyre::Result<()> {
    let blocks = linked_chain(4)?;
    let server = chain_server(&blocks)?;
    let source = regtest_source(&server)?;

    let segment = fetch_from_first_block(&source, 4).await??;

    assert_eq!(connected_block_ids(&segment)?, chain_ids(&blocks));
    for (block, update) in blocks.iter().zip(segment.updates()) {
        let SourceChainUpdate::ConnectedBlock { block: fetched, .. } = update else {
            return Err(eyre!("expected a connected block, got {update:?}"));
        };
        assert_eq!(fetched.parent_hash, block.parent_hash);
        assert_eq!(hex::encode(&fetched.raw_block_bytes), block.raw_block_hex);
    }
    assert_eq!(server.batch_sizes(), vec![4]);
    let getblock_params: Vec<Value> = server
        .requests_for("getblock")?
        .into_iter()
        .map(|request| request.params)
        .collect();
    assert_eq!(
        getblock_params,
        vec![
            json!(["1", 0]),
            json!(["2", 0]),
            json!(["3", 0]),
            json!(["4", 0]),
        ]
    );
    assert_eq!(segment.stats().connected_blocks(), 4);
    assert_eq!(segment.stats().split_count(), 0);
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_bounds_the_batch_by_max_connected_blocks() -> eyre::Result<()> {
    let blocks = linked_chain(5)?;
    let server = chain_server(&blocks)?;
    let source = regtest_source(&server)?;

    let segment = fetch_from_first_block(&source, 3).await??;

    assert_eq!(connected_block_ids(&segment)?, chain_ids(&blocks[..3]));
    assert_eq!(server.batch_sizes(), vec![3]);
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_accepts_out_of_order_batch_responses() -> eyre::Result<()> {
    let blocks = linked_chain(4)?;
    let server = chain_server(&blocks)?;
    server.shape_batch_responses(|_batch_size| vec![BatchResponseShape::Reverse]);
    let source = regtest_source(&server)?;

    let segment = fetch_from_first_block(&source, 4).await??;

    assert_eq!(connected_block_ids(&segment)?, chain_ids(&blocks));
    assert_eq!(server.batch_sizes(), vec![4]);
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_maps_a_batch_member_refusal_to_its_height() -> eyre::Result<()> {
    let blocks = linked_chain(4)?;
    let refused_height = BlockHeight::new(3);

    for (refusal, expects_unreadable_body) in [
        ("block height not in best chain", false),
        (INCOMPLETE_STORED_BLOCK_REFUSAL, true),
    ] {
        let server = JsonRpcTestServer::start([
            tip_stub(tip_of(&blocks)?),
            getblock_stub(&blocks, HashMap::from([(refused_height.value(), refusal)])),
        ])?;
        let source = regtest_source(&server)?;

        let outcome = fetch_from_first_block(&source, 4).await?;

        match outcome {
            Err(SourceError::BlockUnavailable { height, reason }) if !expects_unreadable_body => {
                assert_eq!(height, refused_height);
                assert_eq!(reason, format!("getblock: {refusal}"));
            }
            Err(SourceError::HistoricalBlockBodyUnavailable { height, reason })
                if expects_unreadable_body =>
            {
                assert_eq!(height, refused_height);
                assert_eq!(reason, format!("getblock: {refusal}"));
            }
            other => return Err(eyre!("refusal {refusal:?} mapped to {other:?}")),
        }
        assert_eq!(server.batch_sizes(), vec![4]);
    }
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_splits_a_batch_the_node_answers_with_one_error_object()
-> eyre::Result<()> {
    let blocks = linked_chain(4)?;
    let server = chain_server(&blocks)?;
    server.shape_batch_responses(|batch_size| {
        if batch_size > 2 {
            vec![BatchResponseShape::single_error_object(
                -32603,
                "response is too large",
            )]
        } else {
            Vec::new()
        }
    });
    let source = regtest_source(&server)?;

    let segment = fetch_from_first_block(&source, 4).await??;

    assert_eq!(connected_block_ids(&segment)?, chain_ids(&blocks));
    assert_eq!(server.batch_sizes(), vec![4, 2, 2]);
    assert_eq!(segment.stats().split_count(), 1);
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_splits_a_batch_above_max_response_bytes() -> eyre::Result<()> {
    let blocks = linked_chain(4)?;
    let raw_block_hex_len = u64::try_from(block_at(&blocks, 1)?.raw_block_hex.len())?;
    let server = chain_server(&blocks)?;
    // Two blocks plus JSON-RPC framing fit; four do not.
    let source = source_with_response_limit(&server, raw_block_hex_len * 3)?;

    let segment = fetch_from_first_block(&source, 4).await??;

    assert_eq!(connected_block_ids(&segment)?, chain_ids(&blocks));
    assert_eq!(server.batch_sizes(), vec![4, 2, 2]);
    assert_eq!(segment.stats().split_count(), 1);
    assert_eq!(
        segment.stats().response_payload_bytes(),
        raw_block_hex_len * 4
    );
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_rejects_a_single_block_above_max_response_bytes() -> eyre::Result<()> {
    let blocks = linked_chain(2)?;
    let server = chain_server(&blocks)?;
    let max_response_bytes = 1_000;
    let source = source_with_response_limit(&server, max_response_bytes)?;

    let outcome = fetch_from_first_block(&source, 2).await?;

    assert!(
        matches!(
            outcome,
            Err(SourceError::SourceResponseTooLarge {
                operation: "batch_getblock",
                max_response_bytes: limit,
            }) if limit == max_response_bytes
        ),
        "unexpected outcome: {outcome:?}"
    );
    assert_eq!(server.batch_sizes(), vec![2, 1]);
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_reverts_the_cursor_block_when_the_first_parent_differs()
-> eyre::Result<()> {
    let blocks = linked_chain(3)?;
    let server = chain_server(&blocks)?;
    let source = regtest_source(&server)?;
    let stale_cursor_block = BlockId::new(
        BlockHeight::new(1),
        BlockHash::from_bytes(UNRELATED_PARENT_HASH),
    );

    let segment = source
        .fetch_chain_segment(segment_limits(
            SourceChainCursor::at_block(stale_cursor_block),
            4,
        )?)
        .await?;

    assert_eq!(
        segment.updates(),
        [SourceChainUpdate::reverted_block(stale_cursor_block)]
    );
    assert_eq!(server.batch_sizes(), vec![2]);
    assert_eq!(
        server
            .requests_for("getblock")?
            .into_iter()
            .map(|request| request.params)
            .collect::<Vec<_>>(),
        vec![json!(["2", 0]), json!(["3", 0])]
    );
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_connects_blocks_after_a_matching_cursor_block() -> eyre::Result<()> {
    let blocks = linked_chain(3)?;
    let server = chain_server(&blocks)?;
    let source = regtest_source(&server)?;

    let segment = source
        .fetch_chain_segment(segment_limits(
            SourceChainCursor::at_block(block_at(&blocks, 1)?.id),
            4,
        )?)
        .await?;

    assert_eq!(connected_block_ids(&segment)?, chain_ids(&blocks[1..]));
    assert_eq!(server.batch_sizes(), vec![2]);
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_rejects_batch_members_that_are_not_parent_linked() -> eyre::Result<()>
{
    let mut blocks = linked_chain(4)?;
    let fixture_bytes = fixture_raw_block_bytes()?;
    let unlinked = chain_block(
        &fixture_bytes,
        3,
        Some(BlockHash::from_bytes(UNRELATED_PARENT_HASH)),
    )?;
    if let Some(slot) = blocks.get_mut(2) {
        *slot = unlinked;
    }
    let server = chain_server(&blocks)?;
    let source = regtest_source(&server)?;

    let outcome = fetch_from_first_block(&source, 4).await?;

    assert!(
        matches!(
            outcome,
            Err(SourceError::BlockReorgDuringFetch { height, .. }) if height == BlockHeight::new(3)
        ),
        "unexpected outcome: {outcome:?}"
    );
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_rejects_batch_responses_with_unknown_ids() -> eyre::Result<()> {
    let blocks = linked_chain(4)?;
    let server = chain_server(&blocks)?;
    server.shape_batch_responses(|_batch_size| vec![BatchResponseShape::ShiftIds(1_000)]);
    let source = regtest_source(&server)?;

    let outcome = fetch_from_first_block(&source, 4).await?;

    assert!(
        matches!(
            &outcome,
            Err(SourceError::NodeUnavailable { reason }) if reason.contains("is not a pending call")
        ),
        "unexpected outcome: {outcome:?}"
    );
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_rejects_a_batch_response_with_fewer_entries() -> eyre::Result<()> {
    let blocks = linked_chain(4)?;
    let server = chain_server(&blocks)?;
    server.shape_batch_responses(|_batch_size| vec![BatchResponseShape::DropLast]);
    let source = regtest_source(&server)?;

    let outcome = fetch_from_first_block(&source, 4).await?;

    assert!(
        matches!(
            outcome,
            Err(SourceError::SourceProtocolMismatch {
                reason: "batched block response contained fewer entries than requested",
            })
        ),
        "unexpected outcome: {outcome:?}"
    );
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_rejects_a_batch_response_with_an_extra_duplicate_id()
-> eyre::Result<()> {
    let blocks = linked_chain(4)?;
    let server = chain_server(&blocks)?;
    server.shape_batch_responses(|_batch_size| vec![BatchResponseShape::DuplicateFirst]);
    let source = regtest_source(&server)?;

    let outcome = fetch_from_first_block(&source, 4).await?;

    assert!(
        matches!(
            outcome,
            Err(SourceError::SourceProtocolMismatch {
                reason: "batched block response contained more entries than requested",
            })
        ),
        "unexpected outcome: {outcome:?}"
    );
    Ok(())
}

/// A duplicated id in place of a missing one is reported as a height refusal.
///
/// The response count still equals the request count. jsonrpsee leaves the missing member's
/// slot holding its placeholder error (code 0, empty message), which the
/// source reports as a refusal of that height rather than as a protocol
/// mismatch.
#[tokio::test]
async fn fetch_chain_segment_reports_a_duplicate_replacing_a_missing_id_as_block_unavailable()
-> eyre::Result<()> {
    let blocks = linked_chain(4)?;
    let server = chain_server(&blocks)?;
    server.shape_batch_responses(|_batch_size| {
        vec![
            BatchResponseShape::DropLast,
            BatchResponseShape::DuplicateFirst,
        ]
    });
    let source = regtest_source(&server)?;

    let outcome = fetch_from_first_block(&source, 4).await?;

    match outcome {
        Err(SourceError::BlockUnavailable { height, reason }) => {
            assert_eq!(height, BlockHeight::new(4));
            assert_eq!(reason, "getblock: ");
        }
        other => return Err(eyre!("unexpected outcome: {other:?}")),
    }
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_maps_a_closed_batch_connection_to_node_unavailable() -> eyre::Result<()>
{
    let blocks = linked_chain(2)?;
    let server = JsonRpcTestServer::start([
        tip_stub(tip_of(&blocks)?),
        method("getblock").reply(RpcReply::close()),
        getblock_stub(&blocks, HashMap::new()),
    ])?;
    let source = regtest_source(&server)?;

    let outcome = fetch_from_first_block(&source, 2).await?;

    assert!(
        matches!(outcome, Err(SourceError::NodeUnavailable { .. })),
        "unexpected outcome: {outcome:?}"
    );
    assert_eq!(server.batch_sizes(), vec![2]);
    assert_eq!(server.requests_for("getblock")?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn fetch_chain_segment_maps_a_batch_slower_than_the_request_timeout_to_node_unavailable()
-> eyre::Result<()> {
    let blocks = linked_chain(2)?;
    let raw_block_hex = block_at(&blocks, 1)?.raw_block_hex.clone();
    let server = JsonRpcTestServer::start([
        tip_stub(tip_of(&blocks)?),
        method("getblock").reply(RpcReply::delayed(
            Duration::from_secs(2),
            RpcReply::result(json!(raw_block_hex)),
        )),
    ])?;
    let source = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        server.url(),
        NodeAuth::None,
        Duration::from_millis(200),
    )?;

    let outcome = fetch_from_first_block(&source, 1).await?;

    assert!(
        matches!(
            &outcome,
            Err(SourceError::NodeUnavailable { reason }) if reason == "Request timeout"
        ),
        "unexpected outcome: {outcome:?}"
    );
    Ok(())
}
