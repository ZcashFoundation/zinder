#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::num::NonZeroU32;
use std::sync::Arc;

use eyre::eyre;
use prost::Message;
use zinder_core::wire::{encode_rpc_block_hash_hex, encode_zinder_native_chain_name};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpoch, ChainEpochId,
    CompactBlockArtifact, MAX_SUBTREE_ROOTS_PER_REQUEST, Network, ShieldedProtocol,
    SubtreeRootIndex, SubtreeRootRange,
};
use zinder_proto::v1::wallet;
use zinder_query::{
    ArtifactKey, DEFAULT_MAX_COMPACT_BLOCK_RANGE, QueryError, WalletQueryApi,
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
    latest_tree_state_checkpoint_response, subtree_roots_response, tree_state_at_response,
    visible_tip_block_response,
};
use zinder_source::{SourceError, SourceTreeState};
use zinder_store::{ArtifactFamily, RawBlobRetention};
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

use crate::common::{
    ProbedNode, probed_serving_query_over, serving_query_after_live_append, serving_query_over,
};

/// Tree-state payload the release store derives from a checkpoint whose
/// commitment-tree frontiers are all empty.
const EMPTY_FRONTIER_TREE_STATE_PAYLOAD: &[u8] = b"{}";

#[tokio::test]
async fn compact_block_range_reads_from_one_chain_epoch() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let (wallet_query, _store_fixture) = serving_query_after_live_append(&chain)?;
    let current_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;

    let compact_block_range = wallet_query
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2)),
            None,
        )
        .await?;

    assert_eq!(current_epoch.id, ChainEpochId::new(2));
    assert_eq!(compact_block_range.chain_epoch, current_epoch);
    assert_eq!(
        compact_block_identities(&compact_block_range.compact_blocks),
        fixture_block_identities(&chain)
    );

    Ok(())
}

#[tokio::test]
async fn compact_block_range_serves_visible_blocks_above_settled_tip() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let (wallet_query, _store_fixture) = serving_query_after_live_append(&chain)?;
    let current_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;
    assert_eq!(current_epoch.settled_tip_height, BlockHeight::new(1));
    assert_eq!(current_epoch.visible_tip_height, BlockHeight::new(2));

    let response = wallet_query
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(2)),
            Some(current_epoch.id),
        )
        .await?;

    assert_eq!(response.chain_epoch, current_epoch);
    assert_eq!(
        compact_block_identities(&response.compact_blocks),
        fixture_block_identities(&chain)[1..]
    );
    Ok(())
}

#[tokio::test]
async fn tree_state_at_serves_visible_height_above_settled_tip_under_epoch_pin() -> eyre::Result<()>
{
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let tip = chain
        .block_at(BlockHeight::new(2))
        .ok_or_else(|| eyre!("fixture must contain height 2"))?;
    let tip_id = BlockId::new(tip.height, tip.hash);
    let node = ProbedNode::with_tree_state(
        tip_id,
        SourceTreeState::new(tip_id, tip.block_time_seconds, b"{}".to_vec()),
    )?;
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture =
        WalletServingStoreFixture::from_chain_after_live_append(&chain, &activations)?;
    let wallet_query = WalletServingQuery::from_probed_node_source(
        crate::common::serving_pair_slot(&mut store_fixture)?,
        node.clone(),
        activations,
    )?;
    let current_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;
    assert!(current_epoch.settled_tip_height < tip.height);

    let response = wallet_query
        .tree_state_at(tip.height, Some(current_epoch.id))
        .await?;

    assert_eq!(response.chain_epoch, current_epoch);
    assert_eq!(response.height, tip.height);
    assert_eq!(response.block_hash, tip.hash);
    assert_eq!(response.block_time_seconds, tip.block_time_seconds);
    let payload: serde_json::Value = serde_json::from_slice(&response.payload_bytes)?;
    assert!(
        payload.get("sapling").is_some(),
        "the live-appended checkpoint must carry the active Sapling frontier, got {payload}"
    );
    assert_eq!(
        node.tree_state_fetch_count(),
        0,
        "a canonical tip checkpoint must not consult the node"
    );
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the test keeps the complete native compact-block wire shape in one assertion scope"
)]
async fn compact_block_range_chunk_uses_native_wallet_proto_shape() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;

    let compact_block_range = wallet_query
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1)),
            None,
        )
        .await?;
    let chain_epoch = compact_block_range.chain_epoch;
    let compact_block = compact_block_range
        .compact_blocks
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("missing compact block"))?;
    let chunk = compact_block_range_chunk(chain_epoch, &compact_block);
    let encoded_chunk = chunk.encode_to_vec();
    let decoded_chunk = wallet::CompactBlocksInRangeChunk::decode(encoded_chunk.as_slice())?;
    let response_chain_epoch = decoded_chunk
        .chain_view
        .as_ref()
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .ok_or_else(|| eyre!("missing chunk chain epoch"))?;
    let response_visible_tip = response_chain_epoch
        .visible_tip
        .as_ref()
        .ok_or_else(|| eyre!("missing chunk visible tip"))?;
    let response_settled_tip = response_chain_epoch
        .settled_tip
        .as_ref()
        .ok_or_else(|| eyre!("missing chunk settled tip"))?;
    let response_compact_block = decoded_chunk
        .compact_block
        .as_ref()
        .ok_or_else(|| eyre!("missing chunk compact block"))?;

    assert_eq!(response_chain_epoch.chain_epoch_id, chain_epoch.id.value());
    assert_eq!(
        response_chain_epoch.network_name,
        encode_zinder_native_chain_name(chain_epoch.network)
    );
    assert_eq!(
        response_visible_tip.height,
        chain_epoch.visible_tip_height.value()
    );
    assert_eq!(
        response_visible_tip.hash,
        encode_rpc_block_hash_hex(chain_epoch.visible_tip_hash)
    );
    assert_eq!(
        response_settled_tip.height,
        chain_epoch.settled_tip_height.value()
    );
    assert_eq!(
        response_settled_tip.hash,
        encode_rpc_block_hash_hex(chain_epoch.settled_tip_hash)
    );
    assert_eq!(
        response_chain_epoch.artifact_schema_version,
        u32::from(chain_epoch.artifact_schema_version.value())
    );
    assert_eq!(
        response_chain_epoch.created_at_millis,
        chain_epoch.created_at.value()
    );
    assert_eq!(
        response_compact_block,
        &zinder_proto::wire::compact_block_message(&compact_block)
    );
    assert_eq!(
        compact_block_identities(&[compact_block]),
        fixture_block_identities(&chain)
    );

    Ok(())
}

#[tokio::test]
async fn visible_tip_block_response_uses_native_wallet_proto_shape() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;
    let chain_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;

    let response = visible_tip_block_response(&wallet_query, None).await?;
    let encoded_response = response.encode_to_vec();
    let decoded_response = wallet::VisibleTipBlockResponse::decode(encoded_response.as_slice())?;
    let response_chain_epoch = decoded_response
        .chain_view
        .as_ref()
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .ok_or_else(|| eyre!("missing response chain epoch"))?;
    let visible_tip_block = decoded_response
        .visible_tip_block
        .as_ref()
        .ok_or_else(|| eyre!("missing visible-tip block identity"))?;

    assert_eq!(response_chain_epoch.chain_epoch_id, chain_epoch.id.value());
    assert_eq!(
        visible_tip_block.height,
        chain_epoch.visible_tip_height.value()
    );
    assert_eq!(
        visible_tip_block.block_hash,
        encode_rpc_block_hash_hex(chain_epoch.visible_tip_hash)
    );
    assert_eq!(chain.tip_hash(), Some(chain_epoch.visible_tip_hash));

    Ok(())
}

#[tokio::test]
async fn tree_state_checkpoint_response_uses_native_wallet_proto_shape() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let tip = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("fixture must contain height 1"))?;
    let tip_id = BlockId::new(tip.height, tip.hash);
    let node = ProbedNode::with_tree_state(
        tip_id,
        SourceTreeState::new(tip_id, tip.block_time_seconds, b"node".to_vec()),
    )?;
    let (wallet_query, _store_fixture) = probed_serving_query_over(&chain, node.clone())?;
    let chain_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;

    let response = tree_state_at_response(&wallet_query, BlockHeight::new(1), None).await?;
    let encoded_response = response.encode_to_vec();
    let decoded_response = wallet::TreeStateResponse::decode(encoded_response.as_slice())?;
    let response_chain_epoch = decoded_response
        .chain_view
        .as_ref()
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .ok_or_else(|| eyre!("missing response chain epoch"))?;

    assert_eq!(response_chain_epoch.chain_epoch_id, chain_epoch.id.value());
    assert_eq!(decoded_response.height, tip.height.value());
    assert_eq!(
        decoded_response.block_hash,
        encode_rpc_block_hash_hex(tip.hash)
    );
    assert_eq!(
        decoded_response.payload_bytes,
        EMPTY_FRONTIER_TREE_STATE_PAYLOAD
    );
    assert_eq!(node.tree_state_fetch_count(), 0);

    Ok(())
}

#[tokio::test]
async fn latest_tree_state_checkpoint_response_uses_tip_tree_state() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;
    let tip = chain
        .block_at(BlockHeight::new(2))
        .ok_or_else(|| eyre!("fixture must contain height 2"))?;

    let response = latest_tree_state_checkpoint_response(&wallet_query, None).await?;
    let encoded_response = response.encode_to_vec();
    let decoded_response = wallet::TreeStateResponse::decode(encoded_response.as_slice())?;

    assert_eq!(decoded_response.height, tip.height.value());
    assert_eq!(
        decoded_response.block_hash,
        encode_rpc_block_hash_hex(tip.hash)
    );
    assert_eq!(
        decoded_response.payload_bytes,
        EMPTY_FRONTIER_TREE_STATE_PAYLOAD
    );

    Ok(())
}

#[tokio::test]
async fn sparse_tree_state_rejects_upstream_time_that_disagrees_with_canonical_header()
-> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let sparse = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("fixture must contain height 1"))?;
    let block_id = BlockId::new(sparse.height, sparse.hash);
    let node = ProbedNode::with_tree_state(
        block_id,
        SourceTreeState::new(
            block_id,
            sparse.block_time_seconds.saturating_add(1),
            br#"{"sapling":{"commitments":{"finalState":"aa"}}}"#.to_vec(),
        ),
    )?;
    let (wallet_query, _store_fixture) = probed_serving_query_over(&chain, node.clone())?;

    let error = match wallet_query.tree_state_at(block_id.height, None).await {
        Ok(tree_state) => {
            return Err(eyre!(
                "conflicting upstream time must fail closed, got {tree_state:?}"
            ));
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        QueryError::Node(SourceError::SourceProtocolMismatch {
            reason: "tree-state source time does not match the canonical block"
        })
    ));
    assert_eq!(node.tree_state_fetch_count(), 1);

    Ok(())
}

#[tokio::test]
async fn sparse_tree_state_uses_canonical_identity_and_time_with_frontier_only_payload()
-> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let sparse = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("fixture must contain height 1"))?;
    let block_id = BlockId::new(sparse.height, sparse.hash);
    let payload = br#"{"sapling":{"commitments":{"finalState":"aa"}}}"#.to_vec();
    let node = ProbedNode::with_tree_state(
        block_id,
        SourceTreeState::new(block_id, sparse.block_time_seconds, payload.clone()),
    )?;
    let (wallet_query, _store_fixture) = probed_serving_query_over(&chain, node.clone())?;

    let tree_state = wallet_query.tree_state_at(block_id.height, None).await?;

    assert_eq!(tree_state.height, block_id.height);
    assert_eq!(tree_state.block_hash, block_id.hash);
    assert_eq!(tree_state.block_time_seconds, sparse.block_time_seconds);
    assert_eq!(tree_state.payload_bytes, payload);
    assert_eq!(node.tree_state_fetch_count(), 1);

    Ok(())
}

#[tokio::test]
async fn serving_query_rejects_oversized_subtree_root_range() -> eyre::Result<()> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(1);
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical),
        Arc::new(wallet),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(pair),
        (),
        ingest_control,
        activations,
    );
    let requested = MAX_SUBTREE_ROOTS_PER_REQUEST.saturating_add(1);
    let max_entries = NonZeroU32::new(requested).ok_or_else(|| eyre!("invalid max entries"))?;

    let query_outcome = wallet_query
        .subtree_roots(
            SubtreeRootRange::new(
                ShieldedProtocol::Sapling,
                SubtreeRootIndex::new(0),
                max_entries,
            ),
            None,
        )
        .await;

    assert!(matches!(
        query_outcome,
        Err(QueryError::SubtreeRootRangeTooLarge {
            requested: actual,
            maximum,
        }) if actual == requested && maximum == MAX_SUBTREE_ROOTS_PER_REQUEST
    ));
    Ok(())
}

#[tokio::test]
async fn subtree_roots_response_returns_valid_empty_range() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;

    let response = subtree_roots_response(
        &wallet_query,
        SubtreeRootRange::new(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            NonZeroU32::new(MAX_SUBTREE_ROOTS_PER_REQUEST)
                .ok_or_else(|| eyre!("invalid max entries"))?,
        ),
        None,
    )
    .await?;
    let encoded_response = response.encode_to_vec();
    let decoded_response = wallet::SubtreeRootsResponse::decode(encoded_response.as_slice())?;

    assert_eq!(decoded_response.start_index, 0);
    assert!(decoded_response.subtree_roots.is_empty());

    Ok(())
}

#[tokio::test]
async fn compact_block_range_rejects_inverted_height_range() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let (wallet_query, _store_fixture) = crate::common::serving_query_over(&chain)?;

    let error = match wallet_query
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(1)),
            None,
        )
        .await
    {
        Ok(compact_block_range) => {
            return Err(eyre!("expected invalid range, got {compact_block_range:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(error, QueryError::InvalidBlockRange { .. }));

    Ok(())
}

#[tokio::test]
async fn serving_pair_compact_range_enforces_the_release_limit_before_storage() -> eyre::Result<()>
{
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(1);
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical),
        Arc::new(wallet),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(pair),
        (),
        ingest_control,
        activations,
    );
    let requested = DEFAULT_MAX_COMPACT_BLOCK_RANGE.get().saturating_add(1);
    let requested_count = usize::try_from(requested)?;
    let maximum_count = usize::try_from(DEFAULT_MAX_COMPACT_BLOCK_RANGE.get())?;

    let outcome = query
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(requested)),
            None,
        )
        .await;

    assert!(matches!(
        outcome,
        Err(QueryError::BlockRangeTooLarge {
            requested: actual,
            maximum,
        }) if actual == requested_count && maximum == maximum_count
    ));
    Ok(())
}

/// Absent serving-pair range heights are typed coverage misses.
///
/// The release serving pair retains compact blocks from height 1 through its
/// visible tip. A range touching an absent height is `ArtifactUnavailable`
/// (gRPC `NOT_FOUND`), as the single-block read on the same pair reports, not a
/// `STORAGE_UNAVAILABLE` outage.
#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "One fixture drives every out-of-history case through both the query and gRPC boundaries."
)]
async fn serving_pair_compact_range_outside_retained_history_is_artifact_unavailable()
-> eyre::Result<()> {
    use tonic_types::StatusExt as _;
    use zinder_proto::v1::wallet::wallet_query_server::WalletQuery as WalletQueryService;

    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(3);
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical),
        Arc::new(wallet),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(pair),
        (),
        ingest_control,
        activations,
    );
    let adapter = zinder_query::WalletQueryGrpcAdapter::new(
        query.clone(),
        zinder_query::WalletEndpointMetadata::default(),
    );

    let cases = [
        ("start above visible tip", 4_u32, 5_u32, 4_u32),
        ("start at unretained height 0", 0, 1, 0),
        ("only unretained height 0", 0, 0, 0),
    ];
    let mut mismatches = Vec::new();
    for (label, start_height, end_height, missing_height) in cases {
        let block_range = BlockHeightRange::inclusive(
            BlockHeight::new(start_height),
            BlockHeight::new(end_height),
        );
        let query_outcome = query.compact_blocks_in_range(block_range, None).await;
        let query_matches = matches!(
            &query_outcome,
            Err(QueryError::ArtifactUnavailable {
                family: ArtifactFamily::CompactBlock,
                key: ArtifactKey::BlockHeight(height),
            }) if *height == BlockHeight::new(missing_height)
        );
        if !query_matches {
            mismatches.push(format!(
                "{label} ({start_height}..={end_height}) query: expected ArtifactUnavailable(compact_block, {missing_height}), observed {:?}",
                query_outcome.as_ref().map(|range| range.compact_blocks.len()),
            ));
        }

        let Err(status) = WalletQueryService::compact_blocks_in_range(
            &adapter,
            tonic::Request::new(wallet::CompactBlocksInRangeRequest {
                start_height,
                end_height,
                at_epoch_id: None,
            }),
        )
        .await
        else {
            mismatches.push(format!(
                "{label} ({start_height}..={end_height}) gRPC: expected NOT_FOUND, observed an open stream"
            ));
            continue;
        };
        let reason = status
            .get_details_error_info()
            .map(|error_info| error_info.reason)
            .unwrap_or_default();
        if status.code() != tonic::Code::NotFound || reason != "ARTIFACT_UNAVAILABLE" {
            mismatches.push(format!(
                "{label} ({start_height}..={end_height}) gRPC: expected NotFound/ARTIFACT_UNAVAILABLE, observed {:?}/{reason} \"{}\"",
                status.code(),
                status.message(),
            ));
        }
    }

    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(eyre!(mismatches.join("\n")))
    }
}

fn compact_block_range_chunk(
    chain_epoch: ChainEpoch,
    compact_block: &CompactBlockArtifact,
) -> wallet::CompactBlocksInRangeChunk {
    wallet::CompactBlocksInRangeChunk {
        chain_view: Some(wallet::ChainView {
            chain_epoch: Some(wallet::ChainEpoch {
                chain_epoch_id: chain_epoch.id.value(),
                network_name: encode_zinder_native_chain_name(chain_epoch.network).to_owned(),
                artifact_schema_version: u32::from(chain_epoch.artifact_schema_version.value()),
                created_at_millis: chain_epoch.created_at.value(),
                visible_tip: Some(wallet::BlockTip {
                    height: chain_epoch.visible_tip_height.value(),
                    hash: encode_rpc_block_hash_hex(chain_epoch.visible_tip_hash),
                }),
                settled_tip: Some(wallet::BlockTip {
                    height: chain_epoch.settled_tip_height.value(),
                    hash: encode_rpc_block_hash_hex(chain_epoch.settled_tip_hash),
                }),
                sapling_commitment_tree_size: chain_epoch.tip_metadata.sapling_commitment_tree_size,
                orchard_commitment_tree_size: chain_epoch.tip_metadata.orchard_commitment_tree_size,
                ironwood_commitment_tree_size: chain_epoch
                    .tip_metadata
                    .ironwood_commitment_tree_size,
            }),
            indexed_tip: None,
            upstream_tip: None,
            materialized_views: None,
        }),
        compact_block: Some(zinder_proto::wire::compact_block_message(compact_block)),
    }
}

fn compact_block_identities(
    compact_blocks: &[CompactBlockArtifact],
) -> Vec<(BlockHeight, BlockHash)> {
    compact_blocks
        .iter()
        .map(|compact_block| (compact_block.height(), compact_block.block_hash()))
        .collect()
}

fn fixture_block_identities(chain: &ChainFixture) -> Vec<(BlockHeight, BlockHash)> {
    chain
        .blocks()
        .iter()
        .map(|block| (block.height, block.hash))
        .collect()
}
