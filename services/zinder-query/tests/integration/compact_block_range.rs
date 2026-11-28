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
    BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpochId, CommitmentTreeFrontiers,
    CompactBlockArtifact, CompactChainMetadata, MAX_SUBTREE_ROOTS_PER_REQUEST, Network,
    SUBTREE_LEAF_COUNT, ShieldedProtocol, SubtreeRootHash, SubtreeRootIndex, SubtreeRootRange,
};
use zinder_proto::v1::wallet;
use zinder_query::{
    ArtifactKey, DEFAULT_MAX_COMPACT_BLOCK_RANGE, QueryError, RocksDbCanonicalReader,
    RocksDbWalletReader, WalletQueryApi, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair, compact_blocks_in_range_header, compact_blocks_in_range_item,
    latest_tree_state_checkpoint_response, subtree_roots_response, tree_state_at_response,
    visible_tip_block_response,
};
use zinder_store::{ArtifactFamily, CanonicalBuildSubtreeRoot, RawBlobRetention};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, WalletServingStoreFixture,
    completed_subtree_frontier, sample_regtest_upgrade_activations,
};

use crate::common::{serving_query_after_live_append, serving_query_from, serving_query_over};

/// Tree-state payload at height 1 of the Regtest fixture, where only Sapling
/// is active and its tree is empty.
const EMPTY_SAPLING_TREE_STATE_PAYLOAD: &[u8] =
    br#"{"sapling":{"commitments":{"finalState":"000000"}}}"#;
/// Tree-state payload at height 2 of the Regtest fixture, where Sapling and
/// Orchard are active and both trees are empty.
const EMPTY_SAPLING_AND_ORCHARD_TREE_STATE_PAYLOAD: &[u8] = concat!(
    r#"{"orchard":{"commitments":{"finalState":"000000"}},"#,
    r#""sapling":{"commitments":{"finalState":"000000"}}}"#,
)
.as_bytes();

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

fn native_range_fixture() -> eyre::Result<ChainFixture> {
    use zinder_core::{
        CompactSaplingSpend, CompactTransaction, CompactTransactionData, TransactionId,
    };
    use zinder_testkit::FixtureTransactionRows;

    let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let block = chain
        .block_at(BlockHeight::new(2))
        .cloned()
        .ok_or_else(|| eyre!("fixture is missing block 2"))?;
    let mut transactions = Vec::new();
    for index in 0_u8..8 {
        let txid = TransactionId::from_bytes([index + 10; 32]);
        chain = chain.with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            txid,
            block.height,
            block.hash,
            u32::from(index),
            b"compact range fixture".as_slice(),
        ));
        transactions.push(CompactTransaction {
            index: u64::from(index),
            transaction_id: txid,
            data: CompactTransactionData {
                sapling_spends: vec![CompactSaplingSpend {
                    nullifier: [index + 30; 32],
                }],
                ..CompactTransactionData::default()
            },
        });
    }
    Ok(chain.with_compact_block_artifact(CompactBlockArtifact::new(
        BlockId::new(block.height, block.hash),
        block.parent_hash,
        block.block_time_seconds,
        transactions,
        CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    )?))
}

async fn assert_native_range_byte_budget(
    query: &impl WalletQueryApi,
    range: BlockHeightRange,
    blocks: &zinder_query::CompactBlockRange,
) -> eyre::Result<()> {
    use zinder_store::{HeightOrder, encode_compact_block_artifact};

    let size = blocks
        .compact_blocks
        .iter()
        .map(|block| encode_compact_block_artifact(block).len())
        .sum::<usize>();
    for (order, kept) in [
        (HeightOrder::Ascending, &blocks.compact_blocks[..1]),
        (HeightOrder::Descending, &blocks.compact_blocks[1..]),
    ] {
        assert_eq!(
            query
                .compact_blocks_in_range_up_to(range, order, size - 1, None)
                .await?
                .compact_blocks,
            kept,
        );
        assert_eq!(
            query
                .compact_blocks_in_range_up_to(range, order, size, None)
                .await?
                .compact_blocks,
            blocks.compact_blocks,
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_compact_range_preserves_stream_items_and_message_size_limit() -> eyre::Result<()> {
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Code, transport::Server};
    use wallet::{wallet_query_client::WalletQueryClient, wallet_query_server::WalletQueryServer};
    use zinder_query::{WalletEndpointMetadata, WalletQueryGrpcAdapter};

    let chain = native_range_fixture()?;
    let fixture = WalletServingStoreFixture::from_chain_after_live_append(
        &chain,
        &sample_regtest_upgrade_activations(),
    )?;
    let (query, _store, _ingest) = crate::common::native_serving_query_from(fixture).await?;
    let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2));
    let blocks = query.compact_blocks_in_range(range, None).await?;
    let header = compact_blocks_in_range_header(blocks.chain_epoch);
    let items: Vec<_> = blocks
        .compact_blocks
        .iter()
        .map(compact_blocks_in_range_item)
        .collect();
    let [settled, live] = items.as_slice() else {
        return Err(eyre!("expected a settled and a live block"));
    };
    assert_native_range_byte_budget(&query, range, &blocks).await?;
    assert!(header.encoded_len() < live.encoded_len());
    assert!(settled.encoded_len() < live.encoded_len());
    let adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());
    for max_bytes in [live.encoded_len() - 1, live.encoded_len()] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = Server::builder()
            .add_service(
                WalletQueryServer::new(adapter.clone()).max_encoding_message_size(max_bytes),
            )
            .serve_with_incoming(TcpListenerStream::new(listener));
        let task = tokio::spawn(server);
        let mut client = WalletQueryClient::connect(endpoint).await?;
        let mut stream = client
            .compact_blocks_in_range(wallet::CompactBlocksInRangeRequest {
                start_height: 1,
                end_height: 2,
                at_epoch_id: Some(blocks.chain_epoch.id.value()),
                pool_types: Vec::new(),
            })
            .await?
            .into_inner();
        if max_bytes == live.encoded_len() {
            for expected in [&header, settled, live] {
                assert_eq!(stream.message().await?.as_ref(), Some(expected));
            }
            assert!(stream.message().await?.is_none());
        } else {
            let mut received = Vec::new();
            let status = loop {
                match stream.message().await {
                    Ok(Some(chunk)) => received.push(chunk),
                    Ok(None) => return Err(eyre!("oversized compact block was accepted")),
                    Err(status) => break status,
                }
            };
            assert_eq!(status.code(), Code::OutOfRange);
            // An encoding error can discard frames that tonic had buffered.
            assert!(received.len() <= 2);
            assert!(
                received
                    .iter()
                    .eq([&header, settled].into_iter().take(received.len()))
            );
        }
        task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn tree_state_at_serves_visible_height_above_settled_tip_under_epoch_pin() -> eyre::Result<()>
{
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let tip = chain
        .block_at(BlockHeight::new(2))
        .ok_or_else(|| eyre!("fixture must contain height 2"))?;
    let (wallet_query, _store_fixture) = serving_query_after_live_append(&chain)?;
    let current_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;
    assert!(current_epoch.settled_tip_height < tip.height);

    let response = wallet_query
        .tree_state_at(tip.height, Some(current_epoch.id))
        .await?;

    assert_eq!(response.chain_epoch, current_epoch);
    assert_eq!(response.height, tip.height);
    assert_eq!(response.block_hash, tip.hash);
    assert_eq!(response.block_time_seconds, tip.block_time_seconds);
    assert!(
        response.frontiers.sapling_final_state.is_some(),
        "the live-appended checkpoint must carry the active Sapling frontier, got {:?}",
        response.frontiers
    );
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the test keeps the complete native compact-block wire shape in one assertion scope"
)]
async fn compact_block_range_stream_sends_one_header_then_blocks() -> eyre::Result<()> {
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
        .first()
        .cloned()
        .ok_or_else(|| eyre!("missing compact block"))?;
    let decoded_chunks = std::iter::once(compact_blocks_in_range_header(chain_epoch))
        .chain(
            compact_block_range
                .compact_blocks
                .iter()
                .map(compact_blocks_in_range_item),
        )
        .map(|chunk| wallet::CompactBlocksInRangeChunk::decode(chunk.encode_to_vec().as_slice()))
        .collect::<Result<Vec<_>, _>>()?;
    let [header_chunk, item_chunk] = decoded_chunks.as_slice() else {
        return Err(eyre!(
            "expected one header and one block chunk, got {} chunks",
            decoded_chunks.len()
        ));
    };
    let Some(wallet::compact_blocks_in_range_chunk::Body::Header(header)) = &header_chunk.body
    else {
        return Err(eyre!("first chunk is not the chain-view header"));
    };
    let Some(wallet::compact_blocks_in_range_chunk::Body::Item(response_compact_block)) =
        &item_chunk.body
    else {
        return Err(eyre!("second chunk is not a compact block"));
    };
    let response_chain_epoch = header
        .chain_epoch
        .as_ref()
        .ok_or_else(|| eyre!("missing header chain epoch"))?;
    let response_visible_tip = response_chain_epoch
        .visible_tip
        .as_ref()
        .ok_or_else(|| eyre!("missing header visible tip"))?;
    let response_settled_tip = response_chain_epoch
        .settled_tip
        .as_ref()
        .ok_or_else(|| eyre!("missing header settled tip"))?;

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
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;
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
        EMPTY_SAPLING_TREE_STATE_PAYLOAD
    );

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
        EMPTY_SAPLING_AND_ORCHARD_TREE_STATE_PAYLOAD
    );

    Ok(())
}

#[tokio::test]
async fn serving_query_rejects_oversized_subtree_root_range() -> eyre::Result<()> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(1);
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(pair),
        ingest_control,
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
    assert_eq!(
        decoded_response.subtree_roots,
        [] as [zinder_proto::v1::wallet::SubtreeRoot; 0]
    );

    Ok(())
}

#[tokio::test]
async fn subtree_roots_response_uses_native_wallet_proto_shape() -> eyre::Result<()> {
    let chain = completed_sapling_subtree_chain(1)?;
    let completing_block = chain
        .block_at(BlockHeight::new(1))
        .cloned()
        .ok_or_else(|| eyre!("fixture must contain height 1"))?;
    let root_hash = SubtreeRootHash::from_bytes([0x71; 32]);
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_subtree_root(CanonicalBuildSubtreeRoot {
            protocol: ShieldedProtocol::Sapling,
            subtree_index: SubtreeRootIndex::new(0),
            root_hash,
            completing_block_height: completing_block.height,
        }),
        &sample_regtest_upgrade_activations(),
    )?;
    let (wallet_query, _store_fixture) = serving_query_from(writer.into_serving_store_fixture()?)?;
    let chain_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;

    let response = subtree_roots_response(
        &wallet_query,
        SubtreeRootRange::new(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            NonZeroU32::new(1).ok_or_else(|| eyre!("invalid max entries"))?,
        ),
        None,
    )
    .await?;
    let encoded_response = response.encode_to_vec();
    let decoded_response = wallet::SubtreeRootsResponse::decode(encoded_response.as_slice())?;
    let response_chain_epoch = decoded_response
        .chain_view
        .as_ref()
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .ok_or_else(|| eyre!("missing response chain epoch"))?;
    let response_subtree_root = decoded_response
        .subtree_roots
        .first()
        .ok_or_else(|| eyre!("missing response subtree root"))?;

    assert_eq!(
        chain_epoch.tip_metadata.sapling_commitment_tree_size,
        SUBTREE_LEAF_COUNT
    );
    assert_eq!(response_chain_epoch.chain_epoch_id, chain_epoch.id.value());
    assert_eq!(
        decoded_response.shielded_protocol,
        wallet::ShieldedProtocol::Sapling as i32
    );
    assert_eq!(decoded_response.start_index, 0);
    assert_eq!(decoded_response.subtree_roots.len(), 1);
    assert_eq!(response_subtree_root.root_hash, root_hash.as_bytes());
    assert_eq!(
        response_subtree_root.completing_block_hash,
        encode_rpc_block_hash_hex(completing_block.hash)
    );
    assert_eq!(
        response_subtree_root.completing_block_height,
        completing_block.height.value()
    );

    Ok(())
}

/// A checkpointed store retains only the roots its retained blocks complete.
///
/// The floor's trees already hold a completed Sapling subtree, so the epoch
/// advertises root 0 while the store never loaded it.
#[tokio::test]
async fn subtree_roots_response_reports_unavailable_when_completed_root_is_missing()
-> eyre::Result<()> {
    let chain = completed_sapling_subtree_chain(2)?;
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_history_floor(BlockHeight::new(1)),
        &sample_regtest_upgrade_activations(),
    )?;
    let (wallet_query, _store_fixture) = serving_query_from(writer.into_serving_store_fixture()?)?;
    let chain_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;
    assert_eq!(
        chain_epoch
            .tip_metadata
            .completed_subtree_count(ShieldedProtocol::Sapling),
        1
    );

    let error = match subtree_roots_response(
        &wallet_query,
        SubtreeRootRange::new(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            NonZeroU32::new(1).ok_or_else(|| eyre!("invalid max entries"))?,
        ),
        None,
    )
    .await
    {
        Ok(response) => return Err(eyre!("expected unavailable roots, got {response:?}")),
        Err(error) => error,
    };

    assert!(
        matches!(
            error,
            QueryError::ArtifactUnavailable {
                family: ArtifactFamily::SubtreeRoot,
                key: ArtifactKey::SubtreeRootIndex {
                    protocol: ShieldedProtocol::Sapling,
                    index
                }
            } if index == SubtreeRootIndex::new(0)
        ),
        "unexpected missing-root error: {error:?}"
    );

    Ok(())
}

/// Builds a chain whose first block completes the first Sapling subtree and
/// whose later blocks, up to `block_count`, add no note commitments.
fn completed_sapling_subtree_chain(block_count: u32) -> eyre::Result<ChainFixture> {
    let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(block_count);
    let completed_tree = CompactChainMetadata {
        sapling_commitment_tree_size: SUBTREE_LEAF_COUNT,
        orchard_commitment_tree_size: 0,
        ironwood_commitment_tree_size: 0,
    };
    for block in chain.blocks().to_vec() {
        let parent_hash = if block.height == BlockHeight::new(1) {
            Network::ZcashRegtest.genesis_hash()
        } else {
            block.parent_hash
        };
        chain = chain.with_compact_block_artifact(CompactBlockArtifact::empty(
            BlockId::new(block.height, block.hash),
            parent_hash,
            block.block_time_seconds,
            completed_tree,
        ));
    }
    Ok(chain.with_commitment_tree_frontiers_at(
        BlockHeight::new(1),
        CommitmentTreeFrontiers::from_validated_parts(
            Some(completed_subtree_frontier(ShieldedProtocol::Sapling)?),
            None,
            None,
        )?,
    ))
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
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(1);
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(pair),
        ingest_control,
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
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(3);
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(pair),
        ingest_control,
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
                pool_types: Vec::new(),
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
            .map_or_default(|error_info| error_info.reason);
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

fn dense_compact_block_chain() -> eyre::Result<ChainFixture> {
    use zinder_core::{
        CompactSaplingSpend, CompactTransaction, CompactTransactionData, TransactionId,
    };

    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(2);
    let height = BlockHeight::new(2);
    let block = chain
        .block_at(height)
        .ok_or_else(|| eyre!("missing dense block"))?;
    let txid = TransactionId::from_bytes([2; 32]);
    let mut rows = zinder_testkit::FixtureTransactionRows::from_raw_transaction(
        txid,
        height,
        block.hash,
        0,
        [2],
    );
    rows.facts.public_facts.counts.sapling_spend_count = 2048;
    let compact = CompactBlockArtifact::new(
        BlockId::new(height, block.hash),
        block.parent_hash,
        block.block_time_seconds,
        vec![CompactTransaction {
            index: 0,
            transaction_id: txid,
            data: CompactTransactionData {
                sapling_spends: vec![CompactSaplingSpend { nullifier: [2; 32] }; 2048],
                ..Default::default()
            },
        }],
        CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    )?;
    Ok(chain
        .with_transaction_rows(rows)
        .with_compact_block_artifact(compact))
}

#[tokio::test]
async fn native_dense_range_compression_reduces_wire_bytes() -> eyre::Result<()> {
    use http_body_util::{BodyExt as _, Full};
    use tonic::{
        body::Body,
        codegen::{Bytes, Service as _, http},
    };

    let (query, _store, _ingest) =
        crate::common::native_serving_query_over(&dense_compact_block_chain()?).await?;
    let mut server = zinder_query::WalletQueryGrpcAdapter::new(
        query,
        zinder_query::WalletEndpointMetadata::default(),
    )
    .into_server();
    let message = wallet::CompactBlocksInRangeRequest {
        start_height: 2,
        end_height: 2,
        at_epoch_id: None,
        pool_types: Vec::new(),
    }
    .encode_to_vec();
    let mut frame = vec![0];
    frame.extend_from_slice(&u32::try_from(message.len())?.to_be_bytes());
    frame.extend(message);
    let frame = Bytes::from(frame);
    let mut identity_bytes = None;
    for accepted in [None, Some("identity"), Some("gzip"), Some("zstd")] {
        let mut request = http::Request::post("/zinder.v1.wallet.WalletQuery/CompactBlocksInRange")
            .header("content-type", "application/grpc");
        if let Some(encoding) = accepted {
            request = request.header("grpc-accept-encoding", encoding);
        }
        let response = server
            .call(request.body(Body::new(Full::new(frame.clone())))?)
            .await?;
        let compressed = matches!(accepted, Some("gzip" | "zstd"));
        assert_eq!(
            response
                .headers()
                .get("grpc-encoding")
                .map(http::HeaderValue::to_str)
                .transpose()?,
            accepted.filter(|_| compressed),
        );
        let body = response.into_body().collect().await?;
        assert_eq!(
            body.trailers()
                .and_then(|trailers| trailers.get("grpc-status")),
            Some(&http::HeaderValue::from_static("0")),
        );
        let bytes = body.to_bytes();
        assert_eq!(bytes.first().copied(), Some(u8::from(compressed)));
        if let Some(identity) = identity_bytes {
            if compressed {
                assert!(bytes.len() < identity / 2);
            } else {
                assert_eq!(bytes.len(), identity);
            }
        } else {
            assert!(bytes.len() > 64 * 1024);
            identity_bytes = Some(bytes.len());
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_wallet_clients_round_trip_compressed_calls_with_bounded_decoding()
-> eyre::Result<()> {
    use futures_util::TryStreamExt as _;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Code, codec::CompressionEncoding, transport::Server};
    use wallet::wallet_query_client::WalletQueryClient;

    let (query, _store, _ingest) =
        crate::common::native_serving_query_over(&dense_compact_block_chain()?).await?;
    let server = zinder_query::WalletQueryGrpcAdapter::new(
        query,
        zinder_query::WalletEndpointMetadata::default(),
    )
    .into_server();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(
        Server::builder()
            .add_service(server)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.clone().cancelled_owned(),
            ),
    );
    let mut identity = WalletQueryClient::connect(format!("http://{address}")).await?;
    let block_request = wallet::CompactBlockRequest {
        height: 2,
        at_epoch_id: None,
    };
    let range_request = wallet::CompactBlocksInRangeRequest {
        start_height: 2,
        end_height: 1,
        at_epoch_id: None,
        pool_types: vec![wallet::CompactPool::Sapling.into()],
    };
    let expected_block = identity.compact_block(block_request).await?.into_inner();
    let expected_range = identity
        .compact_blocks_in_range(range_request.clone())
        .await?
        .into_inner()
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(compact_range_heights(&expected_range), [2, 1]);
    for encoding in [CompressionEncoding::Gzip, CompressionEncoding::Zstd] {
        let mut client = identity
            .clone()
            .accept_compressed(encoding)
            .send_compressed(encoding);
        let block = client.compact_block(block_request).await?;
        assert_eq!(
            block
                .metadata()
                .get("grpc-encoding")
                .and_then(|header| header.to_str().ok()),
            Some(encoding.to_string().as_str())
        );
        assert_eq!(block.into_inner(), expected_block);
        let range = client
            .compact_blocks_in_range(range_request.clone())
            .await?
            .into_inner()
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(range, expected_range);
        let refusal = client
            .broadcast_transaction(wallet::BroadcastTransactionRequest {
                raw_transaction: vec![0; zinder_runtime::MAX_DECODING_MESSAGE_BYTES + 1],
            })
            .await
            .err()
            .ok_or_else(|| eyre!("oversized compressed transaction was accepted"))?;
        assert_eq!(refusal.code(), Code::ResourceExhausted);
        assert!(refusal.message().contains("size limit"), "{refusal}");
    }
    assert_nullifier_range_compression(&mut identity).await?;
    drop(identity);
    shutdown.cancel();
    task.await??;
    Ok(())
}

fn compact_range_heights(chunks: &[wallet::CompactBlocksInRangeChunk]) -> Vec<u32> {
    use wallet::compact_blocks_in_range_chunk::Body;

    chunks
        .iter()
        .filter_map(|chunk| match chunk.body.as_ref()? {
            Body::Header(_) => None,
            Body::Item(block) => Some(block.height),
        })
        .collect()
}

async fn assert_nullifier_range_compression(
    identity: &mut wallet::wallet_query_client::WalletQueryClient<tonic::transport::Channel>,
) -> eyre::Result<()> {
    use futures_util::TryStreamExt as _;
    use tonic::codec::CompressionEncoding;
    use wallet::compact_nullifiers_in_range_chunk::Body;

    let request = wallet::CompactNullifiersInRangeRequest {
        start_height: 2,
        end_height: 1,
        at_epoch_id: None,
        pool_types: vec![wallet::CompactPool::Sapling.into()],
    };
    let expected_range = identity
        .compact_nullifiers_in_range(request.clone())
        .await?
        .into_inner()
        .try_collect::<Vec<_>>()
        .await?;
    let blocks = expected_range
        .iter()
        .filter_map(|chunk| match chunk.body.as_ref()? {
            Body::Header(_) => None,
            Body::Block(block) => Some(block),
        });
    assert_eq!(blocks.map(|block| block.height).collect::<Vec<_>>(), [2, 1]);
    for encoding in [CompressionEncoding::Gzip, CompressionEncoding::Zstd] {
        let response = identity
            .clone()
            .send_compressed(encoding)
            .accept_compressed(encoding)
            .compact_nullifiers_in_range(request.clone())
            .await?;
        assert_eq!(
            response
                .metadata()
                .get("grpc-encoding")
                .and_then(|header| header.to_str().ok()),
            Some(encoding.to_string().as_str()),
        );
        let range = response.into_inner().try_collect::<Vec<_>>().await?;
        assert_eq!(range, expected_range);
    }
    Ok(())
}

/// An adapter refuses a `CompactBlocksInRange` stream past its limit.
///
/// The refusal is `RESOURCE_EXHAUSTED` with the typed
/// `COMPACT_BLOCK_STREAM_LIMIT_REACHED` reason until an open stream ends, and
/// a finished stream sends its header and every block.
#[tokio::test]
async fn native_compact_block_streams_are_refused_past_the_stream_limit() -> eyre::Result<()> {
    use tokio_stream::StreamExt as _;
    use zinder_proto::v1::wallet::wallet_query_server::WalletQuery as WalletQueryService;

    let activations = sample_regtest_upgrade_activations();
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let adapter = zinder_query::WalletQueryGrpcAdapter::new(
        WalletServingQuery::from_admitted_native_serving_pair(
            WalletServingPairSlot::new(pair),
            ingest_control,
        ),
        zinder_query::WalletEndpointMetadata::default(),
    )
    .with_max_concurrent_compact_block_streams(NonZeroU32::MIN);
    let request = || {
        tonic::Request::new(wallet::CompactBlocksInRangeRequest {
            start_height: 1,
            end_height: 3,
            at_epoch_id: None,
            pool_types: Vec::new(),
        })
    };

    let open = WalletQueryService::compact_blocks_in_range(&adapter, request()).await?;
    let refused = WalletQueryService::compact_blocks_in_range(&adapter, request()).await;
    let refusal = refused
        .err()
        .ok_or_else(|| eyre::eyre!("the second stream was not refused"))?;
    assert_eq!(refusal.code(), tonic::Code::ResourceExhausted);
    let delay = refusal
        .metadata()
        .get("grpc-retry-pushback-ms")
        .ok_or_else(|| eyre!("missing retry pushback"))?
        .to_str()?
        .parse::<i32>()?;
    assert!((500..=1500).contains(&delay));
    let error_info = tonic_types::StatusExt::get_details_error_info(&refusal)
        .ok_or_else(|| eyre::eyre!("the refusal carries no google.rpc.ErrorInfo"))?;
    assert_eq!(error_info.domain, "zinder.dev");
    assert_eq!(error_info.reason, "COMPACT_BLOCK_STREAM_LIMIT_REACHED");

    let chunks = open.into_inner().collect::<Vec<_>>().await;
    assert_eq!(chunks.len(), 4, "one header and three blocks");
    assert!(matches!(
        chunks.first(),
        Some(Ok(wallet::CompactBlocksInRangeChunk {
            body: Some(wallet::compact_blocks_in_range_chunk::Body::Header(_))
        }))
    ));
    let reopened = WalletQueryService::compact_blocks_in_range(&adapter, request()).await?;
    assert_eq!(reopened.into_inner().collect::<Vec<_>>().await.len(), 4);
    Ok(())
}
