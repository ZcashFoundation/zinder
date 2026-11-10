#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::sync::Arc;

use eyre::eyre;
use prost::Message;
use zinder_core::wire::encode_rpc_block_hash_hex;
use zinder_core::{
    BlockBlobArtifact, BlockHeight, BlockHeightRange, ChainEpoch, ChainEpochId, Network,
};
use zinder_proto::{
    capabilities::{WALLET_READ_FULL_BLOCK_AT_V1, WALLET_READ_FULL_BLOCK_RANGE_V1},
    v1::wallet,
};
use zinder_query::{
    ArtifactKey, DEFAULT_MAX_FULL_BLOCK_RANGE, FullBlockStream, QueryError, WalletQueryApi,
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_store::{ArtifactFamily, RawBlobRetention};
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

use crate::common::{serving_query_after_live_append, serving_query_over};

/// Drains a full-block stream into its pinned epoch, the blobs delivered in
/// order, and the single terminal error if the stream ended with one.
async fn drain_full_block_stream(
    mut stream: FullBlockStream,
) -> (ChainEpoch, Vec<BlockBlobArtifact>, Option<QueryError>) {
    let chain_epoch = stream.chain_epoch;
    let mut blobs = Vec::new();
    let mut terminal_error = None;
    while let Some(block) = stream.blocks.recv().await {
        match block {
            Ok(blob) => blobs.push(blob),
            Err(error) => {
                terminal_error = Some(error);
                break;
            }
        }
    }
    (chain_epoch, blobs, terminal_error)
}

fn all_retention_chain(block_count: u32) -> ChainFixture {
    ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(block_count)
}

/// Block blobs the production fixture publishes for `chain`, whose first
/// block is re-parented onto the network genesis hash.
fn expected_block_blobs(chain: &ChainFixture) -> Vec<BlockBlobArtifact> {
    chain
        .blocks()
        .iter()
        .enumerate()
        .map(|(index, block)| {
            let blob = block.block_blob_artifact();
            let parent_hash = if index == 0 {
                Network::ZcashRegtest.genesis_hash()
            } else {
                blob.parent_hash
            };
            BlockBlobArtifact::new(
                blob.height,
                blob.block_hash,
                parent_hash,
                blob.raw_block_bytes,
            )
        })
        .collect()
}

#[tokio::test]
async fn full_block_at_returns_serialized_bytes_height_and_hash() -> eyre::Result<()> {
    let chain = all_retention_chain(1);
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;
    let chain_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;

    let full_block = wallet_query
        .full_block_at(BlockHeight::new(1), None)
        .await?;

    assert_eq!(full_block.chain_epoch, chain_epoch);
    assert_eq!(full_block.block_blob, expected_block_blobs(&chain)[0]);

    Ok(())
}

#[tokio::test]
async fn full_block_range_streams_blocks_in_order() -> eyre::Result<()> {
    let chain = all_retention_chain(2);
    let (wallet_query, _store_fixture) = serving_query_after_live_append(&chain)?;
    let current_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;

    let stream = wallet_query
        .full_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2)),
            None,
        )
        .await?;
    let (chain_epoch, blobs, terminal_error) = drain_full_block_stream(stream).await;

    assert_eq!(current_epoch.id, ChainEpochId::new(2));
    assert_eq!(chain_epoch, current_epoch);
    assert_eq!(blobs, expected_block_blobs(&chain));
    assert!(terminal_error.is_none(), "clean range must not error");

    Ok(())
}

#[tokio::test]
async fn full_block_range_chunk_uses_native_wallet_proto_shape() -> eyre::Result<()> {
    let chain = all_retention_chain(1);
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;
    let chain_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;
    let block_blob = expected_block_blobs(&chain)
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("missing fixture block blob"))?;

    let stream = wallet_query
        .full_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1)),
            None,
        )
        .await?;
    let (stream_chain_epoch, blobs, terminal_error) = drain_full_block_stream(stream).await;
    assert!(
        terminal_error.is_none(),
        "single retained block must not error"
    );
    let response_block_blob = blobs
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("missing full block"))?;
    let chunk = wallet::FullBlocksInRangeChunk {
        chain_view: Some(zinder_store::chain_view_message(stream_chain_epoch)),
        full_block: Some(wallet::FullBlock {
            height: response_block_blob.height.value(),
            block_hash: encode_rpc_block_hash_hex(response_block_blob.block_hash),
            payload_bytes: response_block_blob.raw_block_bytes.clone(),
            parent_block_hash: encode_rpc_block_hash_hex(response_block_blob.parent_hash),
        }),
    };
    let decoded_chunk = wallet::FullBlocksInRangeChunk::decode(chunk.encode_to_vec().as_slice())?;
    let response_chain_epoch = decoded_chunk
        .chain_view
        .as_ref()
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .ok_or_else(|| eyre!("missing chunk chain epoch"))?;
    let response_full_block = decoded_chunk
        .full_block
        .as_ref()
        .ok_or_else(|| eyre!("missing chunk full block"))?;

    assert_eq!(response_chain_epoch.chain_epoch_id, chain_epoch.id.value());
    assert_eq!(response_full_block.height, block_blob.height.value());
    assert_eq!(
        response_full_block.block_hash,
        encode_rpc_block_hash_hex(block_blob.block_hash)
    );
    assert_eq!(
        response_full_block.parent_block_hash,
        encode_rpc_block_hash_hex(block_blob.parent_hash)
    );
    assert_eq!(
        response_full_block.payload_bytes,
        block_blob.raw_block_bytes
    );

    Ok(())
}

#[tokio::test]
async fn full_block_range_streams_one_thousand_blocks_under_one_epoch() -> eyre::Result<()> {
    let block_count = DEFAULT_MAX_FULL_BLOCK_RANGE.get();
    let chain = all_retention_chain(block_count);
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;

    let stream = wallet_query
        .full_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(block_count)),
            None,
        )
        .await?;
    let (chain_epoch, blobs, terminal_error) = drain_full_block_stream(stream).await;

    assert!(terminal_error.is_none(), "retained range must not error");
    assert_eq!(chain_epoch.id, ChainEpochId::new(1));
    assert_eq!(blobs.len(), usize::try_from(block_count)?);
    for (offset, blob) in blobs.iter().enumerate() {
        let expected = BlockHeight::new(u32::try_from(offset)?.saturating_add(1));
        assert_eq!(
            blob.height, expected,
            "blocks must be ascending and contiguous"
        );
    }

    Ok(())
}

#[tokio::test]
async fn full_block_range_above_cap_is_rejected() -> eyre::Result<()> {
    let chain = all_retention_chain(1);
    let (wallet_query, _store_fixture) = serving_query_over(&chain)?;
    let over_cap = DEFAULT_MAX_FULL_BLOCK_RANGE.get().saturating_add(1);

    let outcome = wallet_query
        .full_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(over_cap)),
            None,
        )
        .await;

    assert!(
        matches!(
            outcome,
            Err(QueryError::BlockRangeTooLarge { requested, maximum })
                if requested == usize::try_from(over_cap)?
                    && maximum == usize::try_from(DEFAULT_MAX_FULL_BLOCK_RANGE.get())?
        ),
        "an over-cap range must be rejected before streaming, got {outcome:?}"
    );

    Ok(())
}

#[tokio::test]
async fn serving_pair_with_all_retention_advertises_and_serves_full_blocks() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(2);
    let activations = Arc::new(sample_regtest_upgrade_activations());
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

    assert!(
        query
            .native_endpoint_capabilities()
            .contains(WALLET_READ_FULL_BLOCK_AT_V1)
    );
    assert!(
        query
            .native_endpoint_capabilities()
            .contains(WALLET_READ_FULL_BLOCK_RANGE_V1)
    );

    let tip = query.visible_tip_block(None).await?;
    let block = query
        .full_block_at(BlockHeight::new(1), Some(tip.chain_epoch.id))
        .await?;
    assert_eq!(block.chain_epoch, tip.chain_epoch);
    assert_eq!(block.block_blob.height, BlockHeight::new(1));

    let stream = query
        .full_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2)),
            Some(tip.chain_epoch.id),
        )
        .await?;
    let (stream_epoch, blobs, terminal_error) = drain_full_block_stream(stream).await;
    assert_eq!(stream_epoch, tip.chain_epoch);
    assert_eq!(
        blobs.iter().map(|blob| blob.height).collect::<Vec<_>>(),
        vec![BlockHeight::new(1), BlockHeight::new(2)]
    );
    assert!(terminal_error.is_none());
    Ok(())
}

/// Absent full-block range heights end the stream with a typed coverage miss.
///
/// Above the visible tip and below the retained floor, the stream reports
/// `ArtifactUnavailable` for the first absent height, matching the compact
/// range read on the same serving pair.
#[tokio::test]
async fn serving_pair_full_block_range_outside_retained_history_is_artifact_unavailable()
-> eyre::Result<()> {
    let chain = all_retention_chain(3);
    let (query, _store_fixture) = serving_query_over(&chain)?;

    let cases = [
        ("start above visible tip", 4_u32, 5_u32, 4_u32, 0_usize),
        ("start at unretained height 0", 0, 1, 0, 0),
        ("only unretained height 0", 0, 0, 0, 0),
        ("end above visible tip", 3, 4, 4, 1),
    ];
    let mut mismatches = Vec::new();
    for (label, start_height, end_height, missing_height, delivered) in cases {
        let stream = query
            .full_blocks_in_range(
                BlockHeightRange::inclusive(
                    BlockHeight::new(start_height),
                    BlockHeight::new(end_height),
                ),
                None,
            )
            .await?;
        let (_, blobs, terminal_error) = drain_full_block_stream(stream).await;
        let error_matches = matches!(
            &terminal_error,
            Some(QueryError::ArtifactUnavailable {
                family: ArtifactFamily::BlockBlob,
                key: ArtifactKey::BlockHeight(height),
            }) if *height == BlockHeight::new(missing_height)
        );
        if !error_matches || blobs.len() != delivered {
            mismatches.push(format!(
                "{label} ({start_height}..={end_height}): expected {delivered} blobs then ArtifactUnavailable(block_blob, {missing_height}), observed {} blobs then {terminal_error:?}",
                blobs.len(),
            ));
        }
    }

    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(eyre!(mismatches.join("\n")))
    }
}

#[tokio::test]
async fn serving_pair_without_block_retention_omits_and_rejects_full_blocks() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(1);
    let activations = Arc::new(sample_regtest_upgrade_activations());
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

    assert!(
        !query
            .native_endpoint_capabilities()
            .contains(WALLET_READ_FULL_BLOCK_AT_V1)
    );
    assert!(
        !query
            .native_endpoint_capabilities()
            .contains(WALLET_READ_FULL_BLOCK_RANGE_V1)
    );
    assert!(matches!(
        query.full_block_at(BlockHeight::new(1), None).await,
        Err(QueryError::EndpointCapabilityUnavailable {
            capability: WALLET_READ_FULL_BLOCK_AT_V1,
        })
    ));
    assert!(matches!(
        query
            .full_blocks_in_range(
                BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1)),
                None,
            )
            .await,
        Err(QueryError::EndpointCapabilityUnavailable {
            capability: WALLET_READ_FULL_BLOCK_RANGE_V1,
        })
    ));
    Ok(())
}
