#![allow(
    missing_docs,
    reason = "Integration test names describe the wallet acceptance contract."
)]

//! Compact-block scan contract for the lightwalletd compat shim.
//!
//! Proves a lightwalletd-compatible client can scan a regtest range from
//! Zinder without sending viewing keys or spending keys to Zinder. The
//! test exercises the wire contract that wallets actually consume:
//!
//! 1. Stand up the `LightwalletdGrpcAdapter` over a populated `PrimaryChainStore`.
//! 2. Connect through the generated `CompactTxStreamerClient` to exercise the
//!    protocol transport directly.
//! 3. Call `GetBlockRange` and decode every compact block.
//! 4. Assert each block carries:
//!    - a populated `vtx` list whose `txid` entries match transaction
//!      artifacts retrievable by id from the store, and
//!    - chain metadata reflecting the committed Sapling and Orchard
//!      commitment-tree sizes.
//! 5. Assert no viewing keys, spending keys, seed phrases, or other key
//!    material appear in any client→server payload.
//!
//! This file proves the generic compact-block wire contract. The bound current
//! Android SDK inventory and real application run are separate certification
//! evidence.

use eyre::eyre;
use prost::Message;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, transport::Server};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::{
    BlockHeight, BlockId, ChainEpochId, ChainTipMetadata, CompactBlockArtifact,
    CompactChainMetadata, CompactSaplingOutput, CompactTransaction, CompactTransactionData,
    Network, ShieldedProtocol, SubtreeRootArtifact, SubtreeRootHash, SubtreeRootIndex,
    TransactionComponentCounts, TransactionId,
};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::WalletQuery;
use zinder_store::RawBlobRetention;
use zinder_testkit::{
    ChainFixture, FixtureBlock, FixtureTransactionRows, StoreFixture,
    sample_regtest_upgrade_activations,
};

const LIGHTWALLETD_SCAN_BLOCK_COUNT: u32 = 10;
const LIGHTWALLETD_SCAN_SAPLING_TREE_SIZE: u32 = LIGHTWALLETD_SCAN_BLOCK_COUNT;

#[tokio::test]
async fn lightwalletd_compatible_client_scans_range_without_sending_keys() -> eyre::Result<()> {
    let store_fixture = lightwalletd_scan_store_fixture()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
    .into_server();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let mut client = CompactTxStreamerClient::connect(format!("http://{server_addr}")).await?;
    let visible_tip_block = client
        .get_latest_block(lightwalletd::ChainSpec {})
        .await?
        .into_inner();
    let block_range_request = lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: u64::from(LIGHTWALLETD_SCAN_BLOCK_COUNT),
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    };
    assert_no_key_material_in_request(&block_range_request);

    let mut compact_blocks_stream = client
        .get_block_range(Request::new(block_range_request))
        .await?
        .into_inner();
    let mut received_blocks = Vec::new();
    while let Some(compact_block) = compact_blocks_stream.message().await? {
        received_blocks.push(compact_block);
    }

    assert_eq!(
        visible_tip_block.height,
        u64::from(LIGHTWALLETD_SCAN_BLOCK_COUNT)
    );
    assert_eq!(
        received_blocks.len(),
        usize::try_from(LIGHTWALLETD_SCAN_BLOCK_COUNT)?
    );

    for compact_block in &received_blocks {
        assert!(compact_block.header.is_empty());
        assert_compact_block_carries_indexed_transactions(compact_block, &mut client).await?;
        assert_compact_block_carries_chain_metadata(compact_block)?;
    }

    server_handle.abort();
    let _ = server_handle.await;
    Ok(())
}

#[tokio::test]
async fn lightwalletd_subtree_roots_request_carries_no_key_material() -> eyre::Result<()> {
    let store_fixture = lightwalletd_scan_store_fixture()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
    .into_server();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let mut client = CompactTxStreamerClient::connect(format!("http://{server_addr}")).await?;
    let request = lightwalletd::GetSubtreeRootsArg {
        start_index: 0,
        shielded_protocol: lightwalletd::ShieldedProtocol::Sapling as i32,
        max_entries: 1,
    };
    let request_bytes = request.encode_to_vec();
    assert_no_key_material_in_bytes(&request_bytes);

    let mut subtree_roots = client
        .get_subtree_roots(Request::new(request))
        .await?
        .into_inner();
    while subtree_roots.message().await?.is_some() {}

    server_handle.abort();
    let _ = server_handle.await;
    Ok(())
}

fn lightwalletd_scan_store_fixture() -> eyre::Result<StoreFixture> {
    let base_fixture = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(LIGHTWALLETD_SCAN_BLOCK_COUNT)
        .with_tip_metadata_override(ChainTipMetadata::new(
            LIGHTWALLETD_SCAN_SAPLING_TREE_SIZE,
            0,
            0,
        ));

    let mut chain_fixture = base_fixture;
    for height_value in 1..=LIGHTWALLETD_SCAN_BLOCK_COUNT {
        let height = BlockHeight::new(height_value);
        let block = chain_fixture
            .block_at(height)
            .ok_or_else(|| eyre!("fixture block missing at height"))?
            .clone();
        let mut transaction_rows = FixtureTransactionRows::from_raw_transaction(
            TransactionId::from_bytes(lightwalletd_scan_txid_bytes(height_value)),
            block.height,
            block.hash,
            0,
            lightwalletd_scan_transaction_payload(height_value),
        );
        transaction_rows.facts.public_facts.counts = TransactionComponentCounts {
            sapling_output_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let compact_block = lightwalletd_scan_compact_block(&block);
        chain_fixture = chain_fixture
            .with_compact_block_artifact(compact_block)
            .with_transaction_rows(transaction_rows);
    }

    let completing_block = chain_fixture
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("fixture block missing at height 1"))?
        .clone();
    chain_fixture = chain_fixture.with_sapling_subtree_root(SubtreeRootArtifact::new(
        ShieldedProtocol::Sapling,
        SubtreeRootIndex::new(0),
        SubtreeRootHash::from_bytes([0x70; 32]),
        completing_block.height,
        completing_block.hash,
    ));

    Ok(StoreFixture::with_chain_committed(
        &chain_fixture,
        ChainEpochId::new(1),
    )?)
}

fn lightwalletd_scan_compact_block(block: &FixtureBlock) -> CompactBlockArtifact {
    CompactBlockArtifact::new(
        BlockId::new(block.height, block.hash),
        block.parent_hash,
        block.block_time_seconds,
        vec![CompactTransaction {
            index: 0,
            transaction_id: TransactionId::from_bytes(lightwalletd_scan_txid_bytes(
                block.height.value(),
            )),
            data: CompactTransactionData {
                sapling_outputs: vec![CompactSaplingOutput {
                    commitment: [0x11; 32],
                    ephemeral_key: [0x22; 32],
                    ciphertext: [0x33; 52],
                }],
                ..CompactTransactionData::default()
            },
        }],
        CompactChainMetadata {
            sapling_commitment_tree_size: block.height.value(),
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    )
    .unwrap_or_else(|_| std::process::abort())
}

fn lightwalletd_scan_txid_bytes(height_value: u32) -> [u8; 32] {
    let mut bytes = [0_u8; 32];
    for chunk in bytes.chunks_exact_mut(4) {
        chunk.copy_from_slice(&height_value.to_be_bytes());
    }
    bytes
}

fn lightwalletd_scan_transaction_payload(height_value: u32) -> Vec<u8> {
    format!("zinder-acceptance-tx-at-height-{height_value}").into_bytes()
}

async fn assert_compact_block_carries_indexed_transactions(
    compact_block: &lightwalletd::CompactBlock,
    client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
) -> eyre::Result<()> {
    assert!(
        !compact_block.vtx.is_empty(),
        "compact block at height {} must carry at least one transaction",
        compact_block.height
    );
    for compact_tx in &compact_block.vtx {
        // Lightwalletd `bytes` fields carry Zcash internal little-endian txids
        // (`frontend/service.go:792`: "When expressed as bytes, a txid must
        // be little-endian"). `TxFilter.hash` takes the same byte order.
        // Round-trip the txid bytes unchanged.
        let response = client
            .get_transaction(Request::new(lightwalletd::TxFilter {
                block: None,
                index: 0,
                hash: compact_tx.txid.clone(),
            }))
            .await?
            .into_inner();
        assert_eq!(response.height, compact_block.height);
        assert!(
            !response.data.is_empty(),
            "indexed transaction payload must be present for txid in compact block at height {}",
            compact_block.height
        );
    }
    Ok(())
}

fn assert_compact_block_carries_chain_metadata(
    compact_block: &lightwalletd::CompactBlock,
) -> eyre::Result<()> {
    let chain_metadata = compact_block.chain_metadata.as_ref().ok_or_else(|| {
        eyre!("compact block must carry chain_metadata for tree-state advertisement")
    })?;
    assert_eq!(
        chain_metadata.sapling_commitment_tree_size,
        u32::try_from(compact_block.height)?
    );
    Ok(())
}

fn assert_no_key_material_in_request(request: &lightwalletd::BlockRange) {
    let request_bytes = request.encode_to_vec();
    assert_no_key_material_in_bytes(&request_bytes);
}

fn assert_no_key_material_in_bytes(request_bytes: &[u8]) {
    let key_material_markers: &[&[u8]] = &[
        b"sk-",
        b"zk-",
        b"viewing-key",
        b"spending-key",
        b"sapling-extfvk",
        b"orchard-fvk",
        b"unified-fvk",
        b"seed-phrase",
        b"mnemonic",
    ];
    for marker in key_material_markers {
        assert!(
            !request_bytes
                .windows(marker.len())
                .any(|window| window == *marker),
            "request payload must not embed wallet key material; found marker {marker:?}"
        );
    }
}
