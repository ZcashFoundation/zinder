#![allow(
    missing_docs,
    reason = "Integration test names describe the wallet contract under test."
)]

use std::sync::Arc;

use eyre::{Result, eyre};
use futures_util::TryStreamExt as _;
use zinder_client::{
    BlockHeight, BlockId, Capability, CapabilityDescriptor, ChainIndex, CompactBlockNullifiers,
    CompactPool, CompactRangeQuery, EndpointBackedIndex, IndexerError, Network,
    NetworkUpgradeActivations, RemoteChainIndex, RemoteOpenOptions, StandardFee, TransactionId,
    TransparentAddressScriptHash, TransparentOutPoint, TransparentUnspentOutput,
};
use zinder_core::{CompactBlockArtifact, CompactTransparentOutput};
use zinder_query::{
    AdmittedIngestControl, OrderedBroadcaster, RocksDbCanonicalReader, RocksDbWalletReader,
    WalletEndpointMetadata, WalletQueryGrpcAdapter, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_testkit::{
    ChainFixture, IngestControlFixture, MockTransactionBroadcaster, WalletServingStoreFixture,
    sample_regtest_upgrade_activations, with_note_commitments,
};

use super::remote_chain_index::spawn_wallet_query;

const NETWORK: Network = Network::ZcashRegtest;

#[tokio::test]
async fn compact_scans_select_pools_across_settled_and_live_blocks() -> Result<()> {
    let activations = scan_activations()?;
    let chain = with_note_commitments(&ChainFixture::new(NETWORK).extend_blocks(4), &activations)?;
    let chain = with_transparent_scan_output(chain)?;
    let mut store = WalletServingStoreFixture::from_chain_after_live_append(&chain, &activations)?;
    let (slot, control, _control_fixture) = serving_fixture(&mut store).await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(slot, control);
    let endpoint = spawn_wallet_query(WalletQueryGrpcAdapter::new(
        query,
        WalletEndpointMetadata::default(),
    ))
    .await?;
    let index = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: NETWORK,
    })?;
    let descriptor = index.server_info().await?;
    assert!(descriptor.supports(Capability::CompactBlockScan));
    assert!(descriptor.supports(Capability::CompactNullifierRange));
    let snapshot = index.snapshot().await?;
    assert_eq!(
        snapshot.chain_epoch().settled_tip_height,
        BlockHeight::new(3)
    );

    for pool in [
        CompactPool::Sapling,
        CompactPool::Orchard,
        CompactPool::Ironwood,
        CompactPool::Transparent,
    ] {
        let mut scan = CompactRangeQuery::new(BlockHeight::new(4), BlockHeight::new(1));
        scan.pools.push(pool);
        let blocks: Vec<_> = snapshot
            .scan_compact_blocks(scan)
            .await?
            .try_collect()
            .await?;
        assert_eq!(
            blocks
                .iter()
                .map(|block| block.height().value())
                .collect::<Vec<_>>(),
            [4, 3, 2, 1]
        );
        assert_selected_pool(&blocks, pool);
    }

    let mut scan = CompactRangeQuery::new(BlockHeight::new(4), BlockHeight::new(1));
    scan.pools.push(CompactPool::Orchard);
    let blocks: Vec<_> = snapshot
        .scan_compact_nullifiers(scan)
        .await?
        .try_collect()
        .await?;
    assert_orchard_nullifiers(&blocks, &chain);
    Ok(())
}

fn assert_orchard_nullifiers(blocks: &[CompactBlockNullifiers], chain: &ChainFixture) {
    assert_eq!(
        blocks
            .iter()
            .map(|block| block.height.value())
            .collect::<Vec<_>>(),
        [4, 3, 2, 1]
    );
    assert_eq!(
        blocks
            .iter()
            .map(|block| block.block_hash)
            .collect::<Vec<_>>(),
        chain
            .blocks()
            .iter()
            .rev()
            .map(|block| block.hash)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        blocks[0].transactions[0].orchard_nullifiers,
        vec![[0; 32]; 2]
    );
    assert_eq!(
        blocks[1].transactions[0].orchard_nullifiers,
        vec![[0; 32]; 1]
    );
    assert_eq!(blocks[3].transactions.len(), 0);
    assert!(
        blocks
            .iter()
            .flat_map(|block| &block.transactions)
            .all(|tx| tx.sapling_nullifiers.is_empty() && tx.ironwood_nullifiers.is_empty())
    );
}

fn assert_selected_pool(blocks: &[CompactBlockArtifact], pool: CompactPool) {
    let mut observed = 0;
    for transaction in blocks.iter().flat_map(CompactBlockArtifact::transactions) {
        let present = [
            (
                CompactPool::Sapling,
                !transaction.data.sapling_outputs.is_empty(),
            ),
            (
                CompactPool::Orchard,
                !transaction.data.orchard_actions.is_empty(),
            ),
            (
                CompactPool::Ironwood,
                !transaction.data.ironwood_actions.is_empty(),
            ),
            (
                CompactPool::Transparent,
                !transaction.data.transparent_outputs.is_empty(),
            ),
        ];
        for (present_pool, carries_fields) in present {
            assert_eq!(carries_fields, present_pool == pool);
        }
        observed += 1;
    }
    assert!(observed > 0);
}

fn scan_activations() -> Result<NetworkUpgradeActivations> {
    let mut activations = sample_regtest_upgrade_activations().activations().to_vec();
    let ironwood = activations
        .iter_mut()
        .find(|activation| activation.name == "NU6.3")
        .ok_or_else(|| eyre!("fixture has no Ironwood upgrade"))?;
    ironwood.activation_height = BlockHeight::new(3);
    Ok(NetworkUpgradeActivations::new(NETWORK, activations)?)
}

fn with_transparent_scan_output(chain: ChainFixture) -> Result<ChainFixture> {
    let block = chain
        .compact_block_artifacts()
        .pop()
        .ok_or_else(|| eyre!("fixture is empty"))?;
    let mut transactions = block.transactions().to_vec();
    let coinbase = transactions
        .first_mut()
        .ok_or_else(|| eyre!("fixture has no coinbase"))?;
    let script_pub_key = vec![0x51];
    coinbase
        .data
        .transparent_outputs
        .push(CompactTransparentOutput {
            value_zat: 13,
            script_pub_key: script_pub_key.clone(),
        });
    let output = TransparentUnspentOutput::new(
        TransparentAddressScriptHash::from_bytes([0x51; 32]),
        script_pub_key,
        TransparentOutPoint::new(coinbase.transaction_id, 0),
        13,
        block.height(),
        block.block_hash(),
    );
    Ok(chain
        .with_address_output_index(output)
        .with_compact_block_artifact(CompactBlockArtifact::new(
            BlockId::new(block.height(), block.block_hash()),
            block.previous_block_hash(),
            block.time(),
            transactions,
            block.chain_metadata(),
        )?))
}

#[tokio::test]
async fn transparent_address_batch_answers_at_one_epoch_and_deduplicates() -> Result<()> {
    let mut chain = ChainFixture::new(NETWORK).extend_blocks(2);
    let blocks = chain.blocks().to_vec();
    let addresses = [
        TransparentAddressScriptHash::from_bytes([0x51; 32]),
        TransparentAddressScriptHash::from_bytes([0x52; 32]),
    ];
    for ((block, address), amount) in blocks.iter().zip(addresses).zip([5, 13]) {
        chain = chain.with_address_output_index(TransparentUnspentOutput::new(
            address,
            vec![0x51],
            TransparentOutPoint::new(TransactionId::from_bytes(address.as_bytes()), 0),
            amount,
            block.height,
            block.hash,
        ));
    }
    let mut store =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let (slot, control, _control_fixture) = serving_fixture(&mut store).await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(slot, control);
    let endpoint = spawn_wallet_query(WalletQueryGrpcAdapter::new(
        query,
        WalletEndpointMetadata::default(),
    ))
    .await?;
    let index = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: NETWORK,
    })?;
    let descriptor = index.server_info().await?;
    assert!(descriptor.supports(Capability::TransparentAddressBalances));
    assert!(descriptor.supports(Capability::TransparentAddressesUnspentOutputs));
    let snapshot = index.snapshot().await?;
    let batch = [addresses[1], addresses[0], addresses[1]];
    let balance = snapshot.transparent_address_balances(&batch).await?;
    assert_eq!(balance.chain_epoch, snapshot.chain_epoch());
    assert_eq!((balance.address_count, balance.confirmed_zat), (2, 18));
    let outputs: Vec<_> = snapshot
        .transparent_addresses_unspent_outputs(&batch, BlockHeight::new(0))
        .await?
        .try_collect()
        .await?;
    assert!(
        outputs
            .iter()
            .all(|chunk| chunk.chain_epoch == snapshot.chain_epoch())
    );
    assert_eq!(
        outputs
            .iter()
            .map(|chunk| chunk.output.address_script_hash)
            .collect::<Vec<_>>(),
        [addresses[1], addresses[0]]
    );
    let outputs: Vec<_> = snapshot
        .transparent_addresses_unspent_outputs(&batch, BlockHeight::new(2))
        .await?
        .try_collect()
        .await?;
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].output.value_zat, 13);
    Ok(())
}

#[tokio::test]
async fn standard_fee_uses_a_verified_fallback_and_requires_the_capability() -> Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let chain = ChainFixture::new(NETWORK).extend_blocks(2);
    let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (slot, control, _control_fixture) = serving_fixture(&mut store).await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(slot.clone(), control);
    let endpoint = spawn_wallet_query(WalletQueryGrpcAdapter::new(
        query.clone(),
        WalletEndpointMetadata::default(),
    ))
    .await?;
    let storage_only = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: NETWORK,
    })?;
    assert!(
        !storage_only
            .server_info()
            .await?
            .supports(Capability::StandardFee)
    );
    assert!(matches!(
        storage_only.standard_fee().await,
        Err(IndexerError::FailedPrecondition { .. })
    ));

    let fee = StandardFee::new(5_000, 3).ok_or_else(|| eyre!("invalid fixture fee"))?;
    let primary = MockTransactionBroadcaster::accepted(TransactionId::from_bytes([1; 32]))
        .with_node_activations(activations.clone());
    let fallback = MockTransactionBroadcaster::accepted(TransactionId::from_bytes([2; 32]))
        .with_node_activations(activations)
        .with_standard_fee(fee);
    let nodes = OrderedBroadcaster::new(primary.clone(), [fallback.clone()]);
    nodes.verify(&slot).await?;
    assert_eq!(
        (
            primary.capability_probe_count(),
            fallback.capability_probe_count()
        ),
        (1, 1)
    );
    let query = query.with_node_broadcast(Some(nodes));
    let endpoint = spawn_wallet_query(WalletQueryGrpcAdapter::new(
        query,
        WalletEndpointMetadata::default(),
    ))
    .await?;
    let index = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: NETWORK,
    })?;
    assert!(index.server_info().await?.supports(Capability::StandardFee));
    assert_eq!(index.standard_fee().await?, fee);
    Ok(())
}

async fn serving_fixture(
    store: &mut WalletServingStoreFixture,
) -> Result<(
    WalletServingPairSlot,
    AdmittedIngestControl,
    IngestControlFixture,
)> {
    let (canonical, wallet) = store.take_readers()?;
    let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?));
    let control_fixture = IngestControlFixture::spawn(NETWORK).await?;
    let control = AdmittedIngestControl::connect(control_fixture.endpoint(), None, NETWORK).await?;
    Ok((slot, control, control_fixture))
}
