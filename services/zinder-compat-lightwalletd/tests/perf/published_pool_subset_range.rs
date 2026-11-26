//! Throughput and speedup benchmark for `[PoolType::Orchard]` `GetBlockRangeNullifiers`
//! over published `ZSHRNG01` history plus live tail vs canonical `RocksDB` fallback.

use std::{num::NonZeroU32, sync::Arc, time::Instant};

use eyre::eyre;
use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tonic::transport::{Channel, Server};
use zinder_compat_lightwalletd::{
    LightwalletdGrpcAdapter, PublishedShieldedHistory, ShieldedCatalogPublisher,
};
use zinder_core::{
    BlockHeight, BlockId, CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput,
    CompactSaplingSpend, CompactShieldedAction, CompactTransaction, CompactTransactionData,
    Network, TransactionId,
};
use zinder_proto::compat::lightwalletd::{
    self, PoolType, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::{
    RocksDbCanonicalReader, RocksDbWalletReader, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, sample_regtest_upgrade_activations,
};

const TOTAL_BLOCKS: u32 = 4_000;
const SETTLED_TIP: u32 = 3_900;
const SEGMENT_BLOCKS: u32 = 1_000;

#[cfg(debug_assertions)]
const MAX_PUBLISHED_ORCHARD_SCAN_MS: u128 = 5_000;
#[cfg(not(debug_assertions))]
const MAX_PUBLISHED_ORCHARD_SCAN_MS: u128 = 500;

fn perf_commitment(seed: u8) -> [u8; 32] {
    let mut cmx = [0; 32];
    cmx[0] = seed;
    cmx
}

fn perf_orchard_action(seed: u8) -> CompactShieldedAction {
    CompactShieldedAction {
        nullifier: [seed; 32],
        commitment: perf_commitment(seed.wrapping_add(1)),
        ephemeral_key: [seed.wrapping_add(2); 32],
        ciphertext: [seed.wrapping_add(3); 52],
    }
}

fn perf_sapling_output(seed: u8) -> CompactSaplingOutput {
    CompactSaplingOutput {
        commitment: perf_commitment(seed),
        ephemeral_key: [seed.wrapping_add(1); 32],
        ciphertext: [seed.wrapping_add(2); 52],
    }
}

fn perf_tx_id(height_val: u32, slot: u8) -> TransactionId {
    let mut raw_id = [slot; 32];
    raw_id[0..4].copy_from_slice(&height_val.to_le_bytes());
    TransactionId::from_bytes(raw_id)
}

fn attach_perf_tx_rows(
    mut chain: ChainFixture,
    blk: &zinder_testkit::FixtureBlock,
    shielded_tx_id: TransactionId,
) -> ChainFixture {
    let filler_id = perf_tx_id(blk.height.value(), 0xc0);
    chain = chain.with_transaction_rows(zinder_testkit::FixtureTransactionRows::coinbase(
        filler_id,
        blk.height,
        blk.hash,
        b"perf-coinbase".as_slice(),
    ));
    chain.with_transaction_rows(
        zinder_testkit::FixtureTransactionRows::from_raw_transaction(
            shielded_tx_id,
            blk.height,
            blk.hash,
            1,
            b"perf-shielded-tx".as_slice(),
        ),
    )
}

fn build_perf_chain() -> eyre::Result<ChainFixture> {
    let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(TOTAL_BLOCKS);
    let blocks = chain.blocks().to_vec();
    let mut sapling_tree_size = 0_u32;
    let mut orchard_tree_size = 0_u32;
    for blk in blocks {
        let height_val = blk.height.value();
        let txs = if height_val % 10 == 0 {
            orchard_tree_size += 2;
            let tx_id = perf_tx_id(height_val, 0x44);
            chain = attach_perf_tx_rows(chain, &blk, tx_id);
            vec![CompactTransaction {
                index: 1,
                transaction_id: tx_id,
                data: CompactTransactionData {
                    fee_zat: Some(10_000),
                    orchard_actions: vec![perf_orchard_action(0x11), perf_orchard_action(0x15)],
                    ..Default::default()
                },
            }]
        } else if height_val % 10 == 5 {
            sapling_tree_size += 2;
            let tx_id = perf_tx_id(height_val, 0x55);
            chain = attach_perf_tx_rows(chain, &blk, tx_id);
            vec![CompactTransaction {
                index: 1,
                transaction_id: tx_id,
                data: CompactTransactionData {
                    fee_zat: Some(10_000),
                    sapling_spends: vec![CompactSaplingSpend {
                        nullifier: [0x21; 32],
                    }],
                    sapling_outputs: vec![perf_sapling_output(0x22), perf_sapling_output(0x25)],
                    ..Default::default()
                },
            }]
        } else {
            Vec::new()
        };
        let parent_hash = if height_val == 1 {
            Network::ZcashRegtest.genesis_hash()
        } else {
            blk.parent_hash
        };
        chain = chain.with_compact_block_artifact(CompactBlockArtifact::new(
            BlockId::new(blk.height, blk.hash),
            parent_hash,
            blk.block_time_seconds,
            txs,
            CompactChainMetadata {
                sapling_commitment_tree_size: sapling_tree_size,
                orchard_commitment_tree_size: orchard_tree_size,
                ironwood_commitment_tree_size: 0,
            },
        )?);
    }
    Ok(chain)
}

fn block_range_for_pools(pool_types: Vec<i32>) -> lightwalletd::BlockRange {
    lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: u64::from(TOTAL_BLOCKS),
            hash: Vec::new(),
        }),
        pool_types,
    }
}

async fn drain_encoded_nullifier_range(
    adapter: &LightwalletdGrpcAdapter<WalletServingQuery<()>>,
    pool_types: Vec<i32>,
) -> eyre::Result<(usize, usize)> {
    use zinder_proto::compat::lightwalletd::EncodedBlockRangeNullifiers as _;
    let mut stream = adapter
        .get_encoded_block_range_nullifiers(tonic::Request::new(block_range_for_pools(pool_types)))
        .await?
        .into_inner();
    let mut block_count = 0_usize;
    let mut payload_bytes = 0_usize;
    while let Some(next_block) = stream.next().await {
        let block = next_block?;
        block_count += 1;
        payload_bytes += block.payload_len();
    }
    Ok((block_count, payload_bytes))
}

#[allow(
    deprecated,
    reason = "Benchmarks GetBlockRangeNullifiers throughput for PoolType::Orchard."
)]
async fn drain_nullifier_range(
    client: &mut CompactTxStreamerClient<Channel>,
    pool_types: Vec<i32>,
) -> eyre::Result<(usize, usize)> {
    let mut stream = client
        .get_block_range_nullifiers(block_range_for_pools(pool_types))
        .await?
        .into_inner();
    let mut block_count = 0_usize;
    let mut tx_count = 0_usize;
    while let Some(next_block) = stream.next().await {
        let block = next_block?;
        block_count += 1;
        tx_count += block.vtx.len();
    }
    Ok((block_count, tx_count))
}

async fn start_perf_endpoint(
    adapter: LightwalletdGrpcAdapter<WalletServingQuery<()>>,
) -> eyre::Result<(
    CompactTxStreamerClient<Channel>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(
        Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let client = CompactTxStreamerClient::connect(format!("http://{addr}")).await?;
    Ok((client, handle))
}

async fn setup_perf_adapters(
    dir: &std::path::Path,
) -> eyre::Result<(
    LightwalletdGrpcAdapter<WalletServingQuery<()>>,
    LightwalletdGrpcAdapter<WalletServingQuery<()>>,
    zinder_testkit::WalletServingStoreFixture,
)> {
    let chain = build_perf_chain()?;
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(SETTLED_TIP)),
        &sample_regtest_upgrade_activations(),
    )?;
    let mut paths = writer.into_serving_store_fixture()?;
    let (mut canonical, wallet) = paths.take_readers()?;
    canonical.try_catch_up()?;
    let reader = Arc::new(RocksDbCanonicalReader::new(canonical)?);
    let pair =
        WalletServingReadPair::new(reader.clone(), Arc::new(RocksDbWalletReader::new(wallet)))?;
    let query: WalletServingQuery<()> =
        WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(pair)));
    let catalog_dir = dir.join("catalog");
    let (signer, key) = crate::common::shielded_catalog_keys(dir)?;
    let seg_blocks = NonZeroU32::new(SEGMENT_BLOCKS).ok_or_else(|| eyre!("zero segment size"))?;
    let mut publisher =
        ShieldedCatalogPublisher::open(&catalog_dir, &signer, BlockHeight::new(1), seg_blocks)?;
    while publisher.publish_next(&reader)? {}
    let history = Arc::new(
        PublishedShieldedHistory::open(
            &catalog_dir.join("current.json"),
            &key,
            false,
            reader.as_ref(),
        )
        .await?,
    );
    let pub_adapter =
        LightwalletdGrpcAdapter::new(query.clone()).with_published_shielded_history(history);
    let can_adapter = LightwalletdGrpcAdapter::new(query);
    Ok((pub_adapter, can_adapter, paths))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orchard_nullifier_range_scan_over_published_history_and_tail_beats_canonical_fallback()
-> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let (pub_adapter, can_adapter, _paths) = setup_perf_adapters(dir.path()).await?;
    let orchard_pools = vec![PoolType::Orchard as i32];

    let _ = drain_encoded_nullifier_range(&pub_adapter, Vec::new()).await?;
    let _ = drain_encoded_nullifier_range(&can_adapter, orchard_pools.clone()).await?;

    let started_srv_can = Instant::now();
    let (srv_can_blocks, srv_can_bytes) =
        drain_encoded_nullifier_range(&can_adapter, orchard_pools.clone()).await?;
    let srv_canonical_us = started_srv_can.elapsed().as_micros();

    let started_srv_def = Instant::now();
    let (srv_def_blocks, srv_def_bytes) =
        drain_encoded_nullifier_range(&pub_adapter, Vec::new()).await?;
    let srv_default_us = started_srv_def.elapsed().as_micros();

    let started_srv_orc = Instant::now();
    let (srv_orc_blocks, srv_orc_bytes) =
        drain_encoded_nullifier_range(&pub_adapter, orchard_pools.clone()).await?;
    let srv_orchard_us = started_srv_orc.elapsed().as_micros();

    let (mut pub_client, pub_server) = start_perf_endpoint(pub_adapter).await?;
    let (mut can_client, can_server) = start_perf_endpoint(can_adapter).await?;
    let _ = drain_nullifier_range(&mut pub_client, Vec::new()).await?;
    let _ = drain_nullifier_range(&mut can_client, orchard_pools.clone()).await?;

    let started_canonical = Instant::now();
    let (can_blocks, can_txs) =
        drain_nullifier_range(&mut can_client, orchard_pools.clone()).await?;
    let canonical_micros = started_canonical.elapsed().as_micros();

    let started_default = Instant::now();
    let (def_blocks, def_txs) = drain_nullifier_range(&mut pub_client, Vec::new()).await?;
    let default_micros = started_default.elapsed().as_micros();

    let started_orchard = Instant::now();
    let (orc_blocks, orc_txs) = drain_nullifier_range(&mut pub_client, orchard_pools).await?;
    let orchard_micros = started_orchard.elapsed().as_micros();
    let orchard_millis = orchard_micros / 1_000;

    pub_server.abort();
    can_server.abort();

    assert_eq!(srv_can_blocks, usize::try_from(TOTAL_BLOCKS)?);
    assert_eq!(srv_def_blocks, usize::try_from(TOTAL_BLOCKS)?);
    assert_eq!(srv_orc_blocks, usize::try_from(TOTAL_BLOCKS)?);
    assert_eq!(srv_orc_bytes, srv_can_bytes);
    assert!(srv_orc_bytes < srv_def_bytes);
    assert_eq!(can_blocks, usize::try_from(TOTAL_BLOCKS)?);
    assert_eq!(def_blocks, usize::try_from(TOTAL_BLOCKS)?);
    assert_eq!(orc_blocks, usize::try_from(TOTAL_BLOCKS)?);
    assert_eq!(orc_txs, can_txs);
    assert!(orc_txs < def_txs);

    #[allow(
        clippy::print_stderr,
        reason = "T2 perf benchmark reports measured durations across the 4,000-block corpus."
    )]
    {
        eprintln!(
            "compat_orchard_nullifier_perf blocks={TOTAL_BLOCKS} orchard_txs={orc_txs} default_txs={def_txs} server_canonical_us={srv_canonical_us} server_published_orchard_us={srv_orchard_us} server_published_default_us={srv_default_us} grpc_canonical_us={canonical_micros} grpc_published_orchard_us={orchard_micros} grpc_published_default_us={default_micros}"
        );
    }

    assert!(
        orchard_millis <= MAX_PUBLISHED_ORCHARD_SCAN_MS,
        "published Orchard nullifier scan took {orchard_millis} ms (budget {MAX_PUBLISHED_ORCHARD_SCAN_MS} ms)"
    );
    Ok(())
}
