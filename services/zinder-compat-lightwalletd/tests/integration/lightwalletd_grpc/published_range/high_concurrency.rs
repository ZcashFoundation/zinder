use std::{num::NonZeroU32, path::Path, sync::Arc, time::Duration};

use eyre::{Result, ensure, eyre};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code,
    transport::{Channel, Endpoint as TonicEndpoint, Server},
};
use zinder_compat_lightwalletd::{
    CanonicalReaderSlot, CompactBlocksGrpcAdapter, LightwalletdGrpcAdapter,
    PublishedShieldedHistory, ShieldedCatalogPublisher,
};
use zinder_core::{
    BlockHeight, BlockId, CompactBlockArtifact, CompactChainMetadata, CompactSaplingSpend,
    CompactTransaction, CompactTransactionData, TransactionId,
};
use zinder_proto::compat::lightwalletd::compact_tx_streamer_client::CompactTxStreamerClient;
use zinder_store::RawBlobRetention;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
    sample_regtest_upgrade_activations,
};

use super::{Endpoint, NETWORK, ReaderFixture, block_range_request, drain_slowly, fixture_block};

const TOTAL_MIXED_BLOCKS: u32 = 2_120;
const SETTLED_MIXED_BLOCKS: u32 = 2_050;
const SEGMENT_SPAN: u32 = 700;

fn build_chain_with_spends(total: u32, spends_at: fn(u32) -> usize) -> Result<ChainFixture> {
    let mut chain = ChainFixture::new(NETWORK)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(total);
    let metadata = CompactChainMetadata {
        sapling_commitment_tree_size: 0,
        orchard_commitment_tree_size: 0,
        ironwood_commitment_tree_size: 0,
    };
    for height_num in 2..=total {
        let spends = spends_at(height_num);
        if spends == 0 {
            continue;
        }
        let height = BlockHeight::new(height_num);
        let block = fixture_block(&chain, height)?;
        let seed = height_num.to_le_bytes()[0];
        let mut txid_bytes = [seed; 32];
        txid_bytes[..4].copy_from_slice(&height_num.to_le_bytes());
        let txid = TransactionId::from_bytes(txid_bytes);
        let mut rows =
            FixtureTransactionRows::from_raw_transaction(txid, height, block.hash, 0, [seed]);
        rows.facts.public_facts.counts.sapling_spend_count = u32::try_from(spends)?;
        let compact = CompactBlockArtifact::new(
            BlockId::new(height, block.hash),
            block.parent_hash,
            block.block_time_seconds,
            vec![CompactTransaction {
                index: 0,
                transaction_id: txid,
                data: CompactTransactionData {
                    sapling_spends: vec![
                        CompactSaplingSpend {
                            nullifier: [seed; 32],
                        };
                        spends
                    ],
                    ..Default::default()
                },
            }],
            metadata,
        )?;
        chain = chain
            .with_transaction_rows(rows)
            .with_compact_block_artifact(compact);
    }
    Ok(chain)
}

fn mixed_spends_for_height(height: u32) -> usize {
    if height.is_multiple_of(125) {
        1_100
    } else if height.is_multiple_of(5) {
        8
    } else {
        0
    }
}

fn slot_cap_spends_for_height(_height: u32) -> usize {
    512
}

async fn publish_all_segments(
    store: &ReaderFixture,
    catalog_dir: &Path,
    segment_span: u32,
) -> Result<Arc<PublishedShieldedHistory>> {
    let root = catalog_dir
        .parent()
        .ok_or_else(|| eyre!("missing catalog parent"))?;
    let (signer, key) = crate::common::shielded_catalog_keys(root)?;
    let mut publisher = ShieldedCatalogPublisher::open(
        catalog_dir,
        &signer,
        BlockHeight::new(1),
        NonZeroU32::new(segment_span).ok_or_else(|| eyre!("zero segment span"))?,
    )?;
    let mut published_count = 0_usize;
    while publisher.publish_next(&store.reader)? {
        published_count += 1;
    }
    ensure!(published_count >= 2, "expected multi-segment catalog");
    Ok(Arc::new(
        PublishedShieldedHistory::open(
            &catalog_dir.join("current.json"),
            &key,
            false,
            store.reader.as_ref(),
        )
        .await?,
    ))
}

#[allow(
    deprecated,
    reason = "Compatibility covers the legacy nullifier RPC used by existing wallets."
)]
async fn assert_nullifier_parity(
    wallet: &mut Endpoint,
    stock: &mut Endpoint,
    (first, last): (u64, u64),
) -> Result<()> {
    let req = block_range_request(first, last);
    let expected = drain_slowly(
        stock
            .client
            .get_block_range_nullifiers(req.clone())
            .await?
            .into_inner(),
    )
    .await;
    let actual = drain_slowly(
        wallet
            .client
            .get_block_range_nullifiers(req)
            .await?
            .into_inner(),
    )
    .await;
    ensure!(expected.ending.is_none() && actual.ending.is_none());
    assert_eq!(actual.blocks.len(), usize::try_from(TOTAL_MIXED_BLOCKS)?);
    assert_eq!(actual.blocks, expected.blocks);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tonic_client_decodes_2000_plus_mixed_blocks_identically_across_segments_and_tail()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    let chain = build_chain_with_spends(TOTAL_MIXED_BLOCKS, mixed_spends_for_height)?;
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(SETTLED_MIXED_BLOCKS)),
        &sample_regtest_upgrade_activations(),
    )?;
    let store = ReaderFixture::new(writer.into_serving_store_fixture()?)?;
    let catalog = publish_all_segments(&store, &dir.path().join("catalog"), SEGMENT_SPAN).await?;

    let mut wallet = Endpoint::start(
        Server::builder().add_service(
            LightwalletdGrpcAdapter::new(store.query.clone())
                .with_published_shielded_history(catalog.clone())
                .into_server(),
        ),
    )
    .await?;
    let mut blocks_only = Endpoint::start(
        Server::builder().add_service(
            CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(store.reader.clone()))
                .with_published_shielded_history(catalog)
                .into_server(),
        ),
    )
    .await?;
    let mut stock = Endpoint::start(
        Server::builder()
            .add_service(LightwalletdGrpcAdapter::new(store.query.clone()).into_server()),
    )
    .await?;

    let end = u64::from(TOTAL_MIXED_BLOCKS);
    for (first, last) in [(1, end), (end, 1)] {
        let req = block_range_request(first, last);
        let expected = stock.blocks(req.clone()).await?;
        assert_eq!(expected.len(), usize::try_from(TOTAL_MIXED_BLOCKS)?);
        assert_eq!(wallet.blocks(req.clone()).await?, expected);
        assert_eq!(blocks_only.blocks(req).await?, expected);
        assert_nullifier_parity(&mut wallet, &mut stock, (first, last)).await?;
    }
    Ok(())
}

async fn connect_wide_clients(
    addr: std::net::SocketAddr,
    count: usize,
) -> Result<Vec<CompactTxStreamerClient<Channel>>> {
    let mut clients = Vec::with_capacity(count);
    for _ in 0..count {
        let channel = TonicEndpoint::from_shared(format!("http://{addr}"))?
            .initial_stream_window_size(1024 * 1024)
            .initial_connection_window_size(16 * 1024 * 1024)
            .connect()
            .await?;
        clients.push(CompactTxStreamerClient::new(channel));
    }
    Ok(clients)
}

async fn drain_concurrent_ranges(
    clients: &[CompactTxStreamerClient<Channel>],
    concurrent: usize,
    end: u64,
) -> Result<()> {
    let mut tasks = tokio::task::JoinSet::new();
    for idx in 0..concurrent {
        let mut conn = clients[idx % clients.len()].clone();
        tasks.spawn(async move {
            let mut stream = conn
                .get_block_range(block_range_request(1, end))
                .await?
                .into_inner();
            let mut seen = 0_u64;
            while stream.message().await?.is_some() {
                seen += 1;
            }
            ensure!(seen == end, "expected {end} blocks, got {seen}");
            Ok::<_, eyre::Report>(())
        });
    }
    let mut first_error: Option<eyre::Report> = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Err(err)) if first_error.is_none() => first_error = Some(err),
            Err(err) if first_error.is_none() => first_error = Some(err.into()),
            Ok(Ok(()) | Err(_)) | Err(_) => {}
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_and_dropped_concurrent_streams_respect_slot_cap_and_release_cleanly() -> Result<()>
{
    const SLOT_CAP_BLOCKS: u32 = 80;
    let dir = tempfile::tempdir()?;
    let chain = build_chain_with_spends(SLOT_CAP_BLOCKS, slot_cap_spends_for_height)?;
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(SLOT_CAP_BLOCKS)),
        &sample_regtest_upgrade_activations(),
    )?;
    let store = ReaderFixture::new(writer.into_serving_store_fixture()?)?;
    let catalog = publish_all_segments(&store, &dir.path().join("catalog"), 40).await?;
    let limit = NonZeroU32::new(100).ok_or_else(|| eyre!("zero limit"))?;
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let server_task = tokio::spawn(
        Server::builder()
            .add_service(
                LightwalletdGrpcAdapter::new(store.query)
                    .with_published_shielded_history(catalog)
                    .with_max_concurrent_block_range_streams(limit)
                    .with_block_range_admission_wait(Duration::ZERO)
                    .into_server(),
            )
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let clients = connect_wide_clients(addr, 10).await?;

    let end = u64::from(SLOT_CAP_BLOCKS);
    let mut paused = Vec::with_capacity(100);
    for idx in 0..100 {
        let mut conn = clients[idx % clients.len()].clone();
        let mut stream = conn
            .get_block_range(block_range_request(1, end))
            .await?
            .into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| eyre!("empty paused stream"))?;
        assert_eq!(first.height, 1);
        paused.push(stream);
    }

    let mut probe = clients[0].clone();
    let overflow = probe
        .get_block_range(block_range_request(1, 5))
        .await
        .err()
        .map(|status| status.code());
    assert_eq!(overflow, Some(Code::Unavailable));

    paused.truncate(50);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if probe
                .get_block_range(block_range_request(1, 5))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;

    drop(paused);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if drain_concurrent_ranges(&clients, 100, end).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    server_task.abort();
    Ok(())
}
