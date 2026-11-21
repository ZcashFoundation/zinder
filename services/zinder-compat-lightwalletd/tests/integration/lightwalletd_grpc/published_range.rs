use super::{
    EpochPinRecorder, assert_one_linked_chain, assert_stream_ended_with, block_range_request,
    drain_slowly, fixture_block,
};
use eyre::eyre;
use std::{path::Path, sync::Arc};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code,
    transport::{Channel, Server, server::Router},
};
use zinder_compat_lightwalletd::{
    CanonicalReaderSlot, CompactBlocksGrpcAdapter, LightwalletdGrpcAdapter,
    PublishedShieldedHistory, ShieldedCatalogPublisher,
};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, CompactBlockArtifact, CompactChainMetadata,
    CompactSaplingSpend, CompactTransaction, CompactTransactionData, Network, TransactionId,
    wire::encode_internal_block_hash,
};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::{
    RocksDbCanonicalReader, RocksDbWalletReader, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_store::RawBlobRetention;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
    WalletServingStoreFixture, sample_regtest_upgrade_activations,
};

const NETWORK: Network = Network::ZcashRegtest;
const SEALED_END: u32 = 3;

struct ReaderFixture {
    reader: Arc<RocksDbCanonicalReader>,
    query: WalletServingQuery<()>,
    _paths: WalletServingStoreFixture,
}

impl ReaderFixture {
    fn new(mut paths: WalletServingStoreFixture) -> eyre::Result<Self> {
        let (mut canonical, wallet) = paths.take_readers()?;
        canonical.try_catch_up()?;
        let reader = Arc::new(RocksDbCanonicalReader::new(canonical)?);
        let pair =
            WalletServingReadPair::new(reader.clone(), Arc::new(RocksDbWalletReader::new(wallet)))?;
        let query =
            WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(pair)));
        Ok(Self {
            reader,
            query,
            _paths: paths,
        })
    }

    async fn publish(&self, path: &Path) -> eyre::Result<Arc<PublishedShieldedHistory>> {
        let (signer, key) = crate::common::shielded_catalog_keys(
            path.parent().ok_or_else(|| eyre!("missing fixture root"))?,
        )?;
        let mut publisher = ShieldedCatalogPublisher::open(
            path,
            &signer,
            BlockHeight::new(1),
            SEALED_END.try_into()?,
        )?;
        assert!(publisher.publish_next(&self.reader)?);
        Ok(Arc::new(
            PublishedShieldedHistory::open(
                &path.join("current.json"),
                &key,
                false,
                self.reader.as_ref(),
            )
            .await?,
        ))
    }
}

struct Endpoint {
    client: CompactTxStreamerClient<Channel>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Endpoint {
    async fn start(router: Router) -> eyre::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let task = tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));
        let client = CompactTxStreamerClient::connect(format!("http://{addr}")).await?;
        Ok(Self { client, task })
    }

    async fn blocks(
        &mut self,
        request: lightwalletd::BlockRange,
    ) -> eyre::Result<Vec<lightwalletd::CompactBlock>> {
        let stream = self.client.get_block_range(request).await?.into_inner();
        let drained = drain_slowly(stream).await;
        if let Some(status) = drained.ending {
            return Err(status.into());
        }
        Ok(drained.blocks)
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn writer(chain: &ChainFixture) -> eyre::Result<CanonicalWriterFixture> {
    CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(chain).with_settled_tip(BlockHeight::new(SEALED_END)),
        &sample_regtest_upgrade_activations(),
    )
}

fn wallet_chain(spend_count: usize) -> eyre::Result<ChainFixture> {
    let chain = ChainFixture::new(NETWORK)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(6);
    let block = fixture_block(&chain, BlockHeight::new(2))?;
    let txid = TransactionId::from_bytes([17; 32]);
    let mut rows = FixtureTransactionRows::from_raw_transaction(
        txid,
        block.height,
        block.hash,
        0,
        b"published range wallet lookup",
    );
    rows.facts.public_facts.counts.sapling_spend_count = u32::try_from(spend_count)?;
    let compact = CompactBlockArtifact::new(
        BlockId::new(block.height, block.hash),
        block.parent_hash,
        block.block_time_seconds,
        vec![CompactTransaction {
            index: 0,
            transaction_id: txid,
            data: CompactTransactionData {
                sapling_spends: vec![
                    CompactSaplingSpend {
                        nullifier: [11; 32],
                    };
                    spend_count
                ],
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
async fn generated_wallet_clients_cross_the_published_boundary_in_both_directions()
-> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let chain = wallet_chain(1)?;
    let store = ReaderFixture::new(writer(&chain)?.into_serving_store_fixture()?)?;
    let segment = store.publish(&dir.path().join("shielded")).await?;
    let recorder = EpochPinRecorder::new(store.query.clone());
    let mut wallet = Endpoint::start(
        Server::builder().add_service(
            LightwalletdGrpcAdapter::new(recorder.clone())
                .with_published_shielded_history(segment.clone())
                .into_server(),
        ),
    )
    .await?;
    let mut blocks_only = Endpoint::start(
        Server::builder().add_service(
            CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(store.reader.clone()))
                .with_published_shielded_history(segment)
                .into_server(),
        ),
    )
    .await?;
    let mut stock = Endpoint::start(
        Server::builder()
            .add_service(LightwalletdGrpcAdapter::new(store.query.clone()).into_server()),
    )
    .await?;
    for (first, last) in [(1, 6), (6, 1)] {
        let req = block_range_request(first, last);
        let expected = stock.blocks(req.clone()).await?;
        assert_one_linked_chain(&expected, first, last);
        assert_eq!(wallet.blocks(req.clone()).await?, expected);
        assert_eq!(blocks_only.blocks(req).await?, expected);
    }
    let suffix = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(6));
    assert_eq!(recorder.recorded_compact_block_ranges(), vec![suffix; 2]);

    check_other_wallet_calls(&mut wallet, &mut stock, &mut blocks_only).await
}

#[allow(
    deprecated,
    reason = "Compatibility covers the legacy nullifier RPC used by existing wallets."
)]
async fn check_other_wallet_calls(
    wallet: &mut Endpoint,
    stock: &mut Endpoint,
    blocks_only: &mut Endpoint,
) -> eyre::Result<()> {
    let mut explicit = block_range_request(2, 2);
    explicit.pool_types = vec![lightwalletd::PoolType::Transparent as i32];
    assert_ne!(
        stock.blocks(explicit.clone()).await?,
        stock.blocks(block_range_request(2, 2)).await?
    );
    assert_eq!(
        wallet.blocks(explicit.clone()).await?,
        stock.blocks(explicit).await?
    );
    let request = block_range_request(1, 6);
    let expected = drain_slowly(
        stock
            .client
            .get_block_range_nullifiers(request.clone())
            .await?
            .into_inner(),
    )
    .await;
    let actual = drain_slowly(
        wallet
            .client
            .get_block_range_nullifiers(request)
            .await?
            .into_inner(),
    )
    .await;
    assert!(actual.ending.is_none() && expected.ending.is_none());
    assert_eq!(actual.blocks, expected.blocks);
    let tx = lightwalletd::TxFilter {
        hash: vec![17; 32],
        ..Default::default()
    };
    assert_eq!(
        wallet
            .client
            .get_transaction(tx.clone())
            .await?
            .into_inner()
            .data,
        b"published range wallet lookup"
    );
    assert_eq!(
        blocks_only
            .client
            .get_transaction(tx)
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::Unimplemented)
    );
    Ok(())
}

#[tokio::test]
async fn a_published_range_on_a_different_branch_emits_no_displaced_block() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let chain = ChainFixture::new(NETWORK).extend_blocks(6);
    let branch = chain.fork_at(BlockHeight::new(2))?.extend_blocks(5);
    let old = ReaderFixture::new(writer(&chain)?.into_serving_store_fixture()?)?;
    let current = ReaderFixture::new(writer(&branch)?.into_serving_store_fixture()?)?;
    let segment = old.publish(&dir.path().join("displaced")).await?;
    let mut wallet = Endpoint::start(
        Server::builder().add_service(
            LightwalletdGrpcAdapter::new(current.query.clone())
                .with_published_shielded_history(segment)
                .into_server(),
        ),
    )
    .await?;
    for first in [0, 1] {
        match wallet
            .client
            .get_block_range(block_range_request(first, 6))
            .await
        {
            Err(status) => assert_eq!(status.code(), Code::FailedPrecondition),
            Ok(response) => {
                let drained = drain_slowly(response.into_inner()).await;
                assert_eq!(drained.blocks, [] as [lightwalletd::CompactBlock; 0]);
                assert_stream_ended_with(drained.ending.as_ref(), Code::FailedPrecondition);
            }
        }
    }
    assert_eq!(wallet.blocks(block_range_request(6, 6)).await?.len(), 1);
    Ok(())
}

async fn crossing_a_replaced_live_suffix(
    replacement_tip: u32,
    expected_status: Code,
) -> eyre::Result<()> {
    const TIP: u32 = 1_015;
    const FORK: u32 = 998;
    let dir = tempfile::tempdir()?;
    let mut chain = ChainFixture::new(NETWORK)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(TIP);
    for n in FORK..=TIP {
        let block = fixture_block(&chain, BlockHeight::new(n))?;
        let mut hash = [17; 32];
        hash[..4].copy_from_slice(&n.to_be_bytes());
        chain = chain.with_transaction_rows(FixtureTransactionRows::coinbase(
            TransactionId::from_bytes(hash),
            block.height,
            block.hash,
            format!("coinbase-{n}").into_bytes(),
        ));
    }
    let replacement = chain
        .fork_at(BlockHeight::new(FORK))?
        .extend_blocks(replacement_tip - FORK + 1);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(FORK - 1)),
        &sample_regtest_upgrade_activations(),
    )?;
    let before = ReaderFixture::new(writer.serving_store_fixture()?)?;
    let published = before.publish(&dir.path().join("settled")).await?;
    writer.replace_live_blocks(&replacement, BlockHeight::new(FORK))?;
    let after = ReaderFixture::new(writer.into_serving_store_fixture()?)?;
    let recorder =
        EpochPinRecorder::new(before.query.clone()).with_later_views(vec![after.query.clone()]);
    let mut endpoint = Endpoint::start(
        Server::builder().add_service(
            LightwalletdGrpcAdapter::new(recorder)
                .with_published_shielded_history(published)
                .into_server(),
        ),
    )
    .await?;
    let stream = endpoint
        .client
        .get_block_range(block_range_request(1, u64::from(TIP)))
        .await?
        .into_inner();
    let drained = drain_slowly(stream).await;
    assert_stream_ended_with(drained.ending.as_ref(), expected_status);
    let last = drained
        .blocks
        .last()
        .ok_or_else(|| eyre!("missing settled and original live prefix"))?
        .height;
    assert!(last >= u64::from(FORK) && last < u64::from(TIP));
    assert_one_linked_chain(&drained.blocks, 1, last);
    for returned in drained.blocks {
        let original = fixture_block(&chain, BlockHeight::new(u32::try_from(returned.height)?))?;
        assert_eq!(returned.hash, encode_internal_block_hash(original.hash));
    }
    Ok(())
}

#[tokio::test]
async fn crossing_into_a_replaced_live_chunk_aborts_before_a_forked_block_is_sent()
-> eyre::Result<()> {
    crossing_a_replaced_live_suffix(1_015, Code::Aborted).await
}

#[tokio::test]
async fn crossing_into_a_shorter_live_view_ends_out_of_range_before_the_missing_chunk()
-> eyre::Result<()> {
    crossing_a_replaced_live_suffix(1_005, Code::OutOfRange).await
}

#[tokio::test]
async fn cancelling_at_the_live_handoff_returns_the_only_stream_slot() -> eyre::Result<()> {
    use super::{FirstChunkGate, REFUSAL_WAIT};
    use std::{num::NonZeroU32, time::Duration};
    let dir = tempfile::tempdir()?;
    let chain = wallet_chain(1)?;
    let fixture = ReaderFixture::new(writer(&chain)?.into_serving_store_fixture()?)?;
    let published = fixture.publish(&dir.path().join("settled")).await?;
    let gate = FirstChunkGate::closed();
    let recorder = EpochPinRecorder::new(fixture.query.clone()).with_first_chunk_gate(gate.clone());
    let adapter = LightwalletdGrpcAdapter::new(recorder)
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN)
        .with_block_range_admission_wait(REFUSAL_WAIT)
        .with_published_shielded_history(published);
    let mut endpoint =
        Endpoint::start(Server::builder().add_service(adapter.into_server())).await?;
    let mut cancelling = endpoint.client.clone();
    let pending =
        tokio::spawn(async move { cancelling.get_block_range(block_range_request(1, 6)).await });
    tokio::time::timeout(Duration::from_secs(5), gate.reached.notified()).await?;
    pending.abort();
    assert!(pending.await.is_err_and(|error| error.is_cancelled()));
    let busy = endpoint
        .client
        .get_block_range(block_range_request(1, 1))
        .await;
    assert_eq!(
        busy.err().map(|status| status.code()),
        Some(Code::Unavailable)
    );
    gate.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match endpoint
                .client
                .get_block_range(block_range_request(1, 1))
                .await
            {
                Ok(response) => {
                    let drained = drain_slowly(response.into_inner()).await;
                    assert!(drained.ending.is_none());
                    assert_one_linked_chain(&drained.blocks, 1, 1);
                    return Ok::<_, tonic::Status>(());
                }
                Err(status) if status.code() == Code::Unavailable => tokio::task::yield_now().await,
                Err(status) => return Err(status),
            }
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn appending_above_a_published_range_keeps_the_original_request_linked() -> eyre::Result<()> {
    const REQUEST_END: u32 = 1005;
    let dir = tempfile::tempdir()?;
    let chain = ChainFixture::new(NETWORK).extend_blocks(REQUEST_END + 1);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain)
            .through(BlockHeight::new(REQUEST_END))
            .with_settled_tip(BlockHeight::new(REQUEST_END - 5)),
        &sample_regtest_upgrade_activations(),
    )?;
    let before = ReaderFixture::new(writer.serving_store_fixture()?)?;
    let published = before.publish(&dir.path().join("settled")).await?;
    writer.append_live_block(&chain)?;
    let after = ReaderFixture::new(writer.into_serving_store_fixture()?)?;
    let recorder =
        EpochPinRecorder::new(before.query.clone()).with_later_views(vec![after.query.clone()]);
    let mut endpoint = Endpoint::start(
        Server::builder().add_service(
            LightwalletdGrpcAdapter::new(recorder.clone())
                .with_published_shielded_history(published)
                .into_server(),
        ),
    )
    .await?;
    let received = endpoint
        .blocks(block_range_request(1, u64::from(REQUEST_END)))
        .await?;
    assert_one_linked_chain(&received, 1, u64::from(REQUEST_END));
    let epochs = recorder.served_compact_block_epochs();
    assert!(epochs.len() > 1 && epochs.first() != epochs.last());
    Ok(())
}

#[tokio::test]
async fn a_malformed_live_row_ends_after_the_same_delivered_prefix_as_the_stock_reader()
-> eyre::Result<()> {
    use super::{DrainedBlockRange, damage_compact_block_row};
    use zinder_compat_lightwalletd::BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES;
    async fn received(endpoint: &mut Endpoint, first: u64, last: u64) -> DrainedBlockRange {
        match endpoint
            .client
            .get_block_range(block_range_request(first, last))
            .await
        {
            Err(status) => DrainedBlockRange {
                blocks: Vec::new(),
                ending: Some(status),
            },
            Ok(response) => drain_slowly(response.into_inner()).await,
        }
    }
    for (damaged_length, expected_count) in [(1, 0), (BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES + 1, 3)] {
        let files = tempfile::tempdir()?;
        let chain = wallet_chain(1)?;
        let writer = writer(&chain)?;
        let primary = writer.canonical_primary_path().to_path_buf();
        let paths = writer.into_serving_store_fixture()?;
        damage_compact_block_row(
            &primary,
            BlockHeight::new(4),
            Some(&vec![0; damaged_length]),
        )?;
        let fixture = ReaderFixture::new(paths)?;
        let published = fixture.publish(&files.path().join("settled")).await?;
        let mut stock = Endpoint::start(
            Server::builder()
                .add_service(LightwalletdGrpcAdapter::new(fixture.query.clone()).into_server()),
        )
        .await?;
        let mut qualified = Endpoint::start(
            Server::builder().add_service(
                LightwalletdGrpcAdapter::new(fixture.query.clone())
                    .with_published_shielded_history(published)
                    .into_server(),
            ),
        )
        .await?;
        for (start, end, prefix) in [
            (1, 6, expected_count),
            (6, 1, if expected_count == 0 { 0 } else { 2 }),
        ] {
            let expected = received(&mut stock, start, end).await;
            assert_eq!(expected.blocks.len(), prefix);
            let status = expected
                .ending
                .ok_or_else(|| eyre!("stock accepted invalid compact bytes"))?;
            let actual = received(&mut qualified, start, end).await;
            assert_eq!(actual.blocks, expected.blocks);
            assert_stream_ended_with(actual.ending.as_ref(), status.code());
        }
    }
    Ok(())
}

#[tokio::test]
async fn wallets_read_across_multiple_signed_segments_and_the_live_suffix() -> eyre::Result<()> {
    use std::num::NonZeroU32;
    let chain = wallet_chain(8_000)?;
    let store = ReaderFixture::new(writer(&chain)?.into_serving_store_fixture()?)?;
    let files = tempfile::tempdir()?;
    let (signer, key) = crate::common::shielded_catalog_keys(files.path())?;
    let publication = files.path().join("publication");
    let mut publisher = ShieldedCatalogPublisher::open(
        &publication,
        &signer,
        BlockHeight::new(1),
        NonZeroU32::MIN,
    )?;
    let manifest = publication.join("current.json");
    assert!(publisher.publish_next(&store.reader)?);
    let history = Arc::new(
        PublishedShieldedHistory::open(&manifest, &key, false, store.reader.as_ref()).await?,
    );
    let recorder = EpochPinRecorder::new(store.query.clone());
    let mut wallet = Endpoint::start(
        Server::builder().add_service(
            LightwalletdGrpcAdapter::new(recorder.clone())
                .with_published_shielded_history(history.clone())
                .into_server(),
        ),
    )
    .await?;
    let mut stock = Endpoint::start(
        Server::builder()
            .add_service(LightwalletdGrpcAdapter::new(store.query.clone()).into_server()),
    )
    .await?;

    assert!(publisher.publish_next(&store.reader)?);
    assert!(publisher.publish_next(&store.reader)?);
    assert!(!publisher.publish_next(&store.reader)?);
    assert!(history.refresh(store.reader.as_ref()).await?);
    assert!(!history.refresh(store.reader.as_ref()).await?);
    for (start, end) in [(1, 3), (3, 1), (1, 6), (6, 1)] {
        let request = block_range_request(start, end);
        let expected = stock.blocks(request.clone()).await?;
        assert_one_linked_chain(&expected, start, end);
        assert_eq!(wallet.blocks(request).await?, expected);
    }
    let reads = recorder.offered_compact_block_ranges();
    assert!(reads.iter().any(|range| range.end.value() == 6));
    assert!(reads.iter().all(|range| range.end.value() > SEALED_END));
    Ok(())
}

#[tokio::test]
async fn damaged_admitted_segment_ends_covered_rpc_with_data_loss_in_both_orders()
-> eyre::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let files = tempfile::tempdir()?;
    let fixture = ReaderFixture::new(writer(&wallet_chain(1)?)?.into_serving_store_fixture()?)?;
    let directory = files.path().join("catalog");
    let history = fixture.publish(&directory).await?;
    let segment = std::fs::read_dir(&directory)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|ext| ext == "bin"))
        .ok_or_else(|| eyre!("missing segment"))?;
    let mut file = std::fs::OpenOptions::new().write(true).open(segment)?;
    file.seek(SeekFrom::End(-1))?;
    file.write_all(&[0xff])?;
    let mut wallet = Endpoint::start(
        Server::builder().add_service(
            LightwalletdGrpcAdapter::new(fixture.query)
                .with_published_shielded_history(history)
                .into_server(),
        ),
    )
    .await?;
    for (first, last) in [(1, 3), (3, 1)] {
        let response = wallet
            .client
            .get_block_range(block_range_request(first, last))
            .await;
        match response {
            Err(status) => assert_eq!(status.code(), Code::DataLoss),
            Ok(response) => {
                let drained = drain_slowly(response.into_inner()).await;
                assert_eq!(drained.blocks, [] as [lightwalletd::CompactBlock; 0]);
                assert_stream_ended_with(drained.ending.as_ref(), Code::DataLoss);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn catalog_keeps_the_original_native_budget_when_protobuf_has_unknown_fields()
-> eyre::Result<()> {
    use zinder_compat_lightwalletd::BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES;

    let files = tempfile::tempdir()?;
    let chain = ChainFixture::new(NETWORK).extend_blocks(3);
    let writer = writer(&chain)?;
    let primary = writer.canonical_primary_path().to_owned();
    let paths = writer.into_serving_store_fixture()?;
    let middle = BlockHeight::new(2);
    let mut stored = zinder_store::encode_compact_block_artifact(
        &fixture_block(&chain, middle)?.compact_block_artifact(),
    );
    // Unknown varint field 127 is skipped by the typed protobuf decoder.
    while stored.len() <= BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES {
        stored.extend_from_slice(&[0xf8, 0x07, 1]);
    }
    super::damage_compact_block_row(&primary, middle, Some(&stored))?;
    let fixture = ReaderFixture::new(paths)?;
    let (signer, key) = crate::common::shielded_catalog_keys(files.path())?;
    let directory = files.path().join("history");
    let mut publisher = ShieldedCatalogPublisher::open(
        &directory,
        &signer,
        BlockHeight::new(1),
        std::num::NonZeroU32::MIN,
    )?;
    while publisher.publish_next(&fixture.reader)? {}
    let history = Arc::new(
        PublishedShieldedHistory::open(
            &directory.join("current.json"),
            &key,
            false,
            fixture.reader.as_ref(),
        )
        .await?,
    );
    let mut wallet = Endpoint::start(
        Server::builder().add_service(
            LightwalletdGrpcAdapter::new(fixture.query)
                .with_published_shielded_history(history)
                .into_server(),
        ),
    )
    .await?;
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "bin") {
            let mut segment = std::fs::read(&path)?;
            if segment[12..16] == middle.value().to_be_bytes() {
                let byte = segment
                    .last_mut()
                    .ok_or_else(|| eyre!("missing middle block"))?;
                *byte ^= 1;
                std::fs::write(path, &segment)?;
            }
        }
    }
    for (start, end) in [(1, 3), (3, 1)] {
        let stream = wallet
            .client
            .get_block_range(block_range_request(start, end))
            .await?
            .into_inner();
        let drained = drain_slowly(stream).await;
        assert_eq!(
            drained
                .blocks
                .iter()
                .map(|block| block.height)
                .collect::<Vec<_>>(),
            [start]
        );
        assert_stream_ended_with(drained.ending.as_ref(), Code::DataLoss);
    }
    Ok(())
}
