mod admission;
mod high_concurrency;
mod tail_identity;
mod wire_splice_parity;

use super::{
    DrainedBlockRange, EpochPinRecorder, assert_one_linked_chain, assert_stream_ended_with,
    block_range_request, damage_compact_block_row, drain_slowly, fixture_block,
};
use eyre::eyre;
use http_body_util::BodyExt as _;
use prost::Message;
use std::{num::NonZeroU32, path::Path, sync::Arc, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code,
    body::Body,
    codegen::{Bytes, Service as _, http},
    transport::{Channel, Server, server::Router},
};
use zinder_compat_lightwalletd::{
    BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES, CanonicalReaderSlot, CompactBlocksGrpcAdapter,
    LightwalletdGrpcAdapter,
};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, CompactBlockArtifact, CompactChainMetadata,
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransaction,
    CompactTransactionData, CompactTransparentInput, CompactTransparentOutput, Network,
    TransactionId, wire::encode_internal_block_hash,
};
use zinder_proto::compat::lightwalletd::{
    self, PoolType, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::{
    RocksDbCanonicalReader, RocksDbWalletReader, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_store::RawBlobRetention;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureBlock, FixtureTransactionRows,
    WalletServingStoreFixture, sample_regtest_upgrade_activations,
};

const NETWORK: Network = Network::ZcashRegtest;
const SETTLED_END: u32 = 3;
const SETTLED_CHUNK_END: u32 = 1_000;
const SUBSET_SETTLED_END: u32 = 5;
const SUBSET_IRONWOOD_ACTIVATION: u32 = 4;

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
        &CanonicalBaseline::new(chain).with_settled_tip(BlockHeight::new(SETTLED_END)),
        &sample_regtest_upgrade_activations(),
    )
}

fn handoff_writer(chain: &ChainFixture) -> eyre::Result<CanonicalWriterFixture> {
    CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(chain).with_settled_tip(BlockHeight::new(SETTLED_CHUNK_END)),
        &sample_regtest_upgrade_activations(),
    )
}

fn direct_recorder(query: WalletServingQuery<()>) -> EpochPinRecorder<WalletServingQuery<()>> {
    let mut recorder = EpochPinRecorder::new(query);
    recorder.expose_encoded_range = true;
    recorder
}

fn wallet_chain(spend_count: usize) -> eyre::Result<ChainFixture> {
    let chain = ChainFixture::new(NETWORK)
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(6);
    let block = fixture_block(&chain, BlockHeight::new(2))?;
    let txid = TransactionId::from_bytes([17; 32]);
    let mut rows = FixtureTransactionRows::from_raw_transaction(
        txid,
        block.height,
        block.hash,
        0,
        b"canonical range wallet lookup",
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
async fn generated_wallet_clients_cross_the_settled_boundary_in_both_directions() -> eyre::Result<()>
{
    let chain = wallet_chain(1)?;
    let store = ReaderFixture::new(writer(&chain)?.into_serving_store_fixture()?)?;
    let recorder = direct_recorder(store.query.clone());
    let mut wallet = Endpoint::start(
        Server::builder().add_service(LightwalletdGrpcAdapter::new(recorder.clone()).into_server()),
    )
    .await?;
    let mut blocks_only = Endpoint::start(Server::builder().add_service(
        CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(store.reader.clone())).into_server(),
    ))
    .await?;
    let mut stock = Endpoint::start(Server::builder().add_service(
        LightwalletdGrpcAdapter::new(EpochPinRecorder::new(store.query)).into_server(),
    ))
    .await?;
    for (first, last) in [(1, 6), (6, 1)] {
        let req = block_range_request(first, last);
        let expected = stock.blocks(req.clone()).await?;
        assert_one_linked_chain(&expected, first, last);
        assert_eq!(wallet.blocks(req.clone()).await?, expected);
        assert_eq!(blocks_only.blocks(req).await?, expected);
    }
    assert_eq!(
        recorder.recorded_compact_block_ranges(),
        [] as [BlockHeightRange; 0]
    );

    check_other_wallet_calls(&mut wallet, &mut stock, &mut blocks_only, &recorder).await
}

#[allow(
    deprecated,
    reason = "Compatibility covers the legacy nullifier RPC used by existing wallets."
)]
async fn check_other_wallet_calls(
    wallet: &mut Endpoint,
    stock: &mut Endpoint,
    blocks_only: &mut Endpoint,
    recorder: &EpochPinRecorder<WalletServingQuery<()>>,
) -> eyre::Result<()> {
    let mut explicit = block_range_request(2, 2);
    explicit.pool_types = vec![PoolType::Transparent as i32];
    assert_ne!(
        stock.blocks(explicit.clone()).await?,
        stock.blocks(block_range_request(2, 2)).await?
    );
    assert_eq!(
        wallet.blocks(explicit.clone()).await?,
        stock.blocks(explicit).await?
    );
    let fallback_before_nullifiers = recorder.recorded_compact_block_ranges().len();
    for (first, last) in [(1, 6), (6, 1)] {
        let request = block_range_request(first, last);
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
    }
    assert_eq!(
        recorder.recorded_compact_block_ranges().len(),
        fallback_before_nullifiers
    );
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
        b"canonical range wallet lookup"
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
async fn descending_gap_retry_delivers_every_previously_checked_retained_block() -> eyre::Result<()>
{
    let chain = ChainFixture::new(NETWORK).extend_blocks(6_100);
    let baseline = CanonicalBaseline::new(&chain);
    let activations = sample_regtest_upgrade_activations();
    let before = ReaderFixture::new(
        CanonicalWriterFixture::publish(&baseline, &activations)?.into_serving_store_fixture()?,
    )?;
    let after = ReaderFixture::new(
        CanonicalWriterFixture::publish(
            &baseline.with_history_floor(BlockHeight::new(10)),
            &activations,
        )?
        .into_serving_store_fixture()?,
    )?;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let recorder = direct_recorder(before.query.clone())
        .with_later_views(vec![after.query])
        .with_later_chunk_gate(gate.clone());
    let mut endpoint = Endpoint::start(
        Server::builder().add_service(LightwalletdGrpcAdapter::new(recorder.clone()).into_server()),
    )
    .await?;
    let stream = endpoint
        .client
        .get_block_range(block_range_request(1_010, 1))
        .await?
        .into_inner();
    super::later_chunk_read_at_gate(&recorder).await?;
    recorder.advance_view();
    gate.add_permits(1);
    let received = drain_slowly(stream).await;
    assert_stream_ended_with(received.ending.as_ref(), Code::NotFound);
    assert_one_linked_chain(&received.blocks, 1_010, 11);
    Ok(())
}

async fn crossing_a_replaced_live_suffix(
    replacement_tip: u32,
    expected_status: Code,
) -> eyre::Result<()> {
    const TIP: u32 = 2_015;
    const FORK: u32 = 1_998;
    let mut chain = ChainFixture::new(NETWORK)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(TIP);
    for height_num in FORK..=TIP {
        let block = fixture_block(&chain, BlockHeight::new(height_num))?;
        let mut hash = [17; 32];
        hash[..4].copy_from_slice(&height_num.to_be_bytes());
        chain = chain.with_transaction_rows(FixtureTransactionRows::coinbase(
            TransactionId::from_bytes(hash),
            block.height,
            block.hash,
            format!("coinbase-{height_num}").into_bytes(),
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
    writer.replace_live_blocks(&replacement, BlockHeight::new(FORK))?;
    let after = ReaderFixture::new(writer.into_serving_store_fixture()?)?;
    // Two chunks (1..=1_000 and 1_001..=2_000) read `before`; chunk 3 hits `after`.
    let recorder =
        direct_recorder(before.query.clone()).with_later_views(vec![before.query, after.query]);
    let mut endpoint = Endpoint::start(
        Server::builder().add_service(LightwalletdGrpcAdapter::new(recorder.clone()).into_server()),
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
        .ok_or_else(|| eyre!("missing original prefix before fork"))?
        .height;
    assert_eq!(last, 2_000);
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
    crossing_a_replaced_live_suffix(2_015, Code::Aborted).await
}

#[tokio::test]
async fn crossing_into_a_shorter_live_view_ends_out_of_range_before_the_missing_chunk()
-> eyre::Result<()> {
    crossing_a_replaced_live_suffix(2_005, Code::OutOfRange).await
}

async fn stream_settled_prefix(
    endpoint: &mut Endpoint,
    recorder: &EpochPinRecorder<WalletServingQuery<()>>,
    tip: u32,
) -> eyre::Result<(
    tonic::Streaming<lightwalletd::CompactBlock>,
    Vec<lightwalletd::CompactBlock>,
)> {
    let mut stream = endpoint
        .client
        .get_block_range(block_range_request(1, u64::from(tip)))
        .await?
        .into_inner();
    let mut prefix = Vec::new();
    for _ in 0..SETTLED_CHUNK_END {
        prefix.push(
            stream
                .message()
                .await?
                .ok_or_else(|| eyre!("range ended inside its settled prefix"))?,
        );
    }
    assert_one_linked_chain(&prefix, 1, u64::from(SETTLED_CHUNK_END));
    super::later_chunk_read_at_gate(recorder).await?;
    Ok((stream, prefix))
}

#[tokio::test]
async fn retiring_the_epoch_during_a_covered_later_chunk_reads_the_linked_current_range()
-> eyre::Result<()> {
    const TIP: u32 = 1_005;
    let chain = ChainFixture::new(NETWORK).extend_blocks(TIP + 1);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain)
            .through(BlockHeight::new(TIP))
            .with_settled_tip(BlockHeight::new(SETTLED_CHUNK_END)),
        &sample_regtest_upgrade_activations(),
    )?;
    let before = ReaderFixture::new(writer.serving_store_fixture()?)?;
    writer.append_live_block(&chain)?;
    let after = ReaderFixture::new(writer.into_serving_store_fixture()?)?;
    let recorder = direct_recorder(before.query).with_later_views(vec![after.query]);
    let mut endpoint = Endpoint::start(
        Server::builder().add_service(LightwalletdGrpcAdapter::new(recorder.clone()).into_server()),
    )
    .await?;
    let received = endpoint
        .blocks(block_range_request(1, u64::from(TIP)))
        .await?;
    assert_one_linked_chain(&received, 1, u64::from(TIP));
    assert_eq!(
        recorder.offered_compact_block_ranges(),
        [
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(SETTLED_CHUNK_END)),
            BlockHeightRange::inclusive(
                BlockHeight::new(SETTLED_CHUNK_END + 1),
                BlockHeight::new(TIP),
            ),
        ]
    );
    assert_eq!(recorder.recorded_epoch_ids().last(), Some(&None));
    Ok(())
}

#[tokio::test]
async fn a_replaced_tail_must_link_to_the_last_settled_block() -> eyre::Result<()> {
    const TIP: u32 = 1_015;
    let chain = ChainFixture::new(NETWORK).extend_blocks(TIP);
    let initial = ReaderFixture::new(handoff_writer(&chain)?.into_serving_store_fixture()?)?;

    for fork in [SETTLED_CHUNK_END, SETTLED_CHUNK_END + 1] {
        let branch = chain
            .fork_at(BlockHeight::new(fork))?
            .extend_blocks(TIP - fork + 1);
        let replacement =
            ReaderFixture::new(handoff_writer(&branch)?.into_serving_store_fixture()?)?;
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let recorder = direct_recorder(initial.query.clone())
            .with_later_views(vec![replacement.query])
            .with_later_chunk_gate(gate.clone());
        let mut endpoint = Endpoint::start(
            Server::builder()
                .add_service(LightwalletdGrpcAdapter::new(recorder.clone()).into_server()),
        )
        .await?;
        let (stream, mut prefix) = stream_settled_prefix(&mut endpoint, &recorder, TIP).await?;
        gate.add_permits(1);
        let tail = drain_slowly(stream).await;
        assert_eq!(
            recorder.offered_compact_block_ranges(),
            [
                BlockHeightRange::inclusive(
                    BlockHeight::new(1),
                    BlockHeight::new(SETTLED_CHUNK_END)
                ),
                BlockHeightRange::inclusive(
                    BlockHeight::new(SETTLED_CHUNK_END + 1),
                    BlockHeight::new(TIP)
                ),
            ]
        );
        if fork == SETTLED_CHUNK_END {
            assert_eq!(tail.blocks, [] as [lightwalletd::CompactBlock; 0]);
            assert_stream_ended_with(tail.ending.as_ref(), Code::Aborted);
        } else {
            assert!(tail.ending.is_none());
            for block in &tail.blocks {
                let expected =
                    fixture_block(&branch, BlockHeight::new(u32::try_from(block.height)?))?;
                assert_eq!(block.hash, encode_internal_block_hash(expected.hash));
            }
            prefix.extend(tail.blocks);
            assert_one_linked_chain(&prefix, 1, u64::from(TIP));
        }
    }
    Ok(())
}

#[tokio::test]
async fn cancelling_at_the_chunk_handoff_returns_the_only_stream_slot() -> eyre::Result<()> {
    use super::REFUSAL_WAIT;
    const TIP: u32 = 1_015;
    let chain = ChainFixture::new(NETWORK).extend_blocks(TIP);
    let fixture = ReaderFixture::new(handoff_writer(&chain)?.into_serving_store_fixture()?)?;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let recorder = direct_recorder(fixture.query).with_later_chunk_gate(gate.clone());
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone())
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN)
        .with_block_range_admission_wait(REFUSAL_WAIT);
    let mut endpoint =
        Endpoint::start(Server::builder().add_service(adapter.into_server())).await?;
    let (stream, _) = stream_settled_prefix(&mut endpoint, &recorder, TIP).await?;
    drop(stream);
    let busy = endpoint
        .client
        .get_block_range(block_range_request(1, 1))
        .await;
    assert_eq!(
        busy.err().map(|status| status.code()),
        Some(Code::Unavailable)
    );
    gate.add_permits(1);
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
async fn appending_above_a_settled_range_keeps_the_original_request_linked() -> eyre::Result<()> {
    const REQUEST_END: u32 = 1_005;
    let chain = ChainFixture::new(NETWORK).extend_blocks(REQUEST_END + 1);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain)
            .through(BlockHeight::new(REQUEST_END))
            .with_settled_tip(BlockHeight::new(REQUEST_END - 5)),
        &sample_regtest_upgrade_activations(),
    )?;
    let before = ReaderFixture::new(writer.serving_store_fixture()?)?;
    writer.append_live_block(&chain)?;
    let after = ReaderFixture::new(writer.into_serving_store_fixture()?)?;
    let recorder = direct_recorder(before.query).with_later_views(vec![after.query]);
    let mut endpoint = Endpoint::start(
        Server::builder().add_service(LightwalletdGrpcAdapter::new(recorder.clone()).into_server()),
    )
    .await?;
    let received = endpoint
        .blocks(block_range_request(1, u64::from(REQUEST_END)))
        .await?;
    assert_one_linked_chain(&received, 1, u64::from(REQUEST_END));
    let received = endpoint
        .blocks(block_range_request(u64::from(REQUEST_END), 1))
        .await?;
    assert_one_linked_chain(&received, u64::from(REQUEST_END), 1);
    let final_offer = recorder
        .offered_compact_block_ranges()
        .last()
        .copied()
        .ok_or_else(|| eyre!("missing final descending read"))?;
    assert_eq!(final_offer.start, BlockHeight::new(1));
    assert_eq!(recorder.recorded_epoch_ids().last(), Some(&None));
    Ok(())
}

async fn drain_rpc_range(endpoint: &mut Endpoint, first: u64, last: u64) -> DrainedBlockRange {
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

#[tokio::test]
async fn a_malformed_live_row_ends_after_the_same_delivered_prefix_as_the_fallback_reader()
-> eyre::Result<()> {
    for (damaged_length, expected_count) in [(1, 0), (BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES + 1, 3)] {
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
        let mut stock = Endpoint::start(
            Server::builder().add_service(
                LightwalletdGrpcAdapter::new(EpochPinRecorder::new(fixture.query.clone()))
                    .into_server(),
            ),
        )
        .await?;
        let mut direct = Endpoint::start(
            Server::builder()
                .add_service(LightwalletdGrpcAdapter::new(fixture.query).into_server()),
        )
        .await?;
        for (start, end, prefix) in [
            (1, 6, expected_count),
            (6, 1, if expected_count == 0 { 0 } else { 2 }),
        ] {
            let expected = drain_rpc_range(&mut stock, start, end).await;
            assert_eq!(expected.blocks.len(), prefix);
            let status = expected
                .ending
                .ok_or_else(|| eyre!("fallback accepted invalid compact bytes"))?;
            let actual = drain_rpc_range(&mut direct, start, end).await;
            assert_eq!(actual.blocks, expected.blocks);
            assert_stream_ended_with(actual.ending.as_ref(), status.code());
        }
    }
    Ok(())
}

#[tokio::test]
async fn damaged_settled_shielded_segment_fails_closed_with_data_loss_in_both_orders()
-> eyre::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let chain = ChainFixture::new(NETWORK).extend_blocks(4_196);
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(4_096)),
        &sample_regtest_upgrade_activations(),
    )?;
    let primary = writer.canonical_primary_path().to_path_buf();
    let paths = writer.into_serving_store_fixture()?;
    let shielded_dir = primary.join("segments").join("shielded");
    let segment_path = shielded_dir.join("0000000001-0000004096.zshrng02");
    eyre::ensure!(
        segment_path.exists(),
        "missing settled ZSHRNG02 segment file"
    );
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&segment_path)?;
    file.seek(SeekFrom::Start(64))?;
    file.write_all(&[0xff])?;
    drop(file);

    let fixture = ReaderFixture::new(paths)?;
    let mut direct = Endpoint::start(
        Server::builder().add_service(LightwalletdGrpcAdapter::new(fixture.query).into_server()),
    )
    .await?;
    for (first, last) in [(1, 3), (3, 1)] {
        let drained = drain_rpc_range(&mut direct, first, last).await;
        assert_eq!(drained.blocks, [] as [lightwalletd::CompactBlock; 0]);
        assert_stream_ended_with(drained.ending.as_ref(), Code::DataLoss);
    }
    Ok(())
}

fn pad_live_tail_shielded_payload_with_unknown_fields(
    primary: &Path,
    height: BlockHeight,
    min_len: usize,
) -> eyre::Result<Vec<u8>> {
    use sha2::Digest as _;
    let options = rust_rocksdb::Options::default();
    let families = rust_rocksdb::DB::list_cf(&options, primary)?;
    let db = rust_rocksdb::DB::open_cf(&options, primary, &families)?;
    let family = db
        .cf_handle("live_tail_block")
        .ok_or_else(|| eyre!("the canonical primary has no live_tail_block family"))?;
    let encoded = db
        .get_cf(&family, height.value().to_be_bytes())?
        .ok_or_else(|| eyre!("missing live_tail_block row at middle height"))?;
    let body = encoded
        .get(..encoded.len().saturating_sub(32))
        .ok_or_else(|| eyre!("live_tail_block row missing trailer"))?;
    let len_bytes: [u8; 4] = body
        .get(85..89)
        .ok_or_else(|| eyre!("live_tail_block row missing shielded length"))?
        .try_into()?;
    let old_len = usize::try_from(u32::from_be_bytes(len_bytes))?;
    let mut shielded = body
        .get(89..89 + old_len)
        .ok_or_else(|| eyre!("live_tail_block shielded payload truncated"))?
        .to_vec();
    let suffix = &body[89 + old_len..];
    // Unknown varint field 127 is skipped by the typed protobuf decoder.
    while shielded.len() <= min_len {
        shielded.extend_from_slice(&[0xf8, 0x07, 1]);
    }
    let mut updated = Vec::with_capacity(body.len() + shielded.len() + 32);
    updated.extend_from_slice(&body[..85]);
    updated.extend_from_slice(&u32::try_from(shielded.len())?.to_be_bytes());
    updated.extend_from_slice(&shielded);
    updated.extend_from_slice(suffix);
    let digest = sha2::Sha256::digest(&updated);
    updated.extend_from_slice(&digest);
    Ok(updated)
}

#[tokio::test]
async fn unknown_protobuf_fields_in_live_tail_respect_first_chunk_byte_budget() -> eyre::Result<()>
{
    let chain = ChainFixture::new(NETWORK).extend_blocks(6);
    let writer = writer(&chain)?;
    let primary = writer.canonical_primary_path().to_owned();
    let paths = writer.into_serving_store_fixture()?;
    let tail_mid = BlockHeight::new(5);
    let updated_row = pad_live_tail_shielded_payload_with_unknown_fields(
        &primary,
        tail_mid,
        BLOCK_RANGE_FIRST_CHUNK_MAX_BYTES,
    )?;
    damage_compact_block_row(&primary, tail_mid, Some(&updated_row))?;
    let fixture = ReaderFixture::new(paths)?;
    let recorder = direct_recorder(fixture.query);
    let mut wallet = Endpoint::start(
        Server::builder().add_service(LightwalletdGrpcAdapter::new(recorder.clone()).into_server()),
    )
    .await?;
    let asc = wallet.blocks(block_range_request(4, 6)).await?;
    assert_one_linked_chain(&asc, 4, 6);
    assert!(recorder.offered_compact_block_ranges().len() >= 2);
    Ok(())
}

fn dense_history() -> eyre::Result<ChainFixture> {
    let mut chain = ChainFixture::new(NETWORK)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(40);
    for number in 2_u8..=40 {
        let block = fixture_block(&chain, BlockHeight::new(u32::from(number)))?;
        let txid = TransactionId::from_bytes([number; 32]);
        let mut rows = FixtureTransactionRows::from_raw_transaction(
            txid,
            block.height,
            block.hash,
            0,
            [number],
        );
        rows.facts.public_facts.counts.sapling_spend_count = 2048;
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
                            nullifier: [number; 32]
                        };
                        2048
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
        chain = chain
            .with_transaction_rows(rows)
            .with_compact_block_artifact(compact);
    }
    Ok(chain)
}

#[tokio::test]
async fn dense_canonical_range_negotiates_compression_and_preserves_the_identity_body()
-> eyre::Result<()> {
    let chain = dense_history()?;
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(20)),
        &sample_regtest_upgrade_activations(),
    )?;
    let reader = ReaderFixture::new(writer.into_serving_store_fixture()?)?;
    let mut server = LightwalletdGrpcAdapter::new(reader.query).into_server();

    let message = block_range_request(2, 4).encode_to_vec();
    let mut frame = vec![0];
    frame.extend_from_slice(&u32::try_from(message.len())?.to_be_bytes());
    frame.extend(message);
    let frame = Bytes::from(frame);
    let mut identity_bytes: Option<Bytes> = None;
    for accepted in [None, Some("identity"), Some("gzip"), Some("zstd")] {
        let mut request =
            http::Request::post("/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange")
                .header("content-type", "application/grpc");
        if let Some(encoding) = accepted {
            request = request.header("grpc-accept-encoding", encoding);
        }
        let response = server
            .call(request.body(Body::new(http_body_util::Full::new(frame.clone())))?)
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
        assert_eq!(
            response
                .extensions()
                .get::<lightwalletd::BoundedChunkRetention>()
                .is_some(),
            !compressed,
        );
        let body = response.into_body().collect().await?;
        assert_eq!(
            body.trailers()
                .and_then(|trailers| trailers.get("grpc-status")),
            Some(&http::HeaderValue::from_static("0"))
        );
        let bytes = body.to_bytes();
        assert_eq!(bytes.first().copied(), Some(u8::from(compressed)));
        if let Some(identity) = &identity_bytes {
            if compressed {
                assert!(bytes.len() < identity.len() / 2);
            } else {
                assert_eq!(&bytes, identity);
            }
        } else {
            assert!(bytes.len() > 128 * 1024);
            identity_bytes = Some(bytes);
        }
    }
    Ok(())
}

async fn paced_clients(
    client: &CompactTxStreamerClient<Channel>,
    expected: &Arc<Vec<Vec<u8>>>,
    count: usize,
    cancel_fourth: bool,
) -> eyre::Result<usize> {
    let mut tasks = tokio::task::JoinSet::new();
    for idx in 0..count {
        let mut client = client.clone();
        let expected = Arc::clone(expected);
        let cancel = cancel_fourth && idx % 4 == 0;
        tasks.spawn(async move {
            let mut stream = client
                .get_block_range(block_range_request(1, 40))
                .await?
                .into_inner();
            let mut received = 0_usize;
            while let Some(block) = stream.message().await? {
                if cancel && received == 2 {
                    return Ok::<_, eyre::Report>(false);
                }
                eyre::ensure!(block.encode_to_vec() == expected[received]);
                received += 1;
            }
            eyre::ensure!(received == expected.len());
            Ok(true)
        });
    }
    let mut completed = 0_usize;
    while let Some(joined) = tasks.join_next().await {
        if joined?? {
            completed += 1;
        }
    }
    Ok(completed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paced_dense_ranges_complete_and_cancelled_readers_release_capacity() -> eyre::Result<()> {
    let chain = dense_history()?;
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(20)),
        &sample_regtest_upgrade_activations(),
    )?;
    let reader = ReaderFixture::new(writer.into_serving_store_fixture()?)?;
    let mut stock = Endpoint::start(Server::builder().add_service(
        LightwalletdGrpcAdapter::new(EpochPinRecorder::new(reader.query.clone())).into_server(),
    ))
    .await?;
    let expected = Arc::new(
        stock
            .blocks(block_range_request(1, 40))
            .await?
            .iter()
            .map(Message::encode_to_vec)
            .collect::<Vec<_>>(),
    );
    assert!(
        expected.iter().map(Vec::len).sum::<usize>()
            > zinder_compat_lightwalletd::BLOCK_RANGE_CHUNK_MAX_BYTES
    );
    let direct = Endpoint::start(
        Server::builder().add_service(
            LightwalletdGrpcAdapter::new(reader.query)
                .with_max_concurrent_block_range_streams(32_u32.try_into()?)
                .with_block_range_admission_wait(Duration::ZERO)
                .into_server(),
        ),
    )
    .await?;
    for (count, cancel, completed) in [
        (1, false, 1),
        (16, false, 16),
        (32, true, 24),
        (32, false, 32),
    ] {
        let actual = tokio::time::timeout(
            Duration::from_secs(45),
            paced_clients(&direct.client, &expected, count, cancel),
        )
        .await??;
        assert_eq!(actual, completed);
    }
    Ok(())
}

fn grpc_subset_commitment(seed: u8) -> [u8; 32] {
    let mut cmx = [0; 32];
    cmx[0] = seed;
    cmx
}

fn grpc_subset_action(seed: u8) -> CompactShieldedAction {
    CompactShieldedAction {
        nullifier: [seed; 32],
        commitment: grpc_subset_commitment(seed.wrapping_add(1)),
        ephemeral_key: [seed.wrapping_add(2); 32],
        ciphertext: [seed.wrapping_add(3); 52],
    }
}

fn grpc_subset_sapling_out(seed: u8) -> CompactSaplingOutput {
    CompactSaplingOutput {
        commitment: grpc_subset_commitment(seed),
        ephemeral_key: [seed.wrapping_add(1); 32],
        ciphertext: [seed.wrapping_add(2); 52],
    }
}

fn grpc_settled_subset_txs(height_num: u32) -> Vec<CompactTransaction> {
    match height_num {
        2 => vec![
            CompactTransaction {
                index: 0,
                transaction_id: TransactionId::from_bytes([0x21; 32]),
                data: CompactTransactionData {
                    transparent_inputs: vec![CompactTransparentInput {
                        previous_transaction_id: TransactionId::from_bytes([0x01; 32]),
                        previous_output_index: 0,
                    }],
                    transparent_outputs: vec![CompactTransparentOutput {
                        value_zat: 1_000,
                        script_pub_key: vec![0x51],
                    }],
                    ..Default::default()
                },
            },
            CompactTransaction {
                index: 1,
                transaction_id: TransactionId::from_bytes([0x22; 32]),
                data: CompactTransactionData {
                    fee_zat: Some(1_000),
                    sapling_outputs: vec![grpc_subset_sapling_out(0x23)],
                    orchard_actions: vec![grpc_subset_action(0x24)],
                    ..Default::default()
                },
            },
        ],
        3 => vec![CompactTransaction {
            index: 0,
            transaction_id: TransactionId::from_bytes([0x31; 32]),
            data: CompactTransactionData {
                fee_zat: Some(2_000),
                sapling_spends: vec![CompactSaplingSpend {
                    nullifier: [0x32; 32],
                }],
                sapling_outputs: vec![grpc_subset_sapling_out(0x33)],
                ..Default::default()
            },
        }],
        4 => vec![CompactTransaction {
            index: 1,
            transaction_id: TransactionId::from_bytes([0x41; 32]),
            data: CompactTransactionData {
                fee_zat: Some(2_500),
                orchard_actions: vec![grpc_subset_action(0x42)],
                ironwood_actions: vec![grpc_subset_action(0x46)],
                ..Default::default()
            },
        }],
        5 => vec![CompactTransaction {
            index: 1,
            transaction_id: TransactionId::from_bytes([0x51; 32]),
            data: CompactTransactionData {
                fee_zat: Some(3_000),
                sapling_outputs: vec![grpc_subset_sapling_out(0x52)],
                ironwood_actions: vec![grpc_subset_action(0x53)],
                ..Default::default()
            },
        }],
        _ => Vec::new(),
    }
}

fn grpc_tail_subset_txs(height_num: u32) -> Vec<CompactTransaction> {
    match height_num {
        6 => vec![CompactTransaction {
            index: 2,
            transaction_id: TransactionId::from_bytes([0x61; 32]),
            data: CompactTransactionData {
                fee_zat: Some(4_000),
                orchard_actions: vec![grpc_subset_action(0x62), grpc_subset_action(0x66)],
                ..Default::default()
            },
        }],
        7 => vec![CompactTransaction {
            index: 0,
            transaction_id: TransactionId::from_bytes([0x71; 32]),
            data: CompactTransactionData {
                fee_zat: Some(5_000),
                sapling_spends: vec![CompactSaplingSpend {
                    nullifier: [0x72; 32],
                }],
                orchard_actions: vec![grpc_subset_action(0x73)],
                ironwood_actions: vec![grpc_subset_action(0x77)],
                ..Default::default()
            },
        }],
        _ => Vec::new(),
    }
}

fn grpc_subset_txs(height_num: u32) -> Vec<CompactTransaction> {
    if height_num <= SUBSET_SETTLED_END {
        grpc_settled_subset_txs(height_num)
    } else {
        grpc_tail_subset_txs(height_num)
    }
}

fn subset_writer(chain: &ChainFixture) -> eyre::Result<CanonicalWriterFixture> {
    let entries = sample_regtest_upgrade_activations()
        .activations()
        .iter()
        .cloned()
        .map(|mut entry| {
            if entry.name == "NU6.3" {
                entry.activation_height = BlockHeight::new(SUBSET_IRONWOOD_ACTIVATION);
            }
            entry
        })
        .collect();
    let activations = zinder_core::NetworkUpgradeActivations::new(NETWORK, entries)?;
    CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(chain).with_settled_tip(BlockHeight::new(SUBSET_SETTLED_END)),
        &activations,
    )
}

fn attach_grpc_subset_rows(
    mut chain: ChainFixture,
    blk: &FixtureBlock,
    txs: &[CompactTransaction],
) -> eyre::Result<ChainFixture> {
    let Some(max_index) = txs.iter().map(|tx| tx.index).max() else {
        return Ok(chain);
    };
    for idx in 0..=max_index {
        let tx_id = txs.iter().find(|tx| tx.index == idx).map_or_else(
            || {
                let mut raw_id = [0xfe; 32];
                raw_id[0..4].copy_from_slice(&blk.height.value().to_le_bytes());
                raw_id[4..12].copy_from_slice(&idx.to_le_bytes());
                TransactionId::from_bytes(raw_id)
            },
            |found| found.transaction_id,
        );
        chain = chain.with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            tx_id,
            blk.height,
            blk.hash,
            u32::try_from(idx)?,
            b"grpc-subset-tx".as_slice(),
        ));
    }
    Ok(chain)
}

fn grpc_subset_blocks(mut chain: ChainFixture) -> eyre::Result<ChainFixture> {
    let mut sapling_tree_size = 0_u32;
    let mut orchard_tree_size = 0_u32;
    let mut ironwood_tree_size = 0_u32;
    for height_num in 1_u32..=8 {
        let blk = fixture_block(&chain, BlockHeight::new(height_num))?;
        let txs = grpc_subset_txs(height_num);
        for tx in &txs {
            sapling_tree_size += u32::try_from(tx.data.sapling_outputs.len())?;
            orchard_tree_size += u32::try_from(tx.data.orchard_actions.len())?;
            ironwood_tree_size += u32::try_from(tx.data.ironwood_actions.len())?;
        }
        chain = attach_grpc_subset_rows(chain, &blk, &txs)?;
        let parent_hash = if height_num == 1 {
            NETWORK.genesis_hash()
        } else {
            blk.parent_hash
        };
        let compact = CompactBlockArtifact::new(
            BlockId::new(blk.height, blk.hash),
            parent_hash,
            blk.block_time_seconds,
            txs,
            CompactChainMetadata {
                sapling_commitment_tree_size: sapling_tree_size,
                orchard_commitment_tree_size: orchard_tree_size,
                ironwood_commitment_tree_size: ironwood_tree_size,
            },
        )?;
        chain = chain.with_compact_block_artifact(compact);
    }
    Ok(chain)
}

#[allow(
    deprecated,
    reason = "Compatibility covers GetBlockRangeNullifiers across all pool_types subsets."
)]
async fn assert_grpc_subset_parity(
    wallet: &mut Endpoint,
    blocks_only: &mut Endpoint,
    stock: &mut Endpoint,
    pools: &[i32],
) -> eyre::Result<()> {
    for (first, last) in [(1_u64, 3_u64), (3, 1), (4, 8), (8, 4), (1, 8), (8, 1)] {
        let mut req = block_range_request(first, last);
        req.pool_types = pools.to_vec();
        let encode_blocks = |blocks: Vec<lightwalletd::CompactBlock>| -> Vec<Vec<u8>> {
            blocks.iter().map(Message::encode_to_vec).collect()
        };
        let expected_range = encode_blocks(stock.blocks(req.clone()).await?);
        let wallet_range = encode_blocks(wallet.blocks(req.clone()).await?);
        let compact_range = encode_blocks(blocks_only.blocks(req.clone()).await?);
        assert_eq!(
            wallet_range, expected_range,
            "GetBlockRange {pools:?} {first}..={last}"
        );
        assert_eq!(
            compact_range, expected_range,
            "CompactBlocks {pools:?} {first}..={last}"
        );

        let expected_nf = drain_slowly(
            stock
                .client
                .get_block_range_nullifiers(req.clone())
                .await?
                .into_inner(),
        )
        .await;
        let actual_nf = drain_slowly(
            wallet
                .client
                .get_block_range_nullifiers(req)
                .await?
                .into_inner(),
        )
        .await;
        assert!(expected_nf.ending.is_none() && actual_nf.ending.is_none());
        assert_eq!(
            encode_blocks(actual_nf.blocks),
            encode_blocks(expected_nf.blocks),
            "Nullifiers {pools:?} {first}..={last}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn grpc_pool_subset_ranges_and_nullifiers_match_stock_wire_bytes_with_zero_fallback_reads()
-> eyre::Result<()> {
    let chain = grpc_subset_blocks(
        ChainFixture::new(NETWORK)
            .with_raw_blob_retention(RawBlobRetention::None)
            .extend_blocks(8),
    )?;
    let store = ReaderFixture::new(subset_writer(&chain)?.into_serving_store_fixture()?)?;
    let recorder = direct_recorder(store.query.clone());
    let mut wallet = Endpoint::start(
        Server::builder().add_service(LightwalletdGrpcAdapter::new(recorder.clone()).into_server()),
    )
    .await?;
    let mut blocks_only = Endpoint::start(Server::builder().add_service(
        CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(store.reader.clone())).into_server(),
    ))
    .await?;
    let mut stock = Endpoint::start(Server::builder().add_service(
        LightwalletdGrpcAdapter::new(EpochPinRecorder::new(store.query)).into_server(),
    ))
    .await?;

    let sap = PoolType::Sapling as i32;
    let orc = PoolType::Orchard as i32;
    let irw = PoolType::Ironwood as i32;
    for pools in [
        Vec::new(),
        vec![99],
        vec![sap],
        vec![orc],
        vec![irw],
        vec![sap, orc],
        vec![sap, irw],
        vec![orc, irw],
        vec![sap, orc, irw],
    ] {
        assert_grpc_subset_parity(&mut wallet, &mut blocks_only, &mut stock, &pools).await?;
    }
    assert_eq!(
        recorder.recorded_compact_block_ranges(),
        [] as [BlockHeightRange; 0]
    );

    let mut transparent_orchard = block_range_request(1, 8);
    transparent_orchard.pool_types = vec![PoolType::Transparent as i32, orc];
    assert_eq!(
        wallet.blocks(transparent_orchard.clone()).await?,
        stock.blocks(transparent_orchard).await?
    );
    assert_eq!(
        recorder.recorded_compact_block_ranges(),
        [] as [BlockHeightRange; 0]
    );
    assert!(
        recorder
            .recorded_encoded_ranges
            .lock()
            .iter()
            .any(|covered| *covered
                == BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(8))),
        "expected transparent + orchard range to stream via direct encoded wire splicing"
    );
    Ok(())
}
