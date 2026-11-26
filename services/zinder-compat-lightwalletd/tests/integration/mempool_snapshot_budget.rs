//! `GetMempoolTx` and `GetMempoolStream` over real local gRPC across >4 MiB mempool snapshots.

use std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration};

use async_trait::async_trait;
use eyre::eyre;
use tokio::{net::TcpListener, sync::watch};
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, transport::Server};
use tonic_types::StatusExt as _;
use zebra_chain::{serialization::ZcashDeserialize as _, transaction::Transaction};
use zinder_compat_lightwalletd::{
    IngestControlMempoolSurface, LightwalletdGrpcAdapter, WatchTipChangeWatcher,
};
use zinder_core::{
    AuthDigest, BlockHeight, BlockId, Network, NetworkUpgradeActivations, RawTransactionBytes,
    SubtreeRootRange, TransactionId, UnixTimestampMillis, wire::encode_internal_transaction_id,
};
use zinder_ingest::{
    CanonicalFollowConfig, CanonicalFollower, CanonicalIngestControlGrpcAdapter, LiveMempoolOwner,
    canonical_control_channel, follow_canonical_tip_with_control, mempool_ready_channel,
    run_live_mempool_owner,
};
use zinder_proto::{
    ZINDER_ERROR_DOMAIN,
    compat::lightwalletd::{self, compact_tx_streamer_client::CompactTxStreamerClient},
    v1::ops::ErrorReason,
};
use zinder_query::{
    RocksDbCanonicalReader, RocksDbWalletReader, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_runtime::{ClientAddressSource, ComposedReadiness, Readiness, public_grpc_server};
use zinder_source::{
    MempoolSourceEntry, NodeCapabilities, NodeCapability, NodeSource, SourceBlock,
    SourceChainSegment, SourceChainSegmentLimits, SourceError, SourceSubtreeRoots,
};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalStore,
    RocksDbResourceBudget,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, MockMempoolSource,
    MockMempoolSourceControl, sample_regtest_upgrade_activations,
};

type TestOutcome<T> = eyre::Result<T>;

const LARGE_SNAPSHOT_ENTRY_COUNT: u32 = 600;
const NINE_KIB_SCRIPT_BYTES: usize = 9 * 1_024;
const NEAR_TWO_MIB_UNLOCK_SCRIPT_BYTES: usize = 1_950_000;
const OVERSIZED_LOCK_SCRIPT_BYTES: usize = 1_500_000;

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_pages_past_four_mib_preserving_order_and_exclusions()
-> TestOutcome<()> {
    let entries = (0..LARGE_SNAPSHOT_ENTRY_COUNT)
        .map(|sequence| synthetic_source_entry(sequence, NINE_KIB_SCRIPT_BYTES, 25))
        .collect::<TestOutcome<Vec<_>>>()?;
    let expected_ids = sorted_internal_txids(&entries);

    with_live_compat_grpc(entries, |mut client, _feed, _tip_sender| async move {
        let full_stream = client
            .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
                exclude_txid_suffixes: Vec::new(),
                pool_types: vec![lightwalletd::PoolType::Transparent as i32],
            }))
            .await?
            .into_inner();
        let observed_ids = collect_compact_txids(full_stream).await?;
        assert_eq!(observed_ids, expected_ids);

        let excluded_target = expected_ids
            .get(450)
            .copied()
            .ok_or_else(|| eyre!("expected entry at index 450"))?;
        let filtered_stream = client
            .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
                exclude_txid_suffixes: vec![excluded_target.to_vec()],
                pool_types: vec![lightwalletd::PoolType::Transparent as i32],
            }))
            .await?
            .into_inner();
        let filtered_ids = collect_compact_txids(filtered_stream).await?;
        assert_eq!(filtered_ids.len(), expected_ids.len().saturating_sub(1));
        assert!(!filtered_ids.contains(&excluded_target));
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_pages_past_four_mib_and_hands_off_to_events()
-> TestOutcome<()> {
    let entries = (0..LARGE_SNAPSHOT_ENTRY_COUNT)
        .map(|sequence| synthetic_source_entry(sequence, NINE_KIB_SCRIPT_BYTES, 25))
        .collect::<TestOutcome<Vec<_>>>()?;
    let post_snapshot =
        synthetic_source_entry(LARGE_SNAPSHOT_ENTRY_COUNT + 1, NINE_KIB_SCRIPT_BYTES, 25)?;
    let expected_raw_by_order = sorted_raw_transactions(&entries);
    let post_snapshot_raw = post_snapshot.raw_transaction_bytes.as_slice().to_vec();

    with_live_compat_grpc(entries, |mut client, feed, tip_sender| async move {
        let mut stream = client
            .get_mempool_stream(Request::new(lightwalletd::Empty {}))
            .await?
            .into_inner();
        let mut received = Vec::with_capacity(expected_raw_by_order.len());
        for _ in 0..expected_raw_by_order.len() {
            let raw_tx = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await?
                .ok_or_else(|| {
                    eyre!("GetMempoolStream ended before delivering snapshot entries")
                })??;
            assert_eq!(raw_tx.height, 0);
            received.push(raw_tx.data);
        }
        assert_eq!(received, expected_raw_by_order);

        feed.push_added(post_snapshot)?;
        let live_tx = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await?
            .ok_or_else(|| eyre!("GetMempoolStream ended before post-snapshot Added event"))??;
        assert_eq!(live_tx.data, post_snapshot_raw);

        tip_sender.send(999)?;
        let terminal = tokio::time::timeout(Duration::from_secs(5), stream.next()).await?;
        assert!(
            terminal.is_none(),
            "GetMempoolStream must close cleanly with OK on tip advance"
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_mempool_rpcs_serve_near_two_mib_entries_over_default_grpc() -> TestOutcome<()>
{
    let entries = vec![
        synthetic_source_entry(1, NINE_KIB_SCRIPT_BYTES, 25)?,
        synthetic_source_entry(2, NEAR_TWO_MIB_UNLOCK_SCRIPT_BYTES, 25)?,
        synthetic_source_entry(3, NEAR_TWO_MIB_UNLOCK_SCRIPT_BYTES, 25)?,
    ];
    let expected_ids = sorted_internal_txids(&entries);
    let expected_raw = sorted_raw_transactions(&entries);

    with_live_compat_grpc(entries, |mut client, _feed, tip_sender| async move {
        let tx_stream = client
            .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
                exclude_txid_suffixes: Vec::new(),
                pool_types: vec![lightwalletd::PoolType::Transparent as i32],
            }))
            .await?
            .into_inner();
        assert_eq!(collect_compact_txids(tx_stream).await?, expected_ids);

        let mut raw_stream = client
            .get_mempool_stream(Request::new(lightwalletd::Empty {}))
            .await?
            .into_inner();
        let mut observed_raw = Vec::with_capacity(expected_raw.len());
        for _ in 0..expected_raw.len() {
            let raw_tx = tokio::time::timeout(Duration::from_secs(10), raw_stream.next())
                .await?
                .ok_or_else(|| eyre!("GetMempoolStream ended early on ~2 MiB entries"))??;
            observed_raw.push(raw_tx.data);
        }
        assert_eq!(observed_raw, expected_raw);
        tip_sender.send(999)?;
        assert!(
            tokio::time::timeout(Duration::from_secs(5), raw_stream.next())
                .await?
                .is_none()
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_mempool_rpcs_reject_first_page_oversized_entry_with_error_info()
-> TestOutcome<()> {
    let oversized = synthetic_source_entry(99, 32, OVERSIZED_LOCK_SCRIPT_BYTES)?;

    with_live_compat_grpc(
        vec![oversized],
        |mut client, _feed, _tip_sender| async move {
            let tx_outcome = client
                .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
                    exclude_txid_suffixes: Vec::new(),
                    pool_types: vec![lightwalletd::PoolType::Transparent as i32],
                }))
                .await;
            let tx_status = match tx_outcome {
                Err(status) => status,
                Ok(response) => collect_compact_txids(response.into_inner())
                    .await
                    .err()
                    .and_then(|err| err.downcast::<tonic::Status>().ok())
                    .ok_or_else(|| {
                        eyre!("GetMempoolTx must fail when the first snapshot entry exceeds budget")
                    })?,
            };
            assert_sanitized_unsupported_wallet_encoding(&tx_status)?;

            let raw_outcome = client
                .get_mempool_stream(Request::new(lightwalletd::Empty {}))
                .await;
            let (yielded_before_error, raw_status) = match raw_outcome {
                Err(status) => (Vec::new(), status),
                Ok(response) => collect_raw_until_stream_error(response.into_inner()).await?,
            };
            assert_eq!(yielded_before_error, [] as [Vec<u8>; 0]);
            assert_sanitized_unsupported_wallet_encoding(&raw_status)?;
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_mempool_rpcs_handle_second_page_oversized_entry_deterministically()
-> TestOutcome<()> {
    let (normal_entry, oversized_entry) = ordered_normal_then_oversized_entries()?;
    let expected_normal_raw = normal_entry.raw_transaction_bytes.as_slice().to_vec();

    with_live_compat_grpc(
        vec![normal_entry, oversized_entry],
        |mut client, _feed, _tip_sender| async move {
            let tx_outcome = client
                .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
                    exclude_txid_suffixes: Vec::new(),
                    pool_types: vec![lightwalletd::PoolType::Transparent as i32],
                }))
                .await;
            let tx_status = match tx_outcome {
                Err(status) => status,
                Ok(response) => collect_compact_txids(response.into_inner())
                    .await
                    .err()
                    .and_then(|err| err.downcast::<tonic::Status>().ok())
                    .ok_or_else(|| {
                        eyre!("GetMempoolTx must fail when page 2 encounters an oversized entry")
                    })?,
            };
            assert_sanitized_unsupported_wallet_encoding(&tx_status)?;

            let response = client
                .get_mempool_stream(Request::new(lightwalletd::Empty {}))
                .await?;
            let (yielded_before_error, raw_status) =
                collect_raw_until_stream_error(response.into_inner()).await?;
            assert_eq!(
                yielded_before_error,
                vec![expected_normal_raw],
                "GetMempoolStream must emit page 1's valid transaction before failing on page 2"
            );
            assert_sanitized_unsupported_wallet_encoding(&raw_status)?;
            Ok(())
        },
    )
    .await
}

fn assert_sanitized_unsupported_wallet_encoding(status: &tonic::Status) -> TestOutcome<()> {
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    let message = status.message();
    assert!(
        !message.contains("ingest-control") && !message.contains("MetadataMap"),
        "public CompactTxStreamer status message leaked internal transport text: {message}"
    );
    let details = status.get_error_details();
    let error_detail = details.error_info().ok_or_else(|| {
        eyre!("CompactTxStreamer status omitted google.rpc.ErrorInfo: {status:?}")
    })?;
    assert_eq!(error_detail.domain, ZINDER_ERROR_DOMAIN);
    assert_eq!(
        error_detail.reason,
        ErrorReason::UnsupportedWalletEncoding.as_str_name()
    );
    Ok(())
}

fn ordered_normal_then_oversized_entries() -> TestOutcome<(MempoolSourceEntry, MempoolSourceEntry)>
{
    let oversized = synthetic_source_entry(99, 32, OVERSIZED_LOCK_SCRIPT_BYTES)?;
    for sequence in 1..1_000 {
        if sequence == 99 {
            continue;
        }
        let candidate = synthetic_source_entry(sequence, NINE_KIB_SCRIPT_BYTES, 25)?;
        if candidate.transaction_id < oversized.transaction_id {
            return Ok((candidate, oversized));
        }
    }
    Err(eyre!(
        "failed to synthesize a normal mempool entry ordered before the oversized entry"
    ))
}

async fn collect_compact_txids(
    mut stream: tonic::Streaming<lightwalletd::CompactTx>,
) -> TestOutcome<Vec<[u8; 32]>> {
    let mut ids = Vec::new();
    while let Some(compact_tx) =
        tokio::time::timeout(Duration::from_secs(10), stream.next()).await?
    {
        let compact_tx = compact_tx?;
        let txid: [u8; 32] = compact_tx
            .txid
            .try_into()
            .map_err(|_| eyre!("CompactTx txid was not 32 bytes"))?;
        ids.push(txid);
    }
    Ok(ids)
}

async fn collect_raw_until_stream_error(
    mut stream: tonic::Streaming<lightwalletd::RawTransaction>,
) -> TestOutcome<(Vec<Vec<u8>>, tonic::Status)> {
    let mut yielded = Vec::new();
    while let Some(step) = tokio::time::timeout(Duration::from_secs(10), stream.next()).await? {
        match step {
            Ok(raw_tx) => {
                assert_eq!(raw_tx.height, 0);
                yielded.push(raw_tx.data);
            }
            Err(status) => return Ok((yielded, status)),
        }
    }
    Err(eyre!(
        "expected GetMempoolStream to terminate with an error status"
    ))
}

fn sorted_internal_txids(entries: &[MempoolSourceEntry]) -> Vec<[u8; 32]> {
    let mut ordered = entries
        .iter()
        .map(|entry| entry.transaction_id)
        .collect::<Vec<_>>();
    ordered.sort_unstable();
    ordered
        .into_iter()
        .map(encode_internal_transaction_id)
        .collect()
}

fn sorted_raw_transactions(entries: &[MempoolSourceEntry]) -> Vec<Vec<u8>> {
    let mut by_id = BTreeMap::new();
    for entry in entries {
        by_id.insert(
            entry.transaction_id,
            entry.raw_transaction_bytes.as_slice().to_vec(),
        );
    }
    by_id.into_values().collect()
}

fn synthetic_source_entry(
    sequence: u32,
    unlock_script_len: usize,
    lock_script_len: usize,
) -> TestOutcome<MempoolSourceEntry> {
    let mut previous_hash = [0_u8; 32];
    previous_hash[..4].copy_from_slice(&sequence.to_le_bytes());
    previous_hash[4] = 0xB7;
    let mut unlock_bytes = vec![0x51_u8; unlock_script_len];
    if unlock_bytes.len() >= 4 {
        unlock_bytes[..4].copy_from_slice(&sequence.to_be_bytes());
    }
    let mut lock_bytes = vec![0x76_u8; lock_script_len];
    if lock_bytes.len() >= 4 {
        lock_bytes[..4].copy_from_slice(&sequence.to_le_bytes());
    }
    let mut raw_bytes = Vec::with_capacity(64 + unlock_script_len + lock_script_len);
    raw_bytes.extend_from_slice(&1_u32.to_le_bytes());
    raw_bytes.push(1);
    raw_bytes.extend_from_slice(&previous_hash);
    raw_bytes.extend_from_slice(&sequence.to_le_bytes());
    append_compact_size(&mut raw_bytes, unlock_bytes.len())?;
    raw_bytes.extend_from_slice(&unlock_bytes);
    raw_bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    raw_bytes.push(1);
    raw_bytes.extend_from_slice(&(10_000_u64 + u64::from(sequence)).to_le_bytes());
    append_compact_size(&mut raw_bytes, lock_bytes.len())?;
    raw_bytes.extend_from_slice(&lock_bytes);
    raw_bytes.extend_from_slice(&0_u32.to_le_bytes());
    let transaction = Transaction::zcash_deserialize(raw_bytes.as_slice())?;
    Ok(MempoolSourceEntry {
        transaction_id: TransactionId::from_bytes(transaction.hash().0),
        auth_digest: transaction
            .auth_digest()
            .map(|digest| AuthDigest::from_bytes(digest.0)),
        raw_transaction_bytes: RawTransactionBytes::new(raw_bytes),
        observed_at_unix_millis: UnixTimestampMillis::new(
            1_750_000_000_000_u64.saturating_add(u64::from(sequence)),
        ),
    })
}

fn append_compact_size(buffer: &mut Vec<u8>, len: usize) -> TestOutcome<()> {
    if let Ok(short) = u8::try_from(len)
        && short <= 0xfc
    {
        buffer.push(short);
    } else if let Ok(medium) = u16::try_from(len) {
        buffer.push(0xfd);
        buffer.extend_from_slice(&medium.to_le_bytes());
    } else {
        let wide = u32::try_from(len)?;
        buffer.push(0xfe);
        buffer.extend_from_slice(&wide.to_le_bytes());
    }
    Ok(())
}

#[derive(Clone)]
struct PinnedTipNodeSource {
    tip: BlockId,
    tip_block: SourceBlock,
}

#[async_trait]
impl NodeSource for PinnedTipNodeSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        if height == self.tip.height {
            Ok(self.tip_block.clone())
        } else {
            Err(SourceError::BlockUnavailable {
                height,
                reason: "pinned-tip fixture serves only its tip height".to_owned(),
            })
        }
    }

    async fn fetch_chain_segment(
        &self,
        _limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        Ok(SourceChainSegment::default())
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        Ok(self.tip)
    }

    async fn fetch_subtree_root_range(
        &self,
        _range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        Err(SourceError::NodeCapabilityMissing {
            capability: NodeCapability::SubtreeRoots,
        })
    }
}

async fn with_live_compat_grpc<F, Fut>(
    initial_entries: Vec<MempoolSourceEntry>,
    run_scenario: F,
) -> TestOutcome<()>
where
    F: FnOnce(
            CompactTxStreamerClient<tonic::transport::Channel>,
            MockMempoolSourceControl,
            watch::Sender<u64>,
        ) -> Fut
        + Send,
    Fut: Future<Output = TestOutcome<()>> + Send,
{
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(1);
    let tip_block = chain
        .source_block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("chain fixture omitted block 1"))?;
    let writer_fixture =
        CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
    let canonical_path = writer_fixture.canonical_primary_path().to_path_buf();
    let mut store_fixture = writer_fixture.into_serving_store_fixture()?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(
        WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical_reader)?),
            Arc::new(RocksDbWalletReader::new(wallet_reader)),
        )?,
    )));
    let canonical_primary = RocksDbCanonicalStore::open_ready(
        &canonical_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let node_source = PinnedTipNodeSource {
        tip: canonical_primary.event_fence().visible_tip(),
        tip_block,
    };
    Box::pin(run_compat_grpc_stack(
        query,
        (canonical_primary, activations, node_source),
        initial_entries,
        run_scenario,
    ))
    .await
}

async fn run_compat_grpc_stack<F, Fut>(
    query: WalletServingQuery<()>,
    canonical_context: (
        RocksDbCanonicalStore,
        Arc<NetworkUpgradeActivations>,
        PinnedTipNodeSource,
    ),
    initial_entries: Vec<MempoolSourceEntry>,
    run_scenario: F,
) -> TestOutcome<()>
where
    F: FnOnce(
            CompactTxStreamerClient<tonic::transport::Channel>,
            MockMempoolSourceControl,
            watch::Sender<u64>,
        ) -> Fut
        + Send,
    Fut: Future<Output = TestOutcome<()>> + Send,
{
    let (canonical_primary, activations, node_source) = canonical_context;
    let tip = node_source.tip;
    let cancel = CancellationToken::new();
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let (writer, commands) = canonical_control_channel();
    let (compat_endpoint, feed, gate, tip_sender, tasks) = spawn_compat_services(
        query,
        writer,
        Arc::new(node_source.clone()),
        readiness.clone(),
        cancel.clone(),
    )
    .await?;
    let follower = CanonicalFollower::new(
        &node_source,
        activations,
        test_follow_config(),
        &readiness,
        &cancel,
    );
    let follow = Box::pin(follow_canonical_tip_with_control(
        canonical_primary,
        follower,
        commands,
    ));
    let drive = async {
        while feed.open_count() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for entry in initial_entries {
            feed.push_added(entry)?;
        }
        feed.complete_initial_snapshot(tip)?;
        while !gate.admits_canonical_tip(tip) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let _keep_tip_sender_open = tip_sender.clone();
        let client = CompactTxStreamerClient::connect(compat_endpoint).await?;
        Box::pin(run_scenario(client, feed, tip_sender)).await
    };

    let outcome = tokio::select! {
        outcome = tokio::time::timeout(Duration::from_secs(30), Box::pin(drive)) => outcome?,
        outcome = follow => {
            outcome?;
            Err(eyre!("canonical follower stopped unexpectedly"))
        }
    };
    cancel.cancel();
    tasks.shutdown().await;
    outcome
}

struct SpawnedCompatTasks {
    ingest: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    compat: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    hydrate: tokio::task::JoinHandle<()>,
}

impl SpawnedCompatTasks {
    async fn shutdown(self) {
        let _ = self.hydrate.await;
        self.compat.abort();
        self.ingest.abort();
    }
}

async fn spawn_compat_services(
    query: WalletServingQuery<()>,
    writer: zinder_ingest::CanonicalControlHandle,
    node_source: Arc<dyn NodeSource>,
    readiness: ComposedReadiness,
    cancel: CancellationToken,
) -> TestOutcome<(
    String,
    MockMempoolSourceControl,
    zinder_ingest::MempoolReadyGate,
    watch::Sender<u64>,
    SpawnedCompatTasks,
)> {
    let owner = LiveMempoolOwner::default();
    let (mempool_source, feed) = MockMempoolSource::streaming();
    let (signal, gate) = mempool_ready_channel();
    let ingest = CanonicalIngestControlGrpcAdapter::new(
        Network::ZcashRegtest,
        writer.clone(),
        owner.clone(),
        node_source,
        readiness,
    );
    let ingest_listener = TcpListener::bind("127.0.0.1:0").await?;
    let ingest_endpoint = format!("http://{}", ingest_listener.local_addr()?);
    let compat_listener = TcpListener::bind("127.0.0.1:0").await?;
    let compat_endpoint = format!("http://{}", compat_listener.local_addr()?);
    let (tip_sender, tip_receiver) = watch::channel(0_u64);
    let surface = Arc::new(IngestControlMempoolSurface::new(ingest_endpoint));
    let compat_adapter = LightwalletdGrpcAdapter::new(query)
        .with_mempool_surface(surface)
        .with_tip_change_watcher(Arc::new(WatchTipChangeWatcher::new(tip_receiver)));

    let ingest_task = tokio::spawn(
        Server::builder()
            .add_service(ingest.into_server())
            .serve_with_incoming(TcpListenerStream::new(ingest_listener)),
    );
    let compat_task = tokio::spawn(
        public_grpc_server(ClientAddressSource::Connection)
            .add_service(compat_adapter.into_server())
            .serve_with_incoming(compat_listener),
    );
    let hydrate_task = tokio::spawn(run_live_mempool_owner(
        Arc::new(mempool_source),
        writer,
        owner,
        signal,
        cancel,
    ));
    Ok((
        compat_endpoint,
        feed,
        gate,
        tip_sender,
        SpawnedCompatTasks {
            ingest: ingest_task,
            compat: compat_task,
            hydrate: hydrate_task,
        },
    ))
}

fn test_follow_config() -> CanonicalFollowConfig {
    CanonicalFollowConfig {
        request_timeout: Duration::from_secs(5),
        poll_interval: Duration::from_millis(25),
        lag_threshold_blocks: 0,
        target_height: None,
        event_retention_window: None,
        event_retention_check_interval: Duration::from_secs(60),
        mempool_ready_gate: None,
        max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
        node_failover: None,
    }
}
