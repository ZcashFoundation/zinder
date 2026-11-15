//! Stream-cancellation and concurrent-reader smoke for the native
//! `WalletQuery` server.
//!
//! These tests cover two production-blast-radius properties of the gRPC
//! adapter that the in-process trait tests cannot:
//!
//! - A client that disconnects mid-stream must not leave the server in a
//!   broken state. The next request through the same server has to succeed
//!   with no cooldown.
//! - A client that cancels an unspent-output stream must stop the server's
//!   storage reads for it and release the serving pair the stream holds, so
//!   an abandoned read cannot keep a secondary generation open.
//! - Shutdown must end open event streams, which never end on their own, so
//!   graceful server shutdown can drain instead of waiting forever.
//! - The server must serve many concurrent streamed reads without
//!   serializing them or returning errors. The default deployment shape
//!   puts one `zinder-query` process behind several native wallet clients,
//!   each fanning out wallet scan-back ranges over the same
//!   store; if the read path can't handle parallel streams, that topology
//!   is unusable.
//!
//! Runs under the standard `ci` profile. Tests open a private TCP listener
//! per run so they parallelize freely.

#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::num::NonZeroU16;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use eyre::{Result, eyre};
use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Code, transport::Server};
use tonic_types::StatusExt as _;
use zinder_core::{
    BlockHeight, BlockHeightRange, Network, TransactionId, TransparentAddressScriptHash,
    TransparentOutPoint, TransparentUnspentOutput,
};
use zinder_proto::v1::wallet::{self, wallet_query_client::WalletQueryClient};
use zinder_query::{
    TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE, WalletEndpointMetadata, WalletProjectionReader,
    WalletQueryGrpcAdapter, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_store::RawBlobRetention;
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};
use zinder_wallet_projection::{
    WalletAddressTransactionKey, WalletAddressUnspentOutputKey, WalletProjectionReadyEvidence,
    WalletSpentOutput, WalletUnspentOutput,
};
use zinder_wallet_rocksdb::{
    RocksDbWalletError, WalletAddressTransactionHistoryPage, WalletAddressUnspentOutputsPage,
};

/// Number of pre-committed blocks the test store carries. Picked so each
/// streamed range yields multiple chunks but the fixture build stays fast.
const COMMITTED_BLOCK_COUNT: u32 = 32;

/// Number of concurrent streamed reads the parallel-readers test fans out.
///
/// Sized to exceed `tokio`'s default executor thread count on typical CI
/// hosts so the test would fail if the server quietly serialized streams.
const CONCURRENT_READER_COUNT: u32 = 16;

/// Bound on how long the parallel-readers test waits for all streams to
/// complete. Sized so a stuck server fails the test instead of hanging the
/// runner indefinitely.
const PARALLEL_READERS_DEADLINE: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread")]
async fn dropping_compact_block_range_stream_does_not_break_subsequent_requests() -> Result<()> {
    let server_addr = commit_store_and_spawn_grpc().await?;
    let mut client = WalletQueryClient::connect(server_addr).await?;

    let mut first_stream = client
        .compact_blocks_in_range(wallet::CompactBlocksInRangeRequest {
            start_height: 1,
            end_height: COMMITTED_BLOCK_COUNT,
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    let first_chunk = first_stream
        .next()
        .await
        .ok_or_else(|| eyre!("server closed compact-block-range stream without any chunk"))??;
    assert!(
        matches!(
            first_chunk.body,
            Some(wallet::compact_blocks_in_range_chunk::Body::Header(_))
        ),
        "first stream's first chunk must be its chain-view header"
    );

    // Drop the stream mid-flight. The server must observe the cancellation
    // and tear down its handler without poisoning the adapter.
    drop(first_stream);

    let mut second_stream = client
        .compact_blocks_in_range(wallet::CompactBlocksInRangeRequest {
            start_height: 1,
            end_height: COMMITTED_BLOCK_COUNT,
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    let mut second_header_count: u32 = 0;
    let mut second_block_count: u32 = 0;
    while let Some(chunk) = second_stream.next().await {
        match chunk?.body {
            Some(wallet::compact_blocks_in_range_chunk::Body::Header(_)) => {
                second_header_count = second_header_count.saturating_add(1);
            }
            Some(wallet::compact_blocks_in_range_chunk::Body::Item(_)) => {
                second_block_count = second_block_count.saturating_add(1);
            }
            None => return Err(eyre!("second-stream chunk carried no body")),
        }
    }
    assert_eq!(second_header_count, 1, "the stream sends one header");
    assert_eq!(
        second_block_count, COMMITTED_BLOCK_COUNT,
        "post-cancellation request must drain the full committed range"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_full_block_range_stream_does_not_break_subsequent_requests() -> Result<()> {
    let server_addr = commit_store_and_spawn_grpc().await?;
    let mut client = WalletQueryClient::connect(server_addr).await?;

    let mut first_stream = client
        .full_blocks_in_range(wallet::FullBlocksInRangeRequest {
            start_height: 1,
            end_height: COMMITTED_BLOCK_COUNT,
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    let first_chunk = first_stream
        .next()
        .await
        .ok_or_else(|| eyre!("server closed full-block-range stream without any chunk"))??;
    assert!(
        first_chunk.full_block.is_some(),
        "first stream's first chunk must carry a full block"
    );

    // Drop the stream mid-flight so the bounded producer observes the closed
    // receiver and stops issuing sub-reads instead of leaking a task.
    drop(first_stream);

    let mut second_stream = client
        .full_blocks_in_range(wallet::FullBlocksInRangeRequest {
            start_height: 1,
            end_height: COMMITTED_BLOCK_COUNT,
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    let mut second_chunk_count: u32 = 0;
    while let Some(chunk) = second_stream.next().await {
        let chunk = chunk?;
        assert!(
            chunk.full_block.is_some(),
            "second-stream chunk must carry a full block"
        );
        second_chunk_count = second_chunk_count.saturating_add(1);
    }
    assert_eq!(
        second_chunk_count, COMMITTED_BLOCK_COUNT,
        "post-cancellation request must drain the full committed range"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_compact_block_range_readers_all_drain_to_completion() -> Result<()> {
    let server_addr = commit_store_and_spawn_grpc().await?;

    let reader_tasks: Vec<_> = (0..CONCURRENT_READER_COUNT)
        .map(|_| {
            let server_addr = server_addr.clone();
            tokio::spawn(async move {
                let mut client = WalletQueryClient::connect(server_addr).await?;
                let mut stream = client
                    .compact_blocks_in_range(wallet::CompactBlocksInRangeRequest {
                        start_height: 1,
                        end_height: COMMITTED_BLOCK_COUNT,
                        at_epoch_id: None,
                    })
                    .await?
                    .into_inner();
                let mut received: u32 = 0;
                while let Some(chunk) = stream.next().await {
                    if matches!(
                        chunk?.body,
                        Some(wallet::compact_blocks_in_range_chunk::Body::Item(_))
                    ) {
                        received = received.saturating_add(1);
                    }
                }
                Ok::<u32, eyre::Report>(received)
            })
        })
        .collect();

    let collect_outcomes = async {
        let mut outcomes = Vec::with_capacity(reader_tasks.len());
        for task in reader_tasks {
            outcomes.push(task.await);
        }
        outcomes
    };
    let outcomes = tokio::time::timeout(PARALLEL_READERS_DEADLINE, collect_outcomes)
        .await
        .map_err(|_| {
            eyre!(
                "concurrent compact-block-range readers did not all complete within {:?}",
                PARALLEL_READERS_DEADLINE
            )
        })?;

    for outcome in outcomes {
        let received = outcome??;
        assert_eq!(
            received, COMMITTED_BLOCK_COUNT,
            "each concurrent reader must drain the full committed range; got {received}"
        );
    }
    Ok(())
}

/// Bound on how long the shutdown test waits for the stream to end and the
/// server to drain.
const SHUTDOWN_DRAIN_DEADLINE: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_ends_an_open_chain_events_stream_and_drains_the_server() -> Result<()> {
    let shutdown = CancellationToken::new();
    let (grpc_adapter, _store_fixture) = commit_store_and_build_adapter().await?;
    let grpc_adapter = grpc_adapter.with_shutdown(shutdown.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = format!("http://{}", listener.local_addr()?);
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_adapter.into_server())
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                server_shutdown.cancelled_owned(),
            )
            .await
    });
    let mut client = WalletQueryClient::connect(server_addr).await?;
    let mut events = client
        .chain_events(wallet::ChainEventsRequest {
            start: Some(wallet::EventStreamStart {
                position: Some(wallet::event_stream_start::Position::LiveTail(
                    wallet::LiveTail {},
                )),
            }),
            family: wallet::ChainEventStreamFamily::Visible as i32,
            address_filter: Vec::new(),
        })
        .await?
        .into_inner();

    shutdown.cancel();
    let stream_end = tokio::time::timeout(SHUTDOWN_DRAIN_DEADLINE, events.next())
        .await
        .map_err(|_| eyre!("shutdown did not end the open chain-events stream"))?
        .ok_or_else(|| eyre!("chain-events stream ended without a shutdown status"))?;
    let status = stream_end
        .err()
        .ok_or_else(|| eyre!("an idle live-tail stream must not deliver an event"))?;
    assert_eq!(status.code(), Code::Unavailable);
    let readiness_cause = status
        .get_error_details()
        .error_info()
        .and_then(|error_info| error_info.metadata.get("readiness_cause").cloned());
    assert_eq!(readiness_cause.as_deref(), Some("shutting_down"));

    tokio::time::timeout(SHUTDOWN_DRAIN_DEADLINE, server)
        .await
        .map_err(|_| eyre!("the server did not drain after its streams ended"))???;
    Ok(())
}

/// Holders of the cancellation test's pair while no read holds it: the test
/// and the query's slot.
const IDLE_PAIR_HOLDERS: usize = 2;

/// HTTP/2 stream window of the client that stops reading, the protocol's
/// initial default.
const STALLED_CLIENT_STREAM_WINDOW_BYTES: u32 = 65_535;

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_an_unspent_output_stream_stops_its_storage_reads_and_releases_its_pair()
-> Result<()> {
    let address = TransparentAddressScriptHash::of_script_pub_key(&[0x51]);
    let page_size = u32::from(TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE.get());
    let page_count = 6;
    let (server_addr, served) =
        spawn_unspent_output_server(address, page_count * page_size).await?;
    // A small flow-control window keeps a client that stops reading from
    // absorbing the whole stream, so the server must hold the rest.
    let channel = tonic::transport::Endpoint::from_shared(server_addr)?
        .initial_stream_window_size(Some(STALLED_CLIENT_STREAM_WINDOW_BYTES))
        .connect()
        .await?;
    let mut client = WalletQueryClient::new(channel);
    let mut stream = client
        .transparent_address_unspent_outputs(wallet::TransparentAddressUnspentOutputsRequest {
            address: Some(wallet::AddressLookup {
                selector: Some(wallet::address_lookup::Selector::ScriptHash(
                    address.as_bytes().to_vec(),
                )),
            }),
            start_height: 0,
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    for expected_body in ["header", "item"] {
        let chunk = stream
            .next()
            .await
            .ok_or_else(|| eyre!("the stream ended before its {expected_body}"))??;
        assert!(
            chunk.body.is_some(),
            "the {expected_body} chunk carries no body"
        );
    }
    wait_until("the server to hold the stream's pair", || {
        served.pair_holders() > IDLE_PAIR_HOLDERS && served.page_reads() > 0
    })
    .await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        served.page_reads() < usize::try_from(page_count)?,
        "a client that stopped reading must stall the server's storage reads"
    );

    drop(stream);

    wait_until("the cancelled stream to release its pair", || {
        served.pair_holders() == IDLE_PAIR_HOLDERS
    })
    .await?;
    let page_reads = served.page_reads();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        served.page_reads(),
        page_reads,
        "a cancelled stream must not read storage again"
    );
    Ok(())
}

/// The pair and wallet-read counter behind one unspent-output test server.
struct ServedPair {
    pair: Arc<WalletServingReadPair>,
    page_reads: Arc<AtomicUsize>,
}

impl ServedPair {
    fn pair_holders(&self) -> usize {
        Arc::strong_count(&self.pair)
    }

    fn page_reads(&self) -> usize {
        self.page_reads.load(Ordering::Acquire)
    }
}

/// Serves a store whose block 1 creates `output_count` outputs for `address`,
/// counting the unspent-output pages the server reads.
async fn spawn_unspent_output_server(
    address: TransparentAddressScriptHash,
    output_count: u32,
) -> Result<(String, ServedPair)> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let block = chain_fixture
        .block_at(BlockHeight::new(1))
        .cloned()
        .ok_or_else(|| eyre!("fixture must contain height 1"))?;
    let creator = TransactionId::from_bytes([0x10; 32]);
    for output_index in 0..output_count {
        chain_fixture = chain_fixture.with_address_output_index(TransparentUnspentOutput::new(
            address,
            vec![0x51],
            TransparentOutPoint::new(creator, output_index),
            1_000,
            block.height,
            block.hash,
        ));
    }
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain_fixture, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let page_reads = Arc::new(AtomicUsize::new(0));
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical),
        Arc::new(CountingWalletReader {
            inner: Arc::new(wallet),
            page_reads: Arc::clone(&page_reads),
        }),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(Arc::clone(&pair)),
        (),
        ingest_control,
        activations,
    );
    let grpc_adapter = WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default());
    let server_addr = spawn_wallet_query_server(grpc_adapter, store_fixture).await?;
    Ok((server_addr, ServedPair { pair, page_reads }))
}

async fn wait_until(description: &str, condition: impl Fn() -> bool) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        if tokio::time::Instant::now() > deadline {
            return Err(eyre!("timed out waiting for {description}"));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

/// Delegates every read and counts unspent-output page reads.
struct CountingWalletReader {
    inner: Arc<dyn WalletProjectionReader>,
    page_reads: Arc<AtomicUsize>,
}

impl WalletProjectionReader for CountingWalletReader {
    fn network(&self) -> Network {
        self.inner.network()
    }

    fn ready_evidence(&self) -> &WalletProjectionReadyEvidence {
        self.inner.ready_evidence()
    }

    fn address_unspent_outputs_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError> {
        self.page_reads.fetch_add(1, Ordering::AcqRel);
        self.inner
            .address_unspent_outputs_page(address_script_hash, after, page_size)
    }

    fn address_unspent_outputs_page_from_height(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError> {
        self.page_reads.fetch_add(1, Ordering::AcqRel);
        self.inner.address_unspent_outputs_page_from_height(
            address_script_hash,
            start_height,
            after,
            page_size,
        )
    }

    fn address_transaction_history_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError> {
        self.inner
            .address_transaction_history_page(address_script_hash, after, page_size)
    }

    fn address_transaction_history_range_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        height_range: BlockHeightRange,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError> {
        self.inner.address_transaction_history_range_page(
            address_script_hash,
            height_range,
            after,
            page_size,
        )
    }

    fn address_balance(
        &self,
        address_script_hash: TransparentAddressScriptHash,
    ) -> Result<u64, RocksDbWalletError> {
        self.inner.address_balance(address_script_hash)
    }

    fn find_unspent_output(
        &self,
        outpoint: TransparentOutPoint,
    ) -> Result<Option<WalletUnspentOutput>, RocksDbWalletError> {
        self.inner.find_unspent_output(outpoint)
    }

    fn find_spent_output(
        &self,
        outpoint: TransparentOutPoint,
    ) -> Result<Option<WalletSpentOutput>, RocksDbWalletError> {
        self.inner.find_spent_output(outpoint)
    }
}

type ServingAdapter =
    WalletQueryGrpcAdapter<WalletServingQuery<(), zinder_query::AdmittedIngestControl>>;

async fn commit_store_and_spawn_grpc() -> Result<String> {
    let (grpc_adapter, store_fixture) = commit_store_and_build_adapter().await?;
    spawn_wallet_query_server(grpc_adapter, store_fixture).await
}

async fn commit_store_and_build_adapter() -> Result<(ServingAdapter, WalletServingStoreFixture)> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(COMMITTED_BLOCK_COUNT);
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain_fixture, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical),
        Arc::new(wallet),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(serving_pair),
        (),
        ingest_control,
        activations,
    );
    let grpc_adapter = WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default());
    Ok((grpc_adapter, store_fixture))
}

async fn spawn_wallet_query_server(
    grpc_adapter: ServingAdapter,
    store_fixture: WalletServingStoreFixture,
) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen_addr = listener.local_addr()?;
    let _server_task = tokio::spawn(async move {
        let server_result = Server::builder()
            .add_service(grpc_adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
        drop(store_fixture);
        server_result
    });
    Ok(format!("http://{listen_addr}"))
}
