//! Stream-cancellation and concurrent-reader smoke for the native
//! `WalletQuery` server.
//!
//! These tests cover two production-blast-radius properties of the gRPC
//! adapter that the in-process trait tests cannot:
//!
//! - A client that disconnects mid-stream must not leave the server in a
//!   broken state. The next request through the same server has to succeed
//!   with no cooldown.
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

use std::sync::Arc;
use std::time::Duration;

use eyre::{Result, eyre};
use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Code, transport::Server};
use tonic_types::StatusExt as _;
use zinder_core::Network;
use zinder_proto::v1::wallet::{self, wallet_query_client::WalletQueryClient};
use zinder_query::{
    WalletEndpointMetadata, WalletQueryGrpcAdapter, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_store::RawBlobRetention;
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

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
