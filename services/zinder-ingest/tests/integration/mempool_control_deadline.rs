//! A reachable writer returns its own storage refusal through native wallet query.

use std::{error::Error, sync::Arc, time::Duration};

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Status, transport::Server};
use tonic_types::StatusExt as _;
use zinder_core::Network;
use zinder_ingest::{
    CanonicalIngestControlGrpcAdapter, LiveMempoolOwner, canonical_control_channel,
};
use zinder_proto::v1::{
    ops::ErrorReason,
    wallet::{self, wallet_query_server::WalletQuery as _},
};
use zinder_query::{
    AdmittedIngestControl, RocksDbCanonicalReader, RocksDbWalletReader,
    WALLET_INGEST_CONTROL_REQUEST_TIMEOUT, WalletEndpointMetadata, WalletQueryGrpcAdapter,
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_runtime::{ComposedReadiness, Readiness};
use zinder_store::{EventStreamStartPosition, event_stream_start_message};
use zinder_testkit::WalletServingStoreFixture;

use super::canonical_follow::follower_fixture_at_its_node_tip;

#[tokio::test]
async fn stalled_owner_returns_storage_unavailable_through_native_wallet_query()
-> Result<(), Box<dyn Error>> {
    let (source, activations, store, temporary) = follower_fixture_at_its_node_tip().await?;
    let path = temporary.path().join("canonical");
    let mut serving =
        WalletServingStoreFixture::from_published_canonical(temporary, &path, store, &activations)?;
    let (canonical, wallet) = serving.take_readers()?;
    let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?));
    let (handle, mut commands) = canonical_control_channel();
    let network = Network::ZcashRegtest;
    let ingest = CanonicalIngestControlGrpcAdapter::new(
        network,
        handle,
        LiveMempoolOwner::new(),
        Arc::new(source),
        ComposedReadiness::new(Readiness::default(), &[]),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(
        Server::builder()
            .add_service(ingest.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let admitted = AdmittedIngestControl::connect(&address, None, network).await?;
    let query = WalletQueryGrpcAdapter::new(
        WalletServingQuery::from_admitted_native_serving_pair(slot, admitted),
        WalletEndpointMetadata::default(),
    );
    let stall = async {
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(
                commands
                    .recv()
                    .await
                    .ok_or("writer command channel closed")?,
            );
        }
        tokio::time::pause();
        tokio::time::advance(
            WALLET_INGEST_CONTROL_REQUEST_TIMEOUT.saturating_sub(Duration::from_millis(500)),
        )
        .await;
        tokio::time::resume();
        Ok::<_, Box<dyn Error>>(held)
    };
    let start = tokio::time::Instant::now();
    let (held, snapshot, transaction, events) = tokio::join!(
        stall,
        query.mempool_snapshot(Request::new(wallet::MempoolSnapshotRequest::default())),
        query.transaction(Request::new(wallet::TransactionRequest {
            transaction_id: "ff".repeat(32),
            at_epoch_id: None
        })),
        query.mempool_events(Request::new(wallet::MempoolEventsRequest {
            start: Some(event_stream_start_message(
                &EventStreamStartPosition::LiveTail
            ))
        })),
    );
    let _held = held?;
    for refusal in [snapshot.err(), transaction.err(), events.err()] {
        let status: Status = refusal.ok_or("stalled writer returned a wallet response")?;
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(
            status
                .get_error_details()
                .error_info()
                .map(|detail| detail.reason.as_str()),
            Some(ErrorReason::StorageUnavailable.as_str_name())
        );
    }
    assert!(start.elapsed() < WALLET_INGEST_CONTROL_REQUEST_TIMEOUT);
    server.abort();
    Ok(())
}
