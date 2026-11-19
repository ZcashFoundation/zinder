#![allow(
    missing_docs,
    reason = "Integration test names describe the gRPC health contract under test."
)]

use std::sync::Arc;

use eyre::{Result, eyre};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Streaming, service::interceptor::InterceptedService, transport::Channel};
use tonic_health::pb::{
    HealthCheckRequest, HealthCheckResponse, health_check_response::ServingStatus,
    health_client::HealthClient,
};
use zinder_runtime::{Readiness, ReadinessState, TrafficReadinessInterceptor};

const WAIT_LIMIT: Duration = Duration::from_secs(5);
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::Network;
use zinder_query::{WalletServingPairSlot, WalletServingQuery, WalletServingReadPair};
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

#[tokio::test]
async fn lightwalletd_listener_serves_health_that_follows_readiness_and_shutdown() -> Result<()> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(1),
        &activations,
    )?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let wallet_query =
        WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(
            WalletServingReadPair::new(Arc::new(canonical), Arc::new(wallet))?,
        )));
    let readiness = Readiness::default();
    let shutdown = CancellationToken::new();
    let streamer_server = LightwalletdGrpcAdapter::new(wallet_query).into_server();
    let health =
        zinder_runtime::public_grpc_health(readiness.clone(), shutdown.clone(), &streamer_server);
    let gated = InterceptedService::new(
        streamer_server,
        TrafficReadinessInterceptor::new(readiness.clone()),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let server_shutdown = shutdown.clone();
    tokio::spawn(
        zinder_runtime::public_grpc_server()
            .add_service(gated)
            .add_service(health)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                server_shutdown.cancelled_owned(),
            ),
    );
    let channel = tonic::transport::Endpoint::new(format!("http://{server_addr}"))?
        .connect()
        .await?;

    let mut client = HealthClient::new(channel);
    let mut watch = client
        .watch(Request::new(HealthCheckRequest::default()))
        .await?
        .into_inner();

    assert_eq!(check(&mut client).await?, ServingStatus::NotServing);
    assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);

    readiness.set(ReadinessState::ready(Some(1)));
    assert_eq!(check(&mut client).await?, ServingStatus::Serving);
    assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);

    readiness.set(ReadinessState::syncing(Some(3), Some(1), Some(4)));
    assert_eq!(check(&mut client).await?, ServingStatus::NotServing);
    assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);

    readiness.set(ReadinessState::ready(Some(1)));
    assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);

    shutdown.cancel();
    assert!(readiness.report().is_ready);
    assert_eq!(check(&mut client).await?, ServingStatus::NotServing);
    assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);
    let end = tokio::time::timeout(WAIT_LIMIT, watch.next())
        .await
        .map_err(|_| eyre!("health stream stayed open after shutdown"))?;
    assert!(end.is_none(), "health stream stayed open: {end:?}");
    Ok(())
}

async fn next_status(stream: &mut Streaming<HealthCheckResponse>) -> Result<ServingStatus> {
    let message = tokio::time::timeout(WAIT_LIMIT, stream.next())
        .await
        .map_err(|_| eyre!("no health message within {WAIT_LIMIT:?}"))?
        .ok_or_else(|| eyre!("health stream ended"))??;
    Ok(ServingStatus::try_from(message.status).unwrap_or(ServingStatus::Unknown))
}

async fn check(client: &mut HealthClient<Channel>) -> Result<ServingStatus> {
    let response = client
        .check(Request::new(HealthCheckRequest::default()))
        .await?
        .into_inner();
    Ok(ServingStatus::try_from(response.status).unwrap_or(ServingStatus::Unknown))
}
