#![allow(
    missing_docs,
    reason = "Integration test names describe the gRPC health contract under test."
)]

use std::{net::SocketAddr, sync::Arc};

use eyre::Result;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic_health::pb::health_check_response::ServingStatus;
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::Network;
use zinder_proto::compat::lightwalletd::{
    ChainSpec, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::{
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair, WalletServingReadiness,
};
use zinder_runtime::{
    OpsEndpointHandle, OpsServer, Readiness, ReadinessState, RuntimeService, SupervisedRuntime,
    SupervisedTasks,
};
use zinder_testkit::{
    ChainFixture, PublicListenerProbe, WalletServingStoreFixture, next_watch_status,
    sample_regtest_upgrade_activations, watch_ends,
};

const STREAMER_SERVICE: &str = "cash.z.wallet.sdk.rpc.CompactTxStreamer";
const HEALTH_SERVICE: &str = "grpc.health.v1.Health";

/// The wallet-mode listener's health follows `/readyz`.
///
/// The listener is assembled and supervised as the runtime assembles and
/// supervises it. The test publishes the shutdown itself first, so it can
/// check health while the listener still answers.
#[tokio::test]
async fn wallet_mode_listener_health_follows_readyz_through_readiness_and_shutdown() -> Result<()> {
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
    let serving_readiness = WalletServingReadiness::pair_only(readiness.clone());
    let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd);
    let router = runtime.public_grpc_router(
        LightwalletdGrpcAdapter::new(wallet_query).into_server(),
        zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET,
        &readiness,
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listener_address = listener.local_addr()?;
    let server = router.serve_with_incoming_shutdown(
        TcpListenerStream::new(listener),
        runtime.grpc_server_shutdown(),
    );
    let shutdown = runtime.shutdown_request();
    let supervised = tokio::spawn({
        let serving_readiness = serving_readiness.clone();
        runtime.supervise(server, SupervisedTasks::new(), move || {
            serving_readiness.publish_shutting_down();
        })
    });
    let (ops_address, _readyz) = spawn_readyz(&readiness).await?;
    let probe = PublicListenerProbe::connect(listener_address, ops_address).await?;
    let mut streamer = CompactTxStreamerClient::new(probe.channel());
    let mut watch = probe.watch("").await?;

    assert_eq!(probe.agreed_status().await?, ServingStatus::NotServing);
    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::NotServing
    );
    let refused = streamer.get_latest_block(ChainSpec {}).await;
    assert_eq!(
        refused.err().map(|status| status.code()),
        Some(Code::Unavailable)
    );
    let reflected = probe.reflected_services().await?;
    assert!(
        reflected.iter().any(|name| name == STREAMER_SERVICE)
            && reflected.iter().any(|name| name == HEALTH_SERVICE),
        "reflection did not describe the streamer and health services: {reflected:?}"
    );

    readiness.set(ReadinessState::ready(Some(1)));
    assert_eq!(probe.agreed_status().await?, ServingStatus::Serving);
    assert_eq!(probe.check(STREAMER_SERVICE).await?, ServingStatus::Serving);
    assert_eq!(next_watch_status(&mut watch).await?, ServingStatus::Serving);
    streamer.get_latest_block(ChainSpec {}).await?;

    readiness.set(ReadinessState::syncing(Some(1), Some(1), Some(4)));
    assert_eq!(probe.agreed_status().await?, ServingStatus::NotServing);
    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::NotServing
    );
    readiness.set(ReadinessState::ready(Some(1)));
    assert_eq!(next_watch_status(&mut watch).await?, ServingStatus::Serving);

    serving_readiness.publish_shutting_down();
    assert_eq!(probe.agreed_status().await?, ServingStatus::NotServing);
    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::NotServing
    );

    shutdown.request();
    watch_ends(&mut watch).await?;
    supervised.await??;
    Ok(())
}

async fn spawn_readyz(readiness: &Readiness) -> Result<(SocketAddr, OpsEndpointHandle)> {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
    let ops_address = reservation.local_addr()?;
    drop(reservation);
    let handle = zinder_runtime::spawn_ops_endpoint(
        ops_address,
        OpsServer {
            service_name: "zinder-compat-lightwalletd",
            service_version: "0.0.0",
            network_name: "zcash-regtest",
            advertised_capabilities: Arc::from([]),
        },
        readiness.clone(),
    )
    .await?;
    Ok((ops_address, handle))
}
