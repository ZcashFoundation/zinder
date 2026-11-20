//! `grpc.health.v1.Health` and reflection on a listener assembled by
//! `public_grpc_router`, run against a real server on loopback beside the
//! `/readyz` endpoint of the same readiness.

use std::{net::SocketAddr, sync::Arc};

use eyre::Result;
use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request};
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_reflection::pb::v1alpha::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest,
};
use zinder_runtime::{
    OpsEndpointHandle, OpsServer, Readiness, ReadinessCause, ReadinessState, public_grpc_router,
    spawn_ops_endpoint,
};
use zinder_testkit::{PublicListenerProbe, next_watch_status, watch_ends};

/// The service the test listener serves behind the traffic gate, standing in
/// for a wallet service.
const GATED_SERVICE: &str = "grpc.reflection.v1alpha.ServerReflection";
const HEALTH_SERVICE: &str = "grpc.health.v1.Health";
const DESCRIBED_WALLET_SERVICE: &str = "zinder.v1.wallet.WalletQuery";

struct TestListener {
    readiness: Readiness,
    drain: CancellationToken,
    probe: PublicListenerProbe,
    _readyz: OpsEndpointHandle,
}

impl TestListener {
    /// Calls the gated service and returns the code it fails with, if any.
    async fn gated_call_code(&self) -> Option<Code> {
        let request = tokio_stream::once(ServerReflectionRequest {
            host: String::new(),
            message_request: Some(MessageRequest::ListServices(String::new())),
        });
        let mut client = ServerReflectionClient::new(self.probe.channel());
        match client.server_reflection_info(Request::new(request)).await {
            Ok(response) => response
                .into_inner()
                .next()
                .await
                .and_then(Result::err)
                .map(|status| status.code()),
            Err(status) => Some(status.code()),
        }
    }
}

/// Serves a gated stand-in wallet service through the production listener
/// assembly, with `/readyz` over the same readiness.
async fn serve(initial: ReadinessState) -> Result<TestListener> {
    let readiness = Readiness::new(initial);
    let drain = CancellationToken::new();
    let gated = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET)
        .build_v1alpha()?;
    let router = public_grpc_router(
        gated,
        zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET,
        &readiness,
        &drain,
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listener_address = listener.local_addr()?;
    tokio::spawn(router.serve_with_incoming_shutdown(
        TcpListenerStream::new(listener),
        drain.clone().cancelled_owned(),
    ));
    let (ops_address, readyz) = spawn_readyz(&readiness).await?;
    let probe = PublicListenerProbe::connect(listener_address, ops_address).await?;
    Ok(TestListener {
        readiness,
        drain,
        probe,
        _readyz: readyz,
    })
}

async fn spawn_readyz(readiness: &Readiness) -> Result<(SocketAddr, OpsEndpointHandle)> {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
    let ops_address = reservation.local_addr()?;
    drop(reservation);
    let handle = spawn_ops_endpoint(
        ops_address,
        OpsServer {
            service_name: "zinder-health-test",
            service_version: "0.0.0",
            network_name: "zcash-regtest",
            advertised_capabilities: Arc::from([]),
        },
        readiness.clone(),
    )
    .await?;
    Ok((ops_address, handle))
}

#[tokio::test]
async fn health_agrees_with_readyz_through_ready_warning_and_unready() -> Result<()> {
    let listener = serve(ReadinessState::starting()).await?;
    assert_eq!(
        listener.probe.agreed_status().await?,
        ServingStatus::NotServing
    );

    listener.readiness.set(ReadinessState::ready(Some(10)));
    assert_eq!(
        listener.probe.agreed_status().await?,
        ServingStatus::Serving
    );

    listener
        .readiness
        .set(ReadinessState::serving_pair_stale(2, 600, Some(10)));
    assert_eq!(
        listener.probe.agreed_status().await?,
        ServingStatus::Serving
    );

    listener
        .readiness
        .set(ReadinessState::cursor_at_risk(20, 24, Some(10)));
    assert_eq!(
        listener.probe.agreed_status().await?,
        ServingStatus::Serving
    );

    listener
        .readiness
        .set(ReadinessState::syncing(Some(5), Some(10), Some(15)));
    assert_eq!(
        listener.probe.agreed_status().await?,
        ServingStatus::NotServing
    );
    Ok(())
}

#[tokio::test]
async fn watch_streams_each_readiness_change() -> Result<()> {
    let listener = serve(ReadinessState::starting()).await?;
    let mut watch = listener.probe.watch("").await?;
    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::NotServing
    );

    listener.readiness.set(ReadinessState::ready(Some(10)));
    assert_eq!(next_watch_status(&mut watch).await?, ServingStatus::Serving);

    listener
        .readiness
        .set(ReadinessState::syncing(Some(5), Some(10), Some(15)));
    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::NotServing
    );
    Ok(())
}

/// A change that reverses at once, faster than any sampling interval, still
/// reaches an open `Watch`.
#[tokio::test]
async fn watch_reports_a_ready_window_that_closes_at_once() -> Result<()> {
    let listener = serve(ReadinessState::starting()).await?;
    let mut watch = listener.probe.watch("").await?;
    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::NotServing
    );

    listener.readiness.set(ReadinessState::ready(Some(10)));
    listener
        .readiness
        .set(ReadinessState::syncing(Some(5), Some(10), Some(15)));

    assert_eq!(next_watch_status(&mut watch).await?, ServingStatus::Serving);
    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::NotServing
    );
    Ok(())
}

#[tokio::test]
async fn reflection_and_health_answer_while_the_wallet_service_is_refused() -> Result<()> {
    let listener = serve(ReadinessState::starting()).await?;
    assert_eq!(listener.gated_call_code().await, Some(Code::Unavailable));

    let reflected = listener.probe.reflected_services().await?;
    assert!(
        reflected
            .iter()
            .any(|name| name == DESCRIBED_WALLET_SERVICE),
        "reflection did not describe {DESCRIBED_WALLET_SERVICE}: {reflected:?}"
    );
    assert!(
        reflected.iter().any(|name| name == HEALTH_SERVICE),
        "reflection did not describe {HEALTH_SERVICE}: {reflected:?}"
    );
    assert_eq!(
        listener.probe.agreed_status().await?,
        ServingStatus::NotServing
    );

    listener.readiness.set(ReadinessState::ready(Some(10)));
    assert_eq!(listener.gated_call_code().await, None);
    Ok(())
}

/// Shutdown publishes `shutting_down` before the drain, so health and
/// `/readyz` turn together while the listener still answers, and the drain
/// then ends open watches.
#[tokio::test]
async fn shutdown_turns_health_with_readyz_before_the_drain_ends_watches() -> Result<()> {
    let listener = serve(ReadinessState::ready(Some(10))).await?;
    let mut watch = listener.probe.watch("").await?;
    assert_eq!(next_watch_status(&mut watch).await?, ServingStatus::Serving);

    listener
        .readiness
        .set(ReadinessState::not_ready(ReadinessCause::ShuttingDown));

    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::NotServing
    );
    assert_eq!(
        listener.probe.agreed_status().await?,
        ServingStatus::NotServing
    );

    listener.drain.cancel();
    watch_ends(&mut watch).await
}

/// A drain that starts while readiness still admits traffic reports
/// `NOT_SERVING` to open watches before ending them.
#[tokio::test]
async fn a_drain_reports_not_serving_before_ending_watches() -> Result<()> {
    let listener = serve(ReadinessState::ready(Some(10))).await?;
    let mut watch = listener.probe.watch("").await?;
    assert_eq!(next_watch_status(&mut watch).await?, ServingStatus::Serving);

    listener.drain.cancel();

    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::NotServing
    );
    watch_ends(&mut watch).await
}

#[tokio::test]
async fn named_service_reports_the_listener_status_and_unknown_names_are_not_found() -> Result<()> {
    let listener = serve(ReadinessState::ready(Some(10))).await?;
    assert_eq!(
        listener.probe.check(GATED_SERVICE).await?,
        ServingStatus::Serving
    );

    let unknown = listener.probe.check("not.a.Service").await;
    assert_eq!(
        unknown.err().map(|status| status.code()),
        Some(Code::NotFound)
    );

    let mut watch = listener.probe.watch("not.a.Service").await?;
    assert_eq!(
        next_watch_status(&mut watch).await?,
        ServingStatus::ServiceUnknown
    );
    Ok(())
}
