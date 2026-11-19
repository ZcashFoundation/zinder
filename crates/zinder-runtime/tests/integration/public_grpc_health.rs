//! `grpc.health.v1.Health` on a public listener, run against a real server on
//! loopback with the readiness gate the wallet services use.

use std::{future::Future, time::Duration};

use eyre::{Result, bail, eyre};
use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{
    Code, Request, Streaming,
    service::interceptor::InterceptedService,
    transport::{Channel, Endpoint},
};
use tonic_health::pb::{
    HealthCheckRequest, HealthCheckResponse, health_check_response::ServingStatus,
    health_client::HealthClient,
};
use tonic_reflection::pb::v1::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest,
};
use zinder_runtime::{
    Readiness, ReadinessState, TrafficReadinessInterceptor, public_grpc_health,
    public_grpc_reflection, public_grpc_server,
};

/// Bound on each wait in these tests; every expected outcome arrives well within it.
const WAIT_LIMIT: Duration = Duration::from_secs(5);
/// Name of the service the test listener gates behind readiness.
const GATED_SERVICE: &str = "grpc.reflection.v1.ServerReflection";

struct TestListener {
    readiness: Readiness,
    shutdown: CancellationToken,
    channel: Channel,
}

impl TestListener {
    fn health(&self) -> HealthClient<Channel> {
        HealthClient::new(self.channel.clone())
    }

    async fn check(&self, service: &str) -> Result<ServingStatus, tonic::Status> {
        let response = self
            .health()
            .check(Request::new(request_for(service)))
            .await?
            .into_inner();
        Ok(status_of(response))
    }

    async fn watch(&self, service: &str) -> Result<Streaming<HealthCheckResponse>> {
        Ok(self
            .health()
            .watch(Request::new(request_for(service)))
            .await?
            .into_inner())
    }

    async fn gated_call_code(&self) -> Option<Code> {
        let request = tokio_stream::once(ServerReflectionRequest {
            host: String::new(),
            message_request: Some(MessageRequest::ListServices(String::new())),
        });
        let mut client = ServerReflectionClient::new(self.channel.clone());
        let call = client.server_reflection_info(Request::new(request)).await;
        match call {
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

/// Serves health beside a readiness-gated service, the way the wallet
/// listeners do, with the shutdown token that also stops the server.
async fn serve(initial: ReadinessState) -> Result<TestListener> {
    let readiness = Readiness::new(initial);
    let shutdown = CancellationToken::new();
    let reflection = public_grpc_reflection(zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET)?;
    let health = public_grpc_health(readiness.clone(), shutdown.clone(), &reflection);
    let gated = InterceptedService::new(
        reflection,
        TrafficReadinessInterceptor::new(readiness.clone()),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server_shutdown = shutdown.clone();
    tokio::spawn(
        public_grpc_server()
            .add_service(gated)
            .add_service(health)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                server_shutdown.cancelled_owned(),
            ),
    );
    let channel = Endpoint::new(format!("http://{address}"))?
        .connect()
        .await?;
    Ok(TestListener {
        readiness,
        shutdown,
        channel,
    })
}

fn request_for(service: &str) -> HealthCheckRequest {
    HealthCheckRequest {
        service: service.to_owned(),
    }
}

fn status_of(response: HealthCheckResponse) -> ServingStatus {
    ServingStatus::try_from(response.status).unwrap_or(ServingStatus::Unknown)
}

async fn within_wait_limit<T>(future: impl Future<Output = T>) -> Result<T> {
    tokio::time::timeout(WAIT_LIMIT, future)
        .await
        .map_err(|_| eyre!("no outcome within {WAIT_LIMIT:?}"))
}

async fn next_status(stream: &mut Streaming<HealthCheckResponse>) -> Result<ServingStatus> {
    let message = within_wait_limit(stream.next())
        .await?
        .ok_or_else(|| eyre!("health stream ended"))??;
    Ok(status_of(message))
}

#[tokio::test]
async fn check_follows_readiness_through_ready_and_unready() -> Result<()> {
    let listener = serve(ReadinessState::starting()).await?;
    assert_eq!(listener.check("").await?, ServingStatus::NotServing);

    listener.readiness.set(ReadinessState::ready(Some(10)));
    assert_eq!(listener.check("").await?, ServingStatus::Serving);

    listener
        .readiness
        .set(ReadinessState::syncing(Some(5), Some(10), Some(15)));
    assert_eq!(listener.check("").await?, ServingStatus::NotServing);
    Ok(())
}

#[tokio::test]
async fn check_reports_serving_for_warning_causes_that_permit_traffic() -> Result<()> {
    let listener = serve(ReadinessState::serving_pair_stale(2, 600, Some(10))).await?;
    assert!(listener.readiness.report().is_ready);
    assert_eq!(listener.check("").await?, ServingStatus::Serving);

    listener
        .readiness
        .set(ReadinessState::cursor_at_risk(20, 24, Some(10)));
    assert!(listener.readiness.report().is_ready);
    assert_eq!(listener.check("").await?, ServingStatus::Serving);
    Ok(())
}

#[tokio::test]
async fn watch_streams_each_readiness_change() -> Result<()> {
    let listener = serve(ReadinessState::starting()).await?;
    let mut watch = listener.watch("").await?;
    assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);

    listener.readiness.set(ReadinessState::ready(Some(10)));
    assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);

    listener
        .readiness
        .set(ReadinessState::syncing(Some(5), Some(10), Some(15)));
    assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);
    Ok(())
}

#[tokio::test]
async fn health_answers_while_readiness_refuses_the_gated_service() -> Result<()> {
    let listener = serve(ReadinessState::starting()).await?;
    assert_eq!(listener.gated_call_code().await, Some(Code::Unavailable));
    assert_eq!(listener.check("").await?, ServingStatus::NotServing);

    listener.readiness.set(ReadinessState::ready(Some(10)));
    assert_eq!(listener.gated_call_code().await, None);
    Ok(())
}

#[tokio::test]
async fn shutdown_publishes_not_serving_while_readiness_is_still_ready() -> Result<()> {
    let listener = serve(ReadinessState::ready(Some(10))).await?;
    let mut watch = listener.watch("").await?;
    assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);
    assert_eq!(listener.check("").await?, ServingStatus::Serving);

    listener.shutdown.cancel();

    assert!(listener.readiness.report().is_ready);
    assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);
    let end = within_wait_limit(watch.next()).await?;
    if end.is_some() {
        bail!("health stream stayed open after shutdown: {end:?}");
    }
    Ok(())
}

#[tokio::test]
async fn named_service_reports_the_listener_status_and_unknown_names_are_not_found() -> Result<()> {
    let listener = serve(ReadinessState::ready(Some(10))).await?;
    assert_eq!(listener.check(GATED_SERVICE).await?, ServingStatus::Serving);

    let unknown = listener.check("not.a.Service").await;
    assert_eq!(
        unknown.err().map(|status| status.code()),
        Some(Code::NotFound)
    );

    let mut watch = listener.watch("not.a.Service").await?;
    assert_eq!(
        next_status(&mut watch).await?,
        ServingStatus::ServiceUnknown
    );
    Ok(())
}
