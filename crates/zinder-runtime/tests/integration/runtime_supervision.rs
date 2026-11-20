//! A supervised runtime's shutdown against a real gRPC server on loopback.

use std::time::Duration;

use eyre::{Result, eyre};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::{
    StreamExt as _,
    wrappers::{ReceiverStream, TcpListenerStream},
};
use tonic::{Code, transport::Endpoint};
use tonic_reflection::pb::v1::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest,
};
use zinder_runtime::{RuntimeService, SupervisedRuntime, SupervisedTasks, public_grpc_reflection};

/// Bound on each wait in these tests; every expected outcome arrives well within it.
const WAIT_LIMIT: Duration = Duration::from_secs(5);
const SHORT_DRAIN_TIMEOUT: Duration = Duration::from_millis(300);

fn list_services() -> ServerReflectionRequest {
    ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    }
}

async fn within_wait_limit<T>(future: impl Future<Output = T>) -> Result<T> {
    tokio::time::timeout(WAIT_LIMIT, future)
        .await
        .map_err(|_| eyre!("no outcome within {WAIT_LIMIT:?}"))
}

/// A call still open at the drain deadline ends before the serving
/// generations are released, and answers nothing afterwards.
///
/// A reflection stream stays open through a graceful drain, so the drain
/// deadline passes with it open.
#[tokio::test]
async fn a_call_open_at_the_drain_deadline_ends_before_serving_generations_are_released()
-> Result<()> {
    let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd)
        .with_grpc_drain_timeout(SHORT_DRAIN_TIMEOUT);
    let serving_drained = runtime.serving_drained();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = runtime
        .public_grpc_server()
        .add_service(public_grpc_reflection(
            zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET,
        )?)
        .serve_with_incoming_shutdown(
            TcpListenerStream::new(listener),
            runtime.grpc_server_shutdown(),
        );
    let shutdown = runtime.shutdown_request();
    let supervised = tokio::spawn(runtime.supervise(server, SupervisedTasks::new(), || {}));

    let channel = Endpoint::new(format!("http://{address}"))?
        .connect()
        .await?;
    let (sender, receiver) = mpsc::channel(4);
    let mut reflection = ServerReflectionClient::new(channel)
        .server_reflection_info(ReceiverStream::new(receiver))
        .await?
        .into_inner();
    sender.send(list_services()).await?;
    within_wait_limit(reflection.next())
        .await?
        .ok_or_else(|| eyre!("reflection ended before answering"))??;

    shutdown.request();
    within_wait_limit(serving_drained.cancelled()).await?;

    let ended_with = within_wait_limit(reflection.next()).await?;
    assert_eq!(
        ended_with.and_then(Result::err).map(|status| status.code()),
        Some(Code::Unavailable),
        "the open call must end with UNAVAILABLE"
    );
    // The stream has ended, so a request sent now gets no answer.
    let _ = sender.send(list_services()).await;
    assert!(within_wait_limit(reflection.next()).await?.is_none());

    within_wait_limit(supervised).await???;
    Ok(())
}
