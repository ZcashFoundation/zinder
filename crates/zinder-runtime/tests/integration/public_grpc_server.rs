//! Attacks on a public gRPC listener, run against a real server on loopback.

use std::{
    future::Future,
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use eyre::{Result, bail, eyre};
use http::uri::PathAndQuery;
use prost::Message as _;
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_stream::{
    Stream, StreamExt as _,
    wrappers::{ReceiverStream, TcpListenerStream},
};
use tonic::{
    Code, Request, Status,
    transport::{Channel, Endpoint},
};
use tonic_health::{
    ServingStatus,
    pb::{
        HealthCheckRequest, HealthCheckResponse,
        health_check_response::ServingStatus as WireServingStatus, health_client::HealthClient,
    },
    server::HealthReporter,
};
use tonic_prost::ProstCodec;
use tonic_reflection::pb::v1::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest,
};
use zinder_runtime::{GrpcReceiveLimits, MAX_DECODING_MESSAGE_BYTES, public_grpc_server_with};

const HEALTH_CHECK_PATH: &str = "/grpc.health.v1.Health/Check";
/// Bound on each wait in these tests; every expected outcome arrives well within it.
const WAIT_LIMIT: Duration = Duration::from_secs(5);
const SHORT_RECEIVE_TIMEOUT: Duration = Duration::from_millis(300);

struct TestServer {
    channel: Channel,
    health: HealthReporter,
    _server: JoinHandle<Result<(), tonic::transport::Error>>,
}

/// Serves gRPC health (unary `Check`, server-streaming `Watch`) and
/// reflection (bidirectional) behind the public server builder.
async fn serve(slots: usize, receive_timeout: Duration) -> Result<TestServer> {
    let slots = NonZeroUsize::new(slots).ok_or_else(|| eyre!("slots must be non-zero"))?;
    let (health, health_server) = tonic_health::server::health_reporter();
    let reflection = tonic_reflection::server::Builder::configure().build_v1()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(
        public_grpc_server_with(GrpcReceiveLimits::new(slots, receive_timeout))
            .add_service(health_server.max_decoding_message_size(MAX_DECODING_MESSAGE_BYTES))
            .add_service(reflection)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let channel = Endpoint::new(format!("http://{address}"))?
        .connect()
        .await?;
    Ok(TestServer {
        channel,
        health,
        _server: server,
    })
}

/// Sends a unary `Check` whose request body is `request_stream`, which can
/// stall before or after its message.
async fn check_with_body<S>(channel: Channel, request_stream: S) -> Result<(), Status>
where
    S: Stream<Item = HealthCheckRequest> + Send + 'static,
{
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready()
        .await
        .map_err(|error| Status::unknown(error.to_string()))?;
    grpc.client_streaming::<_, _, HealthCheckResponse, _>(
        Request::new(request_stream),
        PathAndQuery::from_static(HEALTH_CHECK_PATH),
        ProstCodec::<HealthCheckRequest, HealthCheckResponse>::default(),
    )
    .await
    .map(|_| ())
}

async fn check(channel: &Channel) -> Result<(), Status> {
    HealthClient::new(channel.clone())
        .check(HealthCheckRequest::default())
        .await
        .map(|_| ())
}

/// Opens a `Check` whose body never arrives and waits until it holds the
/// only request slot, which shows as the next `Check` being refused.
async fn hold_the_request_slot(server: &TestServer) -> Result<JoinHandle<Result<(), Status>>> {
    let stalled = tokio::spawn(check_with_body(
        server.channel.clone(),
        tokio_stream::pending(),
    ));
    let channel = server.channel.clone();
    wait_until(
        "a check is refused while the stalled call holds the slot",
        || {
            let channel = channel.clone();
            async move {
                check(&channel).await.err().map(|status| status.code()) == Some(Code::Unavailable)
            }
        },
    )
    .await?;
    if stalled.is_finished() {
        bail!("the stalled call ended before holding the slot");
    }
    Ok(stalled)
}

async fn wait_until<F, Fut>(what: &str, mut probe: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let started = Instant::now();
    while started.elapsed() < WAIT_LIMIT {
        if probe().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    bail!("timed out waiting until {what}")
}

async fn within_wait_limit<T>(future: impl Future<Output = T>) -> Result<T> {
    tokio::time::timeout(WAIT_LIMIT, future)
        .await
        .map_err(|_| eyre!("no outcome within {WAIT_LIMIT:?}"))
}

fn list_services() -> ServerReflectionRequest {
    ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    }
}

#[tokio::test]
async fn requests_past_the_receive_slots_are_refused_until_one_returns() -> Result<()> {
    let server = serve(1, Duration::from_secs(30)).await?;
    let stalled = hold_the_request_slot(&server).await?;

    stalled.abort();
    let channel = server.channel.clone();
    wait_until("the cancelled call returns its slot", || {
        let channel = channel.clone();
        async move { check(&channel).await.is_ok() }
    })
    .await
}

#[tokio::test]
async fn a_request_that_does_not_arrive_in_time_is_cancelled() -> Result<()> {
    let server = serve(1, SHORT_RECEIVE_TIMEOUT).await?;

    let nothing_sent = within_wait_limit(check_with_body(
        server.channel.clone(),
        tokio_stream::pending(),
    ))
    .await?;
    assert_eq!(
        nothing_sent.err().map(|status| status.code()),
        Some(Code::DeadlineExceeded)
    );

    // A message followed by a body that never ends also holds the slot.
    let never_ends =
        tokio_stream::once(HealthCheckRequest::default()).chain(tokio_stream::pending());
    let message_without_end =
        within_wait_limit(check_with_body(server.channel.clone(), never_ends)).await?;
    assert_eq!(
        message_without_end.err().map(|status| status.code()),
        Some(Code::DeadlineExceeded)
    );

    check(&server.channel).await?;
    Ok(())
}

#[tokio::test]
async fn a_server_streaming_response_holds_no_receive_slot() -> Result<()> {
    let server = serve(1, SHORT_RECEIVE_TIMEOUT).await?;
    let mut watch = HealthClient::new(server.channel.clone())
        .watch(HealthCheckRequest::default())
        .await?
        .into_inner();
    let first = within_wait_limit(watch.next())
        .await?
        .ok_or_else(|| eyre!("watch ended before its first status"))??;
    assert_eq!(first.status(), WireServingStatus::Serving);

    // Outlive the receive timeout, then show the slot is free and the
    // response stream still delivers.
    tokio::time::sleep(SHORT_RECEIVE_TIMEOUT * 2).await;
    check(&server.channel).await?;
    check(&server.channel).await?;
    server
        .health
        .set_service_status("", ServingStatus::NotServing)
        .await;
    let update = within_wait_limit(watch.next())
        .await?
        .ok_or_else(|| eyre!("watch ended instead of reporting the change"))??;
    assert_eq!(update.status(), WireServingStatus::NotServing);
    Ok(())
}

#[tokio::test]
async fn a_reflection_stream_holds_a_client_stream_slot_until_it_half_closes() -> Result<()> {
    let server = serve(1, Duration::from_secs(30)).await?;
    let (sender, receiver) = mpsc::channel(1);
    let mut first = ServerReflectionClient::new(server.channel.clone())
        .server_reflection_info(ReceiverStream::new(receiver))
        .await?
        .into_inner();
    sender.send(list_services()).await?;
    within_wait_limit(first.next())
        .await?
        .ok_or_else(|| eyre!("reflection ended before answering"))??;

    let second = ServerReflectionClient::new(server.channel.clone())
        .server_reflection_info(tokio_stream::once(list_services()))
        .await;
    assert_eq!(
        second.err().map(|status| status.code()),
        Some(Code::Unavailable)
    );
    // Client streams draw on their own pool, so unary calls still go through.
    check(&server.channel).await?;

    drop(sender);
    assert!(within_wait_limit(first.next()).await?.is_none());
    let mut third = ServerReflectionClient::new(server.channel.clone())
        .server_reflection_info(tokio_stream::once(list_services()))
        .await?
        .into_inner();
    within_wait_limit(third.next())
        .await?
        .ok_or_else(|| eyre!("reflection ended before answering"))??;
    Ok(())
}

#[tokio::test]
async fn an_idle_reflection_stream_is_cancelled_and_returns_its_slot() -> Result<()> {
    let server = serve(1, SHORT_RECEIVE_TIMEOUT).await?;
    let (sender, receiver) = mpsc::channel(1);
    let mut idle = ServerReflectionClient::new(server.channel.clone())
        .server_reflection_info(ReceiverStream::new(receiver))
        .await?
        .into_inner();
    sender.send(list_services()).await?;
    within_wait_limit(idle.next())
        .await?
        .ok_or_else(|| eyre!("reflection ended before answering"))??;

    // The client keeps the stream open but sends nothing more.
    while let Some(message) = within_wait_limit(idle.next()).await? {
        message?;
    }
    let mut next = ServerReflectionClient::new(server.channel.clone())
        .server_reflection_info(tokio_stream::once(list_services()))
        .await?
        .into_inner();
    within_wait_limit(next.next())
        .await?
        .ok_or_else(|| eyre!("reflection ended before answering"))??;
    drop(sender);
    Ok(())
}

#[tokio::test]
async fn a_request_message_above_the_decoding_limit_is_refused() -> Result<()> {
    let server = serve(1, Duration::from_secs(30)).await?;
    let just_over_two_mib = HealthCheckRequest {
        service: "x".repeat(2 * 1024 * 1024 + 1),
    };
    let refused = HealthClient::new(server.channel.clone())
        .check(just_over_two_mib)
        .await;
    assert_eq!(
        refused.err().map(|status| status.code()),
        Some(Code::OutOfRange)
    );
    check(&server.channel).await?;
    Ok(())
}

#[test]
fn the_largest_broadcast_requests_fit_the_decoding_limit() {
    let largest_transaction = vec![0xff_u8; zinder_core::MAX_RAW_TRANSACTION_BYTES];
    let lightwalletd_send = zinder_proto::compat::lightwalletd::RawTransaction {
        data: largest_transaction.clone(),
        height: u64::MAX,
    };
    let native_broadcast = zinder_proto::v1::wallet::BroadcastTransactionRequest {
        raw_transaction: largest_transaction,
    };
    assert!(lightwalletd_send.encoded_len() <= MAX_DECODING_MESSAGE_BYTES);
    assert!(native_broadcast.encoded_len() <= MAX_DECODING_MESSAGE_BYTES);
}
