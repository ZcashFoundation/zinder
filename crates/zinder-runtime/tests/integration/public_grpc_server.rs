//! Attacks on a public gRPC listener, run against a real server on loopback.

use std::{
    future::Future,
    net::SocketAddr,
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use bytes::Bytes;
use eyre::{Result, bail, eyre};
use http::uri::PathAndQuery;
use http_body::Frame;
use http_body_util::{BodyExt as _, StreamBody};
use prost::Message as _;
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_stream::{
    Stream, StreamExt as _,
    wrappers::{ReceiverStream, TcpListenerStream},
};
use tonic::{
    Code, Request, Status,
    body::Body,
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
use tower::ServiceExt as _;
use zinder_runtime::{
    GrpcReceiveLimits, MAX_DECODING_MESSAGE_BYTES, public_grpc_reflection, public_grpc_server_with,
};

const HEALTH_CHECK_PATH: &str = "/grpc.health.v1.Health/Check";
const REFLECTION_INFO_PATH: &str = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo";
/// Bound on each wait in these tests; every expected outcome arrives well within it.
const WAIT_LIMIT: Duration = Duration::from_secs(5);
const SHORT_RECEIVE_TIMEOUT: Duration = Duration::from_millis(300);
/// Time for a request started on loopback to reach the server and claim or
/// be refused a slot.
const CLAIM_SETTLE: Duration = Duration::from_millis(300);

struct TestServer {
    address: SocketAddr,
    channel: Channel,
    health: HealthReporter,
    _server: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl TestServer {
    /// Opens another HTTP/2 connection to the server.
    async fn connect(&self) -> Result<Channel> {
        Ok(Endpoint::new(format!("http://{}", self.address))?
            .connect()
            .await?)
    }
}

/// Serves gRPC health (unary `Check`, server-streaming `Watch`) and
/// reflection (bidirectional) behind the public server builder, the way
/// public Zinder listeners do.
async fn serve(slots: usize, receive_timeout: Duration) -> Result<TestServer> {
    let slots = NonZeroUsize::new(slots).ok_or_else(|| eyre!("slots must be non-zero"))?;
    let (health, health_server) = tonic_health::server::health_reporter();
    let reflection = public_grpc_reflection(zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET)?;
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
        address,
        channel,
        health,
        _server: server,
    })
}

/// Starts `count` `Check` calls on `channel` whose bodies never arrive, and
/// waits until each has claimed a slot or been refused one.
async fn stall_checks(channel: &Channel, count: usize) -> Vec<JoinHandle<Result<(), Status>>> {
    let stalled = (0..count)
        .map(|_| tokio::spawn(check_with_body(channel.clone(), tokio_stream::pending())))
        .collect();
    tokio::time::sleep(CLAIM_SETTLE).await;
    stalled
}

async fn outcome(call: JoinHandle<Result<(), Status>>) -> Result<Option<Code>> {
    let finished = within_wait_limit(call).await??;
    Ok(finished.err().map(|status| status.code()))
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

#[tokio::test]
async fn one_connection_cannot_lock_other_connections_out() -> Result<()> {
    let server = serve(4, Duration::from_secs(30)).await?;
    let attacker = server.connect().await?;
    let victim = server.connect().await?;

    let mut stalled = stall_checks(&attacker, 4).await;
    if stalled.iter().any(JoinHandle::is_finished) {
        bail!("a stalled call ended before holding a slot");
    }
    // The attacker holds every slot, so its own next request is refused.
    assert_eq!(
        check(&attacker).await.err().map(|status| status.code()),
        Some(Code::Unavailable)
    );

    // Another connection takes the attacker's oldest slot instead.
    check(&victim).await?;
    let oldest = stalled.remove(0);
    assert_eq!(outcome(oldest).await?, Some(Code::Unavailable));
    if stalled.iter().any(JoinHandle::is_finished) {
        bail!("more than the oldest stalled call lost its slot");
    }
    Ok(())
}

#[tokio::test]
async fn connections_holding_equal_shares_are_refused_across_the_process() -> Result<()> {
    let server = serve(2, Duration::from_secs(30)).await?;
    let first = server.connect().await?;
    let second = server.connect().await?;

    let first_stalled = stall_checks(&first, 1).await;
    let second_stalled = stall_checks(&second, 1).await;
    if first_stalled
        .iter()
        .chain(&second_stalled)
        .any(JoinHandle::is_finished)
    {
        bail!("a stalled call ended before holding a slot");
    }

    // The pool is shared by every connection: each holds one of the two
    // slots, so neither may take the other's.
    for channel in [&first, &second] {
        assert_eq!(
            check(channel).await.err().map(|status| status.code()),
            Some(Code::Unavailable)
        );
    }
    if first_stalled
        .iter()
        .chain(&second_stalled)
        .any(JoinHandle::is_finished)
    {
        bail!("a refused request evicted a stalled call");
    }
    Ok(())
}

#[tokio::test]
async fn a_client_stream_that_drips_bytes_without_a_whole_message_is_cancelled() -> Result<()> {
    let server = serve(1, SHORT_RECEIVE_TIMEOUT).await?;
    let (sender, receiver) = mpsc::channel::<Result<Frame<Bytes>, Status>>(1);
    let request = http::Request::post(REFLECTION_INFO_PATH)
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header(http::header::TE, "trailers")
        .body(Body::new(StreamBody::new(ReceiverStream::new(receiver))))?;
    let response = server.channel.clone().oneshot(request).await?;

    // A message prefix announcing 100 bytes, then one byte at a time, each
    // well inside the receive timeout. No message ever completes.
    let drip = tokio::spawn(async move {
        let prefix = Bytes::from_static(&[0, 0, 0, 0, 100]);
        let mut chunk = prefix;
        while sender.send(Ok(Frame::data(chunk))).await.is_ok() {
            tokio::time::sleep(SHORT_RECEIVE_TIMEOUT / 4).await;
            chunk = Bytes::from_static(b"x");
        }
    });
    let ended = within_wait_limit(response.into_body().collect()).await;
    drip.abort();
    ended?.map_err(|status| eyre!("reflection response failed: {status}"))?;

    let mut next = ServerReflectionClient::new(server.channel.clone())
        .server_reflection_info(tokio_stream::once(list_services()))
        .await?
        .into_inner();
    within_wait_limit(next.next())
        .await?
        .ok_or_else(|| eyre!("reflection ended before answering"))??;
    Ok(())
}

#[tokio::test]
async fn a_reflection_message_above_the_decoding_limit_is_not_decoded() -> Result<()> {
    let server = serve(1, Duration::from_secs(30)).await?;
    let just_over_two_mib = ServerReflectionRequest {
        host: "x".repeat(2 * 1024 * 1024),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    if just_over_two_mib.encoded_len() <= MAX_DECODING_MESSAGE_BYTES {
        bail!("the request must exceed the decoding limit");
    }
    let mut oversized = ServerReflectionClient::new(server.channel.clone())
        .max_decoding_message_size(usize::MAX)
        .server_reflection_info(tokio_stream::once(just_over_two_mib))
        .await?
        .into_inner();
    // Reflection ends its response stream without answering when its request
    // stream fails, here with OUT_OF_RANGE from the decoder.
    let reply = within_wait_limit(oversized.next()).await?;
    assert!(reply.is_none(), "reflection answered an oversized message");

    let mut next = ServerReflectionClient::new(server.channel.clone())
        .server_reflection_info(tokio_stream::once(list_services()))
        .await?
        .into_inner();
    within_wait_limit(next.next())
        .await?
        .ok_or_else(|| eyre!("reflection ended before answering"))??;
    Ok(())
}
