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
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message as _;
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_stream::{Stream, StreamExt as _, wrappers::ReceiverStream};
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
    ServerReflectionRequest, ServerReflectionResponse,
    server_reflection_client::ServerReflectionClient, server_reflection_request::MessageRequest,
};
use tower::ServiceExt as _;
use zinder_runtime::{
    ClientAddressSource, DEFAULT_GRPC_RECEIVE_SLOTS, GrpcReceiveLimits, MAX_DECODING_MESSAGE_BYTES,
    PROXY_CLIENT_ADDRESS_HEADER, Readiness, ReadinessState, RuntimeService, SupervisedRuntime,
    public_grpc_reflection, public_grpc_server_with,
};

const HEALTH_CHECK_PATH: &str = "/grpc.health.v1.Health/Check";
const REFLECTION_INFO_PATH: &str = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo";
/// Bound on each wait in these tests; every expected outcome arrives well within it.
const WAIT_LIMIT: Duration = Duration::from_secs(5);
const SHORT_RECEIVE_TIMEOUT: Duration = Duration::from_millis(300);
/// Send stall timeout for every test server. Responses the tests read are
/// never held that long, so it also shows a response waiting on the server
/// is not taken for a stalled client.
const SHORT_SEND_STALL_TIMEOUT: Duration = Duration::from_millis(300);
/// Time for a request started on loopback to reach the server and claim or
/// be refused a slot.
const CLAIM_SETTLE: Duration = Duration::from_millis(300);

struct TestServer {
    address: SocketAddr,
    channel: Channel,
    health: HealthReporter,
    _server: JoinHandle<Result<(), std::io::Error>>,
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
    serve_with(
        slots,
        receive_timeout,
        SHORT_SEND_STALL_TIMEOUT,
        ClientAddressSource::Connection,
    )
    .await
}

async fn serve_with(
    slots: usize,
    receive_timeout: Duration,
    send_stall_timeout: Duration,
    client_address: ClientAddressSource,
) -> Result<TestServer> {
    let slots = NonZeroUsize::new(slots).ok_or_else(|| eyre!("slots must be non-zero"))?;
    let (health, health_server) = tonic_health::server::health_reporter();
    let reflection = public_grpc_reflection(zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(
        public_grpc_server_with(
            GrpcReceiveLimits::new(slots, receive_timeout),
            send_stall_timeout,
            client_address,
        )
        .add_service(health_server.max_decoding_message_size(MAX_DECODING_MESSAGE_BYTES))
        .add_service(reflection)
        .serve_with_incoming(listener),
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
    let ended_with = within_wait_limit(idle.next()).await?;
    assert_eq!(
        ended_with.and_then(Result::err).map(|status| status.code()),
        Some(Code::DeadlineExceeded)
    );
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
async fn a_reflection_stream_whose_client_stops_reading_ends_and_returns_its_slot() -> Result<()> {
    let server = serve(1, Duration::from_secs(30)).await?;
    let channel = small_stream_window_connection(&server).await?;
    let (sender, mut stalled) = unread_reflection_answers(&channel).await?;

    // The stalled stream holds the only client-stream slot until the server
    // ends it. A probe on the same connection cannot evict it.
    wait_until("the stalled reflection stream returns its slot", || {
        let channel = channel.clone();
        async move {
            match ServerReflectionClient::new(channel)
                .server_reflection_info(tokio_stream::once(list_services()))
                .await
            {
                Ok(probe) => probe
                    .into_inner()
                    .next()
                    .await
                    .is_some_and(|reply| reply.is_ok()),
                Err(_) => false,
            }
        }
    })
    .await?;

    // Reading again delivers what reached the client before the server reset
    // the stream, then the reset, which the client sees as CANCELLED.
    let mut answers = 0;
    let ended_with = loop {
        match within_wait_limit(stalled.next()).await? {
            Some(Ok(_)) => answers += 1,
            Some(Err(status)) => break Some(status),
            None => break None,
        }
    };
    assert!(answers < 3, "the stalled stream delivered every answer");
    assert_eq!(
        ended_with.map(|status| status.code()),
        Some(Code::Cancelled)
    );
    drop(sender);
    Ok(())
}

#[tokio::test]
async fn a_stalled_reflection_stream_waiting_for_its_next_request_returns_its_slot() -> Result<()> {
    let server = serve(1, Duration::from_secs(30)).await?;
    let channel = small_stream_window_connection(&server).await?;
    let (sender, receiver) = mpsc::channel(1);
    let _answers = ServerReflectionClient::new(channel.clone())
        .server_reflection_info(ReceiverStream::new(receiver))
        .await?
        .into_inner();
    // One answer larger than the window: reflection hands it on and waits
    // for the next request, which never comes.
    sender
        .send(ServerReflectionRequest {
            host: "x".repeat(256 * 1024),
            message_request: Some(MessageRequest::ListServices(String::new())),
        })
        .await?;

    // Well before the 30-second receive timeout, the reset ends the request
    // stream too, so reflection lets go of the only client-stream slot.
    wait_until("the stalled reflection stream returns its slot", || {
        let channel = channel.clone();
        async move {
            match ServerReflectionClient::new(channel)
                .server_reflection_info(tokio_stream::once(list_services()))
                .await
            {
                Ok(probe) => probe
                    .into_inner()
                    .next()
                    .await
                    .is_some_and(|reply| reply.is_ok()),
                Err(_) => false,
            }
        }
    })
    .await?;
    drop(sender);
    Ok(())
}

#[tokio::test]
async fn an_evicted_reflection_stream_ends_with_unavailable() -> Result<()> {
    let server = serve(1, Duration::from_secs(30)).await?;
    let (sender, receiver) = mpsc::channel(1);
    let mut evicted = ServerReflectionClient::new(server.connect().await?)
        .server_reflection_info(ReceiverStream::new(receiver))
        .await?
        .into_inner();
    sender.send(list_services()).await?;
    within_wait_limit(evicted.next())
        .await?
        .ok_or_else(|| eyre!("reflection ended before answering"))??;

    // A connection holding no slot takes the open stream's.
    let mut newcomer = ServerReflectionClient::new(server.connect().await?)
        .server_reflection_info(tokio_stream::once(list_services()))
        .await?
        .into_inner();
    within_wait_limit(newcomer.next())
        .await?
        .ok_or_else(|| eyre!("reflection ended before answering"))??;

    let ended_with = within_wait_limit(evicted.next()).await?;
    assert_eq!(
        ended_with.and_then(Result::err).map(|status| status.code()),
        Some(Code::Unavailable)
    );
    drop(sender);
    Ok(())
}

#[tokio::test]
async fn an_evicted_reflection_stream_whose_client_stopped_reading_ends_at_once() -> Result<()> {
    let server = serve_with(
        1,
        Duration::from_secs(30),
        Duration::from_hours(1),
        ClientAddressSource::Connection,
    )
    .await?;
    let attacker = small_stream_window_connection(&server).await?;
    let (sender, mut stalled) = unread_reflection_answers(&attacker).await?;
    tokio::time::sleep(CLAIM_SETTLE).await;

    // A connection holding no slot takes the stalled stream's.
    let mut newcomer = ServerReflectionClient::new(server.connect().await?)
        .server_reflection_info(tokio_stream::once(list_services()))
        .await?
        .into_inner();
    within_wait_limit(newcomer.next())
        .await?
        .ok_or_else(|| eyre!("reflection ended before answering"))??;
    tokio::time::sleep(CLAIM_SETTLE).await;

    // Had the evicted handler survived until now, reading again would
    // deliver all three answers. It was dropped while the client was not
    // reading, so the stream ends with UNAVAILABLE after what the transport
    // already held.
    let mut answers = 0;
    let ended_with = loop {
        match within_wait_limit(stalled.next()).await? {
            Some(Ok(_)) => answers += 1,
            Some(Err(status)) => break Some(status),
            None => break None,
        }
    };
    assert!(answers < 3, "the evicted handler kept answering");
    assert_eq!(
        ended_with.map(|status| status.code()),
        Some(Code::Unavailable)
    );
    drop(sender);
    Ok(())
}

/// Opens a connection with a small stream window and a large connection
/// window, so a stream it stops reading cannot take the window of other
/// streams on it.
async fn small_stream_window_connection(server: &TestServer) -> Result<Channel> {
    Ok(Endpoint::new(format!("http://{}", server.address))?
        .initial_stream_window_size(Some(64 * 1024))
        .initial_connection_window_size(Some(16 * 1024 * 1024))
        .connect()
        .await?)
}

/// Opens a reflection stream on `channel` and sends three requests whose
/// answers the client never reads.
///
/// Reflection echoes the host twice in each answer, so three answers
/// overflow the stream window, reflection's one-message channel, and the
/// message its task is sending, which leaves that task blocked.
async fn unread_reflection_answers(
    channel: &Channel,
) -> Result<(
    mpsc::Sender<ServerReflectionRequest>,
    tonic::Streaming<ServerReflectionResponse>,
)> {
    let (sender, receiver) = mpsc::channel(4);
    let answers = ServerReflectionClient::new(channel.clone())
        .server_reflection_info(ReceiverStream::new(receiver))
        .await?
        .into_inner();
    for _ in 0..3 {
        sender
            .send(ServerReflectionRequest {
                host: "x".repeat(256 * 1024),
                message_request: Some(MessageRequest::ListServices(String::new())),
            })
            .await?;
    }
    Ok((sender, answers))
}

const HTTP2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const HEADERS_FRAME: u8 = 0x1;
const SETTINGS_FRAME: u8 = 0x4;
const GOAWAY_FRAME: u8 = 0x7;
const END_STREAM_AND_HEADERS: u8 = 0x1 | 0x4;
const ENHANCE_YOUR_CALM: u32 = 0xb;
/// An HPACK block holding only `:method: POST`. A request without `:scheme`
/// and `:path` is malformed, so the server resets its stream.
const METHOD_ONLY_HEADER_BLOCK: &[u8] = &[0x83];

fn http2_frame(kind: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(payload.len())?.to_be_bytes();
    let mut frame = Vec::with_capacity(9 + payload.len());
    frame.extend_from_slice(length.get(1..).unwrap_or_default());
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream_id.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Reads frames until the server sends `GOAWAY`, and returns its error code.
async fn goaway_error_code(connection: &mut tokio::net::TcpStream) -> Result<u32> {
    use tokio::io::AsyncReadExt as _;

    loop {
        let mut header = [0; 9];
        connection.read_exact(&mut header).await?;
        let [l0, l1, l2, kind, ..] = header;
        let length = u32::from_be_bytes([0, l0, l1, l2]);
        let mut payload = vec![0; usize::try_from(length)?];
        connection.read_exact(&mut payload).await?;
        if kind == GOAWAY_FRAME {
            let code = payload
                .get(4..8)
                .and_then(|code| <[u8; 4]>::try_from(code).ok())
                .ok_or_else(|| eyre!("GOAWAY frame without an error code"))?;
            return Ok(u32::from_be_bytes(code));
        }
    }
}

#[tokio::test]
async fn a_client_whose_protocol_errors_keep_resetting_streams_loses_its_connection() -> Result<()>
{
    use tokio::io::AsyncWriteExt as _;

    let server = serve(1, Duration::from_secs(30)).await?;
    let mut connection = tokio::net::TcpStream::connect(server.address).await?;
    let mut frames = HTTP2_PREFACE.to_vec();
    frames.extend(http2_frame(SETTINGS_FRAME, 0, 0, &[])?);
    let malformed_requests = u32::try_from(zinder_runtime::HTTP2_MAX_LOCAL_ERROR_RESETS)? + 16;
    for request in 0..malformed_requests {
        frames.extend(http2_frame(
            HEADERS_FRAME,
            END_STREAM_AND_HEADERS,
            2 * request + 1,
            METHOD_ONLY_HEADER_BLOCK,
        )?);
    }
    connection.write_all(&frames).await?;

    let code = within_wait_limit(goaway_error_code(&mut connection)).await??;
    assert_eq!(code, ENHANCE_YOUR_CALM);
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

/// Starts a reverse proxy in front of the listener at `upstream` and returns
/// its address.
///
/// Like the reference Caddy configuration, the proxy forwards every client's
/// requests over one shared HTTP/2 connection and names each client
/// connection in [`PROXY_CLIENT_ADDRESS_HEADER`].
async fn spawn_proxy(upstream: SocketAddr) -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let upstream = Endpoint::new(format!("http://{upstream}"))?
        .connect()
        .await?;
    tokio::spawn(async move {
        while let Ok((stream, client)) = listener.accept().await {
            let client_header = http::HeaderValue::try_from(client.to_string());
            let upstream = upstream.clone();
            let forward = hyper::service::service_fn(move |request: http::Request<Incoming>| {
                let mut request = request.map(Body::new);
                if let Ok(client_header) = &client_header {
                    request
                        .headers_mut()
                        .insert(PROXY_CLIENT_ADDRESS_HEADER, client_header.clone());
                }
                upstream.clone().oneshot(request)
            });
            tokio::spawn(
                hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), forward),
            );
        }
    });
    Ok(address)
}

async fn connect_through(proxy: SocketAddr) -> Result<Channel> {
    Ok(Endpoint::new(format!("http://{proxy}"))?.connect().await?)
}

#[tokio::test]
async fn one_client_behind_a_trusted_proxy_cannot_lock_other_clients_out() -> Result<()> {
    let server = serve_with(
        4,
        Duration::from_secs(30),
        SHORT_SEND_STALL_TIMEOUT,
        ClientAddressSource::ProxyHeader,
    )
    .await?;
    let proxy = spawn_proxy(server.address).await?;
    let attacker = connect_through(proxy).await?;
    let victim = connect_through(proxy).await?;

    let mut stalled = stall_checks(&attacker, 4).await;
    if stalled.iter().any(JoinHandle::is_finished) {
        bail!("a stalled call ended before holding a slot");
    }
    // Every request reaches Zinder over the proxy's one connection, yet the
    // attacker alone holds every slot, so only its own next request is
    // refused.
    assert_eq!(
        check(&attacker).await.err().map(|status| status.code()),
        Some(Code::Unavailable)
    );

    check(&victim).await?;
    let oldest = stalled.remove(0);
    assert_eq!(outcome(oldest).await?, Some(Code::Unavailable));
    if stalled.iter().any(JoinHandle::is_finished) {
        bail!("more than the oldest stalled call lost its slot");
    }
    Ok(())
}

#[tokio::test]
async fn a_supervised_wallet_listener_tells_clients_behind_a_trusted_proxy_apart() -> Result<()> {
    let runtime = SupervisedRuntime::new(RuntimeService::Query);
    let readiness = Readiness::new(ReadinessState::ready(Some(1)));
    let stand_in_wallet_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET)
        .build_v1alpha()?;
    let router = runtime.public_grpc_router(
        stand_in_wallet_service,
        zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET,
        ClientAddressSource::ProxyHeader,
        &readiness,
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listener_address = listener.local_addr()?;
    let _server =
        tokio::spawn(router.serve_with_incoming_shutdown(listener, runtime.grpc_server_shutdown()));
    let proxy = spawn_proxy(listener_address).await?;
    let attacker = connect_through(proxy).await?;
    let victim = connect_through(proxy).await?;

    let mut stalled = stall_checks(&attacker, DEFAULT_GRPC_RECEIVE_SLOTS.get()).await;
    wait_until("the attacker holds every request slot", || {
        let attacker = attacker.clone();
        async move {
            check(&attacker).await.err().map(|status| status.code()) == Some(Code::Unavailable)
        }
    })
    .await?;
    if stalled.iter().any(JoinHandle::is_finished) {
        bail!("a stalled call ended before holding a slot");
    }

    check(&victim).await?;
    let oldest = stalled.remove(0);
    assert_eq!(outcome(oldest).await?, Some(Code::Unavailable));
    if stalled.iter().any(JoinHandle::is_finished) {
        bail!("more than the oldest stalled call lost its slot");
    }
    Ok(())
}

#[tokio::test]
async fn a_listener_that_trusts_no_proxy_counts_every_proxied_client_as_one() -> Result<()> {
    let server = serve(4, Duration::from_secs(30)).await?;
    let proxy = spawn_proxy(server.address).await?;
    let attacker = connect_through(proxy).await?;
    let victim = connect_through(proxy).await?;

    let stalled = stall_checks(&attacker, 4).await;
    if stalled.iter().any(JoinHandle::is_finished) {
        bail!("a stalled call ended before holding a slot");
    }
    // The header names a different client, but this listener ignores it and
    // sees one connection that already holds every slot.
    assert_eq!(
        check(&victim).await.err().map(|status| status.code()),
        Some(Code::Unavailable)
    );
    if stalled.iter().any(JoinHandle::is_finished) {
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
    let collected = ended?.map_err(|status| eyre!("reflection response failed: {status}"))?;
    let status = collected
        .trailers()
        .and_then(|trailers| trailers.get("grpc-status"))
        .map(http::HeaderValue::as_bytes);
    assert_eq!(status, Some(b"4".as_slice()), "expected DEADLINE_EXCEEDED");

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
