//! gRPC-Web responses over real connections, through the listener's own
//! transport layers and the runtime's connection loop.

use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use eyre::{Result, bail, ensure, eyre};
use http::{HeaderValue, Request, header};
use http_body::Frame;
use http_body_util::{BodyExt as _, Full, StreamBody};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::bytes::Bytes;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpSocket, TcpStream},
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, body::Body, server::NamedService};
use tower::Service;
use zinder_runtime::{
    ClientAddressSource, Readiness, ReadinessState, RuntimeService, SupervisedRuntime,
};

use super::{BrowserListener, SERVICE_NAME, serve_listener};

const ORIGIN: &str = "https://wallet.example";
const BINARY: &str = "application/grpc-web+proto";
const TEXT: &str = "application/grpc-web-text+proto";
const STALL: Duration = Duration::from_millis(500);
const WAIT_LIMIT: Duration = Duration::from_secs(10);
const PAYLOAD: usize = 64 * 1024;

/// Storage that counts itself freed when dropped.
struct TrackedChunk {
    bytes: Vec<u8>,
    freed: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for TrackedChunk {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for TrackedChunk {
    fn drop(&mut self) {
        self.freed.fetch_add(1, Ordering::SeqCst);
    }
}

/// Drives one scripted response of the stand-in `CompactTxStreamer`.
struct Script {
    frames: mpsc::Sender<Result<Frame<Bytes>, Status>>,
    sent: Vec<u8>,
    produced: Arc<AtomicUsize>,
    freed: Arc<AtomicUsize>,
}

impl Script {
    /// Sends one gRPC message whose bytes depend on how many came before.
    async fn send_message(&mut self, payload: usize) -> Result<()> {
        let seed = self.produced.fetch_add(1, Ordering::SeqCst);
        let mut frame = vec![0];
        frame.extend_from_slice(&u32::try_from(payload)?.to_be_bytes());
        frame.extend(
            (0..payload)
                .map(|offset| u8::try_from((offset.wrapping_mul(31) ^ seed) % 251).unwrap_or(0)),
        );
        self.sent.extend_from_slice(&frame);
        let chunk = Bytes::from_owner(TrackedChunk {
            bytes: frame,
            freed: Arc::clone(&self.freed),
        });
        self.frames
            .send(Ok(Frame::data(chunk)))
            .await
            .map_err(|_| eyre!("the response is gone"))
    }

    async fn finish(self) -> Result<()> {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", HeaderValue::from_static("0"));
        self.frames
            .send(Ok(Frame::trailers(trailers)))
            .await
            .map_err(|_| eyre!("the response is gone"))
    }
}

/// A stand-in `CompactTxStreamer` whose every response a test scripts.
#[derive(Clone)]
struct ScriptedStreamer {
    scripts: mpsc::UnboundedSender<Script>,
    produced: Arc<AtomicUsize>,
    freed: Arc<AtomicUsize>,
}

impl NamedService for ScriptedStreamer {
    const NAME: &'static str = SERVICE_NAME;
}

impl Service<Request<Body>> for ScriptedStreamer {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: Request<Body>) -> Self::Future {
        let (frames, receiver) = mpsc::channel(8);
        let _ = self.scripts.send(Script {
            frames,
            sent: Vec::new(),
            produced: Arc::clone(&self.produced),
            freed: Arc::clone(&self.freed),
        });
        let mut response =
            http::Response::new(Body::new(StreamBody::new(ReceiverStream::new(receiver))));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/grpc"),
        );
        Box::pin(std::future::ready(Ok(response)))
    }
}

struct TestServer {
    address: SocketAddr,
    scripts: mpsc::UnboundedReceiver<Script>,
    produced: Arc<AtomicUsize>,
    freed: Arc<AtomicUsize>,
}

impl TestServer {
    async fn next_script(&mut self) -> Result<Script> {
        within_wait_limit(self.scripts.recv())
            .await?
            .ok_or_else(|| eyre!("the server stopped"))
    }

    fn all_freed(&self, expected: usize) -> bool {
        let produced = self.produced.load(Ordering::SeqCst);
        produced >= expected && self.freed.load(Ordering::SeqCst) == produced
    }
}

async fn serve() -> Result<TestServer> {
    let (scripts_sender, scripts) = mpsc::unbounded_channel();
    let produced = Arc::new(AtomicUsize::new(0));
    let freed = Arc::new(AtomicUsize::new(0));
    let streamer = ScriptedStreamer {
        scripts: scripts_sender,
        produced: Arc::clone(&produced),
        freed: Arc::clone(&freed),
    };
    let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd)
        .with_grpc_send_stall_timeout(STALL);
    let readiness = Readiness::new(ReadinessState::ready(Some(1)));
    let incoming = TcpListener::bind("127.0.0.1:0").await?;
    let address = incoming.local_addr()?;
    let origins = [HeaderValue::from_static(ORIGIN)];
    let listener = BrowserListener {
        browser_origins: &origins,
        client_address: ClientAddressSource::Connection,
    };
    tokio::spawn(serve_listener(
        &runtime, streamer, incoming, &listener, &readiness,
    )?);
    Ok(TestServer {
        address,
        scripts,
        produced,
        freed,
    })
}

/// A gRPC-Web request for an empty message in `encoding`.
fn request_body(encoding: &str) -> Bytes {
    if encoding == TEXT {
        Bytes::from(STANDARD.encode([0, 0, 0, 0, 0]))
    } else {
        Bytes::from_static(&[0, 0, 0, 0, 0])
    }
}

fn web_request(address: SocketAddr, encoding: &'static str) -> Result<Request<Full<Bytes>>> {
    Ok(
        Request::post(format!("http://{address}/{SERVICE_NAME}/GetBlockRange"))
            .header(header::ORIGIN, ORIGIN)
            .header(header::CONTENT_TYPE, encoding)
            .header(header::ACCEPT, encoding)
            .body(Full::new(request_body(encoding)))?,
    )
}

/// Sends a gRPC-Web request over HTTP/1.1 or HTTP/2 with a stream window of
/// 64 KiB, and returns the response body unread.
async fn send(
    address: SocketAddr,
    encoding: &'static str,
    http2: bool,
) -> Result<hyper::body::Incoming> {
    let io = TokioIo::new(TcpStream::connect(address).await?);
    let request = web_request(address, encoding)?;
    let response = if http2 {
        let (mut sender, connection) =
            hyper::client::conn::http2::Builder::new(TokioExecutor::new())
                .initial_stream_window_size(64 * 1024)
                .handshake(io)
                .await?;
        tokio::spawn(connection);
        sender.send_request(request).await?
    } else {
        let (mut sender, connection) = hyper::client::conn::http1::handshake(io).await?;
        tokio::spawn(connection);
        sender.send_request(request).await?
    };
    ensure!(
        response.status().is_success(),
        "status {}",
        response.status()
    );
    Ok(response.into_body())
}

/// The gRPC-Web frames of a whole response body in `encoding`.
fn web_frames(body: &[u8], encoding: &str) -> Result<Vec<u8>> {
    if encoding != TEXT {
        return Ok(body.to_vec());
    }
    // Each encoded chunk is padded on its own, so decode four characters at
    // a time.
    let mut decoded = Vec::new();
    for group in body.chunks(4) {
        decoded.extend(STANDARD.decode(group)?);
    }
    Ok(decoded)
}

async fn within_wait_limit<T>(future: impl Future<Output = T>) -> Result<T> {
    tokio::time::timeout(WAIT_LIMIT, future)
        .await
        .map_err(|_| eyre!("no outcome within {WAIT_LIMIT:?}"))
}

async fn wait_until(what: &str, mut probe: impl FnMut() -> bool) -> Result<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    while !probe() {
        if Instant::now() >= deadline {
            bail!("{what} did not happen within {WAIT_LIMIT:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// Sends six messages with pauses, so the release gate probes the window,
/// then `OK`, and checks the client decodes exactly those bytes and status.
async fn check_bytes_arrive_unchanged(encoding: &'static str, http2: bool) -> Result<()> {
    let mut server = serve().await?;
    let body = send(server.address, encoding, http2).await?;
    let mut script = server.next_script().await?;
    for round in 0..6 {
        script.send_message(PAYLOAD / 2 + round * 13).await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let sent = script.sent.clone();
    script.finish().await?;

    let collected = within_wait_limit(body.collect()).await??.to_bytes();
    let frames = web_frames(&collected, encoding)?;
    let (messages, trailer_frame) = frames
        .split_at_checked(sent.len())
        .ok_or_else(|| eyre!("the body is shorter than what was sent"))?;
    ensure!(messages == sent.as_slice(), "the message bytes changed");
    ensure!(
        trailer_frame.first() == Some(&0x80),
        "no trailer frame follows the messages"
    );
    let trailers = String::from_utf8_lossy(trailer_frame.get(5..).unwrap_or_default());
    ensure!(trailers.contains("grpc-status:0"), "trailers: {trailers}");
    Ok(())
}

#[tokio::test]
async fn binary_grpc_web_over_http1_keeps_every_byte() -> Result<()> {
    check_bytes_arrive_unchanged(BINARY, false).await
}

#[tokio::test]
async fn text_grpc_web_over_http1_keeps_every_byte() -> Result<()> {
    check_bytes_arrive_unchanged(TEXT, false).await
}

#[tokio::test]
async fn binary_grpc_web_over_http2_keeps_every_byte() -> Result<()> {
    check_bytes_arrive_unchanged(BINARY, true).await
}

#[tokio::test]
async fn text_grpc_web_over_http2_keeps_every_byte() -> Result<()> {
    check_bytes_arrive_unchanged(TEXT, true).await
}

/// Starts an HTTP/1.1 gRPC-Web request from a client that never reads its
/// 4 KiB receive buffer, and checks the server closes the connection and
/// frees what the response held.
async fn check_stalled_http1_closes(encoding: &'static str) -> Result<()> {
    let mut server = serve().await?;
    let socket = TcpSocket::new_v4()?;
    socket.set_recv_buffer_size(4096)?;
    let mut connection = socket.connect(server.address).await?;
    let body = request_body(encoding);
    connection
        .write_all(
            format!(
                "POST /{SERVICE_NAME}/GetBlockRange HTTP/1.1\r\nhost: localhost\r\n\
                 origin: {ORIGIN}\r\ncontent-type: {encoding}\r\naccept: {encoding}\r\n\
                 content-length: {}\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await?;
    connection.write_all(&body).await?;
    let mut script = server.next_script().await?;
    let feeder = tokio::spawn(async move { while script.send_message(PAYLOAD).await.is_ok() {} });

    within_wait_limit(feeder).await??;
    wait_until("the stalled response's messages are freed", || {
        server.all_freed(1)
    })
    .await?;
    let mut delivered = 0;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        match within_wait_limit(connection.read(&mut buffer)).await? {
            Ok(0) => break,
            Ok(read) => delivered += read,
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => break,
            Err(error) => return Err(error.into()),
        }
    }
    let produced = server.produced.load(Ordering::SeqCst);
    ensure!(delivered < produced * PAYLOAD, "the whole response arrived");
    Ok(())
}

#[tokio::test]
async fn a_stalled_binary_grpc_web_response_over_http1_closes_its_connection() -> Result<()> {
    check_stalled_http1_closes(BINARY).await
}

#[tokio::test]
async fn a_stalled_text_grpc_web_response_over_http1_closes_its_connection() -> Result<()> {
    check_stalled_http1_closes(TEXT).await
}

/// Sends a gRPC-Web response over HTTP/2 that ends while its data and its
/// trailer frame wait for window, and checks the stream is reset.
async fn check_finished_http2_tail_is_reset(encoding: &'static str) -> Result<()> {
    let mut server = serve().await?;
    let body = send(server.address, encoding, true).await?;
    let mut script = server.next_script().await?;
    script.send_message(PAYLOAD).await?;
    script.finish().await?;

    // tonic-web turns the trailers into one more DATA frame, which waits
    // for window behind the message. The client reads nothing until well
    // past the send stall timeout; the layer copies the message, so only
    // the response's outcome shows whether the stream was reset.
    tokio::time::sleep(STALL * 3).await;
    match within_wait_limit(body.collect()).await? {
        Ok(_) => bail!("the response ended normally after stalling"),
        Err(error) => ensure!(
            reset_with_cancel(&error),
            "the response ended with {error:?}, not a reset with CANCEL"
        ),
    }
    Ok(())
}

/// Whether `error` comes from the server resetting the stream with `CANCEL`.
fn reset_with_cancel(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(error) = cause {
        // h2's text for a received `RST_STREAM` with `CANCEL`.
        if error.to_string() == "stream error received: stream no longer needed" {
            return true;
        }
        cause = error.source();
    }
    false
}

#[tokio::test]
async fn a_finished_binary_grpc_web_response_waiting_for_window_is_reset() -> Result<()> {
    check_finished_http2_tail_is_reset(BINARY).await
}

#[tokio::test]
async fn a_finished_text_grpc_web_response_waiting_for_window_is_reset() -> Result<()> {
    check_finished_http2_tail_is_reset(TEXT).await
}
