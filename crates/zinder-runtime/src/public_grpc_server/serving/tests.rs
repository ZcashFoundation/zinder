//! The connection loop over real loopback connections.
//!
//! These tests serve a scripted response straight through the listener, with
//! no layer that copies its chunks, so a chunk's storage is freed exactly when
//! everything the transport holds of it is: h2's queue, hyper's parked chunk,
//! and the release gate's look-ahead.

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

use bytes::Bytes;
use eyre::{Result, bail, eyre};
use http::uri::PathAndQuery;
use http_body::Frame;
use http_body_util::{BodyExt as _, Full, StreamBody};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpSocket, tcp::OwnedReadHalf},
    sync::{Semaphore, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::{StreamExt as _, wrappers::ReceiverStream};
use tokio_util::sync::PollSemaphore;
use tonic::{
    Code, Request, Status,
    body::Body,
    server::NamedService,
    transport::{Channel, Endpoint},
};
use tonic_prost::ProstCodec;
use tower::Service;

use super::PublicGrpcServer;
use crate::public_grpc_server::release_gate::GateObserver;

const STALL: Duration = Duration::from_millis(500);
/// A bound that a test never reaches, so the other bound alone acts.
const NEVER: Duration = Duration::from_hours(1);
const WAIT_LIMIT: Duration = Duration::from_secs(10);
const STREAM_PATH: &str = "/zinder.test.v1.Scripted/Stream";
/// Bytes of each scripted message's payload.
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

/// One framed gRPC message (a `BytesValue` of `payload` bytes) whose bytes
/// depend on `seed`, so a misplaced byte changes the reassembled body.
fn message_frame(payload: usize, seed: u8) -> Vec<u8> {
    let value: Vec<u8> = (0..payload)
        .map(|offset| seed.wrapping_add(u8::try_from(offset % 251).unwrap_or(0)))
        .collect();
    let encoded = prost::Message::encode_to_vec(&value);
    let mut frame = Vec::with_capacity(5 + encoded.len());
    frame.push(0);
    frame.extend_from_slice(
        &u32::try_from(encoded.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    frame.extend_from_slice(&encoded);
    frame
}

/// Drives one scripted response: the test sends its frames and ends it.
struct Script {
    frames: mpsc::Sender<Result<Frame<Bytes>, Status>>,
    sent: Vec<u8>,
    produced: Arc<AtomicUsize>,
    freed: Arc<AtomicUsize>,
}

impl Script {
    async fn send_message(&mut self, payload: usize) -> Result<()> {
        let seed = u8::try_from(self.produced.load(Ordering::SeqCst) % 256).unwrap_or(0);
        let frame = message_frame(payload, seed);
        self.sent.extend_from_slice(&frame);
        self.produced.fetch_add(1, Ordering::SeqCst);
        let chunk = Bytes::from_owner(TrackedChunk {
            bytes: frame,
            freed: Arc::clone(&self.freed),
        });
        self.frames
            .send(Ok(Frame::data(chunk)))
            .await
            .map_err(|_| eyre!("the response is gone"))
    }

    /// Ends the response with `OK`.
    async fn finish(self) -> Result<()> {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        self.frames
            .send(Ok(Frame::trailers(trailers)))
            .await
            .map_err(|_| eyre!("the response is gone"))
    }
}

/// A server-streaming method whose every response a test scripts.
#[derive(Clone)]
struct Scripted {
    scripts: mpsc::UnboundedSender<Script>,
    produced: Arc<AtomicUsize>,
    freed: Arc<AtomicUsize>,
}

impl NamedService for Scripted {
    const NAME: &'static str = "zinder.test.v1.Scripted";
}

impl Service<http::Request<Body>> for Scripted {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: http::Request<Body>) -> Self::Future {
        // Room for every frame a test sends, so its sends never wait on the
        // transport it is testing.
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
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/grpc"),
        );
        Box::pin(std::future::ready(Ok(response)))
    }
}

/// Admits one request per permit of a shared semaphore, so a test can hold
/// a request's readiness back.
#[derive(Clone)]
struct Admission<S> {
    inner: S,
    permits: PollSemaphore,
}

impl<S, R> Service<R> for Admission<S>
where
    S: Service<R>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        match self.permits.poll_acquire(cx) {
            Poll::Ready(Some(permit)) => permit.forget(),
            Poll::Ready(None) => {}
            Poll::Pending => return Poll::Pending,
        }
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        self.inner.call(request)
    }
}

struct AdmissionLayer(Arc<Semaphore>);

impl<S> tower::Layer<S> for AdmissionLayer {
    type Service = Admission<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Admission {
            inner,
            permits: PollSemaphore::new(Arc::clone(&self.0)),
        }
    }
}

struct TestServer {
    address: SocketAddr,
    scripts: mpsc::UnboundedReceiver<Script>,
    produced: Arc<AtomicUsize>,
    freed: Arc<AtomicUsize>,
    /// How many times a response has started waiting for h2 to encode its
    /// probe.
    departure_waits: tokio::sync::watch::Receiver<usize>,
    server: JoinHandle<std::io::Result<()>>,
}

impl TestServer {
    /// Whether every message a script sent so far has been freed, and at
    /// least `expected` were sent.
    fn all_freed(&self, expected: usize) -> bool {
        let produced = self.produced.load(Ordering::SeqCst);
        produced >= expected && self.freed.load(Ordering::SeqCst) == produced
    }

    async fn next_script(&mut self) -> Result<Script> {
        within_wait_limit(self.scripts.recv())
            .await?
            .ok_or_else(|| eyre!("the server stopped"))
    }
}

struct Bounds {
    send_stall: Duration,
    write_stall: Duration,
    accept_http1: bool,
    admission: Option<Arc<Semaphore>>,
    /// The server's socket send buffer. Setting it also stops the kernel
    /// growing it, so a client that stops reading blocks the server's
    /// writes after a few KiB.
    send_buffer: Option<u32>,
}

impl Default for Bounds {
    fn default() -> Self {
        Self {
            send_stall: STALL,
            write_stall: NEVER,
            accept_http1: false,
            admission: None,
            send_buffer: None,
        }
    }
}

fn serve(bounds: Bounds) -> Result<TestServer> {
    let (scripts_sender, scripts) = mpsc::unbounded_channel();
    let produced = Arc::new(AtomicUsize::new(0));
    let freed = Arc::new(AtomicUsize::new(0));
    let service = Scripted {
        scripts: scripts_sender,
        produced: Arc::clone(&produced),
        freed: Arc::clone(&freed),
    };
    let socket = TcpSocket::new_v4()?;
    if let Some(send_buffer) = bounds.send_buffer {
        // Accepted connections inherit the listening socket's buffer size.
        socket.set_send_buffer_size(send_buffer)?;
    }
    socket.bind("127.0.0.1:0".parse()?)?;
    let listener = socket.listen(1024)?;
    let address = listener.local_addr()?;
    let (gate_observer, departure_waits) = GateObserver::new();
    let server = PublicGrpcServer::new(bounds.send_stall)
        .with_write_stall_timeout(bounds.write_stall)
        .with_gate_observer(gate_observer)
        .accept_http1(bounds.accept_http1);
    let server = match bounds.admission {
        Some(admission) => tokio::spawn(
            server
                .layer(AdmissionLayer(admission))
                .add_service(service)
                .serve_with_incoming(listener),
        ),
        None => tokio::spawn(server.add_service(service).serve_with_incoming(listener)),
    };
    Ok(TestServer {
        address,
        scripts,
        produced,
        freed,
        departure_waits,
        server,
    })
}

/// A connection that grants a stream no window beyond `stream_window`
/// until the test reads it.
async fn tonic_client(address: SocketAddr, stream_window: u32) -> Result<Channel> {
    Ok(Endpoint::new(format!("http://{address}"))?
        .initial_stream_window_size(Some(stream_window))
        .initial_connection_window_size(Some(64 * 1024 * 1024))
        .connect()
        .await?)
}

async fn open_stream(channel: &Channel) -> Result<tonic::Streaming<Vec<u8>>> {
    let mut grpc = tonic::client::Grpc::new(channel.clone());
    grpc.ready().await?;
    Ok(grpc
        .server_streaming(
            Request::new(()),
            PathAndQuery::from_static(STREAM_PATH),
            ProstCodec::<(), Vec<u8>>::default(),
        )
        .await?
        .into_inner())
}

/// Reads `stream` to its end, returning the messages and the final status.
async fn read_to_end(stream: &mut tonic::Streaming<Vec<u8>>) -> Result<(usize, Code)> {
    let mut messages = 0;
    loop {
        match within_wait_limit(stream.next()).await? {
            Some(Ok(_)) => messages += 1,
            Some(Err(status)) => return Ok((messages, status.code())),
            None => return Ok((messages, Code::Ok)),
        }
    }
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

/// Sends one gRPC request, with `grpc-timeout` when given, on a raw HTTP/2
/// connection whose stream window is `stream_window`.
///
/// Unlike tonic's client, it neither enforces the timeout itself nor reads
/// the body until the test does.
async fn raw_request(
    address: SocketAddr,
    stream_window: u32,
    grpc_timeout: Option<&'static str>,
) -> Result<http::Response<hyper::body::Incoming>> {
    let io = TokioIo::new(tokio::net::TcpStream::connect(address).await?);
    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .initial_stream_window_size(stream_window)
        .handshake(io)
        .await?;
    tokio::spawn(connection);
    let mut request = http::Request::post(format!("http://{address}{STREAM_PATH}"))
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header(http::header::TE, "trailers");
    if let Some(grpc_timeout) = grpc_timeout {
        request = request.header("grpc-timeout", grpc_timeout);
    }
    let request = request.body(Full::new(Bytes::from_static(&[0, 0, 0, 0, 0])))?;
    Ok(sender.send_request(request).await?)
}

/// Reads a raw response body to its end, returning its bytes and trailers.
async fn raw_body(body: hyper::body::Incoming) -> Result<(Vec<u8>, Option<http::HeaderMap>)> {
    let collected = within_wait_limit(body.collect()).await??;
    let trailers = collected.trailers().cloned();
    Ok((collected.to_bytes().to_vec(), trailers))
}

mod framing;
mod gate;
mod transport;
mod window;
