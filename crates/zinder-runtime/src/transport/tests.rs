#![allow(
    missing_docs,
    reason = "Unit test names describe the behavior under test."
)]

use std::{
    io::{Read as _, Write},
    net::SocketAddr,
    str::FromStr as _,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use http::{HeaderValue, header::AUTHORIZATION};
use hyper_util::{
    client::legacy::connect::dns::Name,
    rt::{TokioExecutor, TokioIo},
};
use parking_lot::Mutex;
use rustls::RootCertStore;
use tokio::{
    io::AsyncReadExt as _,
    net::{TcpListener, TcpSocket, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};
use tower::ServiceExt as _;

use super::{
    DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT, ZinderGrpcEndpoint, connect_with_startup_retries,
    connect_zinder_grpc, connect_zinder_grpc_with_timeout, connector::tcp_connector, dial,
};
use crate::auth::{BearerToken, BearerTokenConnectError};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// First byte of a TLS handshake record.
const TLS_HANDSHAKE_RECORD: u8 = 0x16;

/// Bound the stalled-peer tests give a dial.
const SHORT_CONNECT_TIMEOUT: Duration = Duration::from_millis(200);

/// Ceiling on how long a dial with [`SHORT_CONNECT_TIMEOUT`] may take to fail.
const SHORT_DIAL_CEILING: Duration = Duration::from_secs(2);

/// Path of the gRPC health check.
const HEALTH_CHECK_PATH: &str = "/grpc.health.v1.Health/Check";

/// Trusts no certificate, for dials that never finish a TLS handshake.
#[allow(
    clippy::unnecessary_wraps,
    reason = "Matches the trust-root loader that ZinderGrpcEndpoint::parse takes."
)]
fn no_tls_roots() -> Result<RootCertStore, BearerTokenConnectError> {
    Ok(RootCertStore::empty())
}

/// Connects to a listener that records the first byte the client sends.
async fn first_byte_sent(scheme: &str) -> Result<u8, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = ZinderGrpcEndpoint::parse(
        &format!("{scheme}://{}", listener.local_addr()?),
        Duration::from_secs(5),
        no_tls_roots,
    )?;
    let client = tokio::spawn(async move { dial(&endpoint, Duration::from_secs(5)).await });
    let (mut socket, _) = listener.accept().await?;
    let mut first = [0_u8; 1];
    socket.read_exact(&mut first).await?;
    drop(socket);
    client.abort();
    Ok(first[0])
}

fn is_connect_timeout<T>(
    outcome: &Result<T, BearerTokenConnectError>,
    expected_bound: Duration,
) -> bool {
    matches!(
        outcome,
        Err(BearerTokenConnectError::ConnectTimeout { connect_timeout })
            if *connect_timeout == expected_bound
    )
}

/// Accepts every connection and holds it open without sending a byte.
fn spawn_silent_peer(listener: TcpListener) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            hold_silently(stream);
        }
    })
}

/// Reads and discards what the client sends, answering nothing, until the
/// client closes the connection.
fn hold_silently(mut stream: TcpStream) {
    tokio::spawn(async move {
        let _ = tokio::io::copy(&mut stream, &mut tokio::io::sink()).await;
    });
}

/// Serves the gRPC health service on `listener` until `shutdown` fires.
fn spawn_health_server(
    listener: TcpListener,
    shutdown: oneshot::Receiver<()>,
) -> JoinHandle<Result<(), tonic::transport::Error>> {
    let (_reporter, health_service) = tonic_health::server::health_reporter();
    tokio::spawn(
        Server::builder()
            .add_service(health_service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown.await;
            }),
    )
}

#[tokio::test]
async fn https_endpoints_open_with_a_tls_handshake() -> TestResult {
    assert_eq!(first_byte_sent("https").await?, TLS_HANDSHAKE_RECORD);
    Ok(())
}

#[tokio::test]
async fn http_endpoints_stay_plaintext_http2() -> TestResult {
    // The HTTP/2 connection preface starts with "PRI".
    assert_eq!(first_byte_sent("http").await?, b'P');
    Ok(())
}

#[tokio::test]
async fn a_dial_to_a_peer_that_never_answers_fails_within_the_given_bound() -> TestResult {
    // The listener's backlog accepts the TCP connection, but nothing
    // answers the TLS handshake.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://{}", listener.local_addr()?);

    let started_at = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        connect_zinder_grpc_with_timeout(&endpoint, None, SHORT_CONNECT_TIMEOUT),
    )
    .await?;
    let elapsed = started_at.elapsed();

    assert!(
        is_connect_timeout(&outcome, SHORT_CONNECT_TIMEOUT),
        "stalled dial must report the connect timeout, got {outcome:?}"
    );
    assert!(
        elapsed >= SHORT_CONNECT_TIMEOUT && elapsed < SHORT_DIAL_CEILING,
        "dial took {elapsed:?}"
    );
    drop(listener);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_dial_gives_up_within_the_default_bound() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://{}", listener.local_addr()?);

    let outcome = tokio::time::timeout(
        DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT + Duration::from_secs(1),
        connect_zinder_grpc(&endpoint, None),
    )
    .await?;

    assert!(
        is_connect_timeout(&outcome, DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT),
        "stalled dial must report the default connect timeout, got {outcome:?}"
    );
    drop(listener);
    Ok(())
}

#[tokio::test]
async fn a_plaintext_peer_that_never_speaks_http2_fails_the_dial() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let peer = spawn_silent_peer(listener);

    let started_at = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        connect_zinder_grpc_with_timeout(&endpoint, None, SHORT_CONNECT_TIMEOUT),
    )
    .await?;
    let elapsed = started_at.elapsed();
    peer.abort();

    assert!(
        is_connect_timeout(&outcome, SHORT_CONNECT_TIMEOUT),
        "a peer that never sends HTTP/2 frames must time the dial out, got {outcome:?}"
    );
    assert!(elapsed < SHORT_DIAL_CEILING, "dial took {elapsed:?}");
    Ok(())
}

/// Completes one TLS handshake that negotiates HTTP/2, reports it on
/// `handshake_completed`, then reads and discards whatever the client sends
/// without answering.
fn spawn_silent_tls_peer(
    listener: std::net::TcpListener,
    server_config: Arc<rustls::ServerConfig>,
    handshake_completed: mpsc::Sender<()>,
) -> std::thread::JoinHandle<Result<(), String>> {
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().map_err(|error| error.to_string())?;
        let mut connection =
            rustls::ServerConnection::new(server_config).map_err(|error| error.to_string())?;
        while connection.is_handshaking() {
            connection
                .complete_io(&mut stream)
                .map_err(|error| error.to_string())?;
        }
        handshake_completed
            .send(())
            .map_err(|error| error.to_string())?;
        let mut discarded = [0_u8; 1024];
        while matches!(stream.read(&mut discarded), Ok(read) if read > 0) {}
        Ok(())
    })
}

/// A TLS server configuration for `127.0.0.1` that offers HTTP/2, and the
/// roots a client needs to trust it.
fn loopback_tls() -> Result<(Arc<rustls::ServerConfig>, RootCertStore), Box<dyn std::error::Error>>
{
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
    let mut server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(
        vec![certified.cert.der().clone()],
        rustls::pki_types::PrivateKeyDer::Pkcs8(certified.signing_key.serialize_der().into()),
    )?;
    server_config.alpn_protocols = vec![b"h2".to_vec()];
    let mut roots = RootCertStore::empty();
    roots.add(certified.cert.der().clone())?;
    Ok((Arc::new(server_config), roots))
}

#[tokio::test]
async fn a_peer_that_completes_tls_but_never_speaks_http2_fails_the_dial() -> TestResult {
    let (server_config, roots) = loopback_tls()?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (handshake_completed, handshake_observed) = mpsc::channel();
    let _peer = spawn_silent_tls_peer(listener, server_config, handshake_completed);
    let endpoint = ZinderGrpcEndpoint::parse(
        &format!("https://{address}"),
        SHORT_CONNECT_TIMEOUT,
        move || Ok(roots),
    )?;

    let started_at = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        dial(&endpoint, SHORT_CONNECT_TIMEOUT),
    )
    .await?;
    let elapsed = started_at.elapsed();

    assert!(
        handshake_observed.try_recv().is_ok(),
        "the peer must have completed the TLS handshake"
    );
    assert!(
        is_connect_timeout(&outcome, SHORT_CONNECT_TIMEOUT),
        "a peer that never sends HTTP/2 frames after TLS must time the dial out, got {outcome:?}"
    );
    assert!(elapsed < SHORT_DIAL_CEILING, "dial took {elapsed:?}");
    Ok(())
}

/// Token the proxy-like peer accepts.
const PROXY_TOKEN: &str = "proxy-token";

/// Serves the health check only to requests that carry [`PROXY_TOKEN`], the
/// way an authenticating proxy in front of a Zinder runtime does, and resets
/// every other stream without sending headers.
fn spawn_proxy_like_peer(listener: TcpListener) -> Result<JoinHandle<()>, http::Error> {
    let authorization = HeaderValue::from_str(&format!("Bearer {PROXY_TOKEN}"))?;
    let (_reporter, health_service) = tonic_health::server::health_reporter();
    Ok(tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let authorization = authorization.clone();
            let health_service = health_service.clone();
            let route =
                hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
                    let authorized = request.uri().path() == HEALTH_CHECK_PATH
                        && request.headers().get(AUTHORIZATION) == Some(&authorization);
                    let health_service = health_service.clone();
                    async move {
                        if !authorized {
                            // hyper answers a failed request with RST_STREAM.
                            return Err(std::io::Error::other("refused without a response"));
                        }
                        health_service
                            .oneshot(request)
                            .await
                            .map_err(std::io::Error::other)
                    }
                });
            tokio::spawn(
                hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), route),
            );
        }
    }))
}

#[tokio::test]
async fn a_dial_needs_no_route_or_credential_from_a_proxy() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let peer = spawn_proxy_like_peer(listener)?;
    let token = BearerToken::from_str(PROXY_TOKEN)?;

    let anonymous = connect_zinder_grpc_with_timeout(&endpoint, None, SHORT_CONNECT_TIMEOUT).await;
    let anonymous_check = match anonymous {
        Ok(channel) => Some(
            HealthClient::new(channel)
                .check(HealthCheckRequest::default())
                .await,
        ),
        Err(_) => None,
    };
    let authenticated =
        connect_zinder_grpc_with_timeout(&endpoint, Some(&token), SHORT_CONNECT_TIMEOUT).await;
    let authenticated_check = match authenticated {
        Ok(channel) => Some(
            HealthClient::new(channel)
                .check(HealthCheckRequest::default())
                .await,
        ),
        Err(error) => return Err(format!("the dial must succeed, got {error:?}").into()),
    };
    peer.abort();

    assert!(
        matches!(anonymous_check, Some(Err(_))),
        "the peer must refuse an unauthenticated call, got {anonymous_check:?}"
    );
    assert!(
        matches!(authenticated_check, Some(Ok(_))),
        "the authenticated call must succeed, got {authenticated_check:?}"
    );
    Ok(())
}

/// Terminates TLS and serves the health check over HTTP/2, the way a
/// TLS-terminating proxy does, reporting the URI of every request it serves.
fn spawn_tls_terminating_peer(
    listener: TcpListener,
    server_config: Arc<rustls::ServerConfig>,
) -> (
    JoinHandle<()>,
    tokio::sync::mpsc::UnboundedReceiver<http::Uri>,
) {
    let (served, served_uris) = tokio::sync::mpsc::unbounded_channel();
    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
    let (_reporter, health_service) = tonic_health::server::health_reporter();
    let peer = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let Ok(stream) = acceptor.accept(stream).await else {
                continue;
            };
            let served = served.clone();
            let health_service = health_service.clone();
            let route =
                hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
                    let _ = served.send(request.uri().clone());
                    health_service.clone().oneshot(request)
                });
            tokio::spawn(
                hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), route),
            );
        }
    });
    (peer, served_uris)
}

#[tokio::test]
async fn an_https_dial_serves_calls_through_a_tls_terminating_peer() -> TestResult {
    let (server_config, roots) = loopback_tls()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (peer, mut served_uris) = spawn_tls_terminating_peer(listener, server_config);
    let endpoint = ZinderGrpcEndpoint::parse(
        &format!("https://{address}"),
        SHORT_CONNECT_TIMEOUT,
        move || Ok(roots),
    )?;

    let channel = dial(&endpoint, SHORT_CONNECT_TIMEOUT).await?;
    let check = HealthClient::new(channel)
        .check(HealthCheckRequest::default())
        .await;
    let served_uri = served_uris.recv().await;
    peer.abort();

    assert!(
        check.is_ok(),
        "the call must succeed over TLS, got {check:?}"
    );
    let served_uri = served_uri.ok_or("the peer served no request")?;
    assert_eq!(served_uri.scheme_str(), Some("https"));
    assert_eq!(
        served_uri.authority().map(http::uri::Authority::as_str),
        Some(address.to_string().as_str())
    );
    Ok(())
}

/// Listens on `address` with a full accept queue, so further connection
/// attempts get no SYN-ACK. Returns the listener and the connections holding
/// the queue.
async fn listen_with_full_accept_queue(
    address: SocketAddr,
) -> Result<(tokio::net::TcpListener, Vec<TcpStream>), Box<dyn std::error::Error>> {
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(address)?;
    let listener = socket.listen(0)?;
    let mut queued = Vec::new();
    for _ in 0..16 {
        match tokio::time::timeout(Duration::from_millis(100), TcpStream::connect(address)).await {
            Ok(stream) => queued.push(stream?),
            Err(_) => return Ok((listener, queued)),
        }
    }
    Err("the accept queue never filled".into())
}

#[tokio::test]
async fn a_reconnect_after_the_first_dial_stays_within_the_connect_timeout() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop_server, server_stopped) = oneshot::channel();
    let server = spawn_health_server(listener, server_stopped);
    let channel =
        connect_zinder_grpc_with_timeout(&format!("http://{address}"), None, SHORT_CONNECT_TIMEOUT)
            .await?;
    let mut client = HealthClient::new(channel);
    client.check(HealthCheckRequest::default()).await?;

    // The peer goes away, and its address stops answering connection
    // attempts, so the channel's reconnect stalls in the TCP connect.
    let _ = stop_server.send(());
    server.await??;
    let (_listener, _queued) = listen_with_full_accept_queue(address).await?;

    let started_at = Instant::now();
    let reconnecting_call = tokio::time::timeout(
        Duration::from_secs(3),
        client.check(HealthCheckRequest::default()),
    )
    .await;
    let elapsed = started_at.elapsed();

    assert!(
        matches!(reconnecting_call, Ok(Err(_))),
        "a call that must reconnect fails within the connect timeout, got {reconnecting_call:?}"
    );
    assert!(elapsed < SHORT_DIAL_CEILING, "reconnect took {elapsed:?}");
    Ok(())
}

#[tokio::test]
async fn a_reconnect_to_a_peer_that_stays_silent_fails_within_the_connect_timeout() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop_server, server_stopped) = oneshot::channel();
    let server = spawn_health_server(listener, server_stopped);
    let channel =
        connect_zinder_grpc_with_timeout(&format!("http://{address}"), None, SHORT_CONNECT_TIMEOUT)
            .await?;
    let mut client = HealthClient::new(channel);
    client.check(HealthCheckRequest::default()).await?;

    // The peer goes away, and a peer that accepts connections but never
    // answers takes its address, so the channel's reconnect completes the TCP
    // connect and then hears nothing. The pause lets the channel see its
    // first connection close.
    let _ = stop_server.send(());
    server.await??;
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(address)?;
    let silent_listener = socket.listen(16)?;
    let accepted = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&accepted);
    let silent_peer = tokio::spawn(async move {
        while let Ok((stream, _)) = silent_listener.accept().await {
            counted.fetch_add(1, Ordering::SeqCst);
            hold_silently(stream);
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started_at = Instant::now();
    let reconnecting_call = tokio::time::timeout(
        Duration::from_secs(3),
        client.check(HealthCheckRequest::default()),
    )
    .await;
    let elapsed = started_at.elapsed();
    silent_peer.abort();

    assert!(
        accepted.load(Ordering::SeqCst) >= 1,
        "the call must reconnect to the silent peer"
    );
    assert!(
        matches!(reconnecting_call, Ok(Err(_))),
        "a call whose reconnect hears nothing fails within the connect timeout, got {reconnecting_call:?}"
    );
    assert!(elapsed < SHORT_DIAL_CEILING, "reconnect took {elapsed:?}");
    Ok(())
}

#[tokio::test]
async fn a_reconnect_bounds_a_name_lookup_that_never_answers() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop_server, server_stopped) = oneshot::channel();
    let server = spawn_health_server(listener, server_stopped);
    // Answers the first lookup and never answers another.
    let lookups = Arc::new(AtomicUsize::new(0));
    let counted_lookups = Arc::clone(&lookups);
    let resolver = tower::service_fn(move |_name: Name| {
        let first_lookup = counted_lookups.fetch_add(1, Ordering::SeqCst) == 0;
        async move {
            if !first_lookup {
                std::future::pending::<()>().await;
            }
            Ok::<_, std::io::Error>(std::iter::once(address))
        }
    });
    let mut endpoint = ZinderGrpcEndpoint::parse(
        &format!("http://zinder-peer.test:{}", address.port()),
        SHORT_CONNECT_TIMEOUT,
        no_tls_roots,
    )?;
    endpoint.connector.tcp = tcp_connector(resolver, SHORT_CONNECT_TIMEOUT);
    let mut client = HealthClient::new(dial(&endpoint, SHORT_CONNECT_TIMEOUT).await?);
    client.check(HealthCheckRequest::default()).await?;

    // The peer goes away. The pause lets the channel see its connection
    // close, so the next call reconnects and looks the name up again.
    let _ = stop_server.send(());
    server.await??;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started_at = Instant::now();
    let reconnecting_call = tokio::time::timeout(
        Duration::from_secs(3),
        client.check(HealthCheckRequest::default()),
    )
    .await;
    let elapsed = started_at.elapsed();

    assert_eq!(lookups.load(Ordering::SeqCst), 2, "the call must reconnect");
    assert!(
        matches!(reconnecting_call, Ok(Err(_))),
        "a reconnect whose lookup stalls fails within the connect timeout, got {reconnecting_call:?}"
    );
    assert!(elapsed < SHORT_DIAL_CEILING, "reconnect took {elapsed:?}");
    Ok(())
}

/// Holds every connection silently until `serving_after` has passed, then
/// hands new connections to a health server.
fn spawn_peer_that_starts_serving_late(
    listener: TcpListener,
    serving_after: Duration,
) -> JoinHandle<()> {
    let serving_at = Instant::now() + serving_after;
    tokio::spawn(async move {
        let (handoff, incoming) = tokio::sync::mpsc::channel::<std::io::Result<TcpStream>>(4);
        let (_reporter, health_service) = tonic_health::server::health_reporter();
        let server = tokio::spawn(
            Server::builder()
                .add_service(health_service)
                .serve_with_incoming(ReceiverStream::new(incoming)),
        );
        while let Ok((stream, _)) = listener.accept().await {
            if Instant::now() < serving_at {
                hold_silently(stream);
            } else if handoff.send(Ok(stream)).await.is_err() {
                break;
            }
        }
        server.abort();
    })
}

#[tokio::test]
async fn a_startup_dial_keeps_trying_until_a_slow_peer_serves() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let peer = spawn_peer_that_starts_serving_late(listener, Duration::from_millis(700));
    let cancel = CancellationToken::new();

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        connect_with_startup_retries(&endpoint, None, SHORT_CONNECT_TIMEOUT, &cancel),
    )
    .await?;
    peer.abort();

    assert!(
        matches!(outcome, Ok(Some(_))),
        "the startup dial connects once the peer serves, got {outcome:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_startup_dial_stops_when_cancelled() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let peer = spawn_silent_peer(listener);
    let cancel = CancellationToken::new();
    let cancel_later = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel_later.cancel();
    });

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        connect_with_startup_retries(&endpoint, None, SHORT_CONNECT_TIMEOUT, &cancel),
    )
    .await?;
    peer.abort();

    assert!(
        matches!(outcome, Ok(None)),
        "a cancelled startup dial reports no channel, got {outcome:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_startup_dial_returns_a_refused_connection_at_once() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    drop(listener);

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        connect_with_startup_retries(
            &endpoint,
            None,
            SHORT_CONNECT_TIMEOUT,
            &CancellationToken::new(),
        ),
    )
    .await?;

    assert!(
        matches!(outcome, Err(BearerTokenConnectError::Transport(_))),
        "a refused connection ends the startup dial, got {outcome:?}"
    );
    Ok(())
}

/// Collects what a tracing subscriber writes.
#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl CapturedLog {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock()).into_owned()
    }
}

impl Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn startup_dial_warnings_redact_endpoint_credentials() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!(
        "http://reader:hunter2@{}/?token=s3cret",
        listener.local_addr()?
    );
    let peer = spawn_silent_peer(listener);
    let log = CapturedLog::default();
    let writer = log.clone();
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .finish(),
    );
    let cancel = CancellationToken::new();
    let cancel_later = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(SHORT_CONNECT_TIMEOUT * 2).await;
        cancel_later.cancel();
    });

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        connect_with_startup_retries(&endpoint, None, SHORT_CONNECT_TIMEOUT, &cancel),
    )
    .await?;
    peer.abort();
    let logged = log.contents();

    assert!(matches!(outcome, Ok(None)), "got {outcome:?}");
    assert!(
        logged.contains("zinder_grpc_startup_dial_timed_out"),
        "the timed-out dial must be logged: {logged}"
    );
    assert!(
        !logged.contains("hunter2") && !logged.contains("s3cret"),
        "the log must not carry endpoint credentials: {logged}"
    );
    Ok(())
}
