#![allow(
    missing_docs,
    reason = "Unit test names describe the behavior under test."
)]

use std::{
    io::Read as _,
    net::SocketAddr,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

use tokio::{
    io::AsyncReadExt as _,
    net::{TcpListener, TcpSocket, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Server};
use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};

use super::{
    DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT, connect_with_startup_retries, connect_zinder_grpc,
    connect_zinder_grpc_with_timeout, dial, zinder_grpc_endpoint, zinder_grpc_tls_config,
};
use crate::auth::BearerTokenConnectError;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// First byte of a TLS handshake record.
const TLS_HANDSHAKE_RECORD: u8 = 0x16;

/// Bound the stalled-peer tests give a dial.
const SHORT_CONNECT_TIMEOUT: Duration = Duration::from_millis(200);

/// Ceiling on how long a dial with [`SHORT_CONNECT_TIMEOUT`] may take to fail.
const SHORT_DIAL_CEILING: Duration = Duration::from_secs(2);

/// Connects to a listener that records the first byte the client sends.
async fn first_byte_sent(scheme: &str) -> Result<u8, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = zinder_grpc_endpoint(
        &format!("{scheme}://{}", listener.local_addr()?),
        Duration::from_secs(5),
        zinder_grpc_tls_config(),
    )?;
    let client = tokio::spawn(async move { endpoint.connect().await });
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

#[tokio::test]
async fn a_peer_that_completes_tls_but_never_speaks_http2_fails_the_dial() -> TestResult {
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
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (handshake_completed, handshake_observed) = mpsc::channel();
    let _peer = spawn_silent_tls_peer(listener, Arc::new(server_config), handshake_completed);
    let endpoint = zinder_grpc_endpoint(
        &format!("https://{address}"),
        SHORT_CONNECT_TIMEOUT,
        zinder_grpc_tls_config().ca_certificate(Certificate::from_pem(certified.cert.pem())),
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

#[tokio::test]
async fn a_dial_to_a_server_without_the_health_service_succeeds() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let reflection = crate::public_grpc_reflection(tonic_reflection::pb::v1::FILE_DESCRIPTOR_SET)?;
    let server = tokio::spawn(
        Server::builder()
            .add_service(reflection)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    let outcome = connect_zinder_grpc_with_timeout(&endpoint, None, SHORT_CONNECT_TIMEOUT).await;
    server.abort();

    assert!(
        outcome.is_ok(),
        "an UNIMPLEMENTED answer proves the peer serves HTTP/2, got {outcome:?}"
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
