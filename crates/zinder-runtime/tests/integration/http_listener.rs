//! Attack-shaped tests for the shared bounded HTTP listener.

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::Bytes,
    extract::ws::{Message, WebSocketUpgrade},
    response::Response,
    routing::{get, post},
};
use eyre::{Result, eyre};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use zinder_runtime::{HttpListenerLimits, serve_http};

const HEADER_TIMEOUT: Duration = Duration::from_millis(300);
const GIVE_UP_AFTER: Duration = Duration::from_secs(3);
/// Larger than any loopback socket buffer the kernel will autotune.
const BIG_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const GET_HELLO: &[u8] = b"GET /hello HTTP/1.1\r\nHost: zinder\r\n\r\n";

struct TestServer {
    listen_addr: SocketAddr,
    shutdown: CancellationToken,
    join: JoinHandle<std::io::Result<()>>,
}

fn short_limits() -> HttpListenerLimits {
    HttpListenerLimits {
        header_read_timeout: HEADER_TIMEOUT,
        request_body_read_timeout: HEADER_TIMEOUT,
        write_stall_timeout: HEADER_TIMEOUT,
        max_connections: 64,
        max_request_body_bytes: 1024,
    }
}

async fn echo_websocket(upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(|mut socket| async move {
        while let Some(Ok(message)) = socket.recv().await {
            let Message::Text(_) = &message else { continue };
            if socket.send(message).await.is_err() {
                break;
            }
        }
    })
}

async fn start(limits: HttpListenerLimits) -> Result<TestServer> {
    let router = Router::new()
        .route("/hello", get(|| async { "hello" }))
        .route(
            "/length",
            post(|body: Bytes| async move { body.len().to_string() }),
        )
        .route("/big", get(|| async { vec![b'x'; BIG_RESPONSE_BYTES] }))
        .route("/ws", get(echo_websocket));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen_addr = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let join = tokio::spawn(serve_http(
        listener,
        router,
        limits,
        shutdown.clone().cancelled_owned(),
    ));
    Ok(TestServer {
        listen_addr,
        shutdown,
        join,
    })
}

/// Reads until `needle` appears, failing if the peer closes or the wait runs out.
async fn read_until(stream: &mut TcpStream, needle: &[u8]) -> Result<Vec<u8>> {
    let mut received = Vec::new();
    let mut chunk = [0_u8; 512];
    let deadline = Instant::now() + GIVE_UP_AFTER;
    while !received
        .windows(needle.len())
        .any(|window| window == needle)
    {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let read = tokio::time::timeout(remaining, stream.read(&mut chunk))
            .await
            .map_err(|_| eyre!("timed out waiting for {needle:?}"))??;
        if read == 0 {
            return Err(eyre!("connection closed before {needle:?}"));
        }
        received.extend_from_slice(&chunk[..read]);
    }
    Ok(received)
}

/// Waits for the server to close the connection, returning how long it took.
async fn wait_for_close(stream: &mut TcpStream) -> Result<Duration> {
    let started = Instant::now();
    let mut chunk = [0_u8; 512];
    loop {
        let remaining = GIVE_UP_AFTER.saturating_sub(started.elapsed());
        let read = tokio::time::timeout(remaining, stream.read(&mut chunk))
            .await
            .map_err(|_| eyre!("server kept the connection open past {GIVE_UP_AFTER:?}"))?;
        match read {
            Ok(0) | Err(_) => return Ok(started.elapsed()),
            Ok(_) => {}
        }
    }
}

#[tokio::test]
async fn partial_request_head_is_closed_after_header_timeout() -> Result<()> {
    let server = start(short_limits()).await?;
    let mut stream = TcpStream::connect(server.listen_addr).await?;
    stream
        .write_all(b"GET /hello HTTP/1.1\r\nHost: zinder\r\n")
        .await?;

    let closed_after = wait_for_close(&mut stream).await?;

    assert!(closed_after >= HEADER_TIMEOUT / 2, "{closed_after:?}");
    Ok(())
}

#[tokio::test]
async fn trickled_request_head_is_closed_at_the_header_deadline() -> Result<()> {
    let server = start(short_limits()).await?;
    let mut stream = TcpStream::connect(server.listen_addr).await?;
    stream.write_all(b"GET /hello HTTP/1.1\r\n").await?;

    let started = Instant::now();
    let mut chunk = [0_u8; 512];
    loop {
        if started.elapsed() > GIVE_UP_AFTER {
            return Err(eyre!("server tolerated a trickled request head"));
        }
        tokio::select! {
            read = stream.read(&mut chunk) => {
                if matches!(read, Ok(0) | Err(_)) {
                    break;
                }
            }
            () = tokio::time::sleep(Duration::from_millis(50)) => {
                if stream.write_all(b"X-Padding: a\r\n").await.is_err() {
                    break;
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn silent_connection_is_closed_after_header_timeout() -> Result<()> {
    let server = start(short_limits()).await?;
    let mut stream = TcpStream::connect(server.listen_addr).await?;

    wait_for_close(&mut stream).await?;
    Ok(())
}

#[tokio::test]
async fn normal_requests_are_served_and_idle_keep_alive_connections_are_closed() -> Result<()> {
    let server = start(short_limits()).await?;
    let mut stream = TcpStream::connect(server.listen_addr).await?;

    for _ in 0..2 {
        stream.write_all(GET_HELLO).await?;
        let response = read_until(&mut stream, b"hello").await?;
        assert!(response.starts_with(b"HTTP/1.1 200"));
    }

    wait_for_close(&mut stream).await?;
    Ok(())
}

#[tokio::test]
async fn connections_over_the_limit_are_closed_until_a_slot_frees() -> Result<()> {
    let server = start(HttpListenerLimits {
        header_read_timeout: Duration::from_secs(30),
        max_connections: 2,
        ..short_limits()
    })
    .await?;
    let first = TcpStream::connect(server.listen_addr).await?;
    let second = TcpStream::connect(server.listen_addr).await?;
    // Let the server accept both before the third arrives.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut refused = TcpStream::connect(server.listen_addr).await?;
    refused.write_all(GET_HELLO).await?;
    let mut received = Vec::new();
    // A reset after the close is as good as a clean EOF; only a timeout means "left open".
    let read_result = tokio::time::timeout(GIVE_UP_AFTER, refused.read_to_end(&mut received))
        .await
        .map_err(|_| eyre!("over-limit connection was left open"))?;
    drop(read_result);
    assert!(received.is_empty(), "over-limit connection got a response");

    drop(first);
    let deadline = Instant::now() + GIVE_UP_AFTER;
    loop {
        let mut retry = TcpStream::connect(server.listen_addr).await?;
        retry.write_all(GET_HELLO).await?;
        if read_until(&mut retry, b"hello").await.is_ok() {
            break;
        }
        if Instant::now() > deadline {
            return Err(eyre!("slot was not released after a connection closed"));
        }
    }
    drop(second);
    Ok(())
}

#[tokio::test]
async fn stalled_request_body_fails_instead_of_holding_the_request() -> Result<()> {
    let server = start(short_limits()).await?;
    let mut stream = TcpStream::connect(server.listen_addr).await?;
    stream
        .write_all(
            b"POST /length HTTP/1.1\r\nHost: zinder\r\nContent-Length: 100\r\n\r\n0123456789",
        )
        .await?;

    let response = read_until(&mut stream, b"HTTP/1.1 ").await?;

    assert!(!response.starts_with(b"HTTP/1.1 200"));
    Ok(())
}

#[tokio::test]
async fn slowly_dripped_request_body_fails_at_the_absolute_deadline() -> Result<()> {
    let server = start(HttpListenerLimits {
        max_connections: 1,
        ..short_limits()
    })
    .await?;
    let mut stream = TcpStream::connect(server.listen_addr).await?;
    stream
        .write_all(b"POST /length HTTP/1.1\r\nHost: zinder\r\nContent-Length: 1000\r\n\r\n")
        .await?;

    // One byte every two thirds of the deadline never lets a per-frame idle
    // timeout fire, but the absolute deadline still ends the request.
    let started = Instant::now();
    let mut chunk = [0_u8; 512];
    let mut response = Vec::new();
    loop {
        if started.elapsed() > GIVE_UP_AFTER {
            return Err(eyre!(
                "server let a dripped request body outlive its deadline"
            ));
        }
        tokio::select! {
            read = stream.read(&mut chunk) => match read {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    response.extend_from_slice(&chunk[..read]);
                    break;
                }
            },
            () = tokio::time::sleep(HEADER_TIMEOUT * 2 / 3) => {
                if stream.write_all(b"a").await.is_err() {
                    break;
                }
            }
        }
    }
    assert!(!response.starts_with(b"HTTP/1.1 200"), "{response:?}");
    drop(stream);

    get_hello_when_slot_frees(&server).await
}

/// Retries a plain `GET` until the server serves it, so a slot freed
/// asynchronously by a closed connection is awaited rather than raced.
async fn get_hello_when_slot_frees(server: &TestServer) -> Result<()> {
    let deadline = Instant::now() + GIVE_UP_AFTER;
    loop {
        let mut probe = TcpStream::connect(server.listen_addr).await?;
        probe.write_all(GET_HELLO).await?;
        if read_until(&mut probe, b"hello").await.is_ok() {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(eyre!("connection slot was not released"));
        }
    }
}

#[tokio::test]
async fn client_that_never_reads_a_response_is_disconnected() -> Result<()> {
    let server = start(HttpListenerLimits {
        header_read_timeout: Duration::from_secs(30),
        max_connections: 1,
        ..short_limits()
    })
    .await?;
    let socket = TcpSocket::new_v4()?;
    socket.set_recv_buffer_size(4096)?;
    let mut stream = socket.connect(server.listen_addr).await?;
    stream
        .write_all(b"GET /big HTTP/1.1\r\nHost: zinder\r\n\r\n")
        .await?;

    // Never read. The server must give up on the stalled write by itself, which
    // frees the only slot for a well-behaved client.
    let started = Instant::now();
    loop {
        let mut other = TcpStream::connect(server.listen_addr).await?;
        other.write_all(GET_HELLO).await?;
        if read_until(&mut other, b"hello").await.is_ok() {
            break;
        }
        if started.elapsed() > GIVE_UP_AFTER {
            return Err(eyre!("a client that never reads held the only slot"));
        }
    }
    drop(stream);
    Ok(())
}

#[tokio::test]
async fn upgraded_websocket_holds_its_connection_slot_until_it_closes() -> Result<()> {
    let server = start(HttpListenerLimits {
        header_read_timeout: Duration::from_secs(30),
        max_connections: 1,
        ..short_limits()
    })
    .await?;
    let mut websocket = TcpStream::connect(server.listen_addr).await?;
    websocket
        .write_all(
            b"GET /ws HTTP/1.1\r\nHost: zinder\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
              Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
        )
        .await?;
    read_until(&mut websocket, b"\r\n\r\n").await?;

    let mut refused = TcpStream::connect(server.listen_addr).await?;
    refused.write_all(GET_HELLO).await?;
    let mut received = Vec::new();
    drop(tokio::time::timeout(GIVE_UP_AFTER, refused.read_to_end(&mut received)).await);
    assert!(
        received.is_empty(),
        "upgraded connection did not hold its slot"
    );

    drop(websocket);
    get_hello_when_slot_frees(&server).await
}

#[tokio::test]
async fn oversized_request_body_is_rejected() -> Result<()> {
    let server = start(short_limits()).await?;
    let mut stream = TcpStream::connect(server.listen_addr).await?;
    let body = vec![b'a'; 4096];
    let head = format!(
        "POST /length HTTP/1.1\r\nHost: zinder\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    drop(stream.write_all(&body).await);

    let response = read_until(&mut stream, b"HTTP/1.1 ").await?;

    assert!(response.starts_with(b"HTTP/1.1 413"), "{response:?}");
    Ok(())
}

#[tokio::test]
async fn websocket_upgrade_outlives_the_header_timeout() -> Result<()> {
    let server = start(short_limits()).await?;
    let mut stream = TcpStream::connect(server.listen_addr).await?;
    stream
        .write_all(
            b"GET /ws HTTP/1.1\r\nHost: zinder\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
              Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
        )
        .await?;
    let handshake = read_until(&mut stream, b"\r\n\r\n").await?;
    assert!(handshake.starts_with(b"HTTP/1.1 101"), "{handshake:?}");

    tokio::time::sleep(HEADER_TIMEOUT * 3).await;

    let mask = [1_u8, 2, 3, 4];
    let mut frame = vec![0x81, 0x80 | 5];
    frame.extend_from_slice(&mask);
    frame.extend(
        b"hello"
            .iter()
            .zip(mask.iter().cycle())
            .map(|(byte, key)| byte ^ key),
    );
    stream.write_all(&frame).await?;
    let mut echoed = [0_u8; 7];
    tokio::time::timeout(GIVE_UP_AFTER, stream.read_exact(&mut echoed)).await??;
    assert_eq!(&echoed, b"\x81\x05hello");
    Ok(())
}

#[tokio::test]
async fn shutdown_closes_idle_connections_and_returns() -> Result<()> {
    let server = start(HttpListenerLimits {
        header_read_timeout: Duration::from_secs(30),
        ..short_limits()
    })
    .await?;
    let mut stream = TcpStream::connect(server.listen_addr).await?;
    stream.write_all(GET_HELLO).await?;
    read_until(&mut stream, b"hello").await?;

    server.shutdown.cancel();

    tokio::time::timeout(GIVE_UP_AFTER, server.join).await???;
    wait_for_close(&mut stream).await?;
    Ok(())
}
