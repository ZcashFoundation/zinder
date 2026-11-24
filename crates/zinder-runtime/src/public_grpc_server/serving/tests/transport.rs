//! Connection-level bounds, each tested with the other bounds out of reach.

use super::*;

/// Reads `connection` until the server closes it, and returns the bytes read.
async fn drain(connection: &mut (impl tokio::io::AsyncRead + Unpin)) -> Result<usize> {
    let mut received = 0;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        match within_wait_limit(connection.read(&mut buffer)).await? {
            Ok(0) => return Ok(received),
            Ok(read) => received += read,
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                return Ok(received);
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// Connects to `server` with a 4 KiB receive buffer, so the server's writes
/// block soon after the client stops reading.
async fn small_buffer_connection(server: SocketAddr) -> Result<tokio::net::TcpStream> {
    let socket = TcpSocket::new_v4()?;
    socket.set_recv_buffer_size(4096)?;
    Ok(socket.connect(server).await?)
}

#[tokio::test]
async fn a_stalled_http1_response_closes_its_connection() -> Result<()> {
    let mut server = serve(Bounds {
        accept_http1: true,
        ..Bounds::default()
    })?;
    let mut connection = small_buffer_connection(server.address).await?;
    connection
        .write_all(
            format!(
                "POST {STREAM_PATH} HTTP/1.1\r\nhost: localhost\r\n\
                 content-type: application/grpc\r\ncontent-length: 5\r\n\r\n"
            )
            .as_bytes(),
        )
        .await?;
    connection.write_all(&[0; 5]).await?;
    let mut script = server.next_script().await?;
    let feeder = tokio::spawn(async move { while script.send_message(PAYLOAD).await.is_ok() {} });

    // The write stall bound is out of reach, so only the gate closes it.
    within_wait_limit(feeder).await??;
    wait_until("the stalled response's messages are freed", || {
        server.all_freed(1)
    })
    .await?;
    let delivered = drain(&mut connection).await?;
    let produced = server.produced.load(Ordering::SeqCst);
    assert!(delivered < produced * PAYLOAD, "the whole response arrived");
    Ok(())
}

/// A proxy between a client and the server that can stop reading the
/// server's side, as a client that stops reading its socket does.
struct Proxy {
    address: SocketAddr,
    stop_reading: oneshot::Sender<()>,
    drain: oneshot::Sender<()>,
    /// Bytes read from the server's side once told to drain, until EOF.
    drained: JoinHandle<std::io::Result<usize>>,
}

/// Starts a proxy to `server` that can stop reading the server's side.
///
/// It forwards both directions until told to stop reading the server. From
/// then on it reads the server's side at `trickle` bytes per 50 ms, or not at
/// all when that is zero, until told to drain it to EOF. The client's side
/// stays open throughout.
async fn spawn_proxy(server: SocketAddr, trickle: usize) -> Result<Proxy> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop_sender, mut stop_reading) = oneshot::channel::<()>();
    let (drain_sender, mut drain) = oneshot::channel::<()>();
    let drained = tokio::spawn(async move {
        let (client, _) = listener.accept().await?;
        let upstream = small_buffer_connection(server)
            .await
            .map_err(std::io::Error::other)?;
        let (mut upstream_read, mut upstream_write) = upstream.into_split();
        let (mut client_read, mut client_write) = client.into_split();
        tokio::spawn(async move { tokio::io::copy(&mut client_read, &mut upstream_write).await });
        let mut buffer = vec![0; 64 * 1024];
        loop {
            tokio::select! {
                _ = &mut stop_reading => break,
                read = upstream_read.read(&mut buffer) => {
                    let read = read?;
                    if read == 0 {
                        return Ok(0);
                    }
                    client_write.write_all(buffer.get(..read).unwrap_or_default()).await?;
                }
            }
        }
        let mut sip = vec![0; trickle.max(1)];
        loop {
            tokio::select! {
                _ = &mut drain => break,
                () = tokio::time::sleep(Duration::from_millis(50)), if trickle > 0 => {
                    if upstream_read.read(&mut sip).await? == 0 {
                        return Ok(0);
                    }
                }
            }
        }
        let drained = drain_to_eof(&mut upstream_read).await;
        drop(client_write);
        drained
    });
    Ok(Proxy {
        address,
        stop_reading: stop_sender,
        drain: drain_sender,
        drained,
    })
}

/// Reads `connection` to EOF, or to a reset, and returns the bytes read.
async fn drain_to_eof(connection: &mut OwnedReadHalf) -> std::io::Result<usize> {
    let mut received = 0;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        match connection.read(&mut buffer).await {
            Ok(0) => return Ok(received),
            Ok(read) => received += read,
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                return Ok(received);
            }
            Err(error) => return Err(error),
        }
    }
}

/// Serves with only the write stall bound in reach, stops the client reading
/// its socket, or reading more than `trickle` bytes per 50 ms, and checks the
/// server closes the connection and frees the response.
async fn check_unread_socket_closes(trickle: usize) -> Result<()> {
    let mut server = serve(Bounds {
        send_stall: NEVER,
        write_stall: STALL,
        ..Bounds::default()
    })?;
    let proxy = spawn_proxy(server.address, trickle).await?;
    let channel = tonic_client(proxy.address, 64 * 1024 * 1024).await?;
    let stream = open_stream(&channel).await?;
    let mut script = server.next_script().await?;
    let _ = proxy.stop_reading.send(());
    let feeder = tokio::spawn(async move { while script.send_message(PAYLOAD).await.is_ok() {} });

    // The feeder stops once the server drops the response, which happens
    // only when the connection closes.
    within_wait_limit(feeder).await??;
    wait_until("the closed connection's messages are freed", || {
        server.all_freed(1)
    })
    .await?;
    let _ = proxy.drain.send(());
    let drained = within_wait_limit(proxy.drained).await???;
    let produced = server.produced.load(Ordering::SeqCst);
    assert!(drained < produced * PAYLOAD, "the whole response arrived");
    drop(stream);
    Ok(())
}

#[tokio::test]
async fn a_connection_whose_client_stops_reading_its_socket_closes() -> Result<()> {
    check_unread_socket_closes(0).await
}

#[tokio::test]
async fn a_connection_whose_client_reads_a_trickle_closes() -> Result<()> {
    // 64 bytes every 50 ms is progress well inside each window, but far
    // less than the 16 KiB a blocked connection must write per window.
    check_unread_socket_closes(64).await
}

#[tokio::test]
async fn dropping_the_server_drains_its_connections() -> Result<()> {
    let mut server = serve(Bounds::default())?;
    let channel = tonic_client(server.address, 64 * 1024).await?;
    // One whole call first, so the server has accepted the connection.
    let mut stream = open_stream(&channel).await?;
    let mut script = server.next_script().await?;
    script.send_message(400).await?;
    script.finish().await?;
    assert_eq!(read_to_end(&mut stream).await?, (1, Code::Ok));
    server.server.abort();

    // The drained connection closes, and the listener with it, so a call on
    // the same channel fails instead of reaching a detached connection.
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        let mut grpc = tonic::client::Grpc::new(channel.clone());
        // A call the server accepts waits for a script nobody sends, so a
        // call still in progress after a second means it was served.
        let call = tokio::time::timeout(Duration::from_secs(1), async {
            grpc.ready()
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            grpc.unary(
                Request::new(()),
                PathAndQuery::from_static(STREAM_PATH),
                ProstCodec::<(), Vec<u8>>::default(),
            )
            .await
        })
        .await;
        if let Ok(Err(status)) = &call
            && status.code() == Code::Unavailable
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("the dropped server's connection still serves calls: {call:?}");
        }
    }
}

#[tokio::test]
async fn a_request_deadline_counts_from_when_the_service_is_ready() -> Result<()> {
    let admission = Arc::new(Semaphore::new(1));
    let mut server = serve(Bounds {
        admission: Some(Arc::clone(&admission)),
        ..Bounds::default()
    })?;
    // The first call takes the only admission and holds it.
    let first = raw_request(server.address, 65_535, None).await?;
    let first_script = server.next_script().await?;

    let address = server.address;
    let second = tokio::spawn(raw_request(address, 65_535, Some("200m")));
    // The second call waits for readiness longer than its deadline.
    tokio::time::sleep(Duration::from_millis(500)).await;
    admission.add_permits(1);
    let second_script = server.next_script().await?;
    second_script.finish().await?;

    let response = within_wait_limit(second).await???;
    assert!(
        !response.headers().contains_key("grpc-status"),
        "the call ended at once with {:?}",
        response.headers().get("grpc-status")
    );
    let trailers = within_wait_limit(response.into_body().collect())
        .await??
        .trailers()
        .cloned();
    let status = trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"));
    assert_eq!(
        status.map(http::HeaderValue::as_bytes),
        Some(b"0".as_slice())
    );
    drop((first, first_script));
    Ok(())
}

#[tokio::test]
async fn a_request_deadline_includes_registered_middleware() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (scripts, mut calls) = mpsc::unbounded_channel();
    let server = tokio::spawn(
        PublicGrpcServer::new(STALL)
            .layer(tower::layer::layer_fn(|inner: tonic::service::Routes| {
                tower::service_fn(move |request| {
                    let mut inner = inner.clone();
                    async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        inner.call(request).await
                    }
                })
            }))
            .add_service(Scripted {
                scripts,
                produced: Arc::default(),
                freed: Arc::default(),
            })
            .serve_with_incoming(listener),
    );

    let response = within_wait_limit(raw_request(address, 65_535, Some("50m"))).await??;
    server.abort();
    let status = Status::from_header_map(response.headers());
    assert_eq!(status.map(|status| status.code()), Some(Code::Cancelled));
    assert!(calls.try_recv().is_err(), "the request reached the handler");
    Ok(())
}

/// A method whose response is one message in a body that reports its end
/// with its last frame, as tonic-web's gRPC-Web bodies do.
#[derive(Clone)]
struct OneMessage;

impl NamedService for OneMessage {
    const NAME: &'static str = "zinder.test.v1.OneMessage";
}

impl Service<http::Request<Body>> for OneMessage {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = std::future::Ready<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: http::Request<Body>) -> Self::Future {
        let message = Bytes::from(message_frame(400, 1));
        let mut response = http::Response::new(Body::new(Full::new(message)));
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/grpc"),
        );
        std::future::ready(Ok(response))
    }
}

#[tokio::test]
async fn an_http1_connection_carries_a_slow_call_after_a_finished_one() -> Result<()> {
    let admission = Arc::new(Semaphore::new(1));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(
        PublicGrpcServer::new(STALL)
            .with_write_stall_timeout(NEVER)
            .accept_http1(true)
            .layer(AdmissionLayer(Arc::clone(&admission)))
            .add_service(OneMessage)
            .serve_with_incoming(listener),
    );
    let io = TokioIo::new(tokio::net::TcpStream::connect(address).await?);
    let (mut sender, connection) = hyper::client::conn::http1::handshake(io).await?;
    tokio::spawn(connection);
    let request = || {
        http::Request::post(format!("http://{address}/zinder.test.v1.OneMessage/Call"))
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .body(Full::new(Bytes::from_static(&[0; 5])))
    };
    let expected = message_frame(400, 1).len();

    let first = within_wait_limit(sender.send_request(request()?)).await??;
    let first = within_wait_limit(first.into_body().collect()).await??;
    assert_eq!(first.to_bytes().len(), expected);

    // The next call on the same connection waits for readiness for longer
    // than the send stall timeout, so it starts no response meanwhile.
    within_wait_limit(sender.ready()).await??;
    let second = tokio::spawn(sender.send_request(request()?));
    tokio::time::sleep(STALL * 3).await;
    admission.add_permits(1);
    let second = within_wait_limit(second).await???;
    let second = within_wait_limit(second.into_body().collect()).await??;
    assert_eq!(second.to_bytes().len(), expected);
    Ok(())
}
