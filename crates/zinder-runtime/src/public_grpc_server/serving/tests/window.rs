//! How a stream's piece waits for the client's flow-control window, driven by
//! an h2 client that controls exactly when it grants window and when it
//! changes its settings.

use futures_util::FutureExt as _;
use h2::client::{Connection, SendRequest};
use tokio::{net::TcpStream, sync::watch};

use super::*;

/// The client's side of one HTTP/2 connection, with its connection task.
struct H2Client {
    requests: SendRequest<Bytes>,
    window: mpsc::UnboundedSender<(u32, oneshot::Sender<bool>)>,
    connection: JoinHandle<()>,
}

impl H2Client {
    async fn connect(address: SocketAddr, stream_window: u32) -> Result<Self> {
        let (requests, connection) = h2::client::Builder::new()
            .initial_window_size(stream_window)
            .initial_connection_window_size(64 * 1024 * 1024)
            .handshake::<_, Bytes>(TcpStream::connect(address).await?)
            .await?;
        let (window, changes) = mpsc::unbounded_channel();
        let connection = tokio::spawn(drive(connection, changes));
        Ok(Self {
            requests,
            window,
            connection,
        })
    }

    /// Opens a call to the scripted method and returns its response body once
    /// its headers have arrived.
    async fn open_stream(&self) -> Result<h2::RecvStream> {
        let mut requests = within_wait_limit(self.requests.clone().ready()).await??;
        let request = http::Request::post(format!("http://localhost{STREAM_PATH}"))
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header(http::header::TE, "trailers")
            .body(())?;
        let (response, mut request_body) = requests.send_request(request, false)?;
        request_body.send_data(Bytes::from_static(&[0; 5]), true)?;
        Ok(within_wait_limit(response).await??.into_body())
    }

    /// Asks the connection to send `SETTINGS_INITIAL_WINDOW_SIZE`, and
    /// returns whether it did. It refuses while an earlier change waits for
    /// the server's acknowledgement.
    async fn set_stream_window(&self, size: u32) -> Result<bool> {
        let (sent, outcome) = oneshot::channel();
        self.window
            .send((size, sent))
            .map_err(|_| eyre!("the connection ended"))?;
        Ok(within_wait_limit(outcome).await??)
    }
}

/// Drives `connection`, changing its stream window on request.
async fn drive(
    mut connection: Connection<TcpStream, Bytes>,
    mut changes: mpsc::UnboundedReceiver<(u32, oneshot::Sender<bool>)>,
) {
    loop {
        tokio::select! {
            _ = &mut connection => return,
            change = changes.recv() => if let Some((size, sent)) = change {
                let _ = sent.send(connection.set_initial_window_size(size).is_ok());
            } else {
                let _ = connection.await;
                return;
            },
        }
    }
}

/// A proxy to `server` whose server-to-client direction can be paused.
struct PausableProxy {
    address: SocketAddr,
    flowing: watch::Sender<bool>,
    task: JoinHandle<()>,
}

async fn spawn_pausable_proxy(server: SocketAddr) -> Result<PausableProxy> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (flowing, mut flowing_now) = watch::channel(true);
    let task = tokio::spawn(async move {
        let Ok((client, _)) = listener.accept().await else {
            return;
        };
        let Ok(upstream) = small_buffer_connection(server).await else {
            return;
        };
        let (mut upstream_read, mut upstream_write) = upstream.into_split();
        let (mut client_read, mut client_write) = client.into_split();
        let to_server = async {
            let _ = tokio::io::copy(&mut client_read, &mut upstream_write).await;
        };
        let to_client = async {
            let mut buffer = vec![0; 64 * 1024];
            loop {
                if !*flowing_now.borrow_and_update() {
                    if flowing_now.changed().await.is_err() {
                        return;
                    }
                    continue;
                }
                tokio::select! {
                    changed = flowing_now.changed() => if changed.is_err() {
                        return;
                    },
                    read = upstream_read.read(&mut buffer) => match read {
                        Ok(0) | Err(_) => return,
                        Ok(read) => {
                            let chunk = buffer.get(..read).unwrap_or_default();
                            if client_write.write_all(chunk).await.is_err() {
                                return;
                            }
                        }
                    },
                }
            }
        };
        tokio::join!(to_server, to_client);
    });
    Ok(PausableProxy {
        address,
        flowing,
        task,
    })
}

/// Connects to `server` with a 4 KiB receive buffer.
async fn small_buffer_connection(server: SocketAddr) -> Result<TcpStream> {
    let socket = TcpSocket::new_v4()?;
    socket.set_recv_buffer_size(4096)?;
    Ok(socket.connect(server).await?)
}

/// The gap that only an h2 change can close.
///
/// Once the response has ended, a client that takes its window back by
/// `SETTINGS` strands the data HTTP/2 still queues for it, with the stream,
/// until the connection closes.
#[tokio::test]
async fn a_window_taken_back_after_the_end_holds_the_data_until_the_connection_closes() -> Result<()>
{
    let mut server = serve(Bounds {
        send_buffer: Some(4096),
        ..Bounds::default()
    })?;
    let proxy = spawn_pausable_proxy(server.address).await?;
    let client = H2Client::connect(proxy.address, 1024 * 1024).await?;
    let mut body = client.open_stream().await?;
    let mut script = server.next_script().await?;

    // The client stops reading its socket, so after a few KiB HTTP/2 keeps
    // the rest of the response queued. The window covers all of it, so the
    // probe passes and the response ends: hyper drops its body.
    proxy.flowing.send(false)?;
    let body_dropped = script.frames.clone();
    script.send_message(380 * 1024).await?;
    script.finish().await?;
    within_wait_limit(body_dropped.closed()).await?;
    assert_eq!(server.freed.load(Ordering::SeqCst), 0);

    // The client lowers its stream window to zero and reads again. HTTP/2
    // applies the change as it acknowledges it, before it sends more DATA,
    // so once the acknowledgement arrives nothing more comes for the stream.
    assert!(
        client.set_stream_window(0).await?,
        "the window change was refused"
    );
    proxy.flowing.send(true)?;
    wait_until_async("the server acknowledges the window change", || {
        client.set_stream_window(0)
    })
    .await?;
    let mut received = 0;
    while let Some(Some(chunk)) = body.data().now_or_never() {
        let chunk = chunk?;
        received += chunk.len();
        body.flow_control().release_capacity(chunk.len())?;
    }
    let sent = message_frame(380 * 1024, 0).len();
    assert!(received < sent, "the whole response arrived");

    // The stream stays open and its data stays queued, past the send stall
    // timeout, for as long as the connection is open.
    tokio::time::sleep(STALL * 3).await;
    assert!(
        body.data().now_or_never().is_none(),
        "the stream ended or received more data"
    );
    assert!(!body.is_end_stream(), "the stream ended");
    assert_eq!(server.freed.load(Ordering::SeqCst), 0, "the data was freed");

    // Closing the connection releases both.
    proxy.task.abort();
    client.connection.abort();
    drop((body, client));
    wait_until("the stranded data is freed", || server.all_freed(1)).await?;
    Ok(())
}

/// Runs `probe` until it returns true, within the wait limit.
async fn wait_until_async<F, Fut>(what: &str, mut probe: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let started = Instant::now();
    while started.elapsed() < WAIT_LIMIT {
        if probe().await? {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    bail!("{what} did not happen within {WAIT_LIMIT:?}")
}

/// Reads a response of eight 64 KiB messages with a 64 KiB window, granting
/// `credit` bytes of window back every `tick`, and returns the bytes read,
/// the response's outcome, and the bytes the server sent.
async fn read_with_credit(
    credit: usize,
    tick: Duration,
) -> Result<(usize, Result<(), h2::Error>, usize)> {
    const MESSAGES: usize = 8;
    let mut server = serve(Bounds::default())?;
    let client = H2Client::connect(server.address, 64 * 1024).await?;
    let mut body = client.open_stream().await?;
    let mut script = server.next_script().await?;
    // A reset drops the handler, so a send fails once it happens.
    let producer = tokio::spawn(async move {
        for _ in 0..MESSAGES {
            if script.send_message(PAYLOAD).await.is_err() {
                return script.sent.len();
            }
        }
        let sent = script.sent.len();
        let _ = script.finish().await;
        sent
    });

    let mut received = 0;
    let mut unreleased = 0;
    let mut ticks = tokio::time::interval(tick);
    // One deadline for the whole exchange: the tick only grants credit.
    let outcome = within_wait_limit(async {
        loop {
            tokio::select! {
                chunk = body.data() => match chunk {
                    Some(Ok(chunk)) => {
                        received += chunk.len();
                        unreleased += chunk.len();
                    }
                    Some(Err(error)) => return Ok::<_, eyre::Report>(Err(error)),
                    None => return Ok(body.trailers().await.map(|_| ())),
                },
                _ = ticks.tick() => {
                    let grant = unreleased.min(credit);
                    if grant > 0 {
                        body.flow_control().release_capacity(grant)?;
                        unreleased -= grant;
                    }
                }
            }
        }
    })
    .await??;
    let sent = within_wait_limit(producer).await??;
    drop(client);
    Ok((received, outcome, sent))
}

/// A reader granting enough window within the timeout is never reset.
///
/// hyper sends a waiting piece once the window covers everything queued ahead
/// of it plus one byte. Here that shortfall is at most the 64 KiB piece queued
/// ahead, so granting that much within the timeout keeps the stream alive,
/// however small each grant is.
#[tokio::test]
async fn a_reader_granting_a_piece_of_window_within_the_timeout_is_never_reset() -> Result<()> {
    let (received, outcome, sent) = read_with_credit(16 * 1024, STALL / 16).await?;
    outcome?;
    assert_eq!(received, sent);
    Ok(())
}

/// Window updates that come well within the timeout still end the stream
/// when together they grant less than the waiting piece needs: window for
/// everything queued ahead of it plus one byte.
#[tokio::test]
async fn a_reader_granting_less_than_a_piece_of_window_within_the_timeout_is_reset() -> Result<()> {
    let (received, outcome, sent) = read_with_credit(4 * 1024, STALL / 8).await?;
    let error = outcome
        .err()
        .ok_or_else(|| eyre!("the response ended normally"))?;
    assert_eq!(error.reason(), Some(h2::Reason::CANCEL), "{error}");
    assert!(received < sent, "every byte arrived");
    Ok(())
}
