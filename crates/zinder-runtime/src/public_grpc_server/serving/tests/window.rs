//! How a stream's piece waits for the client's flow-control window, driven by
//! an h2 client that controls exactly when it grants window and when it
//! changes its settings.

use h2::client::{Connection, SendRequest};
use tokio::{net::TcpStream, sync::watch};

use super::*;
use crate::MAX_CONCURRENT_STREAMS_PER_CONNECTION;
use eyre::ensure;

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

/// The stall deadline for the window-change tests: long enough that the
/// client's window change and the server's acknowledgement always fall
/// inside it, which each test checks before it relies on it.
const WINDOW_CHANGE_STALL: Duration = Duration::from_secs(2);

/// A response whose data waits behind a blocked socket, held back from its
/// end until h2 has encoded its probe.
struct WithheldEnd {
    server: TestServer,
    proxy: PausableProxy,
    client: H2Client,
    body: h2::RecvStream,
    /// Closes once the server drops the gated response body, which happens
    /// as its stream task ends.
    body_dropped: mpsc::Sender<Result<Frame<Bytes>, Status>>,
    /// Whether the gate let the end go before h2 encoded the probe, as a
    /// gate without the departure wait does.
    ended_early: bool,
}

impl WithheldEnd {
    /// Fails unless the gate is still waiting for the probe to leave.
    fn ensure_still_withheld(&self) -> Result<()> {
        ensure!(!self.ended_early, "the gate released the end at once");
        ensure!(
            !self.body_dropped.is_closed(),
            "the response body was already dropped"
        );
        ensure!(!self.server.all_freed(1), "the response's data was freed");
        Ok(())
    }
}

/// Opens a call whose response ends while its data waits for the socket.
///
/// The client stops reading its socket and the server sends 380 KiB, then
/// ends the response. The window covers it all, so hyper takes the probe, but
/// h2 cannot encode it while the data ahead of it waits for the socket. The
/// call returns once the gate has started waiting for the probe to leave, or
/// once a gate without that wait has released the end.
async fn withhold_end(send_stall: Duration) -> Result<WithheldEnd> {
    let mut server = serve(Bounds {
        send_stall,
        send_buffer: Some(4096),
        ..Bounds::default()
    })?;
    let proxy = spawn_pausable_proxy(server.address).await?;
    let client = H2Client::connect(proxy.address, 1024 * 1024).await?;
    let body = client.open_stream().await?;
    let mut script = server.next_script().await?;
    proxy.flowing.send(false)?;
    let body_dropped = script.frames.clone();
    script.send_message(380 * 1024).await?;
    script.finish().await?;
    let mut departure_waits = server.departure_waits.clone();
    let ended_early = within_wait_limit(async {
        tokio::select! {
            waited = departure_waits.wait_for(|waits| *waits >= 1) => waited.map(|_| false),
            () = body_dropped.closed() => Ok(true),
        }
    })
    .await??;
    Ok(WithheldEnd {
        server,
        proxy,
        client,
        body,
        body_dropped,
        ended_early,
    })
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

/// Lowers the client's stream window to zero and waits until the server has
/// applied it.
///
/// h2 applies a peer's setting as it buffers the acknowledgement, which can
/// need write progress on a blocked connection, and it does so before it
/// takes more DATA from any stream's queue. The socket flows again so the
/// acknowledgement can be written; at most the frame already in h2's write
/// buffer leaves ahead of it.
async fn take_window_back(withheld: &WithheldEnd) -> Result<()> {
    ensure!(
        withheld.client.set_stream_window(0).await?,
        "the window change was refused"
    );
    withheld.proxy.flowing.send(true)?;
    wait_until_async("the server acknowledges the window change", || {
        withheld.client.set_stream_window(0)
    })
    .await
}

/// A window taken back before the end leaves the stall timer to reset it.
///
/// Once the client takes its window back by `SETTINGS`, the data ahead of the
/// probe can never be sent, so the probe stays with hyper, and the stall
/// timer resets the stream: its data and its slot are freed while the
/// connection stays open.
#[tokio::test]
async fn a_window_taken_back_before_the_end_resets_the_stream() -> Result<()> {
    let withheld = withhold_end(WINDOW_CHANGE_STALL).await?;
    if !withheld.ended_early {
        withheld.ensure_still_withheld()?;
    }
    take_window_back(&withheld).await?;
    // The server applied the change while the response was still held, so
    // a reset can only come after it.
    if !withheld.ended_early {
        withheld.ensure_still_withheld()?;
    }

    let WithheldEnd {
        mut server,
        proxy,
        client,
        mut body,
        body_dropped,
        ended_early: _,
    } = withheld;
    within_wait_limit(body_dropped.closed()).await?;
    wait_until("the stranded data is freed", || server.all_freed(1)).await?;
    let ended_with = loop {
        match within_wait_limit(body.data()).await? {
            Some(Ok(_)) => {}
            Some(Err(error)) => break error.reason(),
            None => break None,
        }
    };
    assert_eq!(ended_with, Some(h2::Reason::CANCEL));

    // The reset stream's slot is free again: the connection carries a full
    // set of new streams at once.
    let streams = usize::try_from(MAX_CONCURRENT_STREAMS_PER_CONNECTION)?;
    let opened = within_wait_limit(futures_util::future::try_join_all(
        (0..streams).map(|_| client.open_stream()),
    ))
    .await??;
    for _ in 0..streams {
        server.next_script().await?;
    }
    assert_eq!(opened.len(), streams);
    drop(proxy);
    Ok(())
}

/// The control for the test above: the same withheld end, with the window
/// left as it was, goes out once the socket flows, and nothing is reset.
#[tokio::test]
async fn a_withheld_end_goes_out_once_the_socket_flows() -> Result<()> {
    let withheld = withhold_end(WINDOW_CHANGE_STALL).await?;
    withheld.ensure_still_withheld()?;
    let WithheldEnd {
        server,
        proxy,
        client: _client,
        mut body,
        body_dropped,
        ended_early: _,
    } = withheld;
    proxy.flowing.send(true)?;
    let mut received = 0;
    while let Some(chunk) = within_wait_limit(body.data()).await? {
        let chunk = chunk?;
        received += chunk.len();
        body.flow_control().release_capacity(chunk.len())?;
    }
    let trailers = within_wait_limit(body.trailers()).await??;
    let status = trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"));
    assert_eq!(
        status.map(http::HeaderValue::as_bytes),
        Some(b"0".as_slice())
    );
    assert_eq!(received, message_frame(380 * 1024, 0).len());
    within_wait_limit(body_dropped.closed()).await?;
    wait_until("the response's data is freed", || server.all_freed(1)).await?;
    Ok(())
}

/// A client that resets the stream while its end is held back ends the
/// stream's task without the stall timer, which is out of reach here.
#[tokio::test]
async fn a_client_reset_while_the_end_is_held_back_frees_the_stream() -> Result<()> {
    let withheld = withhold_end(NEVER).await?;
    withheld.ensure_still_withheld()?;
    let WithheldEnd {
        server,
        proxy,
        client: _client,
        body,
        body_dropped,
        ended_early: _,
    } = withheld;
    // Dropping the client's last handle to the stream resets it with CANCEL.
    drop(body);
    within_wait_limit(body_dropped.closed()).await?;
    // h2 discarded the stream's queue. The frame it had already encoded
    // leaves with the socket, which is the write stall bound's to limit.
    proxy.flowing.send(true)?;
    wait_until("the stream's data is freed", || server.all_freed(1)).await?;
    Ok(())
}

/// Closing the connection while a response's end is held back ends the
/// stream's task and frees its data, with the stall timer out of reach.
#[tokio::test]
async fn a_connection_closed_while_the_end_is_held_back_frees_the_stream() -> Result<()> {
    let withheld = withhold_end(NEVER).await?;
    withheld.ensure_still_withheld()?;
    let WithheldEnd {
        server,
        proxy,
        client,
        body,
        body_dropped,
        ended_early: _,
    } = withheld;
    proxy.task.abort();
    client.connection.abort();
    drop((body, client));
    within_wait_limit(body_dropped.closed()).await?;
    wait_until("the stream's data is freed", || server.all_freed(1)).await?;
    Ok(())
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
