//! The connector that opens every connection of an intra-Zinder channel.
//!
//! tonic runs it under the endpoint's connect timeout, so the name lookup,
//! the TCP connect, and the TLS handshake of every connection, including the
//! reconnects a channel makes on its own, share one bound. A connection whose
//! peer has not sent its whole connection preface, the initial HTTP/2
//! `SETTINGS` frame, when that bound ends fails, so a call that waits on a
//! reconnect to a silent or stalled peer fails within the bound too. A
//! connection whose peer opens with any other frame fails at once. The
//! connector runs the TLS handshake itself so that it can see the bytes the
//! peer sends over HTTP/2, which tonic and hyper do not expose.

use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use http::Uri;
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioIo};
use parking_lot::Mutex;
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::oneshot,
    time::{Instant, Sleep, sleep_until},
};
use tokio_rustls::{TlsConnector, client::TlsStream};
use tokio_util::either::Either;
use tower::{Service, ServiceExt as _, util::BoxCloneSyncService};

use super::ZINDER_GRPC_TCP_KEEPALIVE;
use crate::auth::BearerTokenConnectError;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Resolves the endpoint's host and opens a TCP connection to it.
pub(super) type TcpConnector = BoxCloneSyncService<Uri, TokioIo<TcpStream>, BoxError>;

/// The stream a channel's HTTP/2 connection runs over.
type ZinderGrpcStream = PeerPrefaceWatch<Either<TcpStream, TlsStream<TcpStream>>>;

/// What a connection reports about the peer's connection preface.
pub(super) type PeerPrefaceReport = Result<(), InvalidPeerPreface>;

/// ALPN protocol identifier for HTTP/2 over TLS.
const ALPN_H2: &[u8] = b"h2";

/// Length of an HTTP/2 frame header.
const FRAME_HEADER_LEN: usize = 9;

/// HTTP/2 frame type of a `SETTINGS` frame.
const SETTINGS_FRAME_TYPE: u8 = 0x4;

/// `SETTINGS` flag that marks an acknowledgement rather than the sender's
/// settings.
const SETTINGS_ACK_FLAG: u8 = 0x1;

/// Length of one setting in a `SETTINGS` payload.
const SETTING_LEN: usize = 6;

/// Largest preface payload a peer may send: the HTTP/2 default maximum frame
/// size, which an intra-Zinder channel never raises.
const MAX_PREFACE_PAYLOAD_LEN: usize = 16_384;

/// Stream identifier bits of a frame header, without the reserved bit.
const STREAM_ID_MASK: u32 = 0x7fff_ffff;

/// Opens the connections of one intra-Zinder channel to `target`.
#[derive(Clone)]
pub(super) struct ZinderGrpcConnector {
    target: Uri,
    pub(super) tcp: TcpConnector,
    tls: Option<ZinderGrpcTls>,
    /// How long a connection may take, from the name lookup until the peer's
    /// connection preface has arrived.
    connect_timeout: Duration,
    /// Taken by the first connection, which reports once the peer's connection
    /// preface has arrived or been rejected.
    preface_report: Arc<Mutex<Option<oneshot::Sender<PeerPrefaceReport>>>>,
}

impl ZinderGrpcConnector {
    pub(super) fn new(
        target: Uri,
        tcp: TcpConnector,
        tls: Option<ZinderGrpcTls>,
        connect_timeout: Duration,
    ) -> Self {
        Self {
            target,
            tcp,
            tls,
            connect_timeout,
            preface_report: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns a copy whose first connection reports on the returned receiver
    /// once the peer's connection preface has arrived or been rejected. The
    /// receiver fails when that connection ends first.
    pub(super) fn reporting_peer_preface(&self) -> (Self, oneshot::Receiver<PeerPrefaceReport>) {
        let (report, peer_preface) = oneshot::channel();
        let connector = Self {
            preface_report: Arc::new(Mutex::new(Some(report))),
            ..self.clone()
        };
        (connector, peer_preface)
    }
}

impl Service<Uri> for ZinderGrpcConnector {
    type Response = TokioIo<ZinderGrpcStream>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.tcp.poll_ready(cx)
    }

    /// Connects to the connector's own target. tonic passes the `http` form
    /// of an `https` endpoint, because it only accepts an `https` URI when it
    /// runs the TLS handshake itself.
    fn call(&mut self, _channel_uri: Uri) -> Self::Future {
        let preface_deadline = Instant::now() + self.connect_timeout;
        let connecting = self.tcp.call(self.target.clone());
        let tls = self.tls.clone();
        let report = self.preface_report.lock().take();
        Box::pin(async move {
            let tcp = connecting.await?.into_inner();
            let stream = match tls {
                None => Either::Left(tcp),
                Some(tls) => Either::Right(tls.handshake(tcp).await?),
            };
            Ok(TokioIo::new(PeerPrefaceWatch::new(
                stream,
                preface_deadline,
                report,
            )))
        })
    }
}

/// Builds the TCP connector an intra-Zinder channel uses, resolving host
/// names with `resolver`.
pub(super) fn tcp_connector<R>(resolver: R, connect_timeout: Duration) -> TcpConnector
where
    HttpConnector<R>: Service<Uri, Response = TokioIo<TcpStream>> + Clone + Send + Sync + 'static,
    <HttpConnector<R> as Service<Uri>>::Error: Into<BoxError>,
    <HttpConnector<R> as Service<Uri>>::Future: Send + 'static,
{
    let mut http = HttpConnector::new_with_resolver(resolver);
    http.enforce_http(false);
    http.set_nodelay(true);
    http.set_keepalive(Some(ZINDER_GRPC_TCP_KEEPALIVE));
    http.set_connect_timeout(Some(connect_timeout));
    BoxCloneSyncService::new(http.map_err(Into::into))
}

/// TLS settings for an `https` endpoint.
#[derive(Clone)]
pub(super) struct ZinderGrpcTls {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl ZinderGrpcTls {
    /// Trusts `roots` to identify `host`, and offers only HTTP/2.
    pub(super) fn new(host: &str, roots: RootCertStore) -> Result<Self, BearerTokenConnectError> {
        // A URI host keeps the brackets around an IPv6 literal; a server name
        // takes the bare address.
        let host = host
            .strip_prefix('[')
            .and_then(|bracketed| bracketed.strip_suffix(']'))
            .unwrap_or(host);
        let server_name =
            ServerName::try_from(host.to_owned()).map_err(|error| tls_setup_error(&error))?;
        let mut config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|error| tls_setup_error(&error))?
                .with_root_certificates(roots)
                .with_no_client_auth();
        config.alpn_protocols = vec![ALPN_H2.to_vec()];
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
        })
    }

    async fn handshake(self, tcp: TcpStream) -> Result<TlsStream<TcpStream>, BoxError> {
        let stream = self.connector.connect(self.server_name, tcp).await?;
        if stream.get_ref().1.alpn_protocol() != Some(ALPN_H2) {
            return Err(Http2NotNegotiated.into());
        }
        Ok(stream)
    }
}

/// Loads the certificates the host trusts.
pub(super) fn platform_tls_roots() -> Result<RootCertStore, BearerTokenConnectError> {
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
    if roots.is_empty() {
        return Err(BearerTokenConnectError::TlsSetup {
            reason: "no trusted root certificates were found on this host".to_owned(),
        });
    }
    Ok(roots)
}

fn tls_setup_error(error: &dyn std::error::Error) -> BearerTokenConnectError {
    BearerTokenConnectError::TlsSetup {
        reason: error.to_string(),
    }
}

#[derive(Debug, Error)]
#[error("the peer did not negotiate HTTP/2 during the TLS handshake")]
struct Http2NotNegotiated;

#[derive(Debug, Error)]
#[error("the peer did not send its HTTP/2 SETTINGS frame within the connect timeout")]
struct PeerPrefaceTimedOut;

/// Why the peer's opening bytes are not an HTTP/2 connection preface.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum InvalidPeerPreface {
    /// The first frame is not a `SETTINGS` frame.
    #[error("the peer's first frame has type {frame_type:#04x}, not SETTINGS")]
    NotSettings {
        /// Frame type the peer sent.
        frame_type: u8,
    },
    /// The first frame acknowledges settings the client has not had answered.
    #[error("the peer's first frame is a SETTINGS acknowledgement")]
    Acknowledgement,
    /// The first frame names a stream instead of the connection.
    #[error("the peer's first SETTINGS frame names stream {stream_id}, not the connection")]
    OnStream {
        /// Stream identifier the frame names.
        stream_id: u32,
    },
    /// The first frame's payload is too long or does not hold whole settings.
    #[error(
        "the peer's first SETTINGS frame announces a {payload_len}-byte payload, \
         not a multiple of {} bytes up to {}",
        SETTING_LEN,
        MAX_PREFACE_PAYLOAD_LEN
    )]
    PayloadLength {
        /// Payload length the frame header announces.
        payload_len: usize,
    },
}

/// A stream that watches for the peer's connection preface, the initial
/// HTTP/2 `SETTINGS` frame, while passing every byte through unchanged.
///
/// It reports once the whole frame has arrived. It fails the read when the
/// peer opens with any other frame, and when the deadline passes before the
/// frame is complete.
pub(super) struct PeerPrefaceWatch<S> {
    stream: S,
    /// Cleared once the preface has arrived or been rejected.
    awaiting: Option<AwaitingPreface>,
}

struct AwaitingPreface {
    progress: PrefaceProgress,
    deadline: Pin<Box<Sleep>>,
    report: Option<oneshot::Sender<PeerPrefaceReport>>,
}

impl<S> PeerPrefaceWatch<S> {
    fn new(
        stream: S,
        deadline: Instant,
        report: Option<oneshot::Sender<PeerPrefaceReport>>,
    ) -> Self {
        Self {
            stream,
            awaiting: Some(AwaitingPreface {
                progress: PrefaceProgress::default(),
                deadline: Box::pin(sleep_until(deadline)),
                report,
            }),
        }
    }

    /// Stops watching and reports `outcome` when this connection reports.
    fn finish(&mut self, outcome: PeerPrefaceReport) {
        if let Some(report) = self.awaiting.take().and_then(|awaiting| awaiting.report) {
            let _ = report.send(outcome);
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PeerPrefaceWatch<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let filled_before = buf.filled().len();
        let polled = Pin::new(&mut this.stream).poll_read(cx, buf);
        let Some(awaiting) = this.awaiting.as_mut() else {
            return polled;
        };
        let received = buf.filled().get(filled_before..).unwrap_or_default();
        match awaiting.progress.advance(received) {
            Ok(PrefaceArrival::Incomplete) => {}
            Ok(PrefaceArrival::Complete) => {
                this.finish(Ok(()));
                return polled;
            }
            Err(invalid) => {
                this.finish(Err(invalid));
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, invalid)));
            }
        }
        if polled.is_pending() && awaiting.deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                PeerPrefaceTimedOut,
            )));
        }
        polled
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PeerPrefaceWatch<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

/// Whether the peer's connection preface has fully arrived.
#[derive(Debug, PartialEq, Eq)]
enum PrefaceArrival {
    Incomplete,
    Complete,
}

/// How much of the peer's connection preface has arrived.
#[derive(Debug)]
enum PrefaceProgress {
    /// The first `received` bytes of the frame header have arrived.
    Header {
        header: [u8; FRAME_HEADER_LEN],
        received: usize,
    },
    /// The header is valid, and `remaining` payload bytes have yet to arrive.
    Payload { remaining: usize },
}

impl Default for PrefaceProgress {
    fn default() -> Self {
        Self::Header {
            header: [0; FRAME_HEADER_LEN],
            received: 0,
        }
    }
}

impl PrefaceProgress {
    /// Advances over `bytes`, the next bytes the peer sent. Bytes past the end
    /// of the preface are not inspected.
    fn advance(&mut self, mut bytes: &[u8]) -> Result<PrefaceArrival, InvalidPeerPreface> {
        if let Self::Header { header, received } = self {
            let (taken, rest) = bytes.split_at(bytes.len().min(FRAME_HEADER_LEN - *received));
            header[*received..*received + taken.len()].copy_from_slice(taken);
            *received += taken.len();
            if *received < FRAME_HEADER_LEN {
                return Ok(PrefaceArrival::Incomplete);
            }
            *self = Self::Payload {
                remaining: preface_payload_len(*header)?,
            };
            bytes = rest;
        }
        match self {
            Self::Payload { remaining } => {
                *remaining -= bytes.len().min(*remaining);
                Ok(if *remaining == 0 {
                    PrefaceArrival::Complete
                } else {
                    PrefaceArrival::Incomplete
                })
            }
            Self::Header { .. } => Ok(PrefaceArrival::Incomplete),
        }
    }
}

/// Checks that `header` opens a connection preface, and returns the length of
/// its payload.
fn preface_payload_len(header: [u8; FRAME_HEADER_LEN]) -> Result<usize, InvalidPeerPreface> {
    let [len_high, len_mid, len_low, frame_type, flags, stream @ ..] = header;
    if frame_type != SETTINGS_FRAME_TYPE {
        return Err(InvalidPeerPreface::NotSettings { frame_type });
    }
    if flags & SETTINGS_ACK_FLAG != 0 {
        return Err(InvalidPeerPreface::Acknowledgement);
    }
    let stream_id = u32::from_be_bytes(stream) & STREAM_ID_MASK;
    if stream_id != 0 {
        return Err(InvalidPeerPreface::OnStream { stream_id });
    }
    let payload_len =
        (usize::from(len_high) << 16) | (usize::from(len_mid) << 8) | usize::from(len_low);
    if payload_len > MAX_PREFACE_PAYLOAD_LEN || !payload_len.is_multiple_of(SETTING_LEN) {
        return Err(InvalidPeerPreface::PayloadLength { payload_len });
    }
    Ok(payload_len)
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        sync::oneshot::error::TryRecvError,
        time::Instant,
    };

    use super::{InvalidPeerPreface, PeerPrefaceWatch, PrefaceArrival, PrefaceProgress};

    #[tokio::test]
    async fn the_watch_passes_bytes_through_and_reports_once_the_settings_frame_is_whole()
    -> Result<(), Box<dyn std::error::Error>> {
        // A SETTINGS frame with one setting, split inside its header and its
        // payload, and a SETTINGS acknowledgement after it.
        let chunks: [&[u8]; 4] = [
            &[0, 0, 6],
            &[0x4, 0, 0, 0, 0, 0, 0],
            &[0x3, 0, 0],
            &[0, 0x64, 0, 0, 0, 0x4, 0x1, 0, 0, 0, 0],
        ];
        let (mut peer, client) = tokio::io::duplex(64);
        let (report, mut reported) = tokio::sync::oneshot::channel();
        let mut watch = PeerPrefaceWatch::new(
            client,
            Instant::now() + Duration::from_secs(5),
            Some(report),
        );

        let mut passed_through = Vec::new();
        let mut reports = Vec::new();
        for chunk in chunks {
            peer.write_all(chunk).await?;
            let mut read = vec![0; chunk.len()];
            watch.read_exact(&mut read).await?;
            passed_through.extend_from_slice(&read);
            reports.push(reported.try_recv());
        }

        assert_eq!(passed_through, chunks.concat());
        assert!(
            matches!(
                reports.as_slice(),
                [
                    Err(TryRecvError::Empty),
                    Err(TryRecvError::Empty),
                    Err(TryRecvError::Empty),
                    Ok(Ok(())),
                ]
            ),
            "the watch must report only once the whole frame arrived, got {reports:?}"
        );
        Ok(())
    }

    #[test]
    fn only_an_initial_settings_frame_of_bounded_length_opens_the_preface() {
        let cases: [(&[u8], Result<PrefaceArrival, InvalidPeerPreface>); 9] = [
            (&[0, 0, 0, 0x4, 0, 0, 0, 0, 0], Ok(PrefaceArrival::Complete)),
            // The reserved bit of the stream identifier is ignored.
            (
                &[0, 0, 0, 0x4, 0, 0x80, 0, 0, 0],
                Ok(PrefaceArrival::Complete),
            ),
            // The longest whole number of settings within 16384 bytes.
            (
                &[0, 0x3f, 0xfc, 0x4, 0, 0, 0, 0, 0],
                Ok(PrefaceArrival::Incomplete),
            ),
            (
                &[0, 0, 8, 0x6, 0, 0, 0, 0, 0],
                Err(InvalidPeerPreface::NotSettings { frame_type: 0x6 }),
            ),
            (
                b"HTTP/1.1 400",
                Err(InvalidPeerPreface::NotSettings { frame_type: b'P' }),
            ),
            (
                &[0, 0, 0, 0x4, 0x1, 0, 0, 0, 0],
                Err(InvalidPeerPreface::Acknowledgement),
            ),
            (
                &[0, 0, 0, 0x4, 0, 0, 0, 0, 1],
                Err(InvalidPeerPreface::OnStream { stream_id: 1 }),
            ),
            (
                &[0, 0x40, 0x02, 0x4, 0, 0, 0, 0, 0],
                Err(InvalidPeerPreface::PayloadLength {
                    payload_len: 16_386,
                }),
            ),
            (
                &[0, 0, 5, 0x4, 0, 0, 0, 0, 0],
                Err(InvalidPeerPreface::PayloadLength { payload_len: 5 }),
            ),
        ];

        for (opening, expected) in cases {
            assert_eq!(
                PrefaceProgress::default().advance(opening),
                expected,
                "opening {opening:?}"
            );
        }
    }
}
