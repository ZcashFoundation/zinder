//! The connector that opens every connection of an intra-Zinder channel.
//!
//! tonic runs it under the endpoint's connect timeout, so the name lookup,
//! the TCP connect, and the TLS handshake of every connection, including the
//! reconnects a channel makes on its own, share one bound. The connector runs
//! the TLS handshake itself so that it can see the first bytes the peer sends
//! over HTTP/2, which tonic and hyper do not expose.

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
type ZinderGrpcStream = FirstPeerBytes<Either<TcpStream, TlsStream<TcpStream>>>;

/// ALPN protocol identifier for HTTP/2 over TLS.
const ALPN_H2: &[u8] = b"h2";

/// Opens the connections of one intra-Zinder channel to `target`.
#[derive(Clone)]
pub(super) struct ZinderGrpcConnector {
    target: Uri,
    pub(super) tcp: TcpConnector,
    tls: Option<ZinderGrpcTls>,
    /// Taken by the first connection, which reports when the peer first sends
    /// bytes.
    first_peer_bytes: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl ZinderGrpcConnector {
    pub(super) fn new(target: Uri, tcp: TcpConnector, tls: Option<ZinderGrpcTls>) -> Self {
        Self {
            target,
            tcp,
            tls,
            first_peer_bytes: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns a copy whose first connection reports on the returned receiver
    /// once the peer sends its first bytes. The receiver fails when that
    /// connection ends first.
    pub(super) fn reporting_first_peer_bytes(&self) -> (Self, oneshot::Receiver<()>) {
        let (report, first_peer_bytes) = oneshot::channel();
        let connector = Self {
            first_peer_bytes: Arc::new(Mutex::new(Some(report))),
            ..self.clone()
        };
        (connector, first_peer_bytes)
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
        let connecting = self.tcp.call(self.target.clone());
        let tls = self.tls.clone();
        let report = self.first_peer_bytes.lock().take();
        Box::pin(async move {
            let tcp = connecting.await?.into_inner();
            let stream = match tls {
                None => Either::Left(tcp),
                Some(tls) => Either::Right(tls.handshake(tcp).await?),
            };
            Ok(TokioIo::new(FirstPeerBytes { stream, report }))
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

/// A stream that reports when the peer first sends bytes over it.
pub(super) struct FirstPeerBytes<S> {
    stream: S,
    report: Option<oneshot::Sender<()>>,
}

impl<S: AsyncRead + Unpin> AsyncRead for FirstPeerBytes<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let filled_before = buf.filled().len();
        let polled = Pin::new(&mut self.stream).poll_read(cx, buf);
        if buf.filled().len() > filled_before
            && let Some(report) = self.report.take()
        {
            let _ = report.send(());
        }
        polled
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for FirstPeerBytes<S> {
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
