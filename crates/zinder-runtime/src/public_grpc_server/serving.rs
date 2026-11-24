//! The connection loop that serves every public gRPC listener.
//!
//! tonic's own server spawns each HTTP/2 stream task on an executor that a
//! caller cannot choose, so nothing can end one stream whose client stops
//! reading. This loop applies the listener's transport policy and runs each
//! stream task so that its response can reset it ([`super::stream_reset`]).
//! Everything else follows tonic's server: every request carries its
//! connection's [`TcpConnectInfo`], a client's `grpc-timeout` bounds the time
//! until its response starts, and a shutdown sends `GOAWAY` to every
//! connection and waits for them to close.
//!
//! Each connection's writes are also bounded: a connection whose writes make
//! no progress for the listener's write stall timeout closes, because its
//! client has stopped reading every stream on it.

use std::{convert::Infallible, future::Future, io, net::SocketAddr, time::Duration};

use bytes::Bytes;
use hyper::body::Incoming;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    server::conn::auto,
};
use tokio::net::TcpListener;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::{
    body::Body,
    server::NamedService,
    service::{Routes, RoutesBuilder},
    transport::server::TcpConnectInfo,
};
use tower::{
    Layer, Service, ServiceBuilder, ServiceExt as _,
    layer::util::{Identity, Stack},
};

use super::{
    GrpcDeadline, HTTP2_KEEPALIVE_INTERVAL, HTTP2_KEEPALIVE_TIMEOUT, HTTP2_MAX_LOCAL_ERROR_RESETS,
    MAX_CONCURRENT_STREAMS_PER_CONNECTION, grpc_timeout,
    release_gate::{GateObserver, ReleaseGate},
    stream_reset::{ResettableStreams, resettable},
};
use crate::http_listener::{accept_failure_needs_backoff, connection_io::ConnectionIo};

/// Pause after a failed `accept` so descriptor exhaustion does not spin the
/// loop.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// A public gRPC listener's transport policy and the layers around its
/// services.
///
/// [`crate::public_grpc_server`] returns one. Add services with
/// [`Self::add_service`], then serve the [`PublicGrpcRouter`] it returns.
#[derive(Clone, Debug)]
pub struct PublicGrpcServer<L = Identity, T = Identity> {
    layers: ServiceBuilder<L>,
    transport_layers: ServiceBuilder<T>,
    accept_http1: bool,
    send_stall_timeout: Duration,
    write_stall_timeout: Duration,
    gate_observer: GateObserver,
}

impl PublicGrpcServer {
    /// A server with no layers that resets a response's transport once it has
    /// held the response's data for `send_stall_timeout`, and closes a
    /// connection that writes too little for as long once a write blocks.
    pub(super) fn new(send_stall_timeout: Duration) -> Self {
        Self {
            layers: ServiceBuilder::new(),
            transport_layers: ServiceBuilder::new(),
            accept_http1: false,
            send_stall_timeout,
            write_stall_timeout: send_stall_timeout,
            gate_observer: GateObserver::default(),
        }
    }

    /// Replaces the connection write stall timeout, so a test can tell the
    /// two bounds apart.
    #[cfg(test)]
    pub(super) const fn with_write_stall_timeout(mut self, write_stall_timeout: Duration) -> Self {
        self.write_stall_timeout = write_stall_timeout;
        self
    }

    /// Reports to `gate_observer` when a response waits for its probe.
    #[cfg(test)]
    pub(super) fn with_gate_observer(mut self, gate_observer: GateObserver) -> Self {
        self.gate_observer = gate_observer;
        self
    }
}

impl<L, T> PublicGrpcServer<L, T> {
    /// Also accepts HTTP/1.1 connections, which gRPC-Web clients use.
    ///
    /// An HTTP/1.1 connection carries one response at a time, so a response
    /// whose client stops reading it closes its connection.
    #[must_use]
    pub fn accept_http1(mut self, accept_http1: bool) -> Self {
        self.accept_http1 = accept_http1;
        self
    }

    /// Wraps every service in `layer`, inside the deadline and the layers added before it.
    #[must_use]
    pub fn layer<NewLayer>(self, layer: NewLayer) -> PublicGrpcServer<Stack<NewLayer, L>, T> {
        PublicGrpcServer {
            layers: self.layers.layer(layer),
            transport_layers: self.transport_layers,
            accept_http1: self.accept_http1,
            send_stall_timeout: self.send_stall_timeout,
            write_stall_timeout: self.write_stall_timeout,
            gate_observer: self.gate_observer,
        }
    }

    /// Wraps the deadline and services in a transport adapter, inside
    /// transport adapters added before it. Use this for HTTP translation
    /// that also needs to format a gRPC timeout response.
    #[must_use]
    pub fn transport_layer<NewLayer>(
        self,
        layer: NewLayer,
    ) -> PublicGrpcServer<L, Stack<NewLayer, T>> {
        PublicGrpcServer {
            layers: self.layers,
            transport_layers: self.transport_layers.layer(layer),
            accept_http1: self.accept_http1,
            send_stall_timeout: self.send_stall_timeout,
            write_stall_timeout: self.write_stall_timeout,
            gate_observer: self.gate_observer,
        }
    }

    /// Starts the listener's router with `service`.
    pub fn add_service<S>(self, service: S) -> PublicGrpcRouter<L, T>
    where
        S: Service<http::Request<Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: axum::response::IntoResponse,
        S::Future: Send + 'static,
    {
        PublicGrpcRouter {
            server: self,
            routes: RoutesBuilder::default(),
        }
        .add_service(service)
    }
}

/// The services of a public gRPC listener, ready to serve.
#[derive(Debug)]
pub struct PublicGrpcRouter<L = Identity, T = Identity> {
    server: PublicGrpcServer<L, T>,
    routes: RoutesBuilder,
}

impl<L, T> PublicGrpcRouter<L, T> {
    /// Adds `service` to the listener.
    #[must_use]
    pub fn add_service<S>(mut self, service: S) -> Self
    where
        S: Service<http::Request<Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: axum::response::IntoResponse,
        S::Future: Send + 'static,
    {
        self.routes.add_service(service);
        self
    }
}

impl<L, T, InnerBody> PublicGrpcRouter<L, T>
where
    L: Layer<Routes>,
    L::Service: Service<http::Request<Body>, Response = http::Response<InnerBody>>,
    <L::Service as Service<http::Request<Body>>>::Error: 'static,
    <L::Service as Service<http::Request<Body>>>::Future: Send + 'static,
    InnerBody: http_body::Body<Data = Bytes> + Send + 'static,
    InnerBody::Error: Into<axum::BoxError>,
    T: Layer<GrpcDeadline<L::Service>>,
{
    /// Binds `addr` and serves until `signal` completes.
    ///
    /// Once `signal` completes, the listener stops accepting connections,
    /// sends `GOAWAY` on every open one, and returns after they have closed.
    ///
    /// # Errors
    ///
    /// Returns an error when `addr` cannot be bound.
    pub async fn serve_with_shutdown<ResponseBody>(
        self,
        addr: SocketAddr,
        signal: impl Future<Output = ()>,
    ) -> io::Result<()>
    where
        T::Service: Service<http::Request<Body>, Response = http::Response<ResponseBody>>
            + Clone
            + Send
            + 'static,
        <T::Service as Service<http::Request<Body>>>::Future: Send + 'static,
        <T::Service as Service<http::Request<Body>>>::Error: Into<axum::BoxError>,
        ResponseBody: http_body::Body<Data = Bytes> + Send + 'static,
        ResponseBody::Error: Into<axum::BoxError>,
    {
        let listener = TcpListener::bind(addr).await?;
        self.serve_with_incoming_shutdown(listener, signal).await
    }

    /// Serves the connections `listener` accepts until the process stops.
    ///
    /// # Errors
    ///
    /// Never returns an error; the result matches the other serve methods.
    pub async fn serve_with_incoming<ResponseBody>(self, listener: TcpListener) -> io::Result<()>
    where
        T::Service: Service<http::Request<Body>, Response = http::Response<ResponseBody>>
            + Clone
            + Send
            + 'static,
        <T::Service as Service<http::Request<Body>>>::Future: Send + 'static,
        <T::Service as Service<http::Request<Body>>>::Error: Into<axum::BoxError>,
        ResponseBody: http_body::Body<Data = Bytes> + Send + 'static,
        ResponseBody::Error: Into<axum::BoxError>,
    {
        self.serve_with_incoming_shutdown(listener, std::future::pending())
            .await
    }

    /// Serves the connections `listener` accepts until `signal` completes,
    /// shutting down as [`Self::serve_with_shutdown`] does.
    ///
    /// # Errors
    ///
    /// Never returns an error; the result matches
    /// [`Self::serve_with_shutdown`].
    pub async fn serve_with_incoming_shutdown<ResponseBody>(
        self,
        listener: TcpListener,
        signal: impl Future<Output = ()>,
    ) -> io::Result<()>
    where
        T::Service: Service<http::Request<Body>, Response = http::Response<ResponseBody>>
            + Clone
            + Send
            + 'static,
        <T::Service as Service<http::Request<Body>>>::Future: Send + 'static,
        <T::Service as Service<http::Request<Body>>>::Error: Into<axum::BoxError>,
        ResponseBody: http_body::Body<Data = Bytes> + Send + 'static,
        ResponseBody::Error: Into<axum::BoxError>,
    {
        let routes = self.routes.routes().prepare();
        let middleware = self.server.layers.service(routes);
        let service = self
            .server
            .transport_layers
            .service(GrpcDeadline::new(middleware));
        let connections = Connections {
            builder: connection_builder(self.server.accept_http1, self.server.send_stall_timeout),
            send_stall_timeout: self.server.send_stall_timeout,
            write_stall_timeout: self.server.write_stall_timeout,
            gate_observer: self.server.gate_observer,
        };
        connections.serve(listener, service, signal).await;
        Ok(())
    }
}

/// How the listener serves each connection it accepts.
struct Connections {
    builder: auto::Builder<ResettableStreams>,
    send_stall_timeout: Duration,
    write_stall_timeout: Duration,
    gate_observer: GateObserver,
}

impl Connections {
    async fn serve<S, ResponseBody>(
        self,
        listener: TcpListener,
        service: S,
        shutdown: impl Future<Output = ()>,
    ) where
        S: Service<http::Request<Body>, Response = http::Response<ResponseBody>>
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<axum::BoxError>,
        ResponseBody: http_body::Body<Data = Bytes> + Send + 'static,
        ResponseBody::Error: Into<axum::BoxError>,
    {
        let connections = TaskTracker::new();
        let drain = CancellationToken::new();
        // A caller that drops this future before `shutdown` completes still
        // drains the connections it accepted, as tonic's server does.
        let _drain_on_drop = drain.clone().drop_guard();
        let mut shutdown = std::pin::pin!(shutdown);
        loop {
            let accepted = tokio::select! {
                accepted = listener.accept() => accepted,
                () = &mut shutdown => break,
            };
            let (stream, remote_addr) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    if !accept_failure_needs_backoff(&error, "grpc_accept_failed") {
                        continue;
                    }
                    tokio::select! {
                        () = tokio::time::sleep(ACCEPT_ERROR_BACKOFF) => continue,
                        () = &mut shutdown => break,
                    }
                }
            };
            if let Err(error) = stream.set_nodelay(true) {
                tracing::debug!(
                    target: "zinder::runtime",
                    event = "grpc_nodelay_failed",
                    error = %error,
                    "failed to disable Nagle's algorithm on a gRPC connection"
                );
            }
            let connect_info = TcpConnectInfo {
                local_addr: stream.local_addr().ok(),
                remote_addr: Some(remote_addr),
            };
            let io = ConnectionIo::new(TokioIo::new(stream), self.write_stall_timeout, None);
            let requests =
                connection_service(service.clone(), connect_info, self.gate_observer.clone());
            connections.spawn(resettable(
                serve_connection(self.builder.clone(), io, requests, drain.clone()),
                self.send_stall_timeout,
            ));
        }

        drop(listener);
        drain.cancel();
        connections.close();
        connections.wait().await;
    }
}

/// Serves one connection until it closes, starting a graceful shutdown once
/// `drain` is cancelled.
#[allow(
    clippy::significant_drop_tightening,
    reason = "the connection must stay alive across the select that drives it"
)]
async fn serve_connection<I, S>(
    builder: auto::Builder<ResettableStreams>,
    io: I,
    requests: S,
    drain: CancellationToken,
) where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
    S: hyper::service::Service<
            http::Request<Incoming>,
            Response = http::Response<Body>,
            Error = axum::BoxError,
        >,
    S::Future: Send + 'static,
{
    let mut connection = std::pin::pin!(builder.serve_connection(io, requests));
    let mut draining = false;
    loop {
        tokio::select! {
            outcome = connection.as_mut() => {
                if let Err(error) = outcome {
                    tracing::debug!(
                        target: "zinder::runtime",
                        event = "grpc_connection_closed",
                        error = %error,
                        "gRPC connection ended with an error"
                    );
                }
                break;
            }
            () = drain.cancelled(), if !draining => {
                draining = true;
                connection.as_mut().graceful_shutdown();
            }
        }
    }
}

/// The HTTP/2 and HTTP/1.1 settings of every public listener connection.
fn connection_builder(
    accept_http1: bool,
    send_stall_timeout: Duration,
) -> auto::Builder<ResettableStreams> {
    let mut builder = auto::Builder::new(ResettableStreams::new(send_stall_timeout));
    if !accept_http1 {
        builder = builder.http2_only();
    }
    builder
        .http2()
        .timer(TokioTimer::new())
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS_PER_CONNECTION)
        .keep_alive_interval(HTTP2_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(HTTP2_KEEPALIVE_TIMEOUT)
        .max_local_error_reset_streams(HTTP2_MAX_LOCAL_ERROR_RESETS);
    // hyper enforces its HTTP/1.1 header read timeout only with a timer.
    builder.http1().timer(TokioTimer::new());
    builder
}

/// Adapts `service` to one connection.
///
/// Each request carries the connection's `connect_info`. Each response is
/// sent through a [`ReleaseGate`].
fn connection_service<S, ResponseBody>(
    service: S,
    connect_info: TcpConnectInfo,
    gate_observer: GateObserver,
) -> impl hyper::service::Service<
    http::Request<Incoming>,
    Response = http::Response<Body>,
    Error = axum::BoxError,
    Future: Send + 'static,
>
where
    S: Service<http::Request<Body>, Response = http::Response<ResponseBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<axum::BoxError>,
    ResponseBody: http_body::Body<Data = Bytes> + Send + 'static,
    ResponseBody::Error: Into<axum::BoxError>,
{
    hyper::service::service_fn(move |request: http::Request<Incoming>| {
        let mut request = request.map(Body::new);
        request.extensions_mut().insert(connect_info.clone());
        let gate = ReleaseGate {
            probe_window: request.version() == http::Version::HTTP_2,
            observer: gate_observer.clone(),
        };
        let mut service = service.clone();
        async move {
            service.ready().await.map_err(Into::into)?;
            let response = grpc_timeout::transport_response(service.call(request).await)?;
            Ok(gate.apply(response))
        }
    })
}

#[cfg(test)]
mod tests;
