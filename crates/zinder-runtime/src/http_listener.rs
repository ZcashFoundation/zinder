//! Hardened HTTP/1 listener shared by every Zinder runtime that serves an axum router.
//!
//! `axum::serve` installs no hyper timer, so hyper's header read timeout is
//! silently disabled and a client that opens a connection and sends nothing,
//! or sends request headers one byte at a time, holds that connection and its
//! task indefinitely. [`serve_http`] replaces it with a hyper HTTP/1 server
//! that bounds how long a connection may take to deliver request headers,
//! how long a request body may stall, how many connections are open at once,
//! and how large a request body may be. Connections upgraded to `WebSocket`
//! leave the HTTP deadlines behind once the upgrade completes.

use std::{convert::Infallible, future::Future, io, sync::Arc, time::Duration};

use axum::{Router, body::Body, extract::DefaultBodyLimit};
use hyper::{body::Incoming, server::conn::http1};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use tokio::{net::TcpListener, sync::Semaphore};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tower::ServiceExt;
use tower_http::timeout::TimeoutBody;

/// Longest a connection may take to deliver a complete request head.
///
/// The same deadline closes a keep-alive connection that stays idle between
/// requests, because hyper restarts the header timer for every request head.
pub const DEFAULT_HTTP_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest a request body may go without delivering the next frame.
pub const DEFAULT_HTTP_REQUEST_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default cap on concurrently open connections.
///
/// The cap protects file descriptors and tasks. It is not a request rate
/// limit, and a connection upgraded to `WebSocket` keeps its slot.
pub const DEFAULT_HTTP_MAX_CONNECTIONS: usize = 2048;

/// Default cap on a request body, matching axum's own extractor default.
pub const DEFAULT_HTTP_MAX_REQUEST_BODY_BYTES: usize = 2 * 1024 * 1024;

/// Pause after a failed `accept` so descriptor exhaustion does not spin the loop.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// Resource bounds applied by [`serve_http`].
#[derive(Clone, Copy, Debug)]
pub struct HttpListenerLimits {
    /// Longest a connection may take to deliver a complete request head, and
    /// the longest an idle keep-alive connection survives.
    pub header_read_timeout: Duration,
    /// Longest a request body may stall before the request fails.
    pub request_body_idle_timeout: Duration,
    /// Connections beyond this count are closed as soon as they are accepted.
    pub max_connections: usize,
    /// Largest request body that axum body extractors accept.
    pub max_request_body_bytes: usize,
}

impl Default for HttpListenerLimits {
    fn default() -> Self {
        Self {
            header_read_timeout: DEFAULT_HTTP_HEADER_READ_TIMEOUT,
            request_body_idle_timeout: DEFAULT_HTTP_REQUEST_BODY_IDLE_TIMEOUT,
            max_connections: DEFAULT_HTTP_MAX_CONNECTIONS,
            max_request_body_bytes: DEFAULT_HTTP_MAX_REQUEST_BODY_BYTES,
        }
    }
}

/// Serves `router` over HTTP/1 on `listener` until `shutdown` completes.
///
/// Behaves like `axum::serve(listener, router).with_graceful_shutdown(shutdown)`
/// with the bounds in `limits` applied. Once `shutdown` completes, the
/// listener stops accepting, idle connections close, and the call returns after
/// every in-flight connection has finished. `WebSocket` upgrades stay enabled,
/// and an upgraded connection ends when its own handler ends.
///
/// The listener speaks HTTP/1 only. No Zinder client uses cleartext HTTP/2,
/// and the HTTP/1 connection type is the one that bounds an idle connection
/// that has not yet sent a byte.
pub async fn serve_http<Shutdown>(
    listener: TcpListener,
    router: Router,
    limits: HttpListenerLimits,
    shutdown: Shutdown,
) -> io::Result<()>
where
    Shutdown: Future<Output = ()> + Send + 'static,
{
    let router = router.layer(DefaultBodyLimit::max(limits.max_request_body_bytes));
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_read_timeout)
        .keep_alive(true);

    let connection_slots = Arc::new(Semaphore::new(limits.max_connections));
    let connections = TaskTracker::new();
    let drain = CancellationToken::new();
    tokio::pin!(shutdown);

    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            () = &mut shutdown => break,
        };
        let stream = match accepted {
            Ok((stream, _peer)) => stream,
            Err(error) => {
                tracing::warn!(
                    target: "zinder::runtime",
                    event = "http_accept_failed",
                    error = %error,
                    "failed to accept an HTTP connection"
                );
                tokio::select! {
                    () = tokio::time::sleep(ACCEPT_ERROR_BACKOFF) => continue,
                    () = &mut shutdown => break,
                }
            }
        };
        let Ok(slot) = Arc::clone(&connection_slots).try_acquire_owned() else {
            tracing::debug!(
                target: "zinder::runtime",
                event = "http_connection_refused",
                max_connections = limits.max_connections,
                "refused an HTTP connection over the concurrency limit"
            );
            drop(stream);
            continue;
        };

        let service = hyper_service(&router, limits.request_body_idle_timeout);
        let connection = builder
            .serve_connection(TokioIo::new(stream), service)
            .with_upgrades();
        let drain = drain.clone();
        connections.spawn(async move {
            let mut connection = std::pin::pin!(connection);
            let mut draining = false;
            loop {
                tokio::select! {
                    outcome = connection.as_mut() => {
                        if let Err(error) = outcome {
                            tracing::debug!(
                                target: "zinder::runtime",
                                event = "http_connection_closed",
                                error = %error,
                                "HTTP connection ended with an error"
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
            drop(slot);
        });
    }

    drop(listener);
    drain.cancel();
    connections.close();
    connections.wait().await;
    Ok(())
}

/// Adapts the router to hyper, failing any request body that stalls for
/// longer than `body_idle_timeout` between frames.
fn hyper_service(
    router: &Router,
    body_idle_timeout: Duration,
) -> impl hyper::service::Service<
    hyper::Request<Incoming>,
    Response = axum::response::Response,
    Error = Infallible,
    Future: Send,
> + Clone
+ use<> {
    TowerToHyperService::new(router.clone().map_request(
        move |request: hyper::Request<Incoming>| {
            request.map(|body| Body::new(TimeoutBody::new(body_idle_timeout, body)))
        },
    ))
}
