//! Intra-Zinder gRPC channel construction and keep-alive policy.
//!
//! Every long-lived gRPC channel between two Zinder services (explorer ↔
//! query, query ↔ ingest, compat ↔ ingest) is built through
//! [`connect_zinder_grpc`]. The function attaches the
//! [`BearerTokenClientInterceptor`] required by ADR-0011, configures the
//! HTTP/2 + TCP keep-alive policy that ADR-0007 requires, and returns the
//! [`AuthenticatedChannel`] callers wrap in their service-specific client.
//! [`connect_zinder_grpc_at_startup`] dials the same way for a runtime that is
//! still starting, and [`LazyZinderGrpcChannel`] dials on first use and shares
//! that dial among concurrent callers.
//!
//! Direct construction of a [`tonic::transport::Endpoint`] outside this
//! module is a forbidden pattern; the structural invariant in
//! `crates/zinder-source/tests/transport_invariants.rs` will fail. New
//! cross-service plumbing reuses this function.
//!
//! Zebra-facing transports live in `zinder-source::transport`; this
//! module is intra-Zinder only, per the boundary declared in
//! `lib.rs` ("deliberately exposes no domain types") and ADR-0006.

use std::time::Duration;

use bytes::Bytes;
use http::{HeaderValue, Method, Uri, header};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tower::ServiceExt as _;

use crate::auth::{
    AuthenticatedChannel, BearerToken, BearerTokenClientInterceptor, BearerTokenConnectError,
};

mod lazy_channel;

pub use lazy_channel::LazyZinderGrpcChannel;

/// HTTP/2 ping interval that keeps idle intra-Zinder channels honest.
///
/// Without this, a `tonic::Channel` that has been idle since the peer
/// restarted reports healthy while `.await`s hang indefinitely (tonic
/// issues #1254 and #1635). Both endpoints of intra-Zinder traffic are
/// Zinder processes that permit keep-alive-without-calls, so the
/// aggressive 30-second cadence is safe here. See ADR-0007.
const ZINDER_GRPC_HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum time a single HTTP/2 keep-alive ping waits for an ACK before
/// the channel treats the connection as dead.
const ZINDER_GRPC_HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// TCP keep-alive probe interval, catching the case where the peer
/// process vanished but the kernel still reports the socket open. HTTP/2
/// keep-alive alone cannot detect this on every platform.
const ZINDER_GRPC_TCP_KEEPALIVE: Duration = Duration::from_mins(1);

/// Default time a dial to another Zinder service may take, from the TCP
/// connect until the peer answers its first HTTP/2 request.
///
/// Intra-Zinder peers share a host or a network, so a healthy dial completes
/// in milliseconds; five seconds leaves room for a loaded peer while keeping
/// a dropped SYN or a paused peer from holding a caller for the kernel's
/// connect timeout. See ADR-0007.
pub const DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Pause between startup dials that timed out.
const STARTUP_DIAL_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Path of the standard gRPC health check, which a dial calls to confirm the
/// peer speaks HTTP/2. Any HTTP response counts, including `UNIMPLEMENTED`
/// from a server that does not serve the health service.
const DIAL_READINESS_PROBE_PATH: &str = "/grpc.health.v1.Health/Check";

/// One uncompressed, zero-length gRPC message: an empty `HealthCheckRequest`.
const EMPTY_GRPC_MESSAGE: [u8; 5] = [0; 5];

/// Opens a long-lived gRPC channel to another Zinder service, giving up after
/// [`DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT`].
///
/// The returned [`AuthenticatedChannel`] carries the bearer-token
/// interceptor required by ADR-0011 and the HTTP/2 + TCP keep-alive
/// policy required by ADR-0007. Callers wrap it in a generated tonic
/// client (e.g. `WalletQueryClient::new(channel)`) and treat the
/// resulting client as `Clone`-cheap to share across tasks.
pub async fn connect_zinder_grpc(
    endpoint: &str,
    bearer_token: Option<&BearerToken>,
) -> Result<AuthenticatedChannel, BearerTokenConnectError> {
    connect_zinder_grpc_with_timeout(endpoint, bearer_token, DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT)
        .await
}

/// Opens a long-lived gRPC channel like [`connect_zinder_grpc`], giving up
/// after `connect_timeout` with [`BearerTokenConnectError::ConnectTimeout`].
///
/// The bound covers the whole dial: the TCP connect, the TLS handshake, and
/// a first HTTP/2 request the peer must answer. A peer that accepts the TCP
/// connection or completes TLS but never speaks HTTP/2 therefore fails within
/// it. When the channel later reconnects on its own, the TCP connect and the
/// TLS handshake each stay bounded by `connect_timeout`.
pub async fn connect_zinder_grpc_with_timeout(
    endpoint: &str,
    bearer_token: Option<&BearerToken>,
    connect_timeout: Duration,
) -> Result<AuthenticatedChannel, BearerTokenConnectError> {
    let interceptor = BearerTokenClientInterceptor::new(bearer_token)?;
    let endpoint = zinder_grpc_endpoint(endpoint, connect_timeout, zinder_grpc_tls_config())
        .map_err(BearerTokenConnectError::InvalidEndpoint)?;
    let channel = dial(&endpoint, connect_timeout).await?;
    Ok(InterceptedService::new(channel, interceptor))
}

/// Opens a channel like [`connect_zinder_grpc`] for a runtime that is still
/// starting, dialing again after each dial that times out until one connects
/// or `cancel` fires.
///
/// A peer that is slow to come up delays startup instead of ending it. Every
/// other error returns at once: an invalid endpoint, a refused connection, or
/// a failed TLS handshake ends startup. Returns `Ok(None)` when `cancel` fires
/// first.
pub async fn connect_zinder_grpc_at_startup(
    endpoint: &str,
    bearer_token: Option<&BearerToken>,
    cancel: &CancellationToken,
) -> Result<Option<AuthenticatedChannel>, BearerTokenConnectError> {
    connect_with_startup_retries(
        endpoint,
        bearer_token,
        DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT,
        cancel,
    )
    .await
}

async fn connect_with_startup_retries(
    endpoint: &str,
    bearer_token: Option<&BearerToken>,
    connect_timeout: Duration,
    cancel: &CancellationToken,
) -> Result<Option<AuthenticatedChannel>, BearerTokenConnectError> {
    loop {
        let dial = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(None),
            dial = connect_zinder_grpc_with_timeout(endpoint, bearer_token, connect_timeout) => dial,
        };
        match dial {
            Ok(channel) => return Ok(Some(channel)),
            Err(error @ BearerTokenConnectError::ConnectTimeout { .. }) => {
                tracing::warn!(
                    target: "zinder::runtime",
                    event = "zinder_grpc_startup_dial_timed_out",
                    endpoint,
                    %error,
                    retry_delay_ms = STARTUP_DIAL_RETRY_DELAY.as_millis(),
                    "Zinder gRPC peer did not answer the startup dial; dialing again"
                );
            }
            Err(error) => return Err(error),
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(None),
            () = tokio::time::sleep(STARTUP_DIAL_RETRY_DELAY) => {}
        }
    }
}

/// Connects `endpoint` and waits for the peer to answer one HTTP/2 request,
/// all within `connect_timeout`.
///
/// The HTTP/2 client handshake completes once the client has sent its
/// preface, so a connected channel alone does not prove the peer is serving.
async fn dial(
    endpoint: &Endpoint,
    connect_timeout: Duration,
) -> Result<Channel, BearerTokenConnectError> {
    let connect_and_probe = async {
        let channel = endpoint.connect().await?;
        await_http2_response(channel.clone()).await?;
        Ok(channel)
    };
    tokio::select! {
        // Polled first: the endpoint's own TCP and TLS bounds equal this one
        // and start no earlier, so when one fires in the same tick, the dial
        // still reports `ConnectTimeout`.
        biased;
        () = tokio::time::sleep(connect_timeout) => {
            Err(BearerTokenConnectError::ConnectTimeout { connect_timeout })
        }
        outcome = connect_and_probe => outcome.map_err(BearerTokenConnectError::Transport),
    }
}

/// Sends the readiness probe on `channel` and returns once the peer answers
/// with any HTTP response.
async fn await_http2_response(channel: Channel) -> Result<(), tonic::transport::Error> {
    let mut request = http::Request::new(tonic::body::Body::new(http_body_util::Full::new(
        Bytes::from_static(&EMPTY_GRPC_MESSAGE),
    )));
    *request.method_mut() = Method::POST;
    *request.uri_mut() = Uri::from_static(DIAL_READINESS_PROBE_PATH);
    let headers = request.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/grpc"),
    );
    headers.insert(header::TE, HeaderValue::from_static("trailers"));
    channel.oneshot(request).await.map(drop)
}

/// TLS settings for an `https` Zinder endpoint: the platform's trusted roots.
fn zinder_grpc_tls_config() -> ClientTlsConfig {
    ClientTlsConfig::new().with_enabled_roots()
}

/// Parses `endpoint` and applies the intra-Zinder transport policy, with TLS
/// under `tls` when its scheme is `https`. An `http` endpoint stays plaintext
/// HTTP/2.
///
/// `connect_timeout` bounds the TCP connect and the TLS handshake of every
/// connection the channel opens, including the reconnects tonic makes after
/// the first dial.
fn zinder_grpc_endpoint(
    endpoint: &str,
    connect_timeout: Duration,
    tls: ClientTlsConfig,
) -> Result<Endpoint, tonic::transport::Error> {
    let parsed = Endpoint::from_shared(endpoint.to_owned())?
        .connect_timeout(connect_timeout)
        .http2_keep_alive_interval(ZINDER_GRPC_HTTP2_KEEPALIVE_INTERVAL)
        .keep_alive_while_idle(true)
        .keep_alive_timeout(ZINDER_GRPC_HTTP2_KEEPALIVE_TIMEOUT)
        .tcp_keepalive(Some(ZINDER_GRPC_TCP_KEEPALIVE));
    if parsed.uri().scheme_str() == Some("https") {
        return parsed.tls_config(tls.timeout(connect_timeout));
    }
    Ok(parsed)
}

/// The single failure mode [`validate_zinder_grpc_endpoint`] can raise.
///
/// Used by config-loading code that wants to reject malformed URLs at
/// process start, before any connection is attempted.
#[derive(Debug, Error)]
#[error("invalid gRPC endpoint URL: {0}")]
pub struct InvalidZinderGrpcEndpoint(String);

/// Validates that `addr` parses as a gRPC endpoint URL.
///
/// Config loaders call this so a typo in a TOML field surfaces at
/// startup, not at the first request. The actual connect happens later
/// through [`connect_zinder_grpc`]; this seam exists so config code does
/// not need to depend on [`tonic::transport::Endpoint`] directly.
pub fn validate_zinder_grpc_endpoint(addr: &str) -> Result<(), InvalidZinderGrpcEndpoint> {
    Endpoint::from_shared(addr.to_owned())
        .map(|_| ())
        .map_err(|source| InvalidZinderGrpcEndpoint(source.to_string()))
}

#[cfg(test)]
mod tests;
