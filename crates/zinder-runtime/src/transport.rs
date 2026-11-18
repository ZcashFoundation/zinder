//! Intra-Zinder gRPC channel construction and keep-alive policy.
//!
//! Every long-lived gRPC channel between two Zinder services (explorer ↔
//! query, query ↔ ingest, compat ↔ ingest) is built through
//! [`connect_zinder_grpc`]. The function attaches the
//! [`BearerTokenClientInterceptor`] required by ADR-0011, configures the
//! HTTP/2 + TCP keep-alive policy that ADR-0007 requires, and returns the
//! [`AuthenticatedChannel`] callers wrap in their service-specific client.
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

use thiserror::Error;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Endpoint;

use crate::auth::{
    AuthenticatedChannel, BearerToken, BearerTokenClientInterceptor, BearerTokenConnectError,
};

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

/// Opens a long-lived gRPC channel to another Zinder service.
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
    let endpoint = Endpoint::from_shared(endpoint.to_owned())
        .map_err(BearerTokenConnectError::InvalidEndpoint)?
        .http2_keep_alive_interval(ZINDER_GRPC_HTTP2_KEEPALIVE_INTERVAL)
        .keep_alive_while_idle(true)
        .keep_alive_timeout(ZINDER_GRPC_HTTP2_KEEPALIVE_TIMEOUT)
        .tcp_keepalive(Some(ZINDER_GRPC_TCP_KEEPALIVE));
    let channel = endpoint
        .connect()
        .await
        .map_err(BearerTokenConnectError::Transport)?;
    let interceptor = BearerTokenClientInterceptor::new(bearer_token)?;
    Ok(InterceptedService::new(channel, interceptor))
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
