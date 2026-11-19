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
use tonic::transport::{ClientTlsConfig, Endpoint};

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

/// Default time a dial to another Zinder service may spend establishing the
/// connection, from the TCP connect through the TLS and HTTP/2 handshakes.
///
/// Intra-Zinder peers share a host or a network, so a healthy dial completes
/// in milliseconds; five seconds leaves room for a loaded peer while keeping
/// a dropped SYN or a paused peer from holding a caller for the kernel's
/// connect timeout. See ADR-0007.
pub const DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

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
/// The bound covers the whole dial, so a peer that accepts the TCP connection
/// but never completes the TLS or HTTP/2 handshake also fails within it.
pub async fn connect_zinder_grpc_with_timeout(
    endpoint: &str,
    bearer_token: Option<&BearerToken>,
    connect_timeout: Duration,
) -> Result<AuthenticatedChannel, BearerTokenConnectError> {
    let endpoint = zinder_grpc_endpoint(endpoint)
        .map_err(BearerTokenConnectError::InvalidEndpoint)?
        .http2_keep_alive_interval(ZINDER_GRPC_HTTP2_KEEPALIVE_INTERVAL)
        .keep_alive_while_idle(true)
        .keep_alive_timeout(ZINDER_GRPC_HTTP2_KEEPALIVE_TIMEOUT)
        .tcp_keepalive(Some(ZINDER_GRPC_TCP_KEEPALIVE));
    let channel = tokio::time::timeout(connect_timeout, endpoint.connect())
        .await
        .map_err(|_| BearerTokenConnectError::ConnectTimeout { connect_timeout })?
        .map_err(BearerTokenConnectError::Transport)?;
    let interceptor = BearerTokenClientInterceptor::new(bearer_token)?;
    Ok(InterceptedService::new(channel, interceptor))
}

/// Parses `endpoint`, with TLS verified against the platform's trusted roots
/// when its scheme is `https`. An `http` endpoint stays plaintext HTTP/2.
fn zinder_grpc_endpoint(endpoint: &str) -> Result<Endpoint, tonic::transport::Error> {
    let parsed = Endpoint::from_shared(endpoint.to_owned())?;
    if parsed.uri().scheme_str() == Some("https") {
        return parsed.tls_config(ClientTlsConfig::new().with_enabled_roots());
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
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use std::time::Duration;

    use tokio::{io::AsyncReadExt as _, net::TcpListener};

    use super::{
        DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT, connect_zinder_grpc, connect_zinder_grpc_with_timeout,
        zinder_grpc_endpoint,
    };
    use crate::auth::BearerTokenConnectError;

    /// First byte of a TLS handshake record.
    const TLS_HANDSHAKE_RECORD: u8 = 0x16;

    /// Connects to a listener that records the first byte the client sends.
    async fn first_byte_sent(scheme: &str) -> Result<u8, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = zinder_grpc_endpoint(&format!("{scheme}://{}", listener.local_addr()?))?
            .connect_timeout(Duration::from_secs(5));
        let client = tokio::spawn(async move { endpoint.connect().await });
        let (mut socket, _) = listener.accept().await?;
        let mut first = [0_u8; 1];
        socket.read_exact(&mut first).await?;
        drop(socket);
        client.abort();
        Ok(first[0])
    }

    #[tokio::test]
    async fn https_endpoints_open_with_a_tls_handshake() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(first_byte_sent("https").await?, TLS_HANDSHAKE_RECORD);
        Ok(())
    }

    #[tokio::test]
    async fn http_endpoints_stay_plaintext_http2() -> Result<(), Box<dyn std::error::Error>> {
        // The HTTP/2 connection preface starts with "PRI".
        assert_eq!(first_byte_sent("http").await?, b'P');
        Ok(())
    }

    #[tokio::test]
    async fn a_dial_to_a_peer_that_never_answers_fails_within_the_given_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        // The listener's backlog accepts the TCP connection, but nothing
        // answers the TLS handshake.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://{}", listener.local_addr()?);
        let connect_timeout = Duration::from_millis(200);

        let started_at = std::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            connect_zinder_grpc_with_timeout(&endpoint, None, connect_timeout),
        )
        .await?;
        let elapsed = started_at.elapsed();

        assert!(
            matches!(
                outcome,
                Err(BearerTokenConnectError::ConnectTimeout { connect_timeout: bound })
                    if bound == connect_timeout
            ),
            "stalled dial must report the connect timeout, got {outcome:?}"
        );
        assert!(
            elapsed >= connect_timeout && elapsed < Duration::from_secs(2),
            "dial took {elapsed:?}"
        );
        drop(listener);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_dial_gives_up_within_the_default_bound() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://{}", listener.local_addr()?);

        let outcome = tokio::time::timeout(
            DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT + Duration::from_secs(1),
            connect_zinder_grpc(&endpoint, None),
        )
        .await?;

        assert!(
            matches!(
                outcome,
                Err(BearerTokenConnectError::ConnectTimeout { connect_timeout })
                    if connect_timeout == DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT
            ),
            "stalled dial must report the default connect timeout, got {outcome:?}"
        );
        drop(listener);
        Ok(())
    }
}
