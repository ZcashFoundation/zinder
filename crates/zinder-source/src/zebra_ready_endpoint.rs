//! Zebra HTTP `/ready` endpoint probe used by
//! [`ZebraJsonRpcSource::poll_upstream_health`].
//!
//! Zebra exposes an opt-in HTTP health server with `/ready` returning
//! `200 OK` with body `ok` when the node is at network tip and
//! `503 Service Unavailable` with a documented sentinel body otherwise.
//! See the
//! [Zebra user guide](https://zebra.zfnd.org/user/health.html) and
//! [ADR-0009 §Upstream sync detection].
//!
//! [`ZebraJsonRpcSource::poll_upstream_health`]: crate::ZebraJsonRpcSource
//! [ADR-0009 §Upstream sync detection]:
//!     ../../../docs/adrs/0009-ingest-lifecycle-and-construction.md#upstream-sync-detection

use std::{borrow::Cow, time::Duration};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Empty, Limited};
use hyper::{Uri, body::Incoming, client::conn::http1, http::uri::Scheme};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

use crate::{
    UPSTREAM_HEALTH_REASON_INSUFFICIENT_PEERS, UPSTREAM_HEALTH_REASON_NO_TIP,
    UPSTREAM_HEALTH_REASON_SYNCING, UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT,
    UpstreamHealthSnapshot,
};

/// Maximum number of body bytes the probe will read from the endpoint.
///
/// Zebra's response bodies are short sentinel strings (under 64 bytes).
/// The cap defends against pathological proxies that stream unbounded
/// payloads from the configured URL.
const READY_RESPONSE_BODY_BYTES_CAP: usize = 4 * 1024;

/// Longest one probe waits for the endpoint's answer.
///
/// Zebra answers `/ready` at once with a short body, so a slower probe counts
/// as failed and the same health poll falls back to JSON-RPC. The bound keeps
/// an endpoint that accepts connections but never answers from using up the
/// deadline of a caller that also needs that fallback, such as ingest's
/// five-second switch check.
const READY_PROBE_TIMEOUT_CEILING: Duration = Duration::from_secs(2);

/// One-shot HTTP/1 probe of Zebra's `/ready` endpoint.
///
/// Opens a fresh TCP connection each call: the probe runs once per
/// `[node.health].poll_interval_ms` (default 30s), so connection reuse
/// adds nothing over the cost of a new handshake at that cadence.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ZebraReadyClient {
    request_timeout: Duration,
}

impl ZebraReadyClient {
    /// Builds a client for probes made every `poll_interval`. Each probe
    /// waits at most that interval, and never longer than two seconds.
    pub(crate) fn new(poll_interval: Duration) -> Self {
        Self {
            request_timeout: poll_interval.min(READY_PROBE_TIMEOUT_CEILING),
        }
    }

    /// Issues a `GET` against `addr` and returns the parsed snapshot.
    ///
    /// Errors map to `Err` so the caller can fall back to the JSON-RPC
    /// `verificationprogress` path per ADR-0009 (a transient `/ready`
    /// outage must not be reported as `upstream_not_ready`).
    pub(crate) async fn probe(
        &self,
        addr: &str,
    ) -> Result<UpstreamHealthSnapshot, ZebraReadyProbeError> {
        tokio::time::timeout(self.request_timeout, send_ready_probe(addr))
            .await
            .map_err(|_| ZebraReadyProbeError::Timeout)?
    }
}

async fn send_ready_probe(addr: &str) -> Result<UpstreamHealthSnapshot, ZebraReadyProbeError> {
    let uri: Uri = addr
        .parse()
        .map_err(
            |source: hyper::http::uri::InvalidUri| ZebraReadyProbeError::InvalidUri {
                reason: source.to_string(),
            },
        )?;

    let host = uri.host().ok_or_else(|| ZebraReadyProbeError::InvalidUri {
        reason: "ready probe URI is missing a host".to_owned(),
    })?;
    if uri.scheme() == Some(&Scheme::HTTPS) {
        return Err(ZebraReadyProbeError::InvalidUri {
            reason: "ready probe URI uses https; only http is supported".to_owned(),
        });
    }
    let port = uri.port_u16().unwrap_or(80);

    let stream = TcpStream::connect((host, port)).await.map_err(|source| {
        ZebraReadyProbeError::Transport {
            reason: source.to_string(),
        }
    })?;
    let (mut sender, conn) = http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|source| ZebraReadyProbeError::Transport {
            reason: source.to_string(),
        })?;

    // Drive the connection in the background. It terminates when the
    // server closes (we send `Connection: close`) or when `sender` is
    // dropped after the response body is read.
    tokio::spawn(async move {
        if let Err(error) = conn.await {
            tracing::debug!(
                target: "zinder::source",
                event = "ready_probe_connection_closed",
                reason = %error,
                "ready probe connection terminated"
            );
        }
    });

    let authority = uri
        .authority()
        .map_or_else(|| host.to_owned(), ToString::to_string);
    let path_and_query = uri
        .path_and_query()
        .map_or("/", hyper::http::uri::PathAndQuery::as_str)
        .to_owned();
    let request = hyper::Request::builder()
        .uri(path_and_query)
        .header(hyper::header::HOST, authority)
        .header(hyper::header::CONNECTION, "close")
        .body(Empty::<Bytes>::new())
        .map_err(|source| ZebraReadyProbeError::Transport {
            reason: source.to_string(),
        })?;

    let response =
        sender
            .send_request(request)
            .await
            .map_err(|source| ZebraReadyProbeError::Transport {
                reason: source.to_string(),
            })?;

    let status = response.status();
    let body_text = read_bounded_body(response.into_body())
        .await
        .map_err(|reason| ZebraReadyProbeError::BodyRead { reason })?;

    Ok(snapshot_from_ready_response(status, &body_text))
}

/// Reasons a `/ready` probe call may fail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ZebraReadyProbeError {
    /// The configured `[node.health].addr` is not a valid URI.
    InvalidUri { reason: String },
    /// The request exceeded the per-probe timeout.
    Timeout,
    /// The TCP/HTTP layer surfaced a transport error.
    Transport { reason: String },
    /// The response body could not be read.
    BodyRead { reason: String },
}

impl std::fmt::Display for ZebraReadyProbeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUri { reason } => write!(formatter, "invalid ready probe URI: {reason}"),
            Self::Timeout => write!(formatter, "ready probe timed out"),
            Self::Transport { reason } => {
                write!(formatter, "ready probe transport error: {reason}")
            }
            Self::BodyRead { reason } => {
                write!(formatter, "ready probe body read failed: {reason}")
            }
        }
    }
}

async fn read_bounded_body(body: Incoming) -> Result<String, String> {
    let bytes = Limited::new(body, READY_RESPONSE_BODY_BYTES_CAP)
        .collect()
        .await
        .map_err(|source| source.to_string())?
        .to_bytes();
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// Parses an HTTP `/ready` response into an [`UpstreamHealthSnapshot`].
///
/// Public-in-crate so the unit tests cover every sentinel without
/// standing up a hyper server.
pub(crate) fn snapshot_from_ready_response(
    status: hyper::StatusCode,
    body: &str,
) -> UpstreamHealthSnapshot {
    if status.is_success() {
        return UpstreamHealthSnapshot::ready(
            UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT,
            None,
            None,
            None,
        );
    }
    let trimmed = body.trim();
    let reason = classify_not_ready_body(trimmed);
    UpstreamHealthSnapshot::not_ready(
        UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT,
        reason,
        None,
        None,
        None,
    )
}

fn classify_not_ready_body(body: &str) -> Cow<'static, str> {
    if body.eq_ignore_ascii_case(UPSTREAM_HEALTH_REASON_INSUFFICIENT_PEERS) {
        return Cow::Borrowed(UPSTREAM_HEALTH_REASON_INSUFFICIENT_PEERS);
    }
    if body.eq_ignore_ascii_case(UPSTREAM_HEALTH_REASON_SYNCING) {
        return Cow::Borrowed(UPSTREAM_HEALTH_REASON_SYNCING);
    }
    if body.eq_ignore_ascii_case(UPSTREAM_HEALTH_REASON_NO_TIP) {
        return Cow::Borrowed(UPSTREAM_HEALTH_REASON_NO_TIP);
    }
    if body.is_empty() {
        return Cow::Borrowed("not ready");
    }
    Cow::Owned(body.to_owned())
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use super::*;
    use crate::{UPSTREAM_HEALTH_REASON_OK, UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT};

    #[test]
    fn ok_body_emits_ready_snapshot() {
        let snapshot = snapshot_from_ready_response(hyper::StatusCode::OK, "ok");
        assert!(snapshot.ready_for_queries);
        assert_eq!(snapshot.source, UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT);
        assert_eq!(snapshot.reason.as_ref(), UPSTREAM_HEALTH_REASON_OK);
    }

    #[test]
    fn ok_trims_whitespace() {
        let snapshot = snapshot_from_ready_response(hyper::StatusCode::OK, "  ok\n");
        assert!(snapshot.ready_for_queries);
    }

    #[test]
    fn syncing_body_maps_to_syncing_reason() {
        let snapshot =
            snapshot_from_ready_response(hyper::StatusCode::SERVICE_UNAVAILABLE, "syncing");
        assert!(!snapshot.ready_for_queries);
        assert_eq!(snapshot.reason.as_ref(), UPSTREAM_HEALTH_REASON_SYNCING);
    }

    #[test]
    fn insufficient_peers_body_maps_to_insufficient_peers_reason() {
        let snapshot = snapshot_from_ready_response(
            hyper::StatusCode::SERVICE_UNAVAILABLE,
            "insufficient peers",
        );
        assert!(!snapshot.ready_for_queries);
        assert_eq!(
            snapshot.reason.as_ref(),
            UPSTREAM_HEALTH_REASON_INSUFFICIENT_PEERS
        );
    }

    #[test]
    fn no_tip_body_maps_to_no_tip_reason() {
        let snapshot =
            snapshot_from_ready_response(hyper::StatusCode::SERVICE_UNAVAILABLE, "no tip");
        assert!(!snapshot.ready_for_queries);
        assert_eq!(snapshot.reason.as_ref(), UPSTREAM_HEALTH_REASON_NO_TIP);
    }

    #[test]
    fn parametric_body_is_passed_through_verbatim() {
        let snapshot =
            snapshot_from_ready_response(hyper::StatusCode::SERVICE_UNAVAILABLE, "tip_age=42s");
        assert!(!snapshot.ready_for_queries);
        assert_eq!(snapshot.reason.as_ref(), "tip_age=42s");
    }

    #[test]
    fn empty_body_uses_neutral_not_ready_reason() {
        let snapshot = snapshot_from_ready_response(hyper::StatusCode::SERVICE_UNAVAILABLE, "");
        assert!(!snapshot.ready_for_queries);
        assert_eq!(snapshot.reason.as_ref(), "not ready");
    }
}
