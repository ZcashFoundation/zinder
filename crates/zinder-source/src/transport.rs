//! Construction and keep-alive policy for every long-lived HTTP/gRPC
//! client this crate opens to a Zebra full node.
//!
//! This module is the canonical home for the contract Zinder enforces
//! when talking to Zebra: which keep-alive settings each protocol
//! receives, which knobs are deliberately *not* set (and why), and how
//! callers convert a transport-layer failure into the
//! [`crate::SourceError`] vocabulary the upstream-source contract uses.
//!
//! Direct construction of [`jsonrpsee::http_client::HttpClientBuilder`],
//! [`tonic::transport::Endpoint::from_shared`], or
//! [`reqwest::Client::builder`] outside this module is a forbidden
//! pattern; the `transport_invariants.rs` structural test rejects new
//! call sites. New Zebra-facing adapters add a factory here and consume
//! it.
//!
//! See [ADR-0019](../../../docs/adrs/0019-transport-policy-ownership.md)
//! for the boundary and policy decisions this module enforces.

use std::future::Future;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use futures_util::future::BoxFuture;
use jsonrpsee::http_client::{HeaderMap, HttpClient, HttpClientBuilder};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
use thiserror::Error;
use tokio::sync::Mutex;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::{NodeIndexerTls, SourceError, SourceFailureClass, ZebraIndexerSourceTarget};

/// Consecutive transport-class failures that trigger a
/// [`ResilientClient`] rebuild.
///
/// Three is enough that a single jittered upstream blip does not churn
/// connections; small enough that a genuine wedge self-heals on the
/// call that crosses the threshold instead of waiting for an operator.
pub const ZEBRA_REBUILD_THRESHOLD: NonZeroU32 = NonZeroU32::MIN.saturating_add(2);

/// HTTP/2 ping interval on Zebra Indexer gRPC channels.
///
/// The Indexer's two server streams (`MempoolChange`, `ChainTipChange`)
/// can go quiet between mined blocks (~75 s on testnet). A 30 s ping
/// gives the kernel a chance to surface a dead peer well within the
/// typical NAT timeout window. We deliberately omit
/// `keep_alive_while_idle(true)` to avoid tonic issue #258
/// (`ENHANCE_YOUR_CALM` from servers that don't permit
/// keep-alive-without-calls); the streams are always active, so pings
/// flow regardless. See ADR-0019.
const ZEBRA_INDEXER_HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum time a single HTTP/2 keep-alive ping waits for an ACK before
/// the channel treats the connection as dead.
const ZEBRA_INDEXER_HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// TCP keep-alive probe interval. Catches dead-peer cases the HTTP/2
/// layer's PING/PONG cannot detect on every platform.
const ZEBRA_INDEXER_TCP_KEEPALIVE: Duration = Duration::from_secs(45);

/// Failure modes a [`build_zebra_json_rpc_client`] or
/// [`connect_zebra_indexer_channel`] call can raise.
///
/// Adapters translate this at the boundary into the
/// [`crate::SourceError`] variant their `Source*Stream` contract
/// expects.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ZebraTransportError {
    /// The endpoint URL could not be parsed.
    #[error("Zebra endpoint URL is invalid: {0}")]
    InvalidEndpoint(String),
    /// The transport could not connect to the endpoint.
    #[error("Zebra transport connect failed: {0}")]
    ConnectFailed(String),
    /// The HTTP/JSON-RPC client could not be built (header or builder error).
    #[error("Zebra HTTP client build failed: {0}")]
    ClientBuildFailed(String),
    /// An indexer TLS file could not be read or holds no usable PEM item.
    #[error("indexer TLS {role} file {} is unusable: {reason}", path.display())]
    TlsMaterialUnreadable {
        /// What the file supplies, such as `client key`.
        role: &'static str,
        /// Configured file path.
        path: PathBuf,
        /// Why the file was rejected. Never contains file contents.
        reason: String,
    },
    /// The indexer TLS settings were rejected before connecting.
    #[error("indexer TLS configuration rejected: {0}")]
    TlsConfigRejected(String),
}

/// Builds a jsonrpsee HTTP client for a Zebra full node's JSON-RPC.
///
/// jsonrpsee 0.26 deliberately hides the hyper connection-pool config
/// from `HttpClientBuilder`. To compensate, callers should:
///
/// - Set a tight `request_timeout` so hangs fail fast (the reconnect
///   loop above the client treats the resulting error as a transport
///   failure and rebuilds the client through `ResilientClient<C>`).
/// - Avoid sharing a single client across long network-availability
///   events. The `ResilientClient` wrapper enforces this; this function
///   produces the client it wraps.
///
/// `headers` typically carries a single `authorization` entry; pass an
/// empty `HeaderMap` for unauthenticated endpoints.
pub fn build_zebra_json_rpc_client(
    json_rpc_addr: &str,
    request_timeout: Duration,
    max_response_bytes: NonZeroU64,
    headers: HeaderMap,
) -> Result<HttpClient, ZebraTransportError> {
    let max_response_size = u32::try_from(max_response_bytes.get()).unwrap_or(u32::MAX);
    HttpClientBuilder::default()
        .request_timeout(request_timeout)
        .max_response_size(max_response_size)
        .set_headers(headers)
        .build(json_rpc_addr)
        .map_err(|error| ZebraTransportError::ClientBuildFailed(error.to_string()))
}

/// Runtime options for [`connect_zebra_indexer_channel`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZebraIndexerChannelOptions {
    /// Connect timeout for the indexer endpoint.
    pub connect_timeout: Duration,
    /// Per-RPC request timeout. The Indexer's two streams are
    /// long-lived, so adapters typically set this to a generous
    /// duration (≥ 1 minute).
    pub request_timeout: Duration,
}

/// Opens a long-lived gRPC channel to a Zebra Indexer endpoint.
///
/// The returned [`Channel`] carries the HTTP/2 + TCP keep-alive policy
/// mandated by ADR-0019. Callers wrap it in the generated Indexer client
/// (`IndexerClient::new(channel)`). Streaming sources open a fresh channel on
/// every subscription; [`ResilientClient`] does not wrap indexer channels.
///
/// An `https://` endpoint negotiates TLS. Its PEM files are read on every
/// call, so a subscription opened after a certificate rotation presents the
/// rotated files.
pub async fn connect_zebra_indexer_channel(
    target: &ZebraIndexerSourceTarget,
    options: ZebraIndexerChannelOptions,
) -> Result<Channel, ZebraTransportError> {
    zebra_indexer_endpoint(target, options)?
        .connect()
        .await
        .map_err(|error| ZebraTransportError::ConnectFailed(transport_error_chain(&error)))
}

/// Checks that an indexer channel to `target` can be built, without
/// connecting.
///
/// Parses the endpoint URL and, for an `https://` endpoint, reads and checks
/// every configured PEM file the way [`connect_zebra_indexer_channel`] does.
/// Startup calls this so a missing or empty certificate file fails the
/// process instead of every later subscription.
pub fn validate_zebra_indexer_endpoint(
    target: &ZebraIndexerSourceTarget,
) -> Result<(), ZebraTransportError> {
    zebra_indexer_endpoint(
        target,
        ZebraIndexerChannelOptions {
            connect_timeout: ZEBRA_INDEXER_TLS_VALIDATION_TIMEOUT,
            request_timeout: ZEBRA_INDEXER_TLS_VALIDATION_TIMEOUT,
        },
    )
    .map(drop)
}

/// Placeholder timeout for [`validate_zebra_indexer_endpoint`], which builds
/// an endpoint it never connects.
const ZEBRA_INDEXER_TLS_VALIDATION_TIMEOUT: Duration = Duration::from_secs(1);

fn zebra_indexer_endpoint(
    target: &ZebraIndexerSourceTarget,
    options: ZebraIndexerChannelOptions,
) -> Result<Endpoint, ZebraTransportError> {
    let endpoint = Endpoint::from_shared(target.endpoint_url.clone())
        .map_err(|error| ZebraTransportError::InvalidEndpoint(error.to_string()))?
        .connect_timeout(options.connect_timeout)
        .timeout(options.request_timeout)
        .http2_keep_alive_interval(ZEBRA_INDEXER_HTTP2_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(ZEBRA_INDEXER_HTTP2_KEEPALIVE_TIMEOUT)
        .tcp_keepalive(Some(ZEBRA_INDEXER_TCP_KEEPALIVE));
    if endpoint.uri().scheme_str() != Some("https") {
        if target.tls.is_some() {
            return Err(ZebraTransportError::TlsConfigRejected(
                "indexer TLS settings require an https:// indexer URL".to_owned(),
            ));
        }
        return Ok(endpoint);
    }
    let tls_config = zebra_indexer_tls_config(target.tls.as_ref(), options.connect_timeout)?;
    endpoint
        .tls_config(tls_config)
        .map_err(|error| ZebraTransportError::TlsConfigRejected(transport_error_chain(&error)))
}

fn zebra_indexer_tls_config(
    tls: Option<&NodeIndexerTls>,
    handshake_timeout: Duration,
) -> Result<ClientTlsConfig, ZebraTransportError> {
    let mut config = ClientTlsConfig::new().timeout(handshake_timeout);
    config = match tls.and_then(|tls| tls.ca_cert_path.as_deref()) {
        Some(ca_cert_path) => config.ca_certificate(Certificate::from_pem(read_pem_certificates(
            ca_cert_path,
            "CA certificate",
        )?)),
        None => config.with_native_roots(),
    };
    if let Some(identity) = tls.and_then(|tls| tls.client_identity.as_ref()) {
        let cert_chain = read_pem_certificates(&identity.cert_path, "client certificate")?;
        let key = read_pem_private_key(&identity.key_path)?;
        config = config.identity(Identity::from_pem(cert_chain, key));
    }
    if let Some(server_name) = tls.and_then(|tls| tls.server_name.as_ref()) {
        config = config.domain_name(server_name.clone());
    }
    Ok(config)
}

/// Reads a PEM file that must hold at least one certificate.
///
/// tonic skips unparsable entries, so a file without certificates would
/// otherwise yield an empty trust store or chain without an error.
fn read_pem_certificates(path: &Path, role: &'static str) -> Result<Vec<u8>, ZebraTransportError> {
    let pem = read_tls_file(path, role)?;
    let mut certificate_count = 0_usize;
    for certificate in CertificateDer::pem_slice_iter(&pem) {
        certificate.map_err(|error| tls_material_unreadable(path, role, error.to_string()))?;
        certificate_count = certificate_count.saturating_add(1);
    }
    if certificate_count == 0 {
        return Err(tls_material_unreadable(
            path,
            role,
            "contains no PEM certificate",
        ));
    }
    Ok(pem)
}

fn read_pem_private_key(path: &Path) -> Result<Vec<u8>, ZebraTransportError> {
    const ROLE: &str = "client key";
    let pem = read_tls_file(path, ROLE)?;
    PrivateKeyDer::from_pem_slice(&pem).map_err(|error| {
        let reason = if matches!(error, rustls_pki_types::pem::Error::NoItemsFound) {
            "contains no PEM private key".to_owned()
        } else {
            error.to_string()
        };
        tls_material_unreadable(path, ROLE, reason)
    })?;
    Ok(pem)
}

fn read_tls_file(path: &Path, role: &'static str) -> Result<Vec<u8>, ZebraTransportError> {
    std::fs::read(path).map_err(|error| tls_material_unreadable(path, role, error.to_string()))
}

fn tls_material_unreadable(
    path: &Path,
    role: &'static str,
    reason: impl Into<String>,
) -> ZebraTransportError {
    ZebraTransportError::TlsMaterialUnreadable {
        role,
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

/// Renders a tonic transport error with its sources, which carry the I/O or
/// rustls reason, such as an unknown certificate issuer, that tonic's own
/// message omits.
fn transport_error_chain(error: &tonic::transport::Error) -> String {
    let mut rendered = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        rendered.push_str(": ");
        rendered.push_str(&cause.to_string());
        source = cause.source();
    }
    rendered
}

/// Erased rebuilder closure: takes no arguments and produces a future
/// that resolves to a fresh client. Used by [`ResilientClient`] to swap
/// the inner client after consecutive transport-class failures.
type ClientRebuilder<C> =
    Arc<dyn Fn() -> BoxFuture<'static, Result<C, ZebraTransportError>> + Send + Sync>;

/// Self-healing wrapper around a `Clone` HTTP/gRPC client whose
/// underlying connection can silently rot.
///
/// jsonrpsee 0.26 (and to a lesser extent tonic 0.14) caches connections
/// inside the type, so a long-lived client whose socket is dead reports
/// healthy at the type level even though every subsequent `.await`
/// hangs or fails fast. The fix is structural: count consecutive
/// transport-class failures, and after a threshold, rebuild the client.
///
/// Adapters call [`Self::record_outcome`] on every RPC result. On the
/// call that crosses the threshold a background task swaps the inner
/// client through an atomic pointer (`arc_swap::ArcSwap`); readers pay
/// one atomic load via [`Self::snapshot`].
///
/// The wrapper is `Clone`: every clone shares the same self-healing
/// state through an `Arc`, so source structs that derive `Clone` keep
/// doing so without further work. See ADR-0019.
pub struct ResilientClient<C> {
    state: Arc<ResilientState<C>>,
}

impl<C> Clone for ResilientClient<C> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

struct ResilientState<C> {
    inner: ArcSwap<C>,
    consecutive_transport_failures: AtomicU32,
    rebuild_threshold: NonZeroU32,
    rebuilder: ClientRebuilder<C>,
    rebuild_lock: Mutex<()>,
    peer_label: String,
}

impl<C> ResilientClient<C>
where
    C: Send + Sync + 'static,
{
    /// Builds a self-healing wrapper around `initial`.
    ///
    /// `peer_label` is the value attached to the `peer` label on
    /// `zinder_transport_reconnect_total` and on `zinder::transport`
    /// log events. Use a stable, low-cardinality identifier such as
    /// `"zebra_json_rpc"`; never use a host:port.
    ///
    /// `rebuilder` is invoked from a background task whenever the
    /// failure counter crosses `rebuild_threshold`. The closure may
    /// capture configuration; in this codebase that is the endpoint
    /// URL, request timeout, response cap, and headers resolved at
    /// startup. Configurations that drift after construction would
    /// produce stale rebuilds; this codebase never mutates them after
    /// process startup so the precondition holds.
    pub fn new<F, Fut>(
        initial: C,
        peer_label: impl Into<String>,
        rebuild_threshold: NonZeroU32,
        rebuilder: F,
    ) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<C, ZebraTransportError>> + Send + 'static,
    {
        let erased: ClientRebuilder<C> = Arc::new(move || Box::pin(rebuilder()));
        Self {
            state: Arc::new(ResilientState {
                inner: ArcSwap::from_pointee(initial),
                consecutive_transport_failures: AtomicU32::new(0),
                rebuild_threshold,
                rebuilder: erased,
                rebuild_lock: Mutex::new(()),
                peer_label: peer_label.into(),
            }),
        }
    }

    /// Returns the current inner client.
    ///
    /// O(1) atomic load. Hold the returned `Arc` only as long as needed
    /// for the RPC call; rebuilds swap the pointer atomically, so a
    /// long-held snapshot will continue to use the dead client.
    #[must_use]
    pub fn snapshot(&self) -> Arc<C> {
        self.state.inner.load_full()
    }

    /// Folds the result of one RPC call into the failure counter.
    ///
    /// On `Ok` the counter resets. On `Err` the counter only advances
    /// when the error classifies as transport (see
    /// [`is_transport_failure`]). Once it reaches
    /// `rebuild_threshold`, a background task rebuilds the inner
    /// client; concurrent crossings collapse onto a single rebuild
    /// through an internal mutex.
    pub fn record_outcome<T>(&self, outcome: &Result<T, SourceError>) {
        self.record_outcome_inner(outcome.as_ref().err());
    }

    fn record_outcome_inner(&self, error: Option<&SourceError>) {
        match error {
            None => {
                self.state
                    .consecutive_transport_failures
                    .store(0, Ordering::Relaxed);
            }
            Some(error) if is_transport_failure(error) => {
                let previous = self
                    .state
                    .consecutive_transport_failures
                    .fetch_add(1, Ordering::Relaxed);
                let crossed = previous.saturating_add(1);
                if crossed >= self.state.rebuild_threshold.get() {
                    let this = self.clone();
                    let reason = error.upstream_classification().label();
                    tokio::spawn(async move {
                        let _ = this.rebuild_now(reason).await;
                    });
                }
            }
            Some(_) => {}
        }
    }

    /// Forces a rebuild attempt now.
    ///
    /// Exposed for tests and for adapters that want to trigger a rebuild
    /// from an event other than a transport-class failure (for example,
    /// a long idle period detected by an external watchdog). Production
    /// adapters rely on [`Self::record_outcome`] and never call this
    /// directly.
    ///
    /// `reason` becomes the `reason` label on the emitted reconnect
    /// metric and log line.
    pub async fn rebuild_now(&self, reason: &'static str) -> Result<(), ZebraTransportError> {
        let _guard = self.state.rebuild_lock.lock().await;
        // If a concurrent rebuilder already finished, the counter is back to
        // zero and this attempt is redundant.
        if self
            .state
            .consecutive_transport_failures
            .load(Ordering::Relaxed)
            < self.state.rebuild_threshold.get()
        {
            return Ok(());
        }
        record_transport_event(&self.state.peer_label, TransportEvent::Reconnecting, reason);
        match (self.state.rebuilder)().await {
            Ok(fresh) => {
                self.state.inner.store(Arc::new(fresh));
                self.state
                    .consecutive_transport_failures
                    .store(0, Ordering::Relaxed);
                record_transport_event(&self.state.peer_label, TransportEvent::Reconnected, reason);
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    target: "zinder::transport",
                    peer = %self.state.peer_label,
                    reason,
                    %error,
                    "transport rebuild failed; next transport-class error will retry",
                );
                Err(error)
            }
        }
    }
}

/// Returns `true` when `error` classifies as a transport-layer failure.
///
/// Reuses the existing [`SourceFailureClass`] mapping: a transport
/// failure is anything that means "the wire to the upstream node is
/// broken or unreachable", which today is
/// [`SourceFailureClass::NodeUnreachable`] (transport-level connect or
/// dispatch failure on the JSON-RPC client) or
/// [`SourceFailureClass::StreamDisconnected`] (gRPC stream torn down).
/// Everything else (`UpstreamViewChanged`, `CapabilityMissing`,
/// `ProtocolMismatch`, `Malformed`, `Configuration`) is application
/// state and must not trigger reconnects.
#[must_use]
pub fn is_transport_failure(error: &SourceError) -> bool {
    matches!(
        error.upstream_classification(),
        SourceFailureClass::NodeUnreachable | SourceFailureClass::StreamDisconnected,
    )
}

/// Transport-lifecycle events surfaced through tracing + metrics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransportEvent {
    Reconnecting,
    Reconnected,
}

impl TransportEvent {
    const fn label(self) -> &'static str {
        match self {
            Self::Reconnecting => "transport_reconnecting",
            Self::Reconnected => "transport_reconnected",
        }
    }
}

fn record_transport_event(peer: &str, event: TransportEvent, reason: &'static str) {
    match event {
        TransportEvent::Reconnecting => tracing::warn!(
            target: "zinder::transport",
            peer,
            reason,
            "transport reconnecting",
        ),
        TransportEvent::Reconnected => tracing::info!(
            target: "zinder::transport",
            peer,
            reason,
            "transport reconnected",
        ),
    }
    metrics::counter!(
        "zinder_transport_reconnect_total",
        "peer" => peer.to_owned(),
        "event" => event.label(),
        "reason" => reason,
    )
    .increment(1);
}

#[cfg(test)]
mod resilient_client_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn transport_failure() -> SourceError {
        SourceError::NodeUnavailable {
            reason: "test".to_owned(),
        }
    }

    fn non_transport_failure() -> SourceError {
        SourceError::SourceProtocolMismatch { reason: "test" }
    }

    fn rebuilder_counting(
        invocations: Arc<AtomicUsize>,
    ) -> impl Fn() -> BoxFuture<'static, Result<u32, ZebraTransportError>> + Send + Sync + 'static
    {
        move || {
            let invocations = Arc::clone(&invocations);
            Box::pin(async move {
                let next = invocations
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                let next_u32 = u32::try_from(next).unwrap_or(u32::MAX);
                Ok(next_u32)
            })
        }
    }

    #[test]
    fn is_transport_failure_classifies_node_unreachable_and_stream_disconnected() {
        assert!(is_transport_failure(&transport_failure()));
        assert!(is_transport_failure(
            &SourceError::ChainTipStreamUnavailable {
                reason: "test".to_owned()
            }
        ));
        assert!(!is_transport_failure(&non_transport_failure()));
    }

    #[tokio::test]
    async fn record_outcome_does_not_rebuild_below_threshold() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let invocations_for_rebuilder = Arc::clone(&invocations);
        let client = ResilientClient::new(0_u32, "test", ZEBRA_REBUILD_THRESHOLD, move || {
            let invocations = Arc::clone(&invocations_for_rebuilder);
            Box::pin(async move {
                invocations.fetch_add(1, Ordering::Relaxed);
                Ok(1)
            })
        });

        // Two failures: still under threshold of 3.
        client.record_outcome::<()>(&Err(transport_failure()));
        client.record_outcome::<()>(&Err(transport_failure()));
        tokio::task::yield_now().await;

        assert_eq!(invocations.load(Ordering::Relaxed), 0);
        assert_eq!(*client.snapshot(), 0);
    }

    #[tokio::test]
    async fn rebuild_now_swaps_inner_client_after_threshold() -> Result<(), ZebraTransportError> {
        let invocations = Arc::new(AtomicUsize::new(0));
        let client = ResilientClient::new(
            0_u32,
            "test",
            ZEBRA_REBUILD_THRESHOLD,
            rebuilder_counting(Arc::clone(&invocations)),
        );

        for _ in 0..ZEBRA_REBUILD_THRESHOLD.get() {
            client.record_outcome::<()>(&Err(transport_failure()));
        }
        client.rebuild_now("node_unreachable").await?;

        assert_eq!(invocations.load(Ordering::Relaxed), 1);
        assert_eq!(*client.snapshot(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_now_is_noop_when_below_threshold() -> Result<(), ZebraTransportError> {
        let invocations = Arc::new(AtomicUsize::new(0));
        let client = ResilientClient::new(
            7_u32,
            "test",
            ZEBRA_REBUILD_THRESHOLD,
            rebuilder_counting(Arc::clone(&invocations)),
        );

        client.rebuild_now("node_unreachable").await?;

        assert_eq!(invocations.load(Ordering::Relaxed), 0);
        assert_eq!(*client.snapshot(), 7);
        Ok(())
    }

    #[tokio::test]
    async fn success_resets_failure_counter() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let client = ResilientClient::new(
            0_u32,
            "test",
            ZEBRA_REBUILD_THRESHOLD,
            rebuilder_counting(Arc::clone(&invocations)),
        );

        // Two failures, then a success: counter should reset before threshold.
        client.record_outcome::<()>(&Err(transport_failure()));
        client.record_outcome::<()>(&Err(transport_failure()));
        client.record_outcome::<()>(&Ok(()));
        client.record_outcome::<()>(&Err(transport_failure()));
        tokio::task::yield_now().await;

        // Only one of the four record_outcome calls left the counter
        // non-zero, which is below threshold; no rebuild should fire.
        assert_eq!(invocations.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn non_transport_failures_do_not_advance_counter() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let client = ResilientClient::new(
            0_u32,
            "test",
            ZEBRA_REBUILD_THRESHOLD,
            rebuilder_counting(Arc::clone(&invocations)),
        );

        for _ in 0..10 {
            client.record_outcome::<()>(&Err(non_transport_failure()));
        }
        tokio::task::yield_now().await;

        assert_eq!(invocations.load(Ordering::Relaxed), 0);
    }
}
