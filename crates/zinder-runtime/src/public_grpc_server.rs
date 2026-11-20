//! The gRPC server that every public Zinder listener serves with.
//!
//! `zinder-query`, `zinder-compat-lightwalletd`, and `zinder-explorer` accept
//! connections from clients Zinder does not control. [`public_grpc_server`]
//! gives them one transport policy:
//!
//! - at most [`MAX_CONCURRENT_STREAMS_PER_CONNECTION`] streams on one HTTP/2
//!   connection;
//! - an HTTP/2 keepalive ping after a minute without a frame, closing the
//!   connection when the ping goes unanswered for 20 seconds; and
//! - a server-wide bound on requests whose message is still arriving
//!   ([`GrpcReceiveLimitLayer`]); and
//! - an end to any response whose client stops reading it
//!   ([`GrpcSendStallLayer`]).
//!
//! tonic reads a whole request message into memory before the handler runs,
//! and the per-connection stream limit does not bound how many messages the
//! process reads at once across connections. The receive limit does, so
//! many large requests sent together cannot exhaust memory.
//!
//! The receive limit keeps one client from locking the others out by
//! counting each client's slots. A client is the remote address of its
//! connection, unless the listener trusts a reverse proxy to name it
//! ([`ClientAddressSource::ProxyHeader`]). A proxy reaches the listener over
//! a few pooled connections, so without that header every client behind it
//! counts as the same few.
//!
//! A wallet listener, which [`crate::SupervisedRuntime::public_grpc_router`]
//! assembles, serves the wallet service behind the
//! [`TrafficReadinessInterceptor`], and gRPC reflection and
//! `grpc.health.v1.Health` outside it, so both keep answering while the
//! runtime refuses wallet calls. Both reflection versions apply the public
//! request size bound. Every call on the listener also passes through
//! [`GrpcCallTerminationLayer`], outside every other layer, which ends the
//! calls still open once the runtime's drain deadline passes.

mod call_termination;
mod message_boundaries;
mod receive_slots;
mod send_stall;

use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures_util::future::{Either, Ready, ready};
use http_body::{Frame, SizeHint};
use tokio::time::{Instant, Sleep};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tonic::{
    Status,
    body::Body,
    server::NamedService,
    service::interceptor::InterceptedService,
    transport::{
        Server,
        server::{Router, TcpConnectInfo, TlsConnectInfo},
    },
};
use tonic_reflection::server::{
    Builder as ReflectionBuilder,
    v1::{ServerReflection, ServerReflectionServer},
};
use tower::{
    Layer, Service, ServiceBuilder,
    layer::util::{Identity, Stack},
};
use zinder_proto::compat::lightwalletd::compact_tx_streamer_server;

pub(crate) use self::call_termination::OpenGrpcCalls;
pub use self::call_termination::{GrpcCallTermination, GrpcCallTerminationLayer};
use self::send_stall::ResponseCancellation;
pub use self::send_stall::{GrpcSendStall, GrpcSendStallLayer};
use self::{
    message_boundaries::MessageBoundaries,
    receive_slots::{ReceiveSlot, ReceiveSlots},
};
use crate::{Readiness, TrafficReadinessInterceptor, public_grpc_health::public_grpc_health};

/// Maximum concurrent HTTP/2 streams on one client connection.
///
/// tonic otherwise advertises no limit, which also removes hyper's own
/// server default of 200; this restores that value.
pub const MAX_CONCURRENT_STREAMS_PER_CONNECTION: u32 = 200;

/// Time without a frame from a client after which the server pings it.
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_mins(1);

/// Time a keepalive ping waits for its acknowledgement before the server
/// closes the connection and ends the streams on it, so a client that
/// vanished stops holding stream slots.
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// Requests each receive pool admits at once across every connection.
///
/// With the 2 MiB [`crate::MAX_DECODING_MESSAGE_BYTES`], each pool holds
/// at most 64 MiB of request messages being decoded.
pub const DEFAULT_GRPC_RECEIVE_SLOTS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(31);

/// Time a request has to arrive in full, and the longest a client stream
/// may go without sending a whole message.
pub const DEFAULT_GRPC_RECEIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// Time a response may wait for its client to read it before the server
/// ends the stream, matching lightwalletd's stream send timeout.
pub const DEFAULT_GRPC_SEND_STALL_TIMEOUT: Duration = Duration::from_mins(1);

/// Methods whose client sends a stream of messages.
///
/// A unit test checks this list against every service descriptor that a
/// public server registers, including both gRPC reflection versions.
const CLIENT_STREAMING_METHOD_PATHS: &[&str] = &[
    "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTaddressBalanceStream",
    "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
    "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
];

/// Request header in which a trusted reverse proxy names the client
/// connection a request arrived on, as one `ip:port` address.
///
/// Read only when the listener uses [`ClientAddressSource::ProxyHeader`].
pub const PROXY_CLIENT_ADDRESS_HEADER: &str = "x-zinder-client-address";

const RECEIVE_REFUSALS_METRIC: &str = "zinder_grpc_request_receive_refusals_total";
const RECEIVE_TIMEOUTS_METRIC: &str = "zinder_grpc_request_receive_timeouts_total";
const RECEIVE_EVICTIONS_METRIC: &str = "zinder_grpc_request_receive_evictions_total";
const RECEIVE_SLOTS_HELD_METRIC: &str = "zinder_grpc_request_receive_slots_held";
pub(crate) const RECEIVE_SLOTS_HELD_AT_ADMISSION_METRIC: &str =
    "zinder_grpc_request_receive_slots_held_at_admission";
const RECEIVE_SLOT_HOLD_DURATION_METRIC: &str =
    "zinder_grpc_request_receive_slot_hold_duration_seconds";
const PROXY_CLIENT_ADDRESS_FALLBACKS_METRIC: &str =
    "zinder_grpc_proxy_client_address_fallbacks_total";
const SEND_STALLS_METRIC: &str = "zinder_grpc_response_send_stalls_total";

/// The layers every public Zinder listener applies to its requests.
type PublicGrpcLayers = Stack<GrpcSendStallLayer, Stack<GrpcReceiveLimitLayer, Identity>>;

/// The tonic server type that [`public_grpc_server`] returns.
pub type PublicGrpcServer = Server<PublicGrpcLayers>;

/// The layers of a supervised public listener, outermost first: call
/// termination, the receive limit, and the send stall bound.
pub(crate) type SupervisedGrpcLayers = Stack<
    GrpcSendStallLayer,
    Stack<GrpcReceiveLimitLayer, Stack<GrpcCallTerminationLayer, Identity>>,
>;

/// Returns the gRPC server builder for a public Zinder listener, with the
/// default receive limits and send stall timeout, telling clients apart by
/// `client_address`.
///
/// Add services and serve as with [`tonic::transport::Server::builder`].
#[must_use]
pub fn public_grpc_server(client_address: ClientAddressSource) -> PublicGrpcServer {
    public_grpc_server_with(
        GrpcReceiveLimits::default(),
        DEFAULT_GRPC_SEND_STALL_TIMEOUT,
        client_address,
    )
}

/// Returns the gRPC server builder for a public Zinder listener, with the
/// given receive limits and send stall timeout.
///
/// Production listeners use [`public_grpc_server`]. This form exists so
/// tests can exercise the limits with small values.
#[must_use]
pub fn public_grpc_server_with(
    limits: GrpcReceiveLimits,
    send_stall_timeout: Duration,
    client_address: ClientAddressSource,
) -> PublicGrpcServer {
    describe_limit_metrics();
    public_grpc_transport()
        .layer(GrpcReceiveLimitLayer::new(limits, client_address))
        .layer(GrpcSendStallLayer::new(send_stall_timeout))
}

/// The layers of a supervised listener, telling clients apart by
/// `client_address`.
///
/// Call termination wraps the others, so a terminated call drops what the
/// send stall bound still holds of its response, and a refused call never
/// takes a receive slot.
pub(crate) fn supervised_grpc_layers(
    calls: Arc<OpenGrpcCalls>,
    limits: GrpcReceiveLimits,
    send_stall_timeout: Duration,
    client_address: ClientAddressSource,
) -> SupervisedGrpcLayers {
    describe_limit_metrics();
    ServiceBuilder::new()
        .layer(GrpcCallTerminationLayer::new(calls))
        .layer(GrpcReceiveLimitLayer::new(limits, client_address))
        .layer(GrpcSendStallLayer::new(send_stall_timeout))
        .into_inner()
}

/// The HTTP/2 transport policy every public listener shares.
pub(crate) fn public_grpc_transport() -> Server {
    Server::builder()
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS_PER_CONNECTION)
        .http2_keepalive_interval(Some(HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(HTTP2_KEEPALIVE_TIMEOUT))
}

/// Assembles a supervised wallet listener that serves `wallet_service`
/// behind the traffic gate, and reflection and health outside it.
///
/// Open health `Watch` streams end once `drain` is cancelled.
pub(crate) fn supervised_wallet_router<S, L>(
    mut server: Server<L>,
    wallet_service: S,
    wallet_descriptor_set: &'static [u8],
    readiness: &Readiness,
    drain: &CancellationToken,
) -> Result<Router<L>, tonic_reflection::server::Error>
where
    L: Clone,
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let health = public_grpc_health(readiness, drain, S::NAME);
    let gated_wallet_service = InterceptedService::new(
        wallet_service,
        TrafficReadinessInterceptor::new(readiness.clone()),
    );
    let mut router = server.add_service(gated_wallet_service).add_service(health);
    let descriptors = || {
        ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(wallet_descriptor_set)
            .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
    };
    let mut v1 = descriptors();
    if S::NAME == compact_tx_streamer_server::SERVICE_NAME {
        let alpha = descriptors()
            .register_encoded_file_descriptor_set(tonic_reflection::pb::v1::FILE_DESCRIPTOR_SET)
            .build_v1alpha()?
            .max_decoding_message_size(crate::MAX_DECODING_MESSAGE_BYTES);
        router = router.add_service(alpha);
        v1 = v1.register_encoded_file_descriptor_set(
            tonic_reflection::pb::v1alpha::FILE_DESCRIPTOR_SET,
        );
    }
    Ok(router.add_service(reflection_v1(v1)?))
}

/// Where a public listener learns which client sent a request, so that the
/// receive limit can keep one client from locking the others out.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientAddressSource {
    /// The remote address of the connection the request arrived on. Use it
    /// when clients connect to the listener directly.
    #[default]
    Connection,
    /// The [`PROXY_CLIENT_ADDRESS_HEADER`] that a reverse proxy sets on every
    /// request it forwards. A request counts against the connection's remote
    /// address instead when it carries no such header, carries it more than
    /// once or as a comma-separated list, or carries a value that is not one
    /// `ip:port` address.
    ///
    /// Any client that reaches the listener can set the header, so use this
    /// only when the listener is reachable solely through a proxy that
    /// replaces it.
    ProxyHeader,
}

impl ClientAddressSource {
    /// The source for a listener whose `trust_proxy_client_address` setting
    /// is `trusted`.
    #[must_use]
    pub const fn from_trust_proxy_client_address(trusted: bool) -> Self {
        if trusted {
            Self::ProxyHeader
        } else {
            Self::Connection
        }
    }

    /// The `trust_proxy_client_address` setting that selects this source.
    #[must_use]
    pub const fn trust_proxy_client_address(self) -> bool {
        matches!(self, Self::ProxyHeader)
    }
}

/// Returns the gRPC reflection v1 service for a public Zinder listener,
/// describing the services in `encoded_file_descriptor_set`.
///
/// The service decodes request messages up to
/// [`crate::MAX_DECODING_MESSAGE_BYTES`], like every other public service,
/// instead of tonic's 4 MiB default.
pub fn public_grpc_reflection(
    encoded_file_descriptor_set: &'static [u8],
) -> Result<ServerReflectionServer<impl ServerReflection>, tonic_reflection::server::Error> {
    reflection_v1(
        ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(encoded_file_descriptor_set),
    )
}

fn reflection_v1(
    builder: ReflectionBuilder<'static>,
) -> Result<ServerReflectionServer<impl ServerReflection>, tonic_reflection::server::Error> {
    Ok(builder
        .build_v1()?
        .max_decoding_message_size(crate::MAX_DECODING_MESSAGE_BYTES))
}

fn describe_limit_metrics() {
    metrics::describe_counter!(
        RECEIVE_REFUSALS_METRIC,
        metrics::Unit::Count,
        "gRPC requests refused with UNAVAILABLE because every receive slot of their pool was \
         taken. Label: pool (request|client_stream)."
    );
    metrics::describe_counter!(
        RECEIVE_TIMEOUTS_METRIC,
        metrics::Unit::Count,
        "gRPC requests cancelled with DEADLINE_EXCEEDED because their request did not arrive \
         in time (pool=request) or their client stream went idle (pool=client_stream)."
    );
    metrics::describe_counter!(
        RECEIVE_EVICTIONS_METRIC,
        metrics::Unit::Count,
        "gRPC requests cancelled with UNAVAILABLE because a full pool gave their slot to a \
         request from a client holding fewer slots. Label: pool (request|client_stream)."
    );
    metrics::describe_gauge!(
        RECEIVE_SLOTS_HELD_METRIC,
        metrics::Unit::Count,
        "Receive slots a pool holds now. Label: pool (request|client_stream)."
    );
    metrics::describe_histogram!(
        RECEIVE_SLOTS_HELD_AT_ADMISSION_METRIC,
        metrics::Unit::Count,
        "Receive slots a pool held once it admitted a gRPC request, the request's own slot \
         included. Label: pool (request|client_stream)."
    );
    metrics::describe_histogram!(
        RECEIVE_SLOT_HOLD_DURATION_METRIC,
        metrics::Unit::Seconds,
        "Time an admitted gRPC request held its receive slot, until its request arrived, its \
         client stream ended, or another client's request took the slot. \
         Label: pool (request|client_stream)."
    );
    metrics::describe_counter!(
        PROXY_CLIENT_ADDRESS_FALLBACKS_METRIC,
        metrics::Unit::Count,
        "gRPC requests counted against the transport peer because the trusted proxy's address \
         header was missing, repeated, or invalid. Excludes headerless loopback Health/Check calls."
    );
    metrics::describe_counter!(
        SEND_STALLS_METRIC,
        metrics::Unit::Count,
        "gRPC responses ended with DEADLINE_EXCEEDED because their client stopped reading \
         them for the send stall timeout."
    );
}

/// Server-wide bounds on gRPC requests whose message is still arriving.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GrpcReceiveLimits {
    slots_per_pool: NonZeroUsize,
    receive_timeout: Duration,
}

impl GrpcReceiveLimits {
    /// Limits with `slots_per_pool` receive slots in each pool and
    /// `receive_timeout` for a request to arrive or a client stream to send
    /// its next message.
    #[must_use]
    pub const fn new(slots_per_pool: NonZeroUsize, receive_timeout: Duration) -> Self {
        Self {
            slots_per_pool,
            receive_timeout,
        }
    }

    /// Receive slots in each of the two pools.
    #[must_use]
    pub const fn slots_per_pool(&self) -> NonZeroUsize {
        self.slots_per_pool
    }

    /// Time a request has to arrive in full, and the longest a client
    /// stream may go without sending a whole message.
    #[must_use]
    pub const fn receive_timeout(&self) -> Duration {
        self.receive_timeout
    }
}

impl Default for GrpcReceiveLimits {
    fn default() -> Self {
        Self::new(DEFAULT_GRPC_RECEIVE_SLOTS, DEFAULT_GRPC_RECEIVE_TIMEOUT)
    }
}

/// Tower layer that bounds how many gRPC requests a server receives at once.
///
/// A request claims a slot when its stream opens, before any of its body
/// arrives. There are two pools of the same size, so client streams cannot
/// starve single-message calls:
///
/// - A unary or server-streaming call frees its slot once its request body
///   ends, before the handler runs. A long-lived response stream holds no
///   slot. A request that has not finished arriving within the receive
///   timeout of its stream opening fails with `DEADLINE_EXCEEDED`.
/// - A client-streaming or bidirectional call, including gRPC reflection,
///   keeps its slot until the client half-closes or the call ends. A client
///   stream that sends no whole message for the receive timeout fails with
///   `DEADLINE_EXCEEDED`.
///
/// When a pool is full, a new request takes the oldest slot of the client
/// holding the most slots, provided that client holds more than the
/// requester; the evicted request fails with `UNAVAILABLE`. Otherwise the
/// new request is refused with `UNAVAILABLE` and its body is never read. One
/// client therefore cannot lock the others out. A client is the address that
/// [`ClientAddressSource`] names.
///
/// Every clone shares the same slots, so one layer bounds the whole server.
#[derive(Clone, Debug)]
pub struct GrpcReceiveLimitLayer {
    pools: ReceivePools,
}

impl GrpcReceiveLimitLayer {
    /// Creates a layer whose slots are shared by every connection it wraps,
    /// telling clients apart by `client_address`.
    #[must_use]
    pub fn new(limits: GrpcReceiveLimits, client_address: ClientAddressSource) -> Self {
        Self {
            pools: ReceivePools {
                requests: ReceiveSlots::new(limits.slots_per_pool, ReceivePool::Request),
                client_streams: ReceiveSlots::new(limits.slots_per_pool, ReceivePool::ClientStream),
                receive_timeout: limits.receive_timeout,
                client_address,
            },
        }
    }
}

impl<S> Layer<S> for GrpcReceiveLimitLayer {
    type Service = GrpcReceiveLimit<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcReceiveLimit {
            inner,
            pools: self.pools.clone(),
        }
    }
}

/// The tower service that [`GrpcReceiveLimitLayer`] wraps around a router.
#[derive(Clone, Debug)]
pub struct GrpcReceiveLimit<S> {
    inner: S,
    pools: ReceivePools,
}

impl<S> Service<http::Request<Body>> for GrpcReceiveLimit<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>>,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = Either<Ready<Result<Self::Response, Self::Error>>, S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let pool = ReceivePool::for_path(request.uri().path());
        let client = client_address(&request, self.pools.client_address);
        let Some(admission) = self.pools.slots(pool).claim(client) else {
            metrics::counter!(RECEIVE_REFUSALS_METRIC, "pool" => pool.label()).increment(1);
            return Either::Left(ready(Ok(too_many_requests().into_http())));
        };
        if admission.evicted_another {
            metrics::counter!(RECEIVE_EVICTIONS_METRIC, "pool" => pool.label()).increment(1);
        }
        let receive_timeout = self.pools.receive_timeout;
        let mut request = request;
        // A client stream's handler may never poll its request body again,
        // or may end successfully when it fails, so its response learns of
        // eviction and of receive failures through the send stall layer.
        let response_cancellation = (pool == ReceivePool::ClientStream).then(|| {
            ResponseCancellation::new(admission.slot.eviction_token(), too_many_requests())
        });
        if let Some(cancellation) = &response_cancellation {
            request.extensions_mut().insert(cancellation.clone());
        }
        let request = request.map(|body| {
            Body::new(ReceiveBoundBody {
                inner: body,
                state: ReceiveState::Receiving(Box::new(Receiving::new(
                    admission.slot,
                    receive_timeout,
                ))),
                pool,
                receive_timeout,
                response_cancellation,
            })
        });
        Either::Right(self.inner.call(request))
    }
}

/// The client a request counts against, as `source` names it.
fn client_address<B>(
    request: &http::Request<B>,
    source: ClientAddressSource,
) -> Option<SocketAddr> {
    match source {
        ClientAddressSource::Connection => remote_address(request),
        ClientAddressSource::ProxyHeader => match proxy_named_client(request.headers()) {
            Ok(client) => Some(client),
            Err(fallback) => {
                let peer = remote_address(request);
                let direct_health_check = fallback == ProxyHeaderFallback::Missing
                    && request.method() == http::Method::POST
                    && request.uri().path() == "/grpc.health.v1.Health/Check"
                    && peer.is_some_and(|addr| addr.ip().to_canonical().is_loopback());
                if !direct_health_check {
                    metrics::counter!(
                        PROXY_CLIENT_ADDRESS_FALLBACKS_METRIC,
                        "reason" => fallback.label()
                    )
                    .increment(1);
                }
                peer
            }
        },
    }
}

/// Why a request on a listener that trusts its proxy counts against the
/// proxy's own connection instead of the client the proxy names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProxyHeaderFallback {
    /// The request carries no [`PROXY_CLIENT_ADDRESS_HEADER`].
    Missing,
    /// The request carries the header more than once, or as a
    /// comma-separated list. Which value the proxy set is unknown, and a
    /// proxy that appends to the header rather than replacing it forwards
    /// the client's own value first.
    Repeated,
    /// The header is not one `ip:port` address.
    Invalid,
}

impl ProxyHeaderFallback {
    const fn label(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Repeated => "repeated",
            Self::Invalid => "invalid",
        }
    }
}

/// The client connection that the single [`PROXY_CLIENT_ADDRESS_HEADER`] in
/// `headers` names.
fn proxy_named_client(headers: &http::HeaderMap) -> Result<SocketAddr, ProxyHeaderFallback> {
    let mut fields = headers.get_all(PROXY_CLIENT_ADDRESS_HEADER).iter();
    let field = fields.next().ok_or(ProxyHeaderFallback::Missing)?;
    if fields.next().is_some() {
        return Err(ProxyHeaderFallback::Repeated);
    }
    let address = field.to_str().map_err(|_| ProxyHeaderFallback::Invalid)?;
    if address.contains(',') {
        return Err(ProxyHeaderFallback::Repeated);
    }
    address.parse().map_err(|_| ProxyHeaderFallback::Invalid)
}

/// The remote address of the connection a request arrived on, which tonic
/// records in the request extensions before any user layer runs.
fn remote_address<B>(request: &http::Request<B>) -> Option<SocketAddr> {
    let extensions = request.extensions();
    extensions
        .get::<TcpConnectInfo>()
        .and_then(TcpConnectInfo::remote_addr)
        .or_else(|| {
            extensions
                .get::<TlsConnectInfo<TcpConnectInfo>>()
                .and_then(|tls| tls.get_ref().remote_addr())
        })
}

fn too_many_requests() -> Status {
    Status::unavailable("server is receiving too many requests at once; retry with backoff")
}

#[derive(Clone, Debug)]
struct ReceivePools {
    requests: Arc<ReceiveSlots>,
    client_streams: Arc<ReceiveSlots>,
    receive_timeout: Duration,
    client_address: ClientAddressSource,
}

impl ReceivePools {
    fn slots(&self, pool: ReceivePool) -> &Arc<ReceiveSlots> {
        match pool {
            ReceivePool::Request => &self.requests,
            ReceivePool::ClientStream => &self.client_streams,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReceivePool {
    /// Unary and server-streaming calls, whose client sends one message.
    Request,
    /// Client-streaming and bidirectional calls.
    ClientStream,
}

impl ReceivePool {
    fn for_path(path: &str) -> Self {
        if CLIENT_STREAMING_METHOD_PATHS.contains(&path) {
            Self::ClientStream
        } else {
            Self::Request
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::ClientStream => "client_stream",
        }
    }
}

/// A request body that holds its receive slot until it ends.
struct ReceiveBoundBody {
    inner: Body,
    state: ReceiveState,
    pool: ReceivePool,
    receive_timeout: Duration,
    /// Where a client stream records why its request failed.
    response_cancellation: Option<ResponseCancellation>,
}

enum ReceiveState {
    Receiving(Box<Receiving>),
    Received,
    /// The request timed out or lost its slot, and the body reported it.
    Cancelled,
}

struct Receiving {
    _slot: ReceiveSlot,
    deadline: Pin<Box<Sleep>>,
    evicted: Pin<Box<WaitForCancellationFutureOwned>>,
    boundaries: MessageBoundaries,
}

impl Receiving {
    fn new(slot: ReceiveSlot, receive_timeout: Duration) -> Self {
        Self {
            deadline: Box::pin(tokio::time::sleep(receive_timeout)),
            evicted: Box::pin(slot.evicted()),
            _slot: slot,
            boundaries: MessageBoundaries::default(),
        }
    }
}

impl ReceiveBoundBody {
    /// Records `status` as the request's failure for its response.
    fn report(&self, status: Status) -> Status {
        if let Some(cancellation) = &self.response_cancellation {
            cancellation.fail(&status);
        }
        status
    }
}

impl http_body::Body for ReceiveBoundBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let receiving = match &mut this.state {
            ReceiveState::Receiving(receiving) => receiving,
            ReceiveState::Received => return Pin::new(&mut this.inner).poll_frame(cx),
            ReceiveState::Cancelled => return Poll::Ready(None),
        };
        if receiving.evicted.as_mut().poll(cx).is_ready() {
            this.state = ReceiveState::Cancelled;
            return Poll::Ready(Some(Err(this.report(too_many_requests()))));
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if this.inner.is_end_stream() {
                    this.state = ReceiveState::Received;
                } else if this.pool == ReceivePool::ClientStream
                    && frame
                        .data_ref()
                        .is_some_and(|chunk| receiving.boundaries.completes_a_message(chunk))
                {
                    receiving
                        .deadline
                        .as_mut()
                        .reset(Instant::now() + this.receive_timeout);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(ended_or_failed) => {
                this.state = ReceiveState::Received;
                Poll::Ready(ended_or_failed)
            }
            Poll::Pending => {
                if receiving.deadline.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                this.state = ReceiveState::Cancelled;
                metrics::counter!(RECEIVE_TIMEOUTS_METRIC, "pool" => this.pool.label())
                    .increment(1);
                Poll::Ready(Some(Err(this.report(timeout_status(this.pool)))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match self.state {
            ReceiveState::Cancelled => true,
            ReceiveState::Receiving { .. } | ReceiveState::Received => self.inner.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn timeout_status(pool: ReceivePool) -> Status {
    match pool {
        ReceivePool::Request => Status::deadline_exceeded("request did not arrive in time"),
        ReceivePool::ClientStream => {
            Status::deadline_exceeded("client stream sent no message for too long")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    use metrics_exporter_prometheus::PrometheusBuilder;
    use prost::Message as _;
    use prost_types::FileDescriptorSet;
    use tonic::transport::server::TcpConnectInfo;

    use super::{
        CLIENT_STREAMING_METHOD_PATHS, ClientAddressSource, PROXY_CLIENT_ADDRESS_HEADER,
        client_address,
    };

    const PROXY_CONNECTION: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40_000);

    /// A request arriving on the proxy's connection, carrying one
    /// [`PROXY_CLIENT_ADDRESS_HEADER`] field per entry of `fields`.
    fn proxied_request(fields: &[&str]) -> Result<http::Request<()>, http::Error> {
        let mut builder = http::Request::builder();
        for field in fields {
            builder = builder.header(PROXY_CLIENT_ADDRESS_HEADER, *field);
        }
        let mut request = builder.body(())?;
        request.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(PROXY_CONNECTION),
        });
        Ok(request)
    }

    #[test]
    fn a_trusted_proxy_header_names_the_client() -> Result<(), Box<dyn std::error::Error>> {
        for (header, client) in [
            ("203.0.113.7:51234", "203.0.113.7:51234"),
            ("[2001:db8::7]:443", "[2001:db8::7]:443"),
        ] {
            let request = proxied_request(&[header])?;
            assert_eq!(
                client_address(&request, ClientAddressSource::ProxyHeader),
                Some(client.parse()?)
            );
        }
        Ok(())
    }

    #[test]
    fn a_listener_that_trusts_no_proxy_ignores_the_header() -> Result<(), http::Error> {
        let request = proxied_request(&["203.0.113.7:51234"])?;
        assert_eq!(
            client_address(&request, ClientAddressSource::Connection),
            Some(PROXY_CONNECTION)
        );
        Ok(())
    }

    #[test]
    fn a_proxy_header_that_is_not_one_address_counts_the_connection() -> Result<(), http::Error> {
        let not_one_address: [&[&str]; 9] = [
            &[],
            &["203.0.113.7"],
            &["client-1"],
            &[""],
            &[" 203.0.113.7:51234"],
            &["203.0.113.7:51234, 198.51.100.4:443"],
            &["203.0.113.7:51234,"],
            // A proxy that appends instead of replacing forwards the
            // client's value first.
            &["198.51.100.4:443", "203.0.113.7:51234"],
            &["203.0.113.7:51234", "203.0.113.7:51234"],
        ];
        for fields in not_one_address {
            let request = proxied_request(fields)?;
            assert_eq!(
                client_address(&request, ClientAddressSource::ProxyHeader),
                Some(PROXY_CONNECTION),
                "{fields:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn proxy_header_fallbacks_are_counted_by_reason_without_the_address() -> Result<(), http::Error>
    {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let requests = [
            proxied_request(&[])?,
            proxied_request(&["198.51.100.4:443", "203.0.113.7:51234"])?,
            proxied_request(&["203.0.113.7:51234, 198.51.100.4:443"])?,
            proxied_request(&["203.0.113.7"])?,
            proxied_request(&["203.0.113.7:51234"])?,
        ];

        metrics::with_local_recorder(&recorder, || {
            for request in &requests {
                let _ = client_address(request, ClientAddressSource::ProxyHeader);
            }
            // A listener that trusts no proxy never reads the header.
            let _ = client_address(&requests[0], ClientAddressSource::Connection);
        });

        let exposition = handle.render();
        for expected in [
            "zinder_grpc_proxy_client_address_fallbacks_total{reason=\"missing\"} 1",
            "zinder_grpc_proxy_client_address_fallbacks_total{reason=\"repeated\"} 2",
            "zinder_grpc_proxy_client_address_fallbacks_total{reason=\"invalid\"} 1",
        ] {
            assert!(exposition.contains(expected), "{expected}\n{exposition}");
        }
        for address in ["203.0.113.7", "198.51.100.4", "127.0.0.1"] {
            assert!(!exposition.contains(address), "{exposition}");
        }
        Ok(())
    }

    #[test]
    fn only_headerless_loopback_health_checks_use_the_peer_without_a_proxy_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        use http::Method;

        use super::PROXY_CLIENT_ADDRESS_FALLBACKS_METRIC;

        let check = "/grpc.health.v1.Health/Check";
        let watch = "/grpc.health.v1.Health/Watch";
        let local = Some("127.0.0.1:1234");
        let remote = Some("192.0.2.1:5678");
        for (method, path, peer, headers, reason) in [
            (Method::POST, check, local, &[][..], None),
            (Method::POST, check, Some("[::1]:1234"), &[], None),
            (
                Method::POST,
                check,
                Some("[::ffff:127.0.0.1]:1234"),
                &[],
                None,
            ),
            (Method::POST, check, remote, &[], Some("missing")),
            (Method::POST, check, None, &[], Some("missing")),
            (Method::GET, check, local, &[], Some("missing")),
            (Method::POST, watch, local, &[], Some("missing")),
            (Method::POST, check, local, &["invalid"], Some("invalid")),
            (
                Method::POST,
                check,
                local,
                &["192.0.2.1:5678"; 2],
                Some("repeated"),
            ),
            (Method::POST, check, local, &["192.0.2.1:5678"], None),
        ] {
            let peer = peer.map(str::parse).transpose()?;
            let mut request = proxied_request(headers)?;
            *request.uri_mut() = path.parse()?;
            *request.method_mut() = method;
            request.extensions_mut().insert(TcpConnectInfo {
                local_addr: None,
                remote_addr: peer,
            });
            let recorder = PrometheusBuilder::new().build_recorder();
            let client = metrics::with_local_recorder(&recorder, || {
                client_address(&request, ClientAddressSource::ProxyHeader)
            });
            let expected = if let [address] = headers
                && reason.is_none()
            {
                Some(address.parse()?)
            } else {
                peer
            };
            assert_eq!(client, expected);
            let scrape = recorder.handle().render();
            let counter = scrape
                .lines()
                .find(|line| line.starts_with(PROXY_CLIENT_ADDRESS_FALLBACKS_METRIC));
            let expected = reason.map(|reason| {
                format!("{PROXY_CLIENT_ADDRESS_FALLBACKS_METRIC}{{reason=\"{reason}\"}} 1")
            });
            assert_eq!(
                counter,
                expected.as_deref(),
                "{} {path} {peer:?} {headers:?}",
                request.method()
            );
        }
        Ok(())
    }

    /// Every client-streaming method a public server can register.
    #[test]
    fn client_streaming_paths_match_the_served_descriptors() -> Result<(), prost::DecodeError> {
        let descriptor_sets = [
            zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET,
            zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET,
            tonic_reflection::pb::v1::FILE_DESCRIPTOR_SET,
            tonic_reflection::pb::v1alpha::FILE_DESCRIPTOR_SET,
            tonic_health::pb::FILE_DESCRIPTOR_SET,
        ];
        let mut client_streaming = BTreeSet::new();
        for encoded in descriptor_sets {
            for file in FileDescriptorSet::decode(encoded)?.file {
                for service in &file.service {
                    for method in &service.method {
                        if method.client_streaming() {
                            client_streaming.insert(format!(
                                "/{}.{}/{}",
                                file.package(),
                                service.name(),
                                method.name()
                            ));
                        }
                    }
                }
            }
        }
        let listed = CLIENT_STREAMING_METHOD_PATHS
            .iter()
            .map(|path| (*path).to_owned())
            .collect::<BTreeSet<_>>();
        assert_eq!(listed, client_streaming);
        Ok(())
    }
}
