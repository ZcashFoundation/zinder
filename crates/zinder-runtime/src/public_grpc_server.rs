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
//! A wallet listener assembles its services with [`public_grpc_router`]: the
//! wallet service behind the [`TrafficReadinessInterceptor`], and gRPC
//! reflection and `grpc.health.v1.Health` outside it, so both keep answering
//! while the runtime refuses wallet calls. Reflection, built with
//! [`public_grpc_reflection`], applies the same request size bound as every
//! other public service.
//!
//! A listener a [`crate::SupervisedRuntime`] serves also passes through
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
use tonic_reflection::server::v1::{ServerReflection, ServerReflectionServer};
use tower::{
    Layer, Service, ServiceBuilder,
    layer::util::{Identity, Stack},
};

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

const RECEIVE_REFUSALS_METRIC: &str = "zinder_grpc_request_receive_refusals_total";
const RECEIVE_TIMEOUTS_METRIC: &str = "zinder_grpc_request_receive_timeouts_total";
const RECEIVE_EVICTIONS_METRIC: &str = "zinder_grpc_request_receive_evictions_total";
const SEND_STALLS_METRIC: &str = "zinder_grpc_response_send_stalls_total";

/// The layers every public Zinder listener applies to its requests.
type PublicGrpcLayers = Stack<GrpcSendStallLayer, Stack<GrpcReceiveLimitLayer, Identity>>;

/// The tonic server type that [`public_grpc_server`] returns.
pub type PublicGrpcServer = Server<PublicGrpcLayers>;

/// The tonic router type that [`public_grpc_router`] returns.
pub type PublicGrpcRouter = Router<PublicGrpcLayers>;

/// The layers of a supervised public listener, outermost first: call
/// termination, the receive limit, and the send stall bound.
pub(crate) type SupervisedGrpcLayers = Stack<
    GrpcSendStallLayer,
    Stack<GrpcReceiveLimitLayer, Stack<GrpcCallTerminationLayer, Identity>>,
>;

/// Returns the gRPC server builder for a public Zinder listener, with the
/// default receive limits and send stall timeout.
///
/// Add services and serve as with [`tonic::transport::Server::builder`].
#[must_use]
pub fn public_grpc_server() -> PublicGrpcServer {
    public_grpc_server_with(
        GrpcReceiveLimits::default(),
        DEFAULT_GRPC_SEND_STALL_TIMEOUT,
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
) -> PublicGrpcServer {
    describe_limit_metrics();
    public_grpc_transport()
        .layer(GrpcReceiveLimitLayer::new(limits))
        .layer(GrpcSendStallLayer::new(send_stall_timeout))
}

/// Returns the gRPC server builder for a supervised public listener, whose
/// calls `calls` can end.
pub(crate) fn supervised_grpc_server(
    calls: Arc<OpenGrpcCalls>,
) -> Server<Stack<SupervisedGrpcLayers, Identity>> {
    public_grpc_transport().layer(supervised_grpc_layers(
        calls,
        GrpcReceiveLimits::default(),
        DEFAULT_GRPC_SEND_STALL_TIMEOUT,
    ))
}

/// The layers of a supervised listener.
///
/// Call termination wraps the others, so a terminated call drops what the
/// send stall bound still holds of its response, and a refused call never
/// takes a receive slot.
pub(crate) fn supervised_grpc_layers(
    calls: Arc<OpenGrpcCalls>,
    limits: GrpcReceiveLimits,
    send_stall_timeout: Duration,
) -> SupervisedGrpcLayers {
    describe_limit_metrics();
    ServiceBuilder::new()
        .layer(GrpcCallTerminationLayer::new(calls))
        .layer(GrpcReceiveLimitLayer::new(limits))
        .layer(GrpcSendStallLayer::new(send_stall_timeout))
        .into_inner()
}

/// The HTTP/2 transport policy every public listener shares.
fn public_grpc_transport() -> Server {
    Server::builder()
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS_PER_CONNECTION)
        .http2_keepalive_interval(Some(HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(HTTP2_KEEPALIVE_TIMEOUT))
}

/// Assembles a public Zinder wallet listener that serves `wallet_service`.
///
/// The router serves, on [`public_grpc_server`]'s transport policy:
///
/// - `wallet_service`, behind a [`TrafficReadinessInterceptor`] over
///   `readiness`;
/// - gRPC reflection v1, describing `wallet_descriptor_set` and
///   `grpc.health.v1`; and
/// - `grpc.health.v1.Health`, `SERVING` exactly when `readiness` admits
///   traffic, as `/readyz` reports it.
///
/// Reflection and health stay outside the traffic gate, so they answer while
/// the runtime refuses wallet calls.
///
/// Serve the router with `drain` as its shutdown signal. To stop, publish
/// `shutting_down` to `readiness` first and cancel `drain` after it: health
/// then reports `NOT_SERVING` together with `/readyz`, and open health
/// `Watch` streams end as the drain starts.
pub fn public_grpc_router<S>(
    wallet_service: S,
    wallet_descriptor_set: &'static [u8],
    readiness: &Readiness,
    drain: &CancellationToken,
) -> Result<PublicGrpcRouter, tonic_reflection::server::Error>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let reflection =
        reflection_describing(&[wallet_descriptor_set, tonic_health::pb::FILE_DESCRIPTOR_SET])?;
    let health = public_grpc_health(readiness, drain, S::NAME);
    let gated_wallet_service = InterceptedService::new(
        wallet_service,
        TrafficReadinessInterceptor::new(readiness.clone()),
    );
    Ok(public_grpc_server()
        .add_service(gated_wallet_service)
        .add_service(reflection)
        .add_service(health))
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
    reflection_describing(&[encoded_file_descriptor_set])
}

fn reflection_describing(
    encoded_file_descriptor_sets: &[&'static [u8]],
) -> Result<ServerReflectionServer<impl ServerReflection + use<>>, tonic_reflection::server::Error>
{
    let builder = encoded_file_descriptor_sets.iter().copied().fold(
        tonic_reflection::server::Builder::configure(),
        tonic_reflection::server::Builder::register_encoded_file_descriptor_set,
    );
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
         request from a connection holding fewer slots. Label: pool (request|client_stream)."
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
/// connection holding the most slots, provided that connection holds more
/// than the requester's; the evicted request fails with `UNAVAILABLE`.
/// Otherwise the new request is refused with `UNAVAILABLE` and its body is
/// never read. One connection therefore cannot lock the others out.
///
/// Every clone shares the same slots, so one layer bounds the whole server.
#[derive(Clone, Debug)]
pub struct GrpcReceiveLimitLayer {
    pools: ReceivePools,
}

impl GrpcReceiveLimitLayer {
    /// Creates a layer whose slots are shared by every connection it wraps.
    #[must_use]
    pub fn new(limits: GrpcReceiveLimits) -> Self {
        Self {
            pools: ReceivePools {
                requests: ReceiveSlots::new(limits.slots_per_pool),
                client_streams: ReceiveSlots::new(limits.slots_per_pool),
                receive_timeout: limits.receive_timeout,
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
        let Some(admission) = self.pools.slots(pool).claim(remote_address(&request)) else {
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
    use std::collections::BTreeSet;

    use prost::Message as _;
    use prost_types::FileDescriptorSet;

    use super::CLIENT_STREAMING_METHOD_PATHS;

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
