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
//!   ([`GrpcReceiveLimitLayer`]).
//!
//! tonic reads a whole request message into memory before the handler runs,
//! and the per-connection stream limit does not bound how many messages the
//! process reads at once across connections. The receive limit does, so
//! many large requests sent together cannot exhaust memory.

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures_util::future::{Either, Ready, ready};
use http_body::{Frame, SizeHint};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, Sleep},
};
use tonic::{Status, body::Body, transport::Server};
use tower::{
    Layer, Service,
    layer::util::{Identity, Stack},
};

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
/// With the 2 MiB [`crate::MAX_DECODING_MESSAGE_BYTES`], the request pool
/// holds at most 64 MiB of request messages being decoded.
pub const DEFAULT_GRPC_RECEIVE_SLOTS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(31);

/// Time a request has to arrive in full, and the longest a client stream
/// may go without sending a frame.
pub const DEFAULT_GRPC_RECEIVE_TIMEOUT: Duration = Duration::from_secs(20);

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

/// The tonic server type that [`public_grpc_server`] returns.
pub type PublicGrpcServer = Server<Stack<GrpcReceiveLimitLayer, Identity>>;

/// Returns the gRPC server builder for a public Zinder listener, with the
/// default receive limits.
///
/// Add services and serve as with [`tonic::transport::Server::builder`].
#[must_use]
pub fn public_grpc_server() -> PublicGrpcServer {
    public_grpc_server_with(GrpcReceiveLimits::default())
}

/// Returns the gRPC server builder for a public Zinder listener, with the
/// given receive limits.
///
/// Production listeners use [`public_grpc_server`]. This form exists so
/// tests can exercise the limits with small values.
#[must_use]
pub fn public_grpc_server_with(limits: GrpcReceiveLimits) -> PublicGrpcServer {
    describe_receive_limit_metrics();
    Server::builder()
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS_PER_CONNECTION)
        .http2_keepalive_interval(Some(HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(HTTP2_KEEPALIVE_TIMEOUT))
        .layer(GrpcReceiveLimitLayer::new(limits))
}

fn describe_receive_limit_metrics() {
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
    /// its next frame.
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
    /// stream may go without sending a frame.
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
/// arrives. When its pool has no free slot, the request is refused with
/// `UNAVAILABLE` and its body is never read. There are two pools of the same
/// size, so client streams cannot starve single-message calls:
///
/// - A unary or server-streaming call frees its slot once its request body
///   ends, before the handler runs. A long-lived response stream holds no
///   slot. A request that has not finished arriving within the receive
///   timeout of its stream opening fails with `DEADLINE_EXCEEDED`.
/// - A client-streaming or bidirectional call, including gRPC reflection,
///   keeps its slot until the client half-closes or the call ends. A client
///   stream that sends no frame for the receive timeout fails with
///   `DEADLINE_EXCEEDED`.
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
        let slots = limits.slots_per_pool.get();
        Self {
            pools: ReceivePools {
                requests: Arc::new(Semaphore::new(slots)),
                client_streams: Arc::new(Semaphore::new(slots)),
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
        let Ok(permit) = self.pools.semaphore(pool).try_acquire_owned() else {
            metrics::counter!(RECEIVE_REFUSALS_METRIC, "pool" => pool.label()).increment(1);
            let status = Status::unavailable(
                "server is receiving too many requests at once; retry with backoff",
            );
            return Either::Left(ready(Ok(status.into_http())));
        };
        let receive_timeout = self.pools.receive_timeout;
        let request = request.map(|body| {
            Body::new(ReceiveBoundBody {
                inner: body,
                state: ReceiveState::Receiving {
                    _permit: permit,
                    deadline: Box::pin(tokio::time::sleep(receive_timeout)),
                },
                pool,
                receive_timeout,
            })
        });
        Either::Right(self.inner.call(request))
    }
}

#[derive(Clone, Debug)]
struct ReceivePools {
    requests: Arc<Semaphore>,
    client_streams: Arc<Semaphore>,
    receive_timeout: Duration,
}

impl ReceivePools {
    fn semaphore(&self, pool: ReceivePool) -> Arc<Semaphore> {
        match pool {
            ReceivePool::Request => Arc::clone(&self.requests),
            ReceivePool::ClientStream => Arc::clone(&self.client_streams),
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
}

enum ReceiveState {
    Receiving {
        _permit: OwnedSemaphorePermit,
        deadline: Pin<Box<Sleep>>,
    },
    Received,
    TimedOut,
}

impl http_body::Body for ReceiveBoundBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let deadline = match &mut this.state {
            ReceiveState::Receiving { deadline, .. } => deadline,
            ReceiveState::Received => return Pin::new(&mut this.inner).poll_frame(cx),
            ReceiveState::TimedOut => return Poll::Ready(None),
        };
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if this.inner.is_end_stream() {
                    this.state = ReceiveState::Received;
                } else if this.pool == ReceivePool::ClientStream {
                    deadline
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
                if deadline.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                this.state = ReceiveState::TimedOut;
                metrics::counter!(RECEIVE_TIMEOUTS_METRIC, "pool" => this.pool.label())
                    .increment(1);
                Poll::Ready(Some(Err(timeout_status(this.pool))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match self.state {
            ReceiveState::TimedOut => true,
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
            Status::deadline_exceeded("client stream sent nothing for too long")
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
