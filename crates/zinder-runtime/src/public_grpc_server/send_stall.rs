//! Ending gRPC responses whose client stops reading them.
//!
//! hyper polls a response body for its next frame only after the client's
//! HTTP/2 flow-control window has room for the previous one. A client that
//! keeps its connection alive but sends no `WINDOW_UPDATE` therefore parks the
//! response body, and whatever produces it, indefinitely: a handler task
//! blocked on a full channel keeps its request stream, receive slot, and
//! buffered messages. Nothing inside the parked body runs, so a watchdog task
//! tracks how long the transport has held the last piece handed to it and,
//! after the stall timeout, drops the handler's body. Dropping it unwinds the
//! handler the way a client cancellation does.
//!
//! Data chunks are handed to hyper as copies of at most
//! [`RESPONSE_PIECE_BYTES`], so the bytes hyper itself keeps for a stalled
//! stream stay small and pin none of the handler's buffers, and a slow but
//! steady client is polled often enough that it never looks stalled.
//!
//! An outer layer can also end a response early by attaching a
//! [`ResponseCancellation`] to its request. The receive limit uses it for a
//! client stream that loses its receive slot, because a handler blocked on
//! its response never polls the request body that reports the eviction.

use std::{
    mem,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::{Buf as _, Bytes};
use futures_util::{FutureExt as _, TryFutureExt as _, future::BoxFuture};
use http_body::{Frame, SizeHint};
use parking_lot::Mutex;
use tokio::{sync::OwnedSemaphorePermit, time::Instant};
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::{Status, body::Body};
use tower::{Layer, Service};

use super::SEND_STALLS_METRIC;

/// Largest piece of a response data chunk handed to the transport at once.
const RESPONSE_PIECE_BYTES: usize = 64 * 1024;

/// How a request's receive side tells [`GrpcSendStall`] to end its
/// response.
///
/// An outer layer attaches it to the request extensions. Once `token` is
/// cancelled, the handler's body is dropped and the response ends with
/// `status`. A failure recorded with [`ResponseCancellation::fail`] replaces
/// the status the handler ends its response with, because a handler such as
/// gRPC reflection ends successfully when its request stream fails.
#[derive(Clone, Debug)]
pub(super) struct ResponseCancellation {
    token: CancellationToken,
    status: Status,
    failure: Arc<Mutex<Option<Status>>>,
}

impl ResponseCancellation {
    pub(super) fn new(token: CancellationToken, status: Status) -> Self {
        Self {
            token,
            status,
            failure: Arc::new(Mutex::new(None)),
        }
    }

    /// Records why the request side failed. The first failure wins.
    pub(super) fn fail(&self, status: &Status) {
        self.failure.lock().get_or_insert_with(|| status.clone());
    }

    fn failure(&self) -> Option<Status> {
        self.failure.lock().clone()
    }
}

#[derive(Clone)]
struct ResponsePermit {
    _permit: Arc<OwnedSemaphorePermit>,
}

/// Holds `permit` until the public gRPC transport finishes or discards the response.
///
/// Use with a server that installs [`GrpcSendStallLayer`], as
/// [`super::public_grpc_server`] does. This keeps an admission slot charged
/// while the layer still holds encoded response bytes.
#[must_use]
pub fn hold_grpc_response_permit<T>(
    mut response: tonic::Response<T>,
    permit: OwnedSemaphorePermit,
) -> tonic::Response<T> {
    response.extensions_mut().insert(ResponsePermit {
        _permit: Arc::new(permit),
    });
    response
}

/// Tower layer that ends a response stream once its client has stopped
/// reading for the send stall timeout.
///
/// A response stalls when the transport has held a piece of it for the
/// timeout without asking for the next, which happens when the client
/// withholds HTTP/2 flow-control window. The handler's response body is then
/// dropped, which cancels the handler and frees what it holds. If the client
/// resumes reading, the stream ends with `DEADLINE_EXCEEDED`. A response
/// whose server side is waiting for something to send is not stalled.
#[derive(Clone, Copy, Debug)]
pub struct GrpcSendStallLayer {
    send_stall_timeout: Duration,
}

impl GrpcSendStallLayer {
    /// Creates a layer that ends responses stalled for `send_stall_timeout`.
    #[must_use]
    pub const fn new(send_stall_timeout: Duration) -> Self {
        Self { send_stall_timeout }
    }
}

impl<S> Layer<S> for GrpcSendStallLayer {
    type Service = GrpcSendStall<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcSendStall {
            inner,
            send_stall_timeout: self.send_stall_timeout,
        }
    }
}

/// The tower service that [`GrpcSendStallLayer`] wraps around a router.
#[derive(Clone, Debug)]
pub struct GrpcSendStall<S> {
    inner: S,
    send_stall_timeout: Duration,
}

impl<S> Service<http::Request<Body>> for GrpcSendStall<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>>,
    S::Future: Send + 'static,
    S::Error: 'static,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let send_stall_timeout = self.send_stall_timeout;
        let cancellation = request.extensions().get::<ResponseCancellation>().cloned();
        self.inner
            .call(request)
            .map_ok(move |mut response| {
                let permit = response.extensions_mut().remove::<ResponsePermit>();
                response.map(|body| {
                    Body::new(SendStallBoundBody::new(
                        HandlerResponse { body, permit },
                        send_stall_timeout,
                        cancellation,
                    ))
                })
            })
            .boxed()
    }
}

/// A response body that is dropped once its client stops reading it.
struct SendStallBoundBody {
    sending: Arc<Mutex<Sending>>,
    send_stall_timeout: Duration,
    /// Shared with the watchdog when it starts.
    cancellation: Option<ResponseCancellation>,
    /// Stops the watchdog when the response ends. Set once the first data
    /// piece is handed out, or at once for a cancellable response.
    watchdog: Option<DropGuard>,
}

struct Sending {
    source: Source,
    /// The part of the handler's last data chunk not yet handed out.
    unsent: Bytes,
    /// When the transport took the last data piece, until it polls again.
    handed_out_at: Option<Instant>,
    /// The transport's waker from the last poll the handler left pending.
    /// Dropping the handler's body does not wake it, so abandoning does.
    waker: Option<Waker>,
}

struct HandlerResponse {
    body: Body,
    permit: Option<ResponsePermit>,
}

enum Source {
    Handler(HandlerResponse),
    /// The handler's body was dropped; the stream ends with this status.
    Abandoned(Status),
    /// The abandoned stream reported its status.
    Reported,
}

impl SendStallBoundBody {
    fn new(
        inner: HandlerResponse,
        send_stall_timeout: Duration,
        cancellation: Option<ResponseCancellation>,
    ) -> Self {
        let cancellable = cancellation.is_some();
        let mut body = Self {
            sending: Arc::new(Mutex::new(Sending {
                source: Source::Handler(inner),
                unsent: Bytes::new(),
                handed_out_at: None,
                waker: None,
            })),
            send_stall_timeout,
            cancellation,
            watchdog: None,
        };
        if cancellable {
            body.start_watchdog();
        }
        body
    }

    fn start_watchdog(&mut self) {
        if self.watchdog.is_some() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let finished = CancellationToken::new();
        runtime.spawn(abandon_when_stalled_or_cancelled(
            Arc::downgrade(&self.sending),
            self.send_stall_timeout,
            finished.clone(),
            self.cancellation.clone(),
        ));
        self.watchdog = Some(finished.drop_guard());
    }
}

impl Sending {
    /// Copies the next piece of `unsent` into an allocation of its own.
    ///
    /// A slice of `unsent` would share the handler's allocation, often
    /// tonic's whole encode buffer, so the one piece hyper keeps for a
    /// stalled stream would keep that buffer alive after the handler's body
    /// is dropped.
    fn hand_out_piece(&mut self) -> Bytes {
        let piece_len = self.unsent.len().min(RESPONSE_PIECE_BYTES);
        let piece = Bytes::copy_from_slice(self.unsent.get(..piece_len).unwrap_or_default());
        self.unsent.advance(piece_len);
        if self.unsent.is_empty() {
            // An advanced-out `Bytes` still owns the handler's allocation.
            self.unsent = Bytes::new();
        }
        self.handed_out_at = Some(Instant::now());
        piece
    }

    fn is_stalled(&self, send_stall_timeout: Duration) -> bool {
        self.handed_out_at
            .is_some_and(|handed_out_at| handed_out_at.elapsed() >= send_stall_timeout)
    }

    /// Drops what the response still holds and records `status` for the
    /// transport. Returns the handler's body and the transport's waker, so
    /// the caller drops the one and wakes the other unlocked.
    fn abandon(&mut self, status: Status) -> (Option<HandlerResponse>, Option<Waker>) {
        self.unsent = Bytes::new();
        match mem::replace(&mut self.source, Source::Abandoned(status)) {
            Source::Handler(body) => (Some(body), self.waker.take()),
            finished @ (Source::Abandoned(_) | Source::Reported) => {
                self.source = finished;
                (None, None)
            }
        }
    }
}

/// Drops the handler's body once the transport has held the last piece for
/// `send_stall_timeout`, or at once when `cancellation` fires.
async fn abandon_when_stalled_or_cancelled(
    sending: Weak<Mutex<Sending>>,
    send_stall_timeout: Duration,
    finished: CancellationToken,
    cancellation: Option<ResponseCancellation>,
) {
    let (cancelled, cancel_status) = match cancellation {
        Some(ResponseCancellation { token, status, .. }) => (token, status),
        None => (
            CancellationToken::new(),
            Status::cancelled("response cancelled"),
        ),
    };
    loop {
        let Some(shared) = sending.upgrade() else {
            return;
        };
        let checked_at = shared.lock().handed_out_at.unwrap_or_else(Instant::now);
        drop(shared);
        let stalled = tokio::select! {
            () = finished.cancelled() => return,
            () = cancelled.cancelled() => false,
            () = tokio::time::sleep_until(checked_at + send_stall_timeout) => true,
        };
        let Some(shared) = sending.upgrade() else {
            return;
        };
        let mut sending_state = shared.lock();
        let abandoned = if !stalled {
            sending_state.abandon(cancel_status)
        } else if sending_state.is_stalled(send_stall_timeout) {
            metrics::counter!(SEND_STALLS_METRIC).increment(1);
            sending_state.abandon(client_stopped_reading())
        } else {
            continue;
        };
        drop(sending_state);
        drop(shared);
        let (handler_body, transport) = abandoned;
        drop(handler_body);
        if let Some(transport) = transport {
            transport.wake();
        }
        return;
    }
}

fn client_stopped_reading() -> Status {
    Status::deadline_exceeded("client stopped reading the stream")
}

fn trailers_reporting(status: &Status) -> Result<Frame<Bytes>, Status> {
    let mut trailers = http::HeaderMap::new();
    status.add_header(&mut trailers)?;
    Ok(Frame::trailers(trailers))
}

impl http_body::Body for SendStallBoundBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut sending = self.sending.lock();
        sending.handed_out_at = None;
        if !sending.unsent.is_empty() {
            return Poll::Ready(Some(Ok(Frame::data(sending.hand_out_piece()))));
        }
        let (finish_admitted_response, outcome) = match &mut sending.source {
            Source::Handler(handler) => (
                handler.permit.is_some(),
                Pin::new(&mut handler.body).poll_frame(cx),
            ),
            Source::Abandoned(status) => {
                let trailers = trailers_reporting(status);
                sending.source = Source::Reported;
                return Poll::Ready(Some(trailers));
            }
            Source::Reported => return Poll::Ready(None),
        };
        let final_trailers = match outcome {
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(chunk) => {
                    sending.unsent = chunk;
                    let piece = sending.hand_out_piece();
                    drop(sending);
                    self.start_watchdog();
                    return Poll::Ready(Some(Ok(Frame::data(piece))));
                }
                Err(frame) if frame.is_trailers() => Some(frame),
                Err(frame) => return Poll::Ready(Some(Ok(frame))),
            },
            Poll::Ready(None) => None,
            Poll::Pending => {
                match &mut sending.waker {
                    Some(waker) if waker.will_wake(cx.waker()) => {}
                    waker => *waker = Some(cx.waker().clone()),
                }
                return Poll::Pending;
            }
            failed @ Poll::Ready(Some(Err(_))) => {
                let finished = finish_admitted_response
                    .then(|| mem::replace(&mut sending.source, Source::Reported));
                drop(sending);
                drop(finished);
                return failed;
            }
        };
        // The handler has ended its response. A request-side failure it did
        // not report becomes the response's status.
        let receive_failure = self
            .cancellation
            .as_ref()
            .and_then(ResponseCancellation::failure);
        let finished = (finish_admitted_response || receive_failure.is_some())
            .then(|| mem::replace(&mut sending.source, Source::Reported));
        drop(sending);
        drop(finished);
        Poll::Ready(receive_failure.map_or_else(
            || final_trailers.map(Ok),
            |status| Some(trailers_reporting(&status)),
        ))
    }

    fn is_end_stream(&self) -> bool {
        let sending = self.sending.lock();
        sending.unsent.is_empty()
            && match &sending.source {
                Source::Handler(handler_body) => handler_body.body.is_end_stream(),
                Source::Abandoned(_) => false,
                Source::Reported => true,
            }
    }

    fn size_hint(&self) -> SizeHint {
        let sending = self.sending.lock();
        match &sending.source {
            Source::Handler(handler_body) if sending.unsent.is_empty() => {
                handler_body.body.size_hint()
            }
            Source::Handler(_) | Source::Abandoned(_) | Source::Reported => SizeHint::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use http_body::{Body as _, Frame};
    use http_body_util::{BodyExt as _, StreamBody};
    use tokio::sync::{Semaphore, oneshot};
    use tokio_stream::StreamExt as _;
    use tokio_util::sync::CancellationToken;
    use tonic::{Status, body::Body};

    use super::{
        HandlerResponse, RESPONSE_PIECE_BYTES, ResponseCancellation, ResponsePermit,
        SendStallBoundBody,
    };

    /// A response chunk that reports when its allocation is freed.
    struct TrackedChunk {
        bytes: Vec<u8>,
        _freed: oneshot::Sender<()>,
    }

    impl AsRef<[u8]> for TrackedChunk {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    #[tokio::test]
    async fn cancelling_a_pending_response_wakes_the_transport_with_its_status()
    -> Result<(), Box<dyn std::error::Error>> {
        let handler_body = Body::new(StreamBody::new(tokio_stream::pending::<
            Result<Frame<Bytes>, Status>,
        >()));
        let cancelled = CancellationToken::new();
        let slots = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&slots).try_acquire_owned()?;
        let mut body = SendStallBoundBody::new(
            HandlerResponse {
                body: handler_body,
                permit: Some(ResponsePermit {
                    _permit: Arc::new(permit),
                }),
            },
            Duration::from_hours(1),
            Some(ResponseCancellation::new(
                cancelled.clone(),
                Status::unavailable("evicted"),
            )),
        );
        let (polled_sender, polled) = oneshot::channel();
        let transport = tokio::spawn(async move {
            let mut first_poll = Some(polled_sender);
            let next_frame = std::future::poll_fn(|cx| {
                let next = std::pin::Pin::new(&mut body).poll_frame(cx);
                if let Some(polled_sender) = first_poll.take() {
                    let _ = polled_sender.send(next.is_pending());
                }
                next
            });
            (next_frame.await, body)
        });
        assert!(polled.await?, "the handler body was not pending");
        assert_eq!(slots.available_permits(), 0);

        cancelled.cancel();
        let (frame, _body) = tokio::time::timeout(Duration::from_secs(5), transport)
            .await
            .map_err(|_| "the transport was never woken")??;
        assert_eq!(slots.available_permits(), 1);
        let frame = frame.ok_or("the response ended without a status")??;
        let status = frame
            .trailers_ref()
            .and_then(|trailers| trailers.get("grpc-status"))
            .map(http::HeaderValue::as_bytes);
        assert_eq!(status, Some(b"14".as_slice()), "expected UNAVAILABLE");
        Ok(())
    }

    #[tokio::test]
    async fn handing_out_a_chunk_s_last_piece_frees_the_chunk()
    -> Result<(), Box<dyn std::error::Error>> {
        let (freed_sender, mut freed) = oneshot::channel();
        let chunk = Bytes::from_owner(TrackedChunk {
            bytes: vec![0; RESPONSE_PIECE_BYTES + 1],
            _freed: freed_sender,
        });
        let handler_body = Body::new(StreamBody::new(
            tokio_stream::once(Ok::<_, Status>(Frame::data(chunk))).chain(tokio_stream::pending()),
        ));
        let mut body = SendStallBoundBody::new(
            HandlerResponse {
                body: handler_body,
                permit: None,
            },
            Duration::from_hours(1),
            None,
        );

        let first = body
            .frame()
            .await
            .ok_or("the response ended before its first piece")??;
        assert_eq!(freed.try_recv(), Err(oneshot::error::TryRecvError::Empty));
        let last = body
            .frame()
            .await
            .ok_or("the response ended before its last piece")??;

        // The transport holds both pieces and the handler is still running.
        assert_eq!(
            freed.try_recv(),
            Err(oneshot::error::TryRecvError::Closed),
            "the sent chunk stayed alive until the next chunk"
        );
        drop((first, last));
        Ok(())
    }

    #[tokio::test]
    async fn abandoning_a_response_frees_its_chunk_while_the_transport_holds_a_piece()
    -> Result<(), Box<dyn std::error::Error>> {
        let (freed_sender, freed) = oneshot::channel();
        let chunk = Bytes::from_owner(TrackedChunk {
            bytes: vec![0; 4 * 1024 * 1024],
            _freed: freed_sender,
        });
        let handler_body = Body::new(StreamBody::new(
            tokio_stream::once(Ok::<_, Status>(Frame::data(chunk))).chain(tokio_stream::pending()),
        ));
        let cancelled = CancellationToken::new();
        let mut body = SendStallBoundBody::new(
            HandlerResponse {
                body: handler_body,
                permit: None,
            },
            Duration::from_hours(1),
            Some(ResponseCancellation::new(
                cancelled.clone(),
                Status::unavailable("evicted"),
            )),
        );

        // The transport takes one piece and keeps it, as hyper does while
        // the client withholds flow-control window.
        let held_piece = body
            .frame()
            .await
            .ok_or("the response ended before its first piece")??
            .into_data()
            .map_err(|_| "the first frame carried no data")?;
        assert_eq!(held_piece.len(), RESPONSE_PIECE_BYTES);

        cancelled.cancel();
        let chunk_freed = tokio::time::timeout(Duration::from_secs(5), freed).await;
        assert!(
            chunk_freed.is_ok(),
            "the held piece kept the handler's chunk alive"
        );
        assert_eq!(held_piece.len(), RESPONSE_PIECE_BYTES);
        Ok(())
    }
}
