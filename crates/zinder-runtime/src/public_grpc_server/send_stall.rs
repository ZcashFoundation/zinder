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
//! A public listener whose transport can be reset watches stalls at the
//! transport instead ([`super::stream_reset`]): there, a stall resets the
//! HTTP/2 stream or HTTP/1.1 connection, which also drops this body and the
//! handler. This layer then starts no stall watchdog of its own and watches
//! only for a [`ResponseCancellation`].
//!
//! Data chunks are handed to hyper in pieces of at most
//! [`RESPONSE_PIECE_BYTES`], so a slow but steady client is polled often
//! enough that it never looks stalled. A piece is a copy, so the bytes hyper
//! keeps for a stalled stream pin none of the handler's buffers, unless the
//! response carries [`BoundedChunkRetention`]: then pieces are slices of
//! chunks whose allocations its producer bounds.
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
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::{Status, body::Body};
use tower::{Layer, Service};
use zinder_proto::compat::lightwalletd::BoundedChunkRetention;

use super::{SEND_STALLS_METRIC, stream_reset::TransportReset};

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
            .map_ok(move |response| {
                // The response future resolves inside the task that carries
                // the response, where a resettable transport is in scope.
                let watch_stalls = TransportReset::current().is_none();
                let pieces = Pieces::for_response(&response);
                response.map(|body| {
                    Body::new(SendStallBoundBody::new(
                        body,
                        send_stall_timeout,
                        cancellation,
                        watch_stalls,
                        pieces,
                    ))
                })
            })
            .boxed()
    }
}

/// How the layer hands a response's data chunks to the transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Pieces {
    /// Copies, so a held piece pins only itself.
    Copied,
    /// Slices of the handler's chunks, each of which pins at most
    /// [`BoundedChunkRetention::MAX_ALLOCATION_BYTES`] and at most twice its
    /// bytes.
    Sliced,
}

impl Pieces {
    /// Slices only a response that zinder-proto's range body marked.
    ///
    /// Layers outside this one, such as gRPC-Web, see the slices: binary
    /// gRPC-Web passes them on unchanged, and base64 gRPC-Web re-encodes each
    /// into a buffer of its own and drops it.
    fn for_response<B>(response: &http::Response<B>) -> Self {
        if response
            .extensions()
            .get::<BoundedChunkRetention>()
            .is_some()
        {
            Self::Sliced
        } else {
            Self::Copied
        }
    }
}

/// A response body that is dropped once its client stops reading it.
struct SendStallBoundBody {
    sending: Arc<Mutex<Sending>>,
    send_stall_timeout: Duration,
    /// Shared with the watchdog when it starts.
    cancellation: Option<ResponseCancellation>,
    /// Whether this body ends its response when the transport stalls, which
    /// it does only when the transport cannot be reset.
    watch_stalls: bool,
    /// Stops the watchdog when the response ends. Set once the first data
    /// piece is handed out, or at once for a cancellable response.
    watchdog: Option<DropGuard>,
}

struct Sending {
    source: Source,
    pieces: Pieces,
    /// Whether the pieces of `unsent` are slices of it.
    slice_unsent: bool,
    /// The part of the handler's last data chunk not yet handed out.
    unsent: Bytes,
    /// When the transport took the last data piece, until it polls again.
    handed_out_at: Option<Instant>,
    /// The transport's waker from the last poll the handler left pending.
    /// Dropping the handler's body does not wake it, so abandoning does.
    waker: Option<Waker>,
}

enum Source {
    Handler(Body),
    /// The handler's body was dropped; the stream ends with this status.
    Abandoned(Status),
    /// The abandoned stream reported its status.
    Reported,
}

impl SendStallBoundBody {
    fn new(
        inner: Body,
        send_stall_timeout: Duration,
        cancellation: Option<ResponseCancellation>,
        watch_stalls: bool,
        pieces: Pieces,
    ) -> Self {
        let cancellable = cancellation.is_some();
        let mut body = Self {
            sending: Arc::new(Mutex::new(Sending {
                source: Source::Handler(inner),
                pieces,
                slice_unsent: false,
                unsent: Bytes::new(),
                handed_out_at: None,
                waker: None,
            })),
            send_stall_timeout,
            cancellation,
            watch_stalls,
            watchdog: None,
        };
        if cancellable {
            body.start_watchdog();
        }
        body
    }

    fn start_watchdog(&mut self) {
        if self.watchdog.is_some() || (!self.watch_stalls && self.cancellation.is_none()) {
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
            self.watch_stalls,
        ));
        self.watchdog = Some(finished.drop_guard());
    }
}

impl Sending {
    fn take_chunk(&mut self, chunk: Bytes) {
        // A chunk longer than the bound cannot sit in an allocation within it.
        self.slice_unsent = self.pieces == Pieces::Sliced
            && chunk.len() <= BoundedChunkRetention::MAX_ALLOCATION_BYTES;
        self.unsent = chunk;
    }

    /// Hands out the next piece of `unsent`.
    ///
    /// A slice of `unsent` would share the handler's allocation, often
    /// tonic's whole encode buffer, so the piece hyper keeps for a stalled
    /// stream would keep that buffer alive after the handler's body is
    /// dropped. A piece is therefore a copy, unless the response's producer
    /// bounds that allocation.
    fn hand_out_piece(&mut self) -> Bytes {
        let piece_len = self.unsent.len().min(RESPONSE_PIECE_BYTES);
        if self.slice_unsent {
            // Splitting off the last piece leaves `unsent` holding nothing.
            let piece = self.unsent.split_to(piece_len);
            self.handed_out_at = Some(Instant::now());
            return piece;
        }
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
    fn abandon(&mut self, status: Status) -> (Option<Body>, Option<Waker>) {
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
/// `send_stall_timeout`, when `watch_stalls` is set, or at once when
/// `cancellation` fires.
async fn abandon_when_stalled_or_cancelled(
    sending: Weak<Mutex<Sending>>,
    send_stall_timeout: Duration,
    finished: CancellationToken,
    cancellation: Option<ResponseCancellation>,
    watch_stalls: bool,
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
            () = tokio::time::sleep_until(checked_at + send_stall_timeout), if watch_stalls => true,
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
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let mut sending = this.sending.lock();
        sending.handed_out_at = None;
        if !sending.unsent.is_empty() {
            return Poll::Ready(Some(Ok(Frame::data(sending.hand_out_piece()))));
        }
        let handler_body = match &mut sending.source {
            Source::Handler(handler_body) => handler_body,
            Source::Abandoned(status) => {
                let trailers = trailers_reporting(status);
                sending.source = Source::Reported;
                return Poll::Ready(Some(trailers));
            }
            Source::Reported => return Poll::Ready(None),
        };
        let final_trailers = match Pin::new(handler_body).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(chunk) => {
                    sending.take_chunk(chunk);
                    let piece = sending.hand_out_piece();
                    drop(sending);
                    this.start_watchdog();
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
                return failed;
            }
        };
        // The handler has ended its response. A request-side failure it did
        // not report becomes the response's status.
        let receive_failure = this
            .cancellation
            .as_ref()
            .and_then(ResponseCancellation::failure);
        if let Some(status) = receive_failure {
            sending.source = Source::Reported;
            drop(sending);
            return Poll::Ready(Some(trailers_reporting(&status)));
        }
        drop(sending);
        Poll::Ready(final_trailers.map(Ok))
    }

    fn is_end_stream(&self) -> bool {
        let sending = self.sending.lock();
        sending.unsent.is_empty()
            && match &sending.source {
                Source::Handler(handler_body) => handler_body.is_end_stream(),
                Source::Abandoned(_) => false,
                Source::Reported => true,
            }
    }

    fn size_hint(&self) -> SizeHint {
        let sending = self.sending.lock();
        match &sending.source {
            Source::Handler(handler_body) if sending.unsent.is_empty() => handler_body.size_hint(),
            Source::Handler(_) | Source::Abandoned(_) | Source::Reported => SizeHint::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use http_body::{Body as _, Frame};
    use http_body_util::{BodyExt as _, StreamBody};
    use tokio::sync::oneshot;
    use tokio_stream::StreamExt as _;
    use tokio_util::sync::CancellationToken;
    use tonic::{Status, body::Body};

    use super::{
        BoundedChunkRetention, Pieces, RESPONSE_PIECE_BYTES, ResponseCancellation,
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
        let mut body = SendStallBoundBody::new(
            handler_body,
            Duration::from_hours(1),
            Some(ResponseCancellation::new(
                cancelled.clone(),
                Status::unavailable("evicted"),
            )),
            true,
            Pieces::Copied,
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
            next_frame.await
        });
        assert!(polled.await?, "the handler body was not pending");

        cancelled.cancel();
        let frame = tokio::time::timeout(Duration::from_secs(5), transport)
            .await
            .map_err(|_| "the transport was never woken")??
            .ok_or("the response ended without a status")??;
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
            handler_body,
            Duration::from_hours(1),
            None,
            true,
            Pieces::Copied,
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
            handler_body,
            Duration::from_hours(1),
            Some(ResponseCancellation::new(
                cancelled.clone(),
                Status::unavailable("evicted"),
            )),
            true,
            Pieces::Copied,
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

    /// A handler body that sends one tracked chunk of `len` bytes, then
    /// stays pending, and the receiver that reports the chunk freed.
    fn one_tracked_chunk(len: usize) -> (Body, oneshot::Receiver<()>) {
        let (freed_sender, freed) = oneshot::channel();
        let chunk = Bytes::from_owner(TrackedChunk {
            bytes: vec![0; len],
            _freed: freed_sender,
        });
        let handler_body = Body::new(StreamBody::new(
            tokio_stream::once(Ok::<_, Status>(Frame::data(chunk))).chain(tokio_stream::pending()),
        ));
        (handler_body, freed)
    }

    async fn first_piece(
        body: &mut SendStallBoundBody,
    ) -> Result<Bytes, Box<dyn std::error::Error>> {
        Ok(body
            .frame()
            .await
            .ok_or("the response ended before its first piece")??
            .into_data()
            .map_err(|_| "the first frame carried no data")?)
    }

    /// Drops a stalled response of one `len`-byte chunk, and with it the
    /// handler's body, while the transport holds its first piece, and reports
    /// whether that piece still keeps the chunk alive.
    async fn held_piece_pins_chunk(len: usize) -> Result<bool, Box<dyn std::error::Error>> {
        let (handler_body, mut freed) = one_tracked_chunk(len);
        let mut body = SendStallBoundBody::new(
            handler_body,
            Duration::from_hours(1),
            None,
            true,
            Pieces::Sliced,
        );
        let held_piece = first_piece(&mut body).await?;
        assert_eq!(held_piece.len(), RESPONSE_PIECE_BYTES);

        drop(body);
        let pinned = freed.try_recv() == Err(oneshot::error::TryRecvError::Empty);
        drop(held_piece);
        assert_eq!(freed.try_recv(), Err(oneshot::error::TryRecvError::Closed));
        Ok(pinned)
    }

    #[tokio::test]
    async fn a_held_slice_pins_a_chunk_within_the_bound_and_nothing_larger()
    -> Result<(), Box<dyn std::error::Error>> {
        let bound = BoundedChunkRetention::MAX_ALLOCATION_BYTES;
        assert!(
            held_piece_pins_chunk(bound).await?,
            "a chunk within the bound is sliced"
        );
        assert!(
            !held_piece_pins_chunk(bound + 1).await?,
            "a chunk over the bound is copied"
        );
        Ok(())
    }

    #[tokio::test]
    async fn handing_out_the_last_slice_leaves_the_layer_holding_nothing()
    -> Result<(), Box<dyn std::error::Error>> {
        let (handler_body, mut freed) = one_tracked_chunk(RESPONSE_PIECE_BYTES + 1);
        let mut body = SendStallBoundBody::new(
            handler_body,
            Duration::from_hours(1),
            None,
            true,
            Pieces::Sliced,
        );
        let first = first_piece(&mut body).await?;
        let last = first_piece(&mut body).await?;
        assert_eq!(last.len(), 1);
        drop((first, last));
        // The handler is still running and the layer still exists.
        assert_eq!(freed.try_recv(), Err(oneshot::error::TryRecvError::Closed));
        drop(body);
        Ok(())
    }

    #[test]
    fn an_unmarked_response_is_copied() {
        let response = http::Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .body(())
            .unwrap_or_default();
        assert_eq!(Pieces::for_response(&response), Pieces::Copied);
    }

    #[tokio::test(start_paused = true)]
    async fn a_response_on_a_resettable_transport_leaves_stalls_to_the_transport()
    -> Result<(), Box<dyn std::error::Error>> {
        let (freed_sender, mut freed) = oneshot::channel();
        let chunk = Bytes::from_owner(TrackedChunk {
            bytes: vec![0; 4 * 1024 * 1024],
            _freed: freed_sender,
        });
        let handler_body = Body::new(StreamBody::new(
            tokio_stream::once(Ok::<_, Status>(Frame::data(chunk))).chain(tokio_stream::pending()),
        ));
        let mut body = SendStallBoundBody::new(
            handler_body,
            Duration::from_secs(60),
            None,
            false,
            Pieces::Copied,
        );

        // The transport takes one piece and never asks for the next.
        let held_piece = body
            .frame()
            .await
            .ok_or("the response ended before its first piece")??;
        tokio::time::sleep(Duration::from_mins(5)).await;
        assert_eq!(
            freed.try_recv(),
            Err(oneshot::error::TryRecvError::Empty),
            "the layer ended a response whose transport bounds its stalls"
        );
        drop((held_piece, body));
        Ok(())
    }
}
