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
//! Data chunks are handed to hyper in pieces of at most
//! [`RESPONSE_PIECE_BYTES`], so the bytes hyper itself keeps for a stalled
//! stream stay small, and a slow but steady client is polled often enough
//! that it never looks stalled.

use std::{
    mem,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{FutureExt as _, TryFutureExt as _, future::BoxFuture};
use http_body::{Frame, SizeHint};
use parking_lot::Mutex;
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::{Status, body::Body};
use tower::{Layer, Service};

use super::SEND_STALLS_METRIC;

/// Largest piece of a response data chunk handed to the transport at once.
const RESPONSE_PIECE_BYTES: usize = 64 * 1024;

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
        self.inner
            .call(request)
            .map_ok(move |response| {
                response.map(|body| Body::new(SendStallBoundBody::new(body, send_stall_timeout)))
            })
            .boxed()
    }
}

/// A response body that is dropped once its client stops reading it.
struct SendStallBoundBody {
    sending: Arc<Mutex<Sending>>,
    send_stall_timeout: Duration,
    /// Stops the watchdog when the response ends. Set once the first data
    /// piece is handed out.
    watchdog: Option<DropGuard>,
}

struct Sending {
    source: Source,
    /// The part of the handler's last data chunk not yet handed out.
    unsent: Bytes,
    /// When the transport took the last data piece, until it polls again.
    handed_out_at: Option<Instant>,
}

enum Source {
    Handler(Body),
    /// The client stopped reading and the handler's body was dropped.
    Abandoned,
    /// The abandoned stream reported its status.
    Reported,
}

impl SendStallBoundBody {
    fn new(inner: Body, send_stall_timeout: Duration) -> Self {
        Self {
            sending: Arc::new(Mutex::new(Sending {
                source: Source::Handler(inner),
                unsent: Bytes::new(),
                handed_out_at: None,
            })),
            send_stall_timeout,
            watchdog: None,
        }
    }

    fn start_watchdog(&mut self) {
        if self.watchdog.is_some() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let finished = CancellationToken::new();
        runtime.spawn(abandon_when_stalled(
            Arc::downgrade(&self.sending),
            self.send_stall_timeout,
            finished.clone(),
        ));
        self.watchdog = Some(finished.drop_guard());
    }
}

impl Sending {
    fn hand_out_piece(&mut self) -> Bytes {
        let piece_len = self.unsent.len().min(RESPONSE_PIECE_BYTES);
        self.handed_out_at = Some(Instant::now());
        self.unsent.split_to(piece_len)
    }

    /// Drops the handler's body if the transport has held the last piece for
    /// `send_stall_timeout`, returning it so the caller drops it unlocked.
    fn abandon_if_stalled(&mut self, send_stall_timeout: Duration) -> Option<Body> {
        let stalled = self
            .handed_out_at
            .is_some_and(|handed_out_at| handed_out_at.elapsed() >= send_stall_timeout);
        if !stalled {
            return None;
        }
        self.unsent = Bytes::new();
        match mem::replace(&mut self.source, Source::Abandoned) {
            Source::Handler(body) => Some(body),
            finished @ (Source::Abandoned | Source::Reported) => {
                self.source = finished;
                None
            }
        }
    }
}

async fn abandon_when_stalled(
    sending: Weak<Mutex<Sending>>,
    send_stall_timeout: Duration,
    finished: CancellationToken,
) {
    loop {
        let Some(shared) = sending.upgrade() else {
            return;
        };
        let checked_at = shared.lock().handed_out_at.unwrap_or_else(Instant::now);
        drop(shared);
        tokio::select! {
            () = finished.cancelled() => return,
            () = tokio::time::sleep_until(checked_at + send_stall_timeout) => {}
        }
        let Some(shared) = sending.upgrade() else {
            return;
        };
        let abandoned = shared.lock().abandon_if_stalled(send_stall_timeout);
        drop(shared);
        if let Some(handler_body) = abandoned {
            drop(handler_body);
            metrics::counter!(SEND_STALLS_METRIC).increment(1);
            return;
        }
    }
}

fn client_stopped_reading_trailers() -> Result<Frame<Bytes>, Status> {
    let mut trailers = http::HeaderMap::new();
    Status::deadline_exceeded("client stopped reading the stream").add_header(&mut trailers)?;
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
            Source::Abandoned => {
                sending.source = Source::Reported;
                return Poll::Ready(Some(client_stopped_reading_trailers()));
            }
            Source::Reported => return Poll::Ready(None),
        };
        match Pin::new(handler_body).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(chunk) => {
                    sending.unsent = chunk;
                    let piece = sending.hand_out_piece();
                    drop(sending);
                    this.start_watchdog();
                    Poll::Ready(Some(Ok(Frame::data(piece))))
                }
                Err(frame) => Poll::Ready(Some(Ok(frame))),
            },
            ended_or_failed @ (Poll::Ready(Some(Err(_)) | None) | Poll::Pending) => ended_or_failed,
        }
    }

    fn is_end_stream(&self) -> bool {
        let sending = self.sending.lock();
        sending.unsent.is_empty()
            && match &sending.source {
                Source::Handler(handler_body) => handler_body.is_end_stream(),
                Source::Abandoned => false,
                Source::Reported => true,
            }
    }

    fn size_hint(&self) -> SizeHint {
        let sending = self.sending.lock();
        match &sending.source {
            Source::Handler(handler_body) if sending.unsent.is_empty() => handler_body.size_hint(),
            Source::Handler(_) | Source::Abandoned | Source::Reported => SizeHint::default(),
        }
    }
}
