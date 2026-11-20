//! Ending every gRPC call a listener still serves once its runtime stops.
//!
//! A graceful drain lets open calls finish, but the transport keeps a call's
//! handler running for as long as its client keeps the stream open, and its
//! connection tasks outlive the server future. [`OpenGrpcCalls::terminate`]
//! therefore drops each call's handler future or response body itself.
//!
//! A handler is created, polled, and dropped only under its call's lock, and
//! a call is admitted, before its handler exists, only while the listener
//! still serves. When [`OpenGrpcCalls::terminate`] returns, no handler future
//! or response body of the listener exists, a drop that another thread had
//! started has finished, and every later call is refused before its service
//! is called. The next frame each terminated response yields is its status.
//!
//! A handler can hand its client's request stream to work of its own, as
//! gRPC reflection does. That work could keep the HTTP/2 stream, and so the
//! connection, open after the handler is gone, so termination also ends a
//! client-streaming call's request stream, even once its response has ended:
//! the stream's next poll fails with the call's status, and a consumer parked
//! on it is woken to make that poll.
//!
//! hyper polls a response body only while its client grants flow-control
//! window, so a body cannot be relied on to observe the termination itself;
//! dropping it from [`OpenGrpcCalls::terminate`] works for parked bodies too.
//!
//! Calls register in one of [`REGISTRY_SHARDS`] shards, picked by the thread
//! that admits them, so concurrent calls on different threads rarely share a
//! lock.

use std::{
    future::Future,
    mem,
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use bytes::Bytes;
use futures_util::future::{BoxFuture, ready};
use http_body::{Body as _, Frame, SizeHint};
use parking_lot::Mutex;
use slab::Slab;
use tonic::{Status, body::Body};
use tower::{Layer, Service};

use super::CLIENT_STREAMING_METHOD_PATHS;
use crate::{ReadinessCause, not_ready_status};

/// How many shards a listener's calls register in.
const REGISTRY_SHARDS: usize = 16;

/// The shard the next thread to admit a call registers its calls in.
static NEXT_HOME_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// The shard this thread registers the calls it admits in.
    static HOME_SHARD: usize = NEXT_HOME_SHARD.fetch_add(1, Ordering::Relaxed) % REGISTRY_SHARDS;
}

/// Calls the runtime is serving, and whether it has stopped serving them.
pub(crate) struct OpenGrpcCalls {
    shards: [Arc<CallShard>; REGISTRY_SHARDS],
}

impl Default for OpenGrpcCalls {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| Arc::default()),
        }
    }
}

impl std::fmt::Debug for OpenGrpcCalls {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenGrpcCalls")
            .finish_non_exhaustive()
    }
}

/// The calls one shard tracks. Aligned so that two shards never share a
/// cache line.
#[derive(Default)]
#[repr(align(128))]
struct CallShard {
    calls: Mutex<ShardCalls>,
}

#[derive(Default)]
struct ShardCalls {
    terminated: bool,
    calls: Slab<Weak<dyn TerminateCall>>,
}

impl OpenGrpcCalls {
    /// Ends every open call with `UNAVAILABLE` and refuses every later one.
    ///
    /// Returns how many calls still had a running handler.
    pub(crate) fn terminate(&self) -> usize {
        self.shards.iter().map(|shard| shard.terminate()).sum()
    }

    /// Admits a call before its handler is created, or returns `None` once
    /// the calls are terminated.
    fn admit<F>(&self) -> Option<Arc<OpenCall<F>>>
    where
        F: Send + 'static,
    {
        let shard = &self.shards[HOME_SHARD.with(|home_shard| *home_shard)];
        let mut shard_calls = shard.calls.lock();
        if shard_calls.terminated {
            return None;
        }
        let entry = shard_calls.calls.vacant_entry();
        let call = Arc::new(OpenCall {
            state: Mutex::new(CallState::Admitted),
            request: Mutex::new(RequestState::Untracked),
            _registration: CallRegistration {
                shard: Arc::clone(shard),
                key: entry.key(),
            },
        });
        let tracked: Weak<OpenCall<F>> = Arc::downgrade(&call);
        entry.insert(tracked);
        drop(shard_calls);
        Some(call)
    }
}

impl CallShard {
    fn terminate(&self) -> usize {
        let open_calls = {
            let mut shard_calls = self.calls.lock();
            shard_calls.terminated = true;
            mem::take(&mut shard_calls.calls)
        };
        open_calls
            .into_iter()
            .filter_map(|(_, call)| call.upgrade())
            .filter(|call| call.terminate())
            .count()
    }

    fn forget(&self, key: usize) {
        let forgotten = self.calls.lock().calls.try_remove(key);
        drop(forgotten);
    }
}

/// Tower layer that lets a supervised runtime end every call its gRPC
/// listener serves.
///
/// [`crate::SupervisedRuntime::public_grpc_router`] adds it outside every
/// other layer. Once the runtime stops serving, each open call's handler is
/// dropped and the call ends with `UNAVAILABLE` and `SERVICE_NOT_READY`; a
/// call that arrives later is refused the same way before the service it
/// wraps is called.
#[derive(Clone, Debug)]
pub struct GrpcCallTerminationLayer {
    calls: Arc<OpenGrpcCalls>,
}

impl GrpcCallTerminationLayer {
    pub(crate) const fn new(calls: Arc<OpenGrpcCalls>) -> Self {
        Self { calls }
    }
}

impl<S> Layer<S> for GrpcCallTerminationLayer {
    type Service = GrpcCallTermination<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcCallTermination {
            inner,
            calls: Arc::clone(&self.calls),
        }
    }
}

/// The tower service that [`GrpcCallTerminationLayer`] wraps around a router.
#[derive(Clone, Debug)]
pub struct GrpcCallTermination<S> {
    inner: S,
    calls: Arc<OpenGrpcCalls>,
}

impl<S> Service<http::Request<Body>> for GrpcCallTermination<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let Some(call) = self.calls.admit::<S::Future>() else {
            return refused();
        };
        let request = if CLIENT_STREAMING_METHOD_PATHS.contains(&request.uri().path()) {
            call.track_request(request)
        } else {
            request
        };
        if !call.start(|| self.inner.call(request)) {
            return refused();
        }
        Box::pin(TerminableCall {
            call,
            responded: false,
        })
    }
}

fn refused<E: Send + 'static>() -> BoxFuture<'static, Result<http::Response<Body>, E>> {
    Box::pin(ready(Ok(server_stopped().into_http())))
}

fn server_stopped() -> Status {
    not_ready_status(&ReadinessCause::ShuttingDown)
}

trait TerminateCall: Send + Sync {
    /// Drops the call's handler and returns whether it was still running.
    fn terminate(&self) -> bool;
}

struct OpenCall<F> {
    state: Mutex<CallState<F>>,
    /// A client stream's request body, which can outlive the handler.
    request: Mutex<RequestState>,
    _registration: CallRegistration,
}

enum RequestState {
    /// The handler owns the request body directly.
    Untracked,
    /// The client is still sending its stream.
    Receiving { body: Body, waker: Option<Waker> },
    /// The runtime ended the call; the stream has not reported it yet.
    Terminated,
    /// The stream has ended, and its body is gone.
    Ended,
}

enum CallState<F> {
    /// The call was admitted and its handler is not created yet.
    Admitted,
    /// The handler is producing its response head.
    Responding {
        future: Pin<Box<F>>,
        waker: Option<Waker>,
    },
    /// The handler is producing its response body.
    Streaming { body: Body, waker: Option<Waker> },
    /// The runtime ended the call; its status has not been reported yet.
    Terminated,
    /// The call has ended, and its handler is gone.
    Finished,
}

impl<F: Send + 'static> OpenCall<F> {
    /// Moves the client's request stream into the call, so termination can
    /// end it wherever the handler hands it.
    fn track_request(self: &Arc<Self>, request: http::Request<Body>) -> http::Request<Body> {
        request.map(|body| {
            *self.request.lock() = RequestState::Receiving { body, waker: None };
            Body::new(TerminableRequest {
                call: Arc::clone(self),
            })
        })
    }
}

impl<F> OpenCall<F> {
    /// Creates the call's handler with `handler`, unless the call was
    /// terminated after it was admitted.
    fn start(&self, handler: impl FnOnce() -> F) -> bool {
        let mut state = self.state.lock();
        match *state {
            CallState::Admitted => {
                *state = CallState::Responding {
                    future: Box::pin(handler()),
                    waker: None,
                };
                true
            }
            CallState::Responding { .. }
            | CallState::Streaming { .. }
            | CallState::Terminated
            | CallState::Finished => false,
        }
    }
}

/// Removes a call from its shard once nothing refers to it.
struct CallRegistration {
    shard: Arc<CallShard>,
    key: usize,
}

impl Drop for CallRegistration {
    fn drop(&mut self) {
        self.shard.forget(self.key);
    }
}

impl<F: Send> TerminateCall for OpenCall<F> {
    fn terminate(&self) -> bool {
        let mut state = self.state.lock();
        let (was_running, response_waker) = match &mut *state {
            CallState::Admitted => {
                *state = CallState::Terminated;
                (false, None)
            }
            CallState::Responding { waker, .. } | CallState::Streaming { waker, .. } => {
                let waker = waker.take();
                // Replacing the state drops the handler under the lock, so
                // neither a poll nor another drop of it is still running when
                // this returns.
                *state = CallState::Terminated;
                (true, waker)
            }
            // The response has ended, but work its handler handed the request
            // stream to can still hold that stream.
            CallState::Terminated | CallState::Finished => (false, None),
        };
        drop(state);
        let mut request = self.request.lock();
        let request_waker = match &mut *request {
            RequestState::Receiving { waker, .. } => {
                let waker = waker.take();
                // Replacing the state drops the client's stream, which does
                // not wake a consumer parked on it.
                *request = RequestState::Terminated;
                waker
            }
            RequestState::Untracked | RequestState::Terminated | RequestState::Ended => None,
        };
        drop(request);
        for waker in [response_waker, request_waker].into_iter().flatten() {
            waker.wake();
        }
        was_running
    }
}

/// The request body of a client-streaming call.
struct TerminableRequest<F> {
    call: Arc<OpenCall<F>>,
}

impl<F: Send + 'static> http_body::Body for TerminableRequest<F> {
    type Data = Bytes;
    type Error = Status;

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the request body is polled under its lock, so termination never drops it mid-poll"
    )]
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut request = self.call.request.lock();
        match &mut *request {
            RequestState::Receiving { body, waker } => {
                let polled = Pin::new(body).poll_frame(cx);
                match &polled {
                    Poll::Pending => remember_waker(waker, cx),
                    Poll::Ready(None | Some(Err(_))) => *request = RequestState::Ended,
                    Poll::Ready(Some(Ok(_))) => {}
                }
                polled
            }
            RequestState::Terminated => {
                *request = RequestState::Ended;
                Poll::Ready(Some(Err(server_stopped())))
            }
            RequestState::Untracked | RequestState::Ended => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &*self.call.request.lock() {
            RequestState::Receiving { body, .. } => body.is_end_stream(),
            RequestState::Terminated => false,
            RequestState::Untracked | RequestState::Ended => true,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &*self.call.request.lock() {
            RequestState::Receiving { body, .. } => body.size_hint(),
            RequestState::Untracked | RequestState::Terminated | RequestState::Ended => {
                SizeHint::default()
            }
        }
    }
}

impl<F> Drop for TerminableRequest<F> {
    /// Drops the request stream with the handler's last hold on it, as the
    /// transport expects.
    fn drop(&mut self) {
        *self.call.request.lock() = RequestState::Ended;
    }
}

fn remember_waker(slot: &mut Option<Waker>, cx: &Context<'_>) {
    match slot {
        Some(waker) if waker.will_wake(cx.waker()) => {}
        waker => *waker = Some(cx.waker().clone()),
    }
}

/// The response future of a tracked call.
struct TerminableCall<F> {
    call: Arc<OpenCall<F>>,
    /// Whether the call has produced its response, which leaves the future
    /// nothing to drop.
    responded: bool,
}

impl<F, E> Future for TerminableCall<F>
where
    F: Future<Output = Result<http::Response<Body>, E>> + Send + 'static,
{
    type Output = Result<http::Response<Body>, E>;

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the handler is polled under its call's lock, so termination never drops it mid-poll"
    )]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this.call.state.lock();
        match &mut *state {
            CallState::Responding { future, waker } => match future.as_mut().poll(cx) {
                Poll::Pending => {
                    remember_waker(waker, cx);
                    Poll::Pending
                }
                Poll::Ready(Ok(response)) => {
                    let (parts, body) = response.into_parts();
                    *state = CallState::Streaming { body, waker: None };
                    drop(state);
                    this.responded = true;
                    let body = Body::new(TerminableBody {
                        call: Arc::clone(&this.call) as Arc<dyn CallBody>,
                        ended: false,
                    });
                    Poll::Ready(Ok(http::Response::from_parts(parts, body)))
                }
                Poll::Ready(Err(error)) => {
                    *state = CallState::Finished;
                    this.responded = true;
                    Poll::Ready(Err(error))
                }
            },
            CallState::Terminated => {
                *state = CallState::Finished;
                this.responded = true;
                Poll::Ready(Ok(server_stopped().into_http()))
            }
            CallState::Admitted | CallState::Streaming { .. } | CallState::Finished => {
                this.responded = true;
                Poll::Ready(Ok(server_stopped().into_http()))
            }
        }
    }
}

impl<F> Drop for TerminableCall<F> {
    /// Drops a handler whose response was abandoned, under its call's lock.
    fn drop(&mut self) {
        if self.responded {
            return;
        }
        let mut state = self.call.state.lock();
        if matches!(*state, CallState::Responding { .. }) {
            *state = CallState::Finished;
        }
    }
}

/// The body side of a tracked call, with the handler's future type erased.
trait CallBody: Send + Sync {
    fn poll_frame(&self, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Status>>>;
    fn is_end_stream(&self) -> bool;
    fn size_hint(&self) -> SizeHint;
    /// Drops the handler's response body, under the call's lock.
    fn release(&self);
}

impl<F: Send> CallBody for OpenCall<F> {
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the handler's body is polled under its call's lock, so termination never drops it mid-poll"
    )]
    fn poll_frame(&self, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let mut state = self.state.lock();
        match &mut *state {
            CallState::Streaming { body, waker } => match Pin::new(body).poll_frame(cx) {
                Poll::Pending => {
                    remember_waker(waker, cx);
                    Poll::Pending
                }
                Poll::Ready(Some(Ok(frame))) => {
                    if frame.is_trailers() {
                        *state = CallState::Finished;
                    }
                    Poll::Ready(Some(Ok(frame)))
                }
                ended @ Poll::Ready(Some(Err(_)) | None) => {
                    *state = CallState::Finished;
                    ended
                }
            },
            CallState::Terminated => {
                *state = CallState::Finished;
                Poll::Ready(Some(trailers_reporting(&server_stopped())))
            }
            CallState::Admitted | CallState::Responding { .. } | CallState::Finished => {
                Poll::Ready(None)
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match &*self.state.lock() {
            CallState::Streaming { body, .. } => body.is_end_stream(),
            CallState::Admitted | CallState::Responding { .. } | CallState::Finished => true,
            CallState::Terminated => false,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &*self.state.lock() {
            CallState::Streaming { body, .. } => body.size_hint(),
            CallState::Admitted
            | CallState::Responding { .. }
            | CallState::Terminated
            | CallState::Finished => SizeHint::default(),
        }
    }

    fn release(&self) {
        let mut state = self.state.lock();
        if matches!(*state, CallState::Streaming { .. }) {
            *state = CallState::Finished;
        }
    }
}

fn trailers_reporting(status: &Status) -> Result<Frame<Bytes>, Status> {
    let mut trailers = http::HeaderMap::new();
    status.add_header(&mut trailers)?;
    Ok(Frame::trailers(trailers))
}

/// The response body of a tracked call.
struct TerminableBody {
    call: Arc<dyn CallBody>,
    /// Whether the body has reported its end, which leaves nothing to drop.
    ended: bool,
}

impl http_body::Body for TerminableBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let polled = this.call.poll_frame(cx);
        this.ended = match &polled {
            Poll::Ready(Some(Ok(frame))) => frame.is_trailers(),
            Poll::Ready(Some(Err(_)) | None) => true,
            Poll::Pending => false,
        };
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.call.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.call.size_hint()
    }
}

impl Drop for TerminableBody {
    fn drop(&mut self) {
        if !self.ended {
            self.call.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        future,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc,
        },
        task::{Context, Poll, Wake, Waker},
        time::Duration,
    };

    use futures_util::FutureExt as _;
    use http_body_util::{BodyExt as _, StreamBody};
    use tokio::sync::oneshot;
    use tokio_stream::StreamExt as _;
    use tonic::{Code, Status, body::Body};
    use tonic_types::StatusExt as _;
    use tower::{Layer as _, Service as _, ServiceExt as _, service_fn};

    use super::{GrpcCallTerminationLayer, OpenGrpcCalls};
    use crate::{GrpcReceiveLimits, public_grpc_server::supervised_grpc_layers};

    type TestError = Box<dyn std::error::Error + Send + Sync>;
    type TestResult = Result<(), TestError>;

    /// Reports when the handler state it belongs to is dropped.
    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    /// Records whether the task it belongs to was woken.
    #[derive(Default)]
    struct WakeRecord(AtomicBool);

    impl Wake for WakeRecord {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    impl OpenGrpcCalls {
        fn registered_calls(&self) -> usize {
            self.shards
                .iter()
                .map(|shard| shard.calls.lock().calls.len())
                .sum()
        }
    }

    /// Asserts that `headers` carry the status of a call the stopping
    /// runtime ended or refused.
    fn assert_shutting_down(headers: &http::HeaderMap) -> TestResult {
        let status = Status::from_header_map(headers).ok_or("the response carries no status")?;
        assert_eq!(status.code(), Code::Unavailable);
        let details = status.get_error_details();
        let error_info = details
            .error_info()
            .ok_or("the status carries no ErrorInfo")?;
        assert_eq!(error_info.reason, "SERVICE_NOT_READY");
        assert_eq!(
            error_info
                .metadata
                .get("readiness_cause")
                .map(String::as_str),
            Some("shutting_down")
        );
        Ok(())
    }

    #[tokio::test]
    async fn terminating_drops_a_pending_handler_and_ends_the_call() -> TestResult {
        let calls = Arc::new(OpenGrpcCalls::default());
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let dropped_tx = parking_lot::Mutex::new(Some(dropped_tx));
        let mut service = GrpcCallTerminationLayer::new(Arc::clone(&calls)).layer(service_fn(
            move |_request: http::Request<Body>| {
                let signal = DropSignal(dropped_tx.lock().take());
                async move {
                    let _signal = signal;
                    future::pending::<Result<http::Response<Body>, Infallible>>().await
                }
            },
        ));
        let call = tokio::spawn(
            service
                .ready()
                .await?
                .call(http::Request::new(Body::empty())),
        );
        tokio::task::yield_now().await;

        assert_eq!(calls.terminate(), 1);
        dropped_rx.await?;
        let response = call.await??;
        assert_shutting_down(response.headers())
    }

    #[tokio::test]
    async fn a_call_after_termination_never_reaches_its_service() -> TestResult {
        let calls = Arc::new(OpenGrpcCalls::default());
        let service_calls = Arc::new(AtomicUsize::new(0));
        let mut service = GrpcCallTerminationLayer::new(Arc::clone(&calls)).layer(service_fn({
            let service_calls = Arc::clone(&service_calls);
            move |_request: http::Request<Body>| {
                service_calls.fetch_add(1, Ordering::SeqCst);
                future::ready(Ok::<_, Infallible>(http::Response::new(Body::empty())))
            }
        }));
        let served = service
            .ready()
            .await?
            .call(http::Request::new(Body::empty()))
            .await?;
        served.into_body().collect().await?;
        assert_eq!(service_calls.load(Ordering::SeqCst), 1);

        assert_eq!(calls.terminate(), 0);
        let refused = service
            .ready()
            .await?
            .call(http::Request::new(Body::empty()))
            .await?;

        assert_eq!(
            service_calls.load(Ordering::SeqCst),
            1,
            "a call after termination reached the service"
        );
        assert_shutting_down(refused.headers())
    }

    #[tokio::test]
    async fn terminating_drops_an_unpolled_response_body() -> TestResult {
        let calls = Arc::new(OpenGrpcCalls::default());
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let dropped_tx = parking_lot::Mutex::new(Some(dropped_tx));
        let mut service = GrpcCallTerminationLayer::new(Arc::clone(&calls)).layer(service_fn(
            move |_request: http::Request<Body>| {
                let signal = DropSignal(dropped_tx.lock().take());
                let frames =
                    tokio_stream::pending::<Result<http_body::Frame<bytes::Bytes>, Status>>().map(
                        move |frame| {
                            let _held = &signal;
                            frame
                        },
                    );
                future::ready(Ok::<_, Infallible>(http::Response::new(Body::new(
                    StreamBody::new(frames),
                ))))
            },
        ));
        let response = service
            .ready()
            .await?
            .call(http::Request::new(Body::empty()))
            .await?;

        // Nothing polls the body, as with a client that withholds window.
        assert_eq!(calls.terminate(), 1);
        dropped_rx.await?;
        let trailers = response
            .into_body()
            .collect()
            .await?
            .trailers()
            .cloned()
            .unwrap_or_default();
        assert_shutting_down(&trailers)
    }

    /// A client-streaming call whose handler handed its request stream to work
    /// of its own, as gRPC reflection hands it to a task.
    struct HandedOffCall {
        /// The call's response, not read yet. Its body ends without data
        /// once read.
        response: http::Response<Body>,
        /// The request stream the handler handed off.
        request_stream: Body,
        /// Reports when the client's stream behind `request_stream` is
        /// dropped. The client never sends on it.
        client_stream_dropped: oneshot::Receiver<()>,
    }

    async fn call_handing_off_its_request_stream(
        calls: &Arc<OpenGrpcCalls>,
    ) -> Result<HandedOffCall, TestError> {
        let handed_off = Arc::new(parking_lot::Mutex::new(None::<Body>));
        let mut service = GrpcCallTerminationLayer::new(Arc::clone(calls)).layer(service_fn({
            let handed_off = Arc::clone(&handed_off);
            move |request: http::Request<Body>| {
                *handed_off.lock() = Some(request.into_body());
                // An empty `Body` would end the response before it is read.
                let frames =
                    tokio_stream::empty::<Result<http_body::Frame<bytes::Bytes>, Status>>();
                future::ready(Ok::<_, Infallible>(http::Response::new(Body::new(
                    StreamBody::new(frames),
                ))))
            }
        }));
        let (dropped_tx, client_stream_dropped) = oneshot::channel();
        let signal = DropSignal(Some(dropped_tx));
        let client_stream =
            tokio_stream::pending::<Result<http_body::Frame<bytes::Bytes>, Status>>().map(
                move |frame| {
                    let _held = &signal;
                    frame
                },
            );
        let request = http::Request::builder()
            .uri("/grpc.reflection.v1.ServerReflection/ServerReflectionInfo")
            .body(Body::new(StreamBody::new(client_stream)))?;
        let response = service.ready().await?.call(request).await?;
        let request_stream = handed_off
            .lock()
            .take()
            .ok_or("the handler kept no request stream")?;
        Ok(HandedOffCall {
            response,
            request_stream,
            client_stream_dropped,
        })
    }

    /// Asserts that a request stream's next poll reported the status of a
    /// call the stopping runtime ended.
    fn assert_request_stream_ended(
        polled: Option<Result<http_body::Frame<bytes::Bytes>, Status>>,
    ) -> TestResult {
        let status = polled
            .ok_or("the request stream ended without its status")?
            .err()
            .ok_or("the request stream yielded a frame after termination")?;
        let mut headers = http::HeaderMap::new();
        status.add_header(&mut headers)?;
        assert_shutting_down(&headers)
    }

    /// gRPC reflection hands its client's request stream to a task of its
    /// own. Terminating the call must end that stream too, or the task
    /// keeps the HTTP/2 stream, and so the connection, open.
    #[tokio::test]
    async fn terminating_a_client_stream_ends_the_request_stream_its_handler_handed_off()
    -> TestResult {
        let calls = Arc::new(OpenGrpcCalls::default());
        let HandedOffCall {
            response: _response,
            mut request_stream,
            client_stream_dropped,
        } = call_handing_off_its_request_stream(&calls).await?;

        assert_eq!(calls.terminate(), 1);
        client_stream_dropped.await?;
        assert_request_stream_ended(request_stream.frame().await)
    }

    /// The task holding a handed-off request stream can outlive the call's
    /// response, so termination must end the stream after the response has
    /// ended, whether it was read to its end or dropped.
    #[tokio::test]
    async fn terminating_ends_a_handed_off_request_stream_after_the_response_ended() -> TestResult {
        for read_response_to_its_end in [true, false] {
            let calls = Arc::new(OpenGrpcCalls::default());
            let HandedOffCall {
                response,
                mut request_stream,
                mut client_stream_dropped,
            } = call_handing_off_its_request_stream(&calls).await?;
            if read_response_to_its_end {
                response.into_body().collect().await?;
            } else {
                drop(response);
            }

            assert_eq!(calls.terminate(), 0);
            client_stream_dropped.try_recv()?;
            assert_request_stream_ended(
                request_stream
                    .frame()
                    .now_or_never()
                    .ok_or("the request stream is still open")?,
            )?;
        }
        Ok(())
    }

    /// Dropping the client's stream does not wake a task already parked on
    /// the request stream it was handed, so termination must wake it.
    #[tokio::test]
    async fn terminating_wakes_a_task_parked_on_a_handed_off_request_stream() -> TestResult {
        let calls = Arc::new(OpenGrpcCalls::default());
        let HandedOffCall {
            response: _response,
            mut request_stream,
            ..
        } = call_handing_off_its_request_stream(&calls).await?;
        let wake_record = Arc::new(WakeRecord::default());
        let waker = Waker::from(Arc::clone(&wake_record));
        let mut context = Context::from_waker(&waker);
        assert!(request_stream.frame().poll_unpin(&mut context).is_pending());

        assert_eq!(calls.terminate(), 1);
        assert!(
            wake_record.0.load(Ordering::SeqCst),
            "termination left the parked task asleep"
        );
        let Poll::Ready(polled) = request_stream.frame().poll_unpin(&mut context) else {
            return Err("the woken request stream is still open".into());
        };
        assert_request_stream_ended(polled)
    }

    /// The send stall bound hands the transport a large chunk in pieces. A
    /// terminated response must not send the pieces it still holds.
    #[tokio::test]
    async fn a_terminated_response_sends_its_status_before_any_more_data() -> TestResult {
        let calls = Arc::new(OpenGrpcCalls::default());
        let chunk = bytes::Bytes::from(vec![0_u8; 1024 * 1024]);
        let mut service = supervised_grpc_layers(
            Arc::clone(&calls),
            GrpcReceiveLimits::default(),
            Duration::from_hours(1),
        )
        .layer(service_fn(move |_request: http::Request<Body>| {
            let frames = tokio_stream::once(Ok::<_, Status>(http_body::Frame::data(chunk.clone())))
                .chain(tokio_stream::pending());
            future::ready(Ok::<_, Infallible>(http::Response::new(Body::new(
                StreamBody::new(frames),
            ))))
        }));
        let response = service
            .ready()
            .await?
            .call(http::Request::new(Body::empty()))
            .await?;
        let mut body = response.into_body();
        let first = body.frame().await.ok_or("the response ended at once")??;
        assert!(first.is_data());

        assert_eq!(calls.terminate(), 1);
        let next = body
            .frame()
            .await
            .ok_or("the response ended without a status")??;

        let trailers = next
            .into_trailers()
            .map_err(|_| "the terminated response sent more data")?;
        assert_shutting_down(&trailers)
    }

    /// A handler that another thread is dropping, such as one whose client
    /// went away, is still being torn down when termination reaches it.
    /// Termination must not return before that drop has finished.
    #[test]
    fn terminate_returns_only_after_a_handler_another_thread_drops() -> TestResult {
        struct BlockingDrop {
            dropping: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
            log: Arc<parking_lot::Mutex<Vec<&'static str>>>,
        }

        impl Drop for BlockingDrop {
            fn drop(&mut self) {
                let _ = self.dropping.send(());
                let _ = self.release.recv();
                self.log.lock().push("handler dropped");
            }
        }

        let calls = Arc::new(OpenGrpcCalls::default());
        let log = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (dropping_tx, dropping_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let handler_state = parking_lot::Mutex::new(Some(BlockingDrop {
            dropping: dropping_tx,
            release: release_rx,
            log: Arc::clone(&log),
        }));
        let mut service = GrpcCallTerminationLayer::new(Arc::clone(&calls)).layer(service_fn(
            move |_request: http::Request<Body>| {
                let held = handler_state.lock().take();
                async move {
                    let _held = held;
                    future::pending::<Result<http::Response<Body>, Infallible>>().await
                }
            },
        ));
        service
            .ready()
            .now_or_never()
            .ok_or("the service was not ready")??;
        let call = service.call(http::Request::new(Body::empty()));

        let dropper = std::thread::spawn(move || drop(call));
        dropping_rx.recv()?;
        let (terminating_tx, terminating_rx) = mpsc::channel();
        let terminator = std::thread::spawn({
            let calls = Arc::clone(&calls);
            let log = Arc::clone(&log);
            move || {
                let _ = terminating_tx.send(());
                calls.terminate();
                log.lock().push("terminate returned");
            }
        });
        terminating_rx.recv()?;
        std::thread::sleep(Duration::from_millis(100));
        release_tx.send(())?;
        dropper.join().map_err(|_| "the dropping thread panicked")?;
        terminator
            .join()
            .map_err(|_| "the terminating thread panicked")?;

        assert_eq!(*log.lock(), ["handler dropped", "terminate returned"]);
        Ok(())
    }

    #[tokio::test]
    async fn a_finished_call_leaves_nothing_to_terminate() -> TestResult {
        let calls = Arc::new(OpenGrpcCalls::default());
        let mut service = GrpcCallTerminationLayer::new(Arc::clone(&calls)).layer(service_fn(
            |_request: http::Request<Body>| {
                future::ready(Ok::<_, Infallible>(http::Response::new(Body::empty())))
            },
        ));
        let response = service
            .ready()
            .await?
            .call(http::Request::new(Body::empty()))
            .await?;
        response.into_body().collect().await?;

        assert_eq!(calls.registered_calls(), 0);
        assert_eq!(calls.terminate(), 0);
        Ok(())
    }
}
