//! Ending every gRPC call a listener still serves once its runtime stops.
//!
//! A graceful drain lets open calls finish, but the transport keeps a call's
//! handler running for as long as its client keeps the stream open, and its
//! connection tasks outlive the server future. [`OpenGrpcCalls::terminate`]
//! therefore drops each call's handler future or response body itself, under
//! the lock that every poll of that handler takes. When it returns, no
//! handler future or response body of the listener exists, and every later
//! call is refused before its handler runs.
//!
//! hyper polls a response body only while its client grants flow-control
//! window, so a body cannot be relied on to observe the termination itself;
//! dropping it from [`OpenGrpcCalls::terminate`] works for parked bodies too.

use std::{
    collections::HashMap,
    future::Future,
    mem,
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
};

use bytes::Bytes;
use futures_util::future::{BoxFuture, ready};
use http_body::{Body as _, Frame, SizeHint};
use parking_lot::Mutex;
use tonic::{Status, body::Body};
use tower::{Layer, Service};

/// Calls the runtime is serving, and whether it has stopped serving them.
#[derive(Debug, Default)]
pub(crate) struct OpenGrpcCalls {
    next_call_id: AtomicU64,
    registry: Mutex<CallRegistry>,
}

#[derive(Default)]
struct CallRegistry {
    terminated: bool,
    calls: HashMap<u64, Weak<dyn TerminateCall>>,
}

impl std::fmt::Debug for CallRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CallRegistry")
            .field("terminated", &self.terminated)
            .field("open_calls", &self.calls.len())
            .finish_non_exhaustive()
    }
}

impl OpenGrpcCalls {
    /// Ends every open call with `UNAVAILABLE` and refuses every later one.
    ///
    /// Returns how many calls still had a running handler.
    pub(crate) fn terminate(&self) -> usize {
        let open_calls = {
            let mut registry = self.registry.lock();
            registry.terminated = true;
            mem::take(&mut registry.calls)
        };
        open_calls
            .into_values()
            .filter_map(|call| call.upgrade())
            .filter(|call| call.terminate())
            .count()
    }

    /// Tracks the call whose handler is `future`, or returns `None` once the
    /// calls are terminated.
    fn track<F>(self: &Arc<Self>, future: F) -> Option<Arc<OpenCall<F>>>
    where
        F: Send + 'static,
    {
        let call_id = self.next_call_id.fetch_add(1, Ordering::Relaxed);
        let call = Arc::new(OpenCall {
            state: Mutex::new(CallState::Responding {
                future: Box::pin(future),
                waker: None,
            }),
            _registration: CallRegistration {
                calls: Arc::clone(self),
                call_id,
            },
        });
        let tracked: Weak<OpenCall<F>> = Arc::downgrade(&call);
        let tracked: Weak<dyn TerminateCall> = tracked;
        let admitted = {
            let mut registry = self.registry.lock();
            let admitted = !registry.terminated;
            if admitted {
                registry.calls.insert(call_id, tracked);
            }
            admitted
        };
        admitted.then_some(call)
    }

    fn forget(&self, call_id: u64) {
        self.registry.lock().calls.remove(&call_id);
    }
}

/// Tower layer that lets a supervised runtime end every call its gRPC
/// listener serves.
///
/// [`crate::SupervisedRuntime::public_grpc_server`] adds it. Once the runtime
/// stops serving, each open call's handler is dropped and the call ends with
/// `UNAVAILABLE`; a call that arrives later is refused the same way before
/// its handler runs.
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
        match self.calls.track(self.inner.call(request)) {
            Some(call) => Box::pin(TerminableCall { call }),
            None => Box::pin(ready(Ok(server_stopped().into_http()))),
        }
    }
}

fn server_stopped() -> Status {
    Status::unavailable("server is shutting down; retry against another instance")
}

trait TerminateCall: Send + Sync {
    /// Drops the call's handler and returns whether it was still running.
    fn terminate(&self) -> bool;
}

struct OpenCall<F> {
    state: Mutex<CallState<F>>,
    _registration: CallRegistration,
}

enum CallState<F> {
    /// The handler is producing its response head.
    Responding {
        future: Pin<Box<F>>,
        waker: Option<Waker>,
    },
    /// The handler is producing its response body.
    Streaming { body: Body, waker: Option<Waker> },
    /// The runtime ended the call; its status has not been reported yet.
    Terminated,
    /// The call has ended and reported how.
    Finished,
}

/// Removes a call from the registry once nothing refers to it.
struct CallRegistration {
    calls: Arc<OpenGrpcCalls>,
    call_id: u64,
}

impl Drop for CallRegistration {
    fn drop(&mut self) {
        self.calls.forget(self.call_id);
    }
}

impl<F: Send> TerminateCall for OpenCall<F> {
    fn terminate(&self) -> bool {
        let mut state = self.state.lock();
        let mut handler = mem::replace(&mut *state, CallState::Terminated);
        let waker = match &mut handler {
            CallState::Responding { waker, .. } | CallState::Streaming { waker, .. } => {
                waker.take()
            }
            CallState::Terminated | CallState::Finished => {
                *state = handler;
                return false;
            }
        };
        drop(state);
        // The handler is dropped unlocked, before its poller is woken.
        drop(handler);
        if let Some(waker) = waker {
            waker.wake();
        }
        true
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
                    let body = Body::new(TerminableBody {
                        call: Arc::clone(&this.call) as Arc<dyn CallBody>,
                    });
                    Poll::Ready(Ok(http::Response::from_parts(parts, body)))
                }
                Poll::Ready(Err(error)) => {
                    *state = CallState::Finished;
                    Poll::Ready(Err(error))
                }
            },
            CallState::Terminated => {
                *state = CallState::Finished;
                Poll::Ready(Ok(server_stopped().into_http()))
            }
            CallState::Streaming { .. } | CallState::Finished => {
                Poll::Ready(Ok(server_stopped().into_http()))
            }
        }
    }
}

/// The body side of a tracked call, with the handler's future type erased.
trait CallBody: Send + Sync {
    fn poll_frame(&self, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Status>>>;
    fn is_end_stream(&self) -> bool;
    fn size_hint(&self) -> SizeHint;
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
            CallState::Responding { .. } | CallState::Finished => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &*self.state.lock() {
            CallState::Streaming { body, .. } => body.is_end_stream(),
            CallState::Responding { .. } | CallState::Finished => true,
            CallState::Terminated => false,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &*self.state.lock() {
            CallState::Streaming { body, .. } => body.size_hint(),
            CallState::Responding { .. } | CallState::Terminated | CallState::Finished => {
                SizeHint::default()
            }
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
}

impl http_body::Body for TerminableBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.call.poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.call.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.call.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, future, sync::Arc};

    use http_body_util::{BodyExt as _, StreamBody};
    use tokio::sync::oneshot;
    use tokio_stream::StreamExt as _;
    use tonic::{Code, Status, body::Body};
    use tower::{Layer as _, Service as _, ServiceExt as _, service_fn};

    use super::{GrpcCallTerminationLayer, OpenGrpcCalls};

    /// Reports when the handler state it belongs to is dropped.
    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    fn status_of(response: &http::Response<Body>) -> Option<Code> {
        response
            .headers()
            .get("grpc-status")
            .and_then(|status| status.to_str().ok())
            .and_then(|status| status.parse::<i32>().ok())
            .map(Code::from)
    }

    #[tokio::test]
    async fn terminating_drops_a_pending_handler_and_refuses_later_calls()
    -> Result<(), Box<dyn std::error::Error>> {
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
        assert_eq!(status_of(&response), Some(Code::Unavailable));

        let refused = service
            .ready()
            .await?
            .call(http::Request::new(Body::empty()))
            .await?;
        assert_eq!(status_of(&refused), Some(Code::Unavailable));
        Ok(())
    }

    #[tokio::test]
    async fn terminating_drops_an_unpolled_response_body() -> Result<(), Box<dyn std::error::Error>>
    {
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
        assert_eq!(
            Status::from_header_map(&trailers).map(|status| status.code()),
            Some(Code::Unavailable)
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_finished_call_leaves_nothing_to_terminate() -> Result<(), Box<dyn std::error::Error>>
    {
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

        assert_eq!(calls.terminate(), 0);
        assert!(calls.registry.lock().calls.is_empty());
        Ok(())
    }
}
