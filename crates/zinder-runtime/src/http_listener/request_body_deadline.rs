//! Request body wrapper that enforces one absolute read deadline.

use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use hyper::body::{Body, Frame, Incoming, SizeHint};
use tokio::time::Sleep;

/// Failure to read a request body.
#[derive(Debug, thiserror::Error)]
pub(super) enum RequestBodyError {
    /// The body was not fully received before its deadline.
    #[error("request body was not received before its deadline")]
    DeadlineExceeded,
    /// The underlying connection failed while reading the body.
    #[error("request body read failed: {0}")]
    Read(#[from] hyper::Error),
}

/// A request body that fails once a single deadline, armed when the request
/// head arrives, elapses.
///
/// Unlike a per-frame idle timeout, the deadline is never extended by
/// progress, so a client that drips one byte at a time cannot hold the request
/// beyond it.
pub(super) struct RequestBodyDeadline {
    inner: Incoming,
    deadline: Pin<Box<Sleep>>,
}

impl RequestBodyDeadline {
    pub(super) fn new(inner: Incoming, deadline: Duration) -> Self {
        Self {
            inner,
            deadline: Box::pin(tokio::time::sleep(deadline)),
        }
    }
}

impl Body for RequestBodyDeadline {
    type Data = <Incoming as Body>::Data;
    type Error = RequestBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if let Poll::Ready(frame) = Pin::new(&mut this.inner).poll_frame(cx) {
            return Poll::Ready(frame.map(|frame| frame.map_err(RequestBodyError::from)));
        }
        if this.deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(Err(RequestBodyError::DeadlineExceeded)));
        }
        Poll::Pending
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
