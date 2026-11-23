//! Ending a response only once the client's window covers the data before it.
//!
//! hyper finishes an HTTP/2 response's task as soon as it queues the
//! response's end, and h2 then counts the stream as closed. If DATA queued
//! before that end still waits for the client's flow-control window, nothing
//! can reset the stream any more: it keeps that data and its place among the
//! connection's streams until the client grants window or the connection
//! closes. The gate keeps that state from arising while the client's window
//! settings stay as they were. A client that lowers its stream window with
//! `SETTINGS` after the end can still leave data queued, which only an h2
//! change can bound.
//!
//! hyper hands a data chunk it holds to h2 only once the stream has send
//! capacity left after the data already queued, and asks the body for its
//! next frame only after that. Before the gate ends a response, and whenever
//! the response pauses because its producer has nothing ready, it hands hyper
//! the data's last byte on its own, as a probe. hyper asks for the next frame
//! only once the window covers everything queued before the probe, and only
//! then does the gate release the end or poll the producer again. Until it
//! releases the end, the gate reports that it has not ended, because hyper
//! sends `END_STREAM` with the last data frame of a body that has.
//!
//! A producer that yields to the scheduler, as `tokio_stream::iter` does every
//! 32 items, is ready again as soon as its task runs. The gate therefore waits
//! one turn of its task before it treats a pending producer as paused, so such
//! a yield sends no probe and leaves the response's frames as they were. The
//! frame it holds for that turn goes out whole if more data follows, and is
//! never merged with later data.
//!
//! The probe depends on hyper waiting for capacity before it sends a chunk it
//! holds (`proto::h2::PipeToSendStream` in hyper 1.12). The listener's tests
//! fail if an upgrade changes that.
//!
//! The gate records each piece the transport takes and each time the
//! transport asks for the next. The task that carries the response resets
//! the transport once it has held one piece for the send stall timeout
//! ([`super::stream_reset`]), which ends the HTTP/2 stream, or closes the
//! HTTP/1.1 connection, and drops the response. HTTP/1.1 has no flow-control
//! window, so the gate sends no probe there.

use std::{
    mem,
    pin::Pin,
    task::{Context, Poll, ready},
};

use bytes::Bytes;
use http_body::{Body as _, Frame, SizeHint};
use tonic::{Status, body::Body};

use super::{RELEASE_PROBES_METRIC, stream_reset::TransportReset};

/// How a listener gates the responses it sends.
#[derive(Clone, Copy, Debug)]
pub(super) struct ReleaseGate {
    /// Whether the response travels on an HTTP/2 stream, whose end the gate
    /// releases only after a probe.
    pub(super) probe_window: bool,
}

impl ReleaseGate {
    /// Gates `response` on the transport whose task is polling the caller.
    ///
    /// Outside a public listener's transport task there is nothing to reset,
    /// and the response is returned unchanged.
    pub(super) fn apply(self, response: http::Response<Body>) -> http::Response<Body> {
        let Some(transport) = TransportReset::current() else {
            return response;
        };
        response.map(|inner| Body::new(ReleaseGatedBody::new(inner, self, transport)))
    }
}

/// A response body whose end waits for the client's window.
struct ReleaseGatedBody {
    inner: Body,
    /// A data frame taken from `inner` that the transport has not taken.
    ahead: Option<Bytes>,
    /// The last byte of the data handed out, held back to probe the window.
    probe: Option<Bytes>,
    /// What follows the data the gate handed out last.
    next: Next,
    /// Whether the gate already waited one turn for the producer after the
    /// frame it holds in `ahead`.
    waited_for_producer: bool,
    gate: ReleaseGate,
    transport: TransportReset,
}

enum Next {
    /// The gate polls `inner` when it needs a frame.
    Unknown,
    /// `inner` had nothing ready, so the gate polls it again after the probe.
    Paused,
    /// `inner` ended with these trailers, or with none.
    End(Option<http::HeaderMap>),
    /// `inner` failed after the data the gate handed out last.
    Failed(Status),
    /// The gate released the end.
    Released,
}

/// What `inner` produced when the gate polled it.
enum Polled {
    Data(Bytes),
    End(Option<http::HeaderMap>),
    Failed(Status),
}

impl ReleaseGatedBody {
    fn new(inner: Body, gate: ReleaseGate, transport: TransportReset) -> Self {
        Self {
            inner,
            ahead: None,
            probe: None,
            next: Next::Unknown,
            waited_for_producer: false,
            gate,
            transport,
        }
    }

    /// The next non-empty data chunk, or the end, of `inner`.
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Polled> {
        loop {
            let polled = match ready!(Pin::new(&mut self.inner).poll_frame(cx)) {
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(chunk) if chunk.is_empty() => continue,
                    Ok(chunk) => Polled::Data(chunk),
                    Err(frame) => Polled::End(frame.into_trailers().ok()),
                },
                Some(Err(status)) => Polled::Failed(status),
                None => Polled::End(None),
            };
            return Poll::Ready(polled);
        }
    }

    /// Hands `chunk` to the transport, which times how long it holds it.
    fn hand_out(&mut self, chunk: Bytes) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        self.waited_for_producer = false;
        self.transport.took_piece();
        Poll::Ready(Some(Ok(Frame::data(chunk))))
    }

    /// Splits off the last byte of `chunk` as the probe and returns the rest.
    /// A single byte is its own probe.
    fn hold_back_probe(&mut self, mut chunk: Bytes) -> Bytes {
        metrics::counter!(RELEASE_PROBES_METRIC).increment(1);
        if let Some(last) = chunk.len().checked_sub(1).filter(|last| *last > 0) {
            // A copy, so the probe pins no allocation of the bytes before it.
            self.probe = Some(Bytes::copy_from_slice(
                chunk.get(last..).unwrap_or_default(),
            ));
            chunk.truncate(last);
        }
        chunk
    }

    fn poll_window_gated(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if let Some(probe) = self.probe.take() {
            return self.hand_out(probe);
        }
        match mem::replace(&mut self.next, Next::Unknown) {
            Next::End(trailers) => return self.release(trailers),
            Next::Released => return self.release(None),
            Next::Failed(status) => return Poll::Ready(Some(Err(status))),
            Next::Unknown | Next::Paused => {}
        }
        let current = match self.ahead.take() {
            Some(current) => current,
            None => match ready!(self.poll_inner(cx)) {
                Polled::Data(chunk) => chunk,
                // Nothing the gate handed out is waiting: either it sent no
                // data, or its last data was a probe the transport accepted.
                Polled::End(trailers) => return self.release(trailers),
                Polled::Failed(status) => return Poll::Ready(Some(Err(status))),
            },
        };
        // Look at what follows `current` before handing it out.
        match self.poll_inner(cx) {
            Poll::Ready(Polled::Data(next)) => {
                self.ahead = Some(next);
                return self.hand_out(current);
            }
            Poll::Ready(Polled::End(trailers)) => self.next = Next::End(trailers),
            // The data still goes first, as it would without the gate. hyper
            // resets the stream at once on the error, which discards what is
            // queued, so nothing waits for window and no probe is needed.
            Poll::Ready(Polled::Failed(status)) => {
                self.next = Next::Failed(status);
                return self.hand_out(current);
            }
            // A producer that only yielded is ready on the next turn.
            Poll::Pending if !self.waited_for_producer => {
                self.waited_for_producer = true;
                self.ahead = Some(current);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Pending => self.next = Next::Paused,
        }
        let rest = self.hold_back_probe(current);
        self.hand_out(rest)
    }

    fn release(
        &mut self,
        trailers: Option<http::HeaderMap>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        self.next = Next::Released;
        Poll::Ready(trailers.map(|trailers| Ok(Frame::trailers(trailers))))
    }

    fn poll_passed_through(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let frame = ready!(Pin::new(&mut self.inner).poll_frame(cx));
        if let Some(Ok(frame)) = &frame
            && frame.data_ref().is_some_and(|chunk| !chunk.is_empty())
        {
            self.transport.took_piece();
        }
        Poll::Ready(frame)
    }
}

impl http_body::Body for ReleaseGatedBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        this.transport.asked_for_next();
        if this.gate.probe_window {
            this.poll_window_gated(cx)
        } else {
            this.poll_passed_through(cx)
        }
    }

    fn is_end_stream(&self) -> bool {
        if !self.gate.probe_window {
            return self.inner.is_end_stream();
        }
        match self.next {
            Next::Released => true,
            Next::End(_) | Next::Paused | Next::Failed(_) => false,
            Next::Unknown => {
                self.probe.is_none() && self.ahead.is_none() && self.inner.is_end_stream()
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self.next {
            Next::Unknown if self.probe.is_none() && self.ahead.is_none() => self.inner.size_hint(),
            Next::Unknown | Next::Paused | Next::End(_) | Next::Failed(_) | Next::Released => {
                SizeHint::default()
            }
        }
    }
}

impl Drop for ReleaseGatedBody {
    /// hyper drops an HTTP/1.1 body as soon as it reports its end, without
    /// asking for another frame, and the connection then carries the next
    /// response. Its last piece no longer belongs to this response.
    fn drop(&mut self) {
        self.transport.asked_for_next();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use http_body_util::{BodyExt as _, StreamBody};
    use tonic::Code;

    use super::*;
    use crate::public_grpc_server::stream_reset::resettable;

    #[tokio::test]
    async fn data_produced_before_an_error_goes_out_before_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let inner = Body::new(StreamBody::new(tokio_stream::iter([
            Ok(Frame::data(Bytes::from_static(b"message"))),
            Err(Status::internal("the producer failed")),
        ])));
        let frames = resettable(
            async move {
                let mut body = ReleaseGate { probe_window: true }
                    .apply(http::Response::new(inner))
                    .into_body();
                let first = body.frame().await;
                let second = body.frame().await;
                (first, second)
            },
            Duration::from_secs(60),
        )
        .await
        .ok_or("the transport was reset")?;
        let data = frames
            .0
            .ok_or("the body ended at once")??
            .into_data()
            .map_err(|_| "the first frame carried no data")?;
        assert_eq!(data, Bytes::from_static(b"message"));
        let error = frames.1.ok_or("the body ended without its error")?.err();
        assert_eq!(error.map(|status| status.code()), Some(Code::Internal));
        Ok(())
    }
}
