//! Per-connection transport that bounds stalled writes and owns the
//! connection's concurrency slot.

use std::{
    future::Future,
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use hyper::rt::{Read, ReadBufCursor, Write};
use tokio::{sync::OwnedSemaphorePermit, time::Sleep};

/// Wraps a connection transport.
///
/// A write, flush, or shutdown that stays pending for longer than
/// `write_stall_timeout` fails with [`io::ErrorKind::TimedOut`], so a client
/// that stops reading cannot hold the connection. The transport also owns the
/// connection's concurrency slot. hyper moves the transport into the upgraded
/// connection, so the slot is released only once the socket is dropped, which
/// covers `WebSocket` sessions.
pub(super) struct ConnectionIo<T> {
    inner: T,
    write_stall_timeout: Duration,
    stall: Option<Pin<Box<Sleep>>>,
    _slot: OwnedSemaphorePermit,
}

impl<T> ConnectionIo<T> {
    pub(super) fn new(inner: T, write_stall_timeout: Duration, slot: OwnedSemaphorePermit) -> Self {
        Self {
            inner,
            write_stall_timeout,
            stall: None,
            _slot: slot,
        }
    }

    /// Arms the stall timer on the first pending poll of a write and clears it
    /// as soon as the write makes progress.
    fn bound_write<R>(
        &mut self,
        cx: &mut Context<'_>,
        poll: Poll<io::Result<R>>,
    ) -> Poll<io::Result<R>> {
        if poll.is_ready() {
            self.stall = None;
            return poll;
        }
        let timeout = self.write_stall_timeout;
        let stall = self
            .stall
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
        if stall.as_mut().poll(cx).is_ready() {
            self.stall = None;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "client did not read the response in time",
            )));
        }
        Poll::Pending
    }
}

impl<T: Read + Unpin> Read for ConnectionIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<T: Write + Unpin> Write for ConnectionIo<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.bound_write(cx, poll)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_flush(cx);
        this.bound_write(cx, poll)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.bound_write(cx, poll)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.bound_write(cx, poll)
    }
}
