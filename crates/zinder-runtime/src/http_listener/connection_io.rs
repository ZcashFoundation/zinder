//! Per-connection transport that bounds stalled writes and owns the
//! connection's concurrency slot.
//!
//! The HTTP listener and the public gRPC listeners both serve through it.

use std::{
    future::Future,
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use hyper::rt::{Read, ReadBufCursor, Write};
use tokio::{sync::OwnedSemaphorePermit, time::Sleep};

/// Bytes a connection must write within its write stall timeout once a write
/// has blocked.
pub(crate) const MIN_WRITE_PROGRESS_BYTES: usize = 16 * 1024;

/// Wraps a connection transport.
///
/// Once a write blocks, the connection must write
/// [`MIN_WRITE_PROGRESS_BYTES`], or finish writing everything its writer had
/// buffered, within `write_stall_timeout`. Otherwise its next pending write,
/// flush, or shutdown fails with [`io::ErrorKind::TimedOut`]. Smaller progress
/// does not restart the clock, so a client that stops reading, or reads only
/// a trickle, cannot hold the connection. The transport also owns the
/// connection's concurrency slot, when its listener has one. hyper moves the
/// transport into the upgraded connection, so the slot is released only once
/// the socket is dropped, which covers `WebSocket` sessions.
pub(crate) struct ConnectionIo<T> {
    inner: T,
    write_stall_timeout: Duration,
    stall: Option<WriteStall>,
    _slot: Option<OwnedSemaphorePermit>,
}

/// A blocked write's deadline, and what the connection has written since.
struct WriteStall {
    deadline: Pin<Box<Sleep>>,
    written: usize,
}

impl<T> ConnectionIo<T> {
    pub(crate) fn new(
        inner: T,
        write_stall_timeout: Duration,
        slot: Option<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            inner,
            write_stall_timeout,
            stall: None,
            _slot: slot,
        }
    }

    /// Counts `written` bytes towards the progress a blocked write needs.
    fn record_written(&mut self, written: usize) {
        if let Some(stall) = &mut self.stall {
            stall.written = stall.written.saturating_add(written);
            if stall.written >= MIN_WRITE_PROGRESS_BYTES {
                self.stall = None;
            }
        }
    }

    /// Arms the deadline on the first pending write after the connection last
    /// made enough progress, and fails the write once it passes.
    fn bound_pending<R>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<R>> {
        let timeout = self.write_stall_timeout;
        let stall = self.stall.get_or_insert_with(|| WriteStall {
            deadline: Box::pin(tokio::time::sleep(timeout)),
            written: 0,
        });
        if stall.deadline.as_mut().poll(cx).is_ready() {
            self.stall = None;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "client did not read the response in time",
            )));
        }
        Poll::Pending
    }

    fn bound_write(
        &mut self,
        cx: &mut Context<'_>,
        poll: Poll<io::Result<usize>>,
    ) -> Poll<io::Result<usize>> {
        match poll {
            Poll::Ready(Ok(written)) => {
                self.record_written(written);
                Poll::Ready(Ok(written))
            }
            Poll::Ready(Err(error)) => {
                self.stall = None;
                Poll::Ready(Err(error))
            }
            Poll::Pending => self.bound_pending(cx),
        }
    }

    /// A completed flush or shutdown means the writer has written everything
    /// it buffered, so nothing is blocked any more.
    fn bound_drain(
        &mut self,
        cx: &mut Context<'_>,
        poll: Poll<io::Result<()>>,
    ) -> Poll<io::Result<()>> {
        if poll.is_ready() {
            self.stall = None;
            return poll;
        }
        self.bound_pending(cx)
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
        this.bound_drain(cx, poll)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.bound_drain(cx, poll)
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

#[cfg(test)]
mod tests {
    use std::future::poll_fn;

    use eyre::{Result, ensure};
    use hyper_util::rt::TokioIo;
    use tokio::{
        io::{AsyncReadExt as _, DuplexStream},
        task::JoinHandle,
        time::Instant,
    };

    use super::*;

    const WRITE_STALL: Duration = Duration::from_secs(1);
    /// Bytes the in-memory socket buffers between the two ends.
    const SOCKET_BUFFER: usize = 4096;

    /// A client that reads `chunk` bytes every 100 ms until the server's end
    /// closes.
    fn spawn_reader(mut client: DuplexStream, chunk: usize) -> JoinHandle<io::Result<()>> {
        tokio::spawn(async move {
            let mut buffer = vec![0; chunk];
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if client.read(&mut buffer).await? == 0 {
                    return Ok(());
                }
            }
        })
    }

    /// Writes `total` bytes, a kilobyte at a time.
    async fn write_all(
        io: &mut ConnectionIo<TokioIo<DuplexStream>>,
        total: usize,
    ) -> io::Result<()> {
        let chunk = [0; 1024];
        let mut written = 0;
        while written < total {
            written += poll_fn(|cx| Pin::new(&mut *io).poll_write(cx, &chunk)).await?;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_connection_whose_client_reads_less_than_the_minimum_is_closed() -> Result<()> {
        let (server, client) = tokio::io::duplex(SOCKET_BUFFER);
        let mut io = ConnectionIo::new(TokioIo::new(server), WRITE_STALL, None);
        // 5 KiB a second: progress at every read, but under 16 KiB a stall.
        let reader = spawn_reader(client, 512);

        let started = Instant::now();
        let outcome = tokio::time::timeout(WRITE_STALL * 5, write_all(&mut io, usize::MAX)).await;
        let Ok(Err(error)) = outcome else {
            eyre::bail!("the trickling client kept the connection: {outcome:?}");
        };
        ensure!(error.kind() == io::ErrorKind::TimedOut, "{error}");
        ensure!(
            started.elapsed() < WRITE_STALL * 2,
            "closed after {:?}",
            started.elapsed()
        );
        drop(io);
        reader.await??;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_connection_whose_client_reads_the_minimum_stays_open() -> Result<()> {
        let (server, client) = tokio::io::duplex(SOCKET_BUFFER);
        let mut io = ConnectionIo::new(TokioIo::new(server), WRITE_STALL, None);
        // 20 KiB a second: slow, but over 16 KiB a stall.
        let reader = spawn_reader(client, 2048);

        write_all(&mut io, 128 * 1024).await?;
        drop(io);
        reader.await??;
        Ok(())
    }
}
