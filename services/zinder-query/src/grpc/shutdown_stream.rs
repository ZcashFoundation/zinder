//! Ends open server streams when the runtime shuts down.
//!
//! Graceful gRPC shutdown waits for every open call to finish, and event
//! streams never finish on their own. Each server stream therefore ends with
//! the typed `shutting_down` status once shutdown starts, which clients treat
//! like any other readiness refusal and retry elsewhere.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio_stream::Stream;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tonic::Status;
use zinder_runtime::{ReadinessCause, not_ready_status};

type ServerStream<Message> = Pin<Box<dyn Stream<Item = Result<Message, Status>> + Send>>;

/// Yields `stream` until `shutdown` is cancelled, then one `shutting_down`
/// status, then ends.
pub(super) fn end_on_shutdown<Message: Send + 'static>(
    stream: ServerStream<Message>,
    shutdown: Option<&CancellationToken>,
) -> ServerStream<Message> {
    match shutdown {
        Some(shutdown) => Box::pin(EndOnShutdown {
            stream: Some(stream),
            shutdown: Box::pin(shutdown.clone().cancelled_owned()),
        }),
        None => stream,
    }
}

struct EndOnShutdown<Message> {
    /// `None` once the stream has ended, by itself or through shutdown.
    stream: Option<ServerStream<Message>>,
    shutdown: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl<Message> Stream for EndOnShutdown<Message> {
    type Item = Result<Message, Status>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(stream) = this.stream.as_mut() else {
            return Poll::Ready(None);
        };
        if this.shutdown.as_mut().poll(context).is_ready() {
            this.stream = None;
            return Poll::Ready(Some(Err(not_ready_status(&ReadinessCause::ShuttingDown))));
        }
        let next = stream.as_mut().poll_next(context);
        if matches!(next, Poll::Ready(None)) {
            this.stream = None;
        }
        next
    }
}

#[cfg(test)]
mod tests {
    use tokio_stream::{self as stream, StreamExt as _};
    use tokio_util::sync::CancellationToken;
    use tonic::{Code, Status};

    use super::{ServerStream, end_on_shutdown};

    #[tokio::test]
    async fn shutdown_ends_an_open_stream_with_a_shutting_down_status() -> Result<(), Status> {
        let shutdown = CancellationToken::new();
        let open: ServerStream<u32> = Box::pin(stream::once(Ok(1)).chain(stream::pending()));
        let mut guarded = end_on_shutdown(open, Some(&shutdown));

        assert_eq!(guarded.next().await.transpose()?, Some(1));
        shutdown.cancel();
        let status = guarded
            .next()
            .await
            .and_then(Result::err)
            .ok_or_else(|| Status::internal("shutdown did not end the stream with a status"))?;
        assert_eq!(status.code(), Code::Unavailable);
        assert!(status.message().contains("shutting_down"));
        assert!(guarded.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn a_stream_that_ends_before_shutdown_ends_without_a_status() -> Result<(), Status> {
        let shutdown = CancellationToken::new();
        let finite: ServerStream<u32> = Box::pin(stream::iter([Ok(1), Ok(2)]));
        let guarded = end_on_shutdown(finite, Some(&shutdown));

        let delivered = guarded.collect::<Result<Vec<_>, _>>().await?;
        assert_eq!(delivered, vec![1, 2]);
        Ok(())
    }
}
