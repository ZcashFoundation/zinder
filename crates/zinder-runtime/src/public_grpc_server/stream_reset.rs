//! Ending the HTTP/2 stream, or the HTTP/1.1 connection, that carries one
//! response.
//!
//! hyper sends each HTTP/2 response from a task of its own. While that task
//! waits for the client's flow-control window, it polls neither the response
//! body nor anything a service can reach, and hyper offers no way to reset the
//! stream. The public listener therefore runs each stream task inside
//! [`resettable`], which ends the task when the stream's [`TransportReset`]
//! fires. Ending it drops the stream's last handles, so h2 discards the data
//! still queued for the stream, sends `RST_STREAM` with `CANCEL`, and frees
//! the stream's concurrent-stream slot. The connection and its other streams
//! stay open.
//!
//! An HTTP/1.1 connection carries one response at a time. Its whole
//! connection task runs inside [`resettable`], and a reset closes it.
//!
//! The reset is a task-local value. hyper polls the service future and the
//! response body inside the stream's own task, or inside the connection task
//! for HTTP/1.1, so [`TransportReset::current`] called from either one returns
//! the reset of the transport that carries that response.
//!
//! The same task bounds how long the transport may hold a piece of the
//! response. The response records each piece the transport takes and each
//! time the transport asks for the next. Whenever the task waits while the
//! transport holds a piece, it times that piece, and once the transport has
//! held it for the send stall timeout, the task resets itself. A transport
//! that asks for the next piece before the task waits arms no timer.

use std::{
    future::{Future, poll_fn},
    pin::{Pin, pin},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use futures_util::task::AtomicWaker;
use tokio::{
    sync::Notify,
    time::{Instant, Sleep},
};

use super::SEND_STALLS_METRIC;

tokio::task_local! {
    static CURRENT_TRANSPORT_RESET: TransportReset;
}

/// Ends the HTTP/2 stream or HTTP/1.1 connection that carries a response.
#[derive(Clone, Debug, Default)]
pub(super) struct TransportReset(Arc<Transport>);

#[derive(Debug, Default)]
struct Transport {
    reset: AtomicBool,
    /// Wakes the single transport task executing [`resettable`].
    task_waker: AtomicWaker,
    /// Wakes external observers waiting on [`TransportReset::wait_for_reset`],
    /// used only by client-streaming receive bodies.
    waiters: Notify,
    /// Counts each piece the transport takes and each time it asks for the
    /// next, so the count is odd while the transport holds a piece.
    handoffs: AtomicU64,
}

impl TransportReset {
    /// The reset of the transport whose task is polling the caller, or
    /// `None` outside a public listener's transport task.
    pub(super) fn current() -> Option<Self> {
        CURRENT_TRANSPORT_RESET.try_with(Self::clone).ok()
    }

    /// Ends the transport. Its task drops everything it holds the next time
    /// it runs.
    pub(super) fn reset(&self) {
        if !self.0.reset.swap(true, Ordering::AcqRel) {
            self.0.task_waker.wake();
            self.0.waiters.notify_waiters();
        }
    }

    /// Completes once the transport is reset.
    pub(super) fn wait_for_reset(&self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let transport = Arc::clone(&self.0);
        Box::pin(async move {
            let notified = transport.waiters.notified();
            let mut notified = pin!(notified);
            // Register before re-checking `reset` so a concurrent reset can
            // never slip between the check and parking.
            notified.as_mut().enable();
            if transport.reset.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        })
    }

    /// Records that the transport took a piece of the response.
    pub(super) fn took_piece(&self) {
        self.count_handoff(1);
    }

    /// Records that the transport asked for the response's next piece.
    pub(super) fn asked_for_next(&self) {
        self.count_handoff(0);
    }

    /// Advances the hand-off count to the next value of `parity`, if it is
    /// not there already. Only the transport's own task writes the count.
    fn count_handoff(&self, parity: u64) {
        let count = self.0.handoffs.load(Ordering::Relaxed);
        if count % 2 != parity {
            self.0
                .handoffs
                .store(count.wrapping_add(1), Ordering::Relaxed);
        }
    }

    /// The hand-off count, while the transport holds a piece.
    fn held_piece(&self) -> Option<u64> {
        let count = self.0.handoffs.load(Ordering::Relaxed);
        (count % 2 == 1).then_some(count)
    }

    #[cfg(test)]
    pub(super) fn is_reset(&self) -> bool {
        self.0.reset.load(Ordering::Acquire)
    }
}

/// Times the piece the transport holds while its task waits.
///
/// hyper parks a stream whenever its send buffer is full, so a busy stream
/// waits holding a piece many times a second. The watch therefore keeps one
/// timer, which may fire before the current piece is due but never after, and
/// moves it only when it fires.
struct StallWatch {
    timeout: Duration,
    /// The hand-off count of the piece being timed, and when the task first
    /// waited while the transport held it.
    timed: Option<(u64, Instant)>,
    timer: Option<Pin<Box<Sleep>>>,
}

impl StallWatch {
    fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            timed: None,
            timer: None,
        }
    }

    /// Whether the transport has held its current piece for the timeout.
    fn poll_stalled(&mut self, transport: &TransportReset, cx: &mut Context<'_>) -> bool {
        let Some(held) = transport.held_piece() else {
            return false;
        };
        let since = match self.timed {
            Some((piece, since)) if piece == held => since,
            Some(_) | None => {
                let now = Instant::now();
                self.timed = Some((held, now));
                now
            }
        };
        let due = since + self.timeout;
        let timer = self
            .timer
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(due)));
        while timer.as_mut().poll(cx).is_ready() {
            if timer.deadline() >= due {
                return true;
            }
            timer.as_mut().reset(due);
        }
        false
    }
}

/// Runs `task` as the transport whose reset [`TransportReset::current`]
/// returns, and ends it, dropping `task`, once that reset fires or the
/// transport holds one piece of a response for `send_stall_timeout`.
pub(super) async fn resettable<F: Future>(
    task: F,
    send_stall_timeout: Duration,
) -> Option<F::Output> {
    let transport = TransportReset::default();
    CURRENT_TRANSPORT_RESET
        .scope(transport.clone(), async move {
            let mut task = pin!(task);
            let mut stalls = StallWatch::new(send_stall_timeout);
            poll_fn(|cx| {
                if transport.0.reset.load(Ordering::Acquire) {
                    return Poll::Ready(None);
                }
                if let Poll::Ready(output) = task.as_mut().poll(cx) {
                    return Poll::Ready(Some(output));
                }
                transport.0.task_waker.register(cx.waker());
                if transport.0.reset.load(Ordering::Acquire) {
                    return Poll::Ready(None);
                }
                if stalls.poll_stalled(&transport, cx) {
                    metrics::counter!(SEND_STALLS_METRIC).increment(1);
                    transport.reset();
                    return Poll::Ready(None);
                }
                Poll::Pending
            })
            .await
        })
        .await
}

/// The executor of a public listener's connections, which spawns each HTTP/2
/// stream task so that its response can reset it.
#[derive(Clone, Copy, Debug)]
pub(super) struct ResettableStreams {
    send_stall_timeout: Duration,
}

impl ResettableStreams {
    /// Spawns stream tasks that reset themselves once their transport holds
    /// a piece of their response for `send_stall_timeout`.
    pub(super) fn new(send_stall_timeout: Duration) -> Self {
        Self { send_stall_timeout }
    }
}

impl<F> hyper::rt::Executor<F> for ResettableStreams
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, stream_task: F) {
        tokio::spawn(resettable(stream_task, self.send_stall_timeout));
    }
}

#[cfg(test)]
mod tests {
    use std::{future, time::Duration};

    use tokio::sync::oneshot;

    use super::{TransportReset, resettable};

    const STALL: Duration = Duration::from_secs(1);

    /// Reports when it is dropped.
    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(dropped) = self.0.take() {
                let _ = dropped.send(());
            }
        }
    }

    #[tokio::test]
    async fn a_reset_ends_the_task_and_drops_what_it_holds()
    -> Result<(), Box<dyn std::error::Error>> {
        let (dropped_sender, dropped) = oneshot::channel();
        let (reset_sender, reset_receiver) = oneshot::channel();
        let task = tokio::spawn(resettable(
            async move {
                let held = DropSignal(Some(dropped_sender));
                let _ = reset_sender.send(TransportReset::current());
                future::pending::<()>().await;
                drop(held);
            },
            STALL,
        ));
        let reset = reset_receiver
            .await?
            .ok_or("the task saw no transport reset")?;

        reset.reset();
        let ended = tokio::time::timeout(Duration::from_secs(5), task).await??;
        assert_eq!(ended, None, "a reset task reports no output");
        tokio::time::timeout(Duration::from_secs(5), dropped).await??;
        Ok(())
    }

    #[tokio::test]
    async fn each_task_sees_its_own_reset_and_outside_code_sees_none()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(TransportReset::current().is_none());
        let first = resettable(async { TransportReset::current() }, STALL)
            .await
            .flatten()
            .ok_or("the first task saw no reset")?;
        let second = resettable(async { TransportReset::current() }, STALL)
            .await
            .flatten()
            .ok_or("the second task saw no reset")?;

        first.reset();
        assert!(first.is_reset());
        assert!(!second.is_reset(), "resetting one task reset another");
        Ok(())
    }

    /// Runs a task that hands the transport a piece, has the transport ask
    /// for the next one if `asked_again`, then waits forever.
    async fn hold_a_piece(asked_again: bool) -> Option<()> {
        resettable(
            async move {
                let transport = TransportReset::current();
                if let Some(transport) = &transport {
                    transport.asked_for_next();
                    transport.took_piece();
                    if asked_again {
                        transport.asked_for_next();
                    }
                }
                future::pending::<()>().await;
            },
            STALL,
        )
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn a_task_whose_transport_holds_a_piece_for_the_timeout_ends() {
        let started = tokio::time::Instant::now();
        assert_eq!(hold_a_piece(false).await, None);
        assert_eq!(started.elapsed(), STALL);
    }

    #[tokio::test(start_paused = true)]
    async fn a_task_whose_transport_asked_for_the_next_piece_keeps_running() {
        let outcome = tokio::time::timeout(STALL * 10, hold_a_piece(true)).await;
        assert!(outcome.is_err(), "the task ended: {outcome:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_piece_taken_after_the_timer_started_gets_its_whole_timeout() {
        let started = tokio::time::Instant::now();
        let outcome = resettable(
            async {
                let Some(transport) = TransportReset::current() else {
                    return;
                };
                transport.took_piece();
                tokio::time::sleep(STALL / 2).await;
                transport.asked_for_next();
                transport.took_piece();
                future::pending::<()>().await;
            },
            STALL,
        )
        .await;
        assert_eq!(outcome, None);
        assert_eq!(started.elapsed(), STALL / 2 + STALL);
    }
}
