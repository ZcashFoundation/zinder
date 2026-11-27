use super::{CompactBlockServingError, close_unpublished_generation, tear_down_on_blocking_thread};
use std::sync::Arc;
use tokio::sync::{Notify, mpsc};

#[tokio::test]
async fn a_slow_candidate_close_leaves_the_async_worker_free()
-> Result<(), Box<dyn std::error::Error>> {
    struct SlowClose {
        started: Arc<Notify>,
        release: mpsc::Receiver<()>,
    }

    impl Drop for SlowClose {
        fn drop(&mut self) {
            self.started.notify_one();
            let _ = self.release.blocking_recv();
        }
    }

    let started = Arc::new(Notify::new());
    let (release, receiver) = mpsc::channel(1);
    let closing = tokio::spawn(close_unpublished_generation(Some(SlowClose {
        started: Arc::clone(&started),
        release: receiver,
    })));
    started.notified().await;
    assert!(
        !closing.is_finished(),
        "cleanup must wait for the close to finish"
    );
    release.send(()).await?;
    closing.await?;
    Ok(())
}

/// Stands in for a publisher whose secondary panics while closing.
struct PanicsWhenDropped;

impl Drop for PanicsWhenDropped {
    #[allow(clippy::panic, reason = "the test needs a teardown that panics")]
    fn drop(&mut self) {
        panic!("closing a secondary panicked");
    }
}

#[tokio::test]
async fn a_teardown_that_panics_is_an_error() {
    let teardown = tear_down_on_blocking_thread(PanicsWhenDropped).await;

    assert!(
        matches!(teardown, Err(CompactBlockServingError::Teardown(_))),
        "a panicking teardown must fail the publisher task, got {teardown:?}"
    );
}

#[tokio::test]
async fn a_clean_teardown_succeeds() -> Result<(), CompactBlockServingError> {
    tear_down_on_blocking_thread(()).await
}
