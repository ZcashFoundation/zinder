//! Tests for the bounded tip wait, against fake reads and paused time.

use std::{
    future::Future,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use eyre::{Result, eyre};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::{sync::watch, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};
use zinder_core::{
    ArtifactSchemaVersion, BlockHash, BlockHeight, ChainEpoch, ChainEpochId, ChainTipMetadata,
    CompactBlockArtifact, Network, NetworkUpgradeActivations, UnixTimestampMillis,
};
use zinder_proto::compat::lightwalletd;

use super::{TipWait, TipWaitProgress};
use crate::{
    block_methods::{
        BlockMethods, ChunkRead, ChunkReadFailure, ChunkReadOutcome, CompactBlockReads,
        RequestedBlock, compact_block,
    },
    compact_block_messages::CompactBlockShape,
};

const METHOD: &str = "GetBlock";
const CAP: Duration = Duration::from_secs(3);

/// A captured view that counts itself while it is alive.
struct CountedView {
    epoch: ChainEpoch,
    live: Arc<AtomicUsize>,
}

impl Drop for CountedView {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Reads whose visible tip a test sets and publishes.
#[derive(Clone)]
struct FakeReads {
    tip: Arc<AtomicU32>,
    publisher: Arc<watch::Sender<()>>,
    subscriptions: Arc<AtomicUsize>,
    live_views: Arc<AtomicUsize>,
    fail_captures: Arc<AtomicBool>,
}

impl FakeReads {
    fn at_tip(tip: u32) -> Self {
        Self {
            tip: Arc::new(AtomicU32::new(tip)),
            publisher: Arc::new(watch::channel(()).0),
            subscriptions: Arc::new(AtomicUsize::new(0)),
            live_views: Arc::new(AtomicUsize::new(0)),
            fail_captures: Arc::new(AtomicBool::new(false)),
        }
    }

    fn subscriptions(&self) -> usize {
        self.subscriptions.load(Ordering::SeqCst)
    }

    /// Moves the tip and publishes the new view.
    fn publish_tip(&self, tip: u32) {
        self.tip.store(tip, Ordering::SeqCst);
        self.publisher.send_replace(());
    }

    /// Publishes a view without moving the tip.
    fn publish_same_tip(&self) {
        self.publisher.send_replace(());
    }

    fn live_views(&self) -> usize {
        self.live_views.load(Ordering::SeqCst)
    }
}

impl CompactBlockReads for FakeReads {
    type View = CountedView;

    fn capture_view(&self) -> impl Future<Output = Result<CountedView, Status>> + Send {
        let outcome = if self.fail_captures.load(Ordering::SeqCst) {
            Err(Status::unavailable("the test fails captures"))
        } else {
            self.live_views.fetch_add(1, Ordering::SeqCst);
            let tip = BlockHeight::new(self.tip.load(Ordering::SeqCst));
            Ok(CountedView {
                epoch: ChainEpoch {
                    id: ChainEpochId::new(1),
                    network: Network::ZcashRegtest,
                    visible_tip_height: tip,
                    visible_tip_hash: BlockHash::from_bytes([1; 32]),
                    settled_tip_height: tip,
                    settled_tip_hash: BlockHash::from_bytes([1; 32]),
                    artifact_schema_version: ArtifactSchemaVersion::new(1),
                    tip_metadata: ChainTipMetadata::new(0, 0, 0),
                    created_at: UnixTimestampMillis::new(0),
                },
                live: Arc::clone(&self.live_views),
            })
        };
        std::future::ready(outcome)
    }

    fn subscribe_publications(&self) -> Option<watch::Receiver<()>> {
        self.subscriptions.fetch_add(1, Ordering::SeqCst);
        Some(self.publisher.subscribe())
    }

    fn view_chain_epoch(view: &CountedView) -> ChainEpoch {
        view.epoch
    }

    fn view_activations(
        &self,
        _view: &CountedView,
    ) -> impl Future<Output = Result<NetworkUpgradeActivations, Status>> + Send {
        std::future::ready(Ok(zinder_testkit::sample_regtest_upgrade_activations()))
    }

    fn tip_block_time(
        &self,
        _view: &CountedView,
    ) -> impl Future<Output = Result<i64, Status>> + Send {
        std::future::ready(Ok(0))
    }

    fn read_compact_block(
        &self,
        _view: &CountedView,
        _block: RequestedBlock,
    ) -> impl Future<Output = Result<CompactBlockArtifact, Status>> + Send {
        std::future::ready(Err(Status::unimplemented("the test serves no block")))
    }

    fn read_compact_block_chunk<Output, Convert>(
        &self,
        _read: ChunkRead<CountedView>,
        _convert: Convert,
    ) -> impl Future<Output = ChunkReadOutcome<Output>> + Send
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, Vec<CompactBlockArtifact>) -> Output + Send + 'static,
    {
        std::future::ready(ChunkReadOutcome::unread(ChunkReadFailure::Failed(
            Status::unimplemented("the test serves no block range"),
        )))
    }
}

/// Reads with no publication channel, like a mode that publishes nothing.
#[derive(Clone)]
struct SilentReads(FakeReads);

impl CompactBlockReads for SilentReads {
    type View = CountedView;

    fn capture_view(&self) -> impl Future<Output = Result<CountedView, Status>> + Send {
        self.0.capture_view()
    }

    fn view_chain_epoch(view: &CountedView) -> ChainEpoch {
        view.epoch
    }

    fn view_activations(
        &self,
        view: &CountedView,
    ) -> impl Future<Output = Result<NetworkUpgradeActivations, Status>> + Send {
        self.0.view_activations(view)
    }

    fn tip_block_time(
        &self,
        view: &CountedView,
    ) -> impl Future<Output = Result<i64, Status>> + Send {
        self.0.tip_block_time(view)
    }

    fn read_compact_block(
        &self,
        view: &CountedView,
        block: RequestedBlock,
    ) -> impl Future<Output = Result<CompactBlockArtifact, Status>> + Send {
        self.0.read_compact_block(view, block)
    }

    fn read_compact_block_chunk<Output, Convert>(
        &self,
        read: ChunkRead<CountedView>,
        convert: Convert,
    ) -> impl Future<Output = ChunkReadOutcome<Output>> + Send
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, Vec<CompactBlockArtifact>) -> Output + Send + 'static,
    {
        self.0.read_compact_block_chunk(read, convert)
    }
}

fn nonzero(count: u32) -> Result<NonZeroU32> {
    NonZeroU32::new(count).ok_or_else(|| eyre!("count must be nonzero"))
}

fn tip_wait(max_blocks_ahead: u32, max_waiters: u32) -> Result<TipWait> {
    Ok(TipWait::new(
        CAP,
        nonzero(max_blocks_ahead)?,
        nonzero(max_waiters)?,
    ))
}

/// Starts a wait for `requested` and returns the time it ended after.
fn spawn_wait<Reads: CompactBlockReads>(
    wait: &TipWait,
    reads: &Reads,
    requested: u32,
) -> JoinHandle<Result<Duration, Status>> {
    let wait = wait.clone();
    let reads = reads.clone();
    tokio::spawn(async move {
        let started = Instant::now();
        wait.await_height(&reads, METHOD, BlockHeight::new(requested))
            .await?;
        Ok(started.elapsed())
    })
}

/// Lets every spawned task run until it parks.
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

fn height_id(height: u64) -> lightwalletd::BlockId {
    lightwalletd::BlockId {
        height,
        hash: Vec::new(),
    }
}

#[tokio::test(start_paused = true)]
async fn a_height_one_past_the_tip_is_served_when_a_publication_brings_the_tip_up() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let waiting = spawn_wait(&tip_wait(2, 8)?, &reads, 10);
    settle().await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    reads.publish_tip(10);

    let elapsed = waiting.await??;

    assert_eq!(elapsed, Duration::from_millis(700));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn an_expired_wait_returns_after_the_cap_and_the_method_answers_out_of_range() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let wait = tip_wait(2, 8)?;

    let elapsed = spawn_wait(&wait, &reads, 10).await??;
    let status = compact_block(&reads, &height_id(10), CompactBlockShape::FULL_BLOCK)
        .await
        .err()
        .ok_or_else(|| eyre!("the method must refuse a height past the tip"))?;

    assert_eq!(elapsed, CAP);
    assert_eq!(status.code(), Code::OutOfRange);
    assert_eq!(status.message(), "block 10 is newer than latest block 9");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_height_beyond_the_window_returns_at_once() -> Result<()> {
    let reads = FakeReads::at_tip(9);

    let elapsed = spawn_wait(&tip_wait(2, 8)?, &reads, 12).await??;

    assert_eq!(elapsed, Duration::ZERO);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_height_at_or_below_the_tip_returns_at_once() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let wait = tip_wait(2, 8)?;

    assert_eq!(spawn_wait(&wait, &reads, 9).await??, Duration::ZERO);
    assert_eq!(spawn_wait(&wait, &reads, 3).await??, Duration::ZERO);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_hash_only_block_id_never_waits() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let methods = BlockMethods::default().with_tip_wait(tip_wait(2, 8)?);
    let hash_only = lightwalletd::BlockId {
        height: 0,
        hash: vec![7; 32],
    };

    let started = Instant::now();
    methods.await_block_tip(&reads, METHOD, &hash_only).await?;

    assert_eq!(started.elapsed(), Duration::ZERO);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_height_block_id_waits_through_block_methods() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let methods = BlockMethods::default().with_tip_wait(tip_wait(2, 8)?);
    let waiting = {
        let reads = reads.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            methods
                .await_block_tip(&reads, METHOD, &height_id(10))
                .await?;
            Ok::<_, Status>(started.elapsed())
        })
    };
    settle().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    reads.publish_tip(10);

    assert_eq!(waiting.await??, Duration::from_secs(1));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_publication_without_a_tip_advance_keeps_the_wait_going() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let waiting = spawn_wait(&tip_wait(2, 8)?, &reads, 10);
    settle().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    reads.publish_same_tip();
    settle().await;
    assert!(
        !waiting.is_finished(),
        "a same-tip publication must not end the wait"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    reads.publish_tip(10);

    assert_eq!(waiting.await??, Duration::from_secs(2));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_tip_regression_out_of_the_window_ends_the_wait_at_once() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let waiting = spawn_wait(&tip_wait(2, 8)?, &reads, 11);
    settle().await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    reads.publish_tip(8);

    assert_eq!(waiting.await??, Duration::from_millis(400));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_full_waiter_cap_refuses_the_next_request_at_once() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let wait = tip_wait(2, 1)?;
    let parked = spawn_wait(&wait, &reads, 10);
    settle().await;

    let refused = spawn_wait(&wait, &reads, 10)
        .await?
        .err()
        .ok_or_else(|| eyre!("a full waiter cap should refuse the request"))?;
    assert_eq!(refused.code(), tonic::Code::Unavailable);
    let delay = refused
        .metadata()
        .get("grpc-retry-pushback-ms")
        .ok_or_else(|| eyre!("missing retry pushback"))?
        .to_str()?
        .parse::<i32>()?;
    assert!((500..=1500).contains(&delay));
    assert!(!parked.is_finished(), "the first waiter keeps its place");

    reads.publish_tip(10);
    parked.await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dropping_a_waiting_request_releases_its_place() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let wait = tip_wait(2, 1)?;
    let abandoned = spawn_wait(&wait, &reads, 10);
    settle().await;
    abandoned.abort();
    assert!(abandoned.await.is_err());
    settle().await;

    let replacement = spawn_wait(&wait, &reads, 10);
    settle().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !replacement.is_finished(),
        "the released place must let a new request wait"
    );
    reads.publish_tip(10);

    assert_eq!(replacement.await??, Duration::from_secs(1));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_parked_wait_holds_no_view() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let waiting = spawn_wait(&tip_wait(2, 8)?, &reads, 10);
    settle().await;

    assert_eq!(reads.live_views(), 0);
    tokio::time::sleep(Duration::from_secs(1)).await;
    reads.publish_same_tip();
    settle().await;
    assert_eq!(reads.live_views(), 0, "a wake must drop its view too");

    reads.publish_tip(10);
    waiting.await??;
    assert_eq!(reads.live_views(), 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn parked_range_waiters_do_not_take_block_range_stream_slots() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let methods = BlockMethods::default()
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN)
        .with_tip_wait(tip_wait(2, 8)?);
    let request = lightwalletd::BlockRange {
        start: Some(height_id(5)),
        end: Some(height_id(10)),
        pool_types: Vec::new(),
    };
    let waiters: Vec<_> = (0..3)
        .map(|_| {
            let methods = methods.clone();
            let reads = reads.clone();
            let request = request.clone();
            tokio::spawn(async move {
                methods
                    .await_range_tip(&reads, "GetBlockRange", &request)
                    .await
            })
        })
        .collect();
    settle().await;
    assert!(waiters.iter().all(|waiter| !waiter.is_finished()));

    let started = Instant::now();
    let slot = methods.block_range_streams().open().await?;

    assert_eq!(started.elapsed(), Duration::ZERO);
    drop(slot);
    reads.publish_tip(10);
    for waiter in waiters {
        waiter.await??;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn shutdown_ends_a_parked_wait_at_once() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let shutdown = CancellationToken::new();
    let wait = tip_wait(2, 8)?.with_shutdown(shutdown.clone());
    let waiting = spawn_wait(&wait, &reads, 10);
    settle().await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    shutdown.cancel();

    assert_eq!(waiting.await??, Duration::from_millis(250));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_disabled_wait_never_waits() -> Result<()> {
    let reads = FakeReads::at_tip(9);

    let elapsed = spawn_wait(&TipWait::disabled(), &reads, 10).await??;

    assert_eq!(elapsed, Duration::ZERO);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn reads_without_publications_never_wait() -> Result<()> {
    let reads = SilentReads(FakeReads::at_tip(9));

    let elapsed = spawn_wait(&tip_wait(2, 8)?, &reads, 10).await??;

    assert_eq!(elapsed, Duration::ZERO);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_failed_tip_capture_is_returned_to_the_caller() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    reads.fail_captures.store(true, Ordering::SeqCst);

    let status = spawn_wait(&tip_wait(2, 8)?, &reads, 10)
        .await?
        .err()
        .ok_or_else(|| eyre!("a failed capture must fail the wait"))?;

    assert_eq!(status.code(), Code::Unavailable);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_failed_capture_after_a_wake_ends_the_wait_with_that_status() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let waiting = spawn_wait(&tip_wait(2, 8)?, &reads, 10);
    settle().await;
    reads.fail_captures.store(true, Ordering::SeqCst);
    reads.publish_same_tip();

    let status = waiting
        .await?
        .err()
        .ok_or_else(|| eyre!("a failed capture must fail the wait"))?;

    assert_eq!(status.code(), Code::Unavailable);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn waits_record_outcome_duration_and_waiter_metrics() -> Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let reads = FakeReads::at_tip(9);
    let wait = tip_wait(2, 1)?;

    // served, one block ahead
    let served = spawn_wait(&wait, &reads, 10);
    settle().await;
    let gauge_while_waiting = recorder.handle().render();
    assert!(
        gauge_while_waiting.contains("zinder_compat_tip_waiters 1"),
        "one parked waiter: {gauge_while_waiting}"
    );
    // waiters_full, while the single place is taken
    assert!(spawn_wait(&wait, &reads, 10).await?.is_err());
    reads.publish_tip(10);
    served.await??;

    // expired, two blocks ahead
    spawn_wait(&wait, &reads, 12).await??;

    // cancelled, dropped mid-wait
    let abandoned = spawn_wait(&wait, &reads, 12);
    settle().await;
    abandoned.abort();
    assert!(abandoned.await.is_err());

    // left_window
    let leaving = spawn_wait(&wait, &reads, 12);
    settle().await;
    reads.publish_tip(9);
    leaving.await??;

    // refused
    let refused = spawn_wait(&wait, &reads, 10);
    settle().await;
    reads.fail_captures.store(true, Ordering::SeqCst);
    reads.publish_same_tip();
    refused.await?.err();

    let text = recorder.handle().render();
    for (outcome, blocks_ahead) in [
        ("served", "1"),
        ("waiters_full", "1"),
        ("expired", "2"),
        ("cancelled", "2"),
        ("left_window", "2"),
        ("refused", "1"),
    ] {
        let counter = text.lines().find(|line| {
            line.starts_with("zinder_compat_tip_wait_total{")
                && line.contains(&format!("outcome=\"{outcome}\""))
        });
        let counter = counter.ok_or_else(|| eyre!("no counter for {outcome}: {text}"))?;
        assert!(
            counter.contains(&format!("method=\"{METHOD}\"")),
            "{counter}"
        );
        assert!(
            counter.contains(&format!("blocks_ahead=\"{blocks_ahead}\"")),
            "{counter}"
        );
        assert!(
            counter.ends_with(" 1"),
            "{outcome} counted once, got {counter}"
        );
        assert!(
            text.lines().any(|line| {
                line.starts_with("zinder_compat_tip_wait_duration_seconds_count{")
                    && line.contains(&format!("outcome=\"{outcome}\""))
                    && line.ends_with(" 1")
            }),
            "{outcome} duration recorded once: {text}"
        );
    }
    assert!(
        text.lines()
            .any(|line| line == "zinder_compat_tip_waiters 0"),
        "every waiter released its gauge: {text}"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn await_height_until_respects_enabled_state_and_deadline_cap() -> Result<()> {
    let reads = FakeReads::at_tip(9);
    let wait = tip_wait(2, 8)?;

    assert_eq!(
        TipWait::disabled()
            .await_height_until(&reads, METHOD, BlockHeight::new(10), None)
            .await?,
        TipWaitProgress::NotReached
    );

    assert_eq!(
        wait.await_height_until(&reads, METHOD, BlockHeight::new(9), None)
            .await?,
        TipWaitProgress::Reached
    );

    let expired_cap = Instant::now();
    let started = Instant::now();
    assert_eq!(
        wait.await_height_until(&reads, METHOD, BlockHeight::new(10), Some(expired_cap))
            .await?,
        TipWaitProgress::NotReached
    );
    assert_eq!(started.elapsed(), Duration::ZERO);

    let short_cap = Instant::now() + Duration::from_millis(250);
    let started = Instant::now();
    assert_eq!(
        wait.await_height_until(&reads, METHOD, BlockHeight::new(10), Some(short_cap))
            .await?,
        TipWaitProgress::NotReached
    );
    assert_eq!(started.elapsed(), Duration::from_millis(250));

    let generous_cap = Instant::now() + Duration::from_secs(10);
    let waiting = {
        let wait = wait.clone();
        let reads = reads.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let progress = wait
                .await_height_until(&reads, METHOD, BlockHeight::new(10), Some(generous_cap))
                .await?;
            Ok::<_, Status>((progress, started.elapsed()))
        })
    };
    settle().await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    reads.publish_tip(10);
    assert_eq!(
        waiting.await??,
        (TipWaitProgress::Reached, Duration::from_millis(400))
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn fast_path_at_or_below_tip_and_beyond_window_skips_publication_subscription() -> Result<()>
{
    let reads = FakeReads::at_tip(9);
    let wait = tip_wait(2, 8)?;

    assert_eq!(
        wait.await_height_until(&reads, METHOD, BlockHeight::new(9), None)
            .await?,
        TipWaitProgress::Reached
    );
    assert_eq!(
        wait.await_height_until(&reads, METHOD, BlockHeight::new(3), None)
            .await?,
        TipWaitProgress::Reached
    );
    assert_eq!(
        wait.await_height_until(&reads, METHOD, BlockHeight::new(12), None)
            .await?,
        TipWaitProgress::NotReached
    );
    assert_eq!(
        reads.subscriptions(),
        0,
        "heights at/below tip or outside the wait window must not subscribe to publications"
    );

    let waiting = spawn_wait(&wait, &reads, 10);
    settle().await;
    assert_eq!(reads.subscriptions(), 1);
    reads.publish_tip(10);
    waiting.await??;
    Ok(())
}
