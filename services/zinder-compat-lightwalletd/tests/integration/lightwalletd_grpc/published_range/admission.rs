use std::{
    path::Path,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use eyre::{Result, eyre};
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use tonic::{Code, Request};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::BlockHeight;
use zinder_proto::compat::lightwalletd::{self, EncodedBlockRange as _};
use zinder_query::WalletServingQuery;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, sample_regtest_upgrade_activations,
};

use super::{
    EpochPinRecorder, ReaderFixture, SEALED_END, block_range_request, wallet_chain, writer,
};
use crate::integration::lightwalletd_grpc::GatedChunkRead;

type Adapter = LightwalletdGrpcAdapter<EpochPinRecorder<WalletServingQuery<()>>>;

async fn gated_adapter(
    store: &ReaderFixture,
    directory: &Path,
    later_views: Vec<WalletServingQuery<()>>,
) -> Result<(Adapter, mpsc::UnboundedReceiver<GatedChunkRead>)> {
    let history = store
        .publish(&directory.join("shielded"), SEALED_END)
        .await?;
    let (gates, reads) = mpsc::unbounded_channel();
    let mut recorder = EpochPinRecorder::new(store.query.clone()).with_later_views(later_views);
    recorder.gated_chunk_reads = Some(gates);
    recorder.expose_range_fence = true;
    Ok((
        LightwalletdGrpcAdapter::new(recorder).with_published_shielded_history(history),
        reads,
    ))
}

// Explicit transparent blocks take the published listener's canonical read path.
fn gated_range(start: u64, end: u64) -> Request<lightwalletd::BlockRange> {
    let mut request = block_range_request(start, end);
    request.pool_types = vec![lightwalletd::PoolType::Transparent as i32];
    Request::new(request)
}

fn poll_once<F: Future + Unpin>(call: &mut F) -> Poll<F::Output> {
    std::pin::Pin::new(call).poll(&mut Context::from_waker(Waker::noop()))
}

#[derive(Default)]
struct AdmissionWake(tokio::sync::Notify);

impl Wake for AdmissionWake {
    fn wake(self: Arc<Self>) {
        self.0.notify_one();
    }
}

fn queue<F: Future + Unpin>(call: &mut F) -> impl Future<Output = ()> + Unpin + use<F> {
    let wake = Arc::new(AdmissionWake::default());
    let waker = Waker::from(wake.clone());
    assert!(
        std::pin::Pin::new(call)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    Box::pin(async move { wake.0.notified().await })
}

async fn next_read(reads: &mut mpsc::UnboundedReceiver<GatedChunkRead>) -> Result<GatedChunkRead> {
    reads.recv().await.ok_or_else(|| eyre!("read gate closed"))
}

#[tokio::test]
async fn published_reads_keep_fifo_order_and_capacity_across_cancellation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let chain = wallet_chain(10_000)?;
    let store = ReaderFixture::new(writer(&chain)?.into_serving_store_fixture()?)?;
    let shorter = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).through(BlockHeight::new(3)),
        &sample_regtest_upgrade_activations(),
    )?;
    let shorter = ReaderFixture::new(shorter.into_serving_store_fixture()?)?;
    let (adapter, mut reads) =
        gated_adapter(&store, dir.path(), vec![shorter.query.clone()]).await?;
    tokio::time::timeout(
        Duration::from_secs(10),
        check_read_admission(&adapter, &mut reads),
    )
    .await?
}

async fn check_read_admission(
    adapter: &Adapter,
    reads: &mut mpsc::UnboundedReceiver<GatedChunkRead>,
) -> Result<()> {
    let mut running = Vec::new();
    for _ in 0..16 {
        let mut call = adapter.get_encoded_block_range(gated_range(1, 1));
        assert!(poll_once(&mut call).is_pending());
        running.push((call, next_read(reads).await?));
    }

    let mut future_height = adapter.get_encoded_block_range(gated_range(7, 7));
    assert!(
        matches!(poll_once(&mut future_height), Poll::Ready(Err(status)) if status.code() == Code::OutOfRange)
    );

    // A chunk that can read the catalog waits, then rechecks the tip it ends at.
    let mut displaced = adapter.get_encoded_block_range(gated_range(3, 6));
    let mut admission = queue(&mut displaced);
    let mut oldest = adapter.get_encoded_block_range(gated_range(1, 3));
    let mut cancelled = adapter.get_encoded_block_range(gated_range(2, 2));
    let mut youngest = adapter.get_encoded_block_range(gated_range(3, 3));
    assert!(poll_once(&mut oldest).is_pending());
    assert!(poll_once(&mut cancelled).is_pending());
    assert!(poll_once(&mut youngest).is_pending());
    drop(cancelled);

    let (call, blocked) = running.pop().ok_or_else(|| eyre!("no active read"))?;
    drop(call);
    assert!(poll_once(&mut admission).is_pending());
    assert!(blocked.release.send(()).is_ok());
    admission.await;
    // The completing read installs the shorter view while the other reads wait.
    assert_eq!(
        displaced.await.err().map(|status| status.code()),
        Some(Code::OutOfRange)
    );

    // Poll in reverse order with only one permit available.
    assert!(poll_once(&mut youngest).is_pending());
    assert!(poll_once(&mut oldest).is_pending());
    let first = next_read(reads).await?;
    assert_eq!((first.range.start.value(), first.range.end.value()), (1, 3));
    assert!(first.release.send(()).is_ok());
    let stream = oldest.await?;

    assert!(poll_once(&mut youngest).is_pending());
    let next = next_read(reads).await?;
    assert_eq!((next.range.start.value(), next.range.end.value()), (3, 3));
    assert!(next.release.send(()).is_ok());
    drop(youngest.await?);

    let later = next_read(reads).await?;
    assert_eq!((later.range.start.value(), later.range.end.value()), (2, 3));
    let mut after_disconnect = adapter.get_encoded_block_range(gated_range(1, 1));
    let mut admission = queue(&mut after_disconnect);
    drop(stream);
    assert!(poll_once(&mut admission).is_pending());
    assert!(later.release.send(()).is_ok());
    admission.await;
    assert!(poll_once(&mut after_disconnect).is_pending());
    assert!(next_read(reads).await?.release.send(()).is_ok());
    drop(after_disconnect.await?);
    Ok(())
}

#[tokio::test]
async fn published_first_and_later_reads_share_the_byte_budget_across_open_streams() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = ReaderFixture::new(writer(&wallet_chain(10_000)?)?.into_serving_store_fixture()?)?;
    let (adapter, mut reads) = gated_adapter(&store, dir.path(), Vec::new()).await?;
    let mut held = Vec::new();
    for (streams, later_budget) in [(16, 2 * 1024 * 1024), (32, 1024 * 1024)] {
        while held.len() < streams - 1 {
            held.push(
                adapter
                    .get_encoded_block_range(Request::new(block_range_request(1, 1)))
                    .await?,
            );
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut call = adapter.get_encoded_block_range(gated_range(1, 3));
            assert!(poll_once(&mut call).is_pending());
            let first = next_read(&mut reads).await?;
            assert_eq!(first.max_bytes, 256 * 1024);
            assert!(first.release.send(()).is_ok());
            let mut stream = call.await?.into_inner();
            let later = next_read(&mut reads).await?;
            assert_eq!(later.max_bytes, later_budget);
            assert!(later.release.send(()).is_ok());
            for _ in 1..=3 {
                stream
                    .next()
                    .await
                    .ok_or_else(|| eyre!("range ended early"))??;
            }
            assert!(stream.next().await.is_none());
            Ok::<_, eyre::Report>(())
        })
        .await??;
    }
    Ok(())
}
