//! Incremental delivery of one address's unspent transparent outputs.
//!
//! A read walks the wallet projection's address index one bounded page at a
//! time and hands each page to the caller before it reads the next, so the
//! outputs one stream holds stay near a few pages however many outputs the
//! address has. The producer stops when the caller drops the stream, and it
//! fails the stream when the publisher revokes reads on the stream's pair. A
//! page read that finds a table file of the pair deleted continues on the
//! pair that replaces it only at the stream's epoch.
//!
//! An on-demand stream reads nothing until its caller first polls it and
//! never reads ahead: it asks for the next page only once the caller has
//! consumed the previous one. Its first page holds the number of outputs its
//! caller chooses, at most [`TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE`], and each
//! later page doubles up to that size as the caller consumes, so a caller
//! that merges many streams holds at most one page per stream.

use std::{
    num::NonZeroU16,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use tokio::sync::mpsc;
use tokio_stream::Stream;
use tracing::Instrument as _;
use zinder_core::{
    BlockHeight, ChainEpoch, TransparentAddressScriptHash, TransparentUnspentOutput,
};
use zinder_wallet_projection::{WalletAddressUnspentOutputKey, WalletUnspentOutput};

use crate::{
    QueryError, TransparentAddressUnspentOutputsRequest, WalletServingPairSlot,
    WalletServingReadPair, serving_read,
};

/// Most outputs one storage read of a transparent unspent-output stream
/// returns.
///
/// A dropped or revoked stream stops after at most one such read.
pub const TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE: NonZeroU16 = NonZeroU16::MIN.saturating_add(1023);

/// Pages buffered between the storage reader and the caller.
const BUFFERED_PAGES: usize = 1;

/// Unspent transparent outputs of one address, read at one chain epoch.
///
/// The stream yields outputs in ascending `(block_height, outpoint)` order.
/// It ends after the address's last output or with one terminal error, and
/// never ends early without one. Storage reads advance only as the caller
/// consumes outputs, and dropping the stream stops them. An eager stream
/// starts reading when it opens and keeps up to three pages ahead of its
/// caller; an on-demand stream reads one page at a time, only when polled.
///
/// A stream that keeps reading after its pair has been released for longer
/// than the released-pair read limit fails with
/// [`QueryError::ChainEpochPinUnavailable`] for [`Self::chain_epoch`]. So
/// does a stream whose pair lost a table file to compaction when the pair
/// that replaces it serves a newer epoch. The caller discards the outputs it
/// received and restarts the read at a newer epoch.
#[derive(Debug)]
pub struct TransparentAddressUnspentOutputs {
    /// Chain epoch every output is read at.
    pub chain_epoch: ChainEpoch,
    pages: mpsc::Receiver<Result<UnspentOutputPage, QueryError>>,
    page: std::vec::IntoIter<WalletUnspentOutput>,
    finished: bool,
    on_demand: Option<OnDemandPages>,
}

/// The read requests of an on-demand stream.
#[derive(Debug)]
struct OnDemandPages {
    requests: mpsc::Sender<NonZeroU16>,
    next_page_size: NonZeroU16,
    requested: bool,
}

/// How a producer decides when to read its next page.
enum Pacing {
    /// Read the next page as soon as the previous one is queued.
    Eager(NonZeroU16),
    /// Read a page only when the stream requests one, at the requested size.
    OnDemand(mpsc::Receiver<NonZeroU16>),
}

struct UnspentOutputSelection {
    addresses: Vec<TransparentAddressScriptHash>,
    start_height: BlockHeight,
}

impl From<&TransparentAddressUnspentOutputsRequest> for UnspentOutputSelection {
    fn from(request: &TransparentAddressUnspentOutputsRequest) -> Self {
        Self {
            addresses: vec![request.address_script_hash],
            start_height: request.start_height,
        }
    }
}

impl TransparentAddressUnspentOutputs {
    /// Starts reading the outputs `request` selects from `pair`, whose visible
    /// epoch is `chain_epoch`, and which `slot` published.
    pub(crate) fn spawn(
        slot: WalletServingPairSlot,
        pair: Arc<WalletServingReadPair>,
        request: &TransparentAddressUnspentOutputsRequest,
        chain_epoch: ChainEpoch,
    ) -> Self {
        Self::spawn_paged(
            slot,
            pair,
            request,
            chain_epoch,
            TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE,
        )
    }

    /// Starts reading as [`Self::spawn`] does, `page_size` outputs per
    /// storage read.
    pub(crate) fn spawn_paged(
        slot: WalletServingPairSlot,
        pair: Arc<WalletServingReadPair>,
        request: &TransparentAddressUnspentOutputsRequest,
        chain_epoch: ChainEpoch,
        page_size: NonZeroU16,
    ) -> Self {
        Self::spawn_selection(slot, pair, request.into(), chain_epoch, page_size)
    }

    pub(crate) fn spawn_addresses(
        slot: WalletServingPairSlot,
        pair: Arc<WalletServingReadPair>,
        addresses: Vec<TransparentAddressScriptHash>,
        start_height: BlockHeight,
        chain_epoch: ChainEpoch,
    ) -> Self {
        Self::spawn_selection(
            slot,
            pair,
            UnspentOutputSelection {
                addresses,
                start_height,
            },
            chain_epoch,
            TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE,
        )
    }

    fn spawn_selection(
        slot: WalletServingPairSlot,
        pair: Arc<WalletServingReadPair>,
        selection: UnspentOutputSelection,
        chain_epoch: ChainEpoch,
        page_size: NonZeroU16,
    ) -> Self {
        let (pages, receiver) = mpsc::channel(BUFFERED_PAGES);
        tokio::spawn(
            produce_pages(slot, pair, selection, Pacing::Eager(page_size), pages).in_current_span(),
        );
        Self {
            chain_epoch,
            pages: receiver,
            page: Vec::new().into_iter(),
            finished: false,
            on_demand: None,
        }
    }

    /// Opens a stream of the outputs `request` selects from `pair` that
    /// reads a page only when its caller polls it with no output left, the
    /// first of `first_page_size` outputs, at most
    /// [`TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE`].
    ///
    /// Opening reads no storage. The producer still ends the stream when the
    /// publisher revokes reads on `pair`, polled or not.
    pub(crate) fn spawn_on_demand(
        slot: WalletServingPairSlot,
        pair: Arc<WalletServingReadPair>,
        request: &TransparentAddressUnspentOutputsRequest,
        chain_epoch: ChainEpoch,
        first_page_size: NonZeroU16,
    ) -> Self {
        let (pages, receiver) = mpsc::channel(BUFFERED_PAGES);
        let (requests, request_receiver) = mpsc::channel(1);
        tokio::spawn(
            produce_pages(
                slot,
                pair,
                request.into(),
                Pacing::OnDemand(request_receiver),
                pages,
            )
            .in_current_span(),
        );
        Self {
            chain_epoch,
            pages: receiver,
            page: Vec::new().into_iter(),
            finished: false,
            on_demand: Some(OnDemandPages {
                requests,
                next_page_size: first_page_size.min(TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE),
                requested: false,
            }),
        }
    }

    /// Outputs read from storage and not yet returned to the caller.
    #[cfg(test)]
    fn buffered_outputs(&self) -> usize {
        self.page.len()
    }

    /// Reads the next unspent output and its transaction index within the block.
    ///
    /// This advances the same stream in the same order as polling it directly.
    /// The index lets callers order all outputs of a height by block position.
    ///
    /// # Errors
    ///
    /// Returns the same terminal query errors as the stream.
    pub async fn next_with_tx_index(
        &mut self,
    ) -> Option<Result<(u32, TransparentUnspentOutput), QueryError>> {
        std::future::poll_fn(|context| self.poll_output(context))
            .await
            .map(|output| {
                output.map(|output| (output.created_at.tx_index_in_block, unspent_output(output)))
            })
    }

    fn poll_output(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<WalletUnspentOutput, QueryError>>> {
        loop {
            if let Some(output) = self.page.next() {
                return Poll::Ready(Some(Ok(output)));
            }
            if self.finished {
                return Poll::Ready(None);
            }
            if let Some(on_demand) = self.on_demand.as_mut().filter(|pages| !pages.requested) {
                // A failed request means the producer ended; its pages
                // channel then reports why.
                let _ = on_demand.requests.try_send(on_demand.next_page_size);
                on_demand.requested = true;
            }
            match ready!(self.pages.poll_recv(context)) {
                Some(Ok(page)) => {
                    self.finished = page.last;
                    self.page = page.outputs.into_iter();
                    if let Some(on_demand) = self.on_demand.as_mut() {
                        on_demand.requested = false;
                        on_demand.next_page_size = on_demand
                            .next_page_size
                            .saturating_mul(NonZeroU16::MIN.saturating_add(1))
                            .min(TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE);
                    }
                }
                Some(Err(error)) => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(error)));
                }
                None => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(QueryError::BlockingTaskFailed {
                        reason: "the unspent-output read ended before its last page".to_owned(),
                    })));
                }
            }
        }
    }
}

impl Stream for TransparentAddressUnspentOutputs {
    type Item = Result<TransparentUnspentOutput, QueryError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_output(context)
            .map(|output| output.map(|output| output.map(unspent_output)))
    }
}

fn unspent_output(output: WalletUnspentOutput) -> TransparentUnspentOutput {
    TransparentUnspentOutput::new(
        output.address_script_hash,
        output.script_pub_key,
        output.outpoint,
        output.value_zat,
        output.created_at.block.height,
        output.created_at.block.hash,
    )
}

/// One storage page of a stream. `last` marks the page that completes it.
#[derive(Debug)]
struct UnspentOutputPage {
    outputs: Vec<WalletUnspentOutput>,
    last: bool,
}

/// How delivering one page ended.
enum PageDelivery {
    Sent,
    ReceiverDropped,
    ReadsRevoked,
}

/// Reads and sends pages until the set is complete, the caller drops the
/// stream, a read fails, or the publisher revokes reads on `pair`.
async fn produce_pages(
    slot: WalletServingPairSlot,
    mut pair: Arc<WalletServingReadPair>,
    selection: UnspentOutputSelection,
    mut pacing: Pacing,
    pages: mpsc::Sender<Result<UnspentOutputPage, QueryError>>,
) {
    let chain_epoch_id = pair.canonical_fence().chain_epoch_id();
    let UnspentOutputSelection {
        addresses,
        start_height,
    } = selection;
    let mut addresses = addresses.into_iter().peekable();
    let Some(mut address_script_hash) = addresses.next() else {
        return;
    };
    let mut after = None;
    loop {
        let page_size = match &mut pacing {
            Pacing::Eager(page_size) => *page_size,
            // An idle on-demand stream still releases its pair when reads
            // are revoked or its caller leaves.
            Pacing::OnDemand(requests) => tokio::select! {
                biased;
                () = pair.reads_revoked() => return fail(pair, &pages).await,
                () = pages.closed() => return,
                requested = requests.recv() => match requested {
                    Some(page_size) => page_size,
                    None => return,
                },
            },
        };
        if pair.are_reads_revoked() {
            return fail(pair, &pages).await;
        }
        let page_read =
            serving_read::read_pair(&slot, pair, Some(chain_epoch_id), move |pair| async move {
                let page =
                    read_page(&pair, address_script_hash, start_height, after, page_size).await?;
                Ok((pair, page))
            });
        // The storage read cannot be interrupted, but a caller that leaves
        // while it runs should not have to wait for this task to notice.
        let page_read = tokio::select! {
            biased;
            () = pages.closed() => return,
            page_read = page_read => page_read,
        };
        let (outputs, next_page_after) = match page_read {
            Ok((read_pair, page)) => {
                pair = read_pair;
                page
            }
            Err(error) => {
                // The failed read has already released the pair.
                let _ = pages.send(Err(error)).await;
                return;
            }
        };
        let last = next_page_after.is_none() && addresses.peek().is_none();
        let delivery = tokio::select! {
            biased;
            () = pair.reads_revoked() => PageDelivery::ReadsRevoked,
            sent = pages.send(Ok(UnspentOutputPage { outputs, last })) => {
                if sent.is_ok() { PageDelivery::Sent } else { PageDelivery::ReceiverDropped }
            }
        };
        match delivery {
            PageDelivery::Sent if last => return,
            PageDelivery::Sent => {
                after = next_page_after;
                if after.is_none()
                    && let Some(next_address) = addresses.next()
                {
                    address_script_hash = next_address;
                }
            }
            PageDelivery::ReceiverDropped => return,
            PageDelivery::ReadsRevoked => return fail(pair, &pages).await,
        }
    }
}

/// Releases `pair`, then ends the stream with the revoked pair's epoch.
///
/// The pair is dropped before the send, which can wait on a stalled caller.
async fn fail(
    pair: Arc<WalletServingReadPair>,
    pages: &mpsc::Sender<Result<UnspentOutputPage, QueryError>>,
) {
    let error = pair.revoked_read_error();
    drop(pair);
    let _ = pages.send(Err(error)).await;
}

async fn read_page(
    pair: &WalletServingReadPair,
    address_script_hash: TransparentAddressScriptHash,
    start_height: BlockHeight,
    after: Option<WalletAddressUnspentOutputKey>,
    page_size: NonZeroU16,
) -> Result<
    (
        Vec<WalletUnspentOutput>,
        Option<WalletAddressUnspentOutputKey>,
    ),
    QueryError,
> {
    let page = pair
        .wallet()
        .address_unspent_outputs_page_from_height(
            address_script_hash,
            start_height,
            after,
            page_size,
        )
        .await
        .map_err(QueryError::wallet_read)?;
    Ok((page.outputs, page.next_page_after))
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        num::NonZeroU16,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use parking_lot::Mutex;
    use tokio_stream::StreamExt as _;
    use zinder_core::{
        BlockHeight, BlockHeightRange, ChainEpochId, Network, TransactionId,
        TransparentAddressScriptHash, TransparentOutPoint, TransparentOutpointSpendStatus,
        TransparentUnspentOutput,
    };
    use zinder_testkit::{
        ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations,
    };
    use zinder_wallet_projection::{
        WalletAddressTransactionHistoryPage, WalletAddressTransactionKey,
        WalletAddressUnspentOutputKey, WalletAddressUnspentOutputsPage,
        WalletProjectionReadyEvidence,
    };

    use super::TransparentAddressUnspentOutputs;
    use crate::{
        QueryError, RocksDbCanonicalReader, RocksDbWalletReader, SpendStatusReadEnd,
        StorageReadError, TransparentAddressUnspentOutputsRequest, WalletProjectionReader,
        WalletQueryApi, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
    };

    const SCRIPT_PUB_KEY: &[u8] = &[0x51];
    /// Page size the paged tests read with, so a few dozen outputs span
    /// several pages.
    const TEST_PAGE_SIZE: NonZeroU16 = NonZeroU16::MIN.saturating_add(7);
    /// First page size of the on-demand tests that watch pages grow.
    const SMALL_FIRST_PAGE_SIZE: NonZeroU16 = NonZeroU16::MIN.saturating_add(15);
    const OTHER_SCRIPT_PUB_KEY: &[u8] = &[0x52];
    const WAIT_LIMIT: Duration = Duration::from_secs(10);
    /// Holders of a pair no read holds: the test and the query's slot.
    const IDLE_PAIR_HOLDERS: usize = 2;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_delivers_every_page_in_address_index_order() -> Result<(), Box<dyn Error>> {
        let output_count = 2 * page_size() + 7;
        let fixture = CountedPair::with_outputs(output_count)?;
        let stream = fixture.open_paged();
        assert_eq!(
            stream.chain_epoch.id,
            fixture.pair.canonical_fence().chain_epoch_id()
        );

        let outputs: Vec<_> = stream.collect::<Result<_, _>>().await?;

        let output_indexes: Vec<u32> = outputs
            .iter()
            .map(|output| output.outpoint.output_index)
            .collect();
        assert_eq!(
            output_indexes,
            (0..u32::try_from(output_count)?).collect::<Vec<_>>()
        );
        assert_eq!(fixture.page_reads(), 3);
        fixture.wait_until_released().await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_address_without_outputs_streams_no_output_at_its_epoch()
    -> Result<(), Box<dyn Error>> {
        let fixture = CountedPair::with_outputs(3)?;
        let empty_address = TransparentAddressScriptHash::of_script_pub_key(OTHER_SCRIPT_PUB_KEY);
        let stream = fixture.open(empty_address).await?;
        assert_eq!(
            stream.chain_epoch.id,
            fixture.pair.canonical_fence().chain_epoch_id()
        );

        let outputs: Vec<_> = stream.collect::<Result<_, _>>().await?;

        assert_eq!(outputs, [] as [zinder_core::TransparentUnspentOutput; 0]);
        assert_eq!(fixture.page_reads(), 1);
        fixture.wait_until_released().await
    }

    /// A caller that stops reading leaves at most three pages in memory: the
    /// one it is reading, one buffered, and one waiting to be sent.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_reads_storage_only_as_fast_as_its_caller_consumes()
    -> Result<(), Box<dyn Error>> {
        let fixture = CountedPair::with_outputs(4 * page_size())?;
        let mut stream = fixture.open_paged();

        stream
            .next()
            .await
            .ok_or("the stream ended before its first output")??;
        fixture.wait_for_page_reads(3).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            fixture.page_reads(),
            3,
            "a stalled caller must stall the reads"
        );
        assert!(fixture.pair_holders() > IDLE_PAIR_HOLDERS);
        drop(stream);
        fixture.wait_until_released().await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_a_stream_mid_read_stops_its_storage_reads_and_releases_its_pair()
    -> Result<(), Box<dyn Error>> {
        let fixture = CountedPair::with_outputs(4 * page_size())?;
        let mut stream = fixture.open_paged();
        stream
            .next()
            .await
            .ok_or("the stream ended before its first output")??;
        fixture.wait_for_page_reads(3).await?;

        drop(stream);

        fixture.wait_until_released().await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            fixture.page_reads(),
            3,
            "an abandoned stream must not read again"
        );
        Ok(())
    }

    /// Revocation releases the pair even though the caller is not reading.
    ///
    /// The caller then receives the outputs already buffered and the epoch
    /// failure, never a stream that looks complete.
    #[tokio::test(flavor = "multi_thread")]
    async fn revoking_reads_releases_a_stalled_stream_and_ends_it_with_its_epoch_unavailable()
    -> Result<(), Box<dyn Error>> {
        let output_count = 4 * page_size();
        let fixture = CountedPair::with_outputs(output_count)?;
        let mut stream = fixture.open_paged();
        let epoch = stream.chain_epoch.id;
        stream
            .next()
            .await
            .ok_or("the stream ended before its first output")??;
        fixture.wait_for_page_reads(3).await?;

        assert!(fixture.pair.revoke_reads());
        fixture.wait_until_released().await?;

        let mut delivered = 1;
        let failure = loop {
            match stream.next().await {
                Some(Ok(_)) => delivered += 1,
                Some(Err(error)) => break error,
                None => return Err("a revoked stream ended as if complete".into()),
            }
        };
        assert!(
            matches!(
                failure,
                QueryError::ChainEpochPinUnavailable { chain_epoch_id } if chain_epoch_id == epoch
            ),
            "unexpected failure {failure:?}"
        );
        assert!(delivered < output_count);
        assert!(stream.next().await.is_none());
        assert_eq!(fixture.page_reads(), 3);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_on_a_revoked_pair_fails_without_reading_storage() -> Result<(), Box<dyn Error>>
    {
        let fixture = CountedPair::with_outputs(3)?;
        fixture.pair.revoke_reads();

        let outcome: Result<Vec<_>, _> = fixture.open(fixture.address).await?.collect().await;

        assert!(matches!(
            outcome,
            Err(QueryError::ChainEpochPinUnavailable { .. })
        ));
        assert_eq!(fixture.page_reads(), 0);
        fixture.wait_until_released().await
    }

    /// Streams opened on demand and never polled read no storage, however
    /// many of them a caller holds.
    #[tokio::test(flavor = "multi_thread")]
    async fn on_demand_streams_read_nothing_until_polled() -> Result<(), Box<dyn Error>> {
        let fixture = CountedPair::with_outputs(40)?;
        let mut streams = Vec::new();
        for _ in 0..256 {
            streams.push(fixture.open_on_demand().await?);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(fixture.page_reads(), 0, "an unpolled stream read storage");
        assert!(
            streams
                .iter()
                .all(|stream| stream.chain_epoch.id
                    == fixture.pair.canonical_fence().chain_epoch_id())
        );
        drop(streams);
        fixture.wait_until_released().await
    }

    /// An on-demand stream reads a page only when its caller has consumed
    /// the previous one, starting at 16 outputs and doubling, so a slow
    /// caller holds at most the one page it is reading.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_on_demand_stream_reads_one_growing_page_at_a_time() -> Result<(), Box<dyn Error>> {
        let fixture = CountedPair::with_outputs(100)?;
        let mut stream = fixture.open_on_demand().await?;

        stream
            .next()
            .await
            .ok_or("the stream ended before its first output")??;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(fixture.page_sizes(), [16], "the stream read ahead");
        assert_eq!(stream.buffered_outputs(), 15);

        for _ in 1..16 {
            stream.next().await.ok_or("the first page ended early")??;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            fixture.page_sizes(),
            [16],
            "a consumed page must not trigger a read until the caller asks again"
        );

        stream
            .next()
            .await
            .ok_or("the stream ended after its first page")??;
        assert_eq!(fixture.page_sizes(), [16, 32]);
        assert_eq!(stream.buffered_outputs(), 31);
        drop(stream);
        fixture.wait_until_released().await
    }

    /// Opening an on-demand stream needs no blocking thread, and a pin the
    /// query does not serve is refused at open.
    #[test]
    fn on_demand_streams_open_without_the_blocking_pool() -> Result<(), Box<dyn Error>> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()?
            .block_on(open_on_demand_streams_while_the_blocking_pool_is_held())
    }

    async fn open_on_demand_streams_while_the_blocking_pool_is_held() -> Result<(), Box<dyn Error>>
    {
        let fixture = CountedPair::with_outputs(3)?;
        let (pool_held, pool_held_receiver) = tokio::sync::oneshot::channel();
        let (release_pool, pool_released) = tokio::sync::oneshot::channel::<()>();
        let held_pool = tokio::task::spawn_blocking(move || {
            let _ = pool_held.send(());
            let _ = pool_released.blocking_recv();
        });
        pool_held_receiver.await?;

        let streams = tokio::time::timeout(Duration::from_secs(1), async {
            let mut streams = Vec::new();
            for _ in 0..64 {
                streams.push(fixture.open_on_demand().await?);
            }
            Ok::<_, QueryError>(streams)
        })
        .await
        .map_err(|_| "opening an on-demand stream waited for the blocking pool")??;
        let unserved = ChainEpochId::new(u64::MAX);
        let refused = fixture
            .query
            .transparent_address_unspent_outputs_on_demand(
                TransparentAddressUnspentOutputsRequest {
                    address_script_hash: fixture.address,
                    start_height: BlockHeight::new(0),
                },
                SMALL_FIRST_PAGE_SIZE,
                Some(unserved),
            )
            .await;
        assert!(matches!(
            refused,
            Err(QueryError::ChainEpochPinUnavailable { chain_epoch_id }) if chain_epoch_id == unserved
        ));
        assert_eq!(fixture.page_reads(), 0);

        release_pool
            .send(())
            .map_err(|()| "the held blocking thread stopped waiting")?;
        held_pool.await?;
        drop(streams);
        Ok(())
    }

    /// The on-demand mode yields the eager mode's outputs in the eager
    /// mode's order.
    #[tokio::test(flavor = "multi_thread")]
    async fn on_demand_and_eager_streams_yield_the_same_outputs_in_order()
    -> Result<(), Box<dyn Error>> {
        let fixture = CountedPair::with_outputs(300)?;

        let eager: Vec<_> = fixture
            .open(fixture.address)
            .await?
            .collect::<Result<_, _>>()
            .await?;
        let eager_reads = fixture.page_reads();
        let on_demand: Vec<_> = fixture
            .open_on_demand()
            .await?
            .collect::<Result<_, _>>()
            .await?;

        assert_eq!(eager.len(), 300);
        assert_eq!(on_demand, eager);
        assert_eq!(eager_reads, 1);
        assert_eq!(
            fixture.page_sizes().get(eager_reads..),
            Some([16, 32, 64, 128, 256].as_slice())
        );
        fixture.wait_until_released().await
    }

    /// An on-demand stream's first page holds the outputs its caller asks
    /// for, at most the eager page size, and later pages double from there.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_on_demand_stream_starts_at_the_first_page_size_its_caller_chooses()
    -> Result<(), Box<dyn Error>> {
        let fixture = CountedPair::with_outputs(300)?;
        let first_page_sizes = [
            (NonZeroU16::MIN.saturating_add(119), vec![120, 240]),
            (NonZeroU16::MIN.saturating_add(511), vec![512]),
            (NonZeroU16::MAX, vec![1_024]),
        ];

        for (first_page_size, expected_page_sizes) in first_page_sizes {
            let reads_before = fixture.page_sizes().len();
            let outputs: Vec<_> = fixture
                .open_on_demand_from(first_page_size)
                .await?
                .collect::<Result<_, _>>()
                .await?;
            assert_eq!(outputs.len(), 300);
            assert_eq!(
                fixture.page_sizes().get(reads_before..),
                Some(expected_page_sizes.as_slice()),
                "first page size {first_page_size}"
            );
        }
        fixture.wait_until_released().await
    }

    /// Revocation releases the pair of an on-demand stream that was never
    /// polled, and the stream then ends with its epoch unavailable.
    #[tokio::test(flavor = "multi_thread")]
    async fn revoking_reads_releases_an_idle_on_demand_stream() -> Result<(), Box<dyn Error>> {
        let fixture = CountedPair::with_outputs(40)?;
        let mut stream = fixture.open_on_demand().await?;
        let epoch = stream.chain_epoch.id;

        assert!(fixture.pair.revoke_reads());
        fixture.wait_until_released().await?;

        assert!(matches!(
            stream.next().await,
            Some(Err(QueryError::ChainEpochPinUnavailable { chain_epoch_id })) if chain_epoch_id == epoch
        ));
        assert!(stream.next().await.is_none());
        assert_eq!(fixture.page_reads(), 0);
        Ok(())
    }

    const FINAL_SCRIPT_PUB_KEY: &[u8] = &[0x53];

    #[tokio::test]
    async fn native_batch_pages_addresses_once_and_bounds_read_ahead() -> Result<(), Box<dyn Error>>
    {
        let page_capacity = usize::from(super::TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE.get());
        let first_count = 3 * page_capacity + 7;
        let fixture = CountedPair::with_batch(first_count, None)?;
        let (mut stream, _ingest) = fixture.open_batch().await?;
        assert_batch_epoch(&fixture, &mut stream).await?;

        let first = next_output(&mut stream).await?;
        assert_eq!(first.block_height, 2);
        assert_eq!(first.address_script_hash, fixture.address.as_bytes());
        fixture.wait_for_page_reads(3).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(fixture.page_reads(), 3);
        assert_eq!(
            first.outpoint.ok_or("missing first outpoint")?.output_index,
            0
        );
        for expected_index in 1..u32::try_from(first_count)? {
            let output = next_output(&mut stream).await?;
            assert_eq!(output.block_height, 2);
            assert_eq!(output.address_script_hash, fixture.address.as_bytes());
            assert_eq!(
                output.outpoint.ok_or("missing outpoint")?.output_index,
                expected_index
            );
        }
        let final_output = next_output(&mut stream).await?;
        assert_eq!(
            final_output.address_script_hash,
            TransparentAddressScriptHash::of_script_pub_key(FINAL_SCRIPT_PUB_KEY).as_bytes()
        );
        assert_eq!(final_output.block_height, 2);
        assert!(stream.next().await.is_none());
        assert_eq!(fixture.page_reads(), 6);
        fixture.wait_until_released().await
    }

    #[tokio::test]
    async fn dropping_a_native_batch_at_an_address_boundary_releases_its_pair()
    -> Result<(), Box<dyn Error>> {
        let fixture = CountedPair::with_batch(1, Some(Arc::new(tokio::sync::Notify::new())))?;
        let (mut stream, _ingest) = fixture.open_batch().await?;
        assert_batch_epoch(&fixture, &mut stream).await?;
        next_output(&mut stream).await?;
        fixture.wait_for_page_reads(3).await?;
        drop(stream);
        fixture.wait_until_released().await
    }

    #[tokio::test]
    async fn revoking_a_native_batch_at_an_address_boundary_releases_its_pair()
    -> Result<(), Box<dyn Error>> {
        let gate = Arc::new(tokio::sync::Notify::new());
        let fixture = CountedPair::with_batch(1, Some(Arc::clone(&gate)))?;
        let (mut stream, _ingest) = fixture.open_batch().await?;
        assert_batch_epoch(&fixture, &mut stream).await?;
        next_output(&mut stream).await?;
        fixture.wait_for_page_reads(3).await?;

        assert!(fixture.pair.revoke_reads());
        gate.notify_one();
        fixture.wait_until_released().await?;
        let error = stream
            .next()
            .await
            .ok_or("expected terminal status")?
            .err()
            .ok_or("received output after revocation")?;
        let details = tonic_types::StatusExt::get_error_details(&error);
        assert_eq!(
            details.error_info().map(|error| error.reason.as_str()),
            Some(zinder_proto::v1::ops::ErrorReason::ChainEpochPinUnavailable.as_str_name())
        );
        assert!(stream.next().await.is_none());
        Ok(())
    }

    type NativeBatchStream = <crate::WalletQueryGrpcAdapter<WalletServingQuery<(), crate::AdmittedIngestControl>>
        as zinder_proto::v1::wallet::wallet_query_server::WalletQuery>::TransparentAddressesUnspentOutputsStream;

    async fn assert_batch_epoch(
        fixture: &CountedPair,
        stream: &mut NativeBatchStream,
    ) -> Result<(), Box<dyn Error>> {
        use zinder_proto::v1::wallet::transparent_unspent_outputs_chunk::Body;
        let header = stream.next().await.ok_or("missing stream header")??;
        assert_eq!(
            header.body,
            Some(Body::Header(zinder_store::chain_view_message(
                fixture.pair.chain_epoch()
            )))
        );
        Ok(())
    }

    async fn next_output(
        stream: &mut NativeBatchStream,
    ) -> Result<zinder_proto::v1::wallet::TransparentUnspentOutput, Box<dyn Error>> {
        use zinder_proto::v1::wallet::transparent_unspent_outputs_chunk::Body;
        let Some(Body::Item(output)) = stream.next().await.ok_or("missing output")??.body else {
            return Err("expected an output after the chain view".into());
        };
        Ok(output)
    }

    fn page_size() -> usize {
        usize::from(TEST_PAGE_SIZE.get())
    }

    /// A pair over `output_count` outputs of one address, all created at
    /// height 1, whose wallet reader counts unspent-output page reads.
    struct CountedPair {
        pair: Arc<WalletServingReadPair>,
        slot: WalletServingPairSlot,
        query: WalletServingQuery<()>,
        address: TransparentAddressScriptHash,
        page_reads: Arc<AtomicUsize>,
        page_sizes: Arc<Mutex<Vec<u16>>>,
        _store: WalletServingStoreFixture,
    }

    impl CountedPair {
        fn with_outputs(output_count: usize) -> Result<Self, Box<dyn Error>> {
            let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
            let block = chain
                .block_at(BlockHeight::new(1))
                .cloned()
                .ok_or("fixture must contain height 1")?;
            let address = TransparentAddressScriptHash::of_script_pub_key(SCRIPT_PUB_KEY);
            let creator = TransactionId::from_bytes([0x10; 32]);
            let mut chain = chain;
            for output_index in 0..u32::try_from(output_count)? {
                chain = chain.with_address_output_index(TransparentUnspentOutput::new(
                    address,
                    SCRIPT_PUB_KEY.to_vec(),
                    TransparentOutPoint::new(creator, output_index),
                    1_000,
                    block.height,
                    block.hash,
                ));
            }
            Self::with_chain(&chain, None)
        }

        fn with_batch(
            first_count: usize,
            final_read_gate: Option<Arc<tokio::sync::Notify>>,
        ) -> Result<Self, Box<dyn Error>> {
            let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
            let early = chain.blocks()[0].clone();
            let selected = chain.blocks()[1].clone();
            let addresses = [
                (SCRIPT_PUB_KEY, early, 1, 0x11),
                (SCRIPT_PUB_KEY, selected.clone(), first_count, 0x22),
                (FINAL_SCRIPT_PUB_KEY, selected, 1, 0x33),
            ];
            for (script, block, count, tx_tag) in addresses {
                for index in 0..u32::try_from(count)? {
                    chain = chain.with_address_output_index(TransparentUnspentOutput::new(
                        TransparentAddressScriptHash::of_script_pub_key(script),
                        script.to_vec(),
                        TransparentOutPoint::new(TransactionId::from_bytes([tx_tag; 32]), index),
                        1000,
                        block.height,
                        block.hash,
                    ));
                }
            }
            Self::with_chain(&chain, final_read_gate)
        }

        fn with_chain(
            chain: &ChainFixture,
            final_read_gate: Option<Arc<tokio::sync::Notify>>,
        ) -> Result<Self, Box<dyn Error>> {
            let activations = sample_regtest_upgrade_activations();
            let address = TransparentAddressScriptHash::of_script_pub_key(SCRIPT_PUB_KEY);
            let mut store = WalletServingStoreFixture::from_chain(chain, &activations)?;
            let (canonical, wallet) = store.take_readers()?;
            let page_reads = Arc::new(AtomicUsize::new(0));
            let page_sizes = Arc::new(Mutex::new(Vec::new()));
            let wallet = CountingWalletReader {
                inner: Arc::new(RocksDbWalletReader::new(wallet)),
                page_reads: Arc::clone(&page_reads),
                page_sizes: Arc::clone(&page_sizes),
                final_read_gate,
            };
            let pair = Arc::new(WalletServingReadPair::new(
                Arc::new(RocksDbCanonicalReader::new(canonical)?),
                Arc::new(wallet),
            )?);
            let slot = WalletServingPairSlot::new(Arc::clone(&pair));
            let query = WalletServingQuery::from_serving_pair_slot(slot.clone());
            Ok(Self {
                pair,
                slot,
                query,
                address,
                page_reads,
                page_sizes,
                _store: store,
            })
        }

        async fn open_batch(
            &self,
        ) -> Result<(NativeBatchStream, zinder_testkit::IngestControlFixture), Box<dyn Error>>
        {
            use crate::{AdmittedIngestControl, WalletEndpointMetadata, WalletQueryGrpcAdapter};
            use zinder_proto::v1::wallet::{self, wallet_query_server::WalletQuery};
            let ingest = zinder_testkit::IngestControlFixture::spawn(Network::ZcashRegtest).await?;
            let admitted =
                AdmittedIngestControl::connect(ingest.endpoint(), None, Network::ZcashRegtest)
                    .await?;
            let query =
                WalletServingQuery::from_admitted_native_serving_pair(self.slot.clone(), admitted);
            let adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());
            let empty = TransparentAddressScriptHash::of_script_pub_key(OTHER_SCRIPT_PUB_KEY);
            let last = TransparentAddressScriptHash::of_script_pub_key(FINAL_SCRIPT_PUB_KEY);
            let addresses = [self.address, empty, self.address, last, last]
                .into_iter()
                .map(|address| wallet::AddressLookup {
                    selector: Some(wallet::address_lookup::Selector::ScriptHash(
                        address.as_bytes().to_vec(),
                    )),
                })
                .collect();
            let response = adapter
                .transparent_addresses_unspent_outputs(tonic::Request::new(
                    wallet::TransparentAddressesUnspentOutputsRequest {
                        addresses,
                        start_height: 2,
                        at_epoch_id: Some(self.pair.chain_epoch().id.value()),
                    },
                ))
                .await?;
            Ok((response.into_inner(), ingest))
        }

        async fn open_on_demand(&self) -> Result<TransparentAddressUnspentOutputs, QueryError> {
            self.open_on_demand_from(SMALL_FIRST_PAGE_SIZE).await
        }

        /// Opens an on-demand stream of the fixture address whose first page
        /// asks for `first_page_size` outputs.
        async fn open_on_demand_from(
            &self,
            first_page_size: NonZeroU16,
        ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
            self.query
                .transparent_address_unspent_outputs_on_demand(
                    TransparentAddressUnspentOutputsRequest {
                        address_script_hash: self.address,
                        start_height: BlockHeight::new(0),
                    },
                    first_page_size,
                    None,
                )
                .await
        }

        fn page_sizes(&self) -> Vec<u16> {
            self.page_sizes.lock().clone()
        }

        async fn open(
            &self,
            address_script_hash: TransparentAddressScriptHash,
        ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
            self.query
                .transparent_address_unspent_outputs(
                    TransparentAddressUnspentOutputsRequest {
                        address_script_hash,
                        start_height: BlockHeight::new(0),
                    },
                    None,
                )
                .await
        }

        /// Opens a stream of the fixture address that reads
        /// [`TEST_PAGE_SIZE`] outputs per page.
        fn open_paged(&self) -> TransparentAddressUnspentOutputs {
            TransparentAddressUnspentOutputs::spawn_paged(
                self.slot.clone(),
                Arc::clone(&self.pair),
                &TransparentAddressUnspentOutputsRequest {
                    address_script_hash: self.address,
                    start_height: BlockHeight::new(0),
                },
                self.pair.canonical().chain_epoch(),
                TEST_PAGE_SIZE,
            )
        }

        fn page_reads(&self) -> usize {
            self.page_reads.load(Ordering::Acquire)
        }

        fn pair_holders(&self) -> usize {
            Arc::strong_count(&self.pair)
        }

        async fn wait_for_page_reads(&self, page_reads: usize) -> Result<(), Box<dyn Error>> {
            self.wait_until(|fixture| fixture.page_reads() >= page_reads)
                .await
                .map_err(|()| format!("the stream did not reach {page_reads} page reads").into())
        }

        async fn wait_until_released(&self) -> Result<(), Box<dyn Error>> {
            self.wait_until(|fixture| fixture.pair_holders() == IDLE_PAIR_HOLDERS)
                .await
                .map_err(|()| "the stream kept its pair".into())
        }

        async fn wait_until(&self, condition: impl Fn(&Self) -> bool) -> Result<(), ()> {
            let deadline = tokio::time::Instant::now() + WAIT_LIMIT;
            while !condition(self) {
                if tokio::time::Instant::now() > deadline {
                    return Err(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(())
        }
    }

    /// Delegates every read and counts unspent-output page reads.
    struct CountingWalletReader {
        inner: Arc<dyn WalletProjectionReader>,
        page_reads: Arc<AtomicUsize>,
        page_sizes: Arc<Mutex<Vec<u16>>>,
        final_read_gate: Option<Arc<tokio::sync::Notify>>,
    }

    #[async_trait::async_trait]
    impl WalletProjectionReader for CountingWalletReader {
        fn network(&self) -> Network {
            self.inner.network()
        }

        fn ready_evidence(&self) -> &WalletProjectionReadyEvidence {
            self.inner.ready_evidence()
        }

        async fn address_unspent_outputs_page_from_height(
            &self,
            address_script_hash: TransparentAddressScriptHash,
            start_height: BlockHeight,
            after: Option<WalletAddressUnspentOutputKey>,
            page_size: NonZeroU16,
        ) -> Result<WalletAddressUnspentOutputsPage, StorageReadError> {
            self.page_reads.fetch_add(1, Ordering::AcqRel);
            self.page_sizes.lock().push(page_size.get());
            if address_script_hash
                == TransparentAddressScriptHash::of_script_pub_key(FINAL_SCRIPT_PUB_KEY)
                && let Some(gate) = &self.final_read_gate
            {
                gate.notified().await;
            }
            self.inner
                .address_unspent_outputs_page_from_height(
                    address_script_hash,
                    start_height,
                    after,
                    page_size,
                )
                .await
        }

        async fn address_transaction_history_range_page(
            &self,
            address_script_hash: TransparentAddressScriptHash,
            height_range: BlockHeightRange,
            after: Option<WalletAddressTransactionKey>,
            page_size: NonZeroU16,
        ) -> Result<WalletAddressTransactionHistoryPage, StorageReadError> {
            self.inner
                .address_transaction_history_range_page(
                    address_script_hash,
                    height_range,
                    after,
                    page_size,
                )
                .await
        }

        async fn total_address_balance(
            &self,
            addresses: Vec<TransparentAddressScriptHash>,
        ) -> Result<u64, StorageReadError> {
            self.inner.total_address_balance(addresses).await
        }

        async fn outpoint_spend_statuses(
            &self,
            outpoints: Vec<TransparentOutPoint>,
            end: SpendStatusReadEnd,
        ) -> Result<Vec<TransparentOutpointSpendStatus>, StorageReadError> {
            self.inner.outpoint_spend_statuses(outpoints, end).await
        }
    }
}
