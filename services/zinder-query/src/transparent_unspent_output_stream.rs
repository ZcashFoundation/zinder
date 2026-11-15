//! Incremental delivery of one address's unspent transparent outputs.
//!
//! A read walks the wallet projection's address index one bounded page at a
//! time and hands each page to the caller before it reads the next, so the
//! outputs one stream holds stay near a few pages however many outputs the
//! address has. The producer stops when the caller drops the stream, and it
//! fails the stream when the publisher revokes reads on the stream's pair.

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
use zinder_wallet_projection::WalletAddressUnspentOutputKey;

use crate::{
    QueryError, TransparentAddressUnspentOutputsRequest, WalletServingReadPair, serving_read,
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
/// consumes outputs, and dropping the stream stops them.
///
/// A stream that keeps reading after its pair has been released for longer
/// than the released-pair read limit fails with
/// [`QueryError::ChainEpochPinUnavailable`] for [`Self::chain_epoch`]. The
/// caller discards the outputs it received and restarts the read at a newer
/// epoch.
#[derive(Debug)]
pub struct TransparentAddressUnspentOutputs {
    /// Chain epoch every output is read at.
    pub chain_epoch: ChainEpoch,
    pages: mpsc::Receiver<Result<UnspentOutputPage, QueryError>>,
    page: std::vec::IntoIter<TransparentUnspentOutput>,
    finished: bool,
}

impl TransparentAddressUnspentOutputs {
    /// Starts reading the outputs `request` selects from `pair`, whose visible
    /// epoch is `chain_epoch`.
    pub(crate) fn spawn(
        pair: Arc<WalletServingReadPair>,
        request: &TransparentAddressUnspentOutputsRequest,
        chain_epoch: ChainEpoch,
    ) -> Self {
        Self::spawn_paged(
            pair,
            request,
            chain_epoch,
            TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE,
        )
    }

    fn spawn_paged(
        pair: Arc<WalletServingReadPair>,
        request: &TransparentAddressUnspentOutputsRequest,
        chain_epoch: ChainEpoch,
        page_size: NonZeroU16,
    ) -> Self {
        let (pages, receiver) = mpsc::channel(BUFFERED_PAGES);
        tokio::spawn(
            produce_pages(
                pair,
                request.address_script_hash,
                request.start_height,
                page_size,
                pages,
            )
            .in_current_span(),
        );
        Self {
            chain_epoch,
            pages: receiver,
            page: Vec::new().into_iter(),
            finished: false,
        }
    }
}

impl Stream for TransparentAddressUnspentOutputs {
    type Item = Result<TransparentUnspentOutput, QueryError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(output) = this.page.next() {
                return Poll::Ready(Some(Ok(output)));
            }
            if this.finished {
                return Poll::Ready(None);
            }
            match ready!(this.pages.poll_recv(context)) {
                Some(Ok(page)) => {
                    this.finished = page.last;
                    this.page = page.outputs.into_iter();
                }
                Some(Err(error)) => {
                    this.finished = true;
                    return Poll::Ready(Some(Err(error)));
                }
                None => {
                    this.finished = true;
                    return Poll::Ready(Some(Err(QueryError::BlockingTaskFailed {
                        reason: "the unspent-output read ended before its last page".to_owned(),
                    })));
                }
            }
        }
    }
}

/// One storage page of a stream. `last` marks the page that completes it.
#[derive(Debug)]
struct UnspentOutputPage {
    outputs: Vec<TransparentUnspentOutput>,
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
    pair: Arc<WalletServingReadPair>,
    address_script_hash: TransparentAddressScriptHash,
    start_height: BlockHeight,
    page_size: NonZeroU16,
    pages: mpsc::Sender<Result<UnspentOutputPage, QueryError>>,
) {
    let mut after = None;
    loop {
        if pair.are_reads_revoked() {
            return fail(pair, &pages, None).await;
        }
        let page_read = serving_read::off_reactor(Arc::clone(&pair), move |pair| {
            read_page(&pair, address_script_hash, start_height, after, page_size)
        });
        // The blocking read cannot be interrupted, but a caller that leaves
        // while it runs should not have to wait for this task to notice.
        let page_read = tokio::select! {
            biased;
            () = pages.closed() => return,
            page_read = page_read => page_read,
        };
        let (outputs, next_page_after) = match page_read {
            Ok(page) => page,
            Err(error) => return fail(pair, &pages, Some(error)).await,
        };
        let last = next_page_after.is_none();
        let delivery = tokio::select! {
            biased;
            () = pair.reads_revoked() => PageDelivery::ReadsRevoked,
            sent = pages.send(Ok(UnspentOutputPage { outputs, last })) => {
                if sent.is_ok() { PageDelivery::Sent } else { PageDelivery::ReceiverDropped }
            }
        };
        match delivery {
            PageDelivery::Sent if last => return,
            PageDelivery::Sent => after = next_page_after,
            PageDelivery::ReceiverDropped => return,
            PageDelivery::ReadsRevoked => return fail(pair, &pages, None).await,
        }
    }
}

/// Releases `pair`, then ends the stream with `error`, or with the revoked
/// pair's epoch when `error` is `None`.
///
/// The pair is dropped before the send, which can wait on a stalled caller.
async fn fail(
    pair: Arc<WalletServingReadPair>,
    pages: &mpsc::Sender<Result<UnspentOutputPage, QueryError>>,
    error: Option<QueryError>,
) {
    let error = error.unwrap_or_else(|| pair.revoked_read_error());
    drop(pair);
    let _ = pages.send(Err(error)).await;
}

fn read_page(
    pair: &WalletServingReadPair,
    address_script_hash: TransparentAddressScriptHash,
    start_height: BlockHeight,
    after: Option<WalletAddressUnspentOutputKey>,
    page_size: NonZeroU16,
) -> Result<
    (
        Vec<TransparentUnspentOutput>,
        Option<WalletAddressUnspentOutputKey>,
    ),
    QueryError,
> {
    let page = pair.wallet().address_unspent_outputs_page_from_height(
        address_script_hash,
        start_height,
        after,
        page_size,
    )?;
    let outputs = page
        .outputs
        .into_iter()
        .map(|output| {
            TransparentUnspentOutput::new(
                output.address_script_hash,
                output.script_pub_key,
                output.outpoint,
                output.value_zat,
                output.created_at.block.height,
                output.created_at.block.hash,
            )
        })
        .collect();
    Ok((outputs, page.next_page_after))
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

    use tokio_stream::StreamExt as _;
    use zinder_core::{
        BlockHeight, BlockHeightRange, Network, TransactionId, TransparentAddressScriptHash,
        TransparentOutPoint, TransparentUnspentOutput,
    };
    use zinder_testkit::{
        ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations,
    };
    use zinder_wallet_projection::{
        WalletAddressTransactionKey, WalletAddressUnspentOutputKey, WalletProjectionReadyEvidence,
        WalletSpentOutput, WalletUnspentOutput,
    };
    use zinder_wallet_rocksdb::{
        RocksDbWalletError, WalletAddressTransactionHistoryPage, WalletAddressUnspentOutputsPage,
    };

    use super::TransparentAddressUnspentOutputs;
    use crate::{
        QueryError, TransparentAddressUnspentOutputsRequest, WalletProjectionReader,
        WalletQueryApi, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
    };

    const SCRIPT_PUB_KEY: &[u8] = &[0x51];
    /// Page size the paged tests read with, so a few dozen outputs span
    /// several pages.
    const TEST_PAGE_SIZE: NonZeroU16 = NonZeroU16::MIN.saturating_add(7);
    const OTHER_SCRIPT_PUB_KEY: &[u8] = &[0x52];
    const WAIT_LIMIT: Duration = Duration::from_secs(10);
    /// Holders of a pair no read holds: the test and the query's slot.
    const IDLE_PAIR_HOLDERS: usize = 2;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_delivers_every_page_in_address_index_order() -> Result<(), Box<dyn Error>> {
        let output_count = 2 * page_size() + 7;
        let fixture = CountedPair::with_outputs(output_count)?;
        let stream = fixture.open_paged()?;
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

        assert!(outputs.is_empty());
        assert_eq!(fixture.page_reads(), 1);
        fixture.wait_until_released().await
    }

    /// A caller that stops reading leaves at most three pages in memory: the
    /// one it is reading, one buffered, and one waiting to be sent.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_reads_storage_only_as_fast_as_its_caller_consumes()
    -> Result<(), Box<dyn Error>> {
        let fixture = CountedPair::with_outputs(4 * page_size())?;
        let mut stream = fixture.open_paged()?;

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
        let mut stream = fixture.open_paged()?;
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
        let mut stream = fixture.open_paged()?;
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

    fn page_size() -> usize {
        usize::from(TEST_PAGE_SIZE.get())
    }

    /// A pair over `output_count` outputs of one address, all created at
    /// height 1, whose wallet reader counts unspent-output page reads.
    struct CountedPair {
        pair: Arc<WalletServingReadPair>,
        query: WalletServingQuery<()>,
        address: TransparentAddressScriptHash,
        page_reads: Arc<AtomicUsize>,
        _store: WalletServingStoreFixture,
    }

    impl CountedPair {
        fn with_outputs(output_count: usize) -> Result<Self, Box<dyn Error>> {
            let activations = sample_regtest_upgrade_activations();
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
            let mut store = WalletServingStoreFixture::from_chain(&chain, &activations)?;
            let (canonical, wallet) = store.take_readers()?;
            let page_reads = Arc::new(AtomicUsize::new(0));
            let wallet = CountingWalletReader {
                inner: Arc::new(wallet),
                page_reads: Arc::clone(&page_reads),
            };
            let pair = Arc::new(WalletServingReadPair::new(
                Arc::new(canonical),
                Arc::new(wallet),
            )?);
            let query = WalletServingQuery::from_serving_pair_slot(
                WalletServingPairSlot::new(Arc::clone(&pair)),
                (),
                Arc::new(activations),
            );
            Ok(Self {
                pair,
                query,
                address,
                page_reads,
                _store: store,
            })
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
        fn open_paged(&self) -> Result<TransparentAddressUnspentOutputs, Box<dyn Error>> {
            Ok(TransparentAddressUnspentOutputs::spawn_paged(
                Arc::clone(&self.pair),
                &TransparentAddressUnspentOutputsRequest {
                    address_script_hash: self.address,
                    start_height: BlockHeight::new(0),
                },
                self.pair.canonical().chain_epoch()?,
                TEST_PAGE_SIZE,
            ))
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
    }

    impl WalletProjectionReader for CountingWalletReader {
        fn network(&self) -> Network {
            self.inner.network()
        }

        fn ready_evidence(&self) -> &WalletProjectionReadyEvidence {
            self.inner.ready_evidence()
        }

        fn address_unspent_outputs_page(
            &self,
            address_script_hash: TransparentAddressScriptHash,
            after: Option<WalletAddressUnspentOutputKey>,
            page_size: NonZeroU16,
        ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError> {
            self.page_reads.fetch_add(1, Ordering::AcqRel);
            self.inner
                .address_unspent_outputs_page(address_script_hash, after, page_size)
        }

        fn address_unspent_outputs_page_from_height(
            &self,
            address_script_hash: TransparentAddressScriptHash,
            start_height: BlockHeight,
            after: Option<WalletAddressUnspentOutputKey>,
            page_size: NonZeroU16,
        ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError> {
            self.page_reads.fetch_add(1, Ordering::AcqRel);
            self.inner.address_unspent_outputs_page_from_height(
                address_script_hash,
                start_height,
                after,
                page_size,
            )
        }

        fn address_transaction_history_page(
            &self,
            address_script_hash: TransparentAddressScriptHash,
            after: Option<WalletAddressTransactionKey>,
            page_size: NonZeroU16,
        ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError> {
            self.inner
                .address_transaction_history_page(address_script_hash, after, page_size)
        }

        fn address_transaction_history_range_page(
            &self,
            address_script_hash: TransparentAddressScriptHash,
            height_range: BlockHeightRange,
            after: Option<WalletAddressTransactionKey>,
            page_size: NonZeroU16,
        ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError> {
            self.inner.address_transaction_history_range_page(
                address_script_hash,
                height_range,
                after,
                page_size,
            )
        }

        fn address_balance(
            &self,
            address_script_hash: TransparentAddressScriptHash,
        ) -> Result<u64, RocksDbWalletError> {
            self.inner.address_balance(address_script_hash)
        }

        fn find_unspent_output(
            &self,
            outpoint: TransparentOutPoint,
        ) -> Result<Option<WalletUnspentOutput>, RocksDbWalletError> {
            self.inner.find_unspent_output(outpoint)
        }

        fn find_spent_output(
            &self,
            outpoint: TransparentOutPoint,
        ) -> Result<Option<WalletSpentOutput>, RocksDbWalletError> {
            self.inner.find_spent_output(outpoint)
        }
    }
}
