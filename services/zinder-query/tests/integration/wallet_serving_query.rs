#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::sync::Arc;
use std::thread::{self, ThreadId};

use parking_lot::Mutex;
use tracing::Instrument as _;
use tracing_subscriber::{Layer, layer::Context, layer::SubscriberExt as _, registry::LookupSpan};
use zinder_core::{BlockHeight, BlockHeightRange, Network};
use zinder_query::{
    CanonicalReader, QueryError, RocksDbCanonicalReader, RocksDbWalletReader,
    WalletProjectionReader, WalletQueryApi, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

/// A wallet read runs its storage work on another thread than the runtime
/// that serves the request, inside the request's span.
///
/// The runtime is single-threaded and runs on the test thread, so the
/// request span entered on any other thread is the reader's storage read.
#[test]
fn wallet_serving_reads_run_off_the_reactor_inside_the_request_span() -> eyre::Result<()> {
    let span_entries = Arc::new(SpanEntries::default());
    let subscriber = tracing_subscriber::registry().with(SpanEntryLayer {
        entries: Arc::clone(&span_entries),
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let reactor_thread = thread::current().id();
    tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async {
            let activations = Arc::new(sample_regtest_upgrade_activations());
            let mut store_fixture = WalletServingStoreFixture::from_chain(
                &ChainFixture::new(Network::ZcashRegtest).extend_blocks(2),
                activations.as_ref(),
            )?;
            let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
            let query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(
                Arc::new(WalletServingReadPair::new(
                    Arc::new(RocksDbCanonicalReader::new(canonical_reader)?),
                    Arc::new(RocksDbWalletReader::new(wallet_reader)),
                )?),
            ));
            let compact_block = query
                .compact_block_at(BlockHeight::new(2), None)
                .instrument(tracing::info_span!("wallet_request"))
                .await?;
            assert_eq!(compact_block.compact_block.height(), BlockHeight::new(2));
            Ok::<_, eyre::Report>(())
        })
    })?;

    assert!(
        span_entries.entered_off(reactor_thread, "wallet_request"),
        "the storage read must run off the reactor inside the request span"
    );
    Ok(())
}

#[tokio::test]
async fn compact_block_range_is_capped_before_any_canonical_read() -> eyre::Result<()> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(2),
        activations.as_ref(),
    )?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(
        WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical_reader)?) as Arc<dyn CanonicalReader>,
            Arc::new(RocksDbWalletReader::new(wallet_reader)) as Arc<dyn WalletProjectionReader>,
        )?,
    )));

    let over_cap = query
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1001)),
            None,
        )
        .await;
    assert!(
        matches!(
            over_cap,
            Err(QueryError::BlockRangeTooLarge {
                requested: 1001,
                maximum: 1000
            })
        ),
        "over-cap range returned {over_cap:?}"
    );

    let inverted = query
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(1)),
            None,
        )
        .await;
    assert!(
        matches!(inverted, Err(QueryError::InvalidBlockRange { .. })),
        "inverted range returned {inverted:?}"
    );

    let served = query
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2)),
            None,
        )
        .await?;
    assert_eq!(served.compact_blocks.len(), 2);
    Ok(())
}

/// The threads each span was entered on, by span name.
#[derive(Default)]
struct SpanEntries {
    entries: Mutex<Vec<(&'static str, ThreadId)>>,
}

impl SpanEntries {
    /// Reports whether a span named `span_name` was entered on a thread other
    /// than `thread`.
    fn entered_off(&self, thread: ThreadId, span_name: &str) -> bool {
        self.entries
            .lock()
            .iter()
            .any(|(name, entered_on)| *name == span_name && *entered_on != thread)
    }
}

/// Records the thread every span is entered on.
struct SpanEntryLayer {
    entries: Arc<SpanEntries>,
}

impl<Subscriber> Layer<Subscriber> for SpanEntryLayer
where
    Subscriber: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_enter(&self, id: &tracing::span::Id, context: Context<'_, Subscriber>) {
        if let Some(span) = context.span(id) {
            self.entries
                .entries
                .lock()
                .push((span.metadata().name(), thread::current().id()));
        }
    }
}
