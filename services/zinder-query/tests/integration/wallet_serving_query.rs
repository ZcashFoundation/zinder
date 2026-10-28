#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use parking_lot::Mutex;
use zinder_core::{
    BlockBlobArtifact, BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId,
    CanonicalHistoryBounds, ChainEpoch, ChainEpochId, CommitmentTreeCheckpoint,
    CompactBlockArtifact, Network, SubtreeRootArtifact, SubtreeRootRange, TransactionBlobArtifact,
    TransactionId, TransactionLocation,
};
use zinder_query::{
    CanonicalReader, QueryError, WalletProjectionReader, WalletQueryApi, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_store::{
    BlockHashLookup, CanonicalEventFence, CanonicalStoreError, ChainEventEnvelope,
    ChainEventHistoryRequest, ChainEventStreamFamily, ChainEventStreamResume,
    EventStreamStartPosition, RawBlobRetention,
};
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

/// Bounds how long the gated read waits for the reactor to make progress.
const REACTOR_PROGRESS_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn wallet_serving_reads_leave_the_reactor_free() -> eyre::Result<()> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(2),
        activations.as_ref(),
    )?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let reactor_gate = Arc::new(ReactorGate::default());
    let query = WalletServingQuery::from_serving_pair_slot(
        WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::new(GatedCanonicalReader {
                canonical_reader,
                reactor_gate: Arc::clone(&reactor_gate),
                reported_history_bounds: None,
            }) as Arc<dyn CanonicalReader>,
            Arc::new(wallet_reader) as Arc<dyn WalletProjectionReader>,
        )?)),
        (),
        Arc::clone(&activations),
    );

    // The runtime is single-threaded, so this task only runs while the store
    // read is off the reactor.
    let (reactor_progressed, gate) = mpsc::channel();
    reactor_gate.arm(gate);
    tokio::spawn(async move {
        let _ = reactor_progressed.send(());
    });

    let visible_tip_block = query.visible_tip_block(None).await?;

    assert_eq!(visible_tip_block.height, BlockHeight::new(2));
    assert!(
        reactor_gate.observed_reactor_progress(),
        "the canonical read held the reactor for its whole duration"
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
    let query = WalletServingQuery::from_serving_pair_slot(
        WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::new(canonical_reader) as Arc<dyn CanonicalReader>,
            Arc::new(wallet_reader) as Arc<dyn WalletProjectionReader>,
        )?)),
        (),
        Arc::clone(&activations),
    );

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

/// A serving lookup refuses to call a miss below a history floor an absence.
///
/// Wallet-serving admission requires complete history, so a serving pair never
/// starts after a checkpoint in production; the check is defense in depth.
#[tokio::test]
async fn serving_transaction_miss_below_a_history_floor_is_not_an_absence() -> eyre::Result<()> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(2),
        activations.as_ref(),
    )?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let checkpointed = CanonicalHistoryBounds::checkpointed(BlockId::new(
        BlockHeight::new(1),
        BlockHash::from_bytes([1; 32]),
    ))?;
    let query = WalletServingQuery::from_serving_pair_slot(
        WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::new(GatedCanonicalReader {
                canonical_reader,
                reactor_gate: Arc::new(ReactorGate::default()),
                reported_history_bounds: Some(checkpointed),
            }) as Arc<dyn CanonicalReader>,
            Arc::new(wallet_reader) as Arc<dyn WalletProjectionReader>,
        )?)),
        (),
        Arc::clone(&activations),
    );
    let chain_epoch_id = query.visible_tip_block(None).await?.chain_epoch.id;

    for at_epoch_id in [None, Some(chain_epoch_id)] {
        let outcome = query
            .transaction(TransactionId::from_bytes([0x5c; 32]), at_epoch_id)
            .await;
        assert!(
            matches!(
                outcome,
                Err(QueryError::CanonicalHistoryUnavailable {
                    first_available_height
                }) if first_available_height == BlockHeight::new(2)
            ),
            "expected the serving history floor, got {outcome:?}"
        );
    }
    Ok(())
}

/// One-shot rendezvous proving a canonical read released the reactor.
///
/// The gate is armed after pair admission so the validation read that
/// `WalletServingReadPair::new` performs does not consume it.
#[derive(Default)]
struct ReactorGate {
    gate: Mutex<Option<mpsc::Receiver<()>>>,
    reactor_progressed: AtomicBool,
}

impl ReactorGate {
    fn arm(&self, gate: mpsc::Receiver<()>) {
        *self.gate.lock() = Some(gate);
    }

    fn wait_for_reactor_progress(&self) {
        let Some(gate) = self.gate.lock().take() else {
            return;
        };
        if gate.recv_timeout(REACTOR_PROGRESS_TIMEOUT).is_ok() {
            self.reactor_progressed.store(true, Ordering::Release);
        }
    }

    fn observed_reactor_progress(&self) -> bool {
        self.reactor_progressed.load(Ordering::Acquire)
    }
}

/// Canonical reader whose epoch read blocks until the reactor makes progress.
///
/// `reported_history_bounds` overrides the store's retained history range.
struct GatedCanonicalReader {
    canonical_reader: zinder_store::RocksDbCanonicalSecondary,
    reactor_gate: Arc<ReactorGate>,
    reported_history_bounds: Option<CanonicalHistoryBounds>,
}

impl CanonicalReader for GatedCanonicalReader {
    fn raw_blob_retention(&self) -> RawBlobRetention {
        self.canonical_reader.raw_blob_retention()
    }

    fn network(&self) -> Network {
        self.canonical_reader.network()
    }

    fn history_bounds(&self) -> CanonicalHistoryBounds {
        self.reported_history_bounds
            .unwrap_or_else(|| self.canonical_reader.history_bounds())
    }

    fn event_fence(&self) -> CanonicalEventFence {
        self.canonical_reader.event_fence()
    }

    fn chain_epoch(&self) -> Result<ChainEpoch, CanonicalStoreError> {
        self.reactor_gate.wait_for_reactor_progress();
        self.canonical_reader.chain_epoch()
    }

    fn chain_epoch_at(&self, epoch_id: ChainEpochId) -> Result<ChainEpoch, CanonicalStoreError> {
        self.canonical_reader.chain_epoch_at(epoch_id)
    }

    fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockHeaderArtifact>, CanonicalStoreError> {
        self.canonical_reader.block_header_at(height)
    }

    fn block_hash_lookup(
        &self,
        block_hash: BlockHash,
    ) -> Result<BlockHashLookup, CanonicalStoreError> {
        self.canonical_reader.block_hash_lookup(block_hash)
    }

    fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CompactBlockArtifact>, CanonicalStoreError> {
        self.canonical_reader.compact_block_at(height)
    }

    fn compact_blocks_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<CompactBlockArtifact>, CanonicalStoreError> {
        self.canonical_reader.compact_blocks_in_range(range)
    }

    fn block_blob_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockBlobArtifact>, CanonicalStoreError> {
        self.canonical_reader.block_blob_at(height)
    }

    fn block_blobs_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockBlobArtifact>>, CanonicalStoreError> {
        self.canonical_reader.block_blobs_in_range(range)
    }

    fn transaction_location(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionLocation>, CanonicalStoreError> {
        self.canonical_reader.transaction_location(transaction_id)
    }

    fn transaction_blob(
        &self,
        location: TransactionLocation,
    ) -> Result<Option<TransactionBlobArtifact>, CanonicalStoreError> {
        self.canonical_reader.transaction_blob(location)
    }

    fn tree_state_checkpoint_at_or_before(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CommitmentTreeCheckpoint>, CanonicalStoreError> {
        self.canonical_reader
            .tree_state_checkpoint_at_or_before(height)
    }

    fn subtree_roots(
        &self,
        range: SubtreeRootRange,
    ) -> Result<Vec<SubtreeRootArtifact>, CanonicalStoreError> {
        self.canonical_reader.subtree_roots(range)
    }

    fn wallet_chain_event_history(
        &self,
        request: ChainEventHistoryRequest<'_>,
    ) -> Result<Vec<ChainEventEnvelope>, CanonicalStoreError> {
        self.canonical_reader.wallet_chain_event_history(request)
    }

    fn resolve_wallet_chain_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, CanonicalStoreError> {
        self.canonical_reader
            .resolve_wallet_chain_event_stream_start(start, requested_family)
    }
}
