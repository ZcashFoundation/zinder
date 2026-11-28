//! Expired cursors travel from the canonical writer through wallet gRPC to the SDK.

use std::{error::Error, num::NonZeroU32, num::NonZeroU64, path::Path, sync::Arc, time::Duration};

use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use zebra_chain::{
    block::Block,
    serialization::{ZcashDeserializeInto as _, ZcashSerialize as _},
};
use zinder_client::{
    EndpointBackedIndex, ErrorReason, EventStreamStart, MempoolEventCursor, MempoolSnapshotCursor,
    MempoolSnapshotRequest, RemoteChainIndex, RemoteOpenOptions, RetryPolicy,
};
use zinder_core::{
    AuthDigest, BlockHeight, MempoolEvictionReason, Network, NetworkUpgradeActivations,
    RawTransactionBytes, TransactionId, UnixTimestampMillis,
};
use zinder_ingest::{
    CanonicalIngestControlGrpcAdapter, LiveMempoolOwner, canonical_control_channel,
    follow_canonical_tip_with_control, mempool_ready_channel, run_live_mempool_owner,
};
use zinder_query::{
    AdmittedIngestControl, RocksDbCanonicalReader, RocksDbWalletReader, WalletEndpointMetadata,
    WalletQueryGrpcAdapter, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_runtime::{ComposedReadiness, Readiness};
use zinder_source::{MempoolSourceEntry, NodeSource};
use zinder_store::{
    CanonicalReorgPolicy, MempoolEvent, MempoolEventRetentionConfig,
    MempoolEventRetentionStepBudget, RawBlobRetention, RocksDbCanonicalStore,
    RocksDbResourceBudget,
};
use zinder_testkit::{MockMempoolSource, WalletServingStoreFixture};

use super::canonical_follow::{
    RecordingParseableSource, follower_fixture_at_its_node_tip,
    follower_with_default_request_timeout,
};

type TestResult<T> = Result<T, Box<dyn Error>>;

#[tokio::test]
async fn expired_writer_cursors_recover_with_a_fresh_wallet_snapshot() -> TestResult<()> {
    let (source, activations, mut writer, temporary) = follower_fixture_at_its_node_tip().await?;
    let canonical_path = temporary.path().join("canonical");
    let checkpoint = temporary.path().join("older-writer");
    for tag in 1..=3 {
        writer.append_mempool_event(
            MempoolEvent::Invalidated {
                transaction_id: TransactionId::from_bytes([tag; 32]),
                reason: MempoolEvictionReason::Expired,
            },
            UnixTimestampMillis::new(1),
        )?;
    }
    let _admission = writer.create_owner_checkpoint_physical(&checkpoint)?;
    let mut serving = WalletServingStoreFixture::from_published_canonical(
        temporary,
        &canonical_path,
        writer,
        &activations,
    )?;
    let (canonical, wallet) = serving.take_readers()?;
    let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?));
    let fixture = WalletFixture {
        source,
        activations,
        slot,
    };

    let entries = fixture.two_source_entries().await?;
    let (event_cursor, page_cursor) = fixture
        .with_wallet(
            fixture.open_writer(&canonical_path)?,
            entries,
            read_wallet_cursors,
        )
        .await?;

    let restored = fixture.open_writer(&checkpoint)?;
    let retention = MempoolEventRetentionConfig::new(None, Some(Duration::from_millis(1)));
    let budget = MempoolEventRetentionStepBudget::new(NonZeroU32::MAX, NonZeroU64::MAX);
    let pruned = restored.advance_mempool_event_retention(
        UnixTimestampMillis::new(10),
        retention,
        budget,
    )?;
    assert_eq!(pruned.report.oldest_retained_sequence, Some(3));
    assert_eq!(pruned.report.current_event_sequence, 3);
    let expected_anchor = restored
        .begin_mempool_snapshot(&[])?
        .events_resume_cursor()
        .ok_or("restored writer has no event head")?
        .as_bytes()
        .to_vec();

    fixture
        .with_wallet(restored, Vec::new(), async |client| {
            assert_wallet_recovery(client, event_cursor, page_cursor, &expected_anchor).await
        })
        .await
}

async fn read_wallet_cursors(
    client: RemoteChainIndex,
) -> TestResult<(MempoolEventCursor, MempoolSnapshotCursor)> {
    let mut events = client
        .mempool_events(EventStreamStart::EarliestRetained)
        .await?;
    let first = events.next().await.ok_or("missing first mempool event")??;
    assert_eq!(first.event_sequence, 1);
    let page = client
        .mempool_snapshot(MempoolSnapshotRequest {
            max_entries: 1,
            from_cursor: None,
        })
        .await?;
    assert_eq!(page.entries.len(), 1);
    let cursor = page.next_cursor.ok_or("missing second page")?;
    let last_page = client
        .mempool_snapshot(MempoolSnapshotRequest {
            max_entries: 1,
            from_cursor: Some(cursor.clone()),
        })
        .await?;
    assert_eq!(last_page.entries.len(), 1);
    assert!(last_page.next_cursor.is_none());
    assert_eq!(last_page.events_resume_cursor, page.events_resume_cursor);
    Ok((first.cursor, cursor))
}

async fn assert_wallet_recovery(
    client: RemoteChainIndex,
    event_cursor: MempoolEventCursor,
    page_cursor: MempoolSnapshotCursor,
    expected_anchor: &[u8],
) -> TestResult<()> {
    let event_error = client
        .mempool_events(EventStreamStart::AfterCursor(event_cursor))
        .await
        .err()
        .ok_or("writer replayed a cursor below its retained floor")?;
    assert_eq!(
        event_error.reason(),
        Some(ErrorReason::MempoolEventCursorExpired)
    );
    assert_eq!(
        event_error.retry_policy(),
        RetryPolicy::RefreshMempoolSnapshot
    );

    let page_error = client
        .mempool_snapshot(MempoolSnapshotRequest {
            max_entries: 1,
            from_cursor: Some(page_cursor),
        })
        .await
        .err()
        .ok_or("writer served a page anchored ahead of its event head")?;
    assert_eq!(
        page_error.reason(),
        Some(ErrorReason::SnapshotPageCursorExpired)
    );
    assert_eq!(
        page_error.retry_policy(),
        RetryPolicy::RefreshMempoolSnapshot
    );

    let fresh = client
        .mempool_snapshot(MempoolSnapshotRequest::default())
        .await?;
    assert_eq!(fresh.entries, []);
    let resume = fresh
        .events_resume_cursor
        .ok_or("fresh snapshot has no event cursor")?;
    assert_eq!(resume.as_bytes(), expected_anchor);
    let _stream = client
        .mempool_events(EventStreamStart::AfterCursor(resume))
        .await?;
    Ok(())
}

struct WalletFixture {
    source: RecordingParseableSource,
    activations: Arc<NetworkUpgradeActivations>,
    slot: WalletServingPairSlot,
}

impl WalletFixture {
    fn open_writer(&self, path: &Path) -> TestResult<RocksDbCanonicalStore> {
        Ok(RocksDbCanonicalStore::open_ready(
            path,
            &self.activations,
            RawBlobRetention::None,
            CanonicalReorgPolicy::new(4)?,
            RocksDbResourceBudget::for_local_tests(),
        )?)
    }

    async fn two_source_entries(&self) -> TestResult<Vec<MempoolSourceEntry>> {
        let mut entries = Vec::new();
        for height in [1, 2] {
            let source_block = self.source.fetch_block_at(BlockHeight::new(height)).await?;
            let block: Block = source_block
                .raw_block_bytes
                .as_slice()
                .zcash_deserialize_into()?;
            let transaction = block
                .transactions
                .first()
                .ok_or("fixture block has no transaction")?;
            entries.push(MempoolSourceEntry {
                transaction_id: TransactionId::from_bytes(transaction.hash().0),
                auth_digest: transaction
                    .auth_digest()
                    .map(|digest| AuthDigest::from_bytes(digest.0)),
                raw_transaction_bytes: RawTransactionBytes::new(
                    transaction.zcash_serialize_to_vec()?,
                ),
                observed_at_unix_millis: UnixTimestampMillis::new(2),
            });
        }
        Ok(entries)
    }

    async fn with_wallet<T>(
        &self,
        store: RocksDbCanonicalStore,
        entries: Vec<MempoolSourceEntry>,
        read: impl AsyncFnOnce(RemoteChainIndex) -> TestResult<T>,
    ) -> TestResult<T> {
        let tip = store.event_fence().visible_tip();
        let network = Network::ZcashRegtest;
        let cancel = CancellationToken::new();
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        let follower = follower_with_default_request_timeout(
            &self.source,
            &self.activations,
            &readiness,
            &cancel,
        );
        let (writer, commands) = canonical_control_channel();
        let owner = LiveMempoolOwner::default();
        let (source, feed) = MockMempoolSource::streaming();
        let (signal, _gate) = mempool_ready_channel();
        let ingest = CanonicalIngestControlGrpcAdapter::new(
            network,
            writer.clone(),
            owner.clone(),
            Arc::new(self.source.clone()),
            readiness.clone(),
        );
        let ingest_listener = TcpListener::bind("127.0.0.1:0").await?;
        let ingest_endpoint = format!("http://{}", ingest_listener.local_addr()?);
        let wallet_listener = TcpListener::bind("127.0.0.1:0").await?;
        let wallet_endpoint = format!("http://{}", wallet_listener.local_addr()?);
        let serve_wallet = async {
            let admitted = AdmittedIngestControl::connect(&ingest_endpoint, None, network).await?;
            let query =
                WalletServingQuery::from_admitted_native_serving_pair(self.slot.clone(), admitted);
            let wallet = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());
            Server::builder()
                .add_service(wallet.into_server())
                .serve_with_incoming(TcpListenerStream::new(wallet_listener))
                .await?;
            Err("wallet gRPC server stopped".into())
        };
        let requests = async {
            while feed.open_count() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            for entry in entries {
                feed.push_added(entry)?;
            }
            feed.complete_initial_snapshot(tip)?;
            read(RemoteChainIndex::connect(RemoteOpenOptions {
                endpoint: wallet_endpoint,
                network,
            })?)
            .await
        };
        let serve_ingest = Server::builder()
            .add_service(ingest.into_server())
            .serve_with_incoming(TcpListenerStream::new(ingest_listener));
        let follow = Box::pin(follow_canonical_tip_with_control(store, follower, commands));
        let hydrate =
            run_live_mempool_owner(Arc::new(source), writer, owner, signal, cancel.clone());
        tokio::select! {
            outcome = tokio::time::timeout(Duration::from_secs(10), requests) => outcome?,
            outcome = Box::pin(serve_wallet) => outcome,
            outcome = serve_ingest => {
                outcome?;
                Err("ingest gRPC server stopped".into())
            }
            outcome = follow => {
                outcome?;
                Err("canonical follower stopped".into())
            }
            () = hydrate => {
                Err("mempool owner stopped".into())
            }
        }
    }
}
