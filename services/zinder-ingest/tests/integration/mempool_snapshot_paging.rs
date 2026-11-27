//! Native `WalletQuery.MempoolSnapshot` paging over real local gRPC within tonic's 4 MiB decode limit.

use std::{future::Future, sync::Arc, time::Duration};

use eyre::eyre;
use prost::Message as _;
use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, transport::Server};
use tonic_types::StatusExt as _;
use zebra_chain::{serialization::ZcashDeserialize as _, transaction::Transaction};
use zinder_core::{
    AuthDigest, BlockId, Network, NetworkUpgradeActivations, RawTransactionBytes, TransactionId,
    UnixTimestampMillis, wire::decode_rpc_transaction_id_hex,
};
use zinder_ingest::{
    CANONICAL_WRITER_MAX_MEMPOOL_SNAPSHOT_RESPONSE_BYTES, CanonicalIngestControlGrpcAdapter,
    LiveMempoolOwner, canonical_control_channel, follow_canonical_tip_with_control,
    mempool_ready_channel, run_live_mempool_owner,
};
use zinder_proto::{
    ZINDER_ERROR_DOMAIN,
    v1::{
        ops::ErrorReason,
        wallet::{
            self, MempoolEventsRequest, MempoolSnapshotRequest, mempool_event_envelope,
            wallet_query_client::WalletQueryClient,
        },
    },
};
use zinder_query::{
    AdmittedIngestControl, RocksDbCanonicalReader, RocksDbWalletReader, WalletEndpointMetadata,
    WalletQueryGrpcAdapter, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_runtime::{ComposedReadiness, Readiness};
use zinder_source::MempoolSourceEntry;
use zinder_store::{EventStreamStartPosition, RocksDbCanonicalStore, event_stream_start_message};
use zinder_testkit::{MockMempoolSource, MockMempoolSourceControl, WalletServingStoreFixture};

use super::canonical_follow::{
    RecordingParseableSource, follower_fixture_at_its_node_tip,
    follower_with_default_request_timeout,
};

type TestOutcome<T> = eyre::Result<T>;

const TONIC_DEFAULT_MAX_DECODING_MESSAGE_BYTES: usize = 4 * 1_024 * 1_024;
const LARGE_SNAPSHOT_ENTRY_COUNT: u32 = 600;
const NINE_KIB_SCRIPT_BYTES: usize = 9 * 1_024;
const NEAR_TWO_MIB_UNLOCK_SCRIPT_BYTES: usize = 1_950_000;
const OVERSIZED_LOCK_SCRIPT_BYTES: usize = 1_500_000;

#[tokio::test(flavor = "multi_thread")]
async fn wallet_query_mempool_snapshot_pages_past_four_mib_and_anchors_events() -> TestOutcome<()> {
    let entries = (0..LARGE_SNAPSHOT_ENTRY_COUNT)
        .map(|sequence| synthetic_source_entry(sequence, NINE_KIB_SCRIPT_BYTES, 25))
        .collect::<TestOutcome<Vec<_>>>()?;
    let post_snapshot_entry =
        synthetic_source_entry(LARGE_SNAPSHOT_ENTRY_COUNT + 1, NINE_KIB_SCRIPT_BYTES, 25)?;
    let mut expected_ids = entries
        .iter()
        .map(|entry| entry.transaction_id)
        .collect::<Vec<_>>();
    expected_ids.sort_unstable();

    with_live_wallet_grpc(entries, async |mut client, feed| {
        let walk = collect_all_snapshot_pages(&mut client, 1_024).await?;
        assert!(
            walk.page_count >= 2,
            "600 x 9 KiB entries must split across multiple byte-bounded pages, got {}",
            walk.page_count
        );
        assert_eq!(walk.transaction_ids, expected_ids);
        assert_ne!(walk.events_resume_cursor, [] as [u8; 0]);

        let mut events = client
            .mempool_events(Request::new(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::AfterCursor(
                        zinder_store::StreamCursorTokenV1::from_bytes(walk.events_resume_cursor),
                    ),
                )),
            }))
            .await?
            .into_inner();
        let post_snapshot_id = post_snapshot_entry.transaction_id;
        feed.push_added(post_snapshot_entry)?;
        let observed = receive_added_after_cursor(&mut events, post_snapshot_id).await?;
        assert_eq!(observed, post_snapshot_id);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn wallet_query_mempool_snapshot_serves_near_two_mib_single_entry_pages() -> TestOutcome<()> {
    let entries = vec![
        synthetic_source_entry(1, NINE_KIB_SCRIPT_BYTES, 25)?,
        synthetic_source_entry(2, NEAR_TWO_MIB_UNLOCK_SCRIPT_BYTES, 25)?,
        synthetic_source_entry(3, NEAR_TWO_MIB_UNLOCK_SCRIPT_BYTES, 25)?,
    ];
    let mut expected_ids = entries
        .iter()
        .map(|entry| entry.transaction_id)
        .collect::<Vec<_>>();
    expected_ids.sort_unstable();

    with_live_wallet_grpc(entries, |mut client, _feed| async move {
        let walk = collect_all_snapshot_pages(&mut client, 1_024).await?;
        assert!(
            walk.page_count >= 2,
            "two ~1.95 MiB entries must split across pages, got {}",
            walk.page_count
        );
        assert_eq!(walk.transaction_ids, expected_ids);
        assert!(walk.largest_raw_tx_bytes >= NEAR_TWO_MIB_UNLOCK_SCRIPT_BYTES);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn wallet_query_mempool_snapshot_rejects_oversized_entry_with_typed_reason() -> TestOutcome<()>
{
    let oversized = synthetic_source_entry(99, 32, OVERSIZED_LOCK_SCRIPT_BYTES)?;
    with_live_wallet_grpc(vec![oversized], |mut client, _feed| async move {
        let status = client
            .mempool_snapshot(Request::new(MempoolSnapshotRequest {
                max_entries: 1_024,
                from_cursor: Vec::new(),
            }))
            .await
            .err()
            .ok_or_else(|| eyre!("an entry whose encoded response exceeds 4 MiB must fail"))?;
        assert_unsupported_wallet_encoding_status(&status)?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn wallet_query_mempool_snapshot_flushes_preceding_entry_before_oversized_page_failure()
-> TestOutcome<()> {
    let (normal_entry, oversized_entry) = ordered_normal_then_oversized_entries()?;
    let expected_normal_id = normal_entry.transaction_id;

    with_live_wallet_grpc(
        vec![normal_entry, oversized_entry],
        |mut client, _feed| async move {
            let first_page = client
                .mempool_snapshot(Request::new(MempoolSnapshotRequest {
                    max_entries: 1_024,
                    from_cursor: Vec::new(),
                }))
                .await?
                .into_inner();
            assert!(
                first_page.encoded_len() <= CANONICAL_WRITER_MAX_MEMPOOL_SNAPSHOT_RESPONSE_BYTES
            );
            assert_eq!(first_page.entries.len(), 1);
            assert_eq!(
                decode_rpc_transaction_id_hex(&first_page.entries[0].transaction_id)?,
                expected_normal_id
            );
            assert_ne!(first_page.next_cursor, [] as [u8; 0]);

            let second_status = client
                .mempool_snapshot(Request::new(MempoolSnapshotRequest {
                    max_entries: 1_024,
                    from_cursor: first_page.next_cursor,
                }))
                .await
                .err()
                .ok_or_else(|| eyre!("second snapshot page at oversized entry must fail"))?;
            assert_unsupported_wallet_encoding_status(&second_status)?;
            Ok(())
        },
    )
    .await
}

fn assert_unsupported_wallet_encoding_status(status: &tonic::Status) -> TestOutcome<()> {
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    let details = status.get_error_details();
    let error_detail = details
        .error_info()
        .ok_or_else(|| eyre!("oversized mempool entry refusal must carry ErrorInfo"))?;
    assert_eq!(error_detail.domain, ZINDER_ERROR_DOMAIN);
    assert_eq!(
        error_detail.reason,
        ErrorReason::UnsupportedWalletEncoding.as_str_name()
    );
    Ok(())
}

fn ordered_normal_then_oversized_entries() -> TestOutcome<(MempoolSourceEntry, MempoolSourceEntry)>
{
    let oversized = synthetic_source_entry(99, 32, OVERSIZED_LOCK_SCRIPT_BYTES)?;
    for sequence in 1..1_000 {
        if sequence == 99 {
            continue;
        }
        let candidate = synthetic_source_entry(sequence, NINE_KIB_SCRIPT_BYTES, 25)?;
        if candidate.transaction_id < oversized.transaction_id {
            return Ok((candidate, oversized));
        }
    }
    Err(eyre!(
        "failed to synthesize a normal mempool entry ordered before the oversized entry"
    ))
}

struct SnapshotWalkSummary {
    page_count: usize,
    transaction_ids: Vec<TransactionId>,
    events_resume_cursor: Vec<u8>,
    largest_raw_tx_bytes: usize,
}

async fn collect_all_snapshot_pages(
    client: &mut WalletQueryClient<tonic::transport::Channel>,
    max_entries: u32,
) -> TestOutcome<SnapshotWalkSummary> {
    let mut from_cursor = Vec::new();
    let mut page_count = 0_usize;
    let mut transaction_ids = Vec::new();
    let mut expected_resume_cursor: Option<Vec<u8>> = None;
    let mut largest_raw_tx_bytes = 0_usize;

    loop {
        let response = client
            .mempool_snapshot(Request::new(MempoolSnapshotRequest {
                max_entries,
                from_cursor,
            }))
            .await?
            .into_inner();
        let encoded_len = response.encoded_len();
        assert!(
            encoded_len <= CANONICAL_WRITER_MAX_MEMPOOL_SNAPSHOT_RESPONSE_BYTES,
            "snapshot page encoded_len {encoded_len} exceeded budget {CANONICAL_WRITER_MAX_MEMPOOL_SNAPSHOT_RESPONSE_BYTES}"
        );
        assert!(encoded_len < TONIC_DEFAULT_MAX_DECODING_MESSAGE_BYTES);
        assert_ne!(
            response.entries.len(),
            0,
            "non-empty mempool walk must never emit an empty page"
        );
        if let Some(prior_cursor) = &expected_resume_cursor {
            assert_eq!(&response.events_resume_cursor, prior_cursor);
        } else {
            expected_resume_cursor = Some(response.events_resume_cursor.clone());
        }
        for entry in &response.entries {
            largest_raw_tx_bytes = largest_raw_tx_bytes.max(entry.raw_transaction_bytes.len());
            transaction_ids.push(decode_rpc_transaction_id_hex(&entry.transaction_id)?);
        }
        page_count = page_count.saturating_add(1);
        if response.next_cursor.is_empty() {
            return Ok(SnapshotWalkSummary {
                page_count,
                transaction_ids,
                events_resume_cursor: expected_resume_cursor.unwrap_or_default(),
                largest_raw_tx_bytes,
            });
        }
        from_cursor = response.next_cursor;
    }
}

async fn receive_added_after_cursor(
    events: &mut tonic::Streaming<wallet::MempoolEventEnvelope>,
    expected_id: TransactionId,
) -> TestOutcome<TransactionId> {
    loop {
        let envelope = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await?
            .ok_or_else(|| eyre!("mempool event stream ended before post-snapshot event"))??;
        if let Some(mempool_event_envelope::Event::Added(added)) = envelope.event {
            let entry = added
                .entry
                .ok_or_else(|| eyre!("Added event omitted entry"))?;
            let decoded_id = decode_rpc_transaction_id_hex(&entry.transaction_id)?;
            if decoded_id == expected_id {
                return Ok(decoded_id);
            }
        }
    }
}

fn synthetic_source_entry(
    sequence: u32,
    unlock_script_len: usize,
    lock_script_len: usize,
) -> TestOutcome<MempoolSourceEntry> {
    let mut previous_hash = [0_u8; 32];
    previous_hash[..4].copy_from_slice(&sequence.to_le_bytes());
    previous_hash[4] = 0xA5;
    let mut unlock_bytes = vec![0x51_u8; unlock_script_len];
    if unlock_bytes.len() >= 4 {
        unlock_bytes[..4].copy_from_slice(&sequence.to_be_bytes());
    }
    let mut lock_bytes = vec![0x76_u8; lock_script_len];
    if lock_bytes.len() >= 4 {
        lock_bytes[..4].copy_from_slice(&sequence.to_le_bytes());
    }
    let mut raw_bytes = Vec::with_capacity(64 + unlock_script_len + lock_script_len);
    raw_bytes.extend_from_slice(&1_u32.to_le_bytes());
    raw_bytes.push(1);
    raw_bytes.extend_from_slice(&previous_hash);
    raw_bytes.extend_from_slice(&sequence.to_le_bytes());
    append_compact_size(&mut raw_bytes, unlock_bytes.len())?;
    raw_bytes.extend_from_slice(&unlock_bytes);
    raw_bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    raw_bytes.push(1);
    raw_bytes.extend_from_slice(&(10_000_u64 + u64::from(sequence)).to_le_bytes());
    append_compact_size(&mut raw_bytes, lock_bytes.len())?;
    raw_bytes.extend_from_slice(&lock_bytes);
    raw_bytes.extend_from_slice(&0_u32.to_le_bytes());
    let transaction = Transaction::zcash_deserialize(raw_bytes.as_slice())?;
    Ok(MempoolSourceEntry {
        transaction_id: TransactionId::from_bytes(transaction.hash().0),
        auth_digest: transaction
            .auth_digest()
            .map(|digest| AuthDigest::from_bytes(digest.0)),
        raw_transaction_bytes: RawTransactionBytes::new(raw_bytes),
        observed_at_unix_millis: UnixTimestampMillis::new(
            1_750_000_000_000_u64.saturating_add(u64::from(sequence)),
        ),
    })
}

fn append_compact_size(buffer: &mut Vec<u8>, len: usize) -> TestOutcome<()> {
    if let Ok(short) = u8::try_from(len)
        && short <= 0xfc
    {
        buffer.push(short);
    } else if let Ok(medium) = u16::try_from(len) {
        buffer.push(0xfd);
        buffer.extend_from_slice(&medium.to_le_bytes());
    } else {
        let wide = u32::try_from(len)?;
        buffer.push(0xfe);
        buffer.extend_from_slice(&wide.to_le_bytes());
    }
    Ok(())
}

async fn with_live_wallet_grpc<F, Fut>(
    initial_entries: Vec<MempoolSourceEntry>,
    run_scenario: F,
) -> TestOutcome<()>
where
    F: FnOnce(WalletQueryClient<tonic::transport::Channel>, MockMempoolSourceControl) -> Fut + Send,
    Fut: Future<Output = TestOutcome<()>> + Send,
{
    let (node_source, activations, store, temporary) = follower_fixture_at_its_node_tip()
        .await
        .map_err(|err| eyre!("{err}"))?;
    let canonical_path = temporary.path().join("canonical");
    let mut serving = WalletServingStoreFixture::from_published_canonical(
        temporary,
        &canonical_path,
        store,
        &activations,
    )?;
    let reopened = open_ready_canonical(&canonical_path, &activations)?;
    let tip = reopened.event_fence().visible_tip();
    let (canonical_reader, wallet_reader) = serving.take_readers()?;
    let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical_reader)?),
        Arc::new(RocksDbWalletReader::new(wallet_reader)),
    )?));
    Box::pin(run_wallet_grpc_stack(
        &node_source,
        &activations,
        reopened,
        slot,
        (tip, initial_entries, run_scenario),
    ))
    .await
}

fn open_ready_canonical(
    path: &std::path::Path,
    activations: &NetworkUpgradeActivations,
) -> TestOutcome<RocksDbCanonicalStore> {
    Ok(RocksDbCanonicalStore::open_ready(
        path,
        activations,
        zinder_store::RawBlobRetention::None,
        zinder_store::CanonicalReorgPolicy::new(4)?,
        zinder_store::RocksDbResourceBudget::for_local_tests(),
    )?)
}

async fn run_wallet_grpc_stack<F, Fut>(
    node_source: &RecordingParseableSource,
    activations: &Arc<NetworkUpgradeActivations>,
    store: RocksDbCanonicalStore,
    slot: WalletServingPairSlot,
    scenario: (BlockId, Vec<MempoolSourceEntry>, F),
) -> TestOutcome<()>
where
    F: FnOnce(WalletQueryClient<tonic::transport::Channel>, MockMempoolSourceControl) -> Fut + Send,
    Fut: Future<Output = TestOutcome<()>> + Send,
{
    let (tip, initial_entries, run_scenario) = scenario;
    let network = Network::ZcashRegtest;
    let cancel = CancellationToken::new();
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let follower =
        follower_with_default_request_timeout(node_source, activations, &readiness, &cancel);
    let (writer, commands) = canonical_control_channel();
    let owner = LiveMempoolOwner::default();
    let (mempool_source, feed) = MockMempoolSource::streaming();
    let (signal, gate) = mempool_ready_channel();
    let ingest = CanonicalIngestControlGrpcAdapter::new(
        network,
        writer.clone(),
        owner.clone(),
        Arc::new(node_source.clone()),
        readiness.clone(),
    );
    let ingest_listener = TcpListener::bind("127.0.0.1:0").await?;
    let ingest_endpoint = format!("http://{}", ingest_listener.local_addr()?);
    let wallet_listener = TcpListener::bind("127.0.0.1:0").await?;
    let wallet_endpoint = format!("http://{}", wallet_listener.local_addr()?);

    let serve_ingest = tokio::spawn(
        Server::builder()
            .add_service(ingest.into_server())
            .serve_with_incoming(TcpListenerStream::new(ingest_listener)),
    );
    let follow = Box::pin(follow_canonical_tip_with_control(store, follower, commands));
    let hydrate = tokio::spawn(run_live_mempool_owner(
        Arc::new(mempool_source),
        writer,
        owner,
        signal,
        cancel.clone(),
    ));
    let drive = async {
        while feed.open_count() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for entry in initial_entries {
            feed.push_added(entry)?;
        }
        feed.complete_initial_snapshot(tip)?;
        while !gate.admits_canonical_tip(tip) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let admitted = AdmittedIngestControl::connect(&ingest_endpoint, None, network).await?;
        let query = WalletServingQuery::from_admitted_native_serving_pair(slot, admitted);
        let wallet_adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());
        let wallet_server = tokio::spawn(
            Server::builder()
                .add_service(wallet_adapter.into_server())
                .serve_with_incoming(TcpListenerStream::new(wallet_listener)),
        );
        let client = WalletQueryClient::connect(wallet_endpoint).await?;
        let outcome = Box::pin(run_scenario(client, feed)).await;
        wallet_server.abort();
        outcome
    };

    let outcome = tokio::select! {
        outcome = tokio::time::timeout(Duration::from_secs(30), Box::pin(drive)) => outcome?,
        outcome = follow => {
            outcome?;
            Err(eyre!("canonical follower stopped unexpectedly"))
        }
    };
    cancel.cancel();
    let _ = hydrate.await;
    serve_ingest.abort();
    outcome
}
