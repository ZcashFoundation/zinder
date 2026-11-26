//! Explorer pagination and aggregation across byte-split `WalletQuery.MempoolSnapshot` pages.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use eyre::{Result, eyre};
use prost::Message as _;
use tokio::{net::TcpListener, sync::Mutex};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Response, Status, transport::Channel};
use tonic_types::StatusExt as _;
use zebra_chain::{serialization::ZcashDeserialize as _, transaction::Transaction};
use zinder_core::{
    BlockHeight, Network, NetworkUpgradeActivations,
    wire::{encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex},
};
use zinder_explorer::{ExplorerQueryGrpcAdapter, ExplorerServerInfoSettings};
use zinder_materialized_views::{
    BLOCK_SUMMARY_COLUMN_FAMILY, BLOCK_SUMMARY_CONSUMER_NAME, BlockSummaryConsumer,
    MaterializedViewStore, MaterializedViewStoreOptions,
};
use zinder_proto::{
    ZINDER_ERROR_DOMAIN,
    v1::{
        explorer::{
            BlockSummary, BlockSummaryRecord, MempoolActivityRequest, MempoolSnapshotRequest,
            MempoolSummaryRequest, OverviewSnapshotRequest,
            explorer_query_client::ExplorerQueryClient,
        },
        ops::ErrorReason,
        wallet::{
            self, ChainValuePoolsAtTipRequest, ChainValuePoolsAtTipResponse,
            MempoolSnapshotRequest as WalletMempoolSnapshotRequest,
            MempoolSnapshotResponse as WalletMempoolSnapshotResponse, VisibleTipBlockRequest,
            VisibleTipBlockResponse,
            wallet_query_server::{WalletQuery, WalletQueryServer},
        },
    },
};
use zinder_source::parse_transaction_public_facts;
use zinder_testkit::ChainFixture;

const NINE_KIB_SCRIPT_BYTES: usize = 9 * 1_024;
const PAGE_ONE_ENTRIES: u32 = 250;
const PAGE_TWO_ENTRIES: u32 = 250;
const PAGE_THREE_ENTRIES: u32 = 100;
const TOTAL_BYTE_SPLIT_ENTRIES: u32 = PAGE_ONE_ENTRIES + PAGE_TWO_ENTRIES + PAGE_THREE_ENTRIES;

#[tokio::test(flavor = "multi_thread")]
async fn explorer_drains_byte_split_wallet_mempool_across_overview_summary_snapshot_and_activity()
-> Result<()> {
    let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);
    let page_one = build_wallet_entries(0, PAGE_ONE_ENTRIES, 1_750_000_000_000, &activations)?;
    let page_two = build_wallet_entries(
        PAGE_ONE_ENTRIES,
        PAGE_TWO_ENTRIES,
        1_750_000_100_000,
        &activations,
    )?;
    let page_three = build_wallet_entries(
        PAGE_ONE_ENTRIES + PAGE_TWO_ENTRIES,
        PAGE_THREE_ENTRIES,
        1_750_000_200_000,
        &activations,
    )?;
    let expected_total_bytes: u64 = page_one
        .iter()
        .chain(page_two.iter())
        .chain(page_three.iter())
        .map(|entry| u64::try_from(entry.raw_transaction_bytes.len()).unwrap_or(u64::MAX))
        .sum();
    let aggregate_wire_bytes = encoded_page_bytes(1, &page_one, &[1])
        + encoded_page_bytes(1, &page_two, &[2])
        + encoded_page_bytes(1, &page_three, &[]);
    assert!(
        aggregate_wire_bytes > 4 * 1_024 * 1_024,
        "3 pages must exceed tonic's 4 MiB single-message limit in aggregate ({aggregate_wire_bytes} bytes)"
    );
    let expected_newest_ids = globally_sorted_txids(&[&page_one, &page_two, &page_three]);

    let wallet_service = ScriptedWalletQuery::three_page_snapshot(page_one, page_two, page_three);
    let recorded_max_entries = wallet_service.recorded_max_entries.clone();
    let (mut client, guard) = spawn_explorer_over_scripted_wallet(wallet_service).await?;

    assert_overview_and_summary_totals(&mut client, expected_total_bytes).await?;
    assert_eq!(
        recorded_max_entries.lock().await.as_slice(),
        &[4_096, 3_846, 3_596, 4_096, 3_846, 3_596],
        "each continuation page must request the remaining 4,096-entry budget"
    );

    assert_snapshot_and_activity_global_ordering(
        &mut client,
        expected_total_bytes,
        &expected_newest_ids,
    )
    .await?;

    guard.shutdown().await;
    Ok(())
}

async fn assert_overview_and_summary_totals(
    client: &mut ExplorerQueryClient<Channel>,
    expected_total_bytes: u64,
) -> Result<()> {
    let overview = client
        .overview_snapshot(OverviewSnapshotRequest {
            recent_blocks_limit: 0,
            recent_transactions_limit: 0,
            mempool_window_seconds: 0,
            fee_summary_block_count: 0,
        })
        .await?
        .into_inner();
    let overview_mempool = overview
        .mempool
        .ok_or_else(|| eyre!("overview missing mempool summary"))?;
    assert_eq!(overview_mempool.transaction_count, TOTAL_BYTE_SPLIT_ENTRIES);
    assert_eq!(overview_mempool.total_size_bytes, expected_total_bytes);
    assert!(
        overview_mempool.oldest_entry_age_millis
            >= overview_mempool.newest_entry_age_millis + 199_000,
        "overview ages must span page 1 oldest and page 3 newest entries: {overview_mempool:?}"
    );

    let summary = client
        .mempool_summary(MempoolSummaryRequest::default())
        .await?
        .into_inner();
    assert_eq!(summary.transaction_count, TOTAL_BYTE_SPLIT_ENTRIES);
    assert_eq!(summary.total_size_bytes, expected_total_bytes);
    let shape_total: u32 = summary
        .privacy_shape_distribution
        .iter()
        .map(|bucket| bucket.count)
        .sum();
    assert_eq!(shape_total, TOTAL_BYTE_SPLIT_ENTRIES);
    Ok(())
}

async fn assert_snapshot_and_activity_global_ordering(
    client: &mut ExplorerQueryClient<Channel>,
    expected_total_bytes: u64,
    expected_newest_ids: &[String],
) -> Result<()> {
    let snap_first = client
        .mempool_snapshot(MempoolSnapshotRequest {
            max_entries: 120,
            from_cursor: Vec::new(),
        })
        .await?
        .into_inner();
    let snap_summary = snap_first
        .summary
        .ok_or_else(|| eyre!("MempoolSnapshot missing summary"))?;
    assert_eq!(snap_summary.transaction_count, TOTAL_BYTE_SPLIT_ENTRIES);
    assert_eq!(snap_summary.total_size_bytes, expected_total_bytes);
    assert_eq!(snap_first.entries.len(), 120);
    let first_ids: Vec<String> = snap_first
        .entries
        .iter()
        .map(|row| row.transaction_id.clone())
        .collect();
    assert_eq!(first_ids.as_slice(), &expected_newest_ids[..120]);

    let snap_second = client
        .mempool_snapshot(MempoolSnapshotRequest {
            max_entries: 120,
            from_cursor: snap_first.next_cursor,
        })
        .await?
        .into_inner();
    let second_ids: Vec<String> = snap_second
        .entries
        .iter()
        .map(|row| row.transaction_id.clone())
        .collect();
    assert_eq!(second_ids.as_slice(), &expected_newest_ids[120..240]);

    let mut activity_ids = Vec::with_capacity(expected_newest_ids.len());
    let mut from_cursor = Vec::new();
    loop {
        let activity_page = client
            .mempool_activity(MempoolActivityRequest {
                max_entries: 256,
                from_cursor,
            })
            .await?
            .into_inner();
        if activity_page.entries.is_empty() {
            break;
        }
        for row in activity_page.entries {
            activity_ids.push(row.transaction_id);
        }
        if activity_page.next_cursor.is_empty() {
            break;
        }
        from_cursor = activity_page.next_cursor;
    }
    assert_eq!(activity_ids, expected_newest_ids);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn explorer_restarts_on_mid_walk_epoch_change_and_rejects_stalled_or_churning_walks()
-> Result<()> {
    let activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);
    let page_one = build_wallet_entries(0, 3, 1_750_000_000_000, &activations)?;
    let page_two = build_wallet_entries(3, 2, 1_750_000_010_000, &activations)?;

    // 1. A single mid-walk cursor expiry restarts cleanly from `from_cursor = []` without double counting.
    let recovering_wallet =
        ScriptedWalletQuery::recovering_after_cursor_expiry(page_one.clone(), page_two.clone());
    let (mut client, guard) = spawn_explorer_over_scripted_wallet(recovering_wallet).await?;
    let summary = client
        .mempool_summary(MempoolSummaryRequest::default())
        .await?
        .into_inner();
    assert_eq!(summary.transaction_count, 5);
    guard.shutdown().await;

    // 2. Perpetual epoch churn across all 3 attempts surfaces `UNAVAILABLE`.
    let churning_wallet = ScriptedWalletQuery::perpetual_epoch_mismatch(page_one);
    let (mut churn_client, churn_guard) =
        spawn_explorer_over_scripted_wallet(churning_wallet).await?;
    let churn_status = churn_client
        .mempool_summary(MempoolSummaryRequest::default())
        .await
        .err()
        .ok_or_else(|| eyre!("expected UNAVAILABLE after exhausted pagination restarts"))?;
    assert_eq!(churn_status.code(), Code::Unavailable);
    assert_eq!(
        churn_status
            .get_error_details()
            .error_info()
            .map(|detail| (detail.domain.clone(), detail.reason.clone())),
        Some((
            ZINDER_ERROR_DOMAIN.to_owned(),
            ErrorReason::UpstreamUnreachable.as_str_name().to_owned(),
        )),
    );
    churn_guard.shutdown().await;

    // 3. An empty page carrying a non-empty `next_cursor` fails closed with `INTERNAL`.
    let stalled_wallet = ScriptedWalletQuery::stalled_empty_cursor();
    let (mut stalled_client, stalled_guard) =
        spawn_explorer_over_scripted_wallet(stalled_wallet).await?;
    let stalled_status = stalled_client
        .mempool_summary(MempoolSummaryRequest::default())
        .await
        .err()
        .ok_or_else(|| eyre!("expected INTERNAL on empty page with non-empty next_cursor"))?;
    assert_eq!(stalled_status.code(), Code::Internal);
    stalled_guard.shutdown().await;
    Ok(())
}

#[derive(Clone)]
enum WalletSnapshotBehavior {
    ThreePages([Vec<wallet::MempoolEntry>; 3]),
    ExpireOnceThenTwoPages {
        expired_once: Arc<AtomicBool>,
        pages: [Vec<wallet::MempoolEntry>; 2],
    },
    PerpetualEpochMismatch(Vec<wallet::MempoolEntry>),
    StalledEmptyCursor,
}

#[derive(Clone)]
struct ScriptedWalletQuery {
    behavior: WalletSnapshotBehavior,
    recorded_max_entries: Arc<Mutex<Vec<u32>>>,
}

impl ScriptedWalletQuery {
    fn three_page_snapshot(
        first: Vec<wallet::MempoolEntry>,
        second: Vec<wallet::MempoolEntry>,
        third: Vec<wallet::MempoolEntry>,
    ) -> Self {
        Self {
            behavior: WalletSnapshotBehavior::ThreePages([first, second, third]),
            recorded_max_entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn recovering_after_cursor_expiry(
        first: Vec<wallet::MempoolEntry>,
        second: Vec<wallet::MempoolEntry>,
    ) -> Self {
        Self {
            behavior: WalletSnapshotBehavior::ExpireOnceThenTwoPages {
                expired_once: Arc::new(AtomicBool::new(false)),
                pages: [first, second],
            },
            recorded_max_entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn perpetual_epoch_mismatch(entries: Vec<wallet::MempoolEntry>) -> Self {
        Self {
            behavior: WalletSnapshotBehavior::PerpetualEpochMismatch(entries),
            recorded_max_entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn stalled_empty_cursor() -> Self {
        Self {
            behavior: WalletSnapshotBehavior::StalledEmptyCursor,
            recorded_max_entries: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

type EmptyWalletStream<T> =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send + 'static>>;

fn unused_wallet_method<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented(
        "unused in explorer mempool pagination test",
    ))
}

#[tonic::async_trait]
impl WalletQuery for ScriptedWalletQuery {
    type CompactBlocksInRangeStream = EmptyWalletStream<wallet::CompactBlocksInRangeChunk>;
    type FullBlocksInRangeStream = EmptyWalletStream<wallet::FullBlocksInRangeChunk>;
    type ChainEventsStream = EmptyWalletStream<wallet::ChainEventEnvelope>;
    type MempoolEventsStream = EmptyWalletStream<wallet::MempoolEventEnvelope>;
    type TransparentAddressUnspentOutputsStream =
        EmptyWalletStream<wallet::TransparentUnspentOutputsChunk>;
    type TransparentAddressTxIdsInRangeStream =
        EmptyWalletStream<wallet::TransparentAddressTxIdsChunk>;

    async fn settled_tip_block(
        &self,
        _request: Request<wallet::SettledTipBlockRequest>,
    ) -> Result<Response<wallet::SettledTipBlockResponse>, Status> {
        unused_wallet_method()
    }

    async fn block_id_by_selector(
        &self,
        _request: Request<wallet::BlockSelectorRequest>,
    ) -> Result<Response<wallet::BlockIdResponse>, Status> {
        unused_wallet_method()
    }

    async fn block_header_by_selector(
        &self,
        _request: Request<wallet::BlockSelectorRequest>,
    ) -> Result<Response<wallet::BlockHeaderResponse>, Status> {
        unused_wallet_method()
    }

    async fn compact_block(
        &self,
        _request: Request<wallet::CompactBlockRequest>,
    ) -> Result<Response<wallet::CompactBlockResponse>, Status> {
        unused_wallet_method()
    }

    async fn compact_blocks_in_range(
        &self,
        _request: Request<wallet::CompactBlocksInRangeRequest>,
    ) -> Result<Response<Self::CompactBlocksInRangeStream>, Status> {
        unused_wallet_method()
    }

    async fn full_block(
        &self,
        _request: Request<wallet::FullBlockRequest>,
    ) -> Result<Response<wallet::FullBlockResponse>, Status> {
        unused_wallet_method()
    }

    async fn full_blocks_in_range(
        &self,
        _request: Request<wallet::FullBlocksInRangeRequest>,
    ) -> Result<Response<Self::FullBlocksInRangeStream>, Status> {
        unused_wallet_method()
    }

    async fn transaction(
        &self,
        _request: Request<wallet::TransactionRequest>,
    ) -> Result<Response<wallet::TransactionStatusResponse>, Status> {
        unused_wallet_method()
    }

    async fn tree_state_at_height(
        &self,
        _request: Request<wallet::TreeStateAtHeightRequest>,
    ) -> Result<Response<wallet::TreeStateResponse>, Status> {
        unused_wallet_method()
    }

    async fn latest_tree_state_checkpoint(
        &self,
        _request: Request<wallet::LatestTreeStateCheckpointRequest>,
    ) -> Result<Response<wallet::TreeStateResponse>, Status> {
        unused_wallet_method()
    }

    async fn subtree_roots(
        &self,
        _request: Request<wallet::SubtreeRootsRequest>,
    ) -> Result<Response<wallet::SubtreeRootsResponse>, Status> {
        unused_wallet_method()
    }

    async fn broadcast_transaction(
        &self,
        _request: Request<wallet::BroadcastTransactionRequest>,
    ) -> Result<Response<wallet::BroadcastTransactionResponse>, Status> {
        unused_wallet_method()
    }

    async fn chain_events(
        &self,
        _request: Request<wallet::ChainEventsRequest>,
    ) -> Result<Response<Self::ChainEventsStream>, Status> {
        unused_wallet_method()
    }

    async fn mempool_events(
        &self,
        _request: Request<wallet::MempoolEventsRequest>,
    ) -> Result<Response<Self::MempoolEventsStream>, Status> {
        unused_wallet_method()
    }

    async fn transparent_address_unspent_outputs(
        &self,
        _request: Request<wallet::TransparentAddressUnspentOutputsRequest>,
    ) -> Result<Response<Self::TransparentAddressUnspentOutputsStream>, Status> {
        unused_wallet_method()
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        _request: Request<wallet::TransparentAddressTxIdsInRangeRequest>,
    ) -> Result<Response<Self::TransparentAddressTxIdsInRangeStream>, Status> {
        unused_wallet_method()
    }

    async fn transparent_outpoint_spend_status(
        &self,
        _request: Request<wallet::TransparentOutpointSpendStatusRequest>,
    ) -> Result<Response<wallet::TransparentOutpointSpendStatusResponse>, Status> {
        unused_wallet_method()
    }

    async fn server_info(
        &self,
        _request: Request<wallet::ServerInfoRequest>,
    ) -> Result<Response<wallet::ServerInfoResponse>, Status> {
        unused_wallet_method()
    }

    async fn network_upgrade_activations(
        &self,
        _request: Request<wallet::NetworkUpgradeActivationsRequest>,
    ) -> Result<Response<wallet::NetworkUpgradeActivationsResponse>, Status> {
        unused_wallet_method()
    }

    async fn visible_tip_block(
        &self,
        _request: Request<VisibleTipBlockRequest>,
    ) -> Result<Response<VisibleTipBlockResponse>, Status> {
        Ok(Response::new(VisibleTipBlockResponse {
            chain_view: Some(test_chain_view(1)),
            visible_tip_block: Some(wallet::BlockId {
                height: 1,
                block_hash: "11".repeat(32),
            }),
        }))
    }

    async fn chain_value_pools_at_tip(
        &self,
        _request: Request<ChainValuePoolsAtTipRequest>,
    ) -> Result<Response<ChainValuePoolsAtTipResponse>, Status> {
        Ok(Response::new(ChainValuePoolsAtTipResponse {
            chain_view: Some(test_chain_view(1)),
            pools: Vec::new(),
            source_tip: Some(test_block_tip()),
        }))
    }

    async fn mempool_snapshot(
        &self,
        request: Request<WalletMempoolSnapshotRequest>,
    ) -> Result<Response<WalletMempoolSnapshotResponse>, Status> {
        let inner = request.into_inner();
        self.recorded_max_entries
            .lock()
            .await
            .push(inner.max_entries);
        match &self.behavior {
            WalletSnapshotBehavior::ThreePages(pages) => match inner.from_cursor.as_slice() {
                [] => Ok(Response::new(snapshot_response(
                    1,
                    pages[0].clone(),
                    vec![1],
                ))),
                [1] => Ok(Response::new(snapshot_response(
                    1,
                    pages[1].clone(),
                    vec![2],
                ))),
                [2] => Ok(Response::new(snapshot_response(
                    1,
                    pages[2].clone(),
                    Vec::new(),
                ))),
                other => Err(Status::invalid_argument(format!(
                    "unexpected cursor {other:?}"
                ))),
            },
            WalletSnapshotBehavior::ExpireOnceThenTwoPages {
                expired_once,
                pages,
            } => match inner.from_cursor.as_slice() {
                [] => Ok(Response::new(snapshot_response(
                    2,
                    pages[0].clone(),
                    vec![1],
                ))),
                [1] => {
                    if !expired_once.swap(true, Ordering::SeqCst) {
                        return Err(zinder_proto::status_for_reason(
                            ErrorReason::SnapshotPageCursorExpired,
                            "snapshot page cursor expired",
                        ));
                    }
                    Ok(Response::new(snapshot_response(
                        2,
                        pages[1].clone(),
                        Vec::new(),
                    )))
                }
                other => Err(Status::invalid_argument(format!(
                    "unexpected cursor {other:?}"
                ))),
            },
            WalletSnapshotBehavior::PerpetualEpochMismatch(entries) => {
                match inner.from_cursor.as_slice() {
                    [] => Ok(Response::new(snapshot_response(
                        1,
                        entries.clone(),
                        vec![1],
                    ))),
                    [1] => Ok(Response::new(snapshot_response(
                        2,
                        entries.clone(),
                        Vec::new(),
                    ))),
                    other => Err(Status::invalid_argument(format!(
                        "unexpected cursor {other:?}"
                    ))),
                }
            }
            WalletSnapshotBehavior::StalledEmptyCursor => {
                Ok(Response::new(snapshot_response(1, Vec::new(), vec![9])))
            }
        }
    }
}

struct RunningServers {
    _tempdir: tempfile::TempDir,
    explorer_handle: tokio::task::JoinHandle<std::result::Result<(), tonic::transport::Error>>,
    wallet_handle: tokio::task::JoinHandle<std::result::Result<(), tonic::transport::Error>>,
}

impl RunningServers {
    async fn shutdown(self) {
        self.explorer_handle.abort();
        self.wallet_handle.abort();
        let _ = self.explorer_handle.await;
        let _ = self.wallet_handle.await;
    }
}

async fn spawn_explorer_over_scripted_wallet(
    wallet_service: ScriptedWalletQuery,
) -> Result<(ExplorerQueryClient<Channel>, RunningServers)> {
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (tempdir, secondary_store) = open_seeded_materialized_view_store(&chain_fixture)?;

    let wallet_listener = TcpListener::bind("127.0.0.1:0").await?;
    let wallet_addr = wallet_listener.local_addr()?;
    let wallet_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(WalletQueryServer::new(wallet_service))
            .serve_with_incoming(TcpListenerStream::new(wallet_listener))
            .await
    });

    let explorer_listener = TcpListener::bind("127.0.0.1:0").await?;
    let explorer_addr = explorer_listener.local_addr()?;
    let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    })
    .with_materialized_view_store(secondary_store)
    .with_wallet_query_endpoint(format!("http://{wallet_addr}"))
    .with_prevout_resolution_online(true);
    let explorer_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(explorer_listener))
            .await
    });

    let channel = connect_loopback(explorer_addr).await?;
    Ok((
        ExplorerQueryClient::new(channel),
        RunningServers {
            _tempdir: tempdir,
            explorer_handle,
            wallet_handle,
        },
    ))
}

fn open_seeded_materialized_view_store(
    chain_fixture: &ChainFixture,
) -> Result<(tempfile::TempDir, MaterializedViewStore)> {
    let tempdir = tempfile::tempdir()?;
    let primary_path = tempdir.path().join("mv-primary");
    let secondary_path = tempdir.path().join("mv-secondary");
    let primary = MaterializedViewStore::open(
        &primary_path,
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?;
    let fixture_block = chain_fixture
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("fixture block 1 missing"))?;
    let record = BlockSummaryRecord {
        summary: Some(BlockSummary {
            block_height: 1,
            block_hash: encode_rpc_block_hash_hex(fixture_block.hash),
            block_time_unix_seconds: i64::from(fixture_block.block_time_seconds),
            transaction_count: 1,
            previous_block_hash: encode_rpc_block_hash_hex(fixture_block.parent_hash),
            total_size_bytes: u64::try_from(fixture_block.raw_block_bytes.len())?,
            confirmations: 1,
            is_canonical: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    primary.put_consumer(
        BLOCK_SUMMARY_COLUMN_FAMILY,
        &BlockSummaryConsumer::key_for_height(BlockHeight::new(1)),
        &record.encode_to_vec(),
    )?;
    primary.put_chain_event_cursor(BLOCK_SUMMARY_CONSUMER_NAME, &[1])?;

    let secondary = MaterializedViewStore::open_secondary(
        &primary_path,
        &secondary_path,
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?;
    secondary.try_catch_up()?;
    Ok((tempdir, secondary))
}

async fn connect_loopback(addr: SocketAddr) -> Result<Channel> {
    let endpoint = format!("http://{addr}");
    for _ in 0..20 {
        if let Ok(channel) = Channel::from_shared(endpoint.clone())?.connect().await {
            return Ok(channel);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(eyre!("server at {addr} did not accept connections"))
}

fn snapshot_response(
    epoch_id: u64,
    entries: Vec<wallet::MempoolEntry>,
    next_cursor: Vec<u8>,
) -> WalletMempoolSnapshotResponse {
    WalletMempoolSnapshotResponse {
        chain_view: Some(test_chain_view(epoch_id)),
        events_resume_cursor: Vec::new(),
        snapshot_age_millis: 25,
        entries,
        next_cursor,
        source_tip: Some(test_block_tip()),
    }
}

fn test_chain_view(epoch_id: u64) -> wallet::ChainView {
    wallet::ChainView {
        chain_epoch: Some(wallet::ChainEpoch {
            chain_epoch_id: epoch_id,
            network_name: "zcash-regtest".to_owned(),
            visible_tip: Some(test_block_tip()),
            settled_tip: Some(test_block_tip()),
            artifact_schema_version: 1,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn test_block_tip() -> wallet::BlockTip {
    wallet::BlockTip {
        height: 1,
        hash: "11".repeat(32),
    }
}

fn encoded_page_bytes(
    epoch_id: u64,
    entries: &[wallet::MempoolEntry],
    next_cursor: &[u8],
) -> usize {
    snapshot_response(epoch_id, entries.to_vec(), next_cursor.to_vec()).encoded_len()
}

fn globally_sorted_txids(pages: &[&[wallet::MempoolEntry]]) -> Vec<String> {
    let mut rows: Vec<((u64, u32), String)> = pages
        .iter()
        .flat_map(|page| page.iter())
        .map(|entry| {
            (
                (
                    entry.first_seen_unix_millis,
                    txid_tail(&entry.transaction_id),
                ),
                entry.transaction_id.clone(),
            )
        })
        .collect();
    rows.sort_by_key(|row| std::cmp::Reverse(row.0));
    rows.into_iter().map(|(_, txid)| txid).collect()
}

fn txid_tail(rpc_hex: &str) -> u32 {
    let Ok(decoded) = zinder_core::wire::decode_rpc_transaction_id_hex(rpc_hex) else {
        return 0;
    };
    let bytes = decoded.as_bytes();
    let mut tail = [0_u8; 4];
    tail.copy_from_slice(&bytes[bytes.len() - 4..]);
    u32::from_be_bytes(tail)
}

fn build_wallet_entries(
    start_sequence: u32,
    count: u32,
    base_first_seen_millis: u64,
    activations: &NetworkUpgradeActivations,
) -> Result<Vec<wallet::MempoolEntry>> {
    let mut entries = Vec::with_capacity(usize::try_from(count)?);
    for offset in 0..count {
        let sequence = start_sequence + offset;
        let raw_transaction_bytes = synthetic_raw_transaction(sequence, NINE_KIB_SCRIPT_BYTES)?;
        let facts = parse_transaction_public_facts(&raw_transaction_bytes, None, activations)?;
        let _parsed = Transaction::zcash_deserialize(raw_transaction_bytes.as_slice())?;
        entries.push(wallet::MempoolEntry {
            transaction_id: encode_rpc_transaction_id_hex(facts.transaction_id),
            auth_digest: String::new(),
            raw_transaction_bytes,
            compact_transaction_data: Some(wallet::CompactTransactionData::default()),
            first_seen_unix_millis: base_first_seen_millis + u64::from(offset),
            first_seen_chain_epoch: None,
            transparent_outputs: Vec::new(),
            transparent_spends: Vec::new(),
        });
    }
    Ok(entries)
}

fn synthetic_raw_transaction(sequence: u32, unlock_len: usize) -> Result<Vec<u8>> {
    let mut raw_bytes = Vec::with_capacity(64 + unlock_len);
    raw_bytes.extend_from_slice(&1_u32.to_le_bytes());
    raw_bytes.push(1);
    let mut prev_hash = [0xC3_u8; 32];
    prev_hash[..4].copy_from_slice(&sequence.to_le_bytes());
    raw_bytes.extend_from_slice(&prev_hash);
    raw_bytes.extend_from_slice(&sequence.to_le_bytes());
    let wide = u16::try_from(unlock_len)?;
    raw_bytes.push(0xfd);
    raw_bytes.extend_from_slice(&wide.to_le_bytes());
    raw_bytes.extend(std::iter::repeat_n(0x51_u8, unlock_len));
    raw_bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    raw_bytes.push(1);
    raw_bytes.extend_from_slice(&(5_000_u64 + u64::from(sequence)).to_le_bytes());
    raw_bytes.extend_from_slice(&[1, 0x51]);
    raw_bytes.extend_from_slice(&0_u32.to_le_bytes());
    Ok(raw_bytes)
}
