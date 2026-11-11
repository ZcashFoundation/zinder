#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

//! Smoke test that boots an `ExplorerQueryGrpcAdapter` against an in-process
//! tonic server and verifies `ServerInfo` returns the expected capability set.

use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use eyre::{Result, eyre};
use prost::Message as _;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, ChainEpoch, ChainEpochId, Network,
    TransparentAddressScriptHash, explorer_reasons::WALLET_QUERY_CAPABILITY_NOT_SUPPORTED,
    wire::encode_rpc_block_hash_hex,
};
use zinder_explorer::{ExplorerQueryGrpcAdapter, ExplorerServerInfoSettings};
use zinder_materialized_views::{
    BLOCK_SUMMARY_COLUMN_FAMILY, BLOCK_SUMMARY_CONSUMER_NAME, BlockSummaryConsumer,
    MaterializedViewStore, MaterializedViewStoreOptions, REORG_INCIDENTS_CONSUMER_NAME,
    TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILY, TRANSPARENT_ADDRESS_DELTAS_CONSUMER_NAME,
    TransparentAddressDeltasConsumer,
};
use zinder_proto::capabilities::{
    EXPLORER_BLOCK_ACTIVITY_DISTRIBUTION_V1, EXPLORER_BLOCK_SUMMARY_V1,
    EXPLORER_CHAIN_REORG_HISTORY_V1, EXPLORER_OVERVIEW_SNAPSHOT_V1, EXPLORER_SERVER_INFO_V1,
    EXPLORER_TRANSACTION_DETAIL_V4, EXPLORER_TRANSACTION_FEES_V1,
    EXPLORER_TRANSPARENT_ADDRESS_DELTAS_V1,
};
use zinder_proto::v1::explorer::{
    BlockActivityDistributionRequest, BlockSummariesInRangeRequest, BlockSummary,
    BlockSummaryRecord, ChainReorgHistoryRequest, OverviewSnapshotRequest,
    OverviewSnapshotResponse, ServerInfoRequest, TransactionDetailRequest,
    TransparentAddressDeltasRecord, TransparentAddressDeltasRequest, TransparentDeltaKind,
    UnavailableReason, explorer_query_client::ExplorerQueryClient,
};
use zinder_proto::v1::wallet::{AddressLookup, address_lookup::Selector as AddressSelector};
use zinder_proto::wire::{TRANSPARENT_DELTA_KIND_RECEIVED_BYTE, TRANSPARENT_DELTA_KIND_SPENT_BYTE};
use zinder_query::{
    AdmittedIngestControl, WalletEndpointMetadata, WalletQueryGrpcAdapter, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceError,
    UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT, UpstreamHealthSnapshot,
};
use zinder_store::{CanonicalEventCursor, ChainEpochCommitted, ChainEvent, ChainRangeReverted};
use zinder_testkit::{
    ChainFixture, IngestControlFixture, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

type ServerHandle = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

struct SeededMaterializedViewStore {
    _tempdir: tempfile::TempDir,
    secondary_store: MaterializedViewStore,
}

#[tokio::test]
async fn explorer_query_server_info_advertises_ready_capability() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    });
    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let channel = await_with_retry(server_addr).await?;
    let mut client = ExplorerQueryClient::new(channel);
    let response = client.server_info(ServerInfoRequest {}).await?.into_inner();
    assert!(
        response
            .freshness
            .as_ref()
            .and_then(|freshness| freshness.chain_view.as_ref())
            .is_none(),
        "ServerInfo without a materialized-view store or upstream probe carries no chain_view",
    );
    let explorer_info = response
        .info
        .ok_or_else(|| eyre!("server info response missing info envelope"))?;
    let common = explorer_info
        .common
        .as_ref()
        .ok_or_else(|| eyre!("explorer info missing common ops.ServerInfo"))?;

    assert_eq!(explorer_info.vendor, "Zinder");
    assert_eq!(common.network, "zcash-regtest");
    assert!(
        common
            .capabilities
            .iter()
            .any(|advertised| { advertised == EXPLORER_SERVER_INFO_V1 })
    );

    server_handle.abort();
    let _ = server_handle.await;
    Ok(())
}

/// Without a configured `wallet_query_endpoint`, the wallet-backed explorer
/// capabilities are omitted from `ServerInfo` and the corresponding methods
/// return `FAILED_PRECONDITION`.
///
/// This pins the operational contract that capability advertisement gates on
/// a wired federation, not on the binary's mere presence.
#[tokio::test]
async fn explorer_query_failed_precondition_without_wallet_query_endpoint() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    });
    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let channel = await_with_retry(server_addr).await?;
    let mut client = ExplorerQueryClient::new(channel);
    let explorer_info = client
        .server_info(ServerInfoRequest {})
        .await?
        .into_inner()
        .info
        .ok_or_else(|| eyre!("server info missing info envelope"))?;
    let common = explorer_info
        .common
        .as_ref()
        .ok_or_else(|| eyre!("explorer info missing common ops.ServerInfo"))?;

    assert!(
        !common
            .capabilities
            .iter()
            .any(|advertised| { advertised == EXPLORER_TRANSACTION_DETAIL_V4 }),
        "transaction_detail capability must not advertise without a wallet_query_endpoint",
    );
    let detail_outcome = client
        .transaction_detail(TransactionDetailRequest {
            transaction_id: "00".repeat(32),
            at_epoch_id: None,
        })
        .await;
    let detail_status = detail_outcome
        .err()
        .ok_or_else(|| eyre!("expected FAILED_PRECONDITION without wallet_query_endpoint"))?;
    assert_eq!(detail_status.code(), tonic::Code::FailedPrecondition);

    assert!(
        !common
            .capabilities
            .iter()
            .any(|advertised| { advertised == EXPLORER_OVERVIEW_SNAPSHOT_V1 }),
        "overview_snapshot capability must not advertise without a materialized-view store",
    );
    let overview_outcome = client
        .overview_snapshot(OverviewSnapshotRequest {
            recent_blocks_limit: 0,
            recent_transactions_limit: 0,
            mempool_window_seconds: 0,
            fee_summary_block_count: 0,
        })
        .await;
    let overview_status = overview_outcome
        .err()
        .ok_or_else(|| eyre!("expected FAILED_PRECONDITION without materialized-view store"))?;
    assert_eq!(overview_status.code(), tonic::Code::FailedPrecondition);

    server_handle.abort();
    let _ = server_handle.await;
    Ok(())
}

#[tokio::test]
async fn explorer_query_serves_block_summary_from_secondary_materialized_view_store() -> Result<()>
{
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (_store_fixture, wallet_addr, wallet_handle) =
        spawn_wallet_query_server(&chain_fixture).await?;
    let seeded_materialized_view_store =
        seeded_block_summary_materialized_view_store(&chain_fixture)?;
    let (mut client, explorer_handle) =
        spawn_explorer_query_server(seeded_materialized_view_store.secondary_store, wallet_addr)
            .await?;

    let server_info = client.server_info(ServerInfoRequest {}).await?.into_inner();
    let explorer_info = server_info
        .info
        .as_ref()
        .ok_or_else(|| eyre!("server info missing info envelope"))?;
    let common = explorer_info
        .common
        .as_ref()
        .ok_or_else(|| eyre!("explorer info missing common ops.ServerInfo"))?;
    assert_advertises_capability(&common.capabilities, EXPLORER_BLOCK_SUMMARY_V1);
    assert_advertises_capability(&common.capabilities, EXPLORER_TRANSACTION_FEES_V1);

    let chain_view = server_info
        .freshness
        .as_ref()
        .and_then(|freshness| freshness.chain_view.as_ref())
        .ok_or_else(|| eyre!("ServerInfo freshness missing chain_view"))?;
    assert!(
        chain_view.chain_epoch.is_none(),
        "ServerInfo makes no snapshot-consistency claim",
    );
    let indexed_tip = chain_view
        .indexed_tip
        .as_ref()
        .and_then(|indexed_tip| indexed_tip.tip.as_ref())
        .ok_or_else(|| eyre!("ServerInfo freshness missing indexed_tip"))?;
    let fixture_block = chain_fixture
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("fixture block missing"))?;
    assert_eq!(indexed_tip.height, 1);
    assert_eq!(
        indexed_tip.hash,
        encode_rpc_block_hash_hex(fixture_block.hash)
    );

    let response = client
        .block_summaries_in_range(BlockSummariesInRangeRequest {
            start_height: 1,
            end_height: 1,
            at_epoch_id: None,
        })
        .await?
        .into_inner();
    assert_eq!(response.summaries.len(), 1);
    assert_eq!(response.summaries[0].block_height, 1);
    assert_eq!(response.summaries[0].confirmations, 1);

    explorer_handle.abort();
    let _ = explorer_handle.await;
    wallet_handle.abort();
    let _ = wallet_handle.await;
    Ok(())
}

#[tokio::test]
async fn explorer_query_aggregates_block_activity_with_explicit_coverage() -> Result<()> {
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (_store_fixture, wallet_addr, wallet_handle) =
        spawn_wallet_query_server(&chain_fixture).await?;
    let seeded_materialized_view_store =
        seeded_block_summary_materialized_view_store(&chain_fixture)?;
    let (mut client, explorer_handle) =
        spawn_explorer_query_server(seeded_materialized_view_store.secondary_store, wallet_addr)
            .await?;

    let explorer_info = client
        .server_info(ServerInfoRequest {})
        .await?
        .into_inner()
        .info
        .ok_or_else(|| eyre!("server info missing info envelope"))?;
    let common = explorer_info
        .common
        .as_ref()
        .ok_or_else(|| eyre!("explorer info missing common ops.ServerInfo"))?;
    assert_advertises_capability(
        &common.capabilities,
        EXPLORER_BLOCK_ACTIVITY_DISTRIBUTION_V1,
    );

    let response = client
        .block_activity_distribution(BlockActivityDistributionRequest {
            start_height: 0,
            end_height: 1,
        })
        .await?
        .into_inner();
    assert_eq!(response.start_height, 0);
    assert_eq!(response.end_height, 1);
    assert_eq!(response.materialized_block_count, 1);
    assert_eq!(response.missing_block_count, 1);
    assert_eq!(response.transaction_count, 0);
    assert_eq!(response.buckets.len(), 168);
    assert!(response.first_block_time_unix_seconds.is_some());
    assert!(response.last_block_time_unix_seconds.is_some());

    explorer_handle.abort();
    let _ = explorer_handle.await;
    wallet_handle.abort();
    let _ = wallet_handle.await;
    Ok(())
}

/// `OverviewSnapshot` returns one coherent bundle anchored to a single chain epoch.
///
/// Two consecutive calls against the same upstream tip return the same
/// `tip_hash`; the response's `recent_blocks[0]` carries the seeded
/// block's height and timestamp; the bundle's single
/// `freshness.capability_version` is the overview capability string.
#[tokio::test]
async fn explorer_query_serves_overview_snapshot_with_seeded_materialized_view_store() -> Result<()>
{
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (_store_fixture, wallet_addr, wallet_handle) =
        spawn_wallet_query_server(&chain_fixture).await?;
    let seeded_materialized_view_store =
        seeded_block_summary_materialized_view_store(&chain_fixture)?;
    let (mut client, explorer_handle) =
        spawn_explorer_query_server(seeded_materialized_view_store.secondary_store, wallet_addr)
            .await?;

    let explorer_info = client
        .server_info(ServerInfoRequest {})
        .await?
        .into_inner()
        .info
        .ok_or_else(|| eyre!("server info missing info envelope"))?;
    let common = explorer_info
        .common
        .as_ref()
        .ok_or_else(|| eyre!("explorer info missing common ops.ServerInfo"))?;
    assert_advertises_capability(&common.capabilities, EXPLORER_OVERVIEW_SNAPSHOT_V1);

    let first = client
        .overview_snapshot(OverviewSnapshotRequest {
            recent_blocks_limit: 0,
            recent_transactions_limit: 0,
            mempool_window_seconds: 0,
            fee_summary_block_count: 0,
        })
        .await?
        .into_inner();
    let first_freshness = first
        .freshness
        .as_ref()
        .ok_or_else(|| eyre!("overview response missing freshness"))?;
    let first_visible_tip = freshness_visible_tip(first_freshness)?;
    assert_eq!(
        first_freshness.capability_version,
        EXPLORER_OVERVIEW_SNAPSHOT_V1
    );
    assert_eq!(first_visible_tip.height, 1);
    assert_eq!(first.recent_blocks.len(), 1);
    assert_eq!(first.recent_blocks[0].block_height, 1);
    assert_eq!(first.recent_blocks[0].confirmations, 1);
    assert!(first.recent_blocks[0].is_canonical);
    assert_eq!(
        first.tip_block_time_unix_seconds,
        first.recent_blocks[0].block_time_unix_seconds
    );
    assert_overview_wallet_fields_from_production_endpoint(&first)?;

    // Coherence guarantee: a second call against the same upstream tip
    // returns the same snapshot identity (tip_hash). The bundle never
    // straddles two tips.
    let second = client
        .overview_snapshot(OverviewSnapshotRequest {
            recent_blocks_limit: 0,
            recent_transactions_limit: 0,
            mempool_window_seconds: 0,
            fee_summary_block_count: 0,
        })
        .await?
        .into_inner();
    let second_freshness = second
        .freshness
        .as_ref()
        .ok_or_else(|| eyre!("second response missing freshness"))?;
    let second_visible_tip = freshness_visible_tip(second_freshness)?;
    assert_eq!(second_visible_tip.hash, first_visible_tip.hash);
    assert_eq!(second_visible_tip.height, first_visible_tip.height);

    explorer_handle.abort();
    let _ = explorer_handle.await;
    wallet_handle.abort();
    let _ = wallet_handle.await;
    Ok(())
}

/// The upstream-observation probe surfaces the cached `UpstreamHealthSnapshot`
/// on every `ExplorerFreshness` once the probe has fired.
///
/// Wires a stub `NodeSource` that reports a synthetic snapshot, spawns the
/// adapter's background probe at a short cadence, and asserts the resulting
/// `OverviewSnapshot` carries the same fields the stub returned.
#[tokio::test]
async fn explorer_query_freshness_carries_upstream_observation_after_probe_fires() -> Result<()> {
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (_store_fixture, wallet_addr, wallet_handle) =
        spawn_wallet_query_server(&chain_fixture).await?;
    let seeded_materialized_view_store =
        seeded_block_summary_materialized_view_store(&chain_fixture)?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let explorer_addr = listener.local_addr()?;
    let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    })
    .with_materialized_view_store(seeded_materialized_view_store.secondary_store)
    .with_wallet_query_endpoint(format!("http://{wallet_addr}"))
    .with_prevout_resolution_online(true);
    let probe_cancel = CancellationToken::new();
    let probe_handle = adapter.spawn_upstream_observation_probe(
        Arc::new(StubUpstreamSource::ready(2_530_000, 2_544_375, 0.9943)),
        Duration::from_millis(10),
        probe_cancel.clone(),
    );
    let explorer_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    let channel = await_with_retry(explorer_addr).await?;
    let mut client = ExplorerQueryClient::new(channel);

    // The probe loop waits one `poll_interval` before its first tick. Spin
    // a few requests with a short pause between them so the test passes
    // deterministically once the first snapshot lands.
    let mut observed_upstream = None;
    for _ in 0..50 {
        let response = client
            .overview_snapshot(OverviewSnapshotRequest {
                recent_blocks_limit: 0,
                recent_transactions_limit: 0,
                mempool_window_seconds: 0,
                fee_summary_block_count: 0,
            })
            .await?
            .into_inner();
        let upstream_tip = response
            .freshness
            .and_then(|freshness| freshness.chain_view)
            .and_then(|chain_view| chain_view.upstream_tip);
        if let Some(upstream) = upstream_tip {
            observed_upstream = Some(upstream);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let upstream = observed_upstream
        .ok_or_else(|| eyre!("upstream observation probe never refreshed the cached snapshot"))?;
    assert_eq!(upstream.committed_height, Some(2_530_000));
    assert_eq!(upstream.estimated_height, Some(2_544_375));

    let server_info = client.server_info(ServerInfoRequest {}).await?.into_inner();
    let server_info_upstream = server_info
        .freshness
        .and_then(|freshness| freshness.chain_view)
        .and_then(|chain_view| chain_view.upstream_tip)
        .ok_or_else(|| eyre!("ServerInfo freshness missing upstream observation"))?;
    assert_eq!(server_info_upstream.committed_height, Some(2_530_000));
    assert_eq!(server_info_upstream.estimated_height, Some(2_544_375));

    probe_cancel.cancel();
    let _ = probe_handle.await;
    explorer_handle.abort();
    let _ = explorer_handle.await;
    wallet_handle.abort();
    let _ = wallet_handle.await;
    Ok(())
}

#[tokio::test]
async fn explorer_query_serves_recorded_chain_reorg_history() -> Result<()> {
    let canonical_directory = tempfile::tempdir()?;
    let seeded_materialized_view_store =
        seeded_reorg_history_materialized_view_store(canonical_directory.path())?;
    let (mut client, explorer_handle) = spawn_explorer_query_server_with_materialized_view_store(
        seeded_materialized_view_store.secondary_store,
    )
    .await?;

    assert_reorg_history_capability(&mut client).await?;
    let reorg_cursor = assert_recorded_reorg_history_page(&mut client).await?;
    assert_reorg_history_empty_after(&mut client, reorg_cursor).await?;

    explorer_handle.abort();
    let _ = explorer_handle.await;
    Ok(())
}

/// One retained canonical transition, as the materialized-view tailer dispatches it.
struct RecordedChainEvent {
    event_sequence: u64,
    chain_epoch: ChainEpoch,
    chain_event: ChainEvent,
}

/// Returns the transitions a canonical writer retains when it commits heights
/// 1..=2 above a settled height 1 and then replaces height 2.
fn recorded_chain_reorg_events() -> Result<[RecordedChainEvent; 2]> {
    let initial_chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let settled_block = initial_chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("initial fixture missing height 1"))?;
    let mut initial_epoch = initial_chain
        .chain_epoch(ChainEpochId::new(1))
        .ok_or_else(|| eyre!("initial fixture missing chain epoch"))?;
    initial_epoch.settled_tip_height = settled_block.height;
    initial_epoch.settled_tip_hash = settled_block.hash;

    let replacement_chain = initial_chain.fork_at(BlockHeight::new(2))?.extend_blocks(1);
    let mut replacement_epoch = replacement_chain
        .chain_epoch(ChainEpochId::new(2))
        .ok_or_else(|| eyre!("replacement fixture missing chain epoch"))?;
    replacement_epoch.settled_tip_height = settled_block.height;
    replacement_epoch.settled_tip_hash = settled_block.hash;

    let replaced_range = BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(2));
    Ok([
        RecordedChainEvent {
            event_sequence: 1,
            chain_epoch: initial_epoch,
            chain_event: ChainEvent::ChainCommitted {
                committed: ChainEpochCommitted {
                    chain_epoch: initial_epoch,
                    block_range: BlockHeightRange::inclusive(
                        BlockHeight::new(1),
                        BlockHeight::new(2),
                    ),
                },
            },
        },
        RecordedChainEvent {
            event_sequence: 2,
            chain_epoch: replacement_epoch,
            chain_event: ChainEvent::ChainReorged {
                reverted: ChainRangeReverted {
                    chain_epoch: initial_epoch,
                    block_range: replaced_range,
                },
                committed: ChainEpochCommitted {
                    chain_epoch: replacement_epoch,
                    block_range: replaced_range,
                },
            },
        },
    ])
}

fn seeded_reorg_history_materialized_view_store(
    canonical_path: &Path,
) -> Result<SeededMaterializedViewStore> {
    let primary_materialized_view_store = zinder_ingest::open_primary_materialized_view_store(
        canonical_path,
        zinder_materialized_views::MaterializedViewPreset::Explorer,
        zinder_store::RocksDbResourceBudget::for_local_tests(),
    )?;
    record_reorg_incidents(&primary_materialized_view_store)?;
    let reorg_cursor = primary_materialized_view_store
        .get_chain_event_cursor(REORG_INCIDENTS_CONSUMER_NAME)?
        .ok_or_else(|| eyre!("reorg incidents cursor missing after materialized-view replay"))?;
    assert!(!reorg_cursor.is_empty());

    let materialized_view_secondary_tempdir = tempfile::tempdir()?;
    let materialized_view_store = MaterializedViewStore::open_secondary(
        MaterializedViewStore::path_for_canonical(canonical_path),
        materialized_view_secondary_tempdir.path(),
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
        },
    )?;
    materialized_view_store.try_catch_up()?;
    Ok(SeededMaterializedViewStore {
        _tempdir: materialized_view_secondary_tempdir,
        secondary_store: materialized_view_store,
    })
}

async fn assert_reorg_history_capability(client: &mut ExplorerQueryClient<Channel>) -> Result<()> {
    let explorer_info = client
        .server_info(ServerInfoRequest {})
        .await?
        .into_inner()
        .info
        .ok_or_else(|| eyre!("server info missing info envelope"))?;
    let common = explorer_info
        .common
        .as_ref()
        .ok_or_else(|| eyre!("explorer info missing common ops.ServerInfo"))?;
    assert_advertises_capability(&common.capabilities, EXPLORER_CHAIN_REORG_HISTORY_V1);
    Ok(())
}

async fn assert_recorded_reorg_history_page(
    client: &mut ExplorerQueryClient<Channel>,
) -> Result<Vec<u8>> {
    let first_page = client
        .chain_reorg_history(ChainReorgHistoryRequest {
            max_events: 1,
            from_cursor: Vec::new(),
        })
        .await?
        .into_inner();
    let freshness = first_page
        .freshness
        .as_ref()
        .ok_or_else(|| eyre!("reorg history response missing freshness"))?;
    assert_eq!(
        freshness.capability_version,
        EXPLORER_CHAIN_REORG_HISTORY_V1
    );
    assert!(first_page.next_cursor.is_empty());
    assert_eq!(first_page.events.len(), 1);
    let reorg = first_page
        .events
        .first()
        .ok_or_else(|| eyre!("reorg event missing"))?;
    let reorg_cursor = reorg.cursor.clone();
    assert_eq!(reorg.event_sequence, 2);
    assert!(!reorg_cursor.is_empty());
    assert_eq!(reorg.chain_epoch_id, 2);
    assert_eq!(
        reorg
            .visible_tip
            .as_ref()
            .ok_or_else(|| eyre!("reorg visible tip missing"))?
            .height,
        2
    );
    assert_eq!(
        reorg
            .settled_tip
            .as_ref()
            .ok_or_else(|| eyre!("reorg settled tip missing"))?
            .height,
        1
    );
    let reverted = reorg
        .reverted
        .as_ref()
        .ok_or_else(|| eyre!("reorg reverted range missing"))?;
    assert_eq!(reverted.start_height, 2);
    assert_eq!(reverted.end_height, 2);
    let committed = reorg
        .committed
        .as_ref()
        .ok_or_else(|| eyre!("reorg committed range missing"))?;
    assert_eq!(committed.start_height, 2);
    assert_eq!(committed.end_height, 2);
    Ok(reorg_cursor)
}

async fn assert_reorg_history_empty_after(
    client: &mut ExplorerQueryClient<Channel>,
    cursor: Vec<u8>,
) -> Result<()> {
    let empty_page = client
        .chain_reorg_history(ChainReorgHistoryRequest {
            max_events: 10,
            from_cursor: cursor,
        })
        .await?
        .into_inner();
    assert!(empty_page.events.is_empty());
    assert!(empty_page.next_cursor.is_empty());
    Ok(())
}

/// Minimal `NodeSource` stub used by the upstream-observation probe test.
///
/// Returns a fixed [`UpstreamHealthSnapshot`] from
/// `poll_upstream_health` and surfaces `NodeCapabilityMissing` from
/// every other method. Used only to exercise the adapter's
/// upstream-observation probe; never hits a real node.
struct StubUpstreamSource {
    snapshot: UpstreamHealthSnapshot,
}

impl StubUpstreamSource {
    fn ready(committed: u32, estimated: u32, progress: f64) -> Self {
        Self {
            snapshot: UpstreamHealthSnapshot::ready(
                UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT,
                Some(committed),
                Some(estimated),
                Some(progress),
            ),
        }
    }
}

#[async_trait]
impl NodeSource for StubUpstreamSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, _height: BlockHeight) -> Result<SourceBlock, SourceError> {
        Err(SourceError::NodeCapabilityMissing {
            capability: zinder_source::NodeCapability::ReadinessProbe,
        })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        Err(SourceError::NodeCapabilityMissing {
            capability: zinder_source::NodeCapability::ReadinessProbe,
        })
    }

    async fn poll_upstream_health(&self) -> Result<UpstreamHealthSnapshot, SourceError> {
        Ok(self.snapshot.clone())
    }
}

/// Production wallet endpoint the explorer federates to, and the fixtures
/// that must outlive it.
struct WalletEndpointFixture {
    _store_fixture: WalletServingStoreFixture,
    _ingest_control_fixture: IngestControlFixture,
}

/// Serves the native `WalletQuery` endpoint over the production canonical and
/// wallet secondaries built from `chain_fixture`.
async fn spawn_wallet_query_server(
    chain_fixture: &ChainFixture,
) -> Result<(WalletEndpointFixture, SocketAddr, ServerHandle)> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(chain_fixture, &activations)?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let ingest_control_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let ingest_control = AdmittedIngestControl::connect(
        ingest_control_fixture.endpoint(),
        None,
        Network::ZcashRegtest,
    )
    .await?;
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::new(canonical_reader),
            Arc::new(wallet_reader),
        )?)),
        (),
        ingest_control,
        activations,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let adapter = WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default());
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    let _channel = await_with_retry(addr).await?;
    Ok((
        WalletEndpointFixture {
            _store_fixture: store_fixture,
            _ingest_control_fixture: ingest_control_fixture,
        },
        addr,
        handle,
    ))
}

fn seeded_block_summary_materialized_view_store(
    chain_fixture: &ChainFixture,
) -> Result<SeededMaterializedViewStore> {
    seeded_block_summary_materialized_view_store_with_transaction_ids(chain_fixture, &[])
}

fn seeded_block_summary_materialized_view_store_with_transaction_ids(
    chain_fixture: &ChainFixture,
    transaction_ids: &[String],
) -> Result<SeededMaterializedViewStore> {
    let tempdir = tempfile::tempdir()?;
    let primary_path = tempdir.path().join("materialized-view-primary");
    let secondary_path = tempdir.path().join("materialized-view-secondary");
    let primary_store = MaterializedViewStore::open(
        &primary_path,
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
        },
    )?;
    seed_block_summary(&primary_store, chain_fixture, transaction_ids)?;

    let secondary_store = MaterializedViewStore::open_secondary(
        &primary_path,
        &secondary_path,
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
        },
    )?;
    secondary_store.try_catch_up()?;
    Ok(SeededMaterializedViewStore {
        _tempdir: tempdir,
        secondary_store,
    })
}

/// Replays the recorded canonical transitions into the reorg-incident log.
fn record_reorg_incidents(materialized_view_store: &MaterializedViewStore) -> Result<()> {
    let blocks = std::collections::HashMap::new();
    for recorded in recorded_chain_reorg_events()? {
        let chain_cursor = CanonicalEventCursor::at(recorded.event_sequence)?.as_bytes();
        let mut reorg_incidents = zinder_materialized_views::ReorgIncidentsConsumer::new();
        let mut block_consumers: [&mut dyn zinder_materialized_views::BlockKeyedConsumer; 0] = [];
        let mut event_consumers: [&mut dyn zinder_materialized_views::MaterializedViewConsumer; 1] =
            [&mut reorg_incidents];
        materialized_view_store.write_chain_event_chunk_with_event_consumers(
            zinder_materialized_views::ChainEventDispatchConsumers {
                block_consumers: &mut block_consumers,
                event_consumers: &mut event_consumers,
            },
            zinder_materialized_views::ChainEventDispatchInputs {
                chain_epoch: recorded.chain_epoch,
                chain_event: &recorded.chain_event,
                chain_cursor: &chain_cursor,
                event_sequence: recorded.event_sequence,
                settled_tip_height: recorded.chain_epoch.settled_tip_height,
            },
            &blocks,
            true,
        )?;
    }
    Ok(())
}

fn seed_block_summary(
    materialized_view_store: &MaterializedViewStore,
    chain_fixture: &ChainFixture,
    transaction_ids: &[String],
) -> Result<()> {
    let fixture_block = chain_fixture
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("fixture block missing"))?;
    let record = BlockSummaryRecord {
        summary: Some(BlockSummary {
            block_height: fixture_block.height.value(),
            block_hash: encode_rpc_block_hash_hex(fixture_block.hash),
            block_time_unix_seconds: i64::from(fixture_block.block_time_seconds),
            transaction_count: u32::try_from(transaction_ids.len())?,
            previous_block_hash: encode_rpc_block_hash_hex(fixture_block.parent_hash),
            total_size_bytes: u64::try_from(fixture_block.raw_block_bytes.len())?,
            fees_collected_zat: 0,
            paid_fees_collected_zat: None,
            coinbase_reward_zat: 0,
            sapling_output_count: 0,
            orchard_action_count: 0,
            ironwood_action_count: 0,
            confirmations: 0,
            is_canonical: true,
        }),
        transaction_ids: transaction_ids.to_vec(),
        fee_transaction_count: 0,
        min_zip317_conventional_fee_zat: 0,
        max_zip317_conventional_fee_zat: 0,
    };
    materialized_view_store.put_consumer(
        BLOCK_SUMMARY_COLUMN_FAMILY,
        &BlockSummaryConsumer::key_for_height(fixture_block.height),
        &record.encode_to_vec(),
    )?;
    materialized_view_store.put_chain_event_cursor(BLOCK_SUMMARY_CONSUMER_NAME, &[1])?;
    Ok(())
}

async fn spawn_explorer_query_server(
    materialized_view_store: MaterializedViewStore,
    wallet_addr: SocketAddr,
) -> Result<(ExplorerQueryClient<Channel>, ServerHandle)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    })
    .with_materialized_view_store(materialized_view_store)
    .with_wallet_query_endpoint(format!("http://{wallet_addr}"))
    .with_prevout_resolution_online(true);
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    let channel = await_with_retry(addr).await?;
    Ok((ExplorerQueryClient::new(channel), handle))
}

async fn spawn_explorer_query_server_with_materialized_view_store(
    materialized_view_store: MaterializedViewStore,
) -> Result<(ExplorerQueryClient<Channel>, ServerHandle)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    })
    .with_materialized_view_store(materialized_view_store);
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    let channel = await_with_retry(addr).await?;
    Ok((ExplorerQueryClient::new(channel), handle))
}

fn assert_advertises_capability(capabilities: &[String], capability: &str) {
    assert!(
        capabilities
            .iter()
            .any(|advertised| advertised == capability),
        "expected capability {capability}",
    );
}

/// The production wallet endpoint serves an empty mempool snapshot through
/// its admitted ingest control but does not serve chain value pools, so only
/// the value pools are reported unavailable.
fn assert_overview_wallet_fields_from_production_endpoint(
    response: &OverviewSnapshotResponse,
) -> Result<()> {
    assert!(
        response.mempool.is_some(),
        "the production wallet endpoint serves a mempool snapshot"
    );
    assert!(response.value_pools.is_empty());
    let freshness = response
        .freshness
        .as_ref()
        .ok_or_else(|| eyre!("overview response missing freshness"))?;
    assert_eq!(freshness.unavailable.len(), 1);
    let unavailable_field = freshness
        .unavailable
        .iter()
        .find(|unavailable| unavailable.field_path == "value_pools")
        .ok_or_else(|| eyre!("overview freshness missing unavailable field value_pools"))?;
    assert_eq!(
        unavailable_field.reason,
        UnavailableReason::UnavailableUpstreamNotSupported as i32
    );
    assert_eq!(
        unavailable_field.human_reason,
        WALLET_QUERY_CAPABILITY_NOT_SUPPORTED
    );
    Ok(())
}

#[tokio::test]
async fn explorer_query_bearer_token_rejects_unauthenticated_clients() -> Result<()> {
    use std::str::FromStr as _;
    use zinder_runtime::{BearerToken, BearerTokenClientInterceptor};

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let server_token =
        BearerToken::from_str("expected").map_err(|error| eyre!("token parse: {error}"))?;
    let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    })
    .with_bearer_token(server_token.clone());
    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let unauthenticated_channel = await_with_retry(server_addr).await?;
    let mut unauthenticated_client = ExplorerQueryClient::new(unauthenticated_channel);
    let unauthenticated_outcome = unauthenticated_client
        .server_info(ServerInfoRequest {})
        .await;
    let unauthenticated_status = unauthenticated_outcome
        .err()
        .ok_or_else(|| eyre!("expected unauthenticated rejection"))?;
    assert_eq!(unauthenticated_status.code(), tonic::Code::Unauthenticated);

    let wrong_token =
        BearerToken::from_str("wrong").map_err(|error| eyre!("token parse: {error}"))?;
    let wrong_channel = Endpoint::from_shared(format!("http://{server_addr}"))?
        .connect()
        .await?;
    let wrong_interceptor = BearerTokenClientInterceptor::new(Some(&wrong_token))
        .map_err(|error| eyre!("interceptor build: {error}"))?;
    let mut wrong_client = ExplorerQueryClient::with_interceptor(wrong_channel, wrong_interceptor);
    let wrong_outcome = wrong_client.server_info(ServerInfoRequest {}).await;
    let wrong_status = wrong_outcome
        .err()
        .ok_or_else(|| eyre!("expected wrong-token rejection"))?;
    assert_eq!(wrong_status.code(), tonic::Code::Unauthenticated);

    let correct_channel = Endpoint::from_shared(format!("http://{server_addr}"))?
        .connect()
        .await?;
    let correct_interceptor = BearerTokenClientInterceptor::new(Some(&server_token))
        .map_err(|error| eyre!("interceptor build: {error}"))?;
    let mut correct_client =
        ExplorerQueryClient::with_interceptor(correct_channel, correct_interceptor);
    let correct_response = correct_client
        .server_info(ServerInfoRequest {})
        .await?
        .into_inner();
    let correct_info = correct_response
        .info
        .ok_or_else(|| eyre!("server info missing info envelope"))?;
    let correct_common = correct_info
        .common
        .as_ref()
        .ok_or_else(|| eyre!("explorer info missing common ops.ServerInfo"))?;
    assert_eq!(correct_common.network, "zcash-regtest");

    server_handle.abort();
    let _ = server_handle.await;
    Ok(())
}

async fn await_with_retry(addr: std::net::SocketAddr) -> Result<Channel> {
    let endpoint = format!("http://{addr}");
    for _ in 0..20 {
        if let Ok(channel) = Channel::from_shared(endpoint.clone())?.connect().await {
            return Ok(channel);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(eyre!(
        "explorer query gRPC server did not accept connections"
    ))
}

/// Extracts the visible tip from a freshness envelope's chain view, the common
/// path the overview-snapshot coherence assertions read.
fn freshness_visible_tip(
    freshness: &zinder_proto::v1::explorer::ExplorerFreshness,
) -> Result<zinder_proto::v1::wallet::BlockTip> {
    freshness
        .chain_view
        .as_ref()
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .and_then(|chain_epoch| chain_epoch.visible_tip.clone())
        .ok_or_else(|| eyre!("freshness missing chain_view.chain_epoch.visible_tip"))
}

/// One value event the deltas seeder writes into the materialized-view store.
struct SeedDelta {
    height: u32,
    in_block_position: u32,
    kind_byte: u8,
    event_index: u32,
    transaction_id: String,
    value_zat: i64,
}

/// Seeds the `transparent_address_deltas` column family for one address with
/// the given events, mirroring what `TransparentAddressDeltasConsumer` writes
/// at commit time.
fn seed_transparent_address_deltas(
    materialized_view_store: &MaterializedViewStore,
    address: TransparentAddressScriptHash,
    deltas: &[SeedDelta],
) -> Result<()> {
    for delta in deltas {
        let key = TransparentAddressDeltasConsumer::key_for_event(
            address,
            BlockHeight::new(delta.height),
            delta.in_block_position,
            delta.kind_byte,
            delta.event_index,
        );
        let record = TransparentAddressDeltasRecord {
            transaction_id: delta.transaction_id.clone(),
            block_time_unix_seconds: 1_700_000_000 + i64::from(delta.height),
            value_zat: delta.value_zat,
        };
        materialized_view_store.put_consumer(
            TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILY,
            &key,
            &record.encode_to_vec(),
        )?;
    }
    materialized_view_store
        .put_chain_event_cursor(TRANSPARENT_ADDRESS_DELTAS_CONSUMER_NAME, &[1])?;
    Ok(())
}

/// Opens a primary materialized-view store seeded with the given address deltas, then
/// returns a caught-up secondary handle for the explorer to read.
fn seeded_deltas_materialized_view_store(
    address: TransparentAddressScriptHash,
    deltas: &[SeedDelta],
) -> Result<SeededMaterializedViewStore> {
    let tempdir = tempfile::tempdir()?;
    let primary_path = tempdir.path().join("materialized-view-primary");
    let secondary_path = tempdir.path().join("materialized-view-secondary");
    let primary_store = MaterializedViewStore::open(
        &primary_path,
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
        },
    )?;
    seed_transparent_address_deltas(&primary_store, address, deltas)?;

    let secondary_store = MaterializedViewStore::open_secondary(
        &primary_path,
        &secondary_path,
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
        },
    )?;
    secondary_store.try_catch_up()?;
    Ok(SeededMaterializedViewStore {
        _tempdir: tempdir,
        secondary_store,
    })
}

fn deltas_request(
    address: TransparentAddressScriptHash,
    start_height: u32,
    end_height: u32,
    max_entries: u32,
    from_cursor: Vec<u8>,
) -> TransparentAddressDeltasRequest {
    TransparentAddressDeltasRequest {
        address: Some(AddressLookup {
            selector: Some(AddressSelector::ScriptHash(address.as_bytes().to_vec())),
        }),
        start_height,
        end_height,
        max_entries,
        from_cursor,
        at_epoch_id: None,
    }
}

const DELTAS_TEST_ADDRESS: TransparentAddressScriptHash =
    TransparentAddressScriptHash::from_bytes([42; 32]);

/// The fixture seeds two receives and one spend; the spend at height 12 nets
/// the second receive to zero so the activity sum is unambiguous.
fn seeded_deltas() -> [SeedDelta; 3] {
    [
        SeedDelta {
            height: 10,
            in_block_position: 1,
            kind_byte: TRANSPARENT_DELTA_KIND_RECEIVED_BYTE,
            event_index: 0,
            transaction_id: "a".repeat(64),
            value_zat: 9_000,
        },
        SeedDelta {
            height: 12,
            in_block_position: 2,
            kind_byte: TRANSPARENT_DELTA_KIND_RECEIVED_BYTE,
            event_index: 3,
            transaction_id: "b".repeat(64),
            value_zat: 5_000,
        },
        SeedDelta {
            height: 12,
            in_block_position: 2,
            kind_byte: TRANSPARENT_DELTA_KIND_SPENT_BYTE,
            event_index: 1,
            transaction_id: "b".repeat(64),
            value_zat: -9_000,
        },
    ]
}

async fn spawn_deltas_explorer(
    deltas: &[SeedDelta],
) -> Result<(ExplorerQueryClient<Channel>, ServerHandle, ServerHandle)> {
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (_store_fixture, wallet_addr, wallet_handle) =
        spawn_wallet_query_server(&chain_fixture).await?;
    let seeded = seeded_deltas_materialized_view_store(DELTAS_TEST_ADDRESS, deltas)?;
    let (client, explorer_handle) =
        spawn_explorer_query_server(seeded.secondary_store, wallet_addr).await?;
    Ok((client, explorer_handle, wallet_handle))
}

/// Per-event rows arrive ascending by height with correct signs and indices,
/// the advertised capability is present, and the net equals the delta sum.
#[tokio::test]
async fn explorer_query_serves_transparent_address_deltas_ascending() -> Result<()> {
    let deltas = seeded_deltas();
    let (mut client, explorer_handle, wallet_handle) = spawn_deltas_explorer(&deltas).await?;

    let common = client
        .server_info(ServerInfoRequest {})
        .await?
        .into_inner()
        .info
        .and_then(|info| info.common)
        .ok_or_else(|| eyre!("explorer info missing common ops.ServerInfo"))?;
    assert_advertises_capability(&common.capabilities, EXPLORER_TRANSPARENT_ADDRESS_DELTAS_V1);

    let entries = client
        .transparent_address_deltas(deltas_request(DELTAS_TEST_ADDRESS, 0, 100, 0, Vec::new()))
        .await?
        .into_inner()
        .entries;
    assert_eq!(entries.len(), 3);

    let heights: Vec<u32> = entries.iter().map(|entry| entry.block_height).collect();
    assert_eq!(heights, vec![10, 12, 12]);
    assert_eq!(entries[0].kind, TransparentDeltaKind::Received as i32);
    assert_eq!(entries[0].index, 0);
    assert_eq!(entries[0].value_zat, 9_000);
    assert_eq!(entries[1].index, 3);
    assert_eq!(entries[1].value_zat, 5_000);
    assert_eq!(entries[2].kind, TransparentDeltaKind::Spent as i32);
    assert_eq!(entries[2].index, 1);
    assert_eq!(entries[2].value_zat, -9_000);

    let net: i64 = entries.iter().map(|entry| entry.value_zat).sum();
    assert_eq!(net, 9_000 + 5_000 - 9_000);

    explorer_handle.abort();
    let _ = explorer_handle.await;
    wallet_handle.abort();
    let _ = wallet_handle.await;
    Ok(())
}

/// The height range filters the series, an out-of-range window returns no rows
/// and no cursor, and the page cursor resumes strictly after the prior page.
#[tokio::test]
async fn explorer_query_pages_transparent_address_deltas() -> Result<()> {
    let deltas = seeded_deltas();
    let (mut client, explorer_handle, wallet_handle) = spawn_deltas_explorer(&deltas).await?;

    let ranged = client
        .transparent_address_deltas(deltas_request(DELTAS_TEST_ADDRESS, 11, 100, 0, Vec::new()))
        .await?
        .into_inner();
    assert_eq!(ranged.entries.len(), 2);
    assert!(ranged.entries.iter().all(|entry| entry.block_height == 12));

    let empty = client
        .transparent_address_deltas(deltas_request(DELTAS_TEST_ADDRESS, 200, 300, 0, Vec::new()))
        .await?
        .into_inner();
    assert!(empty.entries.is_empty());
    assert!(empty.next_cursor.is_empty());

    let first_page = client
        .transparent_address_deltas(deltas_request(DELTAS_TEST_ADDRESS, 0, 100, 1, Vec::new()))
        .await?
        .into_inner();
    assert_eq!(first_page.entries.len(), 1);
    assert_eq!(first_page.entries[0].block_height, 10);
    assert!(!first_page.next_cursor.is_empty());

    let second_page = client
        .transparent_address_deltas(deltas_request(
            DELTAS_TEST_ADDRESS,
            0,
            100,
            1,
            first_page.next_cursor,
        ))
        .await?
        .into_inner();
    assert_eq!(second_page.entries.len(), 1);
    assert_eq!(second_page.entries[0].block_height, 12);

    explorer_handle.abort();
    let _ = explorer_handle.await;
    wallet_handle.abort();
    let _ = wallet_handle.await;
    Ok(())
}
