//! Live federation test for [`ExplorerQuery::ValuePoolSummary`].
//!
//! Exercises Explorer's forwarding and freshness contract over the current
//! `WalletQuery.ChainValuePoolsAtTip` control path. The fixture runs the
//! production canonical writer control adapter with the live
//! `ZebraJsonRpcSource`, so the source read crosses Explorer, `WalletQuery`,
//! ingest control, and Zebra rather than stopping at a protocol stub.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use eyre::{Result, eyre};
use tempfile::{TempDir, tempdir};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use zinder_core::wire::encode_zinder_native_chain_name;
use zinder_core::{BlockHeight, Network};
use zinder_explorer::{ExplorerQueryGrpcAdapter, ExplorerServerInfoSettings};
use zinder_ingest::{
    CanonicalConstructionConfig, CanonicalFollowConfig, CanonicalIngestControlGrpcAdapter,
    CanonicalPipelineLimits, CanonicalWriterConfig, LiveMempoolOwner, canonical_control_channel,
    run_canonical_writer_with_control,
};
use zinder_proto::capabilities::EXPLORER_VALUE_POOL_SUMMARY_V1;
use zinder_proto::v1::explorer::{
    ValuePoolSummaryRequest, ValuePoolSummaryResponse,
    explorer_query_server::ExplorerQuery as ExplorerQueryService,
};
use zinder_query::WalletQuery;
use zinder_source::{NodeSource, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_store::{
    ChainStoreOptions, PrimaryChainStore, RocksDbCanonicalStore, RocksDbResourceBudget,
};
use zinder_testkit::live::{LiveTestEnv, init, require_live_for};
use zinder_testkit::sample_regtest_upgrade_activations;

use crate::common::{
    WalletQueryServerOptions, await_grpc_endpoint, fetch_live_network_upgrade_activations,
    fetch_live_tip_height, serve_wallet_query_grpc,
};

const BACKFILL_DEPTH_BLOCKS: u32 = 50;
const CANONICAL_FOLLOW_POLL_INTERVAL: Duration = Duration::from_millis(250);
const WRITER_READY_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn value_pool_summary_returns_upstream_pools_through_federation() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[
        Network::ZcashRegtest,
        Network::ZcashTestnet,
        Network::ZcashMainnet,
    ])?
    else {
        return Ok(());
    };
    let mut fixture = ValuePoolSummaryFixture::open(&env).await?;
    let response = fixture.value_pool_summary().await?;
    let freshness = response
        .freshness
        .as_ref()
        .ok_or_else(|| eyre!("ValuePoolSummary response missing freshness"))?;
    assert_eq!(freshness.capability_version, EXPLORER_VALUE_POOL_SUMMARY_V1);
    assert!(
        freshness
            .chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
            .is_some(),
        "value pool summary freshness must carry a chain epoch",
    );
    let source_tip = response
        .source_tip
        .as_ref()
        .ok_or_else(|| eyre!("ValuePoolSummary response missing source_tip"))?;
    assert!(
        source_tip.height > 0,
        "source_tip height should be non-zero on any non-empty chain",
    );
    assert_eq!(source_tip.hash.len(), 64);
    assert!(
        !response.pools.is_empty(),
        "value pool summary must carry at least one upstream pool entry",
    );
    let has_known_pool = response
        .pools
        .iter()
        .any(|pool| matches!(pool.id.as_str(), "transparent" | "sapling" | "orchard"));
    assert!(
        has_known_pool,
        "expected at least one of transparent / sapling / orchard in the upstream report; got {:?}",
        response
            .pools
            .iter()
            .map(|pool| pool.id.as_str())
            .collect::<Vec<_>>(),
    );

    tracing::info!(
        target: "zinder::live",
        event = "value_pool_summary_validated",
        network = %encode_zinder_native_chain_name(fixture.network),
        source_tip_height = source_tip.height,
        source_tip_hash = source_tip.hash,
        pool_count = response.pools.len(),
        "explorer value pool summary validated against live node",
    );

    fixture.shutdown().await?;
    Ok(())
}

struct ValuePoolSummaryFixture {
    network: Network,
    explorer_adapter: ExplorerQueryGrpcAdapter,
    wallet_server_handle: JoinHandle<Result<(), tonic::transport::Error>>,
    ingest_control_handle: JoinHandle<Result<(), tonic::transport::Error>>,
    canonical_writer_handle:
        JoinHandle<Result<RocksDbCanonicalStore, zinder_ingest::CanonicalWriterError>>,
    writer_cancel: CancellationToken,
    _store_tempdir: TempDir,
}

impl ValuePoolSummaryFixture {
    async fn open(env: &LiveTestEnv) -> Result<Self> {
        let network = env.network();
        let (
            store_tempdir,
            wallet_store_path,
            canonical_writer_path,
            checkpoint_height,
            source,
            activations,
        ) = prepare_live_fixture(env).await?;
        let readiness = zinder_runtime::Readiness::default();
        let writer_cancel = CancellationToken::new();
        let (canonical, commands) = canonical_control_channel();
        let writer_config = canonical_writer_config(
            env,
            canonical_writer_path,
            checkpoint_height,
            Arc::clone(&activations),
        )?;
        let writer_source = source.clone();
        let writer_activations = Arc::clone(&activations);
        let writer_readiness = readiness.clone();
        let writer_task_cancel = writer_cancel.clone();
        let canonical_writer_handle = tokio::spawn(async move {
            run_canonical_writer_with_control(
                &writer_source,
                writer_activations,
                writer_config,
                &writer_readiness,
                &writer_task_cancel,
                Some(commands),
            )
            .await
        });
        wait_for_writer(&canonical).await?;

        // Value-pool reads are always proxied to the writer. WalletQuery still
        // needs a canonical store for its other RPCs, so keep this deliberately
        // empty fixture disjoint from the writer-owned canonical store.
        let store =
            PrimaryChainStore::open(&wallet_store_path, ChainStoreOptions::for_network(network))?;
        let wallet_query = WalletQuery::new(
            store.clone(),
            (),
            Arc::new(sample_regtest_upgrade_activations()),
        );
        let (ingest_control_addr, ingest_control_handle) =
            serve_ingest_control_grpc(network, canonical, source, readiness, writer_cancel.clone())
                .await?;
        let (wallet_grpc_addr, wallet_server_handle) = serve_wallet_query_grpc(
            wallet_query,
            WalletQueryServerOptions {
                network: Some(network),
                ingest_control_endpoint: Some(format!("http://{ingest_control_addr}")),
            },
        )
        .await?;
        let wallet_endpoint = format!("http://{wallet_grpc_addr}");

        let explorer_adapter =
            ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings { network })
                .with_wallet_query_endpoint(wallet_endpoint);

        Ok(Self {
            network,
            explorer_adapter,
            wallet_server_handle,
            ingest_control_handle,
            canonical_writer_handle,
            writer_cancel,
            _store_tempdir: store_tempdir,
        })
    }

    async fn value_pool_summary(&self) -> Result<ValuePoolSummaryResponse> {
        let response = ExplorerQueryService::value_pool_summary(
            &self.explorer_adapter,
            Request::new(ValuePoolSummaryRequest {}),
        )
        .await?
        .into_inner();
        Ok(response)
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.wallet_server_handle.abort();
        self.ingest_control_handle.abort();
        self.writer_cancel.cancel();
        let _ = (&mut self.wallet_server_handle).await;
        let _ = (&mut self.ingest_control_handle).await;
        let writer_result = (&mut self.canonical_writer_handle).await?;
        writer_result?;
        Ok(())
    }
}

fn canonical_writer_config(
    env: &LiveTestEnv,
    storage_path: std::path::PathBuf,
    checkpoint_height: BlockHeight,
    activations: Arc<zinder_core::NetworkUpgradeActivations>,
) -> Result<CanonicalWriterConfig> {
    Ok(CanonicalWriterConfig {
        storage_path,
        resource_budget: RocksDbResourceBudget::for_local_tests(),
        construction: CanonicalConstructionConfig {
            request_timeout: env.target.request_timeout,
            pipeline_limits: CanonicalPipelineLimits::resolve(
                None,
                NonZeroU32::new(2).ok_or_else(|| eyre!("invalid test core count"))?,
                env.target.max_response_bytes,
            ),
            network_upgrade_activations: activations,
        },
        checkpoint_height: Some(checkpoint_height),
        raw_blob_retention: zinder_store::RawBlobRetention::Transactions,
        reorg_window_blocks: 100,
        follow: CanonicalFollowConfig {
            request_timeout: env.target.request_timeout,
            poll_interval: CANONICAL_FOLLOW_POLL_INTERVAL,
            lag_threshold_blocks: 1,
            // Leaving this open makes construction resolve Zebra's currently
            // observed tip, then keeps the writer available to serve the
            // control request under test.
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
        },
    })
}

async fn prepare_live_fixture(
    env: &LiveTestEnv,
) -> Result<(
    TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    BlockHeight,
    ZebraJsonRpcSource,
    Arc<zinder_core::NetworkUpgradeActivations>,
)> {
    let tip_height = fetch_live_tip_height(env).await?;
    if tip_height.value() <= BACKFILL_DEPTH_BLOCKS {
        return Err(eyre!(
            "tip height {} is at or below the minimum {BACKFILL_DEPTH_BLOCKS}",
            tip_height.value(),
        ));
    }
    let checkpoint_height = BlockHeight::new(tip_height.value() - BACKFILL_DEPTH_BLOCKS - 1);
    let tempdir = tempdir()?;
    let wallet_store_path = tempdir.path().join("wallet-query-canonical");
    let canonical_writer_path = tempdir.path().join("canonical-writer");
    let activations = fetch_live_network_upgrade_activations(env).await?;
    let source = ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    Ok((
        tempdir,
        wallet_store_path,
        canonical_writer_path,
        checkpoint_height,
        source,
        activations,
    ))
}

async fn wait_for_writer(canonical: &zinder_ingest::CanonicalControlHandle) -> Result<()> {
    tokio::time::timeout(WRITER_READY_TIMEOUT, canonical.writer_status())
        .await
        .map_err(|_| eyre!("canonical writer did not begin serving control requests"))?
        .map_err(|error| eyre!("canonical writer control request failed: {error}"))?;
    Ok(())
}

async fn serve_ingest_control_grpc(
    network: Network,
    canonical: zinder_ingest::CanonicalControlHandle,
    source: zinder_source::ZebraJsonRpcSource,
    readiness: zinder_runtime::Readiness,
    cancel: CancellationToken,
) -> Result<(SocketAddr, JoinHandle<Result<(), tonic::transport::Error>>)> {
    let node_source: Arc<dyn NodeSource> = Arc::new(source);
    let adapter = CanonicalIngestControlGrpcAdapter::new(
        network,
        canonical,
        // The current production adapter owns its `MempoolIndex` through this
        // live owner rather than accepting the index as a separate argument.
        LiveMempoolOwner::default(),
        node_source,
        readiness,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                cancel.cancelled_owned(),
            )
            .await
    });
    await_grpc_endpoint(addr).await?;
    Ok((addr, handle))
}
