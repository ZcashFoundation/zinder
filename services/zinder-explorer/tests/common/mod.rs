//! Shared helpers for `zinder-explorer`'s integration and live tests.
//!
//! Subset of `services/zinder-ingest/tests/common/mod.rs` that the
//! `services/zinder-explorer` test crate needs to build a production wallet
//! endpoint from a live upstream node and probe the federated read paths.
//! Duplicated deliberately so the live gating contract stays colocated with
//! the consumer; a third consumer is the prompt for consolidating into
//! `zinder-testkit::live`.

#![allow(
    dead_code,
    reason = "Each test file consumes only a subset of the common helpers."
)]

use std::{net::SocketAddr, sync::Arc, time::Duration};

use eyre::{Result, eyre};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use zinder_core::{
    BlockHeight, BlockId, NetworkUpgradeActivations, UnixTimestampMillis,
    wire::encode_zinder_native_chain_name,
};
use zinder_ingest::{CanonicalConstructionConfig, load_fresh_canonical};
use zinder_query::{
    AdmittedIngestControl, WalletEndpointMetadata, WalletQueryGrpcAdapter, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_source::{NodeSource, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
    CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder, RocksDbResourceBudget,
};
use zinder_testkit::{IngestControlFixture, WalletServingStoreFixture, live::LiveTestEnv};

/// Reorg window for canonical stores built from a live node in tests.
const LIVE_CANONICAL_REORG_WINDOW_BLOCKS: u32 = 100;

/// Fetches the node-advertised upgrade table used to build the live serving pair.
pub(crate) async fn fetch_live_network_upgrade_activations(
    env: &LiveTestEnv,
) -> Result<Arc<NetworkUpgradeActivations>> {
    let probe_source = ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    Ok(Arc::new(
        probe_source.fetch_network_upgrade_activations().await?,
    ))
}

/// Production wallet endpoint built from a live node, and the fixtures that
/// must outlive it.
pub(crate) struct LiveWalletEndpoint {
    /// Loopback address serving the native `WalletQuery` service.
    pub(crate) addr: SocketAddr,
    /// Server task; abort it to stop serving.
    pub(crate) server: JoinHandle<Result<(), tonic::transport::Error>>,
    /// Build tip of the published canonical store.
    pub(crate) tip_height: BlockHeight,
    /// Owner of the store directories; dropping it removes them.
    _store_fixture: WalletServingStoreFixture,
    /// Structurally valid ingest-control endpoint with an empty mempool.
    _ingest_control: IngestControlFixture,
}

/// Builds a READY production canonical store holding the live chain from
/// genesis through the node tip, projects its wallet store, and serves the
/// native `WalletQuery` endpoint over the admitted pair.
///
/// History is genesis-complete because the wallet projection must resolve
/// every transparent input's predecessor output, so this only fits test
/// budgets on regtest. Mempool reads go to an ingest-control fixture that
/// serves an empty mempool.
pub(crate) async fn serve_live_wallet_endpoint(env: &LiveTestEnv) -> Result<LiveWalletEndpoint> {
    let network = env.network();
    let activations = fetch_live_network_upgrade_activations(env).await?;
    let (store_fixture, slot, tip_height) =
        Box::pin(build_live_serving_pair(env, &activations)).await?;
    let ingest_control = IngestControlFixture::spawn(network).await?;
    let admitted_ingest_control =
        AdmittedIngestControl::connect(ingest_control.endpoint(), None, network).await?;
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        slot,
        (),
        admitted_ingest_control,
        activations,
    );
    let adapter = WalletQueryGrpcAdapter::new(
        wallet_query,
        WalletEndpointMetadata {
            network: encode_zinder_native_chain_name(network).to_owned(),
            ..WalletEndpointMetadata::default()
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    await_grpc_endpoint(addr).await?;
    Ok(LiveWalletEndpoint {
        addr,
        server,
        tip_height,
        _store_fixture: store_fixture,
        _ingest_control: ingest_control,
    })
}

/// Runs the fresh-load and baseline publication path the canonical writer
/// uses from genesis through the node tip, then projects the wallet store and
/// admits one serving pair.
async fn build_live_serving_pair(
    env: &LiveTestEnv,
    activations: &Arc<NetworkUpgradeActivations>,
) -> Result<(
    WalletServingStoreFixture,
    WalletServingPairSlot,
    BlockHeight,
)> {
    let source = ZebraJsonRpcSource::with_options(
        env.network(),
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    source.probe_capabilities().await?;
    let tip_height = NodeSource::tip_id(&source).await?.height;
    let build_tip_block = source.fetch_block_at(tip_height).await?;
    let build_tip = BlockId::new(build_tip_block.height, build_tip_block.hash);
    let genesis_block = source.fetch_block_at(BlockHeight::new(0)).await?;
    let build_plan = CanonicalStoreBuildPlan::complete(
        activations,
        genesis_block.block_time_seconds,
        build_tip,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(LIVE_CANONICAL_REORG_WINDOW_BLOCKS)?,
    )?;
    let temporary_directory = tempdir()?;
    let canonical_primary_path = temporary_directory.path().join("canonical-primary");
    let builder = RocksDbCanonicalBuilder::create_fresh(
        &canonical_primary_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let construction = CanonicalConstructionConfig::for_local_tests(
        env.target.request_timeout,
        Arc::clone(activations),
    );
    let loaded = load_fresh_canonical(builder, &source, &construction).await?;
    let validated = loaded.builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        build_tip,
        UnixTimestampMillis::now(),
    ))?;
    let canonical_primary = validated.publish_baseline(publication)?;
    let mut store_fixture = WalletServingStoreFixture::from_published_canonical(
        temporary_directory,
        &canonical_primary_path,
        canonical_primary,
        activations,
    )?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
    )?));
    Ok((store_fixture, slot, tip_height))
}

/// Waits until `addr` accepts connections.
pub(crate) async fn await_grpc_endpoint(addr: SocketAddr) -> Result<()> {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(eyre!("gRPC endpoint {addr} did not become reachable"))
}
