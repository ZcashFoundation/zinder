//! Live regtest reorg coverage for the canonical writer and ingest stream.
//!
//! The test starts the production `RocksDbCanonicalStore` writer behind its
//! command channel, subscribes to `IngestControl.VisibleChainEvents`, then
//! invalidates a small suffix in Zebra. It proves that the writer publishes a
//! visible reorg envelope whose reverted range covers the invalidated blocks.

#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use std::{
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant},
};

use eyre::{Result, eyre};
use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use zinder_core::{BlockHeight, Network};
use zinder_ingest::{
    CanonicalConstructionConfig, CanonicalFollowConfig, CanonicalIngestControlGrpcAdapter,
    CanonicalPipelineLimits, CanonicalWriterConfig, LiveMempoolOwner, canonical_control_channel,
    run_canonical_writer_with_control,
};
use zinder_proto::v1::{
    ingest::ingest_control_client::IngestControlClient,
    wallet::{ChainEventEnvelope, chain_event_envelope},
};
use zinder_runtime::Readiness;
use zinder_source::NodeSource;
use zinder_store::{EventStreamStartPosition, RocksDbResourceBudget, event_stream_start_message};
use zinder_testkit::live::{init, require_live_for};

use crate::common::{
    fetch_live_network_upgrade_activations, fetch_live_tip_height, regtest_generate_blocks,
    rpc_block_hash_at_height, rpc_invalidate_block, rpc_reconsider_block,
    zebra_source_for_live_env,
};

const TIP_FOLLOW_POLL_INTERVAL: Duration = Duration::from_millis(50);
const REORG_WINDOW_BLOCKS: u32 = 100;
const REORG_DEPTH: u32 = 3;
const OBSERVE_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
#[expect(
    clippy::too_many_lines,
    reason = "one live proof keeps writer construction, Zebra invalidation, and canonical reorg observation together"
)]
async fn canonical_writer_publishes_visible_reorg_for_invalidated_zebra_suffix() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let source = zebra_source_for_live_env(&env)?;
    let activations = fetch_live_network_upgrade_activations(&env).await?;

    // Start construction from a current authenticated checkpoint and one new
    // block. This exercises the current writer without replaying the sidecar's
    // entire accumulated regtest history.
    let checkpoint_height = fetch_live_tip_height(&env).await?;
    regtest_generate_blocks(&env, 1).await?;

    let temporary = tempfile::tempdir()?;
    let storage_path = temporary.path().join("canonical");
    let cancel = CancellationToken::new();
    let readiness = Readiness::default();
    let writer_config = CanonicalWriterConfig {
        storage_path,
        resource_budget: RocksDbResourceBudget::for_local_tests(),
        construction: CanonicalConstructionConfig {
            request_timeout: env.target.request_timeout,
            pipeline_limits: CanonicalPipelineLimits::resolve(
                None,
                NonZeroU32::new(2).ok_or_else(|| eyre!("invalid test core count"))?,
                env.target.max_response_bytes,
            ),
            network_upgrade_activations: Arc::clone(&activations),
        },
        checkpoint_height: Some(checkpoint_height),
        raw_blob_retention: zinder_store::RawBlobRetention::Transactions,
        reorg_window_blocks: REORG_WINDOW_BLOCKS,
        follow: CanonicalFollowConfig {
            request_timeout: env.target.request_timeout,
            poll_interval: TIP_FOLLOW_POLL_INTERVAL,
            lag_threshold_blocks: 1,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
        },
    };
    let (canonical, commands) = canonical_control_channel();
    let writer_cancel = cancel.clone();
    let writer_readiness = readiness.clone();
    let writer_activations = Arc::clone(&activations);
    let writer_source = source.clone();
    let writer = tokio::spawn(async move {
        run_canonical_writer_with_control(
            &writer_source,
            writer_activations,
            writer_config,
            &writer_readiness,
            &writer_cancel,
            Some(commands),
        )
        .await
    });

    wait_for_writer(&canonical).await?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server_cancel = cancel.clone();
    let node_source: Arc<dyn NodeSource> = Arc::new(source.clone());
    let ingest_adapter = CanonicalIngestControlGrpcAdapter::new(
        env.network(),
        canonical,
        LiveMempoolOwner::default(),
        node_source,
        readiness,
    );
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ingest_adapter.into_server())
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                server_cancel.cancelled_owned(),
            )
            .await
    });

    let result = observe_reorg(&env, address).await;
    cancel.cancel();
    let server_result = server.await?;
    server_result?;
    let writer_result = writer.await?;
    writer_result?;
    result
}

async fn wait_for_writer(canonical: &zinder_ingest::CanonicalControlHandle) -> Result<()> {
    tokio::time::timeout(OBSERVE_TIMEOUT, canonical.writer_status())
        .await
        .map_err(|_| eyre!("canonical writer did not begin serving control requests"))?
        .map_err(|error| eyre!("canonical writer control request failed: {error}"))?;
    Ok(())
}

async fn observe_reorg(
    env: &zinder_testkit::live::LiveTestEnv,
    address: std::net::SocketAddr,
) -> Result<()> {
    let mut client = IngestControlClient::connect(format!("http://{address}")).await?;
    let mut events = client
        .visible_chain_events(event_stream_start_message(
            &EventStreamStartPosition::EarliestRetained,
        ))
        .await?
        .into_inner();

    let blocks_to_mine = REORG_DEPTH.saturating_add(2);
    let previous_tip = fetch_live_tip_height(env).await?;
    regtest_generate_blocks(env, blocks_to_mine).await?;
    let pre_reorg_tip = BlockHeight::new(previous_tip.value().saturating_add(blocks_to_mine));
    wait_for_visible_height(&mut events, pre_reorg_tip).await?;

    let reorg_floor = pre_reorg_tip.value().saturating_sub(REORG_DEPTH - 1);
    let invalidated_hash = rpc_block_hash_at_height(env, reorg_floor).await?;
    rpc_invalidate_block(env, &invalidated_hash).await?;
    let reorged = async {
        regtest_generate_blocks(env, blocks_to_mine).await?;
        wait_for_reorg(&mut events).await
    }
    .await;
    let _ = rpc_reconsider_block(env, &invalidated_hash).await;
    let reverted = reorged?
        .reverted
        .ok_or_else(|| eyre!("visible reorg envelope omitted its reverted range"))?;
    assert!(
        reverted.start_height <= reorg_floor,
        "reverted range must include the invalidated block: start={}, floor={reorg_floor}",
        reverted.start_height
    );
    assert!(
        reverted.end_height >= pre_reorg_tip.value(),
        "reverted range must reach the pre-reorg tip: end={}, tip={}",
        reverted.end_height,
        pre_reorg_tip.value()
    );
    assert_eq!(
        reverted
            .end_height
            .saturating_sub(reverted.start_height)
            .saturating_add(1),
        REORG_DEPTH,
        "reverted range must have the requested depth"
    );
    Ok(())
}

async fn wait_for_visible_height(
    events: &mut tonic::Streaming<ChainEventEnvelope>,
    target: BlockHeight,
) -> Result<()> {
    let started = Instant::now();
    while started.elapsed() < OBSERVE_TIMEOUT {
        let remaining = OBSERVE_TIMEOUT.saturating_sub(started.elapsed());
        match tokio::time::timeout(remaining, events.next()).await {
            Ok(Some(Ok(envelope)))
                if visible_event_end_height(&envelope) >= Some(target.value()) =>
            {
                return Ok(());
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(error))) => {
                return Err(eyre!("visible chain-event stream failed: {error}"));
            }
            Ok(None) => return Err(eyre!("visible chain-event stream ended before tip follow")),
            Err(_) => break,
        }
    }
    Err(eyre!(
        "visible chain-event stream did not reach height {} within {OBSERVE_TIMEOUT:?}",
        target.value()
    ))
}

async fn wait_for_reorg(
    events: &mut tonic::Streaming<ChainEventEnvelope>,
) -> Result<zinder_proto::v1::wallet::ChainReorged> {
    let started = Instant::now();
    while started.elapsed() < OBSERVE_TIMEOUT {
        let remaining = OBSERVE_TIMEOUT.saturating_sub(started.elapsed());
        match tokio::time::timeout(remaining, events.next()).await {
            Ok(Some(Ok(envelope))) => {
                if let Some(chain_event_envelope::Event::ChainReorged(reorged)) = envelope.event {
                    return Ok(reorged);
                }
            }
            Ok(Some(Err(error))) => {
                return Err(eyre!("visible chain-event stream failed: {error}"));
            }
            Ok(None) => return Err(eyre!("visible chain-event stream ended before reorg")),
            Err(_) => break,
        }
    }
    Err(eyre!(
        "visible chain-event stream did not publish a reorg within {OBSERVE_TIMEOUT:?}"
    ))
}

fn visible_event_end_height(envelope: &ChainEventEnvelope) -> Option<u32> {
    match envelope.event.as_ref()? {
        chain_event_envelope::Event::ChainCommitted(committed) => {
            committed.committed.as_ref().map(|range| range.end_height)
        }
        chain_event_envelope::Event::ChainReorged(reorged) => {
            reorged.committed.as_ref().map(|range| range.end_height)
        }
    }
}
