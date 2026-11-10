#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::sync::Arc;
use std::time::Duration;

use eyre::eyre;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio_stream::StreamExt as _;
use tonic::Request;
use zinder_core::{BlockHeight, BlockHeightRange, Network};
use zinder_proto::v1::wallet::{self, wallet_query_server::WalletQuery as WalletQueryService};
use zinder_query::{
    AdmittedIngestControl, WalletEndpointMetadata, WalletQueryApi, WalletQueryGrpcAdapter,
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_store::{EventStreamStartPosition, event_stream_start_message};
use zinder_testkit::{
    ChainFixture, IngestControlFixture, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

const FIRST_CHAIN_EVENT_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn release_native_reads_record_query_request_samples() -> eyre::Result<()> {
    let exposition = render_metrics_of(async {
        let release = ReleaseAdapter::open().await?;
        WalletQueryService::visible_tip_block(
            &release.adapter,
            Request::new(wallet::VisibleTipBlockRequest::default()),
        )
        .await?;
        let over_cap = WalletQueryService::compact_blocks_in_range(
            &release.adapter,
            Request::new(wallet::CompactBlocksInRangeRequest {
                start_height: 1,
                end_height: 1001,
                at_epoch_id: None,
            }),
        )
        .await;
        if over_cap.is_ok() {
            return Err(eyre!("over-cap compact block range was served"));
        }
        Ok(())
    })?;

    assert_eq!(
        request_count(&exposition, "visible_tip_block", "ok", "none"),
        1,
        "{exposition}"
    );
    assert_eq!(
        request_count(
            &exposition,
            "compact_blocks_in_range",
            "error",
            "block_range_too_large"
        ),
        1,
        "{exposition}"
    );
    assert!(
        exposition.contains("zinder_query_request_duration_seconds"),
        "{exposition}"
    );
    assert!(
        exposition.contains("zinder_query_compact_block_range_block_count"),
        "{exposition}"
    );
    Ok(())
}

#[test]
fn chain_event_streams_record_one_request_sample() -> eyre::Result<()> {
    let exposition = render_metrics_of(async {
        let release = ReleaseAdapter::open().await?;
        let mut events = WalletQueryService::chain_events(
            &release.adapter,
            Request::new(wallet::ChainEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::EarliestRetained,
                )),
                family: wallet::ChainEventStreamFamily::Visible as i32,
                address_filter: Vec::new(),
            }),
        )
        .await?
        .into_inner();
        tokio::time::timeout(FIRST_CHAIN_EVENT_TIMEOUT, events.next())
            .await?
            .ok_or_else(|| eyre!("chain-event stream closed before its first event"))??;
        // Direct domain page reads belong to the stream the adapter records.
        release
            .adapter_query
            .chain_events(None, zinder_store::ChainEventStreamFamily::Visible)
            .await?;
        Ok(())
    })?;

    assert_eq!(
        request_count(&exposition, "chain_events", "ok", "none"),
        1,
        "{exposition}"
    );
    Ok(())
}

#[test]
fn shared_serving_query_records_validation_failures() -> eyre::Result<()> {
    let exposition = render_metrics_of(async {
        let activations = Arc::new(sample_regtest_upgrade_activations());
        let mut store_fixture = WalletServingStoreFixture::from_chain(
            &ChainFixture::new(Network::ZcashRegtest).extend_blocks(2),
            activations.as_ref(),
        )?;
        let (canonical, wallet) = store_fixture.take_readers()?;
        let query = WalletServingQuery::from_serving_pair_slot(
            WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
                Arc::new(canonical),
                Arc::new(wallet),
            )?)),
            (),
            activations,
        );
        let inverted = query
            .compact_blocks_in_range(
                BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(1)),
                None,
            )
            .await;
        if inverted.is_ok() {
            return Err(eyre!("inverted compact block range was served"));
        }
        query.compact_block_at(BlockHeight::new(1), None).await?;
        Ok(())
    })?;

    assert_eq!(
        request_count(
            &exposition,
            "compact_blocks_in_range",
            "error",
            "invalid_block_range"
        ),
        1,
        "{exposition}"
    );
    assert_eq!(
        request_count(&exposition, "compact_block_at", "ok", "none"),
        1,
        "{exposition}"
    );
    Ok(())
}

/// Release native adapter over the admitted serving query, with the fixtures
/// that must outlive it.
struct ReleaseAdapter {
    adapter: WalletQueryGrpcAdapter<WalletServingQuery<(), AdmittedIngestControl>>,
    adapter_query: WalletServingQuery<(), AdmittedIngestControl>,
    _store_fixture: WalletServingStoreFixture,
    _ingest_control_fixture: IngestControlFixture,
}

impl ReleaseAdapter {
    async fn open() -> eyre::Result<Self> {
        let activations = Arc::new(sample_regtest_upgrade_activations());
        let mut store_fixture = WalletServingStoreFixture::from_chain(
            &ChainFixture::new(Network::ZcashRegtest).extend_blocks(2),
            activations.as_ref(),
        )?;
        let (canonical, wallet) = store_fixture.take_readers()?;
        let (ingest_control, ingest_control_fixture) =
            crate::common::admitted_ingest_control_fixture().await?;
        let query = WalletServingQuery::from_admitted_native_serving_pair(
            WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
                Arc::new(canonical),
                Arc::new(wallet),
            )?)),
            (),
            ingest_control,
            activations,
        );
        Ok(Self {
            adapter: WalletQueryGrpcAdapter::new(query.clone(), WalletEndpointMetadata::default()),
            adapter_query: query,
            _store_fixture: store_fixture,
            _ingest_control_fixture: ingest_control_fixture,
        })
    }
}

/// Runs `scenario` on a current-thread runtime so every request outcome is
/// recorded on the thread that owns the local recorder.
fn render_metrics_of(
    scenario: impl std::future::Future<Output = eyre::Result<()>>,
) -> eyre::Result<String> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    metrics::with_local_recorder(&recorder, || runtime.block_on(scenario))?;
    Ok(handle.render())
}

fn request_count(exposition: &str, operation: &str, status: &str, error_class: &str) -> u64 {
    let labels = [
        format!("operation=\"{operation}\""),
        format!("status=\"{status}\""),
        format!("error_class=\"{error_class}\""),
    ];
    exposition
        .lines()
        .filter(|line| line.starts_with("zinder_query_request_total{"))
        .filter(|line| labels.iter().all(|label| line.contains(label.as_str())))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}
