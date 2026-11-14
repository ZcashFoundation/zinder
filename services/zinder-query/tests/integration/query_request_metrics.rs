#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use eyre::eyre;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio_stream::StreamExt as _;
use tonic::Request;
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockSelector, Network, RawTransactionBytes, ShieldedProtocol,
    SubtreeRootIndex, SubtreeRootRange, TransactionId, TransparentAddressScriptHash,
};
use zinder_proto::v1::wallet::{self, wallet_query_server::WalletQuery as WalletQueryService};
use zinder_query::{
    AdmittedIngestControl, MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
    TransparentAddressTxIdsInRangeRequest, TransparentAddressUnspentOutputsRequest,
    WalletEndpointMetadata, WalletQueryApi, WalletQueryGrpcAdapter, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_store::{ChainEventStreamFamily, EventStreamStartPosition, event_stream_start_message};
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
        let (query, _store_fixture) = shared_serving_query()?;
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

/// Operations that each `WalletServingQuery` request records exactly once.
const SERVING_QUERY_OPERATIONS: &[&str] = &[
    "network_upgrade_activations",
    "chain_value_pools_at_tip",
    "visible_tip_block",
    "settled_tip_block",
    "block_id_by_selector",
    "block_header_by_selector",
    "compact_block_at",
    "compact_blocks_in_range",
    "full_block_at",
    "full_blocks_in_range",
    "transaction_by_id",
    "transaction_at_block_index",
    "raw_transaction",
    "transparent_address_unspent_outputs",
    "transparent_address_tx_ids_in_range",
    "transparent_address_balance",
    "tree_state_at",
    "latest_tree_state_checkpoint",
    "subtree_roots_in_range",
    "broadcast_transaction",
];

#[test]
fn every_serving_query_request_records_exactly_one_sample() -> eyre::Result<()> {
    let exposition = render_metrics_of(async {
        let (query, _store_fixture) = shared_serving_query()?;
        request_every_block_read(&query).await;
        request_every_transaction_read(&query).await;
        request_every_transparent_read(&query).await;
        request_chain_event_reads(&query).await;
        Ok(())
    })?;

    for operation in SERVING_QUERY_OPERATIONS {
        assert_eq!(
            operation_count(&exposition, operation),
            1,
            "{operation} must record exactly one sample:\n{exposition}"
        );
    }
    for operation in ["chain_events", "resolve_chain_events_start"] {
        assert_eq!(
            operation_count(&exposition, operation),
            0,
            "the adapter stream records {operation}:\n{exposition}"
        );
    }
    assert_eq!(
        compact_block_range_sample_count(&exposition),
        1,
        "only compact_blocks_in_range samples its block count:\n{exposition}"
    );
    Ok(())
}

#[test]
fn transparent_address_balance_records_its_own_rejection() -> eyre::Result<()> {
    let exposition = render_metrics_of(async {
        let release = ReleaseAdapter::open().await?;
        let over_cap = (0..=MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES)
            .map(|index| {
                let mut script_hash = [0_u8; 32];
                script_hash[..4].copy_from_slice(&index.to_be_bytes());
                TransparentAddressScriptHash::from_bytes(script_hash)
            })
            .collect();
        if release
            .adapter_query
            .transparent_address_balance(over_cap, None)
            .await
            .is_ok()
        {
            return Err(eyre!("an over-cap balance request was served"));
        }
        Ok(())
    })?;

    assert_eq!(
        request_count(
            &exposition,
            "transparent_address_balance",
            "error",
            "transparent_balance_address_count_exceeded"
        ),
        1,
        "the domain read records its own rejection:\n{exposition}"
    );
    assert_eq!(
        operation_count(&exposition, "transparent_address_balance"),
        1,
        "{exposition}"
    );
    Ok(())
}

async fn request_every_block_read(query: &WalletServingQuery<()>) {
    let first = BlockHeight::new(1);
    let _ = query.network_upgrade_activations().await;
    let _ = query.chain_value_pools_at_tip().await;
    let _ = query.visible_tip_block(None).await;
    let _ = query.settled_tip_block(None).await;
    let _ = query
        .block_id_by_selector(BlockSelector::Height(first), None)
        .await;
    let _ = query
        .block_header_by_selector(BlockSelector::Height(first), None)
        .await;
    let _ = query.compact_block_at(first, None).await;
    let _ = query
        .compact_blocks_in_range(BlockHeightRange::inclusive(first, first), None)
        .await;
    let _ = query.full_block_at(first, None).await;
    let _ = query
        .full_blocks_in_range(BlockHeightRange::inclusive(first, first), None)
        .await;
    let _ = query.tree_state_at(first, None).await;
    let _ = query.latest_tree_state_checkpoint(None).await;
    let _ = query
        .subtree_roots(
            SubtreeRootRange::new(
                ShieldedProtocol::Sapling,
                SubtreeRootIndex::new(0),
                NonZeroU32::MIN,
            ),
            None,
        )
        .await;
}

async fn request_every_transaction_read(query: &WalletServingQuery<()>) {
    let transaction_id = TransactionId::from_bytes([0x22; 32]);
    let _ = query.transaction(transaction_id, None).await;
    let _ = query
        .transaction_at_block_index(BlockHeight::new(1), 0, None)
        .await;
    let _ = query.raw_transaction(transaction_id, None).await;
    let _ = query
        .broadcast_transaction(RawTransactionBytes::new(vec![0x00]))
        .await;
}

async fn request_every_transparent_read(query: &WalletServingQuery<()>) {
    let address_script_hash = TransparentAddressScriptHash::from_bytes([0x11; 32]);
    let _ = query
        .transparent_address_unspent_outputs(
            TransparentAddressUnspentOutputsRequest {
                address_script_hash,
                start_height: BlockHeight::new(0),
            },
            None,
        )
        .await;
    let _ = query
        .transparent_address_tx_ids_in_range(
            TransparentAddressTxIdsInRangeRequest {
                address_script_hash,
                start_height: BlockHeight::new(0),
                end_height: BlockHeight::new(2),
                max_entries: NonZeroU32::MIN,
                from_cursor: None,
            },
            None,
        )
        .await;
    let _ = query
        .transparent_address_balance(vec![address_script_hash], None)
        .await;
}

async fn request_chain_event_reads(query: &WalletServingQuery<()>) {
    let _ = query
        .resolve_chain_events_start(
            EventStreamStartPosition::EarliestRetained,
            ChainEventStreamFamily::Visible,
        )
        .await;
    let _ = query
        .chain_events(None, ChainEventStreamFamily::Visible)
        .await;
}

/// Shared serving query over a two-block fixture, with the fixture that must
/// outlive it.
fn shared_serving_query() -> eyre::Result<(WalletServingQuery<()>, WalletServingStoreFixture)> {
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
    Ok((query, store_fixture))
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

fn operation_count(exposition: &str, operation: &str) -> u64 {
    let label = format!("operation=\"{operation}\"");
    exposition
        .lines()
        .filter(|line| line.starts_with("zinder_query_request_total{"))
        .filter(|line| line.contains(label.as_str()))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

/// Sums the sample count of the block-count summary. The exporter renders
/// that count without a second `_count` suffix, so the count line is the one
/// that is neither a quantile nor the sum.
fn compact_block_range_sample_count(exposition: &str) -> u64 {
    exposition
        .lines()
        .filter(|line| line.starts_with("zinder_query_compact_block_range_block_count"))
        .filter(|line| !line.contains("quantile=") && !line.contains("_sum{"))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}
