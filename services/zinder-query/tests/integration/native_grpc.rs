#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::{net::SocketAddr, pin::Pin, time::Duration};

use eyre::eyre;
use tokio::net::TcpListener;
use tokio_stream::{Stream, StreamExt as _, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Response, Status, transport::Server};
use tonic_types::StatusExt;
use zinder_core::wire::{encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex};
use zinder_core::{
    BlockHeight, ChainEpoch, ChainEpochId, MAX_SUBTREE_ROOTS_PER_REQUEST, Network, TransactionId,
    TransparentAddressScriptHash, TransparentOutPoint, TransparentUnspentOutput,
};
use zinder_proto::capabilities::{
    INGEST_CONTROL_MEMPOOL_EVENTS_V2, INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3,
    INGEST_CONTROL_MEMPOOL_TRANSACTION_V2, INGEST_CONTROL_SERVER_INFO_V1,
    INGEST_CONTROL_TRANSPARENT_MEMPOOL_OUTPUTS_BY_ADDRESS_V1,
    INGEST_CONTROL_TRANSPARENT_MEMPOOL_SPENDS_BY_OUTPOINT_V1, INGEST_CONTROL_WRITER_STATUS_V1,
    WALLET_ADDRESS_TRANSPARENT_BALANCE_V1, WALLET_BROADCAST_TRANSACTION_V1, WALLET_EVENTS_CHAIN_V1,
    WALLET_EVENTS_MEMPOOL_V2, WALLET_MEMPOOL_TRANSPARENT_OUTPUTS_BY_ADDRESS_V1,
    WALLET_MEMPOOL_TRANSPARENT_SPENDS_BY_OUTPOINT_V1, WALLET_READ_FULL_BLOCK_AT_V1,
    WALLET_READ_TRANSACTION_BY_ID_V2, WALLET_READ_TRANSACTION_BYTES_V1, WALLET_SNAPSHOT_MEMPOOL_V3,
};
use zinder_proto::v1::{
    ingest::{
        MempoolTransactionRequest, WriterPhase, WriterStatusRequest, WriterStatusResponse,
        ingest_control_server::{IngestControl, IngestControlServer},
    },
    ops,
    wallet::{self, wallet_query_server::WalletQuery as WalletQueryService},
};
use zinder_proto::wire::TransactionAbsence;
use zinder_query::{
    AdmittedIngestControl, DEFAULT_MAX_COMPACT_BLOCK_RANGE, IngestControlAdmissionError,
    WalletEndpointMetadata, WalletQueryApi, WalletQueryGrpcAdapter, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_store::{
    EventStreamStartPosition, RawBlobRetention, StreamCursorTokenV1, chain_view_message,
    event_stream_start_message,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
    MockTransactionBroadcaster, WalletServingStoreFixture, sample_regtest_upgrade_activations,
};

/// Operational upper bound used to prove that ingest-control establishment is finite without
/// exposing the implementation timeout as public API.
const TEST_ADVANCE_PAST_INGEST_CONTROL_REQUEST_BOUND: Duration = Duration::from_mins(1);

use crate::common::{native_serving_query_from, native_serving_query_over};

#[tokio::test]
async fn ingest_control_admission_accepts_exact_runtime_identity_and_capabilities()
-> eyre::Result<()> {
    let (ingest_control_addr, cancel, server_handle) =
        spawn_ingest_control_server(StaticIngestControl::new()).await?;

    let _admitted = AdmittedIngestControl::connect(
        &format!("http://{ingest_control_addr}"),
        None,
        Network::ZcashRegtest,
    )
    .await?;

    cancel.cancel();
    server_handle.await??;
    Ok(())
}

#[tokio::test]
async fn native_mempool_snapshot_forwards_typed_stale_status_unchanged() -> eyre::Result<()> {
    let stale_status = zinder_proto::status_for_reason(
        zinder_proto::v1::ops::ErrorReason::ChainEpochPinUnavailable,
        "requested chain epoch is no longer available",
    );
    let (adapter, _store_fixture, cancel, server_task) =
        native_serving_adapter_with_ingest_control(
            StaticIngestControl::new().with_mempool_snapshot_status(stale_status),
        )
        .await?;

    let status = WalletQueryService::mempool_snapshot(
        &adapter,
        Request::new(wallet::MempoolSnapshotRequest {
            max_entries: 1,
            from_cursor: Vec::new(),
        }),
    )
    .await
    .err()
    .ok_or_else(|| eyre!("typed stale snapshot status was not forwarded"))?;
    assert_eq!(status.code(), Code::FailedPrecondition);
    let status_details = status.get_error_details();
    let error_info = status_details
        .error_info()
        .ok_or_else(|| eyre!("typed stale snapshot omitted ErrorInfo"))?;
    assert_eq!(error_info.domain, zinder_proto::ZINDER_ERROR_DOMAIN);
    assert_eq!(
        error_info.reason,
        zinder_proto::v1::ops::ErrorReason::ChainEpochPinUnavailable.as_str_name()
    );

    cancel.cancel();
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn ingest_control_admission_rejects_a_different_service_identity() -> eyre::Result<()> {
    let ingest_control = StaticIngestControl::new().with_service_name("not-zinder-ingest");
    let (ingest_control_addr, cancel, server_handle) =
        spawn_ingest_control_server(ingest_control).await?;

    let outcome = AdmittedIngestControl::connect(
        &format!("http://{ingest_control_addr}"),
        None,
        Network::ZcashRegtest,
    )
    .await;

    assert!(matches!(
        outcome,
        Err(IngestControlAdmissionError::ServiceNameMismatch { .. })
    ));
    cancel.cancel();
    server_handle.await??;
    Ok(())
}

#[tokio::test]
async fn ingest_control_admission_rejects_missing_identity_payload() -> eyre::Result<()> {
    let (ingest_control_addr, cancel, server_handle) =
        spawn_ingest_control_server(StaticIngestControl::new().without_server_info()).await?;

    let outcome = AdmittedIngestControl::connect(
        &format!("http://{ingest_control_addr}"),
        None,
        Network::ZcashRegtest,
    )
    .await;

    assert!(matches!(
        outcome,
        Err(IngestControlAdmissionError::ServerInfoMissing)
    ));
    cancel.cancel();
    server_handle.await??;
    Ok(())
}

#[tokio::test]
async fn ingest_control_admission_rejects_a_different_network() -> eyre::Result<()> {
    let ingest_control = StaticIngestControl::new().with_network("zcash-testnet");
    let (ingest_control_addr, cancel, server_handle) =
        spawn_ingest_control_server(ingest_control).await?;

    let outcome = AdmittedIngestControl::connect(
        &format!("http://{ingest_control_addr}"),
        None,
        Network::ZcashRegtest,
    )
    .await;

    assert!(matches!(
        outcome,
        Err(IngestControlAdmissionError::NetworkMismatch { .. })
    ));
    cancel.cancel();
    server_handle.await??;
    Ok(())
}

#[tokio::test]
async fn ingest_control_admission_rejects_a_different_contract_revision() -> eyre::Result<()> {
    let ingest_control =
        StaticIngestControl::new().with_contract_revision(zinder_proto::CONTRACT_REVISION + 1);
    let (ingest_control_addr, cancel, server_handle) =
        spawn_ingest_control_server(ingest_control).await?;

    let outcome = AdmittedIngestControl::connect(
        &format!("http://{ingest_control_addr}"),
        None,
        Network::ZcashRegtest,
    )
    .await;

    assert!(matches!(
        outcome,
        Err(IngestControlAdmissionError::ContractRevisionMismatch { .. })
    ));
    cancel.cancel();
    server_handle.await??;
    Ok(())
}

#[tokio::test]
async fn ingest_control_admission_reports_each_missing_required_capability() -> eyre::Result<()> {
    for missing_capability in [
        INGEST_CONTROL_SERVER_INFO_V1,
        INGEST_CONTROL_WRITER_STATUS_V1,
        INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3,
        INGEST_CONTROL_MEMPOOL_TRANSACTION_V2,
        INGEST_CONTROL_MEMPOOL_EVENTS_V2,
        INGEST_CONTROL_TRANSPARENT_MEMPOOL_OUTPUTS_BY_ADDRESS_V1,
        INGEST_CONTROL_TRANSPARENT_MEMPOOL_SPENDS_BY_OUTPOINT_V1,
    ] {
        let ingest_control = StaticIngestControl::new().without_capability(missing_capability);
        let (ingest_control_addr, cancel, server_handle) =
            spawn_ingest_control_server(ingest_control).await?;

        let outcome = AdmittedIngestControl::connect(
            &format!("http://{ingest_control_addr}"),
            None,
            Network::ZcashRegtest,
        )
        .await;

        assert!(matches!(
            outcome,
            Err(IngestControlAdmissionError::CapabilitiesMissing { missing })
                if missing == [missing_capability]
        ));
        cancel.cancel();
        server_handle.await??;
    }
    Ok(())
}

#[tokio::test]
async fn ingest_control_admission_reports_transport_failure() {
    let outcome =
        AdmittedIngestControl::connect("http://127.0.0.1:1", None, Network::ZcashRegtest).await;

    assert!(matches!(
        outcome,
        Err(IngestControlAdmissionError::Connect(_))
    ));
}

#[tokio::test]
async fn native_grpc_service_returns_wallet_reads_from_stored_artifacts() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let (wallet_query, _store_fixture, _ingest_control_fixture) =
        native_serving_query_over(&chain).await?;
    let expected_compact_block = wallet_query
        .compact_block_at(zinder_core::BlockHeight::new(1), None)
        .await?;
    let grpc_adapter = WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default());
    let grpc_responses = read_wallet_grpc_responses(&grpc_adapter).await?;

    assert_wallet_grpc_response_epochs(
        &grpc_responses,
        expected_compact_block.chain_epoch.id.value(),
    );
    assert_eq!(
        grpc_responses
            .visible_tip_block
            .visible_tip_block
            .ok_or_else(|| eyre!("missing latest block"))?
            .height,
        1
    );
    assert_eq!(
        grpc_responses.compact_block_range,
        vec![zinder_proto::wire::compact_block_message(
            &expected_compact_block.compact_block
        )]
    );
    assert_eq!(grpc_responses.latest_tree_state_checkpoint.height, 1);
    assert_eq!(
        grpc_responses.latest_tree_state_checkpoint.payload_bytes,
        b"{}"
    );
    assert!(
        grpc_responses.subtree_roots.subtree_roots.is_empty(),
        "a chain without completed Sapling subtrees serves an empty root range"
    );
    assert!(
        grpc_responses
            .network_upgrade_activations
            .activations
            .iter()
            .any(|activation| activation.name == "NU6.2")
    );

    Ok(())
}

#[tokio::test]
async fn native_grpc_service_checks_range_limit_before_opening_reader() -> eyre::Result<()> {
    let (grpc_adapter, _store_fixture, _ingest_control_fixture) = native_adapter().await?;
    let over_limit = DEFAULT_MAX_COMPACT_BLOCK_RANGE.get().saturating_add(1);

    let status = match WalletQueryService::compact_blocks_in_range(
        &grpc_adapter,
        Request::new(wallet::CompactBlocksInRangeRequest {
            start_height: 1,
            end_height: over_limit,
            at_epoch_id: None,
        }),
    )
    .await
    {
        Ok(_response) => return Err(eyre!("expected range error, got success")),
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::InvalidArgument);

    Ok(())
}

#[tokio::test]
async fn native_grpc_service_rejects_oversized_subtree_root_range() -> eyre::Result<()> {
    let (grpc_adapter, _store_fixture, _ingest_control_fixture) = native_adapter().await?;
    let requested = MAX_SUBTREE_ROOTS_PER_REQUEST.saturating_add(1);

    let status = match WalletQueryService::subtree_roots(
        &grpc_adapter,
        Request::new(wallet::SubtreeRootsRequest {
            shielded_protocol: wallet::ShieldedProtocol::Sapling as i32,
            start_index: 0,
            max_entries: requested,
            at_epoch_id: None,
        }),
    )
    .await
    {
        Ok(response) => {
            return Err(eyre!("expected subtree-root range error, got {response:?}"));
        }
        Err(status) => status,
    };
    let details = status.get_error_details();
    let reason = details
        .error_info()
        .map(|error_info| error_info.reason.as_str());
    let violation = details
        .bad_request()
        .and_then(|bad_request| bad_request.field_violations.first());

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(reason, Some("SUBTREE_ROOT_RANGE_TOO_LARGE"));
    assert!(matches!(
        violation,
        Some(violation)
            if violation.field == "max_entries"
                && violation.description.contains("maximum is 1024")
    ));
    assert!(status.message().contains("maximum 1024"));
    Ok(())
}

#[tokio::test]
async fn native_grpc_service_maps_missing_artifacts_to_not_found() -> eyre::Result<()> {
    let (grpc_adapter, _store_fixture, _ingest_control_fixture) = native_adapter().await?;

    let tree_state_status = match WalletQueryService::tree_state_at_height(
        &grpc_adapter,
        Request::new(wallet::TreeStateAtHeightRequest {
            height: 1,
            at_epoch_id: None,
        }),
    )
    .await
    {
        Ok(response) => return Err(eyre!("expected tree-state error, got {response:?}")),
        Err(status) => status,
    };
    let missing_block_status = match WalletQueryService::compact_block(
        &grpc_adapter,
        Request::new(wallet::CompactBlockRequest {
            height: 99,
            at_epoch_id: None,
        }),
    )
    .await
    {
        Ok(response) => return Err(eyre!("expected compact-block error, got {response:?}")),
        Err(status) => status,
    };

    assert_eq!(tree_state_status.code(), Code::FailedPrecondition);
    assert_eq!(missing_block_status.code(), Code::NotFound);

    Ok(())
}

#[tokio::test]
async fn pinned_transaction_miss_is_a_proven_canonical_absence() -> eyre::Result<()> {
    let (grpc_adapter, _store_fixture, _ingest_control_fixture) = native_adapter().await?;
    let chain_epoch_id = visible_chain_epoch(&grpc_adapter).await?.id;
    let requested_transaction_id = TransactionId::from_bytes([0x45; 32]);

    // A pinned lookup never consults the mempool, so it proves a canonical
    // absence at that epoch and says the mempool was not searched.
    let pinned = transaction_status(
        &grpc_adapter,
        requested_transaction_id,
        Some(chain_epoch_id),
    )
    .await?;
    let absence = TransactionAbsence::from_status(&pinned)?
        .ok_or_else(|| eyre!("pinned canonical miss was not a typed absence: {pinned:?}"))?;
    assert_eq!(pinned.code(), Code::NotFound);
    assert_eq!(absence.chain_epoch_id, chain_epoch_id);
    assert!(!absence.covers_mempool);

    Ok(())
}

#[tokio::test]
async fn transaction_absence_requires_a_mempool_miss_at_the_same_epoch() -> eyre::Result<()> {
    let ingest_control = StaticIngestControl::new();
    let mempool_miss = ingest_control.clone();
    let (adapter, _store_fixture, cancel, server_task) =
        native_serving_adapter_with_ingest_control(ingest_control).await?;
    let chain_epoch = visible_chain_epoch(&adapter).await?;
    let missing_transaction_id = TransactionId::from_bytes([0x46; 32]);

    // A mempool miss at the replica's epoch completes a proven absence.
    mempool_miss.set_mempool_transaction_miss(
        TransactionAbsence::at_epoch(chain_epoch, true).into_status("not in the test mempool"),
    );
    let proven = transaction_status(&adapter, missing_transaction_id, None).await?;
    assert_eq!(
        TransactionAbsence::from_status(&proven)?,
        Some(TransactionAbsence::at_epoch(chain_epoch, true))
    );

    // A writer at another epoch, ahead because this replica lags or behind
    // because the writer restarted, describes a different chain than the
    // canonical read. The transaction may have moved between the two views, so
    // the lookup fails and names the writer's epoch.
    for writer_epoch_id in [chain_epoch.id.value() + 1, chain_epoch.id.value() - 1] {
        let writer_epoch = ChainEpoch {
            id: ChainEpochId::new(writer_epoch_id),
            ..chain_epoch
        };
        mempool_miss.set_mempool_transaction_miss(
            TransactionAbsence::at_epoch(writer_epoch, true).into_status("not in the test mempool"),
        );
        let mismatched = transaction_status(&adapter, missing_transaction_id, None).await?;
        assert_eq!(mismatched.code(), Code::NotFound);
        assert_eq!(
            error_reason(&mismatched),
            Some(ops::ErrorReason::ChainEpochMissing)
        );
        assert_eq!(
            mismatched
                .get_error_details()
                .resource_info()
                .map(|resource| resource.resource_name.clone()),
            Some(format!("chain_epoch:{writer_epoch_id}"))
        );
        assert_eq!(TransactionAbsence::from_status(&mismatched)?, None);
    }

    // A writer that answers without the typed absence keeps the ambiguous
    // legacy shape rather than becoming an absence.
    mempool_miss.set_mempool_transaction_miss(Status::not_found("legacy mempool miss"));
    let legacy = transaction_status(&adapter, missing_transaction_id, None).await?;
    assert_eq!(legacy.code(), Code::NotFound);
    assert_eq!(
        error_reason(&legacy),
        Some(ops::ErrorReason::ArtifactUnavailable)
    );
    assert!(legacy.get_error_details().resource_info().is_none());

    // An unavailable mempool view is an error, never an absence.
    mempool_miss.set_mempool_transaction_miss(Status::unavailable("mempool is hydrating"));
    let unavailable = transaction_status(&adapter, missing_transaction_id, None).await?;
    assert_eq!(unavailable.code(), Code::Unavailable);
    assert_eq!(TransactionAbsence::from_status(&unavailable)?, None);

    cancel.cancel();
    server_task.await??;
    Ok(())
}

/// A checkpointed store cannot prove a transaction absent, but a pending
/// transaction needs no such proof.
#[tokio::test]
async fn history_floor_miss_still_reports_a_pending_transaction() -> eyre::Result<()> {
    let pending_transaction_id = TransactionId::from_bytes([0x47; 32]);
    let unknown_transaction_id = TransactionId::from_bytes([0x48; 32]);
    let ingest_control = StaticIngestControl::new().with_mempool_transaction_response(
        in_mempool_status_response(pending_transaction_id, vec![0x04, 0x05]),
    );
    let mempool = ingest_control.clone();
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_history_floor(BlockHeight::new(2)),
        &sample_regtest_upgrade_activations(),
    )?;
    let (adapter, _store_fixture, cancel, server_task) =
        native_serving_adapter_over(writer.into_serving_store_fixture()?, ingest_control).await?;
    let chain_epoch_id = visible_chain_epoch(&adapter).await?.id;
    let first_available_height = BlockHeight::new(3);

    // An unpinned lookup reports a pending transaction from the mempool.
    let pending = WalletQueryService::transaction(
        &adapter,
        Request::new(wallet::TransactionRequest {
            transaction_id: encode_rpc_transaction_id_hex(pending_transaction_id),
            at_epoch_id: None,
        }),
    )
    .await?
    .into_inner();
    assert_eq!(
        in_mempool_transaction_id(&pending),
        Some(encode_rpc_transaction_id_hex(pending_transaction_id).as_str())
    );

    // A mempool miss leaves the history floor standing, because the
    // transaction may be mined below it.
    let unknown = transaction_status(&adapter, unknown_transaction_id, None).await?;
    assert_history_floor(&unknown, first_available_height)?;
    assert_eq!(mempool.mempool_transaction_call_count(), 2);

    // A pinned lookup is a canonical read and never consults the mempool.
    let pinned = transaction_status(&adapter, pending_transaction_id, Some(chain_epoch_id)).await?;
    assert_history_floor(&pinned, first_available_height)?;
    assert_eq!(mempool.mempool_transaction_call_count(), 2);

    // A transient mempool failure keeps its own status, so a retry can still
    // find the transaction pending.
    mempool.set_mempool_transaction_miss(Status::unavailable("mempool is hydrating"));
    let unavailable = transaction_status(&adapter, unknown_transaction_id, None).await?;
    assert_eq!(unavailable.code(), Code::Unavailable);

    cancel.cancel();
    server_task.await??;
    Ok(())
}

fn assert_history_floor(status: &Status, first_available_height: BlockHeight) -> eyre::Result<()> {
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(
        error_reason(status),
        Some(ops::ErrorReason::CanonicalHistoryUnavailable)
    );
    assert_eq!(
        zinder_proto::wire::canonical_history_first_available_height(status)?,
        Some(first_available_height)
    );
    assert_eq!(TransactionAbsence::from_status(status)?, None);
    Ok(())
}

async fn transaction_status<Q>(
    adapter: &WalletQueryGrpcAdapter<Q>,
    transaction_id: TransactionId,
    at_epoch_id: Option<ChainEpochId>,
) -> eyre::Result<Status>
where
    WalletQueryGrpcAdapter<Q>: WalletQueryService,
{
    match WalletQueryService::transaction(
        adapter,
        Request::new(wallet::TransactionRequest {
            transaction_id: encode_rpc_transaction_id_hex(transaction_id),
            at_epoch_id: at_epoch_id.map(ChainEpochId::value),
        }),
    )
    .await
    {
        Ok(response) => Err(eyre!(
            "expected a transaction status error, got {response:?}"
        )),
        Err(status) => Ok(status),
    }
}

async fn visible_chain_epoch<Q>(adapter: &WalletQueryGrpcAdapter<Q>) -> eyre::Result<ChainEpoch>
where
    WalletQueryGrpcAdapter<Q>: WalletQueryService,
{
    let chain_epoch = WalletQueryService::visible_tip_block(
        adapter,
        Request::new(wallet::VisibleTipBlockRequest { at_epoch_id: None }),
    )
    .await?
    .into_inner()
    .chain_view
    .and_then(|chain_view| chain_view.chain_epoch)
    .ok_or_else(|| eyre!("visible tip response omitted its chain epoch"))?;
    Ok(zinder_proto::wire::chain_epoch_from_message(chain_epoch)?)
}

fn error_reason(status: &Status) -> Option<ops::ErrorReason> {
    status
        .get_error_details()
        .error_info()
        .and_then(|error_info| ops::ErrorReason::from_str_name(&error_info.reason))
}

#[tokio::test]
async fn native_grpc_service_streams_chain_events_from_the_store() -> eyre::Result<()> {
    let (grpc_adapter, _store_fixture, _ingest_control_fixture) = native_adapter().await?;

    let mut event_stream = WalletQueryService::chain_events(
        &grpc_adapter,
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
    let first_event = event_stream
        .next()
        .await
        .ok_or_else(|| eyre!("chain-events stream closed before first event"))??;

    assert_eq!(first_event.event_sequence, 1);
    assert_eq!(
        first_event
            .chain_view
            .and_then(|chain_view| chain_view.chain_epoch)
            .ok_or_else(|| eyre!("missing chain epoch"))?
            .chain_epoch_id,
        1
    );
    assert!(matches!(
        first_event.event,
        Some(wallet::chain_event_envelope::Event::ChainCommitted(committed))
            if committed.committed.as_ref().is_some_and(|inner| {
                inner.start_height == 1 && inner.end_height == 1
            })
    ));
    assert!(!first_event.cursor.is_empty());

    Ok(())
}

/// A cursor whose next event was pruned from the canonical store expires.
///
/// The cursor is taken from a serving pair before the writer prunes; a pair
/// admitted after the prune reports it expired with the new retention floor.
#[tokio::test]
async fn native_grpc_service_expires_pruned_chain_event_cursors() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).through(BlockHeight::new(1)),
        &sample_regtest_upgrade_activations(),
    )?;
    writer.append_live_block(&chain)?;
    writer.append_live_block(&chain)?;
    let (before_prune, _before_prune_store, _before_prune_ingest_control) =
        native_serving_query_from(writer.serving_store_fixture()?).await?;
    let first_cursor = earliest_chain_event_cursor(&WalletQueryGrpcAdapter::new(
        before_prune,
        WalletEndpointMetadata::default(),
    ))
    .await?;

    let retention = writer.prune_canonical_events_before(3)?;
    assert_eq!(retention.oldest_retained_sequence, 3);
    let (after_prune, _after_prune_store, _after_prune_ingest_control) =
        native_serving_query_from(writer.serving_store_fixture()?).await?;
    let grpc_adapter = WalletQueryGrpcAdapter::new(after_prune, WalletEndpointMetadata::default());
    let request = Request::new(wallet::ChainEventsRequest {
        start: Some(event_stream_start_message(
            &EventStreamStartPosition::AfterCursor(StreamCursorTokenV1::from_bytes(first_cursor)),
        )),
        family: wallet::ChainEventStreamFamily::Visible as i32,
        address_filter: Vec::new(),
    });
    let status = match WalletQueryService::chain_events(&grpc_adapter, request).await {
        Err(status) => status,
        Ok(response) => match response.into_inner().next().await {
            Some(Err(status)) => status,
            Some(Ok(event)) => return Err(eyre!("expected cursor expiry, got event {event:?}")),
            None => return Err(eyre!("chain-events stream closed before cursor error")),
        },
    };
    let details = status.get_error_details();
    let violation = details
        .precondition_failure()
        .and_then(|failure| failure.violations.first())
        .cloned();

    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        matches!(
            &violation,
            Some(violation)
                if violation.r#type == "CHAIN_EVENT_CURSOR_EXPIRED"
                    && violation.subject == "chain_event:1"
                    && violation.description.contains('3')
        ),
        "unexpected cursor-expiry violation: {violation:?}"
    );

    Ok(())
}

/// Returns the cursor of the first event retained by the adapter's store.
async fn earliest_chain_event_cursor(grpc_adapter: &NativeServingAdapter) -> eyre::Result<Vec<u8>> {
    let mut event_stream = WalletQueryService::chain_events(
        grpc_adapter,
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
    let first_event = event_stream
        .next()
        .await
        .ok_or_else(|| eyre!("chain-events stream closed before the first event"))??;
    assert_eq!(first_event.event_sequence, 1);
    Ok(first_event.cursor)
}

#[tokio::test]
async fn native_grpc_service_rejects_unset_event_stream_start() -> eyre::Result<()> {
    let (grpc_adapter, _store_fixture, _ingest_control_fixture) = native_adapter().await?;

    for start in [None, Some(wallet::EventStreamStart { position: None })] {
        let status = match WalletQueryService::chain_events(
            &grpc_adapter,
            Request::new(wallet::ChainEventsRequest {
                start,
                family: wallet::ChainEventStreamFamily::Visible as i32,
                address_filter: Vec::new(),
            }),
        )
        .await
        {
            Ok(_response) => return Err(eyre!("expected unset-start rejection")),
            Err(status) => status,
        };
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    Ok(())
}

#[tokio::test]
async fn native_grpc_service_honors_request_epoch_pin() -> eyre::Result<()> {
    let (grpc_adapter, _store_fixture, _ingest_control_fixture) = native_adapter().await?;
    let current_epoch_id = 1;

    let response = WalletQueryService::visible_tip_block(
        &grpc_adapter,
        Request::new(wallet::VisibleTipBlockRequest {
            at_epoch_id: Some(current_epoch_id),
        }),
    )
    .await?
    .into_inner();
    let response_epoch = response
        .chain_view
        .clone()
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| eyre!("missing response chain epoch"))?;
    let visible_tip_block = response
        .visible_tip_block
        .ok_or_else(|| eyre!("missing latest block"))?;
    assert_eq!(response_epoch.chain_epoch_id, current_epoch_id);
    assert_eq!(visible_tip_block.height, 1);

    let unavailable = WalletQueryService::visible_tip_block(
        &grpc_adapter,
        Request::new(wallet::VisibleTipBlockRequest {
            at_epoch_id: Some(current_epoch_id + 1),
        }),
    )
    .await
    .err()
    .ok_or_else(|| eyre!("a pin to a non-current epoch must fail closed"))?;
    assert_eq!(unavailable.code(), Code::FailedPrecondition);

    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "each retention case proves binding-time discovery and its canonical, live, pinned, and full-block call paths together"
)]
async fn native_serving_pair_capabilities_and_calls_follow_raw_blob_retention() -> eyre::Result<()>
{
    for raw_blob_retention in [
        RawBlobRetention::None,
        RawBlobRetention::Transactions,
        RawBlobRetention::All,
    ] {
        assert_native_retention_case(raw_blob_retention).await?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one retention case proves its capability descriptor and every canonical, live, pinned, snapshot, and full-block outcome as one contract"
)]
async fn assert_native_retention_case(raw_blob_retention: RawBlobRetention) -> eyre::Result<()> {
    let (expect_transaction_bytes, expect_full_blocks) = match raw_blob_retention {
        RawBlobRetention::None => (false, false),
        RawBlobRetention::Transactions => (true, false),
        RawBlobRetention::All => (true, true),
    };
    let mined_transaction_id = TransactionId::from_bytes([0x78; 32]);
    let mempool_transaction_id = TransactionId::from_bytes([0x89; 32]);
    let mined_raw_transaction_bytes =
        format!("mined-transaction-{raw_blob_retention}").into_bytes();
    let mempool_raw_transaction_bytes = vec![0x01, 0x02, 0x03];
    let ingest_control =
        StaticIngestControl::new().with_mempool_transaction_response(in_mempool_status_response(
            mempool_transaction_id,
            mempool_raw_transaction_bytes.clone(),
        ));
    let ingest_control_observer = ingest_control.clone();
    let (ingest_control_addr, cancel, server_handle) =
        spawn_ingest_control_server(ingest_control).await?;
    let admitted_ingest_control = AdmittedIngestControl::connect(
        &format!("http://{ingest_control_addr}"),
        None,
        Network::ZcashRegtest,
    )
    .await?;
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(raw_blob_retention)
        .extend_blocks(1);
    let block = chain
        .blocks()
        .first()
        .cloned()
        .ok_or_else(|| eyre!("retention fixture omitted its canonical block"))?;
    let chain = chain.with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
        mined_transaction_id,
        block.height,
        block.hash,
        0,
        mined_raw_transaction_bytes.clone(),
    ));
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical),
        Arc::new(wallet),
    )?);
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(serving_pair),
        (),
        admitted_ingest_control,
        Arc::clone(&activations),
    );
    let grpc_adapter = WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default());

    let server_info =
        WalletQueryService::server_info(&grpc_adapter, Request::new(wallet::ServerInfoRequest {}))
            .await?
            .into_inner()
            .info
            .ok_or_else(|| eyre!("native ServerInfo omitted its binding-time descriptor"))?;
    assert!(
        has_capability(&server_info, WALLET_READ_TRANSACTION_BY_ID_V2),
        "{raw_blob_retention} retention must admit typed canonical and live transaction lookup"
    );
    for live_capability in [WALLET_SNAPSHOT_MEMPOOL_V3, WALLET_EVENTS_MEMPOOL_V2] {
        assert!(
            has_capability(&server_info, live_capability),
            "{raw_blob_retention} retention omitted admitted live capability {live_capability}"
        );
    }
    assert_eq!(
        has_capability(&server_info, WALLET_READ_TRANSACTION_BYTES_V1),
        expect_transaction_bytes,
        "{raw_blob_retention} retention advertised incorrect mined transaction-byte support"
    );
    assert_eq!(
        has_capability(&server_info, WALLET_READ_FULL_BLOCK_AT_V1),
        expect_full_blocks,
        "{raw_blob_retention} retention advertised incorrect full-block support"
    );

    let mempool_snapshot = WalletQueryService::mempool_snapshot(
        &grpc_adapter,
        Request::new(wallet::MempoolSnapshotRequest {
            max_entries: 1,
            from_cursor: Vec::new(),
        }),
    )
    .await?
    .into_inner();
    assert!(
        mempool_snapshot.entries.is_empty(),
        "{raw_blob_retention} retention returned an unexpected fixture mempool entry"
    );
    assert_eq!(
        mempool_snapshot.source_tip,
        mempool_snapshot
            .chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
            .and_then(|chain_epoch| chain_epoch.visible_tip.clone()),
        "{raw_blob_retention} retention changed the admitted ingest snapshot identity"
    );

    let canonical_response = WalletQueryService::transaction(
        &grpc_adapter,
        Request::new(wallet::TransactionRequest {
            transaction_id: encode_rpc_transaction_id_hex(mined_transaction_id),
            at_epoch_id: None,
        }),
    )
    .await?
    .into_inner();
    let canonical_epoch_id = canonical_response
        .chain_view
        .as_ref()
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .map(|chain_epoch| chain_epoch.chain_epoch_id)
        .ok_or_else(|| eyre!("canonical transaction response omitted its chain epoch"))?;
    let mined = match canonical_response
        .location
        .as_ref()
        .and_then(|location| location.location.as_ref())
    {
        Some(wallet::transaction_location::Location::Mined(mined)) => mined,
        other => {
            return Err(eyre!(
                "{raw_blob_retention} retention returned non-mined canonical location: {other:?}"
            ));
        }
    };
    let mined_location = mined
        .location
        .as_ref()
        .ok_or_else(|| eyre!("canonical mined response omitted its location"))?;
    assert_eq!(
        mined_location.transaction_id,
        encode_rpc_transaction_id_hex(mined_transaction_id),
        "canonical response must identify the requested transaction"
    );
    assert_eq!(mined_location.block_height, block.height.value());
    assert_eq!(
        mined_location.block_hash,
        encode_rpc_block_hash_hex(block.hash)
    );
    assert_eq!(mined_location.tx_index_in_block, 0);
    let chain_context = mined
        .chain_context
        .as_ref()
        .ok_or_else(|| eyre!("canonical mined response omitted its branch context"))?;
    assert_eq!(
        chain_context.consensus_branch_id,
        activations.consensus_branch_id_at(block.height).value()
    );
    assert_eq!(
        chain_context.block_time,
        i64::from(block.block_time_seconds)
    );
    assert_eq!(chain_context.confirmations, 1);
    assert_eq!(
        mined.raw_transaction_bytes.as_deref(),
        expect_transaction_bytes.then_some(mined_raw_transaction_bytes.as_slice()),
        "{raw_blob_retention} retention returned incorrect mined transaction bytes"
    );
    assert_eq!(
        ingest_control_observer.mempool_transaction_call_count(),
        0,
        "an unpinned canonical hit must not consult the live provider"
    );

    let mempool_response = WalletQueryService::transaction(
        &grpc_adapter,
        Request::new(wallet::TransactionRequest {
            transaction_id: encode_rpc_transaction_id_hex(mempool_transaction_id),
            at_epoch_id: None,
        }),
    )
    .await?
    .into_inner();
    let mempool_entry = match mempool_response
        .location
        .as_ref()
        .and_then(|location| location.location.as_ref())
    {
        Some(wallet::transaction_location::Location::InMempool(entry)) => entry,
        other => {
            return Err(eyre!(
                "{raw_blob_retention} retention returned non-mempool live location: {other:?}"
            ));
        }
    };
    assert_eq!(
        mempool_entry.transaction_id,
        encode_rpc_transaction_id_hex(mempool_transaction_id)
    );
    assert_eq!(
        mempool_entry.raw_transaction_bytes,
        mempool_raw_transaction_bytes
    );
    assert_eq!(
        ingest_control_observer.mempool_transaction_call_count(),
        1,
        "an unpinned canonical miss must consult the live provider exactly once"
    );

    let Err(pinned_status) = WalletQueryService::transaction(
        &grpc_adapter,
        Request::new(wallet::TransactionRequest {
            transaction_id: encode_rpc_transaction_id_hex(mempool_transaction_id),
            at_epoch_id: Some(canonical_epoch_id),
        }),
    )
    .await
    else {
        return Err(eyre!(
            "pinned canonical miss unexpectedly read live mempool state"
        ));
    };
    assert_eq!(pinned_status.code(), Code::NotFound);
    let pinned_absence = TransactionAbsence::from_status(&pinned_status)?
        .ok_or_else(|| eyre!("pinned canonical miss was not a typed absence"))?;
    assert_eq!(pinned_absence.chain_epoch_id.value(), canonical_epoch_id);
    assert!(!pinned_absence.covers_mempool);
    assert_eq!(
        ingest_control_observer.mempool_transaction_call_count(),
        1,
        "a pinned canonical miss must not consult the live provider"
    );

    let full_block_outcome = WalletQueryService::full_block(
        &grpc_adapter,
        Request::new(wallet::FullBlockRequest {
            height: block.height.value(),
            at_epoch_id: Some(canonical_epoch_id),
        }),
    )
    .await;
    if expect_full_blocks {
        let full_block = full_block_outcome?
            .into_inner()
            .full_block
            .ok_or_else(|| eyre!("full-block response omitted its block"))?;
        assert_eq!(full_block.height, block.height.value());
        assert_eq!(full_block.block_hash, encode_rpc_block_hash_hex(block.hash));
        assert_eq!(
            full_block.parent_block_hash,
            encode_rpc_block_hash_hex(Network::ZcashRegtest.genesis_hash())
        );
        assert_eq!(full_block.payload_bytes, block.raw_block_bytes);
    } else {
        let status = full_block_outcome.err().ok_or_else(|| {
            eyre!("{raw_blob_retention} retention served an unretained full block")
        })?;
        assert_eq!(status.code(), Code::FailedPrecondition);
    }

    cancel.cancel();
    server_handle.await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn established_native_mempool_event_stream_outlives_the_unary_ingest_timeout()
-> eyre::Result<()> {
    let (adapter, _store_fixture, cancel, server_task) =
        native_serving_adapter_with_ingest_control(StaticIngestControl::new()).await?;
    let mut events = WalletQueryService::mempool_events(
        &adapter,
        Request::new(wallet::MempoolEventsRequest { start: None }),
    )
    .await?
    .into_inner();

    tokio::time::advance(TEST_ADVANCE_PAST_INGEST_CONTROL_REQUEST_BOUND).await;
    tokio::task::yield_now().await;
    let pending_event =
        tokio::spawn(
            async move { tokio::time::timeout(Duration::from_millis(1), events.next()).await },
        );
    tokio::task::yield_now().await;
    assert!(
        !pending_event.is_finished(),
        "a healthy quiet event stream must remain open after the unary ingest timeout"
    );
    tokio::time::advance(Duration::from_millis(2)).await;
    assert!(
        pending_event.await?.is_err(),
        "the test-local wait should expire while the event stream remains pending"
    );

    cancel.cancel();
    server_task.await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stalled_native_mempool_event_stream_establishment_is_bounded() -> eyre::Result<()> {
    let (adapter, _store_fixture, cancel, server_task) =
        native_serving_adapter_with_ingest_control(
            StaticIngestControl::new().with_stalled_mempool_events_establishment(),
        )
        .await?;
    let establishment = tokio::spawn(async move {
        WalletQueryService::mempool_events(
            &adapter,
            Request::new(wallet::MempoolEventsRequest { start: None }),
        )
        .await
    });

    tokio::task::yield_now().await;
    tokio::time::advance(TEST_ADVANCE_PAST_INGEST_CONTROL_REQUEST_BOUND).await;
    let status = establishment
        .await?
        .err()
        .ok_or_else(|| eyre!("stalled mempool-event establishment unexpectedly succeeded"))?;
    assert_eq!(status.code(), Code::Unavailable);
    let error_details = status.get_error_details();
    let error_info = error_details
        .error_info()
        .ok_or_else(|| eyre!("establishment timeout omitted ErrorInfo"))?;
    assert_eq!(
        error_info.reason,
        ops::ErrorReason::UpstreamUnreachable.as_str_name()
    );

    cancel.cancel();
    server_task.await??;
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the scenario proves admitted transparent primitives while the unfenced composite balance remains omitted"
)]
async fn native_grpc_service_omits_unfenced_transparent_balance_but_serves_its_primitives()
-> eyre::Result<()> {
    let (chain, fixture) = confirmed_transparent_balance_chain_fixture()?;

    let pending_inflow_zat = 300;
    let ingest_control = configured_mempool_balance_ingest_control(&fixture, pending_inflow_zat);
    let (ingest_control_addr, cancel, server_handle) =
        spawn_ingest_control_server(ingest_control).await?;
    let admitted_ingest_control = AdmittedIngestControl::connect(
        &format!("http://{ingest_control_addr}"),
        None,
        Network::ZcashRegtest,
    )
    .await?;
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical),
        Arc::new(wallet),
    )?);
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(serving_pair),
        (),
        admitted_ingest_control,
        activations,
    );
    for capability in [
        WALLET_MEMPOOL_TRANSPARENT_OUTPUTS_BY_ADDRESS_V1,
        WALLET_MEMPOOL_TRANSPARENT_SPENDS_BY_OUTPOINT_V1,
    ] {
        assert!(
            wallet_query
                .native_endpoint_capabilities()
                .contains(capability)
        );
    }
    assert!(
        !wallet_query
            .native_endpoint_capabilities()
            .contains(WALLET_ADDRESS_TRANSPARENT_BALANCE_V1),
        "separate live calls cannot prove one coherent mempool generation"
    );
    let grpc_adapter = WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default());

    let live_outputs = WalletQueryService::transparent_mempool_outputs_by_address(
        &grpc_adapter,
        Request::new(wallet::TransparentMempoolOutputsByAddressRequest {
            address: Some(wallet::AddressLookup {
                selector: Some(wallet::address_lookup::Selector::ScriptHash(
                    fixture.address_script_hash.as_bytes().to_vec(),
                )),
            }),
            max_entries: Some(1),
        }),
    )
    .await?
    .into_inner();
    assert_eq!(live_outputs.outputs.len(), 1);
    assert_eq!(live_outputs.outputs[0].value_zat, pending_inflow_zat);

    let live_spends = WalletQueryService::transparent_mempool_spends_by_outpoint(
        &grpc_adapter,
        Request::new(wallet::TransparentMempoolSpendsByOutpointRequest {
            outpoints: vec![wallet::OutPoint {
                transaction_id: encode_rpc_transaction_id_hex(
                    fixture.confirmed_outpoint.transaction_id,
                ),
                output_index: fixture.confirmed_outpoint.output_index,
            }],
        }),
    )
    .await?
    .into_inner();
    assert_eq!(live_spends.spends.len(), 1);

    let sentinel_status = WalletQueryService::transparent_mempool_spends_by_outpoint(
        &grpc_adapter,
        Request::new(wallet::TransparentMempoolSpendsByOutpointRequest {
            outpoints: vec![wallet::OutPoint {
                transaction_id: "00".repeat(32),
                output_index: u32::MAX,
            }],
        }),
    )
    .await
    .err()
    .ok_or_else(|| eyre!("mempool spend lookup accepted the coinbase sentinel"))?;
    assert_eq!(sentinel_status.code(), Code::InvalidArgument);
    assert!(sentinel_status.message().contains("coinbase sentinel"));

    let status = WalletQueryService::transparent_address_balance(
        &grpc_adapter,
        Request::new(wallet::TransparentAddressBalanceRequest {
            addresses: vec![wallet::AddressLookup {
                selector: Some(wallet::address_lookup::Selector::ScriptHash(
                    fixture.address_script_hash.as_bytes().to_vec(),
                )),
            }],
            at_epoch_id: Some(fixture.chain_epoch.id.value()),
        }),
    )
    .await
    .err()
    .ok_or_else(|| eyre!("unfenced composite balance unexpectedly served"))?;
    assert_eq!(status.code(), Code::FailedPrecondition);

    cancel.cancel();
    server_handle.await??;
    Ok(())
}

struct ConfirmedTransparentBalanceFixture {
    chain_epoch: ChainEpoch,
    address_script_hash: TransparentAddressScriptHash,
    confirmed_outpoint: TransparentOutPoint,
}

fn confirmed_transparent_balance_chain_fixture()
-> eyre::Result<(ChainFixture, ConfirmedTransparentBalanceFixture)> {
    let address_script_hash = TransparentAddressScriptHash::from_bytes([0x42; 32]);
    let confirmed_outpoint = TransparentOutPoint::new(TransactionId::from_bytes([0x51; 32]), 0);
    let confirmed_value_zat = 10_000;
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(1);
    let block = chain
        .block_at(zinder_core::BlockHeight::new(1))
        .ok_or_else(|| eyre!("balance fixture omitted block one"))?;
    let transaction_rows = FixtureTransactionRows::from_raw_transaction(
        confirmed_outpoint.transaction_id,
        block.height,
        block.hash,
        0,
        vec![0x01],
    );
    let confirmed_output = TransparentUnspentOutput::new(
        address_script_hash,
        vec![0x76, 0xa9, 0x14],
        confirmed_outpoint,
        confirmed_value_zat,
        block.height,
        block.hash,
    );
    let chain_epoch = chain
        .chain_epoch(ChainEpochId::new(1))
        .ok_or_else(|| eyre!("balance fixture omitted chain epoch"))?;
    let chain = chain
        .with_transaction_rows(transaction_rows)
        .with_address_output_index(confirmed_output);

    Ok((
        chain,
        ConfirmedTransparentBalanceFixture {
            chain_epoch,
            address_script_hash,
            confirmed_outpoint,
        },
    ))
}

fn configured_mempool_balance_ingest_control(
    fixture: &ConfirmedTransparentBalanceFixture,
    pending_inflow_zat: u64,
) -> StaticIngestControl {
    StaticIngestControl::new()
        .with_transparent_mempool_outputs_by_address_response(
            wallet::TransparentMempoolOutputsByAddressResponse {
                chain_view: Some(chain_view_message(fixture.chain_epoch)),
                outputs: vec![wallet::TransparentMempoolOutput {
                    address_script_hash: fixture.address_script_hash.as_bytes().to_vec(),
                    script_pub_key: vec![0x76, 0xa9, 0x14],
                    outpoint: Some(wallet::OutPoint {
                        transaction_id: encode_rpc_transaction_id_hex(TransactionId::from_bytes(
                            [0x52; 32],
                        )),
                        output_index: 0,
                    }),
                    value_zat: pending_inflow_zat,
                }],
            },
        )
        .with_transparent_mempool_spends_by_outpoint_response(
            wallet::TransparentMempoolSpendsByOutpointResponse {
                chain_view: Some(chain_view_message(fixture.chain_epoch)),
                spends: vec![wallet::TransparentMempoolSpend {
                    spent_outpoint: Some(wallet::OutPoint {
                        transaction_id: encode_rpc_transaction_id_hex(
                            fixture.confirmed_outpoint.transaction_id,
                        ),
                        output_index: fixture.confirmed_outpoint.output_index,
                    }),
                    spending_transaction_id: encode_rpc_transaction_id_hex(
                        TransactionId::from_bytes([0x53; 32]),
                    ),
                }],
            },
        )
}

#[tokio::test]
async fn native_grpc_service_uses_query_owned_capabilities() -> eyre::Result<()> {
    let (read_only_adapter, _read_only_store, _read_only_ingest) = native_adapter().await?;
    let read_only_info = WalletQueryService::server_info(
        &read_only_adapter,
        Request::new(wallet::ServerInfoRequest {}),
    )
    .await?
    .into_inner()
    .info
    .ok_or_else(|| eyre!("missing read-only info"))?;

    assert!(has_capability(&read_only_info, WALLET_EVENTS_CHAIN_V1));
    assert!(!has_capability(
        &read_only_info,
        WALLET_BROADCAST_TRANSACTION_V1
    ));

    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(1),
        &activations,
    )?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let broadcast_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::new(canonical),
            Arc::new(wallet),
        )?)),
        MockTransactionBroadcaster::accepted(TransactionId::from_bytes([0x33; 32])),
        ingest_control,
        activations,
    );
    let broadcast_adapter =
        WalletQueryGrpcAdapter::new(broadcast_query, WalletEndpointMetadata::default());
    let broadcast_info = WalletQueryService::server_info(
        &broadcast_adapter,
        Request::new(wallet::ServerInfoRequest {}),
    )
    .await?
    .into_inner()
    .info
    .ok_or_else(|| eyre!("missing broadcast info"))?;

    assert!(
        !has_capability(&broadcast_info, WALLET_BROADCAST_TRANSACTION_V1),
        "a broadcaster without a probed node must not advertise broadcast"
    );

    Ok(())
}

struct WalletGrpcResponses {
    visible_tip_block: wallet::VisibleTipBlockResponse,
    compact_block_range_header: wallet::ChainView,
    compact_block_range: Vec<wallet::CompactBlock>,
    latest_tree_state_checkpoint: wallet::TreeStateResponse,
    subtree_roots: wallet::SubtreeRootsResponse,
    network_upgrade_activations: wallet::NetworkUpgradeActivationsResponse,
}

async fn read_wallet_grpc_responses(
    grpc_adapter: &NativeServingAdapter,
) -> Result<WalletGrpcResponses, tonic::Status> {
    let visible_tip_block = WalletQueryService::visible_tip_block(
        grpc_adapter,
        Request::new(wallet::VisibleTipBlockRequest { at_epoch_id: None }),
    )
    .await?
    .into_inner();
    let mut compact_block_stream = WalletQueryService::compact_blocks_in_range(
        grpc_adapter,
        Request::new(wallet::CompactBlocksInRangeRequest {
            start_height: 1,
            end_height: 1,
            at_epoch_id: None,
        }),
    )
    .await?
    .into_inner();
    let mut compact_block_range_header = None;
    let mut compact_block_range = Vec::new();
    while let Some(compact_block_chunk) = compact_block_stream.next().await {
        match (
            compact_block_chunk?.body,
            compact_block_range_header.is_some(),
        ) {
            (Some(wallet::compact_blocks_in_range_chunk::Body::Header(header)), false) => {
                compact_block_range_header = Some(header);
            }
            (Some(wallet::compact_blocks_in_range_chunk::Body::Item(compact_block)), true) => {
                compact_block_range.push(compact_block);
            }
            (body, _) => {
                return Err(tonic::Status::internal(format!(
                    "compact block range chunk out of order: {body:?}"
                )));
            }
        }
    }
    let compact_block_range_header = compact_block_range_header.ok_or_else(|| {
        tonic::Status::internal("compact block range ended without its chain-view header")
    })?;
    let latest_tree_state_checkpoint = WalletQueryService::latest_tree_state_checkpoint(
        grpc_adapter,
        Request::new(wallet::LatestTreeStateCheckpointRequest { at_epoch_id: None }),
    )
    .await?
    .into_inner();
    let subtree_roots = WalletQueryService::subtree_roots(
        grpc_adapter,
        Request::new(wallet::SubtreeRootsRequest {
            shielded_protocol: wallet::ShieldedProtocol::Sapling as i32,
            start_index: 0,
            max_entries: 1,
            at_epoch_id: None,
        }),
    )
    .await?
    .into_inner();
    let network_upgrade_activations = WalletQueryService::network_upgrade_activations(
        grpc_adapter,
        Request::new(wallet::NetworkUpgradeActivationsRequest {}),
    )
    .await?
    .into_inner();

    Ok(WalletGrpcResponses {
        visible_tip_block,
        compact_block_range_header,
        compact_block_range,
        latest_tree_state_checkpoint,
        subtree_roots,
        network_upgrade_activations,
    })
}

fn assert_wallet_grpc_response_epochs(responses: &WalletGrpcResponses, chain_epoch_id: u64) {
    assert_eq!(
        response_chain_epoch_id(&responses.visible_tip_block),
        chain_epoch_id
    );
    assert_eq!(
        response_chain_epoch_id(&responses.compact_block_range_header),
        chain_epoch_id
    );
    assert_eq!(
        response_chain_epoch_id(&responses.latest_tree_state_checkpoint),
        chain_epoch_id
    );
    assert_eq!(
        response_chain_epoch_id(&responses.subtree_roots),
        chain_epoch_id
    );
}

trait HasChainEpoch {
    fn chain_epoch(&self) -> Option<&wallet::ChainEpoch>;
}

impl HasChainEpoch for wallet::VisibleTipBlockResponse {
    fn chain_epoch(&self) -> Option<&wallet::ChainEpoch> {
        self.chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
    }
}

impl HasChainEpoch for wallet::ChainView {
    fn chain_epoch(&self) -> Option<&wallet::ChainEpoch> {
        self.chain_epoch.as_ref()
    }
}

impl HasChainEpoch for wallet::TreeStateResponse {
    fn chain_epoch(&self) -> Option<&wallet::ChainEpoch> {
        self.chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
    }
}

impl HasChainEpoch for wallet::SubtreeRootsResponse {
    fn chain_epoch(&self) -> Option<&wallet::ChainEpoch> {
        self.chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
    }
}

fn response_chain_epoch_id(response: &impl HasChainEpoch) -> u64 {
    response
        .chain_epoch()
        .map_or(0, |chain_epoch| chain_epoch.chain_epoch_id)
}

fn has_capability(wallet_info: &wallet::WalletServerInfo, capability: &str) -> bool {
    wallet_info.common.as_ref().is_some_and(|common| {
        common
            .capabilities
            .iter()
            .any(|advertised| advertised == capability)
    })
}

type StaticVisibleChainEventsStream =
    Pin<Box<dyn Stream<Item = Result<wallet::ChainEventEnvelope, Status>> + Send>>;

#[derive(Clone)]
struct StaticIngestControl {
    server_info: Option<ops::ServerInfo>,
    transaction: Option<wallet::TransactionStatusResponse>,
    mempool_snapshot_status: Option<Status>,
    mempool_transaction_calls: Arc<AtomicUsize>,
    mempool_transaction_miss: Arc<parking_lot::Mutex<Option<Status>>>,
    mempool_events_establishment_stalled: bool,
    outputs_by_address: Option<wallet::TransparentMempoolOutputsByAddressResponse>,
    spends_by_outpoint: Option<wallet::TransparentMempoolSpendsByOutpointResponse>,
}

impl StaticIngestControl {
    fn new() -> Self {
        Self {
            server_info: Some(ops::ServerInfo {
                network: "zcash-regtest".to_owned(),
                service_name: "zinder-ingest".to_owned(),
                service_version: "test".to_owned(),
                build_git_commit: "test".to_owned(),
                capabilities: [
                    INGEST_CONTROL_SERVER_INFO_V1,
                    INGEST_CONTROL_WRITER_STATUS_V1,
                    INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3,
                    INGEST_CONTROL_MEMPOOL_TRANSACTION_V2,
                    INGEST_CONTROL_MEMPOOL_EVENTS_V2,
                    INGEST_CONTROL_TRANSPARENT_MEMPOOL_OUTPUTS_BY_ADDRESS_V1,
                    INGEST_CONTROL_TRANSPARENT_MEMPOOL_SPENDS_BY_OUTPOINT_V1,
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
                contract_revision: zinder_proto::CONTRACT_REVISION,
                materialized_view_preset: String::new(),
                materialized_view_identities: Vec::new(),
            }),
            transaction: None,
            mempool_snapshot_status: None,
            mempool_transaction_calls: Arc::new(AtomicUsize::new(0)),
            mempool_transaction_miss: Arc::new(parking_lot::Mutex::new(None)),
            mempool_events_establishment_stalled: false,
            outputs_by_address: None,
            spends_by_outpoint: None,
        }
    }

    fn with_mempool_transaction_response(
        mut self,
        response: wallet::TransactionStatusResponse,
    ) -> Self {
        self.transaction = Some(response);
        self
    }

    fn with_mempool_snapshot_status(mut self, status: Status) -> Self {
        self.mempool_snapshot_status = Some(status);
        self
    }

    /// Replaces the status every later mempool miss returns.
    fn set_mempool_transaction_miss(&self, status: Status) {
        *self.mempool_transaction_miss.lock() = Some(status);
    }

    fn mempool_transaction_call_count(&self) -> usize {
        self.mempool_transaction_calls.load(Ordering::SeqCst)
    }

    fn with_stalled_mempool_events_establishment(mut self) -> Self {
        self.mempool_events_establishment_stalled = true;
        self
    }

    fn with_service_name(mut self, service_name: &str) -> Self {
        if let Some(server_info) = self.server_info.as_mut() {
            service_name.clone_into(&mut server_info.service_name);
        }
        self
    }

    fn without_server_info(mut self) -> Self {
        self.server_info = None;
        self
    }

    fn with_network(mut self, network: &str) -> Self {
        if let Some(server_info) = self.server_info.as_mut() {
            network.clone_into(&mut server_info.network);
        }
        self
    }

    fn with_contract_revision(mut self, contract_revision: u32) -> Self {
        if let Some(server_info) = self.server_info.as_mut() {
            server_info.contract_revision = contract_revision;
        }
        self
    }

    fn without_capability(mut self, capability: &str) -> Self {
        if let Some(server_info) = self.server_info.as_mut() {
            server_info
                .capabilities
                .retain(|advertised| advertised != capability);
        }
        self
    }

    fn with_transparent_mempool_outputs_by_address_response(
        mut self,
        response: wallet::TransparentMempoolOutputsByAddressResponse,
    ) -> Self {
        self.outputs_by_address = Some(response);
        self
    }

    fn with_transparent_mempool_spends_by_outpoint_response(
        mut self,
        response: wallet::TransparentMempoolSpendsByOutpointResponse,
    ) -> Self {
        self.spends_by_outpoint = Some(response);
        self
    }
}

#[tonic::async_trait]
impl IngestControl for StaticIngestControl {
    type VisibleChainEventsStream = StaticVisibleChainEventsStream;
    type MempoolEventsStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<wallet::MempoolEventEnvelope, Status>> + Send>,
    >;

    async fn server_info(
        &self,
        _request: Request<zinder_proto::v1::ingest::ServerInfoRequest>,
    ) -> Result<Response<zinder_proto::v1::ingest::ServerInfoResponse>, Status> {
        Ok(Response::new(
            zinder_proto::v1::ingest::ServerInfoResponse {
                server_info: self.server_info.clone(),
            },
        ))
    }

    async fn writer_status(
        &self,
        _request: Request<WriterStatusRequest>,
    ) -> Result<Response<WriterStatusResponse>, Status> {
        Ok(Response::new(WriterStatusResponse {
            chain_view: Some(wallet::ChainView {
                chain_epoch: Some(wallet::ChainEpoch {
                    chain_epoch_id: 11,
                    network_name: "zcash-regtest".to_owned(),
                    artifact_schema_version: 1,
                    created_at_millis: 123,
                    visible_tip: Some(wallet::BlockTip {
                        height: 5,
                        hash: "05".repeat(32),
                    }),
                    settled_tip: Some(wallet::BlockTip {
                        height: 4,
                        hash: "04".repeat(32),
                    }),
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                }),
                indexed_tip: None,
                upstream_tip: None,
                materialized_views: None,
            }),
            network_name: "zcash-regtest".to_owned(),
            phase: WriterPhase::FollowingTip.into(),
            gap_blocks: Some(0),
            upstream_not_ready: None,
            reorg_window_exceeded: None,
        }))
    }

    async fn visible_chain_events(
        &self,
        _request: Request<wallet::EventStreamStart>,
    ) -> Result<Response<Self::VisibleChainEventsStream>, Status> {
        Err(Status::unimplemented(
            "test scaffold does not stub VisibleChainEvents",
        ))
    }

    async fn mempool_snapshot(
        &self,
        _request: Request<wallet::MempoolSnapshotRequest>,
    ) -> Result<Response<wallet::MempoolSnapshotResponse>, Status> {
        if let Some(status) = &self.mempool_snapshot_status {
            return Err(status.clone());
        }
        let chain_epoch = test_chain_epoch();
        Ok(Response::new(wallet::MempoolSnapshotResponse {
            source_tip: chain_epoch.visible_tip.clone(),
            chain_view: Some(wallet::ChainView {
                chain_epoch: Some(chain_epoch),
                indexed_tip: None,
                upstream_tip: None,
                materialized_views: None,
            }),
            events_resume_cursor: Vec::new(),
            snapshot_age_millis: 0,
            entries: Vec::new(),
            next_cursor: Vec::new(),
        }))
    }

    async fn mempool_transaction(
        &self,
        request: Request<MempoolTransactionRequest>,
    ) -> Result<Response<wallet::TransactionStatusResponse>, Status> {
        self.mempool_transaction_calls
            .fetch_add(1, Ordering::SeqCst);
        let requested_transaction_id = request.into_inner().transaction_id;
        if let Some(transaction) = self.transaction.as_ref().filter(|transaction| {
            in_mempool_transaction_id(transaction) == Some(requested_transaction_id.as_str())
        }) {
            return Ok(Response::new(transaction.clone()));
        }
        Err(self
            .mempool_transaction_miss
            .lock()
            .clone()
            .unwrap_or_else(|| Status::not_found("transaction is not in the test mempool")))
    }

    async fn mempool_events(
        &self,
        _request: Request<wallet::MempoolEventsRequest>,
    ) -> Result<Response<Self::MempoolEventsStream>, Status> {
        if self.mempool_events_establishment_stalled {
            return std::future::pending().await;
        }
        Ok(Response::new(Box::pin(tokio_stream::pending())))
    }

    async fn transparent_mempool_outputs_by_address(
        &self,
        _request: Request<wallet::TransparentMempoolOutputsByAddressRequest>,
    ) -> Result<Response<wallet::TransparentMempoolOutputsByAddressResponse>, Status> {
        Ok(Response::new(
            self.outputs_by_address.clone().unwrap_or_else(|| {
                wallet::TransparentMempoolOutputsByAddressResponse {
                    chain_view: Some(wallet::ChainView {
                        chain_epoch: Some(test_chain_epoch()),
                        indexed_tip: None,
                        upstream_tip: None,
                        materialized_views: None,
                    }),
                    outputs: Vec::new(),
                }
            }),
        ))
    }

    async fn transparent_mempool_spends_by_outpoint(
        &self,
        _request: Request<wallet::TransparentMempoolSpendsByOutpointRequest>,
    ) -> Result<Response<wallet::TransparentMempoolSpendsByOutpointResponse>, Status> {
        Ok(Response::new(
            self.spends_by_outpoint.clone().unwrap_or_else(|| {
                wallet::TransparentMempoolSpendsByOutpointResponse {
                    chain_view: Some(wallet::ChainView {
                        chain_epoch: Some(test_chain_epoch()),
                        indexed_tip: None,
                        upstream_tip: None,
                        materialized_views: None,
                    }),
                    spends: Vec::new(),
                }
            }),
        ))
    }

    async fn transparent_mempool_outputs_by_outpoint(
        &self,
        _request: Request<wallet::TransparentMempoolOutputsByOutpointRequest>,
    ) -> Result<Response<wallet::TransparentOutputsByOutpointResponse>, Status> {
        Err(Status::unimplemented(
            "test scaffold does not stub TransparentMempoolOutputsByOutpoint",
        ))
    }

    async fn chain_value_pools_at_tip(
        &self,
        _request: Request<wallet::ChainValuePoolsAtTipRequest>,
    ) -> Result<Response<wallet::ChainValuePoolsAtTipResponse>, Status> {
        Err(Status::unimplemented(
            "test scaffold does not stub ChainValuePoolsAtTip",
        ))
    }
}

/// Writer answer for a transaction pending in the live mempool.
fn in_mempool_status_response(
    transaction_id: TransactionId,
    raw_transaction_bytes: Vec<u8>,
) -> wallet::TransactionStatusResponse {
    wallet::TransactionStatusResponse {
        chain_view: Some(wallet::ChainView {
            chain_epoch: Some(test_chain_epoch()),
            indexed_tip: None,
            upstream_tip: None,
            materialized_views: None,
        }),
        location: Some(wallet::TransactionLocation {
            location: Some(wallet::transaction_location::Location::InMempool(
                wallet::MempoolEntry {
                    transaction_id: encode_rpc_transaction_id_hex(transaction_id),
                    auth_digest: String::new(),
                    raw_transaction_bytes,
                    compact_transaction_data: Some(wallet::CompactTransactionData::default()),
                    first_seen_unix_millis: 1_700_000_000_000,
                    first_seen_chain_epoch: Some(test_chain_epoch()),
                    transparent_outputs: Vec::new(),
                    transparent_spends: Vec::new(),
                },
            )),
        }),
    }
}

/// Returns the transaction id of a pending-transaction response.
fn in_mempool_transaction_id(response: &wallet::TransactionStatusResponse) -> Option<&str> {
    match response.location.as_ref()?.location.as_ref()? {
        wallet::transaction_location::Location::InMempool(entry) => Some(&entry.transaction_id),
        wallet::transaction_location::Location::Mined(_) => None,
    }
}

fn test_chain_epoch() -> wallet::ChainEpoch {
    wallet::ChainEpoch {
        chain_epoch_id: 11,
        network_name: "zcash-regtest".to_owned(),
        artifact_schema_version: 1,
        created_at_millis: 123,
        visible_tip: Some(wallet::BlockTip {
            height: 5,
            hash: "05".repeat(32),
        }),
        settled_tip: Some(wallet::BlockTip {
            height: 4,
            hash: "04".repeat(32),
        }),
        sapling_commitment_tree_size: 0,
        orchard_commitment_tree_size: 0,
        ironwood_commitment_tree_size: 0,
    }
}

type NativeServingAdapter = WalletQueryGrpcAdapter<WalletServingQuery<(), AdmittedIngestControl>>;

/// Native adapter over a one-block release pair, plus the fixtures that must
/// outlive it.
async fn native_adapter() -> eyre::Result<(
    NativeServingAdapter,
    WalletServingStoreFixture,
    zinder_testkit::IngestControlFixture,
)> {
    let (query, store_fixture, ingest_control_fixture) =
        native_serving_query_over(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(1))
            .await?;
    Ok((
        WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default()),
        store_fixture,
        ingest_control_fixture,
    ))
}
type IngestControlServerTask = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

type NativeServingAdapterFixture = (
    NativeServingAdapter,
    WalletServingStoreFixture,
    CancellationToken,
    IngestControlServerTask,
);

async fn native_serving_adapter_with_ingest_control(
    ingest_control: StaticIngestControl,
) -> eyre::Result<NativeServingAdapterFixture> {
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(1);
    let store_fixture =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    native_serving_adapter_over(store_fixture, ingest_control).await
}

/// Native adapter over the readers of `store_fixture`, admitted against
/// `ingest_control` served on a loopback endpoint.
async fn native_serving_adapter_over(
    mut store_fixture: WalletServingStoreFixture,
    ingest_control: StaticIngestControl,
) -> eyre::Result<NativeServingAdapterFixture> {
    let (ingest_control_addr, cancel, server_task) =
        spawn_ingest_control_server(ingest_control).await?;
    let admitted_ingest_control = AdmittedIngestControl::connect(
        &format!("http://{ingest_control_addr}"),
        None,
        Network::ZcashRegtest,
    )
    .await?;
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let (canonical, wallet) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical),
        Arc::new(wallet),
    )?);
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(serving_pair),
        (),
        admitted_ingest_control,
        activations,
    );
    Ok((
        WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default()),
        store_fixture,
        cancel,
        server_task,
    ))
}

async fn spawn_ingest_control_server(
    ingest_control: StaticIngestControl,
) -> eyre::Result<(SocketAddr, CancellationToken, IngestControlServerTask)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen_addr = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let server_cancel = cancel.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(IngestControlServer::new(ingest_control))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                server_cancel.cancelled_owned(),
            )
            .await
    });

    Ok((listen_addr, cancel, server))
}
