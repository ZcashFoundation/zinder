//! `RemoteChainIndex::transaction_by_id` reports `NotFound` only for a proven
//! absence.
//!
//! The scripted server answers `WalletQuery.Transaction` with one fixed status,
//! which reproduces both current servers and servers that predate the typed
//! absence signal.

use std::{
    convert::Infallible,
    sync::Arc,
    task::{Context, Poll},
};

use eyre::eyre;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code, Request, Response, Status,
    body::Body as TonicBody,
    codegen::{Body, BoxFuture, Service, StdError, http},
    server::{NamedService, UnaryService},
    transport::Server,
};
use tonic_types::ErrorDetails;
use zinder_client::{
    BlockHeight, ChainEpochId, ChainIndex, ErrorReason, IndexerError, Network, RemoteChainIndex,
    RemoteOpenOptions, RetryPolicy, TransactionId, TxStatus,
};
use zinder_proto::status_with_reason;
use zinder_proto::v1::{ops, wallet};
use zinder_proto::wire::TransactionAbsence;
use zinder_query::{
    AdmittedIngestControl, ArtifactKey, QueryError, WalletEndpointMetadata, WalletQueryGrpcAdapter,
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair, status_from_query_error,
};
use zinder_store::ArtifactFamily;
use zinder_testkit::{
    ChainFixture, IngestControlFixture, MockTransactionBroadcaster, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

const TRANSACTION_ID: TransactionId = TransactionId::from_bytes([0x5a; 32]);

#[tokio::test]
async fn typed_absence_is_the_only_not_found_status() -> eyre::Result<()> {
    let pinned_epoch = ChainEpochId::new(7);
    let pinned_absence = TransactionAbsence {
        chain_epoch_id: pinned_epoch,
        first_covered_height: BlockHeight::new(1),
        visible_tip_height: BlockHeight::new(120),
        covers_mempool: false,
    };

    let pinned = lookup(pinned_absence.into_status("absent"), Some(pinned_epoch)).await?;
    assert!(matches!(pinned, Ok(TxStatus::NotFound)), "{pinned:?}");

    let unpinned_absence = TransactionAbsence {
        covers_mempool: true,
        ..pinned_absence
    };
    let unpinned = lookup(unpinned_absence.into_status("absent"), None).await?;
    assert!(matches!(unpinned, Ok(TxStatus::NotFound)), "{unpinned:?}");
    Ok(())
}

#[tokio::test]
async fn absence_that_does_not_answer_the_request_is_rejected() -> eyre::Result<()> {
    let absence = TransactionAbsence {
        chain_epoch_id: ChainEpochId::new(7),
        first_covered_height: BlockHeight::new(1),
        visible_tip_height: BlockHeight::new(120),
        covers_mempool: false,
    };

    // An absence at a different epoch than the pin does not cover the pin.
    let other_epoch = lookup(absence.into_status("absent"), Some(ChainEpochId::new(8))).await?;
    assert!(
        matches!(
            other_epoch,
            Err(IndexerError::MalformedResponse {
                field: "error_info.metadata.chain_epoch_id",
                ..
            })
        ),
        "{other_epoch:?}"
    );

    // An unpinned lookup asked about the mempool too.
    let canonical_only = lookup(absence.into_status("absent"), None).await?;
    assert!(
        matches!(
            canonical_only,
            Err(IndexerError::MalformedResponse {
                field: "error_info.metadata.covers_mempool",
                ..
            })
        ),
        "{canonical_only:?}"
    );

    // A covered range that starts above height 1 leaves earlier heights
    // unsearched.
    let partial = TransactionAbsence {
        first_covered_height: BlockHeight::new(101),
        ..absence
    };
    let partial_outcome = lookup(partial.into_status("absent"), Some(ChainEpochId::new(7))).await?;
    assert!(
        matches!(
            partial_outcome,
            Err(IndexerError::CanonicalHistoryUnavailable { first_available_height })
                if first_available_height == BlockHeight::new(101)
        ),
        "{partial_outcome:?}"
    );

    // A range starting at genesis claims a block Zinder never indexes.
    let from_genesis = TransactionAbsence {
        first_covered_height: BlockHeight::new(0),
        ..absence
    };
    let from_genesis_outcome = lookup(
        from_genesis.into_status("absent"),
        Some(ChainEpochId::new(7)),
    )
    .await?;
    assert!(
        matches!(
            from_genesis_outcome,
            Err(IndexerError::MalformedResponse {
                field: "error_info.metadata.first_covered_height",
                ..
            })
        ),
        "{from_genesis_outcome:?}"
    );

    // A reversed range covers no indexed height, so it proves nothing.
    let reversed = TransactionAbsence {
        visible_tip_height: BlockHeight::new(0),
        ..absence
    };
    let reversed_outcome =
        lookup(reversed.into_status("absent"), Some(ChainEpochId::new(7))).await?;
    assert!(
        matches!(
            reversed_outcome,
            Err(IndexerError::MalformedResponse {
                field: "error_info.metadata.visible_tip_height",
                ..
            })
        ),
        "{reversed_outcome:?}"
    );
    Ok(())
}

#[tokio::test]
async fn storage_and_chain_failures_are_never_an_absence() -> eyre::Result<()> {
    // A located transaction whose retained bytes are gone.
    let artifact_missing = status_from_query_error(&QueryError::ArtifactUnavailable {
        family: ArtifactFamily::TransactionBlob,
        key: ArtifactKey::TransactionId(TRANSACTION_ID),
    });
    let outcome = lookup(artifact_missing, None).await?;
    assert!(
        matches!(outcome, Err(IndexerError::ArtifactUnavailable { .. })),
        "{outcome:?}"
    );

    let epoch_missing = status_with_reason(
        ops::ErrorReason::ChainEpochMissing,
        "chain epoch 3 is not retained",
        ErrorDetails::with_resource_info(
            zinder_core::artifact_family::CHAIN_EPOCH,
            "chain_epoch:3",
            "zinder-query",
            "the requested chain epoch is not retained",
        ),
    );
    let outcome = lookup(epoch_missing, None).await?;
    assert!(
        matches!(
            &outcome,
            Err(IndexerError::RemoteFailure {
                reason: ErrorReason::ChainEpochMissing,
                retry_policy: RetryPolicy::RetryWithBackoff,
                ..
            })
        ),
        "{outcome:?}"
    );

    let not_in_best_chain = status_from_query_error(&QueryError::BlockNotInBestChain);
    let outcome = lookup(not_in_best_chain, None).await?;
    assert!(
        matches!(outcome, Err(IndexerError::NotFound { resource: "block" })),
        "{outcome:?}"
    );

    let history_floor = status_from_query_error(&QueryError::CanonicalHistoryUnavailable {
        first_available_height: BlockHeight::new(1_001),
    });
    let outcome = lookup(history_floor, Some(ChainEpochId::new(1))).await?;
    let Err(history_error) = outcome else {
        return Err(eyre!("history floor became an answer: {outcome:?}"));
    };
    assert!(matches!(
        history_error,
        IndexerError::CanonicalHistoryUnavailable { first_available_height }
            if first_available_height == BlockHeight::new(1_001)
    ));
    assert_eq!(
        history_error.retry_policy(),
        RetryPolicy::OperatorActionRequired
    );

    let replica_behind_writer = status_with_reason(
        ops::ErrorReason::ChainEpochMissing,
        "the live mempool answered at another epoch",
        ErrorDetails::new(),
    );
    let outcome = lookup(replica_behind_writer, None).await?;
    assert!(
        matches!(
            outcome,
            Err(IndexerError::RemoteFailure {
                reason: ErrorReason::ChainEpochMissing,
                ..
            })
        ),
        "{outcome:?}"
    );
    Ok(())
}

#[tokio::test]
async fn older_server_not_found_is_unverified_not_an_absence() -> eyre::Result<()> {
    // Servers below contract revision 6 answered every miss, and several
    // storage and history failures, with this shape.
    let legacy_absence = status_with_reason(
        ops::ErrorReason::ArtifactUnavailable,
        "transaction is not visible in the canonical chain or live mempool",
        ErrorDetails::new(),
    );
    let foreign_not_found = Status::not_found("not found");

    for status in [legacy_absence, foreign_not_found] {
        for at_epoch_id in [None, Some(ChainEpochId::new(1))] {
            let outcome = lookup(status.clone(), at_epoch_id).await?;
            let Err(error) = outcome else {
                return Err(eyre!("ambiguous NOT_FOUND became an answer: {outcome:?}"));
            };
            assert!(
                matches!(error, IndexerError::TransactionAbsenceUnverified { .. }),
                "{error:?}"
            );
            assert_eq!(error.retry_policy(), RetryPolicy::OperatorActionRequired);
        }
    }
    Ok(())
}

/// A query server proves a pinned absence on its own, and refuses to turn a
/// writer's untyped mempool miss into an absence for an unpinned lookup.
#[tokio::test]
async fn query_server_proves_pinned_absence_and_flags_an_untyped_mempool_miss() -> eyre::Result<()>
{
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let mut store_fixture =
        WalletServingStoreFixture::from_chain(&chain_fixture, activations.as_ref())?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
    )?);
    // The fixture writer answers every mempool miss without the typed
    // absence, the shape of an ingest below contract revision 6.
    let ingest_control_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let admitted_ingest_control = AdmittedIngestControl::connect(
        ingest_control_fixture.endpoint(),
        None,
        Network::ZcashRegtest,
    )
    .await?;
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(serving_pair),
        MockTransactionBroadcaster::broadcast_disabled(),
        admitted_ingest_control,
        activations,
    );
    let endpoint = spawn_service(
        WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default()).into_server(),
    )
    .await?;
    let chain_index = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashRegtest,
    })?;

    let snapshot = chain_index.snapshot().await?;
    assert!(matches!(
        snapshot.transaction_by_id(TRANSACTION_ID).await?,
        TxStatus::NotFound
    ));

    let unpinned = chain_index.transaction_by_id(TRANSACTION_ID, None).await;
    assert!(
        matches!(
            unpinned,
            Err(IndexerError::TransactionAbsenceUnverified { .. })
        ),
        "{unpinned:?}"
    );
    ingest_control_fixture.shutdown().await
}

async fn lookup(
    status: Status,
    at_epoch_id: Option<ChainEpochId>,
) -> eyre::Result<Result<TxStatus, IndexerError>> {
    let endpoint = spawn_service(ScriptedTransactionService {
        status: Arc::new(status),
    })
    .await?;
    let chain_index = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashRegtest,
    })?;
    Ok(chain_index
        .transaction_by_id(TRANSACTION_ID, at_epoch_id)
        .await)
}

async fn spawn_service<S>(service: S) -> eyre::Result<String>
where
    S: Service<http::Request<TonicBody>, Response = http::Response<TonicBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(async move {
        let _server_result = Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await;
    });
    Ok(format!("http://{addr}"))
}

/// `WalletQuery` service that answers `Transaction` with one fixed status.
#[derive(Clone)]
struct ScriptedTransactionService {
    status: Arc<Status>,
}

impl NamedService for ScriptedTransactionService {
    const NAME: &'static str = "zinder.v1.wallet.WalletQuery";
}

impl<B> Service<http::Request<B>> for ScriptedTransactionService
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<TonicBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        struct TransactionMethod(Arc<Status>);

        impl UnaryService<wallet::TransactionRequest> for TransactionMethod {
            type Response = wallet::TransactionStatusResponse;
            type Future = BoxFuture<Response<Self::Response>, Status>;

            fn call(&mut self, _request: Request<wallet::TransactionRequest>) -> Self::Future {
                let status = Status::clone(&self.0);
                Box::pin(async move { Err(status) })
            }
        }

        if request.uri().path() != "/zinder.v1.wallet.WalletQuery/Transaction" {
            return Box::pin(async move {
                Ok(Status::new(Code::Unimplemented, "scripted service").into_http())
            });
        }
        let status = Arc::clone(&self.status);
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
            Ok(grpc.unary(TransactionMethod(status), request).await)
        })
    }
}
