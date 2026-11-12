#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
    convert::Infallible,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use std::{num::NonZeroU32, time::Duration};

use eyre::eyre;
use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tonic::{
    Response, Status,
    body::Body as TonicBody,
    codegen::{Body, BoxFuture, Service, StdError, http},
    server::{NamedService, ServerStreamingService},
    transport::Server,
};
use zinder_client::{
    BlockHeight, BlockHeightRange, Capability, CapabilityDescriptor, ChainEvent, ChainIndex,
    ConsensusBranchId, EndpointBackedIndex, EventStreamStart, IndexerError, Network,
    NetworkUpgradeActivation, OwnedChainSnapshot, RemoteChainIndex, RemoteOpenOptions, RetryPolicy,
    TransactionId, TransparentAddressScriptHash, TransparentAddressTxIdsQuery, TransparentOutPoint,
    TransparentUnspentOutput,
};
use zinder_proto::v1::wallet;
use zinder_query::{
    AdmittedIngestControl, WalletEndpointMetadata, WalletQueryApi, WalletQueryGrpcAdapter,
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_testkit::{
    ChainFixture, IngestControlFixture, MockTransactionBroadcaster, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

#[tokio::test]
async fn remote_chain_index_round_trips_chain_index_calls_over_grpc() -> eyre::Result<()> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let mut store_fixture =
        WalletServingStoreFixture::from_chain(&chain_fixture, activations.as_ref())?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
    )?);
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
    let grpc_adapter = WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default());
    let endpoint = spawn_wallet_query(grpc_adapter).await?;
    let chain_index = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashRegtest,
    })?;

    let server_info = chain_index.server_info().await?;
    let current_epoch = chain_index.current_epoch().await?;
    let compact_block = chain_index
        .compact_block_at(BlockHeight::new(1), None)
        .await?;
    let mut compact_blocks = chain_index
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2)),
            None,
        )
        .await?;
    let mut compact_block_count = 0;
    while let Some(compact_block_result) = compact_blocks.next().await {
        compact_block_result?;
        compact_block_count += 1;
    }
    let mut events = chain_index
        .chain_events(EventStreamStart::EarliestRetained)
        .await?;
    let first_event = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await?
        .ok_or_else(|| eyre!("chain-events stream closed before first event"))??;

    assert_eq!(server_info.network, Network::ZcashRegtest);
    assert_eq!(server_info.service_name, "zinder-query");
    assert_eq!(current_epoch.visible_tip_height, BlockHeight::new(2));
    assert_eq!(compact_block.height(), BlockHeight::new(1));
    assert_eq!(compact_block_count, 2);
    assert!(matches!(
        first_event.event,
        ChainEvent::ChainCommitted { committed }
            if committed.block_range.start == BlockHeight::new(1)
    ));
    assert!(!first_event.cursor.as_bytes().is_empty());

    Ok(())
}

/// A canonical writer parked by a reorg deeper than its window reaches the
/// client as one typed error on new reads and on an already open
/// chain-event stream.
#[tokio::test]
async fn parked_writer_halt_reaches_unary_reads_and_open_chain_event_streams() -> eyre::Result<()> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let mut store_fixture =
        WalletServingStoreFixture::from_chain(&chain_fixture, activations.as_ref())?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
    )?);
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
    let readiness = zinder_runtime::Readiness::new(zinder_runtime::ReadinessState::ready(Some(2)));
    let grpc_adapter = WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default())
        .with_traffic_readiness(readiness.clone());
    let endpoint = spawn_gated_wallet_query(grpc_adapter, readiness.clone()).await?;
    let chain_index = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashRegtest,
    })?;
    let mut events = chain_index
        .chain_events(EventStreamStart::EarliestRetained)
        .await?;
    tokio::time::timeout(Duration::from_secs(2), events.next())
        .await?
        .ok_or_else(|| eyre!("chain-events stream closed before first event"))??;

    readiness.set(zinder_runtime::ReadinessState::reorg_window_exceeded(
        1_001,
        1_000,
        Some(1_000),
        Some(2),
    ));
    let stream_halt = loop {
        match tokio::time::timeout(Duration::from_secs(2), events.next()).await? {
            Some(Ok(_)) => {}
            Some(Err(error)) => break error,
            None => return Err(eyre!("chain-events stream ended without a typed halt")),
        }
    };
    let unary_halt = chain_index
        .current_epoch()
        .await
        .err()
        .ok_or_else(|| eyre!("a parked writer must refuse new reads"))?;

    for halt in [&stream_halt, &unary_halt] {
        assert!(
            matches!(
                halt,
                IndexerError::ReorgWindowExceeded {
                    replacement_depth_blocks: Some(1_001),
                    reorg_window_blocks: Some(1_000),
                    covered_depth_blocks: Some(1_000),
                }
            ),
            "{halt:?}"
        );
        assert_eq!(halt.retry_policy(), RetryPolicy::OperatorActionRequired);
    }
    Ok(())
}

#[tokio::test]
async fn remote_chain_index_returns_typed_network_upgrade_activations() -> eyre::Result<()> {
    let expected = sample_regtest_upgrade_activations();
    let activations = Arc::new(expected.clone());
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let mut store_fixture =
        WalletServingStoreFixture::from_chain(&chain_fixture, activations.as_ref())?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
    )?);
    let serving_pair_slot = WalletServingPairSlot::new(serving_pair);
    let ingest_control_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let admitted_ingest_control = AdmittedIngestControl::connect(
        ingest_control_fixture.endpoint(),
        None,
        Network::ZcashRegtest,
    )
    .await?;
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        serving_pair_slot,
        (),
        admitted_ingest_control,
        activations,
    );
    let endpoint = spawn_wallet_query(WalletQueryGrpcAdapter::new(
        wallet_query,
        WalletEndpointMetadata::default(),
    ))
    .await?;
    let chain_index = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashRegtest,
    })?;

    let activations = chain_index.network_upgrade_activations().await?;
    let erased: &dyn ChainIndex = &chain_index;
    let erased_activations = erased.network_upgrade_activations().await?;

    assert_eq!(activations, expected);
    assert_eq!(erased_activations, expected);
    let first = activations
        .activations()
        .first()
        .ok_or_else(|| eyre!("fixture activation table must not be empty"))?;
    let expected_first = NetworkUpgradeActivation {
        branch_id: ConsensusBranchId::new(0x5BA8_1B19),
        activation_height: BlockHeight::new(1),
        name: "Overwinter".to_owned(),
    };
    assert_eq!(first, &expected_first);
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end contract test keeps capability admission, exact-pair reads, pagination, empty history, and epoch replacement in one server lifecycle"
)]
async fn serving_pair_address_indexes_are_epoch_bound_ascending_and_cursor_resumable()
-> eyre::Result<()> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let address_script_hash = TransparentAddressScriptHash::from_bytes([0x51; 32]);
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let indexed_blocks = chain_fixture.blocks().to_vec();
    let chain_fixture =
        indexed_blocks
            .iter()
            .enumerate()
            .fold(chain_fixture, |chain, (tx_index, block)| {
                chain.with_address_output_index(TransparentUnspentOutput::new(
                    address_script_hash,
                    vec![0x51],
                    TransparentOutPoint::new(
                        TransactionId::from_bytes(
                            [u8::try_from(tx_index + 1).unwrap_or_default(); 32],
                        ),
                        0,
                    ),
                    u64::try_from(tx_index + 1).unwrap_or_default(),
                    block.height,
                    block.hash,
                ))
            });
    let mut store_fixture =
        WalletServingStoreFixture::from_chain(&chain_fixture, activations.as_ref())?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
    )?);
    let ingest_control_fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let admitted_ingest_control = AdmittedIngestControl::connect(
        ingest_control_fixture.endpoint(),
        None,
        Network::ZcashRegtest,
    )
    .await?;
    let wallet_query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(serving_pair),
        (),
        admitted_ingest_control,
        activations,
    );
    let endpoint = spawn_wallet_query(WalletQueryGrpcAdapter::new(
        wallet_query,
        WalletEndpointMetadata::default(),
    ))
    .await?;
    let chain_index = Arc::new(RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashRegtest,
    })?);

    let server_info = chain_index.server_info().await?;
    assert!(server_info.supports(Capability::TransparentAddressUnspentOutputs));
    assert!(server_info.supports(Capability::TransparentAddressHistory));
    assert!(!server_info.supports(Capability::Broadcast));
    let snapshot = OwnedChainSnapshot::capture(Arc::clone(&chain_index)).await?;
    let mut unspent_outputs = snapshot
        .transparent_address_unspent_outputs(address_script_hash, BlockHeight::new(0))
        .await?;
    let mut unspent_count = 0;
    while let Some(output) = unspent_outputs.next().await {
        let output = output?;
        assert_eq!(output.chain_epoch, snapshot.chain_epoch());
        unspent_count += 1;
    }
    assert_eq!(unspent_count, 3);

    let mut first_page = snapshot
        .transparent_address_tx_ids_in_range(TransparentAddressTxIdsQuery {
            address_script_hash,
            start_height: BlockHeight::new(0),
            end_height: BlockHeight::new(3),
            max_entries: Some(NonZeroU32::new(2).ok_or_else(|| eyre!("two is non-zero"))?),
            from_cursor: None,
            at_epoch_id: None,
        })
        .await?;
    let first = first_page
        .next()
        .await
        .ok_or_else(|| eyre!("first history page omitted height one"))??;
    let second = first_page
        .next()
        .await
        .ok_or_else(|| eyre!("first history page omitted height two"))??;
    assert!(first_page.next().await.is_none());
    assert_eq!(
        [first.artifact.block_height, second.artifact.block_height],
        [BlockHeight::new(1), BlockHeight::new(2)]
    );
    assert_eq!(first.chain_epoch, snapshot.chain_epoch());
    assert_eq!(second.chain_epoch, snapshot.chain_epoch());
    assert!(first.cursor.is_none());
    let cursor = second
        .cursor
        .ok_or_else(|| eyre!("first history page omitted its continuation"))?;

    let mut second_page = snapshot
        .transparent_address_tx_ids_in_range(TransparentAddressTxIdsQuery {
            address_script_hash,
            start_height: BlockHeight::new(0),
            end_height: BlockHeight::new(3),
            max_entries: Some(NonZeroU32::new(2).ok_or_else(|| eyre!("two is non-zero"))?),
            from_cursor: Some(cursor),
            at_epoch_id: None,
        })
        .await?;
    let third = second_page
        .next()
        .await
        .ok_or_else(|| eyre!("second history page omitted height three"))??;
    assert!(second_page.next().await.is_none());
    assert_eq!(third.artifact.block_height, BlockHeight::new(3));
    assert_eq!(third.chain_epoch, snapshot.chain_epoch());
    assert!(third.cursor.is_none());

    let mut empty_page = snapshot
        .transparent_address_tx_ids_in_range(TransparentAddressTxIdsQuery {
            address_script_hash: TransparentAddressScriptHash::from_bytes([0x52; 32]),
            start_height: BlockHeight::new(0),
            end_height: BlockHeight::new(3),
            max_entries: None,
            from_cursor: None,
            at_epoch_id: None,
        })
        .await?;
    assert!(empty_page.next().await.is_none());

    let mut expired_page = chain_index
        .transparent_address_tx_ids_in_range(TransparentAddressTxIdsQuery {
            address_script_hash,
            start_height: BlockHeight::new(0),
            end_height: BlockHeight::new(3),
            max_entries: None,
            from_cursor: None,
            at_epoch_id: Some(zinder_client::ChainEpochId::new(
                snapshot.chain_epoch().id.value() + 1,
            )),
        })
        .await?;
    assert!(matches!(
        expired_page.next().await,
        Some(Err(zinder_client::IndexerError::ChainEpochPinUnavailable))
    ));

    Ok(())
}

#[tokio::test]
async fn remote_chain_event_stream_rejects_duplicate_sequence_from_the_wire() -> eyre::Result<()> {
    let chain_epoch = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(2)
        .chain_epoch(zinder_client::ChainEpochId::new(1))
        .ok_or_else(|| eyre!("chain fixture must be non-empty"))?;
    let event = wallet::ChainEventEnvelope {
        cursor: vec![1],
        event_sequence: 1,
        chain_view: Some(wallet::ChainView {
            chain_epoch: Some(zinder_proto::wire::chain_epoch_message(chain_epoch)),
            indexed_tip: None,
            upstream_tip: None,
            materialized_views: None,
        }),
        event: Some(wallet::chain_event_envelope::Event::ChainCommitted(
            wallet::ChainCommitted {
                committed: Some(wallet::ChainEpochCommitted {
                    chain_epoch: Some(zinder_proto::wire::chain_epoch_message(chain_epoch)),
                    start_height: 1,
                    end_height: 2,
                }),
            },
        )),
    };
    let endpoint = spawn_malformed_chain_event_service(vec![event.clone(), event]).await?;
    let client = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashRegtest,
    })?;
    let mut stream = client
        .chain_events(EventStreamStart::EarliestRetained)
        .await?;

    assert!(matches!(stream.next().await, Some(Ok(_))));
    assert!(matches!(
        stream.next().await,
        Some(Err(zinder_client::IndexerError::MalformedResponse {
            field: "event_sequence",
            ..
        }))
    ));
    Ok(())
}

#[tokio::test]
async fn remote_chain_event_stream_rejects_a_nonadjacent_repeated_cursor_from_the_wire()
-> eyre::Result<()> {
    let chain_epoch = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(2)
        .chain_epoch(zinder_client::ChainEpochId::new(1))
        .ok_or_else(|| eyre!("chain fixture must be non-empty"))?;
    let mut first = chain_event_message(chain_epoch, 1, vec![1]);
    let second = chain_event_message(chain_epoch, 2, vec![2]);
    let mut repeated = chain_event_message(chain_epoch, 3, vec![1]);
    // Keep the event payloads independent so the regression specifically
    // exercises cursor identity across nonadjacent stream items.
    first.event_sequence = 1;
    repeated.event_sequence = 3;

    let endpoint = spawn_malformed_chain_event_service(vec![first, second, repeated]).await?;
    let client = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashRegtest,
    })?;
    let mut stream = client
        .chain_events(EventStreamStart::EarliestRetained)
        .await?;

    assert!(matches!(stream.next().await, Some(Ok(_))));
    assert!(matches!(stream.next().await, Some(Ok(_))));
    assert!(matches!(
        stream.next().await,
        Some(Err(zinder_client::IndexerError::MalformedResponse {
            field: "cursor",
            ..
        }))
    ));
    Ok(())
}

#[tokio::test]
async fn remote_mempool_event_stream_accepts_increasing_sequences_from_the_wire() -> eyre::Result<()>
{
    let endpoint = spawn_malformed_mempool_event_service(vec![
        mempool_invalidated_message(1, vec![1]),
        mempool_invalidated_message(4, vec![2]),
    ])
    .await?;
    let mut stream = open_mempool_event_stream(endpoint).await?;

    assert!(matches!(stream.next().await, Some(Ok(event)) if event.event_sequence == 1));
    assert!(matches!(stream.next().await, Some(Ok(event)) if event.event_sequence == 4));
    assert!(stream.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn remote_mempool_event_stream_rejects_repeated_and_decreasing_sequences_from_the_wire()
-> eyre::Result<()> {
    for rejected_sequence in [2, 1] {
        let endpoint = spawn_malformed_mempool_event_service(vec![
            mempool_invalidated_message(2, vec![1]),
            mempool_invalidated_message(rejected_sequence, vec![2]),
        ])
        .await?;
        let mut stream = open_mempool_event_stream(endpoint).await?;

        assert!(matches!(stream.next().await, Some(Ok(_))));
        assert!(matches!(
            stream.next().await,
            Some(Err(IndexerError::MalformedResponse {
                field: "mempool_event_envelope.event_sequence",
                ..
            }))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn remote_mempool_event_stream_rejects_a_nonadjacent_repeated_cursor_from_the_wire()
-> eyre::Result<()> {
    let endpoint = spawn_malformed_mempool_event_service(vec![
        mempool_invalidated_message(1, vec![1]),
        mempool_invalidated_message(2, vec![2]),
        mempool_invalidated_message(3, vec![1]),
    ])
    .await?;
    let mut stream = open_mempool_event_stream(endpoint).await?;

    assert!(matches!(stream.next().await, Some(Ok(_))));
    assert!(matches!(stream.next().await, Some(Ok(_))));
    assert!(matches!(
        stream.next().await,
        Some(Err(IndexerError::MalformedResponse {
            field: "mempool_event_envelope.cursor",
            ..
        }))
    ));
    Ok(())
}

async fn open_mempool_event_stream(
    endpoint: String,
) -> eyre::Result<zinder_client::MempoolEventStream> {
    let client = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashRegtest,
    })?;
    Ok(client
        .mempool_events(EventStreamStart::EarliestRetained)
        .await?)
}

fn mempool_invalidated_message(
    event_sequence: u64,
    cursor: Vec<u8>,
) -> wallet::MempoolEventEnvelope {
    wallet::MempoolEventEnvelope {
        cursor,
        event_sequence,
        source_observed_unix_millis: 1_774_670_400_000,
        event: Some(wallet::mempool_event_envelope::Event::Invalidated(
            wallet::MempoolInvalidatedEvent {
                transaction_id: "11".repeat(32),
                reason: wallet::MempoolEvictionReason::Expired as i32,
            },
        )),
    }
}

fn chain_event_message(
    chain_epoch: zinder_client::ChainEpoch,
    event_sequence: u64,
    cursor: Vec<u8>,
) -> wallet::ChainEventEnvelope {
    wallet::ChainEventEnvelope {
        cursor,
        event_sequence,
        chain_view: Some(wallet::ChainView {
            chain_epoch: Some(zinder_proto::wire::chain_epoch_message(chain_epoch)),
            indexed_tip: None,
            upstream_tip: None,
            materialized_views: None,
        }),
        event: Some(wallet::chain_event_envelope::Event::ChainCommitted(
            wallet::ChainCommitted {
                committed: Some(wallet::ChainEpochCommitted {
                    chain_epoch: Some(zinder_proto::wire::chain_epoch_message(chain_epoch)),
                    start_height: 1,
                    end_height: 2,
                }),
            },
        )),
    }
}

/// Replays a fixed list of events for one server-streaming method and answers
/// every other method as unimplemented.
struct MalformedEventService<Request, Event> {
    method_path: &'static str,
    events: Arc<Vec<Event>>,
    request: PhantomData<fn() -> Request>,
}

impl<Request, Event> Clone for MalformedEventService<Request, Event> {
    fn clone(&self) -> Self {
        Self {
            method_path: self.method_path,
            events: Arc::clone(&self.events),
            request: PhantomData,
        }
    }
}

impl<Request, Event> NamedService for MalformedEventService<Request, Event> {
    const NAME: &'static str = "zinder.v1.wallet.WalletQuery";
}

struct ReplayEventsMethod<Event>(Arc<Vec<Event>>);

impl<Req, Event> ServerStreamingService<Req> for ReplayEventsMethod<Event>
where
    Event: Clone + Send + Sync + 'static,
{
    type Response = Event;
    type ResponseStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<Self::Response, Status>> + Send>>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

    fn call(&mut self, _request: tonic::Request<Req>) -> Self::Future {
        let events = Arc::clone(&self.0);
        Box::pin(async move {
            let stream = tokio_stream::iter(events.as_ref().clone().into_iter().map(Ok));
            Ok(Response::new(Box::pin(stream) as Self::ResponseStream))
        })
    }
}

impl<B, Req, Event> Service<http::Request<B>> for MalformedEventService<Req, Event>
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
    Req: tonic_prost::prost::Message + Default + Send + 'static,
    Event: tonic_prost::prost::Message + Clone + Send + Sync + 'static,
{
    type Response = http::Response<TonicBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path() != self.method_path {
            return Box::pin(async move {
                let mut response = http::Response::new(TonicBody::default());
                response.headers_mut().insert(
                    Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(response)
            });
        }

        let events = Arc::clone(&self.events);
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::<Event, Req>::default();
            let mut grpc = tonic::server::Grpc::new(codec);
            Ok(grpc
                .server_streaming(ReplayEventsMethod(events), request)
                .await)
        })
    }
}

async fn spawn_malformed_event_service<Request, Event>(
    method_path: &'static str,
    events: Vec<Event>,
) -> eyre::Result<String>
where
    Request: tonic_prost::prost::Message + Default + Send + 'static,
    Event: tonic_prost::prost::Message + Clone + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);
    let service = MalformedEventService::<Request, Event> {
        method_path,
        events: Arc::new(events),
        request: PhantomData,
    };
    tokio::spawn(async move {
        let _server_result = Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await;
    });
    Ok(format!("http://{addr}"))
}

async fn spawn_malformed_chain_event_service(
    events: Vec<wallet::ChainEventEnvelope>,
) -> eyre::Result<String> {
    spawn_malformed_event_service::<wallet::ChainEventsRequest, _>(
        "/zinder.v1.wallet.WalletQuery/ChainEvents",
        events,
    )
    .await
}

async fn spawn_malformed_mempool_event_service(
    events: Vec<wallet::MempoolEventEnvelope>,
) -> eyre::Result<String> {
    spawn_malformed_event_service::<wallet::MempoolEventsRequest, _>(
        "/zinder.v1.wallet.WalletQuery/MempoolEvents",
        events,
    )
    .await
}

async fn spawn_gated_wallet_query<QueryApi>(
    grpc_adapter: WalletQueryGrpcAdapter<QueryApi>,
    readiness: zinder_runtime::Readiness,
) -> eyre::Result<String>
where
    QueryApi: WalletQueryApi,
    WalletQueryGrpcAdapter<QueryApi>: zinder_proto::v1::wallet::wallet_query_server::WalletQuery,
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);
    let gated = tonic::service::interceptor::InterceptedService::new(
        grpc_adapter.into_server(),
        zinder_runtime::TrafficReadinessInterceptor::new(readiness),
    );
    tokio::spawn(async move {
        let _server_result = Server::builder()
            .add_service(gated)
            .serve_with_incoming(incoming)
            .await;
    });

    Ok(format!("http://{addr}"))
}

async fn spawn_wallet_query<QueryApi>(
    grpc_adapter: WalletQueryGrpcAdapter<QueryApi>,
) -> eyre::Result<String>
where
    QueryApi: WalletQueryApi,
    WalletQueryGrpcAdapter<QueryApi>: zinder_proto::v1::wallet::wallet_query_server::WalletQuery,
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(async move {
        let _server_result = Server::builder()
            .add_service(grpc_adapter.into_server())
            .serve_with_incoming(incoming)
            .await;
    });

    Ok(format!("http://{addr}"))
}
