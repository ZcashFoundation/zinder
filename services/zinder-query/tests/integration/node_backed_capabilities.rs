#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::{io::Read as _, io::Write as _, net::TcpStream, time::Duration};

use async_trait::async_trait;
use tonic::{Code, Request};
use tonic_types::StatusExt;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, BroadcastAccepted, BroadcastRejected,
    BroadcastRejectionReason, ChainValuePool, ChainValuePools, Network, RawTransactionBytes,
    TransactionBroadcastOutcome, TransactionId,
};
use zinder_proto::capabilities::{
    WALLET_BROADCAST_TRANSACTION_V1, WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
    WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
};
use zinder_proto::v1::wallet::{self, wallet_query_server::WalletQuery as WalletQueryService};
use zinder_query::{
    AdmittedIngestControl, NodeBroadcast, WalletEndpointMetadata, WalletQueryApi,
    WalletQueryGrpcAdapter, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_source::{
    BroadcastNodeProbe, NodeCapabilities, NodeCapability, NodeSource, SourceBlock, SourceError,
    TransactionBroadcaster,
};
use zinder_store::RawBlobRetention;
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

#[tokio::test]
async fn native_and_operations_surfaces_advertise_the_same_admitted_capabilities()
-> eyre::Result<()> {
    let source = ProbedValuePoolSource {
        capabilities: NodeCapabilities::new([
            NodeCapability::OpenRpcDiscovery,
            NodeCapability::TipId,
            NodeCapability::ChainValuePools,
        ])?,
        value_pools: ChainValuePools::new(
            BlockId::new(BlockHeight::new(7), BlockHash::from_bytes([0x77; 32])),
            Vec::new(),
        ),
        fetch_count: Arc::new(AtomicUsize::new(0)),
        broadcast_count: Arc::new(AtomicUsize::new(0)),
        broadcast_outcome: None,
    };
    let NodeQueryFixture { query, .. } = serving_query(source).await?;
    let admitted = query.native_endpoint_capabilities().shared_identifiers();
    let adapter = WalletQueryGrpcAdapter::new(query.clone(), WalletEndpointMetadata::default());
    let native =
        WalletQueryService::server_info(&adapter, Request::new(wallet::ServerInfoRequest {}))
            .await?
            .into_inner()
            .info
            .and_then(|info| info.common)
            .ok_or_else(|| eyre::eyre!("ServerInfo omitted common metadata"))?
            .capabilities;

    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    let readiness = zinder_runtime::Readiness::new(zinder_runtime::ReadinessState::ready(Some(1)));
    let ops = zinder_runtime::spawn_ops_endpoint(
        address,
        zinder_runtime::OpsServer {
            service_name: "zinder-query-test",
            service_version: "0.0.0",
            network_name: "zcash-regtest",
            advertised_capabilities: Arc::clone(&admitted),
        },
        readiness,
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let healthz = tokio::task::spawn_blocking(move || fetch_healthz(address)).await??;
    ops.shutdown().await?;
    let operations = healthz["capabilities"]
        .as_array()
        .ok_or_else(|| eyre::eyre!("/healthz omitted capabilities"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| eyre::eyre!("/healthz capability is not a string"))
        })
        .collect::<eyre::Result<Vec<_>>>()?;

    assert_eq!(
        native,
        admitted
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(operations, native);
    Ok(())
}

fn fetch_healthz(address: std::net::SocketAddr) -> eyre::Result<serde_json::Value> {
    let mut stream = TcpStream::connect(address)?;
    stream.write_all(
        format!("GET /healthz HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n").as_bytes(),
    )?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    let body_offset = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| offset + 4)
        .ok_or_else(|| eyre::eyre!("operations response omitted HTTP body delimiter"))?;
    Ok(serde_json::from_slice(&response[body_offset..])?)
}

#[tokio::test]
async fn openrpc_value_pool_method_is_not_sufficient_native_admission_evidence() -> eyre::Result<()>
{
    let source_tip = BlockId::new(BlockHeight::new(7), BlockHash::from_bytes([0x77; 32]));
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let source = ProbedValuePoolSource {
        capabilities: NodeCapabilities::new([
            NodeCapability::OpenRpcDiscovery,
            NodeCapability::TipId,
            NodeCapability::ChainValuePools,
        ])?,
        value_pools: ChainValuePools::new(
            source_tip,
            vec![ChainValuePool::new("sapling", true, Some(42))],
        ),
        fetch_count: Arc::clone(&fetch_count),
        broadcast_count: Arc::new(AtomicUsize::new(0)),
        broadcast_outcome: None,
    };
    let NodeQueryFixture { query, .. } = serving_query(source).await?;

    assert!(
        !query
            .native_endpoint_capabilities()
            .contains(WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1)
    );
    let adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());
    let server_info =
        WalletQueryService::server_info(&adapter, Request::new(wallet::ServerInfoRequest {}))
            .await?
            .into_inner()
            .info
            .and_then(|info| info.common)
            .ok_or_else(|| eyre::eyre!("ServerInfo omitted common endpoint metadata"))?;
    assert!(
        !server_info
            .capabilities
            .iter()
            .any(|capability| capability == WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1)
    );

    let status = WalletQueryService::chain_value_pools_at_tip(
        &adapter,
        Request::new(wallet::ChainValuePoolsAtTipRequest {}),
    )
    .await
    .err()
    .ok_or_else(|| eyre::eyre!("weakly admitted value-pool read unexpectedly succeeded"))?;
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(fetch_count.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn absent_node_value_pool_capability_is_omitted_and_rejected_before_fetch() -> eyre::Result<()>
{
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let source = ProbedValuePoolSource {
        capabilities: NodeCapabilities::new([NodeCapability::OpenRpcDiscovery])?,
        value_pools: ChainValuePools::new(
            BlockId::new(BlockHeight::new(7), BlockHash::from_bytes([0x77; 32])),
            Vec::new(),
        ),
        fetch_count: Arc::clone(&fetch_count),
        broadcast_count: Arc::new(AtomicUsize::new(0)),
        broadcast_outcome: None,
    };
    let NodeQueryFixture { query, .. } = serving_query(source).await?;

    assert!(
        !query
            .native_endpoint_capabilities()
            .contains(WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1)
    );
    let adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());
    let status = WalletQueryService::chain_value_pools_at_tip(
        &adapter,
        Request::new(wallet::ChainValuePoolsAtTipRequest {}),
    )
    .await
    .err()
    .ok_or_else(|| eyre::eyre!("unadvertised value-pool read unexpectedly succeeded"))?;
    let details = status.get_error_details();
    let reason = details.error_info().map(|info| info.reason.as_str());

    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(reason, Some("ENDPOINT_CAPABILITY_UNAVAILABLE"));
    assert_eq!(fetch_count.load(Ordering::SeqCst), 0);
    Ok(())
}

/// Composition never contacts the node: a configured node advertises
/// broadcast at once, whatever it would report on first contact.
#[tokio::test]
async fn configured_node_advertises_broadcast_without_being_contacted() -> eyre::Result<()> {
    let source = ProbedValuePoolSource {
        capabilities: NodeCapabilities::new([NodeCapability::OpenRpcDiscovery])?,
        value_pools: ChainValuePools::new(
            BlockId::new(BlockHeight::new(7), BlockHash::from_bytes([0x77; 32])),
            Vec::new(),
        ),
        fetch_count: Arc::new(AtomicUsize::new(0)),
        broadcast_count: Arc::new(AtomicUsize::new(0)),
        broadcast_outcome: None,
    };
    let contact_count = Arc::clone(&source.fetch_count);

    let NodeQueryFixture { query, .. } = serving_query(source).await?;

    assert!(
        query
            .native_endpoint_capabilities()
            .contains(WALLET_BROADCAST_TRANSACTION_V1)
    );
    assert!(query.upstream_node_capabilities().is_none());
    assert_eq!(contact_count.load(Ordering::SeqCst), 0);
    Ok(())
}

/// Tree state is served from storage. A node that lists tree state but not
/// `sendrawtransaction` fails first-contact verification, so it never
/// receives a transaction while every stored read keeps working.
#[tokio::test]
async fn node_without_broadcast_fails_verification_and_receives_no_transaction() -> eyre::Result<()>
{
    let broadcast_count = Arc::new(AtomicUsize::new(0));
    let source = ProbedValuePoolSource {
        capabilities: NodeCapabilities::new([
            NodeCapability::OpenRpcDiscovery,
            NodeCapability::TreeState,
        ])?,
        value_pools: ChainValuePools::new(
            BlockId::new(BlockHeight::new(7), BlockHash::from_bytes([0x77; 32])),
            Vec::new(),
        ),
        fetch_count: Arc::new(AtomicUsize::new(0)),
        broadcast_count: Arc::clone(&broadcast_count),
        broadcast_outcome: None,
    };

    let NodeQueryFixture {
        query, node, slot, ..
    } = serving_query(source).await?;

    assert!(matches!(
        node.verify(&slot).await,
        Err(SourceError::NodeCapabilityMissing {
            capability: NodeCapability::TransactionBroadcast
        })
    ));
    assert!(
        query
            .native_endpoint_capabilities()
            .contains(WALLET_READ_TREE_STATE_AT_HEIGHT_V2)
    );
    assert_eq!(
        query.tree_state_at(BlockHeight::new(1), None).await?.height,
        BlockHeight::new(1)
    );
    assert!(matches!(
        query
            .broadcast_transaction(RawTransactionBytes::new(vec![1]))
            .await,
        Err(zinder_query::QueryError::Node(
            SourceError::NodeUnavailable { .. }
        ))
    ));
    assert_eq!(broadcast_count.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn verified_broadcast_is_advertised_and_invokable_beside_stored_tree_state()
-> eyre::Result<()> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(2);
    let fixture_block = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre::eyre!("fixture must contain height 1"))?;
    let block_id = BlockId::new(fixture_block.height, fixture_block.hash);
    let broadcast_count = Arc::new(AtomicUsize::new(0));
    let accepted_transaction_id = TransactionId::from_bytes([0x55; 32]);
    let source = ProbedValuePoolSource {
        capabilities: NodeCapabilities::new([
            NodeCapability::OpenRpcDiscovery,
            NodeCapability::TipId,
            NodeCapability::TransactionBroadcast,
        ])?,
        value_pools: ChainValuePools::new(block_id, Vec::new()),
        fetch_count: Arc::new(AtomicUsize::new(0)),
        broadcast_count: Arc::clone(&broadcast_count),
        broadcast_outcome: Some(TransactionBroadcastOutcome::Accepted(BroadcastAccepted {
            transaction_id: accepted_transaction_id,
        })),
    };
    let NodeQueryFixture {
        query, node, slot, ..
    } = serving_query_from_chain(source, &chain, &activations).await?;
    node.verify(&slot).await?;

    for capability in [
        WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
        WALLET_BROADCAST_TRANSACTION_V1,
    ] {
        assert!(query.native_endpoint_capabilities().contains(capability));
    }
    let adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());
    let tree_state = WalletQueryService::tree_state_at_height(
        &adapter,
        Request::new(wallet::TreeStateAtHeightRequest {
            height: block_id.height.value(),
            at_epoch_id: None,
        }),
    )
    .await?
    .into_inner();
    assert_eq!(tree_state.height, block_id.height.value());
    assert_eq!(
        tree_state.block_hash,
        zinder_core::wire::encode_rpc_block_hash_hex(block_id.hash)
    );
    let broadcast = WalletQueryService::broadcast_transaction(
        &adapter,
        Request::new(wallet::BroadcastTransactionRequest {
            raw_transaction: vec![1],
        }),
    )
    .await?
    .into_inner();
    assert!(matches!(
        broadcast.outcome,
        Some(wallet::broadcast_transaction_response::Outcome::Accepted(
            wallet::BroadcastAccepted {
                transaction_id
            }
        )) if transaction_id
            == zinder_core::wire::encode_rpc_transaction_id_hex(accepted_transaction_id)
    ));
    assert_eq!(broadcast_count.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn native_broadcast_carries_nu7_rejection_reasons_on_the_wire() -> eyre::Result<()> {
    let cases = [
        (
            BroadcastRejectionReason::UnsupportedTransactionVersion,
            wallet::BroadcastRejectionReason::UnsupportedTransactionVersion,
            "transaction version 4 not supported by the network upgrade Nu7",
        ),
        (
            BroadcastRejectionReason::ExceedsBlockActionLimit,
            wallet::BroadcastRejectionReason::ExceedsBlockActionLimit,
            "mempool transaction has 331 Orchard actions, exceeding the next block's ZIP 218 limit of 330",
        ),
    ];

    for (kind, expected_wire_kind, message) in cases {
        let source = ProbedValuePoolSource {
            capabilities: NodeCapabilities::new([
                NodeCapability::OpenRpcDiscovery,
                NodeCapability::TipId,
                NodeCapability::TransactionBroadcast,
            ])?,
            value_pools: ChainValuePools::new(
                BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x11; 32])),
                Vec::new(),
            ),
            fetch_count: Arc::new(AtomicUsize::new(0)),
            broadcast_count: Arc::new(AtomicUsize::new(0)),
            broadcast_outcome: Some(TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                kind,
                error_code: Some(-25),
                message: message.to_owned(),
            })),
        };
        let NodeQueryFixture {
            query, node, slot, ..
        } = serving_query(source).await?;
        node.verify(&slot).await?;
        let adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());

        let broadcast = WalletQueryService::broadcast_transaction(
            &adapter,
            Request::new(wallet::BroadcastTransactionRequest {
                raw_transaction: vec![1],
            }),
        )
        .await?
        .into_inner();

        assert!(matches!(
            broadcast.outcome,
            Some(wallet::broadcast_transaction_response::Outcome::Rejected(rejected))
                if rejected.kind == expected_wire_kind as i32
                    && rejected.error_code == Some(-25)
                    && rejected.message == message
        ));
    }
    Ok(())
}

/// A query without a configured node serves storage only and advertises no
/// broadcast.
#[tokio::test]
async fn query_without_a_node_omits_broadcast_and_keeps_tree_state() -> eyre::Result<()> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(1);
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical),
        Arc::new(wallet),
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(pair),
        ingest_control,
    )
    .with_node_broadcast::<ProbedValuePoolSource>(None);

    assert!(
        query
            .native_endpoint_capabilities()
            .contains(WALLET_READ_TREE_STATE_AT_HEIGHT_V2)
    );
    assert!(
        !query
            .native_endpoint_capabilities()
            .contains(WALLET_BROADCAST_TRANSACTION_V1)
    );
    assert_eq!(
        query.tree_state_at(BlockHeight::new(1), None).await?.height,
        BlockHeight::new(1)
    );
    assert!(matches!(
        query
            .broadcast_transaction(RawTransactionBytes::new(vec![1]))
            .await,
        Err(zinder_query::QueryError::EndpointCapabilityUnavailable {
            capability: WALLET_BROADCAST_TRANSACTION_V1
        })
    ));
    Ok(())
}

/// Native query composed over a node double, with the node handle and slot
/// a test verifies against.
struct NodeQueryFixture {
    query: WalletServingQuery<Option<NodeBroadcast<ProbedValuePoolSource>>, AdmittedIngestControl>,
    node: NodeBroadcast<ProbedValuePoolSource>,
    slot: WalletServingPairSlot,
    _store_fixture: WalletServingStoreFixture,
    _ingest_control_fixture: zinder_testkit::IngestControlFixture,
}

async fn serving_query(source: ProbedValuePoolSource) -> eyre::Result<NodeQueryFixture> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(1);
    serving_query_from_chain(source, &chain, &activations).await
}

async fn serving_query_from_chain(
    source: ProbedValuePoolSource,
    chain: &ChainFixture,
    activations: &Arc<zinder_core::NetworkUpgradeActivations>,
) -> eyre::Result<NodeQueryFixture> {
    let mut store_fixture = WalletServingStoreFixture::from_chain(chain, activations)?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical),
        Arc::new(wallet),
    )?);
    let (ingest_control, ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let slot = WalletServingPairSlot::new(pair);
    let node = NodeBroadcast::new(source);
    let query = WalletServingQuery::from_admitted_native_serving_pair(slot.clone(), ingest_control)
        .with_node_broadcast(Some(node.clone()));
    Ok(NodeQueryFixture {
        query,
        node,
        slot,
        _store_fixture: store_fixture,
        _ingest_control_fixture: ingest_control_fixture,
    })
}

#[derive(Clone)]
struct ProbedValuePoolSource {
    capabilities: NodeCapabilities,
    value_pools: ChainValuePools,
    fetch_count: Arc<AtomicUsize>,
    broadcast_count: Arc<AtomicUsize>,
    broadcast_outcome: Option<TransactionBroadcastOutcome>,
}

#[async_trait]
impl NodeSource for ProbedValuePoolSource {
    fn capabilities(&self) -> NodeCapabilities {
        self.capabilities
    }

    async fn fetch_block_at(&self, _height: BlockHeight) -> Result<SourceBlock, SourceError> {
        Err(SourceError::NodeUnavailable {
            reason: "node-backed capability test does not fetch blocks".to_owned(),
        })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        Ok(self.value_pools.source_tip)
    }

    async fn fetch_chain_value_pools_at_tip(&self) -> Result<ChainValuePools, SourceError> {
        self.fetch_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.value_pools.clone())
    }
}

#[async_trait]
impl BroadcastNodeProbe for ProbedValuePoolSource {
    async fn probe_capabilities(&self) -> Result<NodeCapabilities, SourceError> {
        self.fetch_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.capabilities)
    }

    async fn fetch_network_upgrade_activations(
        &self,
    ) -> Result<zinder_core::NetworkUpgradeActivations, SourceError> {
        Ok(sample_regtest_upgrade_activations())
    }
}

#[async_trait]
impl TransactionBroadcaster for ProbedValuePoolSource {
    async fn broadcast_transaction(
        &self,
        _raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, SourceError> {
        self.broadcast_count.fetch_add(1, Ordering::SeqCst);
        self.broadcast_outcome
            .clone()
            .ok_or(SourceError::TransactionBroadcastDisabled)
    }
}
