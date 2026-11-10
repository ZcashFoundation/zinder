#![allow(
    missing_docs,
    reason = "Integration test names describe the current Android SDK RPC contract."
)]

use std::sync::Arc;

use async_trait::async_trait;
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use zebra_chain::{
    parameters::NetworkKind as ZebraNetworkKind, transparent::Address as ZebraTransparentAddress,
};
use zinder_compat_lightwalletd::{
    LightwalletdGrpcAdapter, MempoolEventEnvelopeStream, MempoolSnapshotPage, MempoolSurface,
    MempoolSurfaceError,
};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, ChainEpoch, ChainEpochId, ChainTipMetadata,
    CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput, CompactTransaction,
    CompactTransactionData, CompactTransparentOutput, MempoolEntry, MempoolObservation, Network,
    RawTransactionBytes, TransactionComponentCounts, TransactionId, TransparentAddressScriptHash,
    TransparentOutPoint, TransparentUnspentOutput, UnixTimestampMillis,
    wire::{encode_branch_id_hex, encode_internal_transaction_id},
};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::{WalletServingPairSlot, WalletServingQuery, WalletServingReadPair};
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSource, SourceError, TransactionBroadcaster,
    TreeStateUpstream,
};
use zinder_store::{CURRENT_ARTIFACT_SCHEMA_VERSION, MempoolEventEnvelope, RawBlobRetention};
use zinder_testkit::{
    ChainFixture, FixtureTransactionRows, MockTransactionBroadcaster, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

const BLOCK_HEIGHT: BlockHeight = BlockHeight::new(1);
const RAW_TRANSACTION_BYTES: &[u8] = b"current-android-sdk-mined-transaction";
const MEMPOOL_TRANSACTION_BYTES: &[u8] = b"current-android-sdk-mempool-transaction";
const SUBMITTED_TRANSACTION_BYTES: &[u8] = b"current-android-sdk-submitted-transaction";
const TREE_STATE_PAYLOAD: &[u8] = br#"{"sapling":{"commitments":{"finalState":"sapling"}},"orchard":{"commitments":{"finalState":"orchard"}},"ironwood":{"commitments":{"finalState":"ironwood"}}}"#;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "One transport-bound scenario keeps the current Android SDK RPC matrix and its shared admitted serving pair together."
)]
async fn current_android_sdk_rpc_contract_uses_admitted_production_pair() -> eyre::Result<()> {
    let transparent_address = transparent_address(0x41);
    let transparent_address_text = transparent_address.to_string();
    let transparent_script = transparent_address.script().as_raw_bytes().to_vec();
    let transaction_id = TransactionId::from_bytes([0x51; 32]);
    let accepted_transaction_id = TransactionId::from_bytes([0x52; 32]);
    let base_chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(1)
        .with_tree_state_checkpoint_payload_at(BLOCK_HEIGHT, TREE_STATE_PAYLOAD.to_vec());
    let block = base_chain
        .block_at(BLOCK_HEIGHT)
        .ok_or_else(|| eyre::eyre!("Android SDK contract fixture must contain block 1"))?
        .clone();
    let compact_block = CompactBlockArtifact::new(
        BlockId::new(block.height, block.hash),
        Network::ZcashRegtest.genesis_hash(),
        block.block_time_seconds,
        vec![CompactTransaction {
            index: 0,
            transaction_id,
            data: CompactTransactionData {
                sapling_outputs: vec![CompactSaplingOutput {
                    commitment: [0x11; 32],
                    ephemeral_key: [0x12; 32],
                    ciphertext: [0x13; 52],
                }],
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 42,
                    script_pub_key: transparent_script.clone(),
                }],
                ..CompactTransactionData::default()
            },
        }],
        CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    )?;
    let mut transaction_rows = FixtureTransactionRows::from_raw_transaction(
        transaction_id,
        block.height,
        block.hash,
        0,
        RAW_TRANSACTION_BYTES.to_vec(),
    );
    transaction_rows.facts.public_facts.counts = TransactionComponentCounts {
        transparent_input_count: 0,
        transparent_output_count: 1,
        sapling_spend_count: 0,
        sapling_output_count: 1,
        orchard_action_count: 0,
        ironwood_action_count: 0,
        sprout_joinsplit_count: 0,
    };
    let chain = base_chain
        .with_compact_block_artifact(compact_block)
        .with_transaction_rows(transaction_rows)
        .with_address_output_index(TransparentUnspentOutput::new(
            TransparentAddressScriptHash::of_script_pub_key(&transparent_script),
            transparent_script,
            TransparentOutPoint::new(transaction_id, 0),
            42,
            block.height,
            block.hash,
        ));
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
    )?);
    let source = AndroidSdkNodeSource::accepted(
        accepted_transaction_id,
        BlockId::new(block.height, block.hash),
        block.block_time_seconds,
    )?;
    let query = WalletServingQuery::from_probed_node_source(
        WalletServingPairSlot::new(serving_pair),
        source.clone(),
        activations.clone(),
    )?;
    let adapter = LightwalletdGrpcAdapter::from_admitted_compatibility_query(query)?
        .with_mempool_surface(Arc::new(AndroidSdkMempoolSurface::new()?));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _shutdown_result = shutdown_receiver.await;
            })
            .await
    });
    let mut client = CompactTxStreamerClient::connect(format!("http://{address}")).await?;

    let mut block_range = client
        .get_block_range(lightwalletd::BlockRange {
            start: Some(block_id_at(BLOCK_HEIGHT)),
            end: Some(block_id_at(BLOCK_HEIGHT)),
            pool_types: Vec::new(),
        })
        .await?
        .into_inner();
    let default_pool_block = block_range
        .message()
        .await?
        .ok_or_else(|| eyre::eyre!("GetBlockRange must return the admitted block"))?;
    assert_eq!(default_pool_block.height, u64::from(BLOCK_HEIGHT.value()));
    assert_eq!(default_pool_block.vtx.len(), 1);
    assert_eq!(default_pool_block.vtx[0].outputs.len(), 1);
    assert!(
        default_pool_block.vtx[0].vin.is_empty() && default_pool_block.vtx[0].vout.is_empty(),
        "the default-empty poolTypes request must preserve shielded data and omit transparent data"
    );
    assert!(block_range.message().await?.is_none());

    let latest_block = client
        .get_latest_block(lightwalletd::ChainSpec {})
        .await?
        .into_inner();
    assert_eq!(latest_block.height, u64::from(BLOCK_HEIGHT.value()));

    let lightd_info = client
        .get_lightd_info(lightwalletd::Empty {})
        .await?
        .into_inner();
    assert!(
        lightd_info.taddr_support,
        "an admitted transaction-retaining production pair must make a positive immutable taddrSupport claim"
    );
    let active_upgrade = activations
        .active_at(BLOCK_HEIGHT)
        .ok_or_else(|| eyre::eyre!("the SDK contract fixture must have an active upgrade"))?;
    assert_eq!(lightd_info.chain_name, "test");
    assert_eq!(
        lightd_info.sapling_activation_height,
        u64::from(
            activations
                .activation_height_by_name("Sapling")
                .ok_or_else(|| eyre::eyre!("the SDK contract fixture must activate Sapling"))?
                .value(),
        )
    );
    assert_eq!(
        lightd_info.consensus_branch_id,
        encode_branch_id_hex(active_upgrade.branch_id)
    );
    let next_pending_upgrade = activations
        .activations()
        .iter()
        .find(|activation| activation.activation_height > BLOCK_HEIGHT)
        .ok_or_else(|| eyre::eyre!("the SDK contract fixture must have a pending upgrade"))?;
    assert_eq!(lightd_info.upgrade_name, next_pending_upgrade.name);
    assert_eq!(
        lightd_info.upgrade_height,
        u64::from(next_pending_upgrade.activation_height.value())
    );

    let send_response = client
        .send_transaction(lightwalletd::RawTransaction {
            data: SUBMITTED_TRANSACTION_BYTES.to_vec(),
            height: 0,
        })
        .await?
        .into_inner();
    assert_eq!(send_response.error_code, 0);
    assert_eq!(source.broadcaster.call_count(), 1);
    assert_eq!(
        source.broadcaster.captured_calls()[0].as_slice(),
        SUBMITTED_TRANSACTION_BYTES
    );

    let transaction = client
        .get_transaction(lightwalletd::TxFilter {
            block: None,
            index: 0,
            hash: encode_internal_transaction_id(transaction_id).to_vec(),
        })
        .await?
        .into_inner();
    assert_eq!(transaction.height, u64::from(BLOCK_HEIGHT.value()));
    assert_eq!(transaction.data, RAW_TRANSACTION_BYTES);

    let mut address_utxos = client
        .get_address_utxos_stream(lightwalletd::GetAddressUtxosArg {
            addresses: vec![transparent_address_text.clone()],
            start_height: u64::from(BLOCK_HEIGHT.value()),
            max_entries: 1,
        })
        .await?
        .into_inner();
    let address_utxo = address_utxos
        .message()
        .await?
        .ok_or_else(|| eyre::eyre!("GetAddressUtxosStream must return the indexed output"))?;
    assert_eq!(address_utxo.address, transparent_address_text);
    assert_eq!(
        address_utxo.txid,
        encode_internal_transaction_id(transaction_id).to_vec()
    );
    assert_eq!(address_utxo.value_zat, 42);
    assert!(address_utxos.message().await?.is_none());

    let mut taddress_transactions = client
        .get_taddress_txids(transparent_address_filter(transparent_address.to_string()))
        .await?
        .into_inner();
    let taddress_transaction = taddress_transactions
        .message()
        .await?
        .ok_or_else(|| eyre::eyre!("GetTaddressTxids must return the indexed transaction"))?;
    assert_eq!(taddress_transaction.height, u64::from(BLOCK_HEIGHT.value()));
    assert_eq!(taddress_transaction.data, RAW_TRANSACTION_BYTES);
    assert!(taddress_transactions.message().await?.is_none());

    for shielded_protocol in [
        lightwalletd::ShieldedProtocol::Sapling,
        lightwalletd::ShieldedProtocol::Orchard,
        lightwalletd::ShieldedProtocol::Ironwood,
    ] {
        let mut subtree_roots = client
            .get_subtree_roots(lightwalletd::GetSubtreeRootsArg {
                start_index: 0,
                shielded_protocol: shielded_protocol as i32,
                max_entries: 1,
            })
            .await?
            .into_inner();
        assert!(
            subtree_roots.message().await?.is_none(),
            "the minimal admitted fixture has no completed {shielded_protocol:?} subtree roots, but the RPC must end promptly"
        );
    }

    let tree_state = client
        .get_tree_state(block_id_at(BLOCK_HEIGHT))
        .await?
        .into_inner();
    assert_eq!(tree_state.height, u64::from(BLOCK_HEIGHT.value()));
    assert_eq!(tree_state.network, "test");

    let mut mempool_transactions = client
        .get_mempool_stream(lightwalletd::Empty {})
        .await?
        .into_inner();
    let mempool_transaction = mempool_transactions
        .message()
        .await?
        .ok_or_else(|| eyre::eyre!("GetMempoolStream must return the accepted snapshot entry"))?;
    assert_eq!(mempool_transaction.data, MEMPOOL_TRANSACTION_BYTES);
    assert_eq!(mempool_transaction.height, 0);
    assert!(mempool_transactions.message().await?.is_none());

    if shutdown_sender.send(()).is_err() {
        return Err(eyre::eyre!(
            "Android SDK contract listener shutdown receiver dropped"
        ));
    }
    server.await??;
    Ok(())
}

fn block_id_at(height: BlockHeight) -> lightwalletd::BlockId {
    lightwalletd::BlockId {
        height: u64::from(height.value()),
        hash: Vec::new(),
    }
}

fn transparent_address(byte: u8) -> ZebraTransparentAddress {
    ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [byte; 20])
}

fn transparent_address_filter(address: String) -> lightwalletd::TransparentAddressBlockFilter {
    lightwalletd::TransparentAddressBlockFilter {
        address,
        range: Some(lightwalletd::BlockRange {
            start: Some(block_id_at(BLOCK_HEIGHT)),
            end: Some(block_id_at(BLOCK_HEIGHT)),
            pool_types: Vec::new(),
        }),
    }
}

struct AndroidSdkMempoolSurface {
    entry: MempoolEntry,
}

impl AndroidSdkMempoolSurface {
    fn new() -> eyre::Result<Self> {
        Ok(Self {
            entry: MempoolEntry::new(
                TransactionId::from_bytes([0x61; 32]),
                None,
                RawTransactionBytes::new(MEMPOOL_TRANSACTION_BYTES.to_vec()),
                CompactTransactionData::default(),
                MempoolObservation {
                    first_seen_unix_millis: UnixTimestampMillis::new(1_774_668_400_000),
                    first_seen_chain_epoch: synthetic_chain_epoch(),
                },
            )?,
        })
    }
}

#[async_trait]
impl MempoolSurface for AndroidSdkMempoolSurface {
    async fn mempool_snapshot_page(
        &self,
        _max_entries: u32,
        from_cursor: Option<Vec<u8>>,
    ) -> Result<MempoolSnapshotPage, MempoolSurfaceError> {
        Ok(MempoolSnapshotPage {
            chain_epoch_id: ChainEpochId::new(1),
            events_resume_cursor: None,
            entries: if from_cursor.is_none() {
                vec![self.entry.clone()]
            } else {
                Vec::new()
            },
            next_cursor: None,
        })
    }

    async fn mempool_events(
        &self,
        _from_cursor: Option<zinder_store::StreamCursorTokenV1>,
    ) -> Result<MempoolEventEnvelopeStream, MempoolSurfaceError> {
        Ok(Box::pin(tokio_stream::empty::<
            Result<MempoolEventEnvelope, MempoolSurfaceError>,
        >()))
    }
}

#[derive(Clone)]
struct AndroidSdkNodeSource {
    capabilities: NodeCapabilities,
    broadcaster: MockTransactionBroadcaster,
    tip: BlockId,
    tree_state: zinder_source::SourceTreeState,
}

impl AndroidSdkNodeSource {
    fn accepted(
        transaction_id: TransactionId,
        tip: BlockId,
        block_time_seconds: u32,
    ) -> eyre::Result<Self> {
        Ok(Self {
            capabilities: NodeCapabilities::new([
                NodeCapability::TipId,
                NodeCapability::TreeState,
                NodeCapability::TransactionBroadcast,
                NodeCapability::OpenRpcDiscovery,
            ])?,
            broadcaster: MockTransactionBroadcaster::accepted(transaction_id),
            tip,
            tree_state: zinder_source::SourceTreeState::new(
                tip,
                block_time_seconds,
                TREE_STATE_PAYLOAD.to_vec(),
            ),
        })
    }
}

#[async_trait]
impl NodeSource for AndroidSdkNodeSource {
    fn capabilities(&self) -> NodeCapabilities {
        self.capabilities
    }

    async fn fetch_block_at(
        &self,
        _height: BlockHeight,
    ) -> Result<zinder_source::SourceBlock, SourceError> {
        Err(SourceError::NodeCapabilityMissing {
            capability: NodeCapability::BestChainBlocks,
        })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        Ok(self.tip)
    }
}

#[async_trait]
impl TransactionBroadcaster for AndroidSdkNodeSource {
    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<zinder_core::TransactionBroadcastOutcome, SourceError> {
        self.broadcaster
            .broadcast_transaction(raw_transaction)
            .await
    }
}

#[async_trait]
impl TreeStateUpstream for AndroidSdkNodeSource {
    async fn fetch_tree_state_for_block(
        &self,
        _block_id: BlockId,
    ) -> Result<zinder_source::SourceTreeState, SourceError> {
        Ok(self.tree_state.clone())
    }
}

fn synthetic_chain_epoch() -> ChainEpoch {
    ChainEpoch {
        id: ChainEpochId::new(1),
        network: Network::ZcashRegtest,
        visible_tip_height: BLOCK_HEIGHT,
        visible_tip_hash: BlockHash::from_bytes([0x71; 32]),
        settled_tip_height: BLOCK_HEIGHT,
        settled_tip_hash: BlockHash::from_bytes([0x71; 32]),
        artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
        tip_metadata: ChainTipMetadata::empty(),
        created_at: UnixTimestampMillis::new(1_774_668_400_000),
    }
}
