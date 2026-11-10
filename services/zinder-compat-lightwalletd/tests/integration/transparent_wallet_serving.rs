#![allow(
    missing_docs,
    reason = "Integration test names describe the production compatibility contract under test."
)]

use std::{num::NonZeroU16, sync::Arc};

use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tonic::{Code, Request, transport::Server};
use zebra_chain::{
    parameters::NetworkKind as ZebraNetworkKind, transparent::Address as ZebraTransparentAddress,
};
use zinder_compat_lightwalletd::{LightwalletdAdmissionError, LightwalletdGrpcAdapter};
use zinder_core::{
    BlockBlobArtifact, BlockHeaderArtifact, BlockHeight, BlockHeightRange,
    CommitmentTreeCheckpoint, CompactBlockArtifact, ConsensusBranchId, Network,
    NetworkUpgradeActivation, NetworkUpgradeActivations, SubtreeRootArtifact, SubtreeRootRange,
    TransactionBlobArtifact, TransactionId, TransactionLocation, TransparentAddressScriptHash,
    TransparentOutPoint, TransparentUnspentOutput, wire::encode_branch_id_hex,
};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::CompactTxStreamer,
};
use zinder_query::{
    CanonicalReader, WalletProjectionReader, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_runtime::{
    NodeUnavailableDetail, Readiness, ReadinessState, TrafficReadinessInterceptor,
};
use zinder_store::{
    CanonicalEventFence, CanonicalStoreError, ChainEventEnvelope, ChainEventHistoryRequest,
    ChainEventStreamFamily, ChainEventStreamResume, EventStreamStartPosition, RawBlobRetention,
};
use zinder_testkit::{
    ChainFixture, FixtureTransactionRows, MockTransactionBroadcaster, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

type WalletServingAdapter = LightwalletdGrpcAdapter<WalletServingQuery<MockTransactionBroadcaster>>;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "One production-pair scenario keeps the list, stream, floor, cap, txid round trip, and support-signal assertions causally connected."
)]
async fn production_pair_serves_transparent_utxo_contract_and_support_signal() -> eyre::Result<()> {
    let address_a = transparent_address(0x11);
    let address_b = transparent_address(0x22);
    let primary_address_text = address_a.to_string();
    let secondary_address_text = address_b.to_string();
    let script_a = address_a.script().as_raw_bytes().to_vec();
    let script_b = address_b.script().as_raw_bytes().to_vec();
    let hash_a = TransparentAddressScriptHash::of_script_pub_key(&script_a);
    let hash_b = TransparentAddressScriptHash::of_script_pub_key(&script_b);
    let before_floor_id = TransactionId::from_bytes([0x10; 32]);
    let at_floor_id = TransactionId::from_bytes([0x20; 32]);
    let other_first_id = TransactionId::from_bytes([0x30; 32]);
    let other_second_id = TransactionId::from_bytes([0x40; 32]);
    let at_floor_transaction_bytes = b"production-utxo-round-trip".to_vec();
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(2);
    let block_one = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre::eyre!("UTXO fixture must include block 1"))?
        .clone();
    let block_two = chain
        .block_at(BlockHeight::new(2))
        .ok_or_else(|| eyre::eyre!("UTXO fixture must include block 2"))?
        .clone();
    let chain = chain
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            before_floor_id,
            block_one.height,
            block_one.hash,
            0,
            b"before-floor".to_vec(),
        ))
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            at_floor_id,
            block_two.height,
            block_two.hash,
            0,
            at_floor_transaction_bytes.clone(),
        ))
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            other_first_id,
            block_two.height,
            block_two.hash,
            1,
            b"other-first".to_vec(),
        ))
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            other_second_id,
            block_two.height,
            block_two.hash,
            2,
            b"other-second".to_vec(),
        ))
        .with_address_output_index(TransparentUnspentOutput::new(
            hash_a,
            script_a.clone(),
            TransparentOutPoint::new(before_floor_id, 0),
            10,
            block_one.height,
            block_one.hash,
        ))
        .with_address_output_index(TransparentUnspentOutput::new(
            hash_a,
            script_a,
            TransparentOutPoint::new(at_floor_id, 0),
            20,
            block_two.height,
            block_two.hash,
        ))
        .with_address_output_index(TransparentUnspentOutput::new(
            hash_b,
            script_b.clone(),
            TransparentOutPoint::new(other_first_id, 0),
            30,
            block_two.height,
            block_two.hash,
        ))
        .with_address_output_index(TransparentUnspentOutput::new(
            hash_b,
            script_b,
            TransparentOutPoint::new(other_second_id, 0),
            40,
            block_two.height,
            block_two.hash,
        ));
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
    let adapter = build_wallet_serving_adapter(&mut store_fixture, activations)?;

    let all_request = lightwalletd::GetAddressUtxosArg {
        addresses: vec![primary_address_text.clone()],
        start_height: 1,
        max_entries: 10,
    };
    let list = adapter
        .get_address_utxos(Request::new(all_request.clone()))
        .await?
        .into_inner();
    let stream = adapter
        .get_address_utxos_stream(Request::new(all_request))
        .await?
        .into_inner();
    let streamed = collect_stream(stream).await?;
    assert_eq!(list.address_utxos, streamed);
    assert_eq!(streamed.len(), 2);

    let floor_response = adapter
        .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg {
            addresses: vec![primary_address_text.clone()],
            start_height: 2,
            max_entries: 10,
        }))
        .await?
        .into_inner();
    assert_eq!(floor_response.address_utxos.len(), 1);
    let floor_utxo = floor_response
        .address_utxos
        .first()
        .ok_or_else(|| eyre::eyre!("start-height floor must retain one UTXO"))?;
    assert_eq!(floor_utxo.height, 2);
    assert_eq!(floor_utxo.txid, at_floor_id.as_bytes());

    let capped = adapter
        .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg {
            addresses: vec![primary_address_text, secondary_address_text],
            start_height: 1,
            max_entries: 2,
        }))
        .await?
        .into_inner();
    assert_eq!(capped.address_utxos.len(), 2);

    let transaction = adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: None,
            index: 0,
            hash: floor_utxo.txid.clone(),
        }))
        .await?
        .into_inner();
    assert_eq!(transaction.height, 2);
    assert_eq!(transaction.data, at_floor_transaction_bytes);

    let info = adapter
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    assert!(info.taddr_support);
    Ok(())
}

/// A runtime that discovered the pre-NU7 node table serves the writer-admitted
/// NU7 schedule: NU7 is the next pending upgrade below its activation height
/// and the active branch from that height on.
#[tokio::test]
async fn lightd_info_reports_the_writer_admitted_upgrade_across_its_activation() -> eyre::Result<()>
{
    let startup_activations = Arc::new(pre_nu7_activations()?);
    let nu7_branch_id = ConsensusBranchId::new(0x7719_0ad9);
    let nu7_height = BlockHeight::new(4);
    let mut admitted_entries = startup_activations.activations().to_vec();
    admitted_entries.push(NetworkUpgradeActivation {
        branch_id: nu7_branch_id,
        activation_height: nu7_height,
        name: "NU7".to_owned(),
    });
    let admitted_activations =
        NetworkUpgradeActivations::new(Network::ZcashRegtest, admitted_entries)?;
    let nu6_3_branch_id = startup_activations
        .active_at(BlockHeight::new(3))
        .map(|activation| encode_branch_id_hex(activation.branch_id))
        .ok_or_else(|| eyre::eyre!("the pre-NU7 table must have an upgrade active at height 3"))?;

    for tip in [3, 4] {
        let chain = ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::Transactions)
            .extend_blocks(tip);
        let mut store_fixture = WalletServingStoreFixture::from_chain_after_admitted_live_append(
            &chain,
            &startup_activations,
            &admitted_activations,
        )?;
        let adapter =
            build_wallet_serving_adapter(&mut store_fixture, Arc::clone(&startup_activations))?;
        let info = adapter
            .get_lightd_info(Request::new(lightwalletd::Empty {}))
            .await?
            .into_inner();
        assert_eq!(info.block_height, u64::from(tip));
        if BlockHeight::new(tip) < nu7_height {
            assert_eq!(info.consensus_branch_id, nu6_3_branch_id);
            assert_eq!(info.upgrade_name, "NU7");
            assert_eq!(info.upgrade_height, u64::from(nu7_height.value()));
        } else {
            assert_eq!(
                info.consensus_branch_id,
                encode_branch_id_hex(nu7_branch_id)
            );
            assert_eq!(info.upgrade_name, "");
            assert_eq!(info.upgrade_height, 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn admitted_transparent_support_stays_immutable_through_readiness_recovery()
-> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(1);
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
    let adapter = build_wallet_serving_adapter(&mut store_fixture, activations)?;
    let readiness = Readiness::new(ReadinessState::ready(Some(1)));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_readiness = TrafficReadinessInterceptor::new(readiness.clone());
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(tonic::service::interceptor::InterceptedService::new(
                adapter.into_server(),
                server_readiness,
            ))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _shutdown_result = shutdown_rx.await;
            })
            .await
    });
    let mut client = CompactTxStreamerClient::connect(format!("http://{address}")).await?;

    let initial = client
        .get_lightd_info(lightwalletd::Empty {})
        .await?
        .into_inner();
    assert!(initial.taddr_support);

    readiness.set(ReadinessState::node_unavailable_with_detail(
        NodeUnavailableDetail::first_iteration("node_unreachable", "synthetic provider outage"),
        Some(1),
    ));
    let outage = match client.get_lightd_info(lightwalletd::Empty {}).await {
        Ok(response) => {
            return Err(eyre::eyre!(
                "expected readiness rejection, got {response:?}"
            ));
        }
        Err(status) => status,
    };
    assert_eq!(outage.code(), Code::Unavailable);

    readiness.set(ReadinessState::ready(Some(1)));
    let recovered = client
        .get_lightd_info(lightwalletd::Empty {})
        .await?
        .into_inner();
    assert!(
        recovered.taddr_support,
        "readiness recovery must not rewrite the immutable LightdInfo claim"
    );

    if shutdown_tx.send(()).is_err() {
        return Err(eyre::eyre!(
            "compatibility listener shutdown receiver dropped"
        ));
    }
    server.await??;
    Ok(())
}

#[tokio::test]
async fn unretained_transactions_reject_compatibility_before_listener_binding() -> eyre::Result<()>
{
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(1);
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
    )?);
    let query = WalletServingQuery::from_serving_pair_slot(
        WalletServingPairSlot::new(serving_pair),
        MockTransactionBroadcaster::broadcast_disabled(),
        activations.clone(),
    );
    let reservation = TcpListener::bind("127.0.0.1:0").await?;
    let address = reservation.local_addr()?;
    drop(reservation);

    let admission = LightwalletdGrpcAdapter::from_admitted_compatibility_query(query);
    assert!(matches!(
        admission,
        Err(
            LightwalletdAdmissionError::TransactionRetentionUnavailable {
                retention: RawBlobRetention::None
            }
        )
    ));

    let listener = TcpListener::bind(address).await?;
    drop(listener);
    Ok(())
}

#[tokio::test]
async fn production_pair_drains_both_transparent_history_rpcs_across_native_pages()
-> eyre::Result<()> {
    let address = transparent_address(0x31);
    let address_text = address.to_string();
    let script = address.script().as_raw_bytes().to_vec();
    let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(2);
    let block_one = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre::eyre!("history fixture must include block 1"))?
        .clone();
    let block_two = chain
        .block_at(BlockHeight::new(2))
        .ok_or_else(|| eyre::eyre!("history fixture must include block 2"))?
        .clone();
    let before_floor_id = history_transaction_id(u32::MAX);
    let mut chain = chain
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            before_floor_id,
            block_one.height,
            block_one.hash,
            0,
            history_transaction_bytes(u32::MAX),
        ))
        .with_address_output_index(TransparentUnspentOutput::new(
            address_script_hash,
            script.clone(),
            TransparentOutPoint::new(before_floor_id, 0),
            1,
            block_one.height,
            block_one.hash,
        ));
    for index in 0..=1_000_u32 {
        let transaction_id = history_transaction_id(index);
        chain = chain
            .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
                transaction_id,
                block_two.height,
                block_two.hash,
                index,
                history_transaction_bytes(index),
            ))
            .with_address_output_index(TransparentUnspentOutput::new(
                address_script_hash,
                script.clone(),
                TransparentOutPoint::new(transaction_id, 0),
                1,
                block_two.height,
                block_two.hash,
            ));
    }
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
    let adapter = build_wallet_serving_adapter(&mut store_fixture, activations)?;
    let request = transparent_history_filter(address_text, 2, 2);

    let deprecated = adapter
        .get_taddress_txids(Request::new(request.clone()))
        .await?
        .into_inner();
    let deprecated = collect_stream(deprecated).await?;
    let current = adapter
        .get_taddress_transactions(Request::new(request))
        .await?
        .into_inner();
    let current = collect_stream(current).await?;

    assert_eq!(deprecated.len(), 1_001);
    assert_eq!(current.len(), 1_001);
    assert!(deprecated.iter().all(|transaction| transaction.height == 2));
    assert!(current.iter().all(|transaction| transaction.height == 2));
    assert_eq!(deprecated[0].data, history_transaction_bytes(0));
    assert_eq!(deprecated[1_000].data, history_transaction_bytes(1_000));
    assert_eq!(current[0].data, history_transaction_bytes(0));
    assert_eq!(current[1_000].data, history_transaction_bytes(1_000));
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "The large pre-range fixture and lower-bound assertions describe one indexed-range contract."
)]
async fn production_pair_seeks_transparent_address_ranges_past_a_large_prefix() -> eyre::Result<()>
{
    let address = transparent_address(0x33);
    let address_text = address.to_string();
    let script = address.script().as_raw_bytes().to_vec();
    let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(2);
    let block_one = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre::eyre!("history fixture must include block 1"))?
        .clone();
    let block_two = chain
        .block_at(BlockHeight::new(2))
        .ok_or_else(|| eyre::eyre!("history fixture must include block 2"))?
        .clone();
    let mut chain = chain;
    for index in 0..=1_000_u32 {
        let transaction_id = history_transaction_id(index);
        chain = chain
            .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
                transaction_id,
                block_one.height,
                block_one.hash,
                index,
                history_transaction_bytes(index),
            ))
            .with_address_output_index(TransparentUnspentOutput::new(
                address_script_hash,
                script.clone(),
                TransparentOutPoint::new(transaction_id, 0),
                1,
                block_one.height,
                block_one.hash,
            ));
    }
    let at_floor_id = history_transaction_id(1_001);
    let chain = chain
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            at_floor_id,
            block_two.height,
            block_two.hash,
            0,
            history_transaction_bytes(1_001),
        ))
        .with_address_output_index(TransparentUnspentOutput::new(
            address_script_hash,
            script,
            TransparentOutPoint::new(at_floor_id, 0),
            1,
            block_two.height,
            block_two.hash,
        ));
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let page_size = NonZeroU16::new(1).ok_or_else(|| eyre::eyre!("page size must be non-zero"))?;

    let outputs = wallet_reader.address_unspent_outputs_page_from_height(
        address_script_hash,
        BlockHeight::new(2),
        None,
        page_size,
    )?;
    assert_eq!(outputs.outputs.len(), 1);
    assert_eq!(
        outputs.outputs[0].created_at.block.height,
        BlockHeight::new(2)
    );
    assert_eq!(outputs.next_page_after, None);

    let history = wallet_reader.address_transaction_history_range_page(
        address_script_hash,
        BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(2)),
        None,
        page_size,
    )?;
    assert_eq!(history.transactions.len(), 1);
    assert_eq!(
        history.transactions[0].key.block_height(),
        BlockHeight::new(2)
    );
    assert_eq!(history.next_page_after, None);

    let adapter = build_wallet_serving_adapter_from_readers(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
        activations,
    )?;
    let history = collect_stream(
        adapter
            .get_taddress_transactions(Request::new(transparent_history_filter(address_text, 2, 2)))
            .await?
            .into_inner(),
    )
    .await?;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].height, 2);
    Ok(())
}

#[tokio::test]
async fn production_history_maps_a_missing_canonical_blob_to_not_found() -> eyre::Result<()> {
    let address = transparent_address(0x32);
    let address_text = address.to_string();
    let script = address.script().as_raw_bytes().to_vec();
    let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
    let transaction_id = TransactionId::from_bytes([0x32; 32]);
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(1);
    let block = chain
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre::eyre!("missing-blob fixture must include block 1"))?
        .clone();
    let chain = chain
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            transaction_id,
            block.height,
            block.hash,
            0,
            b"present-before-fault-injection".to_vec(),
        ))
        .with_address_output_index(TransparentUnspentOutput::new(
            address_script_hash,
            script,
            TransparentOutPoint::new(transaction_id, 0),
            1,
            block.height,
            block.hash,
        ));
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let canonical_reader: Arc<dyn CanonicalReader> =
        Arc::new(MissingTransactionBlobCanonicalReader {
            canonical_reader,
            missing_transaction_id: transaction_id,
        });
    let wallet_reader: Arc<dyn WalletProjectionReader> = Arc::new(wallet_reader);
    let adapter =
        build_wallet_serving_adapter_from_readers(canonical_reader, wallet_reader, activations)?;
    let request = transparent_history_filter(address_text, 1, 1);

    let deprecated_status = first_stream_error(
        adapter
            .get_taddress_txids(Request::new(request.clone()))
            .await?
            .into_inner(),
    )
    .await?;
    let current_status = first_stream_error(
        adapter
            .get_taddress_transactions(Request::new(request))
            .await?
            .into_inner(),
    )
    .await?;
    assert_eq!(deprecated_status.code(), Code::NotFound);
    assert_eq!(current_status.code(), Code::NotFound);
    Ok(())
}

fn build_wallet_serving_adapter(
    store_fixture: &mut WalletServingStoreFixture,
    activations: Arc<zinder_core::NetworkUpgradeActivations>,
) -> eyre::Result<WalletServingAdapter> {
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    build_wallet_serving_adapter_from_readers(
        Arc::new(canonical_reader),
        Arc::new(wallet_reader),
        activations,
    )
}

fn build_wallet_serving_adapter_from_readers(
    canonical_reader: Arc<dyn CanonicalReader>,
    wallet_reader: Arc<dyn WalletProjectionReader>,
    activations: Arc<zinder_core::NetworkUpgradeActivations>,
) -> eyre::Result<WalletServingAdapter> {
    let serving_pair = Arc::new(WalletServingReadPair::new(canonical_reader, wallet_reader)?);
    let serving_pair_slot = WalletServingPairSlot::new(serving_pair);
    let query = WalletServingQuery::from_serving_pair_slot(
        serving_pair_slot,
        MockTransactionBroadcaster::broadcast_disabled(),
        activations,
    );
    LightwalletdGrpcAdapter::from_admitted_compatibility_query(query).map_err(Into::into)
}

/// The regtest table a pre-NU7 node advertises, with NU6.3 moved to height 3
/// so the fixture chain reaches it and no upgrade remains pending.
fn pre_nu7_activations() -> eyre::Result<NetworkUpgradeActivations> {
    let activations = sample_regtest_upgrade_activations()
        .activations()
        .iter()
        .cloned()
        .map(|activation| {
            let activation_height = if activation.name == "NU6.3" {
                BlockHeight::new(3)
            } else {
                activation.activation_height
            };
            NetworkUpgradeActivation {
                activation_height,
                ..activation
            }
        })
        .collect();
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        activations,
    )?)
}

fn transparent_address(byte: u8) -> ZebraTransparentAddress {
    ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [byte; 20])
}

fn transparent_history_filter(
    address: String,
    start_height: u64,
    end_height: u64,
) -> lightwalletd::TransparentAddressBlockFilter {
    lightwalletd::TransparentAddressBlockFilter {
        address,
        range: Some(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: start_height,
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: end_height,
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }),
    }
}

fn history_transaction_id(index: u32) -> TransactionId {
    let mut bytes = [0x71; 32];
    bytes[28..].copy_from_slice(&index.to_be_bytes());
    TransactionId::from_bytes(bytes)
}

fn history_transaction_bytes(index: u32) -> Vec<u8> {
    format!("transparent-history-{index:04}").into_bytes()
}

async fn collect_stream<T, Stream>(mut stream: Stream) -> Result<Vec<T>, tonic::Status>
where
    Stream: tonic::codegen::tokio_stream::Stream<Item = Result<T, tonic::Status>> + Unpin,
{
    let mut items = Vec::new();
    while let Some(stream_result) = stream.next().await {
        items.push(stream_result?);
    }
    Ok(items)
}

async fn first_stream_error<T, Stream>(mut stream: Stream) -> eyre::Result<tonic::Status>
where
    Stream: tonic::codegen::tokio_stream::Stream<Item = Result<T, tonic::Status>> + Unpin,
{
    match stream.next().await {
        Some(Err(status)) => Ok(status),
        Some(Ok(_)) => Err(eyre::eyre!("expected stream error, received an item")),
        None => Err(eyre::eyre!("expected stream error, stream ended")),
    }
}

struct MissingTransactionBlobCanonicalReader {
    canonical_reader: zinder_store::RocksDbCanonicalSecondary,
    missing_transaction_id: TransactionId,
}

impl CanonicalReader for MissingTransactionBlobCanonicalReader {
    fn network_upgrade_schedule(&self) -> Option<&zinder_store::CanonicalActivationSchedule> {
        self.canonical_reader.network_upgrade_schedule()
    }

    fn raw_blob_retention(&self) -> RawBlobRetention {
        self.canonical_reader.raw_blob_retention()
    }

    fn network(&self) -> Network {
        self.canonical_reader.network()
    }

    fn event_fence(&self) -> CanonicalEventFence {
        self.canonical_reader.event_fence()
    }

    fn chain_epoch(&self) -> Result<zinder_core::ChainEpoch, CanonicalStoreError> {
        self.canonical_reader.chain_epoch()
    }

    fn chain_epoch_at(
        &self,
        epoch_id: zinder_core::ChainEpochId,
    ) -> Result<zinder_core::ChainEpoch, CanonicalStoreError> {
        self.canonical_reader.chain_epoch_at(epoch_id)
    }

    fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockHeaderArtifact>, CanonicalStoreError> {
        self.canonical_reader.block_header_at(height)
    }

    fn block_hash_lookup(
        &self,
        block_hash: zinder_core::BlockHash,
    ) -> Result<zinder_store::BlockHashLookup, CanonicalStoreError> {
        self.canonical_reader.block_hash_lookup(block_hash)
    }

    fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CompactBlockArtifact>, CanonicalStoreError> {
        self.canonical_reader.compact_block_at(height)
    }

    fn compact_blocks_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<CompactBlockArtifact>, CanonicalStoreError> {
        self.canonical_reader.compact_blocks_in_range(range)
    }

    fn block_blob_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockBlobArtifact>, CanonicalStoreError> {
        self.canonical_reader.block_blob_at(height)
    }

    fn block_blobs_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockBlobArtifact>>, CanonicalStoreError> {
        self.canonical_reader.block_blobs_in_range(range)
    }

    fn transaction_location(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionLocation>, CanonicalStoreError> {
        self.canonical_reader.transaction_location(transaction_id)
    }

    fn transaction_blob(
        &self,
        location: TransactionLocation,
    ) -> Result<Option<TransactionBlobArtifact>, CanonicalStoreError> {
        if location.transaction_id == self.missing_transaction_id {
            return Ok(None);
        }
        self.canonical_reader.transaction_blob(location)
    }

    fn tree_state_checkpoint_at_or_before(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CommitmentTreeCheckpoint>, CanonicalStoreError> {
        self.canonical_reader
            .tree_state_checkpoint_at_or_before(height)
    }

    fn subtree_roots(
        &self,
        range: SubtreeRootRange,
    ) -> Result<Vec<SubtreeRootArtifact>, CanonicalStoreError> {
        self.canonical_reader.subtree_roots(range)
    }

    fn wallet_chain_event_history(
        &self,
        request: ChainEventHistoryRequest<'_>,
    ) -> Result<Vec<ChainEventEnvelope>, CanonicalStoreError> {
        self.canonical_reader.wallet_chain_event_history(request)
    }

    fn resolve_wallet_chain_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, CanonicalStoreError> {
        self.canonical_reader
            .resolve_wallet_chain_event_stream_start(start, requested_family)
    }
}
