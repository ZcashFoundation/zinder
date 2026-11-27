#![allow(
    missing_docs,
    reason = "Integration test names describe the production compatibility contract under test."
)]

use std::{
    collections::HashSet,
    num::{NonZeroU16, NonZeroU32},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tonic::{Code, Request, transport::Server};
use tonic_types::StatusExt as _;
use zebra_chain::{
    parameters::NetworkKind as ZebraNetworkKind, transparent::Address as ZebraTransparentAddress,
};
use zinder_compat_lightwalletd::{
    LightwalletdAdmissionError, LightwalletdGrpcAdapter, LightwalletdResponseLimits,
};
use zinder_core::{
    BlockBlobArtifact, BlockHeaderArtifact, BlockHeight, BlockHeightRange, CompactBlockArtifact,
    ConsensusBranchId, Network, NetworkUpgradeActivation, NetworkUpgradeActivations,
    SubtreeRootArtifact, SubtreeRootRange, TransactionBlobArtifact, TransactionId,
    TransparentAddressScriptHash, TransparentOutPoint, TransparentOutpointSpendStatus,
    TransparentUnspentOutput, wire::encode_branch_id_hex,
};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::CompactTxStreamer,
};
use zinder_query::{
    CanonicalReader, IndexedTransaction, RocksDbCanonicalReader, RocksDbWalletReader,
    SpendStatusReadEnd, StorageReadError, WalletProjectionReader, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_runtime::{Readiness, ReadinessCause, ReadinessState, TrafficReadinessInterceptor};
use zinder_store::{
    CanonicalEventFence, ChainEventEnvelope, ChainEventStreamFamily, ChainEventStreamResume,
    EventStreamStartPosition, RawBlobRetention, ServedTreeState,
};
use zinder_testkit::{
    ChainFixture, FixtureTransactionRows, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

type WalletServingAdapter = LightwalletdGrpcAdapter<WalletServingQuery<()>>;

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
        .with_raw_blob_retention(RawBlobRetention::All)
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
    let adapter = build_wallet_serving_adapter(&mut store_fixture)?;

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
    assert_eq!(capped.address_utxos.len(), 1);

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

/// A store built with the pre-NU7 table serves the NU7 schedule its writer
/// admitted later: NU7 is the next pending upgrade below its activation
/// height and the active branch from that height on.
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
            .with_raw_blob_retention(RawBlobRetention::All)
            .extend_blocks(tip);
        let mut store_fixture = WalletServingStoreFixture::from_chain_after_admitted_live_append(
            &chain,
            &startup_activations,
            &admitted_activations,
        )?;
        let adapter = build_wallet_serving_adapter(&mut store_fixture)?;
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
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(1);
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
    let adapter = build_wallet_serving_adapter(&mut store_fixture)?;
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

    readiness.set(ReadinessState::not_ready(
        ReadinessCause::StorageUnavailable,
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
        Arc::new(RocksDbCanonicalReader::new(canonical_reader)?),
        Arc::new(RocksDbWalletReader::new(wallet_reader)),
    )?);
    let query =
        WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(serving_pair));
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
        .with_raw_blob_retention(RawBlobRetention::All)
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
    let adapter = build_wallet_serving_adapter(&mut store_fixture)?;
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

/// Open-ended and oversized history ranges read to the tip, like lightwalletd.
///
/// lightwalletd defaults an absent or zero `range.end` to the chain tip
/// (lightwalletd `e7436d2`, `frontend/service.go:128-137`), and Zebra clamps
/// both bounds of `getaddresstxids` to its tip (Zebra 7.0.0,
/// `zebra-rpc/src/methods.rs:5163-5187`). The Swift and Android SDK Tor paths
/// send `end = None` (zcash-swift-wallet-sdk `ebd0add`, `rust/src/tor.rs:274`;
/// zcash-android-wallet-sdk `a95780a`, `backend-lib/src/main/rust/tor.rs:246`),
/// and `WebZjs` birthday detection sends `end = u64::MAX` (`WebZjs` `a50df94`,
/// `crates/webzjs-wallet/src/bindgen/wallet.rs:638-641`).
#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "One fixture keeps every open-ended range shape compared against the same tip-bounded rows."
)]
async fn taddress_history_resolves_open_and_oversized_ends_at_the_tip_like_lightwalletd()
-> eyre::Result<()> {
    let address = transparent_address(0x36);
    let address_text = address.to_string();
    let script = address.script().as_raw_bytes().to_vec();
    let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
    let mut chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(2);
    for height in [1_u32, 2] {
        let block = chain
            .block_at(BlockHeight::new(height))
            .ok_or_else(|| eyre::eyre!("history fixture must include block {height}"))?
            .clone();
        let transaction_id = history_transaction_id(height);
        chain = chain
            .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
                transaction_id,
                block.height,
                block.hash,
                0,
                history_transaction_bytes(height),
            ))
            .with_address_output_index(TransparentUnspentOutput::new(
                address_script_hash,
                script.clone(),
                TransparentOutPoint::new(transaction_id, 0),
                1,
                block.height,
                block.hash,
            ));
    }
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
    let adapter = build_wallet_serving_adapter(&mut store_fixture)?;
    let both_blocks = vec![
        (1, history_transaction_bytes(1)),
        (2, history_transaction_bytes(2)),
    ];
    let tip_block_only = vec![(2, history_transaction_bytes(2))];

    let cases = [
        ("end at the tip", 1, Some(2), &both_blocks),
        ("absent end", 1, None, &both_blocks),
        ("zero end", 1, Some(0), &both_blocks),
        ("end above the tip", 1, Some(1_000), &both_blocks),
        ("end above u32", 1, Some(u64::MAX), &both_blocks),
        ("genesis start", 0, None, &both_blocks),
        ("start above the tip", 1_000, None, &tip_block_only),
    ];
    for (shape, start_height, end_height, expected) in cases {
        let filter = history_filter_with_end(address_text.clone(), start_height, end_height);
        let deprecated = collect_stream(
            adapter
                .get_taddress_txids(Request::new(filter.clone()))
                .await?
                .into_inner(),
        )
        .await?;
        let current = collect_stream(
            adapter
                .get_taddress_transactions(Request::new(filter))
                .await?
                .into_inner(),
        )
        .await?;
        let deprecated: Vec<_> = deprecated
            .into_iter()
            .map(|transaction| (transaction.height, transaction.data))
            .collect();
        let current: Vec<_> = current
            .into_iter()
            .map(|transaction| (transaction.height, transaction.data))
            .collect();
        assert_eq!(&deprecated, expected, "GetTaddressTxids with {shape}");
        assert_eq!(&current, expected, "GetTaddressTransactions with {shape}");
    }

    let inverted = adapter
        .get_taddress_txids(Request::new(history_filter_with_end(
            address_text,
            2,
            Some(1),
        )))
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("an explicit end below the start must be rejected"))?;
    assert_eq!(inverted.code(), Code::InvalidArgument);
    Ok(())
}

/// `maxEntries = 0` returns every UTXO, like lightwalletd.
///
/// lightwalletd treats `maxEntries = 0` as "every UTXO" and stops only at a
/// positive `maxEntries` (lightwalletd `e7436d2`, `frontend/service.go:954`).
/// librustzcash, the Android SDK, Brave, and Zingo sum these rows as the
/// transparent balance, so a dropped row under-reports funds with no error.
#[tokio::test]
async fn address_utxos_with_zero_max_entries_return_every_utxo_like_lightwalletd()
-> eyre::Result<()> {
    let utxo_count = 1_001_u32;
    let (mut store_fixture, address_text) = many_utxo_fixture(0x34, utxo_count)?;
    let adapter = build_wallet_serving_adapter(&mut store_fixture)?;
    let request = lightwalletd::GetAddressUtxosArg {
        addresses: vec![address_text],
        start_height: 0,
        max_entries: 0,
    };

    let list = adapter
        .get_address_utxos(Request::new(request.clone()))
        .await?
        .into_inner()
        .address_utxos;
    let streamed = collect_stream(
        adapter
            .get_address_utxos_stream(Request::new(request))
            .await?
            .into_inner(),
    )
    .await?;

    assert_eq!(list, streamed);
    assert_eq!(streamed.len(), u32_to_usize(utxo_count));
    let returned_txids: Vec<_> = streamed.iter().map(|utxo| utxo.txid.clone()).collect();
    let expected_txids: Vec<_> = (0..utxo_count)
        .map(|index| history_transaction_id(index).as_bytes().to_vec())
        .collect();
    assert_eq!(returned_txids, expected_txids);
    Ok(())
}

/// A height that exceeds the response bound fails even with a smaller target.
#[tokio::test]
async fn address_utxos_refuse_a_height_over_the_response_limit() -> eyre::Result<()> {
    let utxo_count = 1_001_u32;
    let response_limit = NonZeroU32::MIN.saturating_add(999);
    let (mut store_fixture, address_text) = many_utxo_fixture(0x35, utxo_count)?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical_reader)?),
        Arc::new(RocksDbWalletReader::new(wallet_reader)),
    )?);
    let query =
        WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(serving_pair));
    let adapter = LightwalletdGrpcAdapter::with_response_limits(
        query,
        LightwalletdResponseLimits {
            max_address_utxos: response_limit,
            ..LightwalletdResponseLimits::default()
        },
    );
    let request = |max_entries| lightwalletd::GetAddressUtxosArg {
        addresses: vec![address_text.clone()],
        start_height: 0,
        max_entries,
    };

    for max_entries in [0, response_limit.get(), utxo_count] {
        let list_status = adapter
            .get_address_utxos(Request::new(request(max_entries)))
            .await
            .err()
            .ok_or_else(|| {
                eyre::eyre!("maxEntries {max_entries} must exceed the response limit")
            })?;
        assert_eq!(list_status.code(), Code::ResourceExhausted);
        let (streamed, stream_status) = drain_until_error(
            adapter
                .get_address_utxos_stream(Request::new(request(max_entries)))
                .await?
                .into_inner(),
        )
        .await;
        assert_eq!(streamed, []);
        assert_eq!(
            stream_status.map(|status| status.code()),
            Some(Code::ResourceExhausted),
            "maxEntries {max_entries} must exceed the response limit"
        );
    }

    Ok(())
}

/// The transparent history calls send each transaction as they read it,
/// and read its bytes once.
///
/// The history spans more than one native page.
#[tokio::test]
async fn taddress_history_streams_and_reads_each_transaction_once() -> eyre::Result<()> {
    const HISTORY: u32 = 1_001;
    let (mut store_fixture, address_text) = many_utxo_fixture(0x36, HISTORY)?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let canonical_reader = InstrumentedCanonicalReader::new(canonical_reader)?;
    let blob_reads = Arc::clone(&canonical_reader.transaction_blob_reads);
    let adapter = build_wallet_serving_adapter_from_readers(
        Arc::new(canonical_reader),
        Arc::new(RocksDbWalletReader::new(wallet_reader)),
    )?;
    let request = transparent_history_filter(address_text, 2, 2);

    for deprecated in [true, false] {
        blob_reads.store(0, Ordering::SeqCst);
        let mut stream = if deprecated {
            adapter
                .get_taddress_txids(Request::new(request.clone()))
                .await?
                .into_inner()
        } else {
            adapter
                .get_taddress_transactions(Request::new(request.clone()))
                .await?
                .into_inner()
        };
        let first = stream
            .next()
            .await
            .transpose()?
            .ok_or_else(|| eyre::eyre!("the history must not be empty"))?;
        assert_eq!(first.data, history_transaction_bytes(0));
        assert!(
            blob_reads.load(Ordering::SeqCst) < u32_to_usize(HISTORY),
            "the first transaction must arrive before the whole history is read"
        );

        let rest = collect_stream(stream).await?;
        assert_eq!(rest.len() + 1, u32_to_usize(HISTORY));
        assert_eq!(
            rest.last().map(|transaction| transaction.data.clone()),
            Some(history_transaction_bytes(HISTORY - 1))
        );
        assert_eq!(
            blob_reads.load(Ordering::SeqCst),
            u32_to_usize(HISTORY),
            "each transaction's bytes must be read once"
        );
    }
    Ok(())
}

/// The transparent history calls send the first transaction before they
/// read the next page of the address's history.
///
/// The second history page stays blocked until the first transaction
/// arrives, so a response that read the whole history index first would
/// never send it.
#[tokio::test]
async fn taddress_history_sends_a_transaction_before_reading_the_next_history_page()
-> eyre::Result<()> {
    const HISTORY: u32 = 1_001;
    let (mut store_fixture, address_text) = many_utxo_fixture(0x38, HISTORY)?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let gate = HistoryPageGate::new();
    let adapter = build_wallet_serving_adapter_from_readers(
        Arc::new(RocksDbCanonicalReader::new(canonical_reader)?),
        Arc::new(CountingWalletReader {
            wallet_reader: RocksDbWalletReader::new(wallet_reader),
            unspent_output_reads: UnspentOutputReads::default(),
            history_page_gate: Some(Arc::clone(&gate)),
        }),
    )?;
    let request = transparent_history_filter(address_text, 2, 2);

    for deprecated in [true, false] {
        gate.pages_requested.store(0, Ordering::SeqCst);
        let first_transaction = async {
            let mut stream = if deprecated {
                adapter
                    .get_taddress_txids(Request::new(request.clone()))
                    .await?
                    .into_inner()
            } else {
                adapter
                    .get_taddress_transactions(Request::new(request.clone()))
                    .await?
                    .into_inner()
            };
            let first = stream.next().await.transpose()?;
            Ok::<_, tonic::Status>((stream, first))
        };
        let (stream, first) =
            tokio::time::timeout(std::time::Duration::from_secs(5), first_transaction)
                .await
                .map_err(|_| {
                    eyre::eyre!("the first transaction must arrive before the second history page")
                })??;
        assert_eq!(
            first.map(|transaction| transaction.data),
            Some(history_transaction_bytes(0))
        );

        gate.release();
        let rest = collect_stream(stream).await?;
        assert_eq!(rest.len() + 1, u32_to_usize(HISTORY));
        assert_eq!(
            rest.last().map(|transaction| transaction.data.clone()),
            Some(history_transaction_bytes(HISTORY - 1))
        );
        assert_eq!(gate.pages_requested.load(Ordering::SeqCst), 2);
    }
    Ok(())
}

/// Fewest outputs the first page of one address's UTXO stream reads.
const MIN_FIRST_PAGE_OUTPUTS: usize = 16;
const OUTPUTS_PER_ADDRESS_AT_FIRST_HEIGHT: u32 = 3;

/// Outputs the first pages of one UTXO request's address streams share.
const FIRST_PAGE_BUDGET: usize = 8 * 1_024;

/// A request that lists one busy address many times reads that address once,
/// in one first page, and returns each of its outputs once, as lightwalletd
/// does, whether or not `maxEntries` is set.
#[tokio::test]
async fn address_utxos_read_a_busy_address_listed_many_times_once() -> eyre::Result<()> {
    const LISTINGS: usize = 1_000;
    const OUTPUTS: u32 = 100;
    // The response limit admits each output once, so a copy for any repeated
    // listing would fail the response with `RESOURCE_EXHAUSTED`.
    let response_limit = NonZeroU32::new(OUTPUTS).ok_or_else(|| eyre::eyre!("OUTPUTS is 0"))?;
    let (mut store_fixture, address_text) = many_utxo_fixture(0x51, OUTPUTS)?;
    let (adapter, reads) = counting_utxo_adapter(&mut store_fixture, Some(response_limit))?;
    let request = |max_entries| lightwalletd::GetAddressUtxosArg {
        addresses: vec![address_text.clone(); LISTINGS],
        start_height: 0,
        max_entries,
    };

    // A `maxEntries` above the address's outputs, or none, returns every
    // output once, so the repeated listings do not spend it on copies.
    for (max_entries, streamed) in [(500, false), (500, true), (0, false), (0, true)] {
        reads.take_outputs();
        let rows = if streamed {
            collect_stream(
                adapter
                    .get_address_utxos_stream(Request::new(request(max_entries)))
                    .await?
                    .into_inner(),
            )
            .await?
        } else {
            adapter
                .get_address_utxos(Request::new(request(max_entries)))
                .await?
                .into_inner()
                .address_utxos
        };
        let distinct_outputs = rows
            .iter()
            .map(|row| (row.txid.as_slice(), row.index))
            .collect::<HashSet<_>>();
        assert_eq!(
            (rows.len(), distinct_outputs.len()),
            (u32_to_usize(OUTPUTS), u32_to_usize(OUTPUTS)),
            "each output must be returned once, maxEntries {max_entries}"
        );
        assert_eq!(
            (reads.pages(), reads.outputs()),
            (1, u32_to_usize(OUTPUTS)),
            "a repeated address must be read once, in its first page"
        );
    }
    Ok(())
}

/// Checks the error detail clients use to retry with a larger limit.
pub(super) fn assert_address_utxo_height_refused(
    status: &tonic::Status,
    height: u64,
    count: usize,
) -> eyre::Result<()> {
    assert_eq!(status.code(), Code::FailedPrecondition);
    let details = status.get_error_details();
    let detail = details
        .error_info()
        .ok_or_else(|| eyre::eyre!("missing UTXO pagination error detail"))?;
    assert_eq!(detail.reason, "MAX_ENTRIES_EXCEEDED_AT_HEIGHT");
    assert_eq!(detail.domain, "cash.z.wallet.sdk.rpc");
    assert_eq!(detail.metadata.get("height"), Some(&height.to_string()));
    assert_eq!(detail.metadata.get("count"), Some(&count.to_string()));
    assert!(status.message().contains(&format!("height {height}")));
    assert!(
        status
            .message()
            .contains(&format!("{count} unspent outputs"))
    );
    Ok(())
}

#[tokio::test]
async fn address_utxos_refuse_first_height_above_max_entries_and_validate_addresses_first()
-> eyre::Result<()> {
    let (mut store_fixture, address_text) = many_utxo_fixture(0x52, 100)?;
    let (adapter, reads) = counting_utxo_adapter(&mut store_fixture, None)?;
    let request = |max_entries| lightwalletd::GetAddressUtxosArg {
        addresses: vec![address_text.clone()],
        start_height: 0,
        max_entries,
    };

    let limit = u32::try_from(MIN_FIRST_PAGE_OUTPUTS)?;
    let status = adapter
        .get_address_utxos(Request::new(request(limit)))
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("admitted an oversized first height"))?;
    assert_address_utxo_height_refused(&status, 2, 100)?;
    assert_eq!(reads.take_outputs(), 100);
    let stream = adapter
        .get_address_utxos_stream(Request::new(request(limit)))
        .await?
        .into_inner();
    let status = first_stream_error(stream).await?;
    assert_address_utxo_height_refused(&status, 2, 100)?;
    assert_eq!(reads.take_outputs(), 100);

    let rows = adapter
        .get_address_utxos(Request::new(request(100)))
        .await?
        .into_inner()
        .address_utxos;
    let streamed = collect_stream(
        adapter
            .get_address_utxos_stream(Request::new(request(100)))
            .await?
            .into_inner(),
    )
    .await?;
    assert_eq!(rows.len(), 100);
    assert_eq!(rows, streamed);
    reads.take_outputs();

    let status = adapter
        .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg {
            addresses: vec![address_text, "not-an-address".to_owned()],
            start_height: 0,
            max_entries: 0,
        }))
        .await
        .err()
        .map(|status| status.code());
    assert_eq!(status, Some(Code::InvalidArgument));
    assert_eq!(
        reads.outputs(),
        0,
        "an invalid address must be refused before any address is read"
    );
    Ok(())
}

/// A request of a few addresses with a few hundred outputs each reads each
/// address in one storage page, as an eager 1,024-output read does.
#[tokio::test]
async fn address_utxos_read_each_address_of_a_small_request_in_one_page() -> eyre::Result<()> {
    const ADDRESSES: u32 = 4;
    const OUTPUTS_PER_ADDRESS: u32 = 260;
    let (mut store_fixture, addresses) =
        busy_addresses_fixture(0x70, ADDRESSES, OUTPUTS_PER_ADDRESS)?;
    let (adapter, reads) = counting_utxo_adapter(&mut store_fixture, None)?;
    let request = lightwalletd::GetAddressUtxosArg {
        addresses,
        start_height: 0,
        max_entries: 0,
    };

    let list = adapter
        .get_address_utxos(Request::new(request.clone()))
        .await?
        .into_inner()
        .address_utxos;
    let list_pages = reads.pages();
    reads.take_outputs();
    let streamed = collect_stream(
        adapter
            .get_address_utxos_stream(Request::new(request))
            .await?
            .into_inner(),
    )
    .await?;

    assert_eq!(list.len(), u32_to_usize(ADDRESSES * OUTPUTS_PER_ADDRESS));
    assert_eq!(list, streamed);
    assert_eq!(list_pages, u32_to_usize(ADDRESSES));
    assert_eq!(reads.pages(), u32_to_usize(ADDRESSES));
    Ok(())
}

/// A request that lists many addresses holds a bounded number of outputs.
///
/// Before any row is sent it holds the next output of each address, read in
/// a first page that is an even share of the request's budget, capped at
/// `maxEntries`, rather than every address's outputs up to `maxEntries`.
#[tokio::test]
async fn address_utxos_bound_the_outputs_held_across_many_addresses() -> eyre::Result<()> {
    // `maxEntries` above each address's share of the first-page budget, so
    // the first pages are budget-sized, and high enough that reading every
    // address up to `maxEntries` exceeds the bound.
    const ADDRESSES: u32 = 60;
    const OUTPUTS_PER_ADDRESS: u32 = 150;
    const MAX_ENTRIES: u32 = ADDRESSES * OUTPUTS_PER_ADDRESS_AT_FIRST_HEIGHT;
    let (mut store_fixture, addresses) =
        busy_addresses_fixture(0x60, ADDRESSES, OUTPUTS_PER_ADDRESS)?;
    let (adapter, reads) = counting_utxo_adapter(&mut store_fixture, None)?;
    let request = lightwalletd::GetAddressUtxosArg {
        addresses,
        start_height: 0,
        max_entries: MAX_ENTRIES,
    };

    let list = adapter
        .get_address_utxos(Request::new(request.clone()))
        .await?
        .into_inner()
        .address_utxos;
    let list_outputs_read = reads.take_outputs();
    let streamed = collect_stream(
        adapter
            .get_address_utxos_stream(Request::new(request))
            .await?
            .into_inner(),
    )
    .await?;
    let stream_outputs_read = reads.outputs();

    let expected: Vec<_> = (0..ADDRESSES)
        .flat_map(|address_index| {
            (0..OUTPUTS_PER_ADDRESS_AT_FIRST_HEIGHT).map(move |output| {
                history_transaction_id(address_index * OUTPUTS_PER_ADDRESS + output)
                    .as_bytes()
                    .to_vec()
            })
        })
        .collect();
    assert_eq!(
        list.iter().map(|row| row.txid.clone()).collect::<Vec<_>>(),
        expected
    );
    assert_eq!(list, streamed);
    // Each address reads one first page, its share of the budget capped at
    // `maxEntries`, and the address that supplies the rows reads later pages
    // that hold at most what it sent plus one first page. So the request
    // reads at most twice the rows sent plus 2 outputs and one first page per
    // address, and the first pages together stay within the larger of the
    // budget and 16 outputs per address.
    let first_page = (FIRST_PAGE_BUDGET / u32_to_usize(ADDRESSES))
        .min(u32_to_usize(MAX_ENTRIES))
        .max(MIN_FIRST_PAGE_OUTPUTS);
    assert!(
        u32_to_usize(ADDRESSES) * first_page
            <= FIRST_PAGE_BUDGET.max(u32_to_usize(ADDRESSES) * MIN_FIRST_PAGE_OUTPUTS)
    );
    let bound = 2 * expected.len() + u32_to_usize(ADDRESSES) * (first_page + 2);
    assert!(
        first_page < u32_to_usize(MAX_ENTRIES),
        "the first pages must be the budget share, not `maxEntries`"
    );
    assert!(
        bound < u32_to_usize(ADDRESSES * OUTPUTS_PER_ADDRESS.min(MAX_ENTRIES)),
        "the bound of {bound} outputs must be below what reading each address up to \
         `maxEntries` would read"
    );
    for outputs in [list_outputs_read, stream_outputs_read] {
        assert!(
            outputs <= bound,
            "read {outputs} outputs, more than the bound of {bound}"
        );
    }
    Ok(())
}

/// A history stream that fails after it has sent transactions ends with the
/// failure's status, after the transactions it already sent.
#[tokio::test]
async fn taddress_history_ends_with_the_status_of_a_failure_after_partial_delivery()
-> eyre::Result<()> {
    let (mut store_fixture, address_text) = many_utxo_fixture(0x37, 3)?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let canonical_reader = InstrumentedCanonicalReader {
        missing_transaction_id: Some(history_transaction_id(1)),
        ..InstrumentedCanonicalReader::new(canonical_reader)?
    };
    let adapter = build_wallet_serving_adapter_from_readers(
        Arc::new(canonical_reader),
        Arc::new(RocksDbWalletReader::new(wallet_reader)),
    )?;

    let mut stream = adapter
        .get_taddress_transactions(Request::new(transparent_history_filter(address_text, 2, 2)))
        .await?
        .into_inner();
    let first = stream
        .next()
        .await
        .transpose()?
        .ok_or_else(|| eyre::eyre!("the first transaction must be sent"))?;
    assert_eq!(first.data, history_transaction_bytes(0));
    assert_eq!(
        first_stream_error(&mut stream).await?.code(),
        Code::NotFound
    );
    assert!(
        stream.next().await.is_none(),
        "the stream must end after the failure"
    );
    Ok(())
}

/// Several addresses merge into height-complete pages in transaction and output order.
///
/// The addresses receive in interleaved blocks and one transaction pays
/// both. A repeated address is served once, as lightwalletd drops a repeated
/// address, so its rows do not count twice against `maxEntries`.
#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "The interleaved fixture and the expected merged order describe one ordering contract."
)]
async fn address_utxos_merge_several_addresses_in_chain_order() -> eyre::Result<()> {
    let first = transparent_address(0x41);
    let second = transparent_address(0x42);
    let script = |address: &ZebraTransparentAddress| address.script().as_raw_bytes().to_vec();
    let mut chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(3);
    // (height, transaction, position in its block)
    let transactions = [(1, 3, 0), (1, 1, 1), (2, 2, 0), (3, 5, 0), (3, 4, 1)];
    // (transaction, output index, receiving address)
    let outputs = [
        (3, 0, &second),
        (1, 0, &first),
        (2, 1, &first),
        (2, 0, &second),
        (4, 0, &first),
        (5, 0, &second),
    ];
    for (height, transaction, position) in transactions {
        let block = chain
            .block_at(BlockHeight::new(height))
            .ok_or_else(|| eyre::eyre!("merge fixture must include block {height}"))?
            .clone();
        let transaction_id = history_transaction_id(transaction);
        chain = chain.with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            transaction_id,
            block.height,
            block.hash,
            position,
            history_transaction_bytes(transaction),
        ));
        for (_, output_index, address) in outputs
            .iter()
            .filter(|(paying, _, _)| *paying == transaction)
        {
            chain = chain.with_address_output_index(TransparentUnspentOutput::new(
                TransparentAddressScriptHash::of_script_pub_key(&script(address)),
                script(address),
                TransparentOutPoint::new(transaction_id, *output_index),
                1,
                block.height,
                block.hash,
            ));
        }
    }
    let mut store_fixture =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let adapter = build_wallet_serving_adapter(&mut store_fixture)?;
    let row = |height: u64, transaction: u32, index: i32, address: &ZebraTransparentAddress| {
        (
            height,
            history_transaction_id(transaction).as_bytes().to_vec(),
            index,
            address.to_string(),
        )
    };
    let expected = [
        row(1, 3, 0, &second),
        row(1, 1, 0, &first),
        row(2, 2, 0, &second),
        row(2, 2, 1, &first),
        row(3, 5, 0, &second),
        row(3, 4, 0, &first),
    ];
    let observed = |replies: &[lightwalletd::GetAddressUtxosReply]| {
        replies
            .iter()
            .map(|reply| {
                (
                    reply.height,
                    reply.txid.clone(),
                    reply.index,
                    reply.address.clone(),
                )
            })
            .collect::<Vec<_>>()
    };

    for (max_entries, page_sizes) in [(0, &[6, 0][..]), (2, &[2, 2, 2, 0]), (3, &[2, 2, 2, 0])] {
        let mut start_height = 0;
        let mut walked = Vec::new();
        for &page_size in page_sizes {
            let request = lightwalletd::GetAddressUtxosArg {
                addresses: vec![second.to_string(), first.to_string(), second.to_string()],
                start_height,
                max_entries,
            };
            let list = adapter
                .get_address_utxos(Request::new(request.clone()))
                .await?
                .into_inner()
                .address_utxos;
            let streamed = collect_stream(
                adapter
                    .get_address_utxos_stream(Request::new(request))
                    .await?
                    .into_inner(),
            )
            .await?;
            assert_eq!(list, streamed);
            assert_eq!(list.len(), page_size);
            if let Some(last) = list.last() {
                start_height = last.height + 1;
            }
            walked.extend(observed(&list));
        }
        assert_eq!(walked, expected, "maxEntries {max_entries}");
    }
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
        .with_raw_blob_retention(RawBlobRetention::All)
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
        Arc::new(RocksDbCanonicalReader::new(canonical_reader)?),
        Arc::new(RocksDbWalletReader::new(wallet_reader)),
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
        .with_raw_blob_retention(RawBlobRetention::All)
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
    let canonical_reader: Arc<dyn CanonicalReader> = Arc::new(InstrumentedCanonicalReader {
        missing_transaction_id: Some(transaction_id),
        ..InstrumentedCanonicalReader::new(canonical_reader)?
    });
    let wallet_reader: Arc<dyn WalletProjectionReader> =
        Arc::new(RocksDbWalletReader::new(wallet_reader));
    let adapter = build_wallet_serving_adapter_from_readers(canonical_reader, wallet_reader)?;
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

/// Out-of-history block ranges keep the lightwalletd status contract.
///
/// A release wallet-serving pair retains compact blocks from height 1 through
/// its visible tip. Ranges outside that window must keep the lightwalletd
/// status contract: `OUT_OF_RANGE` above the tip and `NOT_FOUND` for an
/// unretained start, never a retryable `UNAVAILABLE` storage outage.
#[tokio::test]
async fn production_pair_get_block_range_outside_retained_history_keeps_lightwalletd_codes()
-> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(3);
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
    let adapter = build_wallet_serving_adapter(&mut store_fixture)?;

    let cases = [
        ("start above visible tip", 4_u64, 5_u64, Code::OutOfRange),
        ("start at unretained height 0", 0, 1, Code::NotFound),
        ("only unretained height 0", 0, 0, Code::NotFound),
    ];
    let mut mismatches = Vec::new();
    for (label, start_height, end_height, expected_code) in cases {
        let observed = first_block_range_status(&adapter, start_height, end_height).await?;
        if observed.code() != expected_code {
            mismatches.push(format!(
                "{label} ({start_height}..={end_height}): expected {expected_code:?}, observed {:?} \"{}\"",
                observed.code(),
                observed.message(),
            ));
        }
    }

    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(eyre::eyre!(mismatches.join("\n")))
    }
}

async fn first_block_range_status(
    adapter: &WalletServingAdapter,
    start_height: u64,
    end_height: u64,
) -> eyre::Result<tonic::Status> {
    let request = lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: start_height,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: end_height,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    };
    match adapter.get_block_range(Request::new(request)).await {
        Err(status) => Ok(status),
        Ok(response) => first_stream_error(response.into_inner()).await,
    }
}

pub(super) fn build_wallet_serving_adapter(
    store_fixture: &mut WalletServingStoreFixture,
) -> eyre::Result<WalletServingAdapter> {
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    build_wallet_serving_adapter_from_readers(
        Arc::new(RocksDbCanonicalReader::new(canonical_reader)?),
        Arc::new(RocksDbWalletReader::new(wallet_reader)),
    )
}

fn build_wallet_serving_adapter_from_readers(
    canonical_reader: Arc<dyn CanonicalReader>,
    wallet_reader: Arc<dyn WalletProjectionReader>,
) -> eyre::Result<WalletServingAdapter> {
    let serving_pair = Arc::new(WalletServingReadPair::new(canonical_reader, wallet_reader)?);
    let serving_pair_slot = WalletServingPairSlot::new(serving_pair);
    let query = WalletServingQuery::from_serving_pair_slot(serving_pair_slot);
    LightwalletdGrpcAdapter::from_admitted_compatibility_query(query).map_err(Into::into)
}

/// The regtest table a pre-NU7 node advertises, with NU6.3 moved to height 3
/// so the fixture chain reaches it and no upgrade remains pending.
fn pre_nu7_activations() -> eyre::Result<NetworkUpgradeActivations> {
    let activations = sample_regtest_upgrade_activations()
        .activations()
        .iter()
        .filter(|activation| activation.name != "NU7")
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

/// Builds a compatibility adapter over `store_fixture` whose wallet reader
/// counts the unspent outputs and pages it reads, with `response_limit` as
/// its UTXO response bound when one is given.
fn counting_utxo_adapter(
    store_fixture: &mut WalletServingStoreFixture,
    response_limit: Option<NonZeroU32>,
) -> eyre::Result<(WalletServingAdapter, UnspentOutputReads)> {
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let outputs_read = UnspentOutputReads::default();
    let wallet_reader: Arc<dyn WalletProjectionReader> = Arc::new(CountingWalletReader {
        wallet_reader: RocksDbWalletReader::new(wallet_reader),
        unspent_output_reads: outputs_read.clone(),
        history_page_gate: None,
    });
    let adapter = match response_limit {
        None => build_wallet_serving_adapter_from_readers(
            Arc::new(RocksDbCanonicalReader::new(canonical_reader)?),
            wallet_reader,
        )?,
        Some(max_address_utxos) => {
            let serving_pair = Arc::new(WalletServingReadPair::new(
                Arc::new(RocksDbCanonicalReader::new(canonical_reader)?),
                wallet_reader,
            )?);
            LightwalletdGrpcAdapter::with_response_limits(
                WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(
                    serving_pair,
                )),
                LightwalletdResponseLimits {
                    max_address_utxos,
                    ..LightwalletdResponseLimits::default()
                },
            )
        }
    };
    Ok((adapter, outputs_read))
}

/// Builds a store where `address_count` addresses each hold `utxo_count` UTXOs.
///
/// Each address receives its first three outputs at height 1 and the rest at
/// height 2, one per transaction.
fn busy_addresses_fixture(
    first_address_byte: u8,
    address_count: u32,
    utxo_count: u32,
) -> eyre::Result<(WalletServingStoreFixture, Vec<String>)> {
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(2);
    let mut chain = chain;
    let mut addresses = Vec::new();
    let first_height_outputs = utxo_count.min(OUTPUTS_PER_ADDRESS_AT_FIRST_HEIGHT);
    for address_offset in 0..address_count {
        let address =
            transparent_address(first_address_byte.wrapping_add(u8::try_from(address_offset)?));
        let script = address.script().as_raw_bytes().to_vec();
        let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
        for output in 0..utxo_count {
            let (height, tx_index) = if output < first_height_outputs {
                (1, address_offset * first_height_outputs + output)
            } else {
                (
                    2,
                    address_offset * (utxo_count - first_height_outputs)
                        + (output - first_height_outputs),
                )
            };
            let block = chain
                .block_at(BlockHeight::new(height))
                .ok_or_else(|| eyre::eyre!("UTXO fixture must include block {height}"))?
                .clone();
            let index = address_offset * utxo_count + output;
            let transaction_id = history_transaction_id(index);
            chain = chain
                .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
                    transaction_id,
                    block.height,
                    block.hash,
                    tx_index,
                    history_transaction_bytes(index),
                ))
                .with_address_output_index(TransparentUnspentOutput::new(
                    address_script_hash,
                    script.clone(),
                    TransparentOutPoint::new(transaction_id, 0),
                    1,
                    block.height,
                    block.hash,
                ));
        }
        addresses.push(address.to_string());
    }
    let store_fixture =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    Ok((store_fixture, addresses))
}

/// Builds a wallet-serving store whose address holds `utxo_count` UTXOs at
/// height 2, one per transaction, in transaction-index order.
pub(super) fn many_utxo_fixture(
    address_byte: u8,
    utxo_count: u32,
) -> eyre::Result<(WalletServingStoreFixture, String)> {
    let address = transparent_address(address_byte);
    let script = address.script().as_raw_bytes().to_vec();
    let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(2);
    let block_two = chain
        .block_at(BlockHeight::new(2))
        .ok_or_else(|| eyre::eyre!("UTXO fixture must include block 2"))?
        .clone();
    let mut chain = chain;
    for index in 0..utxo_count {
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
    let store_fixture =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    Ok((store_fixture, address.to_string()))
}

fn u32_to_usize(count: u32) -> usize {
    usize::try_from(count).unwrap_or(usize::MAX)
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

fn history_filter_with_end(
    address: String,
    start_height: u64,
    end_height: Option<u64>,
) -> lightwalletd::TransparentAddressBlockFilter {
    lightwalletd::TransparentAddressBlockFilter {
        address,
        range: Some(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: start_height,
                hash: Vec::new(),
            }),
            end: end_height.map(|height| lightwalletd::BlockId {
                height,
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

/// Drains `stream` and returns the items it sent and the status it ended
/// with, if it failed.
async fn drain_until_error<T, Stream>(mut stream: Stream) -> (Vec<T>, Option<tonic::Status>)
where
    Stream: tonic::codegen::tokio_stream::Stream<Item = Result<T, tonic::Status>> + Unpin,
{
    let mut items = Vec::new();
    while let Some(received) = stream.next().await {
        match received {
            Ok(message) => items.push(message),
            Err(status) => return (items, Some(status)),
        }
    }
    (items, None)
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

/// A wallet reader that counts the unspent outputs and pages it reads and
/// can hold back a history page.
struct CountingWalletReader {
    wallet_reader: RocksDbWalletReader,
    unspent_output_reads: UnspentOutputReads,
    history_page_gate: Option<Arc<HistoryPageGate>>,
}

/// The unspent outputs, and the storage pages holding them, that a
/// [`CountingWalletReader`] has read.
#[derive(Clone, Default)]
struct UnspentOutputReads {
    outputs: Arc<AtomicUsize>,
    pages: Arc<AtomicUsize>,
}

impl UnspentOutputReads {
    fn record(&self, page: &zinder_wallet_projection::WalletAddressUnspentOutputsPage) {
        self.outputs.fetch_add(page.outputs.len(), Ordering::SeqCst);
        self.pages.fetch_add(1, Ordering::SeqCst);
    }

    fn outputs(&self) -> usize {
        self.outputs.load(Ordering::SeqCst)
    }

    fn pages(&self) -> usize {
        self.pages.load(Ordering::SeqCst)
    }

    /// Returns the outputs read so far and starts both counts again.
    fn take_outputs(&self) -> usize {
        self.pages.store(0, Ordering::SeqCst);
        self.outputs.swap(0, Ordering::SeqCst)
    }
}

/// Holds the second history page a request reads until the test releases it.
struct HistoryPageGate {
    pages_requested: AtomicUsize,
    released: tokio::sync::Semaphore,
}

impl HistoryPageGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            pages_requested: AtomicUsize::new(0),
            released: tokio::sync::Semaphore::new(0),
        })
    }

    /// Waits on the second page request since the last reset until the test
    /// releases it.
    async fn before_page(&self) {
        if self.pages_requested.fetch_add(1, Ordering::SeqCst) == 1
            && let Ok(release) = self.released.acquire().await
        {
            release.forget();
        }
    }

    /// Lets one held page request continue.
    fn release(&self) {
        self.released.add_permits(1);
    }
}

#[async_trait::async_trait]
impl WalletProjectionReader for CountingWalletReader {
    fn network(&self) -> Network {
        self.wallet_reader.network()
    }

    fn ready_evidence(&self) -> &zinder_wallet_projection::WalletProjectionReadyEvidence {
        self.wallet_reader.ready_evidence()
    }

    async fn address_unspent_outputs_page_from_height(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
        after: Option<zinder_wallet_projection::WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<zinder_wallet_projection::WalletAddressUnspentOutputsPage, StorageReadError> {
        let page = self
            .wallet_reader
            .address_unspent_outputs_page_from_height(
                address_script_hash,
                start_height,
                after,
                page_size,
            )
            .await?;
        self.unspent_output_reads.record(&page);
        Ok(page)
    }

    async fn address_transaction_history_range_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        height_range: BlockHeightRange,
        after: Option<zinder_wallet_projection::WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<zinder_wallet_projection::WalletAddressTransactionHistoryPage, StorageReadError>
    {
        if let Some(gate) = &self.history_page_gate {
            gate.before_page().await;
        }
        self.wallet_reader
            .address_transaction_history_range_page(
                address_script_hash,
                height_range,
                after,
                page_size,
            )
            .await
    }

    async fn total_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
    ) -> Result<u64, StorageReadError> {
        self.wallet_reader.total_address_balance(addresses).await
    }

    async fn outpoint_spend_statuses(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        end: SpendStatusReadEnd,
    ) -> Result<Vec<TransparentOutpointSpendStatus>, StorageReadError> {
        self.wallet_reader
            .outpoint_spend_statuses(outpoints, end)
            .await
    }
}

/// A canonical reader that counts mined-transaction reads, each of which
/// reads the transaction's bytes, and can hide one transaction's bytes.
struct InstrumentedCanonicalReader {
    canonical_reader: RocksDbCanonicalReader,
    missing_transaction_id: Option<TransactionId>,
    transaction_blob_reads: Arc<AtomicUsize>,
}

impl InstrumentedCanonicalReader {
    fn new(canonical_reader: zinder_store::RocksDbCanonicalSecondary) -> eyre::Result<Self> {
        Ok(Self {
            canonical_reader: RocksDbCanonicalReader::new(canonical_reader)?,
            missing_transaction_id: None,
            transaction_blob_reads: Arc::new(AtomicUsize::new(0)),
        })
    }
}

#[async_trait::async_trait]
impl CanonicalReader for InstrumentedCanonicalReader {
    fn network(&self) -> Network {
        self.canonical_reader.network()
    }

    fn network_upgrade_schedule(&self) -> Option<&zinder_store::CanonicalActivationSchedule> {
        self.canonical_reader.network_upgrade_schedule()
    }

    fn raw_blob_retention(&self) -> RawBlobRetention {
        self.canonical_reader.raw_blob_retention()
    }

    fn event_fence(&self) -> CanonicalEventFence {
        self.canonical_reader.event_fence()
    }

    fn chain_epoch(&self) -> zinder_core::ChainEpoch {
        self.canonical_reader.chain_epoch()
    }

    fn history_bounds(&self) -> zinder_core::CanonicalHistoryBounds {
        self.canonical_reader.history_bounds()
    }

    fn history_predecessor_digest(&self) -> Option<[u8; 32]> {
        self.canonical_reader.history_predecessor_digest()
    }

    async fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError> {
        self.canonical_reader.block_header_at(height).await
    }

    async fn block_hash_lookup(
        &self,
        block_hash: zinder_core::BlockHash,
    ) -> Result<zinder_store::BlockHashLookup, StorageReadError> {
        self.canonical_reader.block_hash_lookup(block_hash).await
    }

    async fn block_header_by_hash(
        &self,
        block_hash: zinder_core::BlockHash,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError> {
        self.canonical_reader.block_header_by_hash(block_hash).await
    }

    async fn compact_block_by_hash(
        &self,
        block_hash: zinder_core::BlockHash,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError> {
        self.canonical_reader
            .compact_block_by_hash(block_hash)
            .await
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError> {
        self.canonical_reader.compact_block_at(height).await
    }

    async fn compact_blocks_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError> {
        self.canonical_reader.compact_blocks_in_range(range).await
    }

    async fn compact_blocks_in_range_up_to(
        &self,
        range: BlockHeightRange,
        order: zinder_store::HeightOrder,
        max_bytes: usize,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError> {
        self.canonical_reader
            .compact_blocks_in_range_up_to(range, order, max_bytes)
            .await
    }

    async fn block_blob_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockBlobArtifact>, StorageReadError> {
        self.canonical_reader.block_blob_at(height).await
    }

    async fn block_blobs_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockBlobArtifact>>, StorageReadError> {
        self.canonical_reader.block_blobs_in_range(range).await
    }

    async fn mined_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<IndexedTransaction>, StorageReadError> {
        let transaction = self
            .canonical_reader
            .mined_transaction(transaction_id)
            .await?;
        if transaction.is_some() {
            self.transaction_blob_reads.fetch_add(1, Ordering::SeqCst);
        }
        if Some(transaction_id) == self.missing_transaction_id {
            return Ok(transaction.map(|transaction| IndexedTransaction {
                transaction_blob: None,
                ..transaction
            }));
        }
        Ok(transaction)
    }

    async fn transaction_blob_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<TransactionBlobArtifact, StorageReadError> {
        self.canonical_reader
            .transaction_blob_by_id(transaction_id)
            .await
    }

    async fn tree_state_checkpoint_at_or_before(
        &self,
        height: BlockHeight,
    ) -> Result<Option<ServedTreeState>, StorageReadError> {
        self.canonical_reader
            .tree_state_checkpoint_at_or_before(height)
            .await
    }

    async fn tree_state_lookup(
        &self,
        height: BlockHeight,
    ) -> Result<Option<zinder_store::TreeStateLookup>, StorageReadError> {
        self.canonical_reader.tree_state_lookup(height).await
    }

    async fn replay_tree_state(
        &self,
        replay: zinder_store::TreeStateReplay,
    ) -> Result<Option<zinder_store::TreeStateReplayOutcome>, StorageReadError> {
        self.canonical_reader.replay_tree_state(replay).await
    }

    async fn subtree_roots(
        &self,
        range: SubtreeRootRange,
    ) -> Result<Vec<SubtreeRootArtifact>, StorageReadError> {
        self.canonical_reader.subtree_roots(range).await
    }

    async fn wallet_chain_event_history(
        &self,
        from_cursor: Option<zinder_store::StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
        max_events: NonZeroU32,
    ) -> Result<Vec<ChainEventEnvelope>, StorageReadError> {
        self.canonical_reader
            .wallet_chain_event_history(from_cursor, family, max_events)
            .await
    }

    async fn resolve_wallet_chain_event_stream_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StorageReadError> {
        self.canonical_reader
            .resolve_wallet_chain_event_stream_start(start, requested_family)
            .await
    }
}
