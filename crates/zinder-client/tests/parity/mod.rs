//! Consumer-shaped contract tests for the public client surface.
//!
//! Each per-consumer module asserts the typed shape that consumer's contract
//! depends on. Parity here means "Zinder serves the consumer-expected shape",
//! not byte-equivalence with every implementation detail of another indexer.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use zebra_chain::{
    parameters::NetworkKind as ZebraNetworkKind, transparent::Address as ZebraTransparentAddress,
};
use zinder_client::{
    BlockHeight, Network, RemoteChainIndex, RemoteOpenOptions, TransactionId,
    TransparentAddressScriptHash, TransparentOutPoint, TransparentUnspentOutput,
};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_proto::compat::lightwalletd;
use zinder_query::{
    AdmittedIngestControl, CanonicalReader, RocksDbCanonicalReader, RocksDbWalletReader,
    WalletEndpointMetadata, WalletProjectionReader, WalletQueryGrpcAdapter, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_store::RawBlobRetention;
use zinder_testkit::{
    ChainFixture, FixtureTransactionRows, IngestControlFixture, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

const PARITY_TREE_STATE_PAYLOAD: &[u8] =
    br#"{"hash":"010101","height":1,"time":1296694002,"sapling":{"commitments":{"finalState":"000000"}},"orchard":{"commitments":{"finalState":"111111"}}}"#;

type TransparentAddressAdapter = LightwalletdGrpcAdapter<WalletServingQuery<()>>;

struct TransparentAddressServingFixture {
    store_fixture: WalletServingStoreFixture,
    address: String,
    block_height: BlockHeight,
    transaction_id: TransactionId,
    script_pub_key: Vec<u8>,
    value_zat: i64,
    raw_transaction_bytes: Vec<u8>,
}

mod explorers;
mod lightwalletd_adapter_shapes;
mod lightwalletd_operators;
mod zallet;

fn parity_chain_fixture(block_count: u32) -> ChainFixture {
    ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(block_count)
        .with_tree_state_checkpoint_payload_at(
            BlockHeight::new(block_count),
            PARITY_TREE_STATE_PAYLOAD,
        )
}

fn build_transparent_address_serving_fixture() -> eyre::Result<TransparentAddressServingFixture> {
    let address = ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x31; 20]);
    let address_text = address.to_string();
    let script_pub_key = address.script().as_raw_bytes().to_vec();
    let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script_pub_key);
    let transaction_id = TransactionId::from_bytes([0x71; 32]);
    let raw_transaction_bytes = b"parity-transparent-transaction".to_vec();
    let value_zat = 42_000_i64;
    let base_fixture = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::All)
        .extend_blocks(1);
    let block = base_fixture
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre::eyre!("transparent parity fixture must contain block 1"))?;
    let block_height = block.height;
    let block_hash = block.hash;
    let chain_fixture = base_fixture
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            transaction_id,
            block_height,
            block_hash,
            0,
            raw_transaction_bytes.clone(),
        ))
        .with_address_output_index(TransparentUnspentOutput::new(
            address_script_hash,
            script_pub_key.clone(),
            TransparentOutPoint::new(transaction_id, 0),
            42_000,
            block_height,
            block_hash,
        ));
    let store_fixture = WalletServingStoreFixture::from_chain(
        &chain_fixture,
        &sample_regtest_upgrade_activations(),
    )?;

    Ok(TransparentAddressServingFixture {
        store_fixture,
        address: address_text,
        block_height,
        transaction_id,
        script_pub_key,
        value_zat,
        raw_transaction_bytes,
    })
}

fn build_transparent_address_adapter(
    fixture: &mut TransparentAddressServingFixture,
) -> eyre::Result<TransparentAddressAdapter> {
    let (canonical_reader, wallet_reader) = fixture.store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical_reader)?) as Arc<dyn CanonicalReader>,
        Arc::new(RocksDbWalletReader::new(wallet_reader)) as Arc<dyn WalletProjectionReader>,
    )?);
    let serving_pair_slot = WalletServingPairSlot::new(serving_pair);
    let query = WalletServingQuery::from_serving_pair_slot(serving_pair_slot);
    LightwalletdGrpcAdapter::from_admitted_compatibility_query(query).map_err(Into::into)
}

fn address_history_filter(address: String) -> lightwalletd::TransparentAddressBlockFilter {
    lightwalletd::TransparentAddressBlockFilter {
        address,
        range: Some(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }),
    }
}

async fn open_remote_chain_index(chain_fixture: &ChainFixture) -> eyre::Result<RemoteChainIndex> {
    let mut store_fixture = WalletServingStoreFixture::from_chain(
        chain_fixture,
        &sample_regtest_upgrade_activations(),
    )?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical_reader)?) as Arc<dyn CanonicalReader>,
        Arc::new(RocksDbWalletReader::new(wallet_reader)) as Arc<dyn WalletProjectionReader>,
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
        admitted_ingest_control,
    );
    let adapter = WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(async move {
        let _store_fixture = store_fixture;
        let _ingest_control_fixture = ingest_control_fixture;
        let _server_result = Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(incoming)
            .await;
    });

    Ok(RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint: format!("http://{address}"),
        network: Network::ZcashRegtest,
    })?)
}
