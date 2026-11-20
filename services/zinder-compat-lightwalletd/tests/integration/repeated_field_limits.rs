#![allow(
    missing_docs,
    reason = "Integration test names describe the pre-decode request bounds under test."
)]

use std::sync::Arc;

use eyre::{Result, eyre};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Status, transport::Server};
use zinder_compat_lightwalletd::{COMPACT_TX_STREAMER_FIELD_LIMITS, LightwalletdGrpcAdapter};
use zinder_core::Network;
use zinder_proto::{
    LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET,
    compat::lightwalletd::{self, compact_tx_streamer_client::CompactTxStreamerClient},
};
use zinder_query::{
    RocksDbCanonicalReader, RocksDbWalletReader, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_runtime::{MAX_DECODING_MESSAGE_BYTES, RepeatedElementEncoding};
use zinder_testkit::{
    ChainFixture, RepeatedRequestField, WalletServingStoreFixture, repeated_request_fields,
    sample_regtest_upgrade_activations,
};

const COMPACT_TX_STREAMER: &str = "cash.z.wallet.sdk.rpc.CompactTxStreamer";
const SAPLING: i32 = lightwalletd::PoolType::Sapling as i32;
const FIXTURE_TIP_HEIGHT: u64 = 3;

/// Empty `string` or `bytes` elements that fill one request frame: each takes
/// two bytes on the wire and at least 24 once decoded.
const EMPTY_ELEMENTS_PER_FRAME: usize = (MAX_DECODING_MESSAGE_BYTES - 64) / 2;
/// Packed one-byte enum elements that fill one request frame: each takes one
/// byte on the wire and four once decoded.
const PACKED_ENUMS_PER_FRAME: usize = MAX_DECODING_MESSAGE_BYTES - 64;

#[test]
fn field_limits_cover_every_repeated_request_field() -> Result<()> {
    let mut bounded: Vec<RepeatedRequestField> = COMPACT_TX_STREAMER_FIELD_LIMITS
        .iter()
        .flat_map(|method| {
            method.fields().iter().map(|field| RepeatedRequestField {
                method: method.method().to_owned(),
                field_name: field.field_name().to_owned(),
                field_path: field.field_path().to_vec(),
                varint_elements: field.encoding() == RepeatedElementEncoding::Varint,
            })
        })
        .collect();
    bounded.sort();

    assert_eq!(
        bounded,
        repeated_request_fields(LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET, COMPACT_TX_STREAMER)?,
    );
    Ok(())
}

#[tokio::test]
async fn a_balance_request_of_empty_addresses_is_refused_before_decoding() -> Result<()> {
    let mut client = spawn_compat_server().await?;

    let refusal = client
        .get_taddress_balance(lightwalletd::AddressList {
            addresses: vec![String::new(); EMPTY_ELEMENTS_PER_FRAME],
        })
        .await
        .err();

    assert_refused_before_decoding(refusal.as_ref(), "GetTaddressBalance", "addresses")
}

#[tokio::test]
async fn a_balance_request_just_over_the_handler_cap_reaches_the_handler() -> Result<()> {
    let mut client = spawn_compat_server().await?;
    let over_handler_cap =
        usize::try_from(zinder_query::MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES)?.saturating_add(1);

    let refusal = client
        .get_taddress_balance(lightwalletd::AddressList {
            addresses: vec![String::new(); over_handler_cap],
        })
        .await
        .err()
        .ok_or_else(|| eyre!("empty addresses must be refused"))?;

    assert_eq!(refusal.code(), Code::InvalidArgument);
    assert!(
        !refusal.message().contains("elements; at most"),
        "the handler, not the pre-decode bound, must refuse: {refusal:?}",
    );
    Ok(())
}

#[tokio::test]
async fn a_mempool_request_of_empty_txid_suffixes_is_refused_before_decoding() -> Result<()> {
    let mut client = spawn_compat_server().await?;

    let refusal = client
        .get_mempool_tx(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: vec![Vec::new(); EMPTY_ELEMENTS_PER_FRAME],
            pool_types: Vec::new(),
        })
        .await
        .err();

    assert_refused_before_decoding(refusal.as_ref(), "GetMempoolTx", "exclude_txid_suffixes")
}

#[tokio::test]
async fn mempool_suffix_counts_select_the_handler_or_predecode_refusal() -> Result<()> {
    let mut client = spawn_compat_server().await?;

    for count in [20_001u32, 40_000, 40_001] {
        let refusal = client
            .get_mempool_tx(lightwalletd::GetMempoolTxRequest {
                exclude_txid_suffixes: (0..count).map(|i| i.to_be_bytes().to_vec()).collect(),
                pool_types: Vec::new(),
            })
            .await
            .err()
            .ok_or_else(|| eyre!("expected a refusal for {count} suffixes"))?;

        assert_eq!(refusal.code(), Code::InvalidArgument);
        if count <= 40_000 {
            assert_eq!(
                refusal.message(),
                format!(
                    "exclude_txid_suffixes contains {count} entries; at most 20000 are allowed"
                ),
            );
        } else {
            assert_eq!(
                refusal.message(),
                "GetMempoolTx request field exclude_txid_suffixes carries 40001 elements; at most 40000 are accepted"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_block_range_of_packed_pool_types_is_refused_before_decoding() -> Result<()> {
    let mut client = spawn_compat_server().await?;
    let mut request = block_range(1, FIXTURE_TIP_HEIGHT);
    request.pool_types = vec![SAPLING; PACKED_ENUMS_PER_FRAME];

    let refusal = client.get_block_range(request).await.err();

    assert_refused_before_decoding(refusal.as_ref(), "GetBlockRange", "poolTypes")
}

#[tokio::test]
async fn an_address_filter_of_nested_pool_types_is_refused_before_decoding() -> Result<()> {
    let mut client = spawn_compat_server().await?;
    let mut range = block_range(1, FIXTURE_TIP_HEIGHT);
    range.pool_types = vec![SAPLING; PACKED_ENUMS_PER_FRAME];

    let refusal = client
        .get_taddress_txids(lightwalletd::TransparentAddressBlockFilter {
            address: String::new(),
            range: Some(range),
        })
        .await
        .err();

    assert_refused_before_decoding(refusal.as_ref(), "GetTaddressTxids", "range.poolTypes")
}

#[tokio::test]
async fn a_block_range_with_pool_types_still_streams_every_block() -> Result<()> {
    let mut client = spawn_compat_server().await?;
    let mut request = block_range(1, FIXTURE_TIP_HEIGHT);
    request.pool_types = vec![SAPLING, lightwalletd::PoolType::Orchard as i32];

    let mut blocks = client.get_block_range(request).await?.into_inner();
    let mut heights = Vec::new();
    while let Some(block) = blocks.message().await? {
        heights.push(block.height);
    }

    assert_eq!(heights, (1..=FIXTURE_TIP_HEIGHT).collect::<Vec<_>>());
    Ok(())
}

#[tokio::test]
#[allow(
    deprecated,
    reason = "The v0.6.2 wire contract includes GetBlockRangeNullifiers."
)]
async fn generated_client_distinguishes_unknown_pool_values_from_the_invalid_sentinel() -> Result<()>
{
    let mut client = spawn_compat_server().await?;
    let pool_types = vec![99, SAPLING, lightwalletd::PoolType::Invalid as i32];
    let range = lightwalletd::BlockRange {
        pool_types: pool_types.clone(),
        ..block_range(1, FIXTURE_TIP_HEIGHT)
    };
    for response in [
        client.get_block_range(range.clone()).await,
        client.get_block_range_nullifiers(range).await,
    ] {
        let status = response
            .err()
            .ok_or_else(|| eyre!("invalid pool opened a block stream"))?;
        assert_eq!(status.code(), Code::Unknown);
        assert_eq!(
            status.message(),
            "GetBlockRange: invalid pool type requested"
        );
    }
    let status = client
        .get_mempool_tx(lightwalletd::GetMempoolTxRequest {
            pool_types,
            exclude_txid_suffixes: Vec::new(),
        })
        .await
        .err()
        .ok_or_else(|| eyre!("invalid pool opened a mempool stream"))?;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "invalid pool type requested");

    let range = lightwalletd::BlockRange {
        pool_types: vec![99, -1],
        ..block_range(1, FIXTURE_TIP_HEIGHT)
    };
    for response in [
        client.get_block_range(range.clone()).await?,
        client.get_block_range_nullifiers(range).await?,
    ] {
        let mut stream = response.into_inner();
        for expected_height in 1..=FIXTURE_TIP_HEIGHT {
            let block = stream
                .message()
                .await?
                .ok_or_else(|| eyre!("missing height {expected_height}"))?;
            assert_eq!(block.height, expected_height);
            assert_eq!(block.vtx.len(), 0);
        }
        assert!(stream.message().await?.is_none());
    }
    Ok(())
}

fn block_range(start: u64, end: u64) -> lightwalletd::BlockRange {
    let block_id = |height| lightwalletd::BlockId {
        height,
        hash: Vec::new(),
    };
    lightwalletd::BlockRange {
        start: Some(block_id(start)),
        end: Some(block_id(end)),
        pool_types: Vec::new(),
    }
}

fn assert_refused_before_decoding(
    refusal: Option<&Status>,
    method: &str,
    field_name: &str,
) -> Result<()> {
    let refusal = refusal.ok_or_else(|| eyre!("{method} flood was accepted"))?;
    assert_eq!(refusal.code(), Code::InvalidArgument, "{refusal:?}");
    let expected_prefix = format!("{method} request field {field_name} carries ");
    assert!(
        refusal.message().starts_with(&expected_prefix),
        "expected a pre-decode refusal starting {expected_prefix:?}: {refusal:?}",
    );
    Ok(())
}

async fn spawn_compat_server() -> Result<CompactTxStreamerClient<tonic::transport::Channel>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let tip_height = u32::try_from(FIXTURE_TIP_HEIGHT)?;
    let mut store_fixture = WalletServingStoreFixture::from_chain(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(tip_height),
        &activations,
    )?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let wallet_query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(
        Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?),
    ));
    let server = LightwalletdGrpcAdapter::new(wallet_query).into_server();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    tokio::spawn(async move {
        // The fixture owns the store directories the server reads.
        let _store_fixture = store_fixture;
        Server::builder()
            .add_service(server)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    Ok(CompactTxStreamerClient::connect(format!("http://{server_addr}")).await?)
}
