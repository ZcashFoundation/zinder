use std::time::Duration;

use eyre::{Result, eyre};
use http_body_util::{BodyExt as _, Full};
use prost::Message as _;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_stream::StreamExt as _;
use tonic::{Code, Status, body::Body, codegen::Bytes, transport::Channel};
use tower::{Layer as _, ServiceExt as _};
use zinder_core::{BlockId, Network, TransactionId, wire::encode_rpc_transaction_id_hex};
use zinder_proto::compat::lightwalletd::{
    GetAddressUtxosArg, GetAddressUtxosReply, compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::CompactTxStreamer as _,
};
use zinder_runtime::{ClientAddressSource, GrpcSendStallLayer, public_grpc_server};
use zinder_testkit::{ChainFixture, RpcReply, method};

use super::{
    ADDRESS, Fixture, UtxoNode, balance_request, one_upgrade_per_height, populated_utxo_fixture,
    populated_utxo_fixture_with, two_response_slots, utxo_request, utxos_at,
};

const HEIGHTS: u8 = 100;
const OUTPUTS_PER_TX: i32 = 500;
const MAX_ENTRIES: u32 = 100_000;
const DEFAULT_DECODE_BYTES: usize = 4 * 1024 * 1024;

fn wallet_txid(height: u8, rank: u8) -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes[0] = rank;
    bytes[1] = height;
    bytes[31] = 1 - rank;
    bytes
}

fn utxo_page(params: &Value, script_hex: &str, tip: BlockId) -> RpcReply {
    let start = params[0]["startHeight"].as_u64().unwrap_or_default();
    let limit = params[0]["maxEntries"].as_u64().unwrap_or_default();
    let mut rows = Vec::new();
    for height in (1..=HEIGHTS).filter(|height| u64::from(*height) >= start) {
        // Transaction position runs opposite to internal txid order.
        for rank in [1, 0] {
            let txid =
                encode_rpc_transaction_id_hex(TransactionId::from_bytes(wallet_txid(height, rank)));
            for index in 0..OUTPUTS_PER_TX {
                rows.push(
                    json!({"address": ADDRESS, "txid": txid, "outputIndex": index,
                    "script": script_hex, "satoshis": index + 1, "height": height}),
                );
                if u64::try_from(rows.len()) == Ok(limit) {
                    return utxos_at(tip, &Value::Array(rows));
                }
            }
        }
    }
    utxos_at(tip, &Value::Array(rows))
}

fn fixture(script: &[u8]) -> Result<Fixture> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(u32::from(HEIGHTS));
    let tip = chain
        .blocks()
        .last()
        .map(|block| BlockId::new(block.height, block.hash))
        .ok_or_else(|| eyre!("missing fixture tip"))?;
    let script_hex = hex::encode(script);
    Fixture::from_chain(
        &chain,
        &one_upgrade_per_height()?,
        [method("getaddressutxos").reply_with(move |params| utxo_page(params, &script_hex, tip))],
        Duration::from_secs(30),
    )
}

fn request() -> GetAddressUtxosArg {
    GetAddressUtxosArg {
        addresses: vec![ADDRESS.to_owned()],
        start_height: 1,
        max_entries: MAX_ENTRIES,
    }
}

fn block_ordered_outputs(script: &[u8]) -> impl Iterator<Item = GetAddressUtxosReply> {
    (1..=HEIGHTS).flat_map(move |height| {
        [1, 0].into_iter().flat_map(move |rank| {
            let txid = wallet_txid(height, rank);
            (0..OUTPUTS_PER_TX).map(move |index| GetAddressUtxosReply {
                address: ADDRESS.to_owned(),
                txid: txid.to_vec(),
                index,
                script: script.to_vec(),
                value_zat: i64::from(index + 1),
                height: u64::from(height),
            })
        })
    })
}

async fn with_client(
    check: impl AsyncFnOnce(CompactTxStreamerClient<Channel>, &[u8]) -> Result<()>,
) -> Result<()> {
    let script = zinder_source::parse_transparent_address(ADDRESS)?.script();
    let fixture = fixture(script.as_raw_bytes())?;
    with_fixture_client(fixture, async |client| {
        check(client, script.as_raw_bytes()).await
    })
    .await
}

async fn with_fixture_client(
    fixture: Fixture,
    check: impl AsyncFnOnce(CompactTxStreamerClient<Channel>) -> Result<()>,
) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = public_grpc_server(ClientAddressSource::Connection)
        .add_service(fixture.adapter.into_server())
        .serve_with_incoming(listener);
    let request = async {
        let client = CompactTxStreamerClient::connect(endpoint).await?;
        check(client).await
    };
    tokio::select! {
        stopped = server => Err(eyre!("gRPC server stopped during UTXO request: {stopped:?}")),
        outcome = tokio::time::timeout(Duration::from_secs(30), request) => outcome?,
    }
}

#[tokio::test]
async fn default_client_rejects_a_maximum_unary_utxo_response() -> Result<()> {
    with_client(async |mut client, _| {
        let refusal = client
            .get_address_utxos(request())
            .await
            .err()
            .ok_or_else(|| eyre!("default gRPC client accepted oversized unary response"))?;
        assert_eq!(refusal.code(), Code::OutOfRange);
        assert!(
            refusal
                .message()
                .contains(&format!("limit is: {DEFAULT_DECODE_BYTES} bytes")),
            "{refusal}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn larger_client_decode_limit_accepts_every_block_ordered_utxo() -> Result<()> {
    with_client(async |client, script| {
        let reply = client
            .max_decoding_message_size(16 * 1024 * 1024)
            .get_address_utxos(request())
            .await?
            .into_inner();
        assert!(reply.encoded_len() > DEFAULT_DECODE_BYTES);
        assert_eq!(reply.address_utxos.len(), usize::try_from(MAX_ENTRIES)?);
        let mut outputs = reply.address_utxos.into_iter();
        for expected in block_ordered_outputs(script) {
            assert_eq!(outputs.next(), Some(expected));
        }
        assert!(outputs.next().is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn default_client_streams_every_block_ordered_utxo() -> Result<()> {
    with_client(async |mut client, script| {
        let mut stream = client
            .get_address_utxos_stream(request())
            .await?
            .into_inner();
        for expected in block_ordered_outputs(script) {
            assert_eq!(stream.message().await?, Some(expected));
        }
        assert!(stream.message().await?.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_paused_utxo_stream_leaves_node_admission_for_another_call() -> Result<()> {
    let fixture = populated_utxo_fixture(2, 1, UtxoNode::Zakura)?;
    let mut paused = fixture
        .adapter
        .get_address_utxos_stream(utxo_request(2))
        .await?
        .into_inner();
    let first = paused
        .next()
        .await
        .transpose()?
        .ok_or_else(|| eyre!("missing first output"))?;
    let fast = fixture
        .adapter
        .get_address_utxos(utxo_request(2))
        .await?
        .into_inner();
    let second = paused
        .next()
        .await
        .transpose()?
        .ok_or_else(|| eyre!("missing second output"))?;
    assert_eq!(fast.address_utxos, [first, second]);
    assert!(paused.next().await.is_none());
    Ok(())
}

fn unary_utxo_request() -> Result<http::Request<Body>> {
    let request = utxo_request(2).into_inner();
    let mut frame = vec![0];
    frame.extend_from_slice(&u32::try_from(request.encoded_len())?.to_be_bytes());
    request.encode(&mut frame)?;
    http::Request::post("/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetAddressUtxos")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header(http::header::TE, "trailers")
        .body(Body::new(Full::new(Bytes::from(frame))))
        .map_err(Into::into)
}

#[tokio::test]
async fn two_unfinished_unary_utxo_bodies_refuse_a_third_but_allow_balance() -> Result<()> {
    let fixture = populated_utxo_fixture_with(2, 1, UtxoNode::Zakura, two_response_slots())?;
    let service = GrpcSendStallLayer::new(Duration::from_hours(1))
        .layer(fixture.adapter.clone().into_server());
    let unary = async || {
        service
            .clone()
            .oneshot(unary_utxo_request()?)
            .await
            .map_err(eyre::Report::from)
    };
    let mut first = unary().await?.into_body();
    let second = unary().await?.into_body();
    // Hand off the first body's data and hold its terminal poll. The
    // second body stays unpolled. Neither body needs a socket buffer.
    let piece = first
        .frame()
        .await
        .ok_or_else(|| eyre!("no unary data"))??;
    assert!(piece.is_data());
    let refusal = unary().await?;
    let status = Status::from_header_map(refusal.headers())
        .ok_or_else(|| eyre!("a third unary response was admitted"))?;
    assert_eq!(status.code(), Code::Unavailable);
    assert!(status.message().contains("utxo_response"), "{status:?}");

    let balance = fixture
        .adapter
        .get_taddress_balance(balance_request())
        .await?;
    assert_eq!(balance.into_inner().value_zat, 3);

    let trailers = first
        .frame()
        .await
        .ok_or_else(|| eyre!("no unary trailers"))??;
    assert_eq!(
        trailers
            .trailers_ref()
            .and_then(|trailers| trailers.get("grpc-status")),
        Some(&http::HeaderValue::from_static("0"))
    );
    let third = unary().await?;
    assert!(Status::from_header_map(third.headers()).is_none());
    drop(second);
    let fourth = unary().await?;
    assert!(Status::from_header_map(fourth.headers()).is_none());
    drop((piece, first, third, fourth));
    Ok(())
}

#[tokio::test]
async fn light_wallet_listener_returns_a_small_unary_utxo_reply() -> Result<()> {
    let fixture = populated_utxo_fixture(2, 1, UtxoNode::Zakura)?;
    with_fixture_client(fixture, async |mut client| {
        let reply = client.get_address_utxos(utxo_request(2)).await?;
        let outputs = reply.into_inner().address_utxos;
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].address, ADDRESS);
        Ok(())
    })
    .await
}
