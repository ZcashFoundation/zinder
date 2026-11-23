use std::time::Duration;

use eyre::{Result, eyre};
use prost::Message as _;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code,
    transport::{Channel, Server},
};
use zinder_core::{BlockId, Network, TransactionId, wire::encode_rpc_transaction_id_hex};
use zinder_proto::compat::lightwalletd::{
    GetAddressUtxosArg, GetAddressUtxosReply, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_testkit::{ChainFixture, RpcReply, method};

use super::{ADDRESS, Fixture, one_upgrade_per_height, utxos_at};

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
        // Both block position and RPC hex order run opposite to wallet txid order.
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

fn wallet_outputs(script: &[u8]) -> impl Iterator<Item = GetAddressUtxosReply> {
    (1..=HEIGHTS).flat_map(move |height| {
        [0, 1].into_iter().flat_map(move |rank| {
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
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = Server::builder()
        .add_service(fixture.adapter.into_server())
        .serve_with_incoming(TcpListenerStream::new(listener));
    let request = async {
        let client = CompactTxStreamerClient::connect(endpoint).await?;
        check(client, script.as_raw_bytes()).await
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
async fn larger_client_decode_limit_accepts_every_wallet_ordered_utxo() -> Result<()> {
    with_client(async |client, script| {
        let reply = client
            .max_decoding_message_size(16 * 1024 * 1024)
            .get_address_utxos(request())
            .await?
            .into_inner();
        assert!(reply.encoded_len() > DEFAULT_DECODE_BYTES);
        assert_eq!(reply.address_utxos.len(), usize::try_from(MAX_ENTRIES)?);
        let mut outputs = reply.address_utxos.into_iter();
        for expected in wallet_outputs(script) {
            assert_eq!(outputs.next(), Some(expected));
        }
        assert!(outputs.next().is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn default_client_streams_every_wallet_ordered_utxo() -> Result<()> {
    with_client(async |mut client, script| {
        let mut stream = client
            .get_address_utxos_stream(request())
            .await?
            .into_inner();
        for expected in wallet_outputs(script) {
            assert_eq!(stream.message().await?, Some(expected));
        }
        assert!(stream.message().await?.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_slow_utxo_stream_leaves_node_admission_for_another_client() -> Result<()> {
    with_client(async |mut client, script| {
        let mut slow = client
            .get_address_utxos_stream(request())
            .await?
            .into_inner();
        let first = wallet_outputs(script)
            .next()
            .ok_or_else(|| eyre!("missing fixture output"))?;
        assert_eq!(slow.message().await?, Some(first.clone()));
        let fast = client
            .clone()
            .get_address_utxos(GetAddressUtxosArg {
                max_entries: 1,
                ..request()
            })
            .await?
            .into_inner();
        assert_eq!(fast.address_utxos, [first]);
        assert_eq!(slow.message().await?, wallet_outputs(script).nth(1));
        Ok(())
    })
    .await
}
