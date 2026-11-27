//! Test the shipped Darkside command through its native and browser transports.

use std::{process::Stdio, time::Duration};

use eyre::{ContextCompat, Result};
use tokio::{
    io::{AsyncBufReadExt as _, BufReader},
    process::{Child, Command},
    time::timeout,
};
use tonic::{Code, transport::Channel};
use zebra_chain::{serialization::ZcashDeserializeInto as _, transaction::Transaction};
use zinder_core::{
    TransactionId,
    wire::{
        decode_internal_block_hash, decode_rpc_block_hash_hex, encode_internal_block_hash,
        encode_internal_transaction_id, encode_rpc_block_hash_hex,
    },
};
use zinder_proto::compat::lightwalletd::{
    BlockId, BlockRange, ChainSpec, CompactBlock, DarksideBlock, DarksideBlocksUrl,
    DarksideEmptyBlocks, DarksideHeight, DarksideMetaState, DarksideSubtreeRoots,
    DarksideTransactionsUrl, Empty, GetAddressUtxosArg, GetAddressUtxosReply, GetMempoolTxRequest,
    GetSubtreeRootsArg, PoolType, RawTransaction, SubtreeRoot, TreeState, TxFilter,
    compact_tx_streamer_client::CompactTxStreamerClient,
    darkside_streamer_client::DarksideStreamerClient,
};
use zinder_testkit::{
    P2pkhSpendArgs, TransparentTestKey,
    grpc_web::{decode_grpc_web_message, encode_grpc_web_frame},
};

const BROWSER_ORIGIN: &str = "http://127.0.0.1:5173";

struct TestProcess {
    child: Child,
    endpoint: String,
}

fn command() -> Command {
    let mut command = Command::new(zinder_testkit::bin_exe!("zinder"));
    command.args(["dev", "darkside"]);
    command
}

impl TestProcess {
    async fn start(args: &[&str]) -> Result<Self> {
        let mut child = command()
            .args(["--listen-addr", "127.0.0.1:0"])
            .args(args)
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdout = child.stdout.take().wrap_err("no Darkside stdout")?;
        let line = timeout(
            Duration::from_secs(5),
            BufReader::new(stdout).lines().next_line(),
        )
        .await??
        .wrap_err("Darkside exited before binding")?;
        let address = line
            .strip_prefix("Darkside listening on ")
            .wrap_err("missing bound address")?;
        let endpoint = format!("http://{address}");
        Ok(Self { child, endpoint })
    }

    async fn clients(
        &self,
    ) -> Result<(
        DarksideStreamerClient<Channel>,
        CompactTxStreamerClient<Channel>,
    )> {
        let channel = Channel::from_shared(self.endpoint.clone())?
            .connect()
            .await?;
        Ok((
            DarksideStreamerClient::new(channel.clone()),
            CompactTxStreamerClient::new(channel),
        ))
    }

    async fn wait(&mut self) -> Result<()> {
        assert!(
            timeout(Duration::from_secs(5), self.child.wait())
                .await??
                .success()
        );
        Ok(())
    }
}

fn metadata(start: i32) -> DarksideMetaState {
    DarksideMetaState {
        sapling_activation: start,
        branch_id: "c2d6d0b4".to_owned(),
        chain_name: "regtest".to_owned(),
        start_sapling_commitment_tree_size: 7,
        ..DarksideMetaState::default()
    }
}

fn at(height: u64) -> BlockId {
    BlockId {
        height,
        ..BlockId::default()
    }
}

async fn stage_blocks(
    control: &mut DarksideStreamerClient<Channel>,
    height: i32,
    nonce: i32,
    count: i32,
) -> Result<()> {
    control
        .stage_blocks_create(DarksideEmptyBlocks {
            height,
            nonce,
            count,
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn darkside_reorg_replaces_blocks_and_removes_orphaned_fixtures() -> Result<()> {
    let process = TestProcess::start(&[]).await?;
    let (mut control, mut wallet) = process.clients().await?;
    control.reset(metadata(100)).await?;
    stage_blocks(&mut control, 100, 10, 5).await?;
    control.apply_staged(DarksideHeight { height: 103 }).await?;
    assert_eq!(
        wallet
            .get_latest_block(ChainSpec {})
            .await?
            .into_inner()
            .height,
        103
    );
    assert_eq!(
        wallet
            .get_block(at(104))
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::NotFound)
    );
    control.apply_staged(DarksideHeight { height: 104 }).await?;
    let common = wallet.get_block(at(101)).await?.into_inner();
    let orphan = wallet.get_block(at(102)).await?.into_inner();
    let hash = decode_internal_block_hash(&orphan.hash)?;
    let tree = TreeState {
        height: 102,
        network: "regtest".to_owned(),
        hash: encode_rpc_block_hash_hex(hash),
        sapling_tree: "000000".to_owned(),
        ..TreeState::default()
    };
    control.add_tree_state(tree.clone()).await?;
    assert_eq!(wallet.get_tree_state(at(102)).await?.into_inner(), tree);

    let roots_request = GetSubtreeRootsArg {
        start_index: 5,
        ..GetSubtreeRootsArg::default()
    };
    let root = SubtreeRoot {
        root_hash: vec![5; 32],
        completing_block_hash: vec![6; 32],
        completing_block_height: 102,
    };
    control
        .set_subtree_roots(DarksideSubtreeRoots {
            start_index: 5,
            subtree_roots: vec![root.clone()],
            ..DarksideSubtreeRoots::default()
        })
        .await?;
    assert_eq!(
        wallet
            .get_subtree_roots(roots_request)
            .await?
            .into_inner()
            .message()
            .await?,
        Some(root)
    );

    let utxo = GetAddressUtxosReply {
        address: "test-address".to_owned(),
        txid: vec![7; 32],
        value_zat: 50,
        height: 102,
        script: vec![0x51],
        ..GetAddressUtxosReply::default()
    };
    control.add_address_utxo(utxo.clone()).await?;
    let utxos = GetAddressUtxosArg {
        addresses: vec![utxo.address.clone()],
        ..GetAddressUtxosArg::default()
    };
    assert_eq!(
        wallet
            .get_address_utxos(utxos.clone())
            .await?
            .into_inner()
            .address_utxos,
        [utxo]
    );

    stage_blocks(&mut control, 102, 11, 2).await?;
    control.apply_staged(DarksideHeight { height: 104 }).await?;
    let range = BlockRange {
        start: Some(at(103)),
        end: Some(at(101)),
        pool_types: Vec::new(),
    };
    let mut blocks = wallet.get_block_range(range).await?.into_inner();
    let last = blocks.message().await?.wrap_err("missing new tip")?;
    let first = blocks
        .message()
        .await?
        .wrap_err("missing first replacement")?;
    assert_eq!((last.height, first.height), (103, 102));
    assert_ne!(first.hash, orphan.hash);
    assert_eq!(last.prev_hash, first.hash);
    assert_eq!(first.prev_hash, common.hash);
    assert_eq!(blocks.message().await?, Some(common));
    assert!(blocks.message().await?.is_none());
    assert!(
        wallet
            .get_address_utxos(utxos)
            .await?
            .into_inner()
            .address_utxos
            .is_empty()
    );
    assert!(
        wallet
            .get_subtree_roots(roots_request)
            .await?
            .into_inner()
            .message()
            .await?
            .is_none()
    );
    assert_eq!(
        wallet
            .get_tree_state(at(102))
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::NotFound)
    );
    Ok(())
}

fn broadcast_fixture() -> Result<(RawTransaction, Vec<u8>)> {
    let key = TransparentTestKey::from_seed(&[17; 32])?;
    let bytes = key.build_p2pkh_spend(&P2pkhSpendArgs {
        coinbase_txid_be: [42; 32],
        coinbase_vout: 0,
        coinbase_value_zats: 50_000,
        recipient: &key.scratch_recipient_address(1),
        target_height: 120,
    })?;
    let tx: Transaction = bytes.as_slice().zcash_deserialize_into()?;
    let txid = encode_internal_transaction_id(TransactionId::from_bytes(tx.hash().0)).to_vec();
    Ok((
        RawTransaction {
            data: bytes,
            height: 0,
        },
        txid,
    ))
}

#[tokio::test]
async fn darkside_captures_mempool_broadcasts_and_mines_staged_transactions() -> Result<()> {
    let process = TestProcess::start(&[]).await?;
    let (mut control, mut wallet) = process.clients().await?;
    control.reset(metadata(100)).await?;
    stage_blocks(&mut control, 100, 10, 2).await?;
    control.apply_staged(DarksideHeight { height: 100 }).await?;
    let mut mempool = wallet.get_mempool_stream(Empty {}).await?.into_inner();
    let (broadcast, hash) = broadcast_fixture()?;
    let filter = TxFilter {
        hash: hash.clone(),
        ..TxFilter::default()
    };
    assert_eq!(
        wallet
            .send_transaction(broadcast.clone())
            .await?
            .into_inner()
            .error_code,
        0
    );
    let mut pending = broadcast.clone();
    pending.height = 100;
    assert_eq!(
        timeout(Duration::from_secs(2), mempool.message()).await??,
        Some(pending)
    );
    assert_ne!(
        wallet
            .send_transaction(broadcast.clone())
            .await?
            .into_inner()
            .error_code,
        0
    );

    let request = GetMempoolTxRequest {
        pool_types: vec![PoolType::Transparent.into()],
        ..GetMempoolTxRequest::default()
    };
    let mut compact_mempool = wallet.get_mempool_tx(request).await?.into_inner();
    assert_eq!(
        compact_mempool
            .message()
            .await?
            .wrap_err("missing mempool transaction")?
            .txid,
        hash
    );
    assert!(compact_mempool.message().await?.is_none());

    let mut captured = control
        .get_incoming_transactions(Empty {})
        .await?
        .into_inner();
    let mut expected = broadcast.clone();
    expected.height = u64::MAX;
    assert_eq!(captured.message().await?, Some(expected));
    assert!(captured.message().await?.is_none());

    let mut mined = broadcast.clone();
    mined.height = 101;
    control
        .stage_transactions_stream(tokio_stream::iter([mined.clone()]))
        .await?;
    assert_eq!(
        wallet.get_transaction(filter.clone()).await?.into_inner(),
        broadcast
    );
    control.apply_staged(DarksideHeight { height: 101 }).await?;
    assert!(mempool.message().await?.is_none());
    assert_eq!(
        wallet.get_transaction(filter.clone()).await?.into_inner(),
        mined
    );
    let block = wallet.get_block(at(101)).await?.into_inner();
    assert_eq!(
        block
            .vtx
            .as_slice()
            .first()
            .wrap_err("missing compact transaction")?
            .txid,
        hash
    );

    stage_blocks(&mut control, 101, 11, 1).await?;
    control.apply_staged(DarksideHeight { height: 101 }).await?;
    assert_eq!(
        wallet
            .get_transaction(filter)
            .await
            .err()
            .map(|status| status.code()),
        Some(Code::NotFound)
    );
    Ok(())
}

async fn grpc_web<R: prost::Message + Default>(
    client: &reqwest::Client,
    endpoint: &str,
    method: &str,
    message: &impl prost::Message,
) -> Result<R> {
    let reply = client
        .post(format!("{endpoint}/cash.z.wallet.sdk.rpc.{method}"))
        .header("origin", BROWSER_ORIGIN)
        .header("content-type", "application/grpc-web+proto")
        .header("x-grpc-web", "1")
        .body(encode_grpc_web_frame(&message.encode_to_vec())?)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        reply
            .headers()
            .get("access-control-allow-origin")
            .and_then(|header| header.to_str().ok()),
        Some(BROWSER_ORIGIN)
    );
    decode_grpc_web_message(&reply.bytes().await?)
}

#[tokio::test]
async fn darkside_control_and_wallet_responses_work_over_grpc_web() -> Result<()> {
    let mut process = TestProcess::start(&["--grpc-web-origin", BROWSER_ORIGIN]).await?;
    let client = reqwest::Client::builder().http1_only().no_proxy().build()?;
    let (_, mut wallet) = process.clients().await?;
    let _: Empty = grpc_web(
        &client,
        &process.endpoint,
        "DarksideStreamer/Reset",
        &metadata(100),
    )
    .await?;
    let _: Empty = grpc_web(
        &client,
        &process.endpoint,
        "DarksideStreamer/StageBlocksCreate",
        &DarksideEmptyBlocks {
            height: 100,
            count: 1,
            nonce: 77,
        },
    )
    .await?;
    let _: Empty = grpc_web(
        &client,
        &process.endpoint,
        "DarksideStreamer/ApplyStaged",
        &DarksideHeight { height: 100 },
    )
    .await?;
    let block: CompactBlock = grpc_web(
        &client,
        &process.endpoint,
        "CompactTxStreamer/GetBlock",
        &at(100),
    )
    .await?;
    assert_eq!(wallet.get_block(at(100)).await?.into_inner(), block);
    assert_eq!(block.prev_hash, [0; 32]);
    assert_eq!(block.time, 1);
    assert_eq!(
        block
            .chain_metadata
            .wrap_err("missing chain metadata")?
            .sapling_commitment_tree_size,
        7
    );
    let range = BlockRange {
        start: Some(at(100)),
        end: Some(at(100)),
        pool_types: Vec::new(),
    };
    let streamed: CompactBlock = grpc_web(
        &client,
        &process.endpoint,
        "CompactTxStreamer/GetBlockRange",
        &range,
    )
    .await?;
    assert_eq!(
        wallet
            .get_block_range(range)
            .await?
            .into_inner()
            .message()
            .await?,
        Some(streamed)
    );
    let _: Empty = grpc_web(
        &client,
        &process.endpoint,
        "DarksideStreamer/Stop",
        &Empty {},
    )
    .await?;
    process.wait().await
}

#[tokio::test]
async fn darkside_bounds_listener_lifetime_and_refuses_implicit_remote_access() -> Result<()> {
    let refused = command()
        .args(["--listen-addr", "0.0.0.0:0"])
        .output()
        .await?;
    assert!(!refused.status.success());
    assert!(String::from_utf8(refused.stderr)?.contains("--allow-non-loopback"));
    let zero = command().args(["--timeout-secs", "0"]).output().await?;
    assert!(!zero.status.success());

    let mut process = TestProcess::start(&["--timeout-secs", "2", "--reject-broadcasts"]).await?;
    let (mut control, mut wallet) = process.clients().await?;
    control.reset(metadata(100)).await?;
    stage_blocks(&mut control, 100, 1, 1).await?;
    control.apply_staged(DarksideHeight { height: 100 }).await?;
    let _open = wallet.get_mempool_stream(Empty {}).await?;
    let (broadcast, _) = broadcast_fixture()?;
    assert_ne!(
        wallet
            .send_transaction(broadcast)
            .await?
            .into_inner()
            .error_code,
        0
    );
    assert!(
        control
            .get_incoming_transactions(Empty {})
            .await?
            .into_inner()
            .message()
            .await?
            .is_none()
    );
    process.wait().await
}

#[tokio::test]
async fn darkside_stages_serialized_fixtures_and_preserves_active_state_on_failed_apply()
-> Result<()> {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../zinder-ingest/tests/fixtures/z3-regtest-block-1.json"
    ))?;
    let hex_block = fixture["raw_block_hex"]
        .as_str()
        .wrap_err("missing raw block fixture")?
        .to_owned();
    let hash = decode_rpc_block_hash_hex(fixture["hash"].as_str().wrap_err("missing block hash")?)?;
    let (broadcast, _) = broadcast_fixture()?;
    let encoded_tx = hex::encode(&broadcast.data);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let blocks = hex_block.clone();
    let fixtures = axum::Router::new()
        .route("/blocks", axum::routing::get(|| async move { blocks }))
        .route("/txs", axum::routing::get(|| async move { encoded_tx }));
    tokio::spawn(async move { axum::serve(listener, fixtures).await });

    let process = TestProcess::start(&["--fixture-origin", &origin]).await?;
    let (mut control, mut wallet) = process.clients().await?;
    control.reset(metadata(1)).await?;
    control
        .stage_blocks_stream(tokio_stream::iter([DarksideBlock { block: hex_block }]))
        .await?;
    control.apply_staged(DarksideHeight { height: 1 }).await?;
    let original = wallet.get_block(at(1)).await?.into_inner();
    assert_eq!(original.hash, encode_internal_block_hash(hash));
    assert_eq!(original.vtx.len(), 1);
    assert!(original.vtx[0].vin.is_empty());

    stage_blocks(&mut control, 3, 5, 1).await?;
    let failed = control.apply_staged(DarksideHeight { height: 3 }).await;
    assert_eq!(
        failed.err().map(|status| status.code()),
        Some(Code::FailedPrecondition)
    );
    assert_eq!(wallet.get_block(at(1)).await?.into_inner(), original);

    control.reset(metadata(1)).await?;
    let refused = control
        .stage_blocks(DarksideBlocksUrl {
            url: "http://127.0.0.1:1/blocks".to_owned(),
        })
        .await;
    assert_eq!(
        refused.err().map(|status| status.code()),
        Some(Code::PermissionDenied)
    );
    control
        .stage_blocks(DarksideBlocksUrl {
            url: format!("{origin}/blocks"),
        })
        .await?;
    control
        .stage_transactions(DarksideTransactionsUrl {
            height: 1,
            url: format!("{origin}/txs"),
        })
        .await?;
    control.apply_staged(DarksideHeight { height: 1 }).await?;
    let enriched = wallet.get_block(at(1)).await?.into_inner();
    assert_ne!(enriched.hash, original.hash);
    assert_eq!(enriched.vtx.len(), 2);
    Ok(())
}
