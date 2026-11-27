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
        encode_internal_transaction_id, encode_rpc_block_hash_bytes, encode_rpc_block_hash_hex,
        encode_rpc_transaction_id_hex,
    },
};
use zinder_proto::compat::lightwalletd::{
    BlockId, BlockRange, ChainSpec, CompactBlock, CompactTxIn, DarksideBlock, DarksideBlocksUrl,
    DarksideEmptyBlocks, DarksideHeight, DarksideMetaState, DarksideSubtreeRoots,
    DarksideTransactionsUrl, Duration as PingDuration, Empty, GetAddressUtxosArg,
    GetAddressUtxosReply, GetMempoolTxRequest, GetSubtreeRootsArg, PingResponse, PoolType,
    RawTransaction, SendResponse, SubtreeRoot, TreeState, TxFilter,
    compact_tx_streamer_client::CompactTxStreamerClient,
    darkside_streamer_client::DarksideStreamerClient,
};
use zinder_testkit::{
    P2pkhSpendArgs, TransparentTestKey,
    grpc_web::{decode_grpc_web_message, decode_grpc_web_messages, encode_grpc_web_frame},
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
#[allow(
    clippy::too_many_lines,
    reason = "The scenario checks fixture visibility across one continuous chain replacement."
)]
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
    assert_eq!(
        wallet
            .get_address_utxos(utxos)
            .await?
            .into_inner()
            .address_utxos,
        Vec::<GetAddressUtxosReply>::new()
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

fn broadcast_fixture() -> Result<(RawTransaction, TransactionId)> {
    let key = TransparentTestKey::from_seed(&[17; 32])?;
    let bytes = key.build_p2pkh_spend(&P2pkhSpendArgs {
        coinbase_txid_be: [42; 32],
        coinbase_vout: 0,
        coinbase_value_zats: 50_000,
        recipient: &key.scratch_recipient_address(1),
        target_height: 120,
    })?;
    let tx: Transaction = bytes.as_slice().zcash_deserialize_into()?;
    let txid = TransactionId::from_bytes(tx.hash().0);
    Ok((
        RawTransaction {
            data: bytes,
            height: 0,
        },
        txid,
    ))
}

fn successful_broadcast(txid: TransactionId) -> SendResponse {
    SendResponse {
        error_code: 0,
        error_message: encode_rpc_transaction_id_hex(txid),
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "The scenario tracks the same broadcast through mempool, mining, and rollback."
)]
async fn darkside_captures_mempool_broadcasts_and_mines_staged_transactions() -> Result<()> {
    let process = TestProcess::start(&[]).await?;
    let (mut control, mut wallet) = process.clients().await?;
    control.reset(metadata(100)).await?;
    stage_blocks(&mut control, 100, 10, 2).await?;
    control.apply_staged(DarksideHeight { height: 100 }).await?;
    let mut mempool = wallet.get_mempool_stream(Empty {}).await?.into_inner();
    let (broadcast, txid) = broadcast_fixture()?;
    let hash = encode_internal_transaction_id(txid).to_vec();
    let filter = TxFilter {
        hash: hash.clone(),
        ..TxFilter::default()
    };
    assert_eq!(
        wallet
            .send_transaction(broadcast.clone())
            .await?
            .into_inner(),
        successful_broadcast(txid)
    );
    assert_eq!(
        timeout(Duration::from_secs(2), mempool.message()).await??,
        Some(broadcast.clone())
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

struct BrowserRpc<'a> {
    endpoint: &'a str,
    client: reqwest::Client,
}

impl<'a> BrowserRpc<'a> {
    fn new(process: &'a TestProcess) -> Result<Self> {
        Ok(Self {
            endpoint: &process.endpoint,
            client: reqwest::Client::builder().http1_only().no_proxy().build()?,
        })
    }

    async fn request(
        &self,
        method: &str,
        message: &impl prost::Message,
    ) -> Result<reqwest::Response> {
        let endpoint = self.endpoint;
        let reply = self
            .client
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
        Ok(reply)
    }

    async fn unary<R: prost::Message + Default>(
        &self,
        method: &str,
        message: &impl prost::Message,
    ) -> Result<R> {
        decode_grpc_web_message(&self.request(method, message).await?.bytes().await?)
    }

    async fn stream<R: prost::Message + Default>(
        &self,
        method: &str,
        message: &impl prost::Message,
    ) -> Result<Vec<R>> {
        decode_grpc_web_messages(&self.request(method, message).await?.bytes().await?)
    }

    async fn stage_blocks(&self, height: i32, nonce: i32, count: i32) -> Result<()> {
        let _: Empty = self
            .unary(
                "DarksideStreamer/StageBlocksCreate",
                &DarksideEmptyBlocks {
                    height,
                    nonce,
                    count,
                },
            )
            .await?;
        Ok(())
    }

    async fn apply(&self, height: i32) -> Result<()> {
        let _: Empty = self
            .unary("DarksideStreamer/ApplyStaged", &DarksideHeight { height })
            .await?;
        Ok(())
    }

    async fn assert_not_found(&self, method: &str, message: &impl prost::Message) -> Result<()> {
        let reply = self.request(method, message).await?;
        let status = reply
            .headers()
            .get("grpc-status")
            .and_then(|header| header.to_str().ok());
        assert_eq!(status, Some("5"));
        Ok(())
    }
}

async fn register_browser_fixtures(
    browser: &BrowserRpc<'_>,
    orphan: &CompactBlock,
) -> Result<(GetAddressUtxosArg, GetSubtreeRootsArg)> {
    let hash = decode_internal_block_hash(&orphan.hash)?;
    let tree = TreeState {
        height: orphan.height,
        network: "regtest".to_owned(),
        hash: encode_rpc_block_hash_hex(hash),
        sapling_tree: "000000".to_owned(),
        ..TreeState::default()
    };
    let _: Empty = browser
        .unary("DarksideStreamer/AddTreeState", &tree)
        .await?;
    let observed_tree: TreeState = browser
        .unary("CompactTxStreamer/GetTreeState", &at(orphan.height))
        .await?;
    assert_eq!(observed_tree, tree);

    let root = SubtreeRoot {
        root_hash: vec![5; 32],
        completing_block_hash: encode_rpc_block_hash_bytes(hash).to_vec(),
        completing_block_height: orphan.height,
    };
    let roots = GetSubtreeRootsArg {
        start_index: 5,
        ..GetSubtreeRootsArg::default()
    };
    let _: Empty = browser
        .unary(
            "DarksideStreamer/SetSubtreeRoots",
            &DarksideSubtreeRoots {
                start_index: roots.start_index,
                subtree_roots: vec![root.clone()],
                ..DarksideSubtreeRoots::default()
            },
        )
        .await?;
    let observed_roots: Vec<SubtreeRoot> = browser
        .stream("CompactTxStreamer/GetSubtreeRoots", &roots)
        .await?;
    assert_eq!(observed_roots, [root]);

    let utxo = GetAddressUtxosReply {
        address: "test-address".to_owned(),
        txid: vec![7; 32],
        script: vec![0x51],
        value_zat: 50,
        height: orphan.height,
        ..GetAddressUtxosReply::default()
    };
    let utxos = GetAddressUtxosArg {
        addresses: vec![utxo.address.clone()],
        ..GetAddressUtxosArg::default()
    };
    let _: Empty = browser
        .unary("DarksideStreamer/AddAddressUtxo", &utxo)
        .await?;
    let observed_utxos: Vec<GetAddressUtxosReply> = browser
        .stream("CompactTxStreamer/GetAddressUtxosStream", &utxos)
        .await?;
    assert_eq!(observed_utxos, [utxo]);
    Ok((utxos, roots))
}

#[tokio::test]
async fn darkside_grpc_web_streams_reorgs_and_removes_orphaned_fixtures() -> Result<()> {
    let mut process = TestProcess::start(&["--grpc-web-origin", BROWSER_ORIGIN]).await?;
    let browser = BrowserRpc::new(&process)?;
    let (_, mut wallet) = process.clients().await?;
    let _: Empty = browser
        .unary("DarksideStreamer/Reset", &metadata(100))
        .await?;
    browser.stage_blocks(100, 77, 5).await?;
    browser.apply(103).await?;
    let tip: BlockId = browser
        .unary("CompactTxStreamer/GetLatestBlock", &ChainSpec {})
        .await?;
    assert_eq!(tip.height, 103);
    browser
        .assert_not_found("CompactTxStreamer/GetBlock", &at(104))
        .await?;
    browser.apply(104).await?;

    let range = BlockRange {
        start: Some(at(101)),
        end: Some(at(103)),
        pool_types: Vec::new(),
    };
    let original: Vec<CompactBlock> = browser
        .stream("CompactTxStreamer/GetBlockRange", &range)
        .await?;
    let [common, first_orphan, last_orphan] = original.as_slice() else {
        eyre::bail!("expected three original compact blocks");
    };
    let (utxos, roots) = register_browser_fixtures(&browser, first_orphan).await?;
    browser.stage_blocks(102, 78, 2).await?;
    browser.apply(104).await?;

    let replacement: Vec<CompactBlock> = browser
        .stream("CompactTxStreamer/GetBlockRange", &range)
        .await?;
    let [retained, first, last] = replacement.as_slice() else {
        eyre::bail!("expected a common block and two replacements");
    };
    assert_eq!(retained, common);
    assert_eq!((first.height, last.height), (102, 103));
    assert_ne!(first.hash, first_orphan.hash);
    assert_ne!(last.hash, last_orphan.hash);
    assert_eq!(first.prev_hash, common.hash);
    assert_eq!(last.prev_hash, first.hash);
    assert_eq!(wallet.get_block(at(103)).await?.into_inner(), *last);
    let tip: BlockId = browser
        .unary("CompactTxStreamer/GetLatestBlock", &ChainSpec {})
        .await?;
    assert_eq!(
        tip,
        BlockId {
            height: 103,
            hash: last.hash.clone()
        }
    );
    browser
        .assert_not_found("CompactTxStreamer/GetBlock", &at(104))
        .await?;

    let removed_utxos: Vec<GetAddressUtxosReply> = browser
        .stream("CompactTxStreamer/GetAddressUtxosStream", &utxos)
        .await?;
    let removed_roots: Vec<SubtreeRoot> = browser
        .stream("CompactTxStreamer/GetSubtreeRoots", &roots)
        .await?;
    assert_eq!(removed_utxos, Vec::<GetAddressUtxosReply>::new());
    assert_eq!(removed_roots, Vec::<SubtreeRoot>::new());
    browser
        .assert_not_found("CompactTxStreamer/GetTreeState", &at(102))
        .await?;
    let _: Empty = browser.unary("DarksideStreamer/Stop", &Empty {}).await?;
    process.wait().await
}

#[tokio::test]
async fn darkside_grpc_web_captures_and_mines_a_broadcast() -> Result<()> {
    let process = TestProcess::start(&["--grpc-web-origin", BROWSER_ORIGIN]).await?;
    let browser = BrowserRpc::new(&process)?;
    let (mut control, _) = process.clients().await?;
    let _: Empty = browser
        .unary("DarksideStreamer/Reset", &metadata(100))
        .await?;
    browser.stage_blocks(100, 77, 2).await?;
    browser.apply(100).await?;

    let (broadcast, txid) = broadcast_fixture()?;
    let accepted: SendResponse = browser
        .unary("CompactTxStreamer/SendTransaction", &broadcast)
        .await?;
    assert_eq!(accepted, successful_broadcast(txid));
    let filter = TxFilter {
        hash: encode_internal_transaction_id(txid).to_vec(),
        ..TxFilter::default()
    };
    let pending: RawTransaction = browser
        .unary("CompactTxStreamer/GetTransaction", &filter)
        .await?;
    assert_eq!(pending, broadcast);
    let captured: Vec<RawTransaction> = browser
        .stream("DarksideStreamer/GetIncomingTransactions", &Empty {})
        .await?;
    assert_eq!(
        captured,
        [RawTransaction {
            height: u64::MAX,
            ..broadcast.clone()
        }]
    );
    let drained: Vec<RawTransaction> = browser
        .stream("DarksideStreamer/GetIncomingTransactions", &Empty {})
        .await?;
    assert_eq!(drained, Vec::<RawTransaction>::new());

    let mined = RawTransaction {
        height: 101,
        ..broadcast
    };
    control
        .stage_transactions_stream(tokio_stream::iter([mined.clone()]))
        .await?;
    browser.apply(101).await?;
    let observed: RawTransaction = browser
        .unary("CompactTxStreamer/GetTransaction", &filter)
        .await?;
    assert_eq!(observed, mined);
    let block: CompactBlock = browser
        .unary("CompactTxStreamer/GetBlock", &at(101))
        .await?;
    assert_eq!(block.vtx.len(), 1);
    assert_eq!(block.vtx[0].txid, filter.hash);

    browser.stage_blocks(101, 78, 1).await?;
    browser.apply(101).await?;
    browser
        .assert_not_found("CompactTxStreamer/GetTransaction", &filter)
        .await?;
    Ok(())
}

#[tokio::test]
async fn darkside_ping_counts_overlapping_calls() -> Result<()> {
    let process = TestProcess::start(&[]).await?;
    let (_, mut first) = process.clients().await?;
    let mut second = first.clone();
    let ping = PingDuration {
        interval_us: 500_000,
    };
    let (first_reply, second_reply) = tokio::try_join!(first.ping(ping), second.ping(ping))?;
    let first_reply = first_reply.into_inner();
    let second_reply = second_reply.into_inner();
    let mut entries = [first_reply.entry, second_reply.entry];
    let mut exits = [first_reply.exit, second_reply.exit];
    entries.sort_unstable();
    exits.sort_unstable();
    assert_eq!(entries, [1, 2]);
    assert_eq!(exits, [0, 1]);
    let idle = first
        .ping(PingDuration { interval_us: 0 })
        .await?
        .into_inner();
    assert_eq!(idle, PingResponse { entry: 1, exit: 0 });
    Ok(())
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
    assert_eq!(original.vtx[0].vin, Vec::<CompactTxIn>::new());

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
