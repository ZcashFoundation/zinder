use std::{fs, net::SocketAddr, path::Path, sync::Arc};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use eyre::{Result, ensure, eyre};
use http::{Request, header as http_header};
use http_body_util::{BodyExt as _, Full};
use prost::{Message, bytes::Bytes};
use reqwest::{Client, header};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code,
    body::Body,
    transport::{Channel, Server},
};
use tonic_web::GrpcWebLayer;
use tower::{ServiceBuilder, ServiceExt as _};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::{
    BlockHeight, BlockId, CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput,
    CompactSaplingSpend, CompactShieldedAction, CompactTransaction, CompactTransactionData,
    CompactTransparentInput, CompactTransparentOutput, Network, TransactionId,
    wire::encode_height_key_ascending,
};
use zinder_proto::compat::lightwalletd::{
    self, CompactBlock, PoolType, compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::SERVICE_NAME,
};
use zinder_query::{
    RocksDbCanonicalReader, RocksDbWalletReader, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_store::RawBlobRetention;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureBlock, FixtureTransactionRows,
    WalletServingStoreFixture, sample_regtest_upgrade_activations,
};

use super::{
    CONTENT_TYPE, CompatProcess, ORIGIN, TEXT_CONTENT_TYPE, post_web_request, read_web_response,
};

const SEALED_SEGMENT_BLOCKS: u32 = 4_096;
const SETTLED_BLOCKS: u32 = 4_096;
const TOTAL_BLOCKS: u32 = 4_196;

#[derive(Clone, Copy)]
enum RangeRpc {
    BlockRange,
    BlockRangeNullifiers,
}

impl RangeRpc {
    const fn method(self) -> &'static str {
        match self {
            Self::BlockRange => "GetBlockRange",
            Self::BlockRangeNullifiers => "GetBlockRangeNullifiers",
        }
    }
}

fn boundary_txs(height_num: u32) -> Vec<CompactTransaction> {
    if !matches!(height_num, 4_095..=4_098) {
        return Vec::new();
    }
    let seed = height_num.to_le_bytes()[0];
    let mut txid_bytes = [seed; 32];
    txid_bytes[..4].copy_from_slice(&height_num.to_le_bytes());
    let mut cmx = [0_u8; 32];
    cmx[0] = seed;
    vec![CompactTransaction {
        index: 0,
        transaction_id: TransactionId::from_bytes(txid_bytes),
        data: CompactTransactionData {
            fee_zat: Some(2_000),
            transparent_inputs: vec![CompactTransparentInput {
                previous_transaction_id: TransactionId::from_bytes([seed.wrapping_add(1); 32]),
                previous_output_index: 0,
            }],
            transparent_outputs: vec![CompactTransparentOutput {
                value_zat: 1_500,
                script_pub_key: vec![0x51],
            }],
            sapling_spends: vec![CompactSaplingSpend {
                nullifier: [seed.wrapping_add(2); 32],
            }],
            sapling_outputs: vec![CompactSaplingOutput {
                commitment: cmx,
                ephemeral_key: [seed.wrapping_add(3); 32],
                ciphertext: [seed.wrapping_add(4); 52],
            }],
            orchard_actions: vec![CompactShieldedAction {
                nullifier: [seed.wrapping_add(5); 32],
                commitment: cmx,
                ephemeral_key: [seed.wrapping_add(6); 32],
                ciphertext: [seed.wrapping_add(7); 52],
            }],
            ..Default::default()
        },
    }]
}

fn attach_boundary_block(
    mut chain: ChainFixture,
    blk: &FixtureBlock,
    txs: &[CompactTransaction],
) -> Result<ChainFixture> {
    if txs.is_empty() {
        return Ok(chain);
    }
    for tx in txs {
        let index = u32::try_from(tx.index)?;
        let mut rows = FixtureTransactionRows::from_raw_transaction(
            tx.transaction_id,
            blk.height,
            blk.hash,
            index,
            vec![blk.height.value().to_le_bytes()[0]; 16],
        );
        rows.facts.public_facts.counts.sapling_spend_count =
            u32::try_from(tx.data.sapling_spends.len())?;
        rows.facts.public_facts.counts.sapling_output_count =
            u32::try_from(tx.data.sapling_outputs.len())?;
        rows.facts.public_facts.counts.orchard_action_count =
            u32::try_from(tx.data.orchard_actions.len())?;
        chain = chain.with_transaction_rows(rows);
    }
    let compact = CompactBlockArtifact::new(
        BlockId::new(blk.height, blk.hash),
        blk.parent_hash,
        blk.block_time_seconds,
        txs.to_vec(),
        CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    )?;
    Ok(chain.with_compact_block_artifact(compact))
}

fn verify_only_first_segment_sealed(primary: &Path) -> Result<()> {
    let shielded_dir = primary.join("segments").join("shielded");
    let expected_name = format!("0000000001-{SEALED_SEGMENT_BLOCKS:010}.zshrng02");
    ensure!(
        shielded_dir.join(&expected_name).is_file(),
        "expected sealed segment {expected_name} in {}",
        shielded_dir.display()
    );
    let mut segment_names = Vec::new();
    for entry in fs::read_dir(&shielded_dir)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if name.ends_with(".zshrng02") {
            segment_names.push(name);
        }
    }
    ensure!(
        segment_names == vec![expected_name],
        "expected 4097+ to remain in live_tail_block, found segments {segment_names:?}"
    );
    Ok(())
}

fn build_boundary_writer(retention: RawBlobRetention) -> Result<CanonicalWriterFixture> {
    let mut chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(retention)
        .extend_blocks(TOTAL_BLOCKS);
    for height_num in 4_095_u32..=4_098 {
        let height = BlockHeight::new(height_num);
        let blk = chain
            .block_at(height)
            .cloned()
            .ok_or_else(|| eyre!("missing fixture block at {height_num}"))?;
        let txs = boundary_txs(height_num);
        chain = attach_boundary_block(chain, &blk, &txs)?;
    }
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(SETTLED_BLOCKS)),
        &sample_regtest_upgrade_activations(),
    )?;
    verify_only_first_segment_sealed(writer.canonical_primary_path())?;
    Ok(writer)
}

fn range_request(start: u64, end: u64, pool_types: &[i32]) -> lightwalletd::BlockRange {
    lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: start,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: end,
            hash: Vec::new(),
        }),
        pool_types: pool_types.to_vec(),
    }
}

#[allow(
    deprecated,
    reason = "Regression tests cover legacy GetBlockRangeNullifiers gRPC-Web parity."
)]
async fn collect_native_range(
    client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    rpc: RangeRpc,
    request: &lightwalletd::BlockRange,
) -> Result<Vec<Vec<u8>>> {
    let mut stream = match rpc {
        RangeRpc::BlockRange => client.get_block_range(request.clone()).await?.into_inner(),
        RangeRpc::BlockRangeNullifiers => client
            .get_block_range_nullifiers(request.clone())
            .await?
            .into_inner(),
    };
    let mut encoded = Vec::new();
    while let Some(block) = stream.message().await? {
        encoded.push(block.encode_to_vec());
    }
    Ok(encoded)
}

fn assert_decoded_chain(encoded: &[Vec<u8>], first: u64, last: u64) -> Result<()> {
    let expected_heights: Vec<u64> = if first <= last {
        (first..=last).collect()
    } else {
        (last..=first).rev().collect()
    };
    ensure!(encoded.len() == expected_heights.len());
    let descending = first > last;
    let mut prior: Option<CompactBlock> = None;
    for (bytes, expected_height) in encoded.iter().zip(expected_heights) {
        let block = CompactBlock::decode(bytes.as_slice())?;
        assert_eq!(block.height, expected_height);
        if let Some(prev) = prior {
            if descending {
                assert_eq!(prev.prev_hash, block.hash);
            } else {
                assert_eq!(block.prev_hash, prev.hash);
            }
        }
        prior = Some(block);
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct WebTarget {
    addr: SocketAddr,
    expect_cors: bool,
}

struct DualEndpoint {
    addr: SocketAddr,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
    _store: WalletServingStoreFixture,
}

impl DualEndpoint {
    async fn start() -> Result<Self> {
        let mut paths =
            build_boundary_writer(RawBlobRetention::None)?.into_serving_store_fixture()?;
        let (mut canonical, wallet) = paths.take_readers()?;
        canonical.try_catch_up()?;
        let reader = Arc::new(RocksDbCanonicalReader::new(canonical)?);
        let pair = WalletServingReadPair::new(reader, Arc::new(RocksDbWalletReader::new(wallet)))?;
        let query =
            WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(pair)));
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let task = tokio::spawn(
            Server::builder()
                .accept_http1(true)
                .layer(GrpcWebLayer::new())
                .add_service(LightwalletdGrpcAdapter::new(query).into_server())
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        Ok(Self {
            addr,
            task,
            _store: paths,
        })
    }

    async fn native_client(&self) -> Result<CompactTxStreamerClient<Channel>> {
        Ok(CompactTxStreamerClient::connect(format!("http://{}", self.addr)).await?)
    }

    const fn target(&self) -> WebTarget {
        WebTarget {
            addr: self.addr,
            expect_cors: false,
        }
    }
}

impl Drop for DualEndpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn assert_web_parity(
    target: WebTarget,
    http: &Client,
    native: &mut CompactTxStreamerClient<Channel>,
    rpc: RangeRpc,
    request: &lightwalletd::BlockRange,
) -> Result<Vec<Vec<u8>>> {
    let start = request.start.as_ref().map_or(0, |id| id.height);
    let end = request.end.as_ref().map_or(0, |id| id.height);
    let expected = collect_native_range(native, rpc, request).await?;
    assert_decoded_chain(&expected, start, end)?;
    for encoding in [CONTENT_TYPE, TEXT_CONTENT_TYPE] {
        let response = post_web_request(http, target.addr, rpc.method(), encoding, request).await?;
        if target.expect_cors {
            assert_eq!(
                response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
                ORIGIN
            );
        }
        let actual = read_web_response(response, encoding, Code::Ok as i32).await?;
        assert_eq!(
            actual,
            expected,
            "{} {encoding} {start}..={end}",
            rpc.method()
        );
    }
    Ok(expected)
}

async fn assert_web_error_status(
    target: WebTarget,
    http: &Client,
    rpc: RangeRpc,
    request: &lightwalletd::BlockRange,
    expected_code: Code,
) -> Result<()> {
    for encoding in [CONTENT_TYPE, TEXT_CONTENT_TYPE] {
        let response = post_web_request(http, target.addr, rpc.method(), encoding, request).await?;
        if target.expect_cors {
            assert_eq!(
                response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
                ORIGIN
            );
        }
        let messages = read_web_response(response, encoding, expected_code as i32).await?;
        assert_eq!(messages, Vec::<Vec<u8>>::new());
    }
    Ok(())
}

#[allow(
    deprecated,
    reason = "Tests terminal statuses on both GetBlockRange and GetBlockRangeNullifiers."
)]
async fn assert_terminal_error_statuses(
    target: WebTarget,
    http: &Client,
    native: &mut CompactTxStreamerClient<Channel>,
) -> Result<()> {
    let past_tip = range_request(4_096, u64::from(TOTAL_BLOCKS) + 1, &[]);
    let unretained_zero = range_request(0, 4_097, &[]);
    let missing_start = lightwalletd::BlockRange {
        start: None,
        end: Some(lightwalletd::BlockId {
            height: 4_097,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    };
    for (req, expected_code) in [
        (&past_tip, Code::OutOfRange),
        (&unretained_zero, Code::NotFound),
        (&missing_start, Code::InvalidArgument),
    ] {
        let range_err = native
            .get_block_range(req.clone())
            .await
            .err()
            .ok_or_else(|| eyre!("expected GetBlockRange {expected_code:?}"))?;
        assert_eq!(range_err.code(), expected_code);
        let nullifier_err = native
            .get_block_range_nullifiers(req.clone())
            .await
            .err()
            .ok_or_else(|| eyre!("expected GetBlockRangeNullifiers {expected_code:?}"))?;
        assert_eq!(nullifier_err.code(), expected_code);
        assert_web_error_status(target, http, RangeRpc::BlockRange, req, expected_code).await?;
        assert_web_error_status(
            target,
            http,
            RangeRpc::BlockRangeNullifiers,
            req,
            expected_code,
        )
        .await?;
    }
    Ok(())
}

fn assert_boundary_payloads_differ(left: &[Vec<u8>], right: &[Vec<u8>], label: &str) -> Result<()> {
    ensure!(left.len() == 6 && right.len() == 6);
    assert_ne!(
        left[1..=2],
        right[1..=2],
        "{label} on sealed side 4095..=4096"
    );
    assert_ne!(
        left[3..=4],
        right[3..=4],
        "{label} on live-tail side 4097..=4098"
    );
    Ok(())
}

async fn assert_pool_subset_parity(
    target: WebTarget,
    http: &Client,
    native: &mut CompactTxStreamerClient<Channel>,
    range_asc: &[Vec<u8>],
    nf_asc: &[Vec<u8>],
) -> Result<()> {
    for pool in [PoolType::Sapling as i32, PoolType::Orchard as i32] {
        let asc = range_request(4_094, 4_099, &[pool]);
        let desc = range_request(4_099, 4_094, &[pool]);
        let pool_range =
            assert_web_parity(target, http, native, RangeRpc::BlockRange, &asc).await?;
        assert_web_parity(target, http, native, RangeRpc::BlockRange, &desc).await?;
        assert_boundary_payloads_differ(&pool_range, range_asc, "pool subset range vs default")?;
        let pool_nf =
            assert_web_parity(target, http, native, RangeRpc::BlockRangeNullifiers, &asc).await?;
        assert_web_parity(target, http, native, RangeRpc::BlockRangeNullifiers, &desc).await?;
        assert_boundary_payloads_differ(&pool_nf, nf_asc, "pool subset nullifiers vs default")?;
    }
    Ok(())
}

#[tokio::test]
async fn browser_wallet_streams_canonical_ranges_and_nullifiers_across_sealed_boundary()
-> Result<()> {
    let endpoint = DualEndpoint::start().await?;
    let target = endpoint.target();
    let http = Client::builder().http1_only().build()?;
    let mut native = endpoint.native_client().await?;

    // Multi-frame crossing (>8 KiB opening batch) across 4096 -> 4097.
    let wide = range_request(3_950, 4_105, &[]);
    let wide_blocks =
        assert_web_parity(target, &http, &mut native, RangeRpc::BlockRange, &wide).await?;
    let wide_nullifiers = assert_web_parity(
        target,
        &http,
        &mut native,
        RangeRpc::BlockRangeNullifiers,
        &wide,
    )
    .await?;
    assert!(
        wide_blocks.iter().map(Vec::len).sum::<usize>() > 8 * 1024
            && wide_nullifiers.iter().map(Vec::len).sum::<usize>() > 8 * 1024
    );

    let default_asc = range_request(4_094, 4_099, &[]);
    let default_desc = range_request(4_099, 4_094, &[]);
    let range_asc = assert_web_parity(
        target,
        &http,
        &mut native,
        RangeRpc::BlockRange,
        &default_asc,
    )
    .await?;
    assert_web_parity(
        target,
        &http,
        &mut native,
        RangeRpc::BlockRange,
        &default_desc,
    )
    .await?;
    let nf_asc = assert_web_parity(
        target,
        &http,
        &mut native,
        RangeRpc::BlockRangeNullifiers,
        &default_asc,
    )
    .await?;
    assert_web_parity(
        target,
        &http,
        &mut native,
        RangeRpc::BlockRangeNullifiers,
        &default_desc,
    )
    .await?;
    assert_boundary_payloads_differ(&range_asc, &nf_asc, "range vs nullifiers")?;

    assert_pool_subset_parity(target, &http, &mut native, &range_asc, &nf_asc).await?;
    assert_terminal_error_statuses(target, &http, &mut native).await
}

#[tokio::test]
async fn compact_blocks_browser_listener_streams_canonical_boundary_and_rejects_nullifiers()
-> Result<()> {
    let writer = build_boundary_writer(RawBlobRetention::None)?;
    let runtime = CompatProcess::start_with_writer(
        "compact-blocks",
        Some(ORIGIN),
        writer,
        RawBlobRetention::None,
    )
    .await?;
    let target = WebTarget {
        addr: runtime.addr,
        expect_cors: true,
    };
    let http = Client::builder().http1_only().build()?;
    let mut native = runtime.native_client().await?;

    let sap = PoolType::Sapling as i32;
    for req in [
        range_request(3_950, 4_105, &[]),
        range_request(4_094, 4_099, &[]),
        range_request(4_094, 4_099, &[sap]),
        range_request(4_099, 4_094, &[]),
    ] {
        assert_web_parity(target, &http, &mut native, RangeRpc::BlockRange, &req).await?;
    }
    assert_web_error_status(
        target,
        &http,
        RangeRpc::BlockRangeNullifiers,
        &range_request(4_094, 4_099, &[]),
        Code::Unimplemented,
    )
    .await
}

fn corrupt_live_tail_block(primary: &Path, height: BlockHeight) -> Result<()> {
    let options = rust_rocksdb::Options::default();
    let families = rust_rocksdb::DB::list_cf(&options, primary)?;
    let db = rust_rocksdb::DB::open_cf(&options, primary, &families)?;
    let family = db
        .cf_handle("live_tail_block")
        .ok_or_else(|| eyre!("canonical primary has no live_tail_block family"))?;
    let key = encode_height_key_ascending(height);
    db.put_cf(&family, key, b"corrupt-live-tail")?;
    db.flush_cf(&family)?;
    Ok(())
}

fn in_process_web_request(
    rpc: RangeRpc,
    encoding: &str,
    range: &lightwalletd::BlockRange,
) -> Result<Request<Body>> {
    let mut wire = vec![0];
    wire.extend_from_slice(&u32::try_from(range.encoded_len())?.to_be_bytes());
    range.encode(&mut wire)?;
    if encoding == TEXT_CONTENT_TYPE {
        wire = STANDARD.encode(wire).into_bytes();
    }
    Ok(Request::post(format!("/{SERVICE_NAME}/{}", rpc.method()))
        .header(http_header::CONTENT_TYPE, encoding)
        .header(http_header::ACCEPT, encoding)
        .body(Body::new(Full::new(Bytes::from(wire))))?)
}

#[tokio::test]
async fn browser_canonical_streams_report_mid_stream_data_loss_at_live_tail_boundary() -> Result<()>
{
    let writer = build_boundary_writer(RawBlobRetention::None)?;
    let primary = writer.canonical_primary_path().to_path_buf();
    let mut paths = writer.into_serving_store_fixture()?;
    corrupt_live_tail_block(&primary, BlockHeight::new(4_097))?;

    let (mut canonical, wallet) = paths.take_readers()?;
    canonical.try_catch_up()?;
    let reader = Arc::new(RocksDbCanonicalReader::new(canonical)?);
    let pair = WalletServingReadPair::new(reader, Arc::new(RocksDbWalletReader::new(wallet)))?;
    let query =
        WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(pair)));
    let server = ServiceBuilder::new()
        .layer(GrpcWebLayer::new())
        .service(LightwalletdGrpcAdapter::new(query).into_server());

    let request = range_request(3_097, 4_099, &[]);
    for rpc in [RangeRpc::BlockRange, RangeRpc::BlockRangeNullifiers] {
        for encoding in [CONTENT_TYPE, TEXT_CONTENT_TYPE] {
            let response = server
                .clone()
                .oneshot(in_process_web_request(rpc, encoding, &request)?)
                .await?;
            ensure!(
                response.status().is_success(),
                "HTTP status {}",
                response.status()
            );
            ensure!(
                response.headers()[http_header::CONTENT_TYPE] == encoding,
                "unexpected content type"
            );
            let header_status = response
                .headers()
                .get("grpc-status")
                .map(|code| code.to_str()?.parse::<i32>().map_err(eyre::Report::from))
                .transpose()?;
            let body = response.into_body().collect().await?.to_bytes();
            let messages =
                super::decode_web_frames(&body, encoding, header_status, Code::DataLoss as i32)?;
            assert_decoded_chain(&messages, 3_097, 4_096)?;
        }
    }
    Ok(())
}
