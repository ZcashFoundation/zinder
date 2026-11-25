#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
    num::NonZeroU32,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use eyre::Result;
use futures_util::{Stream, future::join_all};
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};
use zinder_core::{
    BlockHeight, BlockId, ConsensusBranchId, Network, NetworkUpgradeActivation,
    NetworkUpgradeActivations, wire::encode_rpc_block_hash_hex,
};
use zinder_proto::external::zebra_indexer_rpc::{
    BlockAndHash, BlockHashAndHeight, BlockRequest, Empty, MempoolChangeMessage,
    NonFinalizedStateChangeRequest,
    indexer_server::{Indexer, IndexerServer},
};
use zinder_source::{
    NodeAuth, NodeSource, ZebraIndexerBlockSource, ZebraIndexerBlockSourceOptions,
    ZebraIndexerSourceTarget, ZebraJsonRpcSource, decode_rpc_block_hash,
};
use zinder_testkit::{JsonRpcTestServer, RpcReply, method};

type ChainTipStream =
    Pin<Box<dyn Stream<Item = Result<BlockHashAndHeight, Status>> + Send + 'static>>;
type BlockStream = Pin<Box<dyn Stream<Item = Result<BlockAndHash, Status>> + Send + 'static>>;
type MempoolStream =
    Pin<Box<dyn Stream<Item = Result<MempoolChangeMessage, Status>> + Send + 'static>>;

#[derive(Clone)]
struct BlockService {
    response: Result<BlockAndHash, Status>,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    response_delay: Duration,
    active_requests: Arc<AtomicU64>,
    max_active_requests: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl Indexer for BlockService {
    type ChainTipChangeStream = ChainTipStream;
    type NonFinalizedStateChangeStream = BlockStream;
    type MempoolChangeStream = MempoolStream;

    async fn chain_tip_change(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ChainTipChangeStream>, Status> {
        Err(Status::unimplemented("test server exposes only GetBlock"))
    }

    async fn non_finalized_state_change(
        &self,
        _request: Request<NonFinalizedStateChangeRequest>,
    ) -> Result<Response<Self::NonFinalizedStateChangeStream>, Status> {
        Err(Status::unimplemented("test server exposes only GetBlock"))
    }

    async fn mempool_change(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::MempoolChangeStream>, Status> {
        Err(Status::unimplemented("test server exposes only GetBlock"))
    }

    async fn get_block(
        &self,
        request: Request<BlockRequest>,
    ) -> Result<Response<BlockAndHash>, Status> {
        let active = self
            .active_requests
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.max_active_requests
            .fetch_max(active, Ordering::Relaxed);
        tokio::time::sleep(self.response_delay).await;
        self.requests
            .lock()
            .push(request.into_inner().hash_or_height);
        self.active_requests.fetch_sub(1, Ordering::Relaxed);
        self.response.clone().map(Response::new)
    }
}

struct RunningBlockSource {
    source: ZebraIndexerBlockSource,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    max_active_requests: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

async fn start_block_source(
    response: Result<BlockAndHash, Status>,
    response_delay: Duration,
    max_in_flight_requests: NonZeroU32,
) -> Result<RunningBlockSource> {
    start_block_source_with_control_plane(
        response,
        response_delay,
        max_in_flight_requests,
        "http://127.0.0.1:1",
    )
    .await
}

async fn start_block_source_with_control_plane(
    response: Result<BlockAndHash, Status>,
    response_delay: Duration,
    max_in_flight_requests: NonZeroU32,
    control_plane_url: &str,
) -> Result<RunningBlockSource> {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let active_requests = Arc::new(AtomicU64::new(0));
    let max_active_requests = Arc::new(AtomicU64::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(
        Server::builder()
            .add_service(IndexerServer::new(BlockService {
                response,
                requests: Arc::clone(&requests),
                response_delay,
                active_requests,
                max_active_requests: Arc::clone(&max_active_requests),
            }))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let json_control = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        control_plane_url,
        NodeAuth::None,
        Duration::from_secs(1),
    )?;
    let source = ZebraIndexerBlockSource::connect(
        ZebraIndexerSourceTarget::new(format!("http://{address}")),
        json_control,
        ZebraIndexerBlockSourceOptions {
            max_in_flight_requests,
            ..ZebraIndexerBlockSourceOptions::default()
        },
    )
    .await?;
    Ok(RunningBlockSource {
        source,
        requests,
        max_active_requests,
        server,
    })
}

fn fixture_block() -> Result<(BlockAndHash, zinder_source::SourceBlock)> {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../services/zinder-ingest/tests/fixtures/z3-regtest-block-1.json"
    ))?;
    let raw_block_hex = fixture["raw_block_hex"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("fixture raw block must be a string"))?;
    let raw_block_bytes = hex::decode(raw_block_hex)?;
    let expected_block = zinder_source::SourceBlock::from_raw_block_bytes(
        Network::ZcashRegtest,
        BlockHeight::new(1),
        raw_block_bytes.clone(),
    )?;
    let response_hash = hex::decode(encode_rpc_block_hash_hex(expected_block.hash))?;
    Ok((
        BlockAndHash {
            hash: response_hash,
            data: raw_block_bytes,
        },
        expected_block,
    ))
}

#[tokio::test]
async fn indexer_get_block_returns_raw_block_by_big_endian_height() -> Result<()> {
    let (response, expected_block) = fixture_block()?;
    let running = start_block_source(
        Ok(response),
        Duration::ZERO,
        ZebraIndexerBlockSourceOptions::default().max_in_flight_requests,
    )
    .await?;

    let block = running.source.fetch_block_at(BlockHeight::new(1)).await?;
    running.server.abort();

    assert_eq!(block, expected_block);
    assert_eq!(*running.requests.lock(), vec![1_u32.to_be_bytes().to_vec()]);
    Ok(())
}

#[tokio::test]
async fn indexer_get_block_rejects_hash_that_disagrees_with_raw_header() -> Result<()> {
    let (mut response, _) = fixture_block()?;
    response.hash = vec![0; 32];
    let running = start_block_source(
        Ok(response),
        Duration::ZERO,
        ZebraIndexerBlockSourceOptions::default().max_in_flight_requests,
    )
    .await?;

    let error = running
        .source
        .fetch_block_at(BlockHeight::new(1))
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("mismatched response hash must fail closed"))?;
    running.server.abort();

    assert!(matches!(
        error,
        zinder_source::SourceError::SourceProtocolMismatch { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn indexer_source_reads_a_block_tree_state_by_hash_through_its_control_plane() -> Result<()> {
    let block_hash_hex = "0f".repeat(32);
    let control_plane =
        JsonRpcTestServer::start([method("z_gettreestate").reply(RpcReply::result(json!({
            "height": 100,
            "hash": block_hash_hex,
            "time": 1_774_668_700,
            "sapling": {"commitments": {}},
            "orchard": {"commitments": {}},
        })))])?;
    let (response, _) = fixture_block()?;
    let running = start_block_source_with_control_plane(
        Ok(response),
        Duration::ZERO,
        ZebraIndexerBlockSourceOptions::default().max_in_flight_requests,
        &control_plane.url(),
    )
    .await?;
    let block_id = BlockId::new(
        BlockHeight::new(100),
        decode_rpc_block_hash(&block_hash_hex)?,
    );
    let pre_sapling_activations = NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        vec![
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(1),
                activation_height: BlockHeight::new(200),
                name: "Sapling".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(2),
                activation_height: BlockHeight::new(300),
                name: "NU5".to_owned(),
            },
        ],
    )?;

    let checkpoint = running
        .source
        .fetch_chain_checkpoint_for_block(block_id, &pre_sapling_activations)
        .await?;
    running.server.abort();

    assert_eq!(checkpoint.block_id, block_id);
    assert_eq!(
        control_plane.requests_for("z_gettreestate")?[0].params,
        json!([block_hash_hex])
    );
    Ok(())
}

#[tokio::test]
async fn indexer_get_block_limit_is_shared_across_source_clones() -> Result<()> {
    let (response, _) = fixture_block()?;
    let request_limit = NonZeroU32::new(3).ok_or_else(|| eyre::eyre!("three is nonzero"))?;
    let running =
        start_block_source(Ok(response), Duration::from_millis(40), request_limit).await?;

    let requests = (1..=9).map(|height| {
        let source = running.source.clone();
        async move { source.fetch_block_at(BlockHeight::new(height)).await }
    });
    let outcomes = join_all(requests).await;
    running.server.abort();

    assert!(outcomes.into_iter().all(|outcome| outcome.is_ok()));
    assert_eq!(
        running.max_active_requests.load(Ordering::Relaxed),
        u64::from(request_limit.get())
    );
    Ok(())
}

#[tokio::test]
async fn indexer_get_block_refusal_of_an_unreadable_stored_block_is_a_missing_capability()
-> Result<()> {
    // Zebra 7.1.0 refuses a stored block it cannot reassemble with
    // `Unavailable`, the code a transport failure also carries.
    let refusals = [
        Status::unavailable(
            "failed to read block: block body unavailable at height Height(1): \
             stored transactions are incomplete",
        ),
        Status::unavailable("failed to read block: database is busy"),
    ];
    let mut errors = Vec::with_capacity(refusals.len());
    for refusal in refusals {
        let running = start_block_source(
            Err(refusal),
            Duration::ZERO,
            ZebraIndexerBlockSourceOptions::default().max_in_flight_requests,
        )
        .await?;
        let error = running
            .source
            .fetch_block_at(BlockHeight::new(1))
            .await
            .err()
            .ok_or_else(|| eyre::eyre!("refused block read must fail"))?;
        running.server.abort();
        errors.push(error);
    }

    let [unreadable, busy] = errors.as_slice() else {
        return Err(eyre::eyre!("expected two refusals"));
    };
    assert!(matches!(
        unreadable,
        zinder_source::SourceError::HistoricalBlockBodyUnavailable { height, .. }
            if *height == BlockHeight::new(1)
    ));
    assert!(unreadable.is_node_capability_missing());
    assert!(matches!(
        busy,
        zinder_source::SourceError::NodeUnavailable { .. }
    ));
    Ok(())
}

const SERIALIZED_HEADER_PARENT_OFFSET: usize = 4;
const SERIALIZED_HEADER_PARENT_END: usize = SERIALIZED_HEADER_PARENT_OFFSET + 32;
const SERIALIZED_HEADER_TIME_OFFSET: usize = 100;

#[derive(Clone)]
struct HeightKeyedBlockService {
    blocks: Arc<Mutex<std::collections::BTreeMap<u32, Result<BlockAndHash, Status>>>>,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    response_delay: Duration,
    active_requests: Arc<AtomicU64>,
    max_active_requests: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl Indexer for HeightKeyedBlockService {
    type ChainTipChangeStream = ChainTipStream;
    type NonFinalizedStateChangeStream = BlockStream;
    type MempoolChangeStream = MempoolStream;

    async fn chain_tip_change(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ChainTipChangeStream>, Status> {
        Err(Status::unimplemented("test server exposes only GetBlock"))
    }

    async fn non_finalized_state_change(
        &self,
        _request: Request<NonFinalizedStateChangeRequest>,
    ) -> Result<Response<Self::NonFinalizedStateChangeStream>, Status> {
        Err(Status::unimplemented("test server exposes only GetBlock"))
    }

    async fn mempool_change(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::MempoolChangeStream>, Status> {
        Err(Status::unimplemented("test server exposes only GetBlock"))
    }

    async fn get_block(
        &self,
        request: Request<BlockRequest>,
    ) -> Result<Response<BlockAndHash>, Status> {
        let active = self
            .active_requests
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.max_active_requests
            .fetch_max(active, Ordering::Relaxed);
        tokio::time::sleep(self.response_delay).await;
        let key = request.into_inner().hash_or_height;
        self.requests.lock().push(key.clone());
        let outcome = if key.len() == 4 {
            let bytes = [key[0], key[1], key[2], key[3]];
            let height = u32::from_be_bytes(bytes);
            self.blocks
                .lock()
                .get(&height)
                .cloned()
                .unwrap_or_else(|| Err(Status::not_found("block unavailable")))
        } else {
            Err(Status::invalid_argument("expected 4-byte height key"))
        };
        self.active_requests.fetch_sub(1, Ordering::Relaxed);
        outcome.map(Response::new)
    }
}

fn fixture_chain(count: u32) -> Result<Vec<(BlockAndHash, zinder_source::SourceBlock)>> {
    let (block_one_rpc, block_one) = fixture_block()?;
    let mut chain = vec![(block_one_rpc, block_one.clone())];
    let mut parent_hash = block_one.hash;
    for height_num in 2..=count {
        let mut raw_bytes = block_one.raw_block_bytes.clone();
        raw_bytes
            .get_mut(SERIALIZED_HEADER_PARENT_OFFSET..SERIALIZED_HEADER_PARENT_END)
            .ok_or_else(|| eyre::eyre!("fixture block missing header parent"))?
            .copy_from_slice(&parent_hash.as_bytes());
        let header_time = raw_bytes
            .get_mut(SERIALIZED_HEADER_TIME_OFFSET)
            .ok_or_else(|| eyre::eyre!("fixture block missing time"))?;
        *header_time = header_time.wrapping_add(u8::try_from(height_num)?);
        let source_block = zinder_source::SourceBlock::from_raw_block_bytes(
            Network::ZcashRegtest,
            BlockHeight::new(height_num),
            raw_bytes.clone(),
        )?;
        let hash = hex::decode(encode_rpc_block_hash_hex(source_block.hash))?;
        parent_hash = source_block.hash;
        chain.push((
            BlockAndHash {
                hash,
                data: raw_bytes,
            },
            source_block,
        ));
    }
    Ok(chain)
}

struct MultiBlockIndexerFixture {
    address: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    max_active_requests: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

async fn start_height_keyed_indexer(
    blocks: std::collections::BTreeMap<u32, Result<BlockAndHash, Status>>,
    response_delay: Duration,
) -> Result<MultiBlockIndexerFixture> {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let active_requests = Arc::new(AtomicU64::new(0));
    let max_active_requests = Arc::new(AtomicU64::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(
        Server::builder()
            .add_service(IndexerServer::new(HeightKeyedBlockService {
                blocks: Arc::new(Mutex::new(blocks)),
                requests: Arc::clone(&requests),
                response_delay,
                active_requests,
                max_active_requests: Arc::clone(&max_active_requests),
            }))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    Ok(MultiBlockIndexerFixture {
        address,
        requests,
        max_active_requests,
        server,
    })
}

fn tip_json_for_block(block: &zinder_source::SourceBlock) -> Value {
    json!({
        "height": block.height.value(),
        "hash": block.hash.as_bytes(),
    })
}

fn indexer_target_for(control_plane_url: String, grpc_addr: String) -> zinder_source::NodeTarget {
    zinder_source::NodeTarget::new(
        Network::ZcashRegtest,
        control_plane_url,
        NodeAuth::None,
        Duration::from_secs(5),
        std::num::NonZeroU64::new(64 * 1_024 * 1_024).unwrap_or(std::num::NonZeroU64::MIN),
    )
    .with_indexer_grpc_addr(Some(grpc_addr))
}

fn start_control_plane_for_node_target_test(
    block_one: zinder_source::SourceBlock,
) -> Result<JsonRpcTestServer> {
    JsonRpcTestServer::start([
        method("rpc.discover").reply(RpcReply::result(json!({
            "openrpc": "1.3.2",
            "methods": [
                {"name": "getblock"},
                {"name": "getbestblockheightandhash"},
                {"name": "z_gettreestate"},
                {"name": "getblockchaininfo"},
                {"name": "rpc.discover"},
            ],
        }))),
        method("getblockchaininfo").reply_with(|_| {
            RpcReply::result(json!({
                "blocks": 1,
                "pruned": false,
                "upgrades": {
                    "5437f330": {
                        "name": "NU6.2",
                        "activationheight": 10,
                        "status": "pending"
                    }
                }
            }))
        }),
        method("getbestblockheightandhash")
            .reply_with(move |_| RpcReply::result(tip_json_for_block(&block_one))),
    ])
}

#[tokio::test]
async fn indexer_from_node_target_lazy_defers_connect_and_delegates_control_plane_methods()
-> Result<()> {
    let dead_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let dead_addr = dead_listener.local_addr()?;
    drop(dead_listener);
    let lazy_target = indexer_target_for(
        "http://127.0.0.1:1".to_owned(),
        format!("http://{dead_addr}"),
    );
    let _lazy_source = ZebraIndexerBlockSource::from_node_target_lazy(&lazy_target)?;

    let (block_one_rpc, block_one) = fixture_block()?;
    let mut blocks = std::collections::BTreeMap::new();
    for height in 1..=6 {
        blocks.insert(height, Ok(block_one_rpc.clone()));
    }
    let fixture = start_height_keyed_indexer(blocks, Duration::from_millis(30)).await?;
    let control_plane = start_control_plane_for_node_target_test(block_one)?;

    let limit = NonZeroU32::new(2).ok_or_else(|| eyre::eyre!("two is nonzero"))?;
    let target = indexer_target_for(control_plane.url(), format!("http://{}", fixture.address))
        .with_max_concurrent_requests(limit);
    let source = ZebraIndexerBlockSource::from_node_target_lazy(&target)?;

    let probed = source.probe_capabilities().await?;
    assert!(probed.supports(zinder_source::NodeCapability::BestChainBlocks));
    assert!(probed.supports(zinder_source::NodeCapability::HistoricalBlockBodies));
    assert_eq!(
        source.fetch_storage_mode().await?,
        zinder_source::NodeStorageMode::Archive
    );
    assert_eq!(
        source
            .fetch_network_upgrade_activations()
            .await?
            .activation_height_by_name("NU6.2"),
        Some(BlockHeight::new(10))
    );
    assert_eq!(source.probe_tip_id().await?.height, BlockHeight::new(1));
    let _mempool = zinder_source::MempoolSourceFactory::mempool_source(
        &source,
        zinder_source::MempoolSourceAdmissionLimits::default(),
    );

    let fetches = (1..=6).map(|height| {
        let source = source.clone();
        async move { source.fetch_block_at(BlockHeight::new(height)).await }
    });
    assert!(
        join_all(fetches)
            .await
            .into_iter()
            .all(|outcome| outcome.is_ok())
    );
    fixture.server.abort();
    assert!(fixture.max_active_requests.load(Ordering::Relaxed) <= u64::from(limit.get()));
    Ok(())
}

fn known_tip_limits(
    cursor: zinder_source::SourceChainCursor,
    max_connected_blocks: NonZeroU32,
    known_tip_height: BlockHeight,
    reorg_window_blocks: u32,
) -> zinder_source::SourceChainSegmentLimits {
    zinder_source::SourceChainSegmentLimits::new(
        cursor,
        max_connected_blocks,
        64 * 1_024 * 1_024,
        64 * 1_024 * 1_024,
    )
    .with_known_tip(zinder_source::SourceKnownTip::new(
        known_tip_height,
        reorg_window_blocks,
    ))
}

#[tokio::test]
async fn indexer_fetch_chain_segment_skips_tip_poll_below_settled_window_only() -> Result<()> {
    let chain = fixture_chain(8)?;
    let tip_block = chain
        .last()
        .ok_or_else(|| eyre::eyre!("expected 8 blocks"))?
        .1
        .clone();
    let tip_for_rpc = tip_block.clone();
    let blocks = chain
        .iter()
        .map(|(rpc_block, block)| (block.height.value(), Ok(rpc_block.clone())))
        .collect::<std::collections::BTreeMap<_, _>>();
    let fixture = start_height_keyed_indexer(blocks, Duration::ZERO).await?;
    let control_plane = JsonRpcTestServer::start([method("getbestblockheightandhash")
        .reply_with(move |_| RpcReply::result(tip_json_for_block(&tip_for_rpc)))])?;
    let target = indexer_target_for(control_plane.url(), format!("http://{}", fixture.address));
    let source = ZebraIndexerBlockSource::from_node_target_lazy(&target)?;
    let max_three = NonZeroU32::new(3).ok_or_else(|| eyre::eyre!("three is nonzero"))?;

    // Strictly below window: tip 8 - reorg window 2 = height 6, settled <= 5.
    let settled_segment = source
        .fetch_chain_segment(known_tip_limits(
            zinder_source::SourceChainCursor::before_first_block(),
            max_three,
            tip_block.height,
            2,
        ))
        .await?;
    assert_eq!(settled_segment.into_updates().len(), 3);
    assert!(
        control_plane
            .requests_for("getbestblockheightandhash")?
            .is_empty(),
        "settled segment strictly below reorg window must skip tip_id() poll"
    );

    // Entering window (heights 4..=6 touches window boundary 6): must poll tip_id().
    let block_three_id = BlockId::new(chain[2].1.height, chain[2].1.hash);
    let boundary_segment = source
        .fetch_chain_segment(known_tip_limits(
            zinder_source::SourceChainCursor::at_block(block_three_id),
            max_three,
            tip_block.height,
            2,
        ))
        .await?;
    assert_eq!(boundary_segment.into_updates().len(), 3);
    assert_eq!(
        control_plane
            .requests_for("getbestblockheightandhash")?
            .len(),
        1
    );

    // Short chain (`tip_height <= reorg_window`): must poll tip_id().
    let _short_segment = source
        .fetch_chain_segment(known_tip_limits(
            zinder_source::SourceChainCursor::before_first_block(),
            max_three,
            BlockHeight::new(2),
            2,
        ))
        .await?;
    fixture.server.abort();
    assert_eq!(
        control_plane
            .requests_for("getbestblockheightandhash")?
            .len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn indexer_settled_segment_observes_tip_on_missing_block_and_parent_mismatch() -> Result<()> {
    let chain = fixture_chain(4)?;
    let tip_four = chain[3].1.clone();
    let tip_four_for_rpc = tip_four.clone();
    let mut blocks = chain
        .iter()
        .map(|(rpc_block, block)| (block.height.value(), Ok(rpc_block.clone())))
        .collect::<std::collections::BTreeMap<_, _>>();
    let fixture = start_height_keyed_indexer(blocks.clone(), Duration::ZERO).await?;
    let control_plane = JsonRpcTestServer::start([method("getbestblockheightandhash")
        .reply_with(move |_| RpcReply::result(tip_json_for_block(&tip_four_for_rpc)))])?;
    let target = indexer_target_for(control_plane.url(), format!("http://{}", fixture.address));
    let source = ZebraIndexerBlockSource::from_node_target_lazy(&target)?;
    let max_three = NonZeroU32::new(3).ok_or_else(|| eyre::eyre!("three is nonzero"))?;
    let max_two = NonZeroU32::new(2).ok_or_else(|| eyre::eyre!("two is nonzero"))?;

    // Stale known_tip (10) causes settled range 3..=5, but height 5 is unavailable at tip 4.
    let block_two_id = BlockId::new(chain[1].1.height, chain[1].1.hash);
    let updates = source
        .fetch_chain_segment(known_tip_limits(
            zinder_source::SourceChainCursor::at_block(block_two_id),
            max_three,
            BlockHeight::new(10),
            2,
        ))
        .await?
        .into_updates();
    fixture.server.abort();
    assert_eq!(updates.len(), 2);
    assert_eq!(
        control_plane
            .requests_for("getbestblockheightandhash")?
            .len(),
        1
    );

    // Parent hash mismatch in settled range must observe tip and emit RevertedBlock.
    blocks.insert(2, Ok(chain[0].0.clone()));
    let mismatch_fixture = start_height_keyed_indexer(blocks, Duration::ZERO).await?;
    let mismatch_target = indexer_target_for(
        control_plane.url(),
        format!("http://{}", mismatch_fixture.address),
    );
    let mismatch_source = ZebraIndexerBlockSource::from_node_target_lazy(&mismatch_target)?;
    let block_one_id = BlockId::new(chain[0].1.height, chain[0].1.hash);
    let mismatch_updates = mismatch_source
        .fetch_chain_segment(known_tip_limits(
            zinder_source::SourceChainCursor::at_block(block_one_id),
            max_two,
            BlockHeight::new(10),
            2,
        ))
        .await?
        .into_updates();
    mismatch_fixture.server.abort();
    assert_eq!(
        mismatch_updates,
        vec![zinder_source::SourceChainUpdate::reverted_block(
            block_one_id
        )]
    );
    assert_eq!(
        control_plane
            .requests_for("getbestblockheightandhash")?
            .len(),
        2
    );
    let _unused = fixture.requests.lock().len();
    Ok(())
}
