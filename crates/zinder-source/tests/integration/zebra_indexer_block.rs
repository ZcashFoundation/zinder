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
    response: BlockAndHash,
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
        Ok(Response::new(self.response.clone()))
    }
}

struct RunningBlockSource {
    source: ZebraIndexerBlockSource,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    max_active_requests: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

async fn start_block_source(
    response: BlockAndHash,
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
    response: BlockAndHash,
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
        response,
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
        response,
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
        response,
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
    let running = start_block_source(response, Duration::from_millis(40), request_limit).await?;

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
