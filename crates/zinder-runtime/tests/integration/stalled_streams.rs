//! Responses whose client stops reading them, through the public listener.
//!
//! The stalled client is a tonic channel with a small stream window and a
//! large connection window. Its connection task answers the server's pings,
//! and it sends no `WINDOW_UPDATE` for a stream it does not read, so the
//! server's transport holds data of each unread response.

use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use bytes::Bytes;
use eyre::{Result, bail, eyre};
use futures_util::future::try_join_all;
use http::uri::PathAndQuery;
use http_body::Frame;
use http_body_util::StreamBody;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tonic::{
    Code, Request, Status,
    body::Body,
    server::NamedService,
    transport::{Channel, Endpoint},
};
use tonic_health::{
    ServingStatus,
    pb::{
        HealthCheckRequest, HealthCheckResponse,
        health_check_response::ServingStatus as WireServingStatus, health_client::HealthClient,
    },
    server::HealthReporter,
};
use tonic_prost::ProstCodec;
use tower::Service;
use zinder_proto::compat::lightwalletd::{
    Address, AddressList, Balance, BlockId, BlockRange, ChainSpec, CompactBlock, CompactTx,
    Duration as PingDuration, Empty, EncodedBlock, EncodedBlockRange, EncodedBlockRangeStream,
    GetAddressUtxosArg, GetAddressUtxosReply, GetAddressUtxosReplyList, GetMempoolTxRequest,
    GetSubtreeRootsArg, LightdInfo, PingResponse, RawTransaction, SendResponse, StandardFee,
    SubtreeRoot, TransparentAddressBlockFilter, TreeState, TxFilter,
    compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::{CompactTxStreamer, CompactTxStreamerServer},
};
use zinder_runtime::{
    ClientAddressSource, GrpcReceiveLimitLayer, GrpcReceiveLimits, GrpcSendStallLayer,
    MAX_CONCURRENT_STREAMS_PER_CONNECTION, public_grpc_server_with,
};

/// Send stall timeout of every test server. It leaves the test time to fill
/// a connection and check that nothing was freed before it.
const STALL_TIMEOUT: Duration = Duration::from_secs(2);
/// Bound on each wait; every expected outcome arrives well within it.
const WAIT_LIMIT: Duration = Duration::from_secs(15);
/// Time a server that cannot reset a stream is given to show it keeps the
/// stream's slot.
const CONTROL_WAIT: Duration = Duration::from_secs(5);
const STREAM_WINDOW: u32 = 96 * 1024;
const CONNECTION_WINDOW: u32 = 64 * 1024 * 1024;
/// Bytes of each message a producing stream sends. One message is larger
/// than the stream window, so the transport holds part of it.
const MESSAGE_PAYLOAD: usize = 256 * 1024;
/// Bytes of the one message a paused or finished stream sends: just over the
/// stream window, so the transport takes it and part of it waits for window.
const LAST_MESSAGE_PAYLOAD: usize = 100 * 1024;
const SERVICE: &str = "zinder.test.v1.StalledStreams";

/// How a stalled response's producer behaves.
#[derive(Clone, Copy, Debug)]
enum Producer {
    /// Sends messages for as long as the transport takes them.
    Producing,
    /// Sends one message, then has nothing more to send.
    Paused,
    /// Sends one message, then ends with `OK`.
    Finished,
}

impl Producer {
    const ALL: [Self; 3] = [Self::Producing, Self::Paused, Self::Finished];

    fn path(self) -> PathAndQuery {
        match self {
            Self::Producing => {
                PathAndQuery::from_static("/zinder.test.v1.StalledStreams/Producing")
            }
            Self::Paused => PathAndQuery::from_static("/zinder.test.v1.StalledStreams/Paused"),
            Self::Finished => PathAndQuery::from_static("/zinder.test.v1.StalledStreams/Finished"),
        }
    }

    fn for_path(path: &str) -> Self {
        match path.rsplit('/').next() {
            Some("Paused") => Self::Paused,
            Some("Finished") => Self::Finished,
            _ => Self::Producing,
        }
    }
}

/// Server-streaming methods whose messages count when they are produced and
/// when their storage is freed.
#[derive(Clone, Default)]
struct StalledStreams {
    produced: Arc<AtomicUsize>,
    freed: Arc<AtomicUsize>,
}

impl StalledStreams {
    /// Whether every message produced so far has been freed, and at least
    /// `expected` were produced.
    fn all_freed(&self, expected: usize) -> bool {
        let produced = self.produced.load(Ordering::SeqCst);
        produced >= expected && self.freed.load(Ordering::SeqCst) == produced
    }

    fn messages(&self, kind: Producer) -> Body {
        let produced = Arc::clone(&self.produced);
        let freed = Arc::clone(&self.freed);
        let (count, payload) = match kind {
            Producer::Producing => (usize::MAX, MESSAGE_PAYLOAD),
            Producer::Paused | Producer::Finished => (1, LAST_MESSAGE_PAYLOAD),
        };
        let messages = tokio_stream::iter(0..count).map(move |_| {
            produced.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Status>(Frame::data(tracked_message(&freed, payload)))
        });
        match kind {
            Producer::Finished => {
                let mut trailers = http::HeaderMap::new();
                trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                Body::new(StreamBody::new(
                    messages.chain(tokio_stream::once(Ok(Frame::trailers(trailers)))),
                ))
            }
            Producer::Producing | Producer::Paused => {
                Body::new(StreamBody::new(messages.chain(tokio_stream::pending())))
            }
        }
    }
}

impl NamedService for StalledStreams {
    const NAME: &'static str = SERVICE;
}

impl Service<http::Request<Body>> for StalledStreams {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = std::future::Ready<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let producer = Producer::for_path(request.uri().path());
        let mut response = http::Response::new(self.messages(producer));
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/grpc"),
        );
        std::future::ready(Ok(response))
    }
}

/// A gRPC message storage that counts itself into `freed` when dropped.
struct TrackedMessage {
    frame: Vec<u8>,
    freed: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for TrackedMessage {
    fn as_ref(&self) -> &[u8] {
        &self.frame
    }
}

impl Drop for TrackedMessage {
    fn drop(&mut self) {
        self.freed.fetch_add(1, Ordering::SeqCst);
    }
}

/// One framed gRPC message of `payload` bytes, in storage of its own.
fn tracked_message(freed: &Arc<AtomicUsize>, payload: usize) -> Bytes {
    let encoded = prost::Message::encode_to_vec(&vec![0_u8; payload]);
    let length = u32::try_from(encoded.len()).unwrap_or(u32::MAX);
    let mut frame = Vec::with_capacity(5 + encoded.len());
    frame.push(0);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&encoded);
    Bytes::from_owner(TrackedMessage {
        frame,
        freed: Arc::clone(freed),
    })
}

/// Which connection loop serves the test listener.
#[derive(Clone, Copy)]
enum ConnectionLoop {
    /// Zinder's public listener, which resets a stalled stream.
    Zinder,
    /// tonic's own server with the same layers, which cannot.
    Tonic,
}

struct TestServer {
    address: SocketAddr,
    health: HealthReporter,
    stalled: StalledStreams,
    _server: JoinHandle<std::io::Result<()>>,
}

async fn serve(connection_loop: ConnectionLoop) -> Result<TestServer> {
    let stalled = StalledStreams::default();
    let (health, health_service) = tonic_health::server::health_reporter();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let slots = NonZeroUsize::new(512).ok_or_else(|| eyre!("slots must be non-zero"))?;
    let limits = GrpcReceiveLimits::new(slots, Duration::from_secs(30));
    let service = stalled.clone();
    let server = match connection_loop {
        ConnectionLoop::Zinder => tokio::spawn(
            public_grpc_server_with(limits, STALL_TIMEOUT, ClientAddressSource::Connection)
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming(listener),
        ),
        ConnectionLoop::Tonic => tokio::spawn(async move {
            tonic::transport::Server::builder()
                .max_concurrent_streams(MAX_CONCURRENT_STREAMS_PER_CONNECTION)
                .layer(GrpcReceiveLimitLayer::new(
                    limits,
                    ClientAddressSource::Connection,
                ))
                .layer(GrpcSendStallLayer::new(STALL_TIMEOUT))
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .map_err(std::io::Error::other)
        }),
    };
    Ok(TestServer {
        address,
        health,
        stalled,
        _server: server,
    })
}

/// One HTTP/2 connection that withholds window from streams it does not read.
async fn withholding_client(address: SocketAddr) -> Result<Channel> {
    Ok(Endpoint::new(format!("http://{address}"))?
        .initial_stream_window_size(Some(STREAM_WINDOW))
        .initial_connection_window_size(Some(CONNECTION_WINDOW))
        .connect()
        .await?)
}

async fn open_stalled_stream(
    channel: Channel,
    producer: Producer,
) -> Result<tonic::Streaming<Vec<u8>>> {
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await?;
    let response = grpc
        .server_streaming(
            Request::new(()),
            producer.path(),
            ProstCodec::<(), Vec<u8>>::default(),
        )
        .await?;
    Ok(response.into_inner())
}

async fn watch(channel: &Channel) -> Result<tonic::Streaming<HealthCheckResponse>> {
    Ok(HealthClient::new(channel.clone())
        .watch(HealthCheckRequest::default())
        .await?
        .into_inner())
}

/// Fills every stream slot of one connection.
///
/// One slot holds a health `Watch` stream the client reads, after its first
/// status. The others hold stalled streams of every kind, opened together.
async fn fill_with_stalled_streams(
    channel: &Channel,
) -> Result<(
    Vec<tonic::Streaming<Vec<u8>>>,
    tonic::Streaming<HealthCheckResponse>,
)> {
    let mut sibling = watch(channel).await?;
    let first = within_wait_limit(sibling.next())
        .await?
        .ok_or_else(|| eyre!("the watch stream ended"))??;
    assert_eq!(first.status(), WireServingStatus::Serving);
    let stalled_count = usize::try_from(MAX_CONCURRENT_STREAMS_PER_CONNECTION)? - 1;
    let stalled = within_wait_limit(try_join_all(
        Producer::ALL
            .into_iter()
            .cycle()
            .take(stalled_count)
            .map(|producer| open_stalled_stream(channel.clone(), producer)),
    ))
    .await??;
    Ok((stalled, sibling))
}

/// A health check that needs a free stream slot on `channel`.
fn queued_check(channel: &Channel) -> JoinHandle<Result<(), Status>> {
    let mut client = HealthClient::new(channel.clone());
    tokio::spawn(async move {
        client
            .check(HealthCheckRequest::default())
            .await
            .map(|_| ())
    })
}

async fn within_wait_limit<T>(future: impl Future<Output = T>) -> Result<T> {
    tokio::time::timeout(WAIT_LIMIT, future)
        .await
        .map_err(|_| eyre!("no outcome within {WAIT_LIMIT:?}"))
}

async fn wait_until(what: &str, mut probe: impl FnMut() -> bool) -> Result<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    while !probe() {
        if Instant::now() >= deadline {
            bail!("{what} did not happen within {WAIT_LIMIT:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

#[tokio::test]
async fn stalled_streams_free_every_slot_while_the_connection_and_a_sibling_stay_open() -> Result<()>
{
    let server = serve(ConnectionLoop::Zinder).await?;
    let channel = withholding_client(server.address).await?;
    let opened = Instant::now();
    let (mut stalled, mut sibling) = fill_with_stalled_streams(&channel).await?;
    let check = queued_check(&channel);

    // Once every stalled stream has produced its message, their messages
    // are still held, unless filling the connection was so slow that the
    // first streams may already have reached their deadline. That the slots
    // stay taken without a reset is the tonic control test's to show; here
    // the check below completes only once the resets free one.
    wait_until("every stalled stream has produced its message", || {
        server.stalled.produced.load(Ordering::SeqCst) >= stalled.len()
    })
    .await?;
    if opened.elapsed() < STALL_TIMEOUT / 2 {
        assert!(
            !server.stalled.all_freed(stalled.len()),
            "messages were freed early"
        );
    }

    // Without the client reading any stalled stream, the server frees what
    // each holds and its slot.
    wait_until("every stalled stream's messages are freed", || {
        server.stalled.all_freed(stalled.len())
    })
    .await?;
    within_wait_limit(check).await???;
    // Every freed slot can hold a stream again, at the same time.
    let watches =
        within_wait_limit(try_join_all((0..stalled.len()).map(|_| watch(&channel)))).await??;
    assert_eq!(watches.len(), stalled.len());

    // The connection and the sibling stream stayed open.
    server
        .health
        .set_service_status("", ServingStatus::NotServing)
        .await;
    let update = within_wait_limit(sibling.next())
        .await?
        .ok_or_else(|| eyre!("the watch stream ended"))??;
    assert_eq!(update.status(), WireServingStatus::NotServing);

    // Reading a stalled stream now delivers what reached the client, then
    // the reset.
    let reset_stream = stalled
        .first_mut()
        .ok_or_else(|| eyre!("no stalled stream"))?;
    let ended_with = loop {
        match within_wait_limit(reset_stream.next()).await? {
            Some(Ok(_)) => {}
            Some(Err(status)) => break Some(status.code()),
            None => break None,
        }
    };
    assert_eq!(ended_with, Some(Code::Cancelled));
    Ok(())
}

#[tokio::test]
async fn tonic_s_own_server_keeps_a_stalled_stream_s_slot() -> Result<()> {
    let server = serve(ConnectionLoop::Tonic).await?;
    let channel = withholding_client(server.address).await?;
    let (_stalled, _sibling) = fill_with_stalled_streams(&channel).await?;
    let check = queued_check(&channel);

    tokio::time::sleep(CONTROL_WAIT).await;
    assert!(
        !check.is_finished(),
        "a slot was freed without a stream reset"
    );
    check.abort();
    Ok(())
}

/// `GetBlockRange` responses whose stored blocks the transport slices.
///
/// The generated lightwalletd server serves them from encoded blocks whose
/// allocation the producer vouches for, marks the response, and the send
/// stall layer then hands HTTP/2 slices of each block instead of copies. A
/// slice held by a stalled stream keeps its whole block alive, so freeing the
/// blocks shows the reset reached the pieces themselves.
#[derive(Clone, Default)]
struct StalledRanges {
    produced: Arc<AtomicUsize>,
    freed: Arc<AtomicUsize>,
    /// Paused producers that have reached their pause.
    paused: Arc<AtomicUsize>,
    /// Finished producers that have reached their end.
    finished: Arc<AtomicUsize>,
}

impl StalledRanges {
    fn all_freed(&self, expected: usize) -> bool {
        let produced = self.produced.load(Ordering::SeqCst);
        produced >= expected && self.freed.load(Ordering::SeqCst) == produced
    }

    /// Whether every stream produced its first block and every paused and
    /// finished producer reached its pause or end, which happens only once
    /// the transport has pulled each response past its block.
    fn reached_tails(&self, streams: usize) -> bool {
        self.produced.load(Ordering::SeqCst) >= streams
            && self.paused.load(Ordering::SeqCst) == streams / 3
            && self.finished.load(Ordering::SeqCst) == streams / 3
    }
}

type Replies<T> = std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<T, Status>> + Send>>;

fn unimplemented<T>() -> Result<tonic::Response<T>, Status> {
    Err(Status::unimplemented("not served by this test"))
}

#[tonic::async_trait]
impl EncodedBlockRange for StalledRanges {
    async fn get_encoded_block_range(
        &self,
        request: Request<BlockRange>,
    ) -> Result<tonic::Response<EncodedBlockRangeStream>, Status> {
        // The range's start height picks how its producer behaves.
        let kind = match request.into_inner().start.map_or(0, |start| start.height) {
            1 => Producer::Paused,
            2 => Producer::Finished,
            _ => Producer::Producing,
        };
        // A paused or finished stream sends one block just over the stream
        // window, so the transport pulls the response to its pause or end.
        let (count, payload) = match kind {
            Producer::Producing => (usize::MAX, MESSAGE_PAYLOAD),
            Producer::Paused | Producer::Finished => (1, LAST_MESSAGE_PAYLOAD),
        };
        let produced = Arc::clone(&self.produced);
        let freed = Arc::clone(&self.freed);
        let blocks = tokio_stream::iter(0..count).map(move |_| {
            produced.fetch_add(1, Ordering::SeqCst);
            let message = tracked_message(&freed, payload);
            let allocation = message.len();
            EncodedBlock::from_length_prefixed_in(message, allocation)
                .map_err(|_| Status::internal("the test block is not length-prefixed"))
        });
        let paused = Arc::clone(&self.paused);
        let finished = Arc::clone(&self.finished);
        let blocks: EncodedBlockRangeStream = match kind {
            Producer::Producing => Box::pin(blocks.chain(tokio_stream::pending())),
            Producer::Paused => Box::pin(
                blocks.chain(
                    futures_util::stream::once(async move {
                        paused.fetch_add(1, Ordering::SeqCst);
                        std::future::pending::<()>().await;
                    })
                    .filter_map(|()| None),
                ),
            ),
            Producer::Finished => Box::pin(
                blocks.chain(
                    futures_util::stream::once(async move {
                        finished.fetch_add(1, Ordering::SeqCst);
                    })
                    .filter_map(|()| None),
                ),
            ),
        };
        Ok(tonic::Response::new(blocks))
    }
}

#[tonic::async_trait]
impl CompactTxStreamer for StalledRanges {
    type GetBlockRangeStream = Replies<CompactBlock>;
    type GetBlockRangeNullifiersStream = Replies<CompactBlock>;
    type GetTaddressTxidsStream = Replies<RawTransaction>;
    type GetTaddressTransactionsStream = Replies<RawTransaction>;
    type GetMempoolTxStream = Replies<CompactTx>;
    type GetMempoolStreamStream = Replies<RawTransaction>;
    type GetSubtreeRootsStream = Replies<SubtreeRoot>;
    type GetAddressUtxosStreamStream = Replies<GetAddressUtxosReply>;

    fn encoded_block_range(&self) -> Option<&dyn EncodedBlockRange> {
        Some(self)
    }

    async fn get_latest_block(
        &self,
        _: Request<ChainSpec>,
    ) -> Result<tonic::Response<BlockId>, Status> {
        unimplemented()
    }

    async fn get_block(
        &self,
        _: Request<BlockId>,
    ) -> Result<tonic::Response<CompactBlock>, Status> {
        unimplemented()
    }

    async fn get_block_nullifiers(
        &self,
        _: Request<BlockId>,
    ) -> Result<tonic::Response<CompactBlock>, Status> {
        unimplemented()
    }

    async fn get_block_range(
        &self,
        _: Request<BlockRange>,
    ) -> Result<tonic::Response<Self::GetBlockRangeStream>, Status> {
        unimplemented()
    }

    async fn get_block_range_nullifiers(
        &self,
        _: Request<BlockRange>,
    ) -> Result<tonic::Response<Self::GetBlockRangeNullifiersStream>, Status> {
        unimplemented()
    }

    async fn get_transaction(
        &self,
        _: Request<TxFilter>,
    ) -> Result<tonic::Response<RawTransaction>, Status> {
        unimplemented()
    }

    async fn send_transaction(
        &self,
        _: Request<RawTransaction>,
    ) -> Result<tonic::Response<SendResponse>, Status> {
        unimplemented()
    }

    async fn get_taddress_txids(
        &self,
        _: Request<TransparentAddressBlockFilter>,
    ) -> Result<tonic::Response<Self::GetTaddressTxidsStream>, Status> {
        unimplemented()
    }

    async fn get_taddress_transactions(
        &self,
        _: Request<TransparentAddressBlockFilter>,
    ) -> Result<tonic::Response<Self::GetTaddressTransactionsStream>, Status> {
        unimplemented()
    }

    async fn get_taddress_balance(
        &self,
        _: Request<AddressList>,
    ) -> Result<tonic::Response<Balance>, Status> {
        unimplemented()
    }

    async fn get_taddress_balance_stream(
        &self,
        _: Request<tonic::Streaming<Address>>,
    ) -> Result<tonic::Response<Balance>, Status> {
        unimplemented()
    }

    async fn get_mempool_tx(
        &self,
        _: Request<GetMempoolTxRequest>,
    ) -> Result<tonic::Response<Self::GetMempoolTxStream>, Status> {
        unimplemented()
    }

    async fn get_mempool_stream(
        &self,
        _: Request<Empty>,
    ) -> Result<tonic::Response<Self::GetMempoolStreamStream>, Status> {
        unimplemented()
    }

    async fn get_tree_state(
        &self,
        _: Request<BlockId>,
    ) -> Result<tonic::Response<TreeState>, Status> {
        unimplemented()
    }

    async fn get_latest_tree_state(
        &self,
        _: Request<Empty>,
    ) -> Result<tonic::Response<TreeState>, Status> {
        unimplemented()
    }

    async fn get_subtree_roots(
        &self,
        _: Request<GetSubtreeRootsArg>,
    ) -> Result<tonic::Response<Self::GetSubtreeRootsStream>, Status> {
        unimplemented()
    }

    async fn get_address_utxos(
        &self,
        _: Request<GetAddressUtxosArg>,
    ) -> Result<tonic::Response<GetAddressUtxosReplyList>, Status> {
        unimplemented()
    }

    async fn get_address_utxos_stream(
        &self,
        _: Request<GetAddressUtxosArg>,
    ) -> Result<tonic::Response<Self::GetAddressUtxosStreamStream>, Status> {
        unimplemented()
    }

    async fn get_lightd_info(
        &self,
        _: Request<Empty>,
    ) -> Result<tonic::Response<LightdInfo>, Status> {
        unimplemented()
    }

    async fn get_standard_fee(
        &self,
        _: Request<Empty>,
    ) -> Result<tonic::Response<StandardFee>, Status> {
        unimplemented()
    }

    async fn ping(
        &self,
        _: Request<PingDuration>,
    ) -> Result<tonic::Response<PingResponse>, Status> {
        unimplemented()
    }
}

struct RangeServer {
    address: SocketAddr,
    health: HealthReporter,
    ranges: StalledRanges,
    _server: JoinHandle<std::io::Result<()>>,
}

async fn serve_ranges(connection_loop: ConnectionLoop) -> Result<RangeServer> {
    let ranges = StalledRanges::default();
    let (health, health_service) = tonic_health::server::health_reporter();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let slots = NonZeroUsize::new(512).ok_or_else(|| eyre!("slots must be non-zero"))?;
    let limits = GrpcReceiveLimits::new(slots, Duration::from_secs(30));
    let service = CompactTxStreamerServer::new(ranges.clone());
    let server = match connection_loop {
        ConnectionLoop::Zinder => tokio::spawn(
            public_grpc_server_with(limits, STALL_TIMEOUT, ClientAddressSource::Connection)
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming(listener),
        ),
        ConnectionLoop::Tonic => tokio::spawn(async move {
            tonic::transport::Server::builder()
                .max_concurrent_streams(MAX_CONCURRENT_STREAMS_PER_CONNECTION)
                .layer(GrpcReceiveLimitLayer::new(
                    limits,
                    ClientAddressSource::Connection,
                ))
                .layer(GrpcSendStallLayer::new(STALL_TIMEOUT))
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .map_err(std::io::Error::other)
        }),
    };
    Ok(RangeServer {
        address,
        health,
        ranges,
        _server: server,
    })
}

/// Fills every stream slot of one connection with a sibling health `Watch`
/// the client reads and stalled range streams of every kind.
async fn fill_with_stalled_ranges(
    channel: &Channel,
) -> Result<(
    Vec<tonic::Streaming<CompactBlock>>,
    tonic::Streaming<HealthCheckResponse>,
)> {
    let mut sibling = watch(channel).await?;
    let first = within_wait_limit(sibling.next())
        .await?
        .ok_or_else(|| eyre!("the watch stream ended"))??;
    assert_eq!(first.status(), WireServingStatus::Serving);
    let stalled_count = usize::try_from(MAX_CONCURRENT_STREAMS_PER_CONNECTION)? - 1;
    let stalled = within_wait_limit(try_join_all((0..stalled_count).map(|index| {
        let mut client = CompactTxStreamerClient::new(channel.clone());
        let range = BlockRange {
            start: Some(BlockId {
                height: u64::try_from(index % 3).unwrap_or(0),
                hash: Vec::new(),
            }),
            ..BlockRange::default()
        };
        async move { Ok::<_, Status>(client.get_block_range(range).await?.into_inner()) }
    })))
    .await??;
    Ok((stalled, sibling))
}

#[tokio::test]
async fn stalled_range_streams_free_their_sliced_blocks_at_the_deadline() -> Result<()> {
    let server = serve_ranges(ConnectionLoop::Zinder).await?;
    let channel = withholding_client(server.address).await?;
    let (stalled, mut sibling) = fill_with_stalled_ranges(&channel).await?;
    wait_until(
        "every range stream reaches its block, pause, or end",
        || server.ranges.reached_tails(stalled.len()),
    )
    .await?;

    // Without the client reading any range stream, the reset frees every
    // block a slice kept alive, and every slot.
    wait_until("every stalled range stream's blocks are freed", || {
        server.ranges.all_freed(stalled.len())
    })
    .await?;
    let watches =
        within_wait_limit(try_join_all((0..stalled.len()).map(|_| watch(&channel)))).await??;
    assert_eq!(watches.len(), stalled.len());

    // The connection and the sibling stream stayed open.
    server
        .health
        .set_service_status("", ServingStatus::NotServing)
        .await;
    let update = within_wait_limit(sibling.next())
        .await?
        .ok_or_else(|| eyre!("the watch stream ended"))??;
    assert_eq!(update.status(), WireServingStatus::NotServing);
    Ok(())
}

/// The control: on tonic's own server, which cannot reset a stream, the
/// slices HTTP/2 holds keep their blocks alive after the send stall layer
/// abandons the responses. A copy would have let them go.
#[tokio::test]
async fn tonic_s_own_server_keeps_sliced_blocks_of_stalled_range_streams() -> Result<()> {
    let server = serve_ranges(ConnectionLoop::Tonic).await?;
    let channel = withholding_client(server.address).await?;
    let (stalled, _sibling) = fill_with_stalled_ranges(&channel).await?;
    wait_until(
        "every range stream reaches its block, pause, or end",
        || server.ranges.reached_tails(stalled.len()),
    )
    .await?;
    tokio::time::sleep(CONTROL_WAIT).await;
    assert!(
        server.ranges.produced.load(Ordering::SeqCst) >= stalled.len(),
        "fewer blocks were produced than streams opened"
    );
    assert!(
        !server.ranges.all_freed(stalled.len()),
        "every sliced block was freed without a reset"
    );
    Ok(())
}
