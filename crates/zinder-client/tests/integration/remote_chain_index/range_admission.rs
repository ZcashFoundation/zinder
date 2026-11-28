use std::{
    convert::Infallible,
    future::{Ready, ready},
    net::SocketAddr,
    num::NonZeroU32,
    task::{Context, Poll},
    time::Duration,
};

use eyre::{OptionExt as _, Result};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{
    Request, Response, Status,
    body::Body,
    codegen::{BoxFuture, Service, http},
    server::{NamedService, ServerStreamingService},
};
use zinder_client::{
    BlockHeight, BlockHeightRange, ChainIndex, ErrorReason, Network, RemoteChainIndex,
    RemoteOpenOptions, RetryPolicy,
};
use zinder_proto::v1::wallet::{CompactBlocksInRangeChunk, CompactBlocksInRangeRequest};
use zinder_runtime::{
    ClientAddressSource, GrpcRangeAdmission, WalletRpcMetricsLayer, public_grpc_server,
};

type RangeChunk = std::result::Result<CompactBlocksInRangeChunk, Status>;

#[derive(Clone)]
struct RangeProbe(mpsc::UnboundedSender<(SocketAddr, mpsc::UnboundedSender<RangeChunk>)>);

impl NamedService for RangeProbe {
    const NAME: &'static str = zinder_proto::v1::wallet::wallet_query_server::SERVICE_NAME;
}

impl Service<http::Request<Body>> for RangeProbe {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let range = self.clone();
        Box::pin(async move {
            Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                .server_streaming(range, request)
                .await)
        })
    }
}

impl ServerStreamingService<CompactBlocksInRangeRequest> for RangeProbe {
    type Response = CompactBlocksInRangeChunk;
    type ResponseStream = UnboundedReceiverStream<RangeChunk>;
    type Future = Ready<std::result::Result<Response<Self::ResponseStream>, Status>>;

    fn call(&mut self, request: Request<CompactBlocksInRangeRequest>) -> Self::Future {
        let Some(peer) = request.remote_addr() else {
            return ready(Err(Status::internal("missing range peer address")));
        };
        let (chunks, stream) = mpsc::unbounded_channel();
        if self.0.send((peer, chunks)).is_err() {
            return ready(Err(Status::internal("range probe receiver closed")));
        }
        ready(Ok(Response::new(UnboundedReceiverStream::new(stream))))
    }
}

#[tokio::test]
async fn native_range_admission_refusals_preserve_the_channel_and_its_quota() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (opened, mut ranges) = mpsc::unbounded_channel();
    let source = ClientAddressSource::Connection;
    let server = tokio::spawn(
        public_grpc_server(source)
            .layer(WalletRpcMetricsLayer::WalletQuery.with_range_admission(
                GrpcRangeAdmission {
                    max_streams_per_client: NonZeroU32::MIN,
                    ..GrpcRangeAdmission::default()
                },
                source,
            ))
            .add_service(RangeProbe(opened))
            .serve_with_incoming(listener),
    );
    let client = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashRegtest,
    })?;
    let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1));
    let call = || client.compact_blocks_in_range(range, None);

    tokio::time::timeout(Duration::from_secs(5), async {
        let held = call().await?;
        let (original_peer, held_chunks) = ranges.recv().await.ok_or_eyre("missing first range")?;
        for _ in 0..2 {
            let refusal = call()
                .await
                .err()
                .ok_or_eyre("client escaped its connection quota")?;
            assert_eq!(
                refusal.reason(),
                Some(ErrorReason::BlockRangeAdmissionRefused)
            );
            assert_eq!(refusal.retry_policy(), RetryPolicy::RetryWithBackoff);
        }
        assert!(!held_chunks.is_closed());
        drop(held);
        held_chunks.closed().await;

        let resumed = call().await?;
        let (resumed_peer, _) = ranges.recv().await.ok_or_eyre("missing resumed range")?;
        assert_eq!(
            resumed_peer, original_peer,
            "the client changed its TCP connection"
        );
        drop(resumed);
        Ok::<_, eyre::Report>(())
    })
    .await??;
    server.abort();
    Ok(())
}
