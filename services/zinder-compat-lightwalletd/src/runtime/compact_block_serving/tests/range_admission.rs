use std::{future, num::NonZeroU32, sync::Arc, time::Duration};

use http::{Request, Response, StatusCode};
use http_body::{Body as _, Frame};
use http_body_util::StreamBody;
use prost::bytes::Bytes;

use tokio::net::TcpListener;
use tonic::{
    Code, Status,
    body::Body,
    transport::{Endpoint, server::TcpConnectInfo},
};
use tower::{Layer, Service, ServiceExt, service_fn};
use zinder_core::Network;
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::RocksDbCanonicalReader;
use zinder_runtime::{
    BrowserOrigins, ClientAddressSource, GrpcRangeAdmission, Readiness, ReadinessState,
    WalletRpcMetricsLayer,
};

use crate::{
    CanonicalReaderSlot, CompactBlocksGrpcAdapter,
    block_range::BlockRangeStreamLimit,
    runtime::listener::{BrowserListener, serve_listener},
};

#[tokio::test]
async fn a_wallet_over_its_range_quota_does_not_interrupt_independent_wallets() -> eyre::Result<()>
{
    use zinder_runtime::{GrpcProxyProtocol, RuntimeService, SupervisedRuntime};

    let chain = zinder_testkit::ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
    let activations = zinder_testkit::sample_regtest_upgrade_activations();
    let mut fixture = zinder_testkit::WalletServingStoreFixture::from_chain(&chain, &activations)?;
    let (canonical, wallet) = fixture.take_readers()?;
    drop(wallet);
    let adapter = CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(Arc::new(
        RocksDbCanonicalReader::new(canonical)?,
    )));
    let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd);
    let readiness = Readiness::new(ReadinessState::ready(Some(4)));
    let range = lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: 4,
            hash: Vec::new(),
        }),
        pool_types: vec![lightwalletd::PoolType::Sapling as i32],
    };

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = Endpoint::from_shared(format!("http://{}", listener.local_addr()?))?
        .timeout(Duration::from_secs(5));
    let config = BrowserListener {
        browser_origins: &BrowserOrigins::default(),
        client_address: ClientAddressSource::Connection,
        proxy_protocol: GrpcProxyProtocol::default(),
        range_admission: GrpcRangeAdmission {
            max_streams_per_client: NonZeroU32::MIN,
            ..GrpcRangeAdmission::default()
        },
    };
    let server = tokio::spawn(serve_listener(
        &runtime,
        adapter.into_server(),
        listener,
        &config,
        &readiness,
    )?);
    let mut busy = CompactTxStreamerClient::new(
        endpoint
            .clone()
            .initial_stream_window_size(32)
            .connect()
            .await?,
    );
    let held = busy.get_block_range(range.clone()).await?.into_inner();
    let refused = busy
        .get_block_range(range.clone())
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("client exceeded its quota"))?;
    assert_eq!(refused.code(), Code::Unavailable);
    assert_eq!(
        refused
            .metadata()
            .get("grpc-retry-pushback-ms")
            .and_then(|header| header.to_str().ok()),
        Some("250")
    );

    let mut independent = CompactTxStreamerClient::new(endpoint.connect().await?);
    let independent_range = independent
        .get_block_range(range.clone())
        .await?
        .into_inner();
    assert_eq!(range_heights(independent_range).await?, [1, 2, 3, 4]);
    assert_eq!(range_heights(held).await?, [1, 2, 3, 4]);
    assert_eq!(
        range_heights(busy.get_block_range(range).await?.into_inner()).await?,
        [1, 2, 3, 4]
    );
    server.abort();
    Ok(())
}

async fn range_heights(
    mut blocks: tonic::Streaming<lightwalletd::CompactBlock>,
) -> eyre::Result<Vec<u64>> {
    let mut heights = Vec::new();
    while let Some(block) = blocks.message().await? {
        heights.push(block.height);
    }
    Ok(heights)
}

fn quota_limited_range(
    pool: BlockRangeStreamLimit,
) -> impl Service<Request<Body>, Response = Response<Body>, Error = Status> + Clone {
    WalletRpcMetricsLayer::Lightwalletd
        .with_range_admission(
            GrpcRangeAdmission {
                max_streams_per_client: NonZeroU32::MIN,
                ..GrpcRangeAdmission::default()
            },
            ClientAddressSource::Connection,
        )
        .layer(service_fn(move |_| {
            let pool = pool.clone();
            async move {
                let slot = pool.open().await?;
                let frames = futures_util::stream::once(async move {
                    let _slot = slot;
                    future::pending::<Result<Frame<Bytes>, Status>>().await
                });
                Ok(Response::new(Body::new(StreamBody::new(frames))))
            }
        }))
}

fn range_request() -> Result<Request<Body>, http::Error> {
    Request::post("/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange")
        .extension(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(([127, 0, 0, 1], 1234).into()),
        })
        .body(Body::empty())
}

fn assert_quota_refusal(response: &Response<Body>) {
    assert_eq!(response.headers()[Status::GRPC_STATUS], "14");
    assert_eq!(response.headers()["grpc-retry-pushback-ms"], "250");
}

#[tokio::test]
async fn cancelling_a_queued_range_immediately_returns_the_clients_quota() -> eyre::Result<()> {
    let pool = BlockRangeStreamLimit::new(NonZeroU32::MIN);
    let shared_slot = pool.open().await?;
    let ranges = quota_limited_range(pool);
    let mut queued = Box::pin(ranges.clone().oneshot(range_request()?));
    assert!(super::poll_once(&mut queued).is_pending());
    assert_quota_refusal(&ranges.clone().oneshot(range_request()?).await?);

    drop(queued);
    let mut replacement = Box::pin(ranges.oneshot(range_request()?));
    assert!(super::poll_once(&mut replacement).is_pending());
    drop(shared_slot);
    let response = tokio::time::timeout(Duration::from_secs(2), replacement).await??;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.body().is_end_stream());
    Ok(())
}

#[tokio::test]
async fn cancelling_an_open_range_immediately_returns_the_clients_quota() -> eyre::Result<()> {
    let ranges = quota_limited_range(BlockRangeStreamLimit::new(NonZeroU32::MIN));
    let open = ranges.clone().oneshot(range_request()?).await?;
    assert!(!open.body().is_end_stream());
    assert_quota_refusal(&ranges.clone().oneshot(range_request()?).await?);

    drop(open);
    let mut replacement = Box::pin(ranges.oneshot(range_request()?));
    let std::task::Poll::Ready(response) = super::poll_once(&mut replacement) else {
        eyre::bail!("the replacement range did not open immediately");
    };
    let response = response?;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.body().is_end_stream());
    Ok(())
}
