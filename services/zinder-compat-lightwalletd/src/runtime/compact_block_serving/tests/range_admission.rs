use std::{sync::Arc, time::Duration};

use tokio::net::TcpListener;
use tonic::{Code, transport::Endpoint};
use zinder_core::Network;
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::RocksDbCanonicalReader;
use zinder_runtime::{Readiness, ReadinessState};

use crate::{CanonicalReaderSlot, CompactBlocksGrpcAdapter};

#[tokio::test]
async fn a_wallet_over_its_range_quota_does_not_interrupt_independent_wallets() -> eyre::Result<()>
{
    use std::num::NonZeroU32;
    use zinder_runtime::{
        ClientAddressSource, GrpcProxyProtocol, GrpcRangeAdmission, RuntimeService,
        SupervisedRuntime,
    };

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
    let config = super::super::super::browser_transport::BrowserListener {
        browser_origins: &[],
        client_address: ClientAddressSource::Connection,
        proxy_protocol: GrpcProxyProtocol::default(),
        range_admission: GrpcRangeAdmission {
            max_streams_per_client: NonZeroU32::MIN,
            ..GrpcRangeAdmission::default()
        },
    };
    let server = tokio::spawn(super::super::super::browser_transport::serve_listener(
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
