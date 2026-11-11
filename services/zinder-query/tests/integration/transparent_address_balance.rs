#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::sync::Arc;

use eyre::eyre;
use tonic::{Code, Request};
use zinder_core::Network;
use zinder_proto::v1::wallet::{
    TransparentAddressBalanceRequest, wallet_query_server::WalletQuery as WalletQueryService,
};
use zinder_query::{
    CanonicalReader, WalletEndpointMetadata, WalletProjectionReader, WalletQueryGrpcAdapter,
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

#[tokio::test]
async fn release_balance_is_unavailable_before_request_parsing_or_store_reads() -> eyre::Result<()>
{
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let mut store_fixture =
        WalletServingStoreFixture::from_chain(&chain_fixture, activations.as_ref())?;
    let (canonical_reader, wallet_reader) = store_fixture.take_readers()?;
    let serving_pair = Arc::new(WalletServingReadPair::new(
        Arc::new(canonical_reader) as Arc<dyn CanonicalReader>,
        Arc::new(wallet_reader) as Arc<dyn WalletProjectionReader>,
    )?);
    let (ingest_control, _ingest_control_fixture) =
        crate::common::admitted_ingest_control_fixture().await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(serving_pair),
        (),
        ingest_control,
        activations,
    );
    let grpc_adapter = WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default());
    let status = WalletQueryService::transparent_address_balance(
        &grpc_adapter,
        Request::new(TransparentAddressBalanceRequest {
            addresses: Vec::new(),
            at_epoch_id: None,
        }),
    )
    .await
    .err()
    .ok_or_else(|| eyre!("release balance unexpectedly accepted an over-cap request"))?;

    assert_eq!(status.code(), Code::FailedPrecondition);
    Ok(())
}
