#![allow(
    missing_docs,
    reason = "Integration test names describe the gRPC reflection contract under test."
)]

use std::sync::Arc;

use eyre::{Result, eyre};
use tokio::net::TcpListener;
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tonic::{Request, transport::Server};
use tonic_reflection::pb::v1::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET;
use zinder_query::WalletQuery;
use zinder_testkit::{StoreFixture, sample_regtest_upgrade_activations};

const EXPECTED_SERVICE: &str = "cash.z.wallet.sdk.rpc.CompactTxStreamer";

#[tokio::test]
async fn server_reflection_lists_compact_tx_streamer_service() -> Result<()> {
    let store_fixture = StoreFixture::open()?;
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        activations.clone(),
    ))
    .into_server();
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let _server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter)
            .add_service(reflection_service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let endpoint = tonic::transport::Endpoint::new(format!("http://{server_addr}"))?
        .connect()
        .await?;
    let mut client = ServerReflectionClient::new(endpoint);
    let request = Request::new(tokio_stream::once(ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    }));
    let mut response_stream = client.server_reflection_info(request).await?.into_inner();
    let response_message = response_stream
        .next()
        .await
        .ok_or_else(|| eyre!("reflection stream closed without a response"))??
        .message_response
        .ok_or_else(|| eyre!("reflection response carried no MessageResponse"))?;

    #[allow(
        clippy::wildcard_enum_match_arm,
        reason = "MessageResponse is non_exhaustive; future variants legitimately surface here as test failures."
    )]
    let advertised_services = match response_message {
        MessageResponse::ListServicesResponse(list) => list.service,
        other => return Err(eyre!("unexpected reflection response: {other:?}")),
    };
    assert!(
        advertised_services
            .iter()
            .any(|service| service.name == EXPECTED_SERVICE),
        "reflection did not advertise {EXPECTED_SERVICE}: {advertised_services:?}",
    );
    Ok(())
}
