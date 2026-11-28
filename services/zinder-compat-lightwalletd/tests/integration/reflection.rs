#![allow(
    missing_docs,
    reason = "Integration test names describe the gRPC reflection contract under test."
)]

use std::sync::Arc;

use eyre::{OptionExt as _, Result, eyre};
use prost::Message as _;
use prost_types::{FileDescriptorProto, FileDescriptorSet};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic_reflection::pb::v1::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
};
use tonic_reflection::pb::v1alpha;
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::Network;
use zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET;
use zinder_query::{
    RocksDbCanonicalReader, RocksDbWalletReader, WalletServingPairSlot, WalletServingQuery,
    WalletServingReadPair,
};
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

const EXPECTED_SERVICE: &str = "cash.z.wallet.sdk.rpc.CompactTxStreamer";

async fn compat_listener() -> Result<Channel> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(1),
        &activations,
    )?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let wallet_query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(
        Arc::new(WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?),
    ));
    let adapter = LightwalletdGrpcAdapter::new(wallet_query).into_server();
    let runtime =
        zinder_runtime::SupervisedRuntime::new(zinder_runtime::RuntimeService::CompatLightwalletd);
    let router = runtime.public_grpc_router(
        adapter,
        LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET,
        zinder_runtime::ClientAddressSource::Connection,
        &zinder_runtime::Readiness::default(),
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _store_fixture = store_fixture;
        router.serve_with_incoming(listener).await
    });
    Ok(Channel::from_shared(format!("http://{addr}"))?
        .connect()
        .await?)
}

const SERVICES: [&str; 4] = [
    EXPECTED_SERVICE,
    "grpc.health.v1.Health",
    "grpc.reflection.v1.ServerReflection",
    "grpc.reflection.v1alpha.ServerReflection",
];

#[tokio::test]
async fn v1_lists_the_services_on_the_compat_listener() -> Result<()> {
    let mut client = ServerReflectionClient::new(compat_listener().await?);
    let mut replies = client
        .server_reflection_info(tokio_stream::once(ServerReflectionRequest {
            message_request: Some(MessageRequest::ListServices(String::new())),
            ..Default::default()
        }))
        .await?
        .into_inner();
    let reply = replies
        .message()
        .await?
        .ok_or_eyre("missing v1 service list")?;
    let Some(MessageResponse::ListServicesResponse(mut list)) = reply.message_response else {
        return Err(eyre!("expected a v1 service list"));
    };
    list.service.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(
        list.service
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
        SERVICES
    );
    Ok(())
}

fn alpha_request(
    message: v1alpha::server_reflection_request::MessageRequest,
) -> v1alpha::ServerReflectionRequest {
    v1alpha::ServerReflectionRequest {
        message_request: Some(message),
        ..Default::default()
    }
}

#[tokio::test]
async fn v1alpha_discovers_wallet_descriptors_and_holds_a_stream_slot_until_half_close()
-> Result<()> {
    use v1alpha::{
        server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let mut client = ServerReflectionClient::new(compat_listener().await?);
    let (send, recv) = mpsc::channel(1);
    let mut replies = client
        .server_reflection_info(ReceiverStream::new(recv))
        .await?
        .into_inner();
    send.send(alpha_request(MessageRequest::ListServices(String::new())))
        .await?;
    let reply = replies
        .message()
        .await?
        .ok_or_eyre("missing v1alpha service list")?;
    let Some(MessageResponse::ListServicesResponse(mut list)) = reply.message_response else {
        return Err(eyre!("expected a v1alpha service list"));
    };
    list.service.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(
        list.service
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
        SERVICES
    );
    let holds_slots = |count: u8| {
        let gauge =
            format!(r#"zinder_grpc_request_receive_slots_held{{pool="client_stream"}} {count}"#);
        recorder.handle().render().lines().any(|line| line == gauge)
    };
    assert!(holds_slots(1));

    let descriptors = FileDescriptorSet::decode(LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET)?;
    for (query, name) in [
        (
            MessageRequest::FileContainingSymbol(EXPECTED_SERVICE.into()),
            "service.proto",
        ),
        (
            MessageRequest::FileByFilename("compact_formats.proto".into()),
            "compact_formats.proto",
        ),
    ] {
        send.send(alpha_request(query)).await?;
        let reply = replies
            .message()
            .await?
            .ok_or_eyre("missing descriptor response")?;
        let Some(MessageResponse::FileDescriptorResponse(files)) = reply.message_response else {
            return Err(eyre!("expected descriptor for {name}"));
        };
        let expected = descriptors
            .file
            .iter()
            .find(|file| file.name() == name)
            .ok_or_eyre("missing vendored descriptor")?;
        let decoded = files
            .file_descriptor_proto
            .iter()
            .map(|file| FileDescriptorProto::decode(file.as_slice()))
            .collect::<Result<Vec<_>, _>>()?;
        assert!(decoded.contains(expected), "missing vendored {name}");
    }
    drop(send);
    assert!(replies.message().await?.is_none());
    assert!(holds_slots(0));
    Ok(())
}

#[tokio::test]
async fn v1alpha_applies_the_public_message_size_limit() -> Result<()> {
    use v1alpha::{
        server_reflection_client::ServerReflectionClient, server_reflection_request::MessageRequest,
    };
    let mut client = ServerReflectionClient::new(compat_listener().await?);
    let mut request = alpha_request(MessageRequest::ListServices(String::new()));
    request.host = "a".repeat(zinder_runtime::MAX_DECODING_MESSAGE_BYTES);
    let mut replies = client
        .server_reflection_info(tokio_stream::once(request))
        .await?
        .into_inner();
    assert!(replies.message().await?.is_none());
    Ok(())
}
