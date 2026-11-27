use std::{
    convert::Infallible,
    num::NonZeroUsize,
    sync::Arc,
    task::{Context, Waker},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use eyre::{OptionExt as _, Result, ensure};
use http::{HeaderValue, Request, Response, header};
use http_body::Frame;
use http_body_util::{BodyExt as _, StreamBody};
use prost::{Message, bytes::Bytes};
use tokio_stream::StreamExt as _;
use tonic::{Code, Status, body::Body, transport::server::TcpConnectInfo};
use tower::{Layer as _, Service, ServiceExt as _};
use zinder_core::Network;
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_server::CompactTxStreamerServer,
};
use zinder_query::RocksDbCanonicalReader;
use zinder_runtime::{
    ClientAddressSource, GrpcRangeAdmission, GrpcReceiveLimitLayer, GrpcReceiveLimits,
    MAX_UNCAPPED_ENUM_LIST_ELEMENTS, RepeatedFieldGuard,
};
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

use crate::{
    COMPACT_TX_STREAMER_FIELD_LIMITS, CanonicalReaderSlot, CompactBlocksGrpcAdapter,
    LightwalletdGrpcAdapter,
};

use super::{SERVICE_NAME, transport_layers, wallet_layers};

const WALLET_ORIGIN: &str = "https://wallet.example";
const TEXT: &str = "application/grpc-web-text+proto";
const ENCODINGS: [&str; 2] = ["application/grpc-web+proto", TEXT];
const DECODE_LIMIT: usize = 2048;
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(2);

fn reader() -> Result<WalletServingStoreFixture> {
    WalletServingStoreFixture::from_chain(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(1),
        &sample_regtest_upgrade_activations(),
    )
}

fn service(
    reader: &mut WalletServingStoreFixture,
) -> Result<
    impl Service<Request<Body>, Response = Response<Body>, Error = Infallible> + Clone + use<>,
> {
    let (canonical, _) = reader.take_readers()?;
    let adapter = CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(Arc::new(
        RocksDbCanonicalReader::new(canonical)?,
    )));
    let service = CompactTxStreamerServer::new(adapter).max_decoding_message_size(DECODE_LIMIT);
    let guard = RepeatedFieldGuard::new(service, COMPACT_TX_STREAMER_FIELD_LIMITS);
    let receive = GrpcReceiveLimitLayer::new(
        GrpcReceiveLimits::new(NonZeroUsize::MIN, RECEIVE_TIMEOUT),
        ClientAddressSource::Connection,
    );
    Ok(
        transport_layers(&[HeaderValue::from_static(WALLET_ORIGIN)]).layer(
            wallet_layers(
                true,
                GrpcRangeAdmission::default(),
                ClientAddressSource::Connection,
            )
            .layer(receive.layer(guard)),
        ),
    )
}

fn request(method: &str, encoding: &'static str, body: Body) -> Result<Request<Body>> {
    Ok(Request::post(format!("/{SERVICE_NAME}/{method}"))
        .version(http::Version::HTTP_11)
        .header(header::ORIGIN, WALLET_ORIGIN)
        .extension(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(([127, 0, 0, 1], 1234).into()),
        })
        .header(header::CONTENT_TYPE, encoding)
        .header(header::ACCEPT, encoding)
        .body(body)?)
}

fn message_request(
    method: &str,
    encoding: &'static str,
    message: &impl Message,
) -> Result<Request<Body>> {
    let mut wire = vec![0];
    wire.extend_from_slice(&u32::try_from(message.encoded_len())?.to_be_bytes());
    message.encode(&mut wire)?;
    let wire: Bytes = if encoding == TEXT {
        STANDARD.encode(wire).into()
    } else {
        wire.into()
    };
    // Split within both the gRPC header and the first base64 quartet.
    let parts = [wire.slice(..1), wire.slice(1..3), wire.slice(3..)];
    let fragments = parts.map(|part| Ok::<_, Status>(Frame::data(part)));
    request(
        method,
        encoding,
        Body::new(StreamBody::new(tokio_stream::iter(fragments))),
    )
}

async fn assert_response_error(
    response: Response<Body>,
    encoding: &str,
    code: Code,
) -> Result<Status> {
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], encoding);
    let status =
        Status::from_header_map(response.headers()).ok_or_eyre("missing early gRPC status")?;
    assert_eq!(status.code(), code);
    assert!(response.into_body().collect().await?.to_bytes().is_empty());
    Ok(status)
}

async fn read_one_block(
    service: impl Service<Request<Body>, Response = Response<Body>, Error = Infallible>,
    encoding: &'static str,
) -> Result<()> {
    let id = lightwalletd::BlockId {
        height: 1,
        hash: vec![],
    };
    let response = service
        .oneshot(message_request("GetBlock", encoding, &id)?)
        .await?;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], encoding);
    let mut body = response.into_body();
    let mut wire = Vec::new();
    while let Some(frame) = body.frame().await {
        let bytes = frame?
            .into_data()
            .ok()
            .ok_or_eyre("expected browser response data")?;
        if encoding == TEXT {
            STANDARD.decode_vec(bytes, &mut wire)?;
        } else {
            wire.extend_from_slice(&bytes);
        }
    }
    let (header, payload) = wire.split_at_checked(5).ok_or_eyre("missing block frame")?;
    ensure!(header[0] == 0, "expected a protobuf message");
    let len = usize::try_from(u32::from_be_bytes(header[1..].try_into()?))?;
    let (block, tail) = payload
        .split_at_checked(len)
        .ok_or_eyre("incomplete block frame")?;
    assert_eq!(lightwalletd::CompactBlock::decode(block)?.height, 1);
    assert_eq!(tail, b"\x80\0\0\0\x0fgrpc-status:0\r\n");
    Ok(())
}

#[tokio::test]
async fn fragmented_browser_messages_preserve_decoding_limits_and_return_the_receive_slot()
-> Result<()> {
    let mut reader = reader()?;
    let service = service(&mut reader)?;
    for encoding in ENCODINGS {
        let oversized = lightwalletd::BlockId {
            height: 0,
            hash: vec![0; DECODE_LIMIT],
        };
        let response = service
            .clone()
            .oneshot(message_request("GetBlock", encoding, &oversized)?)
            .await?;
        assert_response_error(response, encoding, Code::OutOfRange).await?;
        read_one_block(service.clone(), encoding).await?;

        let repeated = lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: 1,
                hash: vec![],
            }),
            end: Some(lightwalletd::BlockId {
                height: 1,
                hash: vec![],
            }),
            pool_types: vec![
                lightwalletd::PoolType::Sapling as i32;
                MAX_UNCAPPED_ENUM_LIST_ELEMENTS + 1
            ],
        };
        let response = service
            .clone()
            .oneshot(message_request("GetBlockRange", encoding, &repeated)?)
            .await?;
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            WALLET_ORIGIN
        );
        assert!(
            response.headers()[header::ACCESS_CONTROL_EXPOSE_HEADERS]
                .to_str()?
                .split(',')
                .any(|name| name.trim().eq_ignore_ascii_case("grpc-status"))
        );
        let status = assert_response_error(response, encoding, Code::InvalidArgument).await?;
        assert!(
            status
                .message()
                .contains("GetBlockRange request field poolTypes carries")
        );
        read_one_block(service.clone(), encoding).await?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stalled_browser_receives_refuse_excess_calls_until_the_receive_deadline() -> Result<()> {
    let mut reader = reader()?;
    let service = service(&mut reader)?;
    for encoding in ENCODINGS {
        let prefix = if encoding == TEXT {
            Bytes::from_static(b"A")
        } else {
            Bytes::from_static(&[0])
        };
        let stream =
            tokio_stream::once(Ok::<_, Status>(Frame::data(prefix))).chain(tokio_stream::pending());
        let request_body = Body::new(StreamBody::new(stream));
        let mut stalled = Box::pin(service.clone().oneshot(request(
            "GetBlock",
            encoding,
            request_body,
        )?));
        assert!(
            stalled
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );

        let response = service
            .clone()
            .oneshot(message_request(
                "GetLatestBlock",
                encoding,
                &lightwalletd::ChainSpec {},
            )?)
            .await?;
        assert_response_error(response, encoding, Code::Unavailable).await?;
        tokio::time::advance(RECEIVE_TIMEOUT).await;
        assert_response_error(stalled.await?, encoding, Code::DeadlineExceeded).await?;
        read_one_block(service.clone(), encoding).await?;
    }
    Ok(())
}

#[tokio::test]
async fn browser_listener_records_the_native_status_and_block_count_in_both_encodings() -> Result<()>
{
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let mut reader = reader()?;
    let service = service(&mut reader)?;
    let id = lightwalletd::BlockId {
        height: 1,
        hash: vec![],
    };
    let range = lightwalletd::BlockRange {
        start: Some(id.clone()),
        end: Some(id),
        pool_types: vec![],
    };
    for encoding in ENCODINGS {
        let response = service
            .clone()
            .oneshot(message_request("GetBlockRange", encoding, &range)?)
            .await?;
        response.into_body().collect().await?;
        let refused = service
            .clone()
            .oneshot(message_request(
                "GetTaddressBalanceStream",
                encoding,
                &lightwalletd::Address::default(),
            )?)
            .await?;
        assert_response_error(refused, encoding, Code::Unimplemented).await?;
    }
    let exposition = handle.render();
    for sample in [
        r#"zinder_wallet_rpc_requests_total{surface="lightwalletd",method="GetBlockRange",grpc_code="ok",error_class="none"} 2"#,
        r#"zinder_wallet_rpc_block_range_blocks_sum{surface="lightwalletd",method="GetBlockRange",grpc_code="ok",error_class="none"} 2"#,
        r#"zinder_wallet_rpc_requests_total{surface="lightwalletd",method="GetTaddressBalanceStream",grpc_code="unimplemented",error_class="none"} 2"#,
        r#"zinder_wallet_rpc_open_streams{surface="lightwalletd",method="GetTaddressBalanceStream"} 0"#,
    ] {
        assert!(
            exposition.lines().any(|line| line == sample),
            "{sample}\n{exposition}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn wallet_rpc_in_wallet_mode_counts_one_lightwalletd_range_across_query_reads() -> Result<()>
{
    use zinder_query::{
        RocksDbWalletReader, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
    };

    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(zinder_store::RawBlobRetention::All)
        .extend_blocks(1);
    let mut store =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let (canonical, wallet) = store.take_readers()?;
    let pair = WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?;
    let query =
        WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(pair)));
    let adapter = LightwalletdGrpcAdapter::from_admitted_compatibility_query(query)?.into_server();
    let native = transport_layers(&[]).layer(
        wallet_layers(
            false,
            GrpcRangeAdmission::default(),
            ClientAddressSource::Connection,
        )
        .layer(adapter),
    );
    let id = lightwalletd::BlockId {
        height: 1,
        hash: vec![],
    };
    let range = lightwalletd::BlockRange {
        start: Some(id.clone()),
        end: Some(id),
        pool_types: vec![],
    };
    let mut request = message_request("GetBlockRangeNullifiers", "application/grpc", &range)?;
    *request.version_mut() = http::Version::HTTP_2;
    observe_balance_upload(native.clone(), &handle).await?;
    let response = native.oneshot(request).await?.into_body().collect().await?;
    let trailers = response.trailers().ok_or_eyre("missing native trailers")?;
    assert_eq!(trailers[Status::GRPC_STATUS], "0");
    let exposition = handle.render();
    for sample in [
        r#"zinder_wallet_rpc_requests_total{surface="lightwalletd",method="GetBlockRangeNullifiers",grpc_code="ok",error_class="none"} 1"#,
        r#"zinder_wallet_rpc_block_range_blocks_sum{surface="lightwalletd",method="GetBlockRangeNullifiers",grpc_code="ok",error_class="none"} 1"#,
    ] {
        assert!(
            exposition.lines().any(|line| line == sample),
            "{sample}\n{exposition}"
        );
    }
    Ok(())
}

async fn observe_balance_upload(
    native: impl Service<Request<Body>, Response = Response<Body>, Error = Infallible> + Clone,
    handle: &metrics_exporter_prometheus::PrometheusHandle,
) -> Result<()> {
    let stream_count = r#"zinder_wallet_rpc_open_streams{surface="lightwalletd",method="GetTaddressBalanceStream"}"#;
    assert!(handle.render().contains(&format!("{stream_count} 0")));
    for half_close in [true, false] {
        let (receiving, first_poll) = tokio::sync::oneshot::channel();
        let (close, closed) = tokio::sync::oneshot::channel::<()>();
        let upload =
            tokio_stream::once((receiving, closed)).then(|(receiving, closed)| async move {
                let _ = receiving.send(());
                let _ = closed.await;
                Ok::<_, Status>(Frame::<Bytes>::trailers(http::HeaderMap::new()))
            });
        let mut upload = request(
            "GetTaddressBalanceStream",
            "application/grpc",
            Body::new(StreamBody::new(upload)),
        )?;
        *upload.version_mut() = http::Version::HTTP_2;
        let mut response = Box::pin(native.clone().oneshot(upload));
        tokio::select! {
            _ = &mut response => eyre::bail!("balance RPC completed before reading the upload"),
            read = first_poll => read?,
        }
        assert!(handle.render().contains(&format!("{stream_count} 1")));
        if half_close {
            let _ = close.send(());
            let _head = response.await?;
        } else {
            drop(response);
        }
        assert!(handle.render().contains(&format!("{stream_count} 0")));
    }
    Ok(())
}
