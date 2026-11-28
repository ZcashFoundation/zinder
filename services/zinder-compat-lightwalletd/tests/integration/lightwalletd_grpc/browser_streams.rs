use std::{num::NonZeroU32, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use eyre::{Result, ensure, eyre};
use http::{Request, header};
use http_body_util::{BodyExt as _, Full};
use prost::{Message, bytes::Bytes};
use tokio::sync::Semaphore;
use tonic::{Code, Status, body::Body};
use tonic_web::GrpcWebLayer;
use tower::{ServiceBuilder, ServiceExt as _};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::{BlockHeight, Network};
use zinder_proto::compat::lightwalletd::{CompactBlock, compact_tx_streamer_server::SERVICE_NAME};
use zinder_query::DEFAULT_MAX_COMPACT_BLOCK_RANGE;
use zinder_runtime::GrpcSendStallLayer;
use zinder_testkit::ChainFixture;

use super::{
    AcceptanceStore, EpochPinRecorder, REFUSAL_WAIT, acceptance_store_from_chain,
    block_range_request, later_chunk_read_at_gate,
};

const BINARY: &str = "application/grpc-web+proto";
const TEXT: &str = "application/grpc-web-text+proto";
const STALL: Duration = Duration::from_secs(5);

fn range_request(encoding: &str, end: u32) -> Result<Request<Body>> {
    let range = block_range_request(1, u64::from(end));
    let mut wire = vec![0];
    wire.extend_from_slice(&u32::try_from(range.encoded_len())?.to_be_bytes());
    range.encode(&mut wire)?;
    if encoding == TEXT {
        wire = STANDARD.encode(wire).into_bytes();
    }
    Ok(Request::post(format!("/{SERVICE_NAME}/GetBlockRange"))
        .header(header::CONTENT_TYPE, encoding)
        .header(header::ACCEPT, encoding)
        .body(Body::new(Full::new(Bytes::from(wire))))?)
}

async fn next_web_payload(body: &mut Body, encoding: &str, flag: u8) -> Result<Bytes> {
    let mut wire = body
        .frame()
        .await
        .ok_or_else(|| eyre!("stream ended before the next gRPC-Web frame"))??
        .into_data()
        .map_err(|_| eyre!("expected gRPC-Web data in the HTTP body"))?;
    if encoding == TEXT {
        wire = STANDARD.decode(wire)?.into();
    }
    let (prefix, payload) = wire
        .split_at_checked(5)
        .ok_or_else(|| eyre!("short gRPC-Web frame"))?;
    ensure!(prefix[0] == flag, "unexpected gRPC-Web frame flag");
    let length = usize::try_from(u32::from_be_bytes(prefix[1..].try_into()?))?;
    ensure!(payload.len() >= length, "incomplete gRPC-Web frame");
    Ok(wire.slice(5..5 + length))
}

async fn assert_web_status(body: &mut Body, encoding: &str, code: Code) -> Result<()> {
    let trailers = next_web_payload(body, encoding, 0x80).await?;
    let expected = format!("grpc-status:{}", code as i32);
    assert!(
        std::str::from_utf8(&trailers)?
            .lines()
            .any(|line| line == expected)
    );
    assert!(body.frame().await.is_none(), "status must end the stream");
    Ok(())
}

#[derive(Clone, Copy)]
enum EndRange {
    Cancel,
    StopReading,
}

async fn verify_slot_reuse(fixture: &AcceptanceStore, encoding: &str, end: EndRange) -> Result<()> {
    const SECOND_CHUNK_START: u32 = DEFAULT_MAX_COMPACT_BLOCK_RANGE.get() + 1;
    let gate = Arc::new(Semaphore::new(0));
    let reader = EpochPinRecorder::new(fixture.query())
        .with_synthetic_compact_blocks_to(BlockHeight::new(SECOND_CHUNK_START))
        .with_later_chunk_gate(Arc::clone(&gate));
    let adapter = LightwalletdGrpcAdapter::new(reader.clone())
        .with_max_concurrent_block_range_streams(NonZeroU32::MIN)
        .with_block_range_admission_wait(REFUSAL_WAIT);
    let server = ServiceBuilder::new()
        .layer(GrpcWebLayer::new())
        .layer(GrpcSendStallLayer::new(STALL))
        .service(adapter.into_server());

    let wide = server
        .clone()
        .oneshot(range_request(encoding, SECOND_CHUNK_START)?)
        .await?;
    assert_eq!(wide.headers()[header::CONTENT_TYPE], encoding);
    let mut body = wide.into_body();
    let first = CompactBlock::decode(next_web_payload(&mut body, encoding, 0).await?)?;
    assert_eq!(first.height, 1);
    later_chunk_read_at_gate(&reader).await?;

    // Keep a stalled response unread until the next call has used the slot.
    let stale_body = match end {
        EndRange::Cancel => {
            drop(body);
            None
        }
        EndRange::StopReading => {
            tokio::time::advance(STALL + Duration::from_millis(1)).await;
            Some(body)
        }
    };
    let refused = server.clone().oneshot(range_request(encoding, 1)?).await?;
    let status = Status::from_header_map(refused.headers())
        .ok_or_else(|| eyre!("missing browser range refusal"))?;
    assert_eq!(status.code(), Code::Unavailable);
    let delay_ms = status
        .metadata()
        .get("grpc-retry-pushback-ms")
        .ok_or_else(|| eyre!("missing browser range retry pushback"))?
        .to_str()?
        .parse::<i32>()?;
    assert!((500..=1500).contains(&delay_ms));

    gate.add_permits(1);
    let admitted = server.oneshot(range_request(encoding, 1)?).await?;
    let mut next = admitted.into_body();
    let first = CompactBlock::decode(next_web_payload(&mut next, encoding, 0).await?)?;
    assert_eq!(first.height, 1);
    assert_web_status(&mut next, encoding, Code::Ok).await?;

    if let Some(mut stalled) = stale_body {
        assert_web_status(&mut stalled, encoding, Code::DeadlineExceeded).await?;
    }
    Ok(())
}

async fn verify_both_encodings(end: EndRange) -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let fixture = acceptance_store_from_chain(&chain)?;
    for encoding in [BINARY, TEXT] {
        verify_slot_reuse(&fixture, encoding, end).await?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_browser_range_returns_its_only_slot_after_the_outstanding_read() -> Result<()>
{
    verify_both_encodings(EndRange::Cancel).await
}

#[tokio::test(start_paused = true)]
async fn an_unread_browser_range_reports_its_deadline_and_releases_its_only_slot() -> Result<()> {
    verify_both_encodings(EndRange::StopReading).await
}
