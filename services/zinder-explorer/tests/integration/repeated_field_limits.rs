#![allow(
    missing_docs,
    reason = "Integration test names describe the pre-decode request bounds under test."
)]

use eyre::{Result, eyre};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Status, transport::Server};
use zinder_core::Network;
use zinder_explorer::{
    EXPLORER_QUERY_FIELD_LIMITS, ExplorerQueryGrpcAdapter, ExplorerServerInfoSettings,
};
use zinder_proto::{
    ZINDER_V1_FILE_DESCRIPTOR_SET,
    v1::explorer::{
        PrivacyShape, TransactionHistoryFilter, TransactionHistoryRequest,
        ValuePoolFlowAmountThresholdSummaryRequest, ValuePoolFlowFilter,
        ValuePoolFlowHistoryRequest, ValuePoolFlowPool, ValuePoolFlowSummaryRequest,
        explorer_query_client::ExplorerQueryClient,
    },
};
use zinder_runtime::{MAX_DECODING_MESSAGE_BYTES, RepeatedElementEncoding};
use zinder_testkit::{RepeatedRequestField, repeated_request_fields};

const EXPLORER_QUERY: &str = "zinder.v1.explorer.ExplorerQuery";

/// Packed one-byte varint elements that fill one request frame: each takes one
/// byte on the wire and four or eight once decoded.
const PACKED_VARINTS_PER_FRAME: usize = MAX_DECODING_MESSAGE_BYTES - 64;

#[test]
fn field_limits_cover_every_repeated_request_field() -> Result<()> {
    let mut bounded: Vec<RepeatedRequestField> = EXPLORER_QUERY_FIELD_LIMITS
        .iter()
        .flat_map(|method| {
            method.fields().iter().map(|field| RepeatedRequestField {
                method: method.method().to_owned(),
                field_name: field.field_name().to_owned(),
                field_path: field.field_path().to_vec(),
                varint_elements: field.encoding() == RepeatedElementEncoding::Varint,
            })
        })
        .collect();
    bounded.sort();

    assert_eq!(
        bounded,
        repeated_request_fields(ZINDER_V1_FILE_DESCRIPTOR_SET, EXPLORER_QUERY)?,
    );
    Ok(())
}

#[tokio::test]
async fn a_flow_history_filter_of_packed_pools_is_refused_before_decoding() -> Result<()> {
    let mut client = spawn_explorer().await?;

    let refusal = client
        .value_pool_flow_history(ValuePoolFlowHistoryRequest {
            filter: Some(ValuePoolFlowFilter {
                pools: vec![ValuePoolFlowPool::Sapling as i32; PACKED_VARINTS_PER_FRAME],
                ..ValuePoolFlowFilter::default()
            }),
            ..ValuePoolFlowHistoryRequest::default()
        })
        .await
        .err();

    assert_refused_before_decoding(refusal.as_ref(), "ValuePoolFlowHistory", "filter.pools")
}

#[tokio::test]
async fn a_transaction_history_filter_of_packed_shapes_is_refused_before_decoding() -> Result<()> {
    let mut client = spawn_explorer().await?;

    let refusal = client
        .transaction_history(TransactionHistoryRequest {
            filter: Some(TransactionHistoryFilter {
                privacy_shapes: vec![
                    PrivacyShape::TransparentOnly as i32;
                    PACKED_VARINTS_PER_FRAME
                ],
                ..TransactionHistoryFilter::default()
            }),
            ..TransactionHistoryRequest::default()
        })
        .await
        .err();

    assert_refused_before_decoding(
        refusal.as_ref(),
        "TransactionHistory",
        "filter.privacy_shapes",
    )
}

#[tokio::test]
async fn a_threshold_summary_of_packed_amounts_is_refused_before_decoding() -> Result<()> {
    let mut client = spawn_explorer().await?;

    let refusal = client
        .value_pool_flow_amount_threshold_summary(ValuePoolFlowAmountThresholdSummaryRequest {
            minimum_amounts_zat: vec![0; PACKED_VARINTS_PER_FRAME],
            ..ValuePoolFlowAmountThresholdSummaryRequest::default()
        })
        .await
        .err();

    assert_refused_before_decoding(
        refusal.as_ref(),
        "ValuePoolFlowAmountThresholdSummary",
        "minimum_amounts_zat",
    )
}

#[tokio::test]
async fn requests_within_the_bounds_reach_the_handler() -> Result<()> {
    let mut client = spawn_explorer().await?;

    let summary = client
        .value_pool_flow_summary(ValuePoolFlowSummaryRequest {
            pools: vec![
                ValuePoolFlowPool::Sapling as i32,
                ValuePoolFlowPool::Orchard as i32,
            ],
            ..ValuePoolFlowSummaryRequest::default()
        })
        .await;
    let thresholds = client
        .value_pool_flow_amount_threshold_summary(ValuePoolFlowAmountThresholdSummaryRequest {
            minimum_amounts_zat: (0..33).collect(),
            ..ValuePoolFlowAmountThresholdSummaryRequest::default()
        })
        .await;

    // Without materialized views the handlers refuse, but only after decoding.
    for response in [summary.err(), thresholds.err()] {
        let refusal = response.ok_or_else(|| eyre!("an explorer without views answered"))?;
        assert!(
            !refusal.message().contains("elements; at most"),
            "the handler, not the pre-decode bound, must refuse: {refusal:?}",
        );
    }
    Ok(())
}

fn assert_refused_before_decoding(
    refusal: Option<&Status>,
    method: &str,
    field_name: &str,
) -> Result<()> {
    let refusal = refusal.ok_or_else(|| eyre!("{method} flood was accepted"))?;
    assert_eq!(refusal.code(), Code::InvalidArgument, "{refusal:?}");
    let expected_prefix = format!("{method} request field {field_name} carries ");
    assert!(
        refusal.message().starts_with(&expected_prefix),
        "expected a pre-decode refusal starting {expected_prefix:?}: {refusal:?}",
    );
    Ok(())
}

async fn spawn_explorer() -> Result<ExplorerQueryClient<tonic::transport::Channel>> {
    let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings {
        network: Network::ZcashRegtest,
    });
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    tokio::spawn(
        Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    Ok(ExplorerQueryClient::connect(format!("http://{server_addr}")).await?)
}
