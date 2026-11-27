#![allow(
    missing_docs,
    reason = "Integration test names describe the pre-decode request bounds under test."
)]

use eyre::{Result, eyre};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Status, transport::Server};
use zinder_core::{MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST, Network};
use zinder_proto::{
    ZINDER_V1_FILE_DESCRIPTOR_SET,
    v1::wallet::{self, wallet_query_client::WalletQueryClient},
};
use zinder_query::{WALLET_QUERY_FIELD_LIMITS, WalletEndpointMetadata, WalletQueryGrpcAdapter};
use zinder_runtime::{MAX_DECODING_MESSAGE_BYTES, RepeatedElementEncoding};
use zinder_store::{EventStreamStartPosition, event_stream_start_message};
use zinder_testkit::{ChainFixture, RepeatedRequestField, repeated_request_fields};

use crate::common::native_serving_query_over;

const WALLET_QUERY: &str = "zinder.v1.wallet.WalletQuery";

/// Empty `string` or message elements that fill one request frame: each takes
/// two bytes on the wire and at least 24 once decoded.
const EMPTY_ELEMENTS_PER_FRAME: usize = (MAX_DECODING_MESSAGE_BYTES - 64) / 2;

#[test]
fn field_limits_cover_every_repeated_request_field() -> Result<()> {
    let mut bounded: Vec<RepeatedRequestField> = WALLET_QUERY_FIELD_LIMITS
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
        repeated_request_fields(ZINDER_V1_FILE_DESCRIPTOR_SET, WALLET_QUERY)?,
    );
    Ok(())
}

#[tokio::test]
async fn a_chain_events_request_of_empty_addresses_is_refused_before_decoding() -> Result<()> {
    let (mut client, _fixtures) = spawn_wallet_query().await?;

    let refusal = client
        .chain_events(chain_events_request(vec![
            String::new();
            EMPTY_ELEMENTS_PER_FRAME
        ]))
        .await
        .err();

    assert_refused_before_decoding(refusal.as_ref(), "ChainEvents", "address_filter")
}

#[tokio::test]
async fn an_outpoint_request_of_empty_outpoints_is_refused_before_decoding() -> Result<()> {
    let (mut client, _fixtures) = spawn_wallet_query().await?;

    let refusal = client
        .transparent_outpoint_spend_status(wallet::TransparentOutpointSpendStatusRequest {
            outpoints: vec![wallet::OutPoint::default(); EMPTY_ELEMENTS_PER_FRAME],
            at_epoch_id: None,
        })
        .await
        .err();

    assert_refused_before_decoding(
        refusal.as_ref(),
        "TransparentOutpointSpendStatus",
        "outpoints",
    )
}

#[tokio::test]
async fn an_outpoint_request_just_over_the_handler_cap_gets_the_typed_refusal() -> Result<()> {
    let (mut client, _fixtures) = spawn_wallet_query().await?;

    let refusal = client
        .transparent_outpoint_spend_status(wallet::TransparentOutpointSpendStatusRequest {
            outpoints: vec![
                wallet::OutPoint::default();
                MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST + 1
            ],
            at_epoch_id: None,
        })
        .await
        .err()
        .ok_or_else(|| eyre!("an outpoint list over the cap must be refused"))?;

    assert_eq!(refusal.code(), Code::InvalidArgument);
    assert!(
        !refusal.message().contains("elements; at most"),
        "the handler, not the pre-decode bound, must refuse: {refusal:?}",
    );
    Ok(())
}

#[tokio::test]
async fn chain_events_streams_still_open_and_deliver_events() -> Result<()> {
    let (mut client, _fixtures) = spawn_wallet_query().await?;

    // A filter matching no commit opens a stream that stays open and idle.
    let filtered = client
        .chain_events(chain_events_request(vec![
            "tmBsTi2xWTjUdEXnuTceL7fecEQKeWaPDJd".to_owned(),
        ]))
        .await;
    let mut events = client
        .chain_events(chain_events_request(Vec::new()))
        .await?
        .into_inner();
    let first_event = events.message().await?;

    assert!(
        filtered.is_ok(),
        "the filtered stream was refused: {filtered:?}"
    );
    assert!(first_event.is_some(), "the stream closed before any event");
    Ok(())
}

fn chain_events_request(address_filter: Vec<String>) -> wallet::ChainEventsRequest {
    wallet::ChainEventsRequest {
        start: Some(event_stream_start_message(
            &EventStreamStartPosition::EarliestRetained,
        )),
        family: wallet::ChainEventStreamFamily::Visible as i32,
        address_filter,
    }
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

async fn spawn_wallet_query() -> Result<(
    WalletQueryClient<tonic::transport::Channel>,
    impl Sized + use<>,
)> {
    let (wallet_query, store_fixture, ingest_control_fixture) =
        native_serving_query_over(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(2))
            .await?;
    let server =
        WalletQueryGrpcAdapter::new(wallet_query, WalletEndpointMetadata::default()).into_server();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    tokio::spawn(
        Server::builder()
            .add_service(server)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let client = WalletQueryClient::connect(format!("http://{server_addr}")).await?;
    Ok((client, (store_fixture, ingest_control_fixture)))
}

#[tokio::test]
async fn address_batch_requests_are_refused_before_decoding_above_the_wire_bound() -> Result<()> {
    let (mut client, _fixtures) = spawn_wallet_query().await?;
    let addresses = vec![
        wallet::AddressLookup::default();
        2 * zinder_core::MAX_TRANSPARENT_ADDRESSES_PER_BATCH + 1
    ];
    let balance_refusal = client
        .transparent_address_balances(wallet::TransparentAddressBalancesRequest {
            addresses: addresses.clone(),
            at_epoch_id: None,
        })
        .await
        .err();
    assert_refused_before_decoding(
        balance_refusal.as_ref(),
        "TransparentAddressBalances",
        "addresses",
    )?;
    let output_refusal = client
        .transparent_addresses_unspent_outputs(wallet::TransparentAddressesUnspentOutputsRequest {
            addresses,
            start_height: 1,
            at_epoch_id: None,
        })
        .await
        .err();
    assert_refused_before_decoding(
        output_refusal.as_ref(),
        "TransparentAddressesUnspentOutputs",
        "addresses",
    )
}
