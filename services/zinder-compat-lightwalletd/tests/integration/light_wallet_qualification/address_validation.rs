use eyre::{Result, eyre};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code, Request, Status,
    transport::{Channel, Server},
};
use tonic_types::StatusExt as _;
use zebra_chain::{parameters::NetworkKind, transparent::Address};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::CompactTxStreamer as _,
};

use super::{ADDRESS, Fixture};

#[tokio::test]
async fn a_tex_address_returns_wallet_validation_errors_before_contacting_the_node() -> Result<()> {
    let hash = zinder_source::parse_transparent_address(ADDRESS)?.hash_bytes();
    let tex = Address::from_tex(NetworkKind::Regtest, hash).to_string();
    let fixture = Fixture::new([])?;
    let addresses = vec![ADDRESS.to_owned(), tex];
    let reason = "transparent address does not produce a receivable script";
    let balance = fixture
        .adapter
        .get_taddress_balance(Request::new(lightwalletd::AddressList {
            addresses: addresses.clone(),
        }))
        .await
        .err()
        .ok_or_else(|| eyre!("accepted a balance request containing TEX"))?;
    assert_eq!(balance.code(), Code::InvalidArgument);
    let details = balance.get_error_details();
    let violation = details
        .bad_request()
        .and_then(|bad_request| bad_request.field_violations.first());
    assert!(
        matches!(violation, Some(field) if field.field == "address" && field.description == reason)
    );

    let utxos = fixture
        .adapter
        .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg {
            addresses,
            ..Default::default()
        }))
        .await
        .err()
        .ok_or_else(|| eyre!("accepted a UTXO request containing TEX"))?;
    assert_eq!(utxos.code(), Code::InvalidArgument);
    assert_eq!(utxos.message(), reason);
    assert!(fixture.node.requests()?.is_empty());
    Ok(())
}

async fn rejected_balances(
    client: &mut CompactTxStreamerClient<Channel>,
    address: &str,
) -> Result<[Status; 2]> {
    let addresses = vec![ADDRESS.to_owned(), address.to_owned()];
    let unary = client
        .get_taddress_balance(lightwalletd::AddressList {
            addresses: addresses.clone(),
        })
        .await
        .err()
        .ok_or_else(|| eyre!("unary balance accepted invalid input"))?;
    let input = addresses
        .into_iter()
        .map(|address| lightwalletd::Address { address });
    let streamed = client
        .get_taddress_balance_stream(tokio_stream::iter(input))
        .await
        .err()
        .ok_or_else(|| eyre!("streamed balance accepted invalid input"))?;
    Ok([unary, streamed])
}

#[tokio::test]
async fn invalid_balance_addresses_match_wallet_status_details_over_grpc() -> Result<()> {
    let fixture = Fixture::new([])?;
    let (mut store, _) = super::super::transparent_wallet_serving::many_utxo_fixture(1, 1)?;
    let wallet =
        super::super::transparent_wallet_serving::build_wallet_serving_adapter(&mut store)?;
    let wallet_listener = TcpListener::bind("127.0.0.1:0").await?;
    let wallet_url = format!("http://{}", wallet_listener.local_addr()?);
    let wallet_server = Server::builder()
        .add_service(wallet.into_server())
        .serve_with_incoming(TcpListenerStream::new(wallet_listener));
    let qual_listener = TcpListener::bind("127.0.0.1:0").await?;
    let qual_url = format!("http://{}", qual_listener.local_addr()?);
    let qual_server = Server::builder()
        .add_service(fixture.adapter.into_server())
        .serve_with_incoming(TcpListenerStream::new(qual_listener));

    let compare = async {
        let mut wallet = CompactTxStreamerClient::connect(wallet_url).await?;
        let mut qual = CompactTxStreamerClient::connect(qual_url).await?;
        for address in [
            "invalid!".to_owned(),
            "1".repeat(2048),
            Address::from_pub_key_hash(NetworkKind::Mainnet, [1; 20]).to_string(),
            Address::from_tex(NetworkKind::Regtest, [1; 20]).to_string(),
        ] {
            let expected = rejected_balances(&mut wallet, &address).await?;
            let actual = rejected_balances(&mut qual, &address).await?;
            for (actual, expected) in actual.into_iter().zip(expected) {
                assert_eq!(expected.code(), Code::InvalidArgument);
                let details = expected.get_error_details();
                assert!(
                    details
                        .error_info()
                        .is_some_and(|e| e.reason == "INVALID_ADDRESS")
                );
                assert!(
                    details
                        .bad_request()
                        .is_some_and(|e| e.field_violations.iter().any(|v| v.field == "address"))
                );
                assert_eq!(actual.code(), expected.code());
                assert_eq!(actual.message(), expected.message());
                assert_eq!(actual.details(), expected.details());
            }
        }
        assert!(fixture.node.requests()?.is_empty());
        Ok(())
    };
    tokio::select! {
        result = wallet_server => Err(eyre!("wallet RPC server stopped: {result:?}")),
        result = qual_server => Err(eyre!("qualification RPC server stopped: {result:?}")),
        result = tokio::time::timeout(std::time::Duration::from_secs(20), compare) => result?,
    }
}
