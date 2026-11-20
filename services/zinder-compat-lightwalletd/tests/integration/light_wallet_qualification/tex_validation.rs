use eyre::{Result, eyre};
use tonic::{Code, Request};
use tonic_types::StatusExt as _;
use zebra_chain::{parameters::NetworkKind, transparent::Address};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_server::CompactTxStreamer as _,
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
