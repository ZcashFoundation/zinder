//! Public lightwalletd operator parity assertions.
//!
//! `zec.rocks`-style operators expose the lightwalletd compat surface to
//! third-party clients. These compile-time assertions ensure the typed
//! `ChainIndex` methods that back compat surfaces remain on the trait.
//!
//! Cross-references: [Integration surfaces](../../../../docs/reference/integration-surfaces.md).

use tokio_stream::StreamExt as _;
use tonic::Request;
use zinder_client::{ChainIndex, EndpointBackedIndex, RemoteChainIndex};
use zinder_proto::compat::lightwalletd::{self, compact_tx_streamer_server::CompactTxStreamer};

use super::{
    address_history_filter, build_transparent_address_adapter,
    build_transparent_address_serving_fixture,
};

#[test]
fn parity_chain_index_surface_compiles_for_lightwalletd_operators() {
    fn assert_base_compiles<T: ChainIndex>() {
        // GetBlock and GetTreeState hash-only paths via BlockSelector
        let _ = T::block_id_by_selector;
        // GetSubtreeRoots typed bytes + typed pool enum
        let _ = T::subtree_roots_in_range;
    }
    fn assert_endpoint_compiles<T: EndpointBackedIndex>() {
        // SendTransaction typed bytes path
        let _ = T::broadcast_transaction;
        // ChainCommitted typed signal for the live chain-event stream
        let _ = T::chain_events;
    }
    assert_base_compiles::<RemoteChainIndex>();
    assert_endpoint_compiles::<RemoteChainIndex>();
}

#[tokio::test]
async fn serves_public_transparent_address_shape_from_production_pair() -> eyre::Result<()> {
    let mut fixture = build_transparent_address_serving_fixture()?;
    let adapter = build_transparent_address_adapter(&mut fixture)?;
    let utxo_request = lightwalletd::GetAddressUtxosArg {
        addresses: vec![fixture.address.clone()],
        start_height: 1,
        max_entries: 10,
    };
    let utxo_list = adapter
        .get_address_utxos(Request::new(utxo_request.clone()))
        .await?
        .into_inner();
    let mut utxo_stream = adapter
        .get_address_utxos_stream(Request::new(utxo_request))
        .await?
        .into_inner();
    let first_streamed_utxo = utxo_stream
        .next()
        .await
        .ok_or_else(|| eyre::eyre!("missing streamed UTXO"))??;
    let history_filter = address_history_filter(fixture.address.clone());
    let mut txids = adapter
        .get_taddress_txids(Request::new(history_filter.clone()))
        .await?
        .into_inner();
    let mut transactions = adapter
        .get_taddress_transactions(Request::new(history_filter))
        .await?
        .into_inner();

    assert_eq!(utxo_list.address_utxos, vec![first_streamed_utxo.clone()]);
    assert!(utxo_stream.next().await.is_none());
    assert_eq!(first_streamed_utxo.address, fixture.address);
    assert_eq!(first_streamed_utxo.txid, fixture.transaction_id.as_bytes());
    assert_eq!(first_streamed_utxo.index, 0);
    assert_eq!(first_streamed_utxo.script, fixture.script_pub_key);
    assert_eq!(first_streamed_utxo.value_zat, fixture.value_zat);
    assert_eq!(
        first_streamed_utxo.height,
        u64::from(fixture.block_height.value())
    );

    let txid_response = txids
        .next()
        .await
        .ok_or_else(|| eyre::eyre!("missing transparent txid response"))??;
    let transaction_response = transactions
        .next()
        .await
        .ok_or_else(|| eyre::eyre!("missing transparent transaction response"))??;
    assert_eq!(txid_response.data, fixture.raw_transaction_bytes);
    assert_eq!(transaction_response.data, fixture.raw_transaction_bytes);
    assert_eq!(
        txid_response.height,
        u64::from(fixture.block_height.value())
    );
    assert_eq!(
        transaction_response.height,
        u64::from(fixture.block_height.value())
    );
    assert!(txids.next().await.is_none());
    assert!(transactions.next().await.is_none());
    Ok(())
}
