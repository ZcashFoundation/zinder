//! Block-explorer parity assertions.
//!
//! Block explorers exercise typed `WalletQuery` and federated
//! `explorer.*` surfaces to display per-block, per-transaction, and
//! per-address state. These compile-time assertions ensure the trait surface
//! they depend on stays intact through future refactors.

use zinder_client::{ChainIndex, EndpointBackedIndex, RemoteChainIndex};

#[test]
fn parity_chain_index_surface_compiles_for_block_explorers() {
    fn assert_base_compiles<T: ChainIndex>() {
        // hash-or-height lookup via BlockSelector
        let _ = T::block_id_by_selector;
        // typed block-header read model
        let _ = T::block_header_by_selector;
        // typed TxStatus with mined / mempool / not found
        let _ = T::transaction_by_id;
    }
    fn assert_endpoint_compiles<T: EndpointBackedIndex>() {
        // bounded live-mempool enumeration
        let _ = T::mempool_snapshot;
        // chain-wide value-pool totals at the upstream tip
        let _ = T::chain_value_pools_at_tip;
    }
    assert_base_compiles::<RemoteChainIndex>();
    assert_endpoint_compiles::<RemoteChainIndex>();
}
