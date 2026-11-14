//! Deterministic chain, store, and node fixtures for Zinder tests.
//!
//! `zinder-testkit` is the single source of truth for synthetic chain shapes,
//! tempdir-backed production store pairs, and trait fakes used across the
//! workspace. Tests should reach for these helpers instead of duplicating
//! `synthetic_store()`, `FakeBroadcaster`, or `TestNodeSource` patterns.
//!
//! # Vocabulary
//!
//! - [`ChainFixture`] is a deterministic in-memory chain. It exposes block,
//!   compact-block, source-block, tree-state, and chain-epoch values keyed by
//!   height, plus fork helpers for reorg shape construction.
//! - [`CanonicalWriterFixture`] publishes a [`ChainFixture`] through the
//!   production canonical writer as complete or checkpointed history, then
//!   appends, replaces, and prunes on the admitted primary.
//! - [`WalletServingStoreFixture`] builds the production canonical and wallet
//!   stores, then opens immutable secondaries at one admitted serving fence.
//! - [`process_crash`] runs a child test that aborts at an armed crash point,
//!   so a parent test can reopen the store and assert crash recovery.
//! - [`TransparentTestKey`] signs transparent and shielding spends of a fixed
//!   test seed's outputs, and [`OrchardTestKey`] receives their shielded notes.
//! - [`MockTransactionBroadcaster`] implements
//!   [`zinder_source::TransactionBroadcaster`] with configurable per-call
//!   outcomes and a recording mode that captures calls for later inspection.
//!
//! # DX
//!
//! The crate is `publish = false` and exists purely to give tests a shared
//! vocabulary. Each helper has a small hand-rolled builder API; importing it
//! should never require knowing the layout of any internal struct field.
//!
//! # Boundary contract
//!
//! Every consumer must list `zinder-testkit` under `[dev-dependencies]` only.
//! Items in this crate (notably [`TransparentTestKey`], which uses
//! `zcash_primitives::transaction::builder::Builder::mock_build`) are
//! unsuitable for production signing or production transaction handling, and
//! the dev-dep-only rule is what keeps them out of release binaries. The CI
//! workspace gate (`cargo deny check`) enforces no production crate links
//! against this one.

pub mod canonical_writer_fixture;
pub mod chain_fixture;
pub mod commitment_tree_fixtures;
pub mod env_var_contract;
pub mod ingest_control_fixture;
pub mod json_rpc_test_server;
pub mod live;
pub mod log_capture;
pub mod materialized_view_fixture;
pub mod mock_mempool_source;
pub mod mock_transaction_broadcaster;
pub mod network_upgrade_fixtures;
pub mod orchard_test_key;
pub mod process_crash;
pub mod transparent_signer;
pub mod wallet_serving_store_fixture;

pub use canonical_writer_fixture::{CanonicalBaseline, CanonicalWriterFixture};
pub use chain_fixture::{
    ChainFixture, FixtureBlock, FixtureTransactionRows, build_fixture_transaction_rows,
    encode_fixture_block_replay, encode_fixture_block_replay_with_raw_block,
    synthetic_transaction_public_facts,
};
pub use commitment_tree_fixtures::{
    FIXTURE_NOTE_COMMITMENT, completed_sapling_subtree_frontier, completed_subtree_frontier,
    one_leaf_frontier, one_leaf_sapling_frontier,
};
pub use env_var_contract::{EnvVarContractProbe, wallet_serving_entrypoint_sections};
pub use ingest_control_fixture::{IngestControlFixture, IngestControlFixtureService};
pub use json_rpc_test_server::{
    JsonRpcRequest, JsonRpcStub, JsonRpcStubBuilder, JsonRpcTestServer, RpcReply, method,
};
pub use log_capture::{CapturedEvent, LogCapture};
pub use materialized_view_fixture::{
    MaterializedViewFixtureError, open_test_materialized_view_store_for_canonical,
    seed_transparent_outpoint_spends,
};
pub use mock_mempool_source::{
    MockMempoolSource, MockMempoolSourceClosed, MockMempoolSourceControl,
};
pub use mock_transaction_broadcaster::MockTransactionBroadcaster;
pub use network_upgrade_fixtures::{
    local_network_from_activations, sample_regtest_upgrade_activations,
};
pub use orchard_test_key::{OrchardAddress, OrchardTestKey, regtest_unified_address};
pub use transparent_signer::{
    LocalNetwork, P2pkhShieldingSpendArgs, P2pkhSpendArgs, ShieldedOutputPool,
    TRANSPARENT_BROADCAST_TEST_SEED, TransparentAddress, TransparentSignerError,
    TransparentTestKey, ZIP317_FEE_ONE_IN_ONE_OUT_ZATS, regtest_local_network,
};
pub use wallet_serving_store_fixture::WalletServingStoreFixture;
