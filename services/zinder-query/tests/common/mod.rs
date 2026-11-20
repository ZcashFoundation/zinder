//! Test-support helpers shared across `zinder-query` integration tests.
//!
//! Each top-level test file pulls these in with `mod support;` and uses
//! `support::*` so synthetic chain-epoch shapes stay consistent.

#![allow(
    dead_code,
    reason = "Each test file uses a subset of these helpers; the full surface is shared."
)]
#![allow(
    unreachable_pub,
    reason = "Items are reachable via `mod support;` from each test binary."
)]

use std::sync::Arc;
use zinder_core::Network;

use zinder_query::{
    AdmittedIngestControl, OrderedBroadcaster, RocksDbCanonicalReader, RocksDbWalletReader,
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_testkit::{
    ChainFixture, IngestControlFixture, MockTransactionBroadcaster, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

/// Admits the standard in-process ingest-control identity for serving-query tests.
pub async fn admitted_ingest_control_fixture()
-> eyre::Result<(AdmittedIngestControl, IngestControlFixture)> {
    let fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let ingest_control =
        AdmittedIngestControl::connect(fixture.endpoint(), None, Network::ZcashRegtest).await?;
    Ok((ingest_control, fixture))
}

/// Admits the fixture's immutable readers as one exact serving-pair slot.
pub fn serving_pair_slot(
    store_fixture: &mut WalletServingStoreFixture,
) -> eyre::Result<WalletServingPairSlot> {
    let (canonical, wallet) = store_fixture.take_readers()?;
    Ok(WalletServingPairSlot::new(Arc::new(
        WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?,
    )))
}

/// Builds the release serving query, without a node source, over `chain`
/// published as one baseline epoch.
pub fn serving_query_over(
    chain: &ChainFixture,
) -> eyre::Result<(WalletServingQuery<()>, WalletServingStoreFixture)> {
    serving_query_from(WalletServingStoreFixture::from_chain(
        chain,
        &sample_regtest_upgrade_activations(),
    )?)
}

/// Builds the release serving query, without a node source, over the
/// readers of `store_fixture`.
pub fn serving_query_from(
    mut store_fixture: WalletServingStoreFixture,
) -> eyre::Result<(WalletServingQuery<()>, WalletServingStoreFixture)> {
    let query = WalletServingQuery::from_serving_pair_slot(serving_pair_slot(&mut store_fixture)?);
    Ok((query, store_fixture))
}

/// Native serving query, its store fixture, and the ingest-control fixture
/// that must outlive it.
pub type NativeServingQueryFixture = (
    WalletServingQuery<(), AdmittedIngestControl>,
    WalletServingStoreFixture,
    IngestControlFixture,
);

/// Builds the native release query over `chain`, admitted against the
/// standard in-process ingest-control fixture.
pub async fn native_serving_query_over(
    chain: &ChainFixture,
) -> eyre::Result<NativeServingQueryFixture> {
    native_serving_query_from(WalletServingStoreFixture::from_chain(
        chain,
        &sample_regtest_upgrade_activations(),
    )?)
    .await
}

/// Builds the native release query over the readers of `store_fixture`,
/// admitted against the standard in-process ingest-control fixture.
pub async fn native_serving_query_from(
    mut store_fixture: WalletServingStoreFixture,
) -> eyre::Result<NativeServingQueryFixture> {
    let (ingest_control, ingest_control_fixture) = admitted_ingest_control_fixture().await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        serving_pair_slot(&mut store_fixture)?,
        ingest_control,
    );
    Ok((query, store_fixture, ingest_control_fixture))
}

/// Builds the release serving query, without a node source, over `chain`
/// whose final block was published as a second, live-appended epoch.
pub fn serving_query_after_live_append(
    chain: &ChainFixture,
) -> eyre::Result<(WalletServingQuery<()>, WalletServingStoreFixture)> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture =
        WalletServingStoreFixture::from_chain_after_live_append(chain, &activations)?;
    let query = WalletServingQuery::from_serving_pair_slot(serving_pair_slot(&mut store_fixture)?);
    Ok((query, store_fixture))
}

/// Release serving query that broadcasts through a verified node double.
pub type NodeBroadcastQuery =
    WalletServingQuery<Option<OrderedBroadcaster<MockTransactionBroadcaster>>>;

/// Builds the release serving query over `chain` with `broadcaster` as its
/// node, verified against the served pair.
pub async fn verified_node_query_over(
    chain: &ChainFixture,
    broadcaster: MockTransactionBroadcaster,
) -> eyre::Result<(NodeBroadcastQuery, WalletServingStoreFixture)> {
    let activations = sample_regtest_upgrade_activations();
    let mut store_fixture = WalletServingStoreFixture::from_chain(chain, &activations)?;
    let slot = serving_pair_slot(&mut store_fixture)?;
    let node = OrderedBroadcaster::new(broadcaster.with_node_activations(activations), []);
    node.verify(&slot).await?;
    let query = WalletServingQuery::from_serving_pair_slot(slot).with_node_broadcast(Some(node));
    Ok((query, store_fixture))
}
