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

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use zinder_core::{
    BlockHash, BlockHeaderArtifact, BlockHeight, BlockId, ChainEpoch, ChainEpochId,
    ChainTipMetadata, CompactBlockArtifact, CompactChainMetadata, Network, UnixTimestampMillis,
};

use async_trait::async_trait;
use zinder_core::{RawTransactionBytes, TransactionBroadcastOutcome};
use zinder_query::{
    AdmittedIngestControl, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSource, SourceBlock, SourceError, SourceTreeState,
    TransactionBroadcaster, TreeStateUpstream,
};
use zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION;
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
        WalletServingReadPair::new(Arc::new(canonical), Arc::new(wallet))?,
    )))
}

/// Builds the release serving query, without a node source, over `chain`
/// published as one baseline epoch.
pub fn serving_query_over(
    chain: &ChainFixture,
) -> eyre::Result<(WalletServingQuery<()>, WalletServingStoreFixture)> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(chain, &activations)?;
    let query = WalletServingQuery::from_serving_pair_slot(
        serving_pair_slot(&mut store_fixture)?,
        (),
        activations,
    );
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
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(chain, &activations)?;
    let (ingest_control, ingest_control_fixture) = admitted_ingest_control_fixture().await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        serving_pair_slot(&mut store_fixture)?,
        (),
        ingest_control,
        activations,
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
    let query = WalletServingQuery::from_serving_pair_slot(
        serving_pair_slot(&mut store_fixture)?,
        (),
        activations,
    );
    Ok((query, store_fixture))
}

/// Builds the release serving query over `chain` with a probed node source.
pub fn probed_serving_query_over(
    chain: &ChainFixture,
    node: ProbedNode,
) -> eyre::Result<(WalletServingQuery<ProbedNode>, WalletServingStoreFixture)> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(chain, &activations)?;
    let query = WalletServingQuery::from_probed_node_source(
        serving_pair_slot(&mut store_fixture)?,
        node,
        activations,
    )?;
    Ok((query, store_fixture))
}

/// Probed node double that serves one fixed tree state and delegates
/// broadcasts to a [`MockTransactionBroadcaster`].
#[derive(Clone)]
pub struct ProbedNode {
    capabilities: NodeCapabilities,
    tip: BlockId,
    tree_state: Option<SourceTreeState>,
    tree_state_fetch_count: Arc<AtomicUsize>,
    broadcaster: MockTransactionBroadcaster,
}

impl ProbedNode {
    /// Advertises tree-state fill and answers every fill with `tree_state`.
    pub fn with_tree_state(tip: BlockId, tree_state: SourceTreeState) -> eyre::Result<Self> {
        Ok(Self {
            capabilities: NodeCapabilities::new([
                NodeCapability::OpenRpcDiscovery,
                NodeCapability::TipId,
                NodeCapability::TreeState,
            ])?,
            tip,
            tree_state: Some(tree_state),
            tree_state_fetch_count: Arc::new(AtomicUsize::new(0)),
            broadcaster: MockTransactionBroadcaster::broadcast_disabled(),
        })
    }

    /// Advertises transaction broadcast and forwards it to `broadcaster`.
    pub fn with_broadcaster(
        tip: BlockId,
        broadcaster: MockTransactionBroadcaster,
    ) -> eyre::Result<Self> {
        Ok(Self {
            capabilities: NodeCapabilities::new([
                NodeCapability::OpenRpcDiscovery,
                NodeCapability::TipId,
                NodeCapability::TransactionBroadcast,
            ])?,
            tip,
            tree_state: None,
            tree_state_fetch_count: Arc::new(AtomicUsize::new(0)),
            broadcaster,
        })
    }

    /// Returns how many tree-state fills reached this node.
    pub fn tree_state_fetch_count(&self) -> usize {
        self.tree_state_fetch_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl NodeSource for ProbedNode {
    fn capabilities(&self) -> NodeCapabilities {
        self.capabilities
    }

    async fn fetch_block_at(&self, _height: BlockHeight) -> Result<SourceBlock, SourceError> {
        Err(SourceError::NodeUnavailable {
            reason: "probed query test node does not serve blocks".to_owned(),
        })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        Ok(self.tip)
    }
}

#[async_trait]
impl TreeStateUpstream for ProbedNode {
    async fn fetch_tree_state_for_block(
        &self,
        _block_id: BlockId,
    ) -> Result<SourceTreeState, SourceError> {
        self.tree_state_fetch_count.fetch_add(1, Ordering::SeqCst);
        self.tree_state
            .clone()
            .ok_or(SourceError::NodeCapabilityMissing {
                capability: NodeCapability::TreeState,
            })
    }
}

#[async_trait]
impl TransactionBroadcaster for ProbedNode {
    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, SourceError> {
        self.broadcaster
            .broadcast_transaction(raw_transaction)
            .await
    }
}

/// Builds a block hash whose 32 bytes repeat `seed` as four big-endian u32 chunks.
#[must_use]
pub fn block_hash_from_seed(seed: u32) -> BlockHash {
    let mut bytes = [0; 32];
    for chunk in bytes.chunks_exact_mut(4) {
        chunk.copy_from_slice(&seed.to_be_bytes());
    }
    BlockHash::from_bytes(bytes)
}

/// Returns the deterministic raw block bytes used by synthetic query fixtures.
#[must_use]
pub fn synthetic_raw_block_bytes(height: u32) -> Vec<u8> {
    format!("raw-block-{height}").into_bytes()
}

/// Creates a synthetic single-block chain epoch with deterministic identifiers.
#[must_use]
pub fn synthetic_chain_epoch(
    chain_epoch_id: u64,
    height: u32,
) -> (ChainEpoch, BlockHeaderArtifact, CompactBlockArtifact) {
    let source_hash = block_hash_from_seed(height);
    let parent_hash = block_hash_from_seed(height.saturating_sub(1));
    let block_height = BlockHeight::new(height);

    (
        ChainEpoch {
            id: ChainEpochId::new(chain_epoch_id),
            network: Network::ZcashRegtest,
            visible_tip_height: block_height,
            visible_tip_hash: source_hash,
            settled_tip_height: block_height,
            settled_tip_hash: source_hash,
            artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(1_774_668_300_000 + u64::from(height)),
        },
        BlockHeaderArtifact::new(
            block_height,
            source_hash,
            parent_hash,
            [0; 32],
            [0; 32],
            0,
            0,
            [0; 32],
            0,
            u64::try_from(synthetic_raw_block_bytes(height).len()).unwrap_or(u64::MAX),
        ),
        CompactBlockArtifact::empty(
            BlockId::new(block_height, source_hash),
            parent_hash,
            0,
            CompactChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        ),
    )
}
