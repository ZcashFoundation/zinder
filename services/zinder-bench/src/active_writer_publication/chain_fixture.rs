//! Deterministic Regtest chain fixture generator and advancing [`NodeSource`].
//!
//! Builds sparse, Orchard-dense, and transparent coinbase-spend blocks crossing
//! a configurable segment boundary plus reusable base stores and an advancing
//! [`NodeSource`] with RPC counters.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use zebra_chain::{
    block::{Block, merkle::Root as MerkleRoot},
    parameters::NetworkKind,
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::Transaction,
    transparent::Address as ZebraTransparentAddress,
};
use zinder_core::{
    BlockHeight, BlockId, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontiers, ConsensusBranchId, Network, NetworkUpgradeActivation,
    NetworkUpgradeActivations, ShieldedProtocol, SubtreeRootArtifact, SubtreeRootIndex,
    SubtreeRootRange, UnixTimestampMillis,
};
use zinder_ingest::{
    CanonicalConstructionConfig, RawBlobPolicy, load_fresh_canonical, position_canonical_block,
    prepare_canonical_block,
};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceChainSegment, SourceChainSegmentLimits,
    SourceError, SourceSubtreeRoot, SourceSubtreeRoots,
};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
    CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder, RocksDbIoMode,
    RocksDbResourceBudget, RocksDbStatisticsLevel,
};
use zinder_wallet_rocksdb::{RocksDbWalletBuildOptions, build_wallet_from_canonical};

use super::{ActiveWriterPublicationConfig, report::NodeRpcCounts};
use crate::BenchError;

const REGTEST_BLOCK_1_JSON: &str =
    include_str!("../../../zinder-ingest/tests/fixtures/z3-regtest-block-1.json");
const ORCHARD_TX_BLOCK_JSON: &str =
    include_str!("../../../zinder-ingest/tests/fixtures/zcash-testnet-orchard-block-1842462.json");
const BASE_UNIX_TIMESTAMP_MILLIS: u64 = 1_783_933_200_000;
const COINBASE_SCRIPTSIG_TAIL_TAG: [u8; 4] = [0xff, 0xff, 0xff, 0xff];
const ORCHARD_ACTION_BYTES: usize = 820;
const ORCHARD_SPEND_AUTH_SIG_BYTES: usize = 64;
const ORCHARD_FIXED_MID_BYTES: usize = 1 + 8 + 32;
const V5_COMMON_HEADER_BYTES: usize = 20;
const COINBASE_SPEND_VALUE_A_ZATS: u64 = 300_000_000;
const COINBASE_SPEND_VALUE_B_ZATS: u64 = 324_990_000;
const TRACKED_WALLET_RECIPIENTS: usize = 4;

/// Generated connected Zcash Regtest chain containing sparse, Orchard-dense,
/// and transparent coinbase-spend blocks across at least one segment boundary.
#[derive(Clone)]
pub(crate) struct GeneratedChain {
    pub(crate) network: Network,
    pub(crate) activations: Arc<NetworkUpgradeActivations>,
    pub(crate) blocks: Arc<BTreeMap<BlockHeight, SourceBlock>>,
    pub(crate) checkpoints: Arc<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>>,
    pub(crate) subtrees: Arc<Vec<SubtreeRootArtifact>>,
    pub(crate) base_tip_height: BlockHeight,
    pub(crate) final_tip_height: BlockHeight,
    pub(crate) reorg_window_blocks: u32,
    pub(crate) sparse_block_count: u32,
    pub(crate) dense_block_count: u32,
    pub(crate) transparent_spend_txs: u32,
    pub(crate) sample_v5_tx: Arc<Vec<u8>>,
    pub(crate) tracked_taddresses: Vec<String>,
    pub(crate) settled_txids: Vec<[u8; 32]>,
    pub(crate) live_txids: Vec<(u32, [u8; 32])>,
}

/// Geometry controlling generated chain length, Orchard density, and spends.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ChainSpecConfig {
    pub(crate) base_blocks: u32,
    pub(crate) live_blocks: u32,
    pub(crate) reorg_window_blocks: u32,
    pub(crate) dense_block_cadence: u32,
    pub(crate) dense_txs_per_block: u32,
    pub(crate) dense_orchard_actions_per_tx: u32,
}

impl GeneratedChain {
    /// Generates connected Regtest blocks `1..=(base_blocks + live_blocks)` and
    /// precomputes their exact commitment-tree checkpoints and completed subtrees.
    pub(crate) fn build(spec: ChainSpecConfig) -> Result<Self, BenchError> {
        let base_raw = decode_fixture_block_bytes(REGTEST_BLOCK_1_JSON)?;
        let sample_v5_tx = extract_v5_shielded_tx_bytes(ORCHARD_TX_BLOCK_JSON)?;
        let dense_v5_tx =
            scale_v5_orchard_actions(&sample_v5_tx, spec.dense_orchard_actions_per_tx)?;
        let (template_header_and_sol, template_coinbase) = split_coinbase_block(&base_raw)?;
        let activations = Arc::new(regtest_activations()?);
        let recipient_pkhs = deterministic_recipient_pkhs();
        let tracked_taddresses = encode_regtest_p2pkh_addresses(&recipient_pkhs);
        let total_blocks = spec
            .base_blocks
            .checked_add(spec.live_blocks)
            .ok_or_else(|| BenchError::invalid_argument("chain height overflow"))?;

        let mut build_state = ChainBuildAccumulator::new(spec, total_blocks);
        let templates = BlockTemplateInputs {
            header_and_sol: &template_header_and_sol,
            coinbase: &template_coinbase,
            dense_v5_tx: &dense_v5_tx,
            recipient_pkhs: &recipient_pkhs,
        };
        for raw_height in 1_u32..=total_blocks {
            build_state.push_block(raw_height, templates)?;
        }

        let (checkpoints, subtrees) = derive_chain_commitments(&build_state.blocks, &activations)?;
        Ok(Self {
            network: Network::ZcashRegtest,
            activations,
            blocks: Arc::new(build_state.blocks),
            checkpoints: Arc::new(checkpoints),
            subtrees: Arc::new(subtrees),
            base_tip_height: BlockHeight::new(spec.base_blocks),
            final_tip_height: BlockHeight::new(total_blocks),
            reorg_window_blocks: spec.reorg_window_blocks,
            sparse_block_count: build_state.sparse_block_count,
            dense_block_count: build_state.dense_block_count,
            transparent_spend_txs: build_state.transparent_spend_txs,
            sample_v5_tx: Arc::new(sample_v5_tx),
            tracked_taddresses,
            settled_txids: build_state.settled_txids,
            live_txids: build_state.live_txids,
        })
    }

    /// Returns the `BlockId` at `height`.
    pub(crate) fn block_id(&self, height: BlockHeight) -> Result<BlockId, BenchError> {
        self.blocks
            .get(&height)
            .map(|block| BlockId::new(height, block.hash))
            .ok_or_else(|| {
                BenchError::tip_replay(format!(
                    "missing generated block at height {}",
                    height.value()
                ))
            })
    }
}

#[derive(Clone, Copy)]
struct BlockTemplateInputs<'a> {
    header_and_sol: &'a [u8],
    coinbase: &'a [u8],
    dense_v5_tx: &'a [u8],
    recipient_pkhs: &'a [[u8; 20]],
}

struct ChainBuildAccumulator {
    spec: ChainSpecConfig,
    base_settled_end: u32,
    spend_maturity_blocks: u32,
    total_blocks: u32,
    parent_hash: zinder_core::BlockHash,
    blocks: BTreeMap<BlockHeight, SourceBlock>,
    coinbase_txids: BTreeMap<u32, [u8; 32]>,
    sparse_block_count: u32,
    dense_block_count: u32,
    transparent_spend_txs: u32,
    settled_txids: Vec<[u8; 32]>,
    live_txids: Vec<(u32, [u8; 32])>,
}

impl ChainBuildAccumulator {
    fn new(spec: ChainSpecConfig, total_blocks: u32) -> Self {
        let base_settled_end = spec.base_blocks.saturating_sub(spec.reorg_window_blocks);
        let spend_maturity_blocks = if spec.base_blocks > 110 { 100 } else { 1 };
        Self {
            spec,
            base_settled_end,
            spend_maturity_blocks,
            total_blocks,
            parent_hash: Network::ZcashRegtest.genesis_hash(),
            blocks: BTreeMap::new(),
            coinbase_txids: BTreeMap::new(),
            sparse_block_count: 0,
            dense_block_count: 0,
            transparent_spend_txs: 0,
            settled_txids: Vec::new(),
            live_txids: Vec::new(),
        }
    }

    fn push_block(
        &mut self,
        raw_height: u32,
        templates: BlockTemplateInputs<'_>,
    ) -> Result<(), BenchError> {
        let is_dense = raw_height > 1
            && self.spec.dense_block_cadence > 0
            && raw_height.is_multiple_of(self.spec.dense_block_cadence);
        let tx_copies = if is_dense {
            self.spec.dense_txs_per_block
        } else {
            0
        };
        if is_dense && tx_copies > 0 {
            self.dense_block_count = self.dense_block_count.saturating_add(1);
        } else {
            self.sparse_block_count = self.sparse_block_count.saturating_add(1);
        }

        let transparent_spend = self.build_transparent_spend_for_height(
            raw_height,
            &templates.dense_v5_tx[4..12],
            templates.recipient_pkhs,
        )?;
        let assembled = assemble_block_bytes(&BlockAssemblySpec {
            raw_height,
            parent_hash: self.parent_hash,
            template_header_and_sol: templates.header_and_sol,
            template_coinbase: templates.coinbase,
            dense_v5_tx: templates.dense_v5_tx,
            v5_tx_copies: tx_copies,
            transparent_spend_tx: transparent_spend.as_deref(),
        })?;
        self.coinbase_txids
            .insert(raw_height, assembled.coinbase_txid);
        self.record_txids(raw_height, &assembled);

        let height = BlockHeight::new(raw_height);
        let source_block = SourceBlock::from_raw_block_bytes(
            Network::ZcashRegtest,
            height,
            assembled.block_bytes,
        )?;
        self.parent_hash = source_block.hash;
        self.blocks.insert(height, source_block);
        Ok(())
    }

    fn build_transparent_spend_for_height(
        &mut self,
        raw_height: u32,
        v5_group_and_branch: &[u8],
        recipient_pkhs: &[[u8; 20]],
    ) -> Result<Option<Vec<u8>>, BenchError> {
        let Some(coinbase_height) = raw_height.checked_sub(self.spend_maturity_blocks) else {
            return Ok(None);
        };
        if coinbase_height == 0 {
            return Ok(None);
        }
        let should_spend = raw_height.is_multiple_of(8)
            || raw_height == self.spec.base_blocks.saturating_add(1)
            || raw_height == self.total_blocks;
        if !should_spend {
            return Ok(None);
        }
        let Some(&prev_coinbase_txid) = self.coinbase_txids.get(&coinbase_height) else {
            return Ok(None);
        };
        let idx = usize::try_from(self.transparent_spend_txs).unwrap_or(0);
        let pkh_a = recipient_pkhs[idx % recipient_pkhs.len()];
        let pkh_b = recipient_pkhs[(idx + 1) % recipient_pkhs.len()];
        let raw_tx = build_transparent_coinbase_spend_v5(
            v5_group_and_branch,
            raw_height,
            prev_coinbase_txid,
            pkh_a,
            pkh_b,
        )?;
        self.transparent_spend_txs = self.transparent_spend_txs.saturating_add(1);
        Ok(Some(raw_tx))
    }

    fn record_txids(&mut self, raw_height: u32, assembled: &AssembledBlock) {
        if let Some(txid) = assembled.transparent_spend_txid {
            self.push_txid(raw_height, txid);
        }
        if let Some(&first_shielded) = assembled.shielded_txids.first() {
            self.push_txid(raw_height, first_shielded);
        }
        if raw_height == 1 || raw_height == self.spec.base_blocks {
            self.push_txid(raw_height, assembled.coinbase_txid);
        }
    }

    fn push_txid(&mut self, raw_height: u32, txid: [u8; 32]) {
        if raw_height <= self.base_settled_end {
            self.settled_txids.push(txid);
        } else if raw_height > self.spec.base_blocks {
            self.live_txids.push((raw_height, txid));
        }
    }
}

/// Pre-built canonical and wallet stores positioned at `base_tip_height` so all
/// matched arms start from identical on-disk state.
pub(crate) struct TemplateStores {
    pub(crate) _root: tempfile::TempDir,
    pub(crate) canonical_path: PathBuf,
    pub(crate) wallet_path: PathBuf,
}

impl TemplateStores {
    /// Builds fresh READY canonical and wallet stores at `chain.base_tip_height`.
    pub(crate) async fn build(
        chain: &GeneratedChain,
        config: &ActiveWriterPublicationConfig,
    ) -> Result<Self, BenchError> {
        let root = create_scratch_tempdir(config.scratch_dir.as_deref(), "template")?;
        let canonical_path = root.path().join("canonical-base");
        let wallet_path = root.path().join("wallet-base");

        let source = AdvancingChainSource::new(chain, chain.base_tip_height);
        let first_block = chain
            .blocks
            .get(&BlockHeight::new(1))
            .ok_or_else(|| BenchError::tip_replay("generated chain has no block 1"))?;
        let build_plan = CanonicalStoreBuildPlan::complete(
            &chain.activations,
            first_block.block_time_seconds.saturating_sub(1),
            chain.block_id(chain.base_tip_height)?,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(chain.reorg_window_blocks)?,
        )?;
        let mut canonical_budget = RocksDbResourceBudget::for_local_tests();
        canonical_budget.statistics_level = RocksDbStatisticsLevel::Tickers;
        canonical_budget.preferred_io_mode = config.io_mode;

        let builder = RocksDbCanonicalBuilder::create_fresh(
            &canonical_path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            canonical_budget,
        )?;
        let construction = CanonicalConstructionConfig::for_local_tests(
            Duration::from_secs(10),
            Arc::clone(&chain.activations),
        );
        let built = load_fresh_canonical(builder, &source, &construction).await?;
        let validated = built.builder.prepare_cold_certified_publication()?;
        let settled_height = BlockHeight::new(
            chain
                .base_tip_height
                .value()
                .saturating_sub(chain.reorg_window_blocks),
        );
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            chain.block_id(settled_height)?,
            UnixTimestampMillis::new(BASE_UNIX_TIMESTAMP_MILLIS),
        ))?;
        let canonical = validated.publish_baseline(publication)?;
        build_wallet_from_canonical(
            &canonical,
            &wallet_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: chain.reorg_window_blocks,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        drop(canonical);
        Ok(Self {
            _root: root,
            canonical_path,
            wallet_path,
        })
    }

    /// Recursively copies and fsyncs `canonical_base` and `wallet_base` into
    /// fresh target directories.
    pub(crate) fn clone_to(
        &self,
        canonical_dst: &Path,
        wallet_dst: &Path,
    ) -> Result<(), BenchError> {
        copy_dir_recursive(&self.canonical_path, canonical_dst)?;
        copy_dir_recursive(&self.wallet_path, wallet_dst)?;
        Ok(())
    }
}

/// Creates a scoped temporary workspace under `parent_opt` (or system temp).
pub(crate) fn create_scratch_tempdir(
    parent_opt: Option<&Path>,
    label: &str,
) -> Result<tempfile::TempDir, BenchError> {
    let mut builder = tempfile::Builder::new();
    let prefix = format!("zinder-awp-{label}-");
    builder.prefix(&prefix);
    match parent_opt {
        Some(parent) => {
            fs::create_dir_all(parent).map_err(|source| BenchError::io(parent, source))?;
            builder
                .tempdir_in(parent)
                .map_err(|source| BenchError::io(parent, source))
        }
        None => builder
            .tempdir()
            .map_err(|source| BenchError::io("tempdir", source)),
    }
}

/// [`RocksDbResourceBudget`] configured for ticker collection and `io_mode`.
pub(crate) fn writer_budget(io_mode: RocksDbIoMode) -> RocksDbResourceBudget {
    let mut budget = RocksDbResourceBudget::canonical_writer_defaults();
    budget.block_cache_bytes = RocksDbResourceBudget::MIN_BLOCK_CACHE_BYTES;
    budget.max_wal_bytes = RocksDbResourceBudget::MIN_MAX_WAL_BYTES;
    budget.write_buffer_bytes = RocksDbResourceBudget::MIN_WRITE_BUFFER_BYTES;
    budget.memtable_budget_bytes = 8 * 1024 * 1024;
    budget.statistics_level = RocksDbStatisticsLevel::Tickers;
    budget.preferred_io_mode = io_mode;
    budget
}

/// [`RocksDbResourceBudget`] configured for reader/projector/publisher secondaries.
pub(crate) fn reader_budget(io_mode: RocksDbIoMode) -> RocksDbResourceBudget {
    let mut budget = RocksDbResourceBudget::canonical_reader_defaults();
    budget.block_cache_bytes = RocksDbResourceBudget::MIN_BLOCK_CACHE_BYTES;
    budget.max_open_files = RocksDbResourceBudget::MIN_MAX_OPEN_FILES;
    budget.statistics_level = RocksDbStatisticsLevel::Tickers;
    budget.preferred_io_mode = io_mode;
    budget
}

/// [`NodeSource`] over [`GeneratedChain`] whose visible tip advances atomically
/// during live-writer benchmark modes while counting every upstream RPC.
#[derive(Clone)]
pub(crate) struct AdvancingChainSource {
    chain: GeneratedChain,
    visible_tip: Arc<AtomicU32>,
    tip_id_calls: Arc<AtomicU64>,
    fetch_block_at_calls: Arc<AtomicU64>,
    fetch_chain_segment_calls: Arc<AtomicU64>,
    fetch_chain_checkpoint_calls: Arc<AtomicU64>,
    fetch_subtree_roots_calls: Arc<AtomicU64>,
    #[cfg(test)]
    fail_at_height: Arc<AtomicU32>,
}

impl AdvancingChainSource {
    /// Creates an advancing chain source initially exposing `initial_tip`.
    pub(crate) fn new(chain: &GeneratedChain, initial_tip: BlockHeight) -> Self {
        Self {
            chain: chain.clone(),
            visible_tip: Arc::new(AtomicU32::new(initial_tip.value())),
            tip_id_calls: Arc::new(AtomicU64::new(0)),
            fetch_block_at_calls: Arc::new(AtomicU64::new(0)),
            fetch_chain_segment_calls: Arc::new(AtomicU64::new(0)),
            fetch_chain_checkpoint_calls: Arc::new(AtomicU64::new(0)),
            fetch_subtree_roots_calls: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            fail_at_height: Arc::new(AtomicU32::new(0)),
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_at_height(self, height: u32) -> Self {
        self.fail_at_height.store(height, Ordering::Release);
        self
    }

    #[cfg(test)]
    pub(crate) fn check_injected_failure(&self, height: u32) -> Result<(), BenchError> {
        let target = self.fail_at_height.load(Ordering::Acquire);
        if target > 0 && height >= target {
            return Err(BenchError::tip_replay(format!(
                "injected test writer failure at height {height}"
            )));
        }
        Ok(())
    }

    /// Reveals block `height` as the node's current tip.
    pub(crate) fn advance_tip_to(&self, height: BlockHeight) {
        self.visible_tip.store(height.value(), Ordering::Release);
    }

    /// Returns snapshot of upstream `NodeSource` RPC call counts.
    pub(crate) fn rpc_counts(&self) -> NodeRpcCounts {
        let tip_id = self.tip_id_calls.load(Ordering::Relaxed);
        let fetch_block_at = self.fetch_block_at_calls.load(Ordering::Relaxed);
        let fetch_chain_segment = self.fetch_chain_segment_calls.load(Ordering::Relaxed);
        let fetch_chain_checkpoint = self.fetch_chain_checkpoint_calls.load(Ordering::Relaxed);
        let fetch_subtree_roots = self.fetch_subtree_roots_calls.load(Ordering::Relaxed);
        NodeRpcCounts {
            tip_id,
            fetch_block_at,
            fetch_chain_segment,
            fetch_chain_checkpoint,
            fetch_subtree_roots,
            total: tip_id
                .saturating_add(fetch_block_at)
                .saturating_add(fetch_chain_segment)
                .saturating_add(fetch_chain_checkpoint)
                .saturating_add(fetch_subtree_roots),
        }
    }

    fn current_tip_height(&self) -> BlockHeight {
        BlockHeight::new(self.visible_tip.load(Ordering::Acquire))
    }
}

#[async_trait]
impl NodeSource for AdvancingChainSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_network_upgrade_activations(
        &self,
    ) -> Result<NetworkUpgradeActivations, SourceError> {
        Ok((*self.chain.activations).clone())
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.fetch_block_at_calls.fetch_add(1, Ordering::Relaxed);
        if height > self.current_tip_height() {
            return Err(SourceError::BlockUnavailable {
                height,
                reason: "height is above the advancing node tip".to_owned(),
            });
        }
        self.chain
            .blocks
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "generated block is missing".to_owned(),
            })
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        self.fetch_chain_segment_calls
            .fetch_add(1, Ordering::Relaxed);
        let Some(start) = limits.cursor.next_connected_height() else {
            return Ok(SourceChainSegment::default());
        };
        let tip = self.current_tip_height();
        if start > tip {
            return Ok(SourceChainSegment::default());
        }
        let max_blocks = usize::try_from(limits.max_connected_blocks.get()).unwrap_or(usize::MAX);
        let max_raw_bytes = usize::try_from(limits.max_response_bytes).unwrap_or(usize::MAX);
        let mut blocks = Vec::new();
        let mut accumulated_bytes = 0_usize;
        for (_, block) in self.chain.blocks.range(start..=tip) {
            if blocks.len() >= max_blocks {
                break;
            }
            let block_bytes = block.raw_block_bytes.len();
            if !blocks.is_empty() && accumulated_bytes.saturating_add(block_bytes) > max_raw_bytes {
                break;
            }
            accumulated_bytes = accumulated_bytes.saturating_add(block_bytes);
            blocks.push(block.clone());
        }
        Ok(SourceChainSegment::connected_blocks(blocks))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.fetch_chain_checkpoint_calls
            .fetch_add(1, Ordering::Relaxed);
        if height > self.current_tip_height() {
            return Err(SourceError::BlockUnavailable {
                height,
                reason: "checkpoint above current tip".to_owned(),
            });
        }
        self.chain
            .checkpoints
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "checkpoint missing".to_owned(),
            })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.tip_id_calls.fetch_add(1, Ordering::Relaxed);
        let tip_height = self.current_tip_height();
        let block =
            self.chain
                .blocks
                .get(&tip_height)
                .ok_or_else(|| SourceError::BlockUnavailable {
                    height: tip_height,
                    reason: "missing current tip block".to_owned(),
                })?;
        Ok(BlockId::new(tip_height, block.hash))
    }

    async fn fetch_subtree_roots(
        &self,
        protocol: ShieldedProtocol,
        start_index: SubtreeRootIndex,
        max_entries: NonZeroU32,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.fetch_subtree_roots_calls
            .fetch_add(1, Ordering::Relaxed);
        let tip = self.current_tip_height();
        let roots: Vec<SourceSubtreeRoot> = self
            .chain
            .subtrees
            .iter()
            .filter(|root| {
                root.protocol == protocol
                    && root.subtree_index.value() >= start_index.value()
                    && root.completing_block_height <= tip
            })
            .take(usize::try_from(max_entries.get()).unwrap_or(usize::MAX))
            .map(|root| {
                SourceSubtreeRoot::new(
                    root.subtree_index,
                    root.root_hash,
                    root.completing_block_height,
                )
            })
            .collect();
        Ok(SourceSubtreeRoots::new(protocol, start_index, roots))
    }

    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.fetch_subtree_roots(range.protocol, range.start_index, range.max_entries)
            .await
    }
}

pub(crate) fn regtest_activations() -> Result<NetworkUpgradeActivations, BenchError> {
    NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        vec![
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x5ba8_1b19),
                activation_height: BlockHeight::new(1),
                name: "Overwinter".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x76b8_09bb),
                activation_height: BlockHeight::new(1),
                name: "Sapling".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x2bb4_0e60),
                activation_height: BlockHeight::new(1),
                name: "Blossom".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xf5b9_230b),
                activation_height: BlockHeight::new(1),
                name: "Heartwood".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xe9ff_75a6),
                activation_height: BlockHeight::new(1),
                name: "Canopy".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xc2d6_d0b4),
                activation_height: BlockHeight::new(2),
                name: "NU5".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xc8e7_1055),
                activation_height: BlockHeight::new(2),
                name: "NU6".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x4dec_4df0),
                activation_height: BlockHeight::new(3),
                name: "NU6.1".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x5437_f330),
                activation_height: BlockHeight::new(3),
                name: "NU6.2".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x37a5_165b),
                activation_height: BlockHeight::new(100_000),
                name: "NU6.3".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x7719_0ad9),
                activation_height: BlockHeight::new(100_000),
                name: "NU7".to_owned(),
            },
        ],
    )
    .map_err(|source| BenchError::invalid_argument(source.to_string()))
}

fn deterministic_recipient_pkhs() -> Vec<[u8; 20]> {
    (0_u8..u8::try_from(TRACKED_WALLET_RECIPIENTS).unwrap_or(4))
        .map(|slot| {
            let mut pkh = [0x42_u8; 20];
            pkh[19] = slot.saturating_add(1);
            pkh
        })
        .collect()
}

fn encode_regtest_p2pkh_addresses(pkhs: &[[u8; 20]]) -> Vec<String> {
    pkhs.iter()
        .map(|&pkh| {
            ZebraTransparentAddress::from_pub_key_hash(NetworkKind::Regtest, pkh).to_string()
        })
        .collect()
}

fn append_p2pkh_output(out: &mut Vec<u8>, value_zats: u64, pkh: [u8; 20]) {
    out.extend_from_slice(&value_zats.to_le_bytes());
    out.push(25_u8);
    out.extend_from_slice(&[0x76, 0xa9, 0x14]);
    out.extend_from_slice(&pkh);
    out.extend_from_slice(&[0x88, 0xac]);
}

fn build_transparent_coinbase_spend_v5(
    v5_group_and_branch: &[u8],
    target_height: u32,
    coinbase_txid: [u8; 32],
    pkh_a: [u8; 20],
    pkh_b: [u8; 20],
) -> Result<Vec<u8>, BenchError> {
    let mut raw = Vec::with_capacity(192);
    raw.extend_from_slice(&0x8000_0005_u32.to_le_bytes());
    raw.extend_from_slice(v5_group_and_branch);
    raw.extend_from_slice(&target_height.to_le_bytes());
    raw.extend_from_slice(&0_u32.to_le_bytes());
    raw.push(1_u8);
    raw.extend_from_slice(&coinbase_txid);
    raw.extend_from_slice(&0_u32.to_le_bytes());
    raw.push(65_u8);
    raw.push(64_u8);
    raw.extend_from_slice(&[0x11_u8; 64]);
    raw.extend_from_slice(&u32::MAX.to_le_bytes());
    raw.push(2_u8);
    append_p2pkh_output(&mut raw, COINBASE_SPEND_VALUE_A_ZATS, pkh_a);
    append_p2pkh_output(&mut raw, COINBASE_SPEND_VALUE_B_ZATS, pkh_b);
    raw.extend_from_slice(&[0_u8, 0_u8, 0_u8]);

    let parsed = Transaction::zcash_deserialize(raw.as_slice())
        .map_err(|source| BenchError::fixture_format(source.to_string()))?;
    if parsed.inputs().len() != 1 || parsed.outputs().len() != 2 {
        return Err(BenchError::fixture_format(
            "transparent spend transaction missing expected inputs or outputs",
        ));
    }
    Ok(raw)
}

fn decode_fixture_block_bytes(fixture_json: &str) -> Result<Vec<u8>, BenchError> {
    let parsed: JsonValue = serde_json::from_str(fixture_json)?;
    let hex_bytes = parsed
        .get("raw_block_hex")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| BenchError::fixture_format("fixture raw_block_hex missing"))?;
    hex::decode(hex_bytes).map_err(|source| BenchError::fixture_format(source.to_string()))
}

fn extract_v5_shielded_tx_bytes(fixture_json: &str) -> Result<Vec<u8>, BenchError> {
    let raw_block = decode_fixture_block_bytes(fixture_json)?;
    let block = Block::zcash_deserialize(raw_block.as_slice())
        .map_err(|source| BenchError::fixture_format(source.to_string()))?;
    let tx = block
        .transactions
        .get(1)
        .ok_or_else(|| BenchError::fixture_format("expected Orchard transaction at index 1"))?;
    let raw_tx = tx
        .zcash_serialize_to_vec()
        .map_err(|source| BenchError::fixture_format(source.to_string()))?;
    strip_v5_transparent_section(&raw_tx)
}

struct ParsedOrchardTemplate<'a> {
    prefix_through_sapling: &'a [u8],
    actions: Vec<&'a [u8]>,
    mid_header: &'a [u8],
    proof_len: u64,
    proof: &'a [u8],
    sigs: Vec<&'a [u8]>,
    binding_sig: &'a [u8],
}

fn parse_stripped_orchard_template(
    stripped_v5_tx: &[u8],
) -> Result<ParsedOrchardTemplate<'_>, BenchError> {
    const PREFIX_END: usize = V5_COMMON_HEADER_BYTES + 4;
    if stripped_v5_tx.len() <= PREFIX_END {
        return Err(BenchError::fixture_format("stripped V5 tx too short"));
    }
    let mut offset = PREFIX_END;
    let orig_actions = usize::try_from(read_compact_size(stripped_v5_tx, &mut offset)?)
        .map_err(|_| BenchError::fixture_format("orchard action count overflow"))?;
    if orig_actions == 0 {
        return Err(BenchError::fixture_format("expected Orchard actions > 0"));
    }
    let mut actions = Vec::with_capacity(orig_actions);
    for _ in 0..orig_actions {
        actions.push(read_bytes(
            stripped_v5_tx,
            &mut offset,
            ORCHARD_ACTION_BYTES,
        )?);
    }
    let mid_header = read_bytes(stripped_v5_tx, &mut offset, ORCHARD_FIXED_MID_BYTES)?;
    let proof_len = read_compact_size(stripped_v5_tx, &mut offset)?;
    let proof_usize = usize::try_from(proof_len)
        .map_err(|_| BenchError::fixture_format("orchard proof length overflow"))?;
    let proof = read_bytes(stripped_v5_tx, &mut offset, proof_usize)?;
    let mut sigs = Vec::with_capacity(orig_actions);
    for _ in 0..orig_actions {
        sigs.push(read_bytes(
            stripped_v5_tx,
            &mut offset,
            ORCHARD_SPEND_AUTH_SIG_BYTES,
        )?);
    }
    let binding_sig = read_bytes(stripped_v5_tx, &mut offset, ORCHARD_SPEND_AUTH_SIG_BYTES)?;
    if offset != stripped_v5_tx.len() {
        return Err(BenchError::fixture_format(
            "unexpected trailing bytes in V5 Orchard template",
        ));
    }
    Ok(ParsedOrchardTemplate {
        prefix_through_sapling: &stripped_v5_tx[..PREFIX_END],
        actions,
        mid_header,
        proof_len,
        proof,
        sigs,
        binding_sig,
    })
}

fn scale_v5_orchard_actions(
    stripped_v5_tx: &[u8],
    target_actions: u32,
) -> Result<Vec<u8>, BenchError> {
    let tpl = parse_stripped_orchard_template(stripped_v5_tx)?;
    let target = usize::try_from(target_actions)
        .map_err(|_| BenchError::invalid_argument("action count overflow"))?;
    if target == tpl.actions.len() {
        return Ok(stripped_v5_tx.to_vec());
    }
    let mut out = Vec::with_capacity(
        tpl.prefix_through_sapling.len()
            + 9
            + target * (ORCHARD_ACTION_BYTES + ORCHARD_SPEND_AUTH_SIG_BYTES)
            + tpl.mid_header.len()
            + tpl.proof.len()
            + 72,
    );
    out.extend_from_slice(tpl.prefix_through_sapling);
    write_compact_size(&mut out, u64::from(target_actions));
    for idx in 0..target {
        let mut action = tpl.actions[idx % tpl.actions.len()].to_vec();
        let ordinal = u32::try_from(idx).unwrap_or(0).to_le_bytes();
        for (dst, src) in action[32..36].iter_mut().zip(ordinal) {
            *dst ^= src;
        }
        out.extend_from_slice(&action);
    }
    out.extend_from_slice(tpl.mid_header);
    write_compact_size(&mut out, tpl.proof_len);
    out.extend_from_slice(tpl.proof);
    for idx in 0..target {
        out.extend_from_slice(tpl.sigs[idx % tpl.sigs.len()]);
    }
    out.extend_from_slice(tpl.binding_sig);

    let parsed = Transaction::zcash_deserialize(out.as_slice())
        .map_err(|source| BenchError::fixture_format(source.to_string()))?;
    if parsed.orchard_actions().count() != target {
        return Err(BenchError::fixture_format(
            "scaled V5 tx orchard action count mismatch",
        ));
    }
    Ok(out)
}

fn read_bytes<'bytes>(
    bytes: &'bytes [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'bytes [u8], BenchError> {
    let end = offset
        .checked_add(len)
        .filter(|&end_idx| end_idx <= bytes.len())
        .ok_or_else(|| BenchError::fixture_format("unexpected EOF decoding V5 transaction"))?;
    let slice = &bytes[*offset..end];
    *offset = end;
    Ok(slice)
}

fn read_compact_size(bytes: &[u8], offset: &mut usize) -> Result<u64, BenchError> {
    let tag = read_bytes(bytes, offset, 1)?[0];
    match tag {
        0..=0xfc => Ok(u64::from(tag)),
        0xfd => {
            let raw: [u8; 2] = read_bytes(bytes, offset, 2)?
                .try_into()
                .map_err(|_| BenchError::fixture_format("compact size u16 length mismatch"))?;
            Ok(u64::from(u16::from_le_bytes(raw)))
        }
        0xfe => {
            let raw: [u8; 4] = read_bytes(bytes, offset, 4)?
                .try_into()
                .map_err(|_| BenchError::fixture_format("compact size u32 length mismatch"))?;
            Ok(u64::from(u32::from_le_bytes(raw)))
        }
        0xff => {
            let raw: [u8; 8] = read_bytes(bytes, offset, 8)?
                .try_into()
                .map_err(|_| BenchError::fixture_format("compact size u64 length mismatch"))?;
            Ok(u64::from_le_bytes(raw))
        }
    }
}

fn strip_v5_transparent_section(raw_v5_tx: &[u8]) -> Result<Vec<u8>, BenchError> {
    if raw_v5_tx.len() < V5_COMMON_HEADER_BYTES {
        return Err(BenchError::fixture_format(
            "V5 transaction shorter than 20-byte common header",
        ));
    }
    let header: [u8; 4] = raw_v5_tx[0..4]
        .try_into()
        .map_err(|_| BenchError::fixture_format("V5 header slice mismatch"))?;
    if u32::from_le_bytes(header) != 0x8000_0005 {
        return Err(BenchError::fixture_format(
            "expected V5 transaction header 0x80000005",
        ));
    }
    let mut offset = V5_COMMON_HEADER_BYTES;
    let tx_in_count = read_compact_size(raw_v5_tx, &mut offset)?;
    for _ in 0..tx_in_count {
        let _ = read_bytes(raw_v5_tx, &mut offset, 36)?;
        let script_len = usize::try_from(read_compact_size(raw_v5_tx, &mut offset)?)
            .map_err(|_| BenchError::fixture_format("input script length overflow"))?;
        let script_and_seq = script_len
            .checked_add(4)
            .ok_or_else(|| BenchError::fixture_format("input script length overflow"))?;
        let _ = read_bytes(raw_v5_tx, &mut offset, script_and_seq)?;
    }
    let tx_out_count = read_compact_size(raw_v5_tx, &mut offset)?;
    for _ in 0..tx_out_count {
        let _ = read_bytes(raw_v5_tx, &mut offset, 8)?;
        let script_len = usize::try_from(read_compact_size(raw_v5_tx, &mut offset)?)
            .map_err(|_| BenchError::fixture_format("output script length overflow"))?;
        let _ = read_bytes(raw_v5_tx, &mut offset, script_len)?;
    }

    let mut stripped =
        Vec::with_capacity(V5_COMMON_HEADER_BYTES + 2 + raw_v5_tx.len().saturating_sub(offset));
    stripped.extend_from_slice(&raw_v5_tx[..V5_COMMON_HEADER_BYTES]);
    stripped.extend_from_slice(&[0_u8, 0_u8]);
    stripped.extend_from_slice(&raw_v5_tx[offset..]);

    let parsed = Transaction::zcash_deserialize(stripped.as_slice())
        .map_err(|source| BenchError::fixture_format(source.to_string()))?;
    if !parsed.inputs().is_empty()
        || !parsed.outputs().is_empty()
        || !parsed.has_orchard_shielded_data()
    {
        return Err(BenchError::fixture_format(
            "stripped V5 template must be shielded-only with Orchard data",
        ));
    }
    Ok(stripped)
}

fn split_coinbase_block(raw_block: &[u8]) -> Result<(Vec<u8>, Vec<u8>), BenchError> {
    const HEADER_AND_SOLUTION_END: usize = 1_487;
    if raw_block.len() <= HEADER_AND_SOLUTION_END + 1 {
        return Err(BenchError::fixture_format(
            "regtest block-1 fixture shorter than header and equihash solution",
        ));
    }
    Ok((
        raw_block[..HEADER_AND_SOLUTION_END].to_vec(),
        raw_block[HEADER_AND_SOLUTION_END + 1..].to_vec(),
    ))
}

struct BlockAssemblySpec<'a> {
    raw_height: u32,
    parent_hash: zinder_core::BlockHash,
    template_header_and_sol: &'a [u8],
    template_coinbase: &'a [u8],
    dense_v5_tx: &'a [u8],
    v5_tx_copies: u32,
    transparent_spend_tx: Option<&'a [u8]>,
}

struct AssembledBlock {
    block_bytes: Vec<u8>,
    coinbase_txid: [u8; 32],
    transparent_spend_txid: Option<[u8; 32]>,
    shielded_txids: Vec<[u8; 32]>,
}

fn assemble_block_bytes(spec: &BlockAssemblySpec<'_>) -> Result<AssembledBlock, BenchError> {
    let mut header_and_sol = spec.template_header_and_sol.to_vec();
    header_and_sol[4..36].copy_from_slice(&spec.parent_hash.as_bytes());
    let timestamp = u32::try_from(BASE_UNIX_TIMESTAMP_MILLIS / 1_000)
        .unwrap_or(1_783_933_200)
        .saturating_add(spec.raw_height);
    header_and_sol[100..104].copy_from_slice(&timestamp.to_le_bytes());

    let coinbase_bytes = rewrite_coinbase_with_height(spec.template_coinbase, spec.raw_height)?;
    let coinbase = Transaction::zcash_deserialize(coinbase_bytes.as_slice())
        .map_err(|source| BenchError::tip_replay(source.to_string()))?;
    let coinbase_txid = coinbase.hash().0;

    let mut tx_hashes = Vec::with_capacity(usize::try_from(spec.v5_tx_copies).unwrap_or(0) + 2);
    tx_hashes.push(coinbase.hash());
    let mut extra_tx_bytes = Vec::new();
    let mut transparent_spend_txid = None;
    if let Some(spend_raw) = spec.transparent_spend_tx {
        let parsed = Transaction::zcash_deserialize(spend_raw)
            .map_err(|source| BenchError::tip_replay(source.to_string()))?;
        transparent_spend_txid = Some(parsed.hash().0);
        tx_hashes.push(parsed.hash());
        extra_tx_bytes.extend_from_slice(spend_raw);
    }

    let mut shielded_txids = Vec::with_capacity(usize::try_from(spec.v5_tx_copies).unwrap_or(0));
    for copy_idx in 0..spec.v5_tx_copies {
        let mut tweaked = spec.dense_v5_tx.to_vec();
        if tweaked.len() >= 16 {
            let lock_marker = spec
                .raw_height
                .wrapping_mul(1_000_003)
                .wrapping_add(copy_idx);
            tweaked[12..16].copy_from_slice(&lock_marker.to_le_bytes());
        }
        let parsed = Transaction::zcash_deserialize(tweaked.as_slice())
            .map_err(|source| BenchError::tip_replay(source.to_string()))?;
        shielded_txids.push(parsed.hash().0);
        tx_hashes.push(parsed.hash());
        extra_tx_bytes.extend_from_slice(&tweaked);
    }

    let merkle_root: MerkleRoot = tx_hashes.into_iter().collect();
    header_and_sol[36..68].copy_from_slice(&merkle_root.0);

    let total_txs =
        1_u64 + u64::from(spec.transparent_spend_tx.is_some()) + u64::from(spec.v5_tx_copies);
    let mut out =
        Vec::with_capacity(header_and_sol.len() + 9 + coinbase_bytes.len() + extra_tx_bytes.len());
    out.extend_from_slice(&header_and_sol);
    write_compact_size(&mut out, total_txs);
    out.extend_from_slice(&coinbase_bytes);
    out.extend_from_slice(&extra_tx_bytes);
    Ok(AssembledBlock {
        block_bytes: out,
        coinbase_txid,
        transparent_spend_txid,
        shielded_txids,
    })
}

fn rewrite_coinbase_with_height(
    template_coinbase: &[u8],
    height: u32,
) -> Result<Vec<u8>, BenchError> {
    const SCRIPTSIG_LEN_OFFSET: usize = 45;
    let marker = COINBASE_SCRIPTSIG_TAIL_TAG;
    let marker_offset = template_coinbase
        .windows(4)
        .rposition(|window| window == marker)
        .ok_or_else(|| BenchError::fixture_format("coinbase sequence marker missing"))?;
    let mut script_sig = bip34_height_push(height);
    let miner_entropy = format!("zinder-bench-{height:08}");
    script_sig.extend_from_slice(miner_entropy.as_bytes());
    let script_len = u8::try_from(script_sig.len())
        .map_err(|_| BenchError::invalid_argument("coinbase scriptSig exceeded 255 bytes"))?;

    let mut out = Vec::with_capacity(template_coinbase.len() + script_sig.len());
    out.extend_from_slice(&template_coinbase[..SCRIPTSIG_LEN_OFFSET]);
    out.push(script_len);
    out.extend_from_slice(&script_sig);
    out.extend_from_slice(&template_coinbase[marker_offset..]);
    Ok(out)
}

/// Encodes `height` using BIP34 minimal `CScriptNum` push syntax.
pub(crate) fn bip34_height_push(height: u32) -> Vec<u8> {
    match height {
        0 => vec![0x00],
        1..=16 => vec![0x50 + u8::try_from(height).unwrap_or(0)],
        _ => {
            let mut raw = Vec::with_capacity(5);
            let mut remaining = height;
            while remaining > 0 {
                raw.push(u8::try_from(remaining & 0xff).unwrap_or(0));
                remaining >>= 8;
            }
            if raw.last().is_some_and(|msb| msb & 0x80 != 0) {
                raw.push(0x00);
            }
            let mut push = Vec::with_capacity(raw.len() + 1);
            push.push(u8::try_from(raw.len()).unwrap_or(0));
            push.extend_from_slice(&raw);
            push
        }
    }
}

fn write_compact_size(buffer: &mut Vec<u8>, count: u64) {
    if count < 0xfd {
        buffer.push(u8::try_from(count).unwrap_or(0));
    } else if let Ok(short) = u16::try_from(count) {
        buffer.push(0xfd);
        buffer.extend_from_slice(&short.to_le_bytes());
    } else if let Ok(medium) = u32::try_from(count) {
        buffer.push(0xfe);
        buffer.extend_from_slice(&medium.to_le_bytes());
    } else {
        buffer.push(0xff);
        buffer.extend_from_slice(&count.to_le_bytes());
    }
}

fn derive_chain_commitments(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    activations: &NetworkUpgradeActivations,
) -> Result<
    (
        BTreeMap<BlockHeight, CommitmentTreeCheckpoint>,
        Vec<SubtreeRootArtifact>,
    ),
    BenchError,
> {
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        BlockHeight::new(0),
        &CommitmentTreeFrontiers::default(),
        activations,
    )
    .map_err(|source| BenchError::tip_replay(source.to_string()))?;

    let mut checkpoints = BTreeMap::new();
    let mut subtrees = Vec::new();
    let mut running_sizes = zinder_ingest::CommitmentTreeSizes::default();

    let default_block_time =
        u32::try_from(BASE_UNIX_TIMESTAMP_MILLIS / 1_000).unwrap_or(1_783_933_200);
    let first_block_time = blocks
        .get(&BlockHeight::new(1))
        .map_or(default_block_time, |block| block.block_time_seconds);
    checkpoints.insert(
        BlockHeight::new(0),
        CommitmentTreeCheckpoint::new(
            BlockId::new(BlockHeight::new(0), Network::ZcashRegtest.genesis_hash()),
            first_block_time.saturating_sub(1),
            accumulator
                .validated_frontiers()
                .map_err(|source| BenchError::tip_replay(source.to_string()))?,
        ),
    );

    for (height, source_block) in blocks {
        let prepared =
            prepare_canonical_block(source_block, activations, RawBlobPolicy::Transactions)?;
        let positioned = position_canonical_block(prepared, &mut running_sizes)?;
        let commitments = extract_commitments(&positioned.compact_block);
        accumulator
            .append_block_commitments(
                *height,
                &commitments.sapling,
                &commitments.orchard,
                &commitments.ironwood,
            )
            .map_err(|source| BenchError::tip_replay(source.to_string()))?;

        let block_id = BlockId::new(*height, source_block.hash);
        for completed in accumulator.completed_subtree_roots() {
            subtrees.push(SubtreeRootArtifact {
                protocol: completed.protocol,
                subtree_index: completed.subtree_index,
                root_hash: completed.root_hash,
                completing_block_height: completed.completing_block_height,
                completing_block_hash: block_id.hash,
            });
        }
        checkpoints.insert(
            *height,
            CommitmentTreeCheckpoint::new(
                block_id,
                source_block.block_time_seconds,
                accumulator
                    .validated_frontiers()
                    .map_err(|source| BenchError::tip_replay(source.to_string()))?,
            ),
        );
    }

    Ok((checkpoints, subtrees))
}

struct ExtractedCommitments {
    sapling: Vec<[u8; 32]>,
    orchard: Vec<[u8; 32]>,
    ironwood: Vec<[u8; 32]>,
}

fn extract_commitments(compact_block: &zinder_core::CompactBlockArtifact) -> ExtractedCommitments {
    let mut sapling = Vec::new();
    let mut orchard = Vec::new();
    let mut ironwood = Vec::new();
    for tx in compact_block.transactions() {
        for output in &tx.data.sapling_outputs {
            sapling.push(output.commitment);
        }
        for action in &tx.data.orchard_actions {
            orchard.push(action.commitment);
        }
        for action in &tx.data.ironwood_actions {
            ironwood.push(action.commitment);
        }
    }
    ExtractedCommitments {
        sapling,
        orchard,
        ironwood,
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), BenchError> {
    fs::create_dir_all(dst).map_err(|source| BenchError::io(dst, source))?;
    for entry_res in fs::read_dir(src).map_err(|source| BenchError::io(src, source))? {
        let entry = entry_res.map_err(|source| BenchError::io(src, source))?;
        let file_type = entry
            .file_type()
            .map_err(|source| BenchError::io(entry.path(), source))?;
        let dst_child = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &dst_child)?;
        } else {
            fs::copy(entry.path(), &dst_child)
                .map_err(|source| BenchError::io(&dst_child, source))?;
            File::open(&dst_child)
                .and_then(|file| file.sync_all())
                .map_err(|source| BenchError::io(&dst_child, source))?;
        }
    }
    File::open(dst)
        .and_then(|dir| dir.sync_all())
        .map_err(|source| BenchError::io(dst, source))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use zebra_chain::{serialization::ZcashDeserialize as _, transaction::Transaction};
    use zinder_core::BlockHeight;
    use zinder_ingest::{RawBlobPolicy, prepare_canonical_block};
    use zinder_source::{NodeSource as _, SourceChainCursor, SourceChainSegmentLimits};

    use super::{AdvancingChainSource, ChainSpecConfig, GeneratedChain, bip34_height_push};

    #[test]
    fn bip34_height_push_handles_heights_past_sixteen_and_boundaries() {
        assert_eq!(bip34_height_push(1), vec![0x51]);
        assert_eq!(bip34_height_push(16), vec![0x60]);
        assert_eq!(bip34_height_push(17), vec![0x01, 17]);
        assert_eq!(bip34_height_push(127), vec![0x01, 127]);
        assert_eq!(bip34_height_push(128), vec![0x02, 128, 0x00]);
        assert_eq!(bip34_height_push(4_096), vec![0x02, 0x00, 0x10]);
        assert_eq!(bip34_height_push(4_100), vec![0x02, 0x04, 0x10]);
    }

    #[test]
    fn generated_chain_scales_orchard_actions_and_emits_transparent_spends()
    -> Result<(), crate::BenchError> {
        let chain = GeneratedChain::build(ChainSpecConfig {
            base_blocks: 20,
            live_blocks: 6,
            reorg_window_blocks: 2,
            dense_block_cadence: 5,
            dense_txs_per_block: 1,
            dense_orchard_actions_per_tx: 8,
        })?;

        let sample_tx = Transaction::zcash_deserialize(chain.sample_v5_tx.as_slice())
            .map_err(|source| crate::BenchError::fixture_format(source.to_string()))?;
        assert!(sample_tx.has_orchard_shielded_data());
        assert!(chain.sparse_block_count > 0);
        assert!(chain.dense_block_count > 0);
        assert!(chain.transparent_spend_txs > 0);
        assert_ne!(chain.tracked_taddresses, [] as [String; 0]);
        assert_ne!(chain.settled_txids, [] as [[u8; 32]; 0]);
        assert_ne!(chain.live_txids, [] as [(u32, [u8; 32]); 0]);

        let mut total_transparent_inputs = 0_u32;
        let mut total_orchard_additions = 0_u32;
        for (height, block) in chain.blocks.iter() {
            let prepared =
                prepare_canonical_block(block, &chain.activations, RawBlobPolicy::Transactions)?;
            if height.value().is_multiple_of(5) {
                assert_eq!(prepared.tree_size_additions.orchard, 8);
                total_orchard_additions = total_orchard_additions.saturating_add(8);
            }
            for tx in &prepared.facts.transactions {
                total_transparent_inputs = total_transparent_inputs
                    .saturating_add(tx.public_facts.counts.transparent_input_count);
            }
        }
        assert!(total_orchard_additions >= 32);
        assert!(total_transparent_inputs >= 2);
        Ok(())
    }

    #[tokio::test]
    async fn advancing_source_handles_cursor_past_tip_and_enforces_raw_byte_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain = GeneratedChain::build(ChainSpecConfig {
            base_blocks: 12,
            live_blocks: 4,
            reorg_window_blocks: 2,
            dense_block_cadence: 4,
            dense_txs_per_block: 1,
            dense_orchard_actions_per_tx: 4,
        })?;
        let source = AdvancingChainSource::new(&chain, BlockHeight::new(6));
        let empty = source
            .fetch_chain_segment(SourceChainSegmentLimits::new(
                SourceChainCursor::at_block(chain.block_id(BlockHeight::new(6))?),
                NonZeroU32::new(8).unwrap_or(NonZeroU32::MIN),
                1024 * 1024,
                1024 * 1024,
            ))
            .await?;
        assert_eq!(empty.updates(), []);

        source.advance_tip_to(BlockHeight::new(10));
        let tiny_budget = source
            .fetch_chain_segment(SourceChainSegmentLimits::new(
                SourceChainCursor::at_block(chain.block_id(BlockHeight::new(6))?),
                NonZeroU32::new(8).unwrap_or(NonZeroU32::MIN),
                1,
                1,
            ))
            .await?;
        assert_eq!(tiny_budget.updates().len(), 1);
        Ok(())
    }
}
