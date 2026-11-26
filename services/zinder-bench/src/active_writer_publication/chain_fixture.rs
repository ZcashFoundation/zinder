//! Deterministic Regtest chain fixture generator (sparse and shielded-dense
//! blocks crossing a 4,096-block segment boundary) plus reusable base stores and
//! an advancing [`NodeSource`] with per-RPC counters.

use std::{
    collections::BTreeMap,
    fs,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::BenchError;
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use zebra_chain::{
    block::{Block, merkle::Root as MerkleRoot},
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::Transaction,
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
    CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder, RocksDbResourceBudget,
    RocksDbStatisticsLevel,
};
use zinder_wallet_rocksdb::{RocksDbWalletBuildOptions, build_wallet_from_canonical};

use super::report::NodeRpcCounts;

const REGTEST_BLOCK_1_JSON: &str =
    include_str!("../../../zinder-ingest/tests/fixtures/z3-regtest-block-1.json");
const ORCHARD_TX_BLOCK_JSON: &str =
    include_str!("../../../zinder-ingest/tests/fixtures/zcash-testnet-orchard-block-1842462.json");
const BASE_UNIX_TIMESTAMP_MILLIS: u64 = 1_783_933_200_000;
const COINBASE_SCRIPTSIG_TAIL_TAG: [u8; 4] = [0xff, 0xff, 0xff, 0xff];

/// Generated connected Zcash Regtest chain containing both sparse and
/// commitment-dense shielded blocks across at least one `4,096`-block boundary.
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
    pub(crate) sample_v5_tx: Arc<Vec<u8>>,
}

/// Geometry controlling generated chain length and shielded density.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ChainSpecConfig {
    pub(crate) base_blocks: u32,
    pub(crate) live_blocks: u32,
    pub(crate) reorg_window_blocks: u32,
    pub(crate) dense_block_cadence: u32,
    pub(crate) dense_txs_per_block: u32,
}

impl GeneratedChain {
    /// Generates connected Regtest blocks `1..=(base_blocks + live_blocks)` and
    /// precomputes their exact commitment-tree checkpoints and completed subtrees.
    pub(crate) fn build(spec: ChainSpecConfig) -> Result<Self, BenchError> {
        let base_raw = decode_fixture_block_bytes(REGTEST_BLOCK_1_JSON)?;
        let sample_v5_tx = extract_v5_shielded_tx_bytes(ORCHARD_TX_BLOCK_JSON)?;
        let (template_header_and_sol, template_coinbase) = split_coinbase_block(&base_raw)?;
        let activations = Arc::new(regtest_activations()?);
        let total_blocks = spec
            .base_blocks
            .checked_add(spec.live_blocks)
            .ok_or_else(|| BenchError::invalid_argument("chain height overflow"))?;

        let mut blocks = BTreeMap::new();
        let mut parent_hash = Network::ZcashRegtest.genesis_hash();
        let mut sparse_block_count = 0_u32;
        let mut dense_block_count = 0_u32;

        for raw_height in 1_u32..=total_blocks {
            let is_dense = raw_height > 1
                && spec.dense_block_cadence > 0
                && raw_height.is_multiple_of(spec.dense_block_cadence);
            let tx_copies = if is_dense {
                spec.dense_txs_per_block
            } else {
                0
            };
            if is_dense && tx_copies > 0 {
                dense_block_count = dense_block_count.saturating_add(1);
            } else {
                sparse_block_count = sparse_block_count.saturating_add(1);
            }
            let block_bytes = assemble_block_bytes(&BlockAssemblySpec {
                raw_height,
                parent_hash,
                template_header_and_sol: &template_header_and_sol,
                template_coinbase: &template_coinbase,
                sample_v5_tx: &sample_v5_tx,
                v5_tx_copies: tx_copies,
            })?;
            let height = BlockHeight::new(raw_height);
            let source_block =
                SourceBlock::from_raw_block_bytes(Network::ZcashRegtest, height, block_bytes)?;
            parent_hash = source_block.hash;
            blocks.insert(height, source_block);
        }

        let (checkpoints, subtrees) = derive_chain_commitments(&blocks, &activations)?;
        Ok(Self {
            network: Network::ZcashRegtest,
            activations,
            blocks: Arc::new(blocks),
            checkpoints: Arc::new(checkpoints),
            subtrees: Arc::new(subtrees),
            base_tip_height: BlockHeight::new(spec.base_blocks),
            final_tip_height: BlockHeight::new(total_blocks),
            reorg_window_blocks: spec.reorg_window_blocks,
            sparse_block_count,
            dense_block_count,
            sample_v5_tx: Arc::new(sample_v5_tx),
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

/// Pre-built canonical and wallet stores positioned at `base_tip_height` so all
/// four matched arms start from identical on-disk state.
pub(crate) struct TemplateStores {
    pub(crate) _root: tempfile::TempDir,
    pub(crate) canonical_path: PathBuf,
    pub(crate) wallet_path: PathBuf,
}

impl TemplateStores {
    /// Builds fresh READY canonical and wallet stores at `chain.base_tip_height`.
    pub(crate) async fn build(chain: &GeneratedChain) -> Result<Self, BenchError> {
        let root = tempfile::tempdir().map_err(|source| BenchError::io("tempdir", source))?;
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

    /// Recursively copies `canonical_base` and `wallet_base` into fresh target directories.
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
        let blocks: Vec<SourceBlock> = self
            .chain
            .blocks
            .range(start..=tip)
            .take(usize::try_from(limits.max_connected_blocks.get()).unwrap_or(usize::MAX))
            .map(|(_, block)| block.clone())
            .collect();
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
    if raw_v5_tx.len() < 20 {
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
    let mut offset = 20_usize;
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

    let mut stripped = Vec::with_capacity(20 + 2 + raw_v5_tx.len().saturating_sub(offset));
    stripped.extend_from_slice(&raw_v5_tx[..20]);
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

/// Returns `(header_plus_equihash, template_coinbase)` from the Regtest block-1 fixture.
fn split_coinbase_block(raw_block: &[u8]) -> Result<(Vec<u8>, Vec<u8>), BenchError> {
    const HEADER_AND_SOLUTION_END: usize = 1_487;
    if raw_block.len() <= HEADER_AND_SOLUTION_END + 1 {
        return Err(BenchError::fixture_format(
            "regtest block-1 fixture shorter than header and equihash solution",
        ));
    }
    let header_and_sol = raw_block[..HEADER_AND_SOLUTION_END].to_vec();
    let coinbase_bytes = raw_block[HEADER_AND_SOLUTION_END + 1..].to_vec();
    Ok((header_and_sol, coinbase_bytes))
}

struct BlockAssemblySpec<'a> {
    raw_height: u32,
    parent_hash: zinder_core::BlockHash,
    template_header_and_sol: &'a [u8],
    template_coinbase: &'a [u8],
    sample_v5_tx: &'a [u8],
    v5_tx_copies: u32,
}

fn assemble_block_bytes(spec: &BlockAssemblySpec<'_>) -> Result<Vec<u8>, BenchError> {
    let mut header_and_sol = spec.template_header_and_sol.to_vec();
    header_and_sol[4..36].copy_from_slice(&spec.parent_hash.as_bytes());
    let timestamp = u32::try_from(BASE_UNIX_TIMESTAMP_MILLIS / 1_000)
        .unwrap_or(1_783_933_200)
        .saturating_add(spec.raw_height);
    header_and_sol[100..104].copy_from_slice(&timestamp.to_le_bytes());

    let coinbase_bytes = rewrite_coinbase_with_height(spec.template_coinbase, spec.raw_height)?;
    let coinbase = Transaction::zcash_deserialize(coinbase_bytes.as_slice())
        .map_err(|source| BenchError::tip_replay(source.to_string()))?;

    let mut tx_hashes = Vec::with_capacity(usize::try_from(spec.v5_tx_copies).unwrap_or(0) + 1);
    tx_hashes.push(coinbase.hash());
    let mut extra_tx_bytes = Vec::new();
    for copy_idx in 0..spec.v5_tx_copies {
        let mut tweaked = spec.sample_v5_tx.to_vec();
        if tweaked.len() >= 16 {
            let lock_marker = spec
                .raw_height
                .wrapping_mul(1_000_003)
                .wrapping_add(copy_idx);
            tweaked[12..16].copy_from_slice(&lock_marker.to_le_bytes());
        }
        let parsed = Transaction::zcash_deserialize(tweaked.as_slice())
            .map_err(|source| BenchError::tip_replay(source.to_string()))?;
        tx_hashes.push(parsed.hash());
        extra_tx_bytes.extend_from_slice(&tweaked);
    }

    let merkle_root: MerkleRoot = tx_hashes.into_iter().collect();
    header_and_sol[36..68].copy_from_slice(&merkle_root.0);

    let total_txs = u64::from(spec.v5_tx_copies) + 1;
    let mut out =
        Vec::with_capacity(header_and_sol.len() + 9 + coinbase_bytes.len() + extra_tx_bytes.len());
    out.extend_from_slice(&header_and_sol);
    write_compact_size(&mut out, total_txs);
    out.extend_from_slice(&coinbase_bytes);
    out.extend_from_slice(&extra_tx_bytes);
    Ok(out)
}

/// Replaces the BIP34 height push at offset 41 inside `template_coinbase`'s scriptSig
/// (`04000080 | 85202f89 | 01 | 00..00 (32) | ffffffff (4)` -> scriptSig len at index 45).
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
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use zebra_chain::{serialization::ZcashDeserialize as _, transaction::Transaction};
    use zinder_ingest::{RawBlobPolicy, prepare_canonical_block};

    use super::{ChainSpecConfig, GeneratedChain, bip34_height_push};

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
    fn generated_chain_produces_parseable_sparse_and_orchard_dense_blocks()
    -> Result<(), crate::BenchError> {
        let chain = GeneratedChain::build(ChainSpecConfig {
            base_blocks: 20,
            live_blocks: 6,
            reorg_window_blocks: 2,
            dense_block_cadence: 5,
            dense_txs_per_block: 2,
        })?;

        let sample_tx = Transaction::zcash_deserialize(chain.sample_v5_tx.as_slice())
            .map_err(|source| crate::BenchError::fixture_format(source.to_string()))?;
        assert_eq!(
            sample_tx.inputs(),
            [] as [zebra_chain::transparent::Input; 0]
        );
        assert_eq!(
            sample_tx.outputs(),
            [] as [zebra_chain::transparent::Output; 0]
        );
        assert!(sample_tx.has_orchard_shielded_data());

        assert!(chain.sparse_block_count > 0);
        assert!(chain.dense_block_count > 0);
        for (height, block) in chain.blocks.iter() {
            let prepared =
                prepare_canonical_block(block, &chain.activations, RawBlobPolicy::Transactions)?;
            if height.value().is_multiple_of(5) {
                assert!(prepared.tree_size_additions.orchard > 0);
            }
        }
        Ok(())
    }
}
