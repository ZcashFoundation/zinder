//! Shared multi-block and fixture-chain construction helpers for ingest tests.

use std::{
    num::NonZeroU32,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use zcash_primitives::transaction::{Authorized, Transaction as ZcashTransaction, TransactionData};
use zebra_chain::{
    block::Block as ZebraBlock,
    serialization::{ZcashDeserialize, ZcashDeserializeInto, ZcashSerialize},
    transaction::Transaction as ZebraTransaction,
};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontiers, Network, NetworkUpgradeActivations,
};
use zinder_ingest::{
    CanonicalConstructionConfig, CanonicalFollowConfig, CanonicalWriterConfig,
    DEFAULT_MAX_REPLACEMENT_BYTES, RawBlobPolicy, prepare_canonical_block,
};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceBlockHeader, SourceChainSegment,
    SourceChainSegmentLimits, SourceError, decode_rpc_block_hash,
};
use zinder_store::{RawBlobRetention, RocksDbResourceBudget};
use zinder_testkit::sample_regtest_upgrade_activations;

type DynError = Box<dyn std::error::Error>;

const SERIALIZED_HEADER_PARENT_OFFSET: usize = 4;
const SERIALIZED_HEADER_PARENT_END: usize = SERIALIZED_HEADER_PARENT_OFFSET + 32;
const SERIALIZED_HEADER_TIME_OFFSET: usize = 100;
const COINBASE_SCRIPT_LENGTH_OFFSET: usize = 1_533;
const COINBASE_HEIGHT_OPCODE_OFFSET: usize = COINBASE_SCRIPT_LENGTH_OFFSET + 1;

/// Sample Regtest network upgrade activation schedule for construction tests.
pub(crate) fn regtest_activations() -> Arc<NetworkUpgradeActivations> {
    Arc::new(sample_regtest_upgrade_activations())
}

/// Decodes a captured block fixture JSON payload into a [`SourceBlock`].
pub(crate) fn fixture_source_block_from(
    network: Network,
    fixture_json: &str,
) -> Result<SourceBlock, DynError> {
    let fixture: Value = serde_json::from_str(fixture_json)?;
    let raw_block_hex = fixture_string_field(&fixture, "raw_block_hex")?;
    let raw_block_bytes = hex::decode(raw_block_hex)?;
    let height = fixture_u32_field(&fixture, "height")?;
    let source_block =
        SourceBlock::from_raw_block_bytes(network, BlockHeight::new(height), raw_block_bytes)?;

    assert_eq!(
        source_block.hash,
        decode_rpc_block_hash(fixture_string_field(&fixture, "hash")?)?
    );
    assert_eq!(
        source_block.parent_hash,
        decode_rpc_block_hash(fixture_string_field(&fixture, "previousblockhash")?)?
    );
    assert_eq!(
        source_block.block_time_seconds,
        fixture_u32_field(&fixture, "time")?
    );

    Ok(source_block)
}

/// Regtest block 1 coinbase fixture.
pub(crate) fn fixture_source_block() -> Result<SourceBlock, DynError> {
    fixture_source_block_from(
        Network::ZcashRegtest,
        include_str!("../fixtures/z3-regtest-block-1.json"),
    )
}

/// Testnet Sapling fixture block.
pub(crate) fn fixture_sapling_source_block() -> Result<SourceBlock, DynError> {
    fixture_source_block_from(
        Network::ZcashTestnet,
        include_str!("../fixtures/zcash-testnet-sapling-block-1842432.json"),
    )
}

/// Testnet Orchard fixture block.
pub(crate) fn fixture_orchard_source_block() -> Result<SourceBlock, DynError> {
    fixture_source_block_from(
        Network::ZcashTestnet,
        include_str!("../fixtures/zcash-testnet-orchard-block-1842462.json"),
    )
}

fn fixture_string_field<'fixture>(
    fixture: &'fixture Value,
    field_name: &'static str,
) -> Result<&'fixture str, DynError> {
    fixture
        .get(field_name)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("fixture field {field_name} must be a string").into())
}

fn fixture_u32_field(fixture: &Value, field_name: &'static str) -> Result<u32, DynError> {
    let number = fixture
        .get(field_name)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("fixture field {field_name} must be an integer"))?;

    Ok(u32::try_from(number)?)
}

/// Extracted Sapling, Orchard, and Ironwood note commitments from one block.
#[derive(Default)]
pub(crate) struct PoolCommitments {
    pub(crate) sapling: Vec<[u8; 32]>,
    pub(crate) orchard: Vec<[u8; 32]>,
    pub(crate) ironwood: Vec<[u8; 32]>,
}

/// Extracts per-pool note commitments from a [`SourceBlock`].
pub(crate) fn pool_commitments(source_block: &SourceBlock) -> Result<PoolCommitments, DynError> {
    let compact_block = prepare_canonical_block(
        source_block,
        &sample_regtest_upgrade_activations(),
        RawBlobPolicy::Transactions,
    )?
    .partial_compact_block;
    let mut commitments = PoolCommitments::default();
    for transaction in compact_block.transactions() {
        commitments.sapling.extend(
            transaction
                .data
                .sapling_outputs
                .iter()
                .map(|output| output.commitment),
        );
        commitments.orchard.extend(
            transaction
                .data
                .orchard_actions
                .iter()
                .map(|action| action.commitment),
        );
        commitments.ironwood.extend(
            transaction
                .data
                .ironwood_actions
                .iter()
                .map(|action| action.commitment),
        );
    }
    Ok(commitments)
}

/// Lifts a fixture block's note-commitment-bearing transactions into a synthetic chain.
pub(crate) fn shielded_transactions(
    fixture: &SourceBlock,
) -> Result<Vec<Arc<ZebraTransaction>>, DynError> {
    shielded_transactions_with_lock_time(fixture, None)
}

/// Lifts note-commitment-bearing transactions with an optional `lock_time` override.
pub(crate) fn shielded_transactions_with_lock_time(
    fixture: &SourceBlock,
    lock_time: Option<u32>,
) -> Result<Vec<Arc<ZebraTransaction>>, DynError> {
    let block: ZebraBlock = fixture
        .raw_block_bytes
        .as_slice()
        .zcash_deserialize_into()?;
    let transactions = block
        .transactions
        .iter()
        .filter(|transaction| !transaction.is_coinbase() && creates_note_commitments(transaction))
        .map(|transaction| without_transparent_inputs(transaction, lock_time).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    if transactions.is_empty() {
        return Err("fixture must carry a note-commitment-bearing transaction".into());
    }
    Ok(transactions)
}

fn creates_note_commitments(transaction: &ZebraTransaction) -> bool {
    transaction.sapling_outputs().next().is_some() || transaction.orchard_actions().next().is_some()
}

fn without_transparent_inputs(
    transaction: &ZebraTransaction,
    lock_time: Option<u32>,
) -> Result<ZebraTransaction, DynError> {
    let mut bytes = Vec::new();
    transaction.zcash_serialize(&mut bytes)?;
    let transaction_data =
        ZcashTransaction::read(bytes.as_slice(), transaction.consensus_branch_id())?.into_data();
    if !matches!(
        transaction_data.version(),
        zcash_primitives::transaction::TxVersion::V4 | zcash_primitives::transaction::TxVersion::V5
    ) {
        return Err("fixture shielded transactions must be v4 or v5".into());
    }

    let transparent_bundle = transaction_data
        .transparent_bundle()
        .cloned()
        .map(|mut bundle| {
            bundle.vin.clear();
            bundle
        });
    let rebuilt = TransactionData::<Authorized>::from_parts(
        transaction_data.version(),
        transaction_data.consensus_branch_id(),
        lock_time.unwrap_or_else(|| transaction_data.lock_time()),
        transaction_data.expiry_height(),
        transparent_bundle,
        transaction_data.sprout_bundle().cloned(),
        transaction_data.sapling_bundle().cloned(),
        transaction_data.orchard_bundle().cloned(),
    )
    .freeze()?;
    let mut rebuilt_bytes = Vec::new();
    rebuilt.write(&mut rebuilt_bytes)?;
    ZebraTransaction::zcash_deserialize(rebuilt_bytes.as_slice()).map_err(Into::into)
}

/// Builds a synthetic Regtest block chained to `parent_hash`.
pub(crate) fn chained_source_block(
    height: BlockHeight,
    parent_hash: BlockHash,
    time_nonce: u8,
    extra_transactions: Vec<Arc<ZebraTransaction>>,
) -> Result<SourceBlock, DynError> {
    let fixture = fixture_source_block()?;
    let mut raw_block_bytes = fixture.raw_block_bytes;
    raw_block_bytes
        .get_mut(SERIALIZED_HEADER_PARENT_OFFSET..SERIALIZED_HEADER_PARENT_END)
        .ok_or("fixture block is too short to carry a parent hash")?
        .copy_from_slice(&parent_hash.as_bytes());
    let block_time = raw_block_bytes
        .get_mut(SERIALIZED_HEADER_TIME_OFFSET)
        .ok_or("fixture block is too short to carry a block time")?;
    *block_time = block_time.wrapping_add(time_nonce);
    let height_byte = u8::try_from(height.value())?;
    let coinbase_height_opcode = raw_block_bytes
        .get_mut(COINBASE_HEIGHT_OPCODE_OFFSET)
        .ok_or("fixture block is too short to carry a coinbase height")?;
    if height_byte <= 16 {
        *coinbase_height_opcode = 0x50_u8.saturating_add(height_byte);
    } else {
        assert!(
            height_byte <= 127,
            "fixture needs a one-byte positive script integer"
        );
        *coinbase_height_opcode = 1;
        raw_block_bytes.insert(COINBASE_HEIGHT_OPCODE_OFFSET + 1, height_byte);
        let script_length = raw_block_bytes
            .get_mut(COINBASE_SCRIPT_LENGTH_OFFSET)
            .ok_or("fixture block is too short to carry a coinbase script length")?;
        *script_length = script_length
            .checked_add(1)
            .ok_or("fixture coinbase script is too long")?;
    }
    let mut block: ZebraBlock = raw_block_bytes.as_slice().zcash_deserialize_into()?;
    block.transactions.extend(extra_transactions);
    let raw_block_bytes = block.zcash_serialize_to_vec()?;
    Ok(SourceBlock::from_raw_block_bytes(
        Network::ZcashRegtest,
        height,
        raw_block_bytes,
    )?)
}

/// Multi-block Regtest source with exact per-height checkpoints and optional
/// cancellation hooks for construction lifecycle tests.
#[derive(Clone)]
pub(crate) struct MultiBlockConstructionSource {
    genesis: SourceBlock,
    blocks: Vec<SourceBlock>,
    checkpoints: Vec<CommitmentTreeCheckpoint>,
    pub(crate) hold_checkpoint: Option<(BlockHeight, Arc<Notify>)>,
    pub(crate) chain_segment_requests: Arc<AtomicUsize>,
    cancel_with_tip_checkpoint: Option<(BlockHeight, CancellationToken)>,
}

impl MultiBlockConstructionSource {
    /// Builds a synthetic Regtest chain with Sapling (height 2) and Orchard (height 4) blocks.
    pub(crate) fn build(
        height_count: u32,
        activations: &NetworkUpgradeActivations,
    ) -> Result<Self, DynError> {
        let sapling_shielded = shielded_transactions(&fixture_sapling_source_block()?)?;
        let orchard_shielded = shielded_transactions(&fixture_orchard_source_block()?)?;
        Self::build_with_transactions(height_count, activations, |height| {
            Ok(match height {
                2 => sapling_shielded.clone(),
                4 => orchard_shielded.clone(),
                _ => Vec::new(),
            })
        })
    }

    /// Builds a synthetic Regtest chain using `extra_transactions` per block height.
    pub(crate) fn build_with_transactions(
        height_count: u32,
        activations: &NetworkUpgradeActivations,
        extra_transactions: impl Fn(u32) -> Result<Vec<Arc<ZebraTransaction>>, DynError>,
    ) -> Result<Self, DynError> {
        let block_one = fixture_source_block()?;
        let genesis = SourceBlock::new(
            SourceBlockHeader {
                network: Network::ZcashRegtest,
                height: BlockHeight::new(0),
                hash: block_one.parent_hash,
                parent_hash: block_one.parent_hash,
                block_time_seconds: block_one.block_time_seconds.saturating_sub(1),
            },
            Vec::new(),
        );
        let mut blocks = Vec::with_capacity(usize::try_from(height_count)?);
        let mut parent_hash = genesis.hash;
        for height_num in 1..=height_count {
            let block = if height_num == 1 {
                block_one.clone()
            } else {
                chained_source_block(
                    BlockHeight::new(height_num),
                    parent_hash,
                    u8::try_from(height_num)?,
                    extra_transactions(height_num)?,
                )?
            };
            parent_hash = block.hash;
            blocks.push(block);
        }
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(0),
            &CommitmentTreeFrontiers::default(),
            activations,
        )?;
        let mut checkpoints = vec![CommitmentTreeCheckpoint::new(
            BlockId::new(genesis.height, genesis.hash),
            genesis.block_time_seconds,
            accumulator.cross_checked_frontiers()?,
        )];
        for block in &blocks {
            let commitments = pool_commitments(block)?;
            accumulator.append_block_commitments(
                block.height,
                &commitments.sapling,
                &commitments.orchard,
                &commitments.ironwood,
            )?;
            checkpoints.push(CommitmentTreeCheckpoint::new(
                BlockId::new(block.height, block.hash),
                block.block_time_seconds,
                accumulator.cross_checked_frontiers()?,
            ));
        }
        Ok(Self {
            genesis,
            blocks,
            checkpoints,
            hold_checkpoint: None,
            chain_segment_requests: Arc::new(AtomicUsize::new(0)),
            cancel_with_tip_checkpoint: None,
        })
    }

    /// Configures cancellation when `fetch_chain_checkpoint` reaches `height`.
    pub(crate) fn cancelling_at_checkpoint(
        mut self,
        height: BlockHeight,
        cancel: CancellationToken,
    ) -> Self {
        self.cancel_with_tip_checkpoint = Some((height, cancel));
        self
    }

    /// Returns the expected checkpoint at `height`.
    pub(crate) fn checkpoint_at(&self, height: u32) -> Result<&CommitmentTreeCheckpoint, DynError> {
        self.checkpoints
            .get(usize::try_from(height)?)
            .ok_or_else(|| format!("missing checkpoint at height {height}").into())
    }
}

#[async_trait]
impl NodeSource for MultiBlockConstructionSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        if height == BlockHeight::new(0) {
            return Ok(self.genesis.clone());
        }
        self.blocks
            .iter()
            .find(|block| block.height == height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "multi-block test source has no block at the requested height".to_owned(),
            })
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        self.chain_segment_requests.fetch_add(1, Ordering::SeqCst);
        let next_height =
            limits
                .cursor
                .next_connected_height()
                .ok_or(SourceError::SourceProtocolMismatch {
                    reason: "multi-block construction test cursor has no next height",
                })?;
        let max_blocks = usize::try_from(limits.max_connected_blocks.get()).unwrap_or(usize::MAX);
        let segment = self
            .blocks
            .iter()
            .filter(|block| block.height >= next_height)
            .take(max_blocks)
            .cloned()
            .collect::<Vec<_>>();
        Ok(SourceChainSegment::connected_blocks(segment))
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        let tip = self
            .blocks
            .last()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height: BlockHeight::new(1),
                reason: "multi-block test source has no blocks".to_owned(),
            })?;
        Ok(BlockId::new(tip.height, tip.hash))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        let checkpoint = self
            .checkpoints
            .iter()
            .find(|checkpoint| checkpoint.block_id.height == height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "multi-block test source has no checkpoint at the requested height"
                    .to_owned(),
            })?;
        if let Some((held_height, reached)) = &self.hold_checkpoint
            && *held_height == height
        {
            reached.notify_one();
            std::future::pending::<()>().await;
        }
        if let Some((cancel_height, cancel)) = &self.cancel_with_tip_checkpoint
            && *cancel_height == height
        {
            cancel.cancel();
        }
        Ok(checkpoint)
    }
}

/// Multi-block construction source that holds segment fetches until a test
/// permits the requested height span to proceed.
#[derive(Clone)]
pub(crate) struct GatedConstructionSource {
    inner: MultiBlockConstructionSource,
    allowed_through: Arc<AtomicU32>,
    gate_changed: Arc<Notify>,
}

impl GatedConstructionSource {
    /// Creates a gated source blocking all segment fetches above height `0`.
    pub(crate) fn new(inner: MultiBlockConstructionSource) -> Self {
        Self {
            inner,
            allowed_through: Arc::new(AtomicU32::new(0)),
            gate_changed: Arc::new(Notify::new()),
        }
    }

    /// Releases segment fetches up through `height`.
    pub(crate) fn allow_through(&self, height: u32) {
        self.allowed_through.store(height, Ordering::SeqCst);
        self.gate_changed.notify_waiters();
    }
}

#[async_trait]
impl NodeSource for GatedConstructionSource {
    fn capabilities(&self) -> NodeCapabilities {
        self.inner.capabilities()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.inner.fetch_block_at(height).await
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        let next_height =
            limits
                .cursor
                .next_connected_height()
                .ok_or(SourceError::SourceProtocolMismatch {
                    reason: "gated construction test cursor has no next height",
                })?;
        let required_top = next_height
            .value()
            .saturating_add(limits.max_connected_blocks.get())
            .saturating_sub(1);
        loop {
            let notified = self.gate_changed.notified();
            if required_top <= self.allowed_through.load(Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
        self.inner.fetch_chain_segment(limits).await
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.inner.tip_id().await
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.inner
            .fetch_chain_checkpoint(height, network_upgrade_activations)
            .await
    }
}

/// Default writer config for single-block held construction tests.
pub(crate) fn held_construction_writer_config(
    storage_path: PathBuf,
    activations: Arc<NetworkUpgradeActivations>,
) -> CanonicalWriterConfig {
    CanonicalWriterConfig {
        storage_path,
        resource_budget: RocksDbResourceBudget::for_local_tests(),
        construction: CanonicalConstructionConfig::for_local_tests(
            Duration::from_secs(5),
            activations,
        ),
        checkpoint_height: None,
        raw_blob_retention: RawBlobRetention::Transactions,
        reorg_window_blocks: 100,
        tree_state_checkpoint_selection: None,
        follow: CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(10),
            lag_threshold_blocks: 0,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
        },
    }
}

/// Writer config with `reorg_window_blocks = 1` for multi-block construction tests.
pub(crate) fn multi_block_writer_config(
    storage_path: PathBuf,
    activations: Arc<NetworkUpgradeActivations>,
    every_block_tree_state_checkpoints: bool,
) -> CanonicalWriterConfig {
    CanonicalWriterConfig {
        reorg_window_blocks: 1,
        tree_state_checkpoint_selection: every_block_tree_state_checkpoints
            .then_some(zinder_store::TreeStateCheckpointSelection::EveryBlock),
        ..held_construction_writer_config(storage_path, activations)
    }
}

/// Writer config using 3-block sequential segments for gated construction progress tests.
pub(crate) fn gated_construction_writer_config(
    storage_path: PathBuf,
    activations: Arc<NetworkUpgradeActivations>,
    tree_hashing_workers: u32,
) -> Result<CanonicalWriterConfig, DynError> {
    let mut config = multi_block_writer_config(storage_path, activations, false);
    config.construction.request_timeout = Duration::from_secs(30);
    config
        .construction
        .pipeline_limits
        .source_segment_max_blocks =
        NonZeroU32::new(3).ok_or("segment max blocks must be non-zero")?;
    config
        .construction
        .pipeline_limits
        .source_fetch_max_in_flight_requests =
        NonZeroU32::new(1).ok_or("in-flight requests must be non-zero")?;
    config.construction.pipeline_limits.tree_hashing_workers =
        NonZeroU32::new(tree_hashing_workers).ok_or("tree hashing workers must be non-zero")?;
    Ok(config)
}
