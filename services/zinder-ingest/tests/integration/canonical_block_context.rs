#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{collections::BTreeMap, error::Error, path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use tempfile::TempDir;
use zcash_primitives::transaction::{Authorized as AuthorizedTransaction, TransactionData};
use zcash_protocol::{consensus::BlockHeight as ZcashBlockHeight, value::Zatoshis};
use zcash_transparent::address::Script as TransparentScript;
use zcash_transparent::bundle::{
    Authorized as TransparentAuthorized, Bundle, OutPoint, TxIn, TxOut,
};
use zebra_chain::{
    block::Block as ZebraBlock,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::Transaction as ZebraTransaction,
};
use zinder_core::{
    BlockFinalNoteCommitmentRoots, BlockHash, BlockHeight, BlockHeightRange, BlockId,
    CanonicalBlockFacts, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontier, CommitmentTreeFrontiers, Network, NetworkUpgradeActivations,
    ShieldedProtocol, TransactionId, TransparentOutPoint, UnixTimestampMillis,
};
use zinder_ingest::{
    CanonicalBlockContextReader, CanonicalConstructionConfig, IngestError, RawBlobPolicy,
    load_fresh_canonical, prepare_canonical_block, require_genesis_complete_history,
};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceChainSegment, SourceChainSegmentLimits,
    SourceError,
};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
    CanonicalStoreWorkload, RocksDbCanonicalBuilder, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore, RocksDbResourceBudget,
};
use zinder_testkit::sample_regtest_upgrade_activations;

use super::fixture_block::{
    fixture_orchard_source_block, fixture_sapling_source_block, fixture_source_block,
};

const SERIALIZED_HEADER_PARENT_OFFSET: usize = 4;
const SERIALIZED_HEADER_PARENT_END: usize = SERIALIZED_HEADER_PARENT_OFFSET + 32;
const COINBASE_HEIGHT_OPCODE_OFFSET: usize = 1_534;
const CHAIN_TIP_HEIGHT: u32 = 4;
const SAPLING_BLOCK_HEIGHT: u32 = 2;
const SPENDING_BLOCK_HEIGHT: u32 = 3;
const ORCHARD_BLOCK_HEIGHT: u32 = 4;
const SPEND_OUTPUT_VALUE_ZAT: u64 = 12_345;
const SPEND_OUTPUT_SCRIPT: [u8; 5] = [0x76, 0xa9, 0x14, 0x01, 0x02];
const REORG_WINDOW_BLOCKS: u32 = 100;

#[tokio::test]
async fn canonical_block_contexts_carry_header_transaction_and_intrinsic_facts()
-> Result<(), Box<dyn Error>> {
    let harness = CanonicalHarness::genesis_complete().await?;
    let activations = sample_regtest_upgrade_activations();
    let mut reader = CanonicalBlockContextReader::new(harness.secondary(), &activations);

    let contexts = reader.read_block_commit_contexts(chain_range())?;

    assert_eq!(contexts.len(), usize::try_from(CHAIN_TIP_HEIGHT)?);
    for (height, source_block) in &harness.blocks {
        let context = contexts
            .get(height)
            .ok_or_else(|| format!("hydration must cover height {}", height.value()))?;
        assert_eq!(context.height, *height);
        assert_eq!(context.block_hash, source_block.hash);
        assert_eq!(context.previous_block_hash, source_block.parent_hash);
        assert_eq!(
            context.block_time_unix_seconds,
            i64::from(source_block.block_time_seconds)
        );
        assert_eq!(
            context.block_size_bytes,
            u64::try_from(source_block.raw_block_bytes.len())?
        );

        let facts = source_block_facts(source_block)?;
        let balances = context
            .transaction_intrinsic_value_balances()
            .ok_or("intrinsic value balances must be hydrated")?;
        assert_eq!(context.transactions.len(), facts.transactions.len());
        for (index, expected) in facts.transactions.iter().enumerate() {
            let transaction = context
                .transactions
                .get(index)
                .ok_or("hydrated transaction must exist")?;
            assert_eq!(transaction.public_facts, expected.public_facts);
            assert_eq!(transaction.transparent_inputs, expected.transparent_inputs);
            assert_eq!(
                transaction.transparent_outputs,
                expected.transparent_outputs
            );
            assert_eq!(transaction.location.block_height, *height);
            assert_eq!(transaction.location.block_hash, source_block.hash);
            assert_eq!(
                transaction.location.tx_index_in_block,
                u32::try_from(index)?
            );
            assert_eq!(
                balances.get(&expected.public_facts.transaction_id),
                Some(&expected.intrinsic_value_balances)
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn derived_final_note_commitment_roots_match_independent_accumulation()
-> Result<(), Box<dyn Error>> {
    let harness = CanonicalHarness::genesis_complete().await?;
    let activations = sample_regtest_upgrade_activations();
    let expected = independent_final_roots(&harness.blocks, &activations)?;
    let sapling_root_at = |height: u32| {
        expected
            .get(&BlockHeight::new(height))
            .and_then(|roots| roots.sapling)
    };
    let orchard_root_at = |height: u32| {
        expected
            .get(&BlockHeight::new(height))
            .and_then(|roots| roots.orchard)
    };
    assert_ne!(sapling_root_at(1), sapling_root_at(SAPLING_BLOCK_HEIGHT));
    assert_ne!(
        orchard_root_at(SPENDING_BLOCK_HEIGHT),
        orchard_root_at(ORCHARD_BLOCK_HEIGHT)
    );

    let mut sequential = CanonicalBlockContextReader::new(harness.secondary(), &activations);
    let contexts = sequential.read_block_commit_contexts(chain_range())?;
    for (height, roots) in &expected {
        let context = contexts
            .get(height)
            .ok_or_else(|| format!("hydration must cover height {}", height.value()))?;
        assert_eq!(context.final_note_commitment_roots.as_ref(), Some(roots));
    }

    let mut reseeding = CanonicalBlockContextReader::new(harness.secondary(), &activations);
    let tail = BlockHeightRange::inclusive(
        BlockHeight::new(SPENDING_BLOCK_HEIGHT),
        BlockHeight::new(CHAIN_TIP_HEIGHT),
    );
    let reseeded = reseeding.read_block_commit_contexts(tail)?;
    for height in tail {
        let context = reseeded
            .get(&height)
            .ok_or_else(|| format!("reseeded hydration must cover height {}", height.value()))?;
        assert_eq!(
            context.final_note_commitment_roots.as_ref(),
            expected.get(&height)
        );
    }
    Ok(())
}

#[tokio::test]
async fn cross_block_transparent_spends_resolve_from_the_producing_block()
-> Result<(), Box<dyn Error>> {
    let harness = CanonicalHarness::genesis_complete().await?;
    let activations = sample_regtest_upgrade_activations();
    let produced = harness.block_at(BlockHeight::new(1))?;
    let spent_outpoint = TransparentOutPoint::new(coinbase_transaction_id(produced)?, 0);
    let spending_block = harness.block_at(BlockHeight::new(SPENDING_BLOCK_HEIGHT))?;
    let spending_facts = source_block_facts(spending_block)?;
    let spending_transaction = spending_facts
        .transactions
        .get(1)
        .ok_or("the spending block must carry a second transaction")?;

    let mut whole_range = CanonicalBlockContextReader::new(harness.secondary(), &activations);
    let contexts = whole_range.read_block_commit_contexts(chain_range())?;
    let spends = contexts
        .get(&BlockHeight::new(SPENDING_BLOCK_HEIGHT))
        .ok_or("hydration must cover the spending block")?
        .transparent_spends()
        .ok_or("transparent spends must be hydrated")?;

    assert_eq!(spends.len(), 1);
    let spend = spends
        .get(&spent_outpoint)
        .ok_or("the cross-block spend must resolve")?;
    assert_eq!(spend.input_index, 0);
    assert_eq!(
        spend.spending_transaction_id,
        spending_transaction.public_facts.transaction_id
    );
    assert_eq!(spend.tx_index_in_block, 1);
    assert_eq!(spend.block_height, BlockHeight::new(SPENDING_BLOCK_HEIGHT));
    assert_eq!(spend.block_hash, spending_block.hash);
    assert_eq!(spend.spent_value_zat, coinbase_output_value_zat(produced)?);
    assert_eq!(
        spend.spent_address_script_hash,
        coinbase_output_script_hash(produced)?
    );
    assert_eq!(spend.spent_block_height, BlockHeight::new(1));
    assert_eq!(spend.spent_block_hash, produced.hash);

    let mut single_block = CanonicalBlockContextReader::new(harness.secondary(), &activations);
    let spending_only = single_block.read_block_commit_contexts(BlockHeightRange::inclusive(
        BlockHeight::new(SPENDING_BLOCK_HEIGHT),
        BlockHeight::new(SPENDING_BLOCK_HEIGHT),
    ))?;
    let out_of_range_spends = spending_only
        .get(&BlockHeight::new(SPENDING_BLOCK_HEIGHT))
        .ok_or("hydration must cover the spending block")?
        .transparent_spends()
        .ok_or("transparent spends must be hydrated")?;
    assert_eq!(out_of_range_spends.get(&spent_outpoint), Some(spend));
    Ok(())
}

#[tokio::test]
async fn unresolvable_transparent_prevouts_fail_hydration() -> Result<(), Box<dyn Error>> {
    let fabricated = TransactionId::from_bytes([0x5c; 32]);
    let harness = CanonicalHarness::spending(TransparentOutPoint::new(fabricated, 0)).await?;
    let activations = sample_regtest_upgrade_activations();
    let mut reader = CanonicalBlockContextReader::new(harness.secondary(), &activations);

    let error = reader
        .read_block_commit_contexts(chain_range())
        .err()
        .ok_or("hydration must refuse an unresolvable prevout")?;

    let IngestError::TransparentPrevoutUnresolved {
        transaction_id,
        output_index,
        ..
    } = error
    else {
        return Err(format!("expected an unresolved prevout error, got {error:?}").into());
    };
    assert_eq!(transaction_id, fabricated);
    assert_eq!(output_index, 0);
    Ok(())
}

#[tokio::test]
async fn materialized_view_hydration_requires_genesis_complete_history()
-> Result<(), Box<dyn Error>> {
    let complete = CanonicalHarness::genesis_complete().await?;
    require_genesis_complete_history(complete.secondary())?;

    let checkpointed = CanonicalHarness::checkpointed_at_height_one().await?;
    let error = require_genesis_complete_history(checkpointed.secondary())
        .err()
        .ok_or("a checkpointed store must be refused")?;

    let IngestError::MaterializedViewHistoryIncomplete {
        first_available_height,
    } = error
    else {
        return Err(format!("expected an incomplete-history refusal, got {error:?}").into());
    };
    assert_eq!(first_available_height, BlockHeight::new(2));
    Ok(())
}

const fn chain_range() -> BlockHeightRange {
    BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(CHAIN_TIP_HEIGHT))
}

/// A published canonical store plus the in-process secondary tests read through.
struct CanonicalHarness {
    blocks: BTreeMap<BlockHeight, SourceBlock>,
    secondary: RocksDbCanonicalSecondary,
    _store: RocksDbCanonicalStore,
    _temporary: TempDir,
}

impl CanonicalHarness {
    async fn genesis_complete() -> Result<Self, Box<dyn Error>> {
        let blocks = coinbase_chain()?;
        let produced = blocks
            .get(&BlockHeight::new(1))
            .ok_or("the chain must start at height 1")?;
        let spent_outpoint = TransparentOutPoint::new(coinbase_transaction_id(produced)?, 0);
        Self::spending(spent_outpoint).await
    }

    async fn spending(spent_outpoint: TransparentOutPoint) -> Result<Self, Box<dyn Error>> {
        let blocks = spending_chain(spent_outpoint)?;
        let first_block = blocks
            .values()
            .next()
            .ok_or("the chain must not be empty")?
            .clone();
        let build_plan = CanonicalStoreBuildPlan::complete(
            &sample_regtest_upgrade_activations(),
            first_block.block_time_seconds.saturating_sub(1),
            block_id(&blocks, CHAIN_TIP_HEIGHT)?,
            CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
        )?;
        Self::publish(blocks, build_plan).await
    }

    async fn checkpointed_at_height_one() -> Result<Self, Box<dyn Error>> {
        let blocks = coinbase_chain()?;
        let checkpoint_block = blocks
            .get(&BlockHeight::new(1))
            .ok_or("the chain must start at height 1")?;
        let build_plan = CanonicalStoreBuildPlan::checkpointed(
            &sample_regtest_upgrade_activations(),
            CommitmentTreeCheckpoint::new(
                BlockId::new(BlockHeight::new(1), checkpoint_block.hash),
                checkpoint_block.block_time_seconds,
                sapling_only_frontiers(),
            ),
            block_id(&blocks, CHAIN_TIP_HEIGHT)?,
            CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
        )?;
        Self::publish(blocks, build_plan).await
    }

    async fn publish(
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        build_plan: CanonicalStoreBuildPlan,
    ) -> Result<Self, Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = build_canonical_store(&store_path, &blocks, build_plan).await?;
        let secondary = RocksDbCanonicalSecondary::open_ready(
            &store_path,
            temporary.path().join("canonical-secondary"),
            &sample_regtest_upgrade_activations(),
            CanonicalStoreWorkload::Wallet,
            CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        Ok(Self {
            blocks,
            secondary,
            _store: store,
            _temporary: temporary,
        })
    }

    const fn secondary(&self) -> &RocksDbCanonicalSecondary {
        &self.secondary
    }

    fn block_at(&self, height: BlockHeight) -> Result<&SourceBlock, Box<dyn Error>> {
        self.blocks
            .get(&height)
            .ok_or_else(|| format!("the chain must contain height {}", height.value()).into())
    }
}

async fn build_canonical_store(
    store_path: &Path,
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    build_plan: CanonicalStoreBuildPlan,
) -> Result<RocksDbCanonicalStore, Box<dyn Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let builder = RocksDbCanonicalBuilder::create_fresh(
        store_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let source = StaticChainSource {
        blocks: blocks.clone(),
        checkpoints: chain_checkpoints(blocks, &activations)?,
    };
    let construction = CanonicalConstructionConfig::for_local_tests(
        Duration::from_secs(5),
        Arc::clone(&activations),
    );
    let built = load_fresh_canonical(builder, &source, &construction).await?;
    let validated = built.builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        block_id(blocks, CHAIN_TIP_HEIGHT)?,
        UnixTimestampMillis::new(1_783_933_200_000),
    ))?;
    Ok(validated.publish_baseline(publication)?)
}

/// Deterministic replay of one prepared chain, without a node.
#[derive(Clone)]
struct StaticChainSource {
    blocks: BTreeMap<BlockHeight, SourceBlock>,
    checkpoints: BTreeMap<BlockHeight, CommitmentTreeCheckpoint>,
}

#[async_trait]
impl NodeSource for StaticChainSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.blocks
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "static chain has no block at the requested height".to_owned(),
            })
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        let Some(start_height) = limits.cursor.next_connected_height() else {
            return Ok(SourceChainSegment::default());
        };
        let blocks = self
            .blocks
            .range(start_height..)
            .take(usize::try_from(limits.max_connected_blocks.get()).unwrap_or(usize::MAX))
            .map(|(_, block)| block.clone())
            .collect::<Vec<_>>();
        Ok(SourceChainSegment::connected_blocks(blocks))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.checkpoints
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "static chain has no checkpoint at the requested height".to_owned(),
            })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        let (height, block) =
            self.blocks
                .iter()
                .next_back()
                .ok_or_else(|| SourceError::BlockUnavailable {
                    height: BlockHeight::new(0),
                    reason: "static chain is empty".to_owned(),
                })?;
        Ok(BlockId::new(*height, block.hash))
    }
}

fn chain_checkpoints(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    activations: &NetworkUpgradeActivations,
) -> Result<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>, Box<dyn Error>> {
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        BlockHeight::new(0),
        &CommitmentTreeFrontiers::default(),
        activations,
    )?;
    let mut checkpoints = BTreeMap::new();
    for (height, block) in blocks {
        append_source_block(&mut accumulator, block)?;
        checkpoints.insert(
            *height,
            CommitmentTreeCheckpoint::new(
                BlockId::new(*height, block.hash),
                block.block_time_seconds,
                accumulator.validated_frontiers()?,
            ),
        );
    }
    Ok(checkpoints)
}

fn independent_final_roots(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    activations: &NetworkUpgradeActivations,
) -> Result<BTreeMap<BlockHeight, BlockFinalNoteCommitmentRoots>, Box<dyn Error>> {
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        BlockHeight::new(0),
        &CommitmentTreeFrontiers::default(),
        activations,
    )?;
    let mut roots = BTreeMap::new();
    for (height, block) in blocks {
        append_source_block(&mut accumulator, block)?;
        roots.insert(*height, accumulator.final_note_commitment_roots(block.hash));
    }
    Ok(roots)
}

fn append_source_block(
    accumulator: &mut CommitmentTreeAccumulator,
    source_block: &SourceBlock,
) -> Result<(), Box<dyn Error>> {
    let commitments = pool_commitments(source_block)?;
    accumulator.append_block_commitments(
        source_block.height,
        &commitments.sapling,
        &commitments.orchard,
        &commitments.ironwood,
    )?;
    Ok(())
}

#[derive(Default)]
struct PoolCommitments {
    sapling: Vec<[u8; 32]>,
    orchard: Vec<[u8; 32]>,
    ironwood: Vec<[u8; 32]>,
}

fn pool_commitments(source_block: &SourceBlock) -> Result<PoolCommitments, Box<dyn Error>> {
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

fn sapling_only_frontiers() -> CommitmentTreeFrontiers {
    CommitmentTreeFrontiers::from_validated_parts(
        Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
        None,
        None,
    )
}

fn block_id(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    height: u32,
) -> Result<BlockId, Box<dyn Error>> {
    let height = BlockHeight::new(height);
    let block = blocks
        .get(&height)
        .ok_or_else(|| format!("the chain must contain height {}", height.value()))?;
    Ok(BlockId::new(height, block.hash))
}

fn coinbase_chain() -> Result<BTreeMap<BlockHeight, SourceBlock>, Box<dyn Error>> {
    chain_with_extra_transactions(&|_| Ok(Vec::new()))
}

fn spending_chain(
    spent_outpoint: TransparentOutPoint,
) -> Result<BTreeMap<BlockHeight, SourceBlock>, Box<dyn Error>> {
    chain_with_extra_transactions(&|height| {
        let mut transactions = shielded_transactions_at(height)?;
        if height == SPENDING_BLOCK_HEIGHT {
            transactions.push(Arc::new(spending_transaction(spent_outpoint)?));
        }
        Ok(transactions)
    })
}

type ExtraTransactions<'chain> =
    &'chain dyn Fn(u32) -> Result<Vec<Arc<ZebraTransaction>>, Box<dyn Error>>;

fn chain_with_extra_transactions(
    extra_transactions: ExtraTransactions<'_>,
) -> Result<BTreeMap<BlockHeight, SourceBlock>, Box<dyn Error>> {
    let mut parent_hash = Network::ZcashRegtest.genesis_hash();
    let mut blocks = BTreeMap::new();
    for height in 1..=CHAIN_TIP_HEIGHT {
        let block = chained_source_block(
            BlockHeight::new(height),
            parent_hash,
            extra_transactions(height)?,
        )?;
        parent_hash = block.hash;
        blocks.insert(block.height, block);
    }
    Ok(blocks)
}

fn shielded_transactions_at(height: u32) -> Result<Vec<Arc<ZebraTransaction>>, Box<dyn Error>> {
    if height == SAPLING_BLOCK_HEIGHT {
        return shielded_transactions(&fixture_sapling_source_block()?);
    }
    if height == ORCHARD_BLOCK_HEIGHT {
        return shielded_transactions(&fixture_orchard_source_block()?);
    }
    Ok(Vec::new())
}

/// Lifts a fixture block's note-commitment-bearing transactions into the synthetic chain.
///
/// Transparent inputs are dropped: their previous outputs belong to the
/// fixture's own chain, which the synthetic chain does not contain.
fn shielded_transactions(
    fixture: &SourceBlock,
) -> Result<Vec<Arc<ZebraTransaction>>, Box<dyn Error>> {
    let block: ZebraBlock = fixture
        .raw_block_bytes
        .as_slice()
        .zcash_deserialize_into()?;
    let transactions = block
        .transactions
        .iter()
        .filter(|transaction| !transaction.is_coinbase() && creates_note_commitments(transaction))
        .map(|transaction| without_transparent_inputs(transaction).map(Arc::new))
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
) -> Result<ZebraTransaction, Box<dyn Error>> {
    match transaction.tx_version() {
        zebra_chain::transaction::TxVersion::V4 | zebra_chain::transaction::TxVersion::V5 => {
            let transparent_bundle = transaction.transparent_bundle().map(|bundle| Bundle {
                vin: Vec::new(),
                vout: bundle.vout.clone(),
                authorization: bundle.authorization,
            });
            let transaction_data = TransactionData::<AuthorizedTransaction>::from_parts(
                transaction.tx_version(),
                transaction.consensus_branch_id(),
                transaction.raw_lock_time(),
                ZcashBlockHeight::from(transaction.expiry_height().map_or(0, |height| height.0)),
                transparent_bundle,
                transaction.sprout_bundle().cloned(),
                transaction.sapling_bundle().cloned(),
                transaction.orchard_bundle().cloned(),
            );
            let mut bytes = Vec::new();
            transaction_data.freeze()?.write(&mut bytes)?;
            Ok(bytes.as_slice().zcash_deserialize_into()?)
        }
        zebra_chain::transaction::TxVersion::Sprout(_)
        | zebra_chain::transaction::TxVersion::V3
        | zebra_chain::transaction::TxVersion::V6 => {
            Err("fixture shielded transactions must be v4 or v5".into())
        }
    }
}

fn chained_source_block(
    height: BlockHeight,
    parent_hash: BlockHash,
    extra_transactions: Vec<Arc<ZebraTransaction>>,
) -> Result<SourceBlock, Box<dyn Error>> {
    let fixture = fixture_source_block()?;
    let mut raw_block_bytes = fixture.raw_block_bytes;
    raw_block_bytes
        .get_mut(SERIALIZED_HEADER_PARENT_OFFSET..SERIALIZED_HEADER_PARENT_END)
        .ok_or("fixture block is too short to carry a parent hash")?
        .copy_from_slice(&parent_hash.as_bytes());
    let coinbase_height_opcode = raw_block_bytes
        .get_mut(COINBASE_HEIGHT_OPCODE_OFFSET)
        .ok_or("fixture block is too short to carry a coinbase height")?;
    *coinbase_height_opcode = 0x50_u8.saturating_add(u8::try_from(height.value())?);
    let mut block: ZebraBlock = raw_block_bytes.as_slice().zcash_deserialize_into()?;
    block.transactions.extend(extra_transactions);
    let raw_block_bytes = block.zcash_serialize_to_vec()?;
    Ok(SourceBlock::from_raw_block_bytes(
        Network::ZcashRegtest,
        height,
        raw_block_bytes,
    )?)
}

fn spending_transaction(
    spent_outpoint: TransparentOutPoint,
) -> Result<ZebraTransaction, Box<dyn Error>> {
    let mut script_bytes = vec![u8::try_from(SPEND_OUTPUT_SCRIPT.len())?];
    script_bytes.extend_from_slice(&SPEND_OUTPUT_SCRIPT);
    let lock_script = TransparentScript::read(&mut script_bytes.as_slice())?;
    let bundle = Bundle::<TransparentAuthorized> {
        vin: vec![TxIn::from_parts(
            OutPoint::new(
                spent_outpoint.transaction_id.as_bytes(),
                spent_outpoint.output_index,
            ),
            TransparentScript::default(),
            u32::MAX,
        )],
        vout: vec![TxOut::new(
            Zatoshis::from_u64(SPEND_OUTPUT_VALUE_ZAT)?,
            lock_script,
        )],
        authorization: TransparentAuthorized,
    };
    let transaction = TransactionData::<AuthorizedTransaction>::from_parts(
        zebra_chain::transaction::TxVersion::V5,
        zcash_protocol::consensus::BranchId::Nu5,
        0,
        ZcashBlockHeight::from(0),
        Some(bundle),
        None,
        None,
        None,
    );
    let mut bytes = Vec::new();
    transaction.freeze()?.write(&mut bytes)?;
    Ok(bytes.as_slice().zcash_deserialize_into()?)
}

fn source_block_facts(source_block: &SourceBlock) -> Result<CanonicalBlockFacts, Box<dyn Error>> {
    Ok(prepare_canonical_block(
        source_block,
        &sample_regtest_upgrade_activations(),
        RawBlobPolicy::Transactions,
    )?
    .facts)
}

fn coinbase_transaction_id(source_block: &SourceBlock) -> Result<TransactionId, Box<dyn Error>> {
    Ok(coinbase_facts(source_block)?.public_facts.transaction_id)
}

fn coinbase_output_value_zat(source_block: &SourceBlock) -> Result<u64, Box<dyn Error>> {
    Ok(coinbase_output(source_block)?.value_zat)
}

fn coinbase_output_script_hash(
    source_block: &SourceBlock,
) -> Result<zinder_core::TransparentAddressScriptHash, Box<dyn Error>> {
    Ok(coinbase_output(source_block)?.address_script_hash)
}

fn coinbase_output(
    source_block: &SourceBlock,
) -> Result<zinder_core::TransparentOutputFact, Box<dyn Error>> {
    coinbase_facts(source_block)?
        .transparent_outputs
        .first()
        .cloned()
        .ok_or_else(|| "the fixture coinbase must create a transparent output".into())
}

fn coinbase_facts(
    source_block: &SourceBlock,
) -> Result<zinder_core::CanonicalTransactionFacts, Box<dyn Error>> {
    source_block_facts(source_block)?
        .transactions
        .first()
        .cloned()
        .ok_or_else(|| "every block must carry a coinbase transaction".into())
}
