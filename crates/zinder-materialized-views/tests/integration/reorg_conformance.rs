//! Reorg path-independence conformance for every bundled consumer.
//!
//! Each case applies chain `A[1..n]`, reorgs `A[k..n]` onto `B[k..m]`, and
//! compares every column family a consumer declares against a fresh store
//! that applied `B[1..m]` directly. Events are dispatched through
//! [`MaterializedViewStore::write_chain_event_chunk_with_event_consumers`]
//! with the consumer set the registry selects, so the harness exercises the
//! same batch boundary, checkpoint, and cursor staging as replay.

use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt,
    sync::Arc,
};

use proptest::{
    strategy::{Just, Strategy},
    test_runner::{Config, TestCaseError, TestRunner},
};
use tempfile::TempDir;
use zinder_core::{
    ArtifactSchemaVersion, BlockFinalNoteCommitmentRoots, BlockHash, BlockHeight, BlockHeightRange,
    ChainEpoch, ChainEpochId, ChainTipMetadata, FinalNoteCommitmentRoot, LockTime, Network,
    PrivacyShape, TransactionComponentCounts, TransactionFactsArtifact, TransactionId,
    TransactionIntrinsicValueBalances, TransactionLocation, TransactionPublicFacts,
    TransactionVersion, TransparentAddressScriptHash, TransparentInputFact, TransparentOutPoint,
    TransparentOutputFact, TransparentSpendFact, UnixTimestampMillis, ValuePoolBalance,
};
use zinder_materialized_views::{
    BlockCommitContext, BlockCommitInput, BlockKeyedConsumer, BlockValuePoolBalanceFacts,
    ChainEventBlockJoin, ChainEventDispatchConsumers, ChainEventDispatchInputs,
    MaterializedViewConsumer, MaterializedViewConsumerDefinition, MaterializedViewDispatch,
    MaterializedViewStore, MaterializedViewStoreOptions, REORG_INCIDENTS_COLUMN_FAMILY,
    TransactionIntrinsicValueBalanceFacts, TransparentAddressRankingConsumer,
    TransparentAddressRankingCoverage, TransparentAddressRankingSnapshotPlan,
    TransparentAddressRankingSnapshotRow, TransparentAddressSummary, TransparentSpendFacts,
    bundled_materialized_view_consumer_definitions,
};
use zinder_store::{ChainEpochCommitted, ChainEvent, ChainRangeReverted, RocksDbResourceBudget};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type ColumnFamilyRows = BTreeMap<Vec<u8>, Vec<u8>>;

const PREFIX_BRANCH: u8 = 0;
const REPLACED_BRANCH: u8 = 1;
const REPLACEMENT_BRANCH: u8 = 2;
const FIRST_BLOCK_TIME: i64 = 1_700_006_400;
const BLOCK_SPACING_SECONDS: i64 = 28_800;
const BRANCH_TIME_SHIFT_SECONDS: i64 = 30_000;
const RANKING_GENERATION: u64 = 1;

/// Column families whose contents legitimately depend on the event path.
const PATH_DEPENDENT_COLUMN_FAMILIES: &[(&str, &str)] = &[(
    REORG_INCIDENTS_COLUMN_FAMILY,
    "append-only incident log; a reorg path records the reorg it observed",
)];

/// Reason a registry entry has no chain-event reorg to conform to, if any.
const fn reorg_exemption(definition: &MaterializedViewConsumerDefinition) -> Option<&'static str> {
    match definition.dispatch {
        MaterializedViewDispatch::ChainEventBlocks { .. }
        | MaterializedViewDispatch::ChainEventEnvelope { .. } => None,
        MaterializedViewDispatch::MempoolEvents { .. } => {
            Some("follows mempool events and never observes a chain reorg")
        }
        MaterializedViewDispatch::CanonicalBackfillOnly => {
            Some("written only by a settled-history backfill and never observes a chain reorg")
        }
    }
}

/// One reorg scenario: `A = prefix + replaced`, `B = prefix + replacement`.
#[derive(Clone, Copy, Debug)]
struct ChainShape {
    prefix_len: u32,
    replaced_len: u32,
    replacement_len: u32,
    replaced_chunk: u32,
    replacement_chunk: u32,
}

impl ChainShape {
    const fn fork_height(self) -> u32 {
        self.prefix_len + 1
    }

    const fn replaced_tip(self) -> u32 {
        self.prefix_len + self.replaced_len
    }

    const fn replacement_tip(self) -> u32 {
        self.prefix_len + self.replacement_len
    }
}

fn chain_shapes() -> impl Strategy<Value = ChainShape> {
    (1_u32..=4, 1_u32..=4, 1_u32..=5)
        .prop_flat_map(|(prefix_len, replaced_len, replacement_len)| {
            (
                Just(prefix_len),
                Just(replaced_len),
                Just(replacement_len),
                1..=prefix_len + replaced_len,
                1..=prefix_len + replacement_len,
            )
        })
        .prop_map(
            |(prefix_len, replaced_len, replacement_len, replaced_chunk, replacement_chunk)| {
                ChainShape {
                    prefix_len,
                    replaced_len,
                    replacement_len,
                    replaced_chunk,
                    replacement_chunk,
                }
            },
        )
}

#[test]
fn every_bundled_consumer_is_reorg_path_independent() -> TestResult {
    let mut runner = TestRunner::new(Config {
        cases: 24,
        failure_persistence: None,
        ..Config::default()
    });
    runner
        .run(&chain_shapes(), |shape| {
            assert_path_independent(shape).map_err(|error| TestCaseError::fail(error.to_string()))
        })
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[test]
fn a_reorg_onto_a_longer_branch_that_crosses_a_day_boundary_is_path_independent() -> TestResult {
    assert_path_independent(ChainShape {
        prefix_len: 2,
        replaced_len: 2,
        replacement_len: 3,
        replaced_chunk: 1,
        replacement_chunk: 5,
    })
}

#[test]
fn every_registry_entry_declares_how_it_conforms_to_reorgs() {
    for definition in bundled_materialized_view_consumer_definitions() {
        let declared_column_families = definition.schema.column_families;
        assert!(
            reorg_exemption(definition).is_some() || !declared_column_families.is_empty(),
            "{} must declare column families to compare",
            definition.schema.name.as_str()
        );
    }
    for (column_family, reason) in PATH_DEPENDENT_COLUMN_FAMILIES {
        assert!(!reason.is_empty());
        assert!(
            bundled_materialized_view_consumer_definitions()
                .iter()
                .any(|definition| definition.schema.column_families.contains(column_family)),
            "{column_family} is not declared by any bundled consumer"
        );
    }
}

/// Reorgs `A` onto `B` and back onto `A`, comparing each result with a fresh apply.
fn assert_path_independent(shape: ChainShape) -> TestResult {
    let prefix = Chain::build(shape.prefix_len, PREFIX_BRANCH, &Chain::default());
    let replaced = Chain::build(shape.replaced_tip(), REPLACED_BRANCH, &prefix);
    let replacement = Chain::build(shape.replacement_tip(), REPLACEMENT_BRANCH, &prefix);

    let reorged = ReplayStore::open()?;
    reorged.commit_in_chunks(&replaced, shape.replaced_chunk)?;
    reorged.reorg(shape.fork_height(), &replaced, &replacement)?;
    let fresh_replacement = ReplayStore::open()?;
    fresh_replacement.commit_in_chunks(&replacement, shape.replacement_chunk)?;
    let mut divergences = diverging_column_families(&reorged, &fresh_replacement)?;

    reorged.reorg(shape.fork_height(), &replacement, &replaced)?;
    let fresh_replaced = ReplayStore::open()?;
    fresh_replaced.commit_in_chunks(&replaced, shape.replacement_chunk)?;
    divergences.extend(diverging_column_families(&reorged, &fresh_replaced)?);

    if divergences.is_empty() {
        return Ok(());
    }
    Err(format!("{shape:?} diverged: {divergences:#?}").into())
}

fn diverging_column_families(
    reorged: &ReplayStore,
    fresh: &ReplayStore,
) -> TestResult<Vec<Divergence>> {
    let mut divergences = Vec::new();
    for definition in bundled_materialized_view_consumer_definitions() {
        if reorg_exemption(definition).is_some() {
            continue;
        }
        let mut compared_rows = 0;
        for &column_family in definition.schema.column_families {
            if PATH_DEPENDENT_COLUMN_FAMILIES
                .iter()
                .any(|(excluded, _reason)| *excluded == column_family)
            {
                continue;
            }
            let reorged_rows = reorged.rows(column_family)?;
            let fresh_rows = fresh.rows(column_family)?;
            compared_rows += fresh_rows.len();
            if reorged_rows != fresh_rows {
                divergences.push(Divergence {
                    consumer: definition.schema.name.as_str(),
                    column_family,
                    reorged: reorged_rows,
                    fresh: fresh_rows,
                });
            }
        }
        let fully_excluded = definition
            .schema
            .column_families
            .iter()
            .all(|column_family| {
                PATH_DEPENDENT_COLUMN_FAMILIES
                    .iter()
                    .any(|(excluded, _reason)| excluded == column_family)
            });
        if compared_rows == 0 && !fully_excluded {
            return Err(format!(
                "the fixture chain wrote no comparable rows for {}",
                definition.schema.name.as_str()
            )
            .into());
        }
    }
    Ok(divergences)
}

struct Divergence {
    consumer: &'static str,
    column_family: &'static str,
    reorged: ColumnFamilyRows,
    fresh: ColumnFamilyRows,
}

impl fmt::Debug for Divergence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let only_reorged = self
            .reorged
            .iter()
            .filter(|(key, payload)| self.fresh.get(*key) != Some(*payload))
            .map(|(key, payload)| (hex(key), hex(payload)))
            .collect::<Vec<_>>();
        let only_fresh = self
            .fresh
            .iter()
            .filter(|(key, payload)| self.reorged.get(*key) != Some(*payload))
            .map(|(key, payload)| (hex(key), hex(payload)))
            .collect::<Vec<_>>();
        formatter
            .debug_struct("Divergence")
            .field("consumer", &self.consumer)
            .field("column_family", &self.column_family)
            .field("only_after_reorg", &only_reorged)
            .field("only_after_fresh_apply", &only_fresh)
            .finish()
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|byte| {
            [
                char::from(DIGITS[usize::from(byte >> 4)]),
                char::from(DIGITS[usize::from(byte & 0x0f)]),
            ]
        })
        .collect()
}

/// One branch of synthetic canonical blocks plus the transparent outputs it created.
#[derive(Clone, Default)]
struct Chain {
    blocks: Vec<Arc<BlockCommitContext>>,
    outputs: HashMap<TransparentOutPoint, SpentOutput>,
}

#[derive(Clone, Copy)]
struct SpentOutput {
    value_zat: u64,
    address_script_hash: TransparentAddressScriptHash,
    block_height: BlockHeight,
    block_hash: BlockHash,
}

impl Chain {
    /// Extends `parent` with `branch` blocks until the chain reaches `tip`.
    fn build(tip: u32, branch: u8, parent: &Self) -> Self {
        let mut chain = parent.clone();
        let first = u32::try_from(chain.blocks.len()).map_or(u32::MAX, |len| len + 1);
        for height in first..=tip {
            let block = BlockFixture::new(branch, height, &chain).into_context(&mut chain);
            chain.blocks.push(Arc::new(block));
        }
        chain
    }

    fn block(&self, height: u32) -> TestResult<Arc<BlockCommitContext>> {
        let index = usize::try_from(height.checked_sub(1).ok_or("height zero")?)?;
        self.blocks
            .get(index)
            .cloned()
            .ok_or_else(|| format!("block {height} is not on this chain").into())
    }

    fn tip(&self) -> u32 {
        u32::try_from(self.blocks.len()).unwrap_or(u32::MAX)
    }
}

/// Deterministic transaction content for one block.
struct BlockFixture {
    branch: u8,
    height: BlockHeight,
    block_hash: BlockHash,
    previous_block_hash: BlockHash,
    block_time_unix_seconds: i64,
    spendable_coinbase: Option<(TransparentOutPoint, SpentOutput)>,
}

impl BlockFixture {
    fn new(branch: u8, height: u32, parent: &Chain) -> Self {
        let previous = parent.blocks.last();
        let spendable_coinbase = previous.and_then(|block| {
            let outpoint = TransparentOutPoint::new(transaction_id(block.block_hash, 0), 0);
            parent
                .outputs
                .get(&outpoint)
                .map(|output| (outpoint, *output))
        });
        let branch_shift = if branch == PREFIX_BRANCH {
            0
        } else {
            BRANCH_TIME_SHIFT_SECONDS
        };
        Self {
            branch,
            height: BlockHeight::new(height),
            block_hash: block_hash(branch, height),
            previous_block_hash: previous
                .map_or(BlockHash::from_bytes([0; 32]), |block| block.block_hash),
            block_time_unix_seconds: FIRST_BLOCK_TIME
                + i64::from(height) * BLOCK_SPACING_SECONDS
                + branch_shift,
            spendable_coinbase,
        }
    }

    fn into_context(self, chain: &mut Chain) -> BlockCommitContext {
        let mut transactions = vec![self.coinbase()];
        let mut balances = HashMap::new();
        let mut spends = HashMap::new();
        balances.insert(
            transactions[0].location.transaction_id,
            TransactionIntrinsicValueBalances::new(0, 0, 0, 0),
        );
        if let Some((outpoint, spent)) = self.spendable_coinbase {
            let (transaction, shielded_zat) = self.shielding_spend(outpoint, spent);
            let transaction_id = transaction.location.transaction_id;
            spends.insert(outpoint, self.spend_fact(outpoint, spent, transaction_id));
            balances.insert(
                transaction_id,
                TransactionIntrinsicValueBalances::new(0, -shielded_zat, 0, 0),
            );
            transactions.push(transaction);
        }
        if self.height.value() % 2 == 1 {
            let transaction = self.shielded_only(u32::try_from(transactions.len()).unwrap_or(0));
            balances.insert(
                transaction.location.transaction_id,
                TransactionIntrinsicValueBalances::new(0, 0, 1_500, 0),
            );
            transactions.push(transaction);
        }
        for transaction in &transactions {
            for output in &transaction.transparent_outputs {
                chain.outputs.insert(
                    TransparentOutPoint::new(
                        transaction.location.transaction_id,
                        output.output_index,
                    ),
                    SpentOutput {
                        value_zat: output.value_zat,
                        address_script_hash: output.address_script_hash,
                        block_height: self.height,
                        block_hash: self.block_hash,
                    },
                );
            }
        }
        self.context(transactions, balances, spends)
    }

    fn context(
        &self,
        transactions: Vec<TransactionFactsArtifact>,
        balances: HashMap<TransactionId, TransactionIntrinsicValueBalances>,
        spends: HashMap<TransparentOutPoint, TransparentSpendFact>,
    ) -> BlockCommitContext {
        BlockCommitContext::new(
            BlockCommitInput {
                height: self.height,
                block_hash: self.block_hash,
                previous_block_hash: self.previous_block_hash,
                block_time_unix_seconds: self.block_time_unix_seconds,
                block_size_bytes: 1_000 + u64::from(self.height.value()),
                transactions,
                final_note_commitment_roots: Some(BlockFinalNoteCommitmentRoots::new(
                    self.height,
                    self.block_hash,
                    Some(root(self.branch, self.height.value(), 1)),
                    Some(root(self.branch, self.height.value(), 2)),
                    None,
                )),
            },
            TransparentSpendFacts::from_map(Arc::new(spends)),
        )
        .with_transaction_intrinsic_value_balances(TransactionIntrinsicValueBalanceFacts::from_map(
            Arc::new(balances),
        ))
        .with_block_value_pool_balances(BlockValuePoolBalanceFacts::from_pools(vec![
            ValuePoolBalance::new(
                "transparent",
                true,
                Some(u64::from(self.height.value()) * 50_000),
            ),
            ValuePoolBalance::new(
                "sapling",
                true,
                Some(u64::from(self.height.value()) * 2_000),
            ),
        ]))
    }

    fn coinbase(&self) -> TransactionFactsArtifact {
        let value_zat = 50_000 + u64::from(self.height.value()) * 10 + u64::from(self.branch);
        let script = address_script(self.height.value() + u32::from(self.branch));
        self.transaction(
            0,
            TransactionKind {
                privacy_shape: PrivacyShape::Coinbase,
                is_coinbase: true,
                counts: TransactionComponentCounts {
                    transparent_input_count: 1,
                    transparent_output_count: 1,
                    ..TransactionComponentCounts::EMPTY
                },
            },
        )
        .with_transparent_facts(
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            vec![output(0, value_zat, script)],
        )
    }

    fn shielding_spend(
        &self,
        outpoint: TransparentOutPoint,
        spent: SpentOutput,
    ) -> (TransactionFactsArtifact, i64) {
        let fee_zat = 1_000 + u64::from(self.height.value()) + u64::from(self.branch) * 7;
        let shielded_zat = 2_000 + u64::from(self.height.value()) * 3;
        let change_zat = 300;
        let recipient_zat = spent
            .value_zat
            .saturating_sub(fee_zat + shielded_zat + change_zat);
        let transaction = self
            .transaction(
                1,
                TransactionKind {
                    privacy_shape: PrivacyShape::Mixed,
                    is_coinbase: false,
                    counts: TransactionComponentCounts {
                        transparent_input_count: 1,
                        transparent_output_count: 2,
                        sapling_output_count: 1,
                        ..TransactionComponentCounts::EMPTY
                    },
                },
            )
            .with_transparent_facts(
                vec![TransparentInputFact::new(0, outpoint)],
                vec![
                    output(
                        0,
                        recipient_zat,
                        address_script(self.height.value() + 1 + u32::from(self.branch)),
                    ),
                    output(1, change_zat, address_script(3)),
                ],
            );
        (transaction, i64::try_from(shielded_zat).unwrap_or(i64::MAX))
    }

    fn shielded_only(&self, index: u32) -> TransactionFactsArtifact {
        self.transaction(
            index,
            TransactionKind {
                privacy_shape: PrivacyShape::ShieldedOnly,
                is_coinbase: false,
                counts: TransactionComponentCounts {
                    orchard_action_count: 2,
                    ..TransactionComponentCounts::EMPTY
                },
            },
        )
    }

    fn spend_fact(
        &self,
        outpoint: TransparentOutPoint,
        spent: SpentOutput,
        spending_transaction_id: TransactionId,
    ) -> TransparentSpendFact {
        TransparentSpendFact::new(
            outpoint,
            0,
            spending_transaction_id,
            1,
            self.height,
            self.block_hash,
            spent.value_zat,
            spent.address_script_hash,
            spent.block_height,
            spent.block_hash,
        )
    }

    fn transaction(&self, index: u32, kind: TransactionKind) -> TransactionFactsArtifact {
        let transaction_id = transaction_id(self.block_hash, index);
        TransactionFactsArtifact::new(
            TransactionLocation::new(transaction_id, self.height, self.block_hash, index),
            TransactionPublicFacts {
                transaction_id,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::V5,
                consensus_branch_id: None,
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 250 + index,
                counts: kind.counts,
                privacy_shape: kind.privacy_shape,
                is_coinbase: kind.is_coinbase,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                unsupported_sections: Vec::new(),
            },
        )
    }
}

#[derive(Clone, Copy)]
struct TransactionKind {
    privacy_shape: PrivacyShape,
    is_coinbase: bool,
    counts: TransactionComponentCounts,
}

fn block_hash(branch: u8, height: u32) -> BlockHash {
    let mut bytes = [0x5a; 32];
    bytes[0] = branch.wrapping_add(1);
    bytes[1..5].copy_from_slice(&height.to_be_bytes());
    BlockHash::from_bytes(bytes)
}

fn transaction_id(block_hash: BlockHash, index: u32) -> TransactionId {
    let mut bytes = block_hash.as_bytes();
    bytes[28..32].copy_from_slice(&index.to_be_bytes());
    bytes[27] = 0xee;
    TransactionId::from_bytes(bytes)
}

fn root(branch: u8, height: u32, protocol: u8) -> FinalNoteCommitmentRoot {
    let mut bytes = [protocol; 32];
    bytes[0] = branch;
    bytes[1..5].copy_from_slice(&height.to_be_bytes());
    FinalNoteCommitmentRoot::from_bytes(bytes)
}

fn address_script(address: u32) -> Vec<u8> {
    let mut script = vec![0x76, 0xa9, 0x14];
    let mut hash = [0x11; 20];
    hash[0] = u8::try_from(address % 4).unwrap_or(0);
    script.extend_from_slice(&hash);
    script.extend_from_slice(&[0x88, 0xac]);
    script
}

fn output(index: u32, value_zat: u64, script: Vec<u8>) -> TransparentOutputFact {
    let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
    TransparentOutputFact::new(index, value_zat, script, address_script_hash)
}

/// A bundled materialized-view store driven by synthetic chain events.
struct ReplayStore {
    _directory: TempDir,
    store: MaterializedViewStore,
    next_event_sequence: std::cell::Cell<u64>,
}

impl ReplayStore {
    fn open() -> TestResult<Self> {
        let directory = TempDir::new()?;
        let store = MaterializedViewStore::open(
            directory.path(),
            MaterializedViewStoreOptions {
                sync_writes: false,
                consumers: MaterializedViewStore::bundled_consumers(),
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            },
        )?;
        Ok(Self {
            _directory: directory,
            store,
            next_event_sequence: std::cell::Cell::new(1),
        })
    }

    /// Commits `chain` with the first block alone, then in `chunk_blocks` ranges.
    ///
    /// The ranking consumer activates an empty snapshot at height 1, so every
    /// path starts its ranking state from the same settled base.
    fn commit_in_chunks(&self, chain: &Chain, chunk_blocks: u32) -> TestResult {
        self.commit(chain, 1, 1)?;
        self.activate_ranking_at_first_block(chain)?;
        let mut start = 2;
        while start <= chain.tip() {
            let end = start.saturating_add(chunk_blocks - 1).min(chain.tip());
            self.commit(chain, start, end)?;
            start = end + 1;
        }
        Ok(())
    }

    fn commit(&self, chain: &Chain, start: u32, end: u32) -> TestResult {
        let committed = ChainEpochCommitted {
            chain_epoch: self.epoch(chain, end)?,
            block_range: range(start, end),
        };
        let event = ChainEvent::ChainCommitted { committed };
        self.dispatch(&event, committed.chain_epoch, chain, (start, end))
    }

    /// Reverts `replaced[fork_height..]` and applies `replacement[fork_height..]`.
    fn reorg(&self, fork_height: u32, replaced: &Chain, replacement: &Chain) -> TestResult {
        let committed = ChainEpochCommitted {
            chain_epoch: self.epoch(replacement, replacement.tip())?,
            block_range: range(fork_height, replacement.tip()),
        };
        let event = ChainEvent::ChainReorged {
            reverted: ChainRangeReverted {
                chain_epoch: self.epoch(replaced, replaced.tip())?,
                block_range: range(fork_height, replaced.tip()),
            },
            committed,
        };
        self.dispatch(
            &event,
            committed.chain_epoch,
            replacement,
            (fork_height, replacement.tip()),
        )
    }

    fn dispatch(
        &self,
        event: &ChainEvent,
        chain_epoch: ChainEpoch,
        chain: &Chain,
        (start, end): (u32, u32),
    ) -> TestResult {
        let mut blocks = HashMap::new();
        for height in start..=end {
            blocks.insert(BlockHeight::new(height), chain.block(height)?);
        }
        let event_sequence = self.next_event_sequence.get();
        self.next_event_sequence.set(event_sequence + 1);
        let cursor = event_sequence.to_be_bytes();
        let (mut block_consumers, mut event_consumers) = self.registry_consumers()?;
        let mut block_refs: Vec<&mut dyn BlockKeyedConsumer> = Vec::new();
        for consumer in &mut block_consumers {
            block_refs.push(consumer.as_mut());
        }
        let mut event_refs: Vec<&mut dyn MaterializedViewConsumer> = Vec::new();
        for consumer in &mut event_consumers {
            event_refs.push(consumer.as_mut());
        }
        self.store.write_chain_event_chunk_with_event_consumers(
            ChainEventDispatchConsumers {
                block_consumers: &mut block_refs,
                event_consumers: &mut event_refs,
            },
            ChainEventDispatchInputs {
                chain_epoch,
                chain_event: event,
                chain_cursor: &cursor,
                event_sequence,
                settled_tip_height: chain_epoch.settled_tip_height,
            },
            &blocks,
            true,
        )?;
        Ok(())
    }

    #[allow(
        clippy::type_complexity,
        reason = "the two consumer families mirror ChainEventDispatchConsumers"
    )]
    fn registry_consumers(
        &self,
    ) -> TestResult<(
        Vec<Box<dyn BlockKeyedConsumer>>,
        Vec<Box<dyn MaterializedViewConsumer>>,
    )> {
        let mut block_consumers = Vec::new();
        let mut event_consumers = Vec::new();
        for definition in bundled_materialized_view_consumer_definitions() {
            match definition.dispatch {
                MaterializedViewDispatch::ChainEventBlocks { join, construct } => {
                    let receives = match join {
                        ChainEventBlockJoin::Replay | ChainEventBlockJoin::CanonicalBackfill => {
                            true
                        }
                        ChainEventBlockJoin::CanonicalSnapshot { is_active } => {
                            is_active(&self.store)?
                        }
                    };
                    if receives {
                        block_consumers.push(construct());
                    }
                }
                MaterializedViewDispatch::ChainEventEnvelope { construct } => {
                    event_consumers.push(construct());
                }
                MaterializedViewDispatch::MempoolEvents { .. }
                | MaterializedViewDispatch::CanonicalBackfillOnly => {}
            }
        }
        Ok((block_consumers, event_consumers))
    }

    fn activate_ranking_at_first_block(&self, chain: &Chain) -> TestResult {
        let base = chain.block(1)?;
        let snapshot_rows = ranking_snapshot_rows(&base);
        let coverage = TransparentAddressRankingCoverage {
            balance_complete_through_height: base.height,
            history_complete_from_height: Some(base.height),
            history_complete_through_height: Some(base.height),
            lifetime_statistics_complete: true,
        };
        TransparentAddressRankingConsumer::initialize_snapshot_generation(
            &self.store,
            TransparentAddressRankingSnapshotPlan {
                generation: RANKING_GENERATION,
                base_height: base.height,
                base_block_hash: base.block_hash,
                target_height: base.height,
                target_block_hash: base.block_hash,
                expected_summary_count: u64::try_from(snapshot_rows.len())?,
                base_coverage: coverage,
            },
        )?;
        TransparentAddressRankingConsumer::write_snapshot_batch(
            &self.store,
            RANKING_GENERATION,
            &snapshot_rows,
        )?;
        TransparentAddressRankingConsumer::finalize_snapshot_base(&self.store, RANKING_GENERATION)?;
        let cursor = self
            .store
            .get_chain_event_cursor(zinder_materialized_views::BLOCK_SUMMARY_CONSUMER_NAME)?
            .ok_or("the first commit must advance the replay cursor")?;
        TransparentAddressRankingConsumer::activate_snapshot_generation_at_cursor(
            &self.store,
            RANKING_GENERATION,
            &cursor,
        )?;
        Ok(())
    }

    fn epoch(&self, chain: &Chain, tip: u32) -> TestResult<ChainEpoch> {
        let tip_block = chain.block(tip)?;
        let settled = chain.block(1)?;
        Ok(ChainEpoch {
            id: ChainEpochId::new(self.next_event_sequence.get()),
            network: Network::ZcashRegtest,
            visible_tip_height: tip_block.height,
            visible_tip_hash: tip_block.block_hash,
            settled_tip_height: settled.height,
            settled_tip_hash: settled.block_hash,
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(self.next_event_sequence.get()),
        })
    }

    fn rows(&self, column_family: &'static str) -> TestResult<ColumnFamilyRows> {
        let mut rows = ColumnFamilyRows::new();
        self.store
            .visit_consumer_rows(column_family, |key, payload| {
                rows.insert(key.to_vec(), payload.to_vec());
                Ok(())
            })?;
        Ok(rows)
    }
}

/// Address summaries for the outputs one block creates, used as the settled
/// ranking snapshot base.
fn ranking_snapshot_rows(block: &BlockCommitContext) -> Vec<TransparentAddressRankingSnapshotRow> {
    let mut summaries = BTreeMap::<[u8; 32], TransparentAddressRankingSnapshotRow>::new();
    for transaction in &block.transactions {
        for output in &transaction.transparent_outputs {
            let row = summaries
                .entry(output.address_script_hash.as_bytes())
                .or_insert_with(|| TransparentAddressRankingSnapshotRow {
                    address_script_hash: output.address_script_hash,
                    summary: TransparentAddressSummary {
                        script_pub_key: Some(output.script_pub_key.clone()),
                        balance_zat: 0,
                        total_received_zat: 0,
                        total_sent_zat: 0,
                        distinct_transaction_count: 1,
                        first_seen_unix_seconds: Some(block.block_time_unix_seconds),
                        last_seen_unix_seconds: Some(block.block_time_unix_seconds),
                        snapshot_first_seen_unix_seconds: None,
                        snapshot_last_seen_unix_seconds: None,
                    },
                });
            row.summary.balance_zat += output.value_zat;
            row.summary.total_received_zat += output.value_zat;
        }
    }
    summaries.into_values().collect()
}

const fn range(start: u32, end: u32) -> BlockHeightRange {
    BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end))
}
