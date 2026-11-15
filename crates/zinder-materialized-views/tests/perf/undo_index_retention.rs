//! Disk the height undo indexes of Explorer views take.
//!
//! The fixture is a transparent-dense chain: every block pays its coinbase to
//! one of many addresses and spends earlier outputs to two more. Replaying it
//! into a store opened with a reorg window must leave each height undo index
//! holding only the window's newest heights and every other row identical to
//! an unpruned store. Pruning the unpruned store afterwards, as ingest does on
//! open, must reclaim the same space. The assertions compare SST bytes after a
//! full compaction of both stores, so they hold on any CI worker.

use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap},
    error::Error,
    path::Path,
    sync::Arc,
};

use rust_rocksdb::{
    BottommostLevelCompaction, Cache, CompactOptions, DB, DBCompressionType, Options,
};
use tempfile::TempDir;
use zinder_core::wire::decode_height_key_ascending;
use zinder_core::{
    ArtifactSchemaVersion, BlockHash, BlockHeight, BlockHeightRange, ChainEpoch, ChainEpochId,
    ChainTipMetadata, LockTime, Network, PrivacyShape, TransactionComponentCounts,
    TransactionFactsArtifact, TransactionId, TransactionIntrinsicValueBalances,
    TransactionLocation, TransactionPublicFacts, TransactionVersion, TransparentAddressScriptHash,
    TransparentInputFact, TransparentOutPoint, TransparentOutputFact, TransparentSpendFact,
    UnixTimestampMillis, ValuePoolBalance,
};
use zinder_materialized_views::{
    BlockCommitContext, BlockCommitInput, BlockKeyedConsumer, BlockValuePoolBalanceFacts,
    ChainEventBlockJoin, ChainEventDispatchConsumers, ChainEventDispatchInputs,
    MaterializedViewConsumer, MaterializedViewDispatch, MaterializedViewStore,
    MaterializedViewStoreOptions, TransactionIntrinsicValueBalanceFacts, TransparentSpendFacts,
    bundled_materialized_view_consumer_definitions,
};
use zinder_store::{
    CanonicalReorgPolicy, ChainEpochCommitted, ChainEvent, RocksDbResourceBudget,
    build_block_based_table_factory,
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type ColumnFamilyBytes = BTreeMap<String, u64>;

const BLOCK_COUNT: u32 = 1_500;
const SPENDS_PER_BLOCK: u32 = 40;
const ADDRESS_COUNT: u64 = 20_000;
const REORG_WINDOW_BLOCKS: u32 = 100;
const BATCH_BLOCKS: u32 = 100;
const COINBASE_VALUE_ZAT: u64 = 625_000_000;
const FEE_ZAT: u64 = 1_000;
const FIRST_BLOCK_TIME: i64 = 1_700_006_400;
const BLOCK_SPACING_SECONDS: i64 = 75;
/// Largest share of its unpruned size a pruned undo index may keep: the
/// window holds 100 of 1,500 heights, about 7%.
const MAX_RETAINED_UNDO_PERCENT: u64 = 15;

#[test]
fn pruned_views_keep_only_the_undo_rows_the_reorg_window_reaches() -> TestResult {
    let chain = transparent_dense_chain();
    let reorg_policy = CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?;
    let unpruned_directory = TempDir::new()?;
    let pruned_directory = TempDir::new()?;
    {
        let unpruned = open_store(unpruned_directory.path(), None)?;
        let pruned = open_store(pruned_directory.path(), Some(reorg_policy))?;
        replay(&unpruned, &chain)?;
        replay(&pruned, &chain)?;
        assert_rows_match_within_window(&pruned, &unpruned)?;
    }
    let unpruned_bytes = compacted_column_family_bytes(unpruned_directory.path())?;
    let pruned_bytes = compacted_column_family_bytes(pruned_directory.path())?;
    assert_undo_indexes_shrank(&unpruned_bytes, &pruned_bytes);
    assert!(total(&pruned_bytes) < total(&unpruned_bytes));

    {
        // A restart can stop the pass right after its first range delete hides
        // that index's rows; the next pass must still reclaim their files.
        let interrupted = open_store(unpruned_directory.path(), Some(reorg_policy))?;
        let checks = Cell::new(0);
        interrupted.prune_undo_indexes(|| {
            checks.set(checks.get() + 1);
            checks.get() > 1
        })?;
    }
    {
        let reopened = open_store(unpruned_directory.path(), Some(reorg_policy))?;
        let prunings = reopened.prune_undo_indexes(|| false)?;
        assert_eq!(prunings.len(), undo_index_column_families().len());
        let pruned = open_store(pruned_directory.path(), None)?;
        assert_rows_match(&reopened, &pruned)?;
    }
    let reclaimed_bytes = compacted_column_family_bytes(unpruned_directory.path())?;
    assert_undo_indexes_shrank(&unpruned_bytes, &reclaimed_bytes);
    Ok(())
}

fn open_store(
    path: &Path,
    reorg_policy: Option<CanonicalReorgPolicy>,
) -> TestResult<MaterializedViewStore> {
    Ok(MaterializedViewStore::open(
        path,
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            reorg_policy,
        },
    )?)
}

fn undo_index_column_families() -> Vec<&'static str> {
    bundled_materialized_view_consumer_definitions()
        .iter()
        .filter_map(|definition| definition.schema.height_undo_index)
        .collect()
}

/// Requires the pruned store to hold every row of the unpruned store except
/// the height undo rows below the window.
fn assert_rows_match_within_window(
    pruned: &MaterializedViewStore,
    unpruned: &MaterializedViewStore,
) -> TestResult {
    let pruned_through = BLOCK_COUNT - REORG_WINDOW_BLOCKS;
    for definition in bundled_materialized_view_consumer_definitions() {
        for &column_family in definition.schema.column_families {
            let mut expected = rows(unpruned, column_family)?;
            if definition.schema.height_undo_index == Some(column_family) {
                expected.retain(|key, _payload| {
                    decode_height_key_ascending(key)
                        .map_or(true, |height| height.value() > pruned_through)
                });
                assert_eq!(expected.len(), usize::try_from(REORG_WINDOW_BLOCKS)?);
            }
            assert!(
                rows(pruned, column_family)? == expected,
                "{column_family} differs from the unpruned rows the window reaches"
            );
        }
    }
    Ok(())
}

fn assert_rows_match(left: &MaterializedViewStore, right: &MaterializedViewStore) -> TestResult {
    for definition in bundled_materialized_view_consumer_definitions() {
        for &column_family in definition.schema.column_families {
            assert!(
                rows(left, column_family)? == rows(right, column_family)?,
                "{column_family} differs between the two stores"
            );
        }
    }
    Ok(())
}

fn assert_undo_indexes_shrank(unpruned: &ColumnFamilyBytes, pruned: &ColumnFamilyBytes) {
    for column_family in undo_index_column_families() {
        let before = unpruned.get(column_family).copied().unwrap_or(0);
        let after = pruned.get(column_family).copied().unwrap_or(0);
        assert!(before > 0, "{column_family} must hold undo rows to prune");
        assert!(
            after.saturating_mul(100) <= before.saturating_mul(MAX_RETAINED_UNDO_PERCENT),
            "{column_family} kept {after} of {before} bytes"
        );
    }
}

fn total(bytes: &ColumnFamilyBytes) -> u64 {
    bytes.values().sum()
}

fn rows(
    store: &MaterializedViewStore,
    column_family: &'static str,
) -> TestResult<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut rows = BTreeMap::new();
    store.visit_consumer_rows(column_family, |key, payload| {
        rows.insert(key.to_vec(), payload.to_vec());
        Ok(())
    })?;
    Ok(rows)
}

/// Flushes and fully compacts a closed store with the view store's table
/// settings, then reads each column family's SST bytes.
fn compacted_column_family_bytes(path: &Path) -> TestResult<ColumnFamilyBytes> {
    let cache = Cache::new_lru_cache(32 << 20);
    let mut options = Options::default();
    options.set_compression_type(DBCompressionType::Zstd);
    options.set_block_based_table_factory(&build_block_based_table_factory(&cache));
    let column_families = DB::list_cf(&options, path)?;
    let db = DB::open_cf(&options, path, &column_families)?;
    let mut compact_options = CompactOptions::default();
    compact_options.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
    let mut bytes = ColumnFamilyBytes::new();
    for name in &column_families {
        let handle = db
            .cf_handle(name)
            .ok_or("a listed column family must open")?;
        db.flush_cf(&handle)?;
        db.compact_range_cf_opt(&handle, None::<&[u8]>, None::<&[u8]>, &compact_options);
        let sst_bytes = db
            .property_int_value_cf(&handle, "rocksdb.total-sst-files-size")?
            .unwrap_or(0);
        bytes.insert(name.clone(), sst_bytes);
    }
    Ok(bytes)
}

/// Applies the chain in batches through every chain-event consumer the
/// registry dispatches without a canonical snapshot.
fn replay(store: &MaterializedViewStore, chain: &[Arc<BlockCommitContext>]) -> TestResult {
    let mut start = 1;
    while start <= BLOCK_COUNT {
        let end = start.saturating_add(BATCH_BLOCKS - 1).min(BLOCK_COUNT);
        let blocks = (start..=end)
            .map(|height| {
                let block = block_at(chain, height)?;
                Ok((block.height, Arc::clone(block)))
            })
            .collect::<TestResult<HashMap<_, _>>>()?;
        let chain_epoch = epoch(block_at(chain, end)?, block_at(chain, settled_height(end))?);
        let event = ChainEvent::ChainCommitted {
            committed: ChainEpochCommitted {
                chain_epoch,
                block_range: BlockHeightRange::inclusive(
                    BlockHeight::new(start),
                    BlockHeight::new(end),
                ),
            },
        };
        dispatch(store, &event, chain_epoch, &blocks)?;
        start = end + 1;
    }
    Ok(())
}

fn dispatch(
    store: &MaterializedViewStore,
    event: &ChainEvent,
    chain_epoch: ChainEpoch,
    blocks: &HashMap<BlockHeight, Arc<BlockCommitContext>>,
) -> TestResult {
    let mut block_consumers: Vec<Box<dyn BlockKeyedConsumer>> = Vec::new();
    let mut event_consumers: Vec<Box<dyn MaterializedViewConsumer>> = Vec::new();
    for definition in bundled_materialized_view_consumer_definitions() {
        match definition.dispatch {
            MaterializedViewDispatch::ChainEventBlocks { join, construct } => {
                let receives = match join {
                    ChainEventBlockJoin::Replay | ChainEventBlockJoin::CanonicalBackfill => true,
                    ChainEventBlockJoin::CanonicalSnapshot { is_active } => is_active(store)?,
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
    let mut block_refs = block_consumers
        .iter_mut()
        .map(|consumer| &mut **consumer as &mut dyn BlockKeyedConsumer)
        .collect::<Vec<_>>();
    let mut event_refs = event_consumers
        .iter_mut()
        .map(|consumer| &mut **consumer as &mut dyn MaterializedViewConsumer)
        .collect::<Vec<_>>();
    let event_sequence = chain_epoch.id.value();
    let cursor = event_sequence.to_be_bytes();
    store.write_chain_event_chunk_with_event_consumers(
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
        blocks,
        true,
    )?;
    Ok(())
}

const fn settled_height(tip: u32) -> u32 {
    let settled = tip.saturating_sub(REORG_WINDOW_BLOCKS);
    if settled == 0 { 1 } else { settled }
}

fn epoch(tip: &BlockCommitContext, settled: &BlockCommitContext) -> ChainEpoch {
    ChainEpoch {
        id: ChainEpochId::new(u64::from(tip.height.value())),
        network: Network::ZcashRegtest,
        visible_tip_height: tip.height,
        visible_tip_hash: tip.block_hash,
        settled_tip_height: settled.height,
        settled_tip_hash: settled.block_hash,
        artifact_schema_version: ArtifactSchemaVersion::new(1),
        tip_metadata: ChainTipMetadata::empty(),
        created_at: UnixTimestampMillis::new(u64::from(tip.height.value())),
    }
}

fn block_at(
    chain: &[Arc<BlockCommitContext>],
    height: u32,
) -> TestResult<&Arc<BlockCommitContext>> {
    let index = usize::try_from(height.checked_sub(1).ok_or("height zero")?)?;
    chain
        .get(index)
        .ok_or_else(|| format!("the fixture has no block at height {height}").into())
}

/// One unspent fixture output and the facts a spend of it carries.
#[derive(Clone, Copy)]
struct UnspentOutput {
    outpoint: TransparentOutPoint,
    value_zat: u64,
    address_script_hash: TransparentAddressScriptHash,
    block_height: BlockHeight,
    block_hash: BlockHash,
}

/// Deterministic `SplitMix64` stream for fixture hashes, addresses, and picks.
struct FixtureRandom(u64);

impl FixtureRandom {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }

    fn bytes32(&mut self) -> [u8; 32] {
        let mut bytes = [0; 32];
        for chunk in bytes.chunks_exact_mut(8) {
            chunk.copy_from_slice(&self.next().to_be_bytes());
        }
        bytes
    }

    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(bound).unwrap_or(u64::MAX)).unwrap_or(0)
    }

    fn address_script(&mut self) -> Vec<u8> {
        let mut address = Self(self.next() % ADDRESS_COUNT);
        let hash = address.bytes32();
        let mut script = vec![0x76, 0xa9, 0x14];
        script.extend_from_slice(&hash[..20]);
        script.extend_from_slice(&[0x88, 0xac]);
        script
    }
}

fn transparent_dense_chain() -> Vec<Arc<BlockCommitContext>> {
    let mut random = FixtureRandom(0x7a69_6e64_6572);
    let mut unspent = Vec::<UnspentOutput>::new();
    let mut previous_block_hash = BlockHash::from_bytes([0; 32]);
    let mut chain = Vec::new();
    for raw_height in 1..=BLOCK_COUNT {
        let height = BlockHeight::new(raw_height);
        let block_hash = BlockHash::from_bytes(random.bytes32());
        let mut block = FixtureBlock {
            height,
            block_hash,
            transactions: Vec::new(),
            spends: HashMap::new(),
            produced: Vec::new(),
        };
        block.push_transaction(&mut random, None);
        for _ in 0..SPENDS_PER_BLOCK {
            if unspent.is_empty() {
                break;
            }
            let spent = unspent.swap_remove(random.below(unspent.len()));
            block.push_transaction(&mut random, Some(spent));
        }
        unspent.extend(block.produced.iter().copied());
        chain.push(Arc::new(block.into_context(previous_block_hash)));
        previous_block_hash = block_hash;
    }
    chain
}

struct FixtureBlock {
    height: BlockHeight,
    block_hash: BlockHash,
    transactions: Vec<TransactionFactsArtifact>,
    spends: HashMap<TransparentOutPoint, TransparentSpendFact>,
    produced: Vec<UnspentOutput>,
}

impl FixtureBlock {
    /// Appends a coinbase when `spent` is `None`, otherwise a transaction that
    /// spends it to two addresses.
    fn push_transaction(&mut self, random: &mut FixtureRandom, spent: Option<UnspentOutput>) {
        let index = u32::try_from(self.transactions.len()).unwrap_or(u32::MAX);
        let transaction_id = TransactionId::from_bytes(random.bytes32());
        let (input, output_values) = spent.map_or_else(
            || {
                (
                    TransparentInputFact::new(0, TransparentOutPoint::COINBASE_SENTINEL),
                    vec![COINBASE_VALUE_ZAT],
                )
            },
            |spent| {
                self.record_spend(spent, transaction_id, index);
                let change = spent.value_zat.saturating_sub(FEE_ZAT) / 2;
                (
                    TransparentInputFact::new(0, spent.outpoint),
                    vec![change, change],
                )
            },
        );
        let mut outputs = Vec::new();
        for (output_index, value_zat) in (0_u32..).zip(output_values) {
            let script = random.address_script();
            let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
            self.produced.push(UnspentOutput {
                outpoint: TransparentOutPoint::new(transaction_id, output_index),
                value_zat,
                address_script_hash,
                block_height: self.height,
                block_hash: self.block_hash,
            });
            outputs.push(TransparentOutputFact::new(
                output_index,
                value_zat,
                script,
                address_script_hash,
            ));
        }
        let is_coinbase = spent.is_none();
        let counts = TransactionComponentCounts {
            transparent_input_count: 1,
            transparent_output_count: u32::try_from(outputs.len()).unwrap_or(u32::MAX),
            ..TransactionComponentCounts::EMPTY
        };
        self.transactions.push(
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
                    size_bytes: 250,
                    counts,
                    privacy_shape: if is_coinbase {
                        PrivacyShape::Coinbase
                    } else {
                        PrivacyShape::TransparentOnly
                    },
                    is_coinbase,
                    orchard_value_balance_zat: None,
                    orchard_anchor: None,
                    ironwood_value_balance_zat: None,
                    unsupported_sections: Vec::new(),
                },
            )
            .with_transparent_facts(vec![input], outputs),
        );
    }

    fn record_spend(
        &mut self,
        spent: UnspentOutput,
        spending_transaction_id: TransactionId,
        spending_transaction_index: u32,
    ) {
        self.spends.insert(
            spent.outpoint,
            TransparentSpendFact::new(
                spent.outpoint,
                0,
                spending_transaction_id,
                spending_transaction_index,
                self.height,
                self.block_hash,
                spent.value_zat,
                spent.address_script_hash,
                spent.block_height,
                spent.block_hash,
            ),
        );
    }

    fn into_context(self, previous_block_hash: BlockHash) -> BlockCommitContext {
        let balances = self
            .transactions
            .iter()
            .map(|transaction| {
                (
                    transaction.location.transaction_id,
                    TransactionIntrinsicValueBalances::new(0, 0, 0, 0),
                )
            })
            .collect::<HashMap<_, _>>();
        let transparent_pool_zat = u64::from(self.height.value()) * COINBASE_VALUE_ZAT;
        BlockCommitContext::new(
            BlockCommitInput {
                height: self.height,
                block_hash: self.block_hash,
                previous_block_hash,
                block_time_unix_seconds: FIRST_BLOCK_TIME
                    + i64::from(self.height.value()) * BLOCK_SPACING_SECONDS,
                block_size_bytes: 2_000,
                transactions: self.transactions,
                final_note_commitment_roots: None,
            },
            TransparentSpendFacts::from_map(Arc::new(self.spends)),
        )
        .with_transaction_intrinsic_value_balances(TransactionIntrinsicValueBalanceFacts::from_map(
            Arc::new(balances),
        ))
        .with_block_value_pool_balances(BlockValuePoolBalanceFacts::from_pools(vec![
            ValuePoolBalance::new("transparent", true, Some(transparent_pool_zat)),
        ]))
    }
}
