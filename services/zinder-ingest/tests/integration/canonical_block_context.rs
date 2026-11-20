#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    path::Path,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use metrics_exporter_prometheus::PrometheusBuilder;
use parking_lot::RwLock;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use zcash_primitives::transaction::{Authorized, Transaction as ZcashTransaction, TransactionData};
use zebra_chain::{
    block::Block as ZebraBlock,
    serialization::{ZcashDeserialize, ZcashDeserializeInto, ZcashSerialize},
    transaction::Transaction as ZebraTransaction,
};
use zinder_core::{
    BlockFinalNoteCommitmentRoots, BlockHash, BlockHeight, BlockHeightRange, BlockId,
    CanonicalBlockFacts, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontier, CommitmentTreeFrontiers, ConsensusBranchId, Network,
    NetworkUpgradeActivation, NetworkUpgradeActivations, ShieldedProtocol, TransactionId,
    TransparentOutPoint, UnixTimestampMillis,
};
use zinder_ingest::{
    CanonicalBlockContextReader, CanonicalConstructionConfig, CanonicalFollowConfig,
    CanonicalFollower, ConventionalFeeDistributionBackfillConfig,
    ConventionalFeeDistributionBackfillContext, DEFAULT_MAX_REPLACEMENT_BYTES, IngestError,
    MaterializedViewReplayConfig, MaterializedViewTailer, PrevoutCache, RawBlobPolicy,
    ReplayPrevoutCaches, TransactionComponentBackfillConfig, TransactionComponentBackfillContext,
    follow_canonical_tip, load_fresh_canonical, prepare_canonical_block,
    require_genesis_complete_history, spawn_conventional_fee_distribution_backfill_task,
    spawn_transaction_component_backfill_task,
};
use zinder_materialized_views::{
    BLOCK_SUMMARY_SCHEMA, BlockCommitContext, COMMITMENT_ROOT_SEARCH_SCHEMA,
    CONVENTIONAL_FEE_DISTRIBUTION_CONSUMER_NAME, CommitmentRootSearchConsumer,
    ConventionalFeeDistributionBackfillCoverage, ConventionalFeeDistributionConsumer,
    MaterializedViewConsumerSchema, MaterializedViewStore, MaterializedViewStoreOptions,
    TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME, TRANSPARENT_ADDRESS_RANKING_SCHEMA,
    TRANSPARENT_OUTPOINT_SPEND_SCHEMA, TransactionComponentBackfillCoverage,
    TransactionComponentSummaryConsumer, TransparentAddressRankingConsumer,
    bundled_materialized_view_consumer_definitions,
};
use zinder_runtime::{ComposedReadiness, Readiness};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceChainSegment, SourceChainSegmentLimits,
    SourceError,
};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
    CanonicalStoreError, CanonicalStoreWorkload, RocksDbCanonicalBuilder,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget,
};
use zinder_testkit::{
    LogCapture, P2pkhSpendArgs, TransparentTestKey, ZIP317_FEE_ONE_IN_ONE_OUT_ZATS,
    sample_regtest_upgrade_activations,
};

use super::fixture_block::{
    fixture_orchard_source_block, fixture_sapling_source_block, fixture_source_block,
};
use super::materialized_view_replay::{
    clear_transparent_address_ranking, transparent_address_ranking, view_rows, view_store,
};
use super::tree_state_densification::following_tip_gate;

const SERIALIZED_HEADER_PARENT_OFFSET: usize = 4;
const SERIALIZED_HEADER_PARENT_END: usize = SERIALIZED_HEADER_PARENT_OFFSET + 32;
const SERIALIZED_HEADER_TIME_OFFSET: usize = 100;
const COINBASE_HEIGHT_OPCODE_OFFSET: usize = 1_534;
const CHAIN_TIP_HEIGHT: u32 = 4;
const SAPLING_BLOCK_HEIGHT: u32 = 2;
const SPENDING_BLOCK_HEIGHT: u32 = 3;
const ORCHARD_BLOCK_HEIGHT: u32 = 4;
const SPEND_OUTPUT_VALUE_ZAT: u64 = 12_345;
const REORG_WINDOW_BLOCKS: u32 = 100;
const FORK_PARENT_HEIGHT: u32 = 2;
const REPLACEMENT_TIP_HEIGHT: u32 = 5;
const ROOT_SEARCH_MAX_MATCHES: usize = 8;
const ROOT_AND_RANKING_CONSUMERS: &[MaterializedViewConsumerSchema] = &[
    BLOCK_SUMMARY_SCHEMA,
    COMMITMENT_ROOT_SEARCH_SCHEMA,
    TRANSPARENT_ADDRESS_RANKING_SCHEMA,
    TRANSPARENT_OUTPOINT_SPEND_SCHEMA,
];
const BACKFILL_TIMEOUT: Duration = Duration::from_secs(10);
const RANKING_ACTIVATED_EVENT: &str = "transparent_address_ranking_activated";

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
async fn a_reader_without_final_roots_hydrates_the_same_facts_without_reading_tree_state()
-> Result<(), Box<dyn Error>> {
    let harness = CanonicalHarness::genesis_complete().await?;
    let activations = sample_regtest_upgrade_activations();
    let with_roots = CanonicalBlockContextReader::new(harness.secondary(), &activations)
        .read_ordered_block_commit_contexts(chain_range())?;
    assert!(
        with_roots
            .iter()
            .any(|context| context.final_note_commitment_roots.is_some()),
        "the fixture chain must produce note-commitment roots"
    );

    // The store refuses tree-state reads under an activation table it did not
    // admit, and tree state is the only read hydration makes through the
    // table, so a reader that succeeds with it never read tree state.
    let unadmitted = NetworkUpgradeActivations::empty(Network::ZcashRegtest);
    let refused = CanonicalBlockContextReader::new(harness.secondary(), &unadmitted)
        .read_ordered_block_commit_contexts(chain_range());
    assert!(
        matches!(
            refused,
            Err(IngestError::CanonicalStore(
                CanonicalStoreError::AdmissionRefused { .. }
            ))
        ),
        "deriving roots must read tree state, got {:?}",
        refused.err()
    );
    let without_roots = CanonicalBlockContextReader::new(harness.secondary(), &unadmitted)
        .without_final_roots()
        .read_ordered_block_commit_contexts(chain_range())?;

    assert_eq!(without_roots.len(), with_roots.len());
    for (without, with) in without_roots.iter().zip(&with_roots) {
        assert_eq!(without.final_note_commitment_roots, None);
        assert_same_facts_except_roots(without, with);
    }
    Ok(())
}

#[tokio::test]
async fn the_shared_replay_indexes_final_roots_for_root_search() -> Result<(), Box<dyn Error>> {
    let harness = CanonicalHarness::genesis_complete().await?;
    let expected = independent_final_roots(&harness.blocks, &sample_regtest_upgrade_activations())?;
    let directory = TempDir::new()?;
    let view = open_root_and_ranking_view_store(directory.path())?;
    harness.tailer(&view, "replay-secondary")?.catch_up()?;

    for (protocol, height) in [
        (ShieldedProtocol::Sapling, SAPLING_BLOCK_HEIGHT),
        (ShieldedProtocol::Orchard, ORCHARD_BLOCK_HEIGHT),
    ] {
        let height = BlockHeight::new(height);
        let roots = expected
            .get(&height)
            .ok_or_else(|| format!("the chain must contain height {}", height.value()))?;
        let root = match protocol {
            ShieldedProtocol::Sapling => roots.sapling,
            ShieldedProtocol::Orchard => roots.orchard,
            ShieldedProtocol::Ironwood | _ => roots.ironwood,
        }
        .ok_or_else(|| format!("height {} must carry a {protocol:?} root", height.value()))?;
        let matches = CommitmentRootSearchConsumer::search(&view, root, ROOT_SEARCH_MAX_MATCHES)?;
        assert!(
            matches
                .iter()
                .any(|entry| entry.protocol == protocol && entry.block_height == height),
            "root search must index the {protocol:?} root of height {}, found {matches:?}",
            height.value()
        );
    }
    Ok(())
}

#[tokio::test]
async fn the_ranking_build_writes_the_ranking_the_shared_replay_writes()
-> Result<(), Box<dyn Error>> {
    let harness = CanonicalHarness::genesis_complete().await?;
    let replayed_directory = TempDir::new()?;
    let replayed = open_root_and_ranking_view_store(replayed_directory.path())?;
    harness
        .tailer(&replayed, "replayed-secondary")?
        .catch_up()?;

    let built_directory = TempDir::new()?;
    let built = open_root_and_ranking_view_store(built_directory.path())?;
    harness.tailer(&built, "built-secondary")?.catch_up()?;
    drop(built);
    clear_transparent_address_ranking(built_directory.path())?;
    let built = open_root_and_ranking_view_store(built_directory.path())?;
    assert!(TransparentAddressRankingConsumer::active_metadata(&built)?.is_none());
    let capture = LogCapture::install_for_target("zinder::ingest");
    // The other views are already at the tip, so this catch-up hydrates only
    // for the ranking build. Under a table the store did not admit, a build
    // that derived roots would fail its tree-state read.
    let mut rebuild = harness.tailer(&built, "rebuilt-secondary")?;
    rebuild.activations = Arc::new(NetworkUpgradeActivations::empty(Network::ZcashRegtest));
    rebuild.catch_up()?;

    assert!(
        capture.any(|event| event.field("event") == Some(RANKING_ACTIVATED_EVENT)),
        "the ranking must be rebuilt by its own build, not by the shared replay"
    );
    assert_eq!(
        transparent_address_ranking(&built)?,
        transparent_address_ranking(&replayed)?
    );
    Ok(())
}

#[tokio::test]
async fn the_historical_backfills_write_the_rows_root_bearing_contexts_write()
-> Result<(), Box<dyn Error>> {
    let harness = CanonicalHarness::genesis_complete().await?;
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let tail_boundary = BlockHeight::new(CHAIN_TIP_HEIGHT + 1);

    let (_backfilled_directory, backfilled) = view_store()?;
    TransactionComponentSummaryConsumer::initialize_tail_boundary(&backfilled, tail_boundary)?;
    ConventionalFeeDistributionConsumer::initialize_tail_boundary(&backfilled, tail_boundary)?;
    // Under a table the store did not admit, a backfill that derived roots
    // would fail its tree-state read and never reach the tip.
    let unadmitted = Arc::new(NetworkUpgradeActivations::empty(Network::ZcashRegtest));
    run_historical_backfills(&harness, &unadmitted, &backfilled).await?;

    let (_written_directory, written) = view_store()?;
    TransactionComponentSummaryConsumer::initialize_tail_boundary(&written, tail_boundary)?;
    ConventionalFeeDistributionConsumer::initialize_tail_boundary(&written, tail_boundary)?;
    let contexts = CanonicalBlockContextReader::new(harness.secondary(), &activations)
        .read_ordered_block_commit_contexts(chain_range())?;
    assert!(
        contexts
            .iter()
            .any(|context| context.final_note_commitment_roots.is_some()),
        "the reference contexts must carry roots"
    );
    write_backfill_batches(&written, &contexts)?;

    let mut compared_rows = 0;
    for definition in bundled_materialized_view_consumer_definitions()
        .iter()
        .filter(|definition| {
            [
                TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME,
                CONVENTIONAL_FEE_DISTRIBUTION_CONSUMER_NAME,
            ]
            .contains(&definition.schema.name)
        })
    {
        for &column_family in definition.schema.column_families {
            let rows = view_rows(&backfilled, column_family)?;
            compared_rows += rows.len();
            assert_eq!(
                rows,
                view_rows(&written, column_family)?,
                "{column_family} must hold the rows root-bearing contexts write"
            );
        }
    }
    assert!(compared_rows > 0, "the backfills must write rows");
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
async fn a_prevout_cache_resolves_spends_from_earlier_ranges_without_reading_the_producing_block()
-> Result<(), Box<dyn Error>> {
    let harness = CanonicalHarness::genesis_complete().await?;
    let activations = sample_regtest_upgrade_activations();
    let expected = CanonicalBlockContextReader::new(harness.secondary(), &activations)
        .read_block_commit_contexts(chain_range())?;
    let mut prevouts = PrevoutCache::new(1_024);

    let (paged, reads) = with_prevout_metrics(|| {
        let mut reader = CanonicalBlockContextReader::with_prevout_cache(
            harness.secondary(),
            &activations,
            &mut prevouts,
        );
        let mut contexts = HashMap::new();
        for height in chain_range() {
            contexts.extend(
                reader.read_block_commit_contexts(BlockHeightRange::inclusive(height, height))?,
            );
        }
        Ok(contexts)
    })?;

    assert_same_spends(&paged, &expected)?;
    assert_eq!(reads.producing_block_reads, 0);
    assert_eq!(reads.from_cache, 1);
    assert_eq!(reads.from_canonical, 0);
    Ok(())
}

#[tokio::test]
async fn a_prevout_cache_reads_canonical_storage_after_a_disconnected_range()
-> Result<(), Box<dyn Error>> {
    let harness = CanonicalHarness::genesis_complete().await?;
    let activations = sample_regtest_upgrade_activations();
    let expected = CanonicalBlockContextReader::new(harness.secondary(), &activations)
        .read_block_commit_contexts(chain_range())?;
    let mut prevouts = PrevoutCache::new(1_024);
    let first = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1));
    let spending = BlockHeightRange::inclusive(
        BlockHeight::new(SPENDING_BLOCK_HEIGHT),
        BlockHeight::new(SPENDING_BLOCK_HEIGHT),
    );

    let (contexts, reads) = with_prevout_metrics(|| {
        let mut contexts = CanonicalBlockContextReader::with_prevout_cache(
            harness.secondary(),
            &activations,
            &mut prevouts,
        )
        .read_block_commit_contexts(first)?;
        contexts.extend(
            CanonicalBlockContextReader::with_prevout_cache(
                harness.secondary(),
                &activations,
                &mut prevouts,
            )
            .read_block_commit_contexts(spending)?,
        );
        Ok(contexts)
    })?;

    assert_same_spends(&contexts, &expected)?;
    assert_eq!(reads.from_cache, 0);
    assert_eq!(reads.from_canonical, 1);
    assert_eq!(reads.producing_block_reads, 1);
    Ok(())
}

#[tokio::test]
async fn a_prevout_cache_rereads_outputs_whose_producing_block_a_reorg_replaced()
-> Result<(), Box<dyn Error>> {
    let activations = sample_regtest_upgrade_activations();
    let ForkedSpendChains {
        original,
        replacement,
        first_spend_output,
    } = forked_spend_chains()?;
    let mut harness = CanonicalHarness::forkable(original).await?;
    let mut prevouts = PrevoutCache::new(1_024);
    {
        let mut reader = CanonicalBlockContextReader::with_prevout_cache(
            harness.secondary(),
            &activations,
            &mut prevouts,
        );
        for height in chain_range() {
            reader.read_block_commit_contexts(BlockHeightRange::inclusive(height, height))?;
        }
    }
    assert!(!prevouts.is_empty());

    harness.reorg_to(replacement).await?;
    let tip = BlockHeightRange::inclusive(
        BlockHeight::new(REPLACEMENT_TIP_HEIGHT),
        BlockHeight::new(REPLACEMENT_TIP_HEIGHT),
    );
    let expected = CanonicalBlockContextReader::new(harness.secondary(), &activations)
        .read_block_commit_contexts(tip)?;
    let (contexts, reads) = with_prevout_metrics(|| {
        CanonicalBlockContextReader::with_prevout_cache(
            harness.secondary(),
            &activations,
            &mut prevouts,
        )
        .read_block_commit_contexts(tip)
    })?;

    assert_same_spends(&contexts, &expected)?;
    let spends = contexts
        .get(&BlockHeight::new(REPLACEMENT_TIP_HEIGHT))
        .and_then(|context| context.transparent_spends())
        .ok_or("transparent spends must be hydrated")?;
    let spend = spends
        .get(&first_spend_output)
        .ok_or("the replacement tip must spend the first spend's output")?;
    assert_eq!(
        spend.spent_block_hash,
        harness
            .block_at(BlockHeight::new(SPENDING_BLOCK_HEIGHT))?
            .hash
    );
    assert_eq!(reads.from_cache, 0);
    assert_eq!(reads.from_canonical, 1);
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

#[tokio::test]
async fn stored_sparse_and_mixed_checkpoints_hydrate_byte_identical_roots()
-> Result<(), Box<dyn Error>> {
    const MIXED_TIP: u32 = 6;
    let activations = sample_regtest_upgrade_activations();
    let blocks = shielded_chain_through(MIXED_TIP)?;
    let expected = independent_final_roots(&blocks, &activations)?;
    let full_range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(MIXED_TIP));

    // (a) Sparse construction store: only heights 0 and MIXED_TIP have checkpoints.
    let sparse = CanonicalHarness::publish_chain(blocks.clone(), &activations).await?;
    assert_roots_match_expected(sparse.secondary(), &activations, full_range, &expected)?;

    // (b) Dense store: every height 1..=MIXED_TIP has a stored checkpoint via live following.
    let dense = CanonicalHarness::dense_chain(blocks.clone(), &activations).await?;
    assert_roots_match_expected(dense.secondary(), &activations, full_range, &expected)?;

    // (c) Mixed dense + sparse store: delete checkpoints at heights 3 and 5 so
    // the range alternates stored (1..=2) -> gap (3..=3) -> stored (4..=4) -> gap (5..=5) -> stored (6..=6).
    let mut mixed = CanonicalHarness::dense_chain(blocks, &activations).await?;
    mixed.delete_rows("tree_state_checkpoint", &[3, 5])?;
    assert_roots_match_expected(mixed.secondary(), &activations, full_range, &expected)?;

    // Multi-page sequential reads across dense/sparse boundaries must match byte-for-byte.
    for page_bounds in [
        vec![(1, 2), (3, 4), (5, 6)],
        vec![(1, 3), (4, 6)],
        vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)],
    ] {
        let mut reader = CanonicalBlockContextReader::new(mixed.secondary(), &activations);
        let mut collected = HashMap::new();
        for (start, end) in page_bounds {
            let page = BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end));
            collected.extend(reader.read_block_commit_contexts(page)?);
        }
        for height in full_range {
            let context = collected
                .get(&height)
                .ok_or_else(|| format!("paged hydration must cover height {}", height.value()))?;
            assert_eq!(
                context.final_note_commitment_roots.as_ref(),
                expected.get(&height)
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn pre_sapling_activation_boundaries_omit_roots_identically_for_sparse_and_dense_stores()
-> Result<(), Box<dyn Error>> {
    let activations = staged_pool_activations()?;
    let blocks = chain_through(CHAIN_TIP_HEIGHT, 0, &|_| Ok(Vec::new()))?;
    let expected = independent_final_roots(&blocks, &activations)?;
    assert_eq!(
        expected.get(&BlockHeight::new(1)),
        Some(&BlockFinalNoteCommitmentRoots::unavailable(
            BlockHeight::new(1),
            blocks
                .get(&BlockHeight::new(1))
                .ok_or("height 1 must exist")?
                .hash,
        ))
    );

    let sparse = CanonicalHarness::publish_chain(blocks.clone(), &activations).await?;
    let dense = CanonicalHarness::dense_chain(blocks, &activations).await?;
    for secondary in [sparse.secondary(), dense.secondary()] {
        let contexts = CanonicalBlockContextReader::new(secondary, &activations)
            .read_ordered_block_commit_contexts(chain_range())?;
        assert_eq!(contexts.len(), usize::try_from(CHAIN_TIP_HEIGHT)?);
        assert_eq!(
            contexts[0].final_note_commitment_roots, None,
            "pre-Sapling height 1 must omit final roots"
        );
        for context in &contexts[1..] {
            assert_eq!(
                context.final_note_commitment_roots.as_ref(),
                expected.get(&context.height),
                "post-Sapling height {} must carry exact roots",
                context.height.value()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn dense_checkpoints_hydrate_final_roots_without_reading_compact_blocks()
-> Result<(), Box<dyn Error>> {
    const DENSE_TIP: u32 = 6;
    let activations = sample_regtest_upgrade_activations();
    let blocks = shielded_chain_through(DENSE_TIP)?;
    let expected = independent_final_roots(&blocks, &activations)?;
    let full_range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(DENSE_TIP));

    // Delete compact_block rows for heights 1..=5 while keeping every stored
    // tree_state_checkpoint row (and tip height 6's compact block for admission).
    let mut dense = CanonicalHarness::dense_chain(blocks.clone(), &activations).await?;
    dense.delete_rows("compact_block", &[1, 2, 3, 4, 5])?;
    assert_eq!(
        dense.secondary().compact_block_at(BlockHeight::new(2))?,
        None
    );
    assert!(matches!(
        dense
            .secondary()
            .compact_blocks_in_range(BlockHeightRange::inclusive(
                BlockHeight::new(1),
                BlockHeight::new(5)
            )),
        Err(CanonicalStoreError::PublicationRefused { .. })
    ));
    assert_roots_match_expected(dense.secondary(), &activations, full_range, &expected)?;

    // Mixed case: remove compact_block only at checkpointed heights (1, 2, 4)
    // and remove checkpoints at uncheckpointed heights (3, 5). Hydration must
    // skip compact blocks for 1, 2, 4, 6 and replay only gaps 3..=3 and 5..=5.
    let mut mixed = CanonicalHarness::dense_chain(blocks, &activations).await?;
    mixed.delete_rows("compact_block", &[1, 2, 4])?;
    mixed.delete_rows("tree_state_checkpoint", &[3, 5])?;
    assert_roots_match_expected(mixed.secondary(), &activations, full_range, &expected)?;
    Ok(())
}

fn assert_roots_match_expected(
    secondary: &RocksDbCanonicalSecondary,
    activations: &NetworkUpgradeActivations,
    range: BlockHeightRange,
    expected: &BTreeMap<BlockHeight, BlockFinalNoteCommitmentRoots>,
) -> Result<(), Box<dyn Error>> {
    let mut reader = CanonicalBlockContextReader::new(secondary, activations);
    let contexts = reader.read_block_commit_contexts(range)?;
    for height in range {
        let context = contexts
            .get(&height)
            .ok_or_else(|| format!("hydration must cover height {}", height.value()))?;
        assert_eq!(
            context.final_note_commitment_roots.as_ref(),
            expected.get(&height),
            "roots mismatch at height {}",
            height.value()
        );
    }
    Ok(())
}

fn shielded_chain_through(
    tip_height: u32,
) -> Result<BTreeMap<BlockHeight, SourceBlock>, Box<dyn Error>> {
    chain_through(tip_height, 0, &|height| {
        if height == 5 {
            return shielded_transactions(&fixture_sapling_source_block()?);
        }
        if height == 4 {
            return shielded_transactions(&fixture_orchard_source_block()?);
        }
        Ok(Vec::new())
    })
}

fn staged_pool_activations() -> Result<NetworkUpgradeActivations, Box<dyn Error>> {
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        vec![
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x5ba8_1b19),
                activation_height: BlockHeight::new(1),
                name: "Overwinter".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x76b8_09bb),
                activation_height: BlockHeight::new(2),
                name: "Sapling".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x2bb4_0e60),
                activation_height: BlockHeight::new(2),
                name: "Blossom".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xf5b9_230b),
                activation_height: BlockHeight::new(2),
                name: "Heartwood".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xe9ff_75a6),
                activation_height: BlockHeight::new(2),
                name: "Canopy".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xc2d6_d0b4),
                activation_height: BlockHeight::new(4),
                name: "NU5".to_owned(),
            },
        ],
    )?)
}

/// Two chains that mine the same transparent spend at [`SPENDING_BLOCK_HEIGHT`]
/// in different blocks; only the replacement extends to a block spending it.
struct ForkedSpendChains {
    original: BTreeMap<BlockHeight, SourceBlock>,
    replacement: BTreeMap<BlockHeight, SourceBlock>,
    first_spend_output: TransparentOutPoint,
}

fn forked_spend_chains() -> Result<ForkedSpendChains, Box<dyn Error>> {
    let coinbase = coinbase_chain()?;
    let produced = coinbase
        .get(&BlockHeight::new(1))
        .ok_or("the chain must start at height 1")?;
    let first_spend = Arc::new(spending_transaction(TransparentOutPoint::new(
        coinbase_transaction_id(produced)?,
        0,
    ))?);
    let original = chain_through(CHAIN_TIP_HEIGHT, 0, &|height| {
        Ok(if height == SPENDING_BLOCK_HEIGHT {
            vec![Arc::clone(&first_spend)]
        } else {
            Vec::new()
        })
    })?;
    let first_spend_output = TransparentOutPoint::new(
        source_block_facts(
            original
                .get(&BlockHeight::new(SPENDING_BLOCK_HEIGHT))
                .ok_or("the chain must contain the spending block")?,
        )?
        .transactions
        .get(1)
        .ok_or("the spending block must carry the first spend")?
        .public_facts
        .transaction_id,
        0,
    );
    let second_spend = Arc::new(spending_transaction(first_spend_output)?);
    let replacement = chain_through(REPLACEMENT_TIP_HEIGHT, 1, &|height| {
        Ok(match height {
            SPENDING_BLOCK_HEIGHT => vec![Arc::clone(&first_spend)],
            REPLACEMENT_TIP_HEIGHT => vec![Arc::clone(&second_spend)],
            _ => Vec::new(),
        })
    })?;
    Ok(ForkedSpendChains {
        original,
        replacement,
        first_spend_output,
    })
}

/// Prevout resolution counters recorded while one closure hydrated.
struct PrevoutReads {
    from_cache: u64,
    from_canonical: u64,
    producing_block_reads: u64,
}

fn with_prevout_metrics<T>(
    hydrate: impl FnOnce() -> Result<T, IngestError>,
) -> Result<(T, PrevoutReads), Box<dyn Error>> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let hydrated = metrics::with_local_recorder(&recorder, hydrate)?;
    let rendered = handle.render();
    Ok((
        hydrated,
        PrevoutReads {
            from_cache: counter_value(
                &rendered,
                "zinder_materialized_view_prevout_resolutions_total",
                "source=\"cache\"",
            ),
            from_canonical: counter_value(
                &rendered,
                "zinder_materialized_view_prevout_resolutions_total",
                "source=\"canonical\"",
            ),
            producing_block_reads: counter_value(
                &rendered,
                "zinder_materialized_view_prevout_block_reads_total",
                "",
            ),
        },
    ))
}

/// Sums every rendered sample of `metric` whose labels contain `label`,
/// whatever other labels the sample carries.
fn counter_value(rendered: &str, metric: &str, label: &str) -> u64 {
    rendered
        .lines()
        .filter_map(|line| {
            let (series, sample) = line.rsplit_once(' ')?;
            let labels = series.strip_prefix(metric)?;
            let matches = (labels.is_empty() || labels.starts_with('{')) && labels.contains(label);
            matches.then(|| sample.parse::<u64>().ok()).flatten()
        })
        .sum()
}

/// Asserts that every block in `hydrated` carries the spend facts the
/// uncached whole-range hydration derived for that block's inputs.
fn assert_same_spends(
    hydrated: &HashMap<BlockHeight, Arc<BlockCommitContext>>,
    expected: &HashMap<BlockHeight, Arc<BlockCommitContext>>,
) -> Result<(), Box<dyn Error>> {
    for (height, context) in hydrated {
        let expected_context = expected
            .get(height)
            .ok_or_else(|| format!("expected hydration must cover height {}", height.value()))?;
        let spends = context
            .transparent_spends()
            .ok_or("transparent spends must be hydrated")?;
        let expected_spends = expected_context
            .transparent_spends()
            .ok_or("transparent spends must be hydrated")?;
        for input in context
            .transactions
            .iter()
            .flat_map(|transaction| &transaction.transparent_inputs)
            .filter(|input| !input.spent_outpoint.is_coinbase_sentinel())
        {
            let spend = spends
                .get(&input.spent_outpoint)
                .ok_or("every non-coinbase input must resolve")?;
            assert_eq!(Some(spend), expected_spends.get(&input.spent_outpoint));
        }
    }
    Ok(())
}

/// Runs the transaction-component and conventional-fee historical backfills
/// until both cover the settled tip.
async fn run_historical_backfills(
    harness: &CanonicalHarness,
    activations: &Arc<NetworkUpgradeActivations>,
    view: &MaterializedViewStore,
) -> Result<(), Box<dyn Error>> {
    let tip_height = BlockHeight::new(CHAIN_TIP_HEIGHT);
    let canonical = harness.shared_secondary("backfill-secondary")?;
    let cancel = CancellationToken::new();
    let tasks = [
        spawn_transaction_component_backfill_task(
            TransactionComponentBackfillConfig::DEFAULT,
            TransactionComponentBackfillContext::new(
                Arc::clone(&canonical),
                Arc::clone(activations),
                view.clone(),
            ),
            following_tip_gate(),
            cancel.clone(),
        ),
        spawn_conventional_fee_distribution_backfill_task(
            ConventionalFeeDistributionBackfillConfig::DEFAULT,
            ConventionalFeeDistributionBackfillContext::new(
                Arc::clone(&canonical),
                Arc::clone(activations),
                view.clone(),
            ),
            following_tip_gate(),
            cancel.clone(),
        ),
    ];
    let completed = tokio::time::timeout(BACKFILL_TIMEOUT, async {
        loop {
            let components = TransactionComponentSummaryConsumer::backfill_coverage(view)
                .map_err(|error| error.to_string())?;
            let fees = ConventionalFeeDistributionConsumer::backfill_coverage(view)
                .map_err(|error| error.to_string())?;
            if components.is_some_and(|coverage| coverage.complete_through_height == tip_height)
                && fees.is_some_and(|coverage| coverage.complete_through_height == tip_height)
            {
                return Ok::<_, String>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    cancel.cancel();
    for task in tasks {
        task.await?;
    }
    completed.map_err(|_| "both backfills must reach the settled tip")??;
    Ok(())
}

/// Writes `contexts` as one historical batch of each backfill-owned view.
fn write_backfill_batches(
    view: &MaterializedViewStore,
    contexts: &[BlockCommitContext],
) -> Result<(), Box<dyn Error>> {
    let first = contexts.first().ok_or("the chain must not be empty")?;
    let last = contexts.last().ok_or("the chain must not be empty")?;
    TransactionComponentSummaryConsumer::new()
        .write_backfill_batch(
            view,
            contexts,
            TransactionComponentBackfillCoverage {
                complete_from_height: first.height,
                complete_through_height: last.height,
                complete_from_time_unix_seconds: first.block_time_unix_seconds,
                complete_through_time_unix_seconds: last.block_time_unix_seconds,
            },
        )
        .map_err(|error| error.to_string())?;
    ConventionalFeeDistributionConsumer::new()
        .write_backfill_batch(
            view,
            contexts,
            ConventionalFeeDistributionBackfillCoverage::new(
                first.height,
                last.height,
                first.block_time_unix_seconds,
                last.block_time_unix_seconds,
            ),
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Opens a view store with only the views the shared replay can build from the
/// fixture chain, whose shielded transactions carry synthetic value balances
/// the fee views reject.
fn open_root_and_ranking_view_store(path: &Path) -> Result<MaterializedViewStore, Box<dyn Error>> {
    Ok(MaterializedViewStore::open(
        path,
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: ROOT_AND_RANKING_CONSUMERS,
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?)
}

/// Asserts that two hydrations of one block agree on every fact but roots.
fn assert_same_facts_except_roots(actual: &BlockCommitContext, expected: &BlockCommitContext) {
    assert_eq!(actual.height, expected.height);
    assert_eq!(actual.block_hash, expected.block_hash);
    assert_eq!(actual.previous_block_hash, expected.previous_block_hash);
    assert_eq!(
        actual.block_time_unix_seconds,
        expected.block_time_unix_seconds
    );
    assert_eq!(actual.block_size_bytes, expected.block_size_bytes);
    assert_eq!(actual.transactions, expected.transactions);
    assert_eq!(actual.transparent_spends(), expected.transparent_spends());
    assert_eq!(
        actual.transaction_intrinsic_value_balances(),
        expected.transaction_intrinsic_value_balances()
    );
    assert_eq!(
        actual.block_value_pool_balances(),
        expected.block_value_pool_balances()
    );
}

const fn chain_range() -> BlockHeightRange {
    BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(CHAIN_TIP_HEIGHT))
}

/// A published canonical store plus the in-process secondary tests read through.
struct CanonicalHarness {
    blocks: BTreeMap<BlockHeight, SourceBlock>,
    secondary: RocksDbCanonicalSecondary,
    store: Option<RocksDbCanonicalStore>,
    temporary: TempDir,
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
            zinder_store::RawBlobRetention::Transactions,
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
                sapling_only_frontiers()?,
            ),
            block_id(&blocks, CHAIN_TIP_HEIGHT)?,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
        )?;
        Self::publish(blocks, build_plan).await
    }

    /// Publishes `blocks` with only the fork parent settled, so a later
    /// [`Self::reorg_to`] can replace every height above it.
    async fn forkable(blocks: BTreeMap<BlockHeight, SourceBlock>) -> Result<Self, Box<dyn Error>> {
        let first_block = blocks
            .values()
            .next()
            .ok_or("the chain must not be empty")?
            .clone();
        let build_plan = CanonicalStoreBuildPlan::complete(
            &sample_regtest_upgrade_activations(),
            first_block.block_time_seconds.saturating_sub(1),
            block_id(&blocks, CHAIN_TIP_HEIGHT)?,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
        )?;
        Self::publish_settled_at(blocks, build_plan, FORK_PARENT_HEIGHT).await
    }

    async fn publish(
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        build_plan: CanonicalStoreBuildPlan,
    ) -> Result<Self, Box<dyn Error>> {
        Self::publish_settled_at(blocks, build_plan, CHAIN_TIP_HEIGHT).await
    }

    async fn publish_settled_at(
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        build_plan: CanonicalStoreBuildPlan,
        settled_height: u32,
    ) -> Result<Self, Box<dyn Error>> {
        Self::publish_settled_with_activations(
            blocks,
            build_plan,
            settled_height,
            &sample_regtest_upgrade_activations(),
        )
        .await
    }

    async fn publish_chain(
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        activations: &NetworkUpgradeActivations,
    ) -> Result<Self, Box<dyn Error>> {
        let first_block = blocks
            .values()
            .next()
            .ok_or("the chain must not be empty")?
            .clone();
        let tip_height = blocks
            .keys()
            .next_back()
            .copied()
            .ok_or("the chain must not be empty")?
            .value();
        let build_plan = CanonicalStoreBuildPlan::complete(
            activations,
            first_block.block_time_seconds.saturating_sub(1),
            block_id(&blocks, tip_height)?,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
        )?;
        Self::publish_settled_with_activations(blocks, build_plan, tip_height, activations).await
    }

    /// Publishes block 1 and live-follows every subsequent block so every
    /// height `1..=tip` carries a persisted `tree_state_checkpoint` row.
    async fn dense_chain(
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        activations: &NetworkUpgradeActivations,
    ) -> Result<Self, Box<dyn Error>> {
        let first = blocks
            .get(&BlockHeight::new(1))
            .ok_or("the chain must start at height 1")?
            .clone();
        let initial = BTreeMap::from([(BlockHeight::new(1), first)]);
        let mut harness = Self::publish_chain(initial, activations).await?;
        harness.follow_to(blocks, activations).await?;
        Ok(harness)
    }

    async fn publish_settled_with_activations(
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        build_plan: CanonicalStoreBuildPlan,
        settled_height: u32,
        activations: &NetworkUpgradeActivations,
    ) -> Result<Self, Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = build_canonical_store_with_activations(
            &store_path,
            &blocks,
            build_plan,
            settled_height,
            activations,
        )
        .await?;
        let secondary = RocksDbCanonicalSecondary::open_ready(
            &store_path,
            temporary.path().join("canonical-secondary"),
            activations,
            CanonicalStoreWorkload::Wallet,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        Ok(Self {
            blocks,
            secondary,
            store: Some(store),
            temporary,
        })
    }

    /// Closes the primary, deletes `heights` from `family` directly on disk,
    /// and reopens the secondary so reads observe the modified column family.
    fn delete_rows(&mut self, family: &str, heights: &[u32]) -> Result<(), Box<dyn Error>> {
        let primary_path = self.temporary.path().join("canonical");
        let activations = self
            .secondary
            .network_upgrade_activations()
            .cloned()
            .unwrap_or_else(sample_regtest_upgrade_activations);
        self.store = None;
        {
            let options = rust_rocksdb::Options::default();
            let column_families = rust_rocksdb::DB::list_cf(&options, &primary_path)?;
            let db = rust_rocksdb::DB::open_cf(&options, &primary_path, &column_families)?;
            let handle = db
                .cf_handle(family)
                .ok_or_else(|| format!("missing column family {family}"))?;
            for height in heights {
                db.delete_cf(&handle, height.to_be_bytes())?;
            }
            db.flush_cf(&handle)?;
        }
        self.secondary = RocksDbCanonicalSecondary::open_ready(
            &primary_path,
            self.temporary
                .path()
                .join(format!("canonical-secondary-after-{family}")),
            &activations,
            CanonicalStoreWorkload::Wallet,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        Ok(())
    }

    async fn follow_to(
        &mut self,
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        activations: &NetworkUpgradeActivations,
    ) -> Result<(), Box<dyn Error>> {
        let tip_height = blocks
            .keys()
            .next_back()
            .copied()
            .ok_or("the chain must not be empty")?;
        let source = StaticChainSource {
            checkpoints: chain_checkpoints(&blocks, activations)?,
            blocks: blocks.clone(),
        };
        let store = self
            .store
            .take()
            .ok_or("the canonical store must be open")?;
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        let cancel = CancellationToken::new();
        let follower = CanonicalFollower::new(
            &source,
            Arc::new(activations.clone()),
            CanonicalFollowConfig {
                request_timeout: Duration::from_secs(5),
                poll_interval: Duration::ZERO,
                lag_threshold_blocks: 0,
                target_height: Some(tip_height),
                event_retention_window: None,
                event_retention_check_interval: Duration::from_secs(1),
                mempool_ready_gate: None,
                max_replacement_bytes: DEFAULT_MAX_REPLACEMENT_BYTES,
                node_failover: None,
            },
            &readiness,
            &cancel,
        );
        self.store = Some(follow_canonical_tip(store, follower).await?);
        self.secondary.try_catch_up()?;
        self.blocks = blocks;
        Ok(())
    }

    /// Follows the canonical writer onto `blocks`, replacing every height
    /// where they differ from the published chain, and catches the secondary up.
    async fn reorg_to(
        &mut self,
        blocks: BTreeMap<BlockHeight, SourceBlock>,
    ) -> Result<(), Box<dyn Error>> {
        self.follow_to(blocks, &sample_regtest_upgrade_activations())
            .await
    }

    const fn secondary(&self) -> &RocksDbCanonicalSecondary {
        &self.secondary
    }

    /// Opens another secondary on the published store, for runtimes that
    /// share one behind a lock.
    fn shared_secondary(
        &self,
        secondary_name: &str,
    ) -> Result<Arc<RwLock<RocksDbCanonicalSecondary>>, Box<dyn Error>> {
        let secondary = RocksDbCanonicalSecondary::open_ready(
            self.temporary.path().join("canonical"),
            self.temporary.path().join(secondary_name),
            &sample_regtest_upgrade_activations(),
            CanonicalStoreWorkload::Wallet,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        Ok(Arc::new(RwLock::new(secondary)))
    }

    fn tailer(
        &self,
        view: &MaterializedViewStore,
        secondary_name: &str,
    ) -> Result<MaterializedViewTailer, Box<dyn Error>> {
        Ok(MaterializedViewTailer {
            canonical: self.shared_secondary(secondary_name)?,
            materialized_view_store: view.clone(),
            config: MaterializedViewReplayConfig::DEFAULT,
            activations: Arc::new(sample_regtest_upgrade_activations()),
            reorg_window_blocks: REORG_WINDOW_BLOCKS,
            chain_event_retention_window: Some(Duration::from_hours(168)),
            cursor_at_risk_warning: Duration::from_hours(24),
            prevout_caches: ReplayPrevoutCaches::default(),
            volume_space_guard: None,
        })
    }

    fn block_at(&self, height: BlockHeight) -> Result<&SourceBlock, Box<dyn Error>> {
        self.blocks
            .get(&height)
            .ok_or_else(|| format!("the chain must contain height {}", height.value()).into())
    }
}

async fn build_canonical_store_with_activations(
    store_path: &Path,
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    build_plan: CanonicalStoreBuildPlan,
    settled_height: u32,
    activations: &NetworkUpgradeActivations,
) -> Result<RocksDbCanonicalStore, Box<dyn Error>> {
    let activations = Arc::new(activations.clone());
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
        block_id(blocks, settled_height)?,
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
pub(super) struct PoolCommitments {
    pub(super) sapling: Vec<[u8; 32]>,
    pub(super) orchard: Vec<[u8; 32]>,
    pub(super) ironwood: Vec<[u8; 32]>,
}

pub(super) fn pool_commitments(
    source_block: &SourceBlock,
) -> Result<PoolCommitments, Box<dyn Error>> {
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

fn sapling_only_frontiers()
-> Result<CommitmentTreeFrontiers, zinder_core::CommitmentTreeFrontiersError> {
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
    chain_through(CHAIN_TIP_HEIGHT, 0, extra_transactions)
}

/// Builds heights `1..=tip_height` on the coinbase fixture.
///
/// Every block above [`FORK_PARENT_HEIGHT`] shifts its header time by
/// `fork_time_nonce`, so chains built with different nonces fork there even
/// when they carry the same transactions.
fn chain_through(
    tip_height: u32,
    fork_time_nonce: u8,
    extra_transactions: ExtraTransactions<'_>,
) -> Result<BTreeMap<BlockHeight, SourceBlock>, Box<dyn Error>> {
    let mut parent_hash = Network::ZcashRegtest.genesis_hash();
    let mut blocks = BTreeMap::new();
    for height in 1..=tip_height {
        let block = chained_source_block(
            BlockHeight::new(height),
            parent_hash,
            if height > FORK_PARENT_HEIGHT {
                fork_time_nonce
            } else {
                0
            },
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
pub(super) fn shielded_transactions(
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
        transaction_data.lock_time(),
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

pub(super) fn chained_source_block(
    height: BlockHeight,
    parent_hash: BlockHash,
    time_nonce: u8,
    extra_transactions: Vec<Arc<ZebraTransaction>>,
) -> Result<SourceBlock, Box<dyn Error>> {
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
    let key = TransparentTestKey::from_seed(&[0x42; 32])?;
    let recipient = key.scratch_recipient_address(1);
    let coinbase_value_zats = SPEND_OUTPUT_VALUE_ZAT.saturating_add(ZIP317_FEE_ONE_IN_ONE_OUT_ZATS);
    let bytes = key.build_p2pkh_spend(&P2pkhSpendArgs {
        coinbase_txid_be: spent_outpoint.transaction_id.as_bytes(),
        coinbase_vout: spent_outpoint.output_index,
        coinbase_value_zats,
        recipient: &recipient,
        target_height: 120,
    })?;
    Ok(ZebraTransaction::zcash_deserialize(bytes.as_slice())?)
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
