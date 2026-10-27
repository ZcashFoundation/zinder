//! Production-store fixtures for the immutable wallet-serving boundary.

use eyre::{Context as _, ensure, eyre};
use tempfile::TempDir;
use zinder_core::{
    BlockHeight, BlockId, CanonicalBlockReplayEnvelope, ChainTipMetadata, CommitmentTreeCheckpoint,
    CommitmentTreeFrontiers, NetworkUpgradeActivations, UnixTimestampMillis,
    decode_canonical_block_replay,
};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalLiveAppend, CanonicalReorgPolicy,
    CanonicalStoreBuildPlan, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder,
    RocksDbCanonicalSecondary, RocksDbResourceBudget, TREE_STATE_CHECKPOINT_STRIDE,
};
use zinder_wallet_rocksdb::{
    RocksDbWalletBuildOptions, RocksDbWalletSecondary, build_wallet_from_canonical,
};

use crate::{ChainFixture, FixtureBlock, FixtureTransactionRows};

const TEST_REORG_DEPTH: u32 = 100;

/// Tempdir-backed production canonical and wallet secondaries at one READY fence.
///
/// This fixture deliberately builds both primaries through their production
/// publication paths before opening read-only secondaries. Tests can therefore
/// certify a `WalletServingReadPair` without substituting an
/// in-memory reader or a retired materialized-view adapter.
pub struct WalletServingStoreFixture {
    canonical_reader: Option<RocksDbCanonicalSecondary>,
    wallet_reader: Option<RocksDbWalletSecondary>,
    _temporary_directory: TempDir,
}

impl WalletServingStoreFixture {
    /// Builds READY primaries from `chain_fixture` and opens immutable secondaries.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture is empty, omits a transaction blob,
    /// carries non-empty commitment-tree positions without matching frontiers,
    /// or fails either production store's validation and publication contract.
    #[allow(
        clippy::too_many_lines,
        reason = "The fixture keeps canonical publication, wallet projection, and secondary admission in their production order."
    )]
    pub fn from_chain(
        chain_fixture: &ChainFixture,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        ensure!(
            chain_fixture.network() == network_upgrade_activations.network(),
            "chain fixture and network-upgrade activations must use the same network"
        );
        let canonical_chain_fixture = chain_fixture.clone().with_canonical_genesis_parent();
        let raw_blob_retention = canonical_chain_fixture.raw_blob_retention();
        let tip_block = canonical_chain_fixture
            .blocks()
            .last()
            .ok_or_else(|| eyre!("wallet-serving fixture requires at least one block"))?;
        let tip = BlockId::new(tip_block.height, tip_block.hash);
        let temporary_directory =
            TempDir::new().wrap_err("create wallet-serving fixture directory")?;
        let canonical_primary_path = temporary_directory.path().join("canonical-primary");
        let wallet_primary_path = temporary_directory.path().join("wallet-primary");
        let reorg_policy = CanonicalReorgPolicy::new(TEST_REORG_DEPTH)?;
        let build_plan = CanonicalStoreBuildPlan::complete(
            network_upgrade_activations,
            tip_block.block_time_seconds.saturating_sub(1),
            tip,
            raw_blob_retention,
            reorg_policy,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            &canonical_primary_path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let canonical_blocks = canonical_build_blocks(&canonical_chain_fixture)?;
        builder.bulk_load_blocks(
            canonical_blocks
                .into_iter()
                .map(Ok::<_, std::convert::Infallible>),
        )?;
        builder.load_subtree_roots(std::iter::empty())?;
        let tip_checkpoint = CommitmentTreeCheckpoint::new(
            tip,
            tip_block.block_time_seconds,
            CommitmentTreeFrontiers::default(),
        );
        builder.confirm_source_tip_checkpoint(&tip_checkpoint)?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            tip,
            UnixTimestampMillis::new(u64::from(tip_block.block_time_seconds).saturating_mul(1_000)),
        ))?;
        let canonical_primary = validated.publish_baseline(publication)?;

        let wallet_outcome = build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: TEST_REORG_DEPTH,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        drop(wallet_outcome.store);
        drop(canonical_primary);

        let canonical_reader = RocksDbCanonicalSecondary::open_ready(
            &canonical_primary_path,
            temporary_directory.path().join("canonical-secondary"),
            network_upgrade_activations,
            CanonicalStoreWorkload::Wallet,
            raw_blob_retention,
            reorg_policy,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let wallet_reader = RocksDbWalletSecondary::open_ready(
            &wallet_primary_path,
            temporary_directory.path().join("wallet-secondary"),
            chain_fixture.network(),
            RocksDbResourceBudget::for_local_tests(),
        )?;

        Ok(Self {
            canonical_reader: Some(canonical_reader),
            wallet_reader: Some(wallet_reader),
            _temporary_directory: temporary_directory,
        })
    }

    /// Builds a READY pair whose final block was published through the live
    /// append path, producing a second chain epoch.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture has fewer than two blocks or either
    /// production store rejects the baseline or live append.
    pub fn from_chain_after_live_append(
        chain_fixture: &ChainFixture,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        Self::build_after_live_append(chain_fixture, network_upgrade_activations, None)
    }

    /// Builds a READY live-append pair whose canonical writer admitted
    /// `admitted_activations` over a store built with
    /// `construction_activations`.
    ///
    /// Before the live append, the writer admits the construction table as
    /// revision 1, as a writer's first start does, and then
    /// `admitted_activations` as the next revision. The live block is
    /// committed with the admitted table, and the canonical reader opens with
    /// it, as a serving publisher does after following writer status.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture has fewer than two blocks, the writer
    /// refuses either schedule revision, or either production store rejects
    /// the baseline or live append.
    pub fn from_chain_after_admitted_live_append(
        chain_fixture: &ChainFixture,
        construction_activations: &NetworkUpgradeActivations,
        admitted_activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        Self::build_after_live_append(
            chain_fixture,
            construction_activations,
            Some(admitted_activations),
        )
    }

    #[allow(
        clippy::too_many_lines,
        reason = "The fixture keeps baseline publication, schedule admission, live append, wallet build, and immutable reader admission in production order."
    )]
    fn build_after_live_append(
        chain_fixture: &ChainFixture,
        construction_activations: &NetworkUpgradeActivations,
        admitted_activations: Option<&NetworkUpgradeActivations>,
    ) -> eyre::Result<Self> {
        ensure!(
            chain_fixture.network() == construction_activations.network(),
            "chain fixture and network-upgrade activations must use the same network"
        );
        let live_tip = chain_fixture
            .blocks()
            .last()
            .ok_or_else(|| eyre!("wallet-serving fixture requires at least two blocks"))?;
        let baseline_fixture = chain_fixture.fork_at(live_tip.height)?;
        ensure!(
            !baseline_fixture.blocks().is_empty(),
            "wallet-serving live-append fixture requires at least two blocks"
        );
        let baseline_chain = baseline_fixture.with_canonical_genesis_parent();
        let raw_blob_retention = baseline_chain.raw_blob_retention();
        let baseline_tip = baseline_chain
            .blocks()
            .last()
            .ok_or_else(|| eyre!("wallet-serving fixture requires a baseline block"))?;
        let baseline_tip_id = BlockId::new(baseline_tip.height, baseline_tip.hash);
        let temporary_directory =
            TempDir::new().wrap_err("create wallet-serving fixture directory")?;
        let canonical_primary_path = temporary_directory.path().join("canonical-primary");
        let wallet_primary_path = temporary_directory.path().join("wallet-primary");
        let reorg_policy = CanonicalReorgPolicy::new(TEST_REORG_DEPTH)?;
        let build_plan = CanonicalStoreBuildPlan::complete(
            construction_activations,
            baseline_tip.block_time_seconds.saturating_sub(1),
            baseline_tip_id,
            raw_blob_retention,
            reorg_policy,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            &canonical_primary_path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        builder.bulk_load_blocks(
            canonical_build_blocks(&baseline_chain)?
                .into_iter()
                .map(Ok::<_, std::convert::Infallible>),
        )?;
        builder.load_subtree_roots(std::iter::empty())?;
        let baseline_checkpoint = CommitmentTreeCheckpoint::new(
            baseline_tip_id,
            baseline_tip.block_time_seconds,
            CommitmentTreeFrontiers::default(),
        );
        builder.confirm_source_tip_checkpoint(&baseline_checkpoint)?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            baseline_tip_id,
            UnixTimestampMillis::new(
                u64::from(baseline_tip.block_time_seconds).saturating_mul(1_000),
            ),
        ))?;
        let mut canonical_primary = validated.publish_baseline(publication)?;
        let live_activations = match admitted_activations {
            Some(admitted_activations) => {
                canonical_primary.admit_network_upgrade_schedule(construction_activations)?;
                canonical_primary.admit_network_upgrade_schedule(admitted_activations)?;
                admitted_activations
            }
            None => construction_activations,
        };
        let expected_fence = canonical_primary.event_fence();
        let full_chain = chain_fixture.clone().with_canonical_genesis_parent();
        let live_block = canonical_build_blocks(&full_chain)?
            .pop()
            .ok_or_else(|| eyre!("wallet-serving fixture requires a live block"))?;
        let (canonical_primary, _) = canonical_primary.commit_live_append(
            CanonicalLiveAppend::new(
                expected_fence,
                live_block,
                Vec::new(),
                expected_fence.visible_tip(),
                UnixTimestampMillis::new(
                    u64::from(live_tip.block_time_seconds).saturating_mul(1_000),
                ),
            ),
            live_activations,
        )?;

        let wallet_outcome = build_wallet_from_canonical(
            &canonical_primary,
            &wallet_primary_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: TEST_REORG_DEPTH,
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        drop(wallet_outcome.store);
        drop(canonical_primary);

        let canonical_reader = RocksDbCanonicalSecondary::open_ready(
            &canonical_primary_path,
            temporary_directory.path().join("canonical-secondary"),
            live_activations,
            CanonicalStoreWorkload::Wallet,
            raw_blob_retention,
            reorg_policy,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let wallet_reader = RocksDbWalletSecondary::open_ready(
            &wallet_primary_path,
            temporary_directory.path().join("wallet-secondary"),
            chain_fixture.network(),
            RocksDbResourceBudget::for_local_tests(),
        )?;

        Ok(Self {
            canonical_reader: Some(canonical_reader),
            wallet_reader: Some(wallet_reader),
            _temporary_directory: temporary_directory,
        })
    }

    /// Takes the admitted readers while retaining their temporary primary paths.
    ///
    /// # Errors
    ///
    /// Returns an error if the readers were already taken.
    pub fn take_readers(
        &mut self,
    ) -> eyre::Result<(RocksDbCanonicalSecondary, RocksDbWalletSecondary)> {
        let canonical_reader = self
            .canonical_reader
            .take()
            .ok_or_else(|| eyre!("wallet-serving canonical reader was already taken"))?;
        let wallet_reader = self
            .wallet_reader
            .take()
            .ok_or_else(|| eyre!("wallet-serving wallet reader was already taken"))?;
        Ok((canonical_reader, wallet_reader))
    }
}

fn canonical_build_blocks(chain_fixture: &ChainFixture) -> eyre::Result<Vec<CanonicalBuildBlock>> {
    let transaction_rows = chain_fixture.canonical_transaction_rows();
    let replay_envelopes = chain_fixture.block_replay_envelopes();
    let compact_blocks = chain_fixture.compact_block_artifacts();
    let tip_height = chain_fixture
        .tip_height()
        .ok_or_else(|| eyre!("wallet-serving fixture requires a tip"))?;
    let build_inputs = CanonicalBlockBuildInputs {
        transaction_rows: &transaction_rows,
        tip_height,
        raw_blob_retention: chain_fixture.raw_blob_retention(),
    };
    let mut build_blocks = Vec::with_capacity(chain_fixture.block_count());

    for ((fixture_block, replay_envelope), compact_block) in chain_fixture
        .blocks()
        .iter()
        .zip(replay_envelopes)
        .zip(compact_blocks)
    {
        build_blocks.push(canonical_build_block(
            fixture_block,
            replay_envelope,
            compact_block,
            &build_inputs,
        )?);
    }

    Ok(build_blocks)
}

struct CanonicalBlockBuildInputs<'a> {
    transaction_rows: &'a [FixtureTransactionRows],
    tip_height: BlockHeight,
    raw_blob_retention: RawBlobRetention,
}

fn canonical_build_block(
    fixture_block: &FixtureBlock,
    replay_envelope: CanonicalBlockReplayEnvelope,
    compact_block: zinder_core::CompactBlockArtifact,
    build_inputs: &CanonicalBlockBuildInputs<'_>,
) -> eyre::Result<CanonicalBuildBlock> {
    let facts = decode_canonical_block_replay(replay_envelope.as_bytes())
        .wrap_err_with(|| {
            format!(
                "decode canonical fixture replay at height {}",
                fixture_block.height.value()
            )
        })?
        .into_facts();
    let metadata = compact_block.chain_metadata();
    let tip_metadata = ChainTipMetadata::new(
        metadata.sapling_commitment_tree_size,
        metadata.orchard_commitment_tree_size,
        metadata.ironwood_commitment_tree_size,
    );
    ensure!(
        tip_metadata == ChainTipMetadata::empty(),
        "wallet-serving fixture currently requires empty commitment-tree frontiers"
    );

    let mut block_transaction_rows = build_inputs
        .transaction_rows
        .iter()
        .filter(|rows| {
            rows.location.block_height == fixture_block.height
                && rows.location.block_hash == fixture_block.hash
        })
        .collect::<Vec<_>>();
    block_transaction_rows.sort_by_key(|rows| rows.location.tx_index_in_block);
    let transaction_blobs = if build_inputs.raw_blob_retention.retains_transaction_blobs() {
        block_transaction_rows
            .into_iter()
            .map(|rows| {
                rows.blob.clone().ok_or_else(|| {
                    eyre!(
                        "production canonical fixture transaction {:?} has no raw blob",
                        rows.location.transaction_id
                    )
                })
            })
            .collect::<eyre::Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    let checkpoint_required = fixture_block.height == build_inputs.tip_height
        || fixture_block
            .height
            .value()
            .is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE);
    let tree_state_checkpoint = checkpoint_required.then(|| {
        CommitmentTreeCheckpoint::new(
            BlockId::new(fixture_block.height, fixture_block.hash),
            fixture_block.block_time_seconds,
            CommitmentTreeFrontiers::default(),
        )
    });

    Ok(CanonicalBuildBlock {
        facts,
        replay_envelope,
        compact_block,
        tip_metadata,
        tree_state_checkpoint,
        block_final_note_commitment_roots: None,
        transaction_blobs,
        block_blob: build_inputs
            .raw_blob_retention
            .retains_block_blobs()
            .then(|| fixture_block.block_blob_artifact()),
    })
}
