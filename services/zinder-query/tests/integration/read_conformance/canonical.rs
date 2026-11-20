//! Conformance cases for [`CanonicalReader`].

use std::num::NonZeroU32;

use eyre::{ensure, eyre};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CommitmentTreeCheckpoint,
    CommitmentTreeFrontier, CommitmentTreeFrontiers, CompactBlockArtifact, CompactChainMetadata,
    Network, SUBTREE_LEAF_COUNT, ShieldedProtocol, SubtreeRootHash, SubtreeRootIndex,
    SubtreeRootRange, TransactionId,
};
use zinder_query::{ArtifactKey, CanonicalReader, StorageReadError};
use zinder_store::{
    ArtifactFamily, BlockHashLookup, CanonicalActivationSchedule, CanonicalBuildSubtreeRoot,
    ChainEvent, ChainEventEnvelope, ChainEventStreamFamily, EventStreamStartPosition, HeightOrder,
    RawBlobRetention, ServedTreeState, StreamCursorTokenV1, TreeStateLookup, TreeStateReplay,
    TreeStateReplayOutcome,
};
use zinder_testkit::{
    CanonicalBaseline, ChainFixture, FixtureBlock, FixtureTransactionRows,
    commitment_tree_checkpoints, completed_subtree_frontier, frontier_of_size,
    sample_regtest_upgrade_activations, with_note_commitments,
};

use super::ReaderHarness;

/// Blocks of the chain most cases publish.
const TIP: u32 = 5;
/// Pages read here are far below any engine's page bound.
const MANY_EVENTS: NonZeroU32 = NonZeroU32::MIN.saturating_add(99);

pub(crate) async fn admitted_identity_matches_the_published_fence<H: ReaderHarness>()
-> eyre::Result<()> {
    let chain = chain(RawBlobRetention::All);
    let activations = sample_regtest_upgrade_activations();
    let mut harness = H::publish(&CanonicalBaseline::new(&chain), &activations)?;
    let reader = harness.canonical_reader()?;
    let tip = block_id(&chain, TIP)?;

    assert_eq!(reader.network(), Network::ZcashRegtest);
    assert_eq!(
        reader
            .network_upgrade_schedule()
            .map(CanonicalActivationSchedule::activations),
        Some(&activations)
    );
    assert_eq!(reader.raw_blob_retention(), RawBlobRetention::All);
    assert_eq!(reader.event_fence().visible_tip(), tip);
    let chain_epoch = reader.chain_epoch();
    assert_eq!(chain_epoch.id, reader.event_fence().chain_epoch_id());
    assert_eq!(chain_epoch.network, Network::ZcashRegtest);
    assert_eq!(
        BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash),
        tip
    );
    assert_eq!(
        BlockId::new(chain_epoch.settled_tip_height, chain_epoch.settled_tip_hash),
        tip
    );
    assert_eq!(
        reader.history_bounds().first_available_height(),
        BlockHeight::new(1)
    );
    Ok(())
}

pub(crate) async fn single_block_reads_answer_retained_heights_and_absence<H: ReaderHarness>()
-> eyre::Result<()> {
    let chain = chain(RawBlobRetention::All);
    let reader = published::<H>(&CanonicalBaseline::new(&chain))?;

    for height in 1..=TIP {
        let block = fixture_block(&chain, height)?;
        let header = reader
            .block_header_at(block.height)
            .await?
            .ok_or_else(|| eyre!("height {height} must have a header"))?;
        assert_eq!(header.height, block.height);
        assert_eq!(header.block_hash, block.hash);
        if height > 1 {
            assert_eq!(header.parent_hash, block.parent_hash);
        }
        let compact_block = reader
            .compact_block_at(block.height)
            .await?
            .ok_or_else(|| eyre!("height {height} must have a compact block"))?;
        assert_eq!(
            block_id_of(&compact_block),
            BlockId::new(block.height, block.hash)
        );
        assert_eq!(compact_block.previous_block_hash(), header.parent_hash);
        let blob = reader
            .block_blob_at(block.height)
            .await?
            .ok_or_else(|| eyre!("height {height} must retain its block"))?;
        assert_eq!(
            BlockId::new(blob.height, blob.block_hash),
            BlockId::new(block.height, block.hash)
        );
        assert_eq!(blob.parent_hash, header.parent_hash);
        assert_eq!(blob.raw_block_bytes, block.raw_block_bytes);
        assert_eq!(
            reader.block_hash_lookup(block.hash).await?,
            BlockHashLookup::Resolved(BlockId::new(block.height, block.hash))
        );
        assert_eq!(
            reader.block_header_by_hash(block.hash).await?.as_ref(),
            Some(&header)
        );
        assert_eq!(
            reader.compact_block_by_hash(block.hash).await?.as_ref(),
            Some(&compact_block)
        );
    }

    let above_tip = BlockHeight::new(TIP + 1);
    assert_eq!(reader.block_header_at(above_tip).await?, None);
    assert_eq!(reader.compact_block_at(above_tip).await?, None);
    assert_eq!(reader.block_blob_at(above_tip).await?, None);
    let unknown = BlockHash::from_bytes([0xee; 32]);
    assert_eq!(
        reader.block_hash_lookup(unknown).await?,
        BlockHashLookup::NotIndexed
    );
    assert_eq!(reader.block_header_by_hash(unknown).await?, None);
    assert_eq!(reader.compact_block_by_hash(unknown).await?, None);
    Ok(())
}

pub(crate) async fn compact_block_ranges_answer_their_ends<H: ReaderHarness>() -> eyre::Result<()> {
    let chain = chain(RawBlobRetention::Transactions);
    let reader = published::<H>(&CanonicalBaseline::new(&chain))?;

    let blocks = reader.compact_blocks_in_range(range(1, TIP)).await?;
    assert_eq!(heights(&blocks), (1..=TIP).collect::<Vec<_>>());
    for block in &blocks {
        assert_eq!(
            reader.compact_block_at(block.height()).await?.as_ref(),
            Some(block)
        );
    }
    assert_eq!(
        heights(&reader.compact_blocks_in_range(range(3, 3)).await?),
        vec![3]
    );

    // The lowest height outside retained history names the failure, even
    // where the rest of the range is retained.
    assert_unavailable_height(
        reader.compact_blocks_in_range(range(TIP, TIP + 2)).await,
        TIP + 1,
    )?;
    assert_unavailable_height(reader.compact_blocks_in_range(range(0, 2)).await, 0)?;
    Ok(())
}

pub(crate) async fn byte_bounded_ranges_keep_whole_blocks_from_the_first_end<H: ReaderHarness>()
-> eyre::Result<()> {
    let chain = chain(RawBlobRetention::Transactions);
    let reader = published::<H>(&CanonicalBaseline::new(&chain))?;
    let whole = range(1, TIP);

    let one_ascending = reader
        .compact_blocks_in_range_up_to(whole, HeightOrder::Ascending, 1)
        .await?;
    assert_eq!(heights(&one_ascending), vec![1]);
    let one_descending = reader
        .compact_blocks_in_range_up_to(whole, HeightOrder::Descending, 1)
        .await?;
    assert_eq!(heights(&one_descending), vec![TIP]);
    for order in [HeightOrder::Ascending, HeightOrder::Descending] {
        let all = reader
            .compact_blocks_in_range_up_to(whole, order, usize::MAX)
            .await?;
        assert_eq!(heights(&all), (1..=TIP).collect::<Vec<_>>());
    }

    // The whole range is checked before the budget stops the read.
    assert_unavailable_height(
        reader
            .compact_blocks_in_range_up_to(range(TIP, TIP + 1), HeightOrder::Ascending, 1)
            .await,
        TIP + 1,
    )?;
    Ok(())
}

pub(crate) async fn block_blob_reads_follow_raw_blob_retention<H: ReaderHarness>()
-> eyre::Result<()> {
    let retained = chain(RawBlobRetention::All);
    let reader = published::<H>(&CanonicalBaseline::new(&retained))?;
    let blobs = reader.block_blobs_in_range(range(2, TIP)).await?;
    assert_eq!(blobs.len(), usize_of(TIP - 1));
    for (height, blob) in (2..=TIP).zip(&blobs) {
        let expected = fixture_block(&retained, height)?.block_blob_artifact();
        assert_eq!(blob.as_ref(), Some(&expected), "height {height}");
    }

    let unretained = chain(RawBlobRetention::Transactions);
    let reader = published::<H>(&CanonicalBaseline::new(&unretained))?;
    assert_eq!(reader.raw_blob_retention(), RawBlobRetention::Transactions);
    assert_eq!(reader.block_blob_at(BlockHeight::new(1)).await?, None);
    assert!(
        reader
            .block_blobs_in_range(range(1, TIP))
            .await?
            .iter()
            .all(Option::is_none)
    );
    Ok(())
}

pub(crate) async fn mined_transactions_are_found_or_proven_absent<H: ReaderHarness>()
-> eyre::Result<()> {
    let (chain, mined) = chain_with_mined_transaction(RawBlobRetention::Transactions)?;
    let reader = published::<H>(&CanonicalBaseline::new(&chain))?;
    let block = fixture_block(&chain, MINED_HEIGHT)?;

    let transaction = reader
        .mined_transaction(mined)
        .await?
        .ok_or_else(|| eyre!("the mined transaction must be found"))?;
    assert_eq!(transaction.location.transaction_id, mined);
    assert_eq!(transaction.location.block_height, block.height);
    assert_eq!(transaction.location.block_hash, block.hash);
    assert_eq!(transaction.location.tx_index_in_block, 1);
    assert_eq!(
        transaction
            .block_header
            .map(|header| BlockId::new(header.height, header.block_hash)),
        Some(BlockId::new(block.height, block.hash))
    );
    assert_eq!(
        transaction
            .transaction_blob
            .map(|blob| blob.raw_transaction_bytes),
        Some(MINED_BYTES.to_vec())
    );
    assert_eq!(reader.mined_transaction(never_mined()).await?, None);

    let blob = reader.transaction_blob_by_id(mined).await?;
    assert_eq!(blob.location, transaction.location);
    assert_eq!(blob.raw_transaction_bytes, MINED_BYTES.to_vec());

    let (unretained_chain, mined) = chain_with_mined_transaction(RawBlobRetention::None)?;
    let reader = published::<H>(&CanonicalBaseline::new(&unretained_chain))?;
    let transaction = reader
        .mined_transaction(mined)
        .await?
        .ok_or_else(|| eyre!("an unretained transaction must still be found"))?;
    assert_eq!(transaction.transaction_blob, None);
    Ok(())
}

pub(crate) async fn transaction_bytes_name_the_missing_artifact<H: ReaderHarness>()
-> eyre::Result<()> {
    let (chain, _) = chain_with_mined_transaction(RawBlobRetention::Transactions)?;
    let reader = published::<H>(&CanonicalBaseline::new(&chain))?;
    assert_transaction_unavailable(
        reader.transaction_blob_by_id(never_mined()).await,
        ArtifactFamily::TransactionLocation,
        never_mined(),
    )?;

    let (unretained_chain, mined) = chain_with_mined_transaction(RawBlobRetention::None)?;
    let reader = published::<H>(&CanonicalBaseline::new(&unretained_chain))?;
    assert_transaction_unavailable(
        reader.transaction_blob_by_id(mined).await,
        ArtifactFamily::TransactionBlob,
        mined,
    )
}

pub(crate) async fn a_checkpointed_store_cannot_prove_an_absence<H: ReaderHarness>()
-> eyre::Result<()> {
    let chain = chain(RawBlobRetention::Transactions);
    let floor = BlockHeight::new(2);
    let reader = published::<H>(&CanonicalBaseline::new(&chain).with_history_floor(floor))?;

    assert!(
        reader
            .history_bounds()
            .intentionally_excludes(BlockHeight::new(1))
    );
    assert_eq!(
        reader.history_bounds().first_available_height(),
        BlockHeight::new(3)
    );
    match reader.mined_transaction(never_mined()).await {
        Err(StorageReadError::CanonicalHistoryUnavailable {
            first_available_height,
        }) => assert_eq!(first_available_height, BlockHeight::new(3)),
        other => return Err(eyre!("expected an unprovable absence, got {other:?}")),
    }
    assert_eq!(reader.compact_block_at(floor).await?, None);
    assert_unavailable_height(reader.compact_blocks_in_range(range(2, 3)).await, 2)?;
    assert_eq!(
        heights(&reader.compact_blocks_in_range(range(3, TIP)).await?),
        (3..=TIP).collect::<Vec<_>>()
    );
    Ok(())
}

pub(crate) async fn tree_states_are_stored_or_replayed<H: ReaderHarness>() -> eyre::Result<()> {
    let chain = chain(RawBlobRetention::Transactions);
    let reader = published::<H>(&CanonicalBaseline::new(&chain))?;
    let tip = block_id(&chain, TIP)?;

    let latest = reader
        .tree_state_checkpoint_at_or_before(tip.height)
        .await?
        .ok_or_else(|| eyre!("the tip must carry a checkpoint"))?;
    assert_eq!(latest.block_id(), tip);
    let Some(TreeStateLookup::Stored(stored)) = reader.tree_state_lookup(tip.height).await? else {
        return Err(eyre!("the tip's tree state must be stored"));
    };
    assert_eq!(stored, latest);
    assert_eq!(
        reader.tree_state_lookup(BlockHeight::new(TIP + 1)).await?,
        None
    );

    // Below the tip the baseline stores no checkpoint, so the tree state is
    // rebuilt from the one below it.
    let below_tip = block_id(&chain, TIP - 1)?;
    let Some(TreeStateLookup::Replay(replay)) = reader.tree_state_lookup(below_tip.height).await?
    else {
        return Err(eyre!("a height without a checkpoint must be replayed"));
    };
    assert_eq!(replay.height(), below_tip.height);
    assert!(replay.seed().block_id.height < below_tip.height);
    let Some(TreeStateReplayOutcome::Replayed { tree_state, .. }) =
        reader.replay_tree_state(replay).await?
    else {
        return Err(eyre!("the replay must rebuild the tree state"));
    };
    assert_eq!(tree_state.block_id(), below_tip);
    assert_eq!(
        reader
            .tree_state_checkpoint_at_or_before(below_tip.height)
            .await?
            .map(|checkpoint| checkpoint.block_id().height < below_tip.height),
        Some(true),
        "the newest checkpoint at or below an unstored height lies below it"
    );
    Ok(())
}

/// A replay built outside a reader's lookup proves nothing about its seed,
/// so the reader replays from the seed it stores itself.
///
/// The forged seed names the stored checkpoint's block and tree sizes, so
/// the replayed blocks link to it and reach their recorded sizes, but its
/// frontiers hold other commitments. Replaying from it would serve a wrong
/// tree state.
pub(crate) async fn a_replay_reads_its_own_seed_not_one_built_outside_it<H: ReaderHarness>()
-> eyre::Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let chain = with_note_commitments(&chain(RawBlobRetention::Transactions), &activations)?;
    let floor = BlockHeight::new(2);
    let mut harness = H::publish(
        &CanonicalBaseline::new(&chain).with_history_floor(floor),
        &activations,
    )?;
    let reader = harness.canonical_reader()?;
    let height = BlockHeight::new(TIP - 1);
    let Some(TreeStateLookup::Replay(issued)) = reader.tree_state_lookup(height).await? else {
        return Err(eyre!("a height between checkpoints must be replayed"));
    };
    let seed = issued.seed();
    ensure!(
        seed.block_id.height == floor,
        "the replay must seed from the history predecessor"
    );
    let forged_frontiers = same_sized_fixture_frontiers(&seed.frontiers)?;
    ensure!(
        forged_frontiers != seed.frontiers
            && forged_frontiers.tip_metadata() == seed.frontiers.tip_metadata(),
        "the forged seed must hold other commitments at the same tree sizes"
    );
    let forged = TreeStateReplay::new(
        CommitmentTreeCheckpoint::new(seed.block_id, seed.block_time_seconds, forged_frontiers),
        height,
    )
    .ok_or_else(|| eyre!("the forged seed lies below the replayed height"))?;
    let expected = commitment_tree_checkpoints(&chain, &activations, &[height])?
        .pop()
        .ok_or_else(|| eyre!("the chain has trees at height {}", height.value()))?;

    let outcome = reader
        .replay_tree_state(forged)
        .await?
        .ok_or_else(|| eyre!("the replayed height is retained"))?;
    assert_eq!(outcome.tree_state(), &ServedTreeState::from(&expected));
    if let TreeStateReplayOutcome::Replayed { replay, .. } = &outcome {
        assert_eq!(replay.seed(), seed, "the reader replays from its own seed");
    }
    Ok(())
}

pub(crate) async fn subtree_roots_answer_the_completed_range<H: ReaderHarness>() -> eyre::Result<()>
{
    let activations = sample_regtest_upgrade_activations();
    let completing_height = activations
        .activation_height_by_name(ShieldedProtocol::Ironwood.activation_upgrade_name())
        .ok_or_else(|| eyre!("regtest activations must activate Ironwood"))?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(completing_height.value());
    let completing_block = fixture_block(&chain, completing_height.value())?;
    let chain = chain
        .with_compact_block_artifact(CompactBlockArtifact::empty(
            BlockId::new(completing_block.height, completing_block.hash),
            completing_block.parent_hash,
            completing_block.block_time_seconds,
            CompactChainMetadata {
                sapling_commitment_tree_size: SUBTREE_LEAF_COUNT,
                orchard_commitment_tree_size: SUBTREE_LEAF_COUNT,
                ironwood_commitment_tree_size: SUBTREE_LEAF_COUNT,
            },
        ))
        .with_commitment_tree_frontiers_at(
            completing_height,
            CommitmentTreeFrontiers::from_validated_parts(
                Some(completed_subtree_frontier(ShieldedProtocol::Sapling)?),
                Some(completed_subtree_frontier(ShieldedProtocol::Orchard)?),
                Some(completed_subtree_frontier(ShieldedProtocol::Ironwood)?),
            )?,
        );
    let mut baseline = CanonicalBaseline::new(&chain);
    for (protocol, root_byte) in [
        (ShieldedProtocol::Sapling, 0x17),
        (ShieldedProtocol::Orchard, 0x18),
        (ShieldedProtocol::Ironwood, 0x19),
    ] {
        baseline = baseline.with_subtree_root(CanonicalBuildSubtreeRoot {
            protocol,
            subtree_index: SubtreeRootIndex::new(0),
            root_hash: SubtreeRootHash::from_bytes([root_byte; 32]),
            completing_block_height: completing_height,
        });
    }
    let reader = published::<H>(&baseline)?;

    let roots = reader
        .subtree_roots(SubtreeRootRange::new(
            ShieldedProtocol::Orchard,
            SubtreeRootIndex::new(0),
            NonZeroU32::MIN,
        ))
        .await?;
    let [root] = roots.as_slice() else {
        return Err(eyre!("one Orchard root must be served, got {roots:?}"));
    };
    assert_eq!(root.protocol, ShieldedProtocol::Orchard);
    assert_eq!(root.subtree_index, SubtreeRootIndex::new(0));
    assert_eq!(root.root_hash, SubtreeRootHash::from_bytes([0x18; 32]));
    assert_eq!(root.completing_block_height, completing_height);
    assert_eq!(root.completing_block_hash, completing_block.hash);
    Ok(())
}

pub(crate) async fn a_subtree_root_below_retained_history_is_unavailable<H: ReaderHarness>()
-> eyre::Result<()> {
    // A store whose retained history starts after the block that completed a
    // subtree does not hold that subtree's root.
    let (chain, completing_height) =
        chain_completing_subtrees(&sample_regtest_upgrade_activations())?;
    let reader =
        published::<H>(&CanonicalBaseline::new(&chain).with_history_floor(completing_height))?;
    assert_eq!(
        reader
            .chain_epoch()
            .tip_metadata
            .completed_subtree_count(ShieldedProtocol::Sapling),
        1
    );
    match reader
        .subtree_roots(SubtreeRootRange::new(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            NonZeroU32::MIN,
        ))
        .await
    {
        Err(StorageReadError::ArtifactUnavailable {
            family: ArtifactFamily::SubtreeRoot,
            key:
                ArtifactKey::SubtreeRootIndex {
                    protocol: ShieldedProtocol::Sapling,
                    index,
                },
        }) => assert_eq!(index, SubtreeRootIndex::new(0)),
        other => {
            return Err(eyre!(
                "an uncovered root must be unavailable, got {other:?}"
            ));
        }
    }
    Ok(())
}

/// A chain whose block at the Ironwood activation height completes one
/// subtree of every pool, followed by two blocks with no note commitments,
/// and that height.
fn chain_completing_subtrees(
    activations: &zinder_core::NetworkUpgradeActivations,
) -> eyre::Result<(ChainFixture, BlockHeight)> {
    let completing_height = activations
        .activation_height_by_name(ShieldedProtocol::Ironwood.activation_upgrade_name())
        .ok_or_else(|| eyre!("regtest activations must activate Ironwood"))?;
    let mut chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(completing_height.value().saturating_add(2));
    let completed_trees = CompactChainMetadata {
        sapling_commitment_tree_size: SUBTREE_LEAF_COUNT,
        orchard_commitment_tree_size: SUBTREE_LEAF_COUNT,
        ironwood_commitment_tree_size: SUBTREE_LEAF_COUNT,
    };
    for height in completing_height.value()..=completing_height.value().saturating_add(2) {
        let block = fixture_block(&chain, height)?;
        chain = chain.with_compact_block_artifact(CompactBlockArtifact::empty(
            BlockId::new(block.height, block.hash),
            block.parent_hash,
            block.block_time_seconds,
            completed_trees,
        ));
    }
    let chain = chain.with_commitment_tree_frontiers_at(
        completing_height,
        CommitmentTreeFrontiers::from_validated_parts(
            Some(completed_subtree_frontier(ShieldedProtocol::Sapling)?),
            Some(completed_subtree_frontier(ShieldedProtocol::Orchard)?),
            Some(completed_subtree_frontier(ShieldedProtocol::Ironwood)?),
        )?,
    );
    Ok((chain, completing_height))
}

pub(crate) async fn chain_event_pages_are_bounded_and_resume_after_their_cursor<
    H: ReaderHarness,
>() -> eyre::Result<()> {
    let (harness, chain) = appended::<H>(2)?;
    let mut harness = harness;
    let reader = harness.canonical_reader()?;

    let events = reader
        .wallet_chain_event_history(None, ChainEventStreamFamily::Visible, MANY_EVENTS)
        .await?;
    ensure!(
        events.len() >= 3,
        "the baseline and two appends must each record an event, got {}",
        events.len()
    );
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].event_sequence < pair[1].event_sequence),
        "events must come in sequence order"
    );
    let last = events
        .last()
        .ok_or_else(|| eyre!("the history must not be empty"))?;
    assert_eq!(last.chain_epoch, reader.chain_epoch());
    assert_eq!(
        last.chain_epoch.visible_tip_height,
        BlockHeight::new(chain_tip(&chain)?)
    );

    let mut paged = Vec::new();
    let mut cursor = None;
    loop {
        let page = reader
            .wallet_chain_event_history(
                cursor.clone(),
                ChainEventStreamFamily::Visible,
                NonZeroU32::MIN,
            )
            .await?;
        ensure!(page.len() <= 1, "a page must hold at most its bound");
        let Some(event) = page.into_iter().next() else {
            break;
        };
        cursor = Some(event.cursor.clone());
        paged.push(event);
    }
    assert_eq!(paged, events);
    Ok(())
}

pub(crate) async fn chain_event_starts_resolve_to_a_resume_position<H: ReaderHarness>()
-> eyre::Result<()> {
    let (mut harness, _) = appended::<H>(1)?;
    let reader = harness.canonical_reader()?;
    let events = reader
        .wallet_chain_event_history(None, ChainEventStreamFamily::Visible, MANY_EVENTS)
        .await?;
    let first = events
        .first()
        .ok_or_else(|| eyre!("the history must not be empty"))?;
    let last = events
        .last()
        .ok_or_else(|| eyre!("the history must not be empty"))?;

    let earliest = reader
        .resolve_wallet_chain_event_stream_start(
            EventStreamStartPosition::EarliestRetained,
            ChainEventStreamFamily::Visible,
        )
        .await?;
    assert_eq!(earliest.family, ChainEventStreamFamily::Visible);
    assert_eq!(
        reader
            .wallet_chain_event_history(earliest.cursor, earliest.family, MANY_EVENTS)
            .await?,
        events
    );

    let live_tail = reader
        .resolve_wallet_chain_event_stream_start(
            EventStreamStartPosition::LiveTail,
            ChainEventStreamFamily::Visible,
        )
        .await?;
    assert!(
        reader
            .wallet_chain_event_history(live_tail.cursor, live_tail.family, MANY_EVENTS)
            .await?
            .is_empty(),
        "the live tail resumes after every retained event"
    );

    let after_first = reader
        .resolve_wallet_chain_event_stream_start(
            EventStreamStartPosition::AfterCursor(first.cursor.clone()),
            ChainEventStreamFamily::Visible,
        )
        .await?;
    assert_eq!(after_first.family, ChainEventStreamFamily::Visible);
    let resumed = reader
        .wallet_chain_event_history(after_first.cursor, after_first.family, MANY_EVENTS)
        .await?;
    assert_eq!(resumed.first(), events.get(1));
    assert_eq!(resumed.last(), Some(last));

    let other_family = reader
        .resolve_wallet_chain_event_stream_start(
            EventStreamStartPosition::AfterCursor(first.cursor.clone()),
            ChainEventStreamFamily::Settled,
        )
        .await;
    assert!(
        matches!(
            other_family,
            Err(StorageReadError::ChainEventCursorInvalid { .. })
        ),
        "a cursor resumes only the family it was issued for, got {other_family:?}"
    );
    Ok(())
}

pub(crate) async fn the_settled_stream_commits_each_newly_settled_range<H: ReaderHarness>()
-> eyre::Result<()> {
    let chain = chain(RawBlobRetention::Transactions).extend_blocks(2);
    let mut harness = H::publish(
        &CanonicalBaseline::new(&chain)
            .with_history_floor(BlockHeight::new(1))
            .through(BlockHeight::new(TIP))
            .with_settled_tip(BlockHeight::new(3)),
        &sample_regtest_upgrade_activations(),
    )?;
    harness.append_live_block(&chain, block_id(&chain, 3)?)?;
    let tail = harness
        .canonical_reader()?
        .resolve_wallet_chain_event_stream_start(
            EventStreamStartPosition::LiveTail,
            ChainEventStreamFamily::Settled,
        )
        .await?;
    assert!(tail.cursor.is_some());
    assert_eq!(tail.family, ChainEventStreamFamily::Settled);
    harness.append_live_block(&chain, block_id(&chain, TIP)?)?;
    let reader = harness.canonical_reader()?;
    let visible = reader
        .wallet_chain_event_history(None, ChainEventStreamFamily::Visible, MANY_EVENTS)
        .await?;
    let [baseline, held, advanced] = visible.as_slice() else {
        return Err(eyre!("expected the baseline and two visible appends"));
    };
    assert_eq!(committed_range(baseline)?, range(2, TIP));
    assert_eq!(committed_range(held)?, range(TIP + 1, TIP + 1));
    assert_eq!(committed_range(advanced)?, range(TIP + 2, TIP + 2));
    let settled = reader
        .wallet_chain_event_history(None, ChainEventStreamFamily::Settled, MANY_EVENTS)
        .await?;
    let [prefix, extension] = settled.as_slice() else {
        return Err(eyre!(
            "expected the settled baseline and one settlement advance"
        ));
    };
    for (event, transition, expected) in [
        (prefix, baseline, range(2, 3)),
        (extension, advanced, range(4, TIP)),
    ] {
        assert_eq!(committed_range(event)?, expected);
        assert_eq!(event.event_sequence, transition.event_sequence);
        assert_eq!(event.chain_epoch, transition.chain_epoch);
        assert_eq!(event.settled_tip_height, expected.end);
    }
    harness.prune_chain_events_before(advanced.event_sequence)?;
    let resumed = harness
        .canonical_reader()?
        .wallet_chain_event_history(tail.cursor, tail.family, NonZeroU32::MIN)
        .await?;
    assert_eq!(resumed.as_slice(), std::slice::from_ref(extension));
    Ok(())
}

pub(crate) async fn a_settled_cursor_resumes_with_the_default_family<H: ReaderHarness>()
-> eyre::Result<()> {
    let (mut harness, _) = appended::<H>(1)?;
    let reader = harness.canonical_reader()?;
    let settled = reader
        .wallet_chain_event_history(None, ChainEventStreamFamily::Settled, MANY_EVENTS)
        .await?;
    let first = settled
        .first()
        .ok_or_else(|| eyre!("the settled baseline commit must be in the settled stream"))?;
    ensure!(
        reader
            .wallet_chain_event_history(None, ChainEventStreamFamily::Visible, MANY_EVENTS)
            .await?
            .iter()
            .any(|event| event.event_sequence > first.event_sequence),
        "the visible stream must carry an event after the settled cursor"
    );

    for requested_family in [
        ChainEventStreamFamily::Visible,
        ChainEventStreamFamily::Settled,
    ] {
        let resume = reader
            .resolve_wallet_chain_event_stream_start(
                EventStreamStartPosition::AfterCursor(first.cursor.clone()),
                requested_family,
            )
            .await?;
        assert_eq!(resume.family, ChainEventStreamFamily::Settled);
        assert_eq!(resume.cursor.as_ref(), Some(&first.cursor));
    }
    // The cursor's family applies, so the default family serves no
    // unsettled commit after it.
    let resumed = reader
        .wallet_chain_event_history(
            Some(first.cursor.clone()),
            ChainEventStreamFamily::Visible,
            MANY_EVENTS,
        )
        .await?;
    assert_eq!(resumed, settled.get(1..).unwrap_or_default());
    Ok(())
}

pub(crate) async fn pruned_and_malformed_chain_event_cursors_are_refused<H: ReaderHarness>()
-> eyre::Result<()> {
    let (mut harness, _) = appended::<H>(2)?;
    let first_reader = harness.canonical_reader()?;
    let events = first_reader
        .wallet_chain_event_history(None, ChainEventStreamFamily::Visible, MANY_EVENTS)
        .await?;
    let first = events
        .first()
        .ok_or_else(|| eyre!("the history must not be empty"))?;
    let last = events
        .last()
        .ok_or_else(|| eyre!("the history must not be empty"))?;

    harness.prune_chain_events_before(last.event_sequence)?;
    let reader = harness.canonical_reader()?;
    match reader
        .wallet_chain_event_history(
            Some(first.cursor.clone()),
            ChainEventStreamFamily::Visible,
            MANY_EVENTS,
        )
        .await
    {
        Err(StorageReadError::ChainEventCursorExpired {
            event_sequence,
            oldest_retained_sequence,
        }) => {
            assert_eq!(event_sequence, first.event_sequence);
            assert_eq!(oldest_retained_sequence, last.event_sequence);
        }
        other => return Err(eyre!("a pruned cursor must be expired, got {other:?}")),
    }

    let malformed = StreamCursorTokenV1::from_bytes(vec![0; 7]);
    for outcome in [
        reader
            .wallet_chain_event_history(
                Some(malformed.clone()),
                ChainEventStreamFamily::Visible,
                MANY_EVENTS,
            )
            .await
            .map(drop),
        reader
            .resolve_wallet_chain_event_stream_start(
                EventStreamStartPosition::AfterCursor(malformed.clone()),
                ChainEventStreamFamily::Visible,
            )
            .await
            .map(drop),
    ] {
        assert!(
            matches!(
                outcome,
                Err(StorageReadError::ChainEventCursorInvalid { .. })
            ),
            "a malformed cursor must be invalid, got {outcome:?}"
        );
    }
    Ok(())
}

pub(crate) async fn a_reader_keeps_its_fence_while_the_writer_appends<H: ReaderHarness>()
-> eyre::Result<()> {
    let chain = chain(RawBlobRetention::Transactions).extend_blocks(1);
    let baseline_tip = BlockHeight::new(TIP);
    let mut harness = H::publish(
        &CanonicalBaseline::new(&chain).through(baseline_tip),
        &sample_regtest_upgrade_activations(),
    )?;
    let pinned = harness.canonical_reader()?;
    let pinned_fence = pinned.event_fence();
    let pinned_epoch = pinned.chain_epoch();

    harness.append_live_block(&chain, block_id(&chain, TIP)?)?;
    let current = harness.canonical_reader()?;
    let appended = block_id(&chain, TIP + 1)?;

    assert_eq!(pinned.event_fence(), pinned_fence);
    assert_eq!(pinned.chain_epoch(), pinned_epoch);
    assert_eq!(pinned_epoch.visible_tip_height, baseline_tip);
    assert_eq!(pinned.compact_block_at(appended.height).await?, None);
    assert_unavailable_height(
        pinned.compact_blocks_in_range(range(TIP, TIP + 1)).await,
        TIP + 1,
    )?;
    assert!(
        pinned
            .wallet_chain_event_history(None, ChainEventStreamFamily::Visible, MANY_EVENTS)
            .await?
            .iter()
            .all(|event| event.chain_epoch.id <= pinned_epoch.id),
        "a reader serves no event past its fence"
    );

    assert!(current.chain_epoch().id > pinned_epoch.id);
    assert_eq!(current.event_fence().visible_tip(), appended);
    assert_eq!(
        current
            .compact_block_at(appended.height)
            .await?
            .as_ref()
            .map(block_id_of),
        Some(appended)
    );
    Ok(())
}

pub(crate) async fn a_reorg_replaces_blocks_only_for_readers_opened_after_it<H: ReaderHarness>()
-> eyre::Result<()> {
    let (chain, mined) = chain_with_mined_transaction(RawBlobRetention::Transactions)?;
    let replaced_height = BlockHeight::new(MINED_HEIGHT);
    // The writer archives each block a reorg displaces, which must start
    // with a coinbase.
    let chain = with_coinbases(chain, MINED_HEIGHT + 1..=TIP)?;
    let replacement_chain = chain
        .fork_at(replaced_height)?
        .extend_blocks(TIP - MINED_HEIGHT + 1);
    let settled = BlockHeight::new(MINED_HEIGHT - 1);
    let mut harness = H::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(settled),
        &sample_regtest_upgrade_activations(),
    )?;
    let before = harness.canonical_reader()?;
    harness.replace_live_blocks(&replacement_chain, replaced_height)?;
    let after = harness.canonical_reader()?;

    let displaced = block_id(&chain, MINED_HEIGHT)?;
    let replacement = block_id(&replacement_chain, MINED_HEIGHT)?;
    assert_ne!(displaced, replacement);
    assert!(after.chain_epoch().id > before.chain_epoch().id);
    assert_eq!(
        before
            .compact_block_at(replaced_height)
            .await?
            .as_ref()
            .map(block_id_of),
        Some(displaced)
    );
    assert_eq!(
        after
            .compact_block_at(replaced_height)
            .await?
            .as_ref()
            .map(block_id_of),
        Some(replacement)
    );
    assert_hashes_resolve_on_their_own_chain(&before, &after, displaced, replacement).await?;
    assert!(before.mined_transaction(mined).await?.is_some());
    assert_eq!(after.mined_transaction(mined).await?, None);
    assert_eq!(
        after
            .compact_blocks_in_range(range(1, chain_tip(&replacement_chain)?))
            .await?
            .iter()
            .map(block_id_of)
            .collect::<Vec<_>>(),
        (1..=chain_tip(&replacement_chain)?)
            .map(|height| block_id(&replacement_chain, height))
            .collect::<eyre::Result<Vec<_>>>()?
    );
    Ok(())
}

/// Requires a block a reorg displaced to resolve by hash only for the reader
/// opened before the reorg, and its replacement only for the one after it.
async fn assert_hashes_resolve_on_their_own_chain(
    before: &impl CanonicalReader,
    after: &impl CanonicalReader,
    displaced: BlockId,
    replacement: BlockId,
) -> eyre::Result<()> {
    let displaced_lookup = after.block_hash_lookup(displaced.hash).await?;
    assert!(
        matches!(
            displaced_lookup,
            BlockHashLookup::NotInBestChain | BlockHashLookup::NotIndexed
        ),
        "a displaced block must not resolve, got {displaced_lookup:?}"
    );
    assert_eq!(
        after.block_hash_lookup(replacement.hash).await?,
        BlockHashLookup::Resolved(replacement)
    );
    assert_eq!(
        before.block_hash_lookup(displaced.hash).await?,
        BlockHashLookup::Resolved(displaced)
    );
    assert_eq!(after.block_header_by_hash(displaced.hash).await?, None);
    assert_eq!(after.compact_block_by_hash(displaced.hash).await?, None);
    assert_eq!(
        after
            .compact_block_by_hash(replacement.hash)
            .await?
            .as_ref()
            .map(block_id_of),
        Some(replacement)
    );
    assert_eq!(
        before
            .block_header_by_hash(displaced.hash)
            .await?
            .map(|header| BlockId::new(header.height, header.block_hash)),
        Some(displaced)
    );
    Ok(())
}

pub(crate) async fn a_reorg_is_served_as_a_visible_chain_event<H: ReaderHarness>()
-> eyre::Result<()> {
    let (chain, _) = chain_with_mined_transaction(RawBlobRetention::Transactions)?;
    let replaced_height = BlockHeight::new(MINED_HEIGHT);
    let chain = with_coinbases(chain, MINED_HEIGHT + 1..=TIP)?;
    let replacement_chain = chain.fork_at(replaced_height)?.extend_blocks(1);
    let mut harness = H::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(MINED_HEIGHT - 1)),
        &sample_regtest_upgrade_activations(),
    )?;
    harness.replace_live_blocks(&replacement_chain, replaced_height)?;
    let after = harness.canonical_reader()?;

    let events = after
        .wallet_chain_event_history(None, ChainEventStreamFamily::Visible, MANY_EVENTS)
        .await?;
    let last = events
        .last()
        .ok_or_else(|| eyre!("the history must not be empty"))?;
    assert_eq!(last.chain_epoch, after.chain_epoch());
    assert!(
        matches!(last.event, ChainEvent::ChainReorged { .. }),
        "the replacement must be served as a reorg event, got {:?}",
        last.event
    );
    let settled = after
        .wallet_chain_event_history(None, ChainEventStreamFamily::Settled, MANY_EVENTS)
        .await?;
    let [baseline] = settled.as_slice() else {
        return Err(eyre!("only the baseline advanced settlement"));
    };
    assert_eq!(committed_range(baseline)?, range(1, MINED_HEIGHT - 1));
    Ok(())
}

/// Height of the block that mines the transaction the transaction cases read.
const MINED_HEIGHT: u32 = 3;
/// Raw bytes of that transaction.
const MINED_BYTES: &[u8] = b"conformance-mined-transaction";

/// A [`TIP`]-block chain that retains `raw_blob_retention`.
fn chain(raw_blob_retention: RawBlobRetention) -> ChainFixture {
    ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(raw_blob_retention)
        .extend_blocks(TIP)
}

/// A [`TIP`]-block chain whose block [`MINED_HEIGHT`] mines a coinbase and,
/// at index 1, the returned transaction.
fn chain_with_mined_transaction(
    raw_blob_retention: RawBlobRetention,
) -> eyre::Result<(ChainFixture, TransactionId)> {
    let chain = chain(raw_blob_retention);
    let block = fixture_block(&chain, MINED_HEIGHT)?;
    let mined = TransactionId::from_bytes([0x5a; 32]);
    let chain = chain
        .with_transaction_rows(FixtureTransactionRows::coinbase(
            TransactionId::from_bytes([0x5c; 32]),
            block.height,
            block.hash,
            b"conformance-coinbase".to_vec(),
        ))
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            mined,
            block.height,
            block.hash,
            1,
            MINED_BYTES.to_vec(),
        ));
    Ok((chain, mined))
}

/// Mines a coinbase in each block of `heights`.
fn with_coinbases(
    mut chain: ChainFixture,
    heights: std::ops::RangeInclusive<u32>,
) -> eyre::Result<ChainFixture> {
    for height in heights {
        let block = fixture_block(&chain, height)?;
        let mut coinbase_id = [0x5d; 32];
        coinbase_id[28..].copy_from_slice(&height.to_be_bytes());
        chain = chain.with_transaction_rows(FixtureTransactionRows::coinbase(
            TransactionId::from_bytes(coinbase_id),
            block.height,
            block.hash,
            format!("conformance-coinbase-{height}").into_bytes(),
        ));
    }
    Ok(chain)
}

/// Publishes `baseline` with the sample activations and opens a reader.
fn published<H: ReaderHarness>(baseline: &CanonicalBaseline<'_>) -> eyre::Result<H::Canonical> {
    H::publish(baseline, &sample_regtest_upgrade_activations())?.canonical_reader()
}

/// Publishes a [`TIP`]-block baseline and appends `appended` live blocks.
fn appended<H: ReaderHarness>(appended: u32) -> eyre::Result<(H, ChainFixture)> {
    let chain = chain(RawBlobRetention::Transactions).extend_blocks(appended);
    let mut harness = H::publish(
        &CanonicalBaseline::new(&chain).through(BlockHeight::new(TIP)),
        &sample_regtest_upgrade_activations(),
    )?;
    let settled_tip = block_id(&chain, TIP)?;
    for _ in 0..appended {
        harness.append_live_block(&chain, settled_tip)?;
    }
    Ok((harness, chain))
}

fn committed_range(event: &ChainEventEnvelope) -> eyre::Result<BlockHeightRange> {
    let ChainEvent::ChainCommitted { committed } = &event.event else {
        return Err(eyre!("expected a commit, got {:?}", event.event));
    };
    assert_eq!(committed.chain_epoch, event.chain_epoch);
    Ok(committed.block_range)
}

/// Frontiers of the sizes `frontiers` holds whose leaves are all the
/// testkit's fixture commitment, so a non-empty one differs from a real
/// tree's.
fn same_sized_fixture_frontiers(
    frontiers: &CommitmentTreeFrontiers,
) -> eyre::Result<CommitmentTreeFrontiers> {
    let forge = |frontier: Option<&CommitmentTreeFrontier>| {
        frontier
            .map(|frontier| match frontier.tree_size() {
                0 => Ok(frontier.clone()),
                tree_size => frontier_of_size(frontier.protocol(), tree_size),
            })
            .transpose()
    };
    Ok(CommitmentTreeFrontiers::from_validated_parts(
        forge(frontiers.sapling())?,
        forge(frontiers.orchard())?,
        forge(frontiers.ironwood())?,
    )?)
}

fn never_mined() -> TransactionId {
    TransactionId::from_bytes([0x99; 32])
}

fn fixture_block(chain: &ChainFixture, height: u32) -> eyre::Result<FixtureBlock> {
    chain
        .block_at(BlockHeight::new(height))
        .cloned()
        .ok_or_else(|| eyre!("the fixture has no block at height {height}"))
}

fn block_id(chain: &ChainFixture, height: u32) -> eyre::Result<BlockId> {
    let block = fixture_block(chain, height)?;
    Ok(BlockId::new(block.height, block.hash))
}

fn chain_tip(chain: &ChainFixture) -> eyre::Result<u32> {
    chain
        .tip_height()
        .map(BlockHeight::value)
        .ok_or_else(|| eyre!("the fixture has no blocks"))
}

fn block_id_of(block: &CompactBlockArtifact) -> BlockId {
    BlockId::new(block.height(), block.block_hash())
}

fn heights(blocks: &[CompactBlockArtifact]) -> Vec<u32> {
    blocks.iter().map(|block| block.height().value()).collect()
}

const fn range(start: u32, end: u32) -> BlockHeightRange {
    BlockHeightRange::inclusive(BlockHeight::new(start), BlockHeight::new(end))
}

fn usize_of(count: u32) -> usize {
    usize::try_from(count).unwrap_or(usize::MAX)
}

/// Requires `outcome` to fail as an unavailable compact block at `height`.
fn assert_unavailable_height<Output: std::fmt::Debug>(
    outcome: Result<Output, StorageReadError>,
    height: u32,
) -> eyre::Result<()> {
    match outcome {
        Err(StorageReadError::ArtifactUnavailable {
            family: ArtifactFamily::CompactBlock,
            key: ArtifactKey::BlockHeight(unavailable),
        }) if unavailable == BlockHeight::new(height) => Ok(()),
        other => Err(eyre!(
            "expected compact block {height} to be unavailable, got {other:?}"
        )),
    }
}

/// Requires `outcome` to fail as an unavailable `family` artifact for
/// `transaction_id`.
fn assert_transaction_unavailable<Output: std::fmt::Debug>(
    outcome: Result<Output, StorageReadError>,
    family: ArtifactFamily,
    transaction_id: TransactionId,
) -> eyre::Result<()> {
    match outcome {
        Err(StorageReadError::ArtifactUnavailable {
            family: unavailable_family,
            key: ArtifactKey::TransactionId(unavailable),
        }) if unavailable_family == family && unavailable == transaction_id => Ok(()),
        other => Err(eyre!(
            "expected the {family:?} of the transaction to be unavailable, got {other:?}"
        )),
    }
}
