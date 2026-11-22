use std::{num::NonZeroU32, sync::Arc, time::Duration};

use super::super::TreeStateReplayLimit;
use super::super::settled_cache::{
    LookupStep, ReaderGeneration, SETTLED_MAX_ENTRIES, SettledStates,
};
use super::{ReadyState, ReplayKey};
use crate::{
    CanonicalReader, RocksDbCanonicalReader, StorageReadError,
    observed_canonical_reader::ObservedCanonicalReader,
};
use futures_util::FutureExt as _;
use zinder_core::{BlockHash, BlockHeight, CanonicalHistoryBounds, Network};
use zinder_store::TreeStateLookup;
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

#[tokio::test]
async fn only_settled_rows_at_the_same_retained_fence_survive_eviction() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut store =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let reader = Arc::new(RocksDbCanonicalReader::new(store.take_readers()?.0)?);
    let weak: std::sync::Weak<RocksDbCanonicalReader> = Arc::downgrade(&reader);
    let generation = ReaderGeneration::static_for_test(&weak, true);
    let height = reader.chain_epoch().visible_tip_height;
    let Some(TreeStateLookup::Stored(state)) = reader.tree_state_lookup(height).await? else {
        return Err(eyre::eyre!("missing tip checkpoint"));
    };
    let mut key = ReplayKey::new(height, reader.as_ref());
    key.epoch.settled_tip_height = BlockHeight::new(u32::try_from(SETTLED_MAX_ENTRIES)?);
    let mut cache = SettledStates::default();
    for number in 0..=key.epoch.settled_tip_height.value() {
        key.height = BlockHeight::new(number);
        let entry_state = served_for_height(&state, key.height)?;
        cache.remember(key.settled_anchor(), &generation, &entry_state);
    }
    assert!(matches!(
        cache.classify_lookup(key.settled_anchor(), &generation, key.height),
        LookupStep::Hit(_)
    ));
    assert!(matches!(
        cache.classify_lookup(key.settled_anchor(), &generation, BlockHeight::new(0)),
        LookupStep::Miss
    ));

    // A visible-tip append at the same settled tip reuses the settled cache without re-anchoring.
    let mut appended = key;
    appended.height = BlockHeight::new(1);
    appended.epoch.visible_tip_height =
        BlockHeight::new(appended.epoch.visible_tip_height.value() + 1);
    appended.epoch.visible_tip_hash = BlockHash::from_bytes([24; 32]);
    assert!(matches!(
        cache.classify_lookup(appended.settled_anchor(), &generation, appended.height),
        LookupStep::Hit(_)
    ));

    let mut fork = key;
    fork.height = BlockHeight::new(1);
    fork.epoch.settled_tip_hash = BlockHash::from_bytes([23; 32]);
    assert!(matches!(
        cache.classify_lookup(fork.settled_anchor(), &generation, fork.height),
        LookupStep::Miss
    ));
    assert!(matches!(
        cache.classify_lookup(
            ReplayKey {
                schedule_revision: None,
                ..key
            }
            .settled_anchor(),
            &generation,
            key.height
        ),
        LookupStep::Miss
    ));
    let pruned = ReplayKey {
        retained: CanonicalHistoryBounds::checkpointed(state.block_id())?,
        ..key
    };
    assert!(matches!(
        cache.classify_lookup(pruned.settled_anchor(), &generation, pruned.height),
        LookupStep::Miss
    ));
    key.height = BlockHeight::new(key.epoch.settled_tip_height.value() + 1);
    cache.remember(key.settled_anchor(), &generation, &state);
    assert!(
        cache
            .cached_at_anchor(key.settled_anchor(), key.height)
            .is_none()
    );
    Ok(())
}

fn served_for_height(
    template: &zinder_store::ServedTreeState,
    height: BlockHeight,
) -> eyre::Result<zinder_store::ServedTreeState> {
    let sapling =
        zinder_core::CommitmentTreeFrontier::empty(zinder_core::ShieldedProtocol::Sapling);
    let checkpoint = zinder_core::CommitmentTreeCheckpoint::new(
        zinder_core::BlockId::new(height, template.block_id().hash),
        height.value(),
        zinder_core::CommitmentTreeFrontiers::from_validated_parts(Some(sapling), None, None)?,
    );
    Ok(zinder_store::ServedTreeState::from(checkpoint))
}

fn replay_key() -> eyre::Result<ReplayKey> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut store =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    Ok(ReplayKey::new(BlockHeight::new(2), &reader))
}

#[tokio::test(start_paused = true)]
async fn queued_subscribers_keep_their_place_and_deadline_after_the_first_wallet_leaves()
-> eyre::Result<()> {
    let key = replay_key()?;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(4))
        .with_waiters_per_slot(NonZeroU32::MIN.saturating_add(2));
    let held = limit.admit().await?;
    let generation = Arc::downgrade(&Arc::new(()));
    let deadline = limit.deadline();
    let first = limit.subscribe(&key, &generation, deadline)?;
    assert!(first.admit(&limit).now_or_never().is_none());
    let other = limit.subscribe(
        &ReplayKey {
            height: BlockHeight::new(1),
            ..key
        },
        &generation,
        deadline,
    )?;
    assert!(other.admit(&limit).now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(2)).await;
    let follower = limit.subscribe(&key, &generation, limit.deadline())?;
    assert!(
        limit
            .subscribe(&key, &generation, limit.deadline())
            .is_err()
    );
    drop(first);
    assert_eq!(limit.waiter_count(), 2);
    let recovered = limit.subscribe(
        &ReplayKey {
            height: BlockHeight::new(3),
            ..key
        },
        &generation,
        deadline,
    )?;
    assert_eq!(limit.waiter_count(), 3);
    drop(recovered);
    drop(held);
    let slot = follower
        .admit(&limit)
        .now_or_never()
        .ok_or_else(|| eyre::eyre!("follower lost the first queue position"))??;
    assert!(other.admit(&limit).now_or_never().is_none());
    drop(slot);
    drop(follower);
    other.admit(&limit).await?;
    assert_eq!(limit.waiter_count(), 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn refused_flights_can_be_retried_while_an_old_subscriber_still_holds_the_failure()
-> eyre::Result<()> {
    let key = replay_key()?;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(4));
    let held = limit.admit().await?;
    let generation = Arc::downgrade(&Arc::new(()));
    let deadline = limit.deadline();
    let first = limit.subscribe(&key, &generation, deadline)?;
    assert!(first.admit(&limit).now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(2)).await;
    let follower = limit.subscribe(&key, &generation, limit.deadline())?;
    drop(first);
    assert!(follower.admit(&limit).await.is_err());
    assert_eq!(tokio::time::Instant::now(), deadline);
    drop(held);
    let retried = limit.subscribe(&key, &generation, limit.deadline())?;
    retried.admit(&limit).await?;
    assert!(!Arc::ptr_eq(&follower.flight, &retried.flight));
    Ok(())
}

#[tokio::test]
async fn same_fence_readers_cannot_share_an_invalidation_and_completed_work_frees_its_slot()
-> eyre::Result<()> {
    use crate::StorageReadError;
    let key = replay_key()?;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO)
        .with_waiters_per_slot(NonZeroU32::MIN);
    let generation = Arc::downgrade(&Arc::new(()));
    let replacement = Arc::downgrade(&Arc::new(()));
    let first = limit.subscribe(&key, &generation, limit.deadline())?;
    let slot = first.admit(&limit).await?;
    assert!(
        limit
            .subscribe(&key, &replacement, limit.deadline())
            .is_err()
    );
    let follower = limit.subscribe(&key, &generation, limit.deadline())?;
    let delayed = follower.admit(&limit).await?;
    assert!(
        limit
            .subscribe(&key, &generation, limit.deadline())
            .is_err()
    );
    let failure = first
        .replay(slot, |_| async {
            Err(StorageReadError::ReaderInvalidated {
                source: "retired reader".into(),
            })
        })
        .await;
    assert!(failure.is_err());
    let fresh = limit.subscribe(&key, &replacement, limit.deadline())?;
    let available = fresh.admit(&limit).await?;
    assert!(!Arc::ptr_eq(&first.flight, &fresh.flight));
    assert!(available().is_some());
    assert!(delayed().is_none());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn replacement_flights_only_wait_until_the_original_request_deadline() -> eyre::Result<()> {
    let key = replay_key()?;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(4));
    let held = limit.admit().await?;
    let generation = Arc::downgrade(&Arc::new(()));
    let deadline = limit.deadline();
    let first = limit.subscribe(&key, &generation, deadline)?;
    assert!(first.admit(&limit).now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(3)).await;
    drop(held);
    first.admit(&limit).await?;
    drop(first);
    let other_wallet = limit.admit().await?;
    let replacement = Arc::downgrade(&Arc::new(()));
    let second = limit.subscribe(&key, &replacement, deadline)?;
    assert!(second.admit(&limit).await.is_err());
    assert_eq!(tokio::time::Instant::now(), deadline);
    drop(other_wallet);
    let third = limit.subscribe(&key, &replacement, deadline)?;
    third.admit(&limit).await?;
    Ok(())
}

#[tokio::test]
async fn selection_reuses_a_settled_answer_completed_after_the_checkpoint_miss() -> eyre::Result<()>
{
    use super::{Selection, read_missing_height};
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut store =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let reader = Arc::new(RocksDbCanonicalReader::new(store.take_readers()?.0)?);
    let height = BlockHeight::new(2);
    let key = ReplayKey::new(height, reader.as_ref());
    let generation = Arc::downgrade(&reader);
    let cache_generation = ReaderGeneration::static_for_test(&generation, true);
    let canonical_reader = Arc::clone(&reader) as Arc<dyn CanonicalReader>;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
    assert!(
        limit
            .ready_state(&key, &cache_generation, &canonical_reader, limit.deadline())
            .await?
            .is_none()
    );
    let first = limit.subscribe(&key, &generation, limit.deadline())?;
    let slot = first.admit(&limit).await?;
    let state = first
        .replay(slot, |cancelled| async move {
            read_missing_height(reader.as_ref(), height, cancelled).await
        })
        .await?
        .ok_or_else(|| eyre::eyre!("missing reconstructed state"))?;
    drop(first);
    let _occupied = limit.admit().await?;
    let Selection::Ready(cached) = limit.select(&key, &cache_generation, limit.deadline(), None)?
    else {
        return Err(eyre::eyre!("settled state started a second reconstruction"));
    };
    assert_eq!(cached.serve(), state);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn an_admitted_wallet_supplies_its_slot_to_a_waiting_replacement_without_losing_its_turn()
-> eyre::Result<()> {
    use super::Selection;
    let key = replay_key()?;
    let old = Arc::downgrade(&Arc::new(()));
    let new = Arc::downgrade(&Arc::new(()));
    let new_cache = ReaderGeneration::static_for_test(&new, true);
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(4));
    let deadline = limit.deadline();
    let occupied = limit.admit().await?;
    let first = limit.subscribe(&key, &old, deadline)?;
    assert!(first.admit(&limit).now_or_never().is_none());
    let later_key = ReplayKey {
        height: BlockHeight::new(1),
        ..key
    };
    let later = limit.subscribe(&later_key, &new, deadline)?;
    assert!(later.admit(&limit).now_or_never().is_none());
    let previous = limit.subscribe(&key, &new, deadline)?;
    assert!(previous.admit(&limit).now_or_never().is_none());
    drop(occupied);
    let carried = first.carry(&first.admit(&limit).await?);
    let Selection::Waiting(moved) = limit.select(&key, &new_cache, deadline, carried)? else {
        return Err(eyre::eyre!("unexpected cached state"));
    };
    assert!(Arc::ptr_eq(&moved.flight, &previous.flight));
    assert!(moved.admit(&limit).now_or_never().transpose()?.is_some());
    assert!(later.admit(&limit).now_or_never().is_none());
    let slot = previous.admit(&limit).await?;
    assert!(slot().is_some());
    later.admit(&limit).await?;
    Ok(())
}

#[test]
fn a_cancelled_blocking_seed_lookup_releases_capacity_without_validating_its_row()
-> eyre::Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut store =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?;
    runtime.block_on(async {
        let (release, held) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || held.recv());
        let cancelled = Arc::new(AtomicBool::new(false));
        let lookup =
            reader.tree_state_lookup_until_cancelled(BlockHeight::new(2), cancelled.clone());
        tokio::pin!(lookup);
        assert!(futures_util::poll!(&mut lookup).is_pending());
        cancelled.store(true, Ordering::Relaxed);
        release.send(())?;
        blocker.await??;
        assert!(lookup.await.is_err());
        assert!(matches!(
            reader.tree_state_lookup(BlockHeight::new(2)).await?,
            Some(TreeStateLookup::Replay(_))
        ));
        Ok(())
    })
}

#[tokio::test]
async fn a_full_running_flight_refuses_a_carried_join_and_frees_its_unused_slot() -> eyre::Result<()>
{
    let key = replay_key()?;
    let destination = Arc::downgrade(&Arc::new(()));
    let original = Arc::downgrade(&Arc::new(()));
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN.saturating_add(1), Duration::ZERO)
        .with_waiters_per_slot(NonZeroU32::MIN);
    let first = limit.subscribe(&key, &destination, limit.deadline())?;
    let slot = first.admit(&limit).await?;
    let (finish, wait) = tokio::sync::oneshot::channel::<()>();
    let work = first.replay(slot, |_| async move {
        let _ = wait.await;
        Ok(None)
    });
    tokio::pin!(work);
    assert!(futures_util::poll!(&mut work).is_pending());
    let _subscriber = limit.subscribe(&key, &destination, limit.deadline())?;
    let _another = limit.subscribe(&key, &destination, limit.deadline())?;
    let previous = limit.subscribe(&key, &original, limit.deadline())?;
    let carried = previous.carry(&previous.admit(&limit).await?);
    assert!(carried.is_some());
    assert!(matches!(
        limit.select(
            &key,
            &ReaderGeneration::static_for_test(&destination, true),
            limit.deadline(),
            carried
        ),
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));
    assert_eq!(limit.waiter_count(), 2);
    let unrelated = limit.subscribe(
        &ReplayKey {
            height: BlockHeight::new(1),
            ..key
        },
        &destination,
        limit.deadline(),
    )?;
    assert!(
        unrelated
            .admit(&limit)
            .now_or_never()
            .transpose()?
            .is_some()
    );
    finish
        .send(())
        .map_err(|()| eyre::eyre!("worker ended early"))?;
    assert!(matches!(
        work.await,
        Err(crate::QueryError::BlockNotInBestChain)
    ));
    Ok(())
}

#[tokio::test]
async fn selection_preserves_its_incoming_slot_while_the_destination_is_moving() -> eyre::Result<()>
{
    use super::{Selection, TakeReplayPermit};
    use parking_lot::Mutex;

    let key = replay_key()?;
    let destination = Arc::downgrade(&Arc::new(()));
    let donor = Arc::downgrade(&Arc::new(()));
    let limit =
        TreeStateReplayLimit::new(NonZeroU32::MIN.saturating_add(1), Duration::from_secs(4));
    let first = limit.subscribe(&key, &destination, limit.deadline())?;
    let old_flight = first.flight.clone();
    let slot = first.admit(&limit).await?;
    let second = limit.subscribe(&key, &donor, limit.deadline())?;
    let incoming = second.carry(&second.admit(&limit).await?);
    assert!(incoming.is_some());
    let unrelated = limit.admit();
    tokio::pin!(unrelated);
    assert!(futures_util::poll!(&mut unrelated).is_pending());

    let (entered, entered_move) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (resume, pause) = std::sync::mpsc::channel();
    let pause = Mutex::new(pause);
    let paused_slot: TakeReplayPermit = Arc::new(move || {
        entered.lock().take()?.send(()).ok()?;
        pause.lock().recv().ok()?;
        slot()
    });
    let moving = tokio::task::spawn_blocking(move || first.carry(&paused_slot));
    tokio::time::timeout(Duration::from_secs(3), entered_move).await??;
    let selecting = tokio::task::spawn_blocking({
        let limit = limit.clone();
        let dest_cache = ReaderGeneration::static_for_test(&destination, true);
        move || limit.select(&key, &dest_cache, limit.deadline(), incoming)
    });
    let selected_candidate = tokio::time::timeout(Duration::from_secs(3), async {
        while limit.flights.try_lock().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    resume.send(())?;
    selected_candidate?;
    let _outgoing = moving
        .await?
        .ok_or_else(|| eyre::eyre!("first permit did not move"))?;
    let Selection::Waiting(replacement) = selecting.await?? else {
        return Err(eyre::eyre!("unexpected cached state"));
    };
    assert!(!Arc::ptr_eq(&replacement.flight, &old_flight));
    assert!(
        replacement
            .admit(&limit)
            .now_or_never()
            .transpose()?
            .is_some()
    );
    assert!(futures_util::poll!(&mut unrelated).is_pending());
    Ok(())
}

#[tokio::test]
async fn a_carried_slot_wins_when_the_queue_refuses_in_the_same_poll() -> eyre::Result<()> {
    use super::{Admission, ReplayRefusal, transferable};
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
    let permit = transferable(limit.admit().await?);
    let (publish, contender) = tokio::sync::oneshot::channel::<std::sync::Weak<Admission>>();
    let admission = Arc::new(Admission::queued(async move {
        let active = contender
            .await
            .ok()
            .and_then(|weak| weak.upgrade())
            .ok_or(ReplayRefusal::WaitExpired)?;
        let mut carried = Some(permit);
        assert!(active.accept_carried(&mut carried));
        assert!(carried.is_none());
        Err(ReplayRefusal::WaitExpired)
    }));
    publish
        .send(Arc::downgrade(&admission))
        .map_err(|_| eyre::eyre!("admission ended early"))?;
    let slot = admission
        .slot
        .clone()
        .await
        .map_err(|_| eyre::eyre!("lost transferred slot"))?;
    assert!(slot().is_some());

    let expired = Admission::queued(std::future::ready(Err(ReplayRefusal::WaitExpired)));
    assert!(expired.slot.clone().await.is_err());
    let mut retry = Some(transferable(limit.admit().await?));
    assert!(!expired.accept_carried(&mut retry));
    assert!(retry.is_some());
    Ok(())
}

fn publish_reader(
    chain: &ChainFixture,
    through: u32,
    settled: u32,
) -> eyre::Result<(WalletServingStoreFixture, Arc<ObservedCanonicalReader>)> {
    let baseline = CanonicalBaseline::new(chain)
        .through(BlockHeight::new(through))
        .with_settled_tip(BlockHeight::new(settled));
    let mut store =
        CanonicalWriterFixture::publish(&baseline, &sample_regtest_upgrade_activations())?
            .into_serving_store_fixture()?;
    let reader = Arc::new(ObservedCanonicalReader::new(RocksDbCanonicalReader::new(
        store.take_readers()?.0,
    )?));
    Ok((store, reader))
}

async fn replay_and_cache(
    limit: &TreeStateReplayLimit,
    reader: &Arc<ObservedCanonicalReader>,
    height: BlockHeight,
) -> eyre::Result<zinder_store::ServedTreeState> {
    let key = ReplayKey::new(height, reader.as_ref());
    let generation = Arc::downgrade(reader);
    let cache_generation = ReaderGeneration::static_for_test(&generation, true);
    let canonical_reader = Arc::clone(reader) as Arc<dyn CanonicalReader>;
    assert!(
        limit
            .ready_state(&key, &cache_generation, &canonical_reader, limit.deadline())
            .await?
            .is_none()
    );
    let sub = limit.subscribe(&key, &generation, limit.deadline())?;
    let slot = sub.admit(limit).await?;
    let reader_ref = Arc::clone(reader);
    sub.replay(slot, |cancelled| async move {
        super::read_missing_height(reader_ref.as_ref(), height, cancelled).await
    })
    .await?
    .ok_or_else(|| eyre::eyre!("missing reconstructed state"))
}

#[tokio::test]
async fn settled_cache_promotes_anchor_across_settled_tip_advance_with_one_header_check()
-> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
    let (_s1, first) = publish_reader(&chain, 4, 2)?;
    let (_s2, extended) = publish_reader(&chain, 5, 4)?;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(5));
    let height = BlockHeight::new(1);
    let expected = replay_and_cache(&limit, &first, height).await?;

    let old_key = ReplayKey::new(height, first.as_ref());
    assert!(
        matches!(limit.cached_for_reader(&old_key, &first), Some(ReadyState::Cached(ref state)) if *state == expected)
    );
    assert!(limit.cached_for_reader(&old_key, &extended).is_none());
    let key = ReplayKey::new(height, extended.as_ref());
    assert!(limit.cached_for_reader(&key, &first).is_none());
    let gen_ext = ReaderGeneration::static_for_test(&Arc::downgrade(&extended), true);
    let dyn_extended = Arc::clone(&extended) as Arc<dyn CanonicalReader>;
    let mut joins = Vec::new();
    for _ in 0..4 {
        let lim = limit.clone();
        let obs = Arc::clone(&dyn_extended);
        let gen_ref = gen_ext.clone();
        let deadline = limit.deadline();
        joins.push(tokio::spawn(async move {
            lim.ready_state(&key, &gen_ref, &obs, deadline).await
        }));
    }
    for task in joins {
        assert!(matches!(task.await??, Some(ReadyState::Cached(ref s)) if *s == expected));
    }
    assert_eq!(extended.reads(), 1);
    assert!(matches!(
        limit
            .ready_state(&key, &gen_ext, &dyn_extended, limit.deadline())
            .await?,
        Some(ReadyState::Cached(ref s)) if *s == expected
    ));
    assert_eq!(extended.reads(), 1);
    assert!(limit.cached_for_reader(&old_key, &first).is_none());
    assert!(
        matches!(limit.cached_for_reader(&key, &extended), Some(ReadyState::Cached(ref state)) if *state == expected)
    );
    Ok(())
}

#[tokio::test]
async fn settled_cache_clears_when_settled_tip_extension_is_on_a_fork() -> eyre::Result<()> {
    let chain_a = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
    let chain_b = chain_a.fork_at(BlockHeight::new(2))?.extend_blocks(4);
    let (_sa, reader_a) = publish_reader(&chain_a, 4, 2)?;
    let (_sb, reader_b) = publish_reader(&chain_b, 5, 4)?;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
    let height = BlockHeight::new(1);
    replay_and_cache(&limit, &reader_a, height).await?;

    let key_b = ReplayKey::new(height, reader_b.as_ref());
    let gen_b = ReaderGeneration::static_for_test(&Arc::downgrade(&reader_b), true);
    let dyn_b = Arc::clone(&reader_b) as Arc<dyn CanonicalReader>;
    assert!(
        limit
            .ready_state(&key_b, &gen_b, &dyn_b, limit.deadline())
            .await?
            .is_none()
    );
    assert_eq!(limit.flights.lock().settled.len(), 0);
    Ok(())
}

#[tokio::test]
async fn lagging_reader_at_prior_settled_tip_does_not_wipe_promoted_cache() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
    let (_s1, older) = publish_reader(&chain, 4, 2)?;
    let (_s2, newer) = publish_reader(&chain, 5, 4)?;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
    let height = BlockHeight::new(1);
    let expected = replay_and_cache(&limit, &newer, height).await?;
    let baseline_reads = newer.reads();

    let older_key = ReplayKey::new(height, older.as_ref());
    let older_gen = ReaderGeneration::static_for_test(&Arc::downgrade(&older), false);
    let dyn_older = Arc::clone(&older) as Arc<dyn CanonicalReader>;
    assert!(
        limit
            .ready_state(&older_key, &older_gen, &dyn_older, limit.deadline())
            .await?
            .is_none()
    );

    let newer_key = ReplayKey::new(height, newer.as_ref());
    let newer_gen = ReaderGeneration::static_for_test(&Arc::downgrade(&newer), true);
    let dyn_newer = Arc::clone(&newer) as Arc<dyn CanonicalReader>;
    assert!(matches!(
        limit
            .ready_state(&newer_key, &newer_gen, &dyn_newer, limit.deadline())
            .await?,
        Some(ReadyState::Cached(ref s)) if *s == expected
    ));
    assert_eq!(newer.reads(), baseline_reads);
    Ok(())
}

#[tokio::test]
async fn replacement_store_with_shorter_settled_tip_resets_cache_after_old_reader_drops()
-> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
    let (_s1, shorter) = publish_reader(&chain, 4, 2)?;
    let (_s2, longer) = publish_reader(&chain, 5, 4)?;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
    let height = BlockHeight::new(1);
    replay_and_cache(&limit, &longer, height).await?;
    drop(longer);

    let shorter_key = ReplayKey::new(height, shorter.as_ref());
    let shorter_gen = ReaderGeneration::static_for_test(&Arc::downgrade(&shorter), true);
    let dyn_shorter = Arc::clone(&shorter) as Arc<dyn CanonicalReader>;
    assert!(
        limit
            .ready_state(&shorter_key, &shorter_gen, &dyn_shorter, limit.deadline())
            .await?
            .is_none()
    );
    let rebuilt = replay_and_cache(&limit, &shorter, height).await?;
    assert!(matches!(
        limit
            .ready_state(&shorter_key, &shorter_gen, &dyn_shorter, limit.deadline())
            .await?,
        Some(ReadyState::Cached(ref s)) if *s == rebuilt
    ));
    Ok(())
}

enum HeaderHook {
    AsyncGate {
        entered: tokio::sync::Notify,
        open: tokio::sync::Semaphore,
        fail_stored: bool,
    },
    FailHeader,
    BlockingDispatch {
        dispatches: std::sync::atomic::AtomicUsize,
        started: tokio::sync::Notify,
        release: parking_lot::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        secondary: Arc<zinder_store::RocksDbCanonicalSecondary>,
    },
}

struct TestHeaderReader {
    inner: Arc<ObservedCanonicalReader>,
    hook: Arc<HeaderHook>,
    header_reads: std::sync::atomic::AtomicUsize,
}

impl TestHeaderReader {
    fn gated(inner: Arc<ObservedCanonicalReader>) -> Self {
        Self {
            inner,
            hook: Arc::new(HeaderHook::AsyncGate {
                entered: tokio::sync::Notify::new(),
                open: tokio::sync::Semaphore::new(0),
                fail_stored: false,
            }),
            header_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn gated_with_failing_stored(inner: Arc<ObservedCanonicalReader>) -> Self {
        Self {
            inner,
            hook: Arc::new(HeaderHook::AsyncGate {
                entered: tokio::sync::Notify::new(),
                open: tokio::sync::Semaphore::new(0),
                fail_stored: true,
            }),
            header_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn failing_header(inner: Arc<ObservedCanonicalReader>) -> Self {
        Self {
            inner,
            hook: Arc::new(HeaderHook::FailHeader),
            header_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn blocking(
        inner: Arc<ObservedCanonicalReader>,
        secondary: Arc<zinder_store::RocksDbCanonicalSecondary>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Self {
        Self {
            inner,
            hook: Arc::new(HeaderHook::BlockingDispatch {
                dispatches: std::sync::atomic::AtomicUsize::new(0),
                started: tokio::sync::Notify::new(),
                release: parking_lot::Mutex::new(Some(release)),
                secondary,
            }),
            header_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    async fn wait_entered(&self) {
        match &*self.hook {
            HeaderHook::AsyncGate { entered, .. } => entered.notified().await,
            HeaderHook::FailHeader => {}
            HeaderHook::BlockingDispatch { started, .. } => started.notified().await,
        }
    }

    fn release_gate(&self) {
        if let HeaderHook::AsyncGate { open, .. } = &*self.hook {
            open.close();
        }
    }

    fn blocking_dispatches(&self) -> usize {
        match &*self.hook {
            HeaderHook::AsyncGate { .. } | HeaderHook::FailHeader => 0,
            HeaderHook::BlockingDispatch { dispatches, .. } => {
                dispatches.load(std::sync::atomic::Ordering::Acquire)
            }
        }
    }

    fn header_reads(&self) -> usize {
        self.header_reads.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl CanonicalReader for TestHeaderReader {
    fn network(&self) -> Network {
        self.inner.network()
    }

    fn network_upgrade_schedule(&self) -> Option<&zinder_store::CanonicalActivationSchedule> {
        self.inner.network_upgrade_schedule()
    }

    fn raw_blob_retention(&self) -> zinder_store::RawBlobRetention {
        self.inner.raw_blob_retention()
    }

    fn event_fence(&self) -> zinder_store::CanonicalEventFence {
        self.inner.event_fence()
    }

    fn chain_epoch(&self) -> zinder_core::ChainEpoch {
        self.inner.chain_epoch()
    }

    fn history_bounds(&self) -> CanonicalHistoryBounds {
        self.inner.history_bounds()
    }

    fn history_predecessor_digest(&self) -> Option<[u8; 32]> {
        self.inner.history_predecessor_digest()
    }

    async fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<zinder_core::BlockHeaderArtifact>, StorageReadError> {
        match &*self.hook {
            HeaderHook::AsyncGate { entered, open, .. } => {
                entered.notify_one();
                let _released = open.acquire().await;
                self.header_reads
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                self.inner.block_header_at(height).await
            }
            HeaderHook::FailHeader => Err(StorageReadError::ReaderInvalidated {
                source: Box::new(std::io::Error::other("simulated reader invalidation")),
            }),
            HeaderHook::BlockingDispatch { .. } => {
                let hook = Arc::clone(&self.hook);
                tokio::task::spawn_blocking(move || {
                    let HeaderHook::BlockingDispatch {
                        dispatches,
                        started,
                        release,
                        secondary,
                    } = &*hook
                    else {
                        return Ok(None);
                    };
                    dispatches.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    started.notify_one();
                    let wait_rx = release.lock().take();
                    if let Some(receiver) = wait_rx {
                        let _ = receiver.recv();
                    }
                    secondary.block_header_at(height).map_err(|err| {
                        StorageReadError::ReadTaskFailed {
                            reason: err.to_string(),
                        }
                    })
                })
                .await
                .unwrap_or_else(|err| {
                    Err(StorageReadError::ReadTaskFailed {
                        reason: err.to_string(),
                    })
                })
            }
        }
    }

    async fn block_hash_lookup(
        &self,
        block_hash: BlockHash,
    ) -> Result<zinder_store::BlockHashLookup, StorageReadError> {
        self.inner.block_hash_lookup(block_hash).await
    }

    async fn block_header_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<zinder_core::BlockHeaderArtifact>, StorageReadError> {
        self.inner.block_header_by_hash(block_hash).await
    }

    async fn compact_block_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<zinder_core::CompactBlockArtifact>, StorageReadError> {
        self.inner.compact_block_by_hash(block_hash).await
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<zinder_core::CompactBlockArtifact>, StorageReadError> {
        self.inner.compact_block_at(height).await
    }

    async fn compact_blocks_in_range(
        &self,
        range: zinder_core::BlockHeightRange,
    ) -> Result<Vec<zinder_core::CompactBlockArtifact>, StorageReadError> {
        self.inner.compact_blocks_in_range(range).await
    }

    async fn compact_blocks_in_range_up_to(
        &self,
        range: zinder_core::BlockHeightRange,
        order: zinder_store::HeightOrder,
        max_bytes: usize,
    ) -> Result<Vec<zinder_core::CompactBlockArtifact>, StorageReadError> {
        self.inner
            .compact_blocks_in_range_up_to(range, order, max_bytes)
            .await
    }

    async fn block_blob_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<zinder_core::BlockBlobArtifact>, StorageReadError> {
        self.inner.block_blob_at(height).await
    }

    async fn block_blobs_in_range(
        &self,
        range: zinder_core::BlockHeightRange,
    ) -> Result<Vec<Option<zinder_core::BlockBlobArtifact>>, StorageReadError> {
        self.inner.block_blobs_in_range(range).await
    }

    async fn mined_transaction(
        &self,
        transaction_id: zinder_core::TransactionId,
    ) -> Result<Option<crate::IndexedTransaction>, StorageReadError> {
        self.inner.mined_transaction(transaction_id).await
    }

    async fn transaction_blob_by_id(
        &self,
        transaction_id: zinder_core::TransactionId,
    ) -> Result<zinder_core::TransactionBlobArtifact, StorageReadError> {
        self.inner.transaction_blob_by_id(transaction_id).await
    }

    async fn tree_state_checkpoint_at_or_before(
        &self,
        height: BlockHeight,
    ) -> Result<Option<zinder_store::ServedTreeState>, StorageReadError> {
        self.inner.tree_state_checkpoint_at_or_before(height).await
    }

    async fn stored_tree_state_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<zinder_store::ServedTreeState>, StorageReadError> {
        if let HeaderHook::AsyncGate {
            fail_stored: true, ..
        } = &*self.hook
        {
            return Err(StorageReadError::ReaderInvalidated {
                source: Box::new(std::io::Error::other(
                    "simulated stored checkpoint invalidation",
                )),
            });
        }
        self.inner.stored_tree_state_at(height).await
    }

    async fn tree_state_lookup(
        &self,
        height: BlockHeight,
    ) -> Result<Option<TreeStateLookup>, StorageReadError> {
        self.inner.tree_state_lookup(height).await
    }

    async fn replay_tree_state(
        &self,
        replay: zinder_store::TreeStateReplay,
    ) -> Result<Option<zinder_store::TreeStateReplayOutcome>, StorageReadError> {
        self.inner.replay_tree_state(replay).await
    }

    async fn replay_tree_state_until_cancelled(
        &self,
        replay: zinder_store::TreeStateReplay,
        cancelled: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Option<zinder_store::TreeStateReplayOutcome>, StorageReadError> {
        self.inner
            .replay_tree_state_until_cancelled(replay, cancelled)
            .await
    }

    async fn subtree_roots(
        &self,
        range: zinder_core::SubtreeRootRange,
    ) -> Result<Vec<zinder_core::SubtreeRootArtifact>, StorageReadError> {
        self.inner.subtree_roots(range).await
    }

    async fn wallet_chain_event_history(
        &self,
        from_cursor: Option<zinder_store::StreamCursorTokenV1>,
        family: zinder_store::ChainEventStreamFamily,
        max_events: NonZeroU32,
    ) -> Result<Vec<zinder_store::ChainEventEnvelope>, StorageReadError> {
        self.inner
            .wallet_chain_event_history(from_cursor, family, max_events)
            .await
    }

    async fn resolve_wallet_chain_event_stream_start(
        &self,
        start: zinder_store::EventStreamStartPosition,
        requested_family: zinder_store::ChainEventStreamFamily,
    ) -> Result<zinder_store::ChainEventStreamResume, StorageReadError> {
        self.inner
            .resolve_wallet_chain_event_stream_start(start, requested_family)
            .await
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_extension_header_check_bounds_waiters_and_expires_wait_deadline()
-> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
    let (_s1, first) = publish_reader(&chain, 4, 2)?;
    let (_s2, extended_inner) = publish_reader(&chain, 5, 4)?;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_millis(60))
        .with_waiters_per_slot(NonZeroU32::MIN);
    let height = BlockHeight::new(1);
    replay_and_cache(&limit, &first, height).await?;

    let stalled = Arc::new(TestHeaderReader::gated(Arc::clone(&extended_inner)));
    let key = ReplayKey::new(height, stalled.as_ref());
    let stalled_gen = ReaderGeneration::static_for_test(&Arc::downgrade(&stalled), true);
    let dyn_stalled = Arc::clone(&stalled) as Arc<dyn CanonicalReader>;

    let leader = tokio::spawn({
        let limit = limit.clone();
        let dyn_stalled = Arc::clone(&dyn_stalled);
        let stalled_gen = stalled_gen.clone();
        let deadline = limit.deadline();
        async move {
            limit
                .ready_state(&key, &stalled_gen, &dyn_stalled, deadline)
                .await
        }
    });
    stalled.wait_entered().await;

    let follower = tokio::spawn({
        let limit = limit.clone();
        let dyn_stalled = Arc::clone(&dyn_stalled);
        let stalled_gen = stalled_gen.clone();
        let deadline = limit.deadline();
        async move {
            limit
                .ready_state(&key, &stalled_gen, &dyn_stalled, deadline)
                .await
        }
    });
    limit.wait_for_waiters(1).await?;

    let overflow = limit
        .ready_state(&key, &stalled_gen, &dyn_stalled, limit.deadline())
        .await;
    assert!(matches!(
        overflow,
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));

    tokio::time::advance(Duration::from_millis(65)).await;
    assert!(matches!(
        follower.await?,
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));
    assert!(matches!(
        leader.await?,
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));
    assert_eq!(limit.waiter_count(), 0);
    assert_eq!(stalled.header_reads(), 0);
    assert_eq!(extended_inner.reads(), 3);
    stalled.release_gate();
    Ok(())
}

#[tokio::test]
async fn zero_wait_refuses_contended_extension_check_immediately_without_queueing()
-> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
    let (_s1, first) = publish_reader(&chain, 4, 2)?;
    let (_s2, extended_inner) = publish_reader(&chain, 5, 4)?;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO)
        .with_waiters_per_slot(NonZeroU32::MIN);
    let height = BlockHeight::new(1);
    let expected = replay_and_cache(&limit, &first, height).await?;

    let stalled = Arc::new(TestHeaderReader::gated(Arc::clone(&extended_inner)));
    let key = ReplayKey::new(height, stalled.as_ref());
    let stalled_gen = ReaderGeneration::static_for_test(&Arc::downgrade(&stalled), true);
    let dyn_stalled = Arc::clone(&stalled) as Arc<dyn CanonicalReader>;

    let leader = tokio::spawn({
        let limit = limit.clone();
        let dyn_stalled = Arc::clone(&dyn_stalled);
        let stalled_gen = stalled_gen.clone();
        let deadline = limit.deadline();
        async move {
            limit
                .ready_state(&key, &stalled_gen, &dyn_stalled, deadline)
                .await
        }
    });
    stalled.wait_entered().await;

    let contender = limit
        .ready_state(&key, &stalled_gen, &dyn_stalled, limit.deadline())
        .await;
    assert!(matches!(
        contender,
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));
    assert_eq!(limit.waiter_count(), 0);

    stalled.release_gate();
    assert!(matches!(
        leader.await??,
        Some(ReadyState::Cached(ref state)) if *state == expected
    ));
    assert_eq!(stalled.header_reads(), 1);
    assert_eq!(extended_inner.reads(), 2);
    Ok(())
}

fn frontiers_with_sapling_commitment(
    commitment: [u8; 32],
    activations: &zinder_core::NetworkUpgradeActivations,
) -> eyre::Result<zinder_core::CommitmentTreeFrontiers> {
    let mut accumulator = zinder_core::CommitmentTreeAccumulator::from_validated_frontiers(
        BlockHeight::new(0),
        &zinder_core::CommitmentTreeFrontiers::default(),
        activations,
    )?;
    accumulator.append_block_commitments(BlockHeight::new(1), &[commitment], &[], &[])?;
    Ok(accumulator.validated_frontiers()?)
}

fn checkpointed_chain_with_seed(
    base: &ChainFixture,
    seed_frontiers: zinder_core::CommitmentTreeFrontiers,
) -> ChainFixture {
    let meta = zinder_core::CompactChainMetadata {
        sapling_commitment_tree_size: 1,
        orchard_commitment_tree_size: 0,
        ironwood_commitment_tree_size: 0,
    };
    let mut configured = base.clone();
    for block in base.blocks() {
        let parent_hash = if block.height == BlockHeight::new(1) {
            Network::ZcashRegtest.genesis_hash()
        } else {
            block.parent_hash
        };
        configured =
            configured.with_compact_block_artifact(zinder_core::CompactBlockArtifact::empty(
                zinder_core::BlockId::new(block.height, block.hash),
                parent_hash,
                block.block_time_seconds,
                meta,
            ));
    }
    configured.with_commitment_tree_frontiers_at(BlockHeight::new(1), seed_frontiers)
}

fn publish_checkpointed_reader(
    chain: &ChainFixture,
    through: u32,
    settled: u32,
) -> eyre::Result<(WalletServingStoreFixture, Arc<ObservedCanonicalReader>)> {
    let baseline = CanonicalBaseline::new(chain)
        .through(BlockHeight::new(through))
        .with_history_floor(BlockHeight::new(1))
        .with_settled_tip(BlockHeight::new(settled));
    let mut store =
        CanonicalWriterFixture::publish(&baseline, &sample_regtest_upgrade_activations())?
            .into_serving_store_fixture()?;
    let reader = Arc::new(ObservedCanonicalReader::new(RocksDbCanonicalReader::new(
        store.take_readers()?.0,
    )?));
    Ok((store, reader))
}

#[tokio::test]
async fn predecessor_frontier_mismatch_isolates_replacement_store_on_same_anchor_and_extension()
-> eyre::Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let base_chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
    let mut cmu_a = [0_u8; 32];
    cmu_a[0] = 1;
    let mut cmu_b = [0_u8; 32];
    cmu_b[0] = 2;
    let chain_a = checkpointed_chain_with_seed(
        &base_chain,
        frontiers_with_sapling_commitment(cmu_a, &activations)?,
    );
    let chain_b = checkpointed_chain_with_seed(
        &base_chain,
        frontiers_with_sapling_commitment(cmu_b, &activations)?,
    );
    let (_sa, reader_a) = publish_checkpointed_reader(&chain_a, 4, 3)?;
    let (_sb_same, reader_b_same) = publish_checkpointed_reader(&chain_b, 4, 3)?;
    let (_sb_ext, reader_b_ext) = publish_checkpointed_reader(&chain_b, 5, 4)?;
    let height = BlockHeight::new(2);

    let limit_same = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(5));
    let state_a = replay_and_cache(&limit_same, &reader_a, height).await?;
    let key_b_same = ReplayKey::new(height, reader_b_same.as_ref());
    let gen_b_same = ReaderGeneration::static_for_test(&Arc::downgrade(&reader_b_same), true);
    let dyn_b_same = Arc::clone(&reader_b_same) as Arc<dyn CanonicalReader>;
    assert!(
        limit_same
            .ready_state(&key_b_same, &gen_b_same, &dyn_b_same, limit_same.deadline())
            .await?
            .is_none()
    );
    assert_eq!(limit_same.flights.lock().settled.len(), 0);
    let state_b_same = replay_and_cache(&limit_same, &reader_b_same, height).await?;
    assert_ne!(state_a, state_b_same);

    let limit_ext = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(5));
    replay_and_cache(&limit_ext, &reader_a, height).await?;
    let key_b_ext = ReplayKey::new(height, reader_b_ext.as_ref());
    let gen_b_ext = ReaderGeneration::static_for_test(&Arc::downgrade(&reader_b_ext), true);
    let dyn_b_ext = Arc::clone(&reader_b_ext) as Arc<dyn CanonicalReader>;
    assert!(
        limit_ext
            .ready_state(&key_b_ext, &gen_b_ext, &dyn_b_ext, limit_ext.deadline())
            .await?
            .is_none()
    );
    assert_eq!(limit_ext.flights.lock().settled.len(), 0);
    let state_b_ext = replay_and_cache(&limit_ext, &reader_b_ext, height).await?;
    assert_ne!(state_a, state_b_ext);
    Ok(())
}

fn publish_blocking_reader(
    chain: &ChainFixture,
    through: u32,
    settled: u32,
) -> eyre::Result<(
    WalletServingStoreFixture,
    Arc<TestHeaderReader>,
    std::sync::mpsc::Sender<()>,
)> {
    let baseline = CanonicalBaseline::new(chain)
        .through(BlockHeight::new(through))
        .with_settled_tip(BlockHeight::new(settled));
    let mut store =
        CanonicalWriterFixture::publish(&baseline, &sample_regtest_upgrade_activations())?
            .into_serving_store_fixture()?;
    let secondary = Arc::new(store.take_readers()?.0);
    let inner = Arc::new(ObservedCanonicalReader::new(RocksDbCanonicalReader::new(
        Arc::clone(&secondary),
    )?));
    let (send, recv) = std::sync::mpsc::channel();
    Ok((
        store,
        Arc::new(TestHeaderReader::blocking(inner, secondary, recv)),
        send,
    ))
}

#[tokio::test]
async fn already_dispatched_blocking_header_read_stays_single_flight_across_timeout_and_concurrent_callers()
-> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
    let (_s1, first) = publish_reader(&chain, 4, 2)?;
    let (_s2, blocking, release) = publish_blocking_reader(&chain, 5, 4)?;
    let (_s3, foreign, release_foreign) = publish_blocking_reader(&chain, 5, 4)?;
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_millis(120))
        .with_waiters_per_slot(NonZeroU32::MIN);
    let height = BlockHeight::new(1);
    let expected = replay_and_cache(&limit, &first, height).await?;

    let key = ReplayKey::new(height, blocking.as_ref());
    let gen_primary = ReaderGeneration::static_for_test(&Arc::downgrade(&blocking), true);
    let dyn_primary = Arc::clone(&blocking) as Arc<dyn CanonicalReader>;

    let leader = tokio::spawn({
        let limit = limit.clone();
        let dyn_primary = Arc::clone(&dyn_primary);
        let gen_primary = gen_primary.clone();
        async move {
            limit
                .ready_state(&key, &gen_primary, &dyn_primary, limit.deadline())
                .await
        }
    });
    blocking.wait_entered().await;
    assert!(matches!(
        leader.await?,
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));
    assert_eq!(blocking.blocking_dispatches(), 1);

    let follower = tokio::spawn({
        let limit = limit.clone();
        let dyn_primary = Arc::clone(&dyn_primary);
        let gen_primary = gen_primary.clone();
        async move {
            limit
                .ready_state(&key, &gen_primary, &dyn_primary, limit.deadline())
                .await
        }
    });
    limit.wait_for_waiters(1).await?;
    assert!(matches!(
        limit
            .ready_state(&key, &gen_primary, &dyn_primary, limit.deadline())
            .await,
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));
    assert!(matches!(
        follower.await?,
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));

    let gen_foreign = ReaderGeneration::static_for_test(&Arc::downgrade(&foreign), true);
    let dyn_foreign = Arc::clone(&foreign) as Arc<dyn CanonicalReader>;
    assert!(matches!(
        limit
            .ready_state(&key, &gen_foreign, &dyn_foreign, limit.deadline())
            .await,
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));
    assert_eq!(foreign.blocking_dispatches(), 0);
    assert_eq!(blocking.blocking_dispatches(), 1);

    release.send(())?;
    release_foreign.send(())?;
    let promoted = limit
        .ready_state(
            &key,
            &gen_primary,
            &dyn_primary,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await?;
    assert!(matches!(promoted, Some(ReadyState::Cached(ref state)) if *state == expected));
    assert_eq!(blocking.blocking_dispatches(), 1);
    assert_eq!(limit.waiter_count(), 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stalled_extension_serves_stored_checkpoint_instead_of_replay_limit_refusal()
-> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
    let (_s1, first) = publish_reader(&chain, 4, 2)?;
    let (_s2, extended_inner) = publish_reader(&chain, 5, 4)?;
    let stored_height = BlockHeight::new(0);
    let Some(expected_stored) = first.stored_tree_state_at(stored_height).await? else {
        return Err(eyre::eyre!("missing stored checkpoint"));
    };
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_millis(60))
        .with_waiters_per_slot(NonZeroU32::MIN);
    replay_and_cache(&limit, &first, BlockHeight::new(1)).await?;

    let stalled = Arc::new(TestHeaderReader::gated(Arc::clone(&extended_inner)));
    let replay_key = ReplayKey::new(BlockHeight::new(1), stalled.as_ref());
    let stored_key = ReplayKey::new(stored_height, stalled.as_ref());
    let stalled_gen = ReaderGeneration::static_for_test(&Arc::downgrade(&stalled), true);
    let dyn_stalled = Arc::clone(&stalled) as Arc<dyn CanonicalReader>;

    let leader = tokio::spawn({
        let limit = limit.clone();
        let dyn_stalled = Arc::clone(&dyn_stalled);
        let stalled_gen = stalled_gen.clone();
        let deadline = limit.deadline();
        async move {
            limit
                .ready_state(&replay_key, &stalled_gen, &dyn_stalled, deadline)
                .await
        }
    });
    stalled.wait_entered().await;

    let follower = tokio::spawn({
        let limit = limit.clone();
        let dyn_stalled = Arc::clone(&dyn_stalled);
        let stalled_gen = stalled_gen.clone();
        let deadline = limit.deadline();
        async move {
            limit
                .ready_state(&replay_key, &stalled_gen, &dyn_stalled, deadline)
                .await
        }
    });
    limit.wait_for_waiters(1).await?;

    let served_under_contention = limit
        .ready_state(&stored_key, &stalled_gen, &dyn_stalled, limit.deadline())
        .await?;
    assert!(matches!(
        served_under_contention,
        Some(ReadyState::Stored(ref state)) if *state == expected_stored
    ));

    tokio::time::advance(Duration::from_millis(65)).await;
    assert!(matches!(
        follower.await?,
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));
    assert!(matches!(
        leader.await?,
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));

    let stored_waiter = tokio::spawn({
        let limit = limit.clone();
        let dyn_stalled = Arc::clone(&dyn_stalled);
        let stalled_gen = stalled_gen.clone();
        let deadline = limit.deadline();
        async move {
            limit
                .ready_state(&stored_key, &stalled_gen, &dyn_stalled, deadline)
                .await
        }
    });
    limit.wait_for_waiters(1).await?;
    tokio::time::advance(Duration::from_millis(65)).await;
    let served_after_timeout = stored_waiter.await??;
    assert!(matches!(
        served_after_timeout,
        Some(ReadyState::Stored(ref state)) if *state == expected_stored
    ));

    assert_eq!(stalled.header_reads(), 0);
    stalled.release_gate();
    Ok(())
}

#[tokio::test]
async fn extension_storage_error_is_not_masked_by_stored_checkpoint_fallback() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(5);
    let (_s1, first) = publish_reader(&chain, 4, 2)?;
    let (_s2, extended_inner) = publish_reader(&chain, 5, 4)?;
    let stored_height = BlockHeight::new(0);

    let limit_header_err = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(5));
    replay_and_cache(&limit_header_err, &first, BlockHeight::new(1)).await?;
    let failing = Arc::new(TestHeaderReader::failing_header(Arc::clone(
        &extended_inner,
    )));
    let stored_key = ReplayKey::new(stored_height, failing.as_ref());
    let failing_gen = ReaderGeneration::static_for_test(&Arc::downgrade(&failing), true);
    let dyn_failing = Arc::clone(&failing) as Arc<dyn CanonicalReader>;
    let header_err = limit_header_err
        .ready_state(
            &stored_key,
            &failing_gen,
            &dyn_failing,
            limit_header_err.deadline(),
        )
        .await;
    assert!(matches!(header_err, Err(ref err) if err.invalidates_reader()));
    assert_eq!(extended_inner.reads(), 0);

    let limit_stored_err = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
    replay_and_cache(&limit_stored_err, &first, BlockHeight::new(1)).await?;
    let gated_fail_stored = Arc::new(TestHeaderReader::gated_with_failing_stored(Arc::clone(
        &extended_inner,
    )));
    let replay_key = ReplayKey::new(BlockHeight::new(1), gated_fail_stored.as_ref());
    let stored_err_key = ReplayKey::new(stored_height, gated_fail_stored.as_ref());
    let stalled_gen = ReaderGeneration::static_for_test(&Arc::downgrade(&gated_fail_stored), true);
    let dyn_stalled = Arc::clone(&gated_fail_stored) as Arc<dyn CanonicalReader>;

    let leader = tokio::spawn({
        let limit = limit_stored_err.clone();
        let dyn_stalled = Arc::clone(&dyn_stalled);
        let stalled_gen = stalled_gen.clone();
        let deadline = limit.deadline();
        async move {
            limit
                .ready_state(&replay_key, &stalled_gen, &dyn_stalled, deadline)
                .await
        }
    });
    gated_fail_stored.wait_entered().await;

    let stored_err = limit_stored_err
        .ready_state(
            &stored_err_key,
            &stalled_gen,
            &dyn_stalled,
            limit_stored_err.deadline(),
        )
        .await;
    assert!(matches!(stored_err, Err(ref err) if err.invalidates_reader()));
    gated_fail_stored.release_gate();
    let _ = leader.await?;
    Ok(())
}
