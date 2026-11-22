use std::{num::NonZeroU32, sync::Arc, time::Duration};

use super::super::TreeStateReplayLimit;
use super::{ReplayKey, SETTLED_ENTRIES, SettledStates};
use crate::{CanonicalReader as _, RocksDbCanonicalReader};
use futures_util::FutureExt as _;
use zinder_core::{BlockHash, BlockHeight, CanonicalHistoryBounds, Network};
use zinder_store::TreeStateLookup;
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

#[tokio::test]
async fn only_settled_rows_at_the_same_retained_fence_survive_eviction() -> eyre::Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut store =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    let height = reader.chain_epoch().visible_tip_height;
    let Some(TreeStateLookup::Stored(state)) = reader.tree_state_lookup(height).await? else {
        return Err(eyre::eyre!("missing tip checkpoint"));
    };
    let mut key = ReplayKey::new(height, &reader);
    key.epoch.settled_tip_height = BlockHeight::new(u32::try_from(SETTLED_ENTRIES)?);
    let mut cache = SettledStates::default();
    for number in 0..=key.epoch.settled_tip_height.value() {
        key.height = BlockHeight::new(number);
        cache.remember(key, &state);
    }
    assert_eq!(cache.cached(key), Some(state.clone()));
    assert_eq!(
        cache.cached(ReplayKey {
            height: BlockHeight::new(0),
            ..key
        }),
        None
    );
    let mut fork = key;
    fork.epoch.visible_tip_hash = BlockHash::from_bytes([23; 32]);
    assert!(cache.cached(fork).is_none());
    assert!(
        cache
            .cached(ReplayKey {
                schedule_revision: None,
                ..key
            })
            .is_none()
    );
    let pruned = ReplayKey {
        retained: CanonicalHistoryBounds::checkpointed(state.block_id())?,
        ..key
    };
    assert!(cache.cached(pruned).is_none());
    key.height = BlockHeight::new(key.epoch.settled_tip_height.value() + 1);
    cache.remember(key, &state);
    assert!(cache.cached(key).is_none());
    Ok(())
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
    let first = limit.subscribe(key, &generation, deadline)?;
    assert!(first.admit(&limit).now_or_never().is_none());
    let other = limit.subscribe(
        ReplayKey {
            height: BlockHeight::new(1),
            ..key
        },
        &generation,
        deadline,
    )?;
    assert!(other.admit(&limit).now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(2)).await;
    let follower = limit.subscribe(key, &generation, limit.deadline())?;
    assert!(limit.subscribe(key, &generation, limit.deadline()).is_err());
    drop(first);
    assert_eq!(limit.waiter_count(), 2);
    let recovered = limit.subscribe(
        ReplayKey {
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
    let first = limit.subscribe(key, &generation, deadline)?;
    assert!(first.admit(&limit).now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(2)).await;
    let follower = limit.subscribe(key, &generation, limit.deadline())?;
    drop(first);
    assert!(follower.admit(&limit).await.is_err());
    assert_eq!(tokio::time::Instant::now(), deadline);
    drop(held);
    let retried = limit.subscribe(key, &generation, limit.deadline())?;
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
    let first = limit.subscribe(key, &generation, limit.deadline())?;
    let slot = first.admit(&limit).await?;
    assert!(
        limit
            .subscribe(key, &replacement, limit.deadline())
            .is_err()
    );
    let follower = limit.subscribe(key, &generation, limit.deadline())?;
    let delayed = follower.admit(&limit).await?;
    assert!(limit.subscribe(key, &generation, limit.deadline()).is_err());
    let failure = first
        .replay(slot, |_| async {
            Err(StorageReadError::ReaderInvalidated {
                source: "retired reader".into(),
            })
        })
        .await;
    assert!(failure.is_err());
    let fresh = limit.subscribe(key, &replacement, limit.deadline())?;
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
    let first = limit.subscribe(key, &generation, deadline)?;
    assert!(first.admit(&limit).now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(3)).await;
    drop(held);
    first.admit(&limit).await?;
    drop(first);
    let other_wallet = limit.admit().await?;
    let replacement = Arc::downgrade(&Arc::new(()));
    let second = limit.subscribe(key, &replacement, deadline)?;
    assert!(second.admit(&limit).await.is_err());
    assert_eq!(tokio::time::Instant::now(), deadline);
    drop(other_wallet);
    let third = limit.subscribe(key, &replacement, deadline)?;
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
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::ZERO);
    assert!(limit.ready_state(key, reader.as_ref()).await?.is_none());
    let first = limit.subscribe(key, &generation, limit.deadline())?;
    let slot = first.admit(&limit).await?;
    let state = first
        .replay(slot, |cancelled| async move {
            read_missing_height(reader.as_ref(), height, cancelled).await
        })
        .await?
        .ok_or_else(|| eyre::eyre!("missing reconstructed state"))?;
    drop(first);
    let _occupied = limit.admit().await?;
    let Selection::Ready(cached) = limit.select(key, &generation, limit.deadline(), None)? else {
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
    let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(4));
    let deadline = limit.deadline();
    let occupied = limit.admit().await?;
    let first = limit.subscribe(key, &old, deadline)?;
    assert!(first.admit(&limit).now_or_never().is_none());
    let later_key = ReplayKey {
        height: BlockHeight::new(1),
        ..key
    };
    let later = limit.subscribe(later_key, &new, deadline)?;
    assert!(later.admit(&limit).now_or_never().is_none());
    let previous = limit.subscribe(key, &new, deadline)?;
    assert!(previous.admit(&limit).now_or_never().is_none());
    drop(occupied);
    let carried = first.carry(&first.admit(&limit).await?);
    let Selection::Waiting(moved) = limit.select(key, &new, deadline, carried)? else {
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
    let first = limit.subscribe(key, &destination, limit.deadline())?;
    let slot = first.admit(&limit).await?;
    let (finish, wait) = tokio::sync::oneshot::channel::<()>();
    let work = first.replay(slot, |_| async move {
        let _ = wait.await;
        Ok(None)
    });
    tokio::pin!(work);
    assert!(futures_util::poll!(&mut work).is_pending());
    let _subscriber = limit.subscribe(key, &destination, limit.deadline())?;
    let _another = limit.subscribe(key, &destination, limit.deadline())?;
    let previous = limit.subscribe(key, &original, limit.deadline())?;
    let carried = previous.carry(&previous.admit(&limit).await?);
    assert!(carried.is_some());
    assert!(matches!(
        limit.select(key, &destination, limit.deadline(), carried),
        Err(crate::QueryError::TreeStateReplayLimitReached { .. })
    ));
    assert_eq!(limit.waiter_count(), 2);
    let unrelated = limit.subscribe(
        ReplayKey {
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
    let first = limit.subscribe(key, &destination, limit.deadline())?;
    let old_flight = first.flight.clone();
    let slot = first.admit(&limit).await?;
    let second = limit.subscribe(key, &donor, limit.deadline())?;
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
        move || limit.select(key, &destination, limit.deadline(), incoming)
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
