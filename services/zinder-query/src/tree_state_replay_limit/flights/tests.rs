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
    assert!(first.admit(&limit, deadline).now_or_never().is_none());
    let other = limit.subscribe(
        ReplayKey {
            height: BlockHeight::new(1),
            ..key
        },
        &generation,
        deadline,
    )?;
    assert!(other.admit(&limit, deadline).now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(2)).await;
    let follower = limit.subscribe(key, &generation, limit.deadline())?;
    assert!(limit.subscribe(key, &generation, limit.deadline()).is_err());
    drop(first);
    drop(held);
    let slot = follower
        .admit(&limit, limit.deadline())
        .now_or_never()
        .ok_or_else(|| eyre::eyre!("follower lost the first queue position"))??;
    assert!(other.admit(&limit, deadline).now_or_never().is_none());
    drop(slot);
    drop(follower);
    other.admit(&limit, deadline).await?;
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
    assert!(first.admit(&limit, deadline).now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(2)).await;
    let follower = limit.subscribe(key, &generation, limit.deadline())?;
    drop(first);
    assert!(follower.admit(&limit, limit.deadline()).await.is_err());
    assert_eq!(tokio::time::Instant::now(), deadline);
    drop(held);
    let retried = limit.subscribe(key, &generation, limit.deadline())?;
    retried.admit(&limit, limit.deadline()).await?;
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
    let slot = first.admit(&limit, limit.deadline()).await?;
    assert!(
        limit
            .subscribe(key, &replacement, limit.deadline())
            .is_err()
    );
    let follower = limit.subscribe(key, &generation, limit.deadline())?;
    let delayed = follower.admit(&limit, limit.deadline()).await?;
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
    let available = fresh.admit(&limit, limit.deadline()).await?;
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
    let mut previous = None;
    let deadline = limit.admission_deadline(&mut previous)?;
    let first = limit.subscribe(key, &generation, deadline)?;
    assert!(first.admit(&limit, deadline).now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(3)).await;
    drop(held);
    first.admit(&limit, deadline).await?;
    drop(first);
    let _other_wallet = limit.admit().await?;
    let replacement = Arc::downgrade(&Arc::new(()));
    let deadline = limit.admission_deadline(&mut previous)?;
    let second = limit.subscribe(key, &replacement, deadline)?;
    assert!(second.admit(&limit, deadline).await.is_err());
    assert_eq!(tokio::time::Instant::now(), deadline);
    assert!(limit.admission_deadline(&mut previous).is_err());
    Ok(())
}
