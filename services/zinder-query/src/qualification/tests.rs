use std::{num::NonZeroU32, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use eyre::{Result, eyre};
use zinder_core::{BlockHeight, Network};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

use super::{QualificationWalletQuery, TreeStateReplayLimit};
use crate::{CanonicalReader as _, QueryError, RocksDbCanonicalReader};

fn reader(
    chain: &ChainFixture,
) -> Result<(WalletServingStoreFixture, Arc<RocksDbCanonicalReader>)> {
    let mut store =
        WalletServingStoreFixture::from_chain(chain, &sample_regtest_upgrade_activations())?;
    let (canonical, _) = store.take_readers()?;
    let reader = Arc::new(RocksDbCanonicalReader::new(canonical)?);
    Ok((store, reader))
}

#[tokio::test]
async fn queued_replay_releases_the_generation_and_rejects_a_replaced_block() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(24);
    let height = BlockHeight::new(17);
    let (_old_store, original) = reader(&chain)?;
    let (_new_store, replacement) = reader(&chain.fork_at(height)?.extend_blocks(8))?;
    assert!(matches!(
        original.tree_state_lookup(height).await?,
        Some(zinder_store::TreeStateLookup::Replay(_))
    ));
    let slot = Arc::new(ArcSwap::from(original));
    let capture = Arc::clone(&slot);
    let mut query = QualificationWalletQuery::new(
        move || capture.load_full(),
        ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:1",
            NodeAuth::None,
            Duration::from_secs(1),
        )?,
        Duration::from_secs(1),
        NonZeroU32::MIN,
    );
    query.replays = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(5))
        .with_waiters_per_slot(NonZeroU32::MIN);
    let occupied = query.replays.admit().await?;
    let queued = {
        let query = query.clone();
        let initial = slot.load_full();
        tokio::spawn(async move { query.tree_state(initial, Some(height)).await })
    };
    let old = slot.swap(replacement);
    tokio::time::timeout(Duration::from_secs(3), async {
        while Arc::strong_count(&old) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let refused = query.tree_state(Arc::clone(&old), Some(height)).await;
    assert!(
        matches!(refused, Err(QueryError::TreeStateReplayLimitReached { .. })),
        "{refused:?}"
    );
    drop(occupied);
    assert!(matches!(
        queued.await?,
        Err(QueryError::BlockNotInBestChain)
    ));
    let final_state = query.tree_state(slot.load_full(), Some(height)).await?;
    assert_eq!(
        final_state.block_hash,
        slot.load_full()
            .block_header_at(height)
            .await?
            .ok_or_else(|| eyre!("replacement header missing"))?
            .block_hash
    );
    Ok(())
}
