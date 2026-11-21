use std::{num::NonZeroU32, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use eyre::{Result, eyre};
use metrics_exporter_prometheus::PrometheusBuilder;
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

fn query(slot: &Arc<ArcSwap<RocksDbCanonicalReader>>) -> Result<QualificationWalletQuery> {
    let capture = Arc::clone(slot);
    Ok(QualificationWalletQuery::new(
        move || capture.load_full(),
        ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:1",
            NodeAuth::None,
            Duration::from_secs(1),
        )?,
        Duration::from_secs(1),
        NonZeroU32::MIN,
    ))
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
    let mut query = query(&slot)?;
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
    let refused = query
        .tree_state(Arc::clone(&old), Some(BlockHeight::new(16)))
        .await;
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

#[tokio::test]
async fn tree_state_metrics_count_each_height_read_from_its_served_outcome() -> Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let metrics = recorder.handle();
    let _local_recorder = metrics::set_default_local_recorder(&recorder);
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let (_initial_store, initial) = reader(&chain)?;
    let slot = Arc::new(ArcSwap::from(Arc::clone(&initial)));
    let query = query(&slot)?;
    query.tree_state(Arc::clone(&initial), None).await?;
    query
        .tree_state(Arc::clone(&initial), Some(BlockHeight::new(3)))
        .await?;
    let height = BlockHeight::new(2);
    let Some(zinder_store::TreeStateLookup::Replay(replay)) =
        initial.tree_state_lookup(height).await?
    else {
        return Err(eyre!("expected to rebuild the tree state at height 2"));
    };
    query.tree_state(Arc::clone(&initial), Some(height)).await?;

    let mut stored = WalletServingStoreFixture::from_chain_after_live_append(
        &chain,
        &sample_regtest_upgrade_activations(),
    )?;
    let (canonical, _) = stored.take_readers()?;
    slot.store(Arc::new(RocksDbCanonicalReader::new(canonical)?));
    query.tree_state(initial, Some(height)).await?;

    let rendered = metrics.render();
    for expected in [
        r#"zinder_query_tree_state_reads_total{source="stored",height_band="0"} 2"#,
        r#"zinder_query_tree_state_reads_total{source="replayed",height_band="0"} 1"#,
        r#"zinder_query_tree_state_replayed_blocks_count{height_band="0"} 3"#,
        &format!(
            r#"zinder_query_tree_state_replayed_blocks_sum{{height_band="0"}} {}"#,
            replay.replayed_blocks()
        ),
        r#"zinder_query_tree_state_replayed_commitments_count{pool="sapling",height_band="0"} 3"#,
        r#"zinder_query_tree_state_replayed_commitments_count{pool="orchard",height_band="0"} 3"#,
        r#"zinder_query_tree_state_replayed_commitments_count{pool="ironwood",height_band="0"} 3"#,
        r#"zinder_query_tree_state_replay_duration_seconds_count{height_band="0"} 1"#,
        "zinder_query_tree_state_replay_admissions_total 1",
    ] {
        assert!(
            rendered.lines().any(|line| line == expected),
            "missing {expected}:\n{rendered}"
        );
    }
    Ok(())
}
