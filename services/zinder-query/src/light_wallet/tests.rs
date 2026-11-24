use std::{num::NonZeroU32, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use eyre::{Result, eyre};
use metrics_exporter_prometheus::PrometheusBuilder;
use zinder_core::{BlockHeight, Network};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

use super::{LightWalletNodeQuery, TreeStateReplayLimit};
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

fn query(slot: &Arc<ArcSwap<RocksDbCanonicalReader>>) -> Result<LightWalletNodeQuery> {
    let capture = Arc::clone(slot);
    Ok(LightWalletNodeQuery::new(
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
async fn a_cached_reader_recaptures_once_and_rejects_a_replacement_fork() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
    let height = Some(BlockHeight::new(2));
    let (_old, original) = reader(&chain)?;
    let (_new, replacement) = reader(&chain.fork_at(BlockHeight::new(2))?.extend_blocks(3))?;
    let slot = Arc::new(ArcSwap::from(Arc::clone(&original)));
    let mut query = query(&slot)?;
    let expected = query.tree_state(Arc::clone(&original), height).await?;
    let captures = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&captures);
    let current = Arc::clone(&slot);
    query.capture = Arc::new(move || {
        observed.fetch_add(1, Ordering::Relaxed);
        current.load_full()
    });
    assert_eq!(
        query.tree_state(Arc::clone(&original), height).await?,
        expected
    );
    assert_eq!(captures.load(Ordering::Relaxed), 1);
    slot.store(replacement);
    assert!(matches!(
        query.tree_state(original, height).await,
        Err(QueryError::BlockNotInBestChain)
    ));
    Ok(())
}

#[tokio::test]
async fn replacement_keeps_the_queued_wallet_ahead_of_the_next_admission() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let (_old, initial) = reader(&chain)?;
    let (_new, replacement) = reader(&chain.extend_blocks(1))?;
    let slot = Arc::new(ArcSwap::from(initial.clone()));
    let mut query = query(&slot)?;
    query.replays = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_secs(4));
    let occupied = query.replays.admit().await?;
    let first = tokio::spawn({
        let query = query.clone();
        async move { query.tree_state(initial, Some(BlockHeight::new(2))).await }
    });
    query.replays.wait_for_waiters(1).await?;
    let later = query.replays.admit();
    tokio::pin!(later);
    assert!(futures_util::poll!(&mut later).is_pending());
    slot.store(replacement);
    drop(occupied);
    let answer = first.await??;
    assert_eq!(answer.height, BlockHeight::new(2));
    assert_eq!(answer.chain_epoch.visible_tip_height, BlockHeight::new(4));
    drop(later.await?);
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
        r#"zinder_query_tree_state_replayed_blocks_count{height_band="0"} 1"#,
        &format!(
            r#"zinder_query_tree_state_replayed_blocks_sum{{height_band="0"}} {}"#,
            replay.replayed_blocks()
        ),
        r#"zinder_query_tree_state_replayed_commitments_count{pool="sapling",height_band="0"} 1"#,
        r#"zinder_query_tree_state_replayed_commitments_count{pool="orchard",height_band="0"} 1"#,
        r#"zinder_query_tree_state_replayed_commitments_count{pool="ironwood",height_band="0"} 1"#,
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

/// The sample regtest upgrades one per height, so the node's upgrade table
/// compares equal whatever order it lists them in.
fn one_upgrade_per_height() -> Result<zinder_core::NetworkUpgradeActivations> {
    Ok(zinder_core::NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        sample_regtest_upgrade_activations()
            .activations()
            .iter()
            .zip(1..)
            .map(
                |(activation, height)| zinder_core::NetworkUpgradeActivation {
                    activation_height: BlockHeight::new(height),
                    ..activation.clone()
                },
            )
            .collect(),
    )?)
}

/// A node double that serves `schedule` and holds `tip` at its height.
fn node_holding(
    schedule: &zinder_core::NetworkUpgradeActivations,
    tip: zinder_core::BlockHash,
) -> Result<zinder_testkit::JsonRpcTestServer> {
    use serde_json::json;
    use zinder_testkit::{RpcReply, method};
    let upgrades: serde_json::Map<String, serde_json::Value> = schedule
        .activations()
        .iter()
        .map(|activation| {
            (
                zinder_core::wire::encode_branch_id_hex(activation.branch_id),
                json!({"name": activation.name, "status": "active",
                    "activationheight": activation.activation_height.value()}),
            )
        })
        .collect();
    zinder_testkit::JsonRpcTestServer::start([
        method("rpc.discover")
            .reply_with(|_| RpcReply::result(json!({"methods": [{"name": "sendrawtransaction"}]}))),
        method("getblockchaininfo")
            .reply_with(move |_| RpcReply::result(json!({ "upgrades": upgrades }))),
        method("getblockhash").reply_with(move |_| {
            RpcReply::result(json!(zinder_core::wire::encode_rpc_block_hash_hex(tip)))
        }),
    ])
}

#[tokio::test]
async fn node_admission_checks_the_reader_the_request_captured() -> Result<()> {
    let schedule = one_upgrade_per_height()?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(12);
    let sibling = chain.fork_at(BlockHeight::new(12))?.extend_blocks(1);
    let open = |chain: &ChainFixture| -> Result<_> {
        let mut store = WalletServingStoreFixture::from_chain(chain, &schedule)?;
        let reader = Arc::new(RocksDbCanonicalReader::new(store.take_readers()?.0)?);
        Ok((store, reader))
    };
    let (_captured_store, captured) = open(&chain)?;
    let (_sibling_store, published) = open(&sibling)?;
    let request_target = super::VerifiedNodeTarget::of(&captured)?;
    let published_target = super::VerifiedNodeTarget::of(&published)?;
    // The reader the request captured is replaced by a sibling, and the node
    // follows the sibling.
    let slot = Arc::new(ArcSwap::from(Arc::clone(&published)));
    let tip = sibling
        .blocks()
        .last()
        .ok_or_else(|| eyre!("missing sibling tip"))?
        .hash;
    let node = node_holding(&schedule, tip)?;
    let capture = Arc::clone(&slot);
    let query = LightWalletNodeQuery::new(
        move || capture.load_full(),
        ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            node.url(),
            NodeAuth::None,
            Duration::from_secs(2),
        )?,
        Duration::from_secs(1),
        NonZeroU32::MIN,
    );

    assert!(
        query.verified_node(&request_target).await.is_err(),
        "admitted a node that does not hold the captured reader's tip"
    );
    query.verified_node(&published_target).await?;
    Ok(())
}
