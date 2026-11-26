use std::{num::NonZeroU64, sync::Arc, time::Duration};

use eyre::{Result, eyre};
use metrics_exporter_prometheus::PrometheusBuilder;
use parking_lot::{Condvar, Mutex};
use serde_json::json;
use tokio::{sync::mpsc, task::JoinSet, time::Instant};
use zinder_core::{
    BlockHash, BlockHeight, SerializedBytesDigest, TransactionId, TransactionLocation,
    wire::encode_rpc_block_hash_hex,
};
use zinder_source::{NodeAuth, NodeCapability, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_testkit::{JsonRpcTestServer, RpcReply, method};

use super::*;

fn config(max_concurrent: u32, wait_ms: u64, waiters_per_slot: u32) -> Result<ForwardedReadConfig> {
    Ok(ForwardedReadConfig::resolve(
        Some(max_concurrent),
        Some(wait_ms),
        Some(waiters_per_slot),
        None,
    )?)
}

#[test]
fn the_default_budget_reserves_a_floor_per_kind_and_shares_the_rest() {
    let (shares, elastic) = node_call_shares(ForwardedReadConfig::default());
    let split: Vec<_> = shares
        .iter()
        .map(|share| (share.label, share.reserved, share.cap, share.max_waiters))
        .collect();
    assert_eq!(
        split,
        [
            ("transaction", 12, 24, 1_536),
            ("utxo", 4, 8, 512),
            ("history", 4, 16, 1_024),
        ]
    );
    assert_eq!(elastic, 12);
}

#[test]
fn every_budget_keeps_a_floor_per_kind_and_spends_exactly_its_slots() -> Result<()> {
    for budget in 1..=MAX_FORWARDED_READ_MAX_CONCURRENT.get() {
        let (shares, elastic) = node_call_shares(config(budget, 0, 1)?);
        let reserved: u32 = shares.iter().map(|share| share.reserved).sum();
        assert_eq!(reserved + elastic, budget, "budget {budget}");
        for share in shares {
            assert!(share.cap >= 1, "budget {budget}: {share:?}");
            assert!(
                share.cap <= share.reserved + elastic,
                "budget {budget}: {share:?}"
            );
            assert!(
                budget < 3 || share.reserved >= 1,
                "budget {budget}: {share:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn settings_outside_their_ranges_are_refused() {
    assert_eq!(
        ForwardedReadConfig::resolve(Some(0), None, None, None),
        Err(ForwardedReadConfigError::MaxConcurrent)
    );
    assert_eq!(
        ForwardedReadConfig::resolve(Some(257), None, None, None),
        Err(ForwardedReadConfigError::MaxConcurrent)
    );
    assert_eq!(
        ForwardedReadConfig::resolve(None, Some(30_001), None, None),
        Err(ForwardedReadConfigError::Wait)
    );
    assert_eq!(
        ForwardedReadConfig::resolve(None, None, Some(257), None),
        Err(ForwardedReadConfigError::WaitersPerSlot)
    );
    assert_eq!(
        ForwardedReadConfig::resolve(
            None,
            None,
            None,
            Some(MAX_FORWARDED_TRANSACTION_CACHE_BYTES + 1)
        ),
        Err(ForwardedReadConfigError::TransactionCacheBytes)
    );
    assert_eq!(
        ForwardedReadConfig::resolve(None, Some(0), None, Some(0))
            .map(|config| (config.wait(), config.transaction_cache_bytes())),
        Ok((Duration::ZERO, 0))
    );
}

fn single_slot_pool(max_waiters: usize) -> Arc<SlotPool> {
    SlotPool::new(
        &[ClassShare {
            label: "transaction",
            reserved: 1,
            cap: 1,
            max_waiters,
        }],
        0,
    )
}

async fn wait_for_counts(pool: &SlotPool, expected: (u32, usize)) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.counts(0) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| {
        eyre!(
            "pool never reached {expected:?}; it holds {:?}",
            pool.counts(0)
        )
    })
}

#[tokio::test(start_paused = true)]
async fn waiting_reads_are_admitted_in_arrival_order() -> Result<()> {
    let pool = single_slot_pool(4);
    let deadline = Instant::now() + Duration::from_secs(10);
    let held = pool
        .acquire(0, deadline)
        .await
        .map_err(|_| eyre!("refused"))?;
    let (admitted, mut order) = mpsc::unbounded_channel();
    let mut waiters = JoinSet::new();
    for name in ["first", "second", "third"] {
        let (waiter_pool, admitted) = (Arc::clone(&pool), admitted.clone());
        waiters.spawn(async move {
            let slot = waiter_pool.acquire(0, deadline).await;
            let _ = admitted.send(name);
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(slot);
        });
        wait_for_counts(&pool, (1, waiters.len())).await?;
    }
    drop(held);
    while waiters.join_next().await.is_some() {}
    let mut admitted_order = Vec::new();
    while let Ok(name) = order.try_recv() {
        admitted_order.push(name);
    }
    assert_eq!(admitted_order, ["first", "second", "third"]);
    assert_eq!(pool.counts(0), (0, 0));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_full_queue_refuses_at_once_and_an_expired_wait_is_refused() -> Result<()> {
    let pool = single_slot_pool(1);
    let deadline = Instant::now() + Duration::from_secs(1);
    let _held = pool
        .acquire(0, deadline)
        .await
        .map_err(|_| eyre!("refused"))?;
    let waiting = tokio::spawn({
        let pool = Arc::clone(&pool);
        async move { pool.acquire(0, deadline).await.map(drop) }
    });
    wait_for_counts(&pool, (1, 1)).await?;

    assert_eq!(
        pool.acquire(0, deadline).await.map(drop),
        Err(RefusalReason::WaitersFull)
    );
    assert_eq!(waiting.await?, Err(RefusalReason::WaitTimeout));
    assert_eq!(pool.counts(0), (1, 0));
    assert_eq!(
        pool.acquire(0, Instant::now()).await.map(drop),
        Err(RefusalReason::WaitTimeout)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_waiter_gives_back_its_place_and_any_slot_it_was_handed() -> Result<()> {
    let pool = single_slot_pool(1);
    let deadline = Instant::now() + Duration::from_secs(10);
    let held = pool
        .acquire(0, deadline)
        .await
        .map_err(|_| eyre!("refused"))?;
    let waiting = tokio::spawn({
        let pool = Arc::clone(&pool);
        async move { pool.acquire(0, deadline).await.map(drop) }
    });
    wait_for_counts(&pool, (1, 1)).await?;
    waiting.abort();
    assert!(waiting.await.is_err_and(|error| error.is_cancelled()));
    assert_eq!(pool.counts(0), (1, 0));

    // A slot handed to a waiter that leaves before it runs returns to the pool.
    let mut handed = Box::pin(pool.acquire(0, deadline));
    assert!(futures_util::poll!(&mut handed).is_pending());
    drop(held);
    assert_eq!(pool.counts(0), (1, 0));
    drop(handed);
    assert_eq!(pool.counts(0), (0, 0));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn address_history_cannot_take_the_transaction_floor() -> Result<()> {
    let limit = ForwardedReadLimit::default();
    let now = Instant::now();
    let mut held = Vec::new();
    while let Ok(slot) = limit.node_call_slot(ForwardedReadKind::History, now).await {
        held.push(slot);
    }
    assert_eq!(
        held.len(),
        16,
        "history holds its cap of reserved and shared slots"
    );
    for _ in 0..12 {
        held.push(
            limit
                .node_call_slot(ForwardedReadKind::Transaction, now)
                .await?,
        );
    }
    for _ in 0..4 {
        held.push(limit.node_call_slot(ForwardedReadKind::Utxo, now).await?);
    }
    assert!(
        limit
            .node_call_slot(ForwardedReadKind::Transaction, now)
            .await
            .is_err(),
        "the shared slots are all taken"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn returning_a_shared_slot_rotates_among_waiting_kinds_instead_of_reclaiming() -> Result<()> {
    let limit = ForwardedReadLimit::default();
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);

    let mut history_slots = Vec::new();
    for _ in 0..16 {
        history_slots.push(
            limit
                .node_call_slot(ForwardedReadKind::History, now)
                .await?,
        );
    }
    let mut tx_slots = Vec::new();
    for _ in 0..12 {
        tx_slots.push(
            limit
                .node_call_slot(ForwardedReadKind::Transaction, now)
                .await?,
        );
    }

    // Both history and transaction have queued waiters while history holds
    // all 12 shared slots (16 active history, 12 active transactions).
    let (admitted, mut order) = mpsc::unbounded_channel();
    let mut queued = JoinSet::new();
    for i in 0..4 {
        let (l, tx) = (limit.clone(), admitted.clone());
        queued.spawn(async move {
            let slot = l.node_call_slot(ForwardedReadKind::History, deadline).await;
            let _ = tx.send((ForwardedReadKind::History, i));
            slot
        });
    }
    for i in 0..4 {
        let (l, tx) = (limit.clone(), admitted.clone());
        queued.spawn(async move {
            let slot = l
                .node_call_slot(ForwardedReadKind::Transaction, deadline)
                .await;
            let _ = tx.send((ForwardedReadKind::Transaction, i));
            slot
        });
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while limit.node_call_counts(ForwardedReadKind::History) != (16, 4)
            || limit.node_call_counts(ForwardedReadKind::Transaction) != (12, 4)
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    // Release 4 shared slots from history (16 -> 12). Rotation must hand
    // shared slots to transaction waiters rather than letting history retake all 4.
    let mut returned = Vec::new();
    for _ in 0..4 {
        drop(history_slots.pop());
        let (kind, _) = tokio::time::timeout(Duration::from_secs(5), order.recv())
            .await?
            .ok_or_else(|| eyre!("expected waiter admission after history release"))?;
        returned.push(kind);
    }
    assert!(
        returned.contains(&ForwardedReadKind::Transaction),
        "transaction waiters were starved when history released shared slots: {returned:?}"
    );
    let tx_admitted = returned
        .iter()
        .filter(|kind| **kind == ForwardedReadKind::Transaction)
        .count();
    assert!(
        tx_admitted >= 2,
        "expected fair rotation across waiting kinds, got {returned:?}"
    );

    // Dropping below history's reserved floor (4 -> 3) must admit a history waiter directly.
    drop(history_slots);
    assert_eq!(tx_slots.len(), 12);
    drop(tx_slots);
    drop(returned);
    while let Some(res) = queued.join_next().await {
        res??;
    }
    assert_eq!(limit.node_call_counts(ForwardedReadKind::History), (0, 0));
    assert_eq!(
        limit.node_call_counts(ForwardedReadKind::Transaction),
        (0, 0)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn gauges_return_to_zero_after_admission_refusal_and_cancellation() -> Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let limit = ForwardedReadLimit::new(config(3, 1_000, 1)?);
    let rendered = handle.render();
    for kind in [
        "transaction",
        "utxo",
        "history",
        "utxo_response",
        "history_stream",
    ] {
        assert!(
            rendered.contains(&format!(
                "zinder_forwarded_read_active{{kind=\"{kind}\"}} 0"
            )),
            "{rendered}"
        );
        assert!(
            rendered.contains(&format!(
                "zinder_forwarded_read_refusals_total{{kind=\"{kind}\",reason=\"waiters_full\"}} 0"
            )),
            "{rendered}"
        );
    }

    let deadline = Instant::now() + Duration::from_secs(1);
    let held = limit
        .node_call_slot(ForwardedReadKind::Transaction, deadline)
        .await?;
    let queued = limit.node_call_slot(ForwardedReadKind::Transaction, deadline);
    let mut queued = Box::pin(queued);
    assert!(futures_util::poll!(&mut queued).is_pending());
    assert!(
        limit
            .node_call_slot(ForwardedReadKind::Transaction, deadline)
            .await
            .is_err()
    );
    let rendered = handle.render();
    assert!(rendered.contains("zinder_forwarded_read_active{kind=\"transaction\"} 1"));
    assert!(rendered.contains("zinder_forwarded_read_queued{kind=\"transaction\"} 1"));
    drop(queued);
    drop(held);
    let expired = limit
        .node_call_slot(ForwardedReadKind::Transaction, Instant::now())
        .await?;
    drop(expired);

    let rendered = handle.render();
    assert!(rendered.contains("zinder_forwarded_read_active{kind=\"transaction\"} 0"));
    assert!(rendered.contains("zinder_forwarded_read_queued{kind=\"transaction\"} 0"));
    assert!(rendered.contains(
        "zinder_forwarded_read_refusals_total{kind=\"transaction\",reason=\"waiters_full\"} 1"
    ));
    for outcome in ["admitted", "refused", "cancelled"] {
        assert!(
            rendered.contains(&format!(
                "zinder_forwarded_read_wait_duration_seconds_count{{kind=\"transaction\",outcome=\"{outcome}\"}}"
            )),
            "missing {outcome}: {rendered}"
        );
    }
    Ok(())
}

const TIP_HEIGHT: u32 = 7;

fn mined(byte: u8, bytes: &[u8]) -> MinedTransactionDigest {
    MinedTransactionDigest {
        location: TransactionLocation::new(
            TransactionId::from_bytes([byte; 32]),
            BlockHeight::new(TIP_HEIGHT),
            BlockHash::from_bytes([0x42; 32]),
            0,
        ),
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(bytes),
    }
}

/// A node whose `getrawtransaction` reports every transaction mined in the
/// block [`mined`] names with the bytes `55`, after `gate` opens.
struct TransactionNode {
    server: JsonRpcTestServer,
    calls: mpsc::UnboundedReceiver<String>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}

impl TransactionNode {
    fn start(open: bool, reply: fn(&serde_json::Value) -> RpcReply) -> Result<Self> {
        let (entered, calls) = mpsc::unbounded_channel();
        let gate = Arc::new((Mutex::new(open), Condvar::new()));
        let node_gate = Arc::clone(&gate);
        let server =
            JsonRpcTestServer::start([method("getrawtransaction").reply_with(move |params| {
                let _ = entered.send(params[0].as_str().unwrap_or_default().to_owned());
                let (released, ready) = &*node_gate;
                let mut released = released.lock();
                ready.wait_while_for(
                    &mut released,
                    |released| !*released,
                    Duration::from_secs(20),
                );
                reply(&params[0])
            })])?;
        Ok(Self {
            server,
            calls,
            gate,
        })
    }

    fn source(&self) -> Result<ZebraJsonRpcSource> {
        Ok(ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashRegtest,
            self.server.url(),
            NodeAuth::None,
            Duration::from_secs(20),
        )?)
    }

    fn source_with_max_response_bytes(
        &self,
        max_response_bytes: NonZeroU64,
    ) -> Result<ZebraJsonRpcSource> {
        Ok(ZebraJsonRpcSource::with_options(
            zinder_core::Network::ZcashRegtest,
            self.server.url(),
            NodeAuth::None,
            ZebraJsonRpcSourceOptions {
                request_timeout: Duration::from_secs(20),
                max_response_bytes,
                ..ZebraJsonRpcSourceOptions::default()
            },
        )?)
    }

    fn open(&self) {
        let (released, ready) = &*self.gate;
        *released.lock() = true;
        ready.notify_all();
    }

    fn calls(&mut self) -> Vec<String> {
        let mut calls = Vec::new();
        while let Ok(call) = self.calls.try_recv() {
            calls.push(call);
        }
        calls
    }
}

fn mined_reply(txid: &serde_json::Value) -> RpcReply {
    RpcReply::result(json!({
        "txid": txid, "hex": "55", "height": TIP_HEIGHT,
        "blockhash": encode_rpc_block_hash_hex(BlockHash::from_bytes([0x42; 32])),
    }))
}

#[tokio::test]
async fn concurrent_lookups_share_one_node_call_and_later_ones_hit_the_cache() -> Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let mut node = TransactionNode::start(false, mined_reply)?;
    let source = node.source()?;
    let limit = ForwardedReadLimit::default();
    let transaction = mined(1, &[0x55]);
    let deadline = limit.deadline();
    let lookup = || {
        let (limit, source) = (limit.clone(), source.clone());
        async move {
            limit
                .mined_transaction_bytes(
                    &source,
                    transaction,
                    ForwardedReadKind::Transaction,
                    deadline,
                )
                .await
        }
    };
    let leader = tokio::spawn(lookup());
    let follower = tokio::spawn(lookup());
    tokio::time::timeout(Duration::from_secs(5), node.calls.recv())
        .await?
        .ok_or_else(|| eyre!("node never received the lookup"))?;
    wait_for_metric(
        &handle,
        "zinder_forwarded_transaction_cache_lookups_total{outcome=\"coalesced\"} 1",
    )
    .await?;
    assert_eq!(limit.inner.transactions.running(), 1);
    node.open();
    assert_eq!(leader.await??.as_ref(), [0x55]);
    assert_eq!(follower.await??.as_ref(), [0x55]);
    assert_eq!(lookup().await?.as_ref(), [0x55]);
    assert_eq!(
        limit.cached_transaction(&transaction).as_deref(),
        Some([0x55].as_slice())
    );
    assert!(
        node.calls().is_empty(),
        "a cached or coalesced lookup reached the node"
    );
    assert_eq!(
        limit.node_call_counts(ForwardedReadKind::Transaction),
        (0, 0)
    );
    let rendered = handle.render();
    assert!(
        rendered.contains("zinder_forwarded_transaction_cache_lookups_total{outcome=\"miss\"} 1")
    );
    assert!(
        rendered
            .contains("zinder_forwarded_transaction_cache_lookups_total{outcome=\"coalesced\"} 1")
    );
    assert!(
        rendered.contains("zinder_forwarded_transaction_cache_lookups_total{outcome=\"hit\"} 2")
    );
    Ok(())
}

#[tokio::test]
async fn a_cancelled_leader_leaves_its_node_call_to_the_lookups_that_joined_it() -> Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let mut node = TransactionNode::start(false, mined_reply)?;
    let source = node.source()?;
    let limit = ForwardedReadLimit::default();
    let transaction = mined(2, &[0x55]);
    let deadline = limit.deadline();
    let lookup = || {
        let (limit, source) = (limit.clone(), source.clone());
        tokio::spawn(async move {
            limit
                .mined_transaction_bytes(&source, transaction, ForwardedReadKind::History, deadline)
                .await
        })
    };
    let leader = lookup();
    tokio::time::timeout(Duration::from_secs(5), node.calls.recv())
        .await?
        .ok_or_else(|| eyre!("node never received the lookup"))?;
    let follower = lookup();
    wait_for_metric(
        &handle,
        "zinder_forwarded_transaction_cache_lookups_total{outcome=\"coalesced\"} 1",
    )
    .await?;
    leader.abort();
    assert!(leader.await.is_err_and(|error| error.is_cancelled()));
    node.open();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), follower)
            .await???
            .as_ref(),
        [0x55]
    );
    assert!(
        node.calls().is_empty(),
        "the follower started a second node call"
    );
    assert_eq!(limit.inner.transactions.running(), 0);
    Ok(())
}

/// Waits until the rendered metrics contain `needle`.
async fn wait_for_metric(
    handle: &metrics_exporter_prometheus::PrometheusHandle,
    needle: &str,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !handle.render().contains(needle) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| eyre!("metrics never showed {needle}"))
}

#[tokio::test]
async fn mismatched_bytes_fail_closed_and_are_never_cached() -> Result<()> {
    let mut node = TransactionNode::start(true, mined_reply)?;
    let source = node.source()?;
    let limit = ForwardedReadLimit::default();
    let transaction = mined(3, &[0x66]);
    for _ in 0..2 {
        let outcome = limit
            .mined_transaction_bytes(
                &source,
                transaction,
                ForwardedReadKind::Transaction,
                limit.deadline(),
            )
            .await;
        assert!(matches!(
            outcome,
            Err(ForwardedTransactionError::DigestMismatch)
        ));
        assert!(matches!(
            QueryError::from(ForwardedTransactionError::DigestMismatch),
            QueryError::ArtifactCorrupt { .. }
        ));
    }
    assert_eq!(node.calls().len(), 2);
    assert!(limit.cached_transaction(&transaction).is_none());
    Ok(())
}

#[tokio::test]
async fn another_block_is_outside_the_served_view() -> Result<()> {
    let node = TransactionNode::start(true, |txid| {
        RpcReply::result(json!({
            "txid": txid, "hex": "55", "height": TIP_HEIGHT,
            "blockhash": encode_rpc_block_hash_hex(BlockHash::from_bytes([0x43; 32])),
        }))
    })?;
    let limit = ForwardedReadLimit::default();
    let outcome = limit
        .mined_transaction_bytes(
            &node.source()?,
            mined(4, &[0x55]),
            ForwardedReadKind::Transaction,
            limit.deadline(),
        )
        .await;
    assert!(matches!(
        outcome,
        Err(ForwardedTransactionError::OutsideServedView)
    ));
    Ok(())
}

#[tokio::test]
async fn a_rate_limited_node_call_is_counted_and_keeps_its_class() -> Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let node = TransactionNode::start(true, |_| RpcReply::rate_limited())?;
    let limit = ForwardedReadLimit::default();
    let outcome = limit
        .mined_transaction_bytes(
            &node.source()?,
            mined(5, &[0x55]),
            ForwardedReadKind::Transaction,
            limit.deadline(),
        )
        .await
        .map_err(QueryError::from);
    assert!(
        matches!(
            outcome,
            Err(QueryError::Node(SourceError::NodeRateLimited { .. }))
        ),
        "{outcome:?}"
    );
    assert!(handle.render().contains(
        "zinder_forwarded_read_refusals_total{kind=\"transaction\",reason=\"node_rate_limited\"} 1"
    ));
    assert_eq!(
        limit.node_call_counts(ForwardedReadKind::Transaction),
        (0, 0)
    );
    Ok(())
}

#[test]
fn a_shared_source_error_keeps_its_class_for_every_caller() {
    let hex_error = hex::FromHexError::InvalidHexCharacter { c: 'z', index: 0 };
    let samples = [
        SourceError::NodeRateLimited {
            operation: "getrawtransaction",
        },
        SourceError::SourceProtocolMismatch {
            reason: "bad response",
        },
        SourceError::InvalidRawTransactionHex { source: hex_error },
        SourceError::SourceResponseTooLarge {
            operation: "getrawtransaction",
            max_response_bytes: 256,
        },
        SourceError::NodeResponseTooLarge {
            operation: "getaddressutxos",
            reason: "too large".to_owned(),
        },
        SourceError::TransparentAddressRowLimitExceeded {
            operation: "getaddressutxos",
            max_rows: 10,
        },
        SourceError::TransparentAddressIndexesUnavailable {
            reason: "disabled".to_owned(),
        },
        SourceError::BlockCommittedDuringRead {
            operation: "getaddressutxos",
            reason: "committing".to_owned(),
        },
        SourceError::ChainTipChangedDuringFetch {
            operation: "getaddresstxids",
            attempts: 3,
        },
        SourceError::NodeCapabilityMissing {
            capability: NodeCapability::TransactionBroadcast,
        },
        SourceError::HistoricalTreeStateUnavailable {
            height: BlockHeight::new(1),
            reason: "pruned".to_owned(),
        },
        SourceError::HistoricalBlockBodyUnavailable {
            height: BlockHeight::new(1),
            reason: "pruned".to_owned(),
        },
        SourceError::HistoricalSubtreeRootsUnavailable {
            protocol: zinder_core::ShieldedProtocol::Sapling,
            start_index: zinder_core::SubtreeRootIndex::new(0),
            reason: "pruned".to_owned(),
        },
    ];
    for sample in samples {
        let expected_class = sample.upstream_classification();
        let shared = Arc::new(sample);
        let held = Arc::clone(&shared);
        let first = owned_source_error(shared);
        let second = owned_source_error(held);
        assert_eq!(first.upstream_classification(), expected_class);
        assert_eq!(second.upstream_classification(), expected_class);
        assert_eq!(
            std::mem::discriminant(&first),
            std::mem::discriminant(&second)
        );
    }
}

#[tokio::test]
async fn concurrent_malformed_hex_preserves_invalid_hex_for_every_caller() -> Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let pair = coalesced_error_pair(
        None,
        |txid| {
            RpcReply::result(json!({
                "txid": txid,
                "hex": "zz",
                "height": TIP_HEIGHT,
                "blockhash": encode_rpc_block_hash_hex(BlockHash::from_bytes([0x42; 32])),
            }))
        },
        7,
        &handle,
    )
    .await?;
    for err in pair {
        assert!(
            matches!(
                QueryError::from(err),
                QueryError::Node(SourceError::InvalidRawTransactionHex { .. })
            ),
            "expected InvalidRawTransactionHex for every coalesced caller"
        );
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_oversized_response_preserves_size_error_for_every_caller() -> Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let cap = NonZeroU64::new(256).ok_or_else(|| eyre!("non-zero"))?;
    let pair = coalesced_error_pair(
        Some(cap),
        |txid| {
            RpcReply::result(json!({
                "txid": txid,
                "hex": "55".repeat(512),
                "height": TIP_HEIGHT,
                "blockhash": encode_rpc_block_hash_hex(BlockHash::from_bytes([0x42; 32])),
            }))
        },
        8,
        &handle,
    )
    .await?;
    for err in pair {
        assert!(
            matches!(
                QueryError::from(err),
                QueryError::Node(SourceError::SourceResponseTooLarge { .. })
            ),
            "expected SourceResponseTooLarge for every coalesced caller"
        );
    }
    Ok(())
}

async fn coalesced_error_pair(
    max_response_bytes: Option<NonZeroU64>,
    reply: fn(&serde_json::Value) -> RpcReply,
    tx_byte: u8,
    handle: &metrics_exporter_prometheus::PrometheusHandle,
) -> Result<[ForwardedTransactionError; 2]> {
    let mut node = TransactionNode::start(false, reply)?;
    let source = match max_response_bytes {
        Some(limit_bytes) => node.source_with_max_response_bytes(limit_bytes)?,
        None => node.source()?,
    };
    let limit = ForwardedReadLimit::default();
    let transaction = mined(tx_byte, &[0x55]);
    let deadline = limit.deadline();
    let lookup = || {
        let (limit, source) = (limit.clone(), source.clone());
        tokio::spawn(async move {
            limit
                .mined_transaction_bytes(
                    &source,
                    transaction,
                    ForwardedReadKind::Transaction,
                    deadline,
                )
                .await
        })
    };
    let leader = lookup();
    tokio::time::timeout(Duration::from_secs(5), node.calls.recv())
        .await?
        .ok_or_else(|| eyre!("node never received the lookup"))?;
    let follower = lookup();
    wait_for_metric(
        handle,
        "zinder_forwarded_transaction_cache_lookups_total{outcome=\"coalesced\"} 1",
    )
    .await?;
    node.open();
    let (first_outcome, second_outcome) = tokio::join!(leader, follower);
    let first_err = first_outcome?
        .err()
        .ok_or_else(|| eyre!("expected leader failure"))?;
    let second_err = second_outcome?
        .err()
        .ok_or_else(|| eyre!("expected follower failure"))?;
    assert!(
        node.calls().is_empty(),
        "follower started a second node call"
    );
    Ok([first_err, second_err])
}

#[test]
fn the_cache_evicts_by_bytes_and_recency_and_separates_digests() {
    use transaction_bytes::TransactionCache;

    let mut cache = TransactionCache::default();
    let key = |byte| {
        (
            TransactionId::from_bytes([1; 32]),
            SerializedBytesDigest::from_bytes([byte; 32]),
        )
    };
    cache.insert(key(1), Arc::from([1, 1]), 4);
    cache.insert(key(2), Arc::from([2, 2]), 4);
    assert_eq!(cache.get(key(1)).as_deref(), Some([1, 1].as_slice()));
    cache.insert(key(3), Arc::from([3, 3]), 4);
    assert!(cache.get(key(2)).is_none());
    assert_eq!(cache.get(key(3)).as_deref(), Some([3, 3].as_slice()));
    cache.insert(key(4), Arc::from([4; 5]), 4);
    assert!(cache.get(key(4)).is_none());
    assert_eq!(cache.bytes(), 4);
}

#[tokio::test]
async fn a_zero_byte_cache_keeps_nothing() -> Result<()> {
    let mut node = TransactionNode::start(true, mined_reply)?;
    let source = node.source()?;
    let limit = ForwardedReadLimit::new(ForwardedReadConfig::resolve(None, None, None, Some(0))?);
    let transaction = mined(6, &[0x55]);
    for _ in 0..2 {
        limit
            .mined_transaction_bytes(
                &source,
                transaction,
                ForwardedReadKind::Transaction,
                limit.deadline(),
            )
            .await?;
    }
    assert_eq!(node.calls().len(), 2);
    Ok(())
}

#[tokio::test]
async fn point_transaction_lookup_bypasses_queued_history_flight_and_shares_verified_cache()
-> Result<()> {
    let mut node = TransactionNode::start(true, mined_reply)?;
    let source = node.source()?;
    let limit = ForwardedReadLimit::new(ForwardedReadConfig::resolve(
        Some(4),
        Some(80),
        Some(4),
        None,
    )?);
    let held_a = limit
        .node_call_slot(ForwardedReadKind::History, limit.deadline())
        .await?;
    let held_b = limit
        .node_call_slot(ForwardedReadKind::History, limit.deadline())
        .await?;
    let target = mined(0x21, &[0x55]);

    let queued_history = tokio::spawn({
        let (limit, source) = (limit.clone(), source.clone());
        async move {
            limit
                .mined_transaction_bytes(
                    &source,
                    target,
                    ForwardedReadKind::History,
                    Instant::now() + Duration::from_secs(5),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while limit.node_call_counts(ForwardedReadKind::History) != (2, 1) {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let point_bytes = limit
        .mined_transaction_bytes(
            &source,
            target,
            ForwardedReadKind::Transaction,
            Instant::now() + Duration::from_millis(40),
        )
        .await?;
    assert_eq!(point_bytes.as_ref(), [0x55]);
    let history_bytes = tokio::time::timeout(Duration::from_secs(2), queued_history).await???;
    assert_eq!(history_bytes.as_ref(), [0x55]);
    assert_eq!(node.calls().len(), 1);
    assert_eq!(limit.node_call_counts(ForwardedReadKind::History), (2, 0));
    drop((held_a, held_b));

    assert_uncached_point_lookup_bypasses_queued_history(&source).await
}

async fn assert_uncached_point_lookup_bypasses_queued_history(
    source: &ZebraJsonRpcSource,
) -> Result<()> {
    let uncached = ForwardedReadLimit::new(ForwardedReadConfig::resolve(
        Some(4),
        Some(60),
        Some(4),
        Some(0),
    )?);
    let held_a = uncached
        .node_call_slot(ForwardedReadKind::History, uncached.deadline())
        .await?;
    let held_b = uncached
        .node_call_slot(ForwardedReadKind::History, uncached.deadline())
        .await?;
    let target = mined(0x22, &[0x55]);
    let queued_history = tokio::spawn({
        let (limit, source) = (uncached.clone(), source.clone());
        async move {
            limit
                .mined_transaction_bytes(
                    &source,
                    target,
                    ForwardedReadKind::History,
                    limit.deadline(),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while uncached.node_call_counts(ForwardedReadKind::History) != (2, 1) {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let point_bytes = uncached
        .mined_transaction_bytes(
            source,
            target,
            ForwardedReadKind::Transaction,
            uncached.deadline(),
        )
        .await?;
    assert_eq!(point_bytes.as_ref(), [0x55]);
    let history_refused = queued_history.await?;
    assert!(matches!(
        history_refused,
        Err(ForwardedTransactionError::Refused(refused)) if refused.kind() == "history"
    ));
    drop((held_a, held_b));
    Ok(())
}

#[tokio::test]
async fn unrelated_cache_insertions_preserve_arrival_order_for_waiting_lookup() -> Result<()> {
    let mut node = TransactionNode::start(true, mined_reply)?;
    let source = node.source()?;
    let limit = ForwardedReadLimit::new(ForwardedReadConfig::resolve(
        Some(4),
        Some(5_000),
        Some(4),
        None,
    )?);
    let held_a = limit
        .node_call_slot(ForwardedReadKind::History, limit.deadline())
        .await?;
    let held_b = limit
        .node_call_slot(ForwardedReadKind::History, limit.deadline())
        .await?;
    let older = tokio::spawn({
        let (limit, source) = (limit.clone(), source.clone());
        async move {
            limit
                .mined_transaction_bytes(
                    &source,
                    mined(0x31, &[0x55]),
                    ForwardedReadKind::History,
                    limit.deadline(),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while limit.node_call_counts(ForwardedReadKind::History) != (2, 1) {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let younger = tokio::spawn({
        let limit = limit.clone();
        async move {
            limit
                .node_call_slot(ForwardedReadKind::History, limit.deadline())
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while limit.node_call_counts(ForwardedReadKind::History) != (2, 2) {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    for unrelated in [0x32, 0x33] {
        let bytes = limit
            .mined_transaction_bytes(
                &source,
                mined(unrelated, &[0x55]),
                ForwardedReadKind::Transaction,
                limit.deadline(),
            )
            .await?;
        assert_eq!(bytes.as_ref(), [0x55]);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    drop(held_a);
    let older_bytes = tokio::time::timeout(Duration::from_secs(2), older).await???;
    assert_eq!(older_bytes.as_ref(), [0x55]);
    let younger_slot = tokio::time::timeout(Duration::from_secs(2), younger).await???;
    drop((younger_slot, held_b));
    assert_eq!(node.calls().len(), 3);
    assert_eq!(limit.node_call_counts(ForwardedReadKind::History), (0, 0));
    Ok(())
}
