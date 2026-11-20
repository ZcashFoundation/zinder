//! Qualification reads at the canonical and forwarding boundaries.

use std::{future::Future, sync::Arc, time::Duration};

use eyre::{Result, eyre};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Status};
use tonic_types::StatusExt as _;
use zinder_compat_lightwalletd::IngestControlMempoolSurface;
use zinder_core::{
    BlockHeight, CompactTransactionData, MempoolEntry, MempoolObservation, Network,
    RawTransactionBytes, ShieldedProtocol, TransactionId,
    wire::{
        encode_internal_transaction_id, encode_rpc_block_hash_bytes, encode_rpc_block_hash_hex,
    },
};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_server::CompactTxStreamer as _,
};
use zinder_query::{CanonicalReader as _, WalletServingReadiness};
use zinder_runtime::{Readiness, ReadinessCause, ReadinessInput, ReadinessState};
use zinder_store::RawBlobRetention;
use zinder_testkit::{
    ChainFixture, FixtureTransactionRows, IngestControlFixture, RpcReply,
    commitment_tree_checkpoints, method, sample_regtest_upgrade_activations, with_note_commitments,
};

use super::Fixture;

fn transaction(id: TransactionId) -> Request<lightwalletd::TxFilter> {
    Request::new(lightwalletd::TxFilter {
        hash: encode_internal_transaction_id(id).to_vec(),
        ..Default::default()
    })
}

fn send() -> Request<lightwalletd::RawTransaction> {
    Request::new(lightwalletd::RawTransaction {
        data: vec![0x11],
        height: 0,
    })
}

fn node_unavailable(status: &Status) {
    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(
        status
            .get_error_details()
            .error_info()
            .map(|detail| detail.reason.as_str()),
        Some("NODE_UNAVAILABLE")
    );
}

#[tokio::test]
async fn get_transaction_forwards_mined_pending_and_missing_without_retention() -> Result<()> {
    let fixture = Fixture::new([method("getrawtransaction").reply_with(|params| {
        if params[0] == "03".repeat(32) {
            RpcReply::error_with_code(-5, "No such mempool or main chain transaction")
        } else if params[1] == 0 {
            RpcReply::result(json!("1221"))
        } else if params[0] == "01".repeat(32) {
            RpcReply::result(json!({"height": 1, "blockhash": "45".repeat(32)}))
        } else {
            RpcReply::result(json!({}))
        }
    })])?;
    for (id, height) in [(1, 1), (2, 0)] {
        let reply = fixture
            .adapter
            .get_transaction(transaction(TransactionId::from_bytes([id; 32])))
            .await?
            .into_inner();
        assert_eq!((reply.height, reply.data), (height, vec![0x12, 0x21]));
    }
    let id = TransactionId::from_bytes([3; 32]);
    assert_eq!(
        fixture
            .adapter
            .get_transaction(transaction(id))
            .await
            .err()
            .ok_or_else(|| eyre!("expected an RPC refusal"))?
            .code(),
        Code::NotFound
    );
    drop(fixture.node);
    node_unavailable(
        &fixture
            .adapter
            .get_transaction(transaction(id))
            .await
            .err()
            .ok_or_else(|| eyre!("expected an RPC refusal"))?,
    );
    Ok(())
}

#[tokio::test]
async fn retained_mined_and_ingest_pending_transactions_survive_a_stopped_node() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(1)
        .with_raw_blob_retention(RawBlobRetention::Transactions);
    let block = chain
        .blocks()
        .first()
        .ok_or_else(|| eyre!("missing block"))?;
    let mined = TransactionId::from_bytes([0x55; 32]);
    let rows = FixtureTransactionRows::coinbase(mined, block.height, block.hash, vec![0x55]);
    let fixture = Fixture::from_chain(
        &chain.with_transaction_rows(rows),
        &super::one_upgrade_per_height()?,
        [],
        Duration::from_secs(2),
    )?;
    let epoch = fixture.slot.capture().chain_epoch();
    let pending = MempoolEntry::new(
        TransactionId::from_bytes([0x22; 32]),
        None,
        RawTransactionBytes::new(vec![0x22]),
        CompactTransactionData::default(),
        MempoolObservation {
            first_seen_chain_epoch: epoch,
            first_seen_unix_millis: epoch.created_at,
        },
    )?;
    let control = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    control.set_mempool_entries(std::slice::from_ref(&pending));
    let adapter = fixture
        .adapter
        .with_mempool_surface(Arc::new(IngestControlMempoolSurface::new(
            control.endpoint().to_owned(),
        )));
    drop(fixture.node);
    for (id, height, bytes) in [(mined, 1, 0x55), (pending.transaction_id(), 0, 0x22)] {
        let reply = adapter.get_transaction(transaction(id)).await?.into_inner();
        assert_eq!((reply.height, reply.data), (height, vec![bytes]));
    }
    control.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn forwarding_rechecks_the_captured_tip_and_recovers_after_a_fork() -> Result<()> {
    let schedule = super::one_upgrade_per_height()?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(12);
    let extended = chain.clone().extend_blocks(1);
    let initial = chain
        .blocks()
        .last()
        .ok_or_else(|| eyre!("missing initial tip"))?;
    let next = extended
        .blocks()
        .last()
        .ok_or_else(|| eyre!("missing appended tip"))?;
    let fixture = Fixture::from_chain(
        &chain,
        &schedule,
        [
            method("getblockhash").reply(RpcReply::result(json!(encode_rpc_block_hash_hex(
                initial.hash
            )))),
            method("getblockhash").reply(RpcReply::result(json!("fe".repeat(32)))),
            method("getblockhash").reply(RpcReply::result(json!("fe".repeat(32)))),
            method("getblockhash").reply(RpcReply::result(json!(encode_rpc_block_hash_hex(
                next.hash
            )))),
            method("sendrawtransaction").reply_with(|_| RpcReply::result(json!("01".repeat(32)))),
        ],
        Duration::from_secs(2),
    )?;
    fixture.adapter.send_transaction(send()).await?;
    let mut store = zinder_testkit::WalletServingStoreFixture::from_chain_after_live_append(
        &extended, &schedule,
    )?;
    drop(
        fixture
            .slot
            .publish(Arc::new(zinder_query::RocksDbCanonicalReader::new(
                store.take_readers()?.0,
            )?)),
    );
    node_unavailable(
        &fixture
            .adapter
            .send_transaction(send())
            .await
            .err()
            .ok_or_else(|| eyre!("expected an RPC refusal"))?,
    );
    node_unavailable(
        &fixture
            .adapter
            .get_transaction(transaction(TransactionId::from_bytes([1; 32])))
            .await
            .err()
            .ok_or_else(|| eyre!("expected an RPC refusal"))?,
    );
    assert!(fixture.node.requests_for("getrawtransaction")?.is_empty());
    assert_eq!(fixture.node.requests_for("sendrawtransaction")?.len(), 1);
    fixture.adapter.send_transaction(send()).await?;
    assert_eq!(
        fixture
            .node
            .requests_for("getblockhash")?
            .into_iter()
            .map(|request| request.params)
            .collect::<Vec<_>>(),
        [json!([12]), json!([13]), json!([13]), json!([13])],
    );
    assert_eq!(fixture.node.requests_for("rpc.discover")?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn concurrent_forwarding_shares_discovery_and_schedule_but_checks_each_tip() -> Result<()> {
    let broadcasts = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let advertised = Arc::clone(&broadcasts);
    let fixture = Fixture::new([
        method("rpc.discover").reply_with(move |_| {
            let methods = if advertised.load(std::sync::atomic::Ordering::Relaxed) {
                json!([{"name": "getblockchaininfo"}, {"name": "sendrawtransaction"}])
            } else {
                json!([])
            };
            RpcReply::result(json!({"methods": methods}))
        }),
        method("sendrawtransaction").reply_with(|_| RpcReply::result(json!("01".repeat(32)))),
    ])?;
    let other = fixture.adapter.clone();
    let _ = tokio::try_join!(
        fixture.adapter.send_transaction(send()),
        other.send_transaction(send())
    )?;
    assert_eq!(fixture.node.requests_for("rpc.discover")?.len(), 1);
    assert_eq!(fixture.node.requests_for("getblockchaininfo")?.len(), 2);
    assert_eq!(fixture.node.requests_for("getblockhash")?.len(), 2);
    assert_eq!(fixture.node.requests_for("sendrawtransaction")?.len(), 2);
    fixture.adapter.send_transaction(send()).await?;
    assert_eq!(fixture.node.requests_for("rpc.discover")?.len(), 1);
    assert_eq!(fixture.node.requests_for("getblockchaininfo")?.len(), 2);
    assert_eq!(fixture.node.requests_for("getblockhash")?.len(), 3);

    broadcasts.store(false, std::sync::atomic::Ordering::Relaxed);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::time::resume();
    let refusal = fixture
        .adapter
        .send_transaction(send())
        .await
        .err()
        .ok_or_else(|| eyre!("an expired capability allowed forwarding"))?;
    assert_eq!(refusal.code(), Code::FailedPrecondition);
    assert_eq!(fixture.node.requests_for("rpc.discover")?.len(), 2);
    assert_eq!(fixture.node.requests_for("sendrawtransaction")?.len(), 3);

    broadcasts.store(true, std::sync::atomic::Ordering::Relaxed);
    fixture.adapter.send_transaction(send()).await?;
    assert_eq!(fixture.node.requests_for("rpc.discover")?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn a_changed_fence_during_the_node_tip_check_refuses_the_broadcast() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let schedule = super::one_upgrade_per_height()?;
    let mut initial =
        super::WalletServingStoreFixture::from_chain_after_live_append(&chain, &schedule)?;
    let mut replacement = super::WalletServingStoreFixture::from_chain_after_live_append(
        &chain.extend_blocks(1),
        &schedule,
    )?;
    check_publication_during_tip_lookup(&schedule, &mut initial, &mut replacement, false).await
}

#[tokio::test]
async fn a_changed_schedule_during_the_node_tip_check_refuses_the_broadcast() -> Result<()> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let schedule = super::one_upgrade_per_height()?;
    let mut upgrades = schedule.activations().to_vec();
    upgrades.push(zinder_core::NetworkUpgradeActivation {
        branch_id: zinder_core::ConsensusBranchId::new(0x5a1d_e008),
        activation_height: BlockHeight::new(1_000),
        name: "NU8".to_owned(),
    });
    let revised = zinder_core::NetworkUpgradeActivations::new(Network::ZcashRegtest, upgrades)?;
    let mut initial =
        super::WalletServingStoreFixture::from_chain_after_live_append(&chain, &schedule)?;
    let mut replacement = super::WalletServingStoreFixture::from_chain_after_admitted_live_append(
        &chain, &schedule, &revised,
    )?;
    check_publication_during_tip_lookup(&schedule, &mut initial, &mut replacement, true).await
}

async fn check_publication_during_tip_lookup(
    schedule: &zinder_core::NetworkUpgradeActivations,
    initial: &mut super::WalletServingStoreFixture,
    replacement: &mut super::WalletServingStoreFixture,
    same_fence: bool,
) -> Result<()> {
    let initial = Arc::new(super::RocksDbCanonicalReader::new(
        initial.take_readers()?.0,
    )?);
    let replacement = Arc::new(super::RocksDbCanonicalReader::new(
        replacement.take_readers()?.0,
    )?);
    assert_eq!(
        initial.event_fence() == replacement.event_fence(),
        same_fence
    );
    let revision = |reader: &super::RocksDbCanonicalReader| {
        reader
            .network_upgrade_schedule()
            .map(zinder_store::CanonicalActivationSchedule::revision)
    };
    assert_eq!(revision(&initial) == revision(&replacement), !same_fence);
    let old_tip = initial.event_fence().visible_tip();
    let new_tip = replacement.event_fence().visible_tip();
    let slot = super::CanonicalReaderSlot::new(initial);
    let publisher = slot.clone();
    let node = super::verifiable_node(
        schedule,
        old_tip.hash,
        [
            method("getblockhash").reply_with(move |params| {
                drop(publisher.publish(Arc::clone(&replacement)));
                let tip = if params == &json!([old_tip.height.value()]) {
                    old_tip
                } else {
                    new_tip
                };
                RpcReply::result(json!(encode_rpc_block_hash_hex(tip.hash)))
            }),
            method("sendrawtransaction").reply_with(|_| RpcReply::result(json!("01".repeat(32)))),
        ],
    )?;
    let source = super::ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        node.url(),
        super::NodeAuth::None,
        Duration::from_secs(2),
    )?;
    let adapter = super::LightwalletdGrpcAdapter::for_light_wallet_qualification(
        slot,
        source,
        super::DEFAULT_TREE_STATE_REPLAY_WAIT,
        super::DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
    );
    node_unavailable(
        &adapter
            .send_transaction(send())
            .await
            .err()
            .ok_or_else(|| eyre!("broadcast crossed a changed publication"))?,
    );
    assert!(node.requests_for("sendrawtransaction")?.is_empty());
    adapter.send_transaction(send()).await?;
    assert_eq!(node.requests_for("sendrawtransaction")?.len(), 1);
    assert_eq!(node.requests_for("rpc.discover")?.len(), 2);
    Ok(())
}

async fn assert_unleased_during<T>(
    fixture: &Fixture,
    method_name: &str,
    request: impl Future<Output = Result<T, Status>>,
) -> Result<()> {
    let reader = fixture.slot.capture();
    let mut request = std::pin::pin!(request);
    tokio::select! {
        _ = &mut request => return Err(eyre!("delayed {method_name} finished before inspection")),
        observation = tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.node.requests_for(method_name)?.is_empty() { tokio::task::yield_now().await; }
            Ok::<(),eyre::Report>(())
        }) => observation??,
    }
    assert_eq!(
        Arc::strong_count(&reader),
        2,
        "{method_name} leased a generation"
    );
    Ok(())
}

#[tokio::test]
async fn verification_lookup_and_submission_leave_generations_recyclable() -> Result<()> {
    let id = TransactionId::from_bytes([1; 32]);
    let slow = || RpcReply::Delay(Duration::from_secs(4), Box::new(RpcReply::Close));
    let fixture = Fixture::new([method("getblockhash").reply(slow())])?;
    assert_unleased_during(
        &fixture,
        "getblockhash",
        fixture.adapter.get_transaction(transaction(id)),
    )
    .await?;
    let fixture = Fixture::new([
        method("getrawtransaction").reply(slow()),
        method("sendrawtransaction").reply(slow()),
    ])?;
    assert_unleased_during(
        &fixture,
        "getrawtransaction",
        fixture.adapter.get_transaction(transaction(id)),
    )
    .await?;
    assert_unleased_during(
        &fixture,
        "sendrawtransaction",
        fixture.adapter.send_transaction(send()),
    )
    .await
}

#[tokio::test]
async fn dependency_probe_reports_a_missing_broadcast_capability_and_a_divergent_tip() -> Result<()>
{
    let fixture = Fixture::new([
        method("rpc.discover").reply(RpcReply::result(json!({"methods": []}))),
        method("getblockhash").reply_with(|_| RpcReply::result(json!("ff".repeat(32)))),
    ])?;
    let runtime = Readiness::default();
    let inputs = WalletServingReadiness::with_node_and_ingest_control(runtime.clone());
    inputs
        .canonical_admission()
        .publish_admission(ReadinessState::ready(Some(1)));
    let cancel = CancellationToken::new();
    let probe = fixture.adapter.spawn_node_readiness_probe(
        inputs,
        Duration::from_millis(40),
        cancel.clone(),
    );
    for missing_capability in [true, false] {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let report = runtime.report();
                assert!(report.is_ready);
                if report.degraded.iter().any(|row| {
                    row.input == ReadinessInput::NodeSource
                        && (if missing_capability {
                            matches!(row.cause, ReadinessCause::NodeCapabilityMissing { .. })
                        } else {
                            matches!(&row.cause, ReadinessCause::NodeUnavailable(detail) if detail.last_reason.contains("another block at the served tip"))
                        })
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
    }
    cancel.cancel();
    probe.await?;
    Ok(())
}

#[tokio::test]
async fn stored_replayed_and_latest_tree_states_return_the_canonical_frontiers() -> Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let chain = with_note_commitments(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(12),
        &activations,
    )?;
    let fixture = Fixture::from_chain(&chain, &activations, [], Duration::from_secs(2))?;
    let expected = commitment_tree_checkpoints(
        &chain,
        &activations,
        &[BlockHeight::new(7), BlockHeight::new(12)],
    )?;
    for checkpoint in expected {
        let height = u64::from(checkpoint.block_id.height.value());
        let reply = fixture
            .adapter
            .get_tree_state(Request::new(lightwalletd::BlockId {
                height: 0,
                hash: encode_rpc_block_hash_bytes(checkpoint.block_id.hash).to_vec(),
            }))
            .await?
            .into_inner();
        assert_eq!(reply.height, height);
        assert_eq!(
            reply.hash,
            encode_rpc_block_hash_hex(checkpoint.block_id.hash)
        );
        assert_eq!(reply.time, checkpoint.block_time_seconds);
        for (protocol, tree) in [
            (ShieldedProtocol::Sapling, &reply.sapling_tree),
            (ShieldedProtocol::Orchard, &reply.orchard_tree),
            (ShieldedProtocol::Ironwood, &reply.ironwood_tree),
        ] {
            assert_eq!(
                tree,
                &checkpoint
                    .frontiers
                    .get(protocol)
                    .map_or_default(|frontier| hex::encode(frontier.final_state_bytes()))
            );
        }
        if height == 12 {
            assert_eq!(
                fixture
                    .adapter
                    .get_latest_tree_state(Request::new(lightwalletd::Empty {}))
                    .await?
                    .into_inner(),
                reply
            );
        }
    }
    assert!(fixture.node.requests()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn subtree_pages_follow_an_append_and_reject_replacement_without_holding_a_reader()
-> Result<()> {
    use tokio_stream::StreamExt as _;
    use zinder_core::{BlockId, CompactBlockArtifact};
    use zinder_query::RocksDbCanonicalReader;

    let (mut writer, chain) =
        super::super::lightwalletd_grpc::writer_with_sapling_subtree_roots(1030)?;
    let mut initial_store = writer.serving_store_fixture()?;
    let initial = RocksDbCanonicalReader::new(initial_store.take_readers()?.0)?;
    let fixture = Fixture::new([])?;
    drop(fixture.slot.publish(Arc::new(initial)));
    let divergence = chain
        .tip_height()
        .ok_or_else(|| eyre!("missing original tip"))?;
    let fork = chain.fork_at(divergence)?.extend_blocks(2);
    let roots = || {
        fixture
            .adapter
            .get_subtree_roots(Request::new(lightwalletd::GetSubtreeRootsArg {
                shielded_protocol: lightwalletd::ShieldedProtocol::Sapling.into(),
                start_index: 0,
                max_entries: 0,
            }))
    };
    let mut across_append = roots().await?.into_inner();
    let mut across_reorg = roots().await?.into_inner();
    let metadata = chain
        .compact_block_artifacts()
        .last()
        .ok_or_else(|| eyre!("missing completing block"))?
        .chain_metadata();
    let chain = chain.extend_blocks(1);
    let block = chain
        .blocks()
        .last()
        .ok_or_else(|| eyre!("missing appended block"))?;
    let compact = CompactBlockArtifact::empty(
        BlockId::new(block.height, block.hash),
        block.parent_hash,
        block.block_time_seconds,
        metadata,
    );
    writer.append_live_block(&chain.with_compact_block_artifact(compact))?;
    let mut next_store = writer.serving_store_fixture()?;
    let next = RocksDbCanonicalReader::new(next_store.take_readers()?.0)?;
    let replaced = fixture.slot.publish(Arc::new(next));
    assert_eq!(Arc::strong_count(&replaced), 1);
    for index in 0u32..1030 {
        let root = across_append
            .next()
            .await
            .ok_or_else(|| eyre!("append cut off page"))??;
        assert_eq!(&root.root_hash[..4], &index.to_be_bytes());
    }
    assert!(across_append.next().await.is_none());
    let mut fork_store = zinder_testkit::WalletServingStoreFixture::from_chain_after_live_append(
        &fork,
        &sample_regtest_upgrade_activations(),
    )?;
    drop(fixture.slot.publish(Arc::new(RocksDbCanonicalReader::new(
        fork_store.take_readers()?.0,
    )?)));
    for index in 0u32..1000 {
        let root = across_reorg
            .next()
            .await
            .ok_or_else(|| eyre!("initial page cut off"))??;
        assert_eq!(&root.root_hash[..4], &index.to_be_bytes());
    }
    assert_eq!(
        across_reorg
            .next()
            .await
            .ok_or_else(|| eyre!("truncated stream succeeded"))?
            .err()
            .ok_or_else(|| eyre!("expected an RPC refusal"))?
            .code(),
        Code::Unavailable
    );
    assert!(across_reorg.next().await.is_none());
    Ok(())
}

#[test]
fn saturated_replays_return_unavailable_while_latest_checkpoint_is_readable() -> Result<()> {
    use std::{future::poll_fn, num::NonZeroU32, task::Poll};
    use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
    use zinder_source::{NodeAuth, ZebraJsonRpcSource};

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(2)
        .build()?;
    // Handle::block_on drives the calls and RocksDB's blocking pool. Spawned replay
    // tasks wait for Runtime::block_on, keeping every admitted permit occupied.
    runtime.handle().block_on(async {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(24);
        let fixture = Fixture::from_chain(
            &chain,
            &sample_regtest_upgrade_activations(),
            [],
            Duration::from_secs(2),
        )?;
        let adapter = LightwalletdGrpcAdapter::for_light_wallet_qualification(
            fixture.slot,
            ZebraJsonRpcSource::new(
                Network::ZcashRegtest,
                fixture.node.url(),
                NodeAuth::None,
                Duration::from_secs(1),
            )?,
            Duration::ZERO,
            NonZeroU32::MIN,
        );
        let slots = std::thread::available_parallelism()?.get();
        let mut calls = (0..=slots)
            .map(|_| {
                adapter.get_tree_state(Request::new(lightwalletd::BlockId {
                    height: 17,
                    hash: Vec::new(),
                }))
            })
            .collect::<Vec<_>>();
        let first = poll_fn(|context| {
            calls
                .iter_mut()
                .find_map(|call| match call.as_mut().poll(context) {
                    Poll::Ready(reply) => Some(reply),
                    Poll::Pending => None,
                })
                .map_or(Poll::Pending, Poll::Ready)
        })
        .await;
        let status = first
            .err()
            .ok_or_else(|| eyre!("reconstruction succeeded with all replay slots occupied"))?;
        assert_eq!(status.code(), Code::Unavailable);
        assert!(status.message().contains("retry shortly"));
        let latest = adapter
            .get_latest_tree_state(Request::new(lightwalletd::Empty {}))
            .await?
            .into_inner();
        assert_eq!(latest.height, 24);
        Ok(())
    })
}
