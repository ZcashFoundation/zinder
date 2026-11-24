use std::sync::Arc;

use async_trait::async_trait;
use eyre::{Result, eyre};
use tokio::sync::Notify;
use zinder_core::{BlockId, Network, NetworkUpgradeActivations};
use zinder_source::{BroadcastNodeProbe, NodeCapabilities, NodeCapability, SourceError};
use zinder_testkit::{ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations};

use super::{LightWalletNodeVerification, VerificationTarget};
use crate::RocksDbCanonicalReader;

/// How the double answers `getblockhash` for the served tip.
#[derive(Clone, Copy, Debug)]
enum TipAnswer {
    Holds,
    AnotherBlock,
    RateLimited,
}

#[derive(Default)]
struct Calls {
    capability_probes: usize,
    schedule_reads: usize,
    tip_checks: usize,
}

/// A node double that serves `activations` and counts every call.
#[derive(Clone)]
struct CountingNode {
    activations: NetworkUpgradeActivations,
    tip: Arc<parking_lot::Mutex<TipAnswer>>,
    calls: Arc<parking_lot::Mutex<Calls>>,
    /// When set, tip checks wait for a notification before answering.
    tip_gate: Option<Arc<Notify>>,
}

impl CountingNode {
    fn new(activations: NetworkUpgradeActivations) -> Self {
        Self {
            activations,
            tip: Arc::new(parking_lot::Mutex::new(TipAnswer::Holds)),
            calls: Arc::default(),
            tip_gate: None,
        }
    }

    fn answer_tip(&self, answer: TipAnswer) {
        *self.tip.lock() = answer;
    }

    fn counts(&self) -> (usize, usize, usize) {
        let calls = self.calls.lock();
        (
            calls.capability_probes,
            calls.schedule_reads,
            calls.tip_checks,
        )
    }
}

#[async_trait]
impl BroadcastNodeProbe for CountingNode {
    async fn probe_capabilities(&self) -> Result<NodeCapabilities, SourceError> {
        self.calls.lock().capability_probes += 1;
        NodeCapabilities::new([
            NodeCapability::JsonRpc,
            NodeCapability::TransactionBroadcast,
        ])
        .map_err(|_| SourceError::SourceProtocolMismatch {
            reason: "duplicate capability",
        })
    }

    async fn fetch_network_upgrade_activations(
        &self,
    ) -> Result<NetworkUpgradeActivations, SourceError> {
        self.calls.lock().schedule_reads += 1;
        Ok(self.activations.clone())
    }

    async fn holds_block(&self, _block: BlockId) -> Result<bool, SourceError> {
        self.calls.lock().tip_checks += 1;
        if let Some(gate) = &self.tip_gate {
            gate.notified().await;
        }
        let answer = *self.tip.lock();
        match answer {
            TipAnswer::Holds => Ok(true),
            TipAnswer::AnotherBlock => Ok(false),
            TipAnswer::RateLimited => Err(SourceError::NodeRateLimited {
                operation: "getblockhash",
            }),
        }
    }
}

fn target() -> Result<(WalletServingStoreFixture, VerificationTarget)> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut store =
        WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
    let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
    let target = VerificationTarget::from_canonical(&reader)?;
    Ok((store, target))
}

#[tokio::test]
async fn a_failed_tip_check_never_forces_a_new_schedule_verification() -> Result<()> {
    let (_store, target) = target()?;
    let node = CountingNode::new(sample_regtest_upgrade_activations());
    let verification = LightWalletNodeVerification::default();

    verification.verify(&node, &target).await?;
    assert_eq!(node.counts(), (1, 1, 1));

    for answer in [TipAnswer::AnotherBlock, TipAnswer::RateLimited] {
        node.answer_tip(answer);
        for _ in 0..8 {
            assert!(verification.verify(&node, &target).await.is_err());
        }
    }
    let rate_limited = verification.verify(&node, &target).await;
    assert!(
        matches!(rate_limited, Err(SourceError::NodeRateLimited { .. })),
        "{rate_limited:?}"
    );
    node.answer_tip(TipAnswer::Holds);
    verification.verify(&node, &target).await?;
    verification
        .verify_schedule_and_capabilities(&node, &target)
        .await?;

    let (probes, schedules, tip_checks) = node.counts();
    assert_eq!(
        (probes, schedules),
        (1, 1),
        "a lagging or rate-limited tip check reset the schedule verification"
    );
    assert_eq!(tip_checks, 19);
    Ok(())
}

#[tokio::test]
async fn a_schedule_verification_makes_no_tip_check() -> Result<()> {
    let (_store, target) = target()?;
    let node = CountingNode::new(sample_regtest_upgrade_activations());
    node.answer_tip(TipAnswer::AnotherBlock);
    let verification = LightWalletNodeVerification::default();

    for _ in 0..4 {
        verification
            .verify_schedule_and_capabilities(&node, &target)
            .await?;
    }
    assert_eq!(node.counts(), (1, 1, 0));
    Ok(())
}

#[tokio::test]
async fn concurrent_tip_checks_of_one_block_share_one_call_and_keep_no_answer() -> Result<()> {
    let (_store, target) = target()?;
    let gate = Arc::new(Notify::new());
    let node = CountingNode {
        tip_gate: Some(Arc::clone(&gate)),
        ..CountingNode::new(sample_regtest_upgrade_activations())
    };
    let verification = Arc::new(LightWalletNodeVerification::default());
    let target = Arc::new(target);

    let mut checks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let (verification, node, target) =
            (Arc::clone(&verification), node.clone(), Arc::clone(&target));
        checks.spawn(async move { verification.verify_served_tip(&node, &target).await });
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while node.counts().2 == 0 || verification.tip_checks.running() != 1 {
            tokio::task::yield_now().await;
        }
        // Let every spawned check join the running call before it answers.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    gate.notify_waiters();
    while let Some(check) = checks.join_next().await {
        check??;
    }
    assert_eq!(
        node.counts().2,
        1,
        "concurrent checks of one tip made separate calls"
    );
    assert_eq!(verification.tip_checks.running(), 0);

    node.answer_tip(TipAnswer::AnotherBlock);
    let after = tokio::spawn({
        let (verification, node, target) =
            (Arc::clone(&verification), node.clone(), Arc::clone(&target));
        async move { verification.verify_served_tip(&node, &target).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while node.counts().2 != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    gate.notify_waiters();
    let after = after.await?;
    assert!(
        matches!(after, Err(SourceError::SourceProtocolMismatch { .. })),
        "a completed tip check answered a later caller: {after:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_cancelled_tip_check_leaves_no_running_call_behind() -> Result<()> {
    let (_store, target) = target()?;
    let gate = Arc::new(Notify::new());
    let node = CountingNode {
        tip_gate: Some(Arc::clone(&gate)),
        ..CountingNode::new(sample_regtest_upgrade_activations())
    };
    let verification = Arc::new(LightWalletNodeVerification::default());
    let target = Arc::new(target);
    let check = tokio::spawn({
        let (verification, node, target) =
            (Arc::clone(&verification), node.clone(), Arc::clone(&target));
        async move { verification.verify_served_tip(&node, &target).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while node.counts().2 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    check.abort();
    assert!(check.await.is_err_and(|error| error.is_cancelled()));
    assert_eq!(verification.tip_checks.running(), 0);

    let node = CountingNode::new(sample_regtest_upgrade_activations());
    verification
        .verify_served_tip(&node, &target)
        .await
        .map_err(|error| eyre!("a later check failed: {error}"))?;
    Ok(())
}

/// A JSON-RPC node that serves `schedule`, holds `tip`, and holds every
/// `getrawtransaction` call until `gate` opens, so a caller can take every
/// shared call slot of a source.
struct SaturableNode {
    server: zinder_testkit::JsonRpcTestServer,
    entered: tokio::sync::mpsc::UnboundedReceiver<()>,
    gate: Arc<(parking_lot::Mutex<bool>, parking_lot::Condvar)>,
}

impl SaturableNode {
    fn start(schedule: &NetworkUpgradeActivations, tip: BlockId) -> Result<Self> {
        use serde_json::json;
        use zinder_core::wire::{encode_branch_id_hex, encode_rpc_block_hash_hex};
        use zinder_testkit::{RpcReply, method};

        let upgrades: serde_json::Map<_, _> = schedule
            .activations()
            .iter()
            .map(|activation| {
                (
                    encode_branch_id_hex(activation.branch_id),
                    json!({"name": activation.name, "status": "active",
                        "activationheight": activation.activation_height.value()}),
                )
            })
            .collect();
        let (entered_tx, entered) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new((parking_lot::Mutex::new(false), parking_lot::Condvar::new()));
        let held = Arc::clone(&gate);
        let server = zinder_testkit::JsonRpcTestServer::start([
            method("rpc.discover").reply_with(|_| {
                RpcReply::result(json!({"methods": [{"name": "sendrawtransaction"}]}))
            }),
            method("getblockchaininfo")
                .reply_with(move |_| RpcReply::result(json!({"upgrades": upgrades}))),
            method("getblockhash")
                .reply_with(move |_| RpcReply::result(json!(encode_rpc_block_hash_hex(tip.hash)))),
            method("getrawtransaction").reply_with(move |_| {
                let _ = entered_tx.send(());
                let (released, opened) = &*held;
                let mut released = released.lock();
                opened.wait_while_for(
                    &mut released,
                    |released| !*released,
                    std::time::Duration::from_secs(20),
                );
                RpcReply::error("released")
            }),
        ])?;
        Ok(Self {
            server,
            entered,
            gate,
        })
    }

    /// A source with one shared call slot and a long request timeout.
    fn one_slot_source(&self) -> Result<zinder_source::ZebraJsonRpcSource> {
        Ok(zinder_source::ZebraJsonRpcSource::with_options(
            Network::ZcashRegtest,
            self.server.url(),
            zinder_source::NodeAuth::None,
            zinder_source::ZebraJsonRpcSourceOptions {
                request_timeout: std::time::Duration::from_secs(15),
                max_concurrent_requests: std::num::NonZeroU32::MIN,
                ..zinder_source::ZebraJsonRpcSourceOptions::default()
            },
        )?)
    }

    /// Takes the source's only shared slot with a call the node holds.
    async fn saturate(
        &mut self,
        source: &zinder_source::ZebraJsonRpcSource,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let holder = source.clone();
        let call = tokio::spawn(async move {
            let _ = holder
                .fetch_mined_transaction_bytes(zinder_core::TransactionId::from_bytes([7; 32]))
                .await;
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), self.entered.recv())
            .await?
            .ok_or_else(|| eyre!("the saturating call never reached the node"))?;
        Ok(call)
    }

    fn open(&self) {
        let (released, opened) = &*self.gate;
        *released.lock() = true;
        opened.notify_all();
    }
}

const PROMPTLY: std::time::Duration = std::time::Duration::from_secs(3);

/// The sample Regtest upgrades, one activating at each height from 1, which
/// a node reports the same way it is stored.
fn one_upgrade_per_height() -> Result<NetworkUpgradeActivations> {
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        sample_regtest_upgrade_activations()
            .activations()
            .iter()
            .zip(1..)
            .map(
                |(activation, height)| zinder_core::NetworkUpgradeActivation {
                    activation_height: zinder_core::BlockHeight::new(height),
                    ..activation.clone()
                },
            )
            .collect(),
    )?)
}

#[tokio::test]
async fn light_wallet_schedule_verification_and_readiness_pass_while_shared_slots_are_full()
-> Result<()> {
    let schedule = one_upgrade_per_height()?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut store = WalletServingStoreFixture::from_chain(&chain, &schedule)?;
    let reader = Arc::new(RocksDbCanonicalReader::new(store.take_readers()?.0)?);
    let tip = crate::CanonicalReader::event_fence(reader.as_ref()).visible_tip();
    let mut node = SaturableNode::start(&schedule, tip)?;
    let source = node.one_slot_source()?;
    let captured = Arc::clone(&reader);
    let query = crate::LightWalletNodeQuery::new(
        move || Arc::clone(&captured),
        source.clone(),
        std::time::Duration::from_secs(1),
        std::num::NonZeroU32::MIN,
    );
    let target = crate::VerifiedNodeTarget::of(&reader)?;
    let held = node.saturate(&source).await?;

    // A foreground request refreshes the schedule while it holds the
    // verification lock, and the readiness probe then verifies behind it.
    tokio::time::timeout(PROMPTLY, query.schedule_verified_node(&target))
        .await
        .map_err(|_| eyre!("a schedule refresh waited behind saturated shared slots"))??;
    let probe = query.readiness_probe_node();
    tokio::time::timeout(PROMPTLY, query.verify_current(&probe))
        .await
        .map_err(|_| eyre!("the readiness probe waited behind saturated shared slots"))??;

    node.open();
    held.await?;
    Ok(())
}

#[tokio::test]
async fn broadcast_endpoint_reverification_passes_while_shared_slots_are_full() -> Result<()> {
    let schedule = one_upgrade_per_height()?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
    let mut store = WalletServingStoreFixture::from_chain(&chain, &schedule)?;
    let (canonical, wallet) = store.take_readers()?;
    let pair = Arc::new(crate::WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(crate::RocksDbWalletReader::new(wallet)),
    )?);
    let tip = pair.canonical_fence().visible_tip();
    let slot = crate::WalletServingPairSlot::new(pair);
    let mut node = SaturableNode::start(&schedule, tip)?;
    let source = node.one_slot_source()?;
    let nodes = crate::OrderedBroadcaster::new(source.clone(), []);
    let held = node.saturate(&source).await?;

    tokio::time::timeout(PROMPTLY, nodes.verify_endpoint(0, &slot))
        .await
        .map_err(|_| eyre!("endpoint verification waited behind saturated shared slots"))??;
    assert!(nodes.verified_capabilities().is_some());

    node.open();
    held.await?;
    Ok(())
}
