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
