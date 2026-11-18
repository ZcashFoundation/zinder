#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use metrics_exporter_prometheus::PrometheusBuilder;
use parking_lot::Mutex;
use tempfile::tempdir;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CommitmentTreeCheckpoint, ConsensusBranchId, Network,
    NetworkUpgradeActivation, NetworkUpgradeActivations, SubtreeRootRange,
};
use zinder_ingest::{
    CanonicalFollowConfig, CanonicalFollower, EndpointAdmission, EndpointExclusion,
    FailoverReadiness, IneligibleReason, MempoolReadyGate, NodeEndpointAdmissions,
    NodeEndpointState, NodeFailover, NodeFailoverPolicy, REQUIRED_INGEST_NODE_CAPABILITIES,
    follow_canonical_tip, follow_canonical_tip_with_control, mempool_ready_channel,
    spawn_node_endpoint_monitor, spawn_upstream_health_probe_task,
};
use zinder_proto::v1::ingest::CanonicalWriterStatusResponse;
use zinder_runtime::{
    ActiveNodeEndpoint, ComposedReadiness, Readiness, ReadinessCause, ReadinessInput,
};
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSet, NodeSource, NodeStorageMode, SourceBlock,
    SourceChainSegment, SourceChainSegmentLimits, SourceError, SourceSubtreeRoots,
    UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK, UpstreamHealthSnapshot,
};
use zinder_store::RocksDbCanonicalStore;
use zinder_testkit::{LogCapture, sample_regtest_upgrade_activations};

use super::canonical_follow::{
    RecordingParseableSource, block_id, checkpoints_for_parseable_chain, forked_parseable_chain,
    parseable_multi_block_chain, publish_parseable_store,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// How an endpoint answers every call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    None,
    /// Every call fails as `node_unreachable`.
    Unreachable,
    /// Every call fails as `stream_disconnected`.
    StreamDisconnected,
    /// Every call waits forever, as a black-holed host does.
    Hangs,
}

/// A node endpoint with switchable faults over a parseable fixture chain.
#[derive(Clone)]
struct FailoverEndpoint {
    chain: RecordingParseableSource,
    capabilities: NodeCapabilities,
    activations: NetworkUpgradeActivations,
    genesis_hash: BlockHash,
    storage_mode: Arc<Mutex<Result<NodeStorageMode, &'static str>>>,
    fault: Arc<Mutex<Fault>>,
    /// Delay before capability discovery answers.
    probe_delay: Arc<Mutex<Duration>>,
    /// Makes the upstream health check wait forever.
    health_hangs: Arc<AtomicBool>,
    /// Delay before the upstream health check answers.
    health_delay: Arc<Mutex<Duration>>,
    /// Blocks above this height are unavailable.
    blocks_unavailable_above: Arc<Mutex<Option<BlockHeight>>>,
    ready: Arc<AtomicBool>,
}

impl FailoverEndpoint {
    fn serving(blocks: &BTreeMap<BlockHeight, SourceBlock>, tip_height: u32) -> TestResult<Self> {
        let activations = sample_regtest_upgrade_activations();
        let chain = RecordingParseableSource::new(
            blocks.clone(),
            checkpoints_for_parseable_chain(blocks, &activations)?,
            BlockHeight::new(tip_height),
        );
        Ok(Self {
            chain,
            capabilities: NodeCapabilities::new(
                REQUIRED_INGEST_NODE_CAPABILITIES
                    .iter()
                    .copied()
                    .chain([NodeCapability::ChainValuePools]),
            )?,
            activations,
            genesis_hash: Network::ZcashRegtest.genesis_hash(),
            storage_mode: Arc::new(Mutex::new(Ok(NodeStorageMode::Archive))),
            fault: Arc::new(Mutex::new(Fault::None)),
            probe_delay: Arc::new(Mutex::new(Duration::ZERO)),
            health_hangs: Arc::new(AtomicBool::new(false)),
            health_delay: Arc::new(Mutex::new(Duration::ZERO)),
            blocks_unavailable_above: Arc::new(Mutex::new(None)),
            ready: Arc::new(AtomicBool::new(true)),
        })
    }

    fn set_unreachable(&self, unreachable: bool) {
        self.set_fault(if unreachable {
            Fault::Unreachable
        } else {
            Fault::None
        });
    }

    fn set_fault(&self, fault: Fault) {
        *self.fault.lock() = fault;
    }

    fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    async fn answer(&self) -> Result<(), SourceError> {
        let fault = *self.fault.lock();
        match fault {
            Fault::None => Ok(()),
            Fault::Unreachable => Err(SourceError::NodeUnavailable {
                reason: "connection refused".to_owned(),
            }),
            Fault::StreamDisconnected => Err(SourceError::ChainTipStreamUnavailable {
                reason: "stream reset".to_owned(),
            }),
            Fault::Hangs => std::future::pending().await,
        }
    }
}

#[async_trait]
impl NodeSource for FailoverEndpoint {
    fn capabilities(&self) -> NodeCapabilities {
        self.capabilities
    }

    async fn probe_capabilities(&self) -> Result<NodeCapabilities, SourceError> {
        let probe_delay = *self.probe_delay.lock();
        tokio::time::sleep(probe_delay).await;
        self.answer().await?;
        Ok(self.capabilities)
    }

    async fn fetch_network_upgrade_activations(
        &self,
    ) -> Result<NetworkUpgradeActivations, SourceError> {
        self.answer().await?;
        Ok(self.activations.clone())
    }

    async fn fetch_storage_mode(&self) -> Result<NodeStorageMode, SourceError> {
        self.answer().await?;
        (*self.storage_mode.lock()).map_err(|reason| SourceError::NodeUnavailable {
            reason: reason.to_owned(),
        })
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.answer().await?;
        if height == BlockHeight::new(0) {
            return Ok(SourceBlock {
                network: Network::ZcashRegtest,
                height,
                hash: self.genesis_hash,
                parent_hash: BlockHash::from_bytes([0; 32]),
                block_time_seconds: 0,
                raw_block_bytes: Vec::new(),
            });
        }
        let unavailable_above = *self.blocks_unavailable_above.lock();
        if unavailable_above.is_some_and(|limit| height > limit) {
            return Err(SourceError::BlockUnavailable {
                height,
                reason: "block not yet available".to_owned(),
            });
        }
        self.chain.fetch_block_at(height).await
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        self.answer().await?;
        self.chain.fetch_chain_segment(limits).await
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.answer().await?;
        self.chain
            .fetch_chain_checkpoint(height, network_upgrade_activations)
            .await
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.answer().await?;
        self.chain.tip_id().await
    }

    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.answer().await?;
        self.chain.fetch_subtree_root_range(range).await
    }

    async fn poll_upstream_health(&self) -> Result<UpstreamHealthSnapshot, SourceError> {
        if self.health_hangs.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let health_delay = *self.health_delay.lock();
        tokio::time::sleep(health_delay).await;
        self.answer().await?;
        Ok(if self.ready.load(Ordering::SeqCst) {
            UpstreamHealthSnapshot::ready(
                UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK,
                None,
                None,
                None,
            )
        } else {
            UpstreamHealthSnapshot::not_ready(
                UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK,
                "syncing",
                None,
                None,
                None,
            )
        })
    }
}

fn fast_policy() -> NodeFailoverPolicy {
    NodeFailoverPolicy {
        standby_evaluation_interval: Duration::from_millis(20),
        switch_check_timeout: Duration::from_secs(1),
        ..NodeFailoverPolicy::default()
    }
}

/// A canonical store published to height 2, settled at height 1, over the
/// four-block fixture chain.
struct PublishedStore {
    blocks: BTreeMap<BlockHeight, SourceBlock>,
    store: RocksDbCanonicalStore,
    _directory: tempfile::TempDir,
}

async fn published_store() -> TestResult<PublishedStore> {
    let blocks = parseable_multi_block_chain()?;
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let builder_source = RecordingParseableSource::new(
        blocks.clone(),
        checkpoints_for_parseable_chain(&blocks, &activations)?,
        BlockHeight::new(4),
    );
    let directory = tempdir()?;
    let published: BTreeMap<_, _> = blocks
        .range(..=BlockHeight::new(2))
        .map(|(height, block)| (*height, block.clone()))
        .collect();
    let store = publish_parseable_store(
        &builder_source,
        &published,
        activations,
        &directory.path().join("canonical"),
        2,
        BlockHeight::new(1),
    )
    .await?;
    Ok(PublishedStore {
        blocks,
        store,
        _directory: directory,
    })
}

/// One failover run: the policy, whether `[node.health]` gates readiness,
/// and the height the follower stops at.
#[derive(Clone, Copy)]
struct FailoverRun {
    policy: NodeFailoverPolicy,
    gates_node_source_input: bool,
    target_height: u32,
}

impl FailoverRun {
    fn to_height(target_height: u32) -> Self {
        Self {
            policy: fast_policy(),
            gates_node_source_input: false,
            target_height,
        }
    }
}

/// Admitted endpoints with a running standby monitor.
struct StartedFailover {
    nodes: NodeSet<FailoverEndpoint>,
    failover: NodeFailover,
    readiness: Readiness,
    composed: ComposedReadiness,
    activations: Arc<NetworkUpgradeActivations>,
    cancel: CancellationToken,
    monitor: JoinHandle<()>,
}

impl StartedFailover {
    async fn start(endpoints: Vec<FailoverEndpoint>, run: FailoverRun) -> TestResult<Self> {
        let mut endpoints = endpoints.into_iter();
        let primary = endpoints.next().ok_or("the node list needs [node]")?;
        let (nodes, switch) = NodeSet::new(primary, endpoints);
        let admissions = NodeEndpointAdmissions::admit(&nodes, Network::ZcashRegtest, None).await?;
        let readiness = Readiness::default();
        let required_inputs: &[ReadinessInput] = if run.gates_node_source_input {
            &[ReadinessInput::NodeSource]
        } else {
            &[]
        };
        let composed = ComposedReadiness::new(readiness.clone(), required_inputs);
        let (failover, monitor) = NodeFailover::new(
            nodes.clone(),
            switch,
            &admissions,
            run.policy,
            FailoverReadiness {
                readiness: composed.clone(),
                gates_node_source_input: run.gates_node_source_input,
            },
        );
        let activations = Arc::new(sample_regtest_upgrade_activations());
        let cancel = CancellationToken::new();
        let monitor =
            spawn_node_endpoint_monitor(monitor, Arc::clone(&activations), cancel.clone());
        Ok(Self {
            nodes,
            failover,
            readiness,
            composed,
            activations,
            cancel,
            monitor,
        })
    }

    fn follower(&self, target_height: u32) -> CanonicalFollower<'_, NodeSet<FailoverEndpoint>> {
        self.follower_with_mempool_gate(target_height, None)
    }

    fn follower_with_mempool_gate(
        &self,
        target_height: u32,
        mempool_ready_gate: Option<MempoolReadyGate>,
    ) -> CanonicalFollower<'_, NodeSet<FailoverEndpoint>> {
        CanonicalFollower::new(
            &self.nodes,
            Arc::clone(&self.activations),
            CanonicalFollowConfig {
                request_timeout: Duration::from_secs(5),
                poll_interval: Duration::ZERO,
                lag_threshold_blocks: 0,
                target_height: Some(BlockHeight::new(target_height)),
                event_retention_window: None,
                event_retention_check_interval: Duration::from_secs(1),
                mempool_ready_gate,
                max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
                node_failover: Some(self.failover.clone()),
            },
            &self.composed,
            &self.cancel,
        )
    }

    async fn stop(self) -> TestResult<FailoverOutcomeHandles> {
        self.cancel.cancel();
        self.monitor.await?;
        Ok(FailoverOutcomeHandles {
            failover: self.failover,
            readiness: self.readiness,
            composed: self.composed,
        })
    }
}

struct FailoverOutcomeHandles {
    failover: NodeFailover,
    readiness: Readiness,
    composed: ComposedReadiness,
}

struct FailoverOutcome {
    store: RocksDbCanonicalStore,
    failover: NodeFailover,
    readiness: Readiness,
    composed: ComposedReadiness,
}

/// Admits `endpoints`, starts the standby monitor, and follows to the run's
/// target height with failover enabled.
async fn follow_with_failover(
    store: RocksDbCanonicalStore,
    endpoints: Vec<FailoverEndpoint>,
    run: FailoverRun,
    before_follow: impl FnOnce(),
) -> TestResult<FailoverOutcome> {
    let started = StartedFailover::start(endpoints, run).await?;
    before_follow();
    let follow_outcome = tokio::time::timeout(
        Duration::from_secs(30),
        Box::pin(follow_canonical_tip(
            store,
            started.follower(run.target_height),
        )),
    )
    .await;
    let FailoverOutcomeHandles {
        failover,
        readiness,
        composed,
    } = started.stop().await?;
    Ok(FailoverOutcome {
        store: follow_outcome??,
        failover,
        readiness,
        composed,
    })
}

#[tokio::test]
async fn follower_switches_after_three_unreachable_recoveries_and_keeps_appending() -> TestResult {
    let recorder = PrometheusBuilder::new().build_recorder();
    let recorded_metrics = recorder.handle();
    let _local_recorder = metrics::set_default_local_recorder(&recorder);
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let primary = FailoverEndpoint::serving(&blocks, 2)?;
    let fallback = FailoverEndpoint::serving(&blocks, 4)?;
    let failing_primary = primary.clone();

    let FailoverOutcome {
        store,
        failover,
        readiness,
        ..
    } = follow_with_failover(
        store,
        vec![primary, fallback],
        FailoverRun::to_height(4),
        || failing_primary.set_unreachable(true),
    )
    .await?;

    assert_eq!(store.event_fence().visible_tip(), block_id(&blocks, 4)?);
    assert_eq!(failover.active_position(), 1);
    assert_eq!(
        readiness.report().active_node,
        Some(ActiveNodeEndpoint { position: 1 })
    );
    let rendered = recorded_metrics.render();
    assert!(
        rendered.lines().any(|line| line.starts_with(
            "zinder_ingest_node_endpoint_switches_total{from_position=\"0\",to_position=\"1\",failure_class=\"node_unreachable\"} 1"
        )),
        "{rendered}"
    );
    assert!(rendered.contains("zinder_ingest_node_endpoint_active{position=\"1\"} 1"));
    Ok(())
}

/// A lagging standby is skipped: the follower stays on its endpoint and
/// resumes once that endpoint recovers.
#[tokio::test]
async fn follower_stays_on_its_endpoint_when_the_standby_lags() -> TestResult {
    stays_on_the_active_endpoint(
        |blocks| FailoverEndpoint::serving(blocks, 1),
        IneligibleReason::Lagging,
    )
    .await
}

/// A standby on another branch at or below the settled tip is skipped, so a
/// switch never parks the writer on a fork deeper than its window.
#[tokio::test]
async fn follower_stays_on_its_endpoint_when_the_standby_diverged_below_settlement() -> TestResult {
    stays_on_the_active_endpoint(
        |blocks| FailoverEndpoint::serving(&forked_parseable_chain(blocks, 1, 4, 37)?, 4),
        IneligibleReason::DivergedAtOrBelowSettlement,
    )
    .await
}

async fn stays_on_the_active_endpoint(
    make_standby: impl FnOnce(&BTreeMap<BlockHeight, SourceBlock>) -> TestResult<FailoverEndpoint>,
    expected: IneligibleReason,
) -> TestResult {
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let primary = FailoverEndpoint::serving(&blocks, 4)?;
    let standby = make_standby(&blocks)?;
    let recovering_primary = primary.clone();

    let FailoverOutcome {
        store, failover, ..
    } = follow_with_failover(
        store,
        vec![primary, standby],
        FailoverRun::to_height(4),
        move || {
            recovering_primary.set_unreachable(true);
            tokio::spawn(async move {
                // The third failed recovery, which attempts the switch,
                // follows two production backoffs.
                tokio::time::sleep(Duration::from_secs(5)).await;
                recovering_primary.set_unreachable(false);
            });
        },
    )
    .await?;

    assert_eq!(store.event_fence().visible_tip(), block_id(&blocks, 4)?);
    assert_eq!(failover.active_position(), 0);
    assert_eq!(
        failover.endpoint_states().get(1).copied(),
        Some(NodeEndpointState::Ineligible(expected))
    );
    Ok(())
}

/// A standby that shares the settled tip but not the visible tip takes over
/// only when no standby holds the visible tip, and the switch then replaces
/// the diverged suffix within the reorg window.
#[tokio::test]
async fn standby_holding_the_visible_tip_wins_over_a_diverged_one() -> TestResult {
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let diverged_chain = forked_parseable_chain(&blocks, 2, 4, 41)?;
    let primary = FailoverEndpoint::serving(&blocks, 2)?;
    let diverged = FailoverEndpoint::serving(&diverged_chain, 4)?;
    let holding = FailoverEndpoint::serving(&blocks, 4)?;
    let failing_primary = primary.clone();

    let FailoverOutcome {
        store, failover, ..
    } = follow_with_failover(
        store,
        vec![primary, diverged, holding],
        FailoverRun::to_height(4),
        || failing_primary.set_unreachable(true),
    )
    .await?;

    assert_eq!(failover.active_position(), 2);
    assert_eq!(store.event_fence().visible_tip(), block_id(&blocks, 4)?);
    Ok(())
}

#[tokio::test]
async fn a_diverged_standby_takes_over_when_it_is_the_only_one() -> TestResult {
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let diverged_chain = forked_parseable_chain(&blocks, 2, 4, 43)?;
    let primary = FailoverEndpoint::serving(&blocks, 2)?;
    let diverged = FailoverEndpoint::serving(&diverged_chain, 4)?;
    let failing_primary = primary.clone();

    let FailoverOutcome {
        store, failover, ..
    } = follow_with_failover(
        store,
        vec![primary, diverged],
        FailoverRun::to_height(4),
        || failing_primary.set_unreachable(true),
    )
    .await?;

    assert_eq!(failover.active_position(), 1);
    assert_eq!(
        store.event_fence().visible_tip(),
        block_id(&diverged_chain, 4)?
    );
    assert_eq!(store.displaced_block_count()?, 1);
    Ok(())
}

/// A reachable endpoint that reports not ready past the bound is left at the
/// top of a follow iteration, without any source error.
#[tokio::test]
async fn sustained_not_ready_switches_at_the_top_of_an_iteration() -> TestResult {
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let primary = FailoverEndpoint::serving(&blocks, 2)?;
    let fallback = FailoverEndpoint::serving(&blocks, 4)?;
    let unready_primary = primary.clone();

    let FailoverOutcome {
        store, failover, ..
    } = follow_with_failover(
        store,
        vec![primary, fallback],
        FailoverRun {
            policy: NodeFailoverPolicy {
                upstream_not_ready_before_switch: Duration::from_millis(100),
                ..fast_policy()
            },
            ..FailoverRun::to_height(4)
        },
        || unready_primary.set_ready(false),
    )
    .await?;

    assert_eq!(failover.active_position(), 1);
    assert_eq!(store.event_fence().visible_tip(), block_id(&blocks, 4)?);
    Ok(())
}

#[tokio::test]
async fn startup_reads_from_the_first_admitted_endpoint() -> TestResult {
    let blocks = parseable_multi_block_chain()?;
    let unreachable = FailoverEndpoint::serving(&blocks, 4)?;
    unreachable.set_unreachable(true);
    let admitted = FailoverEndpoint::serving(&blocks, 4)?;
    let (nodes, _switch) = NodeSet::new(unreachable, [admitted]);

    let admissions = NodeEndpointAdmissions::admit(&nodes, Network::ZcashRegtest, None).await?;

    assert_eq!(admissions.active_position(), 1);
    assert!(matches!(
        admissions.verdicts().next(),
        Some((0, EndpointAdmission::Unreachable(_)))
    ));
    Ok(())
}

/// Only positive evidence excludes an endpoint; a storage mode the node did
/// not report leaves it unreachable, to be evaluated again.
#[tokio::test]
async fn startup_excludes_endpoints_only_on_positive_evidence() -> TestResult {
    let blocks = parseable_multi_block_chain()?;
    let primary = FailoverEndpoint::serving(&blocks, 4)?;
    // Admission returns once `[node]` is admitted, so `[node]` answers last
    // to let every fallback's verdict land first.
    *primary.probe_delay.lock() = Duration::from_millis(200);
    let mut pruned = FailoverEndpoint::serving(&blocks, 4)?;
    pruned.capabilities = archive_capabilities_withheld(pruned.capabilities)?;
    *pruned.storage_mode.lock() = Ok(NodeStorageMode::Pruned);
    let mut unreported = FailoverEndpoint::serving(&blocks, 4)?;
    unreported.capabilities = archive_capabilities_withheld(unreported.capabilities)?;
    *unreported.storage_mode.lock() = Err("getblockchaininfo timed out");
    let mut wrong_network = FailoverEndpoint::serving(&blocks, 4)?;
    wrong_network.genesis_hash = Network::ZcashTestnet.genesis_hash();
    let mut other_schedule = FailoverEndpoint::serving(&blocks, 4)?;
    other_schedule.activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);
    let mut without_tree_state = FailoverEndpoint::serving(&blocks, 4)?;
    without_tree_state.capabilities = NodeCapabilities::new(
        without_tree_state
            .capabilities
            .iter()
            .filter(|capability| *capability != NodeCapability::TreeState),
    )?;
    let (nodes, _switch) = NodeSet::new(
        primary,
        [
            pruned,
            unreported,
            wrong_network,
            other_schedule,
            without_tree_state,
        ],
    );

    let admissions = NodeEndpointAdmissions::admit(&nodes, Network::ZcashRegtest, None).await?;
    let labels = admissions
        .verdicts()
        .map(|(_, verdict)| verdict.label())
        .collect::<Vec<_>>();

    assert_eq!(admissions.active_position(), 0);
    assert_eq!(
        labels,
        [
            "admitted",
            EndpointExclusion::Pruned.label(),
            "unreachable",
            EndpointExclusion::GenesisMismatch.label(),
            EndpointExclusion::ScheduleConflict.label(),
            EndpointExclusion::CapabilityMissing(NodeCapability::TreeState).label(),
        ]
    );
    Ok(())
}

/// Only `[node]` may bring a table other than the store's admitted revision.
///
/// A fallback that would remove a pending activation never becomes active,
/// so it cannot rewrite the schedule while `[node]` is unreachable.
#[tokio::test]
async fn startup_refuses_a_fallback_that_would_change_the_admitted_schedule() -> TestResult {
    let PublishedStore {
        blocks,
        mut store,
        _directory: directory,
    } = published_store().await?;
    store.admit_network_upgrade_schedule(&sample_regtest_upgrade_activations())?;
    assert!(
        RocksDbCanonicalStore::read_admitted_network_upgrade_activations(
            directory.path().join("absent")
        )?
        .is_none()
    );
    let store_activations = RocksDbCanonicalStore::read_admitted_network_upgrade_activations(
        directory.path().join("canonical"),
    )?
    .ok_or("the writer admitted a schedule")?;
    assert_eq!(store_activations, sample_regtest_upgrade_activations());
    let older_table = NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        store_activations
            .activations()
            .split_last()
            .map(|(_, older)| older.to_vec())
            .unwrap_or_default(),
    )?;
    let unreachable_primary = || -> TestResult<FailoverEndpoint> {
        let primary = FailoverEndpoint::serving(&blocks, 4)?;
        primary.set_unreachable(true);
        Ok(primary)
    };
    let older_fallback = || -> TestResult<FailoverEndpoint> {
        let mut fallback = FailoverEndpoint::serving(&blocks, 4)?;
        fallback.activations = older_table.clone();
        Ok(fallback)
    };

    let (only_older, _switch) = NodeSet::new(unreachable_primary()?, [older_fallback()?]);
    let refusal =
        NodeEndpointAdmissions::admit(&only_older, Network::ZcashRegtest, Some(&store_activations))
            .await
            .err()
            .ok_or("an older fallback must not start ingest")?;
    let message = refusal.to_string();
    assert!(message.contains("position 1: excluded"), "{message}");

    let (with_matching, _switch) = NodeSet::new(
        unreachable_primary()?,
        [older_fallback()?, FailoverEndpoint::serving(&blocks, 4)?],
    );
    let admissions = NodeEndpointAdmissions::admit(
        &with_matching,
        Network::ZcashRegtest,
        Some(&store_activations),
    )
    .await?;
    assert_eq!(admissions.active_position(), 2);
    assert_eq!(
        admissions
            .verdicts()
            .nth(1)
            .map(|(_, verdict)| verdict.label()),
        Some(EndpointExclusion::ScheduleConflict.label())
    );

    let mut older_primary = FailoverEndpoint::serving(&blocks, 4)?;
    older_primary.activations = older_table;
    let (primary_changes, _switch) =
        NodeSet::new(older_primary, [FailoverEndpoint::serving(&blocks, 4)?]);
    let admissions = NodeEndpointAdmissions::admit(
        &primary_changes,
        Network::ZcashRegtest,
        Some(&store_activations),
    )
    .await?;
    assert_eq!(admissions.active_position(), 0);
    Ok(())
}

/// A fallback that answers before `[node]` with the new table `[node]` also
/// brings stays a standby: the store's revision is compared only for the
/// endpoint that becomes active.
#[tokio::test]
async fn startup_keeps_a_fallback_that_advertises_the_new_table_node_brings() -> TestResult {
    let blocks = parseable_multi_block_chain()?;
    let store_activations = sample_regtest_upgrade_activations();
    let upgraded_table = with_future_upgrade(&store_activations)?;
    let mut upgraded_primary = FailoverEndpoint::serving(&blocks, 4)?;
    upgraded_primary.activations = upgraded_table.clone();
    *upgraded_primary.probe_delay.lock() = Duration::from_millis(200);
    let mut upgraded_fallback = FailoverEndpoint::serving(&blocks, 4)?;
    upgraded_fallback.activations = upgraded_table;
    let (nodes, _switch) = NodeSet::new(upgraded_primary, [upgraded_fallback]);

    let admissions =
        NodeEndpointAdmissions::admit(&nodes, Network::ZcashRegtest, Some(&store_activations))
            .await?;

    assert_eq!(admissions.active_position(), 0);
    assert!(matches!(
        admissions.verdicts().nth(1),
        Some((1, EndpointAdmission::Admitted(_)))
    ));
    Ok(())
}

/// `table` with one more upgrade, activating far above every fixture tip.
fn with_future_upgrade(table: &NetworkUpgradeActivations) -> TestResult<NetworkUpgradeActivations> {
    let mut activations = table.activations().to_vec();
    activations.push(NetworkUpgradeActivation {
        branch_id: ConsensusBranchId::new(0x0fed_cba9),
        activation_height: BlockHeight::new(1_000_000),
        name: "FutureUpgrade".to_owned(),
    });
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        activations,
    )?)
}

#[tokio::test]
async fn startup_fails_when_no_endpoint_is_admitted() -> TestResult {
    let blocks = parseable_multi_block_chain()?;
    let primary = FailoverEndpoint::serving(&blocks, 4)?;
    primary.set_unreachable(true);
    let mut fallback = FailoverEndpoint::serving(&blocks, 4)?;
    fallback.genesis_hash = Network::ZcashMainnet.genesis_hash();
    let (nodes, _switch) = NodeSet::new(primary, [fallback]);

    let refusal = NodeEndpointAdmissions::admit(&nodes, Network::ZcashRegtest, None)
        .await
        .err()
        .ok_or("no admitted endpoint must fail startup")?;

    assert!(matches!(
        refusal.primary(),
        Some(EndpointAdmission::Unreachable(
            SourceError::NodeUnavailable { .. }
        ))
    ));
    let message = refusal.to_string();
    assert!(message.contains("position 0: unreachable"), "{message}");
    assert!(message.contains("position 1: excluded"), "{message}");
    Ok(())
}

/// A fallback that never answers does not hold back a healthy `[node]`; it
/// is left to the standby monitor.
#[tokio::test]
async fn startup_does_not_wait_for_a_fallback_that_does_not_answer() -> TestResult {
    let blocks = parseable_multi_block_chain()?;
    let primary = FailoverEndpoint::serving(&blocks, 4)?;
    let black_holed = FailoverEndpoint::serving(&blocks, 4)?;
    black_holed.set_fault(Fault::Hangs);
    let (nodes, _switch) = NodeSet::new(primary, [black_holed]);

    let admissions = tokio::time::timeout(
        Duration::from_secs(5),
        NodeEndpointAdmissions::admit(&nodes, Network::ZcashRegtest, None),
    )
    .await??;

    assert_eq!(admissions.active_position(), 0);
    assert!(matches!(
        admissions.verdicts().nth(1),
        Some((1, EndpointAdmission::Deferred))
    ));
    Ok(())
}

/// The monitor checks a deferred endpoint's genesis block, so an endpoint
/// on another network never becomes a candidate.
#[tokio::test]
async fn the_monitor_checks_the_genesis_block_of_a_deferred_endpoint() -> TestResult {
    let blocks = parseable_multi_block_chain()?;
    let primary = FailoverEndpoint::serving(&blocks, 4)?;
    let mut other_network = FailoverEndpoint::serving(&blocks, 4)?;
    other_network.genesis_hash = Network::ZcashTestnet.genesis_hash();
    *other_network.probe_delay.lock() = Duration::from_millis(200);

    let started =
        StartedFailover::start(vec![primary, other_network], FailoverRun::to_height(4)).await?;
    let expected = NodeEndpointState::Ineligible(IneligibleReason::Refused(
        EndpointExclusion::GenesisMismatch,
    ));
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        while started.failover.endpoint_states().get(1).copied() != Some(expected) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    started.stop().await?;

    assert!(
        observed.is_ok(),
        "the monitor never checked the genesis block"
    );
    Ok(())
}

/// An active endpoint whose health check hangs past every evaluation bound
/// does not hold back the evidence that a standby can take over.
#[tokio::test]
async fn a_standby_takes_over_while_the_active_health_check_hangs() -> TestResult {
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let primary = FailoverEndpoint::serving(&blocks, 2)?;
    let fallback = FailoverEndpoint::serving(&blocks, 4)?;
    let failing_primary = primary.clone();

    let FailoverOutcome {
        store, failover, ..
    } = follow_with_failover(
        store,
        vec![primary, fallback],
        FailoverRun::to_height(4),
        || {
            failing_primary.health_hangs.store(true, Ordering::SeqCst);
            failing_primary.set_unreachable(true);
        },
    )
    .await?;

    assert_eq!(failover.active_position(), 1);
    assert_eq!(store.event_fence().visible_tip(), block_id(&blocks, 4)?);
    Ok(())
}

#[tokio::test]
async fn follower_switches_after_three_stream_disconnected_recoveries() -> TestResult {
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let primary = FailoverEndpoint::serving(&blocks, 2)?;
    let fallback = FailoverEndpoint::serving(&blocks, 4)?;
    let failing_primary = primary.clone();

    let FailoverOutcome {
        store, failover, ..
    } = follow_with_failover(
        store,
        vec![primary, fallback],
        FailoverRun::to_height(4),
        || failing_primary.set_fault(Fault::StreamDisconnected),
    )
    .await?;

    assert_eq!(failover.active_position(), 1);
    assert_eq!(store.event_fence().visible_tip(), block_id(&blocks, 4)?);
    Ok(())
}

/// With `[node.health]` gating readiness, a switch withholds readiness
/// until the probe observes the new endpoint.
#[tokio::test]
async fn a_switch_resets_the_gated_node_source_input_to_starting() -> TestResult {
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let primary = FailoverEndpoint::serving(&blocks, 2)?;
    let fallback = FailoverEndpoint::serving(&blocks, 4)?;
    let failing_primary = primary.clone();

    let FailoverOutcome {
        failover, composed, ..
    } = follow_with_failover(
        store,
        vec![primary, fallback],
        FailoverRun {
            gates_node_source_input: true,
            ..FailoverRun::to_height(4)
        },
        || failing_primary.set_unreachable(true),
    )
    .await?;

    assert_eq!(failover.active_position(), 1);
    assert!(matches!(
        composed.input_cause(ReadinessInput::NodeSource),
        Some(ReadinessCause::Starting)
    ));
    Ok(())
}

/// A switch withholds ingest readiness until the mempool owner certifies a
/// generation from the new endpoint, even before the owner has processed the
/// end of the previous endpoint's stream.
#[tokio::test]
async fn a_switch_withholds_readiness_until_the_new_endpoint_mempool_is_certified() -> TestResult {
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let primary = FailoverEndpoint::serving(&blocks, 2)?;
    let fallback = FailoverEndpoint::serving(&blocks, 4)?;
    let failing_primary = primary.clone();
    let started =
        StartedFailover::start(vec![primary, fallback], FailoverRun::to_height(4)).await?;
    failing_primary.set_unreachable(true);
    let (mempool_signal, mempool_gate) = mempool_ready_channel();
    // The owner certified `[node]`'s mempool and has not yet seen the switch.
    mempool_signal.certify_source_tip(block_id(&blocks, 2)?);

    let follow_outcome = tokio::time::timeout(
        Duration::from_secs(30),
        Box::pin(follow_canonical_tip(
            store,
            started.follower_with_mempool_gate(4, Some(mempool_gate.clone())),
        )),
    )
    .await;
    let FailoverOutcomeHandles {
        failover,
        readiness,
        ..
    } = started.stop().await?;
    let store = follow_outcome??;

    assert_eq!(failover.active_position(), 1);
    let visible_tip = store.event_fence().visible_tip();
    assert_eq!(visible_tip, block_id(&blocks, 4)?);
    assert!(!readiness.report().is_ready);
    assert!(!mempool_gate.admits_canonical_tip(visible_tip));
    mempool_signal.certify_source_tip(visible_tip);
    assert!(mempool_gate.admits_canonical_tip(visible_tip));
    Ok(())
}

/// A switch to an endpoint holding the visible tip keeps the tip confirmed,
/// so readers keep serving while that endpoint's first iterations fail.
#[tokio::test]
async fn a_switch_to_an_endpoint_holding_the_visible_tip_keeps_the_tip_confirmed() -> TestResult {
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let fallback = FailoverEndpoint::serving(&blocks, 4)?;
    let SwitchStatuses {
        confirmed,
        after_switch,
        store,
    } = writer_statuses_around_switch(&blocks, store, fallback, BlockHeight::new(2)).await?;

    assert!(!confirmed.tip_unconfirmed);
    assert!(!after_switch.tip_unconfirmed);
    assert_eq!(store.event_fence().visible_tip(), block_id(&blocks, 4)?);
    Ok(())
}

/// A switch to a diverged endpoint reports the tip unconfirmed until the
/// replacement of the diverged suffix commits.
#[tokio::test]
async fn a_switch_to_a_diverged_endpoint_reports_the_tip_unconfirmed() -> TestResult {
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let diverged_chain = forked_parseable_chain(&blocks, 2, 4, 47)?;
    let diverged = FailoverEndpoint::serving(&diverged_chain, 4)?;
    let SwitchStatuses {
        confirmed,
        after_switch,
        store,
    } = writer_statuses_around_switch(&blocks, store, diverged, BlockHeight::new(2)).await?;

    assert!(!confirmed.tip_unconfirmed);
    assert!(after_switch.tip_unconfirmed);
    assert_eq!(
        store.event_fence().visible_tip(),
        block_id(&diverged_chain, 4)?
    );
    Ok(())
}

struct SwitchStatuses {
    confirmed: CanonicalWriterStatusResponse,
    after_switch: CanonicalWriterStatusResponse,
    store: RocksDbCanonicalStore,
}

/// Reads `WriterStatus` before and right after ingest switches to `fallback`.
///
/// The follower runs to height 4 over `[node]` and `fallback`; the first
/// status is read once the tip is confirmed and the second right after
/// `[node]` fails and ingest switches. `fallback` serves no block above
/// `blocked_above` until the second status is read, so its first iterations
/// fail.
async fn writer_statuses_around_switch(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    store: RocksDbCanonicalStore,
    fallback: FailoverEndpoint,
    blocked_above: BlockHeight,
) -> TestResult<SwitchStatuses> {
    let primary = FailoverEndpoint::serving(blocks, 2)?;
    *fallback.blocks_unavailable_above.lock() = Some(blocked_above);
    let failing_primary = primary.clone();
    let healing_fallback = fallback.clone();
    let started =
        StartedFailover::start(vec![primary, fallback], FailoverRun::to_height(4)).await?;
    let (canonical, commands) = zinder_ingest::canonical_control_channel();

    let observe = async {
        while !started.readiness.report().is_ready {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let confirmed = canonical.writer_status().await?;
        failing_primary.set_unreachable(true);
        while started.failover.active_position() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Let the follower fail several iterations on the new endpoint.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let after_switch = canonical.writer_status().await?;
        *healing_fallback.blocks_unavailable_above.lock() = None;
        Ok::<_, tonic::Status>((confirmed, after_switch))
    };
    let (statuses, outcome) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(
            observe,
            Box::pin(follow_canonical_tip_with_control(
                store,
                started.follower(4),
                commands,
            )),
        )
    })
    .await?;
    let (confirmed, after_switch) = statuses?;
    let store = outcome?;
    started.stop().await?;
    Ok(SwitchStatuses {
        confirmed,
        after_switch,
        store,
    })
}

/// A switch attempt that finds no candidate is not repeated, or logged
/// again, until the monitor applies another standby evaluation.
#[tokio::test]
async fn a_failed_switch_is_retried_only_after_a_new_standby_evaluation() -> TestResult {
    let capture = LogCapture::install_for_target("zinder::ingest");
    let PublishedStore {
        blocks,
        store,
        _directory,
    } = published_store().await?;
    let primary = FailoverEndpoint::serving(&blocks, 2)?;
    let standby = FailoverEndpoint::serving(&blocks, 4)?;
    standby.set_ready(false);
    let unready_primary = primary.clone();
    let recovering_standby = standby.clone();

    let FailoverOutcome { failover, .. } = follow_with_failover(
        store,
        vec![primary, standby],
        FailoverRun {
            policy: NodeFailoverPolicy {
                upstream_not_ready_before_switch: Duration::from_millis(50),
                standby_evaluation_interval: Duration::from_millis(200),
                ..fast_policy()
            },
            ..FailoverRun::to_height(4)
        },
        move || {
            unready_primary.set_ready(false);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(1_500)).await;
                recovering_standby.set_ready(true);
            });
        },
    )
    .await?;

    let unavailable = capture
        .events()
        .iter()
        .filter(|event| event.field("event") == Some("node_endpoint_switch_unavailable"))
        .count();
    assert_eq!(failover.active_position(), 1);
    // About eight standby evaluations land before the standby recovers; the
    // follower loops at its tip thousands of times in that window.
    assert!((1..=15).contains(&unavailable), "{unavailable}");
    Ok(())
}

#[tokio::test]
async fn upstream_health_probe_observes_the_new_endpoint_right_after_a_switch() -> TestResult {
    let blocks = parseable_multi_block_chain()?;
    let primary = FailoverEndpoint::serving(&blocks, 4)?;
    let syncing_fallback = FailoverEndpoint::serving(&blocks, 4)?;
    syncing_fallback.set_ready(false);
    let (nodes, switch) = NodeSet::new(primary, [syncing_fallback]);
    let composed = ComposedReadiness::new(Readiness::default(), &[ReadinessInput::NodeSource]);
    let cancel = CancellationToken::new();
    let probe = spawn_upstream_health_probe_task(
        Arc::new(nodes.clone()),
        composed.clone(),
        Duration::from_hours(1),
        Some(nodes.subscribe_switches()),
        cancel.clone(),
    );

    assert!(switch.switch_to(1));
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(
            composed.input_cause(ReadinessInput::NodeSource),
            Some(ReadinessCause::UpstreamNotReady(_))
        ) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    cancel.cancel();
    probe.await?;

    assert!(observed.is_ok(), "the probe waited for its next tick");
    Ok(())
}

/// A probe of `[node]` still in flight when ingest switches publishes
/// nothing, so it never replaces the reset input with the previous
/// endpoint's health, even when the new endpoint does not answer.
#[tokio::test]
async fn upstream_health_probe_discards_an_observation_of_the_previous_endpoint() -> TestResult {
    let blocks = parseable_multi_block_chain()?;
    let slow_primary = FailoverEndpoint::serving(&blocks, 4)?;
    *slow_primary.health_delay.lock() = Duration::from_millis(300);
    let silent_fallback = FailoverEndpoint::serving(&blocks, 4)?;
    silent_fallback.set_unreachable(true);
    let (nodes, switch) = NodeSet::new(slow_primary, [silent_fallback]);
    let composed = ComposedReadiness::new(Readiness::default(), &[ReadinessInput::NodeSource]);
    let cancel = CancellationToken::new();
    let probe = spawn_upstream_health_probe_task(
        Arc::new(nodes.clone()),
        composed.clone(),
        Duration::from_millis(10),
        Some(nodes.subscribe_switches()),
        cancel.clone(),
    );

    // Switch while the first probe of `[node]` is in flight, resetting the
    // input as the failover does.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(switch.switch_to(1));
    composed.publish_input(ReadinessInput::NodeSource, ReadinessCause::Starting);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let input = composed.input_cause(ReadinessInput::NodeSource);
    cancel.cancel();
    probe.await?;

    assert!(matches!(input, Some(ReadinessCause::Starting)), "{input:?}");
    Ok(())
}

fn archive_capabilities_withheld(capabilities: NodeCapabilities) -> TestResult<NodeCapabilities> {
    Ok(NodeCapabilities::new(capabilities.iter().filter(
        |capability| *capability != NodeCapability::HistoricalBlockBodies,
    ))?)
}
