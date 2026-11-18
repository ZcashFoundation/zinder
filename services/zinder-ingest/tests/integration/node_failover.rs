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
use tokio_util::sync::CancellationToken;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CommitmentTreeCheckpoint, Network, NetworkUpgradeActivations,
    SubtreeRootRange,
};
use zinder_ingest::{
    CanonicalFollowConfig, CanonicalFollower, EndpointAdmission, EndpointExclusion,
    FailoverReadiness, IneligibleReason, NodeEndpointAdmissions, NodeEndpointState, NodeFailover,
    NodeFailoverPolicy, REQUIRED_INGEST_NODE_CAPABILITIES, follow_canonical_tip,
    spawn_node_endpoint_monitor,
};
use zinder_runtime::{ActiveNodeEndpoint, ComposedReadiness, Readiness};
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSet, NodeSource, NodeStorageMode, SourceBlock,
    SourceChainSegment, SourceChainSegmentLimits, SourceError, SourceSubtreeRoots,
    UPSTREAM_HEALTH_SOURCE_VERIFICATION_PROGRESS_FALLBACK, UpstreamHealthSnapshot,
};
use zinder_store::RocksDbCanonicalStore;
use zinder_testkit::sample_regtest_upgrade_activations;

use super::canonical_follow::{
    RecordingParseableSource, block_id, checkpoints_for_parseable_chain, forked_parseable_chain,
    parseable_multi_block_chain, publish_parseable_store,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// A node endpoint with switchable faults over a parseable fixture chain.
#[derive(Clone)]
struct FailoverEndpoint {
    chain: RecordingParseableSource,
    capabilities: NodeCapabilities,
    activations: NetworkUpgradeActivations,
    genesis_hash: BlockHash,
    storage_mode: Arc<Mutex<Result<NodeStorageMode, &'static str>>>,
    unreachable: Arc<AtomicBool>,
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
            unreachable: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(true)),
        })
    }

    fn set_unreachable(&self, unreachable: bool) {
        self.unreachable.store(unreachable, Ordering::SeqCst);
    }

    fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    fn answer(&self) -> Result<(), SourceError> {
        if self.unreachable.load(Ordering::SeqCst) {
            return Err(SourceError::NodeUnavailable {
                reason: "connection refused".to_owned(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl NodeSource for FailoverEndpoint {
    fn capabilities(&self) -> NodeCapabilities {
        self.capabilities
    }

    async fn probe_capabilities(&self) -> Result<NodeCapabilities, SourceError> {
        self.answer()?;
        Ok(self.capabilities)
    }

    async fn fetch_network_upgrade_activations(
        &self,
    ) -> Result<NetworkUpgradeActivations, SourceError> {
        self.answer()?;
        Ok(self.activations.clone())
    }

    async fn fetch_storage_mode(&self) -> Result<NodeStorageMode, SourceError> {
        self.answer()?;
        (*self.storage_mode.lock()).map_err(|reason| SourceError::NodeUnavailable {
            reason: reason.to_owned(),
        })
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.answer()?;
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
        self.chain.fetch_block_at(height).await
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        self.answer()?;
        self.chain.fetch_chain_segment(limits).await
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.answer()?;
        self.chain
            .fetch_chain_checkpoint(height, network_upgrade_activations)
            .await
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.answer()?;
        self.chain.tip_id().await
    }

    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.answer()?;
        self.chain.fetch_subtree_root_range(range).await
    }

    async fn poll_upstream_health(&self) -> Result<UpstreamHealthSnapshot, SourceError> {
        self.answer()?;
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

/// Admits `endpoints`, starts the standby monitor, and follows to
/// `target_height` with failover enabled.
async fn follow_with_failover(
    store: RocksDbCanonicalStore,
    endpoints: Vec<FailoverEndpoint>,
    policy: NodeFailoverPolicy,
    before_follow: impl FnOnce(),
    target_height: u32,
) -> TestResult<(RocksDbCanonicalStore, NodeFailover, Readiness)> {
    let mut endpoints = endpoints.into_iter();
    let primary = endpoints.next().ok_or("the node list needs [node]")?;
    let (nodes, switch) = NodeSet::new(primary, endpoints);
    let admissions = NodeEndpointAdmissions::admit(&nodes, Network::ZcashRegtest).await?;
    let readiness = Readiness::default();
    let composed = ComposedReadiness::new(readiness.clone(), &[]);
    let (failover, monitor) = NodeFailover::new(
        nodes.clone(),
        switch,
        &admissions,
        policy,
        FailoverReadiness {
            readiness: composed.clone(),
            gates_node_source_input: false,
        },
    );
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let cancel = CancellationToken::new();
    let monitor = spawn_node_endpoint_monitor(monitor, Arc::clone(&activations), cancel.clone());
    before_follow();
    let follower = CanonicalFollower::new(
        &nodes,
        activations,
        CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::ZERO,
            lag_threshold_blocks: 0,
            target_height: Some(BlockHeight::new(target_height)),
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: Some(failover.clone()),
        },
        &composed,
        &cancel,
    );
    let follow_outcome = tokio::time::timeout(
        Duration::from_secs(30),
        Box::pin(follow_canonical_tip(store, follower)),
    )
    .await;
    cancel.cancel();
    monitor.await?;
    Ok((follow_outcome??, failover, readiness))
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

    let (store, failover, readiness) = follow_with_failover(
        store,
        vec![primary, fallback],
        fast_policy(),
        || failing_primary.set_unreachable(true),
        4,
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

    let (store, failover, _readiness) = follow_with_failover(
        store,
        vec![primary, standby],
        fast_policy(),
        move || {
            recovering_primary.set_unreachable(true);
            tokio::spawn(async move {
                // The third failed recovery, which attempts the switch,
                // follows two production backoffs.
                tokio::time::sleep(Duration::from_secs(5)).await;
                recovering_primary.set_unreachable(false);
            });
        },
        4,
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

    let (store, failover, _readiness) = follow_with_failover(
        store,
        vec![primary, diverged, holding],
        fast_policy(),
        || failing_primary.set_unreachable(true),
        4,
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

    let (store, failover, _readiness) = follow_with_failover(
        store,
        vec![primary, diverged],
        fast_policy(),
        || failing_primary.set_unreachable(true),
        4,
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

    let (store, failover, _readiness) = follow_with_failover(
        store,
        vec![primary, fallback],
        NodeFailoverPolicy {
            upstream_not_ready_before_switch: Duration::from_millis(100),
            ..fast_policy()
        },
        || unready_primary.set_ready(false),
        4,
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

    let admissions = NodeEndpointAdmissions::admit(&nodes, Network::ZcashRegtest).await?;

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

    let admissions = NodeEndpointAdmissions::admit(&nodes, Network::ZcashRegtest).await?;
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

#[tokio::test]
async fn startup_fails_when_no_endpoint_is_admitted() -> TestResult {
    let blocks = parseable_multi_block_chain()?;
    let primary = FailoverEndpoint::serving(&blocks, 4)?;
    primary.set_unreachable(true);
    let mut fallback = FailoverEndpoint::serving(&blocks, 4)?;
    fallback.genesis_hash = Network::ZcashMainnet.genesis_hash();
    let (nodes, _switch) = NodeSet::new(primary, [fallback]);

    let refusal = NodeEndpointAdmissions::admit(&nodes, Network::ZcashRegtest)
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

fn archive_capabilities_withheld(capabilities: NodeCapabilities) -> TestResult<NodeCapabilities> {
    Ok(NodeCapabilities::new(capabilities.iter().filter(
        |capability| *capability != NodeCapability::HistoricalBlockBodies,
    ))?)
}
