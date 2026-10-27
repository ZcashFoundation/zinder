//! Canonical construction, admission, and follower handoff.

pub(crate) mod construction;
pub(crate) mod control;
pub(crate) mod follow;
pub(crate) mod ingest_control;

use std::{ffi::OsString, path::PathBuf, sync::Arc};

use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zinder_core::{BlockHeight, BlockId, NetworkUpgradeActivations, UnixTimestampMillis};
use zinder_runtime::{IngestPhase, Readiness, ReadinessState};
use zinder_source::{NodeSource, SourceError};
use zinder_store::{
    CanonicalActivationSchedule, CanonicalBaselinePublication, CanonicalReorgPolicy,
    CanonicalStoreBuildPlan, CanonicalStoreBuildPlanError, CanonicalStoreError,
    CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder, RocksDbCanonicalStore,
    RocksDbResourceBudget,
};

use crate::{
    CanonicalConstructionConfig, CanonicalConstructionError, CanonicalControlCommand,
    CanonicalFollowConfig, CanonicalFollowError, CanonicalFollower, RawBlobPolicy,
    follow_canonical_tip, follow_canonical_tip_with_control, load_fresh_canonical,
};

pub(super) const fn raw_blob_policy_for_retention(retention: RawBlobRetention) -> RawBlobPolicy {
    match retention {
        RawBlobRetention::None => RawBlobPolicy::None,
        RawBlobRetention::Transactions => RawBlobPolicy::Transactions,
        RawBlobRetention::All => RawBlobPolicy::All,
    }
}

/// Complete concrete configuration for the first `RocksDB` single-host writer.
#[derive(Clone, Debug)]
pub struct CanonicalWriterConfig {
    /// Final canonical store path.
    pub storage_path: PathBuf,
    /// Bounded `RocksDB` resources for construction, following, and reopen.
    pub resource_budget: RocksDbResourceBudget,
    /// Bounded source and preparation settings for fresh construction.
    pub construction: CanonicalConstructionConfig,
    /// Optional authenticated predecessor for a checkpointed build.
    pub checkpoint_height: Option<BlockHeight>,
    /// Immutable raw-byte retention contract for fresh construction and reopen.
    pub raw_blob_retention: RawBlobRetention,
    /// Maximum depth used to select the baseline settled tip.
    pub reorg_window_blocks: u32,
    /// Continuous follower policy.
    pub follow: CanonicalFollowConfig,
}

/// Failure while opening or constructing the canonical writer.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CanonicalWriterError {
    /// The configured store path could not be inspected.
    #[error("canonical writer path {path:?} is unavailable: {source}")]
    PathUnavailable {
        /// Path being inspected or changed.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// The source could not provide an identity needed before construction.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// The fixed construction range was invalid.
    #[error(transparent)]
    BuildPlan(#[from] CanonicalStoreBuildPlanError),
    /// Fresh canonical loading or validation failed.
    #[error(transparent)]
    Construction(#[from] CanonicalConstructionError),
    /// Canonical store admission, publication, or readback failed.
    #[error(transparent)]
    Store(#[from] CanonicalStoreError),
    /// The checkpoint cannot yet have a retained successor at the observed source tip.
    #[error(
        "source tip {source_tip:?} has not advanced beyond canonical checkpoint {checkpoint_height:?}"
    )]
    CheckpointNotBehindSource {
        /// Configured authenticated predecessor height.
        checkpoint_height: BlockHeight,
        /// Latest atomic source observation.
        source_tip: BlockId,
    },
    /// A READY store omits canonical history required by this invocation.
    #[error(
        "canonical READY store starts at height {actual_first_available_height:?}, after configured required height {required_first_available_height:?}"
    )]
    InsufficientRetainedHistory {
        /// Earliest height the current configuration requires.
        required_first_available_height: BlockHeight,
        /// Earliest height retained by the admitted READY store.
        actual_first_available_height: BlockHeight,
    },
    /// Continuous following failed after READY admission.
    #[error(transparent)]
    Follow(#[from] CanonicalFollowError),
    /// A staged READY store changed identity while it was installed and cold-reopened.
    #[error("installed canonical READY fence differs from staged publication")]
    InstalledFenceMismatch,
    /// The writer opened a store without admitting a network-upgrade schedule.
    #[error("canonical writer did not admit a network-upgrade schedule")]
    NetworkUpgradeScheduleNotAdmitted,
    /// The private canonical-control server stopped while the writer was still running.
    #[error("canonical control server failed: {reason}")]
    ControlServer {
        /// Redacted tonic transport failure.
        reason: String,
    },
    /// The live mempool owner stopped while the canonical writer was still running.
    #[error("live mempool owner failed: {reason}")]
    MempoolOwner {
        /// Bounded task-completion diagnostic.
        reason: String,
    },
    /// Durable mempool-event retention stopped while the canonical writer was
    /// still running.
    #[error("mempool retention task failed: {reason}")]
    MempoolRetention {
        /// Bounded task-completion diagnostic.
        reason: String,
    },
}

/// Opens or freshly constructs the canonical store, then follows Zebra.
pub async fn run_canonical_writer<Source>(
    source: &Source,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    config: CanonicalWriterConfig,
    readiness: &Readiness,
    cancel: &CancellationToken,
) -> Result<RocksDbCanonicalStore, CanonicalWriterError>
where
    Source: NodeSource + Clone,
{
    run_canonical_writer_with_control(
        source,
        network_upgrade_activations,
        config,
        readiness,
        cancel,
        None,
    )
    .await
}

/// Opens or constructs the one canonical primary, then follows while serving
/// commands from the private canonical-control channel.
#[expect(
    clippy::too_many_arguments,
    reason = "the controlled variant preserves the established writer dependency boundary while adding only the optional owner command receiver"
)]
pub async fn run_canonical_writer_with_control<Source>(
    source: &Source,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    config: CanonicalWriterConfig,
    readiness: &Readiness,
    cancel: &CancellationToken,
    control_commands: Option<mpsc::Receiver<CanonicalControlCommand>>,
) -> Result<RocksDbCanonicalStore, CanonicalWriterError>
where
    Source: NodeSource + Clone,
{
    let store = open_or_construct_canonical_store(
        source,
        Arc::clone(&network_upgrade_activations),
        &config,
        readiness,
    )
    .await?;
    let admitted_activations = admitted_network_upgrade_activations(&store)?;
    let follower = CanonicalFollower::new(
        source,
        admitted_activations,
        config.follow,
        readiness,
        cancel,
    );
    let store = match control_commands {
        Some(control_commands) => {
            follow_canonical_tip_with_control(store, follower, control_commands).await?
        }
        None => follow_canonical_tip(store, follower).await?,
    };
    Ok(store)
}

fn admitted_network_upgrade_activations(
    store: &RocksDbCanonicalStore,
) -> Result<Arc<NetworkUpgradeActivations>, CanonicalWriterError> {
    store
        .network_upgrade_activations()
        .cloned()
        .map(Arc::new)
        .ok_or(CanonicalWriterError::NetworkUpgradeScheduleNotAdmitted)
}

async fn open_or_construct_canonical_store<Source>(
    source: &Source,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    config: &CanonicalWriterConfig,
    readiness: &Readiness,
) -> Result<RocksDbCanonicalStore, CanonicalWriterError>
where
    Source: NodeSource + Clone,
{
    if let Some(store) = open_existing_store(network_upgrade_activations.as_ref(), config)? {
        return Ok(store);
    }
    let staging_path = construction_staging_path(&config.storage_path);
    if let Some(store) =
        recover_staged_store(&staging_path, network_upgrade_activations.as_ref(), config)?
    {
        return Ok(store);
    }
    construct_fresh_store(
        source,
        network_upgrade_activations.as_ref(),
        config,
        readiness,
        &staging_path,
    )
    .await
}

fn open_existing_store(
    network_upgrade_activations: &NetworkUpgradeActivations,
    config: &CanonicalWriterConfig,
) -> Result<Option<RocksDbCanonicalStore>, CanonicalWriterError> {
    if !path_exists(&config.storage_path)? {
        return Ok(None);
    }
    let store = RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        &config.storage_path,
        network_upgrade_activations,
        CanonicalStoreWorkload::Wallet,
        config.raw_blob_retention,
        CanonicalReorgPolicy::new(config.reorg_window_blocks)?,
        config.resource_budget,
    )?;
    validate_retained_history_coverage(&store, config)?;
    tracing::info!(
        target: "zinder::ingest",
        event = "canonical_ready_store_reopened",
        storage_path = %config.storage_path.display(),
        visible_tip_height = store.event_fence().visible_tip().height.value(),
        chain_epoch = store.event_fence().chain_epoch_id().value(),
        chain_event_sequence = store.event_fence().chain_event_sequence(),
        network_upgrade_schedule_revision = store
            .network_upgrade_schedule()
            .map_or(0, CanonicalActivationSchedule::revision),
        "reopened the authenticated canonical fence"
    );
    Ok(Some(store))
}

fn recover_staged_store(
    staging_path: &std::path::Path,
    network_upgrade_activations: &NetworkUpgradeActivations,
    config: &CanonicalWriterConfig,
) -> Result<Option<RocksDbCanonicalStore>, CanonicalWriterError> {
    if !path_exists(staging_path)? {
        discard_unpublished_block_load_staging(staging_path)?;
        return Ok(None);
    }
    if remove_empty_construction_staging(staging_path)? {
        return Ok(None);
    }
    match RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        staging_path,
        network_upgrade_activations,
        CanonicalStoreWorkload::Wallet,
        config.raw_blob_retention,
        CanonicalReorgPolicy::new(config.reorg_window_blocks)?,
        config.resource_budget,
    ) {
        Ok(staged_ready) => {
            validate_retained_history_coverage(&staged_ready, config)?;
            drop(staged_ready);
            install_staged_store(staging_path, &config.storage_path)?;
            let store = RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
                &config.storage_path,
                network_upgrade_activations,
                CanonicalStoreWorkload::Wallet,
                config.raw_blob_retention,
                CanonicalReorgPolicy::new(config.reorg_window_blocks)?,
                config.resource_budget,
            )?;
            validate_retained_history_coverage(&store, config)?;
            Ok(Some(store))
        }
        Err(CanonicalStoreError::StoreNotReady { .. }) => {
            remove_unpublished_staging(staging_path)?;
            Ok(None)
        }
        Err(source) => Err(source.into()),
    }
}

fn remove_empty_construction_staging(
    staging_path: &std::path::Path,
) -> Result<bool, CanonicalWriterError> {
    let mut entries = std::fs::read_dir(staging_path).map_err(|source| {
        CanonicalWriterError::PathUnavailable {
            path: staging_path.to_path_buf(),
            source,
        }
    })?;
    if entries
        .next()
        .transpose()
        .map_err(|source| CanonicalWriterError::PathUnavailable {
            path: staging_path.to_path_buf(),
            source,
        })?
        .is_some()
    {
        return Ok(false);
    }
    discard_unpublished_block_load_staging(staging_path)?;
    std::fs::remove_dir(staging_path).map_err(|source| CanonicalWriterError::PathUnavailable {
        path: staging_path.to_path_buf(),
        source,
    })?;
    tracing::warn!(
        target: "zinder::ingest",
        event = "canonical_empty_construction_staging_removed",
        staging_path = %staging_path.display(),
        "removed an empty writer-owned construction staging directory"
    );
    Ok(true)
}

fn remove_unpublished_staging(staging_path: &std::path::Path) -> Result<(), CanonicalWriterError> {
    discard_unpublished_block_load_staging(staging_path)?;
    std::fs::remove_dir_all(staging_path).map_err(|source| {
        CanonicalWriterError::PathUnavailable {
            path: staging_path.to_path_buf(),
            source,
        }
    })?;
    tracing::warn!(
        target: "zinder::ingest",
        event = "canonical_unpublished_construction_restarted",
        staging_path = %staging_path.display(),
        "removed an unpublished writer-owned construction staging directory"
    );
    Ok(())
}

fn discard_unpublished_block_load_staging(
    staging_path: &std::path::Path,
) -> Result<(), CanonicalWriterError> {
    if RocksDbCanonicalBuilder::discard_unpublished_block_load_staging(staging_path)? {
        tracing::warn!(
            target: "zinder::ingest",
            event = "canonical_unpublished_block_load_staging_discarded",
            canonical_build_path = %staging_path.display(),
            "discarded unpublished writer-owned canonical block-load staging"
        );
    }
    Ok(())
}

async fn construct_fresh_store<Source>(
    source: &Source,
    network_upgrade_activations: &NetworkUpgradeActivations,
    config: &CanonicalWriterConfig,
    readiness: &Readiness,
    staging_path: &std::path::Path,
) -> Result<RocksDbCanonicalStore, CanonicalWriterError>
where
    Source: NodeSource + Clone,
{
    readiness.set(ReadinessState::syncing(None, None, None).with_phase(IngestPhase::BulkCatchup));
    let (build_plan, settled_tip, observed_tip) =
        resolve_construction_range(source, network_upgrade_activations, config).await?;
    readiness.set(
        ReadinessState::syncing(
            Some(u64::from(observed_tip.height.value().saturating_sub(
                build_plan.history_bounds().first_available_height().value(),
            ))),
            None,
            Some(observed_tip.height.value()),
        )
        .with_phase(IngestPhase::BulkCatchup),
    );
    let builder = RocksDbCanonicalBuilder::create_fresh(
        staging_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        config.resource_budget,
    )?;
    tracing::info!(
        target: "zinder::ingest",
        event = "canonical_fresh_construction_started",
        staging_path = %staging_path.display(),
        fixed_tip_height = observed_tip.height.value(),
        "started canonical construction"
    );
    let loaded = load_fresh_canonical(builder, source, &config.construction).await?;
    let validated = loaded.builder.prepare_trusted_fresh_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        settled_tip,
        UnixTimestampMillis::now(),
    ))?;
    let staged_ready = validated.publish_baseline(publication)?;
    let published_fence = staged_ready.event_fence();
    drop(staged_ready);
    install_staged_store(staging_path, &config.storage_path)?;
    let store = RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        &config.storage_path,
        network_upgrade_activations,
        CanonicalStoreWorkload::Wallet,
        config.raw_blob_retention,
        CanonicalReorgPolicy::new(config.reorg_window_blocks)?,
        config.resource_budget,
    )?;
    validate_retained_history_coverage(&store, config)?;
    if store.event_fence() != published_fence {
        return Err(CanonicalWriterError::InstalledFenceMismatch);
    }
    tracing::info!(
        target: "zinder::ingest",
        event = "canonical_fresh_construction_ready",
        storage_path = %config.storage_path.display(),
        visible_tip_height = published_fence.visible_tip().height.value(),
        chain_epoch = published_fence.chain_epoch_id().value(),
        chain_event_sequence = published_fence.chain_event_sequence(),
        "published and cold-reopened the canonical baseline"
    );
    Ok(store)
}

fn validate_retained_history_coverage(
    store: &RocksDbCanonicalStore,
    config: &CanonicalWriterConfig,
) -> Result<(), CanonicalWriterError> {
    validate_first_available_height(
        store.history_bounds().first_available_height(),
        configured_first_available_height(config.checkpoint_height),
    )
}

fn configured_first_available_height(checkpoint_height: Option<BlockHeight>) -> BlockHeight {
    checkpoint_height.map_or(BlockHeight::new(1), |checkpoint_height| {
        BlockHeight::new(checkpoint_height.value().saturating_add(1))
    })
}

fn validate_first_available_height(
    actual_first_available_height: BlockHeight,
    required_first_available_height: BlockHeight,
) -> Result<(), CanonicalWriterError> {
    if actual_first_available_height > required_first_available_height {
        return Err(CanonicalWriterError::InsufficientRetainedHistory {
            required_first_available_height,
            actual_first_available_height,
        });
    }
    Ok(())
}

async fn resolve_construction_range<Source>(
    source: &Source,
    network_upgrade_activations: &NetworkUpgradeActivations,
    config: &CanonicalWriterConfig,
) -> Result<(CanonicalStoreBuildPlan, BlockId, BlockId), CanonicalWriterError>
where
    Source: NodeSource,
{
    let source_tip = source.tip_id().await?;
    let fixed_tip = match config.follow.target_height {
        Some(target_height) if target_height < source_tip.height => {
            let block = source.fetch_block_at(target_height).await?;
            BlockId::new(target_height, block.hash)
        }
        _ => source_tip,
    };
    let build_plan =
        resolve_build_plan(source, network_upgrade_activations, config, fixed_tip).await?;
    let first_retained_height = build_plan.history_bounds().first_available_height();
    let settled_height = BlockHeight::new(
        fixed_tip
            .height
            .value()
            .saturating_sub(config.reorg_window_blocks)
            .max(first_retained_height.value()),
    );
    let settled_tip = if settled_height == fixed_tip.height {
        fixed_tip
    } else {
        let block = source.fetch_block_at(settled_height).await?;
        BlockId::new(settled_height, block.hash)
    };
    Ok((build_plan, settled_tip, fixed_tip))
}

async fn resolve_build_plan<Source>(
    source: &Source,
    network_upgrade_activations: &NetworkUpgradeActivations,
    config: &CanonicalWriterConfig,
    fixed_tip: BlockId,
) -> Result<CanonicalStoreBuildPlan, CanonicalWriterError>
where
    Source: NodeSource,
{
    if let Some(checkpoint_height) = config.checkpoint_height {
        if fixed_tip.height <= checkpoint_height {
            return Err(CanonicalWriterError::CheckpointNotBehindSource {
                checkpoint_height,
                source_tip: fixed_tip,
            });
        }
        if checkpoint_height != BlockHeight::new(0) {
            let checkpoint = source
                .fetch_chain_checkpoint(checkpoint_height, network_upgrade_activations)
                .await?;
            return CanonicalStoreBuildPlan::checkpointed(
                network_upgrade_activations,
                checkpoint,
                fixed_tip,
                config.raw_blob_retention,
                CanonicalReorgPolicy::new(config.reorg_window_blocks)?,
            )
            .map_err(CanonicalWriterError::from);
        }
    }

    let genesis = source.fetch_block_at(BlockHeight::new(0)).await?;
    CanonicalStoreBuildPlan::complete(
        network_upgrade_activations,
        genesis.block_time_seconds,
        fixed_tip,
        config.raw_blob_retention,
        CanonicalReorgPolicy::new(config.reorg_window_blocks)?,
    )
    .map_err(CanonicalWriterError::from)
}

fn construction_staging_path(storage_path: &std::path::Path) -> PathBuf {
    let mut staging_path = OsString::from(storage_path.as_os_str());
    staging_path.push(".building");
    PathBuf::from(staging_path)
}

fn path_exists(path: &std::path::Path) -> Result<bool, CanonicalWriterError> {
    path.try_exists()
        .map_err(|source| CanonicalWriterError::PathUnavailable {
            path: path.to_path_buf(),
            source,
        })
}

fn install_staged_store(
    staging_path: &std::path::Path,
    storage_path: &std::path::Path,
) -> Result<(), CanonicalWriterError> {
    std::fs::rename(staging_path, storage_path).map_err(|source| {
        CanonicalWriterError::PathUnavailable {
            path: storage_path.to_path_buf(),
            source,
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::PathBuf, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use tempfile::tempdir;
    use zinder_core::{
        BlockFinalNoteCommitmentRoots, BlockHash, BlockHeaderArtifact, BlockHeight, BlockId,
        CanonicalBlockFacts, CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion,
        ChainTipMetadata, CommitmentTreeCheckpoint, CommitmentTreeFrontier,
        CommitmentTreeFrontiers, CompactBlockArtifact, CompactChainMetadata, ConsensusBranchId,
        Network, NetworkUpgradeActivation, NetworkUpgradeActivations, SerializedBytesDigest,
        ShieldedProtocol, UnixTimestampMillis, encode_canonical_block_replay,
    };
    use zinder_source::{
        NodeCapabilities, NodeSource, SourceBlock, SourceBlockHeader, SourceError,
    };
    use zinder_testkit::sample_regtest_upgrade_activations;

    use super::{
        CanonicalWriterConfig, CanonicalWriterError, admitted_network_upgrade_activations,
        discard_unpublished_block_load_staging, open_existing_store,
        remove_empty_construction_staging, remove_unpublished_staging, resolve_build_plan,
        validate_first_available_height,
    };
    use crate::{CanonicalConstructionConfig, CanonicalFollowConfig};
    use zinder_store::{
        CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalReorgPolicy,
        CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder,
        RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget,
    };

    #[derive(Clone)]
    struct GenesisSource;

    #[test]
    fn ready_store_rejects_narrower_history_than_configured() {
        let outcome =
            validate_first_available_height(BlockHeight::new(4_189_466), BlockHeight::new(280_000));

        assert!(matches!(
            outcome,
            Err(CanonicalWriterError::InsufficientRetainedHistory {
                required_first_available_height,
                actual_first_available_height,
            }) if required_first_available_height == BlockHeight::new(280_000)
                && actual_first_available_height == BlockHeight::new(4_189_466)
        ));
    }

    #[test]
    fn ready_store_accepts_equal_or_broader_history() -> Result<(), CanonicalWriterError> {
        validate_first_available_height(BlockHeight::new(280_000), BlockHeight::new(280_000))?;
        validate_first_available_height(BlockHeight::new(1), BlockHeight::new(280_000))?;
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one writer restart lifecycle checks future admission and immutable history together"
    )]
    fn writer_admits_future_schedule_and_rejects_historical_change_without_mutation()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempdir()?;
        let store_path = temporary.path().join("canonical");
        let baseline = sample_regtest_upgrade_activations();
        publish_ready_store(&store_path, &baseline)?;
        let baseline_fingerprint =
            baseline.fingerprint(zinder_core::NetworkUpgradeActivationsFingerprintVersion::V1);
        let manifest_before =
            RocksDbCanonicalStore::read_construction_manifest_binding(&store_path)?;

        let mut entries = baseline.activations().to_vec();
        entries.push(NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(0x1234_5678),
            activation_height: BlockHeight::new(604),
            name: "Future writer test activation".to_owned(),
        });
        let extended = NetworkUpgradeActivations::new(Network::ZcashRegtest, entries)?;
        let config = writer_config(&store_path, &baseline);
        let store = open_existing_store(&extended, &config)?
            .ok_or("published canonical store should reopen")?;

        assert_eq!(
            store.build_plan().network_upgrade_activations_fingerprint(),
            baseline_fingerprint
        );
        assert_eq!(
            store
                .network_upgrade_schedule()
                .map(zinder_store::CanonicalActivationSchedule::revision),
            Some(1)
        );
        assert_eq!(store.network_upgrade_activations(), Some(&extended));
        assert_eq!(
            admitted_network_upgrade_activations(&store)?.as_ref(),
            &extended
        );
        assert_eq!(
            RocksDbCanonicalStore::read_construction_manifest_binding(&store_path)?,
            manifest_before
        );

        let fence = store.event_fence();
        let settled_tip = fence.visible_tip();
        let next_height = BlockHeight::new(fence.visible_tip().height.value() + 1);
        let block = canonical_build_block(
            next_height,
            [3; 32],
            fence.visible_tip().hash.as_bytes(),
            &extended,
        );
        let (store, _) = store.commit_live_append(
            zinder_store::CanonicalLiveAppend::new(
                fence,
                block,
                Vec::new(),
                settled_tip,
                UnixTimestampMillis::new(1_750_000_001_000),
            ),
            &extended,
        )?;
        assert_eq!(store.network_upgrade_activations(), Some(&extended));
        assert_eq!(
            store
                .network_upgrade_schedule()
                .map(zinder_store::CanonicalActivationSchedule::revision),
            Some(1)
        );
        assert_eq!(store.event_fence().visible_tip().height, next_height);
        assert_eq!(
            RocksDbCanonicalStore::read_construction_manifest_binding(&store_path)?,
            manifest_before
        );
        let fence_before_rejection = store.event_fence();
        drop(store);

        let mut historical_entries = extended.activations().to_vec();
        historical_entries[0].name = "Rewritten historical activation".to_owned();
        let historical = NetworkUpgradeActivations::new(Network::ZcashRegtest, historical_entries)?;
        let error = open_existing_store(&historical, &writer_config(&store_path, &extended))
            .err()
            .ok_or("writer must reject an activation change at the locked tip")?;
        assert!(matches!(error, CanonicalWriterError::Store(_)));
        assert_eq!(
            RocksDbCanonicalStore::read_construction_manifest_binding(&store_path)?,
            manifest_before
        );

        let reopened = open_existing_store(&extended, &writer_config(&store_path, &extended))?
            .ok_or("canonical store should remain available after rejected admission")?;
        assert_eq!(reopened.event_fence(), fence_before_rejection);
        assert_eq!(
            reopened
                .network_upgrade_schedule()
                .map(zinder_store::CanonicalActivationSchedule::revision),
            Some(1)
        );
        assert_eq!(reopened.network_upgrade_activations(), Some(&extended));

        let secondary = RocksDbCanonicalSecondary::open_ready(
            &store_path,
            temporary.path().join("canonical-secondary"),
            &extended,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(1)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(secondary.network_upgrade_activations(), Some(&extended));
        assert_eq!(
            secondary
                .network_upgrade_schedule()
                .map(zinder_store::CanonicalActivationSchedule::revision),
            Some(1)
        );
        Ok(())
    }

    const NU7_BRANCH_ID: u32 = 0x7719_0ad9;
    /// Mainnet tip on 2026-10-27, before NU7 activates at 3,543,405.
    const MAINNET_PRE_NU7_TIP: u32 = 3_532_453;
    /// Last Testnet block a pre-NU7 parser can ingest; NU7 activates at 4,465,026.
    const TESTNET_PRE_NU7_TIP: u32 = 4_465_025;

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one restart lifecycle per network covers refusal, admission, restart, and reader convergence"
    )]
    fn writer_admits_the_zebra_seven_nu7_row_on_a_store_built_against_zebra_six()
    -> Result<(), Box<dyn Error>> {
        for (network, store_tip) in [
            (Network::ZcashMainnet, MAINNET_PRE_NU7_TIP),
            (Network::ZcashTestnet, TESTNET_PRE_NU7_TIP),
        ] {
            let temporary = tempdir()?;
            let store_path = temporary.path().join("canonical");
            let zebra_six = zebra_upgrade_activations(network, false)?;
            let zebra_seven = zebra_upgrade_activations(network, true)?;
            publish_checkpointed_store(&store_path, &zebra_six, BlockHeight::new(store_tip))?;
            let manifest_before =
                RocksDbCanonicalStore::read_construction_manifest_binding(&store_path)?;

            let premature_reader = RocksDbCanonicalSecondary::open_ready(
                &store_path,
                temporary.path().join("premature-secondary"),
                &zebra_seven,
                CanonicalStoreWorkload::Wallet,
                RawBlobRetention::Transactions,
                CanonicalReorgPolicy::new(1)?,
                RocksDbResourceBudget::for_local_tests(),
            );
            assert!(
                premature_reader.is_err(),
                "{network:?}: a reader must not admit the Zebra 7 table before the writer does"
            );

            for rejected in [
                swap_historical_upgrades(&zebra_seven)?,
                move_historical_upgrade(&zebra_seven)?,
            ] {
                let error = open_existing_store(
                    &rejected,
                    &checkpointed_writer_config(&store_path, &rejected, store_tip),
                )
                .err()
                .ok_or("an unprovable legacy table must be refused")?;
                assert!(
                    error.to_string().contains("exact prefix"),
                    "{network:?}: {error}"
                );
            }

            let store = open_existing_store(
                &zebra_seven,
                &checkpointed_writer_config(&store_path, &zebra_seven, store_tip),
            )?
            .ok_or("published canonical store should reopen")?;
            assert_eq!(
                store.build_plan().network_upgrade_activations_fingerprint(),
                zebra_six.fingerprint(zinder_core::NetworkUpgradeActivationsFingerprintVersion::V1)
            );
            let schedule = store
                .network_upgrade_schedule()
                .ok_or("writer must admit the Zebra 7 schedule")?;
            assert_eq!(schedule.revision(), 1);
            assert_eq!(schedule.activations(), &zebra_seven);
            let admitted = admitted_network_upgrade_activations(&store)?;
            let nu7_height = admitted
                .activation_height_by_name("NU7")
                .ok_or("admitted schedule must carry NU7")?;
            assert!(nu7_height.value() > store_tip);
            let last_pre_nu7 = BlockHeight::new(nu7_height.value() - 1);
            assert_eq!(
                admitted.consensus_branch_id_at(last_pre_nu7),
                zebra_six.consensus_branch_id_at(last_pre_nu7)
            );
            assert_eq!(
                admitted.consensus_branch_id_at(nu7_height),
                ConsensusBranchId::new(NU7_BRANCH_ID)
            );
            drop(store);

            let reopened = open_existing_store(
                &zebra_seven,
                &checkpointed_writer_config(&store_path, &zebra_seven, store_tip),
            )?
            .ok_or("admitted canonical store should reopen")?;
            assert_eq!(
                reopened
                    .network_upgrade_schedule()
                    .map(zinder_store::CanonicalActivationSchedule::revision),
                Some(1),
                "{network:?}: restarting with the same table must not add a revision"
            );
            drop(reopened);

            for rejected in [
                swap_historical_upgrades(&zebra_seven)?,
                move_historical_upgrade(&zebra_seven)?,
            ] {
                let error = open_existing_store(
                    &rejected,
                    &checkpointed_writer_config(&store_path, &rejected, store_tip),
                )
                .err()
                .ok_or("a historical change after admission must be refused")?;
                assert!(
                    error.to_string().contains("at or below canonical tip"),
                    "{network:?}: {error}"
                );
            }

            for reader_table in [&zebra_seven, &zebra_six] {
                let secondary = RocksDbCanonicalSecondary::open_ready(
                    &store_path,
                    temporary
                        .path()
                        .join(format!("secondary-{}", reader_table.activations().len())),
                    reader_table,
                    CanonicalStoreWorkload::Wallet,
                    RawBlobRetention::Transactions,
                    CanonicalReorgPolicy::new(1)?,
                    RocksDbResourceBudget::for_local_tests(),
                )?;
                assert_eq!(secondary.network_upgrade_activations(), Some(&zebra_seven));
            }
            assert_eq!(
                RocksDbCanonicalStore::read_construction_manifest_binding(&store_path)?,
                manifest_before
            );
        }
        Ok(())
    }

    #[test]
    fn writer_refuses_nu7_when_the_store_tip_already_reached_its_activation_height()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempdir()?;
        let store_path = temporary.path().join("canonical");
        let zebra_six = zebra_upgrade_activations(Network::ZcashTestnet, false)?;
        let zebra_seven = zebra_upgrade_activations(Network::ZcashTestnet, true)?;
        publish_checkpointed_store(
            &store_path,
            &zebra_six,
            BlockHeight::new(TESTNET_PRE_NU7_TIP + 1),
        )?;

        let error = open_existing_store(
            &zebra_seven,
            &checkpointed_writer_config(&store_path, &zebra_seven, TESTNET_PRE_NU7_TIP + 1),
        )
        .err()
        .ok_or("NU7 at or below the stored tip must be refused")?;
        assert!(
            error
                .to_string()
                .contains("adds an activation at or below canonical tip"),
            "{error}"
        );
        let unchanged = open_existing_store(
            &zebra_six,
            &checkpointed_writer_config(&store_path, &zebra_six, TESTNET_PRE_NU7_TIP + 1),
        )?
        .ok_or("the refused store must remain available")?;
        assert_eq!(
            unchanged.network_upgrade_activations(),
            Some(&zebra_six),
            "a refused revision must not be persisted"
        );
        Ok(())
    }

    fn checkpointed_writer_config(
        storage_path: &std::path::Path,
        activations: &NetworkUpgradeActivations,
        tip: u32,
    ) -> CanonicalWriterConfig {
        CanonicalWriterConfig {
            checkpoint_height: Some(BlockHeight::new(tip - 1)),
            ..writer_config(storage_path, activations)
        }
    }

    /// `getblockchaininfo.upgrades` as Zebra 6.2.2 advertises it, optionally
    /// with the NU7 row Zebra 7.0.0 appends.
    fn zebra_upgrade_activations(
        network: Network,
        with_nu7: bool,
    ) -> Result<NetworkUpgradeActivations, zinder_core::NetworkUpgradeActivationsError> {
        let (heights, nu7_height): ([u32; 10], u32) = if network == Network::ZcashMainnet {
            (
                [
                    347_500, 419_200, 653_600, 903_000, 1_046_400, 1_687_104, 2_726_400, 3_146_400,
                    3_364_600, 3_428_143,
                ],
                3_543_405,
            )
        } else {
            (
                [
                    207_500, 280_000, 584_000, 903_800, 1_028_500, 1_842_420, 2_976_000, 3_536_500,
                    4_052_000, 4_134_000,
                ],
                4_465_026,
            )
        };
        let upgrades = [
            (0x5ba8_1b19, "Overwinter"),
            (0x76b8_09bb, "Sapling"),
            (0x2bb4_0e60, "Blossom"),
            (0xf5b9_230b, "Heartwood"),
            (0xe9ff_75a6, "Canopy"),
            (0xc2d6_d0b4, "NU5"),
            (0xc8e7_1055, "NU6"),
            (0x4dec_4df0, "NU6.1"),
            (0x5437_f330, "NU6.2"),
            (0x37a5_165b, "NU6.3"),
        ];
        let mut entries: Vec<_> = upgrades
            .into_iter()
            .zip(heights)
            .map(|((branch_id, name), height)| NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(branch_id),
                activation_height: BlockHeight::new(height),
                name: name.to_owned(),
            })
            .collect();
        if with_nu7 {
            entries.push(NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(NU7_BRANCH_ID),
                activation_height: BlockHeight::new(nu7_height),
                name: "NU7".to_owned(),
            });
        }
        NetworkUpgradeActivations::new(network, entries)
    }

    /// Reorders NU6.2 and NU6.3 by exchanging their activation heights.
    fn swap_historical_upgrades(
        activations: &NetworkUpgradeActivations,
    ) -> Result<NetworkUpgradeActivations, Box<dyn Error>> {
        let mut entries = activations.activations().to_vec();
        let nu6_2 = entries
            .iter()
            .position(|entry| entry.name == "NU6.2")
            .ok_or("table must carry NU6.2")?;
        let nu6_3 = entries
            .iter()
            .position(|entry| entry.name == "NU6.3")
            .ok_or("table must carry NU6.3")?;
        let nu6_2_height = entries[nu6_2].activation_height;
        entries[nu6_2].activation_height = entries[nu6_3].activation_height;
        entries[nu6_3].activation_height = nu6_2_height;
        Ok(NetworkUpgradeActivations::new(
            activations.network(),
            entries,
        )?)
    }

    /// Moves the already-crossed NU6.3 activation one block later.
    fn move_historical_upgrade(
        activations: &NetworkUpgradeActivations,
    ) -> Result<NetworkUpgradeActivations, Box<dyn Error>> {
        let mut entries = activations.activations().to_vec();
        let nu6_3 = entries
            .iter_mut()
            .find(|entry| entry.name == "NU6.3")
            .ok_or("table must carry NU6.3")?;
        nu6_3.activation_height = BlockHeight::new(nu6_3.activation_height.value() + 1);
        Ok(NetworkUpgradeActivations::new(
            activations.network(),
            entries,
        )?)
    }

    /// Publishes a READY store whose retained history is the single block at
    /// `tip`, checkpointed at its parent.
    fn publish_checkpointed_store(
        path: &std::path::Path,
        activations: &NetworkUpgradeActivations,
        tip: BlockHeight,
    ) -> Result<(), Box<dyn Error>> {
        let parent = BlockId::new(
            BlockHeight::new(tip.value() - 1),
            BlockHash::from_bytes([1; 32]),
        );
        let build_tip = BlockId::new(tip, BlockHash::from_bytes([2; 32]));
        let build_plan = zinder_store::CanonicalStoreBuildPlan::checkpointed(
            activations,
            CommitmentTreeCheckpoint::new(
                parent,
                1_700_000_000,
                test_frontiers(activations, parent.height),
            ),
            build_tip,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(1)?,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let block = canonical_build_block(tip, [2; 32], [1; 32], activations);
        builder.bulk_load_blocks([Ok::<_, std::io::Error>(block)])?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            build_tip,
            tip.value(),
            test_frontiers(activations, tip),
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            build_tip,
            UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        drop(validated.publish_baseline(publication)?);
        Ok(())
    }

    fn writer_config(
        storage_path: &std::path::Path,
        activations: &NetworkUpgradeActivations,
    ) -> CanonicalWriterConfig {
        CanonicalWriterConfig {
            storage_path: storage_path.to_path_buf(),
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            construction: CanonicalConstructionConfig::for_local_tests(
                Duration::from_secs(1),
                Arc::new(activations.clone()),
            ),
            checkpoint_height: None,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_window_blocks: 1,
            follow: CanonicalFollowConfig {
                request_timeout: Duration::from_secs(1),
                poll_interval: Duration::from_millis(1),
                lag_threshold_blocks: 0,
                target_height: None,
                event_retention_window: None,
                event_retention_check_interval: Duration::from_secs(1),
                mempool_ready_gate: None,
                max_replacement_bytes: crate::DEFAULT_MAX_REPLACEMENT_BYTES,
            },
        }
    }

    fn publish_ready_store(
        path: &std::path::Path,
        activations: &NetworkUpgradeActivations,
    ) -> Result<(), Box<dyn Error>> {
        let build_tip = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32]));
        let reorg_policy = CanonicalReorgPolicy::new(1)?;
        let build_plan = zinder_store::CanonicalStoreBuildPlan::complete(
            activations,
            1_700_000_000,
            build_tip,
            RawBlobRetention::Transactions,
            reorg_policy,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let first = canonical_build_block(
            BlockHeight::new(1),
            [1; 32],
            Network::ZcashRegtest.genesis_hash().as_bytes(),
            activations,
        );
        let tip = canonical_build_block(BlockHeight::new(2), [2; 32], [1; 32], activations);
        builder.bulk_load_blocks([Ok::<_, std::io::Error>(first), Ok(tip)])?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            build_tip,
            2,
            test_frontiers(activations, BlockHeight::new(2)),
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        drop(validated.publish_baseline(publication)?);
        Ok(())
    }

    fn canonical_build_block(
        height: BlockHeight,
        block_hash: [u8; 32],
        parent_hash: [u8; 32],
        activations: &NetworkUpgradeActivations,
    ) -> CanonicalBuildBlock {
        let facts = CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                height,
                BlockHash::from_bytes(block_hash),
                BlockHash::from_bytes(parent_hash),
                [3; 32],
                [4; 32],
                i64::from(height.value()),
                0x1d00_ffff,
                [5; 32],
                4,
                128,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[]),
            transactions: Vec::new(),
        };
        let replay_envelope = encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );
        let tree_state_checkpoint = (height.value() >= 2).then(|| {
            CommitmentTreeCheckpoint::new(
                BlockId::new(height, facts.block_header.block_hash),
                height.value(),
                test_frontiers(activations, height),
            )
        });
        CanonicalBuildBlock {
            compact_block: CompactBlockArtifact::empty(
                BlockId::new(height, facts.block_header.block_hash),
                facts.block_header.parent_hash,
                height.value(),
                CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            ),
            replay_envelope,
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            tree_state_checkpoint,
            block_final_note_commitment_roots: None::<BlockFinalNoteCommitmentRoots>,
            transaction_blobs: Vec::new(),
            block_blob: None,
            facts,
        }
    }

    fn test_frontiers(
        activations: &NetworkUpgradeActivations,
        height: BlockHeight,
    ) -> CommitmentTreeFrontiers {
        let frontier = |protocol: ShieldedProtocol| {
            activations
                .activation_height_by_name(protocol.activation_upgrade_name())
                .is_some_and(|activation_height| activation_height <= height)
                .then(|| CommitmentTreeFrontier::empty(protocol))
        };
        CommitmentTreeFrontiers::from_validated_parts(
            frontier(ShieldedProtocol::Sapling),
            frontier(ShieldedProtocol::Orchard),
            frontier(ShieldedProtocol::Ironwood),
        )
    }

    #[async_trait]
    impl NodeSource for GenesisSource {
        fn capabilities(&self) -> NodeCapabilities {
            NodeCapabilities::new([]).unwrap_or_default()
        }

        async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
            if height != BlockHeight::new(0) {
                return Err(SourceError::BlockUnavailable {
                    height,
                    reason: "genesis source exposes only height zero".to_owned(),
                });
            }
            Ok(SourceBlock::new(
                SourceBlockHeader {
                    network: Network::ZcashRegtest,
                    height,
                    hash: BlockHash::from_bytes([0; 32]),
                    parent_hash: BlockHash::from_bytes([0; 32]),
                    block_time_seconds: 1_700_000_000,
                },
                Vec::new(),
            ))
        }

        async fn tip_id(&self) -> Result<BlockId, SourceError> {
            Ok(BlockId::new(
                BlockHeight::new(2),
                BlockHash::from_bytes([2; 32]),
            ))
        }
    }

    #[tokio::test]
    async fn genesis_predecessor_selects_complete_history_without_genesis_artifact()
    -> Result<(), Box<dyn Error>> {
        let activations = sample_regtest_upgrade_activations();
        let config = CanonicalWriterConfig {
            storage_path: PathBuf::new(),
            resource_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
            construction: CanonicalConstructionConfig::for_local_tests(
                Duration::from_secs(1),
                Arc::new(activations.clone()),
            ),
            checkpoint_height: Some(BlockHeight::new(0)),
            raw_blob_retention: zinder_store::RawBlobRetention::Transactions,
            reorg_window_blocks: 100,
            follow: CanonicalFollowConfig {
                request_timeout: Duration::from_secs(1),
                poll_interval: Duration::from_millis(1),
                lag_threshold_blocks: 0,
                target_height: None,
                event_retention_window: None,
                event_retention_check_interval: Duration::from_secs(1),
                mempool_ready_gate: None,
                max_replacement_bytes: crate::DEFAULT_MAX_REPLACEMENT_BYTES,
            },
        };
        let plan = resolve_build_plan(
            &GenesisSource,
            &activations,
            &config,
            BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32])),
        )
        .await?;

        assert_eq!(
            plan.history_bounds().first_available_height(),
            BlockHeight::new(1)
        );
        assert_eq!(
            plan.history_predecessor().block_id.height,
            BlockHeight::new(0)
        );
        Ok(())
    }

    #[test]
    fn empty_construction_staging_is_removed() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let staging_path = tempdir.path().join("store.building");
        std::fs::create_dir(&staging_path)?;

        assert!(remove_empty_construction_staging(&staging_path)?);
        assert!(!staging_path.exists());
        Ok(())
    }

    #[test]
    fn populated_construction_staging_is_preserved() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let staging_path = tempdir.path().join("store.building");
        std::fs::create_dir(&staging_path)?;
        std::fs::write(staging_path.join("CURRENT"), b"MANIFEST-000001\n")?;

        assert!(!remove_empty_construction_staging(&staging_path)?);
        assert!(staging_path.exists());
        Ok(())
    }

    #[test]
    fn unpublished_construction_removes_block_load_staging() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let staging_path = tempdir.path().join("store.building");
        let block_load_staging_path = tempdir.path().join("store.building.block-load-staging");
        std::fs::create_dir(&staging_path)?;
        std::fs::create_dir(&block_load_staging_path)?;
        std::fs::write(block_load_staging_path.join("partial.sst"), b"partial")?;

        remove_unpublished_staging(&staging_path)?;

        assert!(!staging_path.exists());
        assert!(!block_load_staging_path.exists());
        Ok(())
    }

    #[test]
    fn orphaned_block_load_staging_is_discarded() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let staging_path = tempdir.path().join("store.building");
        let block_load_staging_path = tempdir.path().join("store.building.block-load-staging");
        std::fs::create_dir(&block_load_staging_path)?;
        std::fs::write(block_load_staging_path.join("partial.sst"), b"partial")?;

        discard_unpublished_block_load_staging(&staging_path)?;

        assert!(!staging_path.exists());
        assert!(!block_load_staging_path.exists());
        Ok(())
    }

    #[test]
    fn missing_block_load_staging_is_accepted() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;

        discard_unpublished_block_load_staging(&tempdir.path().join("store.building"))?;

        Ok(())
    }
}
